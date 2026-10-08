"""
Message Bus
Two streams connect the API to its workers:

  commands  API -> workers   Work to do. Each command is handled by one worker.
  updates   workers -> API   Progress. Every API process sees every update.

The API never executes queued work itself and workers never serve HTTP, so
either side can be scaled or restarted on its own.

Two implementations share this interface:
  InMemoryBus (this file)       one process, no extra services. Dev and tests.
  KafkaBus (kafka_bus.py)       durable, shared between pods.

Delivery is at-least-once: a command stays in the queue until a worker
acknowledges it, and a worker only acknowledges after the work succeeded or
its failure was recorded. A command can therefore be delivered twice (for
example when a worker dies after CMS applied a change but before the
acknowledgement), so handlers must be safe to run again.
"""

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Deque, Dict, List, Optional, Protocol, Sequence, Tuple

from app.models.job_model import Command, JobUpdate

logger = logging.getLogger(__name__)


class BusUnavailableError(Exception):
    """The queue could not accept a message. Maps to HTTP 503."""

    code = "queue_unavailable"
    http_status = 503

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "message": self.message, "retryable": True}


class Delivery:
    """One command handed to a worker, plus what the bus needs to acknowledge it"""

    def __init__(self, command: Command, handle: Any = None, attempt: int = 1,
                 queued_at: Optional[datetime] = None):
        self.command = command
        self.handle = handle  # bus-specific (Kafka: topic, partition, offset)
        self.attempt = attempt
        self.queued_at = queued_at  # when the bus stored it, where the bus says (Kafka: the record timestamp)


class CommandConsumer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def next(self, timeout: float = 1.0) -> Optional[Delivery]:
        """Wait up to `timeout` seconds for the next command"""

    async def ack(self, delivery: Delivery) -> None:
        """The command is finished for good; never deliver it again"""

    async def retry(self, delivery: Delivery) -> None:
        """Do not acknowledge; deliver this same command again next"""

    async def dead_letter(self, delivery: Delivery, error: Dict[str, Any]) -> None:
        """Park a command that failed for good, with the reason, for a person to look at"""


class MessageBus(Protocol):
    name: str

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish_command(self, command: Command) -> None:
        """Returns only once the command is safely stored. Raises BusUnavailableError otherwise."""

    async def publish_update(self, update: JobUpdate) -> None: ...

    async def publish_event(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        """
        Publish a JSON message to a topic outside this app's own (for example the
        XGS/SMx topic). Returns only once it is safely stored.
        """

    def updates(self) -> AsyncIterator[JobUpdate]:
        """Every update, including ones published before this call where the bus keeps history"""

    def consume_commands(self, lanes: Sequence[str]) -> CommandConsumer: ...


class InMemoryBus:
    """Queues inside this process. Nothing survives a restart."""

    name = "in-memory"

    def __init__(self) -> None:
        self._queues: Dict[str, Deque[Delivery]] = {}
        self._arrived = asyncio.Event()
        self._subscribers: List[asyncio.Queue] = []
        self.dead_letters: List[Tuple[Command, Dict[str, Any]]] = []
        self.events: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}  # topic -> [(key, value)]

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish_command(self, command: Command) -> None:
        # Round-trip through JSON so anything Kafka could not carry fails here too
        copy = Command.model_validate_json(command.model_dump_json())
        self._queues.setdefault(copy.lane, deque()).append(
            Delivery(copy, attempt=0, queued_at=datetime.now(timezone.utc)))
        self._arrived.set()

    async def publish_update(self, update: JobUpdate) -> None:
        copy = JobUpdate.model_validate_json(update.model_dump_json())
        for queue in self._subscribers:
            queue.put_nowait(copy)

    async def publish_event(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        self.events.setdefault(topic, []).append((key, json.loads(json.dumps(value))))

    def updates(self) -> AsyncIterator[JobUpdate]:
        # Subscribe now, not on first read, so nothing published in between is missed
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.append(queue)

        async def stream() -> AsyncIterator[JobUpdate]:
            try:
                while True:
                    yield await queue.get()
            finally:
                self._subscribers.remove(queue)

        return stream()

    def consume_commands(self, lanes: Sequence[str]) -> "_InMemoryConsumer":
        return _InMemoryConsumer(self, lanes)


class _InMemoryConsumer:
    def __init__(self, bus: InMemoryBus, lanes: Sequence[str]):
        self._bus = bus
        self._lanes = list(lanes)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def next(self, timeout: float = 1.0) -> Optional[Delivery]:
        delivery = self._take()
        if delivery is None:
            self._bus._arrived.clear()
            try:
                await asyncio.wait_for(self._bus._arrived.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                return None
            delivery = self._take()
        if delivery is not None:
            delivery.attempt += 1
        return delivery

    def _take(self) -> Optional[Delivery]:
        for lane in self._lanes:
            queue = self._bus._queues.get(lane)
            if queue:
                return queue.popleft()
        return None

    async def ack(self, delivery: Delivery) -> None:
        return None  # already removed from the queue by next()

    async def retry(self, delivery: Delivery) -> None:
        # Back to the front, so order is kept
        self._bus._queues.setdefault(delivery.command.lane, deque()).appendleft(delivery)
        self._bus._arrived.set()

    async def dead_letter(self, delivery: Delivery, error: Dict[str, Any]) -> None:
        self._bus.dead_letters.append((delivery.command, error))


def build_bus(settings: Any) -> MessageBus:
    """Kafka when KAFKA_BOOTSTRAP_SERVERS is set, otherwise in-process queues"""
    if settings.kafka_bootstrap_servers:
        from app.legacy import events as legacy_events
        from app.messaging.kafka_bus import KafkaBus
        bus = KafkaBus(settings)
        # The legacy topic has its own name and carries plain event JSON
        bus.register_lane(legacy_events.LANE_LEGACY, settings.legacy_events_topic,
                          encode=legacy_events.encode, decode=legacy_events.decode)
        return bus
    return InMemoryBus()
