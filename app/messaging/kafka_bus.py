"""
Kafka Bus
The message bus on Apache Kafka (aiokafka). Used when KAFKA_BOOTSTRAP_SERVERS
is set.

Topics (prefix from KAFKA_TOPIC_PREFIX, default "gpon-cms"):
  gpon-cms.commands.provisioning   ONT commands, keyed by network name
  legacy-gpon-events               signup and cutover events (LEGACY_EVENTS_TOPIC),
                                   as plain event JSON, keyed by network name
  gpon-cms.commands.dlq            commands that failed for good, with the reason
  gpon-cms.job-updates             progress updates, keyed by job id

Delivery rules:
  - The producer waits for all in-sync replicas (acks=all) and is idempotent,
    so a 202 from the API means the command is stored.
  - Workers never auto-commit. An offset is committed only after the command
    succeeded, or after its failure was written to the dead-letter topic. A
    worker that dies mid-task leaves the offset uncommitted and the command
    goes to another worker.
  - A command that fails for a passing reason (CMS down) is not committed:
    the consumer seeks back to it, so that partition waits for CMS.
  - Commands with the same key share a partition and run in order, one at a
    time. The number of partitions is therefore the cap on parallel CMS work.

Tested against a stubbed client (tests/test_kafka_bus.py) and over the real
Kafka wire protocol against librdkafka's mock cluster
(tests/test_kafka_protocol.py). Not yet run against an Apache Kafka broker.
"""

import json
import logging
from typing import Any, AsyncIterator, Callable, Dict, Optional, Sequence, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import CommitFailedError, KafkaError

from app.messaging.bus import BusUnavailableError, Delivery
from app.models.job_model import Command, JobUpdate

logger = logging.getLogger(__name__)


class KafkaBus:
    name = "kafka"

    def __init__(self, settings: Any):
        self._servers = settings.kafka_bootstrap_servers
        self._prefix = settings.kafka_topic_prefix
        self._max_poll_interval_ms = int(settings.kafka_max_poll_interval_seconds * 1000)
        self.updates_topic = f"{self._prefix}.job-updates"
        self.dlq_topic = f"{self._prefix}.commands.dlq"
        self._session_timeout_ms = int(getattr(settings, "kafka_session_timeout_seconds", 30) * 1000)
        self._producer: Optional[AIOKafkaProducer] = None
        self._lane_topics: Dict[str, str] = {}
        self._encoders: Dict[str, Callable[[Command], bytes]] = {}
        self._decoders: Dict[str, Callable[[bytes], Command]] = {}  # by topic

    def register_lane(self, lane: str, topic: str, *, encode: Callable[[Command], bytes],
                      decode: Callable[[bytes], Command]) -> None:
        """Give a lane its own topic name and message format"""
        self._lane_topics[lane] = topic
        self._encoders[lane] = encode
        self._decoders[topic] = decode

    def command_topic(self, lane: str) -> str:
        return self._lane_topics.get(lane, f"{self._prefix}.commands.{lane}")

    def decode_command(self, topic: str, raw: bytes) -> Command:
        decode = self._decoders.get(topic)
        return decode(raw) if decode else Command.model_validate_json(raw)

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._servers,
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()
        logger.info(f"Kafka producer connected to {self._servers}")

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def publish_command(self, command: Command) -> None:
        encode = self._encoders.get(command.lane)
        value = encode(command) if encode else command.model_dump_json().encode()
        await self._send(self.command_topic(command.lane), value, key=command.key, wait=True)

    async def publish_event(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        await self._send(topic, json.dumps(value).encode(), key=key, wait=True)

    async def publish_update(self, update: JobUpdate) -> None:
        # Status changes wait for the broker; chatter in between does not
        await self._send(self.updates_topic, update.model_dump_json().encode(),
                         key=update.job_id, wait=update.status is not None)

    async def updates(self) -> AsyncIterator[JobUpdate]:
        # No consumer group: every API process reads every partition, from the
        # start of the topic, so each one rebuilds the same job history.
        consumer = AIOKafkaConsumer(
            self.updates_topic,
            bootstrap_servers=self._servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            async for record in consumer:
                try:
                    yield JobUpdate.model_validate_json(record.value)
                except ValueError:
                    logger.warning(f"Skipping unreadable job update at offset {record.offset}")
        finally:
            await consumer.stop()

    def consume_commands(self, lanes: Sequence[str]) -> "KafkaCommandConsumer":
        return KafkaCommandConsumer(self, lanes)

    async def _send(self, topic: str, value: bytes, *, key: str, wait: bool, headers: Any = None) -> None:
        if self._producer is None:
            raise BusUnavailableError("The Kafka producer is not running.")
        try:
            if wait:
                await self._producer.send_and_wait(topic, value=value, key=key.encode(), headers=headers)
            else:
                await self._producer.send(topic, value=value, key=key.encode(), headers=headers)
        except KafkaError as e:
            raise BusUnavailableError(f"Kafka did not accept the message for {topic}: {e}") from e


class KafkaCommandConsumer:
    def __init__(self, bus: KafkaBus, lanes: Sequence[str]):
        self._bus = bus
        self._lanes = sorted(lanes)
        self._attempts: Dict[Tuple[str, int, int], int] = {}
        self._consumer: Optional[AIOKafkaConsumer] = None

    @property
    def group_id(self) -> str:
        """Workers serving the same lanes share a group, so Kafka splits partitions between them"""
        return f"{self._bus._prefix}.workers.{'-'.join(self._lanes)}"

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *(self._bus.command_topic(lane) for lane in self._lanes),
            bootstrap_servers=self._bus._servers,
            group_id=self.group_id,
            enable_auto_commit=False,  # offsets are committed by ack() only
            auto_offset_reset="earliest",
            max_poll_interval_ms=self._bus._max_poll_interval_ms,
            session_timeout_ms=self._bus._session_timeout_ms,
        )
        await self._consumer.start()
        logger.info(f"Kafka consumer started in group {self.group_id}")

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()  # leaves the group; uncommitted commands go to other workers
            self._consumer = None

    async def next(self, timeout: float = 1.0) -> Optional[Delivery]:
        batches = await self._consumer.getmany(timeout_ms=int(timeout * 1000), max_records=1)
        for records in batches.values():
            for record in records:
                handle = (record.topic, record.partition, record.offset)
                try:
                    command = self._bus.decode_command(record.topic, record.value)
                except ValueError as e:
                    # Unreadable forever: park it and move on, or it blocks the partition
                    logger.error(f"Unreadable command at {handle}: {e}")
                    await self._park(record.value, record.key, record.topic,
                                     {"code": "invalid_command", "message": str(e)[:500]})
                    await self._commit(handle)
                    return None
                self._attempts[handle] = self._attempts.get(handle, 0) + 1
                return Delivery(command, handle=handle, attempt=self._attempts[handle])
        return None

    async def ack(self, delivery: Delivery) -> None:
        self._attempts.pop(delivery.handle, None)
        await self._commit(delivery.handle)

    async def retry(self, delivery: Delivery) -> None:
        topic, partition, offset = delivery.handle
        self._consumer.seek(TopicPartition(topic, partition), offset)

    async def dead_letter(self, delivery: Delivery, error: Dict[str, Any]) -> None:
        topic, _, _ = delivery.handle
        await self._park(delivery.command.model_dump_json().encode(),
                         delivery.command.key.encode(), topic, error)

    async def _commit(self, handle: Tuple[str, int, int]) -> None:
        topic, partition, offset = handle
        try:
            await self._consumer.commit({TopicPartition(topic, partition): offset + 1})
        except CommitFailedError:
            # The partition moved to another worker mid-task. That worker will get
            # this command again, which is why handlers are safe to repeat.
            logger.warning(f"Commit for {handle} was rejected after a rebalance; "
                           f"the command will be redelivered")

    async def _park(self, value: bytes, key: Optional[bytes], source_topic: str, error: Dict[str, Any]) -> None:
        headers = [("error-code", str(error.get("code", "")).encode()),
                   ("error-message", str(error.get("message", "")).encode()),
                   ("source-topic", source_topic.encode())]
        await self._bus._send(self._bus.dlq_topic, value, key=(key or b"").decode(),
                              wait=True, headers=headers)
