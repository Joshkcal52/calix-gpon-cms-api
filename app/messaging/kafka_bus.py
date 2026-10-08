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
  - Each lane has its own consumer group (gpon-cms.workers.<lane>), whatever
    other lanes a worker serves. A group with no saved position starts at the
    beginning of the topic; the worker's age limit keeps what it finds there
    from running again (see Worker.max_command_age).

Tested against a stubbed client (tests/test_kafka_bus.py) and over the real
Kafka wire protocol against librdkafka's mock cluster
(tests/test_kafka_protocol.py). Not yet run against an Apache Kafka broker.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Sequence, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import CommitFailedError, KafkaError
from aiokafka.partitioner import DefaultPartitioner

from app.messaging.bus import BusUnavailableError, Delivery
from app.models.job_model import OWNERSHIP, Command, JobUpdate

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
        # Status changes and ownership records wait for the broker; chatter in between does not
        await self._send(self.updates_topic, update.model_dump_json().encode(),
                         key=update.job_id, wait=update.status is not None or update.type == OWNERSHIP)

    async def job_history(self, job_id: str, since: datetime, timeout: float) -> List[JobUpdate]:
        """
        Updates are keyed by job id, so one job's are all in one partition. Read that partition
        from the first offset at or after `since` to its end as it is when the read starts.
        """
        try:
            return await asyncio.wait_for(self._read_job_history(job_id, since), timeout=timeout)
        except (KafkaError, OSError, asyncio.TimeoutError) as e:
            raise BusUnavailableError(f"Could not read the history of job {job_id} from {self.updates_topic} "
                                      f"within {timeout:g} seconds: {e or type(e).__name__}") from e

    async def _read_job_history(self, job_id: str, since: datetime) -> List[JobUpdate]:
        if self._producer is None:
            raise BusUnavailableError("The Kafka producer is not running.")
        key = job_id.encode()
        # The partition the producer chose for this key: its partitioner over the topic's partitions
        ordered = sorted(await self._producer.partitions_for(self.updates_topic))
        tp = TopicPartition(self.updates_topic, DefaultPartitioner()(key, ordered, ordered))
        consumer = AIOKafkaConsumer(bootstrap_servers=self._servers, group_id=None, enable_auto_commit=False)
        await consumer.start()
        try:
            consumer.assign([tp])
            end = (await consumer.end_offsets([tp]))[tp]
            found = (await consumer.offsets_for_times({tp: int(since.timestamp() * 1000)}))[tp]
            if found is None or found.offset >= end:
                return []
            consumer.seek(tp, found.offset)
            history: List[JobUpdate] = []
            while await consumer.position(tp) < end:
                for record in (await consumer.getmany(tp, timeout_ms=1000)).get(tp, []):
                    if record.offset >= end:
                        break
                    if record.key == key:
                        try:
                            history.append(JobUpdate.model_validate_json(record.value))
                        except ValueError:
                            logger.warning(f"Skipping unreadable job update at offset {record.offset}")
            return history
        finally:
            await consumer.stop()

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
    """
    One Kafka consumer per lane, each in that lane's own group. A group named after a worker's
    whole lane list would put workers with different lane lists in different groups, each taking
    every command, and a new lane list would start a new group that re-reads the whole topic.
    """

    def __init__(self, bus: KafkaBus, lanes: Sequence[str]):
        self._bus = bus
        self._lanes = sorted(lanes)
        self._attempts: Dict[Tuple[str, int, int], int] = {}
        self._consumers: Dict[str, AIOKafkaConsumer] = {}  # by topic
        self._turn = 0

    def group_id(self, lane: str) -> str:
        """Every worker serving a lane shares its group, so Kafka splits that lane's partitions between them"""
        return f"{self._bus._prefix}.workers.{lane}"

    async def start(self) -> None:
        for lane in self._lanes:
            topic = self._bus.command_topic(lane)
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self._bus._servers,
                group_id=self.group_id(lane),
                enable_auto_commit=False,  # offsets are committed by ack() only
                auto_offset_reset="earliest",
                max_poll_interval_ms=self._bus._max_poll_interval_ms,
                session_timeout_ms=self._bus._session_timeout_ms,
            )
            await consumer.start()
            self._consumers[topic] = consumer
            logger.info(f"Kafka consumer started on {topic} in group {self.group_id(lane)}")

    async def stop(self) -> None:
        consumers, self._consumers = list(self._consumers.values()), {}
        for consumer in consumers:
            await consumer.stop()  # leaves the group; uncommitted commands go to other workers

    async def next(self, timeout: float = 1.0) -> Optional[Delivery]:
        # Each lane in turn, so a busy lane cannot starve the others
        consumers = list(self._consumers.values())
        if not consumers:
            return None
        self._turn = (self._turn + 1) % len(consumers)
        for consumer in consumers[self._turn:] + consumers[:self._turn]:
            batches = await consumer.getmany(timeout_ms=int(timeout * 1000 / len(consumers)), max_records=1)
            for records in batches.values():
                for record in records:
                    return await self._deliver(record)
        return None

    async def _deliver(self, record: Any) -> Optional[Delivery]:
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
        return Delivery(command, handle=handle, attempt=self._attempts[handle], queued_at=_record_time(record))

    async def ack(self, delivery: Delivery) -> None:
        self._attempts.pop(delivery.handle, None)
        await self._commit(delivery.handle)

    async def retry(self, delivery: Delivery) -> None:
        topic, partition, offset = delivery.handle
        self._consumers[topic].seek(TopicPartition(topic, partition), offset)

    async def dead_letter(self, delivery: Delivery, error: Dict[str, Any]) -> None:
        topic, _, _ = delivery.handle
        await self._park(delivery.command.model_dump_json().encode(),
                         delivery.command.key.encode(), topic, error)

    async def _commit(self, handle: Tuple[str, int, int]) -> None:
        topic, partition, offset = handle
        try:
            await self._consumers[topic].commit({TopicPartition(topic, partition): offset + 1})
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


def _record_time(record: Any) -> Optional[datetime]:
    """When Kafka stored the message (or its producer said it sent it), if the record says"""
    timestamp = getattr(record, "timestamp", None)
    if not isinstance(timestamp, (int, float)) or timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
