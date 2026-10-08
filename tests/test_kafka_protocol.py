"""
The Kafka bus and worker over the real Kafka wire protocol, against
librdkafka's mock cluster (see tests/support/mock_kafka.py). Unlike
test_kafka_bus.py nothing in the Kafka client is stubbed here: consumer
groups, offset commits and redelivery are the broker's.

Not covered: two workers sharing one consumer group at the same time. The
mock cluster rejects aiokafka's group sync as soon as a second member joins
(SyncGroup returns InvalidRequestError), so splitting partitions between
workers can only be tested against a real broker.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("aiokafka")
pytest.importorskip("confluent_kafka")

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition  # noqa: E402

from app.controller.job_controller import JobStore  # noqa: E402
from app.legacy import events as legacy_events  # noqa: E402
from app.legacy.handlers import build_legacy_handlers  # noqa: E402
from app.legacy.models import XgsMigrationRequest  # noqa: E402
from app.messaging.bus import BusUnavailableError  # noqa: E402
from app.messaging.kafka_bus import KafkaBus  # noqa: E402
from app.models.job_model import OWNERSHIP, Command, JobStatus, JobUpdate  # noqa: E402
from app.nbi.errors import NbiRejectedError, NbiTimeoutError  # noqa: E402
from app.nbi.gateway import NbiGateway  # noqa: E402
from app.nbi.mock_cms import MOCK_NETWORK, MockCmsBackend  # noqa: E402
from app.worker.runner import Worker  # noqa: E402
from config import settings as app_settings  # noqa: E402
from tests.support.mock_kafka import MockKafkaCluster  # noqa: E402


@pytest.fixture(scope="module")
def cluster():
    return MockKafkaCluster()


@pytest.fixture
def kafka(cluster):
    """Settings for one test, with its own topics and consumer groups"""
    prefix = f"t{uuid.uuid4().hex[:8]}"
    return SimpleNamespace(
        cluster=cluster,
        kafka_bootstrap_servers=cluster.bootstrap_servers,
        kafka_topic_prefix=prefix,
        kafka_max_poll_interval_seconds=300,
        kafka_session_timeout_seconds=10,
        legacy_events_topic=f"{prefix}.legacy-gpon-events",
        xgs_migration_topic=f"{prefix}.xgs-migration-requests",
        legacy_profile_map_path=app_settings.legacy_profile_map_path,
        known_bandwidth_profiles=lambda: None,
        activation_verify_timeout_seconds=1, activation_poll_seconds=0.01)


def make_command(n, key="NTWK-1"):
    return Command(action="DO", lane="provisioning", key=key, kind="activation",
                   summary=f"Command {n}", payload={"n": n})


async def until(condition, timeout=30.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {what}")


async def committed_total(kafka, topic, group):
    """Sum of the offsets a consumer group has committed for a topic"""
    consumer = AIOKafkaConsumer(bootstrap_servers=kafka.kafka_bootstrap_servers, group_id=group,
                                enable_auto_commit=False)
    await consumer.start()
    try:
        await consumer._client.force_metadata_update()
        partitions = consumer.partitions_for_topic(topic) or set()
        offsets = [await consumer.committed(TopicPartition(topic, p)) for p in partitions]
        return sum(o or 0 for o in offsets)
    finally:
        await consumer.stop()


async def read_topic(kafka, topic, count, timeout=20.0):
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=kafka.kafka_bootstrap_servers,
                                group_id=None, auto_offset_reset="earliest")
    await consumer.start()
    records = []
    try:
        deadline = time.monotonic() + timeout
        while len(records) < count and time.monotonic() < deadline:
            for batch in (await consumer.getmany(timeout_ms=200)).values():
                records.extend(batch)
        return records
    finally:
        await consumer.stop()


class Harness:
    """A bus, a job store following the updates topic, and helpers to run workers"""

    def __init__(self, kafka):
        self.kafka = kafka
        self.bus = KafkaBus(kafka)
        self.jobs = JobStore(self.bus)
        self.tasks = []
        self.topic = self.bus.command_topic("provisioning")
        self.group = f"{kafka.kafka_topic_prefix}.workers.provisioning"

    async def __aenter__(self):
        await self.bus.start()
        await self.jobs.start()
        return self

    async def __aexit__(self, *exc):
        for worker, task in self.tasks:
            worker.stop()
        await asyncio.gather(*(task for _, task in self.tasks), return_exceptions=True)
        await self.jobs.stop()
        await self.bus.stop()

    def start_worker(self, handlers, lanes=("provisioning",)):
        worker = Worker(self.bus, list(lanes), handlers, retry_backoff=0.05, max_retry_backoff=0.05)
        task = asyncio.create_task(worker.run())
        self.tasks.append((worker, task))
        return worker, task

    def status(self, command):
        try:
            return self.jobs.get(command.id).status
        except KeyError:
            return None

    async def finished(self, *commands, timeout=30.0):
        await until(lambda: all(self.status(c) in (JobStatus.SUCCEEDED, JobStatus.FAILED) for c in commands),
                    timeout, "jobs to finish")


def run(coroutine, timeout=90):
    return asyncio.run(asyncio.wait_for(coroutine, timeout))


def test_offsets_are_committed_after_success_and_updates_flow_back(kafka):
    async def scenario():
        async with Harness(kafka) as h:
            async def handler(context, payload):
                await context.emit("log", f"working on {payload['n']}")
                return {"n": payload["n"]}

            commands = [make_command(n) for n in range(3)]
            for command in commands:
                await h.jobs.submit(command)
            assert await committed_total(kafka, h.topic, h.group) == 0  # nothing consumed yet

            h.start_worker({"DO": handler})
            await h.finished(*commands)

            assert [h.jobs.get(c.id).result for c in commands] == [{"n": 0}, {"n": 1}, {"n": 2}]
            await until(lambda: committed_total(kafka, h.topic, h.group), what="offsets to be committed")
            assert await committed_total(kafka, h.topic, h.group) == 3

            events = [e async for e in h.jobs.events(commands[0].id)]
            assert [e.type for e in events] == ["status", "status", "log", "result"]

    run(scenario())


def test_a_failing_command_is_not_committed_until_it_succeeds(kafka):
    async def scenario():
        async with Harness(kafka) as h:
            attempts, release = [], asyncio.Event()

            async def handler(context, payload):
                attempts.append(context.attempt)
                if context.attempt == 1:
                    raise NbiTimeoutError("CMS did not answer.")
                await release.wait()  # hold the second attempt open
                return {}

            command, behind = make_command(1), make_command(2)
            await h.jobs.submit(command)
            await h.jobs.submit(behind)
            h.start_worker({"DO": handler})

            await until(lambda: len(attempts) == 2, what="the retry to start")
            assert await committed_total(kafka, h.topic, h.group) == 0  # failed once, still uncommitted
            assert h.status(behind) == JobStatus.QUEUED  # the command behind it waits its turn

            release.set()
            await h.finished(command, behind)
            assert h.jobs.get(command.id).status == JobStatus.SUCCEEDED
            await until(lambda: committed_total(kafka, h.topic, h.group), what="offsets to be committed")
            assert await committed_total(kafka, h.topic, h.group) == 2

    run(scenario())


def test_a_command_in_hand_when_a_worker_stops_goes_to_another_worker(kafka):
    async def scenario():
        async with Harness(kafka) as h:
            first_started = asyncio.Event()
            handled_by = []

            async def stuck(context, payload):
                handled_by.append("first")
                first_started.set()
                await asyncio.sleep(3600)

            async def healthy(context, payload):
                handled_by.append("second")
                return {"by": "second"}

            command = make_command(1)
            await h.jobs.submit(command)

            _, first_task = h.start_worker({"DO": stuck})
            await first_started.wait()
            first_task.cancel()  # the pod goes away mid-command
            await asyncio.gather(first_task, return_exceptions=True)
            assert await committed_total(kafka, h.topic, h.group) == 0  # it never committed

            h.start_worker({"DO": healthy})
            await h.finished(command)

            assert handled_by == ["first", "second"]
            assert h.jobs.get(command.id).result == {"by": "second"}
            await until(lambda: committed_total(kafka, h.topic, h.group), what="offsets to be committed")

    run(scenario())


def test_a_rejected_command_lands_on_the_dead_letter_topic(kafka):
    async def scenario():
        async with Harness(kafka) as h:
            async def handler(context, payload):
                if payload["n"] == 1:
                    raise NbiRejectedError("VLAN 999 does not exist.")
                return {}

            bad, good = make_command(1), make_command(2)
            await h.jobs.submit(bad)
            await h.jobs.submit(good)
            h.start_worker({"DO": handler})
            await h.finished(bad, good)

            assert h.jobs.get(bad.id).error["code"] == "cms_rejected"
            assert h.jobs.get(good.id).status == JobStatus.SUCCEEDED  # not blocked by the bad one

            parked = await read_topic(kafka, h.bus.dlq_topic, 1)
            assert len(parked) == 1
            assert Command.model_validate_json(parked[0].value).id == bad.id
            assert dict(parked[0].headers)["error-code"] == b"cms_rejected"

    run(scenario())


def test_a_restarted_api_rebuilds_its_jobs_from_the_updates_topic(kafka):
    async def scenario():
        async with Harness(kafka) as h:
            async def handler(context, payload):
                return {"n": payload["n"]}

            commands = [make_command(n) for n in range(2)]
            for command in commands:
                await h.jobs.submit(command)
            h.start_worker({"DO": handler})
            await h.finished(*commands)

            # A second API process that was not running while the jobs ran
            other_bus = KafkaBus(kafka)
            await other_bus.start()
            other = JobStore(other_bus)
            await other.start()
            try:
                await until(lambda: len(other.list()) == 2 and all(
                    j.status == JobStatus.SUCCEEDED for j in other.list()), what="the replay")
                assert other.get(commands[1].id).result == {"n": 1}
                assert [e.type for e in [e async for e in other.events(commands[0].id)]] == [
                    "status", "status", "result"]
            finally:
                await other.stop()
                await other_bus.stop()

    run(scenario())


def test_legacy_topic_takes_plain_events_and_a_cutover_reaches_the_xgs_topic(kafka):
    async def scenario():
        async with Harness(kafka) as h:
            h.bus.register_lane(legacy_events.LANE_LEGACY, kafka.legacy_events_topic,
                                encode=legacy_events.encode, decode=legacy_events.decode)
            gateway = NbiGateway(MockCmsBackend(latency=0, ranging_seconds=0), timeout=5)
            h.start_worker(build_legacy_handlers(kafka, gateway, h.bus), lanes=["legacy"])

            # Written the way an outside producer would: raw JSON, no Command envelope
            producer = AIOKafkaProducer(bootstrap_servers=kafka.kafka_bootstrap_servers)
            await producer.start()
            try:
                await producer.send_and_wait(kafka.legacy_events_topic, b"this is not an event", key=b"x")
                await producer.send_and_wait(kafka.legacy_events_topic, key=MOCK_NETWORK.encode(), value=json.dumps({
                    "type": "GPON_SIGNUP", "event_id": "sonar-evt-0001", "network": MOCK_NETWORK,
                    "subscriber_id": "SUB-7001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
                    "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}).encode())
                await producer.send_and_wait(kafka.legacy_events_topic, key=MOCK_NETWORK.encode(), value=json.dumps({
                    "type": "XGS_CUTOVER", "event_id": "sonar-evt-0002", "network": MOCK_NETWORK,
                    "subscriber_id": "SUB-7001", "xgs_serial_number": "CXNK10AA0001"}).encode())
            finally:
                await producer.stop()

            await until(lambda: all(
                h.jobs._records.get(job_id) and h.jobs.get(job_id).status == JobStatus.SUCCEEDED
                for job_id in ("sonar-evt-0001", "sonar-evt-0002")), what="both legacy jobs")
            assert h.jobs.get("sonar-evt-0001").kind == "signup"

            # The subscriber signed up in the first event is handed over by the second
            migrations = await read_topic(kafka, kafka.xgs_migration_topic, 1)
            assert len(migrations) == 1 and migrations[0].key == b"SUB-7001"
            request = XgsMigrationRequest.model_validate_json(migrations[0].value)
            assert request.migration_id == "sonar-evt-0002"
            assert request.source.serial_number == "CXNK00C4D301"
            assert request.xgs_serial_number == "CXNK10AA0001"
            assert [(s.service_type, s.service_profile, s.vlan) for s in request.services] == [
                ("data", "XGS-RESI-1G", 100)]

            # The unreadable message was parked, not left to block the topic
            parked = await read_topic(kafka, h.bus.dlq_topic, 1)
            assert parked[0].value == b"this is not an event"
            assert dict(parked[0].headers)["error-code"] == b"invalid_command"

    run(scenario())


@pytest.fixture
def offsets_for_times(monkeypatch, kafka):
    """
    librdkafka's mock cluster answers a ListOffsets by timestamp with nothing (aiokafka returns None),
    where a real broker gives the first offset at or after that time. Stand in for that one broker
    call by scanning the partition's timestamps; the rest of the read stays on the wire.
    """
    from aiokafka.structs import OffsetAndTimestamp

    async def by_scanning(self, timestamps):
        answer = {}
        for tp, wanted in timestamps.items():
            scan = AIOKafkaConsumer(bootstrap_servers=kafka.kafka_bootstrap_servers, group_id=None)
            await scan.start()
            try:
                scan.assign([tp])
                end = (await scan.end_offsets([tp]))[tp]
                scan.seek(tp, 0)  # the mock answers "earliest" with the end too
                answer[tp] = None
                while answer[tp] is None and await scan.position(tp) < end:
                    for record in (await scan.getmany(tp, timeout_ms=500)).get(tp, []):
                        if record.timestamp >= wanted:
                            answer[tp] = OffsetAndTimestamp(record.offset, record.timestamp)
                            break
            finally:
                await scan.stop()
        return answer

    monkeypatch.setattr(AIOKafkaConsumer, "offsets_for_times", by_scanning)


def test_a_jobs_history_is_read_from_its_own_partition_and_no_further(kafka, offsets_for_times):
    # Updates are keyed by job id across 4 partitions. One job's history is its updates from the
    # time given, in order, and none of another job's
    async def scenario():
        bus = KafkaBus(kafka)
        await bus.start()
        try:
            def update(job_id, n, type="log"):
                return JobUpdate(job_id=job_id, kind="activation", summary="s", type=type, message=f"{job_id} {n}")

            before = datetime.now(timezone.utc)
            for n in range(5):
                for job in ("job-a", "job-b", "job-c", "job-d"):
                    await bus.publish_update(update(job, n, type=OWNERSHIP if n == 2 else "status"))
            history = await bus.job_history("job-c", before - timedelta(minutes=1), timeout=20)
            assert [u.message for u in history] == [f"job-c {n}" for n in range(5)]
            assert [u.type for u in history][2] == OWNERSHIP
            assert await bus.job_history("job-c", datetime.now(timezone.utc) + timedelta(minutes=5), timeout=20) == []
            assert await bus.job_history("job-never", before, timeout=20) == []
        finally:
            await bus.stop()

    run(scenario())


def test_a_history_kafka_cannot_give_is_a_bus_failure_within_the_timeout(kafka):
    # The attempt then fails as retryable: nothing has been changed in CMS at that point
    async def scenario():
        bus = KafkaBus(kafka)
        await bus.start()
        try:
            await bus.publish_update(JobUpdate(job_id="job-a", kind="k", summary="s", type=OWNERSHIP, message="m"))
            bus._servers = "127.0.0.1:1"  # the reader cannot reach a broker
            started = time.monotonic()
            with pytest.raises(BusUnavailableError, match="history of job job-a"):
                await bus.job_history("job-a", datetime.now(timezone.utc), timeout=3)
            assert time.monotonic() - started < 10
        finally:
            await bus.stop()

    run(scenario())
