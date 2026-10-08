"""
Tests for the Kafka bus against a stubbed aiokafka. They pin down how we use
the Kafka client (no auto-commit, commit offset+1 only after the outcome is
settled, seek back to retry, dead-letter topic). They do not prove behaviour
against a real broker.
"""

import asyncio
import time
from types import SimpleNamespace

import pytest

pytest.importorskip("aiokafka")

from aiokafka import TopicPartition  # noqa: E402
from aiokafka.errors import KafkaTimeoutError  # noqa: E402

from app.messaging import kafka_bus  # noqa: E402
from app.messaging.bus import BusUnavailableError  # noqa: E402
from app.models.job_model import Command, JobStatus, JobUpdate  # noqa: E402
from app.nbi.errors import NbiRejectedError, NbiTimeoutError  # noqa: E402
from app.worker.runner import Worker  # noqa: E402

SETTINGS = SimpleNamespace(kafka_bootstrap_servers="kafka:9092", kafka_topic_prefix="gpon-cms",
                           kafka_max_poll_interval_seconds=900)
TOPIC = "gpon-cms.commands.provisioning"


class FakeProducer:
    def __init__(self, **options):
        self.options = options
        self.sent = []  # (how, topic, value, key, headers)
        self.fail = False

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, topic, value=None, key=None, headers=None):
        if self.fail:
            raise KafkaTimeoutError("no brokers")
        self.sent.append(("send_and_wait", topic, value, key, headers))

    async def send(self, topic, value=None, key=None, headers=None):
        self.sent.append(("send", topic, value, key, headers))


class FakeConsumer:
    """One partition whose offsets equal list positions"""
    records = []

    def __init__(self, *topics, **options):
        self.topics = topics
        self.options = options
        self.position = 0
        self.actions = []  # ("commit", offset) / ("seek", offset)
        self.polled = []  # monotonic time of each getmany
        FakeConsumer.last = self
        FakeConsumer.instances.append(self)

    async def start(self):
        pass

    async def stop(self):
        self.actions.append(("stop", None))

    async def getmany(self, timeout_ms=0, max_records=None):
        assert max_records == 1
        self.polled.append(time.monotonic())
        records = [r for r in self.records if r.topic in self.topics]
        if self.position >= len(records):
            await asyncio.sleep(0.001)
            return {}
        record = records[self.position]
        self.position += 1
        return {TopicPartition(record.topic, record.partition): [record]}

    def seek(self, partition, offset):
        self.actions.append(("seek", offset))
        self.position = offset

    async def commit(self, offsets):
        (partition, offset), = offsets.items()
        assert partition == TopicPartition(TOPIC, 0)
        self.actions.append(("commit", offset))


def record(offset, command=None, value=None, timestamp=None):
    value = value if value is not None else command.model_dump_json().encode()
    return SimpleNamespace(topic=TOPIC, partition=0, offset=offset, key=b"NTWK-1", value=value,
                           timestamp=timestamp if timestamp is not None else int(time.time() * 1000))


def make_command(n):
    return Command(action="DO", lane="provisioning", key="NTWK-1", kind="activation",
                   summary=f"Command {n}", payload={"n": n})


@pytest.fixture
def bus(monkeypatch):
    monkeypatch.setattr(kafka_bus, "AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr(kafka_bus, "AIOKafkaConsumer", FakeConsumer)
    FakeConsumer.records = []
    FakeConsumer.last = None
    FakeConsumer.instances = []
    bus = kafka_bus.KafkaBus(SETTINGS)
    asyncio.run(bus.start())
    return bus


def run_worker(bus, handler, expected_commits):
    async def scenario():
        worker = Worker(bus, ["provisioning"], {"DO": handler}, retry_backoff=0.01, max_retry_backoff=0.01)
        task = asyncio.create_task(worker.run())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            consumer = getattr(FakeConsumer, "last", None)
            if consumer and sum(1 for a, _ in consumer.actions if a == "commit") >= expected_commits:
                break
            await asyncio.sleep(0.005)
        worker.stop()
        await task

    asyncio.run(scenario())
    return FakeConsumer.last


def test_producer_waits_for_all_replicas_and_commands_are_keyed_by_network(bus):
    assert bus._producer.options["acks"] == "all"
    assert bus._producer.options["enable_idempotence"] is True

    command = make_command(1)
    asyncio.run(bus.publish_command(command))

    how, topic, value, key, _ = bus._producer.sent[0]
    assert (how, topic, key) == ("send_and_wait", TOPIC, b"NTWK-1")
    assert Command.model_validate_json(value) == command


def test_publish_failure_surfaces_as_bus_unavailable(bus):
    bus._producer.fail = True
    with pytest.raises(BusUnavailableError) as error:
        asyncio.run(bus.publish_command(make_command(1)))
    assert error.value.http_status == 503


def test_status_updates_wait_for_the_broker_and_chatter_does_not(bus):
    base = dict(job_id="j1", kind="activation", summary="s", message="m")
    asyncio.run(bus.publish_update(JobUpdate(type="log", **base)))
    asyncio.run(bus.publish_update(JobUpdate(type="result", status=JobStatus.SUCCEEDED, **base)))

    assert [(how, topic, key) for how, topic, _, key, _ in bus._producer.sent] == [
        ("send", "gpon-cms.job-updates", b"j1"), ("send_and_wait", "gpon-cms.job-updates", b"j1")]


def test_consumer_never_auto_commits_and_workers_share_a_group(bus):
    FakeConsumer.records = [record(0, make_command(1))]

    async def handler(context, payload):
        return {}

    consumer = run_worker(bus, handler, expected_commits=1)

    assert consumer.topics == (TOPIC,)
    assert consumer.options["enable_auto_commit"] is False
    assert consumer.options["group_id"] == "gpon-cms.workers.provisioning"
    assert consumer.options["auto_offset_reset"] == "earliest"
    assert consumer.options["max_poll_interval_ms"] == 900000


def test_each_lane_has_its_own_group_whatever_else_the_worker_serves(bus):
    # A group per lane list would put a provisioning worker and a provisioning+legacy worker in
    # different groups, both taking every command, and a new lane list would re-read the topic
    bus.register_lane("legacy", "legacy-gpon-events", encode=lambda c: b"", decode=lambda raw: None)

    async def scenario():
        for lanes in (["provisioning"], ["provisioning", "legacy"]):
            consumer = bus.consume_commands(lanes)
            await consumer.start()
            await consumer.stop()

    asyncio.run(scenario())
    assert [(c.topics, c.options["group_id"]) for c in FakeConsumer.instances] == [
        ((TOPIC,), "gpon-cms.workers.provisioning"),
        (("legacy-gpon-events",), "gpon-cms.workers.legacy"),
        ((TOPIC,), "gpon-cms.workers.provisioning")]


def test_a_command_kafka_has_held_too_long_is_not_run_whatever_its_own_time_says(bus):
    # A new group re-reads the topic from the start. The command's created_at says now (a producer
    # that left its time out, or a wrong clock), but Kafka stored it two hours ago.
    two_hours_ago = int((time.time() - 7200) * 1000)
    FakeConsumer.records = [record(0, make_command(1), timestamp=two_hours_ago)]
    handled = []

    async def handler(context, payload):
        handled.append(payload["n"])
        return {}

    consumer = run_worker(bus, handler, expected_commits=1)

    assert handled == []
    parked = [s for s in bus._producer.sent if s[1] == "gpon-cms.commands.dlq"]
    assert len(parked) == 1 and dict(parked[0][4])["error-code"] == b"command_expired"
    assert [a for a in consumer.actions if a[0] != "stop"] == [("commit", 1)]


def test_while_one_lane_runs_a_long_command_neither_lane_is_polled(bus):
    # A worker is one loop: while a handler runs, no consumer is polled, the busy lane's own
    # included. The idle lane is no worse off than the busy one, as with one consumer for both
    # lanes before. Both rely on KAFKA_MAX_POLL_INTERVAL_SECONDS outlasting the slowest command
    # (Settings.check_worker_timeouts).
    bus.register_lane("legacy", "legacy-gpon-events", encode=lambda c: b"", decode=lambda raw: None)
    FakeConsumer.records = [record(0, make_command(1))]

    ran = []

    async def slow_handler(context, payload):
        start = time.monotonic()
        await asyncio.sleep(0.3)
        ran.append((start, time.monotonic()))
        return {}

    async def scenario():
        worker = Worker(bus, ["provisioning", "legacy"], {"DO": slow_handler})
        task = asyncio.create_task(worker.run())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not any(
                a == "commit" for c in FakeConsumer.instances for a, _ in c.actions):
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.05)
        worker.stop()
        await task

    asyncio.run(scenario())
    busy, idle = FakeConsumer.instances  # provisioning, legacy (lanes are sorted: legacy first)
    busy, idle = (busy, idle) if busy.topics == (TOPIC,) else (idle, busy)

    (start, end), = ran
    assert busy.polled and idle.polled
    assert not [p for p in busy.polled + idle.polled if start < p < end]


def test_offset_is_committed_only_after_success(bus):
    FakeConsumer.records = [record(0, make_command(1)), record(1, make_command(2))]
    order = []

    async def handler(context, payload):
        order.append(("handled", payload["n"]))
        return {}

    consumer = run_worker(bus, handler, expected_commits=2)

    # commit N+1 means "offset N is done"
    assert [a for a in consumer.actions if a[0] != "stop"] == [("commit", 1), ("commit", 2)]
    assert order == [("handled", 1), ("handled", 2)]
    result_updates = [s for s in bus._producer.sent if s[0] == "send_and_wait" and s[1] == "gpon-cms.job-updates"]
    assert len(result_updates) == 4  # running + succeeded, per command


def test_transient_failure_seeks_back_instead_of_committing(bus):
    FakeConsumer.records = [record(0, make_command(1)), record(1, make_command(2))]
    seen = []

    async def handler(context, payload):
        seen.append((payload["n"], context.attempt))
        if payload["n"] == 1 and context.attempt < 3:
            raise NbiTimeoutError("CMS did not answer.")
        return {}

    consumer = run_worker(bus, handler, expected_commits=2)

    assert [a for a in consumer.actions if a[0] != "stop"] == [
        ("seek", 0), ("seek", 0), ("commit", 1), ("commit", 2)]
    assert seen == [(1, 1), (1, 2), (1, 3), (2, 1)]  # command 2 waits its turn


def test_permanent_failure_goes_to_the_dead_letter_topic_before_the_commit(bus):
    FakeConsumer.records = [record(0, make_command(1))]

    async def handler(context, payload):
        raise NbiRejectedError("VLAN 999 does not exist.")

    consumer = run_worker(bus, handler, expected_commits=1)

    parked = [s for s in bus._producer.sent if s[1] == "gpon-cms.commands.dlq"]
    assert len(parked) == 1
    how, _, value, key, headers = parked[0]
    assert how == "send_and_wait" and key == b"NTWK-1"
    assert Command.model_validate_json(value).summary == "Command 1"
    assert dict(headers) == {"error-code": b"cms_rejected", "error-message": b"VLAN 999 does not exist.",
                             "source-topic": TOPIC.encode()}
    assert [a for a in consumer.actions if a[0] != "stop"] == [("commit", 1)]


def test_unreadable_message_is_parked_so_it_cannot_block_the_partition(bus):
    FakeConsumer.records = [record(0, value=b"not json"), record(1, make_command(2))]
    handled = []

    async def handler(context, payload):
        handled.append(payload["n"])
        return {}

    consumer = run_worker(bus, handler, expected_commits=2)

    assert handled == [2]
    assert [a for a in consumer.actions if a[0] != "stop"] == [("commit", 1), ("commit", 2)]
    parked = [s for s in bus._producer.sent if s[1] == "gpon-cms.commands.dlq"]
    assert parked[0][2] == b"not json" and dict(parked[0][4])["error-code"] == b"invalid_command"


def test_worker_leaves_the_group_on_shutdown(bus):
    async def handler(context, payload):
        return {}

    consumer = run_worker(bus, handler, expected_commits=0)
    assert consumer.actions == [("stop", None)]
