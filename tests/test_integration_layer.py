"""
Tests for the v2 integration layer: the CMS gateway and circuit breaker, the
queue and worker acknowledgement rules, repeat-safe activation, the HTTP API
with live events. Everything runs in mock mode with the
in-memory bus; no CMS, Kafka or network is needed.
"""

import asyncio
import json
import time
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from app.controller.provisioning_controller import activate_ont
from app.messaging.bus import BusUnavailableError, InMemoryBus
from app.models.job_model import Command, JobStatus
from app.models.provisioning_model import ActivationRequest, EthernetService, OntSpec
from app.nbi.breaker import CLOSED, HALF_OPEN, OPEN, CircuitBreaker
from app.nbi.errors import (
    NbiCircuitOpenError, NbiConflictError, NbiNotConfiguredError, NbiRejectedError, NbiTimeoutError
)
from app.nbi.gateway import NbiGateway, build_gateway
from app.nbi.mock_cms import MOCK_NETWORK, MockCmsBackend
from app.worker.runner import Worker
from config import settings
from main import app

NEW_SERIAL = "CXNK00C4D301"  # discovered, not provisioned, in the mock CMS


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def make_gateway(**overrides):
    backend = MockCmsBackend(latency=0, ranging_seconds=0)
    options = dict(timeout=0.05, max_retries=2, retry_backoff=0,
                   breaker=CircuitBreaker(failure_threshold=3, recovery_seconds=30))
    options.update(overrides)
    return NbiGateway(backend, **options), backend


# --- Circuit breaker ---------------------------------------------------------

def test_breaker_opens_after_consecutive_failures_and_says_when_to_retry():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, recovery_seconds=30, clock=clock)

    for _ in range(2):
        breaker.allow()
        breaker.record_failure()
    assert breaker.state == CLOSED

    breaker.allow()
    breaker.record_failure()
    assert breaker.state == OPEN

    clock.now += 10
    with pytest.raises(NbiCircuitOpenError) as refused:
        breaker.allow("get_ont")
    assert refused.value.retry_after == pytest.approx(20)
    assert refused.value.to_dict()["retry_after_seconds"] == 20
    assert "Try again in about 20 seconds" in refused.value.message


def test_breaker_success_resets_the_failure_count():
    breaker = CircuitBreaker(failure_threshold=3)
    for _ in range(5):
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_success()
    assert breaker.state == CLOSED


def test_breaker_half_open_allows_one_probe_then_closes_or_reopens():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_seconds=30, clock=clock)
    breaker.record_failure()
    clock.now += 31

    breaker.allow()  # the probe
    assert breaker.state == HALF_OPEN
    with pytest.raises(NbiCircuitOpenError):
        breaker.allow()  # a second caller has to wait for the probe

    breaker.record_failure()  # probe failed
    assert breaker.state == OPEN
    with pytest.raises(NbiCircuitOpenError):
        breaker.allow()

    clock.now += 31
    breaker.allow()
    breaker.record_success()  # probe worked
    assert breaker.state == CLOSED
    breaker.allow()


# --- Gateway -----------------------------------------------------------------

def test_gateway_retries_reads_but_not_writes():
    async def scenario():
        gateway, backend = make_gateway()

        backend.outage = "refused"
        asyncio.get_running_loop().call_later(0.01, setattr, backend, "outage", None)
        gateway.retry_backoff = 0.02
        onts = await gateway.list_discovered_onts(MOCK_NETWORK)  # fails once, then succeeds
        assert len(onts) == 3 and backend.calls == 2

        backend.outage = "timeout"
        backend.calls = 0
        with pytest.raises(NbiTimeoutError) as error:
            await gateway.create_ont(MOCK_NETWORK, OntSpec(serial_number=NEW_SERIAL, profile="844G-1"))
        assert backend.calls == 1  # a write is attempted once
        assert error.value.operation == "create_ont"
        assert "may or may not have been applied" in error.value.message

    asyncio.run(scenario())


def test_gateway_outage_opens_breaker_and_then_fails_fast():
    async def scenario():
        gateway, backend = make_gateway(max_retries=0)
        backend.outage = "timeout"

        for _ in range(3):
            with pytest.raises(NbiTimeoutError):
                await gateway.get_ont(MOCK_NETWORK, 1)
        assert gateway.breaker.state == OPEN

        backend.calls = 0
        started = time.monotonic()
        with pytest.raises(NbiCircuitOpenError) as refused:
            await gateway.get_ont(MOCK_NETWORK, 1)
        assert backend.calls == 0  # CMS was not touched
        assert time.monotonic() - started < 0.02  # no waiting out a timeout
        assert refused.value.http_status == 503 and refused.value.retryable

    asyncio.run(scenario())


def test_gateway_cms_saying_no_does_not_count_as_an_outage():
    async def scenario():
        gateway, _ = make_gateway()
        for _ in range(10):
            with pytest.raises(NbiRejectedError):
                await gateway.create_ont(MOCK_NETWORK, OntSpec(serial_number="ABCD1234", profile="no-such-profile"))
        assert gateway.breaker.state == CLOSED

    asyncio.run(scenario())


def test_without_mock_mode_the_gateway_explains_that_cms_is_not_configured(monkeypatch):
    monkeypatch.setattr(settings, "mock_mode", False)

    async def scenario():
        with pytest.raises(NbiNotConfiguredError) as error:
            await build_gateway(settings).get_ont(MOCK_NETWORK, 1)
        assert error.value.http_status == 503
        assert "MOCK_MODE=true" in error.value.message

    asyncio.run(scenario())


# --- Worker: when a command is acknowledged -----------------------------------

class RecordingBus(InMemoryBus):
    """In-memory bus that records what the worker did with each command"""

    def __init__(self):
        super().__init__()
        self.log = []
        self.published = []
        self.fail_updates = 0

    async def publish_update(self, update):
        if self.fail_updates and update.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            self.fail_updates -= 1
            raise BusUnavailableError("broker down")
        self.published.append(update)
        self.log.append(f"update:{update.type}")
        await super().publish_update(update)

    def consume_commands(self, lanes):
        consumer = super().consume_commands(lanes)
        bus = self

        class Recording:
            start, stop, next = consumer.start, consumer.stop, consumer.next

            async def ack(self, delivery):
                bus.log.append("ack")
                await consumer.ack(delivery)

            async def retry(self, delivery):
                bus.log.append("retry")
                await consumer.retry(delivery)

            async def dead_letter(self, delivery, error):
                bus.log.append("dead_letter")
                await consumer.dead_letter(delivery, error)

        return Recording()


def command(**overrides):
    fields = dict(action="DO", lane="provisioning", key="k", kind="activation", summary="Test command", payload={})
    fields.update(overrides)
    return Command(**fields)


def run_worker(bus, handler, *commands, until="ack", **worker_options):
    """Run a worker until `until` has been logged once per command, then stop it"""
    async def scenario():
        options = dict(retry_backoff=0.01, max_retry_backoff=0.01)
        options.update(worker_options)
        worker = Worker(bus, ["provisioning"], {"DO": handler}, **options)
        for c in commands:
            await bus.publish_command(c)
        task = asyncio.create_task(worker.run())
        deadline = time.monotonic() + 5
        while bus.log.count(until) < len(commands) and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        worker.stop()
        await task

    asyncio.run(scenario())


def test_worker_acknowledges_only_after_the_result_is_published():
    bus = RecordingBus()

    async def handler(context, payload):
        await context.emit("log", "working")
        return {"done": True}

    run_worker(bus, handler, command())

    assert bus.log == ["update:status", "update:log", "update:result", "ack"]
    assert bus.published[-1].status == JobStatus.SUCCEEDED
    assert bus.published[-1].result == {"done": True}


def test_worker_parks_a_permanent_failure_then_acknowledges():
    bus = RecordingBus()

    async def handler(context, payload):
        raise NbiRejectedError("VLAN 999 does not exist.")

    run_worker(bus, handler, command())

    assert bus.log == ["update:status", "update:error", "dead_letter", "ack"]
    assert bus.published[-1].status == JobStatus.FAILED
    assert bus.published[-1].error["code"] == "cms_rejected"
    parked_command, parked_error = bus.dead_letters[0]
    assert parked_command.summary == "Test command" and parked_error["code"] == "cms_rejected"


def test_worker_does_not_acknowledge_a_transient_failure_and_runs_it_again():
    bus = RecordingBus()
    attempts = []

    async def handler(context, payload):
        attempts.append(context.attempt)
        if len(attempts) < 3:
            raise NbiTimeoutError("CMS did not answer.")
        return {}

    run_worker(bus, handler, command())

    assert attempts == [1, 2, 3]
    assert bus.log == ["update:status", "update:warning", "retry",
                       "update:status", "update:warning", "retry",
                       "update:status", "update:result", "ack"]
    assert "Retrying in" in bus.published[1].message
    assert bus.dead_letters == []


def test_worker_keeps_the_order_while_a_command_is_being_retried():
    bus = RecordingBus()
    seen = []

    async def handler(context, payload):
        seen.append((payload["n"], context.attempt))
        if payload["n"] == 1 and context.attempt == 1:
            raise NbiTimeoutError("CMS did not answer.")
        return {}

    run_worker(bus, handler, command(payload={"n": 1}), command(payload={"n": 2}))

    assert seen == [(1, 1), (1, 2), (2, 1)]


def test_worker_killed_mid_command_acknowledges_nothing():
    bus = RecordingBus()

    async def scenario():
        started = asyncio.Event()

        async def handler(context, payload):
            started.set()
            await asyncio.sleep(3600)

        worker = Worker(bus, ["provisioning"], {"DO": handler})
        await bus.publish_command(command())
        task = asyncio.create_task(worker.run())
        await started.wait()
        task.cancel()  # the pod dies
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())

    assert "ack" not in bus.log and "dead_letter" not in bus.log


def test_worker_does_not_acknowledge_when_the_result_cannot_be_published():
    bus = RecordingBus()
    bus.fail_updates = 1
    runs = []

    async def handler(context, payload):
        runs.append(context.attempt)
        return {}

    run_worker(bus, handler, command())

    assert runs == [1, 2]  # ran again rather than being lost
    assert bus.log == ["update:status", "retry", "update:status", "update:result", "ack"]


def test_worker_fails_a_command_that_waited_too_long_without_running_it():
    bus = RecordingBus()
    called = []

    async def handler(context, payload):
        called.append(True)

    stale = command()
    stale.created_at -= timedelta(hours=2)
    run_worker(bus, handler, stale, max_command_age=3600)

    assert called == []
    assert bus.log == ["update:error", "dead_letter", "ack"]
    assert bus.published[-1].error["code"] == "command_expired"


def test_worker_parks_unknown_actions_and_crashes():
    bus = RecordingBus()

    async def handler(context, payload):
        raise RuntimeError("bug")

    run_worker(bus, handler, command(), command(action="NOPE"))

    codes = [error["code"] for _, error in bus.dead_letters]
    assert codes == ["internal_error", "unknown_action"]
    assert bus.log.count("ack") == 2


def test_events_from_a_worker_thread_arrive_in_order_before_the_result():
    bus = RecordingBus()

    async def handler(context, payload):
        def blocking():
            for n in range(20):
                context.emit_threadsafe("log", f"line {n}")
        await asyncio.to_thread(blocking)
        return {}

    run_worker(bus, handler, command())

    messages = [u.message for u in bus.published if u.type == "log"]
    assert messages == [f"line {n}" for n in range(20)]
    assert bus.log[-2:] == ["update:result", "ack"]


# --- Activation is safe to run twice -------------------------------------------

class NullContext:
    job_id = "test"
    attempt = 1

    def __init__(self):
        self.messages = []

    async def emit(self, type, message, /, **data):
        self.messages.append(message)

    async def step(self, number, total, message):
        self.messages.append(message)


def activation(**overrides):
    fields = dict(serial_number=NEW_SERIAL, profile="844G-1",
                  services=[EthernetService(vlan=100, bandwidth_profile="RES-1G"),
                            EthernetService(vlan=200, bandwidth_profile="VOICE", ont_port=2)])
    fields.update(overrides)
    return ActivationRequest(**fields)


def test_activation_run_twice_creates_one_ont_and_binds_each_service_once():
    async def scenario():
        gateway, backend = make_gateway()
        first = await activate_ont(NullContext(), gateway, MOCK_NETWORK, activation(),
                                   verify_timeout=1, poll_seconds=0.01)
        context = NullContext()
        second = await activate_ont(context, gateway, MOCK_NETWORK, activation(),
                                    verify_timeout=1, poll_seconds=0.01)

        assert first == second
        assert first["online"] is True and first["services"] == 2
        assert sum(1 for o in backend._onts.values() if o.state.serial_number == NEW_SERIAL) == 1
        assert any("already exists" in m for m in context.messages)
        assert "Binding 0 service(s)" in " ".join(context.messages)

    asyncio.run(scenario())


def test_activation_survives_a_create_that_timed_out_after_cms_applied_it():
    """The nasty case: CMS creates the ONT but the answer is lost"""
    bus = RecordingBus()

    async def scenario():
        gateway, backend = make_gateway()
        real_create = backend.create_ont
        lost = []

        async def create_then_lose_the_answer(network, spec):
            ont = await real_create(network, spec)
            if not lost:
                lost.append(True)
                raise NbiTimeoutError("CMS did not answer.")
            return ont

        backend.create_ont = create_then_lose_the_answer

        async def handler(context, payload):
            return await activate_ont(context, gateway, MOCK_NETWORK, activation(),
                                      verify_timeout=1, poll_seconds=0.01)

        worker = Worker(bus, ["provisioning"], {"DO": handler}, retry_backoff=0.01, max_retry_backoff=0.01)
        await bus.publish_command(command())
        task = asyncio.create_task(worker.run())
        while "ack" not in bus.log:
            await asyncio.sleep(0.005)
        worker.stop()
        await task
        return backend

    backend = asyncio.run(asyncio.wait_for(scenario(), timeout=5))

    assert bus.published[-1].status == JobStatus.SUCCEEDED
    assert bus.log.count("retry") == 1 and bus.dead_letters == []
    matching = [o for o in backend._onts.values() if o.state.serial_number == NEW_SERIAL]
    assert len(matching) == 1 and len(matching[0].state.services) == 2


def test_activation_refuses_a_serial_already_provisioned_differently():
    async def scenario():
        gateway, _ = make_gateway()
        with pytest.raises(NbiConflictError) as error:
            # CXNK00A3F101 is ONT 1 in the mock CMS
            await activate_ont(NullContext(), gateway, MOCK_NETWORK,
                               activation(serial_number="CXNK00A3F101", ont_id=77, profile="716GE-I"),
                               verify_timeout=1, poll_seconds=0.01)
        assert "already provisioned as ONT 1" in error.value.message

    asyncio.run(scenario())


# --- HTTP API ------------------------------------------------------------------

@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "nbi_timeout_seconds", 0.1)
    monkeypatch.setattr(settings, "nbi_max_retries", 0)
    monkeypatch.setattr(settings, "nbi_breaker_failure_threshold", 2)
    monkeypatch.setattr(settings, "activation_poll_seconds", 0.01)
    with TestClient(app) as client:
        backend = client.app.state.gateway.backend
        backend.latency = 0
        backend.ranging_seconds = 0.05
        yield client


def sse(text):
    """Parse an SSE body into (event, data) pairs"""
    events = []
    for block in text.strip().split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.split("\n") if ": " in line and not line.startswith(":"))
        if "event" in fields:
            events.append((fields["event"], json.loads(fields["data"])))
    return events


def test_fast_lane_reads(api):
    health = api.get("/api/v2/nbi/health").json()
    assert health["backend"] == "mock-cms" and health["breaker"]["state"] == "closed"

    discovered = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/discovered").json()
    assert [d["serial_number"] for d in discovered] == ["CXNK00C4D301", "CXNK00C4D302", "CXNK00C4D303"]

    ont = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/4").json()
    assert ont["serial_number"] == "CXNK00A3F104" and ont["oper_state"] == "online"


def test_status_check_reports_per_ont_failures_without_failing_the_request(api):
    response = api.post(f"/api/v2/networks/{MOCK_NETWORK}/onts/status-check", json={"ont_ids": [1, 6, 999, 1]})

    assert response.status_code == 200
    body = response.json()
    assert (body["checked"], body["online"], body["failed"]) == (3, 1, 1)
    by_id = {r["ont_id"]: r for r in body["results"]}
    assert by_id[6]["ont"]["oper_state"] == "offline"
    assert by_id[999]["error"]["code"] == "not_found"


def test_bad_input_is_rejected_before_anything_is_queued(api):
    url = f"/api/v2/networks/{MOCK_NETWORK}/activations"
    bad_bodies = [
        {"serial_number": "x<y>", "profile": "844G-1"},
        {"serial_number": NEW_SERIAL, "profile": "844G-1", "services": [{"vlan": 5000, "bandwidth_profile": "a"}]},
        {"serial_number": NEW_SERIAL, "profile": "844G-1", "surprise": True},
        {"serial_number": NEW_SERIAL},
    ]
    for body in bad_bodies:
        assert api.post(url, json=body).status_code == 422, body
    assert api.post("/api/v2/networks/bad%20name!/activations",
                    json={"serial_number": NEW_SERIAL, "profile": "844G-1"}).status_code == 422
    assert api.get("/api/v2/jobs/").json() == []


def test_cms_errors_reach_the_client_in_one_shape(api):
    missing = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/999")
    assert missing.status_code == 404
    assert missing.json() == {"error": {"code": "not_found", "retryable": False, "operation": "get_ont",
                                        "message": f"ONT 999 does not exist on {MOCK_NETWORK}."}}

    api.app.state.gateway.backend.outage = "timeout"
    for _ in range(2):
        timed_out = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/1")
        assert timed_out.status_code == 504
        assert timed_out.json()["error"]["code"] == "cms_timeout"
        assert timed_out.json()["error"]["retryable"] is True

    # Two failures open the breaker (threshold set by the fixture): fail fast, say when to return
    paused = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/1")
    assert paused.status_code == 503
    assert paused.json()["error"]["code"] == "cms_unavailable"
    assert paused.json()["error"]["retry_after_seconds"] == int(paused.headers["Retry-After"]) == 30
    assert api.get("/api/v2/nbi/health").json()["breaker"]["state"] == "open"


def test_activation_returns_202_and_streams_progress(api):
    accepted = api.post(f"/api/v2/networks/{MOCK_NETWORK}/activations", json={
        "serial_number": NEW_SERIAL, "profile": "844G-1", "description": "22 Cedar Ct",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G"},
                     {"vlan": 200, "bandwidth_profile": "VOICE", "ont_port": 2}]})

    assert accepted.status_code == 202
    body = accepted.json()
    assert body["job"]["status"] == "queued" and body["job"]["kind"] == "activation"

    stream = api.get(body["events_url"])  # returns when the job ends
    assert stream.headers["content-type"].startswith("text/event-stream")
    events = sse(stream.text)

    assert [e for e, _ in events][:2] == ["status", "status"]
    steps = [d["data"]["step"] for e, d in events if e == "step"]
    assert steps == [1, 2, 3, 4, 5]  # find, create, ports, services, verify
    assert [d["seq"] for e, d in events if e != "end"] == list(range(1, len(events)))
    assert events[-2][0] == "result" and events[-2][1]["data"]["online"] is True

    end_event, job = events[-1]
    assert end_event == "end" and job["status"] == "succeeded"
    assert job["result"]["services"] == 2

    ont = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/{job['result']['ont_id']}").json()
    assert ont["serial_number"] == NEW_SERIAL and ont["description"] == "22 Cedar Ct"
    assert api.get(body["status_url"]).json()["status"] == "succeeded"

    # A reconnecting client resumes after the last event it saw
    resumed = sse(api.get(body["events_url"], headers={"Last-Event-ID": str(len(events) - 2)}).text)
    assert [e for e, _ in resumed] == ["result", "end"]


def test_activation_rejected_by_cms_rolls_back_and_fails_cleanly(api):
    accepted = api.post(f"/api/v2/networks/{MOCK_NETWORK}/activations", json={
        "serial_number": NEW_SERIAL, "profile": "844G-1",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G"}, {"vlan": 999, "bandwidth_profile": "RES-1G"}]})

    events = sse(api.get(accepted.json()["events_url"]).text)
    job = events[-1][1]

    assert job["status"] == "failed"
    assert job["error"]["code"] == "cms_rejected" and "VLAN 999 does not exist" in job["error"]["message"]
    assert any("Rolled back" in d["message"] for e, d in events if e == "log")
    # Nothing left behind: the ONT is back in the discovered list
    discovered = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/discovered").json()
    assert NEW_SERIAL in [d["serial_number"] for d in discovered]
    assert len(api.app.state.bus.dead_letters) == 1


def test_activation_waits_out_a_cms_outage_instead_of_failing(api, monkeypatch):
    worker = api.app.state.worker
    monkeypatch.setattr(worker, "retry_backoff", 0.02)
    monkeypatch.setattr(worker, "max_retry_backoff", 0.02)
    backend = api.app.state.gateway.backend
    breaker = api.app.state.gateway.breaker
    monkeypatch.setattr(breaker, "recovery_seconds", 0.05)
    backend.outage = "refused"

    accepted = api.post(f"/api/v2/networks/{MOCK_NETWORK}/activations",
                        json={"serial_number": NEW_SERIAL, "profile": "844G-1"})
    assert accepted.status_code == 202  # queued even though CMS is down

    time.sleep(0.3)
    assert api.get(accepted.json()["status_url"]).json()["status"] == "running"
    backend.outage = None  # CMS comes back

    events = sse(api.get(accepted.json()["events_url"]).text)
    assert events[-1][1]["status"] == "succeeded"
    assert any(e == "warning" and "Retrying in" in d["message"] for e, d in events)
    assert api.app.state.bus.dead_letters == []


def test_unknown_job_is_404(api):
    assert api.get("/api/v2/jobs/nope").status_code == 404
    assert api.get("/api/v2/jobs/nope/events").status_code == 404
