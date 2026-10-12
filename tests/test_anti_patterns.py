"""
Tests for the guards added against the anti-patterns in docs/ANTI_PATTERNS.md:
identity checks on reused IDs, dry runs, reading back every write, parsers
that refuse what they cannot read, per-network breakers, a cap on retries,
scoped and rate-limited API keys, and the export's coverage flag.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.auth import WriteLimiter, parse_entries
from app.controller.lifecycle_controller import apply_ont_change
from app.controller.provisioning_controller import activate_ont
from app.legacy.export import COVERAGE_COMPLETE, COVERAGE_INCOMPLETE, export_ont
from app.models.job_model import JobStatus
from app.models.lifecycle_model import DeleteOnt, DeleteService, UpdatePort, UpdateService
from app.models.provisioning_model import ActivationRequest, Attachment, EthernetService, PortChange, ServiceChange
from app.nbi import cms_backend
from app.nbi.breaker import CLOSED, OPEN, CircuitBreaker
from app.nbi.e7_backend import E7CliBackend
from app.nbi.e7_session import E7Node
from app.nbi.errors import (
    NbiConflictError, NbiConnectionError, NbiParseError, NbiTimeoutError, NbiVerifyError
)
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK, MockCmsBackend
from config import settings
from main import app
from tests.test_integration_layer import RecordingBus, command, run_worker

NET = MOCK_NETWORK
BASE = f"/api/v2/networks/{NET}"


class Context:
    """Enough of a JobContext for a controller called directly"""
    attempt = 1

    def __init__(self):
        self.events = []

    async def step(self, number, total, message):
        self.events.append(message)

    async def emit(self, type, message, /, **data):
        self.events.append(message)

    async def record_ownership(self, message, /, **data):
        self.events.append(message)

    async def ownership_records(self):
        return []


def gateway(**options):
    backend = MockCmsBackend(latency=0, ranging_seconds=0)
    return NbiGateway(backend, timeout=1, retry_backoff=0, **options), backend


def change(gw, ont_id, the_change):
    return asyncio.run(apply_ont_change(Context(), gw, NET, ont_id, the_change))


# --- Identity of reused IDs -------------------------------------------------------------

def test_a_change_for_an_ont_id_that_now_holds_another_serial_is_refused():
    gw, _ = gateway()
    with pytest.raises(NbiConflictError, match="not CXNK00A3F105"):
        change(gw, 4, UpdateService(port="eth1", service_index=1, serial_number="CXNK00A3F105", expect_vlan=100,
                                    change=ServiceChange(admin_state="disabled")))


def test_every_change_must_say_which_ont_and_which_service_it_means():
    with pytest.raises(ValidationError, match="serial_number"):
        UpdatePort(port="eth1", change=PortChange(admin_state="disabled"))
    with pytest.raises(ValidationError, match="expect_vlan"):
        UpdateService(port="eth1", service_index=1, serial_number="CXNK00A3F104",
                      change=ServiceChange(admin_state="disabled"))
    with pytest.raises(ValidationError, match="expect_vlan"):
        DeleteService(port="eth1", service_index=1, serial_number="CXNK00A3F104")
    assert DeleteService(port="eth1", service_index=1, serial_number="CXNK00A3F104",
                         expect_vlan="none").expect_vlan == "none"


def test_a_change_for_a_service_number_that_now_holds_another_vlan_is_refused():
    gw, _ = gateway()
    with pytest.raises(NbiConflictError, match="not VLAN 200"):
        change(gw, 4, UpdateService(port="eth1", service_index=1, serial_number="CXNK00A3F104", expect_vlan=200,
                                    change=ServiceChange(admin_state="disabled")))
    with pytest.raises(NbiConflictError, match="not no outer VLAN"):
        change(gw, 4, UpdateService(port="eth1", service_index=1, serial_number="CXNK00A3F104", expect_vlan="none",
                                    change=ServiceChange(admin_state="disabled")))
    done = change(gw, 4, UpdateService(port="eth1", service_index=1, expect_vlan=100, serial_number="CXNK00A3F104",
                                       change=ServiceChange(admin_state="disabled")))
    assert done["changed"] is True and done["verified"] is True


# --- Dry runs -----------------------------------------------------------------------------

def test_a_dry_run_delete_reports_what_it_would_delete_and_deletes_nothing():
    gw, _ = gateway()
    result = change(gw, 4, DeleteOnt(serial_number="CXNK00A3F104", force=True, voice_checked=True, dry_run=True))
    assert result["dry_run"] is True and result["would_delete"] is True and result["deleted"] is False
    assert result["services"] == ["ethernet port 1 service 1 (VLAN 100)"]
    assert len(asyncio.run(gw.get_ont(NET, 4)).services) == 1


# --- Reading back every write ------------------------------------------------------------

def test_a_change_that_cms_accepts_but_does_not_hold_fails_the_job():
    gw, backend = gateway()

    async def ignored(*args, **kwargs):
        return None

    backend.update_service = ignored
    with pytest.raises(NbiVerifyError, match="admin_state"):
        change(gw, 4, UpdateService(port="eth1", service_index=1, serial_number="CXNK00A3F104", expect_vlan=100,
                                    change=ServiceChange(admin_state="disabled")))


def test_an_activation_whose_service_does_not_read_back_is_not_reported_as_success():
    gw, backend = gateway()

    async def ignored(*args, **kwargs):
        return None

    backend.add_ethernet_service = ignored
    request = ActivationRequest(serial_number="CXNK00C4D301", profile="844G-1", wait_for_online=False,
                                services=[EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1")])
    with pytest.raises(NbiVerifyError, match="VLAN 100 \\(missing\\)"):
        asyncio.run(activate_ont(Context(), gw, NET, request, verify_timeout=0, poll_seconds=0))


def test_an_activation_says_whether_the_ont_is_live():
    gw, _ = gateway()
    request = ActivationRequest(serial_number="CXNK00C4D301", profile="844G-1", wait_for_online=False,
                                services=[EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1")])
    result = asyncio.run(activate_ont(Context(), gw, NET, request, verify_timeout=0, poll_seconds=0))
    assert result["verified"] is True and result["outcome"] in ("online", "provisioned_not_waited")


def test_a_refused_ont_delete_says_which_data_services_are_already_gone():
    gw, backend = gateway()

    async def refused(*args, **kwargs):
        raise NbiConflictError("ONT 4 still has a voice service.")

    backend.delete_ont = refused
    with pytest.raises(NbiConflictError) as caught:
        change(gw, 4, DeleteOnt(serial_number="CXNK00A3F104", force=True, voice_checked=True))
    assert "was NOT deleted" in caught.value.message
    assert "ethernet port 1 service 1 (VLAN 100)" in caught.value.message


# --- Parsers that refuse what they cannot read -------------------------------------------

def test_a_cms_service_without_its_number_is_an_error_not_service_1():
    reply = {"type": "EthSvc", "id": {"ont": "4", "ontslot": "3"}, "out-tag": "100"}
    with pytest.raises(NbiParseError, match="ethsvc"):
        cms_backend._service(reply)


def test_a_cms_ont_without_a_serial_is_an_error():
    with pytest.raises(NbiParseError, match="serno"):
        cms_backend._ont_state({"type": "Ont", "id": {"ont": "4"}})


def test_an_e7_service_row_that_cannot_be_read_is_an_error_not_a_missing_service():
    backend = E7CliBackend([E7Node(name="E7A", host="e7a")], sessions={"E7A": object()})

    async def show(network, command):
        return ("Port   Service  BW Profile\n"
                "-----  -------  ----------\n"
                "4/g1   Data1    RES-1G\n"
                "                RES-1G\n")

    backend._show = show
    with pytest.raises(NbiParseError, match="without a port or a name"):
        asyncio.run(backend._services("E7A", 4))


def test_e7_ont_detail_without_a_serial_is_an_error_not_no_such_ont():
    backend = E7CliBackend([E7Node(name="E7A", host="e7a")], sessions={"E7A": object()})

    async def show(network, command):
        return "Something: else\n"

    backend._show = show
    with pytest.raises(NbiParseError, match="no serial number"):
        asyncio.run(backend._ont_detail("E7A", 4))


# --- Isolating a sick device ------------------------------------------------------------

def test_one_unreachable_network_does_not_open_the_breaker_for_the_others():
    backend = MockCmsBackend(latency=0, ranging_seconds=0)
    gw = NbiGateway(backend, timeout=1, retry_backoff=0, max_retries=0,
                    breaker=CircuitBreaker(failure_threshold=2, recovery_seconds=30), breaker_per_network=True)
    real = backend.list_vlans

    async def list_vlans(network):
        if network == "SICK":
            raise NbiConnectionError("no route to host")
        return await real(network)

    backend.list_vlans = list_vlans
    for _ in range(2):
        with pytest.raises(NbiConnectionError):
            asyncio.run(gw.list_vlans("SICK"))
    assert gw.breaker_for("SICK").state == OPEN
    assert gw.breaker_for(NET).state == CLOSED
    assert asyncio.run(gw.list_vlans(NET))
    assert set(gw.health()["breakers"]) == {"SICK", NET}


def test_a_command_that_keeps_timing_out_is_failed_after_max_attempts():
    bus = RecordingBus()

    async def handler(context, payload):
        raise NbiTimeoutError("CMS did not answer.")

    run_worker(bus, handler, command(), until="dead_letter", max_attempts=3)
    assert bus.log.count("retry") == 2
    failed = bus.published[-1]
    assert failed.status == JobStatus.FAILED and "Gave up after 3 attempts" in failed.error["message"]
    assert failed.error["retryable"] is False


# --- Scoped, rate-limited API keys --------------------------------------------------------

READER = "reader-key-0123456789abcdef"
CHANGER = "changer-key-0123456789abcdef"
ADMIN = "admin-key-0123456789abcdef"


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "api_keys", f"reader:{READER}:read,changer:{CHANGER}:read+change,admin:{ADMIN}")
    with TestClient(app) as client:
        client.app.state.gateway.backend.latency = 0
        yield client


def test_scopes_limit_what_a_key_may_do(scoped):
    delete = f"{BASE}/onts/4?serial_number=CXNK00A3F104&dry_run=true"
    assert scoped.get(f"{BASE}/vlans", headers={"X-API-Key": READER}).status_code == 200
    assert scoped.post(f"{BASE}/onts/status-check", json={"ont_ids": [4]},
                       headers={"X-API-Key": READER}).status_code == 200
    refused = scoped.post(f"{BASE}/onts/4/ports/eth1/disable?serial_number=CXNK00A3F104",
                          headers={"X-API-Key": READER})
    assert refused.status_code == 403 and refused.json()["error"]["code"] == "forbidden"
    assert scoped.post(f"{BASE}/onts/4/ports/eth1/disable?serial_number=CXNK00A3F104&dry_run=true",
                       headers={"X-API-Key": CHANGER}).status_code == 202
    assert scoped.delete(delete, headers={"X-API-Key": CHANGER}).status_code == 403
    assert scoped.delete(delete, headers={"X-API-Key": ADMIN}).status_code == 202  # no scopes listed: everything


def test_scopes_are_checked_at_startup():
    assert parse_entries(f"a:{READER}:read+delete")["a"] == (READER, frozenset({"read", "delete"}))
    assert parse_entries(READER)["key-1"] == (READER, None)
    with pytest.raises(ValueError, match="scopes"):
        parse_entries(f"a:{READER}:write")


def test_a_key_over_its_delete_limit_is_refused_and_nothing_is_queued(scoped, monkeypatch):
    scoped.app.state.limiter = WriteLimiter(per_minute=100, deletes_per_hour=2)
    delete = f"{BASE}/onts/4?serial_number=CXNK00A3F104&dry_run=true"
    assert [scoped.delete(delete, headers={"X-API-Key": ADMIN}).status_code for _ in range(3)] == [202, 202, 429]
    assert scoped.get(f"{BASE}/vlans", headers={"X-API-Key": ADMIN}).status_code == 200  # reads are not limited
    assert scoped.post(f"{BASE}/onts/4/ports/eth1/disable?serial_number=CXNK00A3F104&dry_run=true",
                       headers={"X-API-Key": ADMIN}).status_code == 202


def test_the_write_limit_counts_per_minute():
    now = [0.0]
    limiter = WriteLimiter(per_minute=2, deletes_per_hour=0, clock=lambda: now[0])
    limiter.check("k", "change")
    limiter.check("k", "change")
    with pytest.raises(Exception, match="changes a minute"):
        limiter.check("k", "change")
    limiter.check("other", "change")
    now[0] = 61.0
    limiter.check("k", "change")


# --- Coverage that does not look complete when it is not ------------------------------------

def test_an_ont_with_a_service_in_an_unknown_slot_is_exported_as_incomplete():
    gw, backend = gateway()
    assert asyncio.run(export_ont(gw, NET, 4)).coverage == COVERAGE_COMPLETE
    real = backend.get_ont

    async def get_ont(network, ont_id):
        ont = await real(network, ont_id)
        return ont.model_copy(update={"services": [ont.services[0].model_copy(update={"attachment": Attachment.UNKNOWN})]})

    backend.get_ont = get_ont
    record = asyncio.run(export_ont(gw, NET, 4))
    assert record.coverage == COVERAGE_INCOMPLETE
