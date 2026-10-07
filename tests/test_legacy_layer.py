"""
Tests for the legacy GPON layer: the event and translation models, the signup
and cutover flows through the API, and the CMS backend and client talking
SOAP to the mock CMS NBI server.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.legacy import events as legacy_events
from app.legacy.models import (
    BandwidthMapping, GponSignupEvent, LegacySubscriberRecord, ProfileMap, TranslationError,
    XgsCutoverEvent, XgsMigrationRequest, translate
)
from app.legacy.snapshot import take_snapshot
from app.models.provisioning_model import EthernetService, OntSpec, ProvisionedService
from app.nbi.errors import (
    NbiAuthError, NbiConflictError, NbiConnectionError, NbiNotFoundError, NbiRejectedError, NbiTimeoutError
)
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK
from config import settings
from main import app

REPO = Path(__file__).resolve().parents[1]
PROFILE_MAP = ProfileMap(
    bandwidth_profiles={"12": BandwidthMapping(service_profile="XGS-RESI-1G"),
                        "20": BandwidthMapping(service_profile="XGS-VOICE", service_type="voice")},
    tag_action_vlans={"3": 100})


def record(*services):
    return LegacySubscriberRecord(
        network="NTWK-1", subscriber_id="100482", ont_id=1204, serial_number="CXNK00A3F104",
        ont_profile="162", pon_port="1/2/3", description="118 Alder St", services=list(services))


def cutover_event(**overrides):
    fields = dict(event_id="evt-00000001", network="NTWK-1", subscriber_id="100482")
    fields.update(overrides)
    return XgsCutoverEvent(**fields)


# --- Translation -----------------------------------------------------------------

def test_translate_maps_profiles_and_carries_vlans():
    request = translate(
        record(ProvisionedService(service_index=1, ont_port=1, vlan=100, bandwidth_profile="12", tag_action="3"),
               ProvisionedService(service_index=1, ont_port=2, vlan=200, inner_vlan=7, bandwidth_profile="20",
                                  admin_state="disabled")),
        cutover_event(xgs_serial_number="CXNK10AA0001"), PROFILE_MAP)

    assert request.migration_id == "evt-00000001" and request.subscriber_id == "100482"
    assert request.xgs_serial_number == "CXNK10AA0001"
    assert request.source.model_dump() == {
        "system": "calix-cms", "network": "NTWK-1", "ont_id": 1204, "serial_number": "CXNK00A3F104",
        "ont_profile": "162", "pon_port": "1/2/3"}
    assert [s.model_dump() for s in request.services] == [
        {"service_type": "data", "service_profile": "XGS-RESI-1G", "vlan": 100, "inner_vlan": None,
         "ont_port": 1, "enabled": True, "description": None},
        {"service_type": "voice", "service_profile": "XGS-VOICE", "vlan": 200, "inner_vlan": 7,
         "ont_port": 2, "enabled": False, "description": None}]


def test_translate_takes_the_vlan_from_the_tag_action_when_the_service_has_none():
    request = translate(record(ProvisionedService(vlan=None, bandwidth_profile="12", tag_action="3")),
                        cutover_event(), PROFILE_MAP)
    assert request.services[0].vlan == 100


def test_translate_refuses_to_guess_and_names_every_problem():
    with pytest.raises(TranslationError) as error:
        translate(record(ProvisionedService(service_index=1, vlan=100, bandwidth_profile="99"),
                         ProvisionedService(service_index=2, vlan=None, bandwidth_profile="12", tag_action="8")),
                  cutover_event(), PROFILE_MAP)
    assert len(error.value.problems) == 2
    assert "bandwidth profile '99' has no entry" in error.value.problems[0]
    assert "tag action '8' has no entry" in error.value.problems[1]

    with pytest.raises(TranslationError, match="no services on CMS"):
        translate(record(), cutover_event(), PROFILE_MAP)


def test_the_shipped_profile_map_loads_and_bad_ones_are_rejected(tmp_path):
    shipped = ProfileMap.load(settings.legacy_profile_map_path)
    assert shipped.bandwidth_profiles["RES-1G"].service_profile == "XGS-RESI-1G"

    bad = tmp_path / "map.yml"
    bad.write_text("bandwidth_profiles:\n  '12': {service_profile: X, speed: fast}\n")
    with pytest.raises(ValidationError):
        ProfileMap.load(bad)


# --- Events on the topic -------------------------------------------------------------

SIGNUP_JSON = {
    "type": "GPON_SIGNUP", "event_id": "sonar-evt-0001", "occurred_at": "2026-10-06T12:00:00",
    "network": "NTWK-1", "subscriber_id": "100482", "serial_number": "CXNK00A3F104", "profile": "162",
    "services": [{"vlan": 100, "bandwidth_profile": "12", "tag_action": "3"}],
    "plan_name": "Fiber 1 Gig",  # a field this layer does not know about
}


def test_an_event_from_the_topic_becomes_a_command():
    command = legacy_events.decode(json.dumps(SIGNUP_JSON).encode())

    assert (command.id, command.action, command.lane, command.kind) == (
        "sonar-evt-0001", "GPON_SIGNUP", "legacy", "signup")
    assert command.key == "NTWK-1"  # one network's events stay in order
    assert command.created_at == datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)  # naive time read as UTC
    assert "plan_name" not in command.payload  # unknown fields are dropped, not an error
    # ...and what goes back on the topic is plain event JSON again
    assert legacy_events.decode(legacy_events.encode(command)) == command


@pytest.mark.parametrize("change", [
    {"type": "DELETE_EVERYTHING"},
    {"event_id": "short"},
    {"network": "bad name!"},
    {"serial_number": "<xml>"},
    {"services": []},
    {"services": [{"vlan": 5000, "bandwidth_profile": "12"}]},
])
def test_invalid_events_are_rejected(change):
    with pytest.raises(ValueError):
        legacy_events.decode(json.dumps({**SIGNUP_JSON, **change}).encode())


# --- Through the API (in-process queue, simulated CMS) ----------------------------------

@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "activation_poll_seconds", 0.01)
    with TestClient(app) as client:
        client.app.state.gateway.backend.latency = 0
        yield client


def finish(api, response):
    """Follow a 202 to the end of its job and return the finished job"""
    assert response.status_code == 202, response.text
    stream = api.get(response.json()["events_url"]).text
    return json.loads(stream.strip().split("\n\n")[-1].split("data: ", 1)[1])


def published(api):
    return api.app.state.bus.events.get(settings.xgs_migration_topic, [])


def test_signup_provisions_an_ont_that_is_not_installed_yet(api):
    job = finish(api, api.post(f"/api/v2/networks/{MOCK_NETWORK}/signups", json={
        "subscriber_id": "SUB-3001", "serial_number": "CXNK00D5E401", "profile": "844G-1",
        "description": "9 Elm Rd",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}))

    assert job["status"] == "succeeded" and job["kind"] == "signup"
    assert job["result"]["subscriber_id"] == "SUB-3001" and job["result"]["online"] is False
    ont = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/{job['result']['ont_id']}").json()
    assert (ont["serial_number"], ont["subscriber_id"], ont["description"]) == ("CXNK00D5E401", "SUB-3001", "9 Elm Rd")
    assert [(s["vlan"], s["bandwidth_profile"], s["tag_action"]) for s in ont["services"]] == [(100, "RES-1G", "1")]


def test_signup_input_is_validated_before_anything_is_queued(api):
    url = f"/api/v2/networks/{MOCK_NETWORK}/signups"
    good = {"subscriber_id": "SUB-3001", "serial_number": "CXNK00D5E401", "profile": "844G-1",
            "services": [{"vlan": 100, "bandwidth_profile": "RES-1G"}]}
    for change in ({"services": []}, {"subscriber_id": "has spaces"}, {"surprise": 1}, {"serial_number": "x"}):
        assert api.post(url, json={**good, **change}).status_code == 422, change
    assert api.get("/api/v2/jobs/").json() == []


def test_cutover_reads_cms_and_publishes_a_migration_request(api):
    # SUB-1003 has data on port 1 and voice on port 2 in the simulated CMS
    job = finish(api, api.post(f"/api/v2/networks/{MOCK_NETWORK}/cutovers",
                               json={"subscriber_id": "SUB-1003", "xgs_serial_number": "CXNK10AA0001"}))

    assert job["status"] == "succeeded" and job["kind"] == "cutover"
    assert job["result"]["legacy_ont_id"] == 3 and job["result"]["services"] == 2

    (key, value), = published(api)
    request = XgsMigrationRequest.model_validate(value)
    assert key == "SUB-1003" and request.migration_id == job["id"]
    assert request.source.serial_number == "CXNK00A3F103" and request.xgs_serial_number == "CXNK10AA0001"
    assert [(s.service_type, s.service_profile, s.vlan, s.ont_port) for s in request.services] == [
        ("data", "XGS-RESI-1G", 100, 1), ("voice", "XGS-VOICE", 200, 2)]

    # CMS was only read: the legacy ONT is untouched
    ont = api.get(f"/api/v2/networks/{MOCK_NETWORK}/onts/3").json()
    assert ont["admin_state"] == "enabled" and len(ont["services"]) == 2


def test_cutover_failures_publish_nothing_and_say_why(api):
    url = f"/api/v2/networks/{MOCK_NETWORK}/cutovers"

    unknown = finish(api, api.post(url, json={"subscriber_id": "SUB-9999"}))
    assert unknown["status"] == "failed" and unknown["error"]["code"] == "not_found"

    # Two ONTs for one subscriber: refuse to pick one
    backend = api.app.state.gateway.backend
    backend._onts[2].state.subscriber_id = "SUB-1001"
    ambiguous = finish(api, api.post(url, json={"subscriber_id": "SUB-1001"}))
    assert ambiguous["error"]["code"] == "conflict" and "(1, 2)" in ambiguous["error"]["message"]
    chosen = finish(api, api.post(url, json={"subscriber_id": "SUB-1001", "ont_id": 2}))
    assert chosen["status"] == "succeeded" and chosen["result"]["legacy_ont_id"] == 2

    # A bandwidth profile with no modern equivalent
    backend._onts[5].state.services[0].bandwidth_profile = "LEGACY-ODD-PLAN"
    untranslatable = finish(api, api.post(url, json={"subscriber_id": "SUB-1005"}))
    assert untranslatable["error"]["code"] == "untranslatable"
    assert "LEGACY-ODD-PLAN" in untranslatable["error"]["message"]
    assert "Nothing was sent" in untranslatable["error"]["message"]

    assert [key for key, _ in published(api)] == ["SUB-1001"]  # only the one that succeeded
    assert len(api.app.state.bus.dead_letters) == 3


# --- The CMS client against the mock NBI server (real SOAP over HTTP) ----------------------

@pytest.fixture
def cms(nbi_server):
    from app.nbi.cms_backend import CmsBackend

    server, port = nbi_server
    server._fresh_state()

    def connect(password="demo-password", timeout=3.0):
        return CmsBackend("127.0.0.1", port, "nbi-demo", password, timeout=timeout)

    return server, connect


def test_cms_client_backend_reads(cms):
    server, connect = cms

    async def scenario():
        backend = connect()
        discovered = await backend.list_discovered_onts(MOCK_NETWORK)
        assert [(d.serial_number, d.model, d.pon_port) for d in discovered] == [
            ("CXNK00C4D301", "844G-1", "1/1/1"), ("CXNK00C4D302", "716GE-I", "1/1/2"),
            ("CXNK00C4D303", "812G-1", "1/1/2")]

        ont = await backend.get_ont(MOCK_NETWORK, 3)
        assert (ont.serial_number, ont.profile, ont.pon_port, ont.subscriber_id, ont.oper_state.value) == (
            "CXNK00A3F103", "844G-1", "1/1/1", "SUB-1003", "online")
        assert [(s.ont_port, s.service_index, s.vlan, s.bandwidth_profile, s.tag_action) for s in ont.services] == [
            (1, 1, 100, "RES-1G", "1"), (2, 1, 200, "VOICE", "2")]
        assert (await backend.get_ont(MOCK_NETWORK, 6)).oper_state.value == "offline"

        assert (await backend.find_ont_by_serial(MOCK_NETWORK, "CXNK00A3F104")).ont_id == 4
        assert await backend.find_ont_by_serial(MOCK_NETWORK, "CXNK00FFFFFF") is None
        assert [o.ont_id for o in await backend.find_onts_by_subscriber(MOCK_NETWORK, "SUB-1009")] == [9]
        assert await backend.find_onts_by_subscriber(MOCK_NETWORK, "nobody") == []
        assert len(await backend.list_onts(MOCK_NETWORK)) == 14
        await backend.close()
        assert server.app.state.sessions == set()  # logged out

    asyncio.run(scenario())


def test_cms_client_backend_writes_and_escapes_what_it_sends(cms):
    server, connect = cms

    async def scenario():
        backend = connect()
        created = await backend.create_ont(MOCK_NETWORK, OntSpec(
            serial_number="CXNK00C4D301", profile="844G-1", subscriber_id="SUB-2001",
            description='A & B <b>"quoted"</b>'))
        assert created.ont_id == 15 and created.pon_port == "1/1/1"
        assert created.description == 'A & B <b>"quoted"</b>'  # arrived intact, not as broken XML

        await backend.add_ethernet_service(MOCK_NETWORK, 15, EthernetService(
            vlan=100, bandwidth_profile="RES-1G", tag_action="1"))
        await backend.add_ethernet_service(MOCK_NETWORK, 15, EthernetService(
            vlan=200, bandwidth_profile="VOICE", tag_action="2"))
        services = (await backend.get_ont(MOCK_NETWORK, 15)).services
        assert [(s.service_index, s.vlan) for s in services] == [(1, 100), (2, 200)]

        await backend.delete_ont(MOCK_NETWORK, 15)
        with pytest.raises(NbiNotFoundError):
            await backend.get_ont(MOCK_NETWORK, 15)
        await backend.close()

    asyncio.run(scenario())


def test_cms_client_backend_turns_cms_refusals_into_typed_errors(cms):
    server, connect = cms

    async def scenario():
        backend = connect()
        with pytest.raises(NbiNotFoundError, match="ONT 999 does not exist"):
            await backend.get_ont(MOCK_NETWORK, 999)
        with pytest.raises(NbiNotFoundError, match="not known to CMS"):
            await backend.get_ont("NTWK-NOPE", 1)
        with pytest.raises(NbiConflictError, match="already assigned to ONT 4"):
            await backend.create_ont(MOCK_NETWORK, OntSpec(serial_number="CXNK00A3F104", profile="844G-1"))
        with pytest.raises(NbiRejectedError, match="VLAN 999 does not exist"):
            await backend.add_ethernet_service(MOCK_NETWORK, 1, EthernetService(
                vlan=999, bandwidth_profile="RES-1G", tag_action="1"))
        with pytest.raises(NbiRejectedError, match="no tag_action"):
            await backend.add_ethernet_service(MOCK_NETWORK, 1, EthernetService(vlan=100, bandwidth_profile="RES-1G"))
        await backend.close()

        with pytest.raises(NbiAuthError, match="rejected this API's username or password"):
            await connect(password="wrong").get_ont(MOCK_NETWORK, 1)

    asyncio.run(scenario())


def test_cms_client_backend_survives_an_expired_session_and_reports_outages(cms):
    server, connect = cms

    async def scenario():
        backend = connect(timeout=0.5)
        assert (await backend.get_ont(MOCK_NETWORK, 1)).ont_id == 1

        server.app.state.sessions.clear()  # CMS forgets the session
        assert (await backend.get_ont(MOCK_NETWORK, 1)).ont_id == 1  # logged in again by itself

        server.app.state.cms.outage = "refused"
        with pytest.raises(NbiConnectionError, match="HTTP 503"):
            await backend.get_ont(MOCK_NETWORK, 1)
        server.app.state.cms.outage = "timeout"
        with pytest.raises(NbiTimeoutError):
            await backend.get_ont(MOCK_NETWORK, 1)
        server.app.state.cms.outage = None

        dead = type(backend)("127.0.0.1", 1, "nbi-demo", "demo-password", timeout=0.5)  # nothing listens here
        with pytest.raises(NbiConnectionError, match="Could not connect to CMS"):
            await dead.get_ont(MOCK_NETWORK, 1)

    asyncio.run(scenario())


def test_signup_and_cutover_through_the_real_client(cms):
    """The whole legacy path with SOAP on the wire: gateway -> CMS client -> mock NBI server"""
    from app.legacy.handlers import build_legacy_handlers
    from app.messaging.bus import InMemoryBus

    server, connect = cms

    class Context:
        job_id, attempt = "test", 1

        async def emit(self, type, message, /, **data):
            pass

        async def step(self, number, total, message):
            pass

    async def scenario():
        bus, gateway = InMemoryBus(), NbiGateway(connect(), timeout=5)
        handlers = build_legacy_handlers(settings, gateway, bus)

        signup = GponSignupEvent(
            event_id="evt-signup-1", network=MOCK_NETWORK, subscriber_id="SUB-7001",
            serial_number="CXNK00C4D302", profile="716GE-I", wait_for_online=True,
            services=[EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1")])
        first = await handlers["GPON_SIGNUP"](Context(), signup.model_dump(mode="json"))
        again = await handlers["GPON_SIGNUP"](Context(), signup.model_dump(mode="json"))  # delivered twice
        assert first == again and first["online"] is True and first["services"] == 1
        assert len(await gateway.find_onts_by_subscriber(MOCK_NETWORK, "SUB-7001")) == 1

        cutover = XgsCutoverEvent(event_id="evt-cutover-1", network=MOCK_NETWORK, subscriber_id="SUB-7001")
        result = await handlers["XGS_CUTOVER"](Context(), cutover.model_dump(mode="json"))
        await handlers["XGS_CUTOVER"](Context(), cutover.model_dump(mode="json"))  # delivered twice

        sent = bus.events[settings.xgs_migration_topic]
        assert result["legacy_ont_id"] == first["ont_id"]
        assert len(sent) == 2 and sent[0] == sent[1]  # the repeat is identical, same migration_id
        assert sent[0][1]["migration_id"] == "evt-cutover-1"
        assert sent[0][1]["source"]["serial_number"] == "CXNK00C4D302"
        await gateway.close()

    asyncio.run(scenario())


def test_the_committed_demo_snapshot_matches_what_the_exporter_produces(cms):
    from app.nbi.router_side import MOCK_RG_ADDRESSING, RouterAddressing

    server, connect = cms
    backend = connect()
    # Made with CMS_RG_PORT_MODE_ELEMENT set and nothing else, so port modes are read (see deploy/demo/README.md)
    backend._router = RouterAddressing(port_mode_element=MOCK_RG_ADDRESSING.port_mode_element)

    async def scenario():
        gateway = NbiGateway(backend, timeout=10)
        snapshot = await take_snapshot(gateway, MOCK_NETWORK, "OLT-MOCK")
        await gateway.close()
        return snapshot

    fresh = asyncio.run(scenario())
    committed = yaml.safe_load((REPO / "deploy/demo/snapshots/OLT-MOCK.yml").read_text())
    fresh.pop("captured_at"), committed.pop("captured_at")
    assert fresh == committed
