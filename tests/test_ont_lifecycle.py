"""
Tests for one ONT's full configuration and lifecycle: Ethernet ports, the
router side of ONTs such as the 844G, enabling, disabling, changing and
deleting services, deleting ONTs, and the profile and VLAN lookups.

Two paths are covered:
  - the API with its queue and worker, against the in-process simulated CMS
  - cms-nbi-client 0.1.0 speaking SOAP to the mock NBI server
"""

import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from app.controller.lifecycle_controller import apply_ont_change
from app.models.lifecycle_model import DeleteOnt, DeleteService, UpdatePort, UpdateService
from app.models.provisioning_model import (
    Attachment, EthernetService, OntSpec, PortChange, ServiceChange
)
from app.nbi.errors import NbiConflictError, NbiNotFoundError, NbiNotVerifiedError, NbiRejectedError
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK
from app.nbi.router_side import MOCK_RG_ADDRESSING, router_side_addressing
from config import settings
from main import app

NET = MOCK_NETWORK
BASE = f"/api/v2/networks/{NET}"


# --- Through the API (in-process queue and worker, simulated CMS) ---------------------

@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "ansible_mock", None)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "activation_poll_seconds", 0.01)
    monkeypatch.setattr(settings, "api_keys", None)
    with TestClient(app) as client:
        client.app.state.gateway.backend.latency = 0
        yield client


def finish(api, response):
    """Follow a 202 to the end of its job and return the finished job"""
    assert response.status_code == 202, response.text
    stream = api.get(response.json()["events_url"]).text
    return json.loads(stream.strip().split("\n\n")[-1].split("data: ", 1)[1])


def signup(api, **overrides):
    body = {"subscriber_id": "SUB-4001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
            "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}
    body.update(overrides)
    return finish(api, api.post(f"{BASE}/signups", json=body))


def test_signup_configures_ports_and_attaches_services_to_the_router(api):
    job = signup(api, ports=[
        {"port": 1, "admin_state": "enabled", "subscriber_id": "SUB-4001", "security_profile": "3"},
        {"port": 2, "admin_state": "disabled"}],
        services=[{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"},
                  {"vlan": 300, "bandwidth_profile": "RES-1G", "tag_action": "1", "ont_port": 3}])
    assert job["status"] == "succeeded", job
    assert job["result"]["ports_configured"] == 2
    ont_id = job["result"]["ont_id"]

    ports = api.get(f"{BASE}/onts/{ont_id}/ports").json()
    eth = {p["port"]: p for p in ports["ethernet"]}
    assert list(eth) == ["eth1", "eth2", "eth3", "eth4"]
    assert (eth["eth1"]["admin_state"], eth["eth1"]["subscriber_id"], eth["eth1"]["security_profile"]) == (
        "enabled", "SUB-4001", "3")
    assert eth["eth2"]["admin_state"] == "disabled"
    assert ports["router"]["present"] is True and ports["router"]["default_to_rg"] is True
    assert ports["router"]["services"] == 1
    assert [(p["port"], p["admin_state"]) for p in ports["router"]["ports"]] == [("rg1", "enabled")]

    services = api.get(f"{BASE}/onts/{ont_id}").json()["services"]
    assert [(s["attachment"], s["ont_port"], s["vlan"]) for s in services] == [
        ("router", 1, 100), ("ethernet", 3, 300)]

    # Delivered twice: nothing is sent again, nothing is duplicated
    again = signup(api, ports=[
        {"port": 1, "admin_state": "enabled", "subscriber_id": "SUB-4001", "security_profile": "3"},
        {"port": 2, "admin_state": "disabled"}],
        services=[{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"},
                  {"vlan": 300, "bandwidth_profile": "RES-1G", "tag_action": "1", "ont_port": 3}])
    assert again["status"] == "succeeded" and again["result"]["ont_id"] == ont_id
    assert len(api.get(f"{BASE}/onts/{ont_id}").json()["services"]) == 2


def test_a_router_service_on_a_profile_without_a_router_is_refused_before_anything_is_created(api):
    job = signup(api, serial_number="CXNK00C4D302", profile="716GE-I",
                 services=[{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"}])
    assert job["status"] == "failed" and job["error"]["code"] == "cms_rejected"
    assert "has no router" in job["error"]["message"]
    backend = api.app.state.gateway.backend
    assert all(o.state.serial_number != "CXNK00C4D302" for o in backend._onts.values())


def test_a_rejected_port_setting_rolls_the_new_ont_back(api):
    job = signup(api, ports=[{"port": 1, "security_profile": "99"}])
    assert job["status"] == "failed" and "security profile 99" in job["error"]["message"]
    backend = api.app.state.gateway.backend
    assert all(o.state.serial_number != "CXNK00C4D301" for o in backend._onts.values())


def test_port_and_service_settings_are_validated_before_anything_is_queued(api):
    good = {"subscriber_id": "SUB-4001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
            "services": [{"vlan": 100, "bandwidth_profile": "RES-1G"}]}
    for change in ({"ports": [{"port": 9}]},
                   {"ports": [{"port": 1}, {"port": 1, "admin_state": "disabled"}]},
                   {"ports": [{"port": 1, "admin_state": "off"}]},
                   {"ports": [{"port": 1, "security_profile": "high"}]},
                   {"services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "attach_to": "wifi"}]}):
        assert api.post(f"{BASE}/signups", json={**good, **change}).status_code == 422, change
    for url, body in ((f"{BASE}/onts/3/ports/eth9/disable", None), (f"{BASE}/onts/3/ports/x1/disable", None),
                      (f"{BASE}/onts/3/ports/eth1/services/13/disable", None)):
        assert api.post(url, json=body).status_code == 422, url
    assert api.patch(f"{BASE}/onts/3/ports/eth1", json={}).status_code == 422
    assert api.patch(f"{BASE}/onts/3/ports/eth1/services/1", json={"speed": 9}).status_code == 422
    assert api.delete(f"{BASE}/onts/3").status_code == 422  # serial number is required
    assert api.get("/api/v2/jobs/").json() == []


def test_lookups(api):
    profiles = {p["id"]: p for p in api.get(f"{BASE}/ont-profiles").json()}
    assert profiles["844G-1"]["rg_ports"] == 1 and profiles["716GE-I"]["rg_ports"] == 0
    assert [(v["id"], v["name"]) for v in api.get(f"{BASE}/vlans").json()] == [
        (100, "data"), (200, "voice"), (300, "video")]
    assert api.get("/api/v2/networks/NTWK-NOPE/vlans").status_code == 404


def test_service_lifecycle_is_repeat_safe(api):
    url = f"{BASE}/onts/3/ports/eth1/services/1"

    first = finish(api, api.post(f"{url}/disable"))
    assert first["status"] == "succeeded" and first["result"]["changed"] is True
    again = finish(api, api.post(f"{url}/disable"))
    assert again["status"] == "succeeded" and again["result"]["changed"] is False
    assert api.get(f"{BASE}/onts/3").json()["services"][0]["admin_state"] == "disabled"
    assert finish(api, api.post(f"{url}/enable"))["result"]["applied"] == {"admin_state": "enabled"}

    changed = finish(api, api.patch(url, json={"bandwidth_profile": "VOICE", "vlan": 300, "tag_action": "1"}))
    assert changed["result"]["applied"] == {"bandwidth_profile": "VOICE", "vlan": 300}  # tag action already 1
    service = api.get(f"{BASE}/onts/3").json()["services"][0]
    assert (service["bandwidth_profile"], service["vlan"]) == ("VOICE", 300)

    bad_vlan = finish(api, api.patch(url, json={"vlan": 999}))
    assert bad_vlan["error"]["code"] == "cms_rejected" and "VLAN 999" in bad_vlan["error"]["message"]
    missing = finish(api, api.post(f"{BASE}/onts/3/ports/eth4/services/7/disable"))
    assert missing["error"]["code"] == "not_found"

    wrong_vlan = finish(api, api.delete(url, params={"expect_vlan": 100}))
    assert wrong_vlan["error"]["code"] == "conflict"
    deleted = finish(api, api.delete(url, params={"expect_vlan": 300}))
    assert deleted["result"]["deleted"] is True
    gone = finish(api, api.delete(url))
    assert gone["status"] == "succeeded" and gone["result"]["already_absent"] is True
    assert [s["ont_port"] for s in api.get(f"{BASE}/onts/3").json()["services"]] == [2]


def test_port_lifecycle_including_the_router_port(api):
    disabled = finish(api, api.post(f"{BASE}/onts/3/ports/eth2/disable"))
    assert disabled["status"] == "succeeded" and disabled["kind"] == "port-change"
    assert finish(api, api.post(f"{BASE}/onts/3/ports/eth2/disable"))["result"]["changed"] is False
    patched = finish(api, api.patch(f"{BASE}/onts/3/ports/eth2",
                                    json={"subscriber_id": "SUB-1003", "security_profile": "2"}))
    assert patched["result"]["applied"] == {"subscriber_id": "SUB-1003", "security_profile": "2"}

    assert finish(api, api.post(f"{BASE}/onts/3/ports/rg1/disable"))["status"] == "succeeded"
    ports = api.get(f"{BASE}/onts/3/ports").json()
    eth2 = next(p for p in ports["ethernet"] if p["port"] == "eth2")
    assert (eth2["admin_state"], eth2["subscriber_id"], eth2["security_profile"]) == ("disabled", "SUB-1003", "2")
    assert ports["router"]["ports"][0]["admin_state"] == "disabled"

    # The simulated CMS can also switch a port between bridge and router mode
    assert finish(api, api.patch(f"{BASE}/onts/3/ports/eth1", json={"mode": "router"}))["status"] == "succeeded"
    assert api.get(f"{BASE}/onts/3/ports").json()["ethernet"][0]["mode"] == "router"
    bridged = finish(api, api.patch(f"{BASE}/onts/1/ports/eth1", json={"mode": "router"}))
    assert "cannot be in router mode" in bridged["error"]["message"]

    # ONT 1 is a 716GE-I: no router, and only two Ethernet ports in the simulation
    no_router = finish(api, api.post(f"{BASE}/onts/1/ports/rg1/disable"))
    assert no_router["error"]["code"] == "cms_rejected" and "has no router" in no_router["error"]["message"]
    no_port = finish(api, api.post(f"{BASE}/onts/1/ports/eth4/disable"))
    assert no_port["error"]["code"] == "not_found"
    router_subscriber = finish(api, api.patch(f"{BASE}/onts/3/ports/rg1", json={"subscriber_id": "X"}))
    assert "Only the admin state" in router_subscriber["error"]["message"]


def test_ont_delete_checks_the_serial_and_the_services(api):
    url = f"{BASE}/onts/3"
    wrong = finish(api, api.delete(url, params={"serial_number": "CXNK00A3F104"}))
    assert wrong["error"]["code"] == "conflict" and "not CXNK00A3F104" in wrong["error"]["message"]
    busy = finish(api, api.delete(url, params={"serial_number": "CXNK00A3F103"}))
    assert busy["error"]["code"] == "conflict" and "2 service(s)" in busy["error"]["message"]
    assert api.get(url).status_code == 200

    deleted = finish(api, api.delete(url, params={"serial_number": "CXNK00A3F103", "force": "true"}))
    assert deleted["result"] == {"network": NET, "ont_id": 3, "op": "delete_ont", "deleted": True,
                                 "serial_number": "CXNK00A3F103", "services_removed": 2}
    assert api.get(url).status_code == 404
    again = finish(api, api.delete(url, params={"serial_number": "CXNK00A3F103", "force": "true"}))
    assert again["status"] == "succeeded" and again["result"]["already_absent"] is True

    # The ID is gone but the serial lives on elsewhere: refuse rather than report success
    elsewhere = finish(api, api.delete(url, params={"serial_number": "CXNK00A3F104"}))
    assert elsewhere["error"]["code"] == "conflict" and "is ONT 4" in elsewhere["error"]["message"]


# --- cms-nbi-client against the mock NBI server (real SOAP over HTTP) ---------------------

class Recorder:
    """Stands in for JobContext in direct handler calls"""
    job_id, attempt = "test", 1

    def __init__(self):
        self.events = []

    async def emit(self, type, message, /, **data):
        self.events.append((type, message))

    async def step(self, number, total, message):
        self.events.append(("step", message))


@pytest.fixture
def cms(nbi_server):
    pytest.importorskip("cmsnbiclient")
    from app.nbi.cms_client_backend import CmsNbiClientBackend

    server, port = nbi_server
    server._fresh_state()

    def connect(router=MOCK_RG_ADDRESSING):
        return CmsNbiClientBackend("127.0.0.1", port, "nbi-demo", "demo-password", timeout=3.0, router=router)

    return server, connect


def edits(server):
    return [r for r in server.app.state.requests if r["kind"] == "edit"]


def _offline_library_client():
    cmsnbiclient = pytest.importorskip("cmsnbiclient")
    client = cmsnbiclient.Client.__new__(cmsnbiclient.Client)
    client.session_id, client.cms_user_nm = "1", "x"
    client.cms_nbi_config = {"cms_netconf_uri": {"e7": "/cmsexc/ex/netconf"}}
    client.cms_netconf_url = "http://127.0.0.1:1/cmsexc/ex/netconf"
    return cmsnbiclient, client


def test_the_library_cannot_write_ethernet_ports_which_is_why_the_backend_sends_that_request_itself():
    """If this starts failing, a fixed cms-nbi-client is installed and the backend's own OntEthGe XML can go"""
    cmsnbiclient, client = _offline_library_client()
    with pytest.raises(KeyError, match="subscr-id"):
        cmsnbiclient.E7.Update(client, "NTWK-X", 1).ont_geth(ont_id="1", ontethge="1", admin_state="disabled")


def test_the_library_drops_profile_changes_on_a_service_which_is_why_the_backend_sends_that_request_itself(
        monkeypatch):
    """If this starts failing, a fixed cms-nbi-client is installed and the backend's own EthSvc XML can go"""
    cmsnbiclient, client = _offline_library_client()
    sent = []

    def capture(url, headers, data, timeout):
        sent.append(data)
        raise RuntimeError("captured")

    monkeypatch.setattr(cmsnbiclient.E7.update.requests, "post", capture)
    with pytest.raises(RuntimeError, match="captured"):
        cmsnbiclient.E7.Update(client, "NTWK-X", 1).ont_ethsvc(
            ont_id="5", ontslot="3", ontethany="1", ethsvc="1", bwprof_id="12", svctagaction_id="3", out_tag="200")
    assert "<out-tag>200</out-tag>" in sent[0]
    assert "bw-prof" not in sent[0] and "tag-action" not in sent[0]  # silently dropped


def test_ports_router_and_lookups_through_the_real_client(cms):
    server, connect = cms

    async def scenario():
        backend = connect()
        ports = await backend.get_ont_ports(NET, 3)
        assert [p.port for p in ports.ethernet] == ["eth1", "eth2", "eth3", "eth4"]
        assert (ports.router.present, ports.router.default_to_rg) == (True, True)
        assert [(p.port, p.admin_state) for p in ports.router.ports] == [("rg1", "enabled")]

        await backend.update_port(NET, 3, Attachment.ETHERNET, 2, PortChange(
            admin_state="disabled", subscriber_id="SUB-1003", security_profile="4",
            description='Bedroom <2> & "office"'))
        await backend.update_port(NET, 3, Attachment.ROUTER, 1, PortChange(admin_state="disabled"))
        ports = await backend.get_ont_ports(NET, 3)
        eth2 = ports.ethernet[1]
        assert (eth2.admin_state, eth2.subscriber_id, eth2.security_profile, eth2.description) == (
            "disabled", "SUB-1003", "4", 'Bedroom <2> & "office"')
        assert ports.router.ports[0].admin_state == "disabled"
        assert [(e["operation"], e["type"]) for e in edits(server)] == [
            ("merge", "OntEthGe"), ("merge", MOCK_RG_ADDRESSING.port_type)]

        # A 716GE-I in the simulation: two Ethernet ports, no router
        small = await backend.get_ont_ports(NET, 1)
        assert len(small.ethernet) == 2 and small.router.present is False and small.router.ports == []

        profiles = {p.id: p for p in await backend.list_ont_profiles(NET)}
        assert (profiles["844G-1"].ge_ports, profiles["844G-1"].rg_ports, profiles["844G-1"].default_to_rg) == (
            4, 1, True)
        assert [(v.id, v.name) for v in await backend.list_vlans(NET)] == [(100, "data"), (200, "voice"),
                                                                           (300, "video")]
        with pytest.raises(NbiRejectedError, match="security profile 77"):
            await backend.update_port(NET, 3, Attachment.ETHERNET, 1, PortChange(security_profile="77"))
        assert dict(backend.unrecognised) == {"Ont": set(), "EthSvc": set(), "OntProf": set(),
                                              "OntEthGe": set(), MOCK_RG_ADDRESSING.port_type: set(),
                                              "Vlan": set()}
        await backend.close()

    asyncio.run(scenario())


def test_services_on_both_sides_through_the_real_client(cms):
    server, connect = cms

    async def scenario():
        backend = connect()
        created = await backend.create_ont(NET, OntSpec(serial_number="CXNK00C4D301", profile="844G-1"))
        ont_id = created.ont_id
        await backend.add_ethernet_service(NET, ont_id, EthernetService(
            vlan=100, bandwidth_profile="RES-1G", tag_action="1", attach_to="router"))
        await backend.add_ethernet_service(NET, ont_id, EthernetService(
            vlan=100, bandwidth_profile="RES-1G", tag_action="1", ont_port=1))
        await backend.add_ethernet_service(NET, ont_id, EthernetService(
            vlan=300, bandwidth_profile="RES-1G", tag_action="1", ont_port=1))
        services = (await backend.get_ont(NET, ont_id)).services
        assert [(s.attachment, s.ont_slot, s.ont_port, s.service_index, s.vlan) for s in services] == [
            (Attachment.ROUTER, MOCK_RG_ADDRESSING.service_slot, 1, 1, 100),
            (Attachment.ETHERNET, "3", 1, 1, 100),
            (Attachment.ETHERNET, "3", 1, 2, 300)]  # numbered per side and port

        await backend.update_service(NET, ont_id, Attachment.ROUTER, 1, 1, ServiceChange(
            admin_state="disabled", bandwidth_profile="VOICE", vlan=200))
        await backend.delete_service(NET, ont_id, Attachment.ETHERNET, 1, 2)
        services = (await backend.get_ont(NET, ont_id)).services
        assert [(s.attachment, s.admin_state, s.bandwidth_profile, s.vlan) for s in services] == [
            (Attachment.ROUTER, "disabled", "VOICE", 200), (Attachment.ETHERNET, "enabled", "RES-1G", 100)]
        with pytest.raises(NbiNotFoundError):
            await backend.delete_service(NET, ont_id, Attachment.ETHERNET, 1, 2)
        await backend.close()

    asyncio.run(scenario())


def test_without_router_settings_nothing_router_side_is_sent_or_guessed(cms, monkeypatch):
    server, connect = cms
    assert router_side_addressing(settings) is None  # the shipped default: not configured

    async def scenario():
        verified, unverified = connect(), connect(router=None)
        await verified.add_ethernet_service(NET, 3, EthernetService(
            vlan=300, bandwidth_profile="RES-1G", tag_action="1", attach_to="router"))
        before = len(edits(server))

        with pytest.raises(NbiNotVerifiedError, match="not verified"):
            await unverified.add_ethernet_service(NET, 3, EthernetService(
                vlan=100, bandwidth_profile="RES-1G", tag_action="1", attach_to="router"))
        with pytest.raises(NbiNotVerifiedError):
            await unverified.update_port(NET, 3, Attachment.ROUTER, 1, PortChange(admin_state="disabled"))
        with pytest.raises(NbiNotVerifiedError):
            await unverified.delete_service(NET, 3, Attachment.ROUTER, 1, 1)
        with pytest.raises(NbiNotVerifiedError, match="mode through the CMS API is not known"):
            await verified.update_port(NET, 3, Attachment.ETHERNET, 1, PortChange(mode="router"))
        assert len(edits(server)) == before  # refused before anything was sent

        # Read back without the settings: the router-side service is "unknown", not guessed
        ont = await unverified.get_ont(NET, 3)
        odd = [s for s in ont.services if s.attachment == Attachment.UNKNOWN]
        assert [(s.ont_slot, s.vlan) for s in odd] == [(MOCK_RG_ADDRESSING.service_slot, 300)]
        ports = await unverified.get_ont_ports(NET, 3)
        assert ports.router.present is True and ports.router.ports == []
        assert "not verified" in ports.router.note and "do not recognise" not in ports.router.note
        assert "1 service(s) are in ONT slot(s) UNVERIFIED-RG" in ports.router.note
        assert unverified.slots_seen == {"3", MOCK_RG_ADDRESSING.service_slot}
        await verified.close()
        await unverified.close()

    asyncio.run(scenario())


def test_lifecycle_changes_through_the_real_client_are_repeat_safe(cms):
    server, connect = cms

    async def scenario():
        gateway = NbiGateway(connect(), timeout=5)

        async def apply(change, ont_id=3):
            return await apply_ont_change(Recorder(), gateway, NET, ont_id, change)

        first = await apply(UpdatePort(port="eth1", change=PortChange(admin_state="disabled", security_profile="5")))
        again = await apply(UpdatePort(port="eth1", change=PortChange(admin_state="disabled", security_profile="5")))
        assert (first["changed"], again["changed"]) == (True, False)

        first = await apply(UpdateService(port="eth2", service_index=1, change=ServiceChange(admin_state="disabled")))
        again = await apply(UpdateService(port="eth2", service_index=1, change=ServiceChange(admin_state="disabled")))
        assert (first["changed"], again["changed"]) == (True, False)

        assert (await apply(DeleteService(port="eth2", service_index=1)))["deleted"] is True
        assert (await apply(DeleteService(port="eth2", service_index=1)))["already_absent"] is True

        with pytest.raises(NbiConflictError, match="1 service"):
            await apply(DeleteOnt(serial_number="CXNK00A3F103"))
        assert (await apply(DeleteOnt(serial_number="CXNK00A3F103", force=True)))["deleted"] is True
        assert (await apply(DeleteOnt(serial_number="CXNK00A3F103", force=True)))["already_absent"] is True

        sent = [(e["operation"], e["type"]) for e in edits(server)]
        assert sent == [("merge", "OntEthGe"), ("merge", "EthSvc"), ("delete", "EthSvc"), ("delete", "Ont")]
        await gateway.close()

    asyncio.run(scenario())


def test_the_snapshot_leaves_out_onts_it_could_not_restore_correctly(api, caplog):
    from app.legacy.snapshot import take_snapshot

    signup(api, services=[{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"}])
    gateway = api.app.state.gateway
    snapshot = asyncio.run(take_snapshot(gateway, "OLT-MOCK", NET))
    assert 15 not in [o["ont_id"] for o in snapshot["onts"]] and len(snapshot["onts"]) == 14
    assert "ONT 15 left out of the snapshot" in caplog.text
