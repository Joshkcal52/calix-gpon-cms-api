"""
Tests for services without an outer VLAN.

IDI's certified CMS R15.x docs make the outer VLAN optional ("Do not include
if the value is not required by Service Tag Action"), with a range of 2 to
4093. Which tag actions need one is set up on each CMS and is not known
here, so the simulation has its own clearly fake rule
(MOCK_TAG_ACTIONS_NO_VLAN): those two tag actions take no outer VLAN, every
other one needs it.

Two paths are covered:
  - the API with its queue and worker, against the in-process simulated CMS
  - the CMS client (app/nbi/client.py) speaking SOAP to the mock NBI server
"""

import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from app.controller.lifecycle_controller import apply_ont_change
from app.controller.provisioning_controller import activate_ont
from app.legacy import events as legacy_events
from app.legacy.export import CutoverExport, SERVICE_COLUMNS, from_services_csv, service_rows, to_csv
from app.legacy.smx import SmxMap, TagActionRule, plan_export
from app.legacy.snapshot import compare, snapshot_from_export
from app.models.lifecycle_model import UpdateService
from app.models.provisioning_model import (
    ActivationRequest, Attachment, EthernetService, OntSpec, ServiceChange, service_identity
)
from app.nbi.cms_backend import CmsBackend
from app.nbi.errors import NbiRejectedError
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK, MOCK_TAG_ACTIONS_NO_VLAN

from app.nbi.router_side import MOCK_RG_ADDRESSING
from config import settings
from main import app
from tests.test_cms_client import RECORDED, canonical, wire  # noqa: F401 (wire is a fixture)

NET = MOCK_NETWORK
BASE = f"/api/v2/networks/{NET}"
NO_VLAN, OTHER_NO_VLAN = MOCK_TAG_ACTIONS_NO_VLAN
SERIAL = "CXNK00C4D301"  # discovered on PON 1/1/1 in the simulation, an 844G-1 (four Ethernet ports)
ONT3 = {"serial_number": "CXNK00A3F103", "expect_vlan": 100}  # ONT 3's service 1 on eth1, as simulated
NO_VLAN_SERVICE = {"serial_number": SERIAL, "expect_vlan": "none"}  # the service a signup without a VLAN binds


# --- The identity rule on its own ---------------------------------------------------

def test_a_service_without_a_vlan_is_known_by_its_side_port_and_tag_action():
    eth = Attachment.ETHERNET
    assert service_identity(eth, 1, 100, "1") == service_identity(eth, 1, 100, "2")  # VLAN services: as before
    assert service_identity(eth, 1, None, "A") != service_identity(eth, 1, None, "B")
    assert service_identity(eth, 1, None, "A") != service_identity(eth, 2, None, "A")
    assert service_identity(eth, 1, None, "A") != service_identity(Attachment.ROUTER, 1, None, "A")
    # A service with a VLAN is never taken for one without, whatever the tag action
    assert service_identity(eth, 1, 100, "A") != service_identity(eth, 1, None, "A")


def test_a_signup_event_on_the_topic_is_checked_the_same_way():
    event = {"type": "GPON_SIGNUP", "event_id": "sonar-evt-0101", "network": NET, "subscriber_id": "SUB-5001",
             "serial_number": SERIAL, "profile": "844G-1"}
    command = legacy_events.decode(json.dumps({**event, "services": [
        {"bandwidth_profile": "RES-1G", "tag_action": NO_VLAN}]}).encode())
    assert command.payload["services"][0]["vlan"] is None
    for services in ([{"bandwidth_profile": "RES-1G", "tag_action": NO_VLAN}] * 2,
                     [{"bandwidth_profile": "RES-1G"}], [{"vlan": 4094, "bandwidth_profile": "RES-1G"}]):
        with pytest.raises(ValueError):
            legacy_events.decode(json.dumps({**event, "services": services}).encode())
    for vlan in (1, 4094):
        with pytest.raises(ValueError):
            ServiceChange(vlan=vlan)
    assert ServiceChange(vlan=2).vlan == 2 and ServiceChange(vlan=4093).vlan == 4093


# --- Through the API (in-process queue and worker, simulated CMS) ---------------------

@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "activation_poll_seconds", 0.01)
    monkeypatch.setattr(settings, "api_keys", None)
    with TestClient(app) as client:
        client.app.state.gateway.backend.latency = 0
        yield client


def finish(api, response):
    assert response.status_code == 202, response.text
    stream = api.get(response.json()["events_url"]).text
    return json.loads(stream.strip().split("\n\n")[-1].split("data: ", 1)[1])


def signup(api, services, **overrides):
    body = {"subscriber_id": "SUB-5001", "serial_number": SERIAL, "profile": "844G-1", "services": services}
    body.update(overrides)
    return finish(api, api.post(f"{BASE}/signups", json=body))


def untagged(port=1, **extra):
    return {"bandwidth_profile": "RES-1G", "tag_action": NO_VLAN, "ont_port": port, **extra}


def tagged(vlan, port=1, **extra):
    return {"vlan": vlan, "bandwidth_profile": "RES-1G", "tag_action": "1", "ont_port": port, **extra}


def services_of(api, ont_id):
    ont = api.get(f"{BASE}/onts/{ont_id}").json()
    return sorted((s["ont_port"], s["service_index"], s["vlan"], s["tag_action"]) for s in ont["services"])


def test_a_service_without_a_vlan_is_provisioned_and_a_repeat_adds_no_duplicate(api):
    first = signup(api, [untagged()])
    assert first["status"] == "succeeded", first
    ont_id = first["result"]["ont_id"]
    assert services_of(api, ont_id) == [(1, 1, None, NO_VLAN)]

    again = signup(api, [untagged()])  # the same signup run a second time
    assert again["status"] == "succeeded" and again["result"]["ont_id"] == ont_id
    assert services_of(api, ont_id) == [(1, 1, None, NO_VLAN)]
    assert "Binding 0 service(s)" in api.get(f"/api/v2/jobs/{again['id']}/events").text


def test_a_repeat_never_skips_a_wanted_service_that_only_looks_alike(api):
    ont_id = signup(api, [untagged()])["result"]["ont_id"]  # eth1: no VLAN

    # Same port but with a VLAN, same port with another tag action, and no VLAN on another port:
    # none of them is the existing service
    wanted = [untagged(), tagged(100), untagged(tag_action=OTHER_NO_VLAN), untagged(port=2)]
    second = signup(api, wanted)
    assert second["status"] == "succeeded", second
    expected = [(1, 1, None, NO_VLAN), (1, 2, 100, "1"), (1, 3, None, OTHER_NO_VLAN), (2, 1, None, NO_VLAN)]
    assert services_of(api, ont_id) == expected

    third = signup(api, wanted)  # and once more: nothing new
    assert third["status"] == "succeeded" and services_of(api, ont_id) == expected


def test_the_vlan_range_is_2_to_4093_and_a_service_without_one_needs_a_tag_action(api):
    url = f"{BASE}/signups"
    body = {"subscriber_id": "SUB-5001", "serial_number": SERIAL, "profile": "844G-1"}
    for services in ([tagged(1)], [tagged(4094)], [tagged(0)],
                     [{"bandwidth_profile": "RES-1G"}],  # no VLAN and no tag action
                     [untagged(), untagged()],  # the same service twice
                     [tagged(100), tagged(100, tag_action="2")]):  # same port and VLAN twice
        assert api.post(url, json={**body, "services": services}).status_code == 422, services
        assert api.post(f"{BASE}/activations", json={**body, "services": services}).status_code == 422, services
    assert api.get("/api/v2/jobs/").json() == []
    for vlan in (2, 4093):
        assert EthernetService(**tagged(vlan)).vlan == vlan
    for vlan in (1, 4094):
        assert api.patch(f"{BASE}/onts/3/ports/eth1/services/1", params=ONT3, json={"vlan": vlan}).status_code == 422
    assert api.patch(f"{BASE}/onts/3/ports/eth1/services/1", params=ONT3, json={"vlan": 4093}).status_code == 202


def test_the_simulated_tag_action_rule_is_enforced_and_a_failed_signup_is_undone(api):
    backend = api.app.state.gateway.backend

    needs_one = signup(api, [tagged(100), {"bandwidth_profile": "RES-1G", "tag_action": "1", "ont_port": 2}])
    assert needs_one["status"] == "failed" and needs_one["error"]["code"] == "cms_rejected"
    assert "needs an outer VLAN" in needs_one["error"]["message"]
    assert all(o.state.serial_number != SERIAL for o in backend._onts.values())  # rolled back

    refuses_one = signup(api, [untagged(vlan=100)])
    assert refuses_one["status"] == "failed" and "takes no outer VLAN" in refuses_one["error"]["message"]
    assert all(o.state.serial_number != SERIAL for o in backend._onts.values())


def test_lifecycle_changes_on_a_service_without_a_vlan(api):
    ont_id = signup(api, [untagged()])["result"]["ont_id"]
    url = f"{BASE}/onts/{ont_id}/ports/eth1/services/1"

    # expect_vlan=none says "the service without a VLAN": a change or delete reaches it, and only it
    changed = finish(api, api.patch(url, params=NO_VLAN_SERVICE, json={"bandwidth_profile": "VOICE"}))
    assert changed["result"]["applied"] == {"bandwidth_profile": "VOICE"}
    again = finish(api, api.patch(url, params=NO_VLAN_SERVICE, json={"bandwidth_profile": "VOICE"}))
    assert again["result"]["changed"] is False
    assert services_of(api, ont_id) == [(1, 1, None, NO_VLAN)]

    given_a_vlan = finish(api, api.patch(url, params=NO_VLAN_SERVICE, json={"vlan": 100}))
    assert given_a_vlan["status"] == "failed" and "takes no outer VLAN" in given_a_vlan["error"]["message"]

    wrong = finish(api, api.delete(url, params={**NO_VLAN_SERVICE, "expect_vlan": 100}))
    assert wrong["error"]["code"] == "conflict" and "no outer VLAN, not VLAN 100" in wrong["error"]["message"]
    assert finish(api, api.delete(url, params=NO_VLAN_SERVICE))["result"]["deleted"] is True


def test_export_snapshot_and_smx_plan_carry_a_service_without_a_vlan(api):
    ont_id = signup(api, [untagged(), tagged(100, port=2)])["result"]["ont_id"]
    export = CutoverExport.model_validate(api.get(f"{BASE}/export", params={"pon": "1/1/1"}).json())
    record = next(r for r in export.records if r.ont.legacy.ont_id == ont_id)
    assert [(s.legacy_port, s.vlan, s.tag_action) for s in record.data_services] == [
        ("eth1", None, NO_VLAN), ("eth2", 100, "1")]

    # The CSV has an empty VLAN cell, and reads back as no VLAN, not as 0 or a default
    rows = [r for r in service_rows(export) if r["legacy_ont_id"] == str(ont_id)]
    assert [r["service_vlan"] for r in rows] == ["", "100"]
    read_back = from_services_csv(to_csv(service_rows(export), SERVICE_COLUMNS))
    assert [s.vlan for s in next(r for r in read_back.records if r.ont.legacy.ont_id == ont_id).data_services] == [
        None, 100]

    snapshot = snapshot_from_export(export)
    saved = next(o for o in snapshot["onts"] if o["ont_id"] == ont_id)
    assert [s["vlan"] for s in saved["services"]] == [None, 100]
    assert compare(snapshot, snapshot).same

    one = CutoverExport(network=NET, exported_at=export.exported_at, records=[record])
    base = dict(ont_profiles={"844G-1": "P"}, bandwidth_profiles={"RES-1G": "POL"},
                ont_ports={f"eth{n}": f"P{n}" for n in range(1, 5)}, port_modes={"bridge": "B", "router": "R"},
                admin_states={"enabled": "UP"})
    # A tag action the mapping does not list: that service fails, naming it
    unmapped = plan_export(one, SmxMap(**base, tag_actions={"1": TagActionRule(vlan="outer")})).plans[0]
    assert not unmapped.ok and any(f"tag action '{NO_VLAN}' has no entry under tag_actions" in p
                                   for p in unmapped.problems)
    # The "outer" rule on a service with no outer VLAN still fails clearly
    outer = plan_export(one, SmxMap(**base, tag_actions={"1": TagActionRule(vlan="outer"),
                                                         NO_VLAN: TagActionRule(vlan="outer")})).plans[0]
    assert outer.problems == [f"service 1 on eth1: tag action '{NO_VLAN}' takes the outer VLAN, but the service "
                              f"has none on CMS"]
    # Only a fixed VLAN from the mapping file gives it one
    fixed = plan_export(one, SmxMap(**base, tag_actions={"1": TagActionRule(vlan="outer"),
                                                         NO_VLAN: TagActionRule(vlan=500)})).plans[0]
    assert fixed.ok and [r.parameters["VLAN"] for r in fixed.requests[2:]] == [500, 100]

    # The plan route with the shipped demo map: fails the subscriber, does not guess
    plan = api.get(f"{BASE}/subscribers/SUB-5001/smx-plan").json()
    assert plan["failed"] == 1 and any(NO_VLAN in p for p in plan["plans"][0]["problems"])


# --- Through the CMS client, over SOAP to the mock NBI server --------------------------

def test_no_out_tag_is_sent_without_a_vlan_and_the_rest_of_the_request_is_unchanged(wire):  # noqa: F811
    port, log = wire
    recorded = next(s for s in RECORDED["steps"] if s["step"] == "add_ethernet_service")["requests"][-1]["body"]

    async def scenario():
        backend = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", router=MOCK_RG_ADDRESSING)
        await backend.create_ont(NET, OntSpec(serial_number=SERIAL, profile="844G-1"))  # ONT 15, as recorded
        mark = len(log)
        # The recorded request's values, without the VLAN. Tag action 1 needs one in the simulation.
        with pytest.raises(NbiRejectedError, match="needs an outer VLAN"):
            await backend.add_ethernet_service(NET, 15, EthernetService(
                bandwidth_profile="RES-1G", tag_action="1", description="data & more"))
        sent = canonical(log[-1]["body"])
        assert len(log) - mark == 2  # read the services, then the create
        await backend.close()
        return sent

    sent = asyncio.run(scenario())
    assert "<out-tag>100</out-tag>" in recorded and "out-tag" not in sent.replace("in-tag", "")
    assert sent == recorded.replace("<out-tag>100</out-tag>", "")


def test_activation_without_a_vlan_over_soap_is_repeat_safe(nbi_server):
    server, port = nbi_server
    server._fresh_state()

    class Recorder:
        def __init__(self):
            self.events = []

        async def emit(self, kind, message, **data):
            self.events.append((kind, message))

        async def step(self, number, total, message):
            self.events.append(("step", message))

        async def record_ownership(self, message, /, **data):
            await self.emit("ownership", message, **data)

        async def ownership_records(self):
            return []

    async def scenario():
        gateway = NbiGateway(CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", timeout=3.0,
                                        router=MOCK_RG_ADDRESSING), timeout=5)
        request = ActivationRequest(serial_number=SERIAL, profile="844G-1", wait_for_online=False, services=[
            EthernetService(**untagged()), EthernetService(**tagged(100)),
            EthernetService(**untagged(tag_action=OTHER_NO_VLAN)), EthernetService(**untagged(port=2))])
        creates = lambda: [r for r in server.app.state.requests  # noqa: E731
                           if r["kind"] == "edit" and r["operation"] == "create" and r["type"] == "EthSvc"]

        first = await activate_ont(Recorder(), gateway, NET, request, verify_timeout=0, poll_seconds=0)
        assert len(creates()) == 4
        second = await activate_ont(Recorder(), gateway, NET, request, verify_timeout=0, poll_seconds=0)
        assert len(creates()) == 4 and second["ont_id"] == first["ont_id"]

        services = (await gateway.get_ont(NET, first["ont_id"])).services
        assert sorted((s.ont_port, s.service_index, s.vlan, s.tag_action) for s in services) == [
            (1, 1, None, NO_VLAN), (1, 2, 100, "1"), (1, 3, None, OTHER_NO_VLAN), (2, 1, None, NO_VLAN)]

        # A change to the service sends no out-tag and leaves it without a VLAN, and is repeat-safe
        change = UpdateService(port="eth1", service_index=1, serial_number=SERIAL, expect_vlan="none",
                               change=ServiceChange(bandwidth_profile="VOICE"))
        assert (await apply_ont_change(Recorder(), gateway, NET, first["ont_id"], change))["changed"] is True
        assert (await apply_ont_change(Recorder(), gateway, NET, first["ont_id"], change))["changed"] is False
        service = (await gateway.get_ont(NET, first["ont_id"])).service_at(Attachment.ETHERNET, 1, 1)
        assert (service.vlan, service.bandwidth_profile) == (None, "VOICE")

        # The simulation's rule holds over SOAP too, and the failed activation deletes the ONT it created
        await gateway.delete_ont(NET, first["ont_id"], force=True)
        bad = ActivationRequest(serial_number=SERIAL, profile="844G-1", wait_for_online=False,
                                require_discovery=False, services=[EthernetService(**untagged(vlan=100))])
        with pytest.raises(NbiRejectedError, match="takes no outer VLAN"):
            await activate_ont(Recorder(), gateway, NET, bad, verify_timeout=0, poll_seconds=0)
        assert await gateway.find_ont_by_serial(NET, SERIAL) is None
        await gateway.close()

    asyncio.run(scenario())
