"""
Tests for the upstream and downstream peak-rate overrides on a service
(us-pir-override and ds-pir-override on CMS).

The element names are the ones cms-nbi-client always sent as "none". The
value format ("100m") is from IDI's CMS R15.x docs; accepting "g" as well is
inferred from the examples. The SMx plan carries an override only when the
mapping file says how: nothing is guessed.

Two paths are covered:
  - the API with its queue and worker, against the in-process simulated CMS
  - the CMS client (app/nbi/client.py) speaking SOAP to the mock NBI server
"""

import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.controller.lifecycle_controller import apply_ont_change
from app.legacy.export import CutoverExport, SERVICE_COLUMNS, export_ont, from_services_csv, service_rows, to_csv
from app.legacy.smx import PeakRateRule, SmxMap, TagActionRule, plan_export
from app.legacy.snapshot import compare, snapshot_from_export
from app.models.lifecycle_model import UpdateService
from app.models.provisioning_model import Attachment, EthernetService, OntSpec, ServiceChange
from app.nbi.cms_backend import CmsBackend
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK
from app.nbi.router_side import MOCK_RG_ADDRESSING
from config import settings
from main import app
from tests.test_cms_client import RECORDED, canonical, wire  # noqa: F401 (wire is a fixture)

NET = MOCK_NETWORK
BASE = f"/api/v2/networks/{NET}"
SERIAL = "CXNK00C4D301"
GOOD = ("none", "100m", "1g", "1m", "9999m")
BAD = ("100", "100M", "1G", "1t", "100k", "0m", "10000m", "fast", "", " 100m", "100 m")


@pytest.mark.parametrize("value", GOOD)
def test_good_rates_are_accepted(value):
    service = EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1", us_pir_override=value,
                              ds_pir_override=value)
    assert (service.us_pir_override, service.ds_pir_override) == (value, value)
    assert ServiceChange(us_pir_override=value).us_pir_override == value


@pytest.mark.parametrize("value", BAD)
def test_bad_rates_are_refused(value):
    for field in ("us_pir_override", "ds_pir_override"):
        with pytest.raises(ValidationError):
            EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1", **{field: value})
        with pytest.raises(ValidationError):
            ServiceChange(**{field: value})


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


def signup(api, **service):
    body = {"subscriber_id": "SUB-6001", "serial_number": SERIAL, "profile": "844G-1",
            "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", **service}]}
    return finish(api, api.post(f"{BASE}/signups", json=body))


def rates(api, ont_id):
    return [(s["us_pir_override"], s["ds_pir_override"]) for s in api.get(f"{BASE}/onts/{ont_id}").json()["services"]]


def test_bad_rates_are_refused_before_anything_is_queued(api):
    body = {"subscriber_id": "SUB-6001", "serial_number": SERIAL, "profile": "844G-1"}
    for value in ("100", "1G", "fast"):
        service = {"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "us_pir_override": value}
        assert api.post(f"{BASE}/signups", json={**body, "services": [service]}).status_code == 422
        assert api.patch(f"{BASE}/onts/3/ports/eth1/services/1", json={"ds_pir_override": value}).status_code == 422
    assert api.get("/api/v2/jobs/").json() == []


def test_rates_are_set_on_signup_read_back_and_changed_repeat_safely(api):
    job = signup(api, us_pir_override="100m", ds_pir_override="1g")
    assert job["status"] == "succeeded", job
    ont_id = job["result"]["ont_id"]
    assert rates(api, ont_id) == [("100m", "1g")]
    assert signup(api, us_pir_override="100m", ds_pir_override="1g")["status"] == "succeeded"  # a repeat
    assert rates(api, ont_id) == [("100m", "1g")]

    plain = finish(api, api.post(f"{BASE}/signups", json={  # no override given: "none", as always sent
        "subscriber_id": "SUB-6002", "serial_number": "CXNK00C4D302", "profile": "716GE-I",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}))
    assert rates(api, plain["result"]["ont_id"]) == [("none", "none")]

    url = f"{BASE}/onts/{ont_id}/ports/eth1/services/1"
    changed = finish(api, api.patch(url, json={"us_pir_override": "50m", "ds_pir_override": "1g"}))
    assert changed["result"]["applied"] == {"us_pir_override": "50m"}  # downstream already 1g
    assert finish(api, api.patch(url, json={"us_pir_override": "50m"}))["result"]["changed"] is False
    assert finish(api, api.patch(url, json={"ds_pir_override": "none"}))["result"]["changed"] is True
    assert rates(api, ont_id) == [("50m", "none")]


def mapping(**overrides):
    fields = dict(ont_profiles={"844G-1": "P"}, bandwidth_profiles={"RES-1G": "POL"},
                  tag_actions={"1": TagActionRule(vlan="outer")}, ont_ports={f"eth{n}": f"P{n}" for n in range(1, 5)},
                  port_modes={"bridge": "B", "router": "R"}, admin_states={"enabled": "UP"})
    fields.update(overrides)
    return SmxMap(**fields)


def test_export_snapshot_and_smx_plan_carry_the_rates_and_never_guess_them(api):
    ont_id = signup(api, us_pir_override="100m")["result"]["ont_id"]
    export = CutoverExport.model_validate(api.get(f"{BASE}/export", params={"pon": "1/1/1"}).json())
    record = next(r for r in export.records if r.ont.legacy.ont_id == ont_id)
    assert [(s.us_pir_override, s.ds_pir_override) for s in record.data_services] == [("100m", "none")]

    row = next(r for r in service_rows(export) if r["legacy_ont_id"] == str(ont_id))
    assert (row["service_us_pir_override"], row["service_ds_pir_override"]) == ("100m", "none")
    read_back = from_services_csv(to_csv(service_rows(export), SERVICE_COLUMNS))
    service = next(r for r in read_back.records if r.ont.legacy.ont_id == ont_id).data_services[0]
    assert (service.us_pir_override, service.ds_pir_override) == ("100m", "none")
    # A sheet from before these columns existed still reads, with the rates unknown
    old_columns = tuple(c for c in SERVICE_COLUMNS if "pir" not in c)
    older = from_services_csv(to_csv([{c: r[c] for c in old_columns} for r in service_rows(export)], old_columns))
    assert next(r for r in older.records if r.ont.legacy.ont_id == ont_id).data_services[0].us_pir_override is None

    # The snapshot holds them, a change shows up, and a snapshot without them compares as before
    snapshot = snapshot_from_export(export)
    assert next(o for o in snapshot["onts"] if o["ont_id"] == ont_id)["services"][0]["us_pir_override"] == "100m"
    finish(api, api.patch(f"{BASE}/onts/{ont_id}/ports/eth1/services/1", json={"us_pir_override": "200m"}))
    now = snapshot_from_export(CutoverExport.model_validate(api.get(f"{BASE}/export", params={"pon": "1/1/1"}).json()))
    changes = [c for o in compare(snapshot, now).onts_changed for s in o.services_changed for c in s.changes]
    assert [(c.field, c.saved, c.now) for c in changes] == [("us_pir_override", "100m", "200m")]
    for ont in snapshot["onts"]:
        for s in ont["services"]:
            s.pop("us_pir_override"), s.pop("ds_pir_override")
    assert compare(snapshot, now).onts_changed == []

    one = CutoverExport(network=NET, exported_at=export.exported_at, records=[record])
    unmapped = plan_export(one, mapping()).plans[0]
    assert not unmapped.ok and unmapped.problems == [
        "service 1 on eth1: upstream peak-rate override '100m' on CMS, and the mapping file has no "
        "peak_rate_overrides, so it cannot be carried over"]
    rule = PeakRateRule(upstream_parameter="SMX-UP", downstream_parameter="SMX-DOWN", values={"200m": "X"})
    unlisted = plan_export(one, mapping(peak_rate_overrides=rule)).plans[0]
    assert unlisted.problems == ["service 1 on eth1: upstream peak-rate override '100m' has no entry under "
                                 "peak_rate_overrides.values"]
    rule.values["100m"] = "SMX-100M"
    mapped = plan_export(one, mapping(peak_rate_overrides=rule)).plans[0]
    parameters = mapped.requests[2].parameters
    assert mapped.ok and parameters["SMX-UP"] == "SMX-100M" and "SMX-DOWN" not in parameters  # "none": nothing

    # The shipped map maps no rates, and services with none ("none") are unaffected
    assert SmxMap.load(settings.smx_map_path).peak_rate_overrides is None
    plan = api.get(f"{BASE}/subscribers/SUB-6001/smx-plan").json()
    assert plan["failed"] == 1 and "peak-rate override '200m'" in plan["plans"][0]["problems"][0]
    assert api.get(f"{BASE}/subscribers/SUB-1001/smx-plan").json()["failed"] == 0


# --- Through the CMS client, over SOAP to the mock NBI server --------------------------

def test_rates_go_where_none_went_and_nothing_else_in_the_request_changes(wire):  # noqa: F811
    port, log = wire
    recorded = next(s for s in RECORDED["steps"] if s["step"] == "add_ethernet_service")["requests"][-1]["body"]

    async def scenario():
        backend = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", router=MOCK_RG_ADDRESSING)
        await backend.create_ont(NET, OntSpec(serial_number=SERIAL, profile="844G-1"))  # ONT 15, as recorded
        await backend.add_ethernet_service(NET, 15, EthernetService(
            vlan=100, bandwidth_profile="RES-1G", tag_action="1", description="data & more",
            us_pir_override="100m", ds_pir_override="1g"))
        created = canonical(log[-1]["body"])
        service = (await backend.get_ont(NET, 15)).services[0]

        mark = len(log)
        await backend.update_service(NET, 15, Attachment.ETHERNET, 1, 1, ServiceChange(ds_pir_override="none"))
        merged = canonical(log[mark]["body"])
        after = (await backend.get_ont(NET, 15)).services[0]
        observed = {k: v for k, v in backend.observed.items() if "pir" in k}
        await backend.close()
        return created, service, merged, after, observed

    created, service, merged, after, observed = asyncio.run(scenario())
    assert created == recorded.replace("<us-pir-override>none</us-pir-override>",
                                       "<us-pir-override>100m</us-pir-override>").replace(
        "<ds-pir-override>none</ds-pir-override>", "<ds-pir-override>1g</ds-pir-override>")
    assert (service.us_pir_override, service.ds_pir_override) == ("100m", "1g")
    assert ('<object operation="merge"><type>EthSvc</type><id><ont>15</ont><ontslot>3</ontslot><ontethany>1'
            '</ontethany><ethsvc>1</ethsvc></id><ds-pir-override>none</ds-pir-override></object>') in merged
    assert (after.us_pir_override, after.ds_pir_override) == ("100m", "none")
    # Their read-back values are inferred, so cms_check lists what it saw
    assert observed["EthSvc.us-pir-override"] >= {"100m"} and "none" in observed["EthSvc.ds-pir-override"]


def test_rate_changes_over_soap_are_repeat_safe_and_exported(nbi_server):
    server, port = nbi_server
    server._fresh_state()

    class Recorder:
        async def emit(self, kind, message, **data):
            pass

        async def step(self, number, total, message):
            pass

    async def scenario():
        gateway = NbiGateway(CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", timeout=3.0,
                                        router=MOCK_RG_ADDRESSING), timeout=5)
        change = UpdateService(port="eth1", service_index=1, change=ServiceChange(us_pir_override="100m"))
        first = await apply_ont_change(Recorder(), gateway, NET, 3, change)
        again = await apply_ont_change(Recorder(), gateway, NET, 3, change)
        record = await export_ont(gateway, NET, 3)
        await gateway.close()
        return first, again, record

    first, again, record = asyncio.run(scenario())
    assert (first["changed"], again["changed"]) == (True, False)
    merges = [r for r in server.app.state.requests if r["kind"] == "edit"]
    assert merges == [{"kind": "edit", "operation": "merge", "type": "EthSvc"}]
    assert [(s.legacy_port, s.us_pir_override, s.ds_pir_override) for s in record.data_services] == [
        ("eth1", "100m", "none"), ("eth2", "none", "none")]
