"""
Tests for the cutover tools: the export (CMS -> SMx-shaped JSON and CSV), the
SMx plan (dry run, nothing sent), and the read-only check for a real CMS.
"""

import asyncio
import csv
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.legacy.export import (
    FILL_IN_ONT, FILL_IN_SUBSCRIBER, SERVICE_COLUMNS, CutoverExport, export_network, from_services_csv,
    service_rows, to_csv
)
from app.legacy.smx import SmxMap, TagActionRule, plan_export
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK
from app.nbi.router_side import MOCK_RG_ADDRESSING
from config import settings
from main import app

REPO = Path(__file__).resolve().parents[1]
NET = MOCK_NETWORK
BASE = f"/api/v2/networks/{NET}"


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
    assert response.status_code == 202, response.text
    stream = api.get(response.json()["events_url"]).text
    return json.loads(stream.strip().split("\n\n")[-1].split("data: ", 1)[1])


def mapping(**overrides):
    fields = dict(ont_profiles={"716GE-I": "PROF-716", "812G-1": "PROF-812", "844G-1": "PROF-844"},
                  bandwidth_profiles={"RES-1G": "POLICY-1G", "VOICE": "POLICY-VOICE"},
                  tag_actions={"1": TagActionRule(vlan="outer"), "2": TagActionRule(vlan="outer")},
                  ont_ports={"eth1": "P1", "eth2": "P2", "eth3": "P3", "eth4": "P4", "rg1": "P-RG"},
                  port_modes={"bridge": "BRIDGE", "router": "ROUTER"},
                  admin_states={"enabled": "UP", "disabled": "DOWN"},
                  service_name="{account}-{port}-{index}")
    fields.update(overrides)
    return SmxMap(**fields)


def fill_in_everything(text):
    """What a person does in the spreadsheet: fill every fill_in_ column"""
    rows = list(csv.DictReader(io.StringIO(text)))
    for row in rows:
        for column in FILL_IN_SUBSCRIBER + FILL_IN_ONT:
            row[column] = f"{column[8:]}-{row['legacy_ont_id']}"
    return to_csv(rows, SERVICE_COLUMNS)


# --- Export ----------------------------------------------------------------------

def test_export_lays_out_cms_in_smx_terms_with_blanks_to_fill_in(api):
    export = api.get(f"{BASE}/export").json()
    assert export["network"] == NET and len(export["records"]) == 14
    assert export["to_fill_in"] == list(FILL_IN_SUBSCRIBER + FILL_IN_ONT)

    assert FILL_IN_ONT == ("fill_in_olt", "fill_in_pon_port", "fill_in_replacement_serial_number")
    record = next(r for r in export["records"] if r["ont"]["legacy"]["ont_id"] == 3)
    assert record["subscriber"] == {"account_number": "SUB-1003", **{k: None for k in FILL_IN_SUBSCRIBER}}
    assert {k: record["ont"][k] for k in FILL_IN_ONT} == {k: None for k in FILL_IN_ONT}
    assert record["ont"]["serial_number"] == "CXNK00A3F103"  # carried over: the ONT may be kept
    legacy = record["ont"]["legacy"]
    assert (legacy["serial_number"], legacy["profile"], legacy["pon_port"], legacy["router_present"]) == (
        "CXNK00A3F103", "844G-1", "1/1/1", True)
    assert [(s["legacy_port"], s["vlan"], s["bandwidth_profile"], s["tag_action"]) for s in record["data_services"]] == [
        ("eth1", 100, "RES-1G", "1"), ("eth2", 200, "VOICE", "2")]
    assert [p["port"] for p in record["ports"]] == ["eth1", "eth2", "eth3", "eth4", "rg1"]

    # The simulated 844Gs run every port as a bridge: flagged, with the reason
    assert legacy["port_modes"] == {"eth1": "bridge", "eth2": "bridge", "eth3": "bridge", "eth4": "bridge"}
    assert legacy["router_capable_running_as_bridge"] is True
    assert any("Keep it a bridge" in n for n in record["notes"])
    assert [s["port_mode"] for s in record["data_services"]] == ["bridge", "bridge"]
    one = next(r for r in export["records"] if r["ont"]["legacy"]["ont_id"] == 1)  # 716GE-I, no router
    assert one["ont"]["legacy"]["router_capable_running_as_bridge"] is False and one["notes"] == []

    card = api.get(f"{BASE}/export", params={"card": "1/1"}).json()
    assert card["card"] == "1/1" and len(card["records"]) == 14
    assert api.get(f"{BASE}/export", params={"card": "1/2"}).json()["records"] == []
    assert api.get(f"{BASE}/export", params={"card": "1/1", "pon": "1/1/1"}).status_code == 422

    pon = api.get(f"{BASE}/export", params={"pon": "1/1/2"}).json()
    assert {r["ont"]["legacy"]["pon_port"] for r in pon["records"]} == {"1/1/2"}
    assert api.get(f"{BASE}/export", params={"pon": "bad"}).status_code == 422


def test_export_csv_has_one_row_per_service_and_marks_the_fill_in_columns(api):
    response = api.get(f"{BASE}/export", params={"format": "csv"})
    assert response.headers["content-type"].startswith("text/csv")
    rows = list(csv.DictReader(io.StringIO(response.text)))
    services = sum(len(r["data_services"]) for r in api.get(f"{BASE}/export").json()["records"])
    assert len(rows) == services
    assert all(rows[0][c] == "" for c in FILL_IN_SUBSCRIBER + FILL_IN_ONT)
    assert [c for c in rows[0] if c.startswith("fill_in_")] == list(FILL_IN_SUBSCRIBER + FILL_IN_ONT)

    ports = list(csv.DictReader(io.StringIO(api.get(f"{BASE}/export", params={"format": "csv",
                                                                              "table": "ports"}).text)))
    assert {(p["legacy_ont_id"], p["port"]) for p in ports} >= {("3", "eth1"), ("3", "rg1"), ("1", "eth2")}


def test_an_ont_that_cannot_be_read_is_kept_with_a_note(api, monkeypatch):
    backend = api.app.state.gateway.backend
    original = backend.get_ont

    async def flaky(network, ont_id):
        if ont_id == 5:
            from app.nbi.errors import NbiRejectedError
            raise NbiRejectedError("CMS said no to ONT 5.")
        return await original(network, ont_id)

    monkeypatch.setattr(backend, "get_ont", flaky)
    export = api.get(f"{BASE}/export").json()
    five = next(r for r in export["records"] if r["ont"]["legacy"]["ont_id"] == 5)
    assert five["notes"] == ["The ONT could not be read: CMS said no to ONT 5."]
    assert len(export["records"]) == 14


def test_the_export_cli_writes_json_and_both_csvs(tmp_path):
    env = {**os.environ, "MOCK_MODE": "true"}
    run = subprocess.run([sys.executable, "-m", "app.legacy.export", "--network", NET, "--pon", "1/1/1",
                          "--output-dir", str(tmp_path)], cwd=REPO, env=env, capture_output=True, text=True,
                         timeout=120)
    assert run.returncode == 0, run.stderr
    assert "Exported 8 ONTs" in run.stderr
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == ["NTWK-MOCK-E7-pon-1-1-1.json", "NTWK-MOCK-E7-pon-1-1-1.ports.csv",
                     "NTWK-MOCK-E7-pon-1-1-1.services.csv"]
    export = CutoverExport.model_validate_json((tmp_path / names[0]).read_text())
    assert export.pon_port == "1/1/1" and len(export.records) == 8


# --- SMx plan (dry run) -------------------------------------------------------------

def test_a_filled_in_spreadsheet_becomes_ready_smx_requests(api):
    filled = fill_in_everything(api.get(f"{BASE}/export", params={"format": "csv", "pon": "1/1/1"}).text)
    plan = plan_export(from_services_csv(filled), mapping(), "SUB-1003")
    assert plan.dry_run is True and "Nothing was sent" in plan.note
    (one,) = plan.plans
    assert one.ok and one.ready and one.to_fill_in == []
    assert [r.action for r in one.requests] == ["Create/Update Subscriber", "Create/Update ONT",
                                                "Create/Update Data Service", "Create/Update Data Service"]
    assert one.ont == "replaced"  # a replacement serial was filled in
    ont = one.requests[1].parameters
    assert (ont["Serial Number"], ont["OLT"], ont["PON Port"], ont["ONT Profile ID"], ont["Subscriber ID"]) == (
        "replacement_serial_number-3", "olt-3", "pon_port-3", "PROF-844", "SUB-1003")
    assert ont["Port Modes"] == {"P1": "BRIDGE", "P2": "BRIDGE", "P3": "BRIDGE", "P4": "BRIDGE"}
    assert any("Keep it a bridge" in w for w in one.warnings)
    data, voice = (r.parameters for r in one.requests[2:])
    assert (data["Policy Map"], data["VLAN"], data["c-VLAN"], data["ONT Port ID"], data["Admin State"],
            data["Service Name"], data["Port Mode"]) == ("POLICY-1G", 100, None, "P1", "UP", "SUB-1003-eth1-1",
                                                         "BRIDGE")
    assert (voice["Policy Map"], voice["VLAN"], voice["ONT Port ID"]) == ("POLICY-VOICE", 200, "P2")


def test_an_unmapped_value_fails_only_that_subscriber_and_names_it(api):
    export = CutoverExport.model_validate(api.get(f"{BASE}/export").json())
    plan = plan_export(export, mapping(bandwidth_profiles={"RES-1G": "POLICY-1G"}))
    failed = {p.account_number: p for p in plan.plans if not p.ok}
    # Every third subscriber in the simulation also has VOICE on port 2
    assert sorted(failed) == ["SUB-1003", "SUB-1006", "SUB-1009", "SUB-1012"]
    assert failed["SUB-1003"].problems == [
        "service 1 on eth2: bandwidth profile 'VOICE' has no entry under bandwidth_profiles"]
    assert failed["SUB-1003"].requests == []
    assert plan.ok == 10 and plan.failed == 4 and plan.ready == 0  # nothing filled in yet

    plan = plan_export(export, mapping(admin_states={}, tag_actions={}, port_modes={"router": "R"}), "SUB-1001")
    assert plan.plans[0].problems == [
        "port mode 'bridge' has no entry under port_modes",
        "port mode 'bridge' has no entry under port_modes",
        "service 1 on eth1: admin state 'enabled' has no entry under admin_states",
        "service 1 on eth1: port mode 'bridge' has no entry under port_modes",
        "service 1 on eth1: tag action '1' has no entry under tag_actions"]


def test_the_existing_serial_is_kept_unless_a_replacement_is_filled_in(api):
    export = CutoverExport.model_validate(api.get(f"{BASE}/export").json())
    (kept,) = plan_export(export, mapping(), "SUB-1001").plans
    assert kept.ont == "kept" and kept.requests[1].parameters["Serial Number"] == "CXNK00A3F101"
    assert "fill_in_replacement_serial_number" not in kept.to_fill_in  # optional
    assert {"fill_in_olt", "fill_in_pon_port", "fill_in_first_name"} <= set(kept.to_fill_in)


def test_port_modes_are_carried_exactly_and_never_defaulted(api):
    finish(api, api.post(f"{BASE}/signups", json={
        "subscriber_id": "SUB-4001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
        "ports": [{"port": n, "mode": "router"} for n in (1, 2, 3)] + [{"port": 4, "mode": "bridge"}],
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"}]}))
    export = CutoverExport.model_validate(api.get(f"{BASE}/export").json())
    record = next(r for r in export.records if r.subscriber.account_number == "SUB-4001")
    assert record.ont.legacy.port_modes == {"eth1": "router", "eth2": "router", "eth3": "router", "eth4": "bridge"}
    assert record.ont.legacy.router_capable_running_as_bridge is False
    (plan,) = plan_export(export, mapping(), "SUB-4001").plans
    assert plan.requests[1].parameters["Port Modes"] == {"P1": "ROUTER", "P2": "ROUTER", "P3": "ROUTER",
                                                         "P4": "BRIDGE"}
    assert plan.requests[2].parameters["Port Mode"] == "ROUTER" and plan.warnings == [
        "CMS has no registration ID for this ONT; SMx asks for one."]

    record.ont.legacy.port_modes["eth4"] = "unknown"
    (failed,) = plan_export(export, mapping(), "SUB-4001").plans
    assert failed.problems == ["port eth4: its bridge/router mode could not be read from CMS; it is not defaulted"]
    record.ont.legacy.port_modes = {}
    (failed,) = plan_export(export, mapping(), "SUB-4001").plans
    assert "modes were not read" in failed.problems[0]


def test_vlans_come_from_the_tag_action_rule_and_are_never_guessed(api):
    record = CutoverExport.model_validate(api.get(f"{BASE}/export").json()).records[0]  # SUB-1001
    service = record.data_services[0]
    one = CutoverExport(network=NET, exported_at="2026-10-07T00:00:00Z", records=[record])

    fixed = plan_export(one, mapping(tag_actions={"1": TagActionRule(vlan=500, c_vlan=7)}))
    assert (fixed.plans[0].requests[2].parameters["VLAN"], fixed.plans[0].requests[2].parameters["c-VLAN"]) == (500, 7)

    no_inner = plan_export(one, mapping(tag_actions={"1": TagActionRule(vlan="outer", c_vlan="inner")}))
    assert "takes the inner VLAN, but the service has none" in no_inner.plans[0].problems[0]

    service.vlan = None
    no_outer = plan_export(one, mapping())
    assert "takes the outer VLAN, but the service has none" in no_outer.plans[0].problems[0]

    service.attachment, service.ont_slot, service.legacy_port = "unknown", "7", "slot7-port1"
    unknown = plan_export(one, mapping())
    assert "ONT slot '7', which this code does not recognise" in unknown.plans[0].problems[0]


def test_the_shipped_smx_map_loads_and_bad_ones_are_rejected(tmp_path):
    shipped = SmxMap.load(settings.smx_map_path)
    assert shipped.bandwidth_profiles["RES-1G"].startswith("DEMO-")
    for bad in ("tag_actions: {'1': {vlan: sometimes}}", "service_name: '{customer}'", "speed: fast",
                "tag_actions: {'1': {vlan: 5000}}"):
        path = tmp_path / "map.yml"
        path.write_text(bad)
        with pytest.raises(ValueError):
            SmxMap.load(path)


def test_plan_routes(api):
    export = api.get(f"{BASE}/export", params={"pon": "1/1/2"}).json()
    plan = api.post("/api/v2/smx/plans", json={"export": export}).json()
    assert plan["dry_run"] is True and plan["subscribers"] == len(export["records"]) and plan["failed"] == 0
    assert api.post("/api/v2/smx/plans", json={"export": export, "subscriber_id": "nobody"}).status_code == 404

    live = api.get(f"{BASE}/subscribers/SUB-1003/smx-plan").json()
    assert live["plans"][0]["ok"] is True and "fill_in_olt" in live["plans"][0]["to_fill_in"]
    assert api.get(f"{BASE}/subscribers/SUB-9999/smx-plan").status_code == 404
    api.app.state.gateway.backend._onts[2].state.subscriber_id = "SUB-1001"
    assert api.get(f"{BASE}/subscribers/SUB-1001/smx-plan").status_code == 409
    assert api.get(f"{BASE}/subscribers/SUB-1001/smx-plan", params={"ont_id": 2}).status_code == 200


def test_the_plan_cli_reads_a_filled_in_csv_and_sends_nothing(api, tmp_path):
    filled = tmp_path / "filled.csv"
    filled.write_text(fill_in_everything(api.get(f"{BASE}/export", params={"format": "csv"}).text))
    out = tmp_path / "plan.json"
    run = subprocess.run([sys.executable, "-m", "app.legacy.smx", "--input", str(filled), "--output", str(out)],
                         cwd=REPO, capture_output=True, text=True, timeout=60)
    assert run.returncode == 0, run.stderr
    assert "14 subscriber(s): 14 ready" in run.stderr and "Nothing was sent" in run.stderr
    assert json.loads(out.read_text())["ready"] == 14

    bad = tmp_path / "bad.csv"
    bad.write_text("account_number,whatever\n1,2\n")
    run = subprocess.run([sys.executable, "-m", "app.legacy.smx", "--input", str(bad)],
                         cwd=REPO, capture_output=True, text=True, timeout=60)
    assert run.returncode == 2 and "missing column" in run.stderr


# --- Through the real client: router-side services in an export --------------------

def test_router_side_services_export_only_as_far_as_they_are_known(nbi_server):
    pytest.importorskip("cmsnbiclient")
    from app.nbi.cms_client_backend import CmsNbiClientBackend
    from app.models.provisioning_model import EthernetService

    server, port = nbi_server
    server._fresh_state()

    async def scenario():
        verified = CmsNbiClientBackend("127.0.0.1", port, "nbi-demo", "demo-password", router=MOCK_RG_ADDRESSING)
        await verified.add_ethernet_service(NET, 4, EthernetService(
            vlan=300, bandwidth_profile="RES-1G", tag_action="1", attach_to="router"))
        known = await export_network(NbiGateway(verified), NET, "1/1/1")
        unverified = CmsNbiClientBackend("127.0.0.1", port, "nbi-demo", "demo-password")
        unknown = await export_network(NbiGateway(unverified), NET, "1/1/1")
        await verified.close()
        await unverified.close()
        return known, unknown

    known, unknown = asyncio.run(scenario())
    four = next(r for r in known.records if r.ont.legacy.ont_id == 4)
    assert [s.legacy_port for s in four.data_services] == ["eth1", "rg1"]
    assert four.ont.legacy.port_modes == {f"eth{n}": "bridge" for n in range(1, 5)}
    assert plan_export(known, mapping(), "SUB-1004").plans[0].ok is True

    four = next(r for r in unknown.records if r.ont.legacy.ont_id == 4)
    assert [s.legacy_port for s in four.data_services] == ["eth1", "slotUNVERIFIED-RG-port1"]
    assert any("ONT slot" in n for n in four.notes)
    # Without CMS_RG_PORT_MODE_ELEMENT the mode is unknown: not guessed from the 844G model
    assert four.ont.legacy.port_modes == {f"eth{n}": "unknown" for n in range(1, 5)}
    assert four.ont.legacy.router_capable_running_as_bridge is None
    rows = [r for r in service_rows(unknown) if r["legacy_ont_id"] == "4"]
    assert rows[1]["service_attachment"] == "unknown" and rows[1]["service_ont_slot"] == "UNVERIFIED-RG"
    failed = plan_export(unknown, mapping(), "SUB-1004").plans[0]
    assert not failed.ok and any("does not recognise" in p for p in failed.problems)


# --- Read-only check for a real CMS --------------------------------------------------------

def check(port, *extra, password="demo-password", env=None):
    env = {**os.environ, "CMS_HOST": "127.0.0.1", "CMS_PORT": str(port), "CMS_USERNAME": "nbi-demo",
           "CMS_PASSWORD": password, "MOCK_MODE": "false", **(env or {})}
    return subprocess.run([sys.executable, "-m", "app.tools.cms_check", "--network", NET, *extra],
                          cwd=REPO, env=env, capture_output=True, text=True, timeout=120)


def test_the_check_reads_and_reports_without_changing_anything(nbi_server, tmp_path):
    pytest.importorskip("cmsnbiclient")
    server, port = nbi_server
    server._fresh_state()

    run = check(port, "--ont-id", "3", "--dump", str(tmp_path))
    assert run.returncode == 0, run.stdout + run.stderr
    for line in ("[ok] login and list ONTs", "[ok] read ONT 3 with its services", "[ok] read ONT 3 ports and router side",
                 "[ok] list ONT profiles", "[ok] list VLANs", "[ok] log out", "Ont.op-stat: enable, sys-disable",
                 "Service ONT slots seen: 3", "every step worked. Nothing was changed."):
        assert line in run.stdout, line
    assert {r["kind"] for r in server.app.state.requests} == {"login", "action", "get", "logout"}
    assert server.app.state.sessions == set()  # logged out

    dumped = sorted(tmp_path.iterdir())
    assert len(dumped) >= 10
    login = next(p for p in dumped if p.name == "001.request.xml").read_text()
    assert "demo-password" not in login and "********" in login

    # Without CMS_RG_PORT_MODE_ELEMENT the element carrying the port mode is reported as unrecognised:
    # that is how it would be found on a real CMS
    report = json.loads(check(port, "--json", "--ont-id", "3").stdout)
    assert report["ok"] is True
    assert report["unrecognised_reply_fields"] == {"OntEthGe": [MOCK_RG_ADDRESSING.port_mode_element]}

    configured = json.loads(check(port, "--json", "--ont-id", "3",
                                  env={"CMS_RG_PORT_MODE_ELEMENT": MOCK_RG_ADDRESSING.port_mode_element}).stdout)
    assert configured["unrecognised_reply_fields"] == {}
    assert configured["raw_values_of_inferred_fields"][f"OntEthGe.{MOCK_RG_ADDRESSING.port_mode_element}"] == ["none"]
    ports = next(s for s in configured["steps"] if "ports" in s["step"])["detail"]["ethernet"]
    assert {p["mode"] for p in ports} == {"bridge"}


def test_the_check_reports_reply_fields_it_does_not_recognise(nbi_server, monkeypatch):
    pytest.importorskip("cmsnbiclient")
    server, port = nbi_server
    server._fresh_state()
    original = server._ont_xml
    monkeypatch.setattr(server, "_ont_xml", lambda ont: original(ont) + "<battery-state>ok</battery-state>")

    run = check(port)
    assert run.returncode == 0, run.stdout
    assert "Ont: battery-state" in run.stdout


def test_the_check_fails_clearly_and_still_changes_nothing(nbi_server):
    pytest.importorskip("cmsnbiclient")
    server, port = nbi_server
    server._fresh_state()

    run = check(port, password="wrong")
    assert run.returncode == 1
    assert "[FAILED] login and list ONTs" in run.stdout and "rejected this API's username or password" in run.stdout
    assert "SOME STEPS FAILED" in run.stdout

    missing = subprocess.run([sys.executable, "-m", "app.tools.cms_check", "--network", NET], cwd=REPO,
                             env={**os.environ, "CMS_HOST": ""}, capture_output=True, text=True, timeout=60)
    assert missing.returncode == 2 and "Set CMS_HOST" in missing.stderr


def test_the_check_guard_refuses_writes_even_if_code_tries_one(nbi_server):
    pytest.importorskip("cmsnbiclient")
    import requests
    from app.models.provisioning_model import Attachment, PortChange
    from app.nbi.cms_client_backend import CmsNbiClientBackend
    from app.tools.cms_check import WriteBlocked, install_guard

    server, port = nbi_server
    server._fresh_state()

    async def scenario():
        backend = CmsNbiClientBackend("127.0.0.1", port, "nbi-demo", "demo-password")
        remove = install_guard(requests, None)
        try:
            await backend.get_ont(NET, 1)  # reads pass
            with pytest.raises(WriteBlocked):
                await backend.update_port(NET, 1, Attachment.ETHERNET, 1, PortChange(admin_state="disabled"))
            with pytest.raises(WriteBlocked):
                await backend.delete_ont(NET, 1)
        finally:
            remove()
        await backend.close()

    asyncio.run(scenario())
    assert [r for r in server.app.state.requests if r["kind"] == "edit"] == []
