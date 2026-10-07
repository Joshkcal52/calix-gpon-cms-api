"""
Tests for snapshots and the read-only compare (app/legacy/snapshot.py).
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from app.legacy.snapshot import compare, compare_with_cms, take_snapshot
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
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "api_keys", None)
    with TestClient(app) as client:
        client.app.state.gateway.backend.latency = 0
        yield client


def test_a_snapshot_records_ports_modes_and_services_on_both_sides(api):
    snapshot = yaml.safe_load(api.get(f"{BASE}/export", params={"format": "snapshot", "pon": "1/1/1"}).text)
    assert (snapshot["format"], snapshot["network"], snapshot["pon_port"]) == (2, NET, "1/1/1")
    three = next(o for o in snapshot["onts"] if o["ont_id"] == 3)
    assert [(p["port"], p["mode"]) for p in three["ports"]][:2] == [("eth1", "bridge"), ("eth2", "bridge")]
    assert [(s["port"], s["service_index"], s["vlan"]) for s in three["services"]] == [("eth1", 1, 100),
                                                                                        ("eth2", 1, 200)]


def test_compare_finds_every_difference_and_changes_nothing(api):
    backend = api.app.state.gateway.backend
    saved = yaml.safe_load(api.get(f"{BASE}/export", params={"format": "snapshot", "pon": "1/1/1"}).text)
    same = api.post(f"{BASE}/snapshot-compare", json=saved).json()
    assert same["same"] is True and same["read_only"] is True and same["onts_compared"] == 8

    damage = backend.damage("1/1/1")  # deletes ONT 1, disables 2, moves 3 to VLAN 300, adds a stray service to 4
    backend._onts[5].ports[2]["mode"] = "router"
    before = {ont_id: o.state.model_copy(deep=True) for ont_id, o in backend._onts.items()}
    diff = api.post(f"{BASE}/snapshot-compare", json=saved).json()

    assert len(damage) == 4 and diff["same"] is False
    assert diff["onts_missing"] == [{"ont_id": 1, "serial_number": "CXNK00A3F101"}]
    changed = {o["ont_id"]: o for o in diff["onts_changed"]}
    assert changed[2]["changes"] == [{"field": "admin_state", "saved": "enabled", "now": "disabled"}]
    assert changed[3]["services_changed"] == [
        {"item": "eth1 service 1", "changes": [{"field": "vlan", "saved": 100, "now": 300}]}]
    assert changed[4]["services_new"] == ["eth1 service 9"]
    assert changed[5]["ports_changed"] == [
        {"item": "eth2", "changes": [{"field": "mode", "saved": "bridge", "now": "router"}]}]
    assert {ont_id: o.state for ont_id, o in backend._onts.items()} == before  # compare changed nothing

    other = dict(saved, network="NTWK-OTHER")
    assert api.post(f"{BASE}/snapshot-compare", json=other).status_code == 422
    assert api.post(f"{BASE}/snapshot-compare", json={"network": NET, "format": 9}).status_code == 422


def test_a_format_1_snapshot_from_the_old_rollback_can_still_be_compared():
    saved = {"format": 1, "network": NET, "captured_at": "2026-10-06T03:00:00+00:00", "onts": [
        {"ont_id": 7, "serial_number": "CXNK00A3F107", "profile": "716GE-I", "admin_state": "enabled",
         "subscriber_id": "SUB-1007", "description": "x", "pon_port": "1/1/1",
         "services": [{"ont_port": 1, "service_index": 1, "vlan": 100, "inner_vlan": None,
                       "bandwidth_profile": "RES-1G", "tag_action": "1", "admin_state": "enabled",
                       "description": None}]}]}
    current = {"format": 2, "network": NET, "captured_at": "now", "onts": [
        {"ont_id": 7, "serial_number": "CXNK00A3F107", "profile": "716GE-I", "admin_state": "enabled",
         "subscriber_id": "SUB-1007", "description": "x", "pon_port": "1/1/1", "registration_id": "R1",
         "ports": [{"port": "eth1", "mode": "bridge"}],
         "services": [{"port": "eth1", "service_index": 1, "attachment": "ethernet", "ont_slot": "3", "vlan": 100,
                       "inner_vlan": None, "bandwidth_profile": "RES-1G", "tag_action": "2",
                       "admin_state": "enabled", "description": None}]}]}
    result = compare(saved, current)
    # Fields format 1 never had (ports, registration ID, attachment) are not reported as changes
    assert [o.model_dump(include={"ont_id", "changes", "ports_new", "services_changed"})
            for o in result.onts_changed] == [{
                "ont_id": 7, "changes": [], "ports_new": [],
                "services_changed": [{"item": "eth1 service 1",
                                      "changes": [{"field": "tag_action", "saved": "1", "now": "2"}]}]}]


def test_compare_through_the_real_client_and_the_cli(nbi_server, tmp_path):
    from app.nbi.cms_backend import CmsBackend

    server, port = nbi_server
    server._fresh_state()

    async def snapshot():
        backend = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", router=MOCK_RG_ADDRESSING)
        gateway = NbiGateway(backend, timeout=10)
        taken = await take_snapshot(gateway, NET, "OLT-MOCK", card="1/1")
        result = await compare_with_cms(gateway, taken)
        await gateway.close()
        return taken, result

    taken, result = asyncio.run(snapshot())
    assert taken["card"] == "1/1" and len(taken["onts"]) == 14 and result.same is True
    path = tmp_path / "OLT-MOCK.yml"
    path.write_text(yaml.safe_dump(taken))

    server.app.state.cms.damage("1/1/2")
    edits_before = [r for r in server.app.state.requests if r["kind"] == "edit"]
    env = {**os.environ, "MOCK_MODE": "false", "CMS_HOST": "127.0.0.1", "CMS_PORT": str(port),
           "CMS_USERNAME": "nbi-demo", "CMS_PASSWORD": "demo-password",
           "CMS_RG_PORT_MODE_ELEMENT": MOCK_RG_ADDRESSING.port_mode_element}
    run = subprocess.run([sys.executable, "-m", "app.legacy.snapshot", "--compare", str(path)], cwd=REPO, env=env,
                         capture_output=True, text=True, timeout=120)
    assert run.returncode == 1, run.stdout + run.stderr
    assert "in the snapshot, gone from CMS" in run.stdout and "admin_state 'enabled' -> 'disabled'" in run.stdout
    assert "Read-only: nothing was changed." in run.stdout
    assert [r for r in server.app.state.requests if r["kind"] == "edit"] == edits_before == []

    bad = tmp_path / "bad.yml"
    bad.write_text("format: 7\nnetwork: X\n")
    run = subprocess.run([sys.executable, "-m", "app.legacy.snapshot", "--compare", str(bad)], cwd=REPO, env=env,
                         capture_output=True, text=True, timeout=120)
    assert run.returncode == 2 and "Error" in run.stderr
