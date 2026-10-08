"""
Tests for the audit trail (app/audit.py): every change records who asked for
it, by API key name, on the job and in one JSON line when it is accepted and
another when it ends.
"""

import json
import logging

import pytest
from fastapi.testclient import TestClient

from app import audit
from app.legacy import events as legacy_events
from app.models.job_model import Command
from app.nbi.mock_cms import MOCK_NETWORK
from config import settings
from main import app

BASE = f"/api/v2/networks/{MOCK_NETWORK}"
BILLING, NOC = "billing-key-0123456789abcdef", "noc-key-0123456789abcdef"


def start(monkeypatch, keys):
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "activation_poll_seconds", 0.01)
    monkeypatch.setattr(settings, "api_keys", keys)
    client = TestClient(app)
    client.__enter__()
    client.app.state.gateway.backend.latency = 0
    return client


@pytest.fixture
def keyed(monkeypatch):
    client = start(monkeypatch, f"billing:{BILLING},noc:{NOC}")
    yield client
    client.__exit__(None, None, None)


@pytest.fixture
def open_api(monkeypatch):
    client = start(monkeypatch, None)
    yield client
    client.__exit__(None, None, None)


def finish(api, response, key=None):
    assert response.status_code == 202, response.text
    headers = {"X-API-Key": key} if key else {}
    stream = api.get(response.json()["events_url"], headers=headers).text
    return json.loads(stream.strip().split("\n\n")[-1].split("data: ", 1)[1])


def audit_lines(caplog):
    return [json.loads(r.getMessage()) for r in caplog.records if r.name == "gpon_cms.audit"]


def test_each_change_records_the_api_key_that_asked_for_it(keyed, caplog):
    caplog.set_level(logging.INFO, logger="gpon_cms.audit")
    activation = keyed.post(f"{BASE}/activations", headers={"X-API-Key": BILLING}, json={
        "serial_number": "CXNK00C4D302", "profile": "716GE-I", "wait_for_online": False,
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]})
    assert activation.json()["job"]["requested_by"] == "billing"
    done = finish(keyed, activation, BILLING)
    assert (done["status"], done["requested_by"]) == ("succeeded", "billing")

    change = keyed.post(f"{BASE}/onts/1/ports/eth1/disable", headers={"X-API-Key": NOC})
    assert finish(keyed, change, NOC)["requested_by"] == "noc"
    signup = keyed.post(f"{BASE}/signups", headers={"X-API-Key": BILLING}, json={
        "subscriber_id": "SUB-7001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]})
    assert finish(keyed, signup, BILLING)["requested_by"] == "billing"
    # A client cannot claim to be someone else
    assert keyed.post(f"{BASE}/signups", headers={"X-API-Key": NOC}, json={
        "subscriber_id": "SUB-7002", "serial_number": "CXNK00C4D303", "profile": "812G-1", "requested_by": "billing",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}).status_code == 422

    jobs = {j["id"]: j for j in keyed.get("/api/v2/jobs/", headers={"X-API-Key": NOC}).json()}
    assert jobs[activation.json()["job"]["id"]]["requested_by"] == "billing"
    lines = audit_lines(caplog)
    assert [(line["event"], line["requested_by"], line["kind"]) for line in lines] == [
        ("accepted", "billing", "activation"), ("succeeded", "billing", "activation"),
        ("accepted", "noc", "port-change"), ("succeeded", "noc", "port-change"),
        ("accepted", "billing", "signup"), ("succeeded", "billing", "signup")]
    assert lines[0]["network"] == MOCK_NETWORK and lines[0]["job_id"] == activation.json()["job"]["id"]
    assert lines[1]["result"]["ont_id"] == 15


def test_a_failed_change_is_audited_with_its_error(keyed, caplog):
    caplog.set_level(logging.INFO, logger="gpon_cms.audit")
    response = keyed.delete(f"{BASE}/onts/1", headers={"X-API-Key": NOC},
                            params={"serial_number": "CXNK00FFFFFF", "voice_checked": "true"})
    assert finish(keyed, response, NOC)["status"] == "failed"
    failed = audit_lines(caplog)[-1]
    assert (failed["event"], failed["requested_by"], failed["error_code"]) == ("failed", "noc", "conflict")


def test_mock_mode_without_keys_says_so_rather_than_naming_nobody(open_api):
    response = open_api.post(f"{BASE}/onts/1/ports/eth1/disable")
    assert finish(open_api, response)["requested_by"] == audit.ANONYMOUS


def test_events_from_the_topic_carry_what_their_producer_says():
    event = {"type": "GPON_SIGNUP", "event_id": "sonar-evt-0201", "network": MOCK_NETWORK,
             "subscriber_id": "SUB-7003", "serial_number": "CXNK00C4D301", "profile": "844G-1",
             "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}
    named = legacy_events.decode(json.dumps({**event, "requested_by": "sonar-bridge"}).encode())
    assert named.requested_by == "sonar-bridge"
    assert legacy_events.decode(legacy_events.encode(named)).requested_by == "sonar-bridge"  # survives the topic
    assert legacy_events.decode(json.dumps(event).encode()).requested_by == audit.TOPIC_UNNAMED
    command = Command(action="CHANGE_ONT", lane="provisioning", key="N", kind="port-change", summary="s",
                      payload={}, requested_by="noc")
    assert Command.model_validate_json(command.model_dump_json()).requested_by == "noc"  # and the command topic


def test_the_audit_trail_can_also_go_to_a_file(tmp_path):
    path = tmp_path / "audit.jsonl"
    try:
        audit.configure(str(path))
        audit.configure(str(path))  # twice is harmless
        command = Command(action="CHANGE_ONT", lane="provisioning", key=MOCK_NETWORK, kind="port-change",
                          summary="Change port eth1", payload={"network": MOCK_NETWORK}, requested_by="noc")
        audit.record("accepted", command)
        for handler in audit.AUDIT.handlers:
            handler.flush()
        lines = [json.loads(line) for line in path.read_text().splitlines()]
        assert len(lines) == 1 and (lines[0]["event"], lines[0]["requested_by"]) == ("accepted", "noc")
    finally:
        for handler in list(audit.AUDIT.handlers):
            if isinstance(handler, logging.FileHandler):
                audit.AUDIT.removeHandler(handler)
                handler.close()
        audit._configured_path = None
