"""
Tests for findings from a 90-check run against the API in mock mode:

  - an unknown bandwidth profile is refused by the simulated CMS (in-process
    and over SOAP), as an unknown ONT profile or VLAN already was
  - every error has one shape, {"error": {"code", "message", "retryable"}},
    including validation errors and plain HTTP errors, not FastAPI's {"detail"}
  - snapshot compare takes a snapshot as the export gives it (YAML), as JSON,
    or a JSON export, and checks the body is a snapshot before anything else
"""

import asyncio
import json

import pytest
import yaml
from fastapi.testclient import TestClient

from app.models.provisioning_model import Attachment, EthernetService, OntSpec, ServiceChange
from app.nbi.cms_backend import CmsBackend
from app.nbi.errors import NbiRejectedError
from app.nbi.mock_cms import MOCK_BANDWIDTH_PROFILES, MOCK_NETWORK
from config import settings
from main import app

NET = MOCK_NETWORK
BASE = f"/api/v2/networks/{NET}"


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


def error_of(response, status, code):
    """The response is an error in the one shape every error uses"""
    assert response.status_code == status, response.text
    body = response.json()
    assert set(body) == {"error"}, body
    assert body["error"]["code"] == code and body["error"]["message"] and body["error"]["retryable"] is False, body
    return body["error"]


# --- Bandwidth profiles ------------------------------------------------------------------

def test_an_unknown_bandwidth_profile_is_refused_before_anything_is_queued(api, monkeypatch):
    backend = api.app.state.gateway.backend
    service = {"vlan": 100, "bandwidth_profile": "NOPE-BW", "tag_action": "1"}
    activation = error_of(api.post(f"{BASE}/activations", json={
        "serial_number": "CXNK00C4D303", "profile": "812G-1", "services": [service]}), 422, "cms_rejected")
    assert "Unknown bandwidth profile(s): NOPE-BW" in activation["message"]
    assert ", ".join(sorted(MOCK_BANDWIDTH_PROFILES)) in activation["message"]  # names the ones allowed
    signup = error_of(api.post(f"{BASE}/signups", json={
        "subscriber_id": "SUB-4001", "serial_number": "CXNK00C4D303", "profile": "812G-1", "services": [service]}),
        422, "cms_rejected")
    assert "NOPE-BW" in signup["message"]
    error_of(api.patch(f"{BASE}/onts/1/ports/eth1/services/1", json={"bandwidth_profile": "NOPE-BW"}),
             422, "cms_rejected")
    assert api.get("/api/v2/jobs/").json() == []  # nothing was queued
    assert all(o.state.serial_number != "CXNK00C4D303" for o in backend._onts.values())
    assert api.get(f"{BASE}/onts/1").json()["services"][0]["bandwidth_profile"] == "RES-1G"

    # BANDWIDTH_PROFILES replaces the simulated CMS's list: VOICE is no longer allowed
    monkeypatch.setattr(settings, "bandwidth_profiles", "RES-1G")
    narrowed = error_of(api.patch(f"{BASE}/onts/1/ports/eth1/services/1", json={"bandwidth_profile": "VOICE"}),
                        422, "cms_rejected")
    assert "Allowed (BANDWIDTH_PROFILES): RES-1G." in narrowed["message"]


def test_without_the_list_cms_refuses_it_mid_job_and_the_new_ont_is_rolled_back(api, monkeypatch):
    # With no list to check against, the profile reaches CMS, which rejects it after the ONT is
    # created. The ONT this run created is deleted again.
    monkeypatch.setattr(type(settings), "known_bandwidth_profiles", lambda self: None)
    backend = api.app.state.gateway.backend
    job = finish(api, api.post(f"{BASE}/activations", json={
        "serial_number": "CXNK00C4D303", "profile": "812G-1", "wait_for_online": False,
        "services": [{"vlan": 100, "bandwidth_profile": "NOPE-BW", "tag_action": "1"}]}))
    assert job["status"] == "failed" and job["error"]["code"] == "cms_rejected"
    assert "Bandwidth profile 'NOPE-BW' does not exist" in job["error"]["message"]
    assert all(o.state.serial_number != "CXNK00C4D303" for o in backend._onts.values())

    changed = finish(api, api.patch(f"{BASE}/onts/1/ports/eth1/services/1", json={"bandwidth_profile": "NOPE-BW"}))
    assert changed["status"] == "failed" and "NOPE-BW" in changed["error"]["message"]
    assert api.get(f"{BASE}/onts/1").json()["services"][0]["bandwidth_profile"] == "RES-1G"


def test_an_unknown_bandwidth_profile_is_refused_over_soap(nbi_server):
    server, port = nbi_server
    server._fresh_state()

    async def scenario():
        backend = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password")
        ont = await backend.create_ont(NET, OntSpec(serial_number="CXNK00C4D301", profile="844G-1"))
        with pytest.raises(NbiRejectedError, match="Bandwidth profile 'NOPE-BW' does not exist"):
            await backend.add_ethernet_service(NET, ont.ont_id, EthernetService(
                vlan=100, bandwidth_profile="NOPE-BW", tag_action="1"))
        with pytest.raises(NbiRejectedError, match="NOPE-BW"):
            await backend.update_service(NET, 1, Attachment.ETHERNET, 1, 1, ServiceChange(bandwidth_profile="NOPE-BW"))
        services = [(await backend.get_ont(NET, i)).services for i in (ont.ont_id, 1)]
        await backend.close()
        return services

    new, existing = asyncio.run(scenario())
    assert new == [] and existing[0].bandwidth_profile == "RES-1G"


# --- One error shape ---------------------------------------------------------------------

def test_validation_and_http_errors_use_the_same_shape_as_cms_errors(api):
    invalid = error_of(api.post(f"{BASE}/activations", json={"serial_number": "AB", "profile": "844G-1"}),
                       422, "invalid_request")
    assert "body.serial_number" in invalid["message"]
    assert invalid["details"][0]["loc"] == ["body", "serial_number"]  # FastAPI's own list, kept for detail

    malformed = api.post(f"{BASE}/onts/status-check", content=b"{bad", headers={"content-type": "application/json"})
    error_of(malformed, 422, "invalid_request")
    error_of(api.get(f"{BASE}/onts/abc"), 422, "invalid_request")

    assert "Job nope not found" in error_of(api.get("/api/v2/jobs/nope"), 404, "not_found")["message"]
    error_of(api.get("/api/v2/no-such-route"), 404, "not_found")
    error_of(api.put(f"{BASE}/vlans"), 405, "method_not_allowed")

    export = api.get(f"{BASE}/export").json()
    missing = api.post("/api/v2/smx/plans", json={"export": export, "subscriber_id": "NOBODY"})
    error_of(missing, 404, "not_found")
    # ...the same shape the CMS errors always had
    error_of(api.get(f"{BASE}/onts/9999"), 404, "not_found")


# --- Snapshot compare ----------------------------------------------------------------------

def test_snapshot_compare_takes_the_snapshot_as_exported_as_json_or_as_an_export(api):
    url = f"{BASE}/snapshot-compare"
    snapshot_yaml = api.get(f"{BASE}/export", params={"format": "snapshot"}).text

    as_yaml = api.post(url, content=snapshot_yaml.encode(), headers={"content-type": "application/yaml"})
    assert as_yaml.status_code == 200 and as_yaml.json()["same"] is True, as_yaml.text
    as_json = api.post(url, json=yaml.safe_load(snapshot_yaml))
    assert as_json.status_code == 200 and as_json.json()["same"] is True
    export = api.get(f"{BASE}/export").json()
    as_export = api.post(url, json=export)
    assert as_export.status_code == 200 and as_export.json()["same"] is True, as_export.text
    assert as_export.json()["onts_compared"] == as_yaml.json()["onts_compared"] == 14

    # A change made since is found whichever way the snapshot came in
    finish(api, api.patch(f"{BASE}/onts/1/ports/eth1/services/1", json={"vlan": 300}))
    for response in (api.post(url, content=snapshot_yaml.encode(), headers={"content-type": "text/yaml"}),
                     api.post(url, json=export)):
        changes = response.json()["onts_changed"][0]["services_changed"][0]["changes"]
        assert (response.json()["same"], changes) == (False, [{"field": "vlan", "saved": 100, "now": 300}])


def test_snapshot_compare_checks_the_body_is_a_snapshot_before_its_network(api):
    url = f"{BASE}/snapshot-compare"
    empty = error_of(api.post(url, json={}), 422, "invalid_request")
    assert "Not a usable snapshot" in empty["message"] and "None" not in empty["message"]
    error_of(api.post(url, json=[1, 2]), 422, "invalid_request")
    error_of(api.post(url, content=b"{not json", headers={"content-type": "application/json"}), 422,
             "invalid_request")
    error_of(api.post(url, content=b"onts: [unclosed", headers={"content-type": "application/yaml"}), 422,
             "invalid_request")
    error_of(api.post(url, json={"records": [{"nonsense": 1}], "network": NET}), 422, "invalid_request")

    other = api.get(f"{BASE}/export", params={"format": "snapshot"}).text.replace(NET, "NTWK-OTHER")
    wrong = error_of(api.post(url, content=other.encode(), headers={"content-type": "application/yaml"}), 422,
                     "invalid_request")
    assert wrong["message"] == f"This snapshot is of network NTWK-OTHER, not {NET}"
