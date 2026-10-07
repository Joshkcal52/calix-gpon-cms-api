"""
Tests for API keys on /api/v2 (app/auth.py).
"""

import json
import re

import pytest
from fastapi.testclient import TestClient

from app.auth import parse_keys
from app.nbi.mock_cms import MOCK_NETWORK
from config import settings
from main import app

PORTAL = "portal-key-0123456789abcdef"
NOC = "noc-key-fedcba9876543210"
PATH_VALUES = {"network": MOCK_NETWORK, "ont_id": "3", "port": "eth1", "service_index": "1", "job_id": "x",
               "subscriber_id": "SUB-1003", "name": "baseline_audit"}


@pytest.fixture
def keyed(monkeypatch):
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "activation_poll_seconds", 0.01)
    monkeypatch.setattr(settings, "api_keys", f"portal:{PORTAL},noc:{NOC}")
    with TestClient(app) as client:
        client.app.state.gateway.backend.latency = 0
        yield client


def v2_routes():
    for route in app.routes:
        if getattr(route, "path", "").startswith("/api/v2"):
            for method in sorted(route.methods - {"HEAD"}):
                yield method, re.sub(r"\{(\w+)\}", lambda m: PATH_VALUES[m.group(1)], route.path)


def test_every_v2_route_refuses_a_request_without_a_valid_key(keyed):
    routes = list(v2_routes())
    assert len(routes) >= 20  # all of them, not a sample
    for method, path in routes:
        for headers in ({}, {"X-API-Key": "wrong-key-0000000000"}, {"Authorization": f"Basic {PORTAL}"}):
            response = keyed.request(method, path, headers=headers, json={})
            assert response.status_code == 401, (method, path, headers, response.text)
            assert response.json()["error"]["code"] == "unauthorized"
            assert response.headers["WWW-Authenticate"] == "Bearer"
    assert keyed.app.state.jobs.list() == []  # nothing was queued


def test_a_valid_key_works_in_either_header_and_v1_and_health_stay_open(keyed):
    assert keyed.get(f"/api/v2/networks/{MOCK_NETWORK}/vlans", headers={"X-API-Key": PORTAL}).status_code == 200
    assert keyed.get(f"/api/v2/networks/{MOCK_NETWORK}/vlans",
                     headers={"Authorization": f"Bearer {NOC}"}).status_code == 200
    assert keyed.get("/health").status_code == 200
    assert keyed.get("/api/v1/status").status_code == 200


def test_a_queued_job_can_be_followed_with_the_key(keyed):
    headers = {"X-API-Key": PORTAL}
    response = keyed.post(f"/api/v2/networks/{MOCK_NETWORK}/onts/3/ports/eth2/disable", headers=headers)
    assert response.status_code == 202
    assert keyed.get(response.json()["events_url"]).status_code == 401
    stream = keyed.get(response.json()["events_url"], headers=headers).text
    job = json.loads(stream.strip().split("\n\n")[-1].split("data: ", 1)[1])
    assert job["status"] == "succeeded"


def test_without_keys_the_api_is_open_only_in_mock_mode(monkeypatch):
    monkeypatch.setattr(settings, "api_keys", None)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "mock_mode", True)
    with TestClient(app) as client:
        assert client.get(f"/api/v2/networks/{MOCK_NETWORK}/vlans").status_code == 200

    monkeypatch.setattr(settings, "mock_mode", False)
    monkeypatch.setattr(settings, "cms_host", None)
    with TestClient(app) as client:
        response = client.get(f"/api/v2/networks/{MOCK_NETWORK}/vlans")
        assert response.status_code == 503 and response.json()["error"]["code"] == "auth_not_configured"


def test_key_configuration_is_checked():
    assert parse_keys("portal:abcdefghijklmnop, qrstuvwxyz0123456") == {
        "portal": "abcdefghijklmnop", "key-2": "qrstuvwxyz0123456"}
    assert parse_keys(None) == {} and parse_keys(" , ") == {}
    with pytest.raises(ValueError, match="shorter than 16"):
        parse_keys("portal:short")
    with pytest.raises(ValueError, match="used twice"):
        parse_keys("a:abcdefghijklmnop,a:qrstuvwxyz012345")
