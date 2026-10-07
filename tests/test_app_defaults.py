"""
Tests for the app's defaults: the CORS policy (main.configure_cors) allows no
origin unless CORS_ALLOW_ORIGINS lists it, reload is off, and nothing is
called "rogue" any more.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from config import Settings, settings
from main import app, configure_cors, cors_origins

PREFLIGHT = {"Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "x-api-key"}


def test_by_default_no_origin_is_allowed(monkeypatch):
    assert Settings.model_fields["cors_allow_origins"].default == ""
    monkeypatch.setattr(settings, "mock_mode", True)
    monkeypatch.setattr(settings, "kafka_bootstrap_servers", None)
    monkeypatch.setattr(settings, "api_keys", None)
    with TestClient(app) as client:
        origin = {"Origin": "https://evil.example"}
        assert "access-control-allow-origin" not in client.get("/health", headers=origin).headers
        preflight = client.options("/api/v2/networks/NTWK-MOCK-E7/vlans", headers={**origin, **PREFLIGHT})
        assert "access-control-allow-origin" not in preflight.headers


def _app(origins: str) -> TestClient:
    probe = FastAPI()
    probe.get("/ping")(lambda: {"ok": True})
    configure_cors(probe, origins)
    return TestClient(probe)


def test_only_listed_origins_are_allowed_and_never_with_credentials():
    client = _app("https://portal.example.internal, https://noc.example.internal")
    allowed = client.options("/ping", headers={"Origin": "https://noc.example.internal", **PREFLIGHT})
    assert allowed.headers["access-control-allow-origin"] == "https://noc.example.internal"
    assert "access-control-allow-credentials" not in allowed.headers
    refused = client.options("/ping", headers={"Origin": "https://evil.example", **PREFLIGHT})
    assert "access-control-allow-origin" not in refused.headers
    assert "access-control-allow-origin" not in _app("").get("/ping", headers={"Origin": "https://x"}).headers


def test_a_wildcard_is_refused():
    assert cors_origins(" , ") == []
    with pytest.raises(ValueError, match="each origin"):
        cors_origins("https://portal.example.internal,*")


def test_defaults_are_safe_and_named_gpon_cms(monkeypatch):
    for name in ("RELOAD", "KAFKA_TOPIC_PREFIX", "API_TITLE", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    defaults = Settings(_env_file=None)
    assert defaults.reload is False
    assert defaults.kafka_topic_prefix == "gpon-cms"
    assert defaults.api_title == "gpon-cms" and "rogue" not in defaults.api_description.lower()
    with TestClient(app) as client:
        assert client.get("/").json()["message"].startswith("gpon-cms")
        assert client.get("/openapi.json").json()["info"]["title"] == "gpon-cms"
