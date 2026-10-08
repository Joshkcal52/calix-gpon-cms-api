"""
Deleting an ONT on the E7 path: through the API and in a rollback (the gateway passes force since PR #10), without
forced so a voice line the API cannot see stops it, and linked to its PON again when it is refused.
"""

import asyncio
import time

import pytest

from app.nbi.e7_backend import E7CliBackend
from app.nbi.e7_session import E7Node
from app.nbi.errors import NbiConflictError
from app.nbi.mock_e7 import NODE as NET

B = f"/api/v2/networks/{NET}"


@pytest.fixture
def api(monkeypatch):
    """The API with a mock E7, as python main.py --mock --backend e7-cli runs it. Yields (client, MockE7)."""
    from fastapi.testclient import TestClient
    from config import settings
    from main import app

    for name, value in (("mock_mode", True), ("nbi_backend", "e7-cli"), ("e7_nodes", None), ("mock_pon_size", 0),
                        ("kafka_bootstrap_servers", None), ("api_keys", None), ("bandwidth_profiles", None)):
        monkeypatch.setattr(settings, name, value)
    with TestClient(app) as client:
        yield client, app.state.mock_e7


def finished(client, response):
    assert response.status_code == 202, response.text
    job_id = response.json()["job"]["id"]
    deadline = time.monotonic() + 20
    while (job := client.get(f"/api/v2/jobs/{job_id}").json())["status"] not in ("succeeded", "failed"):
        assert time.monotonic() < deadline, "job did not finish"
        time.sleep(0.02)
    return job


def sign_up(client):
    job = finished(client, client.post(f"{B}/signups", json={
        "subscriber_id": "SUB-1", "serial_number": "CXNK00C0FFEE", "profile": "716GE-I",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "ADD-OUTER"}]}))
    assert job["status"] == "succeeded", job
    return job["result"]["ont_id"]


def delete(client, ont_id, **query):
    params = "&".join(f"{k}={v}" for k, v in {"serial_number": "CXNK00C0FFEE", **query}.items())
    return finished(client, client.delete(f"{B}/onts/{ont_id}?{params}"))


def test_deleting_an_ont_through_the_api_removes_its_services_then_the_ont_without_forced(api):
    client, e7 = api
    ont_id = sign_up(client)
    before = len(e7.commands)
    job = delete(client, ont_id, force="true", voice_checked="true")
    assert job["status"] == "succeeded", job
    assert client.get(f"{B}/onts/{ont_id}").status_code == 404
    writes = [c for c in e7.commands[before:] if not c.startswith(("show ", "set session"))]
    assert writes == [f"remove eth-svc Data1 from-ont-port {ont_id}/g1", f"set ont {ont_id} pon-port none",
                      f"delete ont {ont_id}"]


def test_a_voice_line_stops_the_delete_and_the_ont_is_linked_again(api):
    client, e7 = api
    ont_id = sign_up(client)
    e7.voice_lines[ont_id] = 1  # never shown to the API
    job = delete(client, ont_id, force="true", voice_checked="true")
    assert job["status"] == "failed"
    assert "may still be on it" in job["error"]["message"] and "linked to PON port 1/2/1 again" in job["error"]["message"]
    assert f"set ont {ont_id} pon-port 1/2/1" in e7.commands
    ont = client.get(f"{B}/onts/{ont_id}").json()
    assert ont["oper_state"] == "online" and e7.voice_lines[ont_id] == 1


def test_an_ont_with_voice_ports_needs_voice_checked(api):
    client, e7 = api
    ont_id = sign_up(client)
    before = len(e7.commands)
    job = delete(client, ont_id, force="true")
    assert job["status"] == "failed" and "voice_checked=true" in job["error"]["message"]
    assert [c for c in e7.commands[before:] if not c.startswith(("show ", "set session"))] == []


def test_a_rollback_removes_its_services_then_deletes_without_forced(api):
    client, e7 = api
    job = finished(client, client.post(f"{B}/signups", json={  # the second service is refused by the E7
        "subscriber_id": "SUB-1", "serial_number": "CXNK00C0FFEE", "profile": "716GE-I",
        "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "ADD-OUTER"},
                     {"vlan": 300, "bandwidth_profile": "RES-1G", "tag_action": "NOT-ON-THIS-E7"}]}))
    assert job["status"] == "failed"
    deletes = [c for c in e7.commands if c.startswith(("remove eth-svc", "delete ont"))]
    assert len(deletes) == 2 and deletes[0].startswith("remove eth-svc Data1")
    assert deletes[1].startswith("delete ont") and not deletes[1].endswith(" forced")
    assert all(o["serial"] != "00C0FFEE" for o in e7.onts.values())


class ScriptedSession:
    """Answers each command from a script, to reach a failure the mock E7 does not make"""

    def __init__(self, answers):
        self.answers, self.sent = answers, []

    async def run(self, command):
        self.sent.append(command)
        return next((out for start, out in self.answers if command.startswith(start)), "")

    async def close(self):
        pass


def test_a_failed_relink_says_the_ont_is_out_of_service():
    session = ScriptedSession([
        ("show ont 7 detail", "  ONT      : 7\n  PON Port : 1/1/1"),
        ("delete ont 7", "Error: ONT 7 has services; remove them first or use forced."),
        ("set ont 7 pon-port 1/1/1", "Error: Invalid input detected at '^' marker.")])
    backend = E7CliBackend([E7Node(NET, "127.0.0.1")], sessions={NET: session})
    with pytest.raises(NbiConflictError, match="UNLINKED from its PON"):
        asyncio.run(backend.delete_ont(NET, 7, force=False))
    assert session.sent == ["show ont 7 detail", "set ont 7 pon-port none", "delete ont 7", "set ont 7 pon-port 1/1/1"]
