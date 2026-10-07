"""
Voice is out of scope: only Ethernet data services are read from CMS. These
tests pin that the export and the SMx plan never let that look complete:
every ONT with voice (POTS) ports reads "not checked", every ONT whose count
could not be read reads "unknown", and only an ONT known to have none reads
"none".
"""

import asyncio
import csv
import io
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.legacy.export import SERVICE_COLUMNS, export_network, from_services_csv, service_rows, to_csv
from app.legacy.smx import plan_export
from app.nbi import mock_cms
from app.nbi.errors import NbiRejectedError
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK, MockCmsBackend
from config import settings
from main import app
from tests.test_cutover_tools import mapping

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


@pytest.fixture
def no_voice_on_716(monkeypatch):
    """Give the simulated 716GE-I profile no voice ports, so 'none' can be seen"""
    profile = mock_cms.MOCK_PROFILE_DETAILS["716GE-I"]
    monkeypatch.setitem(mock_cms.MOCK_PROFILE_DETAILS, "716GE-I", profile.model_copy(update={"pots_ports": 0}))


def by_ont(export):
    return {r.ont.legacy.ont_id: r for r in export.records}


def export(backend, **kwargs):
    return asyncio.run(export_network(NbiGateway(backend), NET, **kwargs))


def test_every_ont_with_voice_ports_is_flagged_not_checked(no_voice_on_716):
    records = by_ont(export(MockCmsBackend(latency=0)))
    assert len(records) == 14
    for record in records.values():
        if record.ont.legacy.profile == "716GE-I":
            assert (record.voice, record.ont.legacy.voice_ports) == ("none", 0)
        else:  # 844G-1 and 812G-1 have two voice ports each in the simulation
            assert (record.voice, record.ont.legacy.voice_ports) == ("not checked", 2), record.ont.legacy.ont_id


def test_a_voice_port_count_that_cannot_be_read_is_unknown_never_none(no_voice_on_716, monkeypatch):
    backend = MockCmsBackend(latency=0)
    get_ont, get_ports = backend.get_ont, backend.get_ont_ports

    async def flaky_ont(network, ont_id):
        if ont_id == 1:
            raise NbiRejectedError("CMS said no to ONT 1.")
        return await get_ont(network, ont_id)

    async def flaky_ports(network, ont_id):
        if ont_id == 2:
            raise NbiRejectedError("CMS said no to ONT 2's ports.")
        read = await get_ports(network, ont_id)
        if ont_id == 5:  # a profile without a pots attribute
            read.voice_ports = None
        return read

    monkeypatch.setattr(backend, "get_ont", flaky_ont)
    monkeypatch.setattr(backend, "get_ont_ports", flaky_ports)
    records = by_ont(export(backend))
    # All three are 716GE-Is, which have no voice ports here, but that could not be seen
    assert [records[n].voice for n in (1, 2, 5)] == ["unknown"] * 3
    assert records[8].voice == "none"  # a 716GE-I that was read
    assert {r.voice for r in by_ont(export(MockCmsBackend(latency=0), include_ports=False)).values()} == {"unknown"}


def test_the_csv_carries_voice_on_every_row_and_reads_it_back(no_voice_on_716):
    result = export(MockCmsBackend(latency=0))
    assert result.voice_not_checked == 7 and "Voice is not exported" in result.voice_scope
    text = to_csv(service_rows(result), SERVICE_COLUMNS)
    rows = list(csv.DictReader(io.StringIO(text)))
    assert list(rows[0])[:2] == ["account_number", "voice"]  # where a person reads first
    assert {(r["legacy_ont_id"], r["voice"], r["legacy_voice_ports"]) for r in rows if r["legacy_ont_id"] in ("1", "3", "4")} == {
        ("1", "none", "0"), ("3", "not checked", "2"), ("4", "not checked", "2")}
    back = by_ont(from_services_csv(text))
    assert {n: r.voice for n, r in back.items()} == {n: r.voice for n, r in by_ont(result).items()}

    # A sheet without the voice columns, or with them blanked, reads as unknown; a forged "none" is refused
    stripped = [{k: v for k, v in r.items() if k not in ("voice", "legacy_voice_ports")} for r in rows]
    old = to_csv(stripped, tuple(c for c in SERVICE_COLUMNS if c not in ("voice", "legacy_voice_ports")))
    assert {r.voice for r in from_services_csv(old).records} == {"unknown"}
    forged = [dict(r, voice="none") if r["legacy_ont_id"] == "3" else r for r in rows]
    with pytest.raises(ValueError, match="does not match legacy_voice_ports"):
        from_services_csv(to_csv(forged, SERVICE_COLUMNS))
    with pytest.raises(ValueError, match="is not one of"):
        from_services_csv(to_csv([dict(r, voice="done") for r in rows], SERVICE_COLUMNS))


def test_the_smx_plan_carries_voice_and_counts_what_was_not_checked(no_voice_on_716):
    plan = plan_export(export(MockCmsBackend(latency=0)), mapping())
    voice = {p.legacy_ont_id: p.voice for p in plan.plans}
    assert voice[1] == "none" and voice[3] == "not checked" and voice[6] == "not checked"
    assert plan.voice_not_checked == 7 and "Voice is not planned" in plan.voice_note
    # A plan with no data-service problems is still flagged for voice
    three = next(p for p in plan.plans if p.legacy_ont_id == 3)
    assert three.ok and three.voice == "not checked"


def test_the_routes_and_clis_show_voice(api, tmp_path):
    exported = api.get(f"{BASE}/export").json()
    assert exported["voice_not_checked"] == 14 and "Voice is not exported" in exported["voice_scope"]
    assert {r["voice"] for r in exported["records"]} == {"not checked"}
    rows = list(csv.DictReader(io.StringIO(api.get(f"{BASE}/export", params={"format": "csv"}).text)))
    assert {r["voice"] for r in rows} == {"not checked"}
    assert {r["voice"] for r in api.get(f"{BASE}/export", params={"ports": "false"}).json()["records"]} == {"unknown"}
    plan = api.get(f"{BASE}/subscribers/SUB-1003/smx-plan").json()
    assert plan["voice_not_checked"] == 1 and plan["plans"][0]["voice"] == "not checked"

    env = {**os.environ, "MOCK_MODE": "true", "CMS_HOST": ""}
    run = subprocess.run([sys.executable, "-m", "app.legacy.export", "--network", NET, "--pon", "1/1/2",
                          "--output-dir", str(tmp_path), "--name", "pon"], cwd=REPO, env=env,
                         capture_output=True, text=True, timeout=60)
    assert run.returncode == 0, run.stderr
    assert "Voice is NOT exported. 6 ONT(s)" in run.stderr
    planned = subprocess.run([sys.executable, "-m", "app.legacy.smx", "--input", str(tmp_path / "pon.services.csv"),
                              "--output", str(tmp_path / "plan.json")], cwd=REPO, env=env,
                             capture_output=True, text=True, timeout=60)
    assert "Voice is NOT planned. 6 subscriber(s)" in planned.stderr


def test_the_real_client_reads_the_voice_port_count_from_the_profile(nbi_server, monkeypatch, no_voice_on_716):
    from app.nbi.cms_backend import CmsBackend

    server, port = nbi_server
    server._fresh_state()
    # The mock server leaves <pots/> empty for a profile whose count is not known
    profile = mock_cms.MOCK_PROFILE_DETAILS["812G-1"]
    monkeypatch.setitem(mock_cms.MOCK_PROFILE_DETAILS, "812G-1", profile.model_copy(update={"pots_ports": None}))

    async def scenario():
        backend = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password")
        try:
            return await export_network(NbiGateway(backend), NET, "1/1/2"), backend.observed["OntProf.pots"]
        finally:
            await backend.close()

    result, observed = asyncio.run(scenario())
    records = by_ont(result)
    assert (records[9].voice, records[9].ont.legacy.voice_ports) == ("none", 0)  # 716GE-I
    assert (records[10].voice, records[10].ont.legacy.voice_ports) == ("not checked", 2)  # 844G-1
    assert (records[12].voice, records[12].ont.legacy.voice_ports) == ("unknown", None)  # 812G-1, empty <pots>
    assert observed == {"0", "2", "(empty)"}  # cms_check shows the raw values
