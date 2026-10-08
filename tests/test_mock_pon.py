"""
The simulators filled to a full PON (app/nbi/mock_pon.py), and the mock E7's
fault switches the simulation driver (tools/sim_signups.py) uses.
"""

import asyncio
from collections import Counter

import pytest

from app.controller.provisioning_controller import activate_ont
from app.models.provisioning_model import ActivationRequest, Attachment, EthernetService, OntSpec, PortChange
from app.nbi import mock_e7
from app.nbi.e7_backend import E7CliBackend
from app.nbi.e7_session import E7Node
from app.nbi.errors import NbiConnectionError, NbiRejectedError
from app.nbi.gateway import NbiGateway, build_gateway
from app.nbi.mock_cms import MOCK_NETWORK, MOCK_SIM_PON, MockCmsBackend
from app.nbi.mock_e7 import MockE7
from config import Settings

NET = mock_e7.NODE


class Context:
    job_id, attempt = "test", 1

    async def emit(self, type, message, /, **data):
        pass

    async def step(self, number, total, message):
        pass

    async def record_ownership(self, message, /, **data):
        await self.emit("ownership", message, **data)

    async def ownership_records(self):
        return []


async def with_e7(e7, scenario):
    """Run scenario(backend) against e7 over SSH, in this event loop"""
    server, port, _ = await mock_e7.serve(0, e7=e7)
    backend = E7CliBackend([E7Node(NET, "127.0.0.1", port, "e7", "e7", known_hosts="none")])
    try:
        return await scenario(backend)
    finally:
        await backend.close()
        server.close()


# --- Defaults unchanged -------------------------------------------------------------------

def test_the_mocks_are_unchanged_without_a_pon_size():
    e7 = MockE7()
    assert set(e7.present) == {"00A3F104", "00B2C201", "00C0FFEE"}
    assert (e7.vlans, e7.bw_profiles) == ({100: "data", 200: "voice", 300: "video"}, ("RES-1G", "VOICE"))
    cms = MockCmsBackend(latency=0)
    assert len(asyncio.run(cms.list_discovered_onts(MOCK_NETWORK))) == 3
    assert [v.id for v in asyncio.run(cms.list_vlans(MOCK_NETWORK))] == [100, 200, 300]


@pytest.mark.parametrize("make", [lambda: MockE7(pon_size=65), lambda: MockCmsBackend(pon_size=65),
                                  lambda: MockE7(pon_size=-1)])
def test_a_pon_takes_at_most_64_onts(make):
    with pytest.raises(ValueError, match="1:64"):
        make()


# --- A full PON -----------------------------------------------------------------------------

def test_a_mock_e7_with_a_full_pon_lists_them_as_discovered_over_ssh():
    async def scenario(backend):
        return await backend.list_discovered_onts(NET), [v.id for v in await backend.list_vlans(NET)]

    discovered, vlans = asyncio.run(with_e7(MockE7(pon_size=64), scenario))
    on_pon = [d for d in discovered if d.pon_port == "1/1/1"]
    assert len(on_pon) == 64  # the existing 844G (00A3F104) and 63 new ones
    assert all(d.serial_number.startswith("CXNK00A") and len(d.serial_number) == 12 for d in on_pon)
    assert Counter(d.model for d in on_pon) == {"844G": 33, "812G": 16, "716GE-I": 15}
    assert vlans == [100, 200, 300, 400]


def test_a_mock_cms_with_a_full_pon():
    async def scenario():
        cms = MockCmsBackend(latency=0, pon_size=32)
        discovered = [d for d in await cms.list_discovered_onts(MOCK_NETWORK) if d.pon_port == MOCK_SIM_PON]
        ont = await cms.create_ont(MOCK_NETWORK, OntSpec(serial_number=discovered[0].serial_number,
                                                         profile=discovered[0].model))
        await cms.add_ethernet_service(MOCK_NETWORK, ont.ont_id, EthernetService(
            vlan=400, bandwidth_profile="RES-500M", tag_action="ADD-OUTER", ont_port=2))
        return discovered, await cms.get_ont(MOCK_NETWORK, ont.ont_id)

    discovered, ont = asyncio.run(scenario())
    assert len(discovered) == 32 and discovered[0].serial_number == "CXNK00A10001"
    assert {d.model for d in discovered} == {"844G-1", "812G-1", "716GE-I"}
    assert [(s.vlan, s.bandwidth_profile) for s in ont.services] == [(400, "RES-500M")]
    with pytest.raises(NbiRejectedError, match="VLAN 400"):  # not on the default network
        MockCmsBackend(latency=0)._check_tag_vlan(MOCK_NETWORK, "1", 400)


def test_mock_pon_size_reaches_both_mocks_through_settings(monkeypatch):
    backend = build_gateway(Settings(_env_file=None, mock_mode=True, mock_pon_size=32)).backend
    assert sum(d.pon_port == MOCK_SIM_PON for d in asyncio.run(backend.list_discovered_onts(MOCK_NETWORK))) == 32

    from fastapi.testclient import TestClient
    from config import settings
    from main import app
    for name, value in (("mock_mode", True), ("nbi_backend", "e7-cli"), ("e7_nodes", None), ("mock_pon_size", 32),
                        ("kafka_bootstrap_servers", None), ("api_keys", None)):
        monkeypatch.setattr(settings, name, value)
    with TestClient(app) as client:
        discovered = client.get(f"/api/v2/networks/{NET}/onts/discovered").json()
        assert sum(d["pon_port"] == "1/1/1" for d in discovered) == 32
        assert app.state.mock_e7 is not None


# --- Mock E7 behaviour ----------------------------------------------------------------------

def test_a_deleted_ont_takes_its_port_settings_with_it():
    e7 = MockE7()
    assert e7.run("create ont 7 profile 812G-1 serial-number 00C0FFEE") == ""
    assert e7.run("set ont-port 7/g1 admin-state disabled") == ""
    assert e7.run("set ont 7 pon-port none") == "" and e7.run("delete ont 7") == ""
    assert e7.run("create ont 7 profile 812G-1 serial-number 00C0FFEE") == ""
    assert "Admin State     : enabled" in e7.run("show ont-port 7 detail")


def test_a_closed_idle_session_logs_in_again():
    e7 = MockE7()

    async def scenario(backend):
        await backend.get_ont(NET, 101)
        e7.close_idle_sessions()
        await asyncio.sleep(0.2)
        await backend.update_port(NET, 101, Attachment.ETHERNET, 1, PortChange(description="Den"))

    asyncio.run(with_e7(e7, scenario))
    assert (e7.idle_closes, e7.logins) == (1, 2)
    assert e7.commands.count("set ont-port 101/g1 description Den") == 1


def test_a_lost_reply_to_add_eth_svc_does_not_duplicate_the_service_when_the_signup_runs_again():
    """The command ran on the E7 but the session dropped before it answered: the job fails as retryable,
    and its retry reads the service back instead of adding it a second time"""
    e7 = MockE7()
    e7.hang_up_on, e7.hang_up_once, e7.hang_up_after_run = "add eth-svc", True, True
    request = ActivationRequest(serial_number="CXNK00A3F104", profile="844G-1", services=[
        EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="ADD-OUTER", attach_to="router"),
        EthernetService(vlan=300, bandwidth_profile="RES-1G", tag_action="ADD-OUTER", attach_to="router")])

    async def scenario(backend):
        gateway = NbiGateway(backend, timeout=15)
        with pytest.raises(NbiConnectionError) as failed:
            await activate_ont(Context(), gateway, NET, request, verify_timeout=0, poll_seconds=0.05)
        assert failed.value.retryable
        return await activate_ont(Context(), gateway, NET, request, verify_timeout=0, poll_seconds=0.05)

    result = asyncio.run(with_e7(e7, scenario))
    assert result["services"] == 2 and e7.hang_up_on is None
    assert [s["outer"] for s in e7.services[result["ont_id"]]] == ["100", "300"]
    assert [c.split()[2] for c in e7.commands if c.startswith("add eth-svc")] == ["Data1", "Data2"]
