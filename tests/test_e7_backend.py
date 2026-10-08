"""
Tests for the E7 CLI backend (app/nbi/e7_backend.py): the parsers, the
command lines it sends, and the whole activation path over real SSH to the
mock E7 (app/nbi/mock_e7.py).

What the mock E7 prints is invented (Calix publishes no sample output), so
these tests pin the commands, which are documented, and the backend's
behaviour around them. They do not show that a real E7's output parses.
"""

import asyncio
import threading

import pytest

pytest.importorskip("asyncssh")

from app.controller.provisioning_controller import activate_ont
from app.models.provisioning_model import (
    ActivationRequest, Attachment, EthernetService, OntSpec, PortChange, ServiceChange
)
from app.nbi import e7_parse as parse
from app.nbi import mock_e7
from app.nbi.e7_backend import E7CliBackend, arg, e7_serial, nodes_from_settings
from app.nbi.e7_session import E7Node
from app.nbi.errors import (
    NbiAuthError, NbiConflictError, NbiConnectionError, NbiNotFoundError, NbiNotVerifiedError, NbiRejectedError
)
from app.nbi.gateway import NbiGateway, build_gateway
from config import Settings

NET = mock_e7.NODE


@pytest.fixture
def e7():
    """A mock E7 over SSH in its own thread and event loop. Yields (port, MockE7)."""
    loop = asyncio.new_event_loop()
    started = threading.Event()
    box = {}

    def serve():
        asyncio.set_event_loop(loop)
        box["server"], box["port"], box["e7"] = loop.run_until_complete(mock_e7.serve())
        started.set()
        loop.run_forever()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert started.wait(10), "mock E7 did not start"
    yield box["port"], box["e7"]
    loop.call_soon_threadsafe(box["server"].close)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)


def backend_for(port, password="e7"):
    return E7CliBackend([E7Node(NET, "127.0.0.1", port, "e7", password, known_hosts="none")])


def sent(e7, since=0):
    """The provisioning commands the mock received, without the session setup and the reads"""
    return [c for c in e7.commands[since:] if not c.startswith(("show ", "set session "))]


# --- Parsers ------------------------------------------------------------------------

def test_labels_are_normalised_and_looked_up_through_aliases():
    assert parse.normalise("Serial Number:") == "serial-number"
    assert parse.field_name("Subscriber ID") == "subscriber-id"
    assert parse.field_name("Oper Status") == "oper-state"
    assert parse.field_name("Flux Capacitor") == "flux-capacitor"  # unknown labels are kept, normalised


def test_label_value_lines_in_three_spellings_and_several_blocks():
    text = "ONT: 101\nSerial Number = CXNK00A3F104\nAdmin State    enabled\n\nONT: 102\nSerial Number: CXNK00B2C201"
    assert parse.records(text) == [
        {"ont": "101", "serial-number": "CXNK00A3F104", "admin-state": "enabled"},
        {"ont": "102", "serial-number": "CXNK00B2C201"}]
    # Without a blank line, a repeated label starts the next block
    assert len(parse.records("ONT: 1\nModel: 844G\nONT: 2\nModel: 812G")) == 2


def test_tables_are_read_by_column_position_including_empty_and_right_aligned_cells():
    text = ("  ONT  Serial Number  Subscriber ID  Profile\n"
            "-----  -------------  -------------  -------\n"
            "  101  CXNK00A3F104                  844G-1\n"
            "10002  CXNK00B2C201   SUB-7          812G-1\n"
            "Total: 2")
    assert parse.table(text) == [
        {"ont": "101", "serial-number": "CXNK00A3F104", "profile": "844G-1"},
        {"ont": "10002", "serial-number": "CXNK00B2C201", "subscriber-id": "SUB-7", "profile": "812G-1"}]


def test_the_show_mcast_sample_in_calix_cli_reference_parses_by_its_dashed_line():
    """
    The one table Calix prints in the E7 CLI reference (#220-00652 Rev. 10, show mcast). The header and
    the dashed line are as printed: header words are one space apart within a column and between columns,
    so only the dashes say where a column starts. The PDF's text wraps the rows; they are laid out here
    under the dashes as the header and rule place them.
    """
    text = ("VLAN Multicast Group Interfaces, OLT Ports and ERPS Domains\n"
            "---- --------------- ---------------------------------------------------------\n"
            "25   224.0.0.5       traffic\n"
            "25   224.0.0.6       traffic\n"
            "25   225.1.1.1       12/g1, 13/1, traffic\n"
            "25   239.255.5.3     12/g1, 13/1, traffic\n")
    rows = parse.table(text)
    assert rows[0] == {"outer-vlan": "25", "multicast-group": "224.0.0.5",
                       "interfaces-olt-ports-and-erps-domains": "traffic"}
    assert [r["multicast-group"] for r in rows] == ["224.0.0.5", "224.0.0.6", "225.1.1.1", "239.255.5.3"]
    assert rows[3]["interfaces-olt-ports-and-erps-domains"] == "12/g1, 13/1, traffic"


def test_the_same_sample_with_unpadded_rows_is_read_word_by_word():
    """
    The E7 OS R3.1-3.2 Maintenance and Troubleshooting Guide prints the show mcast sample with its rows one
    space apart, not under the dashes. Whether the E7 or the typesetting did that is not known.
    """
    text = ("VLAN Multicast Group Interfaces, OLT Ports and ERPS Domains\n"
            "---- --------------- ---------------------------------------------------------\n"
            "25 224.0.0.5 traffic_domain\n"
            "25 225.1.1.1 12/g1, 13/1, traffic_domain\n")
    assert parse.table(text) == [
        {"outer-vlan": "25", "multicast-group": "224.0.0.5", "interfaces-olt-ports-and-erps-domains": "traffic_domain"},
        {"outer-vlan": "25", "multicast-group": "225.1.1.1",
         "interfaces-olt-ports-and-erps-domains": "12/g1, 13/1, traffic_domain"}]


def test_ont_profile_port_columns_keep_g_and_G_apart():
    """Calix's M&T guide: 'review the "G" (RG) and "F" (FB) Ports columns'. G is the RG, g is Gigabit Ethernet."""
    text = ("Name   g p G F\n"
            "------ - - - -\n"
            "844G-1 4 2 1 0\n")
    [row] = parse.table(text)
    assert parse.port_columns(row) == {"g": 4, "p": 2, "G": 1, "F": 0}
    assert parse.unrecognised([row]) == []


def test_without_a_dashed_line_header_words_two_spaces_apart_are_columns():
    assert parse.table("ONT  Serial Number\n101  CXNK00A3F104") == [{"ont": "101", "serial-number": "CXNK00A3F104"}]


def test_profile_port_letters_follow_calix_legend():
    # f=fast-eth, g=gig-eth, p=pots, G=residential gateway, F=full bridge
    assert parse.port_counts("4g 2p 1G") == {"g": 4, "p": 2, "G": 1}
    assert parse.port_counts("2g,1F") == {"g": 2, "F": 1}
    assert parse.port_counts("") == {}


def test_placeholder_values_read_as_empty():
    row = {"description": "-", "subscriber-id": "none", "ont": "12"}
    assert parse.value(row, "description") is None and parse.value(row, "subscriber-id") is None
    assert parse.number(row, "ont") == 12


# --- What reaches the command line --------------------------------------------------

def test_arguments_with_spaces_are_quoted_and_nothing_can_start_another_command():
    assert arg("Jones home") == '"Jones home"' and arg("RES-1G") == "RES-1G"
    for hostile in ('a"; delete ont 1 forced', "line\ndelete ont 1 forced", "a;b", "tab\there", ""):
        with pytest.raises(NbiRejectedError):
            arg(hostile)


def test_serial_numbers_go_to_the_e7_as_eight_hex_digits():
    assert e7_serial("CXNK00A3F104") == "00A3F104"
    assert e7_serial("cxnka3f104") == "00A3F104"


def test_e7_nodes_setting():
    nodes = nodes_from_settings(Settings(_env_file=None, e7_nodes="OLT-04=10.0.0.4, OLT 05=olt5.example:2022",
                                         e7_username="u", e7_password="p"))
    assert [(n.name, n.host, n.port, n.username) for n in nodes] == [
        ("OLT-04", "10.0.0.4", 22, "u"), ("OLT 05", "olt5.example", 2022, "u")]
    for bad in ("OLT-04", "OLT-04=", "=10.0.0.4", "A=h:x", "A=h,A=i"):
        with pytest.raises(ValueError):
            nodes_from_settings(Settings(_env_file=None, e7_nodes=bad))


def test_the_gateway_picks_the_e7_backend_and_asks_which_when_both_are_set():
    gateway = build_gateway(Settings(_env_file=None, mock_mode=False, e7_nodes="OLT-04=10.0.0.4"))
    assert gateway.backend.name == "e7-cli" and list(gateway.backend.nodes) == ["OLT-04"]
    both = dict(_env_file=None, mock_mode=False, e7_nodes="OLT-04=10.0.0.4", cms_host="cms")
    with pytest.raises(ValueError, match="NBI_BACKEND"):
        build_gateway(Settings(**both))
    # NBI_BACKEND (or python main.py --backend) chooses
    assert build_gateway(Settings(**both, nbi_backend="e7-cli")).backend.name == "e7-cli"
    assert build_gateway(Settings(**both, nbi_backend="cms")).backend.name != "e7-cli"
    with pytest.raises(ValueError, match="needs CMS_HOST"):
        build_gateway(Settings(_env_file=None, mock_mode=False, e7_nodes="OLT-04=10.0.0.4", nbi_backend="cms"))
    with pytest.raises(ValueError, match="needs E7_NODES"):
        build_gateway(Settings(_env_file=None, mock_mode=False, cms_host="cms", nbi_backend="e7-cli"))
    # MOCK_MODE alone is still the simulated CMS
    assert build_gateway(Settings(_env_file=None, mock_mode=True)).backend.name != "e7-cli"


def test_mock_mode_with_the_e7_backend_runs_the_api_against_a_mock_e7(monkeypatch):
    """MOCK_MODE=true NBI_BACKEND=e7-cli python main.py: the API starts a mock E7 itself"""
    from fastapi.testclient import TestClient
    from config import settings
    from main import app

    for name, value in (("mock_mode", True), ("nbi_backend", "e7-cli"), ("e7_nodes", None),
                        ("kafka_bootstrap_servers", None), ("api_keys", None)):
        monkeypatch.setattr(settings, name, value)
    with TestClient(app) as client:
        assert client.get("/api/v2/status").json()["cms_backend"] == "e7-cli"
        ont = client.get(f"/api/v2/networks/{NET}/onts/101").json()
        assert ont["serial_number"] == "CXNK00B2C201"


# --- Over SSH to the mock E7 ---------------------------------------------------------

def test_reads(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            discovered = await backend.list_discovered_onts(NET)
            assert {d.serial_number for d in discovered} == {"CXNK00A3F104", "CXNK00C0FFEE"}
            ont = await backend.get_ont(NET, 101)
            assert (ont.serial_number, ont.profile, ont.subscriber_id, ont.pon_port, ont.oper_state.value) == (
                "CXNK00B2C201", "812G-1", "SUB-1001", "1/1/2", "online")
            assert [(s.attachment, s.ont_port, s.vlan, s.bandwidth_profile) for s in ont.services] == [
                (Attachment.ETHERNET, 1, 100, "RES-1G")]
            profiles = {p.id: p for p in await backend.list_ont_profiles(NET)}
            assert (profiles["844G-1"].ge_ports, profiles["844G-1"].rg_ports, profiles["844G-1"].default_to_rg) == (
                4, 1, True)
            assert profiles["812G-1"].rg_ports == 0
            assert (await backend.get_ont_ports(NET, 101)).router.present is False
            assert [v.id for v in await backend.list_vlans(NET)] == [100, 200, 300]
            assert (await backend.find_ont_by_serial(NET, "CXNK00B2C201")).ont_id == 101
            assert await backend.find_ont_by_serial(NET, "CXNK00FFFFFF") is None
            assert [o.ont_id for o in await backend.find_onts_by_subscriber(NET, "SUB-1001")] == [101]
            with pytest.raises(NbiNotFoundError):
                await backend.get_ont(NET, 999)
            with pytest.raises(NbiNotFoundError, match="No E7 named"):
                await backend.get_ont("OLT-ELSEWHERE", 1)
            assert not any(backend.unrecognised.values())
        finally:
            await backend.close()
        # The session was set up once, with the pager and notifications off
        assert mock.commands[:5] == ["set session pager disabled", "set session timeout disabled",
                                     "set session alarm-notif disabled",
                                     "set session event-notif disabled", "set session tca-notif disabled"]
        assert sent(mock) == []

    asyncio.run(scenario())


def test_writes_send_the_documented_commands(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            ont = await backend.create_ont(NET, OntSpec(serial_number="CXNK00A3F104", profile="844G-1", ont_id=2001,
                                                        subscriber_id="SUB-2", description="Jones home"))
            assert ont.ont_id == 2001 and ont.oper_state.value == "online"
            await backend.add_ethernet_service(NET, 2001, EthernetService(
                vlan=100, bandwidth_profile="RES-1G", tag_action="ADD-OUTER", description="internet"))
            await backend.add_ethernet_service(NET, 2001, EthernetService(
                vlan=100, bandwidth_profile="RES-1G", tag_action="ADD-OUTER", ont_port=2))
            await backend.add_ethernet_service(NET, 2001, EthernetService(
                bandwidth_profile="RES-1G", tag_action="MOCK-NO-VLAN", attach_to="router"))
            await backend.update_service(NET, 2001, Attachment.ETHERNET, 1, 1, ServiceChange(
                bandwidth_profile="VOICE", admin_state="disabled"))
            await backend.update_port(NET, 2001, Attachment.ETHERNET, 2, PortChange(
                description="Office desk", admin_state="disabled"))
            await backend.delete_service(NET, 2001, Attachment.ETHERNET, 2, 1)
            services = (await backend.get_ont(NET, 2001)).services
            assert sorted((s.attachment.value, s.ont_port, s.bandwidth_profile, s.admin_state, s.description)
                          for s in services) == [
                ("ethernet", 1, "VOICE", "disabled", "internet"), ("router", 1, "RES-1G", "enabled", None)]
            ports = await backend.get_ont_ports(NET, 2001)
            assert [p.port for p in ports.ethernet] == ["eth1", "eth2", "eth3", "eth4"]
            assert ports.ethernet[1].description == "Office desk"
            assert ports.router.present is True and ports.router.services == 1
            await backend.delete_ont(NET, 2001)
            with pytest.raises(NbiNotFoundError):
                await backend.get_ont(NET, 2001)
        finally:
            await backend.close()
        assert sent(mock) == [
            'create ont 2001 profile 844G-1 serial-number 00A3F104 subscriber-id SUB-2 description "Jones home"',
            "add eth-svc Data1 to-ont-port 2001/g1 bw-profile RES-1G svc-tag-action ADD-OUTER outer-vlan 100 "
            "description internet",
            "add eth-svc Data1 to-ont-port 2001/g2 bw-profile RES-1G svc-tag-action ADD-OUTER outer-vlan 100",
            "add eth-svc Data1 to-ont-port 2001/G1 bw-profile RES-1G svc-tag-action MOCK-NO-VLAN",
            # A service change removes the service and adds it again, keeping what it does not change
            "remove eth-svc Data1 from-ont-port 2001/g1",
            "add eth-svc Data1 to-ont-port 2001/g1 bw-profile VOICE svc-tag-action ADD-OUTER outer-vlan 100 "
            "description internet admin-state disabled",
            'set ont-port 2001/g2 admin-state disabled description "Office desk"',
            "remove eth-svc Data1 from-ont-port 2001/g2",
            "set ont 2001 pon-port none",
            "delete ont 2001 forced"]

    asyncio.run(scenario())


def test_a_second_service_on_a_port_takes_the_next_name(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            await backend.add_ethernet_service(NET, 101, EthernetService(
                vlan=200, bandwidth_profile="VOICE", tag_action="ADD-OUTER"))
            services = (await backend.get_ont(NET, 101)).services
            assert [(s.service_index, s.vlan) for s in services] == [(1, 100), (2, 200)]
        finally:
            await backend.close()
        assert sent(mock)[-1].startswith("add eth-svc Data2 to-ont-port 101/g1 ")

    asyncio.run(scenario())


def test_no_alarms_is_refused_for_ports_and_services_before_anything_is_sent(e7):
    """Calix: set ont-port and add eth-svc take admin-state enabled or disabled; no-alarms is for the ONT"""
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            with pytest.raises(NbiRejectedError, match="port's admin state"):
                await backend.update_port(NET, 101, Attachment.ETHERNET, 1, PortChange(admin_state="enabled-no-alarms"))
            with pytest.raises(NbiRejectedError, match="service's admin state"):
                await backend.update_service(NET, 101, Attachment.ETHERNET, 1, 1,
                                             ServiceChange(admin_state="enabled-no-alarms"))
        finally:
            await backend.close()
        assert sent(mock) == []

    asyncio.run(scenario())


def test_a_service_change_is_safe_to_run_twice_and_a_refused_one_puts_the_service_back(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            change = ServiceChange(vlan=200)
            await backend.update_service(NET, 101, Attachment.ETHERNET, 1, 1, change)
            count = len(sent(mock))
            await backend.update_service(NET, 101, Attachment.ETHERNET, 1, 1, change)
            assert len(sent(mock)) == count == 2  # remove and add; the second run sent nothing

            with pytest.raises(NbiNotFoundError, match="Bandwidth profile"):
                await backend.update_service(NET, 101, Attachment.ETHERNET, 1, 1,
                                             ServiceChange(bandwidth_profile="NOPE"))
            [service] = (await backend.get_ont(NET, 101)).services
            assert (service.vlan, service.bandwidth_profile) == (200, "RES-1G")
        finally:
            await backend.close()
        assert sent(mock)[2:] == [
            "remove eth-svc Data1 from-ont-port 101/g1",
            "add eth-svc Data1 to-ont-port 101/g1 bw-profile NOPE svc-tag-action ADD-OUTER outer-vlan 200 "
            "admin-state enabled",
            "add eth-svc Data1 to-ont-port 101/g1 bw-profile RES-1G svc-tag-action ADD-OUTER outer-vlan 200 "
            "admin-state enabled"]

    asyncio.run(scenario())


def test_a_residential_gateway_port_takes_four_services(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            await backend.create_ont(NET, OntSpec(serial_number="CXNK00A3F104", profile="844G-1", ont_id=2002))
            for _ in range(4):
                await backend.add_ethernet_service(NET, 2002, EthernetService(
                    bandwidth_profile="RES-1G", tag_action="MOCK-NO-VLAN", attach_to="router"))
            count = len(sent(mock))
            with pytest.raises(NbiConflictError, match="at most 4"):
                await backend.add_ethernet_service(NET, 2002, EthernetService(
                    bandwidth_profile="RES-1G", tag_action="MOCK-NO-VLAN", attach_to="router"))
            assert len(sent(mock)) == count
        finally:
            await backend.close()

    asyncio.run(scenario())


def test_an_ethernet_port_takes_eight_services_whatever_e7_service_names_holds(e7):
    """Calix, System Support Capacities: 8 services on an ONT Ethernet port"""
    port, mock = e7

    async def scenario():
        backend = E7CliBackend([E7Node(NET, "127.0.0.1", port, "e7", "e7", known_hosts="none")],
                               service_names=[f"Data{n}" for n in range(1, 11)])
        try:
            for _ in range(7):  # ONT 101 already has one
                await backend.add_ethernet_service(NET, 101, EthernetService(
                    vlan=100, bandwidth_profile="RES-1G", tag_action="ADD-OUTER"))
            count = len(sent(mock))
            with pytest.raises(NbiConflictError, match="at most 8"):
                await backend.add_ethernet_service(NET, 101, EthernetService(
                    vlan=100, bandwidth_profile="RES-1G", tag_action="ADD-OUTER"))
            assert len(sent(mock)) == count
        finally:
            await backend.close()

    asyncio.run(scenario())


def test_ont_field_lengths_are_checked_before_sending(e7):
    """Calix, create ont: subscriber ID 1-63 characters, description 1-47"""
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            with pytest.raises(NbiRejectedError, match="at most 47"):
                await backend.create_ont(NET, OntSpec(serial_number="CXNK00C0FFEE", profile="812G-1", ont_id=8,
                                                      description="x" * 48))
            with pytest.raises(NbiRejectedError, match="at most 63"):
                await backend.create_ont(NET, OntSpec.model_construct(
                    serial_number="CXNK00C0FFEE", profile="812G-1", ont_id=8, subscriber_id="s" * 64,
                    description=None))
        finally:
            await backend.close()
        assert sent(mock) == []

    asyncio.run(scenario())


def test_refusals_become_typed_errors(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        try:
            with pytest.raises(NbiConflictError, match="already in use"):
                await backend.create_ont(NET, OntSpec(serial_number="CXNK00B2C201", profile="812G-1", ont_id=5))
            with pytest.raises(NbiConflictError, match="already exists"):
                await backend.create_ont(NET, OntSpec(serial_number="CXNK00C0FFEE", profile="812G-1", ont_id=101))
            with pytest.raises(NbiNotFoundError, match="profile"):
                await backend.create_ont(NET, OntSpec(serial_number="CXNK00C0FFEE", profile="NOPE", ont_id=6))
            with pytest.raises(NbiNotFoundError, match="Bandwidth profile"):
                await backend.add_ethernet_service(NET, 101, EthernetService(
                    vlan=100, bandwidth_profile="NOPE", tag_action="ADD-OUTER"))
            with pytest.raises(NbiRejectedError, match="tag_action"):
                await backend.add_ethernet_service(NET, 101, EthernetService(vlan=100, bandwidth_profile="RES-1G"))
            with pytest.raises(NbiNotVerifiedError):
                await backend.add_ethernet_service(NET, 101, EthernetService(
                    vlan=100, bandwidth_profile="RES-1G", tag_action="ADD-OUTER", us_pir_override="100m"))
            with pytest.raises(NbiNotVerifiedError):
                await backend.update_port(NET, 101, Attachment.ETHERNET, 1, PortChange(mode="router"))
            with pytest.raises(NbiNotFoundError, match="no service 3"):
                await backend.delete_service(NET, 101, Attachment.ETHERNET, 1, 3)
            with pytest.raises(NbiRejectedError):
                await backend.create_ont(NET, OntSpec(serial_number="CXNK00C0FFEE", profile="812G-1", ont_id=7,
                                                      description="two\nlines"))
        finally:
            await backend.close()
        # Only the commands the E7 itself refused were sent; the rest were stopped here
        assert len(sent(mock)) == 4

    asyncio.run(scenario())


def test_a_wrong_password_is_an_auth_error_and_nothing_listening_is_unreachable(e7):
    port, _ = e7

    async def scenario():
        with pytest.raises(NbiAuthError):
            await backend_for(port, password="wrong").get_ont(NET, 101)
        with pytest.raises(NbiConnectionError, match="Could not reach"):
            await backend_for(1).get_ont(NET, 101)

    asyncio.run(scenario())


def test_a_command_cut_short_drops_the_session_and_the_next_one_logs_in_again(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        session = backend.sessions[NET]
        try:
            await backend.get_ont(NET, 101)
            first = session._conn
            task = asyncio.ensure_future(session.run("show ont 101 detail"))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert session._conn is None  # not reused: its next output could belong to the abandoned command
            assert (await backend.get_ont(NET, 101)).ont_id == 101
            assert session._conn is not first
        finally:
            await backend.close()
        assert mock.commands.count("set session pager disabled") == 2

    asyncio.run(scenario())


def test_a_session_the_e7_closed_while_idle_logs_in_again_and_sends_the_command_once(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        mock.idle_timeout = 0.5  # read by the mock as each wait for a command starts
        try:
            await backend.get_ont(NET, 101)
            await asyncio.sleep(1.2)  # the mock closes the idle session ("Session timed out.")
            before = len(mock.commands)
            await backend.update_port(NET, 101, Attachment.ETHERNET, 1, PortChange(description="Den"))
        finally:
            mock.idle_timeout = None
            await backend.close()
        assert mock.logins == 2
        assert sent(mock, before) == ["set ont-port 101/g1 description Den"]

    asyncio.run(scenario())


def test_a_session_lost_after_a_command_was_sent_is_not_retried(e7):
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        mock.hang_up_on = "create ont"
        try:
            with pytest.raises(NbiConnectionError):
                await backend.create_ont(NET, OntSpec(serial_number="CXNK00C0FFEE", profile="812G-1", ont_id=9))
        finally:
            await backend.close()
        assert mock.logins == 1 and [c for c in mock.commands if c.startswith("create ont")] == [
            "create ont 9 profile 812G-1 serial-number 00C0FFEE"]

    asyncio.run(scenario())


def test_activation_through_the_gateway_is_safe_to_run_twice(e7):
    """The same activation code the API runs, over SSH to the mock E7"""
    port, mock = e7

    class Context:
        job_id, attempt = "test", 1

        async def emit(self, type, message, /, **data):
            pass

        async def step(self, number, total, message):
            pass

    request = ActivationRequest(serial_number="CXNK00A3F104", profile="844G-1", services=[
        EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="ADD-OUTER"),
        EthernetService(bandwidth_profile="RES-1G", tag_action="MOCK-NO-VLAN", attach_to="router")])

    async def scenario():
        gateway = NbiGateway(backend_for(port), timeout=15)
        try:
            first = await activate_ont(Context(), gateway, NET, request, verify_timeout=5, poll_seconds=0.05)
            count = len(sent(mock))
            second = await activate_ont(Context(), gateway, NET, request, verify_timeout=5, poll_seconds=0.05)
        finally:
            await gateway.close()
        assert first == second and first["online"] is True and first["services"] == 2
        assert len(sent(mock)) == count == 3  # create ont, two add eth-svc; the second run sent nothing

    asyncio.run(scenario())


def test_the_capture_tool_refuses_anything_but_show(e7):
    from app.tools.e7_capture import CommandBlocked, install_guard
    port, mock = e7

    async def scenario():
        backend = backend_for(port)
        install_guard(backend.sessions[NET], None)
        try:
            assert (await backend.get_ont(NET, 101)).ont_id == 101
            with pytest.raises(CommandBlocked):
                await backend.delete_ont(NET, 101)
        finally:
            await backend.close()
        assert 101 in mock.onts and sent(mock) == []

    asyncio.run(scenario())
