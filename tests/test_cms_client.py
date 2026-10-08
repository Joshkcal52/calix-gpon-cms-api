"""
Tests for the CMS NBI client (app/nbi/client.py) and the backend on top of it
(app/nbi/cms_backend.py), over real SOAP to the mock NBI server.

The first test is the contract: every operation sends exactly the requests
cms-nbi-client 0.1.0 sent (recorded in fixtures/cms_nbi_client_requests.json
before the library was removed), apart from the random message and session
IDs. The rest cover what the library did not do: paging, HTTPS with
certificate checks, escaping the login, and characters outside ISO-8859-1.
"""

import asyncio
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from xml.etree import ElementTree

import httpx
import pytest
import uvicorn

from app.models.provisioning_model import Attachment, EthernetService, OntSpec, PortChange, ServiceChange
from app.nbi import mock_cms_server
from app.nbi.client import CmsClient, to_data
from app.nbi.cms_backend import CmsBackend
from app.nbi.errors import NbiAuthError, NbiConnectionError, NbiNotFoundError, NbiTimeoutError
from app.nbi.mock_cms import MOCK_NETWORK
from app.nbi.router_side import MOCK_RG_ADDRESSING
from config import Settings

REPO = Path(__file__).resolve().parents[1]
RECORDED = json.loads((Path(__file__).parent / "fixtures" / "cms_nbi_client_requests.json").read_text())
NET = MOCK_NETWORK


def _serve(app, **ssl):
    """Run an ASGI app on a free local port in a thread. Returns (server, port)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", **ssl))
    threading.Thread(target=server.run, daemon=True).start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    assert server.started
    return server, port


@pytest.fixture
def wire(nbi_server):
    """The mock NBI server behind a recorder of every HTTP request: yields (port, log)"""
    log = []

    async def recorder(scope, receive, send):
        if scope["type"] != "http":
            return await mock_cms_server.app(scope, receive, send)
        messages, body = [], b""
        while True:
            message = await receive()
            messages.append(message)
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        replay = iter(messages)
        log.append({"path": scope["path"], "body": body.decode("iso-8859-1"), "headers": {
            k.decode(): v.decode() for k, v in scope["headers"] if k.decode() in ("content-type", "user-agent")}})

        async def receive_again():
            return next(replay)
        await mock_cms_server.app(scope, receive_again, send)

    mock_cms_server._fresh_state()
    server, port = _serve(recorder)
    yield port, log
    server.should_exit = True


def canonical(body: str) -> str:
    """A request as the fixture holds it: IDs replaced, no whitespace between elements"""
    body = re.sub(r'message-id="\d+"', 'message-id="N"', body)
    body = re.sub(r'sessionid="\d+"', 'sessionid="S"', body)
    body = re.sub(r"<SessionId>\d+</SessionId>", "<SessionId>S</SessionId>", body)
    root = ElementTree.fromstring(re.sub(r"<\?xml[^>]*\?>", "", body).strip())
    for node in root.iter():
        if node.text is not None and not node.text.strip():
            node.text = None
        if node.tail is not None and not node.tail.strip():
            node.tail = None
    return ElementTree.canonicalize(ElementTree.tostring(root, encoding="unicode"))


def as_this_project_sends(recorded: str) -> str:
    """
    The library's request with this project's one deliberate difference: <serno> in lowercase
    (app/nbi/serno.py). The fixture keeps what the library sent.
    """
    return re.sub(r"<serno>([^<]*)</serno>", lambda m: f"<serno>{m.group(1).lower()}</serno>", recorded)


def test_every_operation_sends_the_requests_cms_nbi_client_sent(wire):
    port, log = wire

    async def scenario():
        b = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", router=MOCK_RG_ADDRESSING)
        steps = {
            "list_discovered_onts": lambda: b.list_discovered_onts(NET),
            "get_ont": lambda: b.get_ont(NET, 3),
            "get_ont missing": lambda: b.get_ont(NET, 999),
            "find_ont_by_serial": lambda: b.find_ont_by_serial(NET, "CXNK00A3F104"),
            "find_ont_by_serial none": lambda: b.find_ont_by_serial(NET, "CXNK00FFFFFF"),
            "find_onts_by_subscriber": lambda: b.find_onts_by_subscriber(NET, "SUB-1003 & <x>"),
            "list_onts": lambda: b.list_onts(NET),
            "create_ont": lambda: b.create_ont(NET, OntSpec(
                serial_number="CXNK00C4D301", profile="844G-1", subscriber_id="SUB-3001",
                description='9 Elm "Rd" & <co>')),
            "create_ont dup": lambda: b.create_ont(NET, OntSpec(serial_number="CXNK00C4D301", profile="844G-1")),
            "add_ethernet_service": lambda: b.add_ethernet_service(NET, 15, EthernetService(
                vlan=100, bandwidth_profile="RES-1G", tag_action="1", description="data & more")),
            "add_router_service": lambda: b.add_ethernet_service(NET, 15, EthernetService(
                vlan=300, bandwidth_profile="RES-1G", tag_action="1", attach_to="router")),
            "add_service bad vlan": lambda: b.add_ethernet_service(NET, 15, EthernetService(
                vlan=999, bandwidth_profile="RES-1G", tag_action="1")),
            "update_service": lambda: b.update_service(NET, 15, Attachment.ETHERNET, 1, 1, ServiceChange(
                admin_state="disabled", bandwidth_profile="VOICE", tag_action="2", vlan=200)),
            "get_ont_ports": lambda: b.get_ont_ports(NET, 15),
            "update_port eth": lambda: b.update_port(NET, 15, Attachment.ETHERNET, 2, PortChange.model_construct(
                admin_state="disabled", description="x<y", security_profile="3", subscriber_id="S&1")),
            "update_port rg": lambda: b.update_port(NET, 15, Attachment.ROUTER, 1, PortChange(admin_state="disabled")),
            "list_ont_profiles": lambda: b.list_ont_profiles(NET),
            "list_vlans": lambda: b.list_vlans(NET),
            "delete_service": lambda: b.delete_service(NET, 15, Attachment.ETHERNET, 1, 1),
            "delete_ont": lambda: b.delete_ont(NET, 15, force=True),
            "session re-login": lambda: b.get_ont(NET, 1),
            "network unknown": lambda: b.get_ont("NTWK-NOPE", 1),
            "close": lambda: b.close(),
            "bad login": lambda: CmsBackend("127.0.0.1", port, "nbi-demo", "wrong").get_ont(NET, 1),
        }
        assert list(steps) == [s["step"] for s in RECORDED["steps"]]
        for recorded in RECORDED["steps"]:
            if recorded["step"] == "session re-login":
                mock_cms_server.app.state.sessions.clear()  # CMS forgets the session
            mark = len(log)
            try:
                await steps[recorded["step"]]()
                outcome = "ok"
            except Exception as e:
                outcome = type(e).__name__
            sent = log[mark:]
            assert outcome == recorded["outcome"], recorded["step"]
            assert len(sent) == len(recorded["requests"]), recorded["step"]
            for ours, theirs in zip(sent, recorded["requests"]):
                assert ours["path"] == theirs["path"] and ours["headers"] == theirs["headers"], recorded["step"]
                assert canonical(ours["body"]) == as_this_project_sends(theirs["body"]), recorded["step"]

    asyncio.run(scenario())


def test_long_lists_are_read_page_by_page(wire, monkeypatch):
    port, log = wire
    monkeypatch.setattr(mock_cms_server.app.state, "page_size", 4)

    async def scenario():
        backend = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password")
        onts = await backend.list_onts(NET)
        discovered = await backend.list_discovered_onts(NET)
        profiles = await backend.list_ont_profiles(NET)
        vlans = await backend.list_vlans(NET)
        for index in (1, 2, 3, 4, 5):  # five services on one port: two pages
            await backend.add_ethernet_service(NET, 1, EthernetService(
                vlan=300, bandwidth_profile="RES-1G", tag_action="1", ont_port=2 if index % 2 else 1))
        services = (await backend.get_ont(NET, 1)).services
        await backend.close()
        return onts, discovered, profiles, vlans, services

    onts, discovered, profiles, vlans, services = asyncio.run(scenario())
    assert [o.ont_id for o in onts] == list(range(1, 15))  # 14 ONTs, four pages, none lost or repeated
    assert len(discovered) == 3 and len(profiles) == 3 and [v.id for v in vlans] == [100, 200, 300]
    assert len(services) == 6 and len({(s.ont_port, s.service_index) for s in services}) == 6
    show_onts = [canonical(r["body"]) for r in log if "show-ont" in r["body"] and "<action-args/>" in
                 canonical(r["body"]).replace("<action-args></action-args>", "<action-args/>")
                 or "<after><type>Ont</type>" in r["body"]]
    assert len(show_onts) == 4
    assert "<after><type>Ont</type><id><ont>12</ont></id></after>" in show_onts[-1]


def test_a_cms_that_pages_badly_is_refused_rather_than_looped_over(wire, monkeypatch):
    port, _ = wire
    original = mock_cms_server._page
    monkeypatch.setattr(mock_cms_server, "_page", lambda items, request, path: original(items, None, path))
    monkeypatch.setattr(mock_cms_server.app.state, "page_size", 4)  # ...and always answers the first page

    async def scenario():
        backend = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password")
        with pytest.raises(Exception, match="same page twice"):
            await backend.list_onts(NET)
        await backend.close()

    asyncio.run(scenario())


def test_the_login_is_escaped_and_non_latin_text_arrives_intact(wire, monkeypatch):
    port, log = wire
    monkeypatch.setenv("MOCK_CMS_USER", 'nbi & "ops"')
    monkeypatch.setenv("MOCK_CMS_PASSWORD", "p<a>ss&wörd")

    async def scenario():
        backend = CmsBackend("127.0.0.1", port, 'nbi & "ops"', "p<a>ss&wörd")
        created = await backend.create_ont(NET, OntSpec(serial_number="CXNK00C4D301", profile="844G-1",
                                                         description="Café ☕ 5½"))
        await backend.close()
        return created

    assert asyncio.run(scenario()).description == "Café ☕ 5½"
    login = log[0]["body"]
    assert "<Password>p&lt;a&gt;ss&amp;w&#246;rd</Password>" in login  # escaped; non-ASCII as a reference
    assert "Caf&#233; &#9749; 5&#189;" in log[1]["body"]


def test_reply_xml_becomes_plain_data():
    root = ElementTree.fromstring('<object xmlns="urn:x"><type>Vlan</type><id><vlan name="data">100</vlan></id>'
                                  "<name/><child>1</child><child>2</child></object>")
    assert to_data(root) == {"type": "Vlan", "id": {"vlan": {"@name": "data", "#text": "100"}}, "name": None,
                             "child": ["1", "2"]}


def test_a_session_lost_mid_use_is_renewed_once_and_a_second_refusal_is_an_auth_error(wire, monkeypatch):
    port, log = wire

    async def scenario():
        client = CmsClient("127.0.0.1", port, "nbi-demo", "demo-password", timeout=3.0)
        await client.login()
        first = client.session_id
        mock_cms_server.app.state.sessions.clear()
        assert await client.show_ont(NET, {"ont": "1"})
        assert client.session_id != first

        # A CMS that forgets every session at once: one new login, then a clear error
        original = mock_cms_server._handle_auth

        def login_then_forget(auth):
            reply = original(auth)
            mock_cms_server.app.state.sessions.clear()
            return reply
        monkeypatch.setattr(mock_cms_server, "_handle_auth", login_then_forget)
        mock_cms_server.app.state.sessions.clear()
        with pytest.raises(NbiAuthError, match="keeps rejecting the session"):
            await client.show_ont(NET, {"ont": "1"})
        await client.close()

    asyncio.run(scenario())
    assert [r["body"].count("<login>") for r in log].count(1) == 3


def test_a_login_refused_with_a_session_id_is_still_refused(wire, monkeypatch):
    port, _ = wire
    monkeypatch.setattr(mock_cms_server, "_auth_reply", lambda code, session="": mock_cms_server.Response(
        content=f'<Envelope xmlns="{mock_cms_server.SOAP_NS}"><Body><auth-reply><ResultCode>6</ResultCode>'
                "<SessionId>123456789</SessionId></auth-reply></Body></Envelope>", media_type="text/xml"))

    async def scenario():
        client = CmsClient("127.0.0.1", port, "nbi-demo", "demo-password")
        with pytest.raises(NbiAuthError, match="rejected this API's username or password"):
            await client.login()
        assert client.session_id is None

    asyncio.run(scenario())


def test_the_scheme_must_be_http_or_https():
    with pytest.raises(ValueError, match="http or https"):
        CmsClient("cms", 18080, "u", "p", scheme="ftp")
    assert CmsClient("cms", 18443, "u", "p", scheme="https").url == "https://cms:18443/cmsexc/ex/netconf"


# --- HTTPS ---------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tls_files(tmp_path_factory):
    """A private CA and a certificate for 127.0.0.1 signed by it, made with the openssl command"""
    if not subprocess.run(["openssl", "version"], capture_output=True).returncode == 0:
        pytest.skip("the openssl command is needed to make test certificates")
    d = tmp_path_factory.mktemp("tls")

    def openssl(*args):
        subprocess.run(["openssl", *args], cwd=d, check=True, capture_output=True)
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=Test CMS CA",
            "-keyout", "ca.key", "-out", "ca.pem")
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=127.0.0.1", "-keyout", "cms.key", "-out", "cms.csr")
    (d / "san.cnf").write_text("subjectAltName=IP:127.0.0.1\n")
    openssl("x509", "-req", "-in", "cms.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-days", "2",
            "-extfile", "san.cnf", "-out", "cms.pem")
    return d


@pytest.fixture
def https_cms(nbi_server, tls_files):
    mock_cms_server._fresh_state()
    server, port = _serve(mock_cms_server.app, ssl_certfile=str(tls_files / "cms.pem"),
                          ssl_keyfile=str(tls_files / "cms.key"))
    yield port, tls_files
    server.should_exit = True


def test_https_checks_the_cms_certificate_by_default(https_cms):
    port, tls = https_cms

    async def scenario():
        default = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", scheme="https")
        with pytest.raises(NbiConnectionError, match="certificate could not be verified"):
            await default.get_ont(NET, 3)
        with pytest.raises(NbiConnectionError, match="certificate could not be verified"):
            await CmsClient("127.0.0.1", port, "nbi-demo", "demo-password", scheme="https").login()

        trusted = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", scheme="https",
                             verify=str(tls / "ca.pem"))
        assert (await trusted.get_ont(NET, 3)).serial_number == "CXNK00A3F103"
        with pytest.raises(NbiNotFoundError):
            await trusted.get_ont(NET, 999)
        await trusted.close()

        unchecked = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", scheme="https", verify=False)
        assert (await unchecked.get_ont(NET, 3)).ont_id == 3
        await unchecked.close()

        plain = CmsBackend("127.0.0.1", port, "nbi-demo", "demo-password", scheme="http")
        with pytest.raises(NbiConnectionError):  # HTTP to an HTTPS port
            await plain.get_ont(NET, 3)

    asyncio.run(scenario())
    assert mock_cms_server.app.state.sessions == set()


def test_https_settings_reach_the_client(monkeypatch, tmp_path):
    for name in ("CMS_SCHEME", "CMS_VERIFY_TLS", "CMS_CA_FILE"):
        monkeypatch.delenv(name, raising=False)
    defaults = Settings(_env_file=None)
    assert (defaults.cms_scheme, defaults.cms_verify_tls, defaults.cms_verify()) == ("http", True, True)
    assert Settings(_env_file=None, cms_ca_file="/etc/cms-ca.pem").cms_verify() == "/etc/cms-ca.pem"
    assert Settings(_env_file=None, cms_verify_tls=False, cms_ca_file="/x").cms_verify() is False

    from app.nbi.gateway import build_gateway
    gateway = build_gateway(Settings(_env_file=None, mock_mode=False, cms_host="cms.example", cms_port=18443,
                                     cms_scheme="https", cms_ca_file="/etc/cms-ca.pem"))
    client = gateway.backend.client
    assert (client.url, client._verify) == ("https://cms.example:18443/cmsexc/ex/netconf", "/etc/cms-ca.pem")


def test_the_read_only_check_works_over_https(https_cms):
    port, tls = https_cms
    env = {**os.environ, "CMS_HOST": "127.0.0.1", "CMS_PORT": str(port), "CMS_USERNAME": "nbi-demo",
           "CMS_PASSWORD": "demo-password", "MOCK_MODE": "false", "CMS_SCHEME": "https",
           "CMS_CA_FILE": str(tls / "ca.pem")}
    run = subprocess.run([sys.executable, "-m", "app.tools.cms_check", "--network", NET, "--ont-id", "3"],
                         cwd=REPO, env=env, capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stdout + run.stderr
    assert f"https://127.0.0.1:{port}/cmsexc/ex/netconf" in run.stdout and "every step worked" in run.stdout
    assert [r for r in mock_cms_server.app.state.requests if r["kind"] == "edit"] == []


@pytest.mark.parametrize("raised, expected, says", [
    (httpx.ConnectTimeout, NbiConnectionError, "Nothing was sent"),  # never connected: a write cannot have landed
    (httpx.ConnectError, NbiConnectionError, "Could not connect"),
    (httpx.ReadTimeout, NbiTimeoutError, "did not answer"),  # sent, no answer: a write may have been applied
])
def test_a_connection_that_never_opens_is_unreachable_not_a_timeout(raised, expected, says):
    """
    The same on every OS: Linux refuses a closed port at once (ConnectError),
    Windows retries until the connect timeout (ConnectTimeout).
    """
    def fail(request):
        raise raised("simulated", request=request)

    client = CmsClient("127.0.0.1", 1, "nbi-demo", "demo-password", timeout=0.5)
    client._connection = lambda: httpx.AsyncClient(transport=httpx.MockTransport(fail))
    with pytest.raises(expected, match=says):
        asyncio.run(client.send("<x/>"))


def test_non_numeric_bandwidth_profiles_are_warned_about_not_refused(caplog):
    # Numeric IDs come from docs for an earlier CMS release, so a name is only a warning at start
    from types import SimpleNamespace
    from app.nbi.gateway import build_gateway

    def settings(profiles):
        return SimpleNamespace(
            nbi_backend="cms", mock_mode=False, e7_nodes=None, cms_host="cms.example", cms_port=18443,
            cms_username="u", cms_password="p", cms_scheme="https", cms_verify=lambda: True,
            nbi_timeout_seconds=5, nbi_max_retries=0, nbi_max_concurrency=1, nbi_breaker_failure_threshold=5,
            nbi_breaker_recovery_seconds=30, cms_rg_service_slot=None, cms_rg_port_mode_element=None,
            known_bandwidth_profiles=lambda: frozenset(profiles))

    for profiles, warned in ((["17", "18"], False), (["17", "RES-1G"], True)):
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="app.nbi.gateway"):
            build_gateway(settings(profiles))
        assert any("non-numeric values (RES-1G)" in r.getMessage() for r in caplog.records) is warned
