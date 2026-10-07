"""
Tests for MOCK_MODE: the API must work end to end with no hardware and
without opening a socket, and the real SSH path must be left as it was.
"""

import asyncio
import socket
import sys
import types

import pytest
from fastapi.testclient import TestClient

from app.controller.mock_e7 import MOCK_ONTS, MockE7Connection
from app.controller.ssh_controller import ssh_controller
from app.utils.helpers import (
    parse_ont_id_from_string, parse_optical_levels, parse_performance_metrics
)
from main import app

client = TestClient(app)

CREDENTIALS = {"host": "10.0.0.1", "username": "admin", "password": "anything"}


@pytest.fixture
def no_network(monkeypatch):
    """Fail the test if anything tries to open a socket or import Netmiko"""
    def refuse(*args, **kwargs):
        raise AssertionError("mock mode tried to open a network socket")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setitem(sys.modules, "netmiko", None)  # makes "import netmiko" fail


def test_mock_connect_opens_no_socket(mock_mode, no_network):
    status = asyncio.run(ssh_controller.connect(**CREDENTIALS))

    assert status.connected is True
    assert status.mock_mode is True
    assert status.host == "10.0.0.1"
    assert isinstance(ssh_controller.connection, MockE7Connection)
    assert asyncio.run(ssh_controller.is_connected()) is True


def test_execute_command_returns_mock_output(mock_mode, no_network):
    asyncio.run(ssh_controller.connect(**CREDENTIALS))

    response = asyncio.run(ssh_controller.execute_command("show ont info"))

    assert response.exit_code == 0
    assert response.error is None
    assert "ONT 1/1/1:4" in response.output
    assert asyncio.run(ssh_controller.get_connection_status()).last_command == "show ont info"


def test_execute_command_without_connection_still_errors(mock_mode):
    response = asyncio.run(ssh_controller.execute_command("show version"))

    assert response.exit_code == 1
    assert response.error == "No active SSH connection"


@pytest.mark.parametrize("command", [
    # sent by the controllers
    "show version",
    "show interface",
    "show ont info",
    "show ont info 4",
    "show ont performance 4",
    "show pon port status",
    "show pon port status 1/1/1",
    # from the revive prompt
    "show ont status",
    "show interface gpon",
    # from notes.md
    "show gpon pon-errors gpon-1/1/1",
    "show interface gpon-1/1/1 ont 4 statistics",
    "show gpon ont-provision gpon-1/1/1 4",
    "show alarm gpon-1/1/1 ont 4",
    "show gpon ont-optical gpon-1/1/1",
    "show interface gpon-1/1/1",
    "show gpon serial-number",
    "show system clock",
    "show user",
])
def test_known_commands_have_output(command):
    output = MockE7Connection().send_command(command, expect_string=r"#", delay_factor=2)

    assert output.strip()
    assert not output.startswith("%"), output


def test_unknown_command_and_unknown_ont_report_errors():
    device = MockE7Connection()

    assert device.send_command("show flux capacitor").startswith("% Invalid command")
    assert device.send_command("show ont info 999").startswith("% ONT 999 not found")
    # ONT 9 exists, but on 1/1/2
    assert device.send_command("show gpon ont-provision gpon-1/1/1 9").startswith("% ONT 9 not found")


def test_existing_helpers_can_parse_mock_output():
    device = MockE7Connection()
    rogue = next(o for o in MOCK_ONTS if o.ont_id == 4)

    metrics = parse_performance_metrics(device.send_command("show ont performance 4"))
    assert metrics == {
        "missed_bursts": rogue.missed_bursts,
        "bip_errors": rogue.bip_errors,
        "crc_errors": rogue.crc_errors,
        "fec_errors": rogue.fec_errors,
    }

    detail = device.send_command("show ont info 1/1/1:4")
    assert parse_ont_id_from_string(detail) == "1/1/1:4"
    assert parse_optical_levels(detail) == {
        "tx_power": rogue.tx_power,
        "rx_power": rogue.rx_power,
        "temperature": rogue.temperature,
    }


def test_closed_mock_session_raises():
    device = MockE7Connection()
    device.disconnect()

    with pytest.raises(ConnectionError):
        device.send_command("show version")


def test_api_end_to_end_in_mock_mode(mock_mode, no_network):
    assert client.get("/api/v1/status").json()["mock_mode"] is True

    # Nothing is connected yet
    assert client.get("/api/v1/ssh/test-connection").json()["connected"] is False

    connected = client.post("/api/v1/ssh/connect", json=CREDENTIALS)
    assert connected.status_code == 200
    assert connected.json()["connected"] is True
    assert connected.json()["mock_mode"] is True

    command = client.post("/api/v1/ssh/command", json={"command": "show gpon pon-errors gpon-1/1/1"})
    assert command.status_code == 200
    assert "missedBursts" in command.json()["output"]

    test = client.get("/api/v1/ssh/test-connection").json()
    assert test["connected"] is True and test["success"] is True

    # One row per online ONT (13 of the 14 simulated ONTs are online)
    onts = client.get("/api/v1/onts/")
    assert onts.status_code == 200
    assert onts.json()["total_count"] == sum(1 for o in MOCK_ONTS if o.online)

    assert client.get("/api/v1/onts/4").status_code == 200
    assert client.get("/api/v1/onts/4/performance").status_code == 200
    assert len(client.get("/api/v1/onts/pon/status").json()) == 2
    assert client.post("/api/v1/onts/analyze-rogue", json={}).status_code == 200

    interfaces = client.get("/api/v1/hp/interfaces")
    assert interfaces.status_code == 200
    assert len(interfaces.json()["interfaces"]) == 8

    assert client.post("/api/v1/ssh/disconnect").status_code == 200
    assert client.get("/api/v1/ssh/status").json()["connected"] is False


def test_real_mode_still_uses_netmiko(real_mode, monkeypatch):
    """With MOCK_MODE off, connect() must go to Netmiko exactly as before"""
    calls = {}

    class FakeNetmikoConnection:
        def send_command(self, command, **kwargs):
            calls.setdefault("commands", []).append((command, kwargs))
            return "real device output"

        def disconnect(self):
            calls["disconnected"] = True

    def fake_connect_handler(**device_config):
        calls["device_config"] = device_config
        return FakeNetmikoConnection()

    fake_netmiko = types.ModuleType("netmiko")
    fake_netmiko.ConnectHandler = fake_connect_handler
    fake_netmiko.NetMikoTimeoutException = type("NetMikoTimeoutException", (Exception,), {})
    fake_netmiko.NetMikoAuthenticationException = type("NetMikoAuthenticationException", (Exception,), {})
    fake_paramiko = types.ModuleType("paramiko")
    fake_paramiko.SSHException = type("SSHException", (Exception,), {})
    monkeypatch.setitem(sys.modules, "netmiko", fake_netmiko)
    monkeypatch.setitem(sys.modules, "paramiko", fake_paramiko)

    status = asyncio.run(ssh_controller.connect(
        host="192.168.1.100", username="admin", password="secret", port=2222, timeout=15))

    assert status.connected is True
    assert status.mock_mode is False
    assert calls["device_config"] == {
        "device_type": "hp_procurve",
        "host": "192.168.1.100",
        "username": "admin",
        "password": "secret",
        "port": 2222,
        "timeout": 15,
        "global_delay_factor": 2,
        "fast_cli": False,
        "session_log": None,
    }
    assert calls["commands"] == [("show version", {"expect_string": r"#"})]

    response = asyncio.run(ssh_controller.execute_command("show ont info"))
    assert response.output == "real device output"
    assert calls["commands"][-1] == ("show ont info", {"expect_string": r"#", "delay_factor": 2})


def test_real_mode_connection_failure_is_reported(real_mode, monkeypatch):
    class AuthFailed(Exception):
        pass

    def refuse(**device_config):
        raise AuthFailed("bad password")

    fake_netmiko = types.ModuleType("netmiko")
    fake_netmiko.ConnectHandler = refuse
    fake_netmiko.NetMikoTimeoutException = type("NetMikoTimeoutException", (Exception,), {})
    fake_netmiko.NetMikoAuthenticationException = AuthFailed
    fake_paramiko = types.ModuleType("paramiko")
    fake_paramiko.SSHException = type("SSHException", (Exception,), {})
    monkeypatch.setitem(sys.modules, "netmiko", fake_netmiko)
    monkeypatch.setitem(sys.modules, "paramiko", fake_paramiko)

    status = asyncio.run(ssh_controller.connect(host="192.168.1.100", username="admin", password="wrong"))

    assert status.connected is False
    assert "Authentication failed for 192.168.1.100" in status.error_message
    assert asyncio.run(ssh_controller.is_connected()) is False
