"""
Pytest setup. Living at the repo root also puts the root on sys.path, so
tests can import main, config and app.
"""

import os
import socket
import threading
import time

import pytest


@pytest.fixture(scope="session")
def nbi_server():
    """
    The mock CMS NBI server (SOAP over HTTP) on a free local port.
    Yields (module, port). Call module._fresh_state() to reset its data.
    """
    uvicorn = pytest.importorskip("uvicorn")
    from app.nbi import mock_cms_server

    os.environ["MOCK_CMS_LATENCY"] = "0"
    os.environ["MOCK_CMS_RANGING_SECONDS"] = "0"
    mock_cms_server._fresh_state()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(mock_cms_server.app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    assert server.started, "mock CMS NBI server did not start"
    yield mock_cms_server, port
    server.should_exit = True
    thread.join(timeout=5)
