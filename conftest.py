"""
Pytest setup. Living at the repo root also puts the root on sys.path, so
tests can import main, config and app.
"""

import asyncio

import pytest

from app.controller.ssh_controller import ssh_controller
from config import settings


@pytest.fixture
def mock_mode(monkeypatch):
    """Turn MOCK_MODE on for one test and leave the shared controller clean"""
    monkeypatch.setattr(settings, "mock_mode", True)
    yield
    asyncio.run(ssh_controller.disconnect())


@pytest.fixture
def real_mode(monkeypatch):
    """Force MOCK_MODE off, whatever .env says"""
    monkeypatch.setattr(settings, "mock_mode", False)
    yield
    asyncio.run(ssh_controller.disconnect())
