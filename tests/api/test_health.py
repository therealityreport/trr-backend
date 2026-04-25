"""Tests for /health and /health/live endpoints."""

from __future__ import annotations

import inspect
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.main import health, health_live, health_runtime
from trr_backend.db import pg as _real_pg

_test_app = FastAPI()
_test_app.get("/health")(health)
_test_app.get("/health/live")(health_live)
_test_app.get("/health/runtime")(health_runtime)

client = TestClient(_test_app)


# -- helpers ----------------------------------------------------------------


@contextmanager
def _fake_db_connection_ok(**_kwargs):
    """Simulate a healthy DB connection."""
    conn = MagicMock()
    yield conn


def _fake_db_connection_fail(**_kwargs):
    """Simulate an unreachable DB — raises before yielding."""
    raise ConnectionError("database unreachable")


@contextmanager
def _fake_db_read_connection_ok(**_kwargs):
    conn = MagicMock()
    yield conn


def _fake_db_read_connection_fail(**_kwargs):
    raise ConnectionError("database unreachable")


# -- tests ------------------------------------------------------------------


def test_health_connected():
    with patch.object(_real_pg, "db_read_connection", _fake_db_read_connection_ok):
        resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "healthy"
    assert body["service"] == "trr-backend"
    assert body["database"] == "connected"


def test_health_degraded():
    with patch.object(_real_pg, "db_read_connection", _fake_db_read_connection_fail):
        resp = client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["service"] == "trr-backend"
    assert body["database"] == "unreachable"


def test_health_uses_health_read_pool() -> None:
    calls: list[tuple[str, str]] = []

    @contextmanager
    def _tracking_db_read_connection(*, label: str, pool_name: str):
        calls.append((label, pool_name))
        yield MagicMock()

    with patch.object(_real_pg, "db_read_connection", _tracking_db_read_connection):
        resp = client.get("/health")

    assert resp.status_code == 200
    assert calls == [("health-probe", "health")]


def test_health_live():
    resp = client.get("/health/live")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "alive"
    assert body["service"] == "trr-backend"


def test_health_live_is_async_endpoint() -> None:
    assert inspect.iscoroutinefunction(health_live)


def test_health_live_ignores_database_failure():
    with patch.object(_real_pg, "db_connection", _fake_db_connection_fail):
        resp = client.get("/health/live")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "alive"
    assert body["service"] == "trr-backend"


def test_health_runtime_ignores_database_failure():
    with patch.object(_real_pg, "db_connection", _fake_db_connection_fail):
        resp = client.get("/health/runtime")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "alive"
    assert body["service"] == "trr-backend"
    assert "background_tasks" in body
