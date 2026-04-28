"""Tests for /health and /health/live endpoints."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import admin_health_db_pressure, health, health_db_pressure, health_live, health_runtime
from trr_backend.db import pg as _real_pg

_test_app = FastAPI()
_test_app.get("/health")(health)
_test_app.get("/health/live")(health_live)
_test_app.get("/health/db-pressure")(health_db_pressure)
_test_app.get("/admin/health/db-pressure")(admin_health_db_pressure)
_test_app.get("/health/runtime")(health_runtime)
_test_app.dependency_overrides[require_internal_admin] = lambda: {"role": "internal_admin"}

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


def test_health_live_ignores_database_failure():
    with patch.object(_real_pg, "db_connection", _fake_db_connection_fail):
        resp = client.get("/health/live")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "alive"
    assert body["service"] == "trr-backend"


def test_health_db_pressure_returns_public_safe_summary() -> None:
    with patch.object(
        _real_pg,
        "local_pool_pressure_summary",
        lambda: {"status": "degraded", "reason": "pool_near_capacity", "service": "trr-backend"},
    ):
        resp = client.get("/health/db-pressure")

    assert resp.status_code == 200
    body = resp.json()
    assert body == {"status": "degraded", "reason": "pool_near_capacity", "service": "trr-backend"}
    assert "pools" not in body


def test_admin_health_db_pressure_returns_protected_pool_details() -> None:
    payload = {
        "status": "ok",
        "reason": "pool_pressure_ok",
        "service": "trr-backend",
        "pools": [{"pool_name": "default", "application_name": "trr-backend:default"}],
    }
    with (
        patch.object(_real_pg, "local_pool_pressure_snapshot", lambda: payload),
        patch.object(_real_pg, "db_read_connection", _fake_db_read_connection_fail),
    ):
        resp = client.get("/admin/health/db-pressure")

    assert resp.status_code == 200
    body = resp.json()
    assert body["pools"][0]["pool_name"] == "default"
    assert body["pools"][0]["application_name"] == "trr-backend:default"


def test_admin_health_db_pressure_includes_grouped_holder_activity() -> None:
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query):
            assert "pg_stat_activity" in query
            assert "query" not in query.lower()

        def fetchall(self):
            return [("trr-app:web", "postgres", "active", "127.0.0.1", 2)]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    @contextmanager
    def _fake_activity_connection(**_kwargs):
        yield FakeConnection()

    payload = {
        "status": "ok",
        "reason": "pool_pressure_ok",
        "service": "trr-backend",
        "pools": [],
    }
    with (
        patch.object(_real_pg, "local_pool_pressure_snapshot", lambda: payload.copy()),
        patch.object(_real_pg, "db_read_connection", _fake_activity_connection),
    ):
        resp = client.get("/admin/health/db-pressure")

    assert resp.status_code == 200
    body = resp.json()
    assert body["db_activity"]["status"] == "available"
    assert body["db_activity"]["grouped_by"] == ["application_name", "role", "state", "client_addr"]
    assert body["db_activity"]["holders"] == [
        {
            "application_name": "trr-app:web",
            "role": "postgres",
            "state": "active",
            "client_addr": "127.0.0.1",
            "holder_count": 2,
        }
    ]
    assert "query" not in str(body).lower()


def test_admin_health_db_pressure_marks_activity_permission_blocked() -> None:
    def _permission_denied_connection(**_kwargs):
        raise PermissionError("permission denied for view pg_stat_activity")

    payload = {
        "status": "ok",
        "reason": "pool_pressure_ok",
        "service": "trr-backend",
        "pools": [],
    }
    with (
        patch.object(_real_pg, "local_pool_pressure_snapshot", lambda: payload.copy()),
        patch.object(_real_pg, "db_read_connection", _permission_denied_connection),
    ):
        resp = client.get("/admin/health/db-pressure")

    assert resp.status_code == 200
    body = resp.json()
    assert body["db_activity"] == {
        "status": "unavailable",
        "reason": "permission_blocked",
        "error_type": "PermissionError",
        "holders": [],
    }
    assert "permission denied" not in str(body).lower()


def test_health_runtime_ignores_database_failure():
    with patch.object(_real_pg, "db_connection", _fake_db_connection_fail):
        resp = client.get("/health/runtime")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "alive"
    assert body["service"] == "trr-backend"
    assert "background_tasks" in body
