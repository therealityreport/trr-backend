"""Tests for /health and /health/live endpoints."""

from __future__ import annotations

import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Any, cast
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import main as api_main
from api.auth import require_internal_admin
from api.main import (
    admin_health_db_pressure,
    admin_health_instagram_comment_rollups,
    health,
    health_db_pressure,
    health_live,
    health_runtime,
)
from trr_backend.db import pg as _real_pg

_test_app = FastAPI()
_test_app.get("/health")(health)
_test_app.get("/health/live")(health_live)
_test_app.get("/health/db-pressure")(health_db_pressure)
_test_app.get("/admin/health/db-pressure")(admin_health_db_pressure)
_test_app.get("/admin/health/instagram-comment-rollups")(admin_health_instagram_comment_rollups)
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


def test_health_reports_operator_database_lane() -> None:
    with (
        patch.object(_real_pg, "db_read_connection", _fake_db_read_connection_ok),
        patch.object(
            api_main,
            "resolve_database_url_candidate_details",
            lambda: (
                {
                    "source": "TRR_DB_DIRECT_URL",
                    "host_class": "direct",
                    "connection_class": "direct",
                    "host": "db.example.supabase.co",
                    "port": 5432,
                    "database": "postgres",
                },
            ),
        ),
    ):
        resp = client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["database_lane"] == {
        "url_lane": "direct_url",
        "url_source": "TRR_DB_DIRECT_URL",
        "connection_class": "direct",
        "host_class": "direct",
        "pool_name": "health",
        "pool_lane": "health_pool",
    }


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
    assert body["operator_failure_lanes"]["health"]["pool_lane"] == "health_pool"
    assert body["operator_failure_lanes"]["social_profile"]["pool_lane"] == "social_profile_pool"
    assert body["operator_failure_lanes"]["auth"]["lane"] == "auth"
    assert body["operator_failure_lanes"]["modal"]["lane"] == "modal_deployment_state"


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


def test_admin_health_instagram_comment_rollups_returns_snapshot() -> None:
    with patch.object(
        api_main,
        "instagram_comment_rollup_health",
        lambda sample_limit=25: {
            "status": "healthy",
            "reason": "ok",
            "rollup_table": "social.instagram_post_comment_rollups",
            "sample_limit": sample_limit,
            "mismatch_count": 0,
        },
    ):
        resp = client.get("/admin/health/instagram-comment-rollups?sample_limit=7")

    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "status": "healthy",
        "reason": "ok",
        "rollup_table": "social.instagram_post_comment_rollups",
        "sample_limit": 7,
        "mismatch_count": 0,
    }


def test_api_main_registers_instagram_comment_rollup_health_only_after_provider_ready() -> None:
    source = dedent(
        """
        import importlib
        import sys
        from fastapi import FastAPI

        common_name = "trr_backend.socials.read_models.account_profile.common"
        provider_name = "trr_backend.socials.social_season_analytics_impl"
        events = []
        original_get = FastAPI.get

        def tracked_get(self, path, *args, **kwargs):
            decorator = original_get(self, path, *args, **kwargs)
            if path not in {
                "/admin/health/instagram-comment-rollups",
                "/api/v1/admin/health/instagram-comment-rollups",
            }:
                return decorator

            def tracked_decorator(function):
                common = sys.modules.get(common_name)
                events.append(
                    (
                        path,
                        getattr(common, "_PROVIDER_STATE", None),
                        provider_name in sys.modules,
                        function.__name__,
                    )
                )
                return decorator(function)

            return tracked_decorator

        FastAPI.get = tracked_get
        try:
            api_main = importlib.import_module("api.main")
        finally:
            FastAPI.get = original_get
        common = sys.modules[common_name]
        provider = sys.modules[provider_name]
        assert common._PROVIDER_STATE == "READY"
        assert common._PROVIDER_NAMESPACE is provider.__dict__
        assert api_main.instagram_comment_rollup_health is common.instagram_comment_rollup_health
        assert events == [
            (
                "/api/v1/admin/health/instagram-comment-rollups",
                "READY",
                True,
                "admin_health_instagram_comment_rollups",
            ),
            (
                "/admin/health/instagram-comment-rollups",
                "READY",
                True,
                "admin_health_instagram_comment_rollups",
            ),
        ]
        """
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", source],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_admin_health_instagram_comment_rollups_calls_ready_common_export_offline() -> None:
    import trr_backend.socials.read_models.account_profile.common as common
    import trr_backend.socials.social_season_analytics_impl as provider

    # _PROVIDER_STATE/_PROVIDER_NAMESPACE are injected at runtime by the provider loop.
    common_any: Any = common
    assert common_any._PROVIDER_STATE == "READY"
    assert common_any._PROVIDER_NAMESPACE is provider.__dict__
    assert api_main.instagram_comment_rollup_health is common.instagram_comment_rollup_health
    connection = object()
    db_calls: list[dict[str, object]] = []
    availability_calls: list[object] = []

    @contextmanager
    def fake_read_connection(**kwargs):
        db_calls.append(kwargs)
        yield connection

    def fake_available(*, conn):
        availability_calls.append(conn)
        return False

    with (
        patch.object(provider.pg, "db_read_connection", fake_read_connection),
        patch.object(provider, "_instagram_post_comment_rollups_available", fake_available),
    ):
        result = api_main.admin_health_instagram_comment_rollups(sample_limit=7, _=cast("Any", None))

    assert result == {
        "status": "unavailable",
        "reason": "rollup_table_missing",
        "rollup_table": "social.instagram_post_comment_rollups",
        "sample_limit": 7,
    }
    assert db_calls == [{"label": "instagram-comment-rollup-health", "pool_name": "health"}]
    assert availability_calls == [connection]


def test_health_runtime_ignores_database_failure():
    with patch.object(_real_pg, "db_connection", _fake_db_connection_fail):
        resp = client.get("/health/runtime")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "alive"
    assert body["service"] == "trr-backend"
    assert "background_tasks" in body
    assert "realtime" in body


def test_health_runtime_includes_realtime_snapshot_without_database():
    with (
        patch.object(_real_pg, "db_connection", _fake_db_connection_fail),
        patch.object(
            api_main,
            "broker_runtime_status",
            lambda: {
                "mode": "redis",
                "connected": True,
                "multi_worker_policy": {
                    "workers_requested": 2,
                    "require_redis_for_multi_worker": True,
                    "redis_url_configured": True,
                    "safe_for_multi_worker": True,
                },
            },
        ),
    ):
        resp = client.get("/health/runtime")

    assert resp.status_code == 200
    body = resp.json()
    assert body["realtime"]["mode"] == "redis"
    assert body["realtime"]["multi_worker_policy"]["safe_for_multi_worker"] is True
