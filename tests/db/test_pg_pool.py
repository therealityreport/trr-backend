"""Tests for psycopg2 connection pooling helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg2 import InterfaceError
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS
from psycopg2.pool import PoolError

from trr_backend.db import connection, pg


class _FakeConnection:
    def __init__(self, *, query_results: list[dict[str, object] | None] | None = None) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0
        self._autocommit = False
        self.closed = False
        self.transaction_status = TRANSACTION_STATUS_IDLE
        self.executed_sql: list[str] = []
        self.query_results = list(query_results or [])

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._autocommit = value

    def commit(self) -> None:
        self.commit_calls += 1
        self.transaction_status = TRANSACTION_STATUS_IDLE

    def rollback(self) -> None:
        self.rollback_calls += 1
        self.transaction_status = TRANSACTION_STATUS_IDLE

    def get_transaction_status(self) -> int:
        return self.transaction_status

    def get_backend_pid(self) -> int:
        return 12345

    def cursor(self, *args, **kwargs):
        del args, kwargs
        return _FakeCursor(self)


class _FakeCursor:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def execute(self, sql: str, params=None) -> None:
        del params
        self._connection.executed_sql.append(sql)

    def fetchone(self) -> dict[str, object] | None:
        if not self._connection.query_results:
            return None
        return self._connection.query_results.pop(0)

    def fetchall(self) -> list[dict[str, object]]:
        rows = [row for row in self._connection.query_results if row is not None]
        self._connection.query_results.clear()
        return rows


class _FakePool:
    def __init__(self) -> None:
        self.connection = _FakeConnection()
        self.putconn_calls = 0
        self.getconn_calls = 0
        self.closed_putconn_calls = 0
        self.closeall_calls = 0
        self._pool: list[object] = []
        self._used: dict[int, object] = {}

    def getconn(self) -> _FakeConnection:
        self.getconn_calls += 1
        self._used[id(self.connection)] = self.connection
        return self.connection

    def putconn(self, _conn: _FakeConnection, close: bool = False) -> None:
        self.putconn_calls += 1
        if close:
            self.closed_putconn_calls += 1
            _conn.closed = True
        self._used.pop(id(_conn), None)

    def closeall(self) -> None:
        self.closeall_calls += 1


class _FakeThreadedPool(_FakePool):
    def __init__(self, *, lock_result: dict[str, object] | None = None) -> None:
        super().__init__()
        self.connection = _FakeConnection(query_results=[lock_result or {"locked": True}])
        self.last_connection = self.connection

    @property
    def getconn_count(self) -> int:
        return self.getconn_calls

    @property
    def putconn_count(self) -> int:
        return self.putconn_calls


class _LazyPreviewPool(_FakePool):
    """Creates a fresh physical connection for every checkout in preview tests."""

    def __init__(self) -> None:
        super().__init__()
        self.connections: list[_FakeConnection] = []

    def getconn(self) -> _FakeConnection:
        self.getconn_calls += 1
        conn = _FakeConnection(query_results=[{"transaction_read_only": "on"}])
        self.connections.append(conn)
        self._used[id(conn)] = conn
        return conn


class _FakePoolClosedOnGetconn(_FakePool):
    def getconn(self) -> _FakeConnection:
        self.getconn_calls += 1
        raise PoolError("connection pool is closed")


class _FakePoolExhaustThenSuccess(_FakePool):
    def __init__(self, *, failures_before_success: int) -> None:
        super().__init__()
        self._failures_remaining = failures_before_success

    def getconn(self) -> _FakeConnection:
        self.getconn_calls += 1
        if self._failures_remaining > 0:
            self._failures_remaining -= 1
            raise PoolError("connection pool exhausted")
        return self.connection


class _FakePoolClosedOnPut(_FakePool):
    def putconn(self, _conn: _FakeConnection, close: bool = False) -> None:
        self.putconn_calls += 1
        raise PoolError("connection pool is closed")


class _FakeConnectionClosedOnAutocommitRestore(_FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.fail_restore_once = True

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if self.fail_restore_once and value is False:
            self.closed = True
            self.fail_restore_once = False
            raise InterfaceError("connection already closed")
        self._autocommit = value


class _FakeConnectionRaisesOnBackendPidWhenClosed(_FakeConnectionClosedOnAutocommitRestore):
    def get_backend_pid(self) -> int:
        if self.closed:
            raise InterfaceError("connection already closed")
        return super().get_backend_pid()


def _detail(url: str) -> dict[str, object]:
    return {
        "url": url,
        "source": "test",
        "host_class": "other",
        "connection_class": "other",
        "host": "db.example.com",
        "port": 5432,
        "database": "postgres",
    }


@pytest.fixture(autouse=True)
def _reset_pool_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in (
        "APP_ENV",
        "ENV",
        "ENVIRONMENT",
        "TRR_ENV",
        "TRR_ENVIRONMENT",
        "TRR_LOCAL_DEV",
        "MODAL_TASK_ID",
        "MODAL_ENVIRONMENT",
        "TRR_DB_APPLICATION_NAME",
        "TRR_DB_POOL_MINCONN",
        "TRR_DB_POOL_MAXCONN",
        "TRR_SOCIAL_PROFILE_DB_POOL_MINCONN",
        "TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN",
        "TRR_SOCIAL_CONTROL_DB_POOL_MINCONN",
        "TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN",
        "TRR_SOCIAL_PROGRESS_DB_POOL_MINCONN",
        "TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN",
        "TRR_HEALTH_DB_POOL_MINCONN",
        "TRR_HEALTH_DB_POOL_MAXCONN",
        "TRR_DB_POOL_ACQUIRE_ATTEMPTS",
        "TRR_DB_POOL_ACQUIRE_SLEEP_MS",
        "TRR_DB_POOL_CLOSE_AFTER_RETURN",
        "TRR_DB_CONNECT_TIMEOUT_SECONDS",
        "TRR_PREVIEW_READ_ONLY",
    ):
        monkeypatch.delenv(name, raising=False)
    pg.close_pool()
    yield
    pg.close_pool()


def test_db_connection_commits_and_returns_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pg.db_connection():
        pass

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.connection.commit_calls == 1
    assert fake_pool.connection.rollback_calls == 0


def test_db_read_connection_uses_autocommit_and_returns_clean_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePool()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pg.db_read_connection():
        assert fake_pool.connection.autocommit is True
        fake_pool.connection.transaction_status = TRANSACTION_STATUS_IDLE

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.connection.commit_calls == 0
    assert fake_pool.connection.rollback_calls == 0
    assert fake_pool.connection.autocommit is False
    assert fake_pool.connection.executed_sql == []


def test_preview_pool_reasserts_read_only_on_every_checkout_and_keeps_read_context_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePool()
    fake_pool.connection.query_results = [
        {"transaction_read_only": "on"},
        {"transaction_read_only": "on"},
    ]
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(pg, "_get_pool", lambda pool_name="default": fake_pool)

    with pg.db_read_connection(label="preview-read") as conn:
        assert conn.autocommit is True
        with conn.cursor() as cur:
            cur.execute("SELECT 1")

    with pg.db_read_connection(label="preview-read"):
        pass

    assert fake_pool.getconn_calls == 2
    assert fake_pool.putconn_calls == 2
    assert fake_pool.connection.executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
        "SELECT 1",
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]
    assert fake_pool.connection.autocommit is False


def test_preview_pool_asserts_lazily_created_physical_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lazy_pool = _LazyPreviewPool()
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: lazy_pool)

    with pg.db_read_connection(label="preview-lazy") as conn:
        assert conn.autocommit is True
        with conn.cursor() as cur:
            cur.execute("SELECT 1")

    assert lazy_pool.getconn_calls == 2
    assert lazy_pool.putconn_calls == 2
    assert lazy_pool.connections[0].executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]
    assert lazy_pool.connections[1].executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
        "SELECT 1",
    ]
    assert lazy_pool.connections[1].autocommit is False


def test_preview_checkout_fails_closed_and_discards_unverified_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePool()
    fake_pool.connection.query_results = [{"transaction_read_only": "off"}]
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(pg, "_get_pool", lambda pool_name="default": fake_pool)

    with pytest.raises(connection.PreviewReadOnlyError, match="requires transaction_read_only=on"):
        with pg.db_read_connection(label="preview-off"):
            pass

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.closed_putconn_calls == 1
    assert fake_pool.connection.executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]


def test_db_read_connection_uses_social_profile_pool_sizing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    created: list[tuple[int, int]] = []

    def _pool_factory(*, minconn, maxconn, **_kwargs):
        created.append((minconn, maxconn))
        return fake_pool

    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MINCONN", "1")
    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN", "2")
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    with pg.db_read_connection(label="social-profile-summary", pool_name="social_profile"):
        pass

    assert created == [(1, 2)]
    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1


def test_db_read_connection_uses_health_pool_sizing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    created: list[tuple[int, int]] = []

    def _pool_factory(*, minconn, maxconn, **_kwargs):
        created.append((minconn, maxconn))
        return fake_pool

    monkeypatch.setenv("TRR_HEALTH_DB_POOL_MINCONN", "1")
    monkeypatch.setenv("TRR_HEALTH_DB_POOL_MAXCONN", "2")
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    with pg.db_read_connection(label="health-probe", pool_name="health"):
        pass

    assert created == [(1, 2)]
    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1


def test_resolve_pool_sizing_honors_local_social_profile_session_pooler_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)
    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MINCONN", "1")
    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN", "4")

    sizing = pg._resolve_pool_sizing(
        "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres",
        minconn_env_name="TRR_SOCIAL_PROFILE_DB_POOL_MINCONN",
        maxconn_env_name="TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN",
        pool_name="social_profile",
    )

    assert sizing["requested_minconn"] == 1
    assert sizing["requested_maxconn"] == 4
    assert sizing["minconn"] == 1
    assert sizing["maxconn"] == 4
    assert sizing["session_pooler_override_clamped"] is False
    assert sizing["modal_session_pooler_override_clamped"] is False
    assert sizing["maxconn_source"] == "env:TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN"


def test_db_read_connection_uses_social_control_pool_sizing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    created: list[tuple[int, int]] = []

    def _pool_factory(*, minconn, maxconn, **_kwargs):
        created.append((minconn, maxconn))
        return fake_pool

    monkeypatch.setenv("TRR_SOCIAL_CONTROL_DB_POOL_MINCONN", "1")
    monkeypatch.setenv("TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN", "2")
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    with pg.db_read_connection(label="social-control", pool_name="social_control"):
        pass

    assert created == [(1, 2)]
    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1


def test_session_control_pool_uses_session_candidates_and_one_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePool()
    created: list[tuple[int, int, str]] = []
    session_dsn = "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    def _pool_factory(*, minconn, maxconn, **kwargs):
        created.append((minconn, maxconn, kwargs["dsn"]))
        return fake_pool

    monkeypatch.setenv("TRR_SESSION_CONTROL_DB_POOL_MINCONN", "5")
    monkeypatch.setenv("TRR_SESSION_CONTROL_DB_POOL_MAXCONN", "5")
    monkeypatch.setattr(
        pg,
        "resolve_session_database_url_candidate_details",
        lambda: (_detail(session_dsn),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    with pg.db_read_connection(label="session-control", pool_name="session_control"):
        pass

    assert created == [(1, 1, session_dsn)]
    assert pg.current_pool_dsn(pool_name="session_control") == session_dsn


def test_db_read_connection_discards_closed_connection_on_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePool()
    fake_pool.connection = _FakeConnectionRaisesOnBackendPidWhenClosed()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pg.db_read_connection():
        assert fake_pool.connection.autocommit is True

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.closed_putconn_calls == 1
    assert fake_pool.connection.closed is True


def test_advisory_session_lock_uses_one_connection_for_lock_and_unlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakeThreadedPool()
    monkeypatch.setattr(pg, "_get_pool", lambda pool_name="default": fake_pool)

    with pg.advisory_session_lock(lock_key=123, label="test"):
        pass

    assert fake_pool.getconn_count == 1
    assert fake_pool.putconn_count == 1
    assert fake_pool.last_connection.executed_sql == [
        "select pg_try_advisory_lock(%s) as locked",
        "select pg_advisory_unlock(%s)",
    ]


def test_advisory_session_lock_redirects_legacy_pool_to_session_control(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakeThreadedPool()
    selected_pools: list[str] = []

    def _get_pool(*, pool_name: str = "default"):
        selected_pools.append(pool_name)
        return fake_pool

    monkeypatch.setattr(pg, "_get_pool", _get_pool)

    with pg.advisory_session_lock(lock_key=123, label="test", pool_name="social_control"):
        pass

    assert selected_pools == ["session_control"]


def test_advisory_session_lock_raises_when_lock_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakeThreadedPool(lock_result={"locked": False})
    monkeypatch.setattr(pg, "_get_pool", lambda pool_name="default": fake_pool)

    with pytest.raises(pg.AdvisoryLockUnavailable):
        with pg.advisory_session_lock(lock_key=123, label="test"):
            pass

    assert fake_pool.getconn_count == 1
    assert fake_pool.putconn_count == 1


def test_db_connection_rolls_back_and_returns_connection_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pytest.raises(RuntimeError, match="boom"):
        with pg.db_connection():
            raise RuntimeError("boom")

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.connection.commit_calls == 0
    assert fake_pool.connection.rollback_calls == 1


def test_db_connection_discards_closed_connection_on_return(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pg.db_connection() as conn:
        cast(Any, conn).closed = True

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.closed_putconn_calls == 1
    assert fake_pool.connection.closed is True


def test_db_connection_rolls_back_dirty_connection_on_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    fake_pool.connection.transaction_status = TRANSACTION_STATUS_INTRANS
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pg.db_connection():
        pass

    assert fake_pool.putconn_calls == 1
    assert fake_pool.connection.commit_calls == 1
    assert fake_pool.connection.rollback_calls == 1


def test_pool_init_falls_back_after_transient_dns_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    primary = "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    fallback = "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    calls: list[str] = []
    fake_pool = _FakePool()

    def _pool_factory(*_args, **kwargs):
        dsn = kwargs.get("dsn")
        calls.append(cast(str, dsn))
        if dsn == primary:
            raise RuntimeError("getaddrinfo ENOTFOUND aws-1-us-east-1.pooler.supabase.com")
        return fake_pool

    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (
            {
                "url": primary,
                "source": "TRR_DB_URL",
                "host_class": "pooler",
                "connection_class": "transaction",
                "host": "aws-1-us-east-1.pooler.supabase.com",
                "port": 6543,
                "database": "postgres",
            },
            {
                "url": fallback,
                "source": "TRR_DB_FALLBACK_URL",
                "host_class": "pooler",
                "connection_class": "session",
                "host": "aws-1-us-east-1.pooler.supabase.com",
                "port": 5432,
                "database": "postgres",
            },
        ),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    with pg.db_connection():
        pass

    assert calls == [primary, fallback]
    assert pg.current_pool_dsn() == fallback


def test_fetch_one_retries_once_on_transient_transport_fault(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"fetch": 0, "reset": 0}

    @contextmanager
    def _fake_read_connection(*, label="read", pool_name="default"):  # noqa: ARG001
        yield _FakeConnection()

    def _fetch_one_with_cursor(_cur, _query, _params=None):
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            raise RuntimeError("SSL SYSCALL error: EOF detected")
        return {"ok": True}

    monkeypatch.setattr(pg, "db_read_connection", _fake_read_connection)
    monkeypatch.setattr(pg, "fetch_one_with_cursor", _fetch_one_with_cursor)
    monkeypatch.setattr(
        pg,
        "reset_pool",
        lambda **_kwargs: calls.__setitem__("reset", calls["reset"] + 1),
    )

    result = pg.fetch_one("select 1")

    assert result == {"ok": True}
    assert calls["fetch"] == 2


def test_is_database_service_unavailable_error_detects_pool_exhaustion_and_init_failure() -> None:
    assert pg.is_database_service_unavailable_error(PoolError("connection pool exhausted")) is True
    assert (
        pg.is_database_service_unavailable_error(
            pg.DatabaseServiceUnavailableError("Database pool initialization failed: boom")
        )
        is True
    )
    assert pg.is_database_service_unavailable_error(RuntimeError("Database pool initialization failed: boom")) is True
    assert pg.is_database_service_unavailable_error(RuntimeError("FATAL: EMAXCONNSESSION")) is True
    assert pg.is_database_service_unavailable_error(RuntimeError("FATAL: MaxClientsInSessionMode")) is True
    assert pg.is_database_service_unavailable_error(RuntimeError("other failure")) is False


def test_database_service_unavailable_detail_distinguishes_pool_capacity() -> None:
    detail = pg.database_service_unavailable_detail(
        pg.DatabaseServiceUnavailableError(
            "Database pool initialization failed: FATAL: MaxClientsInSessionMode",
            reason="session_pool_capacity",
        )
    )

    assert detail["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert detail["reason"] == "session_pool_capacity"
    assert "session-pool capacity" in detail["message"]
    assert detail["retryable"] is True
    assert detail["retry_after_ms"] == 1000


def test_database_service_unavailable_detail_distinguishes_configuration_errors() -> None:
    detail = pg.database_service_unavailable_detail(
        pg.DatabaseServiceUnavailableError(
            "Database pool initialization failed: missing runtime database URL",
            reason="database_configuration",
        )
    )

    assert detail["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert detail["reason"] == "database_configuration"
    assert "TRR_DB_URL" in detail["message"]


def test_get_pool_logs_effective_session_pooler_defaults_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    session_pooler_dsn = "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    fake_pool = _FakePool()

    monkeypatch.delenv("TRR_DB_POOL_MINCONN", raising=False)
    monkeypatch.delenv("TRR_DB_POOL_MAXCONN", raising=False)
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (
            {
                "url": session_pooler_dsn,
                "source": "TRR_DB_URL",
                "host_class": "pooler",
                "connection_class": "session",
                "host": "aws-1-us-east-1.pooler.supabase.com",
                "port": 5432,
                "database": "postgres",
            },
        ),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with caplog.at_level("INFO", logger="trr_backend.db.pg"):
        with pg.db_connection():
            pass

    assert "minconn=1 maxconn=2" in caplog.text
    assert "minconn_source=default maxconn_source=default" in caplog.text
    assert "application_name=trr-backend:default" in caplog.text
    assert "session_pooler_tiny_defaults" in caplog.text


def test_resolve_pool_sizing_keeps_production_session_defaults_conservative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_DB_POOL_MINCONN", raising=False)
    monkeypatch.delenv("TRR_DB_POOL_MAXCONN", raising=False)
    monkeypatch.setenv("APP_ENV", "production")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["minconn"] == 1
    assert sizing["maxconn"] == 2


def test_resolve_pool_sizing_treats_trr_local_dev_as_local_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRR_ENV", raising=False)
    monkeypatch.delenv("TRR_ENVIRONMENT", raising=False)
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "4")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "16")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["requested_minconn"] == 4
    assert sizing["requested_maxconn"] == 16
    assert sizing["minconn"] == 4
    assert sizing["maxconn"] == 8
    assert sizing["session_pooler_override_clamped"] is True
    assert sizing["modal_session_pooler_override_clamped"] is False
    assert sizing["maxconn_source"] == "clamped:local_session_pooler_ceiling"


def test_resolve_pool_sizing_treats_trr_local_dev_as_local_runtime_even_with_modal_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRR_ENV", raising=False)
    monkeypatch.delenv("TRR_ENVIRONMENT", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")
    monkeypatch.setenv("MODAL_TASK_ID", "ta-123")
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "4")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "16")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["requested_minconn"] == 4
    assert sizing["requested_maxconn"] == 16
    assert sizing["minconn"] == 4
    assert sizing["maxconn"] == 8
    assert sizing["session_pooler_override_clamped"] is True
    assert sizing["modal_session_pooler_override_clamped"] is False
    assert sizing["maxconn_source"] == "clamped:local_session_pooler_ceiling"


def test_resolve_pool_sizing_treats_trr_local_dev_as_authoritative_over_production_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRR_ENV", raising=False)
    monkeypatch.delenv("TRR_ENVIRONMENT", raising=False)
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "4")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "16")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["requested_minconn"] == 4
    assert sizing["requested_maxconn"] == 16
    assert sizing["minconn"] == 4
    assert sizing["maxconn"] == 8
    assert sizing["session_pooler_override_clamped"] is True
    assert sizing["modal_session_pooler_override_clamped"] is False
    assert sizing["maxconn_source"] == "clamped:local_session_pooler_ceiling"


def test_resolve_pool_sizing_clamps_modal_session_pooler_overrides_without_pytest_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRR_ENV", raising=False)
    monkeypatch.delenv("TRR_ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRR_LOCAL_DEV", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("MODAL_TASK_ID", "ta-123")
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "4")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "16")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["requested_minconn"] == 4
    assert sizing["requested_maxconn"] == 16
    assert sizing["minconn"] == 1
    assert sizing["maxconn"] == 1
    assert sizing["session_pooler_override_clamped"] is False
    assert sizing["modal_session_pooler_override_clamped"] is True
    assert sizing["maxconn_source"] == "clamped:modal_session_pooler_ceiling"


def test_resolve_pool_sizing_caps_local_session_pooler_overrides_at_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "6")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "20")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["requested_minconn"] == 6
    assert sizing["requested_maxconn"] == 20
    assert sizing["minconn"] == 6
    assert sizing["maxconn"] == 8
    assert sizing["session_pooler_override_clamped"] is True
    assert sizing["maxconn_source"] == "clamped:local_session_pooler_ceiling"


def test_resolve_pool_sizing_clamps_modal_social_profile_session_pooler_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.setenv("MODAL_TASK_ID", "ta-123")
    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MINCONN", "4")
    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN", "8")

    sizing = pg._resolve_pool_sizing(
        "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres",
        minconn_env_name="TRR_SOCIAL_PROFILE_DB_POOL_MINCONN",
        maxconn_env_name="TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN",
        pool_name="social_profile",
    )

    assert sizing["requested_minconn"] == 4
    assert sizing["requested_maxconn"] == 8
    assert sizing["minconn"] == 1
    assert sizing["maxconn"] == 1
    assert sizing["session_pooler_override_clamped"] is False
    assert sizing["modal_session_pooler_override_clamped"] is True
    assert sizing["maxconn_source"] == "clamped:modal_session_pooler_ceiling"


def test_get_pool_logs_oversized_session_pool_override_for_local_social_profile(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    session_pooler_dsn = "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    fake_pool = _FakePool()

    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MINCONN", "1")
    monkeypatch.setenv("TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN", "4")
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (
            {
                "url": session_pooler_dsn,
                "source": "TRR_DB_URL",
                "host_class": "pooler",
                "connection_class": "session",
                "host": "aws-1-us-east-1.pooler.supabase.com",
                "port": 5432,
                "database": "postgres",
            },
        ),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with caplog.at_level("INFO", logger="trr_backend.db.pg"):
        with pg.db_read_connection(label="social-profile-summary", pool_name="social_profile"):
            pass

    assert "oversized_session_pool_override" not in caplog.text
    assert "pool_name=social_profile" in caplog.text
    assert "minconn=1 maxconn=4" in caplog.text
    assert "clamped_session_pool_override" not in caplog.text


def test_resolve_pool_sizing_clamps_modal_session_pooler_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.setenv("MODAL_TASK_ID", "ta-123")
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "4")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "16")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["requested_minconn"] == 4
    assert sizing["requested_maxconn"] == 16
    assert sizing["minconn"] == 1
    assert sizing["maxconn"] == 1
    assert sizing["modal_session_pooler_override_clamped"] is True
    assert sizing["minconn_source"] == "clamped:modal_session_pooler_ceiling"
    assert sizing["maxconn_source"] == "clamped:modal_session_pooler_ceiling"


def test_resolve_pool_sizing_keeps_local_clamp_outside_modal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "4")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "16")

    sizing = pg._resolve_pool_sizing("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert sizing["minconn"] == 4
    assert sizing["maxconn"] == 8
    assert sizing["modal_session_pooler_override_clamped"] is False
    assert sizing["session_pooler_override_clamped"] is True


def test_fetch_one_retries_ssl_fault_without_resetting_shared_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"fetch": 0, "reset": 0}

    @contextmanager
    def _fake_read_connection(*, label="read", pool_name="default"):  # noqa: ARG001
        yield _FakeConnection()

    def _fetch_one_with_cursor(_cur, _query, _params=None):
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            raise RuntimeError("SSL connection has been closed unexpectedly")
        return {"ok": True}

    monkeypatch.setattr(pg, "db_read_connection", _fake_read_connection)
    monkeypatch.setattr(pg, "fetch_one_with_cursor", _fetch_one_with_cursor)
    monkeypatch.setattr(
        pg,
        "reset_pool",
        lambda **_kwargs: calls.__setitem__("reset", calls["reset"] + 1),
    )

    result = pg.fetch_one("select 1")

    assert result == {"ok": True}
    assert calls["fetch"] == 2
    assert calls["reset"] == 0


def test_get_connection_with_retry_reraises_last_pool_error_after_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePoolExhaustThenSuccess(failures_before_success=99)

    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)
    monkeypatch.setenv("TRR_DB_POOL_ACQUIRE_ATTEMPTS", "1")
    monkeypatch.setenv("TRR_DB_TRANSIENT_QUERY_ATTEMPTS", "1")

    with pytest.raises(PoolError, match="connection pool exhausted"):
        pg._get_connection_with_retry(label="fetch_one")


def test_fetch_all_retries_closed_cursor_without_resetting_shared_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"fetch": 0, "reset": 0}

    @contextmanager
    def _fake_read_connection(*, label="read", pool_name="default"):  # noqa: ARG001
        yield _FakeConnection()

    def _fetch_all_with_cursor(_cur, _query, _params=None):
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            raise RuntimeError("cursor already closed")
        return [{"ok": True}]

    monkeypatch.setattr(pg, "db_read_connection", _fake_read_connection)
    monkeypatch.setattr(pg, "fetch_all_with_cursor", _fetch_all_with_cursor)
    monkeypatch.setattr(
        pg,
        "reset_pool",
        lambda **_kwargs: calls.__setitem__("reset", calls["reset"] + 1),
    )

    result = pg.fetch_all("select 1")

    assert result == [{"ok": True}]
    assert calls["fetch"] == 2
    assert calls["reset"] == 0


def test_fetch_one_retries_closed_pool_fault_without_global_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"fetch": 0, "reset": 0}

    @contextmanager
    def _fake_read_connection(*, label="read", pool_name="default"):  # noqa: ARG001
        yield _FakeConnection()

    def _fetch_one_with_cursor(_cur, _query, _params=None):
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            raise PoolError("connection pool is closed")
        return {"ok": True}

    monkeypatch.setattr(pg, "db_read_connection", _fake_read_connection)
    monkeypatch.setattr(pg, "fetch_one_with_cursor", _fetch_one_with_cursor)
    monkeypatch.setattr(
        pg,
        "reset_pool",
        lambda **_kwargs: calls.__setitem__("reset", calls["reset"] + 1),
    )

    result = pg.fetch_one("select 1")

    assert result == {"ok": True}
    assert calls["fetch"] == 2
    assert calls["reset"] == 0


def test_db_connection_does_not_mask_errors_when_pool_closes_during_putconn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePoolClosedOnPut()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pytest.raises(RuntimeError, match="boom"):
        with pg.db_connection():
            raise RuntimeError("boom")

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1


def test_db_connection_retries_pool_acquire_without_warning_spam(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_pool = _FakePoolExhaustThenSuccess(failures_before_success=2)
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)
    monkeypatch.setenv("TRR_DB_POOL_ACQUIRE_ATTEMPTS", "3")
    monkeypatch.setenv("TRR_DB_POOL_ACQUIRE_SLEEP_MS", "1")

    with pg.db_connection():
        pass

    assert fake_pool.getconn_calls == 3
    assert fake_pool.putconn_calls == 1
    assert "acquire_failed" not in caplog.text


def test_reset_pool_rotates_active_pool_without_closing_checked_out_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_pool = _FakePool()
    second_pool = _FakePool()
    pools = [first_pool, second_pool]

    def _pool_factory(*_args, **_kwargs):
        return pools.pop(0)

    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    with pg.db_connection() as first_conn:
        assert first_conn is first_pool.connection
        pg.reset_pool()
        assert first_pool.closeall_calls == 0

        with pg.db_connection() as second_conn:
            assert second_conn is second_pool.connection

    assert first_pool.closeall_calls == 1
    assert second_pool.closeall_calls == 0
    assert pg._pool is second_pool


def test_reset_pool_expected_pool_ignores_stale_pool_reference() -> None:
    stale_pool = _FakePool()
    active_pool = _FakePool()
    pg._pool = active_pool
    pg._active_pool_dsn = "postgresql://db.example.com/postgres"

    pg.reset_pool(expected_pool=cast(Any, stale_pool))

    assert pg._pool is active_pool
    assert active_pool.closeall_calls == 0


def test_get_connection_with_retry_uses_new_pool_after_closed_pool_reset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed_pool = _FakePoolClosedOnGetconn()
    replacement_pool = _FakePool()

    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: replacement_pool)
    pg._pool = closed_pool
    pg._active_pool_dsn = "postgresql://db.example.com/postgres"

    pool, conn, checkout_id = pg._get_connection_with_retry(label="fetch_one")

    assert pool is replacement_pool
    assert conn is replacement_pool.connection
    assert checkout_id > 0
    assert closed_pool.closeall_calls == 1
    assert replacement_pool.closeall_calls == 0


def test_fetch_one_uses_social_control_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    fake_pool.connection = _FakeConnection(query_results=[{"id": 1, "name": "control"}])
    pool_names: list[str] = []

    def _get_pool(*, pool_name: str = "default"):
        pool_names.append(pool_name)
        return fake_pool

    monkeypatch.setattr(pg, "_get_pool", _get_pool)

    result = pg.fetch_one("select 1", pool_name="social_control")

    assert result == {"id": 1, "name": "control"}
    assert pool_names == ["social_control"]
    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1


def test_db_connection_closes_connection_after_return_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()

    monkeypatch.setenv("TRR_DB_POOL_CLOSE_AFTER_RETURN", "1")
    monkeypatch.setattr(pg, "_get_pool", lambda *, pool_name="default": fake_pool)

    with pg.db_connection(label="close-after-return"):
        pass

    assert fake_pool.putconn_calls == 1
    assert fake_pool.closed_putconn_calls == 1
    assert fake_pool.connection.closed is True


def test_db_read_connection_closes_connection_after_return_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()

    monkeypatch.setenv("TRR_DB_POOL_CLOSE_AFTER_RETURN", "1")
    monkeypatch.setattr(pg, "_get_pool", lambda *, pool_name="default": fake_pool)

    with pg.db_read_connection(label="read-close-after-return"):
        pass

    assert fake_pool.putconn_calls == 1
    assert fake_pool.closed_putconn_calls == 1
    assert fake_pool.connection.closed is True


def test_fetch_all_uses_social_control_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    fake_pool.connection = _FakeConnection(
        query_results=[
            {"id": 1, "name": "control"},
            {"id": 2, "name": "plane"},
        ],
    )
    pool_names: list[str] = []

    def _get_pool(*, pool_name: str = "default"):
        pool_names.append(pool_name)
        return fake_pool

    monkeypatch.setattr(pg, "_get_pool", _get_pool)

    result = pg.fetch_all("select 1", pool_name="social_control")

    assert result == [
        {"id": 1, "name": "control"},
        {"id": 2, "name": "plane"},
    ]
    assert pool_names == ["social_control"]
    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1


def test_build_pool_for_session_mode_supavisor_uses_conservative_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _pool_factory(*, minconn, maxconn, **kwargs):
        captured["minconn"] = minconn
        captured["maxconn"] = maxconn
        captured["dsn"] = kwargs.get("dsn")
        captured["options"] = kwargs.get("options")
        captured["application_name"] = kwargs.get("application_name")
        captured["connect_timeout"] = kwargs.get("connect_timeout")
        return _FakePool()

    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)
    monkeypatch.delenv("TRR_DB_POOL_MINCONN", raising=False)
    monkeypatch.delenv("TRR_DB_POOL_MAXCONN", raising=False)
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRR_LOCAL_DEV", raising=False)

    pg._build_pool_for_url("postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres")

    assert captured["minconn"] == 1
    assert captured["maxconn"] == 2
    options = captured["options"]
    assert "-c idle_in_transaction_session_timeout=60000" in options
    assert "-c statement_timeout=30000" in options
    assert captured.get("connect_timeout") == 10
    assert captured["application_name"] == "trr-backend:default"


def test_build_pool_for_named_pool_sets_pool_application_name(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _pool_factory(*, minconn, maxconn, **kwargs):
        captured["minconn"] = minconn
        captured["maxconn"] = maxconn
        captured["application_name"] = kwargs.get("application_name")
        return _FakePool()

    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    pg._build_pool_for_url(
        "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:5432/postgres",
        pool_name="health",
    )

    assert captured["application_name"] == "trr-backend:health"


def test_resolve_application_name_rejects_secret_like_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_APPLICATION_NAME", "postgres://user:secret@example.com/db")

    resolved = pg._resolve_application_name(pool_name="social_control")

    assert resolved["application_name"] == "trr-backend:social_control"
    assert resolved["application_name_source"] == "default:pool"


def test_fresh_session_capacity_probe_honors_configured_limit_above_ten(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ProbeConnection(_FakeConnection):
        def close(self) -> None:
            self.closed = True

    connections: list[_ProbeConnection] = []

    def _connect(**_kwargs: object) -> _ProbeConnection:
        connection = _ProbeConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(
        pg,
        "resolve_session_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(pg.psycopg2, "connect", _connect)

    result = pg.probe_fresh_session_capacity(requested_sessions=15, max_probe_sessions=15)

    assert result["available"] is True
    assert result["requested_sessions"] == 15
    assert result["reserved_sessions"] == 15
    assert len(connections) == 15
    assert all(connection.closed for connection in connections)


def test_local_pool_pressure_summary_does_not_expose_pool_details(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(
        pg,
        "resolve_session_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )

    summary = pg.local_pool_pressure_summary()

    assert summary == {
        "status": "ok",
        "reason": "pool_pressure_ok",
        "service": "trr-backend",
    }


def test_local_pool_pressure_snapshot_reports_named_application_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )
    monkeypatch.setattr(
        pg,
        "resolve_session_database_url_candidate_details",
        lambda: (_detail("postgresql://db.example.com/postgres"),),
    )

    snapshot = pg.local_pool_pressure_snapshot()

    application_names = {pool["pool_name"]: pool["application_name"] for pool in snapshot["pools"]}
    assert application_names["default"] == "trr-backend:default"
    assert application_names["social_profile"] == "trr-backend:social_profile"
    assert application_names["social_control"] == "trr-backend:social_control"
    assert application_names["health"] == "trr-backend:health"
    assert application_names["session_control"] == "trr-backend:session_control"


def test_build_pool_for_non_session_urls_keeps_default_pool_size(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _pool_factory(*, minconn, maxconn, **kwargs):
        captured["minconn"] = minconn
        captured["maxconn"] = maxconn
        captured["dsn"] = kwargs.get("dsn")
        captured["options"] = kwargs.get("options")
        captured["connect_timeout"] = kwargs.get("connect_timeout")
        return _FakePool()

    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)
    monkeypatch.delenv("TRR_DB_POOL_MINCONN", raising=False)
    monkeypatch.delenv("TRR_DB_POOL_MAXCONN", raising=False)

    pg._build_pool_for_url("postgresql://db.example.com/postgres")

    assert captured["minconn"] == 2
    assert captured["maxconn"] == 24
    options = captured["options"]
    assert "-c idle_in_transaction_session_timeout=60000" in options
    assert "-c statement_timeout=30000" in options
    assert captured.get("connect_timeout") == 10
