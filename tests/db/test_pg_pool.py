"""Tests for psycopg2 connection pooling helpers."""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from psycopg2.pool import PoolError

from trr_backend.db import pg


class _FakeConnection:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


class _FakePool:
    def __init__(self) -> None:
        self.connection = _FakeConnection()
        self.putconn_calls = 0
        self.getconn_calls = 0

    def getconn(self) -> _FakeConnection:
        self.getconn_calls += 1
        return self.connection

    def putconn(self, _conn: _FakeConnection) -> None:
        self.putconn_calls += 1

    def closeall(self) -> None:
        return None


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
    def putconn(self, _conn: _FakeConnection) -> None:
        self.putconn_calls += 1
        raise PoolError("connection pool is closed")


@pytest.fixture(autouse=True)
def _reset_pool_state() -> None:
    pg.close_pool()
    yield
    pg.close_pool()


def test_db_connection_commits_and_returns_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidates",
        lambda: ("postgresql://db.example.com/postgres",),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pg.db_connection():
        pass

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.connection.commit_calls == 1
    assert fake_pool.connection.rollback_calls == 0


def test_db_connection_rolls_back_and_returns_connection_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePool()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidates",
        lambda: ("postgresql://db.example.com/postgres",),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pytest.raises(RuntimeError, match="boom"):
        with pg.db_connection():
            raise RuntimeError("boom")

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1
    assert fake_pool.connection.commit_calls == 0
    assert fake_pool.connection.rollback_calls == 1


def test_pool_init_falls_back_after_transient_dns_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    primary = "postgresql://postgres.ref:pw@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    fallback = "postgresql://postgres.ref:pw@db.ref.supabase.co:5432/postgres"
    calls: list[str] = []
    fake_pool = _FakePool()

    def _pool_factory(*_args, **kwargs):
        dsn = kwargs.get("dsn")
        calls.append(dsn)
        if dsn == primary:
            raise RuntimeError("getaddrinfo ENOTFOUND aws-1-us-east-1.pooler.supabase.com")
        return fake_pool

    monkeypatch.setattr(pg, "resolve_database_url_candidates", lambda: (primary, fallback))
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _pool_factory)

    with pg.db_connection():
        pass

    assert calls == [primary, fallback]
    assert pg.current_pool_dsn() == fallback


def test_fetch_one_retries_once_on_transient_transport_fault(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"fetch": 0, "reset": 0}

    @contextmanager
    def _fake_cursor(*, conn=None):  # noqa: ARG001
        yield object()

    def _fetch_one_with_cursor(_cur, _query, _params=None):
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            raise RuntimeError("SSL SYSCALL error: EOF detected")
        return {"ok": True}

    monkeypatch.setattr(pg, "db_cursor", _fake_cursor)
    monkeypatch.setattr(pg, "fetch_one_with_cursor", _fetch_one_with_cursor)
    monkeypatch.setattr(pg, "reset_pool", lambda: calls.__setitem__("reset", calls["reset"] + 1))

    result = pg.fetch_one("select 1")

    assert result == {"ok": True}
    assert calls["fetch"] == 2
    assert calls["reset"] == 1


def test_fetch_one_retries_once_on_ssl_connection_closed_fault(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"fetch": 0, "reset": 0}

    @contextmanager
    def _fake_cursor(*, conn=None):  # noqa: ARG001
        yield object()

    def _fetch_one_with_cursor(_cur, _query, _params=None):
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            raise RuntimeError("SSL connection has been closed unexpectedly")
        return {"ok": True}

    monkeypatch.setattr(pg, "db_cursor", _fake_cursor)
    monkeypatch.setattr(pg, "fetch_one_with_cursor", _fetch_one_with_cursor)
    monkeypatch.setattr(pg, "reset_pool", lambda: calls.__setitem__("reset", calls["reset"] + 1))

    result = pg.fetch_one("select 1")

    assert result == {"ok": True}
    assert calls["fetch"] == 2
    assert calls["reset"] == 1


def test_fetch_all_retries_once_on_closed_cursor_fault(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"fetch": 0, "reset": 0}

    @contextmanager
    def _fake_cursor(*, conn=None):  # noqa: ARG001
        yield object()

    def _fetch_all_with_cursor(_cur, _query, _params=None):
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            raise RuntimeError("cursor already closed")
        return [{"ok": True}]

    monkeypatch.setattr(pg, "db_cursor", _fake_cursor)
    monkeypatch.setattr(pg, "fetch_all_with_cursor", _fetch_all_with_cursor)
    monkeypatch.setattr(pg, "reset_pool", lambda: calls.__setitem__("reset", calls["reset"] + 1))

    result = pg.fetch_all("select 1")

    assert result == [{"ok": True}]
    assert calls["fetch"] == 2
    assert calls["reset"] == 1


def test_db_connection_does_not_mask_errors_when_pool_closes_during_putconn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = _FakePoolClosedOnPut()
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidates",
        lambda: ("postgresql://db.example.com/postgres",),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)

    with pytest.raises(RuntimeError, match="boom"):
        with pg.db_connection():
            raise RuntimeError("boom")

    assert fake_pool.getconn_calls == 1
    assert fake_pool.putconn_calls == 1


def test_db_connection_retries_pool_acquire_on_pool_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pool = _FakePoolExhaustThenSuccess(failures_before_success=2)
    monkeypatch.setattr(
        pg,
        "resolve_database_url_candidates",
        lambda: ("postgresql://db.example.com/postgres",),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: fake_pool)
    monkeypatch.setenv("TRR_DB_POOL_ACQUIRE_ATTEMPTS", "3")
    monkeypatch.setenv("TRR_DB_POOL_ACQUIRE_SLEEP_MS", "1")

    with pg.db_connection():
        pass

    assert fake_pool.getconn_calls == 3
    assert fake_pool.putconn_calls == 1
