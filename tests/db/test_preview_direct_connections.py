"""Focused coverage for direct PostgreSQL connections in read-only preview mode."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from trr_backend.db import connection, pg, postgrest_cache


class _Cursor:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str) -> None:
        self._connection.executed_sql.append(statement)
        if statement == self._connection.fail_on_statement:
            raise RuntimeError(f"intentional failure for {statement}")

    def fetchone(self) -> object | None:
        if not self._connection.results:
            return None
        return self._connection.results.pop(0)


class _Connection:
    def __init__(
        self,
        results: list[object | None],
        *,
        fail_on_statement: str | None = None,
    ) -> None:
        self.results = list(results)
        self.executed_sql: list[str] = []
        self.autocommit_history: list[bool] = []
        self._autocommit = False
        self.closed = False
        self.fail_on_statement = fail_on_statement

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self.autocommit_history.append(value)
        self._autocommit = value

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def get_transaction_status(self) -> int:
        return 0

    def close(self) -> None:
        self.closed = True


class _Pool:
    def __init__(self, connection_instance: _Connection) -> None:
        self.connection = connection_instance
        self.getconn_calls = 0
        self.putconn_calls = 0
        self.closeall_calls = 0

    def getconn(self) -> _Connection:
        self.getconn_calls += 1
        return self.connection

    def putconn(self, _connection: _Connection) -> None:
        self.putconn_calls += 1

    def closeall(self) -> None:
        self.closeall_calls += 1


@pytest.fixture(autouse=True)
def _clear_preview_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_PREVIEW_READ_ONLY", raising=False)


def test_preview_pool_is_asserted_read_only_before_return(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _Pool(_Connection([("on",)]))
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: pool)

    result = pg._build_pool_for_url("postgresql://database.example/postgres")

    assert result is pool
    assert pool.connection.executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]
    assert pool.connection.autocommit_history == [True, False]
    assert pool.getconn_calls == 1
    assert pool.putconn_calls == 1
    assert pool.closeall_calls == 0


def test_preview_pool_fails_closed_when_read_only_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _Pool(_Connection([("off",)]))
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(pg, "ThreadedConnectionPool", lambda *args, **kwargs: pool)

    with pytest.raises(connection.PreviewReadOnlyError, match="transaction_read_only=on"):
        pg._build_pool_for_url("postgresql://database.example/postgres")

    assert pool.connection.executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]
    assert pool.connection.autocommit_history == [True, False]
    assert pool.getconn_calls == 1
    assert pool.putconn_calls == 1
    assert pool.closeall_calls == 1


def test_preview_fresh_session_probe_wraps_and_asserts_each_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[_Connection] = []
    captured_kwargs: list[dict[str, Any]] = []

    def _connect(**kwargs: object) -> _Connection:
        captured_kwargs.append(dict(kwargs))
        instance = _Connection([("on",), (1,)])
        created.append(instance)
        return instance

    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(
        pg,
        "resolve_session_database_url_candidate_details",
        lambda: ({"url": "postgresql://database.example/postgres?options=-c%20lock_timeout%3D2500"},),
    )
    monkeypatch.setattr(pg.psycopg2, "connect", _connect)

    result = pg.probe_fresh_session_capacity(requested_sessions=2)

    assert result["available"] is True
    assert len(created) == 2
    assert all(item.closed for item in created)
    assert all(
        item.executed_sql
        == [
            "SET default_transaction_read_only = on",
            "SHOW transaction_read_only",
            "select 1",
        ]
        for item in created
    )
    assert all(item.autocommit_history == [True] for item in created)
    assert all(
        item["options"] == "-c lock_timeout=2500 -c default_transaction_read_only=on" for item in captured_kwargs
    )


def test_preview_fresh_session_probe_fails_closed_when_read_only_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[_Connection] = []

    def _connect(**_kwargs: object) -> _Connection:
        instance = _Connection([("off",)])
        created.append(instance)
        return instance

    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(
        pg,
        "resolve_session_database_url_candidate_details",
        lambda: ({"url": "postgresql://database.example/postgres"},),
    )
    monkeypatch.setattr(pg.psycopg2, "connect", _connect)

    result = pg.probe_fresh_session_capacity(requested_sessions=1)

    assert result["available"] is False
    assert result["blocked"] is True
    assert result["error"] == "PreviewReadOnlyError"
    assert created[0].closed is True
    assert created[0].executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]
    assert created[0].autocommit_history == [True]


def test_preview_ingestion_readiness_skips_cache_reload_after_read_only_check(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[_Connection] = []
    captured_kwargs: list[dict[str, Any]] = []

    def _connect(_url: str, **kwargs: object) -> _Connection:
        captured_kwargs.append(dict(kwargs))
        instance = _Connection([("on",), (1,)])
        created.append(instance)
        return instance

    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(connection.psycopg2, "connect", _connect)

    connection.ensure_ready_for_ingestion("postgresql://database.example/postgres", reload_schema_cache=True)

    assert len(created) == 1
    assert created[0].closed is True
    assert created[0].executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
        "SELECT 1 FROM pg_namespace WHERE nspname = 'core';",
    ]
    assert created[0].autocommit_history == [True, False]
    assert captured_kwargs == [{"options": "-c default_transaction_read_only=on"}]


def test_preview_ingestion_readiness_fails_closed_when_read_only_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    created = _Connection([("off",)])
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(connection.psycopg2, "connect", lambda *_args, **_kwargs: created)

    with pytest.raises(connection.DatabaseConnectionError, match="transaction_read_only=on"):
        connection.ensure_ready_for_ingestion("postgresql://database.example/postgres", reload_schema_cache=False)

    assert created.closed is True
    assert created.executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]
    assert created.autocommit_history == [True, False]


def test_preview_postgrest_schema_check_wraps_and_asserts_direct_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    created = _Connection([("on",), (1,)])
    captured_kwargs: list[dict[str, Any]] = []

    def _connect(_url: str, **kwargs: object) -> _Connection:
        captured_kwargs.append(dict(kwargs))
        return created

    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(postgrest_cache.psycopg2, "connect", _connect)

    assert postgrest_cache.verify_core_schema_exists("postgresql://database.example/postgres") is True
    assert created.closed is True
    assert created.executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
        "SELECT 1 FROM pg_namespace WHERE nspname = 'core';",
    ]
    assert created.autocommit_history == [True, False]
    assert captured_kwargs == [{"options": "-c default_transaction_read_only=on"}]


def test_preview_postgrest_cache_reload_is_refused_after_read_only_check(monkeypatch: pytest.MonkeyPatch) -> None:
    created = _Connection([("on",)])
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(postgrest_cache.psycopg2, "connect", lambda *_args, **_kwargs: created)

    with pytest.raises(postgrest_cache.PostgrestCacheError, match="disabled in read-only preview"):
        postgrest_cache.reload_postgrest_schema("postgresql://database.example/postgres")

    assert created.closed is True
    assert created.executed_sql == [
        "SET default_transaction_read_only = on",
        "SHOW transaction_read_only",
    ]


def test_preview_connection_active_guard_restores_autocommit_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _Connection([("on",)])
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")

    connection.assert_preview_connection_read_only(conn, label="direct success")

    assert conn.executed_sql == ["SET default_transaction_read_only = on", "SHOW transaction_read_only"]
    assert conn.autocommit is False
    assert conn.autocommit_history == [True, False]


@pytest.mark.parametrize(
    ("failing_statement", "expected_sql"),
    [
        ("SET default_transaction_read_only = on", ["SET default_transaction_read_only = on"]),
        (
            "SHOW transaction_read_only",
            ["SET default_transaction_read_only = on", "SHOW transaction_read_only"],
        ),
    ],
)
def test_preview_connection_active_guard_fails_closed_and_restores_autocommit(
    monkeypatch: pytest.MonkeyPatch,
    failing_statement: str,
    expected_sql: list[str],
) -> None:
    conn = _Connection([], fail_on_statement=failing_statement)
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")

    with pytest.raises(connection.PreviewReadOnlyError, match="could not enforce transaction_read_only=on"):
        connection.assert_preview_connection_read_only(conn, label="direct failure")

    assert conn.executed_sql == expected_sql
    assert conn.autocommit is False
    assert conn.autocommit_history == [True, False]


def test_preview_connection_active_guard_fails_closed_when_show_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _Connection([("off",)])
    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")

    with pytest.raises(connection.PreviewReadOnlyError, match="requires transaction_read_only=on"):
        connection.assert_preview_connection_read_only(conn, label="direct off")

    assert conn.executed_sql == ["SET default_transaction_read_only = on", "SHOW transaction_read_only"]
    assert conn.autocommit is False
    assert conn.autocommit_history == [True, False]


def test_preview_connection_active_guard_is_a_normal_mode_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _Connection([("off",)])
    monkeypatch.delenv("TRR_PREVIEW_READ_ONLY", raising=False)

    connection.assert_preview_connection_read_only(conn, label="normal runtime")

    assert conn.executed_sql == []
    assert conn.autocommit_history == []


def test_runtime_direct_psycopg_connectors_are_all_preview_guarded() -> None:
    """Keep every runtime direct connector bound to the shared preview guard."""
    backend_root = Path(pg.__file__).resolve().parents[1]
    calls: list[ast.Call] = []

    for source_path in sorted(backend_root.rglob("*.py")):
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        calls.extend(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "psycopg2"
            and node.func.attr == "connect"
        )

    assert len(calls) == 5
    for call in calls:
        assert any(
            keyword.arg is None
            and isinstance(keyword.value, ast.Call)
            and isinstance(keyword.value.func, ast.Name)
            and keyword.value.func.id == "preview_read_only_connect_kwargs"
            for keyword in call.keywords
        )
