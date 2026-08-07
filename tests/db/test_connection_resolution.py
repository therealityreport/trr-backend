"""Tests for database URL resolution and cloud fallback candidates."""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

import pytest

from trr_backend.db import connection


@pytest.fixture(autouse=True)
def _clear_resolution_cache() -> Iterator[None]:
    connection.resolve_database_url.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()
    connection.resolve_database_url_candidate_details.cache_clear()
    yield
    connection.resolve_database_url.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()
    connection.resolve_database_url_candidate_details.cache_clear()


@pytest.fixture
def _reset_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "TRR_DB_DIRECT_URL",
        "TRR_DB_TRANSACTION_URL",
        "TRR_DB_SESSION_URL",
        "TRR_DB_FALLBACK_URL",
        "TRR_DB_URL",
        "TRR_DB_RUNTIME_LANE",
        "TRR_DB_TRANSACTION_FLIGHT_TEST",
        "SUPABASE_DB_URL",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(name, raising=False)


def test_resolve_database_url_candidates_prefers_trr_runtime_envs(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    primary_pooler = "postgresql://postgres.abcdefghijklmno:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    explicit_fallback = "postgresql://postgres.fallback:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    monkeypatch.setenv("TRR_DB_URL", primary_pooler)
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", explicit_fallback)

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (
        primary_pooler,
        explicit_fallback,
    )


def test_resolve_database_url_candidates_prefers_explicit_direct_lane(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    direct_url = "postgresql://postgres:secret@db.abcdefghijklmno.supabase.co:5432/postgres"
    session_pooler = "postgresql://postgres.session:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    compatibility_pooler = "postgresql://postgres.compat:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    explicit_fallback = "postgresql://postgres.fallback:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    monkeypatch.setenv("TRR_DB_DIRECT_URL", direct_url)
    monkeypatch.setenv("TRR_DB_SESSION_URL", session_pooler)
    monkeypatch.setenv("TRR_DB_URL", compatibility_pooler)
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", explicit_fallback)

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (direct_url, session_pooler, compatibility_pooler, explicit_fallback)


def test_resolve_database_url_candidates_prefers_explicit_session_lane(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    session_pooler = "postgresql://postgres.session:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    compatibility_pooler = "postgresql://postgres.compat:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    monkeypatch.setenv("TRR_DB_SESSION_URL", session_pooler)
    monkeypatch.setenv("TRR_DB_URL", compatibility_pooler)

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (session_pooler, compatibility_pooler)


def test_transaction_url_is_ignored_without_flight_test(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    transaction_pooler = "postgresql://postgres.tx:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    session_pooler = "postgresql://postgres.session:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    monkeypatch.setenv("TRR_DB_RUNTIME_LANE", "transaction")
    monkeypatch.setenv("TRR_DB_TRANSACTION_URL", transaction_pooler)
    monkeypatch.setenv("TRR_DB_SESSION_URL", session_pooler)

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (session_pooler,)


def test_transaction_url_is_first_for_explicit_flight_test(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    transaction_pooler = "postgresql://postgres.tx:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    session_pooler = "postgresql://postgres.session:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    monkeypatch.setenv("TRR_DB_RUNTIME_LANE", "transaction")
    monkeypatch.setenv("TRR_DB_TRANSACTION_FLIGHT_TEST", "1")
    monkeypatch.setenv("TRR_DB_TRANSACTION_URL", transaction_pooler)
    monkeypatch.setenv("TRR_DB_SESSION_URL", session_pooler)

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (
        transaction_pooler,
        connection.derive_supavisor_transaction_url(session_pooler),
    )


def test_transaction_lane_derives_url_from_validated_session_pooler(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    session_pooler = (
        "postgresql://postgres.session:p%40ss@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )
    monkeypatch.setenv("TRR_DB_RUNTIME_LANE", "transaction")
    monkeypatch.setenv("TRR_DB_TRANSACTION_FLIGHT_TEST", "1")
    monkeypatch.setenv("TRR_DB_SESSION_URL", session_pooler)

    details = connection.resolve_database_url_candidate_details()

    assert details[0]["url"] == session_pooler.replace(":5432/", ":6543/")
    assert details[0]["source"] == "TRR_DB_SESSION_URL:derived_transaction"
    assert details[0]["connection_class"] == "transaction"
    assert connection.resolve_session_database_url_candidate_details()[0]["url"] == session_pooler


@pytest.mark.parametrize(
    "url",
    (
        "postgresql://postgres:secret@example.com:5432/postgres",
        "postgresql://postgres:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres",
        "https://aws-1-us-east-1.pooler.supabase.com:5432/postgres",
    ),
)
def test_transaction_url_derivation_rejects_non_session_supavisor_urls(url: str) -> None:
    with pytest.raises(ValueError, match="Supavisor session URL"):
        connection.derive_supavisor_transaction_url(url)


def test_derived_direct_never_produced_at_runtime(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    """Derived direct-host candidates must never appear in runtime resolution."""
    trr_pooler = "postgresql://postgres.asdfgh789012:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    database_pooler = "postgresql://postgres.qwerty123456:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

    monkeypatch.setenv("TRR_DB_URL", trr_pooler)
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", database_pooler)

    candidates = connection.resolve_database_url_candidates()

    # Only the two explicit URLs should appear — no derived direct hosts.
    assert candidates == (trr_pooler, database_pooler)
    for detail in connection.resolve_database_url_candidate_details():
        assert ":derived_direct" not in str(detail.get("source", ""))


def test_resolve_database_url_candidate_details_reports_source_and_host_class(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    pooler_url = "postgresql://postgres.abcdefghijklmno:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    monkeypatch.setenv("TRR_DB_URL", pooler_url)

    details = connection.resolve_database_url_candidate_details()

    assert details == (
        {
            "url": pooler_url,
            "source": "TRR_DB_URL",
            "host_class": "pooler",
            "connection_class": "session",
            "host": "aws-1-us-east-1.pooler.supabase.com",
            "port": 5432,
            "database": "postgres",
        },
    )


def test_resolve_database_url_returns_first_candidate(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://primary:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres")
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", "postgresql://secondary:secret@db2.example.com:5432/postgres")

    assert (
        connection.resolve_database_url()
        == "postgresql://primary:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    )


def test_resolve_database_url_raises_without_candidates(
    _reset_db_env: None,
) -> None:
    with pytest.raises(connection.DatabaseConnectionError):
        connection.resolve_database_url()


class _FakeCursor:
    def __init__(self, result: object | None = None) -> None:
        self.executed: list[str] = []
        self.result = result

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str) -> None:
        self.executed.append(statement)

    def fetchone(self) -> object | None:
        return self.result


class _FakeConnection:
    def __init__(self, result: object | None = None) -> None:
        self.cursor_instance = _FakeCursor(result)
        self.autocommit = False
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_ensure_ready_for_ingestion_keeps_core_check_and_cache_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema_connection = _FakeConnection(result=(1,))
    reload_connection = _FakeConnection()
    connections = [schema_connection, reload_connection]

    monkeypatch.setattr(connection.psycopg2, "connect", lambda _url: connections.pop(0))

    connection.ensure_ready_for_ingestion("postgresql://database.example/postgres")

    assert schema_connection.cursor_instance.executed == ["SELECT 1 FROM pg_namespace WHERE nspname = 'core';"]
    assert schema_connection.closed is True
    assert reload_connection.cursor_instance.executed == ["SELECT pg_notify('pgrst', 'reload schema');"]
    assert reload_connection.autocommit is True
    assert reload_connection.closed is True


def test_ensure_ready_for_ingestion_preserves_schema_and_reload_failure_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_schema_connection = _FakeConnection()
    monkeypatch.setattr(connection.psycopg2, "connect", lambda _url: missing_schema_connection)

    with pytest.raises(connection.DatabaseConnectionError, match="core` schema not found"):
        connection.ensure_ready_for_ingestion("postgresql://database.example/postgres")

    schema_connection = _FakeConnection(result=(1,))
    connect_calls = 0

    def _connect(_url: str) -> _FakeConnection:
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls == 1:
            return schema_connection
        raise connection.psycopg2.OperationalError("reload unavailable")

    monkeypatch.setattr(connection.psycopg2, "connect", _connect)

    connection.ensure_ready_for_ingestion("postgresql://database.example/postgres")
    assert schema_connection.closed is True


def test_connection_module_does_not_import_postgrest_cache() -> None:
    source = Path(connection.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert not any(
        isinstance(node, ast.ImportFrom) and node.module == "trr_backend.db.postgrest_cache" for node in ast.walk(tree)
    )


class TestLegacyEnvsRejected:
    """Legacy runtime envs (SUPABASE_DB_URL, DATABASE_URL) and supabase status
    must not influence persistent service startup."""

    def test_supabase_db_url_ignored_at_runtime(
        self,
        monkeypatch: pytest.MonkeyPatch,
        _reset_db_env: None,
    ) -> None:
        monkeypatch.setenv(
            "SUPABASE_DB_URL",
            "postgresql://postgres.legacy:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres",
        )
        candidates = connection.resolve_database_url_candidate_details()
        assert candidates == ()

    def test_database_url_ignored_at_runtime(
        self,
        monkeypatch: pytest.MonkeyPatch,
        _reset_db_env: None,
    ) -> None:
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql://postgres.tooling:secret@db2.example.com:5432/postgres",
        )
        candidates = connection.resolve_database_url_candidate_details()
        assert candidates == ()

    def test_supabase_status_ignored_at_runtime(
        self,
        _reset_db_env: None,
    ) -> None:
        candidates = connection.resolve_database_url_candidate_details()
        assert candidates == ()
