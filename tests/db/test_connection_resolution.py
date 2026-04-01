"""Tests for database URL resolution and cloud fallback candidates."""

from __future__ import annotations

import pytest

from trr_backend.db import connection


@pytest.fixture(autouse=True)
def _clear_resolution_cache() -> None:
    connection.resolve_database_url.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()
    connection.resolve_database_url_candidate_details.cache_clear()
    yield
    connection.resolve_database_url.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()
    connection.resolve_database_url_candidate_details.cache_clear()


@pytest.fixture
def _reset_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("TRR_DB_FALLBACK_URL", "TRR_DB_URL", "SUPABASE_DB_URL", "DATABASE_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("TRR_DB_ENABLE_DIRECT_FALLBACK", raising=False)


def test_resolve_database_url_candidates_prefers_trr_runtime_envs(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    primary_pooler = "postgresql://postgres.abcdefghijklmno:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    explicit_fallback = "postgresql://postgres.fallback:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    monkeypatch.setenv("TRR_DB_URL", primary_pooler)
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", explicit_fallback)
    monkeypatch.setattr(
        connection,
        "_get_local_supabase_db_url",
        lambda: "postgresql://local-user:secret@127.0.0.1:54322/postgres",
    )

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (
        primary_pooler,
        explicit_fallback,
        "postgresql://local-user:secret@127.0.0.1:54322/postgres",
    )


def test_resolve_database_url_candidates_can_append_direct_fallbacks_when_explicitly_enabled(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    database_pooler = "postgresql://postgres.qwerty123456:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    trr_pooler = "postgresql://postgres.asdfgh789012:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

    monkeypatch.setenv("TRR_DB_ENABLE_DIRECT_FALLBACK", "1")
    monkeypatch.setenv("TRR_DB_URL", trr_pooler)
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", database_pooler)
    monkeypatch.setattr(connection, "_get_local_supabase_db_url", lambda: None)

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (
        trr_pooler,
        database_pooler,
        "postgresql://postgres.asdfgh789012:secret@db.asdfgh789012.supabase.co:5432/postgres",
        "postgresql://postgres.qwerty123456:secret@db.qwerty123456.supabase.co:5432/postgres",
    )


def test_resolve_database_url_candidate_details_reports_source_and_host_class(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    pooler_url = "postgresql://postgres.abcdefghijklmno:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    monkeypatch.setenv("TRR_DB_URL", pooler_url)
    monkeypatch.setattr(connection, "_get_local_supabase_db_url", lambda: None)

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


def test_resolve_database_url_candidates_accepts_legacy_runtime_envs_as_lower_priority(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    legacy_pooler = "postgresql://postgres.legacy:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"
    tooling_url = "postgresql://postgres.tooling:secret@db2.example.com:5432/postgres"

    monkeypatch.setenv("SUPABASE_DB_URL", legacy_pooler)
    monkeypatch.setenv("DATABASE_URL", tooling_url)
    monkeypatch.setattr(connection, "_get_local_supabase_db_url", lambda: None)

    candidates = connection.resolve_database_url_candidate_details(allow_local_fallback=False)

    assert candidates == (
        {
            "url": legacy_pooler,
            "source": "SUPABASE_DB_URL",
            "host_class": "pooler",
            "connection_class": "session",
            "host": "aws-1-us-east-1.pooler.supabase.com",
            "port": 5432,
            "database": "postgres",
        },
        {
            "url": tooling_url,
            "source": "DATABASE_URL",
            "host_class": "other",
            "connection_class": "other",
            "host": "db2.example.com",
            "port": 5432,
            "database": "postgres",
        },
    )


def test_resolve_database_url_raises_without_candidates(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    monkeypatch.setattr(connection, "_get_local_supabase_db_url", lambda: None)

    with pytest.raises(connection.DatabaseConnectionError):
        connection.resolve_database_url(allow_local_fallback=False)
