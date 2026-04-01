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

    candidates = connection.resolve_database_url_candidates()

    assert candidates == (
        primary_pooler,
        explicit_fallback,
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
