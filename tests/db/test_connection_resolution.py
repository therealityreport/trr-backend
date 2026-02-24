"""Tests for database URL resolution and cloud fallback candidates."""

from __future__ import annotations

from urllib.parse import urlsplit

import pytest

from trr_backend.db import connection


@pytest.fixture(autouse=True)
def _clear_resolution_cache() -> None:
    connection.resolve_database_url.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()
    yield
    connection.resolve_database_url.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()


@pytest.fixture
def _reset_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("SUPABASE_DB_URL", "TRR_DB_FALLBACK_URL", "DATABASE_URL", "TRR_DB_URL"):
        monkeypatch.delenv(name, raising=False)


def test_resolve_database_url_candidates_includes_pooler_direct_fallback(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    primary_pooler = "postgresql://postgres.abcdefghijklmno:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    explicit_fallback = "postgresql://postgres.fallback:secret@db.fallback.supabase.co:5432/postgres"
    database_url = "postgresql://legacy-user:secret@db.example.com:5432/postgres"
    trr_url = "postgresql://legacy-trr:secret@trr.example.com:5432/postgres"

    monkeypatch.setenv("SUPABASE_DB_URL", primary_pooler)
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", explicit_fallback)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("TRR_DB_URL", trr_url)
    monkeypatch.setattr(
        connection,
        "_get_local_supabase_db_url",
        lambda: "postgresql://local-user:secret@127.0.0.1:54322/postgres",
    )

    candidates = connection.resolve_database_url_candidates()

    assert candidates[0] == primary_pooler
    assert candidates[1] == explicit_fallback

    derived_direct = candidates[2]
    derived_parts = urlsplit(derived_direct)
    assert derived_parts.hostname == "db.abcdefghijklmno.supabase.co"
    assert derived_parts.port == 5432
    assert candidates[3:] == (
        database_url,
        trr_url,
        "postgresql://local-user:secret@127.0.0.1:54322/postgres",
    )


def test_resolve_database_url_returns_first_candidate(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://primary:secret@db.example.com:5432/postgres")
    monkeypatch.setenv("DATABASE_URL", "postgresql://secondary:secret@db2.example.com:5432/postgres")

    assert connection.resolve_database_url() == "postgresql://primary:secret@db.example.com:5432/postgres"


def test_resolve_database_url_raises_without_candidates(
    monkeypatch: pytest.MonkeyPatch,
    _reset_db_env: None,
) -> None:
    monkeypatch.setattr(connection, "_get_local_supabase_db_url", lambda: None)

    with pytest.raises(connection.DatabaseConnectionError):
        connection.resolve_database_url(allow_local_fallback=False)
