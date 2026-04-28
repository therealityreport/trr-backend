from __future__ import annotations

from scripts import _db_url


def test_to_direct_db_url_converts_session_pooler() -> None:
    url = "postgresql://postgres.vwxfvzutyufrkhfgoeaa:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres"

    assert _db_url.to_direct_db_url(url) == (
        "postgresql://postgres:secret@db.vwxfvzutyufrkhfgoeaa.supabase.co:5432/postgres"
    )


def test_to_direct_db_url_converts_transaction_pooler() -> None:
    url = "postgresql://postgres.vwxfvzutyufrkhfgoeaa:secret@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

    assert _db_url.to_direct_db_url(url) == (
        "postgresql://postgres:secret@db.vwxfvzutyufrkhfgoeaa.supabase.co:5432/postgres"
    )


def test_to_direct_db_url_preserves_local_or_direct_urls() -> None:
    local = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
    direct = "postgresql://postgres.vwxfvzutyufrkhfgoeaa:secret@db.vwxfvzutyufrkhfgoeaa.supabase.co:5432/postgres"

    assert _db_url.to_direct_db_url(local) == local
    assert _db_url.to_direct_db_url(direct) == direct


def test_resolve_direct_db_url_wraps_resolved_db_url(monkeypatch) -> None:
    resolved = _db_url.ResolvedDbUrl(
        value="postgresql://postgres.vwxfvzutyufrkhfgoeaa:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres",
        source="TRR_DB_URL",
    )

    monkeypatch.setattr(_db_url, "resolve_db_url", lambda **_kwargs: resolved)

    out = _db_url.resolve_direct_db_url()

    assert out.source == "TRR_DB_URL"
    assert out.value.endswith("@db.vwxfvzutyufrkhfgoeaa.supabase.co:5432/postgres")


def test_resolve_db_url_prefers_direct_env(monkeypatch) -> None:
    monkeypatch.setenv("TRR_DB_DIRECT_URL", "postgresql://direct")
    monkeypatch.setenv("TRR_DB_SESSION_URL", "postgresql://session")
    monkeypatch.setenv("TRR_DB_URL", "postgresql://compat")

    out = _db_url.resolve_db_url(allow_local_supabase_status=False)

    assert out.source == "TRR_DB_DIRECT_URL"
    assert out.value == "postgresql://direct"
