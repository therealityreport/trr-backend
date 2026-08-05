from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "supabase/migrations/20260805140512_core_show_slug_normalized_unique_20260805.sql"


def _sql() -> str:
    return re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8").lower()).strip()


def test_migration_guards_duplicates_before_creating_casefolded_unique_index() -> None:
    sql = _sql()

    assert sql.index("begin;") < sql.index("set local lock_timeout")
    assert "set local lock_timeout = '5s'" in sql
    assert "set local statement_timeout = '60s'" in sql
    assert "lock table core.shows in share mode" in sql
    assert "group by lower(btrim(slug))" in sql
    assert "having count(*) > 1" in sql
    assert "resolve them before applying the unique index" in sql
    assert "using errcode = '23505'" in sql
    assert "create unique index if not exists core_shows_slug_normalized_unique" in sql
    assert "on core.shows (lower(btrim(slug)))" in sql
    assert "where slug is not null and btrim(slug) <> ''" in sql
    assert sql.index("lock table core.shows in share mode") < sql.index("having count(*) > 1")
    assert sql.index("having count(*) > 1") < sql.index("create unique index")
    assert sql.index("create unique index") < sql.rindex("commit;")
    assert sql.endswith("commit;")


def test_migration_does_not_silently_rewrite_or_delete_show_rows() -> None:
    sql = _sql()

    assert "update core.shows" not in sql
    assert "delete from core.shows" not in sql
    assert "drop index" not in sql
    assert "alter table core.shows" not in sql
