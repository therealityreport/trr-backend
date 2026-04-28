from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "supabase/migrations/20260428163345_enable_index_advisor_extension.sql"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8").lower()


def test_index_advisor_extension_is_created_in_extensions_schema() -> None:
    sql = _read(MIGRATION)

    assert "create schema if not exists extensions" in sql
    assert "create extension if not exists index_advisor" in sql
    assert re.search(r"create\s+extension\s+if\s+not\s+exists\s+index_advisor\s+with\s+schema\s+extensions", sql)
    assert "comment on extension index_advisor" in sql


def test_index_advisor_extension_is_not_created_in_public_schema() -> None:
    sql = _read(MIGRATION)

    assert "with schema public" not in sql
    assert not re.search(r"create\s+extension\s+if\s+not\s+exists\s+index_advisor\s*;", sql)
    assert "set search_path" not in sql
