from __future__ import annotations

from pathlib import Path

MIGRATION = Path(__file__).parents[2] / "supabase/migrations/20260710123000_instagram_comments_shared_cooldown.sql"


def test_shared_cooldown_migration_is_additive_and_cleanup_is_guarded() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").lower()

    assert "add column if not exists cooldown_until timestamptz" in sql
    assert "lower(btrim(platform)) = 'instagram'" in sql
    assert "lower(btrim(blocker_kind)) = 'auth'" in sql
    assert "lower(btrim(last_error_code)) = 'database_capacity'" in sql
    assert "delete from" not in sql
    assert "social.scrape_jobs" not in sql
    assert "blocker_kind = 'checkpoint'" not in sql
