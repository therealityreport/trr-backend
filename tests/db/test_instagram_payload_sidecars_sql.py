from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "supabase/migrations/20260713150000_instagram_current_payload_sidecars.sql"
ROLLBACK = (
    REPO_ROOT / "docs/db/instagram-catalog-performance/20260713150000_instagram_current_payload_sidecars_rollback.sql"
)


def _sql() -> str:
    return re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8").lower())


def test_sidecars_are_one_to_one_cascade_tables_with_exact_payload_defaults() -> None:
    sql = _sql()
    assert "post_id uuid primary key references social.instagram_posts(id) on delete cascade" in sql
    assert (
        "catalog_post_id uuid primary key references social.instagram_account_catalog_posts(id) on delete cascade"
        in sql
    )
    assert "add constraint %i primary key (%i)" in sql
    assert "foreign key (%i) references social.%i(id) on delete cascade" in sql
    assert "alter column asset_manifest set default '{}'::jsonb" in sql
    assert sql.count("alter column child_posts_data set default '[]'::jsonb") == 2
    assert "alter column raw_data set default '{}'::jsonb" in sql
    assert sql.count("alter column payload_updated_at set default now()") == 2
    assert "create index" not in sql


def test_partial_experimental_shapes_are_reconciled_without_dropping_tables_or_rows() -> None:
    sql = _sql()
    for column in ("post_id", "raw_data", "asset_manifest", "child_posts_data", "payload_updated_at"):
        assert f"add column if not exists {column}" in sql
    assert "('instagram_post_payloads', 'post_id', 'uuid')" in sql
    assert "('instagram_post_payloads', 'payload_updated_at', 'timestamp with time zone')" in sql
    assert "('instagram_account_catalog_post_payloads', 'catalog_post_id', 'uuid')" in sql
    assert "('instagram_account_catalog_post_payloads', 'payload_updated_at', 'timestamp with time zone')" in sql
    assert "format('%i::text::uuid', item.column_name)" in sql
    assert "format('%i::text::jsonb', item.column_name)" in sql
    assert "format('%i at time zone ''utc''', item.column_name)" in sql
    assert "cannot safely cast social.%.% to %" in sql
    assert "where asset_manifest is null" in sql
    assert sql.count("where child_posts_data is null") == 2
    assert sql.count("where payload_updated_at is null") == 2
    assert "constraint_row.conkey = array[key_attnum]::smallint[]" in sql
    assert "constraint_row.confdeltype = 'c'" in sql
    assert sql.count("alter column %i set not null") == 1
    assert "row(s) have null keys" in sql
    assert "duplicate key value(s)" in sql
    assert "orphan row(s)" in sql
    assert "alter column post_id drop default" in sql
    assert "alter column catalog_post_id drop default" in sql
    assert "alter column raw_data drop not null" in sql
    assert "delete from social.instagram_post_payloads" not in sql
    assert "delete from social.instagram_account_catalog_post_payloads" not in sql
    assert "drop table" not in sql


def test_sidecars_are_private_and_service_role_only() -> None:
    sql = _sql()
    for table in (
        "social.instagram_post_payloads",
        "social.instagram_account_catalog_post_payloads",
    ):
        assert f"alter table {table} enable row level security" in sql
        assert table in sql
    assert "from public, anon, authenticated" in sql
    assert "to service_role" in sql
    assert "create policy" not in sql


def test_schema_rollback_is_explicit_and_does_not_touch_legacy_payloads() -> None:
    sql = ROLLBACK.read_text(encoding="utf-8").lower()
    assert "drop table if exists social.instagram_account_catalog_post_payloads" in sql
    assert "drop table if exists social.instagram_post_payloads" in sql
    assert "update social.instagram_posts" not in sql
    assert "update social.instagram_account_catalog_posts" not in sql
