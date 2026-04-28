from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "supabase/migrations/20260428114333_social_post_canonical_foundation.sql"
PARITY_SCRIPT = REPO_ROOT / "scripts/db/social_post_schema_parity.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_parity_module():
    spec = importlib.util.spec_from_file_location("social_post_schema_parity", PARITY_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_migration_is_additive_and_preserves_legacy_tables() -> None:
    sql = _read(MIGRATION).lower()

    assert "drop table" not in sql
    assert not re.search(r"\balter\s+table\b[^;]*\bdrop\b", sql)
    assert "create table if not exists social.social_posts" in sql
    assert "unique (platform, source_id)" in sql
    assert "unique (platform, id)" in sql
    assert "foreign key (platform, post_id) references social.social_posts(platform, id)" in sql


def test_raw_payloads_are_private_observations_only() -> None:
    sql = _read(MIGRATION).lower()

    assert "raw_payload jsonb" in sql
    assert sql.count("raw_payload jsonb") == 1
    assert "create table if not exists social.social_post_observations" in sql
    assert "grant select on table social.social_post_observations" not in sql
    assert "social_post_observations_public_read" not in sql
    assert "revoke all on table social.social_post_observations from public, anon, authenticated" in sql


def test_public_reads_are_limited_to_sanitized_shared_tables() -> None:
    sql = _read(MIGRATION).lower()

    public_grant_match = re.search(
        r"grant select on table(?P<tables>.*?)to anon, authenticated;",
        sql,
        flags=re.DOTALL,
    )
    assert public_grant_match, "missing sanitized public grant"
    granted_tables = public_grant_match.group("tables")

    for table in (
        "social.social_posts",
        "social.social_post_memberships",
        "social.social_post_entities",
        "social.social_post_media_assets",
    ):
        assert table in granted_tables

    assert "social.social_post_observations" not in granted_tables
    assert "social.social_post_legacy_refs" not in granted_tables

    for policy_name in (
        "social_posts_public_read",
        "social_post_memberships_public_read",
        "social_post_entities_public_read",
        "social_post_media_assets_public_read",
    ):
        assert f"create policy {policy_name}" in sql
        assert "for select to anon, authenticated using (true)" in sql


def test_child_tables_use_composite_platform_post_foreign_keys() -> None:
    sql = _read(MIGRATION).lower()

    assert sql.count("foreign key (platform, post_id) references social.social_posts(platform, id)") >= 5
    assert "primary key (platform, membership_type, membership_key_norm, post_id)" in sql
    assert "primary key (platform, entity_type, entity_key_norm, post_id)" in sql
    assert "unique (platform, post_id, position)" in sql


def test_normalized_lookup_keys_are_required() -> None:
    sql = _read(MIGRATION).lower()

    assert "membership_key_norm text not null" in sql
    assert "entity_key_norm text not null" in sql
    assert "membership_key_norm = lower(btrim(membership_key_norm))" in sql
    assert "entity_key_norm = lower(btrim(entity_key_norm))" in sql
    assert "social_post_memberships_lookup_idx" in sql
    assert "social_post_entities_lookup_idx" in sql


def test_parity_script_covers_all_supported_platform_surfaces() -> None:
    module = _load_parity_module()

    assert module.SUPPORTED_PLATFORMS == (
        "instagram",
        "tiktok",
        "twitter",
        "facebook",
        "threads",
        "youtube",
        "reddit",
    )
    configs = module.PLATFORM_CONFIGS
    assert configs["twitter"].materialized_table == "twitter_tweets"
    assert configs["youtube"].materialized_table == "youtube_videos"
    assert configs["reddit"].materialized_table == "reddit_posts"
    assert configs["reddit"].catalog_table is None
    assert configs["reddit"].community_expr == "t.subreddit"
    assert configs["instagram"].comment.table == "instagram_comments"
    assert configs["youtube"].comment.fk_column == "video_id"


def test_parity_script_reports_shared_schema_and_observation_exposure() -> None:
    script = _read(PARITY_SCRIPT)

    assert "--platform" in script
    assert "--account" in script
    assert "--community" in script
    assert "--json" in script
    assert "shared_schema_available" in script
    assert "social_post_observations" in script
    assert "information_schema.role_table_grants" in script
    assert "pg_policies" in script
