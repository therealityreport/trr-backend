from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "supabase/migrations/20260428145222_instagram_queryable_profile_schema.sql"
COMMENT_PHASE_MIGRATION = REPO_ROOT / "supabase/migrations/20260505173000_instagram_comment_api_phase_metadata.sql"


def _read_sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _normalized_sql() -> str:
    return re.sub(r"\s+", " ", _read_sql().lower())


def _read_comment_phase_sql() -> str:
    return COMMENT_PHASE_MIGRATION.read_text(encoding="utf-8")


def _normalized_comment_phase_sql() -> str:
    return re.sub(r"\s+", " ", _read_comment_phase_sql().lower())


def _table_body(table_name: str) -> str:
    pattern = rf"create table if not exists social\.{table_name}\s*\((?P<body>.*?)\n\);"
    match = re.search(pattern, _read_sql(), flags=re.IGNORECASE | re.DOTALL)
    assert match, f"missing social.{table_name}"
    return match.group("body").lower()


def test_profile_tables_and_required_columns_exist() -> None:
    profiles = _table_body("instagram_profiles")
    external_links = _table_body("instagram_profile_external_links")
    relationships = _table_body("instagram_profile_relationships")

    for column in (
        "shared_account_source_id uuid references social.shared_account_sources(id) on delete set null",
        "source_scope text not null default 'bravo'",
        "source_account text",
        "profile_id text",
        "input_url text",
        "username text",
        "normalized_username text",
        "url text",
        "full_name text",
        "biography text",
        "country text",
        "date_joined text",
        "date_joined_at timestamptz",
        "date_verified text",
        "date_verified_at timestamptz",
        "former_usernames_count integer",
        "followers_count bigint",
        "follows_count bigint",
        "posts_count bigint",
        "highlight_reel_count integer",
        "igtv_video_count integer",
        "is_business_account boolean",
        "joined_recently boolean",
        "has_channel boolean",
        "business_category_name text",
        "is_private boolean",
        "is_verified boolean",
        "external_url text",
        "external_url_shimmed text",
        "profile_pic_url text",
        "profile_pic_url_hd text",
        "hosted_profile_pic_url text",
        "hosted_profile_pic_url_hd text",
        "about_raw jsonb not null default '{}'::jsonb",
        "raw_data jsonb not null default '{}'::jsonb",
        "last_scrape_job_id uuid references social.scrape_jobs(id) on delete set null",
        "last_scrape_run_id uuid references social.scrape_runs(id) on delete set null",
    ):
        assert column in profiles

    for column in (
        "profile_id uuid not null references social.instagram_profiles(id) on delete cascade",
        "link_index integer not null default 0",
        "url text not null",
        "shim_url text",
        "normalized_url text",
        "normalized_domain text",
        "link_type text",
        "raw_data jsonb not null default '{}'::jsonb",
        "unique (profile_id, link_index, url)",
    ):
        assert column in external_links

    for column in (
        "owner_profile_id uuid not null references social.instagram_profiles(id) on delete cascade",
        "owner_instagram_profile_id text",
        "owner_username text",
        "owner_normalized_username text",
        "related_user_id text",
        "related_username text not null",
        "related_normalized_username text",
        "related_full_name text",
        "related_is_private boolean",
        "related_is_verified boolean",
        "related_profile_pic_url text",
        "hosted_related_profile_pic_url text",
        "raw_data jsonb not null default '{}'::jsonb",
        "source_page_ordinal integer",
        "source_cursor text",
        "source_page_size integer",
        "source_rank integer",
        "is_missing boolean not null default false",
    ):
        assert column in relationships


def test_profile_identity_indexes_are_partial_unique_indexes() -> None:
    sql = _normalized_sql()

    assert re.search(
        r"create unique index if not exists instagram_profiles_profile_id_key "
        r"on social\.instagram_profiles \(profile_id\) where profile_id is not null;",
        sql,
    )
    assert re.search(
        r"create unique index if not exists instagram_profiles_source_scope_normalized_username_key "
        r"on social\.instagram_profiles \(source_scope, normalized_username\) where profile_id is null;",
        sql,
    )


def test_relationship_type_is_following_only_and_relationship_indexes_are_scoped() -> None:
    sql = _normalized_sql()
    relationships = _table_body("instagram_profile_relationships")

    assert "relationship_type text not null check (relationship_type = 'following')" in relationships
    assert "instagram_profile_relationships_related_user_id_key" in sql
    assert "where related_user_id is not null" in sql
    assert "instagram_profile_relationships_related_username_key" in sql
    assert "where related_user_id is null and related_normalized_username is not null" in sql
    assert "instagram_profile_relationships_owner_type_rank_idx" in sql
    assert "instagram_profile_relationships_related_username_idx" in sql
    assert "instagram_profile_relationships_related_user_id_idx" in sql
    assert "instagram_profile_relationships_owner_verified_idx" in sql
    assert "instagram_profile_relationships_owner_private_idx" in sql


def test_new_profile_raw_tables_are_service_role_only() -> None:
    sql = _normalized_sql()

    for table in (
        "social.instagram_profiles",
        "social.instagram_profile_external_links",
        "social.instagram_profile_relationships",
    ):
        assert f"alter table {table} enable row level security;" in sql
        assert table in re.search(
            r"grant all privileges on table (?P<tables>.*?) to service_role;",
            sql,
        ).group("tables")
        assert table in re.search(
            r"revoke all on table (?P<tables>.*?) from public, anon, authenticated;",
            sql,
        ).group("tables")

    assert not re.search(
        r"grant select on table (?P<tables>.*?) to anon, authenticated;",
        sql,
    )
    assert "create policy instagram_profiles" not in sql
    assert "create policy instagram_profile_external_links" not in sql
    assert "create policy instagram_profile_relationships" not in sql
    assert re.search(
        r"transitional raw exposure note:.*legacy instagram raw-data-bearing tables keep.*"
        r"existing broad read grants",
        sql,
    )
    assert "service-role-only" in sql


def test_comment_alignment_columns_and_indexes_are_guarded() -> None:
    sql = _normalized_sql()

    assert "alter table social.instagram_comments add column if not exists author_full_name text" in sql
    for column in (
        "add column if not exists author_profile_pic_url_hd text",
        "add column if not exists parent_comment_external_id text",
        "add column if not exists root_comment_id uuid references social.instagram_comments(id) on delete set null",
        "add column if not exists reply_depth integer check (reply_depth is null or reply_depth >= 0)",
        "add column if not exists source_snapshot_type text",
    ):
        assert column in sql

    for index_name in (
        "instagram_comments_post_parent_created_idx",
        "instagram_comments_username_created_idx",
        "instagram_comments_root_comment_id_idx",
        "instagram_comments_parent_external_id_idx",
    ):
        assert f"create index if not exists {index_name}" in sql

    assert "on social.instagram_comments (post_id, parent_comment_id, created_at asc)" in sql
    assert "on social.instagram_comments (username, created_at desc)" in sql


def test_post_bridge_columns_and_canonical_entity_types_are_declared() -> None:
    sql = _normalized_sql()

    assert (
        "alter table social.social_post_entities drop constraint if exists social_post_entities_entity_type_check"
        in sql
    )
    for entity_type in ("'tagged_user'", "'location'"):
        assert entity_type in sql

    assert "alter table social.instagram_posts add column if not exists source_input_url text" in sql
    for column in (
        "add column if not exists source_post_id text",
        "add column if not exists permalink text",
        "add column if not exists caption_id text",
        "add column if not exists caption_is_edited boolean",
        "add column if not exists caption_has_translation boolean",
        "add column if not exists owner_user_id text",
        "add column if not exists owner_username text",
        "add column if not exists owner_profile_pic_url_hd text",
        "add column if not exists location_id text",
        "add column if not exists location_name text",
        "add column if not exists location_raw jsonb not null default '{}'::jsonb",
        "add column if not exists original_width integer check (original_width is null or original_width >= 0)",
        "add column if not exists original_height integer check (original_height is null or original_height >= 0)",
        "add column if not exists like_and_view_counts_disabled boolean",
        "add column if not exists comments_disabled boolean",
        "add column if not exists commenting_disabled_for_viewer boolean",
        (
            "add column if not exists media_repost_count integer check "
            "(media_repost_count is null or media_repost_count >= 0)"
        ),
        "add column if not exists is_paid_partnership boolean",
        "add column if not exists is_advertisement boolean",
        "add column if not exists can_viewer_reshare boolean",
        "add column if not exists has_audio boolean",
        "add column if not exists audio_url text",
    ):
        assert column in sql

    for index_name in (
        "instagram_posts_owner_username_idx",
        "instagram_posts_owner_user_id_idx",
        "instagram_posts_source_post_id_idx",
        "instagram_posts_location_id_idx",
    ):
        assert f"create index if not exists {index_name}" in sql


def test_job_type_constraint_is_not_changed_and_stage_strategy_is_documented() -> None:
    sql = _normalized_sql()

    assert "config.stage" in sql
    assert re.search(
        r"intentionally does not alter the.*scrape_jobs job_type constraint",
        sql,
    )
    assert "alter table social.scrape_jobs" not in sql
    assert "scrape_jobs_job_type_check" not in sql
    assert "drop constraint if exists scrape_jobs" not in sql


def test_no_embedded_comment_table_or_broad_search_indexes() -> None:
    sql = _normalized_sql()

    assert "social.instagram_post_embedded_comments" not in sql
    assert "create table if not exists social.instagram_post_embedded_comments" not in sql
    assert not re.search(r"\busing\s+gin\b", sql)
    assert not re.search(r"\busing\s+gist\b", sql)
    assert "gin_trgm_ops" not in sql
    assert "no broad gin/trigram indexes are added" in sql
    assert "index lock/rollback note" in sql


def test_migration_is_additive_and_does_not_revoke_legacy_raw_tables() -> None:
    sql = _normalized_sql()

    assert "drop table" not in sql
    dropped_constraints = re.findall(r"alter table\b[^;]*\bdrop\b[^;]*;", sql)
    assert dropped_constraints == [
        "alter table social.social_post_entities drop constraint if exists social_post_entities_entity_type_check;"
    ]

    for legacy_table in (
        "social.instagram_posts",
        "social.instagram_comments",
        "social.instagram_account_catalog_posts",
    ):
        assert f"revoke all on table {legacy_table}" not in sql
        assert f"revoke select on table {legacy_table}" not in sql


def test_instagram_comment_phase_metadata_columns_are_guarded() -> None:
    sql = _normalized_comment_phase_sql()

    for column in (
        "add column if not exists is_covered boolean not null default false",
        "add column if not exists is_ranked boolean not null default false",
        "add column if not exists comment_index integer",
        "add column if not exists phase text",
        "add column if not exists did_report_as_spam boolean not null default false",
        "add column if not exists status text not null default 'active'",
        "add column if not exists is_edited boolean not null default false",
        "add column if not exists is_pinned boolean not null default false",
        "add column if not exists meta_ai_comment_type text not null default 'none'",
        "add column if not exists child_comment_count integer not null default 0",
        "add column if not exists liked_by_media_coauthors boolean not null default false",
        "add column if not exists cursor_min_id text",
        "add column if not exists cursor_param text",
        "add column if not exists cursor_payload jsonb not null default '{}'::jsonb",
        "add column if not exists comment_filter_param text",
    ):
        assert column in sql


def test_instagram_comment_phase_checks_indexes_and_comments_exist() -> None:
    sql = _normalized_comment_phase_sql()

    assert "constraint instagram_comments_phase_check" in sql
    assert "phase in ('ranked', 'headload', 'fb_crosspost', 'child')" in sql
    assert "constraint instagram_comments_cursor_param_check" in sql
    for cursor_name in (
        "'min_id'",
        "'max_id'",
        "'cached_comments_cursor'",
        "'bifilter_token'",
        "'tao_cursor'",
    ):
        assert cursor_name in sql

    assert "create index if not exists instagram_comments_post_phase_comment_index_idx" in sql
    assert "on social.instagram_comments (post_id, phase, comment_index)" in sql
    assert "create index if not exists instagram_comments_post_covered_status_idx" in sql
    assert "where is_covered or status <> 'active'" in sql
    assert "create index if not exists instagram_comments_post_child_count_idx" in sql
    assert "where child_comment_count > 0" in sql

    for comment_target in (
        "comment on column social.instagram_comments.phase is",
        "comment on column social.instagram_comments.is_ranked is",
        "comment on column social.instagram_comments.comment_index is",
        "comment on column social.instagram_comments.reply_count is",
        "comment on column social.instagram_comments.child_comment_count is",
        "comment on column social.instagram_comments.is_edited is",
        "comment on column social.instagram_comments.is_pinned is",
        "comment on column social.instagram_comments.cursor_payload is",
        "comment on column social.instagram_comments.comment_filter_param is",
    ):
        assert comment_target in sql

    assert "legacy scraper-observed reply count" in sql
    assert "authoritative child-comment total" in sql
    assert "preserve ranked ordinals" in sql


def test_instagram_comment_capture_health_view_exists() -> None:
    sql = _normalized_comment_phase_sql()

    assert "create or replace view social.comment_capture_health as" in sql
    assert "from social.instagram_posts p" in sql
    assert "from social.instagram_comments" in sql
    assert "phase is distinct from 'fb_crosspost'" in sql
    for field in (
        "instagram_reported_comments",
        "facebook_reported_comments",
        "saved_parent_comments",
        "saved_child_replies",
        "phase_ranked_count",
        "phase_headload_count",
        "phase_fb_crosspost_count",
        "phase_child_count",
        "covered_comment_count",
        "spam_report_count",
        "inactive_status_count",
        "parent_capture_gap",
        "parent_capture_rate_pct",
    ):
        assert field in sql

    assert "alter view social.comment_capture_health set (security_invoker = on)" in sql
    assert "comment on view social.comment_capture_health is" in sql


def test_instagram_post_comments_audit_table_and_indexes_exist() -> None:
    sql = _normalized_comment_phase_sql()

    assert "create table if not exists social.instagram_post_comments_audit" in sql
    for column in (
        "scrape_run_id uuid references social.scrape_runs(id) on delete set null",
        "scrape_job_id uuid references social.scrape_jobs(id) on delete set null",
        "post_id uuid not null references social.instagram_posts(id) on delete cascade",
        "reported_comment_count integer not null default 0",
        "reported_fb_comment_count integer not null default 0",
        "fetched_comment_count integer not null default 0",
        "fetched_parent_comment_count integer not null default 0",
        "fetched_child_comment_count integer not null default 0",
        "phase_ranked_count integer not null default 0",
        "phase_headload_count integer not null default 0",
        "phase_fb_crosspost_count integer not null default 0",
        "phase_child_count integer not null default 0",
        "phase_counts jsonb not null default '{}'::jsonb",
        "covered_comment_count integer not null default 0",
        "spam_report_count integer not null default 0",
        "inactive_status_count integer not null default 0",
        "status_counts jsonb not null default '{}'::jsonb",
        "cursor_stop_reason text",
        "cursor_payload jsonb not null default '{}'::jsonb",
        "comment_filter_param text",
        "reported_parent_gap_count integer not null default 0",
        "reported_child_gap_count integer not null default 0",
        "reported_total_gap_count integer not null default 0",
        "created_at timestamptz not null default now()",
    ):
        assert column in sql

    assert "create index if not exists instagram_post_comments_audit_post_created_idx" in sql
    assert "on social.instagram_post_comments_audit (post_id, created_at desc)" in sql
    assert "create index if not exists instagram_post_comments_audit_run_job_created_idx" in sql
    assert "grant all privileges on table social.instagram_post_comments_audit to service_role" in sql
    assert "alter table social.instagram_post_comments_audit enable row level security" in sql
    assert "comment on table social.instagram_post_comments_audit is" in sql


def test_instagram_comment_phase_migration_is_additive_and_idempotent() -> None:
    sql = _normalized_comment_phase_sql()

    assert "drop table" not in sql
    assert "drop constraint" not in sql
    assert "add column if not exists" in sql
    assert "create index if not exists" in sql
    assert "create table if not exists social.instagram_post_comments_audit" in sql
    assert "create or replace view social.comment_capture_health as" in sql
    assert "from pg_constraint" in sql
