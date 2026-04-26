from __future__ import annotations

from pathlib import Path


def test_show_source_metadata_migration_has_columns() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    sql = (repo_root / "supabase" / "migrations" / "0047_add_show_source_metadata.sql").read_text()

    for column in (
        "tmdb_meta",
        "imdb_title",
        "imdb_content_rating",
        "tmdb_network_ids",
        "tmdb_production_company_ids",
    ):
        assert column in sql


def test_tmdb_entity_tables_migration_has_tables() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    sql_0048 = (repo_root / "supabase" / "migrations" / "0048_create_tmdb_entities_and_watch_providers.sql").read_text()
    sql_0049 = (repo_root / "supabase" / "migrations" / "0049_rename_tmdb_dimension_tables.sql").read_text()

    for table in ("core.networks", "core.production_companies", "core.watch_providers"):
        assert table in sql_0049

    assert "core.show_watch_providers" in sql_0048


def test_twitter_account_threads_and_bookmarks_migration_contract() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    sql = (
        repo_root
        / "supabase"
        / "migrations"
        / "20260425_twitter_account_threads_and_bookmarks.sql"
    ).read_text()

    for required in (
        "add column if not exists bookmarks integer not null default 0",
        "add column if not exists shares integer not null default 0",
        "add column if not exists thread_root_tweet_id text",
        "add column if not exists twitter_context_role text",
        "add column if not exists bookmarks bigint not null default 0",
        "add column if not exists thread_root_source_id text",
        "private bookmark actors",
        "twitter_tweets_thread_root_tweet_id_idx",
        "twitter_tweets_twitter_context_role_idx",
        "twitter_account_catalog_posts_thread_root_source_id_idx",
        "twitter_tweets_twitter_context_role_check",
        "'account_post'",
        "'reply_parent'",
        "'account_reply'",
        "'audience_reply'",
        "'quote'",
    ):
        assert required in sql
