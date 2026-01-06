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
