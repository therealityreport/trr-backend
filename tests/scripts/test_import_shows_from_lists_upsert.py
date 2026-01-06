from __future__ import annotations

from unittest.mock import MagicMock

from trr_backend.ingestion.show_importer import upsert_candidates_into_supabase
from trr_backend.ingestion.shows_from_lists import CandidateShow


def test_upsert_candidates_inserts_when_missing(monkeypatch):
    from trr_backend.ingestion import show_importer as mod

    fake_db = object()
    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)

    insert_mock = MagicMock(return_value={"id": "00000000-0000-0000-0000-000000000001", "name": "New Show"})
    update_mock = MagicMock()
    monkeypatch.setattr(mod, "insert_show", insert_mock)
    monkeypatch.setattr(mod, "update_show", update_mock)

    candidates = [
        CandidateShow(imdb_id="tt1111111", tmdb_id=123, title="New Show", source_tags={"imdb-list:ls1"})
    ]

    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
        supabase_client=fake_db,
    )

    assert result.created == 1
    assert result.updated == 0
    assert result.skipped == 0
    insert_mock.assert_called_once()
    update_mock.assert_not_called()


def test_upsert_candidates_updates_show_columns_without_clobber(monkeypatch):
    """Test that upsert adds tmdb_id, premiere_date, listed_on without overwriting existing data."""
    from trr_backend.ingestion import show_importer as mod

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    existing = {
        "id": "00000000-0000-0000-0000-000000000002",
        "name": "Existing Show",
        "imdb_id": "tt1111111",
        "tmdb_id": None,
        "premiere_date": None,
        "listed_on": ["old-source"],
    }

    fake_db = object()
    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: existing)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "insert_show", MagicMock())

    update_mock = MagicMock(return_value={**existing, "tmdb_id": 123})
    monkeypatch.setattr(mod, "update_show", update_mock)

    candidates = [
        CandidateShow(
            imdb_id="tt1111111",
            tmdb_id=123,
            title="Existing Show",
            first_air_date="2020-01-01",
            origin_country=["US"],
            imdb_meta={"rating": 6.6, "vote_count": 1125},
            source_tags={"imdb-list:ls1", "tmdb-list:8301263"},
        )
    ]

    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
        supabase_client=fake_db,
    )

    assert result.created == 0
    assert result.updated == 1
    assert result.skipped == 0

    args, kwargs = update_mock.call_args
    assert args[1] == existing["id"]
    patch = args[2]
    # New schema: tmdb_id, premiere_date, and listed_on are individual columns
    assert "tmdb_id" in patch
    assert patch["tmdb_id"] == 123
    assert "premiere_date" in patch
    assert patch["premiere_date"] == "2020-01-01"
    assert "listed_on" in patch
    # listed_on should merge old-source with new sources
    assert set(patch["listed_on"]) == {"imdb", "old-source", "tmdb"}
