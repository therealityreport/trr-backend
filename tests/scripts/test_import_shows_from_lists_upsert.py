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
    update_mock = MagicMock(return_value={"id": "00000000-0000-0000-0000-000000000001", "name": "New Show"})
    monkeypatch.setattr(mod, "insert_show", insert_mock)
    monkeypatch.setattr(mod, "update_show", update_mock)

    candidates = [CandidateShow(imdb_id="tt1111111", tmdb_id=123, title="New Show", source_tags={"imdb-list:ls1"})]

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
    # After insert, update_show is called to set external_ids
    assert update_mock.call_count == 1
    _, call_kwargs = update_mock.call_args_list[0]
    if not call_kwargs:
        call_args = update_mock.call_args_list[0][0]
        assert "external_ids" in call_args[2]  # patch dict contains external_ids


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


def test_upsert_candidates_avoids_removed_show_social_columns(monkeypatch):
    from trr_backend.ingestion import show_importer as mod

    fake_db = object()
    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2026-02-28T00:00:00Z")

    details_payload = {
        "id": 24680,
        "name": "Existing Show",
        "external_ids": {
            "imdb_id": "tt2468000",
            "tvdb_id": 12345,
            "tvrage_id": 67890,
            "wikidata_id": "Q24680",
            "facebook_id": "existing-show-facebook",
            "instagram_id": "existing-show-instagram",
            "twitter_id": "existing-show-twitter",
        },
        "alternative_titles": {"results": []},
    }
    monkeypatch.setattr(mod, "fetch_tv_details", lambda *args, **kwargs: details_payload)

    existing = {
        "id": "00000000-0000-0000-0000-000000000246",
        "name": "Existing Show",
        "imdb_id": "tt2468000",
        "tmdb_id": 24680,
        "tvdb_id": None,
        "tvrage_id": None,
        "wikidata_id": None,
        "external_ids": {},
    }

    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: existing)
    monkeypatch.setattr(mod, "insert_show", MagicMock())

    captured_patches: list[dict] = []

    def _update_show(_db, _show_id, patch):
        captured = dict(patch)
        captured_patches.append(captured)
        return {**existing, **captured}

    monkeypatch.setattr(mod, "update_show", _update_show)

    candidates = [CandidateShow(imdb_id=None, tmdb_id=24680, title="Existing Show", source_tags={"tmdb-list:8301263"})]

    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=True,
        supabase_client=fake_db,
    )

    assert result.created == 0
    assert result.updated == 1
    assert result.skipped == 0
    assert captured_patches

    patch = captured_patches[0]
    assert "facebook_id" not in patch
    assert "instagram_id" not in patch
    assert "twitter_id" not in patch
    assert patch["tvdb_id"] == 12345
    assert patch["tvrage_id"] == 67890
    assert patch["wikidata_id"] == "Q24680"

    external_ids = patch.get("external_ids")
    assert isinstance(external_ids, dict)
    assert external_ids.get("facebook_id") == "existing-show-facebook"
    assert external_ids.get("instagram_id") == "existing-show-instagram"
    assert external_ids.get("twitter_id") == "existing-show-twitter"
