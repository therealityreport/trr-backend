from __future__ import annotations

from unittest.mock import MagicMock


def test_tmdb_details_can_link_tmdb_candidate_to_existing_imdb_show(monkeypatch) -> None:
    """
    Regression: when TMDb list ingestion skips `/tv/{id}/external_ids`, we still want to avoid
    duplicate show rows by linking via `tmdb_meta.external_ids.imdb_id` (fetched during stage 1 details).
    """

    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")

    details_payload = {
        "id": 12345,
        "name": "Test Show",
        "external_ids": {"imdb_id": "tt1234567"},
        "alternative_titles": {"results": []},
    }
    fetch_tv_details_mock = MagicMock(return_value=details_payload)
    monkeypatch.setattr(mod, "fetch_tv_details", fetch_tv_details_mock)

    show_row: dict | None = None

    def _find_by_imdb(_db, imdb_id: str):
        nonlocal show_row
        if show_row and show_row.get("external_ids", {}).get("imdb") == imdb_id:
            return dict(show_row)
        return None

    def _find_by_tmdb(_db, tmdb_id: int):
        nonlocal show_row
        if show_row and show_row.get("tmdb_series_id") == tmdb_id:
            return dict(show_row)
        return None

    insert_show_mock = MagicMock()

    def _insert_show(_db, show_upsert):
        nonlocal show_row
        show_row = {
            "id": "00000000-0000-0000-0000-0000000000aa",
            "name": show_upsert.name,
            "description": show_upsert.description,
            "premiere_date": show_upsert.premiere_date,
            "tmdb_series_id": None,
            "external_ids": dict(show_upsert.external_ids),
        }
        return dict(show_row)

    insert_show_mock.side_effect = _insert_show
    monkeypatch.setattr(mod, "insert_show", insert_show_mock)

    update_show_mock = MagicMock()

    def _update_show(_db, show_id, patch):
        nonlocal show_row
        assert show_row is not None
        assert str(show_id) == show_row["id"]
        show_row = {**show_row, **dict(patch)}
        return dict(show_row)

    update_show_mock.side_effect = _update_show
    monkeypatch.setattr(mod, "update_show", update_show_mock)

    monkeypatch.setattr(mod, "find_show_by_imdb_id", _find_by_imdb)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", _find_by_tmdb)

    result = mod.upsert_candidates_into_supabase(
        [
            CandidateShow(imdb_id="tt1234567", tmdb_id=None, title="Test Show"),
            CandidateShow(imdb_id=None, tmdb_id=12345, title="Test Show"),
        ],
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=True,
        tmdb_fetch_images=False,
        imdb_fetch_episodes=False,
        tmdb_fetch_seasons=False,
        enrich_show_metadata=False,
        supabase_client=object(),
    )

    # First candidate inserts the show; second candidate links via IMDb id and updates the same row.
    assert result.created == 1
    assert result.updated == 1
    assert insert_show_mock.call_count == 1
    assert update_show_mock.call_count == 1

    patch = update_show_mock.call_args[0][2]
    assert patch["tmdb_series_id"] == 12345

    assert fetch_tv_details_mock.call_count == 1
    # Deduplication runs after upsert; downstream pipelines should see the show once.
    assert len(result.upserted_show_rows) == 1
