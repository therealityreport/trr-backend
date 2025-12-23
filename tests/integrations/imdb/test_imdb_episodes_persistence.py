from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest


def test_imdb_episodes_ingestion_upserts_seasons_and_episode_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    repo_root = Path(__file__).resolve().parents[3]
    overview_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_overview_one_season_sample.html").read_text(
        encoding="utf-8"
    )
    season_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_season1_next_data_sample.html").read_text(
        encoding="utf-8"
    )

    class _FakeImdbClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def fetch_episodes_page(self, imdb_series_id: str, *, season: int | None = None) -> str:
            assert imdb_series_id == "tt1234567"
            if season is None:
                return overview_html
            assert season == 1
            return season_html

    monkeypatch.setattr(mod, "HttpImdbTitleMetadataClient", _FakeImdbClient)
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_seasons_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_episodes_table_exists", lambda *args, **kwargs: None)

    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)

    inserted_show_id = "00000000-0000-0000-0000-0000000000aa"

    def _fake_insert_show(_db, show_upsert):
        return {
            "id": inserted_show_id,
            "name": show_upsert.name,
            "description": show_upsert.description,
            "premiere_date": show_upsert.premiere_date,
            "tmdb_series_id": show_upsert.tmdb_series_id,
            "external_ids": show_upsert.external_ids,
        }

    monkeypatch.setattr(mod, "insert_show", _fake_insert_show)
    monkeypatch.setattr(mod, "update_show", MagicMock())

    upsert_seasons_mock = MagicMock(return_value=[])
    monkeypatch.setattr(mod, "upsert_seasons", upsert_seasons_mock)
    monkeypatch.setattr(
        mod,
        "fetch_seasons_by_show",
        lambda db, show_id, season_numbers: [{"id": "00000000-0000-0000-0000-0000000000s1", "season_number": 1}],
    )

    upsert_episodes_mock = MagicMock(return_value=[])
    monkeypatch.setattr(mod, "upsert_episodes", upsert_episodes_mock)

    result = mod.upsert_candidates_into_supabase(
        [CandidateShow(imdb_id="tt1234567", tmdb_id=None, title="Test Show")],
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
        imdb_fetch_episodes=True,
        tmdb_fetch_seasons=False,
        supabase_client=object(),
    )
    assert result.created == 1

    assert upsert_seasons_mock.call_count == 1
    season_rows = upsert_seasons_mock.call_args[0][1]
    assert season_rows == [
        {
            "show_id": inserted_show_id,
            "season_number": 1,
            "imdb_series_id": "tt1234567",
            "language": "en-US",
            "fetched_at": "2025-12-18T00:00:00Z",
        }
    ]

    assert upsert_episodes_mock.call_count == 1
    episode_rows = upsert_episodes_mock.call_args[0][1]
    assert len(episode_rows) == 2

    ep1 = next(r for r in episode_rows if r.get("episode_number") == 1)
    assert ep1["show_id"] == inserted_show_id
    assert ep1["season_id"] == "00000000-0000-0000-0000-0000000000s1"
    assert ep1["season_number"] == 1
    assert ep1["imdb_episode_id"] == "tt9000001"
    assert ep1["title"] == "Episode One"
    assert ep1["overview"] == "Episode one plot."
    assert ep1["synopsis"] == "Episode one plot."
    assert ep1["air_date"] == "2024-04-18"
    assert ep1["imdb_rating"] == 7.3
    assert ep1["imdb_vote_count"] == 123
    assert ep1["imdb_primary_image_url"] == "https://m.media-amazon.com/images/M/ep1.jpg"
    assert ep1["imdb_primary_image_caption"] == "Episode One still"
    assert ep1["imdb_primary_image_width"] == 1280
    assert ep1["imdb_primary_image_height"] == 720
    assert ep1["fetched_at"] == "2025-12-18T00:00:00Z"
    assert "crew" not in ep1
    assert "guest_stars" not in ep1
    assert "tmdb_episode_id" not in ep1
    assert "tmdb_series_id" not in ep1
