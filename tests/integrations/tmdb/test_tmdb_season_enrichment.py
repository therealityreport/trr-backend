from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def test_tmdb_season_enrichment_preserves_imdb_title_and_upserts_posters(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    repo_root = Path(__file__).resolve().parents[3]
    season_payload = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_season_details_sample.json").read_text(encoding="utf-8")
    )

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_seasons_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_episodes_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_season_images_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")

    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)

    inserted_show_id = "00000000-0000-0000-0000-0000000000bb"

    def _fake_insert_show(_db, show_upsert):
        return {
            "id": inserted_show_id,
            "name": show_upsert.name,
            "description": show_upsert.description,
            "premiere_date": show_upsert.premiere_date,
            "imdb_id": show_upsert.imdb_id,
            "tmdb_id": (
                int(show_upsert.tmdb_id) if show_upsert.tmdb_id is not None else None
            ),
            "tmdb_meta": {"seasons": [{"season_number": 1}]},
        }

    monkeypatch.setattr(mod, "insert_show", _fake_insert_show)
    monkeypatch.setattr(mod, "update_show", MagicMock())

    upsert_seasons_mock = MagicMock(return_value=[])
    monkeypatch.setattr(mod, "upsert_seasons", upsert_seasons_mock)
    monkeypatch.setattr(
        mod,
        "fetch_seasons_by_show",
        lambda db, show_id, season_numbers: [{"id": "00000000-0000-0000-0000-0000000000c1", "season_number": 1}],
    )

    monkeypatch.setattr(
        mod,
        "fetch_episodes_for_show_season",
        lambda db, show_id, season_number: [
            {
                "episode_number": 1,
                "title": "IMDb Episode 1",
                "overview": "IMDb overview 1",
                "synopsis": None,
                "air_date": "2024-02-01",
                "imdb_episode_id": "tt9000001",
            }
        ],
    )

    upsert_episodes_mock = MagicMock(return_value=[])
    monkeypatch.setattr(mod, "upsert_episodes", upsert_episodes_mock)

    upsert_season_images_mock = MagicMock(return_value=[])
    monkeypatch.setattr(mod, "upsert_season_images", upsert_season_images_mock)

    fetch_tv_season_details_mock = MagicMock(return_value=season_payload)
    monkeypatch.setattr(mod, "fetch_tv_season_details", fetch_tv_season_details_mock)

    result = mod.upsert_candidates_into_supabase(
        [
            CandidateShow(
                imdb_id=None,
                tmdb_id=12345,
                title="Test Show",
                tmdb_meta={"id": 12345, "seasons": [{"season_number": 1}]},
            )
        ],
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
        imdb_fetch_episodes=False,
        tmdb_fetch_seasons=True,
        supabase_client=object(),
    )
    assert result.created == 1

    assert fetch_tv_season_details_mock.call_count == 1
    _, call_kwargs = fetch_tv_season_details_mock.call_args
    assert call_kwargs["language"] == "en-US"
    assert call_kwargs["include_image_language"] == "en,null"
    assert call_kwargs["append_to_response"] == ["external_ids", "images"]

    assert upsert_seasons_mock.call_count >= 2
    season_patch = next(
        call.args[1][0]
        for call in upsert_seasons_mock.call_args_list
        if call.args[1] and call.args[1][0].get("tmdb_season_id") == 101
    )
    assert season_patch["tmdb_series_id"] == 12345
    assert season_patch["season_number"] == 1
    assert season_patch["tmdb_season_object_id"] == "656fe5d80000000000000000"
    assert season_patch["external_tvdb_id"] == 98765
    assert season_patch["external_wikidata_id"] == "Q123"
    assert season_patch["poster_path"] == "/season1.jpg"
    assert season_patch["fetched_at"] == "2025-12-18T00:00:00Z"

    assert upsert_episodes_mock.call_count == 1
    episode_rows = upsert_episodes_mock.call_args[0][1]
    assert len(episode_rows) == 2

    ep1 = next(r for r in episode_rows if r.get("episode_number") == 1)
    assert ep1["title"] == "IMDb Episode 1"
    assert ep1["overview"] == "IMDb overview 1"
    assert ep1["synopsis"] == "IMDb overview 1"
    assert ep1["air_date"] == "2024-02-01"
    assert ep1["tmdb_episode_id"] == 1001
    assert ep1["tmdb_series_id"] == 12345
    assert ep1["still_path"] == "/still1.jpg"
    assert ep1["tmdb_vote_average"] == 6.5
    assert ep1["tmdb_vote_count"] == 10
    assert "crew" not in ep1
    assert "guest_stars" not in ep1

    ep2 = next(r for r in episode_rows if r.get("episode_number") == 2)
    assert ep2["title"] == "TMDb Episode 2"
    assert ep2["overview"] == "TMDb overview 2"
    assert ep2["synopsis"] == "TMDb overview 2"
    assert ep2["air_date"] == "2024-02-09"
    assert ep2["tmdb_episode_id"] == 1002
    assert ep2["tmdb_series_id"] == 12345

    assert upsert_season_images_mock.call_count == 1
    poster_rows = upsert_season_images_mock.call_args[0][1]
    assert len(poster_rows) == 2
    assert {r["file_path"] for r in poster_rows} == {"/s1_poster_en.jpg", "/s1_poster_null.jpg"}
    assert all(r["show_id"] == inserted_show_id for r in poster_rows)
    assert all(r["season_id"] == "00000000-0000-0000-0000-0000000000c1" for r in poster_rows)
    assert all(r["tmdb_series_id"] == 12345 for r in poster_rows)
    assert all(r["season_number"] == 1 for r in poster_rows)
    assert all(r["kind"] == "poster" for r in poster_rows)
    assert all(r["source"] == "tmdb" for r in poster_rows)
    assert all(r["fetched_at"] == "2025-12-18T00:00:00Z" for r in poster_rows)
    assert all("url_original" not in r for r in poster_rows)
