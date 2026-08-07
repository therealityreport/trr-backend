from __future__ import annotations

from typing import Any, cast

from trr_backend.ingestion.shows_from_lists import CandidateShow
from trr_backend.integrations.imdb.title_metadata_client import ImdbEpisodesPageMetadata, ImdbSeasonEpisode


def test_imdb_episode_fields_take_precedence_and_tmdb_fills_provider_fields(monkeypatch) -> None:
    from trr_backend.ingestion import show_importer as mod

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_seasons_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_episodes_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_season_images_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2026-01-01T00:00:00Z")

    show_row = {
        "id": "show-1",
        "name": "Test Show",
        "description": None,
        "premiere_date": None,
        "imdb_id": "tt0100001",
        "tmdb_id": 999,
        "external_ids": {"imdb": "tt0100001", "tmdb": 999},
        "tmdb_meta": {"seasons": [{"season_number": 1}]},
    }

    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *_args, **_kwargs: dict(show_row))
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *_args, **_kwargs: dict(show_row))
    monkeypatch.setattr(
        mod,
        "insert_show",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no insert")),
    )
    monkeypatch.setattr(mod, "update_show", lambda *_args, **_kwargs: dict(show_row))

    season_store: dict[int, dict] = {1: {"id": "season-1", "season_number": 1, "external_ids": {"imdb": "tt0100001"}}}
    episode_store: dict[int, dict] = {
        1: {
            "episode_number": 1,
            "title": "TMDb Old Title",
            "overview": "TMDb Old Overview",
            "synopsis": "TMDb Old Synopsis",
            "air_date": "2024-01-01",
            "imdb_episode_id": None,
            "tmdb_episode_id": 444,
            "external_ids": {"tmdb": 444},
        }
    }

    def _upsert_seasons(_db, rows):
        for row in rows:
            season_number = int(row["season_number"])
            current = season_store.get(season_number, {"id": f"season-{season_number}", "season_number": season_number})
            season_store[season_number] = {**current, **row}
        return [dict(value) for value in season_store.values()]

    def _fetch_seasons_by_show(_db, *, show_id: str, season_numbers: list[int]):
        assert show_id == "show-1"
        return [dict(season_store[number]) for number in season_numbers if number in season_store]

    def _fetch_episodes_for_show_season(_db, *, show_id: str, season_number: int):
        assert show_id == "show-1"
        assert season_number == 1
        return [dict(value) for value in episode_store.values()]

    def _upsert_episodes(_db, rows):
        for row in rows:
            episode_number = int(row["episode_number"])
            current = episode_store.get(episode_number, {"episode_number": episode_number})
            episode_store[episode_number] = {**current, **row}
        return [dict(value) for value in episode_store.values()]

    monkeypatch.setattr(mod, "upsert_seasons", _upsert_seasons)
    monkeypatch.setattr(mod, "fetch_seasons_by_show", _fetch_seasons_by_show)
    monkeypatch.setattr(mod, "fetch_episodes_for_show_season", _fetch_episodes_for_show_season)
    monkeypatch.setattr(mod, "upsert_episodes", _upsert_episodes)
    monkeypatch.setattr(mod, "upsert_season_images", lambda *_args, **_kwargs: [])

    class _FakeImdbClient:
        def __init__(self, *args, **kwargs):  # noqa: D401, ANN002, ANN003
            pass

        def fetch_episodes_payload(
            self,
            imdb_series_id: str,
            *,
            season: int | None = None,
            allow_html_fallback: bool = True,
        ):
            assert imdb_series_id == "tt0100001"
            return {"season": season}

        def fetch_episodes_page(self, *_args, **_kwargs):
            raise AssertionError("HTML fallback should not be used in this test")

    monkeypatch.setattr(mod, "HttpImdbTitleMetadataClient", _FakeImdbClient)
    monkeypatch.setattr(mod, "parse_imdb_episodes_payload", lambda *_args, **_kwargs: ImdbEpisodesPageMetadata([1], []))
    monkeypatch.setattr(
        mod,
        "parse_imdb_season_episodes_payload",
        lambda *_args, **_kwargs: [
            ImdbSeasonEpisode(
                season=1,
                episode=1,
                imdb_episode_id="tt0200001",
                title="IMDb Canonical Title",
                air_date="2025-02-02",
                overview="IMDb Canonical Overview",
                imdb_rating=8.7,
                imdb_vote_count=1200,
                imdb_primary_image_url="https://imdb.test/still.jpg",
                imdb_primary_image_caption="IMDb still",
                imdb_primary_image_width=1920,
                imdb_primary_image_height=1080,
            )
        ],
    )
    monkeypatch.setattr(
        mod,
        "fetch_tv_season_details",
        lambda *_args, **_kwargs: {
            "id": 2001,
            "_id": "tmdb-season-1",
            "name": "Season 1",
            "overview": "TMDb Season Overview",
            "air_date": "2024-01-01",
            "poster_path": "/poster.jpg",
            "external_ids": {"tvdb_id": 123},
            "episodes": [
                {
                    "id": 555,
                    "episode_number": 1,
                    "name": "TMDb New Title",
                    "overview": "TMDb New Overview",
                    "air_date": "2024-05-05",
                    "episode_type": "standard",
                    "production_code": "PC01",
                    "runtime": 42,
                    "still_path": "/still.jpg",
                    "vote_average": 7.1,
                    "vote_count": 100,
                }
            ],
            "images": {"posters": []},
        },
    )

    result = mod.upsert_candidates_into_supabase(
        [CandidateShow(imdb_id="tt0100001", tmdb_id=999, title="Test Show")],
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
        imdb_fetch_episodes=True,
        tmdb_fetch_seasons=True,
        enrich_show_metadata=False,
        supabase_client=cast(Any, object()),
    )

    final_episode = episode_store[1]
    assert result.skipped == 1
    assert final_episode["title"] == "IMDb Canonical Title"
    assert final_episode["overview"] == "IMDb Canonical Overview"
    assert final_episode["synopsis"] == "IMDb Canonical Overview"
    assert str(final_episode["air_date"]) == "2025-02-02"
    assert final_episode["imdb_episode_id"] == "tt0200001"
    assert float(final_episode["imdb_rating"]) == 8.7
    assert int(final_episode["imdb_vote_count"]) == 1200
    assert final_episode["tmdb_episode_id"] == 555
    assert final_episode["runtime"] == 42
    assert final_episode["episode_type"] == "standard"
    assert final_episode["external_ids"]["imdb"] == "tt0200001"
    assert final_episode["external_ids"]["tmdb"] == 555
