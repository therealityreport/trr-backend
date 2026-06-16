from __future__ import annotations

from unittest.mock import MagicMock, patch

from scripts.sync.episode_id_reconciliation import (
    merge_external_ids,
    reconcile_episode_imdb_ids_from_tmdb,
    safe_match_episode_ref,
)


def test_safe_match_episode_ref_matches_by_season_episode_when_unique() -> None:
    match = safe_match_episode_ref(
        [
            {
                "id": "episode-1",
                "season_number": 10,
                "episode_number": 13,
                "title": "Ship Happens",
                "air_date": "2026-04-28",
                "external_ids": {},
            }
        ],
        imdb_episode_id="tt41542916",
        season_number=10,
        episode_number=13,
        title="Different IMDb Title",
        year=2026,
    )

    assert match is not None
    assert match.episode_id == "episode-1"
    assert match.strategy == "season_episode"


def test_safe_match_episode_ref_matches_by_title_and_year_without_numbering() -> None:
    match = safe_match_episode_ref(
        [
            {
                "id": "episode-1",
                "season_number": 10,
                "episode_number": 13,
                "title": "Ship Happens",
                "air_date": "2026-04-28",
                "external_ids": {},
            }
        ],
        imdb_episode_id="tt41542916",
        title="Ship Happens",
        year=2026,
    )

    assert match is not None
    assert match.episode_id == "episode-1"
    assert match.strategy == "title_year"


def test_safe_match_episode_ref_matches_reunion_part_title_variant() -> None:
    match = safe_match_episode_ref(
        [
            {
                "id": "episode-1",
                "season_number": 10,
                "episode_number": 17,
                "title": "Reunion (1)",
                "air_date": "2026-05-26",
                "external_ids": {},
            }
        ],
        imdb_episode_id="tt42033756",
        title="Reunion Part 1",
        year=2026,
    )

    assert match is not None
    assert match.episode_id == "episode-1"


def test_safe_match_episode_ref_rejects_ambiguous_title_year_match() -> None:
    match = safe_match_episode_ref(
        [
            {"id": "episode-1", "title": "Reunion", "air_date": "2026-06-02", "external_ids": {}},
            {"id": "episode-2", "title": "Reunion", "air_date": "2026-06-09", "external_ids": {}},
        ],
        imdb_episode_id="tt42033756",
        title="Reunion",
        year=2026,
    )

    assert match is None


def test_safe_match_episode_ref_rejects_conflicting_existing_imdb_id() -> None:
    match = safe_match_episode_ref(
        [
            {
                "id": "episode-1",
                "season_number": 10,
                "episode_number": 13,
                "title": "Ship Happens",
                "air_date": "2026-04-28",
                "external_ids": {"imdb": "tt00000000"},
            }
        ],
        imdb_episode_id="tt41542916",
        season_number=10,
        episode_number=13,
    )

    assert match is None


def test_safe_match_episode_ref_ignores_tmdb_season_zero_unless_imdb_has_positive_season() -> None:
    local_rows = [
        {
            "id": "episode-special",
            "season_number": 0,
            "episode_number": 1,
            "title": "Watch What Happens",
            "air_date": "2024-04-01",
            "external_ids": {},
        }
    ]

    blocked = safe_match_episode_ref(
        local_rows,
        imdb_episode_id="tt41542916",
        season_number=0,
        episode_number=1,
        title="Watch What Happens",
        air_date="2024-04-01",
    )
    allowed = safe_match_episode_ref(
        local_rows,
        imdb_episode_id="tt41542916",
        season_number=8,
        episode_number=6,
        title="Watch What Happens",
        air_date="2024-04-01",
    )

    assert blocked is None
    assert allowed is not None
    assert allowed.episode_id == "episode-special"
    assert allowed.strategy == "title_year"


def test_reconcile_episode_imdb_ids_from_tmdb_skips_season_zero_external_id_fetches() -> None:
    db = MagicMock()
    episodes = [
        {
            "id": "episode-special",
            "season_number": 0,
            "episode_number": 1,
            "tmdb_episode_id": 2727437,
            "imdb_episode_id": None,
            "external_ids": {"tmdb": 2727437},
        }
    ]

    with patch("scripts.sync.episode_id_reconciliation.fetch_tv_episode_external_ids") as fetch_external_ids:
        updated = reconcile_episode_imdb_ids_from_tmdb(
            db,
            show_id="show-1",
            tmdb_series_id=69720,
            episodes=episodes,
            api_key="tmdb-key",
        )

    assert updated == 0
    fetch_external_ids.assert_not_called()


def test_merge_external_ids_adds_imdb_without_dropping_tmdb() -> None:
    assert merge_external_ids({"tmdb": 7118363}, {"imdb": "tt41542916"}) == {
        "tmdb": 7118363,
        "imdb": "tt41542916",
    }
