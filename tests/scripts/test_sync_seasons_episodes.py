from __future__ import annotations

from unittest.mock import MagicMock, patch

from scripts.sync import sync_seasons_episodes as cli


def test_reconcile_show_seasons_episodes_updates_counts_and_merges_imdb_branch() -> None:
    db = MagicMock()
    show_id = "show-1"

    with patch.object(
        cli,
        "_fetch_show_rows_for_ids",
        return_value=[{"id": show_id, "most_recent_episode": {"tmdb": {"season": 7, "episode": 2}}}],
    ):
        with patch.object(cli, "_fetch_season_numbers", return_value=[1, 1, 2]):
            with patch.object(
                cli,
                "_fetch_episode_rows",
                return_value=[
                    {
                        "season_number": 1,
                        "episode_number": 3,
                        "title": "Old",
                        "air_date": "2024-01-10",
                        "imdb_episode_id": "tt-old",
                    },
                    {
                        "season_number": 2,
                        "episode_number": 1,
                        "title": "New",
                        "air_date": "2024-02-10",
                        "imdb_episode_id": "tt-new",
                    },
                ],
            ):
                with patch.object(cli, "update_show") as update_show:
                    updated = cli.reconcile_show_seasons_episodes(db, show_ids=[show_id], verbose=False)

    assert updated == 1
    update_show.assert_called_once_with(
        db,
        show_id,
        {
            "show_total_seasons": 2,
            "show_total_episodes": 2,
            "most_recent_episode": {
                "tmdb": {"season": 7, "episode": 2},
                "imdb": {
                    "season": 2,
                    "episode": 1,
                    "title": "New",
                    "air_date": "2024-02-10",
                    "imdb_id": "tt-new",
                },
            },
        },
    )


def test_reconcile_show_seasons_episodes_falls_back_to_season_episode_order_without_air_dates() -> None:
    db = MagicMock()
    show_id = "show-1"

    with patch.object(cli, "_fetch_show_rows_for_ids", return_value=[{"id": show_id, "most_recent_episode": {}}]):
        with patch.object(cli, "_fetch_season_numbers", return_value=[1, 2]):
            with patch.object(
                cli,
                "_fetch_episode_rows",
                return_value=[
                    {
                        "season_number": 1,
                        "episode_number": 10,
                        "title": "Earlier",
                        "air_date": None,
                        "imdb_episode_id": "tt-earlier",
                    },
                    {
                        "season_number": 2,
                        "episode_number": 1,
                        "title": "Later",
                        "air_date": None,
                        "imdb_episode_id": "tt-later",
                    },
                ],
            ):
                with patch.object(cli, "update_show") as update_show:
                    updated = cli.reconcile_show_seasons_episodes(db, show_ids=[show_id], verbose=False)

    assert updated == 1
    update_show.assert_called_once()
    patch_payload = update_show.call_args.args[2]
    assert patch_payload["most_recent_episode"]["imdb"]["season"] == 2
    assert patch_payload["most_recent_episode"]["imdb"]["episode"] == 1
    assert patch_payload["most_recent_episode"]["imdb"]["imdb_id"] == "tt-later"


def test_reconcile_show_seasons_episodes_skips_empty_patches() -> None:
    db = MagicMock()
    show_id = "show-1"

    with patch.object(cli, "_fetch_show_rows_for_ids", return_value=[{"id": show_id, "most_recent_episode": {}}]):
        with patch.object(cli, "_fetch_season_numbers", return_value=[]):
            with patch.object(cli, "_fetch_episode_rows", return_value=[]):
                with patch.object(cli, "update_show") as update_show:
                    updated = cli.reconcile_show_seasons_episodes(db, show_ids=[show_id], verbose=False)

    assert updated == 0
    update_show.assert_not_called()


def test_reconcile_missing_episode_imdb_ids_uses_tmdb_episode_external_ids() -> None:
    db = MagicMock()
    show_id = "show-1"

    with patch.object(cli, "_fetch_show_identity_rows_for_ids", return_value=[{"id": show_id, "tmdb_id": 69720}]):
        with patch.object(
            cli,
            "_fetch_episode_rows",
            return_value=[
                {
                    "id": "episode-1",
                    "season_number": 10,
                    "episode_number": 13,
                    "title": "Ship Happens",
                    "air_date": "2026-04-28",
                    "imdb_episode_id": None,
                    "tmdb_episode_id": 7118363,
                    "external_ids": {"tmdb": 7118363},
                }
            ],
        ):
            with patch.object(cli, "resolve_api_key", return_value="tmdb-key"):
                with patch.object(cli, "reconcile_episode_imdb_ids_from_tmdb", return_value=1) as reconcile:
                    updated = cli.reconcile_missing_episode_imdb_ids(db, show_ids=[show_id], verbose=False)

    assert updated == 1
    reconcile.assert_called_once()
    assert reconcile.call_args.kwargs["tmdb_series_id"] == 69720
