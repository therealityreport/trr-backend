from __future__ import annotations

import json
from contextlib import nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from trr_backend.repositories import admin_show_reads as repo


class _FakeCursorContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def cursor(self, *args, **kwargs):
        return _FakeCursorContext()


def test_search_shows_maps_explicit_show_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "id": "show-1",
                "name": "Bravo Show",
                "slug": "bravo-show",
                "canonical_slug": "bravo-show",
                "alternative_names": ["RHOBH"],
                "imdb_id": "tt123",
                "tmdb_id": 456,
                "show_total_seasons": 5,
                "show_total_episodes": 90,
                "description": "A Bravo show",
                "networks": ["Bravo"],
                "genres": ["Reality"],
                "tmdb_status": "Returning Series",
                "tmdb_vote_average": 7.8,
                "imdb_rating_value": 7.2,
                "poster_url": "https://cdn.example.com/poster.jpg",
                "computed_slug": "bravo-show",
                "slug_collision_count": 0,
            }
        ],
    )

    rows, query_count = repo.search_shows("Bravo", limit=20, offset=0)

    assert query_count == 1
    assert rows == [
        {
            "id": "show-1",
            "name": "Bravo Show",
            "slug": "bravo-show",
            "canonical_slug": "bravo-show",
            "alternative_names": ["RHOBH"],
            "imdb_id": "tt123",
            "tmdb_id": 456,
            "show_total_seasons": 5,
            "show_total_episodes": 90,
            "description": "A Bravo show",
            "networks": ["Bravo"],
            "genres": ["Reality"],
            "tmdb_status": "Returning Series",
            "tmdb_vote_average": 7.8,
            "imdb_rating_value": 7.2,
            "poster_url": "https://cdn.example.com/poster.jpg",
            "streaming_providers": [],
            "watch_providers": [],
            "tags": [],
        }
    ]


def test_resolve_show_slug_prefers_alias_slug_and_counts_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "name": "The Real Housewives of Beverly Hills",
                "alternative_names": ["RHOBH"],
                "slug": "the-real-housewives-of-beverly-hills",
            }
        ],
    )

    resolved, query_count = repo.resolve_show_slug("real-housewives-of-beverly-hills")

    assert query_count == 1
    assert resolved == {
        "show_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "slug": "rhobh",
        "canonical_slug": "rhobh",
        "show_name": "The Real Housewives of Beverly Hills",
    }


def test_get_people_home_preserves_section_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo.pg, "db_read_connection", lambda label="read": nullcontext(_FakeConnection()))
    monkeypatch.setattr(
        repo,
        "_people_most_popular",
        lambda limit, cur=None: (
            [
                {
                    "person_id": "person-1",
                    "full_name": "Brandi Glanville",
                    "known_for": "RHOBH",
                    "photo_url": "https://cdn.example.com/photo.jpg",
                    "metric_value": 8,
                    "show_context": "rhobh",
                    "latest_at": "2024-01-01T00:00:00+00:00",
                }
            ],
            1,
        ),
    )
    monkeypatch.setattr(repo, "_people_most_shows", lambda limit, cur=None: ([], 1))
    monkeypatch.setattr(repo, "_people_top_episodes", lambda limit, cur=None: ([], 1))
    monkeypatch.setattr(repo, "_people_recently_added", lambda limit, cur=None: ([], 1))

    payload, query_count = repo.get_people_home(12)

    assert query_count == 4
    assert payload["pagination"] == {"limit": 12}
    assert payload["sections"]["recentlyViewed"] == {"items": [], "error": None}
    assert payload["sections"]["mostPopular"]["items"] == [
        {
            "person_id": "person-1",
            "person_slug": "brandi-glanville",
            "full_name": "Brandi Glanville",
            "known_for": "RHOBH",
            "photo_url": "https://cdn.example.com/photo.jpg",
            "show_context": "rhobh",
            "metric_label": "News Score",
            "metric_value": 8,
            "latest_at": "2024-01-01T00:00:00+00:00",
        }
    ]


def test_search_global_skips_episode_lookup_for_short_non_episode_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo.pg, "db_read_connection", lambda label="read": nullcontext(_FakeConnection()))
    calls = {"count": 0}

    def fake_fetch_all_with_cursor(_cur, _query, _params=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                {
                    "id": "show-1",
                    "name": "Alan's Show",
                    "slug": "alans-show",
                    "alternative_names": [],
                    "canonical_slug": "alans-show",
                }
            ]
        if calls["count"] == 2:
            return [
                {
                    "id": "person-1",
                    "full_name": "Alan Cumming",
                    "known_for": "Host",
                    "show_context": "the-traitors-us",
                }
            ]
        raise AssertionError("episodes query should not run for short non-episode search terms")

    monkeypatch.setattr(repo.pg, "fetch_all_with_cursor", fake_fetch_all_with_cursor)

    payload, query_count = repo.search_global("ala", limit=8)

    assert query_count == 2
    assert payload["episodes"] == []
    assert payload["people"][0]["person_slug"] == "alan-cumming"


def test_search_global_people_matches_interior_name_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo.pg, "db_read_connection", lambda label="read": nullcontext(_FakeConnection()))
    recorded_params: list[list[object] | None] = []

    def fake_fetch_all_with_cursor(_cur, _query, params=None):
        recorded_params.append(params)
        call_index = len(recorded_params)
        if call_index == 1:
            return []
        if call_index == 2:
            return [
                {
                    "id": "person-1",
                    "full_name": "Alan Cumming",
                    "known_for": "Host",
                    "show_context": "the-traitors-us",
                }
            ]
        return []

    monkeypatch.setattr(repo.pg, "fetch_all_with_cursor", fake_fetch_all_with_cursor)

    payload, query_count = repo.search_global("Cumming", limit=8)

    assert query_count == 3
    assert payload["people"] == [
        {
            "id": "person-1",
            "full_name": "Alan Cumming",
            "known_for": "Host",
            "show_context": "the-traitors-us",
            "person_slug": "alan-cumming",
        }
    ]
    assert recorded_params[1] == ["Cumming", "Cumming%", "%Cumming%", 8]


def test_get_show_detail_strips_raw_metadata_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo,
        "_fetch_one_row",
        lambda query, params=None, cur=None: {
            "id": "show-1",
            "name": "Bravo Show",
            "slug": "bravo-show",
            "canonical_slug": "bravo-show",
            "alternative_names": ["RHOBH"],
            "imdb_id": "tt123",
            "tmdb_id": 456,
            "tvdb_id": 789,
            "tvrage_id": "tv-1",
            "wikidata_id": "Q123",
            "external_ids": {"imdb_id": "tt123"},
            "show_total_seasons": 5,
            "show_total_episodes": 90,
            "description": "A Bravo show",
            "premiere_date": "2020-01-01",
            "genres": ["Reality"],
            "networks": ["Bravo"],
            "streaming_providers": ["Peacock"],
            "watch_providers": ["Peacock"],
            "tags": ["Housewives"],
            "primary_poster_image_id": "poster-1",
            "primary_backdrop_image_id": "backdrop-1",
            "primary_logo_image_id": "logo-1",
            "tmdb_status": "Returning Series",
            "tmdb_vote_average": 7.8,
            "imdb_rating_value": 7.2,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "poster_url": "https://cdn.example.com/poster.jpg",
            "backdrop_url": "https://cdn.example.com/backdrop.jpg",
            "logo_url": "https://cdn.example.com/logo.png",
            "tmdb_meta": {"ignored": True},
            "imdb_meta": {"ignored": True},
            "computed_slug": "bravo-show",
            "slug_collision_count": 0,
        },
    )

    row, query_count = repo.get_show_detail("show-1")

    assert query_count == 1
    assert row is not None
    assert row["canonical_slug"] == "bravo-show"
    assert "tmdb_meta" not in row
    assert "imdb_meta" not in row


def test_get_show_detail_builds_overview_fields_without_mutating_raw_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo,
        "_fetch_one_row",
        lambda query, params=None, cur=None: {
            "id": "show-1",
            "name": "The Real Housewives of Salt Lake City",
            "slug": "rhoslc",
            "canonical_slug": "rhoslc",
            "alternative_names": ["RHOSLC"],
            "imdb_id": "tt123",
            "tmdb_id": 456,
            "tvdb_id": 789,
            "tvrage_id": "tv-1",
            "wikidata_id": "Q123",
            "external_ids": {"imdb_id": "tt123"},
            "show_total_seasons": 5,
            "show_total_episodes": 90,
            "description": "A Bravo show",
            "premiere_date": "2020-01-01",
            "genres": ["Reality"],
            "networks": ["Bravo TV"],
            "streaming_providers": ["Peacock Premium", "Peacock Premium Plus"],
            "watch_providers": ["Hayu", "Hayu Amazon Channel"],
            "overview_watch_availability": [
                {
                    "region": "US",
                    "stream": ["Peacock", "Hayu"],
                    "buy": ["Apple TV", "Prime Video"],
                },
                {
                    "region": "GB",
                    "stream": ["Hayu"],
                    "buy": [],
                },
                {
                    "region": "DE",
                    "stream": ["RTL+"],
                    "buy": ["Apple TV"],
                },
            ],
            "watch_provider_regions": [
                {
                    "region": "DE",
                    "stream": ["RTL+", "Joyn"],
                    "free": [],
                    "buy_rent": ["Apple TV", "Amazon Video"],
                },
                {
                    "region": "US",
                    "stream": ["Peacock", "Hayu", "Peacock"],
                    "free": ["Bravo TV", "Hayu"],
                    "buy_rent": ["Apple TV", "Prime Video", "Prime Video"],
                },
                {
                    "region": "NZ",
                    "stream": ["ThreeNow"],
                    "free": [],
                    "buy_rent": [],
                },
            ],
            "justwatch_url": "https://www.themoviedb.org/tv/110381-the-real-housewives-of-salt-lake-city/watch?locale=US",
            "tags": ["Housewives"],
            "primary_poster_image_id": "poster-1",
            "primary_backdrop_image_id": "backdrop-1",
            "primary_logo_image_id": "logo-1",
            "tmdb_status": "Returning Series",
            "tmdb_vote_average": 7.8,
            "imdb_rating_value": 7.2,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "poster_url": "https://cdn.example.com/poster.jpg",
            "backdrop_url": "https://cdn.example.com/backdrop.jpg",
            "logo_url": "https://cdn.example.com/logo.png",
            "tmdb_meta": {"ignored": True},
            "imdb_meta": {"ignored": True},
            "computed_slug": "rhoslc",
            "slug_collision_count": 0,
        },
    )

    row, query_count = repo.get_show_detail("show-1")

    assert query_count == 1
    assert row is not None
    assert row["alternative_names"] == ["RHOSLC"]
    assert row["networks"] == ["Bravo TV"]
    assert row["streaming_providers"] == ["Peacock Premium", "Peacock Premium Plus"]
    assert row["watch_providers"] == ["Hayu", "Hayu Amazon Channel"]
    assert row["overview_alternative_names"] == ["RHOSLC"]
    assert row["overview_networks"] == ["Bravo"]
    assert row["overview_streaming_providers"] == ["Hayu", "Peacock"]
    assert row["overview_watch_availability"] == [
        {
            "region": "US",
            "stream": ["Hayu", "Peacock"],
            "buy": ["Apple TV", "Prime Video"],
        },
        {
            "region": "GB",
            "stream": ["Hayu"],
            "buy": [],
        },
    ]
    assert row["watch_provider_regions"] == [
        {
            "region": "US",
            "stream": ["Hayu", "Peacock"],
            "free": ["Bravo TV", "Hayu"],
            "buy_rent": ["Apple TV", "Prime Video"],
        },
        {
            "region": "DE",
            "stream": ["Joyn", "RTL+"],
            "free": [],
            "buy_rent": ["Amazon Video", "Apple TV"],
        },
        {
            "region": "NZ",
            "stream": ["ThreeNow"],
            "free": [],
            "buy_rent": [],
        },
    ]
    assert row["derived_external_links"] == {
        "justwatch_url": "https://www.themoviedb.org/tv/110381-the-real-housewives-of-salt-lake-city/watch?locale=US"
    }


def test_get_show_detail_normalizes_json_text_watch_availability_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        repo,
        "_fetch_one_row",
        lambda query, params=None, cur=None: {
            "id": "show-1",
            "name": "The Real Housewives of Salt Lake City",
            "slug": "rhoslc",
            "canonical_slug": "rhoslc",
            "alternative_names": ["RHOSLC"],
            "networks": ["Bravo TV"],
            "streaming_providers": ["Peacock Premium"],
            "watch_providers": ["Amazon Video"],
            "overview_watch_availability": json.dumps(
                [
                    {
                        "region": "US",
                        "stream": ["Peacock Premium"],
                        "buy": ["Amazon Video"],
                    },
                    {
                        "region": "DE",
                        "stream": ["RTL+"],
                        "buy": ["Apple TV"],
                    },
                ]
            ),
            "watch_provider_regions": json.dumps(
                [
                    {
                        "region": "US",
                        "stream": ["Peacock Premium", "YouTube TV"],
                        "free": ["Bravo TV"],
                        "buy_rent": ["Amazon Video", "Apple TV Store"],
                    },
                    {
                        "region": "AU",
                        "stream": ["BINGE"],
                        "free": ["9Now"],
                        "buy_rent": ["Amazon Video"],
                    },
                ]
            ),
            "justwatch_url": "https://www.themoviedb.org/tv/110381-the-real-housewives-of-salt-lake-city/watch?locale=US",
            "tags": [],
        },
    )

    row, query_count = repo.get_show_detail("show-1")

    assert query_count == 1
    assert row is not None
    assert row["overview_watch_availability"] == [
        {
            "region": "US",
            "stream": ["Peacock Premium"],
            "buy": ["Amazon Video"],
        },
    ]
    assert row["watch_provider_regions"] == [
        {
            "region": "US",
            "stream": ["Peacock Premium", "YouTube TV"],
            "free": ["Bravo TV"],
            "buy_rent": ["Amazon Video", "Apple TV Store"],
        },
        {
            "region": "AU",
            "stream": ["BINGE"],
            "free": ["9Now"],
            "buy_rent": ["Amazon Video"],
        },
    ]


def test_get_show_detail_query_projects_watch_availability_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_one_row(query, params=None, cur=None):
        captured["query"] = query
        captured["params"] = params
        return None

    monkeypatch.setattr(repo, "_fetch_one_row", fake_fetch_one_row)

    row, query_count = repo.get_show_detail("show-1")

    assert row is None
    assert query_count == 1
    query = str(captured["query"])
    assert "watch.overview_watch_availability" in query
    assert "watch.watch_provider_regions" in query
    assert captured["params"] == ["show-1"]


def test_get_show_credits_keeps_unassigned_eligible_cast_and_groups_crew(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        [
            {
                "show_id": "show-1",
                "person_id": "person-heather",
                "person_name": "Heather Gay",
                "total_episodes": 80,
                "archive_episodes": 0,
                "seasons_appeared": 4,
                "season_numbers": [1, 2, 3, 4],
                "latest_season": 4,
                "roles": ["Housewife"],
                "photo_url": "https://cdn.example/heather.jpg",
            },
            {
                "show_id": "show-1",
                "person_id": "person-andy",
                "person_name": "Andy Cohen",
                "total_episodes": 99,
                "archive_episodes": 14,
                "seasons_appeared": 6,
                "season_numbers": [1, 2, 3, 4, 5, 6],
                "latest_season": 6,
                "roles": ["Host"],
                "photo_url": "https://cdn.example/andy.jpg",
            },
        ],
        [
            {"person_id": "person-heather", "role_names": ["Housewife"]},
        ],
        [
            {
                "person_id": "person-heather",
                "metadata": {"episode_count": 20, "episodes_label": "20 episodes"},
            },
            {
                "person_id": "person-andy",
                "metadata": {"episode_count": 31, "episodes_label": "31 episodes"},
            },
        ],
        [
            {
                "credit_id": "credit-1",
                "show_id": "show-1",
                "person_id": "person-andy",
                "person_name": "Andy Cohen",
                "credit_category": "Producers",
                "role": "executive producer",
                "billing_order": 2,
                "source_type": "imdb_fullcredits",
                "metadata": {
                    "episode_count": 107,
                    "episodes_label": "107 episodes",
                    "years_label": "2020-2026",
                    "imdb_name_id": "nm0169212",
                    "source_page_url": "https://www.imdb.com/title/tt11363282/fullcredits/",
                    "display_order": 2,
                },
                "updated_at": "2026-03-31T12:00:00Z",
                "imdb_id": "tt11363282",
            }
        ],
    ]

    def fake_fetch_all_rows(query: str, params: list[Any], cur: Any | None = None) -> list[dict[str, Any]]:
        assert cur is None
        assert query
        assert params
        return responses.pop(0)

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all_rows)

    payload, query_count = repo.get_show_credits("show-1")

    assert query_count == 4
    assert [row["person_name"] for row in payload["cast_roster"]] == ["Heather Gay", "Andy Cohen"]
    assert payload["cast_roster"][0]["roles"] == ["Housewife"]
    assert payload["cast_roster"][1]["roles"] == ["Host"]
    assert payload["cast_roster"][0]["total_episodes"] == 80
    assert payload["crew_sections"] == [
        {
            "title": "Producers",
            "rows": [
                {
                    "credit_id": "credit-1",
                    "person_id": "person-andy",
                    "person_name": "Andy Cohen",
                    "role": "executive producer",
                    "billing_order": 2,
                    "source_type": "imdb_fullcredits",
                    "episode_count": 107,
                    "episodes_label": "107 episodes",
                    "years_label": "2020-2026",
                    "imdb_name_id": "nm0169212",
                    "display_order": 2,
                }
            ],
            "grouped_rows": [
                {
                    "person_id": "person-andy",
                    "person_name": "Andy Cohen",
                    "role_lines": [
                        {
                            "credit_id": "credit-1",
                            "role": "executive producer",
                            "billing_order": 2,
                            "source_type": "imdb_fullcredits",
                            "episode_count": 107,
                            "episodes_label": "107 episodes",
                            "years_label": "2020-2026",
                            "imdb_name_id": "nm0169212",
                            "display_order": 2,
                        }
                    ],
                }
            ],
        }
    ]
    assert payload["source_metadata"]["show_imdb_id"] == "tt11363282"


def test_get_show_credits_falls_back_to_existing_cast_roles_when_curated_assignments_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        [
            {
                "show_id": "show-1",
                "person_id": "person-heather",
                "person_name": "Heather Gay",
                "total_episodes": 80,
                "archive_episodes": 0,
                "seasons_appeared": 4,
                "season_numbers": [1, 2, 3, 4],
                "latest_season": 4,
                "roles": ["Housewife"],
                "photo_url": "https://cdn.example/heather.jpg",
            },
            {
                "show_id": "show-1",
                "person_id": "person-meredith",
                "person_name": "Meredith Marks",
                "total_episodes": 75,
                "archive_episodes": 0,
                "seasons_appeared": 4,
                "season_numbers": [1, 2, 3, 4],
                "latest_season": 4,
                "roles": ["Housewife"],
                "photo_url": "https://cdn.example/meredith.jpg",
            },
        ],
        [],
        [],
        [],
    ]

    def fake_fetch_all_rows(query: str, params: list[Any], cur: Any | None = None) -> list[dict[str, Any]]:
        assert cur is None
        assert query
        assert params
        return responses.pop(0)

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all_rows)

    payload, query_count = repo.get_show_credits("show-1")

    assert query_count == 4
    assert [row["person_name"] for row in payload["cast_roster"]] == ["Heather Gay", "Meredith Marks"]
    assert payload["cast_roster"][0]["roles"] == ["Housewife"]


def test_get_show_credits_excludes_voice_only_and_archive_only_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        [
            {
                "show_id": "show-1",
                "person_id": "person-heather",
                "person_name": "Heather Gay",
                "total_episodes": 80,
                "archive_episodes": 0,
                "seasons_appeared": 4,
                "season_numbers": [1, 2, 3, 4],
                "latest_season": 4,
                "roles": ["Housewife"],
                "photo_url": "https://cdn.example/heather.jpg",
            },
            {
                "show_id": "show-1",
                "person_id": "person-voice",
                "person_name": "Voice Guest",
                "total_episodes": 8,
                "archive_episodes": 0,
                "seasons_appeared": 2,
                "season_numbers": [1, 2],
                "latest_season": 2,
                "roles": ["Self (voice)"],
                "photo_url": "https://cdn.example/voice.jpg",
            },
            {
                "show_id": "show-1",
                "person_id": "person-archive",
                "person_name": "Archive Only",
                "total_episodes": 0,
                "archive_episodes": 3,
                "seasons_appeared": 1,
                "season_numbers": [1],
                "latest_season": 1,
                "roles": ["Self"],
                "photo_url": "https://cdn.example/archive.jpg",
            },
        ],
        [],
        [],
        [],
    ]

    def fake_fetch_all_rows(query: str, params: list[Any], cur: Any | None = None) -> list[dict[str, Any]]:
        assert cur is None
        assert query
        assert params
        return responses.pop(0)

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all_rows)

    payload, query_count = repo.get_show_credits("show-1")

    assert query_count == 4
    assert [row["person_name"] for row in payload["cast_roster"]] == ["Heather Gay"]


def test_get_show_credits_groups_multiple_crew_lines_for_one_person(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        [
            {
                "show_id": "show-1",
                "person_id": "person-heather",
                "person_name": "Heather Gay",
                "total_episodes": 80,
                "archive_episodes": 0,
                "seasons_appeared": 4,
                "season_numbers": [1, 2, 3, 4],
                "latest_season": 4,
                "roles": ["Housewife"],
                "photo_url": "https://cdn.example/heather.jpg",
            }
        ],
        [],
        [],
        [
            {
                "credit_id": "credit-casey-1",
                "show_id": "show-1",
                "person_id": "person-casey",
                "person_name": "Casey Allan",
                "credit_category": "Producers",
                "role": "supervising producer",
                "billing_order": 1,
                "source_type": "imdb_fullcredits",
                "metadata": {
                    "episode_count": 12,
                    "episodes_label": "12 episodes",
                    "years_label": "2020-2021",
                    "imdb_name_id": "nm0000001",
                    "display_order": 1,
                },
                "updated_at": "2026-03-31T12:00:00Z",
                "imdb_id": "tt11363282",
            },
            {
                "credit_id": "credit-casey-2",
                "show_id": "show-1",
                "person_id": "person-casey",
                "person_name": "Casey Allan",
                "credit_category": "Producers",
                "role": "associate producer",
                "billing_order": 2,
                "source_type": "imdb_fullcredits",
                "metadata": {
                    "episode_count": 23,
                    "episodes_label": "23 episodes",
                    "years_label": "2021-2024",
                    "imdb_name_id": "nm0000001",
                    "display_order": 2,
                },
                "updated_at": "2026-03-31T12:00:00Z",
                "imdb_id": "tt11363282",
            },
            {
                "credit_id": "credit-casey-3",
                "show_id": "show-1",
                "person_id": "person-casey",
                "person_name": "Casey Allan",
                "credit_category": "Producers",
                "role": "field producer",
                "billing_order": 3,
                "source_type": "imdb_fullcredits",
                "metadata": {
                    "episode_count": 18,
                    "episodes_label": "18 episodes",
                    "years_label": "2024-2026",
                    "imdb_name_id": "nm0000001",
                    "display_order": 3,
                },
                "updated_at": "2026-03-31T12:00:00Z",
                "imdb_id": "tt11363282",
            },
        ],
    ]

    def fake_fetch_all_rows(query: str, params: list[Any], cur: Any | None = None) -> list[dict[str, Any]]:
        assert cur is None
        assert query
        assert params
        return responses.pop(0)

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all_rows)

    payload, query_count = repo.get_show_credits("show-1")

    assert query_count == 4
    assert payload["crew_sections"][0]["grouped_rows"] == [
        {
            "person_id": "person-casey",
            "person_name": "Casey Allan",
            "role_lines": [
                {
                    "credit_id": "credit-casey-1",
                    "role": "supervising producer",
                    "billing_order": 1,
                    "source_type": "imdb_fullcredits",
                    "episode_count": 12,
                    "episodes_label": "12 episodes",
                    "years_label": "2020-2021",
                    "imdb_name_id": "nm0000001",
                    "display_order": 1,
                },
                {
                    "credit_id": "credit-casey-2",
                    "role": "associate producer",
                    "billing_order": 2,
                    "source_type": "imdb_fullcredits",
                    "episode_count": 23,
                    "episodes_label": "23 episodes",
                    "years_label": "2021-2024",
                    "imdb_name_id": "nm0000001",
                    "display_order": 2,
                },
                {
                    "credit_id": "credit-casey-3",
                    "role": "field producer",
                    "billing_order": 3,
                    "source_type": "imdb_fullcredits",
                    "episode_count": 18,
                    "episodes_label": "18 episodes",
                    "years_label": "2024-2026",
                    "imdb_name_id": "nm0000001",
                    "display_order": 3,
                },
            ],
        }
    ]


def test_get_show_credits_uses_self_credit_episode_count_metadata_when_occurrences_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        [
            {
                "show_id": "show-1",
                "person_id": "person-andy",
                "person_name": "Andy Cohen",
                "total_episodes": 0,
                "archive_episodes": 0,
                "seasons_appeared": 0,
                "season_numbers": [],
                "latest_season": None,
                "roles": ["Host"],
                "photo_url": "https://cdn.example/andy.jpg",
            }
        ],
        [],
        [
            {
                "person_id": "person-andy",
                "metadata": {"episode_count": 31, "episodes_label": "31 episodes"},
            }
        ],
        [],
    ]

    def fake_fetch_all_rows(query: str, params: list[Any], cur: Any | None = None) -> list[dict[str, Any]]:
        assert cur is None
        assert query
        assert params
        return responses.pop(0)

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all_rows)

    payload, query_count = repo.get_show_credits("show-1")

    assert query_count == 4
    assert payload["cast_roster"] == [
        {
            "show_id": "show-1",
            "person_id": "person-andy",
            "person_name": "Andy Cohen",
            "photo_url": "https://cdn.example/andy.jpg",
            "total_episodes": 31,
            "archive_episodes": 0,
            "seasons_appeared": 0,
            "season_numbers": [],
            "latest_season": None,
            "roles": ["Host"],
        }
    ]


def test_get_show_seasons_default_path_uses_explicit_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query: str, params=None):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    rows, query_count = repo.get_show_seasons("show-1", limit=20, offset=0)

    assert query_count == 1
    assert rows == []
    assert "SELECT *" not in str(captured["query"])
    assert "s.id::text AS id" in str(captured["query"])
    assert "s.show_id::text AS show_id" in str(captured["query"])
    assert "s.show_name" in str(captured["query"])
    assert "s.title" in str(captured["query"])
    assert "s.premiere_date" in str(captured["query"])
    assert "s.url_original_poster" in str(captured["query"])
    assert "s.overview" in str(captured["query"])
    assert "s.synopsis" not in str(captured["query"])
    assert "s.episode_count" not in str(captured["query"])


def test_get_show_seasons_include_episode_signal_preserves_overview_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query: str, params=None):
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "id": "season-6",
                "show_id": "show-1",
                "show_name": "The Real Housewives of Salt Lake City",
                "season_number": 6,
                "name": "Season 6",
                "title": "S6",
                "overview": "Salt Lake City returns.",
                "premiere_date": "2024-09-18",
                "url_original_poster": "https://cdn.example.com/season-6.jpg",
                "episode_count": 18,
                "episode_airdate_count": 18,
                "first_episode_air_date": "2024-09-18",
                "last_episode_air_date": "2025-01-22",
                "has_scheduled_or_aired_episode": True,
                "fandom_source_url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_6",
                "fandom_page_title": "The Real Housewives of Salt Lake City - Season 6",
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    rows, query_count = repo.get_show_seasons("show-1", limit=20, offset=0, include_episode_signal=True)

    assert query_count == 1
    assert rows == [
        {
            "id": "season-6",
            "show_id": "show-1",
            "show_name": "The Real Housewives of Salt Lake City",
            "season_number": 6,
            "name": "Season 6",
            "title": "S6",
            "overview": "Salt Lake City returns.",
            "premiere_date": "2024-09-18",
            "url_original_poster": "https://cdn.example.com/season-6.jpg",
            "episode_count": 18,
            "episode_airdate_count": 18,
            "first_episode_air_date": "2024-09-18",
            "last_episode_air_date": "2025-01-22",
            "has_scheduled_or_aired_episode": True,
            "fandom_source_url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_6",
            "fandom_page_title": "The Real Housewives of Salt Lake City - Season 6",
        }
    ]
    assert "s.overview" in str(captured["query"])
    assert "s.synopsis" not in str(captured["query"])
    assert "episode_count" in str(captured["query"])
    assert "episode_airdate_count" in str(captured["query"])
    assert "first_episode_air_date" in str(captured["query"])
    assert "last_episode_air_date" in str(captured["query"])
    assert "sf.source_url AS fandom_source_url" in str(captured["query"])
    assert "sf.page_title AS fandom_page_title" in str(captured["query"])
    assert "s.episode_count" not in str(captured["query"])


def test_get_show_seasons_normalizes_json_unsafe_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "id": "season-1",
                "show_id": "show-1",
                "tmdb_season_id": 101,
                "season_number": 3,
                "name": "Season 3",
                "overview": "Salt Lake City returns.",
                "air_date": date(2024, 2, 14),
                "poster_path": "/poster.jpg",
                "episode_count": 18,
                "tmdb_vote_average": float("nan"),
                "tmdb_vote_count": 42,
                "trr_score": Decimal("7.25"),
                "created_at": datetime(2024, 2, 14, 12, 30, tzinfo=UTC),
                "updated_at": datetime(2024, 2, 15, 6, 45, tzinfo=UTC),
                "first_episode_air_date": date(2024, 2, 14),
                "last_episode_air_date": date(2024, 5, 20),
                "episode_airdate_count": "18",
                "has_scheduled_or_aired_episode": True,
            }
        ],
    )

    rows, query_count = repo.get_show_seasons("show-1", limit=20, offset=0, include_episode_signal=True)

    assert query_count == 1
    assert rows == [
        {
            "id": "season-1",
            "show_id": "show-1",
            "tmdb_season_id": 101,
            "season_number": 3,
            "name": "Season 3",
            "overview": "Salt Lake City returns.",
            "air_date": "2024-02-14",
            "poster_path": "/poster.jpg",
            "episode_count": 18,
            "tmdb_vote_average": None,
            "tmdb_vote_count": 42,
            "trr_score": 7.25,
            "created_at": "2024-02-14T12:30:00+00:00",
            "updated_at": "2024-02-15T06:45:00+00:00",
            "first_episode_air_date": "2024-02-14",
            "last_episode_air_date": "2024-05-20",
            "episode_airdate_count": 18,
            "has_scheduled_or_aired_episode": True,
        }
    ]


def test_mirror_media_asset_updates_canonical_ingest_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, object]] = []

    monkeypatch.setattr(
        repo,
        "mirror_media_asset_row",
        lambda asset_row, force=False: {
            "hosted_url": "https://cdn.example.com/asset-1.jpg",
            "hosted_content_type": "image/jpeg",
            "metadata": {"thumb_url": "https://cdn.example.com/asset-1-thumb.jpg"},
        },
    )
    monkeypatch.setattr(repo.pg, "execute", lambda query, params=None: executed.append((query, params)))

    asset_id, failure = repo._mirror_media_asset(
        {"id": "asset-1", "source_url": "https://tmdb.example.com/asset-1.jpg"}
    )

    assert asset_id == "asset-1"
    assert failure is None
    assert any("ingest_completed_at" in query for query, _params in executed)
    assert any("ingest_last_error = null" in query for query, _params in executed)


def test_get_show_assets_preserves_logo_fields_and_dedupes_show_images(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'show'" in sql:
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "logo",
                    "link_is_primary": True,
                    "context": {
                        "source_page_url": "https://www.bravotv.com/the-daily-dish",
                        "context_section": "hero",
                        "people_count": "2",
                        "people_count_source": "manual",
                        "thumbnail_crop": {"x": 0.2, "y": 0.3, "zoom": 1.4, "mode": "manual"},
                    },
                    "media_asset_id": "media-1",
                    "asset_id": "media-1",
                    "source": "web_scrape",
                    "source_url": None,
                    "hosted_url": "https://cdn.example.com/logo.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": 1200,
                    "height": 800,
                    "caption": "Main logo",
                    "metadata": {
                        "display_url": "https://cdn.example.com/logo-card.jpg",
                        "original_source_file_url": "https://origin.example.com/logo.jpg",
                        "logo_black_url": "https://cdn.example.com/logo-black.png",
                        "logo_white_url": "https://cdn.example.com/logo-white.png",
                    },
                    "ingest_status": "hosted",
                    "fetched_at": "2024-01-02T00:00:00Z",
                    "created_at": "2024-01-03T00:00:00Z",
                }
            ]
        if "from core.show_images" in sql:
            return [
                {
                    "id": "show-image-duplicate",
                    "source": "tmdb",
                    "kind": "logo",
                    "image_type": None,
                    "url": "https://tmdb.example.com/logo-duplicate.jpg",
                    "url_original": None,
                    "hosted_url": "https://cdn.example.com/logo.jpg",
                    "width": 1200,
                    "height": 800,
                    "created_at": "2024-01-04T00:00:00Z",
                    "metadata": None,
                },
                {
                    "id": "show-image-2",
                    "source": "tmdb",
                    "kind": "poster",
                    "image_type": "poster",
                    "url": "https://tmdb.example.com/poster.jpg",
                    "url_original": "https://origin.example.com/poster.jpg",
                    "hosted_url": "https://cdn.example.com/poster.jpg",
                    "width": 1000,
                    "height": 1500,
                    "created_at": "2024-01-05T00:00:00Z",
                    "metadata": {"thumb_url": "https://cdn.example.com/poster-thumb.jpg"},
                },
            ]
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo._get_show_assets_impl("show-1", limit=10, offset=0)

    assert query_count == 2
    assert [asset["id"] for asset in assets] == ["media-1", "show-image-2"]
    assert assets[0]["source"] == "bravotv.com"
    assert assets[0]["source_url"] == "https://www.bravotv.com/the-daily-dish"
    assert assets[0]["display_url"] == "https://cdn.example.com/logo-card.jpg"
    assert assets[0]["original_url"] == "https://origin.example.com/logo.jpg"
    assert assets[0]["logo_black_url"] == "https://cdn.example.com/logo-black.png"
    assert assets[0]["logo_white_url"] == "https://cdn.example.com/logo-white.png"
    assert assets[0]["logo_link_is_primary"] is True
    assert assets[0]["people_count"] == 2
    assert assets[0]["people_count_source"] == "manual"
    assert assets[0]["thumbnail_crop_mode"] == "manual"
    assert assets[1]["source"] == "tmdb"
    assert assets[1]["thumb_url"] == "https://cdn.example.com/poster-thumb.jpg"


def test_get_show_assets_paginates_after_fetch_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_limits: list[int] = []

    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'show'" in sql:
            captured_limits.append(int(params[1]))
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "poster",
                    "link_is_primary": False,
                    "context": None,
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "tmdb",
                    "source_url": "https://tmdb.example.com/1.jpg",
                    "hosted_url": "https://cdn.example.com/1.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
                {
                    "link_id": "link-2",
                    "link_kind": "poster",
                    "link_is_primary": False,
                    "context": None,
                    "media_asset_id": "asset-2",
                    "asset_id": "asset-2",
                    "source": "web_scrape:tmdb.com",
                    "source_url": "https://www.tmdb.com/2.jpg",
                    "hosted_url": "https://cdn.example.com/2.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
            ]
        if "from core.show_images" in sql:
            captured_limits.append(int(params[1]))
            return []
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo._get_show_assets_impl("show-1", limit=1, offset=1)

    assert query_count == 2
    assert captured_limits == [2, 2]
    assert [asset["id"] for asset in assets] == ["asset-2"]
    assert assets[0]["source"] == "tmdb.com"


def test_get_show_assets_full_path_uses_full_fetch_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_limits: list[int] = []

    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'show'" in sql:
            captured_limits.append(int(params[1]))
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "logo",
                    "link_is_primary": True,
                    "context": {"thumbnail_crop": {"x": 0.5, "y": 0.4, "zoom": 1.2, "mode": "manual"}},
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "web_scrape",
                    "source_url": "https://www.bravotv.com/logo.jpg",
                    "hosted_url": "https://cdn.example.com/logo.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": 1000,
                    "height": 500,
                    "caption": "Logo",
                    "metadata": {
                        "display_url": "https://cdn.example.com/logo-card.jpg",
                        "detail_url": "https://cdn.example.com/logo-detail.jpg",
                        "logo_black_url": "https://cdn.example.com/logo-black.png",
                        "logo_white_url": "https://cdn.example.com/logo-white.png",
                    },
                    "ingest_status": "hosted",
                    "fetched_at": "2024-01-01T00:00:00Z",
                    "created_at": "2024-01-02T00:00:00Z",
                }
            ]
        if "from core.show_images" in sql:
            captured_limits.append(int(params[1]))
            return []
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo._get_show_assets_impl("show-1", limit=5001, offset=0, full=True)

    assert query_count == 2
    assert captured_limits == [5001, 5001]
    assert len(assets) == 1
    assert assets[0]["source"] == "bravotv.com"
    assert assets[0]["display_url"] == "https://cdn.example.com/logo-card.jpg"
    assert assets[0]["detail_url"] == "https://cdn.example.com/logo-detail.jpg"
    assert assets[0]["logo_black_url"] == "https://cdn.example.com/logo-black.png"
    assert assets[0]["logo_white_url"] == "https://cdn.example.com/logo-white.png"
    assert assets[0]["thumbnail_crop_mode"] == "manual"


def test_get_show_season_assets_default_path_paginates_after_fetch_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_limits: list[int] = []

    def fake_fetch_one(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.seasons" in sql:
            return {
                "id": "season-1",
                "premiere_date": "2024-01-01",
                "air_date": None,
                "episode_start_date": None,
                "episode_end_date": None,
                "name": "Bravo Show",
                "external_ids": {},
            }
        raise AssertionError(f"unexpected query: {sql}")

    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'season'" in sql:
            captured_limits.append(int(params[1]))
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "poster",
                    "context": None,
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "tmdb",
                    "source_url": "https://tmdb.example.com/1.jpg",
                    "hosted_url": "https://cdn.example.com/1.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
                {
                    "link_id": "link-2",
                    "link_kind": "poster",
                    "context": {"source_page_url": "https://www.tmdb.com/2.jpg"},
                    "media_asset_id": "asset-2",
                    "asset_id": "asset-2",
                    "source": "web_scrape",
                    "source_url": None,
                    "hosted_url": "https://cdn.example.com/2.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
            ]
        if "from core.season_images" in sql:
            captured_limits.append(int(params[2]))
            return []
        if "from core.episode_images" in sql:
            captured_limits.append(int(params[2]))
            return []
        if "from core.credits as c" in sql:
            captured_limits.append(int(params[2]))
            return []
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_one_row", fake_fetch_one)
    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo._get_show_season_assets_impl("show-1", 6, limit=1, offset=1)

    assert query_count == 5
    assert captured_limits == [2, 2, 2, 2]
    assert [asset["id"] for asset in assets] == ["asset-2"]
    assert assets[0]["source"] == "tmdb.com"


def test_get_show_season_assets_bounds_large_cast_photo_fanout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_limits: dict[str, int] = {}
    captured_cast_params: list[object] | None = None
    captured_cast_query = ""
    person_ids = [f"00000000-0000-0000-0000-{index:012d}" for index in range(27)]

    def fake_fetch_one(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.seasons" in sql:
            return {
                "id": "11111111-1111-1111-1111-111111111111",
                "premiere_date": "2024-01-01",
                "air_date": None,
                "episode_start_date": "2024-01-01",
                "episode_end_date": "2024-03-01",
                "name": "Bravo Show",
                "external_ids": {"imdb_id": "tt123"},
            }
        raise AssertionError(f"unexpected query: {sql}")

    def fake_fetch_all(query: str, params=None, cur=None):
        nonlocal captured_cast_params, captured_cast_query
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'season'" in sql:
            captured_limits["season_media_links"] = int(params[1])
            return []
        if "from core.season_images" in sql:
            captured_limits["season_images"] = int(params[2])
            return []
        if "from core.episode_images" in sql:
            captured_limits["episode_images"] = int(params[2])
            return []
        if "from core.credits as c" in sql:
            captured_limits["season_cast"] = int(params[2])
            return [
                {
                    "person_id": person_id,
                    "person_name": f"Cast Member {index}",
                }
                for index, person_id in enumerate(person_ids)
            ]
        if "from core.cast_photos as cp" in sql:
            captured_cast_params = list(params)
            captured_cast_query = sql
            captured_limits["cast_photos"] = int(params[8])
            return [
                {
                    "id": f"cast-photo-{index}",
                    "person_id": person_ids[index % len(person_ids)],
                    "source": "bravotv",
                    "url": f"https://bravo.example.com/cast-{index}.jpg",
                    "hosted_url": f"https://cdn.example.com/cast-{index}.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "context_section": "cast",
                    "context_type": "profile",
                    "season": 6,
                    "fetched_at": f"2024-02-{(index % 28) + 1:02d}T00:00:00Z",
                    "hosted_at": None,
                    "updated_at": None,
                    "title_imdb_ids": ["tt123"],
                    "title_names": ["Bravo Show"],
                    "metadata": None,
                    "people_count": None,
                    "people_count_source": None,
                }
                for index in range(int(params[8]))
            ]
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_one_row", fake_fetch_one)
    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo._get_show_season_assets_impl("show-1", 6, limit=49, offset=0)

    assert query_count == 6
    assert captured_limits == {
        "season_media_links": 49,
        "season_images": 49,
        "episode_images": 49,
        "season_cast": 49,
        "cast_photos": 49,
    }
    assert captured_cast_params is not None
    assert len(captured_cast_params[0]) == 27
    assert captured_cast_params[1:4] == ["tt123", "tt123", 6]
    assert "cp.hosted_content_type ilike 'image/%%'" in captured_cast_query
    assert "cardinality(cp.title_imdb_ids)" in captured_cast_query
    assert len(assets) == 49
    assert {asset["type"] for asset in assets} == {"cast"}


def test_assign_season_backdrops_skips_when_everything_already_assigned(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo,
        "_fetch_one_row",
        lambda query, params=None, cur=None: {"id": "season-1", "show_id": "show-1", "season_number": 6},
    )
    monkeypatch.setattr(
        repo,
        "_fetch_all_rows",
        lambda query, params=None, cur=None: [{"media_asset_id": "asset-1"}],
    )

    payload, query_count, show_id = repo.assign_season_backdrops("season-1", ["asset-1"])

    assert show_id == "show-1"
    assert query_count == 2
    assert payload == {
        "requested": 1,
        "assigned": 0,
        "skipped": 1,
        "mirrored_attempted": 0,
        "mirrored_failed": 0,
        "mirrored_failed_ids": [],
    }


def test_assign_season_backdrops_reports_mirror_failures_without_insert(monkeypatch: pytest.MonkeyPatch) -> None:
    call_index = {"value": 0}
    inserted_rows: list[object] = []

    def fake_fetch_one(_query, params=None, cur=None):
        return {"id": "season-1", "show_id": "show-1", "season_number": 6}

    def fake_fetch_all(_query, params=None, cur=None):
        call_index["value"] += 1
        if call_index["value"] == 1:
            return []
        if call_index["value"] == 2:
            return [
                {"id": "asset-2", "hosted_url": None, "source": "tmdb", "source_url": "https://tmdb.example.com/2.jpg"}
            ]
        if call_index["value"] == 3:
            return [{"id": "asset-2", "hosted_url": None}]
        raise AssertionError("unexpected fetch_all call")

    monkeypatch.setattr(repo, "_fetch_one_row", fake_fetch_one)
    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)
    monkeypatch.setattr(repo, "_mirror_media_asset", lambda asset_row: ("asset-2", "Mirror failed"))
    monkeypatch.setattr(
        repo.pg,
        "execute_values_no_return",
        lambda query, rows, conn=None: inserted_rows.extend(rows),
    )

    payload, _query_count, _show_id = repo.assign_season_backdrops("season-1", ["asset-2"])

    assert payload == {
        "requested": 1,
        "assigned": 0,
        "skipped": 0,
        "mirrored_attempted": 1,
        "mirrored_failed": 1,
        "mirrored_failed_ids": ["asset-2"],
    }
    assert inserted_rows == []


def test_get_show_assets_merges_default_rows_with_dedupe_and_logo_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_limits: list[int] = []

    def fake_fetch_all(query: str, params=None, cur=None):
        if "from core.media_links as ml" in query:
            captured_limits.append(int(params[1]))
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "logo",
                    "link_is_primary": True,
                    "context": {
                        "context_section": "branding",
                        "thumbnail_crop": {"x": 0.4, "y": 0.6, "zoom": 1.2, "mode": "manual"},
                    },
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "tmdb",
                    "source_url": "https://tmdb.example.com/logo.png",
                    "hosted_url": "https://cdn.example.com/logo.png",
                    "hosted_content_type": "image/png",
                    "width": 1000,
                    "height": 300,
                    "caption": "Primary logo",
                    "metadata": {
                        "display_url": "https://cdn.example.com/logo-display.png",
                        "logo_black_url": "https://cdn.example.com/logo-black.png",
                        "logo_white_url": "https://cdn.example.com/logo-white.png",
                    },
                    "ingest_status": "completed",
                    "fetched_at": "2026-03-01T00:00:00Z",
                    "created_at": "2026-03-01T00:00:00Z",
                }
            ]
        if "from core.show_images" in query:
            captured_limits.append(int(params[1]))
            return [
                {
                    "id": "legacy-duplicate",
                    "source": "fanart",
                    "kind": "logo",
                    "image_type": "logo",
                    "url": "https://fanart.example.com/logo.png",
                    "url_original": "https://fanart.example.com/logo-original.png",
                    "hosted_url": "https://cdn.example.com/logo.png",
                    "width": 1000,
                    "height": 300,
                    "created_at": "2026-03-01T00:00:00Z",
                    "metadata": {},
                },
                {
                    "id": "legacy-poster",
                    "source": "fanart",
                    "kind": "poster",
                    "image_type": "poster",
                    "url": "https://fanart.example.com/poster.jpg",
                    "url_original": "https://fanart.example.com/poster-original.jpg",
                    "hosted_url": "https://cdn.example.com/poster.jpg",
                    "width": 1000,
                    "height": 1500,
                    "created_at": "2026-03-02T00:00:00Z",
                    "metadata": {
                        "thumb_url": "https://cdn.example.com/poster-thumb.jpg",
                        "display_url": "https://cdn.example.com/poster-display.jpg",
                    },
                },
            ]
        raise AssertionError(f"unexpected query: {query[:80]}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo._get_show_assets_impl("show-1", limit=10, offset=0, sources=["tmdb", "fanart"])

    assert query_count == 2
    assert captured_limits == [501, 501]
    assert len(assets) == 2
    assert assets[0]["id"] == "asset-1"
    assert assets[0]["logo_link_is_primary"] is True
    assert assets[0]["logo_black_url"] == "https://cdn.example.com/logo-black.png"
    assert assets[0]["thumbnail_crop_mode"] == "manual"
    assert assets[1]["id"] == "legacy-poster"
    assert assets[1]["thumb_url"] == "https://cdn.example.com/poster-thumb.jpg"
    assert assets[1]["display_url"] == "https://cdn.example.com/poster-display.jpg"
    assert [asset["hosted_url"] for asset in assets] == [
        "https://cdn.example.com/logo.png",
        "https://cdn.example.com/poster.jpg",
    ]


def test_shape_show_cast_payload_defaults_to_episode_evidence_and_falls_back() -> None:
    payload = repo._shape_show_cast_payload(
        [
            {
                "person_id": "person-1",
                "full_name": "Fallback Person",
                "photo_url": None,
                "total_episodes": 0,
                "archive_episode_count": 1,
            }
        ],
        limit=20,
        offset=0,
        min_episodes=None,
        has_explicit_min_episodes=False,
        exclude_zero_episode_members=False,
        require_image=False,
        roster_mode="episode_evidence",
    )

    assert payload["cast_source"] == "show_fallback"
    assert payload["eligibility_warning"] is not None
    assert payload["cast"][0]["person_id"] == "person-1"
    assert payload["archive_footage_cast"][0]["person_id"] == "person-1"


def test_shape_show_cast_payload_respects_membership_mode_and_filters() -> None:
    payload = repo._shape_show_cast_payload(
        [
            {"person_id": "person-1", "photo_url": None, "total_episodes": 0, "archive_episode_count": 0},
            {
                "person_id": "person-2",
                "photo_url": "https://cdn.example.com/photo.jpg",
                "total_episodes": 5,
                "archive_episode_count": 0,
            },
        ],
        limit=20,
        offset=0,
        min_episodes=0,
        has_explicit_min_episodes=True,
        exclude_zero_episode_members=True,
        require_image=True,
        roster_mode="imdb_show_membership",
    )

    assert payload["cast_source"] == "imdb_show_membership"
    assert payload["eligibility_warning"] is None
    assert payload["cast"] == [
        {
            "person_id": "person-2",
            "photo_url": "https://cdn.example.com/photo.jpg",
            "total_episodes": 5,
            "archive_episode_count": 0,
        }
    ]


def test_shape_show_cast_payload_keeps_membership_rows_without_explicit_minimum() -> None:
    payload = repo._shape_show_cast_payload(
        [
            {
                "person_id": "person-1",
                "photo_url": "https://cdn.example.com/photo.jpg",
                "total_episodes": 0,
                "archive_episode_count": 0,
            }
        ],
        limit=20,
        offset=0,
        min_episodes=None,
        has_explicit_min_episodes=False,
        exclude_zero_episode_members=False,
        require_image=False,
        roster_mode="imdb_show_membership",
    )

    assert payload["cast_source"] == "imdb_show_membership"
    assert payload["eligibility_warning"] is None
    assert payload["cast"] == [
        {
            "person_id": "person-1",
            "photo_url": "https://cdn.example.com/photo.jpg",
            "total_episodes": 0,
            "archive_episode_count": 0,
        }
    ]


def test_shape_show_cast_payload_can_apply_links_eligibility_filters() -> None:
    payload = repo._shape_show_cast_payload(
        [
            {
                "person_id": "person-1",
                "full_name": "Main Cast",
                "role": "Self",
                "photo_url": "https://cdn.example.com/main.jpg",
                "total_episodes": 14,
                "archive_episode_count": 0,
                "effective_total_episodes": 14,
            },
            {
                "person_id": "person-2",
                "full_name": "Archive Only",
                "role": "Self",
                "photo_url": "https://cdn.example.com/archive.jpg",
                "total_episodes": 0,
                "archive_episode_count": 2,
                "effective_total_episodes": 2,
            },
            {
                "person_id": "person-3",
                "full_name": "Voice Guest",
                "role": "Self (voice)",
                "photo_url": "https://cdn.example.com/voice.jpg",
                "total_episodes": 8,
                "archive_episode_count": 0,
                "effective_total_episodes": 8,
            },
            {
                "person_id": "person-4",
                "full_name": "One Scene Friend",
                "role": "Self",
                "photo_url": "https://cdn.example.com/friend.jpg",
                "total_episodes": 0,
                "archive_episode_count": 0,
                "effective_total_episodes": 3,
            },
        ],
        limit=20,
        offset=0,
        min_episodes=0,
        has_explicit_min_episodes=True,
        exclude_zero_episode_members=False,
        require_image=False,
        roster_mode="imdb_show_membership",
        eligibility_mode="links",
        links_eligibility_show_total_seasons=6,
    )

    assert payload["cast_source"] == "imdb_show_membership"
    assert payload["eligibility_warning"] is None
    assert [row["person_id"] for row in payload["cast"]] == ["person-1"]
    assert payload["archive_footage_cast"] == []


def test_fetch_show_cast_base_rows_can_skip_photo_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch_all_rows(query: str, params: list[Any], cur: Any | None = None) -> list[dict[str, Any]]:
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all_rows)

    rows, query_count = repo._fetch_show_cast_base_rows("show-1", include_photos=False)

    assert rows == []
    assert query_count == 1
    assert captured["params"] == ["show-1", "show-1", "show-1"]
    assert "NULL::text AS photo_url" in captured["query"]
    assert "AS primary_photo ON TRUE" not in captured["query"]
    assert "core.v_cast_photos" not in captured["query"]


def test_shape_season_cast_payload_suppresses_fallback_for_archive_only() -> None:
    payload = repo._shape_season_cast_payload(
        [
            {
                "person_id": "person-1",
                "person_name": "Archive Person",
                "episodes_in_season": 0,
                "total_episodes": 8,
                "photo_url": None,
                "archive_episodes_in_season": 2,
            }
        ],
        limit=20,
        offset=0,
        include_archive_only=True,
    )

    assert payload["cast_source"] == "season_evidence"
    assert payload["eligibility_warning"] is None
    assert payload["include_archive_only"] is True
    assert payload["cast"][0]["archive_episodes_in_season"] == 2
