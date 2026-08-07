from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_show_reads as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


@pytest.fixture(autouse=True)
def clear_cache():
    router_module.invalidate_show_reads_cache()
    yield
    router_module.invalidate_show_reads_cache()


def test_search_returns_contract_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_search(query: str, limit: int):
        calls["count"] += 1
        return (
            {
                "query": query,
                "pagination": {"per_type_limit": limit},
                "shows": [
                    {"id": "00000000-0000-0000-0000-0000000000a1", "name": "The Traitors", "slug": "the-traitors-us"}
                ],
                "people": [
                    {
                        "id": "person-1",
                        "full_name": "Alan Cumming",
                        "known_for": "Host",
                        "show_context": "the-traitors-us",
                        "person_slug": "alan-cumming",
                    }
                ],
                "episodes": [
                    {
                        "id": "episode-1",
                        "title": "Pilot",
                        "episode_number": 1,
                        "season_number": 1,
                        "air_date": None,
                        "show_id": "00000000-0000-0000-0000-0000000000a1",
                        "show_name": "The Traitors",
                        "show_slug": "the-traitors-us",
                    }
                ],
            },
            3,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "search_global", fake_search)

    client = TestClient(app)
    first = client.get("/api/v1/admin/trr-api/search?q=ala&limit=7")
    second = client.get("/api/v1/admin/trr-api/search?q=ala&limit=7")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["pagination"] == {"per_type_limit": 7}
    assert calls["count"] == 1


def test_list_shows_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "search_shows",
        lambda query, limit=None, offset=None: (
            [
                {
                    "id": "00000000-0000-0000-0000-0000000000a1",
                    "name": "The Real Housewives of Salt Lake City",
                    "slug": "the-real-housewives-of-salt-lake-city",
                    "canonical_slug": "the-real-housewives-of-salt-lake-city",
                    "alternative_names": ["RHOSLC"],
                }
            ],
            1,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/shows?q=Salt+Lake&limit=20&offset=0")

    assert response.status_code == 200
    assert response.json()["shows"][0]["canonical_slug"] == "the-real-housewives-of-salt-lake-city"


def test_resolve_slug_returns_404_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(router_module.show_reads_repo, "resolve_show_slug", lambda slug: (None, 1))

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/shows/resolve-slug?slug=missing-show")

    assert response.status_code == 404
    assert response.json()["detail"] == "show slug not found"


def test_people_home_returns_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_people_home(limit=None, *, firebase_uid=None):
        captured["limit"] = limit
        captured["firebase_uid"] = firebase_uid
        return (
            {
                "sections": {
                    "recentlyViewed": {"items": [], "error": None},
                    "mostPopular": {
                        "items": [
                            {
                                "person_id": "person-1",
                                "person_slug": "alan-cumming",
                                "full_name": "Alan Cumming",
                                "known_for": "Host",
                                "photo_url": None,
                                "show_context": "the-traitors-us",
                                "metric_label": "News Score",
                                "metric_value": 15,
                                "latest_at": "2026-03-01T00:00:00Z",
                            }
                        ],
                        "error": None,
                    },
                    "mostShows": {"items": [], "error": None},
                    "topEpisodes": {"items": [], "error": None},
                    "recentlyAdded": {"items": [], "error": None},
                },
                "pagination": {"limit": limit or 12},
            },
            5,
        )

    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_people_home",
        fake_get_people_home,
    )

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/trr-api/people/home?limit=9",
        headers={"X-TRR-Admin-User-Uid": "firebase-user-1"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["sections"]["mostPopular"]["items"][0]["metric_label"] == "News Score"
    assert payload["pagination"] == {"limit": 9}
    assert captured == {"limit": 9, "firebase_uid": "firebase-user-1"}


def test_show_detail_and_seasons_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_show_detail",
        lambda show_id: (
            {
                "id": show_id,
                "name": "The Traitors",
                "slug": "the-traitors",
                "canonical_slug": "the-traitors-us",
            },
            1,
        ),
    )
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_show_seasons",
        lambda show_id, limit=None, offset=None, include_episode_signal=False: (
            [
                {
                    "id": "season-1",
                    "season_number": 1,
                    "overview": "Season overview",
                    "air_date": "2024-01-01",
                    "has_scheduled_or_aired_episode": include_episode_signal,
                }
            ],
            1,
        ),
    )

    client = TestClient(app)
    detail = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1")
    seasons = client.get(
        "/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/seasons?include_episode_signal=true"
    )

    assert detail.status_code == 200
    assert detail.json()["show"]["canonical_slug"] == "the-traitors-us"
    assert seasons.status_code == 200
    assert seasons.json() == {
        "seasons": [
            {
                "id": "season-1",
                "season_number": 1,
                "overview": "Season overview",
                "air_date": "2024-01-01",
                "has_scheduled_or_aired_episode": True,
            }
        ],
        "pagination": {"limit": 20, "offset": 0, "count": 1},
    }


def test_admin_show_routes_reject_invalid_show_id_before_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_if_called(show_id: str):
        raise AssertionError(f"repository should not be called for {show_id}")

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_detail", fail_if_called)

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/shows/not-a-uuid")

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid show_id"


def test_show_assets_route_returns_default_gallery_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_show_assets(show_id: str, **kwargs):
        captured["show_id"] = show_id
        captured["kwargs"] = kwargs
        return (
            [
                {
                    "id": "asset-1",
                    "hosted_url": "https://cdn.example.com/1.jpg",
                    "display_url": "https://cdn.example.com/1-card.jpg",
                    "logo_link_is_primary": True,
                }
            ],
            2,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_assets", fake_get_show_assets)

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/assets?limit=48&offset=5&sources=tmdb,bravo.com"
    )

    assert response.status_code == 200
    assert response.json() == {
        "assets": [
            {
                "id": "asset-1",
                "hosted_url": "https://cdn.example.com/1.jpg",
                "display_url": "https://cdn.example.com/1-card.jpg",
                "logo_link_is_primary": True,
            }
        ],
        "pagination": {
            "limit": 48,
            "offset": 5,
            "count": 1,
            "has_more": False,
            "next_cursor": None,
            "cursor": "b2Zmc2V0OjU=",
            "truncated": False,
            "full": False,
        },
    }
    assert captured == {
        "show_id": "00000000-0000-0000-0000-0000000000a1",
        "kwargs": {"limit": 49, "offset": 5, "sources": ["tmdb", "bravo.com"], "full": False},
    }


def test_show_assets_route_decodes_cursor_and_exposes_next_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_show_assets(show_id: str, **kwargs):
        captured["show_id"] = show_id
        captured["kwargs"] = kwargs
        return (
            [
                {"id": "asset-6", "hosted_url": "https://cdn.example.com/6.jpg"},
                {"id": "asset-7", "hosted_url": "https://cdn.example.com/7.jpg"},
                {"id": "asset-8", "hosted_url": "https://cdn.example.com/8.jpg"},
            ],
            1,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_assets", fake_get_show_assets)

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/assets?limit=2&cursor=b2Zmc2V0OjU="
    )

    assert response.status_code == 200
    assert response.json()["pagination"] == {
        "limit": 2,
        "offset": 5,
        "count": 2,
        "has_more": True,
        "next_cursor": "b2Zmc2V0Ojc=",
        "cursor": "b2Zmc2V0OjU=",
        "truncated": False,
        "full": False,
    }
    assert captured == {
        "show_id": "00000000-0000-0000-0000-0000000000a1",
        "kwargs": {"limit": 3, "offset": 5, "sources": None, "full": False},
    }


def test_show_assets_route_returns_full_gallery_contract_with_truthful_truncation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_get_show_assets(show_id: str, **kwargs):
        captured["show_id"] = show_id
        captured["kwargs"] = kwargs
        return (
            [
                {
                    "id": f"asset-{index}",
                    "hosted_url": f"https://cdn.example.com/{index}.jpg",
                    "display_url": f"https://cdn.example.com/{index}-card.jpg",
                    "metadata": {"index": index},
                }
                for index in range(5001)
            ],
            2,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_assets", fake_get_show_assets)

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/assets?full=true&offset=25")

    assert response.status_code == 200
    body = response.json()
    assert len(body["assets"]) == 5000
    assert body["assets"][0]["id"] == "asset-0"
    assert body["assets"][-1]["id"] == "asset-4999"
    assert body["pagination"] == {
        "limit": 5000,
        "offset": 0,
        "count": 5000,
        "has_more": False,
        "next_cursor": None,
        "cursor": None,
        "truncated": True,
        "full": True,
    }
    assert captured == {
        "show_id": "00000000-0000-0000-0000-0000000000a1",
        "kwargs": {"limit": 5001, "offset": 0, "sources": None, "full": True},
    }


def test_season_assets_route_returns_gallery_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_show_season_assets(show_id: str, season_number: int, **kwargs):
        captured["show_id"] = show_id
        captured["season_number"] = season_number
        captured["kwargs"] = kwargs
        return (
            [{"id": f"asset-{index}", "hosted_url": f"https://cdn.example.com/{index}.jpg"} for index in range(49)],
            4,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_season_assets", fake_get_show_season_assets)

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/seasons/6/assets?limit=48")

    assert response.status_code == 200
    body = response.json()
    assert len(body["assets"]) == 48
    assert body["pagination"] == {
        "limit": 48,
        "offset": 0,
        "count": 48,
        "has_more": True,
        "next_cursor": "b2Zmc2V0OjQ4",
        "cursor": None,
        "truncated": False,
        "full": False,
    }
    assert captured == {
        "show_id": "00000000-0000-0000-0000-0000000000a1",
        "season_number": 6,
        "kwargs": {"limit": 49, "offset": 0, "sources": None, "full": False},
    }


def test_season_assets_route_returns_full_gallery_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_show_season_assets(show_id: str, season_number: int, **kwargs):
        captured["show_id"] = show_id
        captured["season_number"] = season_number
        captured["kwargs"] = kwargs
        return ([{"id": "asset-1", "hosted_url": "https://cdn.example.com/1.jpg"}], 4)

    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_show_season_assets",
        fake_get_show_season_assets,
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/seasons/6/assets?full=true")

    assert response.status_code == 200
    assert response.json()["pagination"] == {
        "limit": 5000,
        "offset": 0,
        "count": 1,
        "has_more": False,
        "next_cursor": None,
        "cursor": None,
        "truncated": False,
        "full": True,
    }
    assert captured == {
        "show_id": "00000000-0000-0000-0000-0000000000a1",
        "season_number": 6,
        "kwargs": {"limit": 5001, "offset": 0, "sources": None, "full": True},
    }


def test_season_assets_route_decodes_cursor_and_exposes_next_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_show_season_assets(show_id: str, season_number: int, **kwargs):
        captured["show_id"] = show_id
        captured["season_number"] = season_number
        captured["kwargs"] = kwargs
        return (
            [
                {"id": "asset-9", "hosted_url": "https://cdn.example.com/9.jpg"},
                {"id": "asset-10", "hosted_url": "https://cdn.example.com/10.jpg"},
                {"id": "asset-11", "hosted_url": "https://cdn.example.com/11.jpg"},
            ],
            1,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_season_assets", fake_get_show_season_assets)

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/seasons/6/assets?limit=2&cursor=b2Zmc2V0Ojg="
    )

    assert response.status_code == 200
    assert response.json()["pagination"] == {
        "limit": 2,
        "offset": 8,
        "count": 2,
        "has_more": True,
        "next_cursor": "b2Zmc2V0OjEw",
        "cursor": "b2Zmc2V0Ojg=",
        "truncated": False,
        "full": False,
    }
    assert captured == {
        "show_id": "00000000-0000-0000-0000-0000000000a1",
        "season_number": 6,
        "kwargs": {"limit": 3, "offset": 8, "sources": None, "full": False},
    }


def test_unassigned_backdrops_route_returns_backend_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_unassigned_season_backdrops",
        lambda season_id: (
            {
                "season": {"id": season_id, "show_id": "00000000-0000-0000-0000-0000000000a1", "season_number": 6},
                "backdrops": [{"media_asset_id": "asset-1", "display_url": "https://tmdb.example.com/1.jpg"}],
            },
            3,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/seasons/season-1/backdrops/unassigned")

    assert response.status_code == 200
    assert response.json()["season"]["show_id"] == "00000000-0000-0000-0000-0000000000a1"
    assert response.json()["backdrops"][0]["media_asset_id"] == "asset-1"


def test_assign_backdrops_route_invalidates_show_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    invalidated: list[str | None] = []
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "assign_season_backdrops",
        lambda season_id, media_asset_ids: (
            {
                "requested": 1,
                "assigned": 1,
                "skipped": 0,
                "mirrored_attempted": 1,
                "mirrored_failed": 0,
                "mirrored_failed_ids": [],
                "mirror_failures": [],
            },
            5,
            "00000000-0000-0000-0000-0000000000a1",
        ),
    )
    monkeypatch.setattr(
        router_module,
        "invalidate_show_read_cache",
        lambda *, show_id=None: invalidated.append(show_id),
    )

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/trr-api/seasons/season-1/backdrops/assign",
        json={"media_asset_ids": ["asset-1"]},
    )

    assert response.status_code == 200
    assert response.json()["assigned"] == 1
    assert invalidated == ["00000000-0000-0000-0000-0000000000a1"]


def test_invalidate_show_cache_clears_cached_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_detail(show_id: str):
        calls["count"] += 1
        return (
            {
                "id": show_id,
                "name": "The Traitors",
                "slug": "the-traitors",
                "canonical_slug": "the-traitors-us",
            },
            1,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_detail", fake_detail)

    client = TestClient(app)
    first = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1")
    second = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1")
    invalidate = client.post("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/cache/invalidate")
    third = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1")

    assert first.status_code == 200
    assert second.status_code == 200
    assert invalidate.status_code == 200
    assert third.status_code == 200
    assert calls["count"] == 2


def test_season_episodes_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_season_episodes",
        lambda season_id, limit=None, offset=None: (
            [
                {
                    "id": "episode-1",
                    "episode_number": 1,
                    "title": "Pilot",
                    "air_date": None,
                }
            ],
            1,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/seasons/season-1/episodes?limit=500&offset=0")

    assert response.status_code == 200
    assert response.json()["episodes"][0]["title"] == "Pilot"
    assert response.json()["pagination"]["limit"] == 500


def test_show_cast_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_show_cast",
        lambda show_id, **kwargs: (
            {
                "cast": [{"person_id": "person-1", "total_episodes": 4}],
                "archive_footage_cast": [],
                "cast_source": "episode_evidence",
                "eligibility_warning": None,
                "pagination": {"limit": kwargs["limit"], "offset": kwargs["offset"], "count": 1},
            },
            1,
        ),
    )

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/cast?limit=25&offset=0&minEpisodes=2"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["cast_source"] == "episode_evidence"
    assert payload["pagination"] == {"limit": 25, "offset": 0, "count": 1}


def test_show_cast_forwards_include_photos_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, Any] = {}

    def fake_get_show_cast(show_id: str, **kwargs):
        recorded["show_id"] = show_id
        recorded["kwargs"] = kwargs
        return (
            {
                "cast": [],
                "archive_footage_cast": [],
                "cast_source": "imdb_show_membership",
                "eligibility_warning": None,
                "pagination": {"limit": kwargs["limit"], "offset": kwargs["offset"], "count": 0},
            },
            1,
        )

    monkeypatch.setattr(router_module.show_reads_repo, "get_show_cast", fake_get_show_cast)

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/cast?limit=500&include_photos=false"
    )

    assert response.status_code == 200
    assert recorded["show_id"] == "00000000-0000-0000-0000-0000000000a1"
    assert recorded["kwargs"]["include_photos"] is False


def test_show_credits_returns_grouped_crew_rows_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_show_credits",
        lambda show_id: (
            {
                "cast_roster": [
                    {
                        "person_id": "person-1",
                        "person_name": "Heather Gay",
                        "roles": ["Housewife"],
                        "total_episodes": 80,
                    }
                ],
                "crew_sections": [
                    {
                        "title": "Producers",
                        "rows": [
                            {
                                "credit_id": "credit-1",
                                "person_id": "person-2",
                                "person_name": "Casey Allan",
                                "role": "supervising producer",
                                "episodes_label": "12 episodes",
                            }
                        ],
                        "grouped_rows": [
                            {
                                "person_id": "person-2",
                                "person_name": "Casey Allan",
                                "role_lines": [
                                    {
                                        "credit_id": "credit-1",
                                        "role": "supervising producer",
                                        "episodes_label": "12 episodes",
                                    },
                                    {
                                        "credit_id": "credit-2",
                                        "role": "associate producer",
                                        "episodes_label": "23 episodes",
                                    },
                                ],
                            }
                        ],
                    }
                ],
                "source_metadata": {"show_imdb_id": "tt11363282"},
            },
            1,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/credits")

    assert response.status_code == 200
    payload = response.json()
    assert payload["cast_roster"][0]["person_name"] == "Heather Gay"
    assert payload["crew_sections"][0]["grouped_rows"][0]["person_name"] == "Casey Allan"
    assert len(payload["crew_sections"][0]["grouped_rows"][0]["role_lines"]) == 2


def test_season_cast_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.show_reads_repo,
        "get_season_cast",
        lambda show_id, season_number, **kwargs: (
            {
                "cast": [{"person_id": "person-1", "episodes_in_season": 2}],
                "cast_source": "season_evidence",
                "eligibility_warning": None,
                "pagination": {"limit": kwargs["limit"], "offset": kwargs["offset"], "count": 1},
                "include_archive_only": kwargs["include_archive_only"],
            },
            1,
        ),
    )

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/trr-api/shows/00000000-0000-0000-0000-0000000000a1/seasons/6/cast?limit=10&offset=0&include_archive_only=true"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["cast_source"] == "season_evidence"
    assert payload["include_archive_only"] is True
