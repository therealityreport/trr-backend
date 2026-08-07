from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.v2 import core_show_reads
from trr_backend.db.pg import DatabaseServiceUnavailableError

SHOW_ID = "11111111-1111-1111-1111-111111111111"
SEASON_ID = "22222222-2222-2222-2222-222222222222"
EPISODE_ID = "33333333-3333-3333-3333-333333333333"


def _show(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": SHOW_ID,
        "name": "The Real Housewives of Beverly Hills",
        "description": "A Bravo reality series.",
        "premiere_date": "2010-10-14",
        "network": "Bravo",
        "streaming": "Peacock",
        "show_total_seasons": 14,
        "show_total_episodes": 310,
        "imdb_series_id": "tt1720601",
        "tmdb_series_id": 32390,
        "most_recent_episode": "Reunion Part 3",
        "primary_tmdb_poster_path": "/poster.jpg",
        "primary_tmdb_backdrop_path": "/backdrop.jpg",
        "primary_tmdb_logo_path": "/logo.svg",
        "external_ids": {"imdb": "tt1720601", "tmdb": "32390"},
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    row.update(overrides)
    return row


def _season(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": SEASON_ID,
        "show_id": SHOW_ID,
        "show_name": "The Real Housewives of Beverly Hills",
        "name": "Season 14",
        "season_number": 14,
        "title": "Season 14",
        "overview": "A season overview.",
        "air_date": "2024-11-19",
        "premiere_date": "2024-11-19",
        "tmdb_series_id": 32390,
        "imdb_series_id": "tt1720601",
        "tmdb_season_id": 401,
        "tmdb_season_object_id": "abc123",
        "poster_path": "/season.jpg",
        "url_original_poster": "https://image.tmdb.org/t/p/original/season.jpg",
        "external_tvdb_id": 123,
        "external_wikidata_id": "Q123",
        "external_ids": {"tmdb": "401"},
        "language": "en-US",
        "fetched_at": "2026-01-03T00:00:00Z",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "episode_signal": None,
    }
    row.update(overrides)
    return row


def _episode(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": EPISODE_ID,
        "show_id": SHOW_ID,
        "season_id": SEASON_ID,
        "show_name": "The Real Housewives of Beverly Hills",
        "title": "Grace Time Is Over",
        "season_number": 14,
        "episode_number": 1,
        "air_date": "2024-11-19",
        "synopsis": "Episode synopsis.",
        "overview": "Episode overview.",
        "imdb_episode_id": "tt30000001",
        "imdb_rating": 7.1,
        "imdb_vote_count": 100,
        "imdb_primary_image_url": "https://images.example/episode.jpg",
        "imdb_primary_image_caption": "Episode image",
        "imdb_primary_image_width": 1280,
        "imdb_primary_image_height": 720,
        "tmdb_series_id": 32390,
        "tmdb_episode_id": 9001,
        "episode_type": "standard",
        "production_code": "1401",
        "runtime": 43,
        "still_path": "/still.jpg",
        "url_original_still": "https://image.tmdb.org/t/p/original/still.jpg",
        "tmdb_vote_average": 7.4,
        "tmdb_vote_count": 22,
        "external_ids": {"imdb": "tt30000001", "tmdb": "9001"},
        "fetched_at": "2026-01-03T00:00:00Z",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    row.update(overrides)
    return row


@dataclass
class FakeCoreShowReadsService:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    fail_database: bool = False

    def _maybe_fail(self) -> None:
        if self.fail_database:
            raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    def search_shows(self, query: str, *, limit: int, offset: int):
        self._maybe_fail()
        self.calls.append(("search_shows", {"query": query, "limit": limit, "offset": offset}))
        return [_show(unexpected_database_field="filtered-out")], 1

    def get_show_by_id(self, show_id: str):
        self._maybe_fail()
        self.calls.append(("get_show_by_id", {"show_id": show_id}))
        return (_show(id=show_id), 1) if show_id == SHOW_ID else (None, 1)

    def get_seasons_by_show_id(self, show_id: str, *, limit: int, offset: int, include_episode_signal: bool):
        self._maybe_fail()
        self.calls.append(
            (
                "get_seasons_by_show_id",
                {
                    "show_id": show_id,
                    "limit": limit,
                    "offset": offset,
                    "include_episode_signal": include_episode_signal,
                },
            )
        )
        season = _season(
            episode_airdate_count=19,
            has_scheduled_or_aired_episode=True,
        )
        return [season], 1

    def get_season_by_id(self, season_id: str):
        self._maybe_fail()
        self.calls.append(("get_season_by_id", {"season_id": season_id}))
        return (_season(id=season_id), 1) if season_id == SEASON_ID else (None, 1)

    def get_season_by_show_and_number(self, show_id: str, season_number: int):
        self._maybe_fail()
        self.calls.append(("get_season_by_show_and_number", {"show_id": show_id, "season_number": season_number}))
        return _season(show_id=show_id, season_number=season_number), 1

    def get_episodes_by_season_id(self, season_id: str, *, limit: int, offset: int):
        self._maybe_fail()
        self.calls.append(("get_episodes_by_season_id", {"season_id": season_id, "limit": limit, "offset": offset}))
        return [_episode(season_id=season_id)], 1

    def get_episodes_by_show_and_season(self, show_id: str, season_number: int, *, limit: int, offset: int):
        self._maybe_fail()
        self.calls.append(
            (
                "get_episodes_by_show_and_season",
                {"show_id": show_id, "season_number": season_number, "limit": limit, "offset": offset},
            )
        )
        return [_episode(show_id=show_id, season_number=season_number)], 1

    def get_episode_by_id(self, episode_id: str):
        self._maybe_fail()
        self.calls.append(("get_episode_by_id", {"episode_id": episode_id}))
        return (_episode(id=episode_id), 1) if episode_id == EPISODE_ID else (None, 1)

    def search_episodes(self, query: str, *, limit: int, offset: int):
        self._maybe_fail()
        self.calls.append(("search_episodes", {"query": query, "limit": limit, "offset": offset}))
        return [_episode()], 1


@pytest.fixture
def fake_service(monkeypatch: pytest.MonkeyPatch) -> FakeCoreShowReadsService:
    service = FakeCoreShowReadsService()
    monkeypatch.setattr(core_show_reads, "_core_show_reads_service", service)
    return service


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(core_show_reads.router, prefix="/api/v2")
    return TestClient(app)


def test_show_list_and_detail_are_public_typed_and_paginated(
    client: TestClient,
    fake_service: FakeCoreShowReadsService,
) -> None:
    list_response = client.get("/api/v2/shows?q=housewives&limit=10&offset=5")
    detail_response = client.get(f"/api/v2/shows/{SHOW_ID}")

    assert list_response.status_code == 200
    assert list_response.json()["shows"][0]["id"] == SHOW_ID
    assert list_response.json()["limit"] == 10
    assert detail_response.status_code == 200
    assert detail_response.json()["show"]["name"] == "The Real Housewives of Beverly Hills"
    assert "unexpected_database_field" not in list_response.text
    assert fake_service.calls[:2] == [
        ("search_shows", {"query": "housewives", "limit": 10, "offset": 5}),
        ("get_show_by_id", {"show_id": SHOW_ID}),
    ]


def test_season_routes_include_optional_episode_signal(
    client: TestClient,
    fake_service: FakeCoreShowReadsService,
) -> None:
    seasons = client.get(f"/api/v2/shows/{SHOW_ID}/seasons?include_episode_signal=true&limit=5")
    season_by_id = client.get(f"/api/v2/seasons/{SEASON_ID}")
    season_by_number = client.get(f"/api/v2/shows/{SHOW_ID}/seasons/14")

    assert seasons.status_code == 200
    assert seasons.json()["include_episode_signal"] is True
    assert seasons.json()["seasons"][0]["episode_signal"]["episode_count"] == 19
    assert season_by_id.status_code == 200
    assert season_by_id.json()["season"]["id"] == SEASON_ID
    assert season_by_number.status_code == 200
    assert season_by_number.json()["season"]["season_number"] == 14
    assert fake_service.calls == [
        (
            "get_seasons_by_show_id",
            {"show_id": SHOW_ID, "limit": 5, "offset": 0, "include_episode_signal": True},
        ),
        ("get_season_by_id", {"season_id": SEASON_ID}),
        ("get_season_by_show_and_number", {"show_id": SHOW_ID, "season_number": 14}),
    ]


def test_episode_routes_are_public_typed_and_searchable(
    client: TestClient,
    fake_service: FakeCoreShowReadsService,
) -> None:
    by_season = client.get(f"/api/v2/seasons/{SEASON_ID}/episodes")
    by_show_season = client.get(f"/api/v2/shows/{SHOW_ID}/seasons/14/episodes")
    detail = client.get(f"/api/v2/episodes/{EPISODE_ID}")
    search = client.get("/api/v2/episodes?q=grace&limit=25&offset=2")

    assert by_season.status_code == 200
    assert by_show_season.status_code == 200
    assert detail.status_code == 200
    assert detail.json()["episode"]["id"] == EPISODE_ID
    assert search.status_code == 200
    assert search.json()["episodes"][0]["title"] == "Grace Time Is Over"
    assert fake_service.calls == [
        ("get_episodes_by_season_id", {"season_id": SEASON_ID, "limit": 50, "offset": 0}),
        (
            "get_episodes_by_show_and_season",
            {"show_id": SHOW_ID, "season_number": 14, "limit": 50, "offset": 0},
        ),
        ("get_episode_by_id", {"episode_id": EPISODE_ID}),
        ("search_episodes", {"query": "grace", "limit": 25, "offset": 2}),
    ]


def test_episode_route_preserves_existing_500_row_limit(
    client: TestClient,
    fake_service: FakeCoreShowReadsService,
) -> None:
    response = client.get(f"/api/v2/shows/{SHOW_ID}/seasons/14/episodes?limit=500")

    assert response.status_code == 200
    assert fake_service.calls == [
        (
            "get_episodes_by_show_and_season",
            {"show_id": SHOW_ID, "season_number": 14, "limit": 500, "offset": 0},
        ),
    ]


def test_show_detail_preserves_existing_app_show_fields(
    client: TestClient,
    fake_service: FakeCoreShowReadsService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    poster_id = "44444444-4444-4444-4444-444444444444"
    backdrop_id = "55555555-5555-5555-5555-555555555555"
    logo_id = "66666666-6666-6666-6666-666666666666"

    def get_show_by_id(show_id: str):
        return (
            _show(
                id=show_id,
                imdb_id="tt1720601",
                tmdb_id=32390,
                most_recent_episode={"season": 14, "episode": 3},
                primary_poster_image_id=poster_id,
                primary_backdrop_image_id=backdrop_id,
                primary_logo_image_id=logo_id,
                tmdb_status="Returning Series",
                tmdb_vote_average=7.8,
                imdb_rating_value=6.2,
            ),
            1,
        )

    monkeypatch.setattr(fake_service, "get_show_by_id", get_show_by_id)

    response = client.get(f"/api/v2/shows/{SHOW_ID}")

    assert response.status_code == 200
    show = response.json()["show"]
    assert show["imdb_id"] == "tt1720601"
    assert show["tmdb_id"] == 32390
    assert show["most_recent_episode"] == {"season": 14, "episode": 3}
    assert show["primary_poster_image_id"] == poster_id
    assert show["primary_backdrop_image_id"] == backdrop_id
    assert show["primary_logo_image_id"] == logo_id
    assert show["tmdb_status"] == "Returning Series"
    assert show["tmdb_vote_average"] == 7.8
    assert show["imdb_rating_value"] == 6.2


@pytest.mark.parametrize(
    ("path", "code"),
    [
        ("/api/v2/shows/not-a-uuid", "INVALID_SHOW_ID"),
        (f"/api/v2/shows/{SHOW_ID}/seasons/-1", "INVALID_SEASON_NUMBER"),
        ("/api/v2/shows?limit=0", "INVALID_PAGINATION"),
        ("/api/v2/shows?offset=-1", "INVALID_PAGINATION"),
        ("/api/v2/shows?q=", "INVALID_SEARCH_QUERY"),
        (f"/api/v2/shows/{SHOW_ID}/seasons?include_episode_signal=maybe", "INVALID_BOOLEAN_QUERY"),
    ],
)
def test_invalid_inputs_use_stable_problem_400_without_fastapi_422(
    client: TestClient,
    fake_service: FakeCoreShowReadsService,
    path: str,
    code: str,
) -> None:
    response = client.get(path, headers={"x-request-id": "invalid-core-read"})

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == code
    assert response.json()["detail"]["request_id"] == "invalid-core-read"
    assert "422" not in response.text
    assert fake_service.calls == []


def test_missing_detail_and_database_capacity_use_safe_problem_responses(
    client: TestClient,
    fake_service: FakeCoreShowReadsService,
) -> None:
    missing = client.get("/api/v2/shows/99999999-9999-9999-9999-999999999999")
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "SHOW_NOT_FOUND"

    fake_service.fail_database = True
    unavailable = client.get("/api/v2/shows")
    assert unavailable.status_code == 503
    assert unavailable.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in unavailable.text


def test_missing_backend_service_is_safe_503(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core_show_reads, "_core_show_reads_service", None)

    response = client.get("/api/v2/shows")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "CORE_SHOW_READS_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["retryable"] is True


def test_v2_core_show_reads_openapi_is_explicit_and_public(client: TestClient) -> None:
    schema = cast("Any", client.app).openapi()
    expected = {
        "/api/v2/shows": "listPublicCoreShowsV2",
        "/api/v2/shows/{show_id}": "getPublicCoreShowV2",
        "/api/v2/shows/{show_id}/seasons": "listPublicCoreShowSeasonsV2",
        "/api/v2/seasons/{season_id}": "getPublicCoreSeasonV2",
        "/api/v2/shows/{show_id}/seasons/{season_number}": "getPublicCoreShowSeasonByNumberV2",
        "/api/v2/seasons/{season_id}/episodes": "listPublicCoreSeasonEpisodesV2",
        "/api/v2/shows/{show_id}/seasons/{season_number}/episodes": "listPublicCoreShowSeasonEpisodesV2",
        "/api/v2/episodes/{episode_id}": "getPublicCoreEpisodeV2",
        "/api/v2/episodes": "listPublicCoreEpisodesV2",
    }
    for path, operation_id in expected.items():
        operation = schema["paths"][path]["get"]
        assert operation["operationId"] == operation_id
        assert operation.get("security") in (None, [])
        assert "422" not in operation["responses"]
        assert {"200", "400", "500", "503"}.issubset(operation["responses"])

    season_list_parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/api/v2/shows/{show_id}/seasons"]["get"]["parameters"]
    }
    assert season_list_parameters["include_episode_signal"]["schema"] == {"type": "boolean", "default": False}
    assert season_list_parameters["limit"]["schema"] == {
        "type": "integer",
        "minimum": 1,
        "maximum": 500,
        "default": 50,
    }
