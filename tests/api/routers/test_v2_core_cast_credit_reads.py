from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.v2 import core_cast_credit_reads
from trr_backend.db.pg import DatabaseServiceUnavailableError

SHOW_ID = "11111111-1111-1111-1111-111111111111"
SEASON_ID = "11111111-1111-1111-1111-111111111112"
PERSON_ID = "22222222-2222-2222-2222-222222222222"


def _cast_member(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": "33333333-3333-3333-3333-333333333333",
        "show_id": SHOW_ID,
        "person_id": PERSON_ID,
        "show_name": "Test Show",
        "cast_member_name": "Person One",
        "role": "Self",
        "billing_order": 1,
        "credit_category": "Self",
        "source_type": "imdb",
        "full_name": "Person One",
        "known_for": "Reality TV",
        "photo_url": None,
        "thumbnail_focus_x": None,
        "thumbnail_focus_y": None,
        "thumbnail_zoom": None,
        "thumbnail_crop_mode": None,
        "total_episodes": 4,
        "archive_episode_count": 1,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    row.update(overrides)
    return row


def _season_count_member(**overrides: Any) -> dict[str, Any]:
    row = {
        "person_id": PERSON_ID,
        "person_name": "Person One",
        "episodes_in_season": 4,
        "total_episodes": 4,
        "photo_url": "https://cdn.example/person.jpg",
        "thumbnail_focus_x": None,
        "thumbnail_focus_y": None,
        "thumbnail_zoom": None,
        "thumbnail_crop_mode": None,
        "archive_episodes_in_season": 1,
    }
    row.update(overrides)
    return row


def _person_credit(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": f"imdb-{PERSON_ID}-tt1000001",
        "show_id": SHOW_ID,
        "person_id": PERSON_ID,
        "show_name": "Test Show",
        "role": None,
        "billing_order": None,
        "credit_category": "Self",
        "source_type": "imdb_name_fullcredits",
        "external_imdb_id": "tt1000001",
        "external_url": "https://www.imdb.com/title/tt1000001/",
        "metadata": None,
    }
    row.update(overrides)
    return row


def _episode_credit(**overrides: Any) -> dict[str, Any]:
    row = {
        "show_id": SHOW_ID,
        "credit_id": "33333333-3333-3333-3333-333333333333",
        "credit_category": "Self",
        "role": "Host",
        "billing_order": 1,
        "source_type": "imdb",
        "episode_id": "44444444-4444-4444-4444-444444444444",
        "season_number": 5,
        "episode_number": 2,
        "episode_name": "Dinner",
        "appearance_type": "archive_footage",
    }
    row.update(overrides)
    return row


@dataclass
class FakeCoreCastCreditReadsService:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def get_show_cast(self, show_id: str, **kwargs: Any):
        self.calls.append(("get_show_cast", {"show_id": show_id, **kwargs}))
        return [_cast_member(unexpected_database_field="filtered")], 4

    def get_season_cast(self, season_id: str, **kwargs: Any):
        self.calls.append(("get_season_cast", {"season_id": season_id, **kwargs}))
        return [_season_count_member(unexpected_database_field="filtered")], 6

    def get_person_credits(self, person_id: str, **kwargs: Any):
        self.calls.append(("get_person_credits", {"person_id": person_id, **kwargs}))
        return (
            {
                "credits": [_person_credit(unexpected_database_field="filtered")],
                "curated_cast_show_ids": [SHOW_ID],
                "total_count": 3,
            },
            4,
        )

    def get_person_episode_credits(self, person_id: str, **kwargs: Any):
        self.calls.append(("get_person_episode_credits", {"person_id": person_id, **kwargs}))
        return ({"episode_credits": [_episode_credit(unexpected_database_field="filtered")], "total_count": 1}, 1)


@pytest.fixture
def fake_service(monkeypatch: pytest.MonkeyPatch) -> FakeCoreCastCreditReadsService:
    service = FakeCoreCastCreditReadsService()
    monkeypatch.setattr(core_cast_credit_reads, "_core_cast_credit_reads_service", service)
    return service


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(core_cast_credit_reads.router, prefix="/api/v2")
    return TestClient(app)


def test_show_cast_route_is_public_strict_and_preserves_all_view_options(
    client: TestClient,
    fake_service: FakeCoreCastCreditReadsService,
) -> None:
    response = client.get(
        f"/api/v2/shows/{SHOW_ID}/cast"
        "?view=episode_evidence&include_photos=false&photo_fallback=bravo&limit=500&offset=2"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["show_id"] == SHOW_ID
    assert payload["view"] == "episode_evidence"
    assert payload["include_photos"] is False
    assert payload["photo_fallback"] == "bravo"
    assert payload["cast"][0]["total_episodes"] == 4
    assert "unexpected_database_field" not in response.text
    assert payload["limit"] == 500
    assert payload["offset"] == 2
    assert fake_service.calls == [
        (
            "get_show_cast",
            {
                "show_id": SHOW_ID,
                "view": "episode_evidence",
                "limit": 500,
                "offset": 2,
                "include_photos": False,
                "photo_fallback": "bravo",
            },
        )
    ]


def test_season_cast_route_exposes_episode_counts_and_archive_only_option(
    client: TestClient,
    fake_service: FakeCoreCastCreditReadsService,
) -> None:
    response = client.get(
        f"/api/v2/seasons/{SEASON_ID}/cast?view=episode_counts&include_archive_only=true&photo_fallback=bravo&limit=25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["season_id"] == SEASON_ID
    assert payload["view"] == "episode_counts"
    assert payload["include_archive_only"] is True
    assert payload["photo_fallback"] == "bravo"
    assert payload["cast"][0]["episodes_in_season"] == 4
    assert payload["cast"][0]["archive_episodes_in_season"] == 1
    assert "unexpected_database_field" not in response.text
    assert fake_service.calls == [
        (
            "get_season_cast",
            {
                "season_id": SEASON_ID,
                "view": "episode_counts",
                "limit": 25,
                "offset": 0,
                "include_archive_only": True,
                "photo_fallback": "bravo",
            },
        )
    ]


def test_person_credits_route_keeps_string_ids_pagination_and_curated_show_ids(
    client: TestClient,
    fake_service: FakeCoreCastCreditReadsService,
) -> None:
    response = client.get(f"/api/v2/people/{PERSON_ID}/credits?limit=2&offset=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["person_id"] == PERSON_ID
    assert payload["credits"][0]["id"] == f"imdb-{PERSON_ID}-tt1000001"
    assert payload["curated_cast_show_ids"] == [SHOW_ID]
    assert payload["total_count"] == 3
    assert payload["has_more"] is True
    assert "unexpected_database_field" not in response.text
    assert fake_service.calls == [
        (
            "get_person_credits",
            {"person_id": PERSON_ID, "limit": 2, "offset": 1},
        )
    ]


def test_person_episode_credits_route_supports_optional_show_scope_and_archive_toggle(
    client: TestClient,
    fake_service: FakeCoreCastCreditReadsService,
) -> None:
    response = client.get(
        f"/api/v2/people/{PERSON_ID}/episode-credits?show_id={SHOW_ID}&include_archive_footage=true&limit=500"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["person_id"] == PERSON_ID
    assert payload["show_id"] == SHOW_ID
    assert payload["include_archive_footage"] is True
    assert payload["episode_credits"][0]["show_id"] == SHOW_ID
    assert payload["episode_credits"][0]["appearance_type"] == "archive_footage"
    assert "unexpected_database_field" not in response.text
    assert fake_service.calls == [
        (
            "get_person_episode_credits",
            {
                "person_id": PERSON_ID,
                "show_id": SHOW_ID,
                "include_archive_footage": True,
                "limit": 500,
                "offset": 0,
            },
        )
    ]


@pytest.mark.parametrize(
    ("path", "code"),
    [
        ("/api/v2/shows/not-a-uuid/cast", "INVALID_SHOW_ID"),
        (f"/api/v2/shows/{SHOW_ID}/cast?view=legacy", "INVALID_SHOW_CAST_VIEW"),
        (f"/api/v2/shows/{SHOW_ID}/cast?include_photos=maybe", "INVALID_BOOLEAN_QUERY"),
        (f"/api/v2/shows/{SHOW_ID}/cast?photo_fallback=remote", "INVALID_PHOTO_FALLBACK"),
        (f"/api/v2/seasons/{SEASON_ID}/cast?view=legacy", "INVALID_SEASON_CAST_VIEW"),
        (f"/api/v2/people/{PERSON_ID}/episode-credits?show_id=bad", "INVALID_SHOW_ID"),
        (f"/api/v2/people/{PERSON_ID}/credits?limit=501", "INVALID_PAGINATION"),
    ],
)
def test_invalid_inputs_use_stable_problem_400_without_fastapi_422(
    client: TestClient,
    fake_service: FakeCoreCastCreditReadsService,
    path: str,
    code: str,
) -> None:
    response = client.get(path, headers={"x-request-id": "invalid-cast-credit"})

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == code
    assert response.json()["detail"]["request_id"] == "invalid-cast-credit"
    assert "422" not in response.text
    assert fake_service.calls == []


def test_valid_missing_ids_return_200_empty_arrays(
    client: TestClient,
    fake_service: FakeCoreCastCreditReadsService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fake_service, "get_show_cast", lambda *args, **kwargs: ([], 1))
    monkeypatch.setattr(fake_service, "get_season_cast", lambda *args, **kwargs: ([], 1))
    monkeypatch.setattr(
        fake_service,
        "get_person_credits",
        lambda *args, **kwargs: ({"credits": [], "curated_cast_show_ids": [], "total_count": 0}, 3),
    )
    monkeypatch.setattr(
        fake_service,
        "get_person_episode_credits",
        lambda *args, **kwargs: ({"episode_credits": [], "total_count": 0}, 1),
    )

    show = client.get(f"/api/v2/shows/{SHOW_ID}/cast")
    season = client.get(f"/api/v2/seasons/{SEASON_ID}/cast")
    credits = client.get(f"/api/v2/people/{PERSON_ID}/credits")
    episode_credits = client.get(f"/api/v2/people/{PERSON_ID}/episode-credits")

    assert show.status_code == season.status_code == credits.status_code == episode_credits.status_code == 200
    assert show.json()["cast"] == []
    assert season.json()["cast"] == []
    assert credits.json()["credits"] == []
    assert credits.json()["curated_cast_show_ids"] == []
    assert episode_credits.json()["episode_credits"] == []


def test_database_capacity_problem_is_safe_and_stable(
    client: TestClient,
    fake_service: FakeCoreCastCreditReadsService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*args: Any, **kwargs: Any):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(fake_service, "get_show_cast", unavailable)

    response = client.get(f"/api/v2/shows/{SHOW_ID}/cast")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in response.text


def test_v2_core_cast_credit_openapi_is_explicit_public_and_capped(client: TestClient) -> None:
    schema = cast("Any", client.app).openapi()
    expected = {
        "/api/v2/shows/{show_id}/cast": ("listPublicCoreShowCastV2", "show_id"),
        "/api/v2/seasons/{season_id}/cast": ("listPublicCoreSeasonCastV2", "season_id"),
        "/api/v2/people/{person_id}/credits": ("listPublicCorePersonCreditsV2", "person_id"),
        "/api/v2/people/{person_id}/episode-credits": ("listPublicCorePersonEpisodeCreditsV2", "person_id"),
    }
    for path, (operation_id, path_parameter_name) in expected.items():
        operation = schema["paths"][path]["get"]
        assert operation["operationId"] == operation_id
        assert operation["security"] == []
        assert "422" not in operation["responses"]
        assert {"200", "400", "500", "503"}.issubset(operation["responses"])
        parameters = {parameter["name"]: parameter for parameter in operation["parameters"]}
        assert parameters[path_parameter_name] == {
            "name": path_parameter_name,
            "in": "path",
            "required": True,
            "schema": {"type": "string", "format": "uuid"},
        }
        assert parameters["limit"]["schema"] == {
            "type": "integer",
            "minimum": 1,
            "maximum": 500,
            "default": 50,
        }

    season_parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/api/v2/seasons/{season_id}/cast"]["get"]["parameters"]
    }
    assert season_parameters["photo_fallback"]["schema"] == {
        "type": "string",
        "enum": ["none", "bravo"],
        "default": "none",
    }
