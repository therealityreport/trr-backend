from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2 import admin_people_reads
from trr_backend.db.pg import DatabaseServiceUnavailableError

PERSON_ID = "11111111-1111-1111-1111-111111111111"
MISSING_PERSON_ID = "11111111-1111-1111-1111-111111111199"
SHOW_ID = "22222222-2222-2222-2222-222222222222"


def _person(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": PERSON_ID,
        "full_name": "Lisa Barlow",
        "known_for": "The Real Housewives of Salt Lake City",
        "external_ids": {"imdb": "nm0000001"},
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "birthday": {"tmdb": "1974-12-21"},
        "gender": {"fandom": "Female"},
        "biography": {"tmdb": "Biography"},
        "place_of_birth": {"tmdb": "New York"},
        "homepage": {},
        "profile_image_url": {"tmdb": "/profile.jpg"},
        "alternative_names": {"tmdb": ["Lisa"]},
    }
    row.update(overrides)
    return row


@dataclass
class FakeCorePeopleReadsService:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def search_people(self, query: str, *, limit: int, offset: int):
        self.calls.append(("search_people", {"query": query, "limit": limit, "offset": offset}))
        return [_person(unexpected_database_field="filtered")], 1

    def get_person_by_id(self, person_id: str):
        self.calls.append(("get_person_by_id", {"person_id": person_id}))
        if person_id == MISSING_PERSON_ID:
            return None, 1
        return _person(id=person_id, unexpected_database_field="filtered"), 1

    def get_deduced_family_relationships_by_person_id(
        self,
        person_id: str,
        *,
        show_id: str | None = None,
    ):
        self.calls.append(
            (
                "get_deduced_family_relationships_by_person_id",
                {"person_id": person_id, "show_id": show_id},
            )
        )
        return {"John Barlow": "Dad", "Sibling Barlow": "Sister"}, 4


@pytest.fixture
def fake_service(monkeypatch: pytest.MonkeyPatch) -> FakeCorePeopleReadsService:
    service = FakeCorePeopleReadsService()
    monkeypatch.setattr(admin_people_reads, "people_reads_service", service)
    return service


@pytest.fixture
def app() -> FastAPI:
    test_app = FastAPI()
    test_app.include_router(admin_people_reads.router, prefix="/api/v2")
    test_app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "trr-app-internal-admin",
        "admin_uid": "signed-admin-uid",
        "role": "internal_admin",
    }
    return test_app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def test_people_list_detail_and_relationships_are_strict_admin_contracts(
    client: TestClient,
    fake_service: FakeCorePeopleReadsService,
) -> None:
    listed = client.get("/api/v2/admin/people?q=Lisa&limit=500&offset=2")
    detail = client.get(f"/api/v2/admin/people/{PERSON_ID}")
    relationships = client.get(f"/api/v2/admin/people/{PERSON_ID}/relationships?show_id={SHOW_ID}")

    assert listed.status_code == 200
    assert listed.json() == {
        "people": [
            {
                "id": PERSON_ID,
                "full_name": "Lisa Barlow",
                "known_for": "The Real Housewives of Salt Lake City",
                "external_ids": {"imdb": "nm0000001"},
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
            }
        ],
        "limit": 500,
        "offset": 2,
        "count": 1,
        "total_count": None,
        "has_more": False,
    }
    assert detail.status_code == 200
    assert detail.json()["person"]["birthday"] == {"tmdb": "1974-12-21"}
    assert detail.json()["person"]["alternative_names"] == {"tmdb": ["Lisa"]}
    assert "unexpected_database_field" not in detail.text
    assert relationships.status_code == 200
    assert relationships.json() == {
        "person_id": PERSON_ID,
        "show_id": SHOW_ID,
        "relationships": {"John Barlow": "Dad", "Sibling Barlow": "Sister"},
    }
    assert fake_service.calls == [
        ("search_people", {"query": "Lisa", "limit": 500, "offset": 2}),
        ("get_person_by_id", {"person_id": PERSON_ID}),
        (
            "get_deduced_family_relationships_by_person_id",
            {"person_id": PERSON_ID, "show_id": SHOW_ID},
        ),
    ]


@pytest.mark.parametrize(
    ("path", "expected_code"),
    [
        ("/api/v2/admin/people?limit=501", "INVALID_PAGINATION"),
        ("/api/v2/admin/people?offset=-1", "INVALID_PAGINATION"),
        ("/api/v2/admin/people?q=", "INVALID_SEARCH_QUERY"),
        ("/api/v2/admin/people/not-a-uuid", "INVALID_PERSON_ID"),
        (f"/api/v2/admin/people/{PERSON_ID}/relationships?show_id=bad", "INVALID_SHOW_ID"),
    ],
)
def test_invalid_inputs_use_stable_problem_400_without_fastapi_422(
    client: TestClient,
    fake_service: FakeCorePeopleReadsService,
    path: str,
    expected_code: str,
) -> None:
    response = client.get(path, headers={"x-request-id": "invalid-people-read"})

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == expected_code
    assert response.json()["detail"]["request_id"] == "invalid-people-read"
    assert "422" not in response.text
    assert fake_service.calls == []


def test_missing_person_uses_a_typed_404(
    client: TestClient,
    fake_service: FakeCorePeopleReadsService,
) -> None:
    response = client.get(f"/api/v2/admin/people/{MISSING_PERSON_ID}")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "PERSON_NOT_FOUND"
    assert fake_service.calls == [
        ("get_person_by_id", {"person_id": MISSING_PERSON_ID}),
    ]


def test_relationships_preserve_empty_map_semantics_without_a_person_preflight(
    client: TestClient,
    fake_service: FakeCorePeopleReadsService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def empty_relationships(person_id: str, *, show_id: str | None = None):
        fake_service.calls.append(
            (
                "get_deduced_family_relationships_by_person_id",
                {"person_id": person_id, "show_id": show_id},
            )
        )
        return {}, 1

    monkeypatch.setattr(
        fake_service,
        "get_deduced_family_relationships_by_person_id",
        empty_relationships,
    )

    response = client.get(f"/api/v2/admin/people/{MISSING_PERSON_ID}/relationships")

    assert response.status_code == 200
    assert response.json() == {
        "person_id": MISSING_PERSON_ID,
        "show_id": None,
        "relationships": {},
    }
    assert fake_service.calls == [
        (
            "get_deduced_family_relationships_by_person_id",
            {"person_id": MISSING_PERSON_ID, "show_id": None},
        )
    ]


def test_person_detail_accepts_explicit_null_multisource_fields(
    client: TestClient,
    fake_service: FakeCorePeopleReadsService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nullable_fields = (
        "birthday",
        "gender",
        "biography",
        "place_of_birth",
        "homepage",
        "profile_image_url",
        "alternative_names",
    )
    person = _person(**dict.fromkeys(nullable_fields))
    monkeypatch.setattr(fake_service, "get_person_by_id", lambda _person_id: (person, 1))

    response = client.get(f"/api/v2/admin/people/{PERSON_ID}")

    assert response.status_code == 200
    for field_name in nullable_fields:
        assert response.json()["person"][field_name] is None


def test_database_capacity_and_unexpected_failures_use_safe_problem_responses(
    client: TestClient,
    fake_service: FakeCorePeopleReadsService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args: Any, **_kwargs: Any):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(fake_service, "search_people", unavailable)
    unavailable_response = client.get("/api/v2/admin/people")

    assert unavailable_response.status_code == 503
    assert unavailable_response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in unavailable_response.text

    def unexpected(*_args: Any, **_kwargs: Any):
        raise RuntimeError("secret implementation detail")

    monkeypatch.setattr(fake_service, "get_person_by_id", unexpected)
    failed_response = client.get(f"/api/v2/admin/people/{PERSON_ID}")

    assert failed_response.status_code == 500
    assert failed_response.json()["detail"]["code"] == "PEOPLE_READ_REQUEST_FAILED"
    assert "secret implementation detail" not in failed_response.text


def test_all_people_routes_require_strict_internal_admin_auth(
    fake_service: FakeCorePeopleReadsService,
) -> None:
    unauthenticated_app = FastAPI()
    unauthenticated_app.include_router(admin_people_reads.router, prefix="/api/v2")
    client = TestClient(unauthenticated_app)

    responses = [
        client.get("/api/v2/admin/people"),
        client.get(f"/api/v2/admin/people/{PERSON_ID}"),
        client.get(f"/api/v2/admin/people/{PERSON_ID}/relationships"),
    ]

    assert [response.status_code for response in responses] == [401, 401, 401]
    assert fake_service.calls == []


def test_v2_admin_people_openapi_is_explicit_strict_and_bounded(app: FastAPI) -> None:
    schema = app.openapi()
    expected = {
        "/api/v2/admin/people": "listAdminPeopleV2",
        "/api/v2/admin/people/{person_id}": "getAdminPersonV2",
        "/api/v2/admin/people/{person_id}/relationships": "getAdminPersonRelationshipsV2",
    }
    for path, operation_id in expected.items():
        operation = schema["paths"][path]["get"]
        assert operation["operationId"] == operation_id
        assert operation["security"] == [{"InternalAdminBearer": []}]
        assert "422" not in operation["responses"]
        assert {"200", "400", "500", "503"}.issubset(operation["responses"])

    assert "404" in schema["paths"]["/api/v2/admin/people/{person_id}"]["get"]["responses"]
    assert "404" not in schema["paths"]["/api/v2/admin/people/{person_id}/relationships"]["get"]["responses"]
    list_parameters = {
        parameter["name"]: parameter for parameter in schema["paths"]["/api/v2/admin/people"]["get"]["parameters"]
    }
    assert list_parameters["limit"]["schema"] == {
        "type": "integer",
        "minimum": 1,
        "maximum": 500,
        "default": 20,
    }
    assert list_parameters["offset"]["schema"] == {
        "type": "integer",
        "minimum": 0,
        "default": 0,
    }
    assert list_parameters["q"]["schema"] == {
        "type": "string",
        "minLength": 1,
        "maxLength": 200,
    }
    for model_name in (
        "AdminPersonSummaryV2",
        "AdminPersonV2",
        "AdminPeopleListResponseV2",
        "AdminPersonResponseV2",
        "AdminPersonRelationshipsResponseV2",
        "AdminPeopleReadProblemDetailV2",
        "AdminPeopleReadProblemResponseV2",
    ):
        assert schema["components"]["schemas"][model_name]["additionalProperties"] is False
