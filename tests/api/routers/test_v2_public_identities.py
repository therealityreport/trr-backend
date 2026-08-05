from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers.v2 import identities
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.services import public_identity

SHOW_ID = "00000000-0000-0000-0000-000000000001"
PERSON_ID = "00000000-0000-0000-0000-000000000002"
SEASON_ID = "00000000-0000-0000-0000-000000000014"
MISSING_SHOW_ID = "00000000-0000-0000-0000-000000000099"


def test_show_identity_route_is_public_and_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        identities.public_identity,
        "resolve_show",
        lambda slug: {
            "resource_type": "show",
            "show_id": SHOW_ID,
            "show_name": "The Real Housewives of Beverly Hills",
            "requested_slug": slug,
            "canonical_slug": "rhobh",
            "match_kind": "alias",
            "canonical_path": "/shows/rhobh",
        },
    )

    response = TestClient(app).get("/api/v2/identities/shows/beverly-hills")

    assert response.status_code == 200
    assert response.json() == {
        "resource_type": "show",
        "show_id": SHOW_ID,
        "show_name": "The Real Housewives of Beverly Hills",
        "requested_slug": "beverly-hills",
        "canonical_slug": "rhobh",
        "match_kind": "alias",
        "canonical_path": "/shows/rhobh",
    }


def test_season_identity_route_derives_canonical_show_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        identities.public_identity,
        "resolve_season",
        lambda **kwargs: {
            "resource_type": "season",
            "season_id": SEASON_ID,
            "show_id": SHOW_ID,
            "show_name": "The Real Housewives of Beverly Hills",
            "season_number": kwargs["season_number"],
            "season_title": "Season 14",
            "requested_show_slug": kwargs["show_slug"],
            "canonical_show_slug": "rhobh",
            "show_match_kind": "alias",
            "canonical_path": "/shows/rhobh/seasons/14",
        },
    )

    response = TestClient(app).get("/api/v2/identities/shows/beverly-hills/seasons/14")

    assert response.status_code == 200
    assert response.json()["canonical_path"] == "/shows/rhobh/seasons/14"
    assert response.json()["canonical_show_slug"] == "rhobh"


def test_person_identity_route_accepts_one_show_context_without_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def resolve_person(slug: str, **kwargs):
        captured.update({"slug": slug, **kwargs})
        return {
            "resource_type": "person",
            "person_id": PERSON_ID,
            "full_name": "Alex Smith",
            "requested_slug": slug,
            "canonical_slug": "alex-smith--00000000",
            "match_kind": "alias",
            "canonical_path": "/people/alex-smith--00000000",
            "show_context": {
                "show_id": SHOW_ID,
                "show_name": "Bravo Show",
                "canonical_slug": "bravo-show",
            },
        }

    monkeypatch.setattr(identities.public_identity, "resolve_person", resolve_person)
    response = TestClient(app).get(f"/api/v2/identities/people/alex-smith?show_id={SHOW_ID}")

    assert response.status_code == 200
    assert captured == {"slug": "alex-smith", "show_id": SHOW_ID, "show_slug": None}
    assert response.json()["show_context"]["show_id"] == SHOW_ID


def test_ambiguity_uses_stable_problem_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    def ambiguous(slug: str):
        raise public_identity.IdentityResolutionError(
            code="IDENTITY_AMBIGUOUS",
            status=409,
            message="The requested show alias matches multiple identities.",
            detail={"resource_type": "show", "slug": slug, "candidate_count": 2, "candidates": []},
        )

    monkeypatch.setattr(identities.public_identity, "resolve_show", ambiguous)
    response = TestClient(app).get("/api/v2/identities/shows/shared-show", headers={"x-request-id": "request-1"})

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "code": "IDENTITY_AMBIGUOUS",
        "status": 409,
        "message": "The requested show alias matches multiple identities.",
        "trace_id": "request-1",
        "request_id": "request-1",
        "retryable": False,
        "detail": {"resource_type": "show", "slug": "shared-show", "candidate_count": 2, "candidates": []},
    }


def test_not_found_and_database_capacity_are_safe_problem_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    def not_found(slug: str):
        raise public_identity.IdentityResolutionError(
            code="IDENTITY_NOT_FOUND",
            status=404,
            message="The requested show identity was not found.",
            detail={"resource_type": "show", "slug": slug},
        )

    monkeypatch.setattr(identities.public_identity, "resolve_show", not_found)
    missing = TestClient(app).get("/api/v2/identities/shows/missing")
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "IDENTITY_NOT_FOUND"

    def unavailable(slug: str):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(identities.public_identity, "resolve_show", unavailable)
    unavailable_response = TestClient(app).get("/api/v2/identities/shows/rhobh")
    assert unavailable_response.status_code == 503
    assert unavailable_response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in unavailable_response.text


def test_person_route_rejects_both_show_contexts() -> None:
    response = TestClient(app).get(f"/api/v2/identities/people/alex-smith?show_id={SHOW_ID}&show_slug=bravo-show")

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_IDENTITY_CONTEXT"


@pytest.mark.parametrize(
    ("path", "expected_code"),
    [
        ("/api/v2/identities/people/alex-smith?show_id=not-a-uuid", "INVALID_IDENTITY_CONTEXT"),
        ("/api/v2/identities/people/alex-smith?show_id=", "INVALID_IDENTITY_CONTEXT"),
        ("/api/v2/identities/people/alex-smith?show_slug=", "INVALID_IDENTITY_SLUG"),
        ("/api/v2/identities/shows/rhobh/seasons/-1", "INVALID_SEASON_NUMBER"),
        ("/api/v2/identities/shows/rhobh/seasons/season-fourteen", "INVALID_SEASON_NUMBER"),
        (f"/api/v2/identities/shows/{'a' * 161}", "INVALID_IDENTITY_SLUG"),
    ],
)
def test_malformed_inputs_use_problem_400_instead_of_fastapi_422(path: str, expected_code: str) -> None:
    response = TestClient(app).get(path, headers={"x-request-id": "invalid-request"})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == expected_code
    assert detail["status"] == 400
    assert detail["request_id"] == "invalid-request"
    assert detail["trace_id"] == "invalid-request"
    assert detail["retryable"] is False


def test_invalid_person_slug_precedes_valid_but_missing_show_context_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context_lookup_called = False

    def get_show(value: str):
        nonlocal context_lookup_called
        context_lookup_called = True
        return None

    monkeypatch.setattr(identities.public_identity.identity_repo, "get_show_identity_by_id", get_show)

    response = TestClient(app).get(
        f"/api/v2/identities/people/bad_slug?show_id={MISSING_SHOW_ID}",
        headers={"x-request-id": "invalid-person-slug"},
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "INVALID_IDENTITY_SLUG"
    assert detail["status"] == 400
    assert detail["request_id"] == "invalid-person-slug"
    assert context_lookup_called is False


def test_v2_identity_openapi_is_explicit_and_deterministic() -> None:
    schema = app.openapi()
    expected = {
        "/api/v2/identities/shows/{slug}": "resolvePublicShowIdentityV2",
        "/api/v2/identities/shows/{show_slug}/seasons/{season_number}": "resolvePublicSeasonIdentityV2",
        "/api/v2/identities/people/{slug}": "resolvePublicPersonIdentityV2",
    }
    for path, operation_id in expected.items():
        operation = schema["paths"][path]["get"]
        assert operation["operationId"] == operation_id
        assert operation.get("security") in (None, [])
        assert {"200", "400", "404", "409", "500", "503"}.issubset(operation["responses"])
        assert "422" not in operation["responses"]

    season_parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/api/v2/identities/shows/{show_slug}/seasons/{season_number}"]["get"][
            "parameters"
        ]
    }
    assert season_parameters["season_number"]["schema"] == {
        "type": "integer",
        "minimum": 0,
        "maximum": 2_147_483_647,
        "example": 14,
    }
    person_parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/api/v2/identities/people/{slug}"]["get"]["parameters"]
    }
    assert person_parameters["show_id"]["schema"] == {"type": "string", "format": "uuid"}
