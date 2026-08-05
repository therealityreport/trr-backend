from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.schemas.v2.external_ids import MAX_EXTERNAL_ID_BATCH_SIZE, MAX_PERSON_EXTERNAL_IDS
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.repositories import external_id_reads

PERSON_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PERSON_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
SHOW_A = "11111111-1111-1111-1111-111111111111"
SHOW_B = "22222222-2222-2222-2222-222222222222"


def _external_id(source_id: str = "imdb") -> dict[str, object]:
    return {
        "id": 7,
        "source_id": source_id,
        "external_id": "nm1234567" if source_id == "imdb" else "demo",
        "is_primary": True,
        "valid_from": None,
        "valid_to": None,
        "observed_at": "2026-07-16T12:00:00+00:00",
        "created_at": "2026-07-15T12:00:00+00:00",
        "updated_at": "2026-07-16T12:00:00+00:00",
    }


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "trr-app-internal-admin",
        "admin_uid": "signed-admin-uid",
        "role": "internal_admin",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_v2_person_detail_forwards_include_inactive_and_returns_strict_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_get(person_id: str, *, include_inactive: bool):
        captured.update(person_id=person_id, include_inactive=include_inactive)
        return [_external_id()], 1

    monkeypatch.setattr(external_id_reads, "get_person_external_ids", fake_get)
    response = TestClient(app).get(
        f"/api/v2/admin/people/{PERSON_A}/external-ids?include_inactive=true",
        headers={"X-TRR-Admin-User-Uid": "spoofed-raw-header"},
    )

    assert response.status_code == 200
    assert response.json() == {"person_id": PERSON_A, "external_ids": [_external_id()]}
    assert captured == {"person_id": PERSON_A, "include_inactive": True}


def test_v2_batches_dedupe_inputs_and_preserve_repository_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_people(person_ids: list[str], *, include_inactive: bool):
        captured["people"] = (person_ids, include_inactive)
        return [
            {"person_id": PERSON_B, "external_ids": [_external_id("instagram")]},
            {"person_id": PERSON_A, "external_ids": []},
        ], 1

    def fake_shows(show_ids: list[str]):
        captured["shows"] = show_ids
        return [
            {"show_id": SHOW_B, "external_ids": {"imdb": "tt222"}},
            {"show_id": SHOW_A, "external_ids": None},
        ], 1

    monkeypatch.setattr(external_id_reads, "list_person_external_ids_by_person_ids", fake_people)
    monkeypatch.setattr(external_id_reads, "list_show_external_ids_by_show_ids", fake_shows)
    client = TestClient(app)

    people = client.post(
        "/api/v2/admin/people/external-ids/batch",
        json={"person_ids": [PERSON_B, PERSON_A, PERSON_B], "include_inactive": True},
    )
    shows = client.post(
        "/api/v2/admin/shows/external-ids/batch",
        json={"show_ids": [SHOW_B, SHOW_A, SHOW_B]},
    )

    assert people.status_code == 200
    assert people.json()["people"] == [
        {"person_id": PERSON_B, "external_ids": [_external_id("instagram")]},
        {"person_id": PERSON_A, "external_ids": []},
    ]
    assert shows.status_code == 200
    assert shows.json()["shows"] == [
        {"show_id": SHOW_B, "external_ids": {"imdb": "tt222"}},
        {"show_id": SHOW_A, "external_ids": None},
    ]
    assert captured == {
        "people": ([PERSON_B, PERSON_A], True),
        "shows": [SHOW_B, SHOW_A],
    }


@pytest.mark.parametrize(
    ("method", "path", "body", "expected_code"),
    [
        (
            "GET",
            "/api/v2/admin/people/not-a-uuid/external-ids",
            None,
            "INVALID_PERSON_ID",
        ),
        (
            "GET",
            f"/api/v2/admin/people/{PERSON_A}/external-ids?include_inactive=yes",
            None,
            "INVALID_INCLUDE_INACTIVE",
        ),
        (
            "POST",
            "/api/v2/admin/people/external-ids/batch",
            {"person_ids": [PERSON_A], "unexpected": True},
            "INVALID_PERSON_EXTERNAL_IDS_BATCH_REQUEST",
        ),
        (
            "POST",
            "/api/v2/admin/shows/external-ids/batch",
            {"show_ids": []},
            "INVALID_SHOW_EXTERNAL_IDS_BATCH_REQUEST",
        ),
    ],
)
def test_v2_external_id_invalid_requests_use_stable_problem_400(
    method: str,
    path: str,
    body: dict[str, object] | None,
    expected_code: str,
) -> None:
    response = TestClient(app).request(
        method,
        path,
        json=body,
        headers={"x-request-id": "external-ids-invalid"},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == expected_code
    assert response.json()["detail"]["status"] == 400
    assert response.json()["detail"]["request_id"] == "external-ids-invalid"
    assert response.json()["detail"]["retryable"] is False


def test_v2_person_detail_missing_is_stable_problem_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        external_id_reads,
        "get_person_external_ids",
        lambda *_args, **_kwargs: (None, 1),
    )
    response = TestClient(app).get(f"/api/v2/admin/people/{PERSON_A}/external-ids")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "PERSON_NOT_FOUND"
    assert response.json()["detail"]["message"] == "Person not found"


def test_v2_external_id_database_and_unexpected_failures_are_safe_problems(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args, **_kwargs):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(external_id_reads, "get_person_external_ids", unavailable)
    unavailable_response = TestClient(app).get(
        f"/api/v2/admin/people/{PERSON_A}/external-ids"
    )
    assert unavailable_response.status_code == 503
    assert unavailable_response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in unavailable_response.text

    monkeypatch.setattr(
        external_id_reads,
        "list_show_external_ids_by_show_ids",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("secret query")),
    )
    failed = TestClient(app).post(
        "/api/v2/admin/shows/external-ids/batch",
        json={"show_ids": [SHOW_A]},
    )
    assert failed.status_code == 500
    assert failed.json()["detail"]["code"] == "EXTERNAL_IDS_REQUEST_FAILED"
    assert "secret query" not in failed.text


def test_v2_external_id_response_drift_uses_safe_problem_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalid_record = {**_external_id(), "external_id": ""}
    monkeypatch.setattr(
        external_id_reads,
        "get_person_external_ids",
        lambda *_args, **_kwargs: ([invalid_record], 1),
    )

    response = TestClient(app).get(f"/api/v2/admin/people/{PERSON_A}/external-ids")

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "EXTERNAL_IDS_REQUEST_FAILED"
    assert "validation error" not in response.text.lower()


def test_v2_external_id_openapi_is_explicit_bounded_and_has_no_extra_routes() -> None:
    schema = app.openapi()
    expected = {
        (
            "/api/v2/admin/people/{person_id}/external-ids",
            "get",
        ): "getAdminPersonExternalIdsV2",
        (
            "/api/v2/admin/people/external-ids/batch",
            "post",
        ): "listAdminPersonExternalIdsBatchV2",
        (
            "/api/v2/admin/shows/external-ids/batch",
            "post",
        ): "listAdminShowExternalIdsBatchV2",
    }
    for (path, method), operation_id in expected.items():
        operation = schema["paths"][path][method]
        assert operation["operationId"] == operation_id
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])

    detail_operation = schema["paths"]["/api/v2/admin/people/{person_id}/external-ids"]["get"]
    assert "404" in detail_operation["responses"]
    assert {
        (parameter["name"], parameter["in"])
        for parameter in detail_operation["parameters"]
    } == {("person_id", "path"), ("include_inactive", "query")}
    assert "put" not in schema["paths"]["/api/v2/admin/people/{person_id}/external-ids"]
    assert "get" not in schema["paths"]["/api/v2/admin/people/external-ids/batch"]
    assert "get" not in schema["paths"]["/api/v2/admin/shows/external-ids/batch"]

    person_batch_schema = schema["paths"]["/api/v2/admin/people/external-ids/batch"]["post"][
        "requestBody"
    ]["content"]["application/json"]["schema"]
    show_batch_schema = schema["paths"]["/api/v2/admin/shows/external-ids/batch"]["post"][
        "requestBody"
    ]["content"]["application/json"]["schema"]
    assert person_batch_schema["additionalProperties"] is False
    assert show_batch_schema["additionalProperties"] is False
    assert person_batch_schema["properties"]["person_ids"]["maxItems"] == MAX_EXTERNAL_ID_BATCH_SIZE
    assert show_batch_schema["properties"]["show_ids"]["maxItems"] == MAX_EXTERNAL_ID_BATCH_SIZE
    person_response_schema = schema["components"]["schemas"]["PersonExternalIdsResponseV2"]
    person_record_schema = schema["components"]["schemas"]["PersonExternalIdV2"]
    assert person_response_schema["properties"]["external_ids"]["maxItems"] == MAX_PERSON_EXTERNAL_IDS
    assert set(person_record_schema["properties"]["source_id"]["enum"]) == {
        "imdb",
        "tmdb",
        "wikidata",
        "tvdb",
        "tvrage",
        "fandom",
        "facebook",
        "instagram",
        "threads",
        "twitter",
        "tiktok",
        "youtube",
    }
