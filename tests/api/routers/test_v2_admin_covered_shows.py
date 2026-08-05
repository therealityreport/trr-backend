from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.services import covered_shows as covered_shows_service

COVERED_ID = "00000000-0000-0000-0000-000000000010"
SHOW_ID = "00000000-0000-0000-0000-000000000011"
MISSING_SHOW_ID = "00000000-0000-0000-0000-000000000099"


def _show_payload(show_id: str = SHOW_ID) -> dict[str, object]:
    return {
        "id": COVERED_ID,
        "trr_show_id": show_id,
        "show_name": "Bravo Show",
        "canonical_slug": "bravo-show",
        "alternative_names": ["Bravo"],
        "show_total_episodes": 12,
        "poster_url": "https://cdn.example.com/poster.jpg",
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


@pytest.fixture(autouse=True)
def clear_cache():
    covered_shows_service.invalidate_cache()
    yield
    covered_shows_service.invalidate_cache()


def test_v2_and_v1_reads_share_the_backend_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_list():
        nonlocal calls
        calls += 1
        return [_show_payload()], 1

    monkeypatch.setattr(covered_shows_service.covered_shows_repo, "list_covered_shows", fake_list)
    client = TestClient(app)

    v2_response = client.get("/api/v2/admin/covered-shows")
    v1_response = client.get("/api/v1/admin/covered-shows")

    assert v2_response.status_code == 200
    assert v2_response.json() == {"shows": [_show_payload()]}
    assert v1_response.status_code == 200
    assert calls == 1


def test_v2_create_uses_signed_admin_actor_and_ignores_raw_actor_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def fake_add(**kwargs):
        captured.update(kwargs)
        return _show_payload(kwargs["show_id"]), 2

    monkeypatch.setattr(covered_shows_service.covered_shows_repo, "add_covered_show", fake_add)
    response = TestClient(app).post(
        "/api/v2/admin/covered-shows",
        headers={"X-TRR-Admin-User-Uid": "spoofed-raw-header"},
        json={"trr_show_id": SHOW_ID, "show_name": "Bravo Show"},
    )

    assert response.status_code == 201
    assert response.json() == {"show": _show_payload()}
    assert captured == {
        "show_id": SHOW_ID,
        "show_name": "Bravo Show",
        "actor_uid": "signed-admin-uid",
    }


@pytest.mark.parametrize(
    ("method", "path", "body", "expected_code"),
    [
        ("GET", "/api/v2/admin/covered-shows/not-a-uuid", None, "INVALID_COVERED_SHOW_ID"),
        (
            "POST",
            "/api/v2/admin/covered-shows",
            {"trr_show_id": SHOW_ID, "show_name": "Bravo Show", "unexpected": True},
            "INVALID_COVERED_SHOW_REQUEST",
        ),
    ],
)
def test_malformed_v2_inputs_use_stable_problem_400(
    method: str,
    path: str,
    body: dict[str, object] | None,
    expected_code: str,
) -> None:
    response = TestClient(app).request(
        method,
        path,
        headers={"x-request-id": "covered-invalid"},
        json=body,
    )

    assert response.status_code == 400
    assert response.json()["detail"] == {
        "code": expected_code,
        "status": 400,
        "message": (
            "show_id must be a valid UUID."
            if expected_code == "INVALID_COVERED_SHOW_ID"
            else "trr_show_id and show_name are required, and no extra fields are allowed."
        ),
        "trace_id": "covered-invalid",
        "request_id": "covered-invalid",
        "retryable": False,
    }


def test_v2_missing_and_database_capacity_errors_are_safe_problems(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        covered_shows_service.covered_shows_repo,
        "get_covered_show",
        lambda show_id: (None, 1),
    )
    missing = TestClient(app).get(f"/api/v2/admin/covered-shows/{MISSING_SHOW_ID}")
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "COVERED_SHOW_NOT_FOUND"

    def unavailable():
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(covered_shows_service.covered_shows_repo, "list_covered_shows", unavailable)
    unavailable_response = TestClient(app).get("/api/v2/admin/covered-shows")
    assert unavailable_response.status_code == 503
    assert unavailable_response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in unavailable_response.text


def test_v2_delete_returns_strict_success_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        covered_shows_service.covered_shows_repo,
        "remove_covered_show",
        lambda show_id: (show_id == SHOW_ID, 1),
    )

    deleted = TestClient(app).delete(f"/api/v2/admin/covered-shows/{SHOW_ID}")
    missing = TestClient(app).delete(f"/api/v2/admin/covered-shows/{MISSING_SHOW_ID}")

    assert deleted.status_code == 200
    assert deleted.json() == {"success": True}
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "COVERED_SHOW_NOT_FOUND"


def test_v2_covered_shows_openapi_is_explicit_and_has_no_extra_routes() -> None:
    schema = app.openapi()
    expected = {
        ("/api/v2/admin/covered-shows", "get"): "listAdminCoveredShowsV2",
        ("/api/v2/admin/covered-shows", "post"): "createAdminCoveredShowV2",
        ("/api/v2/admin/covered-shows/{show_id}", "get"): "getAdminCoveredShowV2",
        ("/api/v2/admin/covered-shows/{show_id}", "delete"): "deleteAdminCoveredShowV2",
    }
    for (path, method), operation_id in expected.items():
        operation = schema["paths"][path][method]
        assert operation["operationId"] == operation_id
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])

    assert "patch" not in schema["paths"]["/api/v2/admin/covered-shows"]
    assert "patch" not in schema["paths"]["/api/v2/admin/covered-shows/{show_id}"]
    assert "/api/v2/admin/covered-shows/cache/invalidate" not in schema["paths"]

    covered_show_schema = schema["components"]["schemas"]["CoveredShowV2"]
    assert covered_show_schema["additionalProperties"] is False
    assert set(covered_show_schema["properties"]) == {
        "id",
        "trr_show_id",
        "show_name",
        "canonical_slug",
        "alternative_names",
        "show_total_episodes",
        "poster_url",
    }
    assert set(covered_show_schema["required"]) == set(covered_show_schema["properties"])
