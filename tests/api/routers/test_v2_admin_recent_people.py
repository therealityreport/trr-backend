from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.repositories import recent_people as recent_people_repo

PERSON_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def _person_payload() -> dict[str, object]:
    return {
        "person_id": PERSON_ID,
        "full_name": "Bravo Star",
        "known_for": "Reality TV",
        "photo_url": "https://cdn.example.com/person.jpg",
        "show_context": "show-1",
        "view_count": 4,
        "first_viewed_at": "2026-03-25T00:00:00Z",
        "last_viewed_at": "2026-03-26T00:00:00Z",
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


def test_v2_recent_people_list_and_post_use_signed_admin_actor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    listed_uid: list[str] = []

    def fake_list(firebase_uid, limit=None):
        listed_uid.append(firebase_uid)
        return [_person_payload()], 1

    monkeypatch.setattr(recent_people_repo, "list_recent_people", fake_list)

    def fake_record(**kwargs):
        captured.update(kwargs)
        return {"ok": True}, 2

    monkeypatch.setattr(recent_people_repo, "record_recent_person_view", fake_record)

    client = TestClient(app)
    listed = client.get(
        "/api/v2/admin/recent-people?limit=5",
        headers={"X-TRR-Admin-User-Uid": "spoofed-raw-header"},
    )
    recorded = client.post(
        "/api/v2/admin/recent-people",
        headers={"X-TRR-Admin-User-Uid": "spoofed-raw-header"},
        json={
            "personId": PERSON_ID,
            "showId": "show-1",
        },
    )

    assert listed.status_code == 200
    assert listed.json() == {
        "people": [_person_payload()],
        "pagination": {"limit": 5, "count": 1},
    }
    assert listed_uid == ["signed-admin-uid"]
    assert recorded.status_code == 200
    assert recorded.json() == {"ok": True}
    assert captured == {
        "firebase_uid": "signed-admin-uid",
        "person_id": PERSON_ID,
        "show_context": "show-1",
        "cap": 20,
    }


@pytest.mark.parametrize(
    ("path", "body", "expected_code"),
    [
        ("/api/v2/admin/recent-people?limit=0", None, "INVALID_RECENT_PEOPLE_LIMIT"),
        (
            "/api/v2/admin/recent-people",
            {"personId": PERSON_ID, "showId": "show-1", "unexpected": True},
            "INVALID_RECENT_PERSON_REQUEST",
        ),
    ],
)
def test_v2_recent_people_invalid_requests_use_stable_problem_400(
    path: str,
    body: dict[str, object] | None,
    expected_code: str,
) -> None:
    response = TestClient(app).request(
        "GET" if body is None else "POST",
        path,
        headers={"x-request-id": "recent-people-invalid"},
        json=body,
    )

    assert response.status_code == 400
    assert response.json()["detail"] == {
        "code": expected_code,
        "status": 400,
        "message": (
            "limit must be an integer between 1 and 50."
            if expected_code == "INVALID_RECENT_PEOPLE_LIMIT"
            else "personId must be a valid UUID and showId must be a string or null."
        ),
        "trace_id": "recent-people-invalid",
        "request_id": "recent-people-invalid",
        "retryable": False,
    }


def test_v2_recent_people_database_capacity_uses_safe_problem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args, **_kwargs):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(recent_people_repo, "list_recent_people", unavailable)
    response = TestClient(app).get("/api/v2/admin/recent-people")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in response.text


def test_v2_recent_people_openapi_is_explicit_and_has_no_extra_routes() -> None:
    schema = app.openapi()
    expected = {
        ("/api/v2/admin/recent-people", "get"): "listAdminRecentPeopleV2",
        ("/api/v2/admin/recent-people", "post"): "recordAdminRecentPersonV2",
    }
    for (path, method), operation_id in expected.items():
        operation = schema["paths"][path][method]
        assert operation["operationId"] == operation_id
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])

    assert "delete" not in schema["paths"]["/api/v2/admin/recent-people"]
    recent_person_schema = schema["components"]["schemas"]["RecentPersonV2"]
    assert recent_person_schema["additionalProperties"] is False
    assert set(recent_person_schema["properties"]) == {
        "person_id",
        "full_name",
        "known_for",
        "photo_url",
        "show_context",
        "view_count",
        "first_viewed_at",
        "last_viewed_at",
    }
