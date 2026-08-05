from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2.admin_flashback import router
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.repositories import admin_flashback as flashback_repo

QUIZ_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
EVENT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _quiz(*, published: bool = False) -> dict[str, object]:
    return {
        "id": QUIZ_ID,
        "title": "Bravo Beginnings",
        "publish_date": "2026-03-30",
        "description": None,
        "is_published": published,
        "created_at": datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 3, 30, 13, 0, tzinfo=UTC),
    }


def _event() -> dict[str, object]:
    return {
        "id": EVENT_ID,
        "quiz_id": QUIZ_ID,
        "description": "The table flip",
        "image_url": None,
        "year": 2009,
        "sort_order": 1,
        "point_value": 5,
    }


def _build_app(*, authenticated: bool = True) -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/api/v2")
    if authenticated:
        app.dependency_overrides[require_internal_admin] = lambda: {
            "id": "trr-app-internal-admin",
            "admin_uid": "signed-admin-uid",
            "role": "internal_admin",
        }
    return app


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(_build_app()) as test_client:
        yield test_client


def test_routes_require_internal_admin() -> None:
    response = TestClient(_build_app(authenticated=False)).get("/api/v2/admin/flashback/quizzes")

    assert response.status_code == 401


def test_list_and_create_quizzes_preserve_contract(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(flashback_repo, "list_quizzes", lambda: ([_quiz()], 1))

    def create_quiz(**kwargs):
        captured.update(kwargs)
        return _quiz(), 1

    monkeypatch.setattr(flashback_repo, "create_quiz", create_quiz)

    listed = client.get("/api/v2/admin/flashback/quizzes")
    created = client.post(
        "/api/v2/admin/flashback/quizzes",
        json={
            "title": " Bravo Beginnings ",
            "publish_date": "2026-03-30",
            "description": None,
        },
    )

    assert listed.status_code == 200
    assert listed.json()["quizzes"][0]["id"] == QUIZ_ID
    assert created.status_code == 201
    assert created.json()["quiz"]["publish_date"] == "2026-03-30"
    assert captured == {
        "title": "Bravo Beginnings",
        "publish_date": "2026-03-30",
        "description": None,
    }


def test_update_quiz_and_not_found(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def update_quiz(**kwargs):
        captured.update(kwargs)
        return _quiz(published=True), 1

    monkeypatch.setattr(flashback_repo, "set_quiz_published", update_quiz)

    response = client.patch(
        f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}",
        json={"is_published": True},
    )

    assert response.status_code == 200
    assert response.json()["quiz"]["is_published"] is True
    assert captured == {"quiz_id": QUIZ_ID, "is_published": True}

    monkeypatch.setattr(flashback_repo, "set_quiz_published", lambda **_kwargs: (None, 1))
    missing = client.patch(
        f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}",
        json={"is_published": False},
    )
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "FLASHBACK_QUIZ_NOT_FOUND"


def test_list_create_and_delete_events(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(flashback_repo, "list_events", lambda **_kwargs: ([_event()], 1))

    def create_event(**kwargs):
        captured["create"] = kwargs
        return _event(), 3

    def delete_event(**kwargs):
        captured["delete"] = kwargs
        return True, 4

    monkeypatch.setattr(flashback_repo, "create_event", create_event)
    monkeypatch.setattr(flashback_repo, "delete_event", delete_event)

    listed = client.get(f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}/events")
    created = client.post(
        f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}/events",
        json={
            "description": "The table flip",
            "year": 2009,
            "image_url": None,
            "point_value": 5,
        },
    )
    deleted = client.delete(f"/api/v2/admin/flashback/events/{EVENT_ID}")

    assert listed.status_code == 200
    assert listed.json() == {"events": [_event()]}
    assert created.status_code == 201
    assert created.json()["event"]["sort_order"] == 1
    assert deleted.status_code == 204
    assert deleted.content == b""
    assert captured == {
        "create": {
            "quiz_id": QUIZ_ID,
            "description": "The table flip",
            "year": 2009,
            "image_url": None,
            "point_value": 5,
        },
        "delete": {"event_id": EVENT_ID},
    }


def test_event_parent_and_event_not_found_are_distinct_404s(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(flashback_repo, "create_event", lambda **_kwargs: (None, 1))
    monkeypatch.setattr(flashback_repo, "delete_event", lambda **_kwargs: (False, 1))

    create_response = client.post(
        f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}/events",
        json={"description": "A moment", "year": 2010, "point_value": 2},
    )
    delete_response = client.delete(f"/api/v2/admin/flashback/events/{EVENT_ID}")

    assert create_response.status_code == 404
    assert create_response.json()["detail"]["code"] == "FLASHBACK_QUIZ_NOT_FOUND"
    assert delete_response.status_code == 404
    assert delete_response.json()["detail"]["code"] == "FLASHBACK_EVENT_NOT_FOUND"


@pytest.mark.parametrize(
    ("method", "path", "body", "code"),
    [
        ("PATCH", "/api/v2/admin/flashback/quizzes/not-a-uuid", {"is_published": True}, "INVALID_QUIZ_ID"),
        (
            "POST",
            "/api/v2/admin/flashback/quizzes",
            {"title": "", "publish_date": "not-a-date"},
            "INVALID_FLASHBACK_QUIZ_REQUEST",
        ),
        (
            "PATCH",
            f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}",
            {"is_published": "yes"},
            "INVALID_FLASHBACK_QUIZ_UPDATE_REQUEST",
        ),
        (
            "POST",
            f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}/events",
            {"description": "A moment", "year": "2009", "point_value": 5},
            "INVALID_FLASHBACK_EVENT_REQUEST",
        ),
        (
            "POST",
            f"/api/v2/admin/flashback/quizzes/{QUIZ_ID}/events",
            {"description": "A moment", "year": 2009, "point_value": 1},
            "INVALID_FLASHBACK_EVENT_REQUEST",
        ),
    ],
)
def test_invalid_inputs_use_stable_problem_400(
    client: TestClient,
    method: str,
    path: str,
    body: dict[str, object],
    code: str,
) -> None:
    response = client.request(method, path, json=body)

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == code


def test_database_capacity_error_is_safe(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable():
        raise DatabaseServiceUnavailableError("secret topology", reason="pool_capacity")

    monkeypatch.setattr(flashback_repo, "list_quizzes", unavailable)

    response = client.get("/api/v2/admin/flashback/quizzes")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret topology" not in response.text


def test_openapi_is_explicit_and_internal_admin_secured() -> None:
    document = _build_app().openapi()
    expected = {
        ("/api/v2/admin/flashback/quizzes", "get"): "listAdminFlashbackQuizzesV2",
        ("/api/v2/admin/flashback/quizzes", "post"): "createAdminFlashbackQuizV2",
        ("/api/v2/admin/flashback/quizzes/{quiz_id}", "patch"): "updateAdminFlashbackQuizV2",
        ("/api/v2/admin/flashback/quizzes/{quiz_id}/events", "get"): "listAdminFlashbackEventsV2",
        ("/api/v2/admin/flashback/quizzes/{quiz_id}/events", "post"): "createAdminFlashbackEventV2",
        ("/api/v2/admin/flashback/events/{event_id}", "delete"): "deleteAdminFlashbackEventV2",
    }

    for (path, method), operation_id in expected.items():
        operation = document["paths"][path][method]
        assert operation["operationId"] == operation_id
        assert operation["security"] == [{"InternalAdminBearer": []}]
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])
