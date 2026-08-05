from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2.admin_typography import router
from trr_backend.repositories import admin_typography

SET_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ASSIGNMENT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _roles() -> dict[str, object]:
    return {
        "body": {
            "mobile": {
                "fontFamily": "var(--font-hamburg)",
                "fontSize": "16px",
                "fontWeight": "400",
                "lineHeight": "24px",
                "letterSpacing": "0px",
            },
            "desktop": {
                "fontFamily": "var(--font-hamburg)",
                "fontSize": "18px",
                "fontWeight": "400",
                "lineHeight": "28px",
                "letterSpacing": "0px",
            },
        }
    }


def _set() -> dict[str, object]:
    now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    return {
        "id": SET_ID,
        "slug": "admin-home",
        "name": "Admin Home",
        "area": "admin",
        "seed_source": "src/app/admin/page.tsx",
        "roles": _roles(),
        "created_at": now,
        "updated_at": now,
    }


def _assignment() -> dict[str, object]:
    now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    return {
        "id": ASSIGNMENT_ID,
        "area": "admin",
        "page_key": "home",
        "instance_key": None,
        "set_id": SET_ID,
        "source_path": "src/app/admin/page.tsx",
        "notes": None,
        "created_at": now,
        "updated_at": now,
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
    response = TestClient(_build_app(authenticated=False)).get("/api/v2/admin/site-typography")

    assert response.status_code == 401


def test_get_returns_seeded_or_persisted_state(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        admin_typography,
        "read_typography_state",
        lambda: ({"sets": [_set()], "assignments": [_assignment()]}, 2),
    )

    response = client.get("/api/v2/admin/site-typography")

    assert response.status_code == 200
    assert response.json()["sets"][0]["id"] == SET_ID
    assert response.json()["assignments"][0]["set_id"] == SET_ID


def test_create_forwards_strict_payload_and_returns_created_set(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def create(**kwargs):
        captured.update(kwargs)
        return _set(), 2

    monkeypatch.setattr(admin_typography, "create_typography_set", create)

    response = client.post(
        "/api/v2/admin/site-typography/sets",
        json={
            "name": " Admin Home ",
            "area": "admin",
            "seed_source": " src/app/admin/page.tsx ",
            "roles": _roles(),
        },
    )

    assert response.status_code == 201
    assert response.json()["set"]["seed_source"] == "src/app/admin/page.tsx"
    assert captured == {
        "slug": None,
        "name": "Admin Home",
        "area": "admin",
        "seed_source": "src/app/admin/page.tsx",
        "roles": _roles(),
    }


def test_missing_update_and_assigned_delete_keep_404_and_409(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin_typography, "update_typography_set", lambda *_args, **_kwargs: (None, 1))
    monkeypatch.setattr(admin_typography, "delete_typography_set", lambda *_args, **_kwargs: ("in-use", 1))

    update = client.put(f"/api/v2/admin/site-typography/sets/{SET_ID}", json={"name": "Updated"})
    delete = client.delete(f"/api/v2/admin/site-typography/sets/{SET_ID}")

    assert update.status_code == 404
    assert update.json()["detail"]["code"] == "TYPOGRAPHY_SET_NOT_FOUND"
    assert delete.status_code == 409
    assert delete.json()["detail"]["code"] == "TYPOGRAPHY_SET_IN_USE"


@pytest.mark.parametrize(
    ("method", "path", "body", "code"),
    [
        ("POST", "/api/v2/admin/site-typography/sets", {"name": "", "area": "admin"}, "INVALID_TYPOGRAPHY_SET_REQUEST"),
        (
            "PUT",
            f"/api/v2/admin/site-typography/sets/{SET_ID}",
            {"roles": None},
            "INVALID_TYPOGRAPHY_SET_UPDATE_REQUEST",
        ),
        (
            "PUT",
            "/api/v2/admin/site-typography/assignments",
            {"area": "admin", "set_id": SET_ID, "source_path": "x", "unexpected": True},
            "INVALID_TYPOGRAPHY_ASSIGNMENT_REQUEST",
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


def test_openapi_is_explicit_and_internal_admin_secured() -> None:
    document = _build_app().openapi()
    expected = {
        ("/api/v2/admin/site-typography", "get"): "getAdminSiteTypographyV2",
        ("/api/v2/admin/site-typography/sets", "post"): "createAdminTypographySetV2",
        ("/api/v2/admin/site-typography/sets/{set_id}", "put"): "updateAdminTypographySetV2",
        ("/api/v2/admin/site-typography/sets/{set_id}", "delete"): "deleteAdminTypographySetV2",
        ("/api/v2/admin/site-typography/assignments", "put"): "upsertAdminTypographyAssignmentV2",
    }
    for (path, method), operation_id in expected.items():
        operation = document["paths"][path][method]
        assert operation["operationId"] == operation_id
        assert operation["security"] == [{"InternalAdminBearer": []}]
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])
