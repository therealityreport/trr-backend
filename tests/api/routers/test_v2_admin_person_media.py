from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.services import person_media_admin as person_media_service

PERSON_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PHOTO_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
MISSING_PHOTO_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"


def _cover_photo() -> dict[str, object]:
    return {
        "person_id": PERSON_ID,
        "photo_id": PHOTO_ID,
        "photo_url": "https://cdn.example.com/person.jpg",
        "created_at": datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        "created_by_firebase_uid": "signed-admin-uid",
    }


def _crop_result(origin: str = "cast_photos") -> dict[str, object]:
    return {
        "origin": origin,
        "photo_id": PHOTO_ID,
        "person_id": PERSON_ID,
        "link_id": PHOTO_ID if origin == "media_links" else None,
        "thumbnail_focus_x": 44.0,
        "thumbnail_focus_y": 26.0,
        "thumbnail_zoom": 1.2,
        "thumbnail_crop_mode": "manual",
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


def test_cover_photo_get_put_delete_preserve_strict_contracts_and_signed_actor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(person_media_service.person_media_repo, "get_cover_photo", lambda _id: (_cover_photo(), 1))

    def fake_set(**kwargs):
        captured.update(kwargs)
        return _cover_photo(), 1

    monkeypatch.setattr(person_media_service.person_media_repo, "set_cover_photo", fake_set)
    monkeypatch.setattr(person_media_service.person_media_repo, "remove_cover_photo", lambda _id: (False, 1))
    client = TestClient(app)

    read = client.get(f"/api/v2/admin/people/{PERSON_ID}/cover-photos")
    written = client.put(
        f"/api/v2/admin/people/{PERSON_ID}/cover-photos",
        headers={"X-TRR-Admin-User-Uid": "spoofed-raw-header"},
        json={"photo_id": PHOTO_ID, "photo_url": "https://cdn.example.com/person.jpg"},
    )
    removed = client.delete(f"/api/v2/admin/people/{PERSON_ID}/cover-photos")

    assert read.status_code == 200
    assert read.json()["coverPhoto"]["person_id"] == PERSON_ID
    assert read.json()["coverPhoto"]["created_at"] == "2026-07-15T12:00:00Z"
    assert written.status_code == 200
    assert written.json()["coverPhoto"]["created_by_firebase_uid"] == "signed-admin-uid"
    assert captured == {
        "person_id": PERSON_ID,
        "photo_id": PHOTO_ID,
        "photo_url": "https://cdn.example.com/person.jpg",
        "actor_uid": "signed-admin-uid",
    }
    assert removed.status_code == 200
    assert removed.json() == {"success": True, "removed": False}


def test_cover_photo_get_preserves_null_for_an_unset_cover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(person_media_service.person_media_repo, "get_cover_photo", lambda _id: (None, 1))

    response = TestClient(app).get(f"/api/v2/admin/people/{PERSON_ID}/cover-photos")

    assert response.status_code == 200
    assert response.json() == {"coverPhoto": None}


@pytest.mark.parametrize("origin", ["cast_photos", "media_links"])
def test_thumbnail_crop_put_returns_the_existing_flat_contract(
    monkeypatch: pytest.MonkeyPatch,
    origin: str,
) -> None:
    captured: dict[str, object] = {}

    def fake_update(**kwargs):
        captured.update(kwargs)
        return _crop_result(origin), 1

    monkeypatch.setattr(person_media_service.person_media_repo, "update_thumbnail_crop", fake_update)
    payload: dict[str, object] = {
        "origin": origin,
        "photo_id": PHOTO_ID,
        "link_id": PHOTO_ID if origin == "media_links" else None,
        "crop": {"x": 44, "y": 26, "zoom": 1.2, "mode": "manual"},
    }

    response = TestClient(app).put(
        f"/api/v2/admin/people/{PERSON_ID}/thumbnail-crops",
        json=payload,
    )

    assert response.status_code == 200
    assert response.json() == _crop_result(origin)
    assert captured == {
        "origin": origin,
        "person_id": PERSON_ID,
        "photo_id": PHOTO_ID,
        "crop": payload["crop"],
    }


def test_thumbnail_crop_missing_photo_is_a_typed_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        person_media_service.person_media_repo,
        "update_thumbnail_crop",
        lambda **_kwargs: (None, 1),
    )

    response = TestClient(app).put(
        f"/api/v2/admin/people/{PERSON_ID}/thumbnail-crops",
        json={
            "origin": "cast_photos",
            "photo_id": MISSING_PHOTO_ID,
            "link_id": None,
            "crop": None,
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "PERSON_THUMBNAIL_CROP_NOT_FOUND"


@pytest.mark.parametrize(
    ("path", "body", "expected_code"),
    [
        (
            "/api/v2/admin/people/not-a-uuid/cover-photos",
            None,
            "INVALID_PERSON_ID",
        ),
        (
            f"/api/v2/admin/people/{PERSON_ID}/cover-photos",
            {"photo_id": PHOTO_ID, "photo_url": "javascript:alert(1)"},
            "INVALID_PERSON_COVER_PHOTO_REQUEST",
        ),
        (
            f"/api/v2/admin/people/{PERSON_ID}/thumbnail-crops",
            {
                "origin": "cast_photos",
                "photo_id": PHOTO_ID,
                "link_id": None,
                "crop": {"x": 101, "y": 26, "zoom": 1.2, "mode": "manual"},
            },
            "INVALID_PERSON_THUMBNAIL_CROP_REQUEST",
        ),
    ],
)
def test_malformed_inputs_use_stable_problem_400(
    path: str,
    body: dict[str, object] | None,
    expected_code: str,
) -> None:
    response = TestClient(app).request("GET" if body is None else "PUT", path, json=body)

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == expected_code


def test_database_capacity_error_is_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(_person_id: str):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(person_media_service.person_media_repo, "get_cover_photo", unavailable)

    response = TestClient(app).get(f"/api/v2/admin/people/{PERSON_ID}/cover-photos")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in response.text


def test_v2_person_media_openapi_is_explicit_and_bounded() -> None:
    schema = app.openapi()
    expected = {
        ("/api/v2/admin/people/{person_id}/cover-photos", "get"): "getAdminPersonCoverPhotoV2",
        ("/api/v2/admin/people/{person_id}/cover-photos", "put"): "putAdminPersonCoverPhotoV2",
        ("/api/v2/admin/people/{person_id}/cover-photos", "delete"): "deleteAdminPersonCoverPhotoV2",
        ("/api/v2/admin/people/{person_id}/thumbnail-crops", "put"): "putAdminPersonThumbnailCropV2",
    }
    for (path, method), operation_id in expected.items():
        operation = schema["paths"][path][method]
        assert operation["operationId"] == operation_id
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])

    assert "post" not in schema["paths"]["/api/v2/admin/people/{person_id}/cover-photos"]
    assert "get" not in schema["paths"]["/api/v2/admin/people/{person_id}/thumbnail-crops"]
    assert schema["components"]["schemas"]["PersonCoverPhotoV2"]["additionalProperties"] is False
    assert schema["components"]["schemas"]["PersonThumbnailCropWriteResultV2"]["additionalProperties"] is False
