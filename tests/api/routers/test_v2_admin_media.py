from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2 import admin_media as admin_media_router
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.services import admin_media as admin_media_service

SHOW_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
IMAGE_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ENTITY_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
ASSET_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
LINK_ID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
ADMIN_UID = "signed-admin-uid"


def _app(*, authenticated: bool = True) -> FastAPI:
    app = FastAPI()
    app.include_router(admin_media_router.router, prefix="/api/v2")
    if authenticated:
        app.dependency_overrides[require_internal_admin] = lambda: {
            "id": "internal-admin",
            "admin_uid": ADMIN_UID,
            "role": "internal_admin",
        }
    return app


def _link() -> dict[str, object]:
    return {
        "id": LINK_ID,
        "entity_type": "season",
        "entity_id": ENTITY_ID,
        "media_asset_id": ASSET_ID,
        "kind": "gallery",
        "position": None,
        "context": {"people_count": 0},
        "created_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    }


def test_admin_media_routes_require_internal_admin() -> None:
    response = TestClient(_app(authenticated=False)).get(f"/api/v2/admin/shows/{SHOW_ID}/seasons/6/assets")

    assert response.status_code == 401


def test_admin_media_routes_preserve_success_shapes_and_signed_actor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, dict[str, object]]] = []
    asset = {
        "id": ASSET_ID,
        "type": "season",
        "origin_table": "media_assets",
        "source": "tmdb",
        "kind": "poster",
        "hosted_url": "https://cdn.example.com/poster.jpg",
        "width": 1000,
        "height": 1500,
        "caption": "Season 6",
        "people_count": 0,
    }
    monkeypatch.setattr(
        admin_media_service,
        "get_show_season_assets",
        lambda **_kwargs: (
            {
                "assets": [asset],
                "pagination": {
                    "limit": 200,
                    "offset": 0,
                    "count": 1,
                    "has_more": False,
                    "next_cursor": None,
                    "cursor": None,
                    "full": False,
                    "truncated": False,
                },
            },
            1,
        ),
    )
    monkeypatch.setattr(admin_media_service, "validate_show_featured_image", lambda **_kwargs: (True, 1))
    monkeypatch.setattr(admin_media_service, "get_image", lambda *_args: ({"id": IMAGE_ID}, 1))

    def capture(name: str):
        def inner(**kwargs):
            captured.append((name, kwargs))
            return 1

        return inner

    monkeypatch.setattr(admin_media_service, "delete_image", capture("delete"))
    monkeypatch.setattr(admin_media_service, "set_image_archive_state", capture("archive"))
    monkeypatch.setattr(admin_media_service, "reassign_image", capture("reassign"))
    monkeypatch.setattr(admin_media_service, "get_media_links", lambda _asset_id: ([_link()], 1))
    monkeypatch.setattr(
        admin_media_service,
        "create_media_link",
        lambda **_kwargs: (
            {"link": _link(), "already_exists": False, "message": "Link created successfully"},
            2,
        ),
    )
    monkeypatch.setattr(
        admin_media_service,
        "update_media_link_context",
        lambda *_args: (
            {
                "link_id": LINK_ID,
                "people_count": 0,
                "people_count_source": "manual",
                "thumbnail_crop": None,
            },
            2,
        ),
    )
    client = TestClient(_app())

    assert client.get(f"/api/v2/admin/shows/{SHOW_ID}/seasons/6/assets").json()["assets"][0]["people_count"] == 0
    assert client.post(
        f"/api/v2/admin/shows/{SHOW_ID}/featured-image-validation",
        json={"image_id": IMAGE_ID, "expected_kind": "poster"},
    ).json() == {"valid": True}
    assert client.get(f"/api/v2/admin/images/cast/{IMAGE_ID}").json() == {"image": {"id": IMAGE_ID}}
    assert client.delete(f"/api/v2/admin/images/cast/{IMAGE_ID}").json() == {"success": True}
    assert client.put(
        f"/api/v2/admin/images/cast/{IMAGE_ID}/archive",
        json={"archive": True, "reason": "duplicate"},
    ).json() == {"success": True}
    assert client.put(
        f"/api/v2/admin/images/cast/{IMAGE_ID}/reassign",
        json={"to_entity_id": ENTITY_ID, "to_type": "season", "mode": "copy"},
    ).json() == {"success": True}
    assert client.get(f"/api/v2/admin/media-links?media_asset_id={ASSET_ID}").json()["links"][0]["id"] == LINK_ID
    assert (
        client.post(
            "/api/v2/admin/media-links",
            json={
                "media_asset_id": ASSET_ID,
                "entity_type": "season",
                "entity_id": ENTITY_ID,
                "context": {"people_count": 0},
            },
        ).json()["message"]
        == "Link created successfully"
    )
    assert (
        client.patch(
            f"/api/v2/admin/media-links/{LINK_ID}/context",
            json={"people_count": 0, "people_count_source": "manual"},
        ).json()["people_count"]
        == 0
    )
    assert [item[0] for item in captured] == ["delete", "archive", "reassign"]
    assert all(item[1]["actor_uid"] == ADMIN_UID for item in captured)


@pytest.mark.parametrize(
    ("method", "path", "body", "code"),
    [
        ("GET", "/api/v2/admin/shows/not-a-uuid/seasons/6/assets", None, "INVALID_SHOW_ID"),
        (
            "GET",
            f"/api/v2/admin/shows/{SHOW_ID}/seasons/6/assets?limit=501",
            None,
            "INVALID_PAGINATION",
        ),
        (
            "POST",
            f"/api/v2/admin/shows/{SHOW_ID}/featured-image-validation",
            {"image_id": "bad", "expected_kind": "poster"},
            "INVALID_FEATURED_IMAGE_VALIDATION_REQUEST",
        ),
        ("GET", f"/api/v2/admin/images/unknown/{IMAGE_ID}", None, "INVALID_IMAGE_TYPE"),
        (
            "PUT",
            f"/api/v2/admin/images/cast/{IMAGE_ID}/archive",
            {"archive": "yes"},
            "INVALID_IMAGE_ARCHIVE_REQUEST",
        ),
        (
            "PATCH",
            f"/api/v2/admin/media-links/{LINK_ID}/context",
            {"people_count": 0, "extra": True},
            "INVALID_MEDIA_LINK_CONTEXT_REQUEST",
        ),
    ],
)
def test_invalid_inputs_are_typed_400_not_implicit_422(
    method: str,
    path: str,
    body: dict[str, object] | None,
    code: str,
) -> None:
    response = TestClient(_app()).request(method, path, json=body)

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == code


def test_not_found_and_database_unavailable_are_typed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = TestClient(_app())
    monkeypatch.setattr(admin_media_service, "get_image", lambda *_args: (None, 1))
    missing = client.get(f"/api/v2/admin/images/cast/{IMAGE_ID}")
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "IMAGE_NOT_FOUND"

    def unavailable(_asset_id: str):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(admin_media_service, "get_media_links", unavailable)
    failed = client.get(f"/api/v2/admin/media-links?media_asset_id={ASSET_ID}")
    assert failed.status_code == 503
    assert failed.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in failed.text


def test_admin_media_openapi_is_strict_explicit_and_bounded() -> None:
    schema = _app().openapi()
    expected = {
        ("/api/v2/admin/shows/{show_id}/seasons/{season_number}/assets", "get"): "getAdminShowSeasonAssetsV2",
        ("/api/v2/admin/shows/{show_id}/featured-image-validation", "post"): "validateAdminShowFeaturedImageV2",
        ("/api/v2/admin/images/{image_type}/{image_id}", "get"): "getAdminImageV2",
        ("/api/v2/admin/images/{image_type}/{image_id}", "delete"): "deleteAdminImageV2",
        ("/api/v2/admin/images/{image_type}/{image_id}/archive", "put"): "putAdminImageArchiveV2",
        ("/api/v2/admin/images/{image_type}/{image_id}/reassign", "put"): "putAdminImageReassignV2",
        ("/api/v2/admin/media-links", "get"): "listAdminMediaLinksV2",
        ("/api/v2/admin/media-links", "post"): "createAdminMediaLinkV2",
        ("/api/v2/admin/media-links/{link_id}/context", "patch"): "patchAdminMediaLinkContextV2",
    }
    for (path, method), operation_id in expected.items():
        operation = schema["paths"][path][method]
        assert operation["operationId"] == operation_id
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])
        assert operation["security"] == [{"InternalAdminBearer": []}]

    assets_parameters = schema["paths"]["/api/v2/admin/shows/{show_id}/seasons/{season_number}/assets"]["get"][
        "parameters"
    ]
    limit = next(parameter for parameter in assets_parameters if parameter["name"] == "limit")
    assert limit["schema"]["maximum"] == 500
    archive_schema = schema["paths"]["/api/v2/admin/images/{image_type}/{image_id}/archive"]["put"]["requestBody"][
        "content"
    ]["application/json"]["schema"]
    assert archive_schema["additionalProperties"] is False
    assert schema["components"]["schemas"]["AdminSeasonAssetV2"]["additionalProperties"] is False
    assert schema["components"]["schemas"]["AdminMediaLinkV2"]["additionalProperties"] is False
