from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_social_posts as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
        "email": None,
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_list_social_posts_for_show_validates_season_ownership(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(router_module.social_posts_repo, "get_season_show_id", lambda season_id: ("other-show", 1))

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/shows/11111111-1111-1111-1111-111111111111/social-posts",
        params={"trr_season_id": "22222222-2222-2222-2222-222222222222"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "trr_season_id must belong to the showId route"}


def test_create_and_item_routes_preserve_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.social_posts_repo,
        "get_season_show_id",
        lambda season_id: ("11111111-1111-1111-1111-111111111111", 1),
    )
    monkeypatch.setattr(
        router_module.social_posts_repo,
        "create_post",
        lambda **kwargs: (
            {
                "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "trr_show_id": kwargs["trr_show_id"],
                "trr_season_id": kwargs["trr_season_id"],
                "platform": kwargs["platform"],
                "url": kwargs["url"],
                "title": kwargs["title"],
                "notes": kwargs["notes"],
                "created_by_firebase_uid": kwargs["actor_uid"],
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T00:00:00Z",
            },
            1,
        ),
    )
    monkeypatch.setattr(
        router_module.social_posts_repo,
        "get_post",
        lambda post_id: (
            {
                "id": post_id,
                "trr_show_id": "11111111-1111-1111-1111-111111111111",
                "trr_season_id": None,
                "platform": "instagram",
                "url": "https://instagram.com/p/abc",
                "title": None,
                "notes": None,
                "created_by_firebase_uid": "firebase:admin-1",
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T00:00:00Z",
            },
            1,
        ),
    )
    monkeypatch.setattr(
        router_module.social_posts_repo,
        "update_post",
        lambda **kwargs: (
            {
                "id": kwargs["post_id"],
                "trr_show_id": "11111111-1111-1111-1111-111111111111",
                "trr_season_id": None,
                "platform": "instagram",
                "url": "https://instagram.com/p/updated",
                "title": "Updated",
                "notes": None,
                "created_by_firebase_uid": "firebase:admin-1",
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T01:00:00Z",
            },
            1,
        ),
    )
    monkeypatch.setattr(router_module.social_posts_repo, "delete_post", lambda post_id: (True, 1))

    client = TestClient(app)

    created = client.post(
        "/api/v1/admin/shows/11111111-1111-1111-1111-111111111111/social-posts",
        headers={"X-TRR-Admin-User-Uid": "firebase:admin-1"},
        json={
            "platform": "instagram",
            "url": "https://instagram.com/p/abc",
            "trr_season_id": "22222222-2222-2222-2222-222222222222",
            "title": None,
            "notes": None,
        },
    )
    fetched = client.get("/api/v1/admin/social-posts/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    updated = client.put(
        "/api/v1/admin/social-posts/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        json={"url": "https://instagram.com/p/updated", "title": "Updated"},
    )
    deleted = client.delete("/api/v1/admin/social-posts/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

    assert created.status_code == 201
    assert created.json()["post"]["created_by_firebase_uid"] == "firebase:admin-1"
    assert fetched.status_code == 200
    assert fetched.json()["post"]["platform"] == "instagram"
    assert updated.status_code == 200
    assert updated.json()["post"]["title"] == "Updated"
    assert deleted.status_code == 200
    assert deleted.json() == {"success": True}
