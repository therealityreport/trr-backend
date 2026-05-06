from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import jwt
from fastapi.testclient import TestClient

from api.main import app
from api.routers import socials as socials_router


def _make_admin_token(secret: str) -> str:
    now = datetime.now(tz=UTC)
    return jwt.encode(
        {
            "sub": "admin-1",
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
            "role": "admin",
            "email": "admin@example.com",
        },
        secret,
        algorithm="HS256",
    )


def test_get_instagram_profile_detail_route(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    client = TestClient(app)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {"profile": {"username": "nasa", "id": "528817151"}}

    with patch(
        "trr_backend.socials.instagram.profile_stages.get_instagram_profile_detail",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/nasa/profile?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {"account_handle": "nasa", "source_scope": "bravo"}


def test_get_instagram_profile_relationships_route_forwards_following_type(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    client = TestClient(app)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "owner": {"username": "nasa"},
        "relationship_type": "following",
        "items": [],
        "pagination": {"page": 2, "page_size": 10, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.socials.instagram.profile_stages.get_instagram_profile_relationships",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/nasa/relationships?type=following&page=2&page_size=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "account_handle": "nasa",
        "source_scope": "bravo",
        "relationship_type": "following",
        "page": 2,
        "page_size": 10,
    }


def test_get_instagram_profile_comments_route_returns_comment_breakdown(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    with socials_router._ACCOUNT_PROFILE_POSTS_CACHE_LOCK:
        socials_router._ACCOUNT_PROFILE_POSTS_CACHE.clear()
    client = TestClient(app)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [],
        "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1},
        "pagination_mode": "parent_threads",
        "comment_breakdown": {
            "reported_comments": 149,
            "saved_parent_comments": 104,
            "saved_child_replies": 40,
            "facebook_comments": 0,
            "saved_instagram_comments": 144,
            "accounted_comments": 149,
            "missing_comments": 5,
            "missing_reasons": {"instagram_not_served_after_all_lanes": 5},
            "formula_label": (
                "104 parent comments + 40 child replies + 0 Facebook comments + "
                "5 missing comments = 149 reported comments"
            ),
        },
        "facebook_crosspost": {"comments_count": 0},
    }

    with patch(
        "api.routers.socials.social_profile_reads.get_profile_comments",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/thetraitorsus-route-test/comments"
            "?post_source_id=DU_oEbbgZfJ",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "platform": "instagram",
        "account_handle": "thetraitorsus-route-test",
        "page": 1,
        "page_size": 25,
        "post_source_id": "DU_oEbbgZfJ",
        "search": None,
        "sort_by": None,
        "sort_dir": None,
    }
