"""Tests for Facebook admin scraping endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.socials.facebook.scraper import (
    FacebookComment,
    FacebookMediaProvenance,
    FacebookPost,
    FacebookShare,
)


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def auth_header(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    secret = "test-secret-32-bytes-minimum-abcdef"
    monkeypatch.setenv("SUPABASE_JWT_SECRET", secret)
    return {"Authorization": f"Bearer {_make_admin_token(secret)}"}


def _make_post(**overrides: Any) -> FacebookPost:
    payload: dict[str, Any] = {
        "post_id": "fb-post-1",
        "username": "BravoTV",
        "post_type": "reel",
        "caption": "Opa! The moment you've been waiting for is finally here.",
        "media_urls": ["https://cdn.facebook.test/video.mp4"],
        "thumbnail_url": "https://cdn.facebook.test/thumb.jpg",
        "likes": 120,
        "comments": 10,
        "shares": 6,
        "views": 5000,
        "posted_at": int(datetime(2025, 9, 16, 12, 0, tzinfo=UTC).timestamp()),
        "url": "https://www.facebook.com/BravoTV/posts/123",
        "reactions": {"Like": 100, "Love": 20},
        "share_details": [],
        "media_provenance": FacebookMediaProvenance(),
        "raw_data": {},
    }
    payload.update(overrides)
    return FacebookPost(**payload)


def test_search_facebook_posts_success(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    auth_header: dict[str, str],
) -> None:
    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            self.cookies = cookies or {}
            self.last_retrieval_meta = {"source": "facebook_search", "cookies_supplied": bool(self.cookies)}

        def search_posts(self, config: Any) -> list[FacebookPost]:
            assert config.query == "#RHOSLC"
            return [
                _make_post(
                    share_details=[
                        FacebookShare(
                            sharer_name="Kyle Davis Karnes",
                            profile_url="https://www.facebook.com/kyle.d.karnes",
                            post_url="https://www.facebook.com/kyle.d.karnes/posts/456",
                            caption_snippet="Opa! The moment you've been waiting for is finally here.",
                            posted_at=int(datetime(2025, 9, 16, 13, 0, tzinfo=UTC).timestamp()),
                            privacy_label="Shared with Public",
                            media_preview_urls=["https://cdn.facebook.test/share-preview.jpg"],
                        )
                    ],
                    media_provenance=FacebookMediaProvenance(
                        platform="instagram",
                        matched_by="caption+same_day+type_or_duration",
                        fallback_used=True,
                    ),
                )
            ]

    monkeypatch.setattr("api.routers.socials._load_social_auth_or_503", lambda **_: {"c_user": "1"})
    monkeypatch.setattr("trr_backend.socials.facebook.FacebookScraper", FakeFacebookScraper)

    response = client.post(
        "/api/v1/admin/socials/facebook/search-posts",
        headers=auth_header,
        json={
            "profile_url": "https://www.facebook.com/BravoTV",
            "query": "#RHOSLC",
            "date_start": "2025-01-01T00:00:00Z",
            "date_end": "2025-12-31T23:59:59Z",
            "include_share_details": True,
            "allow_cross_platform_media_fallback": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["posts_found"] == 1
    assert body["posts"][0]["share_details"][0]["sharer_name"] == "Kyle Davis Karnes"
    assert body["posts"][0]["media_provenance"] == {
        "platform": "instagram",
        "matched_by": "caption+same_day+type_or_duration",
        "fallback_used": True,
    }


def test_search_facebook_posts_requires_query_and_source(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.post(
        "/api/v1/admin/socials/facebook/search-posts",
        headers=auth_header,
        json={"query": "   "},
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any("query is required" in str(item.get("msg", "")) for item in detail)


def test_scrape_facebook_post_passes_additive_flags(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    auth_header: dict[str, str],
) -> None:
    captured: dict[str, Any] = {}

    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            self.cookies = cookies or {}

        def scrape_post(self, post_url: str, **kwargs: Any) -> tuple[FacebookPost | None, list[FacebookComment]]:
            captured["post_url"] = post_url
            captured["kwargs"] = kwargs
            return (
                _make_post(
                    share_details=[
                        FacebookShare(
                            sharer_name="Sharer One",
                            profile_url="https://www.facebook.com/sharer.one",
                        )
                    ]
                ),
                [
                    FacebookComment(
                        comment_id="comment-1",
                        username="Commenter",
                        text="First",
                        likes=3,
                    )
                ],
            )

    monkeypatch.setattr("api.routers.socials._load_social_auth_or_503", lambda **_: {"c_user": "1"})
    monkeypatch.setattr("trr_backend.socials.facebook.FacebookScraper", FakeFacebookScraper)

    response = client.post(
        "/api/v1/admin/socials/facebook/scrape-post",
        headers=auth_header,
        json={
            "post_url": "https://www.facebook.com/BravoTV/posts/123",
            "fetch_comments": True,
            "max_comments": 25,
            "fetch_shares": True,
            "max_shares": 50,
            "allow_cross_platform_media_fallback": False,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["comments_found"] == 1
    assert body["shares_found"] == 1
    assert captured["post_url"] == "https://www.facebook.com/BravoTV/posts/123"
    assert captured["kwargs"] == {
        "fetch_comment_list": True,
        "max_comments": 25,
        "fetch_share_list": True,
        "max_shares": 50,
        "allow_cross_platform_media_fallback": False,
    }


def test_scrape_facebook_route_remains_backward_compatible(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    auth_header: dict[str, str],
) -> None:
    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            self.cookies = cookies or {}
            self.last_retrieval_meta = {"source": "facebook_feed"}

        def scrape(self, config: Any) -> list[FacebookPost]:
            assert config.page_handle == "BravoTV"
            return [_make_post(post_type="feed", media_provenance=FacebookMediaProvenance())]

    monkeypatch.setattr("api.routers.socials._load_social_auth_or_503", lambda **_: {"c_user": "1"})
    monkeypatch.setattr("trr_backend.socials.facebook.FacebookScraper", FakeFacebookScraper)

    response = client.post(
        "/api/v1/admin/socials/facebook/scrape",
        headers=auth_header,
        json={"page_handle": "BravoTV", "hashtags": [], "keywords": []},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["page_handle"] == "BravoTV"
    assert body["posts_found"] == 1
    assert body["posts"][0]["post_type"] == "feed"
    assert body["posts"][0]["share_details"] == []
    assert body["posts"][0]["media_provenance"] == {
        "platform": "facebook",
        "matched_by": "native",
        "fallback_used": False,
    }
