from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from trr_backend.socials.facebook.direct_scrape import (
    comment_to_payload,
    post_to_payload,
    preview_facebook_page,
    scrape_facebook,
    scrape_facebook_post,
    search_facebook_posts,
)
from trr_backend.socials.facebook.scraper import (
    FacebookComment,
    FacebookMediaProvenance,
    FacebookPost,
    FacebookShare,
)

DIRECT_SCRAPE_PATH = Path(__file__).resolve().parents[3] / "trr_backend" / "socials" / "facebook" / "direct_scrape.py"


def test_direct_scrape_imports_scraper_leaf_without_package_root_cycle() -> None:
    tree = ast.parse(
        DIRECT_SCRAPE_PATH.read_text(encoding="utf-8"),
        filename=str(DIRECT_SCRAPE_PATH),
    )
    import_from_modules = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}

    assert "trr_backend.socials.facebook" not in import_from_modules
    assert "trr_backend.socials.facebook.scraper" in import_from_modules


def _make_post(**overrides: Any) -> FacebookPost:
    payload: dict[str, Any] = {
        "post_id": "fb-post-1",
        "username": "BravoTV",
        "post_type": "reel",
        "caption": "Opa! #RHOSLC The moment you've been waiting for is finally here.",
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


def test_post_to_payload_preserves_route_shape_for_shares_and_media_provenance() -> None:
    post = _make_post(
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
            source_url="https://www.instagram.com/p/abc",
        ),
    )

    payload = post_to_payload(post)

    assert payload == {
        "post_id": "fb-post-1",
        "post_type": "reel",
        "username": "BravoTV",
        "caption": "Opa! #RHOSLC The moment you've been waiting for is finally here.",
        "likes": 120,
        "comments": 10,
        "shares": 6,
        "views": 5000,
        "url": "https://www.facebook.com/BravoTV/posts/123",
        "thumbnail_url": "https://cdn.facebook.test/thumb.jpg",
        "media_urls": ["https://cdn.facebook.test/video.mp4"],
        "posted_at": "2025-09-16T12:00:00+00:00",
        "reactions": {"Like": 100, "Love": 20},
        "share_details": [
            {
                "sharer_name": "Kyle Davis Karnes",
                "profile_url": "https://www.facebook.com/kyle.d.karnes",
                "post_url": "https://www.facebook.com/kyle.d.karnes/posts/456",
                "caption_snippet": "Opa! The moment you've been waiting for is finally here.",
                "posted_at": "2025-09-16T13:00:00+00:00",
                "privacy_label": "Shared with Public",
                "media_preview_urls": ["https://cdn.facebook.test/share-preview.jpg"],
            }
        ],
        "media_provenance": {
            "platform": "instagram",
            "matched_by": "caption+same_day+type_or_duration",
            "fallback_used": True,
        },
    }


def test_comment_to_payload_preserves_route_shape() -> None:
    comment = FacebookComment(
        comment_id="comment-1",
        username="Commenter",
        text="First",
        likes=3,
        created_at=1_759_000_000,
        is_reply=True,
        reply_count=2,
    )

    assert comment_to_payload(comment) == {
        "comment_id": "comment-1",
        "username": "Commenter",
        "text": "First",
        "likes": 3,
        "created_at": 1_759_000_000,
        "is_reply": True,
        "reply_count": 2,
    }


def test_scrape_facebook_calls_surface_loader_and_filters_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {"surfaces": []}

    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            captured["cookies"] = cookies
            self.last_retrieval_meta = {"source": "facebook_feed"}

        def scrape(self, config: Any) -> list[FacebookPost]:
            captured["config"] = config
            return [
                _make_post(post_id="keep", caption="The #RHOSLC reunion is here"),
                _make_post(post_id="drop", caption="Summer House update"),
            ]

    monkeypatch.setattr("trr_backend.socials.facebook.scraper.FacebookScraper", FakeFacebookScraper)

    request = SimpleNamespace(
        page_handle="BravoTV",
        hashtags=["RHOSLC"],
        keywords=["reunion"],
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 12, 31, tzinfo=UTC),
        delay_seconds=0.25,
        max_pages=2,
    )
    response = scrape_facebook(
        request,
        load_cookies=lambda surface: captured["surfaces"].append(surface) or {"c_user": "1"},
    )

    assert captured["surfaces"] == ["scrape"]
    assert captured["cookies"] == {"c_user": "1"}
    assert captured["config"].page_handle == "BravoTV"
    assert captured["config"].include_feed is True
    assert captured["config"].include_reels is True
    assert captured["config"].include_photos is True
    assert response["success"] is True
    assert response["posts_found"] == 1
    assert response["posts"][0]["post_id"] == "keep"
    assert response["filters_applied"] == {
        "hashtags": ["RHOSLC"],
        "keywords": ["reunion"],
        "date_start": "2025-01-01T00:00:00+00:00",
        "date_end": "2025-12-31T00:00:00+00:00",
    }
    assert response["retrieval_meta"] == {"source": "facebook_feed"}


def test_search_facebook_posts_builds_config_and_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {"surfaces": []}

    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            captured["cookies"] = cookies
            self.last_retrieval_meta = {"source": "facebook_search", "cookies_supplied": bool(cookies)}

        def search_posts(self, config: Any) -> list[FacebookPost]:
            captured["config"] = config
            return [_make_post()]

    monkeypatch.setattr("trr_backend.socials.facebook.scraper.FacebookScraper", FakeFacebookScraper)
    request = SimpleNamespace(
        search_url=None,
        profile_url="https://www.facebook.com/BravoTV",
        query="#RHOSLC",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 12, 31, tzinfo=UTC),
        max_posts=25,
        include_share_details=True,
        include_comments=True,
        max_comments=50,
        max_shares=75,
        allow_cross_platform_media_fallback=False,
        delay_seconds=0.5,
    )

    response = search_facebook_posts(
        request,
        load_cookies=lambda surface: captured["surfaces"].append(surface) or {"c_user": "1"},
    )

    assert captured["surfaces"] == ["search_posts"]
    assert captured["config"].profile_url == "https://www.facebook.com/BravoTV"
    assert captured["config"].query == "#RHOSLC"
    assert captured["config"].include_share_details is True
    assert captured["config"].include_comments is True
    assert captured["config"].allow_cross_platform_media_fallback is False
    assert response["success"] is True
    assert response["query"] == "#RHOSLC"
    assert response["posts_found"] == 1
    assert response["retrieval_meta"] == {"source": "facebook_search", "cookies_supplied": True}


def test_preview_facebook_page_calls_surface_loader_and_returns_latest(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {"surfaces": []}

    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            captured["cookies"] = cookies
            self.last_retrieval_meta = {"source": "preview"}

        def scrape(self, config: Any) -> list[FacebookPost]:
            captured["config"] = config
            return [_make_post()]

    monkeypatch.setattr("trr_backend.socials.facebook.scraper.FacebookScraper", FakeFacebookScraper)

    response = preview_facebook_page(
        "BravoTV",
        load_cookies=lambda surface: captured["surfaces"].append(surface) or {"c_user": "1"},
    )

    assert captured["surfaces"] == ["preview"]
    assert captured["config"].page_handle == "BravoTV"
    assert captured["config"].max_pages == 1
    assert response == {
        "page_handle": "BravoTV",
        "posts_discovered": 1,
        "latest_post": {
            "post_id": "fb-post-1",
            "post_type": "reel",
            "url": "https://www.facebook.com/BravoTV/posts/123",
            "caption": "Opa! #RHOSLC The moment you've been waiting for is finally here.",
        },
        "retrieval_meta": {"source": "preview"},
    }


def test_scrape_facebook_post_passes_flags_and_shapes_comments(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {"surfaces": []}

    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            captured["cookies"] = cookies

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
                [FacebookComment(comment_id="comment-1", username="Commenter", text="First", likes=3)],
            )

    monkeypatch.setattr("trr_backend.socials.facebook.scraper.FacebookScraper", FakeFacebookScraper)
    request = SimpleNamespace(
        post_url="https://www.facebook.com/BravoTV/posts/123",
        fetch_comments=True,
        max_comments=25,
        fetch_shares=True,
        max_shares=50,
        allow_cross_platform_media_fallback=False,
    )

    response = scrape_facebook_post(
        request,
        load_cookies=lambda surface: captured["surfaces"].append(surface) or {"c_user": "1"},
    )

    assert captured["surfaces"] == ["scrape_post"]
    assert captured["post_url"] == "https://www.facebook.com/BravoTV/posts/123"
    assert captured["kwargs"] == {
        "fetch_comment_list": True,
        "max_comments": 25,
        "fetch_share_list": True,
        "max_shares": 50,
        "allow_cross_platform_media_fallback": False,
    }
    assert response["success"] is True
    assert response["comments_found"] == 1
    assert response["comments"][0]["comment_id"] == "comment-1"
    assert response["shares_found"] == 1


def test_scrape_facebook_post_preserves_failed_post_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            pass

        def scrape_post(self, post_url: str, **kwargs: Any) -> tuple[None, list[FacebookComment]]:
            return None, []

    monkeypatch.setattr("trr_backend.socials.facebook.scraper.FacebookScraper", FakeFacebookScraper)
    request = SimpleNamespace(
        post_url="https://www.facebook.com/BravoTV/posts/123",
        fetch_comments=True,
        max_comments=25,
        fetch_shares=True,
        max_shares=50,
        allow_cross_platform_media_fallback=False,
    )

    assert scrape_facebook_post(request, load_cookies=lambda _surface: {"c_user": "1"}) == {
        "success": False,
        "post": None,
        "comments": [],
        "comments_found": 0,
        "shares_found": 0,
        "error": "Failed to fetch post",
    }


def test_preview_facebook_page_preserves_http_500_error_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeFacebookScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None) -> None:
            pass

        def scrape(self, config: Any) -> list[FacebookPost]:
            raise RuntimeError("document fetch failed")

    monkeypatch.setattr("trr_backend.socials.facebook.scraper.FacebookScraper", FakeFacebookScraper)

    with pytest.raises(HTTPException) as exc_info:
        preview_facebook_page("BravoTV", load_cookies=lambda _surface: {"c_user": "1"})

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "document fetch failed"


def test_direct_scrape_keeps_catalog_and_repository_imports_out_of_direct_ownership() -> None:
    source = Path(__file__).resolve().parents[3] / "trr_backend" / "socials" / "facebook" / "direct_scrape.py"
    module_text = source.read_text(encoding="utf-8")

    assert "trr_backend.repositories.social_season_analytics" not in module_text
    assert "posts_catalog" not in module_text
