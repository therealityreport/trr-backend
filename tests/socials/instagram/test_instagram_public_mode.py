from __future__ import annotations

import json

from trr_backend.socials.instagram.comments_scrapling.fetcher import normalize_comments_load_strategy
from trr_backend.socials.instagram.comments_scrapling.job_runner import (
    _config_public_comments_mode,
    _normalize_comments_session_scope,
)
from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
from trr_backend.socials.instagram.posts_scrapling.job_runner import (
    _public_graphql_page_posts,
    _public_scraper_runtime_metadata,
)
from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy
from trr_backend.socials.instagram.public_post_extractor import parse_public_post_from_html


def test_public_proxy_guards_ignore_decodo_and_explicit_proxy_env(monkeypatch):
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setenv("DECODO_PROXY_URL", "http://user:pass@gate.decodo.com:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", "http://proxy.example:9000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", "http://proxy.example:9000")

    assert select_posts_proxy(session_key="bravotv", public_mode=True) is None
    assert select_comments_proxy(session_key="bravotv", public_mode=True) is None


def test_public_comments_mode_selects_public_relay():
    assert _config_public_comments_mode({"instagram_scrape_mode": "public_first"}) is True
    assert _config_public_comments_mode({"instagram_scrape_mode": "public-first"}) is True
    assert _config_public_comments_mode({"comments_scrape_mode": "no_login"}) is True
    assert _config_public_comments_mode({"comments_load_strategy": "public_relay"}) is True
    assert normalize_comments_load_strategy("public_relay") == "public_relay"


def test_cursor_api_comments_mode_stays_authenticated_without_job_scrape_mode(monkeypatch):
    monkeypatch.delenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", raising=False)

    assert _config_public_comments_mode({"comments_load_strategy": "cursor_api"}) is False


def test_public_comments_mode_can_be_explicitly_authenticated(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "authenticated")

    assert _config_public_comments_mode({"comments_load_strategy": "cursor_api"}) is False


def test_legacy_cursor_api_session_scope_canonicalizes_for_metadata():
    assert (
        _normalize_comments_session_scope("cursor_api_worker", default="fallback")
        == "instagram_comments_endpoint_cursor_worker"
    )
    assert (
        _normalize_comments_session_scope("cursor_api", default="fallback")
        == "instagram_comments_endpoint_cursor_worker"
    )
    assert _normalize_comments_session_scope(None, default="fallback") == "fallback"


def test_public_posts_graphql_page_helpers_return_posts_and_metadata():
    payload = {
        "data": {
            "xdt_api__v1__feed__user_timeline_graphql_connection": {
                "edges": [{"node": {"code": "C2025PUBLIC"}}, {"node": {"code": "C2025NEXT"}}],
                "page_info": {"has_next_page": True, "end_cursor": "cursor-2"},
            }
        }
    }

    posts, page_info = _public_graphql_page_posts(payload)

    assert [post["code"] for post in posts] == ["C2025PUBLIC", "C2025NEXT"]
    assert page_info["has_next_page"] is True
    assert page_info["end_cursor"] == "cursor-2"


def test_public_runtime_metadata_reports_no_auth_and_no_proxy():
    metadata = _public_scraper_runtime_metadata()

    assert metadata["auth_state"] == "public"
    assert metadata["proxy_state"] == "none"
    assert metadata["selected_proxy_fingerprint"] == "none"
    assert metadata["fallback_policy"]["decodo_fallback"] == "requires_approval"


def test_public_post_extractor_parses_carousel_media_and_people():
    shortcode = "C2025PUBLIC"
    media = {
        "code": shortcode,
        "media_type": 8,
        "caption": {"text": "A 2025 public post #Bravo @friend"},
        "taken_at": 1767225600,
        "like_count": 15,
        "comment_count": 4,
        "user": {"username": "bravotv", "pk": "2554414", "is_verified": True},
        "coauthor_producers": [{"username": "collab_one", "pk": "42"}],
        "usertags": {"in": [{"user": {"username": "tagged_one", "pk": "7"}, "position": [0.25, 0.75]}]},
        "carousel_media": [
            {
                "media_type": 1,
                "image_versions2": {
                    "candidates": [
                        {"url": "https://cdn.example/small.jpg", "width": 320, "height": 320},
                        {"url": "https://cdn.example/large.jpg", "width": 1080, "height": 1080},
                    ]
                },
            },
            {
                "media_type": 2,
                "image_versions2": {
                    "candidates": [{"url": "https://cdn.example/thumb.jpg", "width": 640, "height": 640}]
                },
                "video_versions": [{"url": "https://cdn.example/video.mp4", "width": 720, "height": 1280}],
            },
        ],
    }
    html = (
        '<html><head><script type="application/json">'
        + json.dumps({"props": {"pageProps": {"media": media}}})
        + "</script></head></html>"
    )

    post = parse_public_post_from_html(html, shortcode=shortcode)

    assert post is not None
    assert post.shortcode == shortcode
    assert post.owner and post.owner["username"] == "bravotv"
    assert post.caption == "A 2025 public post #Bravo @friend"
    assert post.hashtags == ["Bravo"]
    assert post.mentions == ["@friend"]
    assert post.profile_tags == ["tagged_one"]
    assert post.coauthors == ["collab_one"]
    assert post.media_type == "carousel"
    assert post.media_urls == ["https://cdn.example/large.jpg", "https://cdn.example/video.mp4"]
    assert post.thumbnail_url == "https://cdn.example/large.jpg"
    assert len(post.children) == 2
