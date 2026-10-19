from __future__ import annotations

import json

import pytest

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


def test_strict_public_scraper_cannot_repair_auth_or_enter_browser(monkeypatch):
    from trr_backend.socials.instagram.scraper import InstagramScraper, ScrapeConfig

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", "true")
    scraper = InstagramScraper(cookies={}, strict_public_credentials=True, attach_auth_session=False)
    try:
        assert scraper._try_auto_refresh_cookies() == {"refreshed": False, "reason": "public_mode"}
        assert scraper._try_interactive_login() == {"refreshed": False, "reason": "public_mode"}
        assert scraper._fetch_posts_graphql_with_browser("bravotv") is None
        assert scraper._scrape_browser_intercept(ScrapeConfig(username="bravotv", hashtags=[])) == []
        assert scraper._browser_used is False
    finally:
        scraper.session.close()


def test_public_cursor_reuses_context_and_clears_stale_warmup_error(monkeypatch):
    from trr_backend.socials.instagram.scraper import InstagramScraper

    scraper = InstagramScraper(cookies={}, strict_public_credentials=True, attach_auth_session=False)
    monkeypatch.setattr(scraper._rate_controller, "before_query", lambda *a, **k: None)
    monkeypatch.setattr(scraper, "_get_profile_page_context_cache_entry", lambda username: {"lsd": "public-token"})
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("unexpected warmup")),
    )
    observed = []

    def post(*a, **kwargs):
        observed.append(kwargs)
        scraper.last_retrieval_meta.update(
            error_status_code=403, request_error_code="forbidden", session_block_reason="forbidden"
        )
        return {"data": {scraper.PROFILE_POSTS_ROOT_FIELD_NAME: {"edges": [], "page_info": {}}}}

    monkeypatch.setattr(scraper._request_client, "post_form_json", post)
    try:
        assert scraper.fetch_posts_graphql("bravotv", cursor="next", delay=0) is not None
        assert len(observed) == 1
        assert observed[0]["data"]["av"] == "0" and observed[0]["data"]["lsd"] == "public-token"
        assert "error_status_code" not in scraper.last_retrieval_meta
        assert "request_error_code" not in scraper.last_retrieval_meta
    finally:
        scraper.session.close()


@pytest.mark.parametrize("status", [401, 403, 429])
@pytest.mark.parametrize("cursor", [None, "next"])
def test_public_graphql_stops_after_access_rejection(monkeypatch, status, cursor):
    from trr_backend.socials.instagram.request_client import InstagramRequestFailure
    from trr_backend.socials.instagram.scraper import InstagramScraper

    scraper = InstagramScraper(cookies={}, strict_public_credentials=True, attach_auth_session=False)
    monkeypatch.setattr(scraper._rate_controller, "before_query", lambda *a, **k: None)
    monkeypatch.setattr(scraper, "_warm_profile_request_context", lambda *a, **k: {"lsd": "public-token"})
    monkeypatch.setattr(scraper, "_resolve_graphql_cursor_retry_attempts", lambda cursor: 3)
    monkeypatch.setattr(scraper, "_profile_posts_doc_ids", lambda: ["one", "two"])
    calls = []

    def post(*a, **k):
        calls.append(True)
        raise InstagramRequestFailure(
            {401: "unauthorized", 403: "forbidden", 429: "rate_limited"}[status], status_code=status, retryable=True
        )

    monkeypatch.setattr(scraper._request_client, "post_form_json", post)
    try:
        assert scraper.fetch_posts_graphql("bravotv", cursor=cursor, delay=0) is None
        assert calls == [True]
        assert scraper.last_retrieval_meta["request_error_status_code"] == status
        assert scraper._browser_used is False
    finally:
        scraper.session.close()
