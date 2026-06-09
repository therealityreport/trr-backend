"""Scrapling runtime canary tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from trr_backend.socials.instagram.runtimes.protocol import RuntimeUnsupported
from trr_backend.socials.instagram.runtimes.scrapling_runtime import ScraplingRuntime


def _run(coro):
    return asyncio.run(coro)


def test_healthcheck_stays_unhealthy_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INSTAGRAM_SCRAPLING_RUNTIME_ENABLED", raising=False)

    health = ScraplingRuntime().healthcheck()

    assert health.healthy is False
    assert health.reason == "instagram_scrapling_runtime_enabled_not_enabled"


def test_healthcheck_is_healthy_when_canary_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INSTAGRAM_SCRAPLING_RUNTIME_ENABLED", "true")

    health = ScraplingRuntime().healthcheck()

    assert health.healthy is True
    assert health.reason is None


def test_fetch_profile_maps_web_profile_info_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        assert url.endswith("/api/v1/users/web_profile_info/")
        assert params == {"username": "bravotv"}
        return {
            "data": {
                "user": {
                    "username": "bravotv",
                    "id": "123",
                    "full_name": "Bravo",
                    "biography": "Bio",
                    "edge_followed_by": {"count": 10},
                    "edge_follow": {"count": 5},
                    "edge_owner_to_timeline_media": {"count": 2},
                    "is_private": False,
                    "is_verified": True,
                }
            }
        }

    monkeypatch.setattr(ScraplingRuntime, "_fetch_json", fake_fetch_json)

    profile = _run(ScraplingRuntime().fetch_profile("bravotv"))

    assert profile.username == "bravotv"
    assert profile.user_id == "123"
    assert profile.full_name == "Bravo"
    assert profile.biography == "Bio"
    assert profile.follower_count == 10
    assert profile.following_count == 5
    assert profile.post_count == 2
    assert profile.is_verified is True


def test_fetch_profile_raises_runtime_unsupported_for_empty_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"data": {"user": None}}

    monkeypatch.setattr(ScraplingRuntime, "_fetch_json", fake_fetch_json)

    with pytest.raises(RuntimeUnsupported, match="empty profile payload"):
        _run(ScraplingRuntime().fetch_profile("bravotv"))


def test_fetch_posts_maps_timeline_edges_and_respects_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "data": {
                "user": {
                    "edge_owner_to_timeline_media": {
                        "edges": [
                            {
                                "node": {
                                    "__typename": "GraphImage",
                                    "shortcode": "one",
                                    "taken_at_timestamp": 1_776_272_000,
                                    "display_url": "https://cdn.example.com/one.jpg",
                                    "edge_media_to_caption": {"edges": [{"node": {"text": "One #tag"}}]},
                                    "edge_liked_by": {"count": 11},
                                    "edge_media_to_comment": {"count": 3},
                                }
                            },
                            {
                                "node": {
                                    "__typename": "GraphVideo",
                                    "shortcode": "two",
                                    "edge_media_to_caption": {"edges": [{"node": {"text": "Two"}}]},
                                }
                            },
                        ]
                    }
                }
            }
        }

    monkeypatch.setattr(ScraplingRuntime, "_fetch_json", fake_fetch_json)

    posts = _run(ScraplingRuntime().fetch_posts("bravotv", limit=1))

    assert len(posts) == 1
    assert posts[0].shortcode == "one"
    assert posts[0].caption == "One #tag"
    assert posts[0].posted_at == datetime.fromtimestamp(1_776_272_000, tz=UTC)
    assert posts[0].media_type == "image"
    assert posts[0].media_urls == ("https://cdn.example.com/one.jpg",)
    assert posts[0].like_count == 11
    assert posts[0].comment_count == 3


def test_fetch_posts_raises_runtime_unsupported_for_empty_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"data": {"user": {"edge_owner_to_timeline_media": {"edges": []}}}}

    monkeypatch.setattr(ScraplingRuntime, "_fetch_json", fake_fetch_json)

    with pytest.raises(RuntimeUnsupported, match="empty posts payload"):
        _run(ScraplingRuntime().fetch_posts("bravotv", limit=3))


def test_fetch_post_detail_maps_supported_shortcode_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        assert url == "https://www.instagram.com/p/abc123/"
        return {
            "graphql": {
                "shortcode_media": {
                    "__typename": "GraphImage",
                    "shortcode": "abc123",
                    "edge_media_to_caption": {"edges": [{"node": {"text": "Launch #Bravo @traitors"}}]},
                    "display_url": "https://cdn.example.com/abc123.jpg",
                }
            }
        }

    monkeypatch.setattr(ScraplingRuntime, "_fetch_json", fake_fetch_json)

    detail = _run(ScraplingRuntime().fetch_post_detail("abc123"))

    assert detail.post.shortcode == "abc123"
    assert detail.post.caption == "Launch #Bravo @traitors"
    assert detail.hashtags == ("Bravo",)
    assert detail.mentions == ("@traitors",)
    assert detail.permalink == "https://www.instagram.com/p/abc123/"


def test_fetch_post_detail_raises_runtime_unsupported_for_unknown_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"status": "ok"}

    monkeypatch.setattr(ScraplingRuntime, "_fetch_json", fake_fetch_json)

    with pytest.raises(RuntimeUnsupported, match="unsupported detail payload"):
        _run(ScraplingRuntime().fetch_post_detail("abc123"))


def test_fetch_json_uses_basic_fetcher_and_request_cookie_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status_code = 200

        def json(self) -> dict[str, Any]:
            return {"ok": True}

    class FakeFetcher:
        async def async_fetch(self, url: str, **kwargs: Any) -> FakeResponse:
            assert url == "https://www.instagram.com/api/v1/example"
            assert kwargs["params"] == {"username": "bravotv"}
            assert kwargs["cookies"] == {"sessionid": "abc", "csrftoken": "xyz"}
            assert kwargs["timeout"] == 45_000
            return FakeResponse()

    monkeypatch.setattr(
        "trr_backend.socials.instagram.runtimes.scrapling_runtime.build_fetcher",
        lambda: FakeFetcher(),
    )

    payload = _run(
        ScraplingRuntime(cookies={"sessionid": "abc", "csrftoken": "xyz"})._fetch_json(
            "https://www.instagram.com/api/v1/example",
            params={"username": "bravotv"},
        )
    )

    assert payload == {"ok": True}


def test_fetch_json_supports_installed_basic_fetcher_get_method(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status_code = 200

        def json(self) -> dict[str, Any]:
            return {"ok": True}

    class FakeFetcher:
        def get(self, url: str, **kwargs: Any) -> FakeResponse:
            assert url == "https://www.instagram.com/api/v1/example"
            assert kwargs["params"] == {"username": "bravotv"}
            assert kwargs["cookies"] == {"sessionid": "abc"}
            return FakeResponse()

    monkeypatch.setattr(
        "trr_backend.socials.instagram.runtimes.scrapling_runtime.build_fetcher",
        lambda: FakeFetcher(),
    )

    payload = _run(
        ScraplingRuntime(cookies={"sessionid": "abc"})._fetch_json(
            "https://www.instagram.com/api/v1/example",
            params={"username": "bravotv"},
        )
    )

    assert payload == {"ok": True}


def test_fetch_json_raises_runtime_unsupported_for_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status_code = 429
        text = "{}"

    class FakeFetcher:
        async def async_fetch(self, url: str, **kwargs: Any) -> FakeResponse:
            return FakeResponse()

    monkeypatch.setattr(
        "trr_backend.socials.instagram.runtimes.scrapling_runtime.build_fetcher",
        lambda: FakeFetcher(),
    )

    with pytest.raises(RuntimeUnsupported, match="HTTP 429"):
        _run(ScraplingRuntime()._fetch_json("https://www.instagram.com/api/v1/example"))
