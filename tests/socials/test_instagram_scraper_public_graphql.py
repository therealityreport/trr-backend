from __future__ import annotations

from typing import Any

import requests

from trr_backend.socials.instagram.scraper import InstagramScraper


class _FakeResponse:
    def __init__(self, *, json_payload: dict[str, Any] | None = None, text: str = "", status_code: int = 200) -> None:
        self._json_payload = json_payload or {}
        self.text = text
        self.status_code = status_code
        self.cookies: dict[str, str] = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._json_payload


def test_fetch_posts_graphql_warms_profile_page_and_uses_public_web_headers(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})
    captured: dict[str, Any] = {}

    def _fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        captured["warm_url"] = url
        captured["warm_headers"] = kwargs.get("headers") or {}
        scraper.session.cookies.set("csrftoken", "csrf-token")
        scraper.session.cookies.set("mid", "mid-token")
        return _FakeResponse(
            text=(
                '"LSD",[],{"token":"lsd-token"},323],["ServerNonce",[],{"ServerNonce":"nonce"},141],'
                'bloks_version\\":\\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\\"'
            )
        )

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        captured["post_url"] = url
        captured["post_headers"] = kwargs.get("headers") or {}
        captured["post_data"] = dict(kwargs.get("data") or {})
        captured["post_cookies"] = kwargs.get("cookies") or {}
        return _FakeResponse(
            json_payload={
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"code": "ABC123", "pk": "1", "id": "1_1"}}],
                        "page_info": {"has_next_page": False, "end_cursor": None},
                    }
                }
            }
        )

    monkeypatch.setattr(scraper, "_get", _fake_get)
    monkeypatch.setattr(scraper, "_post", _fake_post)

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload is not None
    assert captured["warm_url"] == "https://www.instagram.com/bravotv/"
    assert captured["post_url"] == scraper.GRAPHQL_URL
    assert captured["post_headers"]["x-fb-friendly-name"] == "PolarisProfilePostsTabContentQuery_connection"
    assert captured["post_headers"]["x-fb-lsd"] == "lsd-token"
    assert captured["post_headers"]["x-csrftoken"] == "csrf-token"
    assert captured["post_headers"]["x-asbd-id"] == scraper.WEB_X_ASBD_ID
    assert (
        captured["post_headers"]["x-bloks-version-id"]
        == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    assert captured["post_data"]["doc_id"] == "25645538101792896"
    assert captured["post_data"]["av"] == "0"
    assert captured["post_data"]["__user"] == "0"


def test_fetch_posts_graphql_retries_cursor_pages_with_fresh_profile_context(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})
    warm_force_flags: list[bool] = []
    post_headers: list[dict[str, Any]] = []

    def _fake_warm(username: str, *, timeout: Any = None, force: bool = False) -> dict[str, str]:
        del timeout
        assert username == "bravotv"
        warm_force_flags.append(force)
        scraper.session.cookies.set("csrftoken", f"csrf-{len(warm_force_flags)}")
        return {
            "lsd": f"lsd-{len(warm_force_flags)}",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        assert url == scraper.GRAPHQL_URL
        post_headers.append(dict(kwargs.get("headers") or {}))
        if len(post_headers) == 1:
            raise requests.exceptions.HTTPError("http_401")
        return _FakeResponse(
            json_payload={
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"code": "ABC123", "pk": "1", "id": "1_1"}}],
                        "page_info": {"has_next_page": True, "end_cursor": "cursor-2"},
                    }
                }
            }
        )

    monkeypatch.setattr(scraper, "_warm_profile_request_context", _fake_warm)
    monkeypatch.setattr(scraper, "_post", _fake_post)

    payload = scraper.fetch_posts_graphql("bravotv", "cursor-1", 0.0)

    assert payload is not None
    assert warm_force_flags == [True]
    assert len(post_headers) == 2
    assert post_headers[0]["x-fb-lsd"] == "lsd-1"
    assert post_headers[1]["x-fb-lsd"] == "lsd-1"
    assert post_headers[0]["x-csrftoken"] == "csrf-1"
    assert post_headers[1]["x-csrftoken"] == "csrf-1"
