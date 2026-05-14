from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
import requests

from trr_backend.socials.threads.scraper import (
    _THREADS_POST_VIEW_COUNT_DOC_ID,
    ThreadsComment,
    ThreadsPost,
    ThreadsScrapeConfig,
    ThreadsScraper,
    _PageTokens,
)


class _FakeResponse:
    status_code = 200
    text = "<html></html>"

    @staticmethod
    def json() -> dict[str, Any]:
        return {"data": {"mediaData": {"edges": [], "page_info": {"has_next_page": False}}}}

    @staticmethod
    def raise_for_status() -> None:
        return None


def _graphql_edge(*, impression_count: int | None = None) -> dict[str, Any]:
    tpa: dict[str, Any] = {
        "direct_reply_count": 6,
        "repost_count": 11,
        "quote_count": 2,
        "tag_header": {"display_name": "rhoslc", "tag_cluster_name": "bravotv"},
    }
    if impression_count is not None:
        tpa["impression_count"] = impression_count
    return {
        "node": {
            "thread_items": [
                {
                    "post": {
                        "pk": "3699063449418169480",
                        "code": "DNVtpvYM3yI",
                        "user": {"username": "bravotv", "is_verified": True, "full_name": "Bravo"},
                        "text_post_app_info": tpa,
                        "caption": {"text": "Your prayers have been answered. #RHOSLC is back"},
                        "taken_at": int(datetime(2025, 8, 14, tzinfo=UTC).timestamp()),
                        "canonical_url": "https://www.threads.com/@bravotv/post/DNVtpvYM3yI",
                        "like_count": 186,
                    }
                }
            ]
        }
    }


def test_threads_scraper_uses_configured_proxy_for_document_and_graphql_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"}, proxy_url="http://user:pass@proxy.test:8080")
    captured: dict[str, Any] = {}

    def _fake_get(*_args: Any, **kwargs: Any) -> _FakeResponse:
        captured["get_proxies"] = kwargs.get("proxies")
        return _FakeResponse()

    def _fake_post(*_args: Any, **kwargs: Any) -> _FakeResponse:
        captured["post_proxies"] = kwargs.get("proxies")
        return _FakeResponse()

    monkeypatch.setattr(scraper.session, "get", _fake_get)
    monkeypatch.setattr(scraper.session, "post", _fake_post)

    scraper._fetch_html("https://www.threads.com/@bravotv", delay_seconds=0)  # noqa: SLF001
    scraper._graphql_query(  # noqa: SLF001
        tokens=_PageTokens(fb_dtsg="fb-dtsg", lsd="lsd", jazoest="26474", user_id="123"),
        doc_id="doc",
        variables={},
        friendly_name="BarcelonaProfileThreadsTabDirectQuery",
        delay_seconds=0,
    )

    expected = {
        "http": "http://user:pass@proxy.test:8080",
        "https": "http://user:pass@proxy.test:8080",
    }
    assert captured["get_proxies"] == expected
    assert captured["post_proxies"] == expected
    assert scraper.runtime_metadata["proxy_configured"] is True


def test_threads_fetch_comments_continues_when_token_exists_but_has_more_is_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "cookie", "csrftoken": "token"})
    paging_tokens: list[str | None] = []

    monkeypatch.setattr(scraper, "_resolve_post_pk", lambda *_args, **_kwargs: "1234567890123456789")

    def _fake_fetch_replies_page(
        _post_pk: str,
        *,
        paging_token: str | None = None,
    ) -> tuple[list[ThreadsComment], str | None, bool]:
        paging_tokens.append(paging_token)
        scraper._last_replies_page_meta = {"root_direct_reply_count": 3}  # noqa: SLF001
        if paging_token is None:
            return (
                [ThreadsComment(comment_id="reply-1", username="one", text="one")],
                "next-token",
                False,
            )
        return ([ThreadsComment(comment_id="reply-2", username="two", text="two")], None, False)

    monkeypatch.setattr(scraper, "_fetch_replies_page", _fake_fetch_replies_page)

    comments = scraper.fetch_comments("https://www.threads.com/@bravowwhl/post/DUCpTSVAPrR", max_comments=10)

    assert [comment.comment_id for comment in comments] == ["reply-1", "reply-2"]
    assert paging_tokens == [None, "next-token"]
    assert scraper.last_comment_fetch_reason == "threads_replies_ok"
    assert scraper.last_comment_fetch_meta["root_direct_reply_count"] == 3
    assert scraper.last_comment_fetch_meta["unavailable_reply_count"] == 1


def test_threads_fetch_post_view_count_reads_impression_count(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    captured: dict[str, Any] = {}

    def _fake_graphql_query(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"data": {"media": {"text_post_app_info": {"impression_count": "6,829"}}}}

    monkeypatch.setattr(scraper, "_graphql_query", _fake_graphql_query)

    views = scraper._fetch_post_view_count(  # noqa: SLF001
        tokens=_PageTokens(fb_dtsg="fb-dtsg", lsd="lsd", jazoest="26474", user_id="123"),
        post_pk="3699063449418169480",
        referer="https://www.threads.com/@bravotv/post/DNVtpvYM3yI",
        delay_seconds=0,
    )

    assert views == 6829
    assert captured["doc_id"] == _THREADS_POST_VIEW_COUNT_DOC_ID
    assert captured["friendly_name"] == "BarcelonaPostViewCountQuery"
    assert captured["variables"] == {"postID": "3699063449418169480"}


def test_threads_scrape_via_graphql_populates_views_from_activity_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    tokens = _PageTokens(fb_dtsg="fb-dtsg", lsd="lsd", jazoest="26474", user_id="123")

    monkeypatch.setattr(scraper, "_extract_page_tokens", lambda _html: tokens)
    monkeypatch.setattr(
        scraper,
        "_fetch_profile_posts_page",
        lambda **_kwargs: ([_graphql_edge(impression_count=None)], None, False),
    )

    captured: dict[str, Any] = {}

    def _fake_fetch_post_view_count(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 6829

    monkeypatch.setattr(scraper, "_fetch_post_view_count", _fake_fetch_post_view_count)

    posts = scraper._scrape_via_graphql(  # noqa: SLF001
        ThreadsScrapeConfig(
            username="bravotv",
            date_start=datetime(2025, 8, 1, tzinfo=UTC),
            date_end=datetime(2025, 8, 31, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
        ),
        page_html="<html></html>",
        profile_url="https://www.threads.com/@bravotv",
    )

    assert posts is not None
    assert len(posts) == 1
    assert posts[0].views == 6829
    assert captured["post_pk"] == "3699063449418169480"
    text_post_app_info = posts[0].raw_data.get("text_post_app_info") or {}
    assert text_post_app_info.get("impression_count") == 6829


def test_threads_scrape_via_graphql_keeps_feed_impression_count_without_activity_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    tokens = _PageTokens(fb_dtsg="fb-dtsg", lsd="lsd", jazoest="26474", user_id="123")

    monkeypatch.setattr(scraper, "_extract_page_tokens", lambda _html: tokens)
    monkeypatch.setattr(
        scraper,
        "_fetch_profile_posts_page",
        lambda **_kwargs: ([_graphql_edge(impression_count=120)], None, False),
    )
    monkeypatch.setattr(
        scraper,
        "_fetch_post_view_count",
        lambda **_kwargs: pytest.fail("expected feed impression_count to skip activity lookup"),
    )

    posts = scraper._scrape_via_graphql(  # noqa: SLF001
        ThreadsScrapeConfig(
            username="bravotv",
            date_start=datetime(2025, 8, 1, tzinfo=UTC),
            date_end=datetime(2025, 8, 31, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
        ),
        page_html="<html></html>",
        profile_url="https://www.threads.com/@bravotv",
    )

    assert posts is not None
    assert len(posts) == 1
    assert posts[0].views == 120


def test_threads_scrape_via_graphql_updates_runtime_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    tokens = _PageTokens(fb_dtsg="fb-dtsg", lsd="lsd", jazoest="26474", user_id="123")

    monkeypatch.setattr(scraper, "_extract_page_tokens", lambda _html: tokens)
    monkeypatch.setattr(
        scraper,
        "_fetch_profile_posts_page",
        lambda **_kwargs: ([_graphql_edge(impression_count=120)], None, False),
    )
    monkeypatch.setattr(
        scraper,
        "_fetch_post_view_count",
        lambda **_kwargs: pytest.fail("expected feed impression_count to skip activity lookup"),
    )

    posts = scraper._scrape_via_graphql(  # noqa: SLF001
        ThreadsScrapeConfig(
            username="bravotv",
            date_start=datetime(2025, 8, 1, tzinfo=UTC),
            date_end=datetime(2025, 8, 31, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
        ),
        page_html="<html></html>",
        profile_url="https://www.threads.com/@bravotv",
    )

    assert len(posts or []) == 1
    assert scraper.runtime_metadata["transport"] == "graphql"
    assert scraper.runtime_metadata["fallback_chain"] == ["graphql"]
    assert scraper.runtime_metadata["complete"] is True


def test_threads_scrape_uses_anonymous_graphql_after_authenticated_profile_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "stale", "csrftoken": "token"})
    tokens = _PageTokens(fb_dtsg="fb-dtsg", lsd="lsd", jazoest="26474", user_id="123")
    captured: dict[str, Any] = {}

    def _raise_retry(*_args: Any, **_kwargs: Any) -> str:
        raise requests.exceptions.RetryError("500 error responses")

    monkeypatch.setattr(scraper, "_fetch_html", _raise_retry)
    monkeypatch.setattr(scraper, "_fetch_html_with_cookies", lambda *_args, **_kwargs: "<html></html>")
    monkeypatch.setattr(scraper, "_extract_page_tokens", lambda _html: tokens)

    def _fake_fetch_profile_posts_page(**kwargs: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        captured["cookies_override"] = kwargs.get("cookies_override")
        return ([_graphql_edge(impression_count=120)], None, False)

    monkeypatch.setattr(scraper, "_fetch_profile_posts_page", _fake_fetch_profile_posts_page)
    monkeypatch.setattr(
        scraper,
        "_fetch_post_view_count",
        lambda **_kwargs: pytest.fail("expected feed impression_count to skip activity lookup"),
    )

    posts = scraper.scrape(ThreadsScrapeConfig(username="bravotv", delay_seconds=0, max_pages=None))

    assert len(posts) == 1
    assert captured["cookies_override"] == {}
    assert scraper.last_retrieval_meta["profile_fetch_mode"] == "anonymous_fallback"
    assert scraper.runtime_metadata["fallback_chain"] == [
        "authenticated_profile_fetch",
        "anonymous_profile_fetch",
        "graphql",
    ]


def test_threads_scrape_post_populates_views_from_activity_query(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    html_payload = (
        '{"DTSGInitialData":{"token":"fb-dtsg-token"}}{"LSD":{"token":"lsd-token"}}{"post_id":"3699063449418169480"}'
    )

    monkeypatch.setattr(scraper, "_fetch_html", lambda *_args, **_kwargs: html_payload)
    monkeypatch.setattr(
        scraper,
        "_build_post_from_html",
        lambda **_kwargs: ThreadsPost(
            post_id="DNVtpvYM3yI",
            username="bravotv",
            text="Your prayers have been answered. #RHOSLC is back",
            media_urls=[],
            thumbnail_url=None,
            likes=186,
            replies=6,
            reposts=11,
            quotes=2,
            views=0,
            posted_at=None,
            url="https://www.threads.com/@bravotv/post/DNVtpvYM3yI",
            raw_data={},
        ),
    )

    captured: dict[str, Any] = {}

    def _fake_fetch_post_view_count(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 6829

    monkeypatch.setattr(scraper, "_fetch_post_view_count", _fake_fetch_post_view_count)

    post, comments = scraper.scrape_post(
        "https://www.threads.com/@bravotv/post/DNVtpvYM3yI",
        delay_seconds=0,
        fetch_comment_list=False,
    )

    assert post is not None
    assert post.views == 6829
    assert comments == []
    assert captured["post_pk"] == "3699063449418169480"
    assert post.raw_data.get("text_post_app_info", {}).get("impression_count") == 6829


def test_threads_scrape_retries_profile_fetch_without_cookies_on_authenticated_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    calls: list[dict[str, str] | None] = []

    def _fake_fetch_html_with_cookies(
        _url: str,
        *,
        delay_seconds: float,
        referer: str | None = None,
        document: bool = False,
        cookies_override: dict[str, str] | None = None,
        **_kw: object,
    ) -> str:
        del delay_seconds, referer, document
        calls.append(cookies_override)
        if cookies_override is None:
            response = requests.Response()
            response.status_code = 404
            raise requests.HTTPError(response=response)
        return "<html></html>"

    monkeypatch.setattr(scraper, "_fetch_html_with_cookies", _fake_fetch_html_with_cookies)
    monkeypatch.setattr(scraper, "_scrape_via_graphql", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_scrape_via_fallback", lambda *_args, **_kwargs: [])

    posts = scraper.scrape(
        ThreadsScrapeConfig(
            username="bravotv",
            date_start=datetime(2026, 1, 1, tzinfo=UTC),
            date_end=datetime(2026, 3, 8, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
        )
    )

    assert posts == []
    assert calls == [None, {}]
    assert scraper.last_retrieval_meta["profile_fetch_mode"] == "anonymous_fallback"
    assert scraper.last_retrieval_meta["authenticated_profile_fetch_error"]["error_class"] == "HTTPError"


def test_threads_scrape_retries_profile_fetch_without_cookies_on_authenticated_retry_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "stale-session", "csrftoken": "token"})
    calls: list[dict[str, str] | None] = []

    def _fake_fetch_html_with_cookies(
        _url: str,
        *,
        delay_seconds: float,
        referer: str | None = None,
        document: bool = False,
        cookies_override: dict[str, str] | None = None,
        **_kw: object,
    ) -> str:
        del delay_seconds, referer, document
        calls.append(cookies_override)
        if cookies_override is None:
            raise requests.exceptions.RetryError("too many 500 error responses")
        return "<html></html>"

    monkeypatch.setattr(scraper, "_fetch_html_with_cookies", _fake_fetch_html_with_cookies)
    monkeypatch.setattr(scraper, "_scrape_via_graphql", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_scrape_via_fallback", lambda *_args, **_kwargs: [])

    posts = scraper.scrape(ThreadsScrapeConfig(username="bravotv", delay_seconds=0, max_pages=1))

    assert posts == []
    assert calls == [None, {}]
    assert scraper.last_retrieval_meta["profile_fetch_mode"] == "anonymous_fallback"
    assert scraper.last_retrieval_meta["authenticated_profile_fetch_error"]["error_class"] == "RetryError"


def test_threads_anonymous_profile_fallback_fetches_candidate_without_cookies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "stale-session", "csrftoken": "token"})
    calls: list[tuple[str, dict[str, str] | None]] = []

    def _fake_fetch_html_with_cookies(
        url: str,
        *,
        delay_seconds: float,
        referer: str | None = None,
        document: bool = False,
        cookies_override: dict[str, str] | None = None,
        **_kw: object,
    ) -> str:
        del delay_seconds, referer, document
        calls.append((url, cookies_override))
        if cookies_override is None:
            raise requests.exceptions.RetryError("too many 500 error responses")
        return "<html></html>"

    monkeypatch.setattr(scraper, "_fetch_html_with_cookies", _fake_fetch_html_with_cookies)
    monkeypatch.setattr(
        scraper,
        "_extract_post_urls",
        lambda _html: ["https://www.threads.com/@bravotv/post/ABC123"],
    )
    monkeypatch.setattr(
        scraper,
        "_build_post_from_html",
        lambda *, url, html_text, username: ThreadsPost(
            post_id="ABC123",
            username=username,
            text="hello",
            media_urls=[],
            thumbnail_url=None,
            url=url,
        ),
    )

    posts = scraper.scrape(ThreadsScrapeConfig(username="bravotv", delay_seconds=0, max_pages=1))

    assert [post.post_id for post in posts] == ["ABC123"]
    assert calls == [
        ("https://www.threads.com/@bravotv", None),
        ("https://www.threads.com/@bravotv", {}),
        ("https://www.threads.com/@bravotv/post/ABC123", {}),
    ]
    assert scraper.last_retrieval_meta["profile_fetch_mode"] == "anonymous_fallback"


def test_threads_playwright_discovery_url_normalization_excludes_nested_media_urls() -> None:
    scraper = ThreadsScraper(cookies={})

    assert (
        scraper._normalize_discovered_profile_post_url(  # noqa: SLF001
            "/@bravotv/post/DYPivIQmlGc?x=1",
            username="BravoTV",
        )
        == "https://www.threads.com/@bravotv/post/DYPivIQmlGc"
    )
    assert (
        scraper._normalize_discovered_profile_post_url(  # noqa: SLF001
            "https://www.threads.com/@bravotv/post/DYPivIQmlGc/media",
            username="bravotv",
        )
        is None
    )
    assert (
        scraper._normalize_discovered_profile_post_url(  # noqa: SLF001
            "https://www.threads.com/@other/post/DYPivIQmlGc",
            username="bravotv",
        )
        is None
    )


def test_threads_playwright_discovery_cookie_records_use_threads_domain() -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "session", "csrftoken": "csrf", "empty": ""})

    records = scraper._playwright_cookie_records()  # noqa: SLF001

    assert records == [
        {
            "name": "sessionid",
            "value": "session",
            "domain": ".threads.com",
            "path": "/",
            "secure": True,
        },
        {
            "name": "csrftoken",
            "value": "csrf",
            "domain": ".threads.com",
            "path": "/",
            "secure": True,
        },
    ]


def test_threads_playwright_discovery_can_use_anonymous_cookie_override() -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "stale-session", "csrftoken": "csrf"})

    assert scraper._playwright_cookie_records({}) == []  # noqa: SLF001


def test_threads_playwright_proxy_config_parses_authenticated_proxy_url() -> None:
    scraper = ThreadsScraper(proxy_url="http://user%40mail.test:p%3Ass@proxy.test:8080")

    assert scraper._playwright_proxy_config() == {  # noqa: SLF001
        "server": "http://proxy.test:8080",
        "username": "user@mail.test",
        "password": "p:ss",
    }


def test_threads_playwright_launch_options_include_modal_flags_and_browser_proxy() -> None:
    scraper = ThreadsScraper(proxy_url="http://user:pass@proxy.test:8080")

    options = scraper._playwright_launch_options()  # noqa: SLF001

    assert options["headless"] is True
    assert "--no-sandbox" in options["args"]
    assert "--disable-dev-shm-usage" in options["args"]
    assert "--disable-blink-features=AutomationControlled" in options["args"]
    assert options["proxy"] == {
        "server": "http://proxy.test:8080",
        "username": "user",
        "password": "pass",
    }


def test_threads_playwright_unavailable_records_diagnostic_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    scraper = ThreadsScraper()
    real_import = builtins.__import__

    def _fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "playwright.sync_api":
            raise ModuleNotFoundError("no playwright")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    rows = scraper._discover_posts_with_playwright(  # noqa: SLF001
        username="bravotv",
        profile_url="https://www.threads.com/@bravotv",
        delay_seconds=0,
    )

    assert rows == []
    assert scraper._last_playwright_discovery_meta["playwright_unavailable"] is True  # noqa: SLF001
    assert scraper._last_playwright_discovery_meta["graphql_response_pages"] == 0  # noqa: SLF001


def test_threads_extract_post_urls_reads_relative_profile_links() -> None:
    scraper = ThreadsScraper(cookies={})

    urls = scraper._extract_post_urls(  # noqa: SLF001
        '<a href="/@bravotv/post/DW1vWqrEtlr">one</a>'
        '<a href="https://www.threads.com/@bravotv/post/DYPivIQmlGc">two</a>'
        '<a href="/@bravotv/post/DW1vWqrEtlr">duplicate</a>'
    )

    assert urls == [
        "https://www.threads.com/@bravotv/post/DW1vWqrEtlr",
        "https://www.threads.com/@bravotv/post/DYPivIQmlGc",
    ]


def test_threads_fallback_passes_anonymous_cookie_override_to_playwright(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "stale-session", "csrftoken": "csrf"})
    captured: dict[str, Any] = {}

    monkeypatch.setattr(scraper, "_extract_post_urls", lambda _html: [])

    def _fake_discover_posts_with_playwright(**kwargs: Any) -> list[dict[str, str]]:
        captured["cookies_override"] = kwargs.get("cookies_override")
        return [{"url": "https://www.threads.com/@bravotv/post/DW1vWqrEtlr", "preview": "preview text"}]

    monkeypatch.setattr(scraper, "_discover_posts_with_playwright", _fake_discover_posts_with_playwright)
    monkeypatch.setattr(
        scraper,
        "_fetch_html_with_cookies",
        lambda *_args, **_kwargs: "<html></html>",
    )
    monkeypatch.setattr(
        scraper,
        "_build_post_from_html",
        lambda *, url, html_text, username: ThreadsPost(
            post_id="DW1vWqrEtlr",
            username=username,
            text="",
            media_urls=[],
            thumbnail_url=None,
            url=url,
        ),
    )

    posts = scraper._scrape_via_fallback(  # noqa: SLF001
        ThreadsScrapeConfig(username="bravotv", delay_seconds=0, max_pages=1),
        page_html="<html></html>",
        profile_url="https://www.threads.com/@bravotv",
        cookies_override={},
    )

    assert captured["cookies_override"] == {}
    assert [post.post_id for post in posts] == ["DW1vWqrEtlr"]


def test_threads_scrape_retries_anonymous_when_authenticated_profile_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"sessionid": "stale-session", "csrftoken": "csrf"})
    fallback_cookie_overrides: list[dict[str, str] | None] = []
    anonymous_fetch_cookie_overrides: list[dict[str, str] | None] = []

    monkeypatch.setattr(scraper, "_fetch_html", lambda *_args, **_kwargs: "<html>auth-empty</html>")
    monkeypatch.setattr(scraper, "_scrape_via_graphql", lambda *_args, **_kwargs: None)

    def _fake_fetch_html_with_cookies(*_args: Any, **kwargs: Any) -> str:
        anonymous_fetch_cookie_overrides.append(kwargs.get("cookies_override"))
        return "<html>anonymous-profile</html>"

    def _fake_fallback(*_args: Any, **kwargs: Any) -> list[ThreadsPost]:
        fallback_cookie_overrides.append(kwargs.get("cookies_override"))
        if len(fallback_cookie_overrides) == 1:
            scraper.last_retrieval_meta = {"candidate_urls_found": 0}
            return []
        scraper.last_retrieval_meta = {"candidate_urls_found": 1}
        return [
            ThreadsPost(
                post_id="DYNAMIC2",
                username="bravotv",
                text="anonymous post",
                media_urls=[],
                thumbnail_url=None,
                url="https://www.threads.com/@bravotv/post/DYNAMIC2",
            )
        ]

    monkeypatch.setattr(scraper, "_fetch_html_with_cookies", _fake_fetch_html_with_cookies)
    monkeypatch.setattr(scraper, "_scrape_via_fallback", _fake_fallback)

    posts = scraper.scrape(ThreadsScrapeConfig(username="bravotv", delay_seconds=0, max_pages=None))

    assert [post.post_id for post in posts] == ["DYNAMIC2"]
    assert fallback_cookie_overrides == [None, {}]
    assert anonymous_fetch_cookie_overrides == [{}]
    assert scraper.last_retrieval_meta["profile_fetch_mode"] == "anonymous_empty_authenticated_fallback"
    assert scraper.last_retrieval_meta["authenticated_empty_profile_retry"] is True


def test_threads_fallback_extends_static_profile_links_with_playwright(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={})
    called = {"playwright": False}

    monkeypatch.setattr(
        scraper,
        "_extract_post_urls",
        lambda _html: ["https://www.threads.com/@bravotv/post/STATIC1"],
    )

    def _fake_discover_posts_with_playwright(**_kwargs: Any) -> list[dict[str, str]]:
        called["playwright"] = True
        scraper._last_playwright_discovery_meta = {  # noqa: SLF001
            "graphql_response_pages": 1,
            "graphql_has_next_page": False,
        }
        return [
            {"url": "https://www.threads.com/@bravotv/post/STATIC1", "preview": "duplicate"},
            {"url": "https://www.threads.com/@bravotv/post/DYNAMIC2", "preview": "dynamic"},
        ]

    monkeypatch.setattr(scraper, "_discover_posts_with_playwright", _fake_discover_posts_with_playwright)
    monkeypatch.setattr(
        scraper,
        "_fetch_html_with_cookies",
        lambda *_args, **_kwargs: "<html></html>",
    )
    monkeypatch.setattr(
        scraper,
        "_build_post_from_html",
        lambda *, url, html_text, username: ThreadsPost(
            post_id=url.rsplit("/", 1)[-1],
            username=username,
            text="",
            media_urls=[],
            thumbnail_url=None,
            url=url,
        ),
    )

    posts = scraper._scrape_via_fallback(  # noqa: SLF001
        ThreadsScrapeConfig(username="bravotv", delay_seconds=0, max_pages=2),
        page_html="<html></html>",
        profile_url="https://www.threads.com/@bravotv",
    )

    assert called["playwright"] is True
    assert [post.post_id for post in posts] == ["STATIC1", "DYNAMIC2"]
    assert scraper.last_retrieval_meta["candidate_urls_found"] == 2


def test_threads_full_history_fallback_escalates_static_profile_links_to_playwright(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={})
    called = {"playwright": False}

    monkeypatch.setattr(
        scraper,
        "_extract_post_urls",
        lambda _html: ["https://www.threads.com/@bravotv/post/STATIC1"],
    )

    def _fake_discover_posts_with_playwright(**_kwargs: Any) -> list[dict[str, str]]:
        called["playwright"] = True
        scraper._last_playwright_discovery_meta = {  # noqa: SLF001
            "graphql_response_pages": 1,
            "graphql_has_next_page": False,
        }
        return [
            {"url": "https://www.threads.com/@bravotv/post/STATIC1", "preview": "duplicate"},
            {"url": "https://www.threads.com/@bravotv/post/DYNAMIC2", "preview": "dynamic"},
        ]

    monkeypatch.setattr(scraper, "_discover_posts_with_playwright", _fake_discover_posts_with_playwright)
    monkeypatch.setattr(
        scraper,
        "_fetch_html_with_cookies",
        lambda *_args, **_kwargs: "<html></html>",
    )
    monkeypatch.setattr(
        scraper,
        "_build_post_from_html",
        lambda *, url, html_text, username: ThreadsPost(
            post_id=url.rsplit("/", 1)[-1],
            username=username,
            text="",
            media_urls=[],
            thumbnail_url=None,
            url=url,
        ),
    )

    posts = scraper._scrape_via_fallback(  # noqa: SLF001
        ThreadsScrapeConfig(username="bravotv", delay_seconds=0, max_pages=None),
        page_html="<html></html>",
        profile_url="https://www.threads.com/@bravotv",
    )

    assert called["playwright"] is True
    assert [post.post_id for post in posts] == ["STATIC1", "DYNAMIC2"]
    assert scraper.last_retrieval_meta["source"] == "playwright_profile_discovery"
    assert scraper.last_retrieval_meta["profile_discovery_complete"] is True


def test_threads_scrape_via_graphql_marks_page_fetch_failure_as_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    tokens = _PageTokens(fb_dtsg="fb-dtsg", lsd="lsd", jazoest="26474", user_id="123")
    calls = {"count": 0}

    def _fake_fetch_profile_posts_page(**_kwargs: Any):
        calls["count"] += 1
        if calls["count"] == 1:
            return ([_graphql_edge(impression_count=120)], "cursor-2", True)
        raise RuntimeError("boom")

    monkeypatch.setattr(scraper, "_extract_page_tokens", lambda _html: tokens)
    monkeypatch.setattr(scraper, "_fetch_profile_posts_page", _fake_fetch_profile_posts_page)

    posts = scraper._scrape_via_graphql(  # noqa: SLF001
        ThreadsScrapeConfig(
            username="bravotv",
            date_start=datetime(2025, 8, 1, tzinfo=UTC),
            date_end=datetime(2025, 8, 31, tzinfo=UTC),
            delay_seconds=0,
            max_pages=3,
        ),
        page_html="<html></html>",
        profile_url="https://www.threads.com/@bravotv",
    )

    assert posts is not None
    assert len(posts) == 1
    assert scraper.last_retrieval_meta["error_code"] == "threads_graphql_page_fetch_failed"
    assert scraper.last_retrieval_meta["retryable"] is True
    assert scraper.last_retrieval_meta["posts_checked"] == 1
    assert scraper.last_retrieval_meta["last_cursor"] == "cursor-2"
    assert scraper.last_retrieval_meta["stop_reason"] == "page_fetch_failed"


def test_build_post_from_html_uses_deterministic_fallback_post_id() -> None:
    scraper = ThreadsScraper(cookies={"csrftoken": "token"})
    url = "https://www.threads.com/@bravotv"
    html = "<html><head><meta property='og:title' content='Fallback'></head></html>"

    first = scraper._build_post_from_html(url=url, html_text=html, username="bravotv")  # noqa: SLF001
    second = scraper._build_post_from_html(url=url, html_text=html, username="bravotv")  # noqa: SLF001

    assert first.post_id == second.post_id
    assert first.post_id.startswith("bravotv-")
