from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
import requests

from trr_backend.socials.threads.scraper import (
    _THREADS_POST_VIEW_COUNT_DOC_ID,
    ThreadsPost,
    ThreadsScrapeConfig,
    ThreadsScraper,
    _PageTokens,
)


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
