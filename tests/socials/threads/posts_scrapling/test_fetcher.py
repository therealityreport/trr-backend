from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from trr_backend.socials.threads.posts_scrapling import fetcher as fetcher_module
from trr_backend.socials.threads.posts_scrapling.fetcher import ThreadsPostsScraplingFetcher
from trr_backend.socials.threads.scraper import ThreadsPost, ThreadsScrapeConfig


class _FakeScraplingFetcher:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    async def async_fetch(self, url: str, **kwargs: Any) -> Any:
        self.calls.append({"url": url, **kwargs})
        return self.response


def _post(post_id: str = "post-1") -> ThreadsPost:
    return ThreadsPost(
        post_id=post_id,
        username="trr",
        text="hello",
        media_urls=[],
        thumbnail_url=None,
        url=f"https://www.threads.com/@trr/post/{post_id}",
    )


def test_threads_fetcher_runtime_metadata_includes_final_request_count() -> None:
    fetcher = ThreadsPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
        proxy_config=None,
    )
    fetcher._scraper._request_count = 4  # noqa: SLF001
    fetcher._last_transport = "httpx_after_browser_warmup"  # noqa: SLF001
    fetcher._fallback_chain = ["scrapling_warmup", "graphql_profile_posts"]  # noqa: SLF001
    fetcher._last_stop_reason = "complete"  # noqa: SLF001
    fetcher._last_retryable = False  # noqa: SLF001
    fetcher._last_complete = True  # noqa: SLF001

    assert fetcher.runtime_metadata["request_count"] == 4
    assert fetcher.runtime_metadata["fallback_chain"] == ["scrapling_warmup", "graphql_profile_posts"]
    assert fetcher.runtime_metadata["complete"] is True


def test_threads_fetcher_runtime_metadata_does_not_claim_scrapling_before_use() -> None:
    fetcher = ThreadsPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
        proxy_config=None,
    )

    metadata = fetcher.runtime_metadata

    assert metadata["transport"] == "not_started"
    assert metadata["fallback_chain"] == []
    assert metadata["scrapling_used"] is False


def test_threads_fetcher_runtime_metadata_includes_scrapling_versions(monkeypatch) -> None:
    monkeypatch.setattr(
        fetcher_module,
        "scrapling_runtime_metadata",
        lambda: {
            "scrapling_version": "0.4.9",
            "patchright_version": "1.60.1",
            "playwright_version": "1.60.0",
        },
    )

    fetcher = ThreadsPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
        proxy_config=None,
    )

    assert fetcher.runtime_metadata["scrapling_runtime"] == {
        "scrapling_version": "0.4.9",
        "patchright_version": "1.60.1",
        "playwright_version": "1.60.0",
    }


def test_threads_fetcher_uses_scrapling_warmup_then_graphql(monkeypatch) -> None:
    response = SimpleNamespace(
        status_code=200,
        text="<html>rendered profile</html>",
        cookies={"fresh_cookie": "secret-value"},
    )
    fake_fetcher = _FakeScraplingFetcher(response)
    monkeypatch.setattr(fetcher_module, "build_stealthy_fetcher", lambda: fake_fetcher)

    fetcher = ThreadsPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
        proxy_config=None,
    )
    post = _post()
    monkeypatch.setattr(
        fetcher._scraper,  # noqa: SLF001
        "_extract_page_tokens",
        lambda _html: SimpleNamespace(user_id="123"),
    )
    monkeypatch.setattr(
        fetcher._scraper,  # noqa: SLF001
        "_scrape_via_graphql",
        lambda *_args, **_kwargs: [post],
    )

    result = asyncio.run(fetcher.fetch_posts("trr", max_pages=1))
    metadata = fetcher.runtime_metadata

    assert result.posts == [post]
    assert result.fetch_failed is False
    assert fake_fetcher.calls
    assert fake_fetcher.calls[0]["url"] == "https://www.threads.com/@trr"
    assert fake_fetcher.calls[0]["load_dom"] is True
    assert metadata["transport"] == "graphql_profile_posts"
    assert metadata["fallback_chain"] == ["scrapling_warmup", "graphql_profile_posts"]
    assert metadata["warmup_cookie_names"] == ["fresh_cookie"]
    assert metadata["warmup_cookie_count"] == 1
    assert "secret-value" not in repr(metadata)


def test_threads_fetcher_falls_back_to_threads_scraper_when_bootstrap_tokens_missing(monkeypatch) -> None:
    response = SimpleNamespace(status_code=200, text="<html>no tokens</html>", cookies={})
    fake_fetcher = _FakeScraplingFetcher(response)
    monkeypatch.setattr(fetcher_module, "build_stealthy_fetcher", lambda: fake_fetcher)

    fetcher = ThreadsPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
        proxy_config=None,
    )
    post = _post("legacy-1")
    monkeypatch.setattr(fetcher._scraper, "_extract_page_tokens", lambda _html: None)  # noqa: SLF001

    def _legacy_scrape(_config: Any) -> list[ThreadsPost]:
        fetcher._scraper._request_count = 2  # noqa: SLF001
        fetcher._scraper._set_runtime_state(  # noqa: SLF001
            transport="graphql",
            fallback_chain=["graphql"],
            stop_reason="complete",
            retryable=False,
            complete=True,
        )
        return [post]

    monkeypatch.setattr(fetcher._scraper, "scrape", _legacy_scrape)  # noqa: SLF001

    result = asyncio.run(fetcher.fetch_posts("trr", max_pages=1))
    metadata = fetcher.runtime_metadata

    assert result.posts == [post]
    assert result.fetch_failed is False
    assert metadata["transport"] == "legacy_threads_scraper"
    assert metadata["fallback_chain"] == ["scrapling_warmup", "legacy_threads_scraper"]
    assert metadata["request_count"] == 3
    assert metadata["stop_reason"] == "complete"


def test_threads_fetcher_legacy_scraper_transport_error_is_not_auth_failed(monkeypatch) -> None:
    fetcher = ThreadsPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
        proxy_config=None,
    )

    def _raise_transport_error(_config: ThreadsScrapeConfig) -> list[ThreadsPost]:
        raise ConnectionError("connection reset")

    monkeypatch.setattr(fetcher._scraper, "scrape", _raise_transport_error)  # noqa: SLF001

    result = asyncio.run(
        fetcher._fetch_with_legacy_scraper(  # noqa: SLF001
            ThreadsScrapeConfig(username="trr", delay_seconds=0, max_pages=1),
            reason="legacy_threads_scraper_failed",
        )
    )

    assert result.posts == []
    assert result.auth_failed is False
    assert result.fetch_failed is True
    assert result.retryable is True


def test_threads_fetcher_fallback_does_not_claim_scrapling_when_builder_fails(monkeypatch) -> None:
    def _raise_missing_scrapling() -> Any:
        raise RuntimeError("Scrapling fetchers are unavailable")

    monkeypatch.setattr(fetcher_module, "build_stealthy_fetcher", _raise_missing_scrapling)
    fetcher = ThreadsPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
        proxy_config=None,
    )
    post = _post("legacy-2")

    monkeypatch.setattr(fetcher._scraper, "scrape", lambda _config: [post])  # noqa: SLF001

    result = asyncio.run(fetcher.fetch_posts("trr", max_pages=1))
    metadata = fetcher.runtime_metadata

    assert result.posts == [post]
    assert metadata["scrapling_used"] is False
    assert metadata["fallback_chain"] == ["legacy_threads_scraper"]
    assert "scrapling_warmup" not in metadata["fallback_chain"]
