from __future__ import annotations

from trr_backend.socials.threads.posts_scrapling.fetcher import ThreadsPostsScraplingFetcher


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
