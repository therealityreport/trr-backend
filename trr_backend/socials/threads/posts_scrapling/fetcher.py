from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from trr_backend.socials.threads.scraper import ThreadsPost, ThreadsScrapeConfig, ThreadsScraper

from .proxy import ThreadsPostsProxyConfig

logger = logging.getLogger("socials.threads.posts_scrapling.fetcher")


@dataclass(slots=True)
class ThreadsPostsFetchResult:
    posts: list[ThreadsPost] = field(default_factory=list)
    fetch_failed: bool = False
    auth_failed: bool = False
    retryable: bool = False
    fetch_reason: str | None = None


def _looks_auth_failed(page_html: str) -> bool:
    normalized = str(page_html or "").strip().lower()
    return any(token in normalized for token in ("login", "checkpoint", "challenge"))


class ThreadsPostsScraplingFetcher:
    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        proxy_config: ThreadsPostsProxyConfig | None = None,
        headless: bool | None = None,
        timeout_ms: int = 45_000,
        fast_mode: bool = False,
    ) -> None:
        del cookies, headless, timeout_ms

        self._raw_cookies = dict(raw_cookies or {})
        self._proxy_config = proxy_config
        self._fast_mode = bool(fast_mode)
        self._scraper = ThreadsScraper(cookies=self._raw_cookies)
        self._warmed_profile_html: str | None = None
        self._profile_url: str | None = None
        self._last_transport = "requests"
        self._fallback_chain: list[str] = []
        self._last_stop_reason: str | None = None
        self._last_retryable = False
        self._last_complete = False
        self._warmup_cookie_delta: dict[str, str] = {}
        self._selected_proxy_fingerprint = proxy_config.fingerprint if proxy_config else "none"

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        return {
            "request_count": int(getattr(self._scraper, "_request_count", 0) or 0),
            "transport": str(self._last_transport or "requests"),
            "fallback_chain": list(self._fallback_chain),
            "stop_reason": self._last_stop_reason,
            "retryable": bool(self._last_retryable),
            "complete": bool(self._last_complete),
            "warmup_cookie_delta": dict(self._warmup_cookie_delta),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
        }

    async def warmup(self, username: str) -> None:
        normalized_username = str(username or "").strip().lstrip("@")
        if not normalized_username:
            raise RuntimeError("threads_posts_username_missing")

        self._profile_url = f"{self._scraper.BASE_URL}/@{normalized_username}"

        def _fetch() -> str:
            return self._scraper._fetch_html(  # noqa: SLF001
                self._profile_url,
                delay_seconds=0,
                document=True,
                fast_mode=self._fast_mode,
            )

        page_html = await asyncio.to_thread(_fetch)
        if _looks_auth_failed(page_html):
            self._last_transport = "warmup"
            self._fallback_chain = ["scrapling_warmup"]
            self._last_stop_reason = "auth_failed"
            self._last_retryable = False
            self._last_complete = False
            raise RuntimeError("threads_posts_auth_warmup_failed")

        self._warmed_profile_html = page_html
        self._last_transport = "warmup"
        self._fallback_chain = ["scrapling_warmup"]
        self._last_stop_reason = "warmup_complete"
        self._last_retryable = False
        self._last_complete = False

    async def fetch_posts(
        self,
        username: str,
        *,
        max_pages: int | None = None,
    ) -> ThreadsPostsFetchResult:
        normalized_username = str(username or "").strip().lstrip("@")
        if not normalized_username:
            return ThreadsPostsFetchResult(
                fetch_failed=True,
                auth_failed=False,
                retryable=False,
                fetch_reason="missing_username",
            )
        if not self._warmed_profile_html or not self._profile_url:
            await self.warmup(normalized_username)

        tokens = self._scraper._extract_page_tokens(self._warmed_profile_html or "")  # noqa: SLF001
        config = ThreadsScrapeConfig(
            username=normalized_username,
            delay_seconds=0,
            max_pages=max_pages,
            fast_mode=self._fast_mode,
        )

        if tokens and tokens.user_id:
            def _graphql_fetch() -> list[ThreadsPost] | None:
                return self._scraper._scrape_via_graphql(  # noqa: SLF001
                    config,
                    page_html=self._warmed_profile_html or "",
                    profile_url=self._profile_url or "",
                )

            posts = await asyncio.to_thread(_graphql_fetch)
            retrieval_meta = dict(getattr(self._scraper, "last_retrieval_meta", {}) or {})
            self._last_transport = "graphql_profile_posts"
            self._fallback_chain = ["scrapling_warmup", "graphql_profile_posts"]
            self._last_stop_reason = str(retrieval_meta.get("stop_reason") or "complete")
            self._last_retryable = bool(retrieval_meta.get("retryable"))
            self._last_complete = not self._last_retryable and self._last_stop_reason != "max_posts_reached"
            if posts is not None:
                return ThreadsPostsFetchResult(
                    posts=list(posts),
                    fetch_failed=False,
                    auth_failed=False,
                    retryable=bool(retrieval_meta.get("retryable")),
                    fetch_reason=str(retrieval_meta.get("error_code") or "").strip() or None,
                )

        self._last_transport = "graphql_profile_posts"
        self._fallback_chain = ["scrapling_warmup", "graphql_profile_posts"]
        self._last_stop_reason = "graphql_bootstrap_failed"
        self._last_retryable = False
        self._last_complete = False
        return ThreadsPostsFetchResult(
            posts=[],
            fetch_failed=True,
            auth_failed=bool(self._raw_cookies),
            retryable=False,
            fetch_reason="graphql_bootstrap_failed",
        )

    async def aclose(self) -> None:
        return None
