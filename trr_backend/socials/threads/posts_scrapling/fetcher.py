from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from trr_backend.socials._scrapling_http_utils import (
    env_truthy,
    response_text,
    status_code,
    transport_failure_reason,
)
from trr_backend.socials.scrapling_transport import (
    build_stealthy_fetcher,
    merge_response_cookies,
    resolve_scrapling_fetcher_options,
    safe_cookie_metadata,
    safe_scrapling_proxy_metadata,
    scrapling_fetcher_metadata,
    scrapling_runtime_metadata,
)
from trr_backend.socials.threads.scraper import ThreadsPost, ThreadsScrapeConfig, ThreadsScraper

from .proxy import ThreadsPostsProxyConfig

logger = logging.getLogger("socials.threads.posts_scrapling.fetcher")
_THREADS_SCRAPLING_OPTION_KEYS = frozenset(
    {
        "additional_args",
        "ai_targeted",
        "allow_webgl",
        "block_ads",
        "block_webrtc",
        "blocked_domains",
        "dns_over_https",
        "google_search",
        "hide_canvas",
        "init_script",
        "real_chrome",
        "selector_config",
        "solve_cloudflare",
        "useragent",
        "wait_selector",
        "wait_selector_state",
    }
)


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
        self._cookies = list(cookies or [])
        self._raw_cookies = dict(raw_cookies or {})
        self._proxy_config = proxy_config
        self._proxy_rotator = proxy_config.proxy_rotator if proxy_config else None
        self._headless = headless if headless is not None else env_truthy("SOCIAL_THREADS_POSTS_HEADLESS", True)
        self._timeout_ms = max(5_000, int(timeout_ms or 45_000))
        self._fast_mode = bool(fast_mode)
        self._scraper = ThreadsScraper(
            cookies=self._raw_cookies,
            proxy_url=proxy_config.api_proxy_url if proxy_config else None,
        )
        self._fetcher: Any | None = None
        self._warmed_profile_html: str | None = None
        self._profile_url: str | None = None
        self._last_transport = "not_started"
        self._fallback_chain: list[str] = []
        self._last_stop_reason: str | None = None
        self._last_retryable = False
        self._last_complete = False
        self._warmup_cookie_delta: dict[str, str] = {}
        self._request_count = 0
        self._scrapling_used = False
        self._scrapling_warmup_failed_reason: str | None = None
        self._selected_proxy_fingerprint = proxy_config.fingerprint if proxy_config else "none"
        self._scrapling_runtime_metadata = scrapling_runtime_metadata()
        self._scrapling_fetcher_options = resolve_scrapling_fetcher_options(
            "SOCIAL_THREADS_POSTS_SCRAPLING",
            allowed_keys=_THREADS_SCRAPLING_OPTION_KEYS,
        )
        self._scrapling_fetcher_metadata = scrapling_fetcher_metadata(
            "StealthyFetcher",
            self._scrapling_fetcher_options.metadata,
            safe_scrapling_proxy_metadata(),
        )

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        retrieval_metadata = dict(getattr(self._scraper, "last_retrieval_meta", {}) or {})
        cookie_metadata = safe_cookie_metadata(
            self._raw_cookies,
            self._warmup_cookie_delta,
            prefix="",
        )
        return {
            "scrapling_runtime": dict(self._scrapling_runtime_metadata),
            **self._scrapling_fetcher_metadata,
            "request_count": self._request_count + int(getattr(self._scraper, "_request_count", 0) or 0),
            "transport": str(self._last_transport or "not_started"),
            "fallback_chain": list(self._fallback_chain),
            "stop_reason": self._last_stop_reason,
            "retryable": bool(self._last_retryable),
            "complete": bool(self._last_complete),
            "pages_fetched": int(retrieval_metadata.get("pages_scanned") or 0),
            "scrapling_used": bool(self._scrapling_used),
            "scrapling_warmup_failed_reason": self._scrapling_warmup_failed_reason,
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
            **cookie_metadata,
        }

    async def warmup(self, username: str) -> None:
        normalized_username = str(username or "").strip().lstrip("@")
        if not normalized_username:
            raise RuntimeError("threads_posts_username_missing")

        self._profile_url = f"{self._scraper.BASE_URL}/@{quote(normalized_username, safe='')}"

        try:
            response = await self._fetch_page(self._profile_url)
        except Exception as exc:  # noqa: BLE001
            self._last_transport = "scrapling_warmup" if self._scrapling_used else "legacy_threads_scraper"
            self._fallback_chain = ["scrapling_warmup"] if self._scrapling_used else []
            self._scrapling_warmup_failed_reason = transport_failure_reason(exc)
            self._last_stop_reason = self._scrapling_warmup_failed_reason
            self._last_retryable = True
            self._last_complete = False
            logger.warning(
                "threads_posts_scrapling_warmup_failed account=%s reason=%s",
                normalized_username,
                self._scrapling_warmup_failed_reason,
            )
            return

        self._merge_warmup_cookies(response)
        page_html = response_text(response)
        response_status = status_code(response)
        self._last_transport = "scrapling_warmup"
        self._fallback_chain = ["scrapling_warmup"]

        if response_status >= 400:
            self._scrapling_warmup_failed_reason = f"http_{response_status}"
            self._last_stop_reason = self._scrapling_warmup_failed_reason
            self._last_retryable = response_status in {408, 425, 429, 500, 502, 503, 504}
            self._last_complete = False
            return

        if _looks_auth_failed(page_html):
            self._scrapling_warmup_failed_reason = "auth_failed"
            self._last_stop_reason = "auth_failed"
            self._last_retryable = False
            self._last_complete = False
            return

        if not page_html.strip():
            self._scrapling_warmup_failed_reason = "empty_response"
            self._last_stop_reason = "empty_response"
            self._last_retryable = True
            self._last_complete = False
            return

        self._warmed_profile_html = page_html
        self._scrapling_warmup_failed_reason = None
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

        fallback_reason = self._scrapling_warmup_failed_reason or (
            "graphql_bootstrap_failed" if self._warmed_profile_html else "scrapling_warmup_failed"
        )
        return await self._fetch_with_legacy_scraper(config, reason=fallback_reason)

    async def aclose(self) -> None:
        close = getattr(self._fetcher, "close", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result
        return None

    async def _fetch_page(self, url: str) -> Any:
        if self._fetcher is None:
            self._fetcher = build_stealthy_fetcher()
        self._scrapling_used = True
        self._request_count += 1
        return await self._fetcher.async_fetch(
            url,
            **self._scrapling_fetcher_options.kwargs,
            headless=self._headless,
            network_idle=False,
            load_dom=True,
            disable_resources=False,
            cookies=self._cookies,
            proxy_rotator=self._proxy_rotator,
            extra_headers=self._scraper._headers(document=True),  # noqa: SLF001
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
        )

    def _merge_warmup_cookies(self, response: Any) -> None:
        before = dict(self._raw_cookies)
        merged = merge_response_cookies(before, response)
        self._warmup_cookie_delta = {
            name: value for name, value in merged.items() if str(before.get(name) or "") != str(value or "")
        }
        self._raw_cookies.clear()
        self._raw_cookies.update(merged)
        self._scraper.cookies = self._raw_cookies

    async def _fetch_with_legacy_scraper(
        self,
        config: ThreadsScrapeConfig,
        *,
        reason: str,
    ) -> ThreadsPostsFetchResult:
        def _scrape() -> list[ThreadsPost]:
            return self._scraper.scrape(config)

        try:
            posts = await asyncio.to_thread(_scrape)
        except Exception:  # noqa: BLE001
            self._last_transport = "legacy_threads_scraper"
            self._fallback_chain = self._fallback_prefix() + ["legacy_threads_scraper"]
            self._last_stop_reason = str(reason or "legacy_threads_scraper_failed")
            self._last_retryable = True
            self._last_complete = False
            logger.warning(
                "threads_posts_legacy_scraper_failed account=%s reason=%s",
                config.normalized_username,
                reason,
                exc_info=True,
            )
            return ThreadsPostsFetchResult(
                posts=[],
                fetch_failed=True,
                auth_failed=bool(self._raw_cookies),
                retryable=True,
                fetch_reason=str(reason or "legacy_threads_scraper_failed"),
            )

        retrieval_meta = dict(getattr(self._scraper, "last_retrieval_meta", {}) or {})
        runtime_meta = dict(getattr(self._scraper, "runtime_metadata", {}) or {})
        self._last_transport = "legacy_threads_scraper"
        self._fallback_chain = self._fallback_prefix() + ["legacy_threads_scraper"]
        self._last_stop_reason = str(runtime_meta.get("stop_reason") or retrieval_meta.get("stop_reason") or reason)
        self._last_retryable = bool(runtime_meta.get("retryable") or retrieval_meta.get("retryable"))
        self._last_complete = bool(runtime_meta.get("complete", True))
        return ThreadsPostsFetchResult(
            posts=list(posts or []),
            fetch_failed=False,
            auth_failed=False,
            retryable=self._last_retryable,
            fetch_reason=str(reason or "").strip() or None,
        )

    def _fallback_prefix(self) -> list[str]:
        return ["scrapling_warmup"] if self._scrapling_used else []
