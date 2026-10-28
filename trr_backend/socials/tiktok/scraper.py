"""
TikTok scraper facade for fetching posts from public profiles.

Supports:
- Fetching posts from any public TikTok profile
- Filtering by hashtags (case-insensitive)
- Filtering by date range
- Fetching comments and replies with like counts

``TikTokScraper`` composes the transports in
``trr_backend.socials.tiktok.transports`` (yt-dlp, direct web API, Playwright
browser intercept) with the payload parsers in
``trr_backend.socials.tiktok.parsing``. This module owns scrape-mode routing,
HTTP client composition, and retrieval/path-health metadata. The data models
and ``build_tiktok_http_client`` are re-exported here for existing importers.
"""

import logging
import os
import subprocess  # noqa: F401 - keeps the ``scraper.subprocess`` monkeypatch path used by yt-dlp tests
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from trr_backend.socials.tiktok.constants import (
    DEFAULT_CURL_CFFI_IMPERSONATE,
    DEFAULT_HTTP_CLIENT,
    ENV_CRAWLEE_PROXY_URLS_TIKTOK,
    ENV_TIKTOK_CURL_CFFI_IMPERSONATE,
    ENV_TIKTOK_HTTP_CLIENT,
    ENV_TIKTOK_PROXY_URLS,
    PRESERVED_RETRIEVAL_META_KEYS,
    TIKTOK_COMMENT_REPLIES_URL,
    TIKTOK_COMMENT_REQUEST_TIMEOUT_SECONDS,
    TIKTOK_COMMENTS_URL,
    TIKTOK_MAX_RETRIES,
    TIKTOK_POST_LIST_URL,
    TIKTOK_REQUEST_TIMEOUT_SECONDS,
    TIKTOK_RETRY_BACKOFF_FACTOR,
    TIKTOK_USER_DETAIL_URL,
    YTDLP_API_FALLBACK_ERROR_CODES,
)
from trr_backend.socials.tiktok.http_client import _TikTokHttpClientBase
from trr_backend.socials.tiktok.http_client import (
    build_tiktok_http_client as _build_tiktok_http_client,
)
from trr_backend.socials.tiktok.models import TikTokComment, TikTokPost, TikTokScrapeConfig
from trr_backend.socials.tiktok.parsing.comments import CommentParsingMixin
from trr_backend.socials.tiktok.parsing.posts import PostParsingMixin
from trr_backend.socials.tiktok.parsing.profile import ProfileParsingMixin
from trr_backend.socials.tiktok.parsing.ytdlp import YtDlpParsingMixin
from trr_backend.socials.tiktok.transports.browser_intercept import BrowserInterceptTransportMixin
from trr_backend.socials.tiktok.transports.web_api import WebApiTransportMixin
from trr_backend.socials.tiktok.transports.ytdlp import YtDlpTransportMixin

logger = logging.getLogger(__name__)


class _DeferredTikTokHttpClient:
    """Lazy wrapper so the default yt-dlp path does not eagerly build direct transports."""

    def __init__(self, owner: "TikTokScraper") -> None:
        self._owner = owner

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return self._owner._ensure_http_client().get(*args, **kwargs)


def build_tiktok_http_client(
    client_name: str | None = None,
    *,
    retry_total: int = 3,
    backoff_factor: float = 1.5,
    proxy_url: str | None = None,
    impersonate: str | None = None,
) -> _TikTokHttpClientBase:
    return _build_tiktok_http_client(
        client_name,
        retry_total=retry_total,
        backoff_factor=backoff_factor,
        proxy_url=proxy_url,
        impersonate=impersonate,
    )


class TikTokScraper(
    WebApiTransportMixin,
    YtDlpTransportMixin,
    BrowserInterceptTransportMixin,
    PostParsingMixin,
    CommentParsingMixin,
    ProfileParsingMixin,
    YtDlpParsingMixin,
):
    """TikTok profile scraper for public profiles."""

    # TikTok web API endpoints
    USER_DETAIL_URL = TIKTOK_USER_DETAIL_URL
    USER_POST_URL = TIKTOK_POST_LIST_URL
    COMMENTS_URL = TIKTOK_COMMENTS_URL
    COMMENT_REPLIES_URL = TIKTOK_COMMENT_REPLIES_URL

    MAX_RETRIES = TIKTOK_MAX_RETRIES
    RETRY_BACKOFF_FACTOR = TIKTOK_RETRY_BACKOFF_FACTOR
    REQUEST_TIMEOUT_SECONDS = TIKTOK_REQUEST_TIMEOUT_SECONDS
    COMMENT_REQUEST_TIMEOUT_SECONDS = TIKTOK_COMMENT_REQUEST_TIMEOUT_SECONDS

    def __init__(
        self,
        cookies: dict | None = None,
        *,
        direct_comment_api_enabled_override: bool | None = None,
        http_client_name: str | None = None,
        proxy_urls: list[str] | tuple[str, ...] | None = None,
    ):
        self.cookies = cookies or {}
        self._direct_comment_api_enabled_override = (
            bool(direct_comment_api_enabled_override) if direct_comment_api_enabled_override is not None else None
        )
        self._http_client_name_input = (
            str(http_client_name or os.getenv(ENV_TIKTOK_HTTP_CLIENT) or "").strip().lower() or DEFAULT_HTTP_CLIENT
        )
        self._curl_cffi_impersonate = (
            str(os.getenv(ENV_TIKTOK_CURL_CFFI_IMPERSONATE) or "").strip() or DEFAULT_CURL_CFFI_IMPERSONATE
        )
        explicit_proxy_urls = [str(value or "").strip() for value in (proxy_urls or []) if str(value or "").strip()]
        env_proxy_urls = [
            value.strip() for value in str(os.getenv(ENV_TIKTOK_PROXY_URLS) or "").split(",") if value.strip()
        ]
        crawlee_proxy_urls = [
            value.strip() for value in str(os.getenv(ENV_CRAWLEE_PROXY_URLS_TIKTOK) or "").split(",") if value.strip()
        ]
        if explicit_proxy_urls:
            self._proxy_urls_input = explicit_proxy_urls
            self._proxy_source = "constructor"
        elif env_proxy_urls:
            self._proxy_urls_input = env_proxy_urls
            self._proxy_source = ENV_TIKTOK_PROXY_URLS
        else:
            self._proxy_urls_input = crawlee_proxy_urls
            self._proxy_source = ENV_CRAWLEE_PROXY_URLS_TIKTOK if crawlee_proxy_urls else None
        self._selected_proxy_url = next(
            (str(value or "").strip() for value in self._proxy_urls_input if str(value or "").strip()),
            None,
        )
        self._direct_http_client: _TikTokHttpClientBase | None = None
        self.session = _DeferredTikTokHttpClient(self)
        self._request_count = 0
        self._consecutive_success = 0
        self.last_retrieval_meta: dict[str, Any] = {
            "http_client": self._http_client_name_input,
            "proxy_enabled": bool(self._selected_proxy_url),
            "proxy_label": self._proxy_label(self._selected_proxy_url),
        }
        if self._proxy_source:
            self.last_retrieval_meta["proxy_source"] = self._proxy_source
        if self._http_client_name_input == "curl_cffi":
            self.last_retrieval_meta["curl_cffi_impersonate"] = self._curl_cffi_impersonate
        self._last_api_fail_reason: str | None = None
        self.last_comment_fetch_reason: str | None = None
        self.last_comment_fetch_meta: dict[str, Any] = {}
        self.comments_auth_failed = False

    @staticmethod
    def _proxy_label(proxy_url: str | None) -> str | None:
        if not proxy_url:
            return None
        parsed = urlparse(str(proxy_url))
        return parsed.hostname or str(proxy_url)

    def _create_session(self) -> _TikTokHttpClientBase:
        """Create a session with retry logic."""
        return build_tiktok_http_client(
            self._http_client_name_input,
            retry_total=self.MAX_RETRIES,
            backoff_factor=self.RETRY_BACKOFF_FACTOR,
            proxy_url=self._selected_proxy_url,
            impersonate=self._curl_cffi_impersonate if self._http_client_name_input == "curl_cffi" else None,
        )

    def _ensure_http_client(self) -> _TikTokHttpClientBase:
        if self._direct_http_client is None:
            self._direct_http_client = self._create_session()
        return self._direct_http_client

    @property
    def _http_client(self) -> _TikTokHttpClientBase:
        """Back-compat alias for tests and direct API callers."""
        return self._ensure_http_client()

    def _run_context_meta(self, *, retrieval_mode: str | None = None, auth_mode: str | None = None) -> dict[str, Any]:
        resolved_auth_mode = auth_mode or ("with_cookies" if self.cookies else "without_cookies")
        mode = str(retrieval_mode or "").strip().lower() or None
        if mode == "ytdlp":
            return {
                "http_client": "yt_dlp",
                "proxy_enabled": False,
                "proxy_label": None,
                "auth_mode": resolved_auth_mode,
            }
        return {
            "http_client": self._http_client_name_input or "requests",
            "proxy_enabled": bool(self._selected_proxy_url),
            "proxy_label": self._proxy_label(self._selected_proxy_url),
            "auth_mode": resolved_auth_mode,
        }

    def _set_retrieval_meta(
        self,
        *,
        context_mode: str | None = None,
        auth_mode: str | None = None,
        **values: Any,
    ) -> None:
        endpoint_responses = dict(self.last_retrieval_meta.get("endpoint_responses") or {})
        payload = {}
        if context_mode != "ytdlp":
            for key in PRESERVED_RETRIEVAL_META_KEYS:
                if key in self.last_retrieval_meta and key not in values:
                    payload[key] = self.last_retrieval_meta[key]
        payload.update(
            self._run_context_meta(
                retrieval_mode=context_mode or str(values.get("retrieval_mode") or "").strip() or None,
                auth_mode=auth_mode,
            )
        )
        if context_mode == "ytdlp":
            payload["endpoint_responses"] = {}
        elif endpoint_responses:
            payload["endpoint_responses"] = endpoint_responses
        payload.update(values)
        self.last_retrieval_meta = payload

    def _should_fallback_to_api_after_ytdlp_failure(self, error_code: str | None) -> bool:
        normalized_error_code = str(error_code or "").strip().lower()
        return normalized_error_code in YTDLP_API_FALLBACK_ERROR_CODES

    def _emit_progress(
        self,
        progress_cb: Callable[[dict[str, Any]], None] | None,
        *,
        phase: str,
        pages_scanned: int,
        posts_checked: int,
        matched_posts: int,
    ) -> None:
        if not progress_cb:
            return
        try:
            progress_cb(
                {
                    "phase": phase,
                    "pages_scanned": max(0, int(pages_scanned)),
                    "posts_checked": max(0, int(posts_checked)),
                    "matched_posts": max(0, int(matched_posts)),
                }
            )
        except Exception:
            logger.debug("TikTok scrape progress callback raised", exc_info=True)

    def _ensure_structured_direct_failure(self, *, mode: str) -> None:
        if self.last_retrieval_meta.get("error_code"):
            return
        stop_reason = f"{mode}_zero_posts"
        self._set_retrieval_meta(
            retrieval_mode=mode,
            error_code=stop_reason,
            stop_reason=stop_reason,
            fallback_chain=[mode],
        )

    def _set_tiktok_path_health(
        self,
        *,
        retrieval_mode: str,
        posts_found: int,
        stop_reason: str | None = None,
    ) -> None:
        mode = str(retrieval_mode or "").strip().lower() or "ytdlp"
        fallback_chain = [
            str(stage).strip().lower()
            for stage in (self.last_retrieval_meta.get("fallback_chain") or [])
            if str(stage).strip()
        ]
        used_ytdlp = "yt_dlp" in fallback_chain or mode == "ytdlp"
        used_api = "api" in fallback_chain or mode == "api"
        used_browser_intercept = "browser_intercept" in fallback_chain or mode == "browser_intercept"
        self.last_retrieval_meta["retrieval_mode"] = mode
        if stop_reason:
            self.last_retrieval_meta["stop_reason"] = stop_reason

        if used_ytdlp and used_api:
            self.last_retrieval_meta["path_role"] = "fallback"
            self.last_retrieval_meta["topology_state"] = "ytdlp_with_api_fallback"
            self.last_retrieval_meta["risk_state"] = "healthy" if posts_found > 0 else "critical"
            self.last_retrieval_meta["operator_summary"] = (
                "TikTok posts recovered via API fallback after yt-dlp failure."
                if posts_found > 0
                else "TikTok posts path degraded after yt-dlp failure and API fallback returned zero posts."
            )
        elif used_ytdlp and used_browser_intercept:
            self.last_retrieval_meta["path_role"] = "fallback"
            self.last_retrieval_meta["topology_state"] = "ytdlp_with_browser_fallback"
            self.last_retrieval_meta["risk_state"] = "healthy" if posts_found > 0 else "critical"
            self.last_retrieval_meta["operator_summary"] = (
                "TikTok posts recovered via browser_intercept fallback after yt-dlp failure."
                if posts_found > 0
                else "TikTok posts path degraded after yt-dlp failure and browser_intercept returned zero posts."
            )
        elif mode == "ytdlp" and posts_found <= 0:
            self.last_retrieval_meta["path_role"] = "primary"
            self.last_retrieval_meta["topology_state"] = "single_path_ytdlp"
            self.last_retrieval_meta["risk_state"] = "critical"
            self.last_retrieval_meta["operator_summary"] = (
                "TikTok posts path degraded: yt-dlp returned zero posts while browser_intercept is not proven live."
            )
        elif mode == "ytdlp":
            self.last_retrieval_meta["path_role"] = "primary"
            self.last_retrieval_meta["topology_state"] = "single_path_ytdlp"
            self.last_retrieval_meta["risk_state"] = "healthy"
            self.last_retrieval_meta["operator_summary"] = "TikTok posts path healthy on yt-dlp."
        else:
            self.last_retrieval_meta["path_role"] = "fallback"
            self.last_retrieval_meta["topology_state"] = f"single_path_{mode}"

    def scrape(
        self,
        config: TikTokScrapeConfig,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[TikTokPost]:
        """
        Scrape posts from a TikTok profile with filtering.

        Args:
            config: TikTokScrapeConfig with username, hashtags, date range, etc.

        Returns:
            List of TikTokPost objects matching the filters.
        """
        logger.info(f"Starting TikTok scrape for @{config.username}")
        if config.hashtags:
            logger.info(f"Filtering by hashtags: {config.hashtags}")
        if config.date_start or config.date_end:
            logger.info(f"Date range: {config.date_start} to {config.date_end}")
        if not self.cookies:
            logger.warning(
                "TikTok scraper running without cookies; results may be limited. "
                "Set SOCIAL_TIKTOK_COOKIES_JSON or TIKTOK_COOKIES_FILE env var."
            )

        # ----- scrape_mode routing -----
        mode = (config.scrape_mode or "ytdlp").strip().lower()

        if mode in {"ytdlp", "auto"}:
            if mode == "auto":
                logger.warning("TikTok scrape_mode=auto is deprecated; using ytdlp alias")
            posts = self._scrape_via_ytdlp(
                config,
                max_videos_hint=config.ytdlp_max_videos_hint,
                max_posts_hint=config.ytdlp_max_videos_hint,
                progress_cb=progress_cb,
            )
            self._set_tiktok_path_health(
                retrieval_mode="ytdlp",
                posts_found=len(posts),
                stop_reason=str(self.last_retrieval_meta.get("stop_reason") or "").strip() or None,
            )
            if posts:
                self.last_retrieval_meta["profile_enrichment_status"] = "skipped"
                return posts
            ytdlp_error_code = str(self.last_retrieval_meta.get("error_code") or "").strip() or None
            if self._should_fallback_to_api_after_ytdlp_failure(ytdlp_error_code):
                logger.info("yt-dlp failed with %s; falling back to TikTok API scraper", ytdlp_error_code)
                posts = self._scrape_api(config, progress_cb=progress_cb)
                if not posts:
                    self._ensure_structured_direct_failure(mode="api")
                self._set_tiktok_path_health(
                    retrieval_mode=str(self.last_retrieval_meta.get("retrieval_mode") or "api"),
                    posts_found=len(posts),
                    stop_reason=str(self.last_retrieval_meta.get("stop_reason") or "").strip() or None,
                )
                return posts
            self.last_retrieval_meta["profile_enrichment_status"] = "skipped"
            return posts

        if mode == "browser_intercept":
            posts = self._scrape_browser_intercept(config, progress_cb=progress_cb)
            if not posts:
                browser_intercept_meta = {
                    "intercepted_post_responses": int(self.last_retrieval_meta.get("intercepted_post_responses") or 0),
                    "intercepted_user_detail_responses": int(
                        self.last_retrieval_meta.get("intercepted_user_detail_responses") or 0
                    ),
                    "dom_cards_seen": int(self.last_retrieval_meta.get("dom_cards_seen") or 0),
                    "scroll_iterations": int(self.last_retrieval_meta.get("scroll_iterations") or 0),
                    "auth_mode": str(self.last_retrieval_meta.get("auth_mode") or "").strip() or None,
                    "playwright_error": str(self.last_retrieval_meta.get("playwright_error") or "").strip() or None,
                }
                self._ensure_structured_direct_failure(mode="browser_intercept")
                self.last_retrieval_meta.update(browser_intercept_meta)
                self.last_retrieval_meta["triage_bucket"] = self._classify_browser_intercept_failure(
                    posts_found=0,
                    intercepted_post_responses=browser_intercept_meta["intercepted_post_responses"],
                    intercepted_user_detail_responses=browser_intercept_meta["intercepted_user_detail_responses"],
                    dom_cards_seen=browser_intercept_meta["dom_cards_seen"],
                    scroll_iterations=browser_intercept_meta["scroll_iterations"],
                    authenticated=bool(browser_intercept_meta["auth_mode"] == "with_cookies"),
                    playwright_error=browser_intercept_meta["playwright_error"],
                )
            return posts

        if mode == "api":
            posts = self._scrape_api(config, progress_cb=progress_cb)
            if not posts:
                self._ensure_structured_direct_failure(mode="api")
            return posts

        raise ValueError(f"Unsupported TikTok scrape_mode: {config.scrape_mode}")


__all__ = [
    "TikTokComment",
    "TikTokPost",
    "TikTokScrapeConfig",
    "TikTokScraper",
    "build_tiktok_http_client",
]
