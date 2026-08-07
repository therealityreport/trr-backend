"""
TikTok scraper module for fetching posts from public profiles.

Supports:
- Fetching posts from any public TikTok profile
- Filtering by hashtags (case-insensitive)
- Filtering by date range
- Fetching comments and replies with like counts
"""

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import requests

from trr_backend.socials.tiktok.http_client import (
    DEFAULT_CURL_CFFI_IMPERSONATE,
    DEFAULT_HTTP_CLIENT,
    TIKTOK_DEFAULT_MAX_POSTS,
    TIKTOK_POSTS_PER_PAGE,
    _TikTokHttpClientBase,
)
from trr_backend.socials.tiktok.http_client import (
    build_tiktok_http_client as _build_tiktok_http_client,
)

logger = logging.getLogger(__name__)

_TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
_YTDLP_API_FALLBACK_ERROR_CODES = frozenset({"ytdlp_unavailable", "ytdlp_timeout", "ytdlp_nonzero_exit"})
_PRESERVED_RETRIEVAL_META_KEYS = ("proxy_source", "curl_cffi_impersonate")
URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


@dataclass
class TikTokScrapeConfig:
    """Configuration for a TikTok scrape operation."""

    username: str
    hashtags: list[str] = field(default_factory=list)
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 2.0
    max_pages: int | None = None  # None = no limit

    # Performance tuning
    fast_mode: bool = False
    """When True, uses aggressive rate-limiting tiers and reduced delays."""

    scrape_mode: str = "ytdlp"
    """Scraping strategy: 'ytdlp' (production default), 'api' (experimental
    direct TikTok API), 'browser_intercept' (experimental Playwright
    interception), or 'auto' (compatibility alias to 'ytdlp')."""

    ytdlp_max_videos_hint: int | None = None
    """Advisory upper bound for yt-dlp profile enumeration in fallback mode."""

    fetch_comment_replies: bool = True
    """When False, only fetch top-level comments and skip reply chains."""

    # Metadata for tracking
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def __post_init__(self):
        """Apply fast_mode overrides when enabled."""
        if self.fast_mode:
            if self.delay_seconds == 2.0:  # Only override if at default
                self.delay_seconds = 0.5
            logger.info(
                "TikTokScrapeConfig fast_mode enabled: delay=%.2fs, scrape_mode=%s",
                self.delay_seconds,
                self.scrape_mode,
            )

    @property
    def start_timestamp(self) -> float:
        coerced = _coerce_utc_datetime(self.date_start)
        return coerced.timestamp() if coerced else 0

    @property
    def end_timestamp(self) -> float:
        coerced = _coerce_utc_datetime(self.date_end)
        return coerced.timestamp() if coerced else datetime.now(UTC).timestamp()

    def matches_hashtags(self, text: str) -> bool:
        """Check if text contains any of the configured hashtags."""
        if not self.hashtags:
            return True  # No filter = match all
        text_lower = text.lower()
        return any(f"#{tag.lower().lstrip('#')}" in text_lower for tag in self.hashtags)

    def is_in_date_range(self, timestamp: int) -> bool | None:
        """
        Check if timestamp is in date range.
        Returns None if before range (stop iteration), True if in range, False if after.
        """
        if timestamp < self.start_timestamp:
            return None  # Before range - stop
        if timestamp > self.end_timestamp:
            return False  # After range - skip
        return True  # In range


@dataclass
class TikTokComment:
    """Represents a single TikTok comment with reply support."""

    comment_id: str
    text: str
    username: str
    user_id: str
    nickname: str
    created_at: int
    date_time: str
    likes: int
    is_reply: bool
    parent_comment_id: str | None  # ID of parent comment if this is a reply
    reply_count: int
    replies: list["TikTokComment"] = field(default_factory=list)
    avatar_thumbnail_url: str | None = None
    comment_language: str | None = None
    is_author_liked: bool | None = None
    aweme_id: str | None = None
    parent_source_comment_id: str | None = None
    user_url: str | None = None
    user_bio: str | None = None
    user_avatar_url: str | None = None
    user_region: str | None = None
    user_language: str | None = None
    media_urls: list[str] = field(default_factory=list)
    hosted_media_urls: list[str] = field(default_factory=list)
    media_mirror_status: str | None = None
    media_mirror_error: str | None = None
    media_mirror_attempt_count: int | None = None
    media_mirror_last_attempt_at: str | None = None
    media_mirror_last_job_id: str | None = None

    # Post reference
    video_id: str = ""
    post_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested replies
        result["replies"] = [r.to_dict() if hasattr(r, "to_dict") else r for r in self.replies]
        return result


@dataclass
class TikTokPost:
    """Represents a single TikTok post with extracted data."""

    video_id: str
    date_time: str
    create_time: int
    description: str
    hashtags: list[str]
    mentions: list[str]
    likes: int
    comments: int
    shares: int
    saves: int
    views: int
    url: str
    username: str
    author_nickname: str
    duration: int  # seconds
    music_title: str
    music_author: str
    user_avatar_url: str | None = None

    # Media URLs
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None

    # Comments (populated when fetch_comments is called)
    comment_list: list[TikTokComment] = field(default_factory=list)

    # Optional tracking metadata
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested comments
        result["comment_list"] = [c.to_dict() if hasattr(c, "to_dict") else c for c in self.comment_list]
        return result


class TikTokScraper:
    """TikTok profile scraper for public profiles."""

    # TikTok web API endpoints
    USER_DETAIL_URL = "https://www.tiktok.com/api/user/detail/"
    USER_POST_URL = "https://www.tiktok.com/api/post/item_list/"
    COMMENTS_URL = "https://www.tiktok.com/api/comment/list/"
    COMMENT_REPLIES_URL = "https://www.tiktok.com/api/comment/list/reply/"

    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.5
    REQUEST_TIMEOUT_SECONDS = (10, 45)
    COMMENT_REQUEST_TIMEOUT_SECONDS = REQUEST_TIMEOUT_SECONDS

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
            str(http_client_name or os.getenv("SOCIAL_TIKTOK_HTTP_CLIENT") or "").strip().lower() or DEFAULT_HTTP_CLIENT
        )
        self._curl_cffi_impersonate = (
            str(os.getenv("SOCIAL_TIKTOK_CURL_CFFI_IMPERSONATE") or "").strip() or DEFAULT_CURL_CFFI_IMPERSONATE
        )
        explicit_proxy_urls = [str(value or "").strip() for value in (proxy_urls or []) if str(value or "").strip()]
        env_proxy_urls = [
            value.strip() for value in str(os.getenv("SOCIAL_TIKTOK_PROXY_URLS") or "").split(",") if value.strip()
        ]
        crawlee_proxy_urls = [
            value.strip()
            for value in str(os.getenv("SOCIAL_CRAWLEE_PROXY_URLS_TIKTOK") or "").split(",")
            if value.strip()
        ]
        if explicit_proxy_urls:
            self._proxy_urls_input = explicit_proxy_urls
            self._proxy_source = "constructor"
        elif env_proxy_urls:
            self._proxy_urls_input = env_proxy_urls
            self._proxy_source = "SOCIAL_TIKTOK_PROXY_URLS"
        else:
            self._proxy_urls_input = crawlee_proxy_urls
            self._proxy_source = "SOCIAL_CRAWLEE_PROXY_URLS_TIKTOK" if crawlee_proxy_urls else None
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
    def _is_auth_related_failure(reason: str | None) -> bool:
        value = str(reason or "").strip().lower()
        if not value:
            return False
        markers = (
            "auth",
            "login",
            "challenge",
            "captcha",
            "blocked",
            "forbidden",
            "unauthorized",
        )
        return any(marker in value for marker in markers)

    def _set_comment_failure_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self._last_api_fail_reason = normalized
        self.last_comment_fetch_reason = normalized
        if self._is_auth_related_failure(normalized):
            self.comments_auth_failed = True

    @staticmethod
    def _normalize_optional_non_negative_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_optional_non_negative_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            return None

    def _record_reply_cap_event(
        self,
        *,
        reason: str,
        comment_id: str,
        limit: Any = None,
        replies_fetched: int = 0,
        pages_fetched: int = 0,
    ) -> None:
        events = list(self.last_comment_fetch_meta.get("reply_cap_events") or [])
        event = {
            "reason": str(reason or "").strip() or "reply_cap",
            "comment_id": str(comment_id or "").strip() or None,
            "limit": limit,
            "replies_fetched": max(0, int(replies_fetched or 0)),
            "pages_fetched": max(0, int(pages_fetched or 0)),
        }
        events.append({key: value for key, value in event.items() if value is not None})
        self.last_comment_fetch_meta["reply_cap_events"] = events

    @staticmethod
    def _should_skip_api_pagination(reason: str | None) -> bool:
        normalized = str(reason or "").strip().lower()
        return normalized in {"non_json_response", "challenge_or_blocked"}

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
            for key in _PRESERVED_RETRIEVAL_META_KEYS:
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

    def _env_flag_enabled(self, env_name: str) -> bool:
        return str(os.getenv(env_name) or "").strip().lower() in _TRUTHY_ENV_VALUES

    def _direct_comment_api_experiment_enabled(self) -> bool:
        if self._direct_comment_api_enabled_override is not None:
            return self._direct_comment_api_enabled_override
        return self._env_flag_enabled("SOCIAL_TIKTOK_ENABLE_DIRECT_COMMENT_API_EXPERIMENT")

    def _should_fallback_to_api_after_ytdlp_failure(self, error_code: str | None) -> bool:
        normalized_error_code = str(error_code or "").strip().lower()
        return normalized_error_code in _YTDLP_API_FALLBACK_ERROR_CODES

    def _get_headers(self, referer: str | None = None) -> dict:
        """Get request headers."""
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://www.tiktok.com",
            "referer": referer or "https://www.tiktok.com/",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
        }

    def _rate_limit(self, delay: float, *, fast_mode: bool = False):
        """Apply adaptive rate limiting between requests.

        In fast_mode, reduces delay based on consecutive success count.
        """
        if self._request_count > 0:
            if fast_mode:
                consecutive = getattr(self, "_consecutive_success", 0)
                if consecutive >= 20:
                    effective_delay = delay * 0.15
                elif consecutive >= 5:
                    effective_delay = delay * 0.25
                else:
                    effective_delay = delay * 0.5
            else:
                effective_delay = delay
            logger.debug(f"Rate limiting: waiting {effective_delay:.3f}s (base={delay}s, fast={fast_mode})")
            time.sleep(effective_delay)
        self._request_count += 1

    def _record_endpoint_response(
        self,
        *,
        endpoint: str,
        failure_reason: str | None = None,
        response: requests.Response | None = None,
    ) -> None:
        endpoint_name = str(endpoint or "").strip()
        if not endpoint_name:
            return
        endpoint_responses = dict(self.last_retrieval_meta.get("endpoint_responses") or {})
        payload: dict[str, Any] = {"endpoint": endpoint_name}
        normalized_failure_reason = str(failure_reason or "").strip().lower() or None
        if normalized_failure_reason:
            payload["failure_reason"] = normalized_failure_reason
        if response is not None:
            headers = getattr(response, "headers", None) or {}
            content_type = str(headers.get("content-type") or "").strip() or None
            content_length_raw = str(headers.get("content-length") or "").strip()
            try:
                content_length = int(content_length_raw) if content_length_raw else len(response.content or b"")
            except (TypeError, ValueError):
                content_length = len(response.content or b"")
            request_id = (
                str(headers.get("x-tt-logid") or "").strip() or str(headers.get("x-request-id") or "").strip() or None
            )
            payload.update(
                {
                    "http_status": int(getattr(response, "status_code", 0) or 0),
                    "content_type": content_type,
                    "content_length": max(0, int(content_length)),
                    "request_id": request_id,
                }
            )
        endpoint_responses[endpoint_name] = {key: value for key, value in payload.items() if value is not None}
        self.last_retrieval_meta["endpoint_responses"] = endpoint_responses

    def _safe_response_json(self, response: requests.Response, *, endpoint: str) -> dict | None:
        """Parse response JSON and classify challenge/non-json failures."""
        try:
            return response.json()
        except ValueError:
            content_type = (response.headers.get("content-type") or "").lower()
            body = (response.text or "")[:256].lower()
            if "text/html" in content_type or "<html" in body or "captcha" in body or "verify" in body:
                self._last_api_fail_reason = "challenge_or_blocked"
            else:
                self._last_api_fail_reason = "non_json_response"
            self._record_endpoint_response(
                endpoint=endpoint,
                failure_reason=self._last_api_fail_reason,
                response=response,
            )
            logger.warning(
                "TikTok returned non-JSON response (status=%s, content-type=%s, reason=%s)",
                response.status_code,
                content_type or "unknown",
                self._last_api_fail_reason,
            )
            return None

    @staticmethod
    def _coerce_timestamp(value: Any) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return 0
            if raw.isdigit():
                return int(raw)
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return 0
            return int(parsed.timestamp())
        return 0

    @staticmethod
    def _safe_int_metric(value: Any) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, (int, float)):
            return max(0, int(float(value)))
        raw = str(value or "").strip()
        if not raw:
            return 0
        compact = raw.replace(",", "").replace("_", "").strip()
        if not compact:
            return 0
        suffix = compact[-1].upper()
        numeric_portion = compact
        multiplier = 1.0
        if suffix in {"K", "M", "B"}:
            numeric_portion = compact[:-1].strip()
            multiplier = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}[suffix]
        try:
            return max(0, int(float(numeric_portion) * multiplier))
        except ValueError:
            return 0

    @staticmethod
    def _dedupe_preserve_order(values: list[str]) -> list[str]:
        output: list[str] = []
        seen: set[str] = set()
        for value in values:
            token = str(value or "").strip()
            if not token:
                continue
            key = token.lower()
            if key in seen:
                continue
            seen.add(key)
            output.append(token)
        return output

    @staticmethod
    def _extract_video_id_from_url(url: str | None) -> str:
        value = str(url or "").strip()
        if not value:
            return ""
        path = urlparse(value).path
        match = re.search(r"/video/(\d+)", path)
        return match.group(1) if match else ""

    @staticmethod
    def _normalize_hashtag_token(value: Any) -> str | None:
        token = str(value or "").strip().lstrip("#")
        if not token:
            return None
        match = re.search(r"[A-Za-z0-9_]+", token)
        return match.group(0) if match else None

    @staticmethod
    def _normalize_mention_token(value: Any) -> str | None:
        token = str(value or "").strip().lstrip("@")
        if not token:
            return None
        match = re.search(r"[A-Za-z0-9._]+", token)
        if not match:
            return None
        normalized = match.group(0).rstrip(".,:;!?)]}>'\"")
        return normalized if normalized else None

    def _find_ytdlp_cookie_file(self) -> str | None:
        """Find a Netscape-format cookie file for yt-dlp."""
        import os
        from pathlib import Path

        # Check env var first
        path = (os.getenv("TIKTOK_COOKIES_NETSCAPE_FILE") or "").strip()
        if path:
            p = Path(path).expanduser()
            if p.is_file():
                return str(p)

        # Auto-detect alongside the JSON cookie file
        for env_key in ("TIKTOK_COOKIES_FILE", "SOCIAL_TIKTOK_COOKIES_FILE"):
            json_path = (os.getenv(env_key) or "").strip()
            if json_path:
                netscape = Path(json_path).with_name(Path(json_path).stem + "_netscape.txt")
                if netscape.is_file():
                    return str(netscape)

        # Default location
        default = Path("data/tiktok_cookies_netscape.txt")
        if default.is_file():
            return str(default)

        return None

    # Pattern for extracting embedded JSON from TikTok HTML pages.
    _REHYDRATION_RE = re.compile(
        r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
        re.DOTALL,
    )
    _SIGI_STATE_RE = re.compile(
        r'<script\s+id="SIGI_STATE"[^>]*>(.*?)</script>',
        re.DOTALL,
    )

    def _fetch_profile_html(self, username: str, delay: float = 2.0) -> dict | None:
        """Fetch profile page HTML and extract embedded post data."""
        self._rate_limit(delay)

        url = f"https://www.tiktok.com/@{username}"
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
        }

        try:
            response = self.session.get(
                url,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            self._record_endpoint_response(endpoint="profile_html", response=response)
            html = response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch TikTok profile HTML for @{username}: {e}")
            return None

        # Try __UNIVERSAL_DATA_FOR_REHYDRATION__ first (current TikTok format)
        match = self._REHYDRATION_RE.search(html)
        if match:
            try:
                data = json.loads(match.group(1))
                return data
            except json.JSONDecodeError:
                logger.warning("Failed to parse __UNIVERSAL_DATA_FOR_REHYDRATION__ JSON")

        # Fallback to SIGI_STATE (older TikTok format)
        match = self._SIGI_STATE_RE.search(html)
        if match:
            try:
                data = json.loads(match.group(1))
                return data
            except json.JSONDecodeError:
                logger.warning("Failed to parse SIGI_STATE JSON")

        logger.error("Could not extract embedded data from TikTok profile page")
        return None

    def _extract_posts_from_html_data(self, data: dict, username: str) -> tuple[list[dict], str | None]:
        """Extract post items from HTML-embedded JSON data."""
        items: list[dict] = []
        sec_uid = None

        # __UNIVERSAL_DATA_FOR_REHYDRATION__ format
        default_scope = data.get("__DEFAULT_SCOPE__", {})
        webapp_detail = default_scope.get("webapp.user-detail", {})
        user_info = webapp_detail.get("userInfo", {})
        if user_info:
            user = user_info.get("user", {})
            sec_uid = user.get("secUid")
            # itemList lives directly under userInfo (not under a "post" sub-key)
            item_list = user_info.get("itemList", [])
            if item_list:
                items = item_list

        # Fallback: SIGI_STATE / ItemModule format
        if not items:
            item_module = data.get("ItemModule", {})
            if item_module and isinstance(item_module, dict):
                items = list(item_module.values())

        if not sec_uid:
            user_module = data.get("UserModule", {}).get("users", {})
            user_data = user_module.get(username, {})
            sec_uid = user_data.get("secUid")

        return items, sec_uid

    # yt-dlp based fallback methods (no auth required)

    @staticmethod
    def _has_ytdlp() -> bool:
        """Check if yt-dlp is available on the system."""
        return shutil.which("yt-dlp") is not None

    def _ytdlp_list_videos(
        self,
        username: str,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
        max_videos: int = 500,
    ) -> list[dict]:
        """
        List videos from a TikTok profile using yt-dlp --flat-playlist.

        Returns a list of dicts with id, upload_date, view_count, title.
        Note: TikTok's public profile pagination is limited (typically
        last ~50 videos). Date filtering is done in Python because
        yt-dlp's --dateafter/--datebefore flags don't work reliably
        with TikTok's flat-playlist mode.
        """
        if not self._has_ytdlp():
            logger.warning("yt-dlp not found; skipping yt-dlp fallback")
            return []

        url = f"https://www.tiktok.com/@{username}"
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--print",
            "%(id)s\t%(upload_date)s\t%(view_count)s\t%(title)s",
            "--playlist-end",
            str(max_videos),
            url,
        ]

        # Format date boundaries for Python-side filtering
        ds_str = date_start.strftime("%Y%m%d") if date_start else None
        de_str = date_end.strftime("%Y%m%d") if date_end else None

        logger.info(f"yt-dlp listing videos for @{username} (max {max_videos})...")
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            logger.warning("yt-dlp listing timed out")
            return []

        videos: list[dict] = []
        for line in proc.stdout.strip().splitlines():
            parts = line.split("\t", 3)
            if len(parts) < 2:
                continue
            vid_id = parts[0]
            upload_date = parts[1] if len(parts) > 1 else ""
            view_count = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            title = parts[3] if len(parts) > 3 else ""

            # Filter by date in Python
            if upload_date and ds_str and upload_date < ds_str:
                continue
            if upload_date and de_str and upload_date > de_str:
                continue

            videos.append(
                {
                    "id": vid_id,
                    "upload_date": upload_date,
                    "view_count": view_count,
                    "title": title,
                }
            )

        logger.info(f"yt-dlp listed {len(videos)} videos for @{username} (filtered to date range)")
        return videos

    def _ytdlp_get_video_metadata(self, video_url: str) -> dict | None:
        """Get full metadata for a single TikTok video via yt-dlp."""
        if not self._has_ytdlp():
            return None

        cmd = ["yt-dlp", "--dump-json", "--no-download", video_url]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if proc.returncode != 0:
                logger.warning(f"yt-dlp metadata failed for {video_url}: {proc.stderr[:200]}")
                return None
            return json.loads(proc.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            logger.warning(f"yt-dlp metadata error for {video_url}: {exc}")
            return None

    def _parse_ytdlp_metadata(self, data: dict, config: TikTokScrapeConfig) -> TikTokPost:
        """Convert yt-dlp JSON metadata into a TikTokPost."""
        description = data.get("description", "") or data.get("title", "")
        create_time = data.get("timestamp", 0)
        username = data.get("uploader", config.username)
        video_id = data.get("id", "")
        media_urls = self._extract_ytdlp_video_urls(data)
        thumbnail_url = self._extract_ytdlp_thumbnail_url(data)
        user_avatar_url = self._pick_best_avatar_url(
            data.get("uploader_avatar"),
            data.get("channel_thumbnail"),
            data.get("channelAvatarUrl"),
            data.get("author_avatar_url"),
        )

        return TikTokPost(
            video_id=video_id,
            date_time=(datetime.fromtimestamp(create_time).strftime("%Y-%m-%d %H:%M:%S") if create_time else ""),
            create_time=create_time,
            description=description,
            hashtags=self._extract_hashtags(description),
            mentions=self._extract_mentions(description),
            likes=self._safe_int_metric(data.get("like_count", 0) or 0),
            comments=self._safe_int_metric(data.get("comment_count", 0) or 0),
            shares=self._safe_int_metric(data.get("repost_count", 0) or 0),
            saves=self._safe_int_metric(data.get("collect_count", 0) or data.get("save_count", 0) or 0),
            views=self._safe_int_metric(data.get("view_count", 0) or 0),
            url=f"https://www.tiktok.com/@{username}/video/{video_id}" if video_id else "",
            username=username,
            author_nickname=data.get("channel", ""),
            duration=data.get("duration", 0) or 0,
            music_title=data.get("track", ""),
            music_author=data.get("artist", ""),
            user_avatar_url=user_avatar_url,
            media_urls=media_urls,
            thumbnail_url=thumbnail_url or (media_urls[0] if media_urls else None),
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    @staticmethod
    def _extract_ytdlp_video_urls(data: dict) -> list[str]:
        urls: list[str] = []

        primary_url = str(data.get("url") or "").strip()
        if primary_url:
            urls.append(primary_url)

        best_requested_url = ""
        best_requested_score = (-1, -1)
        requested_formats = data.get("requested_formats")
        if isinstance(requested_formats, list):
            for fmt in requested_formats:
                if not isinstance(fmt, dict):
                    continue
                url = str(fmt.get("url") or "").strip()
                if not url:
                    continue
                has_video = str(fmt.get("vcodec") or "").lower() not in {"", "none"}
                has_audio = str(fmt.get("acodec") or "").lower() not in {"", "none"}
                height = int(fmt.get("height") or 0)
                score = (1 if (has_video and has_audio) else (0 if has_video else -1), height)
                if score > best_requested_score:
                    best_requested_score = score
                    best_requested_url = url
        if best_requested_url:
            urls.append(best_requested_url)

        best_format_url = ""
        best_format_score = (-1, -1, -1)
        formats = data.get("formats")
        if isinstance(formats, list):
            for fmt in formats:
                if not isinstance(fmt, dict):
                    continue
                url = str(fmt.get("url") or "").strip()
                if not url:
                    continue
                has_video = str(fmt.get("vcodec") or "").lower() not in {"", "none"}
                if not has_video:
                    continue
                has_audio = str(fmt.get("acodec") or "").lower() not in {"", "none"}
                height = int(fmt.get("height") or 0)
                tbr = int(fmt.get("tbr") or 0)
                score = (1 if has_audio else 0, height, tbr)
                if score > best_format_score:
                    best_format_score = score
                    best_format_url = url
        if best_format_url:
            urls.append(best_format_url)

        deduped: list[str] = []
        for url in urls:
            if url and url not in deduped:
                deduped.append(url)
        return deduped[:1]

    @staticmethod
    def _extract_ytdlp_thumbnail_url(data: dict) -> str | None:
        thumbnail = str(data.get("thumbnail") or "").strip()
        if thumbnail:
            return thumbnail
        thumbnails = data.get("thumbnails")
        if not isinstance(thumbnails, list):
            return None
        best_url = ""
        best_width = -1
        for entry in thumbnails:
            if not isinstance(entry, dict):
                continue
            url = str(entry.get("url") or "").strip()
            if not url:
                continue
            width = int(entry.get("width") or 0)
            if width >= best_width:
                best_width = width
                best_url = url
        return best_url or None

    def _scrape_via_ytdlp(
        self,
        config: TikTokScrapeConfig,
        *,
        max_videos_hint: int | None = None,
        max_posts_hint: int | None = None,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[TikTokPost]:
        """
        Fallback scraper using yt-dlp bulk mode.

        Uses --flat-playlist --dump-json to get full metadata in a single
        pass, avoiding slow per-video fetches. Automatically scales the
        playlist depth based on how far back the date range extends.
        When a Netscape-format cookie file is available, it's passed to
        yt-dlp for authenticated access.
        """
        if not self._has_ytdlp():
            self._set_retrieval_meta(
                context_mode="ytdlp",
                auth_mode="none",
                retrieval_mode="ytdlp",
                error_code="ytdlp_unavailable",
                stop_reason="ytdlp_unavailable",
                pages_scanned=0,
                posts_checked=0,
                videos_scanned=0,
                ytdlp_posts_found=0,
                ytdlp_max_videos=0,
                ytdlp_cookie_file_present=False,
                ytdlp_cookie_file_used=False,
                fallback_chain=["yt_dlp"],
            )
            return []

        logger.info("Attempting yt-dlp bulk fallback scraper...")

        # Estimate how many videos to fetch based on date range.
        # @bravotv posts ~16 videos/day across all shows; use 22/day for buffer.
        max_videos: int | None = None
        start_dt = _coerce_utc_datetime(config.date_start)
        end_dt = _coerce_utc_datetime(config.date_end)
        if start_dt:
            days_back = max(0, (datetime.now(tz=UTC) - start_dt).days)
            max_videos = max(500, min(12000, days_back * 22))
        config_max_videos_hint = (
            max(0, int(config.ytdlp_max_videos_hint or 0)) if config.ytdlp_max_videos_hint is not None else 0
        )
        if max_videos_hint is None and config_max_videos_hint > 0:
            max_videos_hint = config_max_videos_hint
        if max_posts_hint is None and max_videos_hint is not None and max_videos_hint > 0:
            max_posts_hint = max_videos_hint
        # With a date range, keep the date-based floor so older posts are not
        # silently missed. Without a date range, an explicit small hint is a
        # real canary/backfill cap and keeps fallback runs bounded.
        if max_videos_hint is not None and max_videos_hint > 0:
            if start_dt is None and max_posts_hint is not None:
                max_videos = min(25_000, max_videos_hint)
            elif max_videos is None or max_videos_hint > max_videos:
                max_videos = min(25_000, max_videos_hint)

        url = f"https://www.tiktok.com/@{config.username}"
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--dump-json",
        ]
        if max_videos is not None:
            cmd.extend(["--playlist-end", str(max_videos)])

        # Pass cookies if a Netscape-format file exists
        cookie_file = self._find_ytdlp_cookie_file()
        cookie_file_used = bool(cookie_file)
        if cookie_file:
            cmd.extend(["--cookies", cookie_file])
            logger.info(f"yt-dlp using cookies from {cookie_file}")

        cmd.append(url)

        listing_scope = f"up to {max_videos} videos" if max_videos is not None else "entire playlist"
        logger.info("yt-dlp bulk listing @%s (%s)...", config.username, listing_scope)
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=max(120, max_videos // 3) if max_videos is not None else 900,
            )
        except subprocess.TimeoutExpired:
            logger.warning("yt-dlp bulk listing timed out")
            self._set_retrieval_meta(
                context_mode="ytdlp",
                auth_mode="ytdlp_cookies" if cookie_file_used else "none",
                retrieval_mode="ytdlp",
                error_code="ytdlp_timeout",
                stop_reason="timeout",
                pages_scanned=0,
                posts_checked=0,
                videos_scanned=0,
                ytdlp_posts_found=0,
                ytdlp_max_videos=max_videos,
                ytdlp_cookie_file_present=bool(cookie_file),
                ytdlp_cookie_file_used=cookie_file_used,
                fallback_chain=["yt_dlp"],
            )
            return []
        if int(getattr(proc, "returncode", 0) or 0) != 0:
            logger.warning("yt-dlp bulk listing exited non-zero: %s", getattr(proc, "stderr", "")[:240])
            self._set_retrieval_meta(
                context_mode="ytdlp",
                auth_mode="ytdlp_cookies" if cookie_file_used else "none",
                retrieval_mode="ytdlp",
                error_code="ytdlp_nonzero_exit",
                stop_reason="nonzero_exit",
                error_message=str(getattr(proc, "stderr", "") or "")[:240] or None,
                pages_scanned=0,
                posts_checked=0,
                videos_scanned=0,
                ytdlp_posts_found=0,
                ytdlp_max_videos=max_videos,
                ytdlp_cookie_file_present=bool(cookie_file),
                ytdlp_cookie_file_used=cookie_file_used,
                fallback_chain=["yt_dlp"],
            )
            return []

        posts: list[TikTokPost] = []
        total = 0
        stop_reason = "playlist_exhausted"
        for line in proc.stdout.strip().splitlines():
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1

            ts = data.get("timestamp", 0) or 0
            item_dt = datetime.fromtimestamp(ts, tz=UTC)
            if start_dt and item_dt < start_dt:
                stop_reason = "date_start_reached"
                break  # Before date range, stop
            if end_dt and item_dt > end_dt:
                continue  # After date range, skip

            description = data.get("title", "") or data.get("description", "") or ""
            if not config.matches_hashtags(description):
                continue

            post = self._parse_ytdlp_metadata(data, config)
            posts.append(post)
            logger.info(f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views")

            if progress_cb and (total == 1 or total % 100 == 0):
                try:
                    progress_cb(
                        {
                            "phase": "scrape_ytdlp_fallback",
                            "pages_scanned": 0,
                            "posts_checked": total,
                            "matched_posts": len(posts),
                            "total_posts": max_videos_hint or config.ytdlp_max_videos_hint or None,
                        }
                    )
                except Exception:
                    logger.debug("TikTok yt-dlp progress callback raised", exc_info=True)

            if max_posts_hint and len(posts) >= max_posts_hint:
                stop_reason = "max_posts_reached"
                break

        logger.info(f"yt-dlp bulk: scanned {total} videos, found {len(posts)} matches")
        self._set_retrieval_meta(
            context_mode="ytdlp",
            auth_mode="ytdlp_cookies" if cookie_file_used else "none",
            retrieval_mode="ytdlp",
            pages_scanned=0,
            posts_checked=total,
            videos_scanned=total,
            ytdlp_posts_found=len(posts),
            ytdlp_max_videos=max_videos,
            stop_reason=stop_reason,
            fallback_chain=["yt_dlp"],
            ytdlp_cookie_file_present=bool(cookie_file),
            ytdlp_cookie_file_used=cookie_file_used,
        )
        return posts

    def _extract_hashtags(self, text: str) -> list[str]:
        """Extract hashtags from text."""
        values: list[str] = []
        for token in re.findall(r"#([A-Za-z0-9_]+)", text or ""):
            normalized = self._normalize_hashtag_token(token)
            if normalized:
                values.append(normalized)
        return self._dedupe_preserve_order(values)

    def _extract_mentions(self, text: str) -> list[str]:
        """Extract @mentions from text."""
        values: list[str] = []
        for token in re.findall(r"@([A-Za-z0-9._]+)", text or ""):
            normalized = self._normalize_mention_token(token)
            if normalized:
                values.append(normalized)
        return self._dedupe_preserve_order(values)

    def _extract_structured_hashtags(self, item: dict) -> list[str]:
        hashtags: list[str] = []
        for row in item.get("hashtags") or []:
            value = row
            if isinstance(row, dict):
                value = row.get("name") or row.get("hashtagName")
            normalized = self._normalize_hashtag_token(value)
            if normalized:
                hashtags.append(normalized)
        return self._dedupe_preserve_order(hashtags)

    def _extract_structured_mentions(self, item: dict) -> list[str]:
        mentions: list[str] = []
        for key in ("mentions", "detailedMentions"):
            for row in item.get(key) or []:
                value = row
                if isinstance(row, dict):
                    value = row.get("name") or row.get("username") or row.get("uniqueId")
                normalized = self._normalize_mention_token(value)
                if normalized:
                    mentions.append(normalized)
        return self._dedupe_preserve_order(mentions)

    def _parse_post_item(self, item: dict, config: TikTokScrapeConfig) -> TikTokPost:
        """Parse a post item into TikTokPost."""
        video_id = str(
            item.get("id")
            or item.get("aweme_id")
            or item.get("videoId")
            or self._extract_video_id_from_url(item.get("webVideoUrl") or item.get("url"))
            or ""
        )
        create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
        description = str(item.get("desc") or item.get("text") or "")
        media_urls = self._extract_media_urls(item)
        thumbnail_url = self._extract_thumbnail_url(item)

        # Author info
        author = _dict_or_empty(item.get("author"))
        author_meta = _dict_or_empty(item.get("authorMeta"))
        username = str(
            author.get("uniqueId")
            or author.get("unique_id")
            or author_meta.get("name")
            or author_meta.get("uniqueId")
            or config.username
        )
        nickname = str(author.get("nickname") or author_meta.get("nickName") or author_meta.get("nickname") or "")
        user_avatar_url = self._pick_best_avatar_url(
            author.get("avatarLarger"),
            author.get("avatar_larger"),
            author.get("originalAvatarUrl"),
            author.get("avatarUrl"),
            author.get("avatar"),
            author.get("avatar_thumb"),
            author.get("avatar_thumb_url"),
            author_meta.get("avatarLarger"),
            author_meta.get("avatar_larger"),
            author_meta.get("avatarUrl"),
            author_meta.get("avatar"),
            author_meta.get("avatarThumb"),
            author_meta.get("avatar_thumb"),
            item.get("avatarUrl"),
            item.get("avatar"),
        )

        # Stats
        stats = _dict_or_empty(item.get("stats"))
        stats_v2 = _dict_or_empty(item.get("statsV2"))

        # Music info
        music = _dict_or_empty(item.get("music"))
        music_meta = _dict_or_empty(item.get("musicMeta"))

        # Video info
        video = _dict_or_empty(item.get("video"))
        video_meta = _dict_or_empty(item.get("videoMeta"))
        duration = int(video.get("duration") or video_meta.get("duration") or 0)
        hashtags = self._dedupe_preserve_order(
            [*self._extract_structured_hashtags(item), *self._extract_hashtags(description)]
        )
        mentions = self._dedupe_preserve_order(
            [*self._extract_structured_mentions(item), *self._extract_mentions(description)]
        )
        canonical_url = str(item.get("webVideoUrl") or item.get("url") or "").strip() or (
            f"https://www.tiktok.com/@{username}/video/{video_id}" if video_id else ""
        )

        return TikTokPost(
            video_id=video_id,
            date_time=datetime.fromtimestamp(create_time, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if create_time else "",
            create_time=create_time,
            description=description,
            hashtags=hashtags,
            mentions=mentions,
            likes=self._safe_int_metric(
                stats.get("diggCount")
                or stats.get("digg_count")
                or stats_v2.get("diggCount")
                or stats_v2.get("digg_count")
                or item.get("diggCount")
                or item.get("digg_count")
                or 0
            ),
            comments=self._safe_int_metric(
                stats.get("commentCount")
                or stats.get("comment_count")
                or stats_v2.get("commentCount")
                or stats_v2.get("comment_count")
                or item.get("commentCount")
                or item.get("comment_count")
                or 0
            ),
            shares=self._safe_int_metric(
                stats.get("shareCount")
                or stats.get("share_count")
                or stats_v2.get("shareCount")
                or stats_v2.get("share_count")
                or item.get("shareCount")
                or item.get("share_count")
                or 0
            ),
            saves=self._safe_int_metric(
                stats.get("collectCount")
                or stats.get("collect_count")
                or stats.get("favoriteCount")
                or stats.get("favorite_count")
                or stats_v2.get("collectCount")
                or stats_v2.get("collect_count")
                or stats_v2.get("favoriteCount")
                or stats_v2.get("favorite_count")
                or item.get("collectCount")
                or item.get("collect_count")
                or item.get("favoriteCount")
                or item.get("favorite_count")
                or 0
            ),
            views=self._safe_int_metric(
                stats.get("playCount")
                or stats.get("play_count")
                or stats_v2.get("playCount")
                or stats_v2.get("play_count")
                or item.get("playCount")
                or item.get("play_count")
                or 0
            ),
            url=canonical_url,
            username=username,
            author_nickname=nickname,
            duration=duration,
            music_title=str(music.get("title") or music_meta.get("musicName") or music_meta.get("title") or ""),
            music_author=str(
                music.get("authorName")
                or music.get("author_name")
                or music_meta.get("musicAuthor")
                or music_meta.get("authorName")
                or ""
            ),
            user_avatar_url=user_avatar_url,
            media_urls=media_urls,
            thumbnail_url=thumbnail_url or (media_urls[0] if media_urls else None),
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _extract_user_detail_avatar_url(self, user_data: dict | None) -> str | None:
        if not isinstance(user_data, dict):
            return None
        user_info = _dict_or_empty(user_data.get("userInfo"))
        user = _dict_or_empty(user_info.get("user"))
        return self._pick_best_avatar_url(
            user.get("avatarLarger"),
            user.get("avatar_larger"),
            user.get("originalAvatarUrl"),
            user.get("avatarMedium"),
            user.get("avatar_medium"),
            user.get("avatarThumb"),
            user.get("avatar_thumb"),
            user.get("avatar_thumb_url"),
            user.get("avatarUrl"),
            user.get("avatar"),
        )

    def _extract_user_detail_profile_snapshot(self, user_data: dict | None, username: str) -> dict[str, Any]:
        if not isinstance(user_data, dict):
            return {}
        user_info = _dict_or_empty(user_data.get("userInfo"))
        user = _dict_or_empty(user_info.get("user"))
        stats = _dict_or_empty(user_info.get("stats"))
        resolved_username = str(
            user.get("uniqueId") or user.get("unique_id") or user.get("uniqueIdModifyTime") or username or ""
        ).strip()
        if "@" in resolved_username:
            resolved_username = resolved_username.split("@")[-1].strip()
        profile_url = f"https://www.tiktok.com/@{resolved_username}" if resolved_username else None
        return {
            "username": resolved_username or username,
            "sec_uid": str(user.get("secUid") or user.get("sec_uid") or "").strip() or None,
            "display_name": str(user.get("nickname") or user.get("nickName") or "").strip() or None,
            "bio": str(user.get("signature") or user.get("bio") or "").strip() or None,
            "avatar_url": self._extract_user_detail_avatar_url(user_data),
            "is_verified": bool(user.get("verified") or user.get("isVerified") or False),
            "follower_count": self._safe_int_metric(
                stats.get("followerCount") or stats.get("follower_count") or user.get("followerCount") or 0
            ),
            "following_count": self._safe_int_metric(
                stats.get("followingCount") or stats.get("following_count") or user.get("followingCount") or 0
            ),
            "total_posts": self._safe_int_metric(
                stats.get("videoCount")
                or stats.get("video_count")
                or stats.get("itemCount")
                or stats.get("item_count")
                or user.get("videoCount")
                or user.get("video_count")
                or user.get("itemCount")
                or user.get("item_count")
                or 0
            ),
            "profile_url": profile_url,
        }

    def _extract_html_profile_snapshot(self, data: dict | None, username: str) -> dict[str, Any]:
        if not isinstance(data, dict):
            return {}
        default_scope = data.get("__DEFAULT_SCOPE__", {}) if isinstance(data.get("__DEFAULT_SCOPE__"), dict) else {}
        webapp_detail = (
            default_scope.get("webapp.user-detail", {})
            if isinstance(default_scope.get("webapp.user-detail"), dict)
            else {}
        )
        user_info = webapp_detail.get("userInfo", {}) if isinstance(webapp_detail.get("userInfo"), dict) else {}
        user = user_info.get("user", {}) if isinstance(user_info.get("user"), dict) else {}
        stats = user_info.get("stats", {}) if isinstance(user_info.get("stats"), dict) else {}
        item_list = user_info.get("itemList", []) if isinstance(user_info.get("itemList"), list) else []
        user_module = data.get("UserModule", {}) if isinstance(data.get("UserModule"), dict) else {}
        users = user_module.get("users", {}) if isinstance(user_module.get("users"), dict) else {}
        user_fallback = users.get(username, {}) if isinstance(users.get(username), dict) else {}
        stats_module = user_module.get("stats", {}) if isinstance(user_module.get("stats"), dict) else {}
        stats_fallback = stats_module.get(username, {}) if isinstance(stats_module.get(username), dict) else {}
        resolved_username = str(
            user.get("uniqueId") or user.get("unique_id") or user_fallback.get("uniqueId") or username or ""
        ).strip()
        profile_url = f"https://www.tiktok.com/@{resolved_username}" if resolved_username else None
        return {
            "username": resolved_username or username,
            "sec_uid": str(user.get("secUid") or user_fallback.get("secUid") or "").strip() or None,
            "display_name": str(
                user.get("nickname")
                or user.get("nickName")
                or user_fallback.get("nickname")
                or user_fallback.get("nickName")
                or ""
            ).strip()
            or None,
            "bio": (
                str(user.get("signature") or user.get("bio") or user_fallback.get("signature") or "").strip() or None
            ),
            "avatar_url": self._pick_best_avatar_url(
                user.get("avatarLarger"),
                user.get("avatar_larger"),
                user.get("originalAvatarUrl"),
                user.get("avatarMedium"),
                user.get("avatar_medium"),
                user.get("avatarThumb"),
                user.get("avatar_thumb"),
                user.get("avatar_thumb_url"),
                user.get("avatarUrl"),
                user.get("avatar"),
                user_fallback.get("avatarLarger"),
                user_fallback.get("avatar_larger"),
                user_fallback.get("originalAvatarUrl"),
                user_fallback.get("avatarMedium"),
                user_fallback.get("avatar_medium"),
                user_fallback.get("avatarThumb"),
                user_fallback.get("avatar_thumb"),
                user_fallback.get("avatar_thumb_url"),
                user_fallback.get("avatarUrl"),
                user_fallback.get("avatar"),
            ),
            "is_verified": bool(
                user.get("verified") or user.get("isVerified") or user_fallback.get("verified") or False
            ),
            "follower_count": self._safe_int_metric(
                stats.get("followerCount")
                or stats.get("follower_count")
                or stats_fallback.get("followerCount")
                or stats_fallback.get("follower_count")
                or 0
            ),
            "following_count": self._safe_int_metric(
                stats.get("followingCount")
                or stats.get("following_count")
                or stats_fallback.get("followingCount")
                or stats_fallback.get("following_count")
                or 0
            ),
            "total_posts": max(
                self._safe_int_metric(
                    stats.get("videoCount")
                    or stats.get("video_count")
                    or stats.get("itemCount")
                    or stats.get("item_count")
                    or stats_fallback.get("videoCount")
                    or stats_fallback.get("video_count")
                    or stats_fallback.get("itemCount")
                    or stats_fallback.get("item_count")
                    or 0
                ),
                len(item_list),
            ),
            "profile_url": profile_url,
        }

    @staticmethod
    def _merge_profile_snapshots(*snapshots: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                continue
            for key in ("username", "sec_uid", "display_name", "bio", "avatar_url", "profile_url"):
                value = str(snapshot.get(key) or "").strip()
                if value and not str(merged.get(key) or "").strip():
                    merged[key] = value
            for key in ("follower_count", "following_count", "total_posts"):
                if snapshot.get(key) is None:
                    continue
                merged[key] = max(int(merged.get(key) or 0), max(0, int(snapshot.get(key) or 0)))
            if snapshot.get("is_verified"):
                merged["is_verified"] = True
        return merged

    def build_profile_snapshot(
        self,
        username: str,
        *,
        user_data: dict | None = None,
        html_data: dict | None = None,
    ) -> dict[str, Any]:
        return self._merge_profile_snapshots(
            self._extract_user_detail_profile_snapshot(user_data, username),
            self._extract_html_profile_snapshot(html_data, username),
        )

    def fetch_user_detail(self, username: str, delay: float = 2.0) -> dict | None:
        """Fetch user detail to get secUid needed for post list."""
        self._rate_limit(delay)

        params = {"uniqueId": username}
        headers = self._get_headers(f"https://www.tiktok.com/@{username}")

        try:
            response = self.session.get(
                self.USER_DETAIL_URL,
                params=params,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            parsed = self._safe_response_json(response, endpoint="fetch_user_detail")
            if parsed is None:
                return None
            return parsed
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch user detail for @{username}: {e}")
            self._last_api_fail_reason = "request_error"
            response = getattr(e, "response", None)
            self._record_endpoint_response(
                endpoint="fetch_user_detail",
                failure_reason=self._last_api_fail_reason,
                response=response,
            )
            return None

    def fetch_posts(
        self,
        username: str,
        sec_uid: str,
        cursor: int = 0,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
    ) -> dict | None:
        """Fetch posts from user's profile."""
        self._rate_limit(delay, fast_mode=fast_mode)

        params = {
            "secUid": sec_uid,
            "count": 30,
            "cursor": cursor,
        }
        headers = self._get_headers(f"https://www.tiktok.com/@{username}")

        try:
            response = self.session.get(
                self.USER_POST_URL,
                params=params,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            parsed = self._safe_response_json(response, endpoint="fetch_posts")
            if parsed is None:
                return None
            self._record_endpoint_response(endpoint="fetch_posts", response=response)
            return parsed
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch posts: {e}")
            self._last_api_fail_reason = "request_error"
            response = getattr(e, "response", None)
            self._record_endpoint_response(
                endpoint="fetch_posts",
                failure_reason=self._last_api_fail_reason,
                response=response,
            )
            return None

    def _browser_cookie_payload(self) -> list[dict[str, Any]]:
        """Convert flat cookie dict to Playwright's ``add_cookies`` format."""
        cookies: list[dict[str, Any]] = []
        for name, value in (self.cookies or {}).items():
            if not str(name or "").strip() or not str(value or "").strip():
                continue
            cookies.append(
                {
                    "name": str(name),
                    "value": str(value),
                    "domain": ".tiktok.com",
                    "path": "/",
                    "secure": True,
                }
            )
        return cookies

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

    # ------------------------------------------------------------------
    # Browser-intercept scraping (Playwright scroll + response capture)
    # ------------------------------------------------------------------

    def _scrape_browser_intercept(
        self,
        config: TikTokScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[TikTokPost]:
        """Scrape by scrolling a headless browser and intercepting API responses.

        Navigates to the TikTok profile, auto-scrolls, and captures post data
        from the ``/api/post/item_list/`` responses TikTok sends during its own
        infinite-scroll pagination.  Zero rate-limiting overhead because requests
        originate from a real browser session.
        """
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            logger.error("Playwright not available for browser_intercept mode: %s", exc)
            self.last_retrieval_meta.update(
                {
                    "retrieval_mode": "browser_intercept",
                    "auth_mode": "with_cookies" if self.cookies else "without_cookies",
                    "error_code": "playwright_unavailable",
                    "error_class": type(exc).__name__,
                    "playwright_error": type(exc).__name__,
                    "intercepted_post_responses": 0,
                    "intercepted_user_detail_responses": 0,
                    "dom_cards_seen": 0,
                    "scroll_iterations": 0,
                }
            )
            return []

        from trr_backend.socials.browser_cookie_refresh import launch_browser

        logger.info(
            "Starting browser_intercept scrape for @%s",
            config.username,
        )

        posts: list[TikTokPost] = []
        seen_ids: set[str] = set()
        reached_date_limit = False
        no_new_data_scrolls = 0
        max_no_new_data_scrolls = 5
        scroll_count = 0
        intercepted_post_responses = 0
        intercepted_user_detail_responses = 0
        dom_cards_seen = 0
        playwright_error: str | None = None
        auth_mode = "with_cookies" if self.cookies else "without_cookies"

        self.last_retrieval_meta.update(
            {
                "retrieval_mode": "browser_intercept",
                "auth_mode": auth_mode,
                "intercepted_post_responses": intercepted_post_responses,
                "intercepted_user_detail_responses": intercepted_user_detail_responses,
                "dom_cards_seen": dom_cards_seen,
                "scroll_iterations": scroll_count,
                "playwright_error": playwright_error,
            }
        )

        user_agent = self._get_headers().get("user-agent", "Mozilla/5.0")
        timeout_ms = 60_000
        max_posts = config.max_pages * TIKTOK_POSTS_PER_PAGE if config.max_pages else TIKTOK_DEFAULT_MAX_POSTS

        browser: Any | None = None
        context: Any | None = None
        try:
            with sync_playwright() as playwright:
                browser = launch_browser(playwright, headless=True)
                assert browser is not None  # launch_browser always returns a browser
                context = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent=user_agent,
                )
                assert context is not None  # new_context always returns a context
                browser_cookies = self._browser_cookie_payload()
                if browser_cookies:
                    context.add_cookies(browser_cookies)
                page = context.new_page()

                # Navigate to profile
                page.goto(
                    f"https://www.tiktok.com/@{config.username}",
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                page.wait_for_timeout(3_000)

                # Build profile snapshot for avatar backfill
                profile_avatar_url: str | None = None
                try:
                    user_data = self.fetch_user_detail(config.username, 0)
                    if user_data:
                        snapshot = self.build_profile_snapshot(
                            config.username,
                            user_data=user_data,
                        )
                        profile_avatar_url = str(snapshot.get("avatar_url") or "").strip() or None
                except Exception:  # noqa: BLE001
                    pass

                # Register response interceptor for browser traffic relevant to recovery triage.
                def _handle_post_list_response(response: Any) -> None:
                    nonlocal intercepted_post_responses, intercepted_user_detail_responses, reached_date_limit
                    nonlocal no_new_data_scrolls
                    try:
                        response_url = str(response.url or "")
                        normalized_response_url = response_url.lower()
                        if (
                            "/api/user/detail/" in normalized_response_url
                            or "fetch_user_detail" in normalized_response_url
                        ):
                            if response.ok:
                                try:
                                    browser_user_detail_payload = response.json()
                                except Exception:  # noqa: BLE001
                                    browser_user_detail_payload = None
                                if isinstance(browser_user_detail_payload, (dict, list)):
                                    intercepted_user_detail_responses += 1
                                    self.last_retrieval_meta["intercepted_user_detail_responses"] = (
                                        intercepted_user_detail_responses
                                    )
                        if "/api/post/item_list/" not in normalized_response_url:
                            return
                        intercepted_post_responses += 1
                        self.last_retrieval_meta["intercepted_post_responses"] = intercepted_post_responses
                        if not response.ok:
                            return
                        payload = response.json()
                        items = payload.get("itemList") or []
                        if not items:
                            return

                        new_posts_this_batch = 0
                        for item in items:
                            video_id = str(item.get("id") or item.get("aweme_id") or item.get("videoId") or "")
                            if not video_id or video_id in seen_ids:
                                continue
                            seen_ids.add(video_id)

                            # Check date range
                            create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
                            if create_time <= 0:
                                continue
                            in_range = config.is_in_date_range(create_time)
                            if in_range is None:  # Before date range — stop
                                reached_date_limit = True
                                return
                            if in_range is False:  # After date range — skip
                                continue

                            # Check hashtag filter
                            description = str(item.get("desc") or item.get("text") or "")
                            if config.matches_hashtags(description):
                                post = self._parse_post_item(item, config)
                                if profile_avatar_url and not post.user_avatar_url:
                                    post.user_avatar_url = profile_avatar_url
                                posts.append(post)
                                new_posts_this_batch += 1

                        if new_posts_this_batch > 0:
                            no_new_data_scrolls = 0
                            logger.info(
                                "browser_intercept: +%d posts (total: %d)",
                                new_posts_this_batch,
                                len(posts),
                            )
                            self._emit_progress(
                                progress_cb,
                                phase="browser_intercept_batch",
                                pages_scanned=scroll_count,
                                posts_checked=len(seen_ids),
                                matched_posts=len(posts),
                            )
                    except Exception:  # noqa: BLE001
                        pass  # Ignore parse errors — don't break the scroll loop

                page.on("response", _handle_post_list_response)

                # Auto-scroll loop
                scroll_interval_ms = 800
                while not reached_date_limit and no_new_data_scrolls < max_no_new_data_scrolls:
                    if len(posts) >= max_posts:
                        logger.info(
                            "browser_intercept: reached max posts (%d)",
                            max_posts,
                        )
                        break

                    prev_count = len(posts)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(scroll_interval_ms)
                    scroll_count += 1

                    # Check if we got new posts from the intercepted responses
                    if len(posts) == prev_count:
                        no_new_data_scrolls += 1
                    else:
                        no_new_data_scrolls = 0
                    dom_cards_seen = max(dom_cards_seen, int(page.locator("a[href*='/video/']").count() or 0))
                    self.last_retrieval_meta["dom_cards_seen"] = dom_cards_seen
                    self.last_retrieval_meta["scroll_iterations"] = scroll_count

                    if scroll_count % 20 == 0:
                        logger.info(
                            "browser_intercept: scrolled %d times, %d posts collected",
                            scroll_count,
                            len(posts),
                        )

        except Exception as exc:
            playwright_error = type(exc).__name__
            logger.error(
                "browser_intercept failed for @%s: %s",
                config.username,
                exc,
            )
            self.last_retrieval_meta.update(
                {
                    "retrieval_mode": "browser_intercept",
                    "error_code": "browser_intercept_error",
                    "error_class": playwright_error,
                    "error_message": str(exc),
                    "intercepted_post_responses": intercepted_post_responses,
                    "intercepted_user_detail_responses": intercepted_user_detail_responses,
                    "dom_cards_seen": dom_cards_seen,
                    "scroll_iterations": scroll_count,
                    "playwright_error": playwright_error,
                }
            )
        finally:
            # Close context first so pages/cookies are flushed; wrap each
            # close independently so one teardown failure doesn't skip the
            # other. The outer `with sync_playwright()` also cleans up, but
            # explicit close releases FDs sooner under high concurrency.
            if context is not None:
                try:
                    context.close()
                except Exception:  # noqa: BLE001 - teardown best-effort
                    logger.debug("tiktok playwright context close failed", exc_info=True)
            if browser is not None:
                try:
                    browser.close()
                except Exception:  # noqa: BLE001 - teardown best-effort
                    logger.debug("tiktok playwright browser close failed", exc_info=True)

        stop_reason = (
            "date_start_reached"
            if reached_date_limit
            else "max_posts_reached"
            if len(posts) >= max_posts
            else "no_new_data"
            if no_new_data_scrolls >= max_no_new_data_scrolls
            else "unknown"
        )
        logger.info(
            "browser_intercept complete for @%s: %d posts in %d scrolls (stop: %s)",
            config.username,
            len(posts),
            scroll_count,
            stop_reason,
        )
        error_code = str(self.last_retrieval_meta.get("error_code") or "").strip() or None
        error_class = str(self.last_retrieval_meta.get("error_class") or "").strip() or None
        error_message = str(self.last_retrieval_meta.get("error_message") or "").strip() or None
        fallback_chain = self.last_retrieval_meta.get("fallback_chain") or None
        self._set_retrieval_meta(
            retrieval_mode="browser_intercept",
            auth_mode=auth_mode,
            posts_checked=len(seen_ids),
            pages_scanned=scroll_count,
            stop_reason=stop_reason,
            intercepted_post_responses=intercepted_post_responses,
            intercepted_user_detail_responses=intercepted_user_detail_responses,
            dom_cards_seen=dom_cards_seen,
            scroll_iterations=scroll_count,
            playwright_error=playwright_error,
            error_code=error_code,
            error_class=error_class,
            error_message=error_message,
            fallback_chain=fallback_chain,
        )
        self.last_retrieval_meta["playwright_error"] = playwright_error
        return posts

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

    def _classify_browser_intercept_failure(
        self,
        *,
        posts_found: int,
        intercepted_post_responses: int,
        intercepted_user_detail_responses: int,
        dom_cards_seen: int,
        scroll_iterations: int,
        authenticated: bool,
        playwright_error: str | None,
    ) -> str:
        if playwright_error:
            return "playwright_runtime_change"
        if not authenticated:
            return "auth_or_session_state"
        if intercepted_post_responses == 0 and intercepted_user_detail_responses > 0:
            return "interception_target_drift"
        if dom_cards_seen == 0 and scroll_iterations > 0:
            return "scroll_or_pagination_drift"
        if posts_found <= 0:
            return "unclassified_zero_posts"
        return "healthy"

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

    def _scrape_api(
        self,
        config: TikTokScrapeConfig,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[TikTokPost]:
        """Scrape via the direct TikTok web API (original implementation)."""

        html_data: dict | None = None

        # Get user detail first to get secUid
        user_data = self.fetch_user_detail(config.username, config.delay_seconds)
        api_preflight_fail_reason = self._last_api_fail_reason

        sec_uid = None
        use_api = False
        api_pagination_blocked_reason: str | None = None
        api_posts_found = False
        ytdlp_used = False

        if user_data and user_data.get("userInfo", {}).get("user", {}).get("secUid"):
            user_info = user_data.get("userInfo", {})
            user = user_info.get("user", {})
            sec_uid = user.get("secUid")
            use_api = True
            logger.info(f"Found user: {user.get('nickname')} (@{config.username})")
        else:
            logger.warning(f"API returned no data for @{config.username}; falling back to HTML scraping")

        # Try HTML scraping fallback to get initial posts + secUid
        html_posts: list[dict] = []
        if not use_api:
            html_data = self._fetch_profile_html(config.username, config.delay_seconds)
            if html_data:
                html_posts, sec_uid = self._extract_posts_from_html_data(html_data, config.username)
                if html_posts:
                    logger.info(f"Extracted {len(html_posts)} posts from HTML page")
                if sec_uid:
                    logger.info(f"Got secUid from HTML: {sec_uid[:20]}...")
                    if self._should_skip_api_pagination(api_preflight_fail_reason):
                        api_pagination_blocked_reason = api_preflight_fail_reason
                        logger.info(
                            "Skipping TikTok API pagination for @%s after poisoned preflight (%s)",
                            config.username,
                            api_preflight_fail_reason,
                        )
                    else:
                        use_api = True  # Can try API for pagination now

        # Process HTML-extracted posts first
        profile_snapshot = self.build_profile_snapshot(config.username, user_data=user_data, html_data=html_data)
        profile_avatar_url = str(profile_snapshot.get("avatar_url") or "").strip() or None

        posts = []
        existing_ids: set[str] = set()
        posts_checked = 0
        reached_date_limit = False
        pages_scanned = 0

        for item in html_posts:
            posts_checked += 1
            create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
            if create_time <= 0:
                continue

            in_range = config.is_in_date_range(create_time)
            if in_range is None:
                reached_date_limit = True
                break
            if in_range is False:
                continue

            description = str(item.get("desc") or item.get("text") or "")
            if config.matches_hashtags(description):
                post = self._parse_post_item(item, config)
                if not getattr(post, "author_nickname", None) and profile_snapshot.get("display_name"):
                    post.author_nickname = str(profile_snapshot.get("display_name") or "")
                if not getattr(post, "user_avatar_url", None) and profile_avatar_url:
                    post.user_avatar_url = profile_avatar_url
                if post.video_id and post.video_id not in existing_ids:
                    posts.append(post)
                    existing_ids.add(post.video_id)
                logger.info(f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views")
        if html_posts:
            self._emit_progress(
                progress_cb,
                phase="scrape_html_page",
                pages_scanned=1,
                posts_checked=posts_checked,
                matched_posts=len(posts),
            )

        # Continue with API pagination if we have secUid and haven't hit date limit
        if sec_uid and use_api and not reached_date_limit:
            cursor = 0
            page_num = 0

            while not reached_date_limit:
                page_num += 1
                pages_scanned = page_num
                if config.max_pages and page_num > config.max_pages:
                    logger.info(f"Reached max pages limit ({config.max_pages})")
                    break

                logger.info(f"Fetching API page {page_num}...")
                data = self.fetch_posts(
                    config.username,
                    sec_uid,
                    cursor,
                    config.delay_seconds,
                    fast_mode=config.fast_mode,
                )
                if not data:
                    break

                items = data.get("itemList", [])
                if not items:
                    logger.info("No more posts found")
                    break

                for item in items:
                    posts_checked += 1
                    create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
                    if create_time <= 0:
                        continue

                    in_range = config.is_in_date_range(create_time)
                    if in_range is None:
                        reached_date_limit = True
                        break
                    if in_range is False:
                        continue

                    description = str(item.get("desc") or item.get("text") or "")
                    if config.matches_hashtags(description):
                        post = self._parse_post_item(item, config)
                        if not getattr(post, "author_nickname", None) and profile_snapshot.get("display_name"):
                            post.author_nickname = str(profile_snapshot.get("display_name") or "")
                        if not getattr(post, "user_avatar_url", None) and profile_avatar_url:
                            post.user_avatar_url = profile_avatar_url
                        # Avoid duplicates from HTML extraction / prior pages.
                        if post.video_id and post.video_id not in existing_ids:
                            posts.append(post)
                            existing_ids.add(post.video_id)
                            api_posts_found = True
                            logger.info(
                                f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views"
                            )
                self._emit_progress(
                    progress_cb,
                    phase="scrape_api_page",
                    pages_scanned=page_num,
                    posts_checked=posts_checked,
                    matched_posts=len(posts),
                )

                has_more = data.get("hasMore", False)
                cursor = data.get("cursor", 0)
                if not has_more or not cursor:
                    logger.info("No more pages available")
                    break

                logger.info(f"Page {page_num}: checked {len(items)} posts, {len(posts)} matches total")

        logger.info(f"Scrape complete: checked {posts_checked} posts, found {len(posts)} matches")

        # yt-dlp fallback: always try when API/HTML found few or no matching posts,
        # since yt-dlp can paginate much deeper into the profile history.
        if len(posts) < 5 and self._has_ytdlp():
            logger.info(
                f"API/HTML found only {len(posts)} posts in date range; "
                "trying yt-dlp bulk fallback for deeper pagination..."
            )
            ytdlp_posts = self._scrape_via_ytdlp(
                config,
                max_videos_hint=config.ytdlp_max_videos_hint,
                max_posts_hint=config.ytdlp_max_videos_hint,
                progress_cb=progress_cb,
            )
            ytdlp_used = bool(ytdlp_posts)
            ytdlp_meta = dict(self.last_retrieval_meta or {})
            for p in ytdlp_posts:
                if p.video_id and p.video_id not in existing_ids:
                    posts.append(p)
                    existing_ids.add(p.video_id)
            if ytdlp_posts:
                posts_checked = max(
                    posts_checked,
                    self._safe_int_metric(ytdlp_meta.get("posts_checked") or ytdlp_meta.get("videos_scanned") or 0),
                    len(posts),
                )
                logger.info(f"yt-dlp added {len(ytdlp_posts)} posts (total now {len(posts)})")
                self._emit_progress(
                    progress_cb,
                    phase="scrape_ytdlp_fallback",
                    pages_scanned=pages_scanned,
                    posts_checked=max(posts_checked, len(posts)),
                    matched_posts=len(posts),
                )

        # Backfill missing thumbnails via oembed API
        missing_thumb = [p for p in posts if not p.thumbnail_url and p.url]
        if missing_thumb:
            logger.info(f"Backfilling thumbnails via oembed for {len(missing_thumb)} posts...")
            filled = 0
            for p in missing_thumb:
                thumb = self._fetch_oembed_thumbnail(p.url)
                if thumb:
                    p.thumbnail_url = thumb
                    filled += 1
            if filled:
                logger.info(f"Oembed backfill: {filled}/{len(missing_thumb)} thumbnails resolved")

        self._set_retrieval_meta(
            retrieval_mode=(
                "ytdlp_fallback" if ytdlp_used else ("api" if api_posts_found else ("html" if html_posts else "none"))
            ),
            api_fail_reason=self._last_api_fail_reason,
            api_preflight_fail_reason=api_preflight_fail_reason,
            api_pagination_blocked_reason=api_pagination_blocked_reason,
            pages_scanned=pages_scanned,
            posts_checked=posts_checked,
            videos_scanned=posts_checked,
            first_page_count=len(posts[:30]),
            total_posts=max(self._safe_int_metric(profile_snapshot.get("total_posts")), len(posts)),
            profile_snapshot=profile_snapshot,
        )
        return posts

    def fetch_comments(
        self,
        video_id: str,
        username: str | None = None,
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        max_replies_per_comment: int | None = None,
        max_reply_pages: int | None = None,
        reply_fetch_deadline_seconds: float | None = None,
    ) -> list["TikTokComment"]:
        """
        Fetch comments for a TikTok video including replies.

        Args:
            video_id: The video ID (aweme_id)
            username: Username for building the post URL (optional)
            max_comments: Maximum number of top-level comments to fetch
            fetch_replies: Whether to fetch replies to comments
            delay: Delay between API requests
            max_replies_per_comment: Maximum replies to fetch for each top-level comment
            max_reply_pages: Maximum reply API pages to fetch for each top-level comment
            reply_fetch_deadline_seconds: Shared reply-fetch deadline for this video

        Returns:
            List of TikTokComment objects with nested replies
        """
        self._last_api_fail_reason = None
        self.last_comment_fetch_reason = None
        self.comments_auth_failed = False
        self.last_comment_fetch_meta = {}
        if not self._direct_comment_api_experiment_enabled():
            self.last_comment_fetch_reason = "direct_api_parked"
            self.last_comment_fetch_meta = {
                "retrieval_mode": "direct_api_parked",
                "reason": "direct_api_parked",
            }
            logger.warning("tiktok_comments_direct_api_parked video_id=%s", video_id)
            return []
        max_replies_per_comment = self._normalize_optional_non_negative_int(max_replies_per_comment)
        max_reply_pages = self._normalize_optional_non_negative_int(max_reply_pages)
        normalized_reply_deadline_seconds = self._normalize_optional_non_negative_float(reply_fetch_deadline_seconds)
        reply_deadline_at = (
            time.monotonic() + normalized_reply_deadline_seconds
            if normalized_reply_deadline_seconds is not None
            else None
        )
        self.last_comment_fetch_meta = {
            "retrieval_mode": "direct_api",
            "reply_cap_events": [],
            "reply_caps": {
                "max_replies_per_comment": max_replies_per_comment,
                "max_reply_pages": max_reply_pages,
                "reply_fetch_deadline_seconds": normalized_reply_deadline_seconds,
            },
        }
        post_url = f"https://www.tiktok.com/@{username}/video/{video_id}" if username else ""
        logger.info(f"Fetching comments for video {video_id}")

        comments = []
        cursor = 0
        comments_fetched = 0

        def _emit_progress(phase: str, **payload: Any) -> None:
            if progress_callback is None:
                return
            event = {
                "phase": phase,
                "video_id": video_id,
                "comments_fetched": comments_fetched,
                **payload,
            }
            try:
                progress_callback(event)
            except Exception:  # noqa: BLE001
                logger.debug("TikTok comment progress callback failed video_id=%s", video_id, exc_info=True)

        _emit_progress("fetch_tiktok_comments_start")

        while True:
            self._rate_limit(delay, fast_mode=fast_mode)

            params = {
                "aweme_id": video_id,
                "count": 50,
                "cursor": cursor,
                # TikTok's comments endpoints currently require aid=1988.
                "aid": 1988,
            }
            headers = self._get_headers(post_url or "https://www.tiktok.com/")
            _emit_progress("fetch_tiktok_comments_request", cursor=cursor)

            try:
                response = self.session.get(
                    self.COMMENTS_URL,
                    params=params,
                    headers=headers,
                    cookies=self.cookies,
                    timeout=self.COMMENT_REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                data = self._safe_response_json(response, endpoint="fetch_comments")
                if data is not None:
                    self._record_endpoint_response(endpoint="fetch_comments", response=response)
            except requests.exceptions.RequestException as e:
                self._set_comment_failure_reason("request_error")
                self._record_endpoint_response(
                    endpoint="fetch_comments",
                    failure_reason=self._last_api_fail_reason,
                    response=getattr(e, "response", None),
                )
                logger.error(f"Failed to fetch comments: {e}")
                break
            if not data:
                if self._last_api_fail_reason:
                    self._set_comment_failure_reason(self._last_api_fail_reason)
                else:
                    self._set_comment_failure_reason("empty_response")
                break

            status_code = int(data.get("status_code", 0) or 0)
            if status_code != 0:
                self._set_comment_failure_reason(f"comment_status_{status_code}")
                logger.warning(
                    "TikTok comments API returned non-zero status (video_id=%s status_code=%s status_msg=%s)",
                    video_id,
                    status_code,
                    data.get("status_msg", ""),
                )
                break

            # Parse comments
            for comment_data in data.get("comments", []):
                comment = self._parse_comment(comment_data, video_id, post_url)
                comments.append(comment)
                comments_fetched += 1

                # Fetch replies if requested and comment has replies
                if fetch_replies and comment.reply_count > 0:
                    replies = self._fetch_comment_replies(
                        video_id,
                        comment.comment_id,
                        post_url,
                        delay,
                        fast_mode=fast_mode,
                        progress_callback=progress_callback,
                        max_replies=max_replies_per_comment,
                        max_pages=max_reply_pages,
                        deadline_at=reply_deadline_at,
                        deadline_seconds=normalized_reply_deadline_seconds,
                    )
                    comment.replies = replies
                    logger.info(f"  Comment {comment.comment_id}: {len(replies)} replies fetched")

                if max_comments and comments_fetched >= max_comments:
                    break

            logger.info(f"Fetched {len(comments)} comments so far...")
            _emit_progress("fetch_tiktok_comments_page", cursor=cursor, comments_fetched=len(comments))

            # Check for more pages
            if max_comments and comments_fetched >= max_comments:
                break
            if not data.get("has_more", False):
                break
            cursor = data.get("cursor", 0)
            if not cursor:
                break

        logger.info(f"Total: {len(comments)} comments fetched for video {video_id}")
        _emit_progress("fetch_tiktok_comments_done", comments_fetched=len(comments))
        return comments

    def _fetch_comment_replies(
        self,
        video_id: str,
        comment_id: str,
        post_url: str,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        max_replies: int | None = None,
        max_pages: int | None = None,
        deadline_at: float | None = None,
        deadline_seconds: float | None = None,
    ) -> list["TikTokComment"]:
        """Fetch replies to a specific comment."""
        replies = []
        cursor = 0
        pages_fetched = 0
        cap_recorded = False

        def _record_cap(reason: str, *, limit: Any = None) -> None:
            nonlocal cap_recorded
            if cap_recorded:
                return
            cap_recorded = True
            self._record_reply_cap_event(
                reason=reason,
                comment_id=comment_id,
                limit=limit,
                replies_fetched=len(replies),
                pages_fetched=pages_fetched,
            )

        def _emit_progress(phase: str, **payload: Any) -> None:
            if progress_callback is None:
                return
            event = {
                "phase": phase,
                "video_id": video_id,
                "comment_id": comment_id,
                "replies_fetched": len(replies),
                **payload,
            }
            try:
                progress_callback(event)
            except Exception:  # noqa: BLE001
                logger.debug(
                    "TikTok reply progress callback failed video_id=%s comment_id=%s",
                    video_id,
                    comment_id,
                    exc_info=True,
                )

        while True:
            if max_replies is not None and len(replies) >= max_replies:
                _record_cap("max_replies_per_comment", limit=max_replies)
                break
            if max_pages is not None and pages_fetched >= max_pages:
                _record_cap("max_reply_pages", limit=max_pages)
                break
            if deadline_at is not None and time.monotonic() >= deadline_at:
                _record_cap("reply_deadline_seconds", limit=deadline_seconds)
                break
            self._rate_limit(delay, fast_mode=fast_mode)

            params = {
                "item_id": video_id,
                "comment_id": comment_id,
                "count": 50,
                "cursor": cursor,
                # TikTok's replies endpoint currently requires aid=1988.
                "aid": 1988,
            }
            headers = self._get_headers(post_url or "https://www.tiktok.com/")
            _emit_progress("fetch_tiktok_comment_replies_request", cursor=cursor)

            try:
                response = self.session.get(
                    self.COMMENT_REPLIES_URL,
                    params=params,
                    headers=headers,
                    cookies=self.cookies,
                    timeout=self.COMMENT_REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                data = self._safe_response_json(response, endpoint="fetch_comment_replies")
                if data is not None:
                    self._record_endpoint_response(endpoint="fetch_comment_replies", response=response)
            except requests.exceptions.RequestException as e:
                self._set_comment_failure_reason("request_error")
                self._record_endpoint_response(
                    endpoint="fetch_comment_replies",
                    failure_reason=self._last_api_fail_reason,
                    response=getattr(e, "response", None),
                )
                logger.error(f"Failed to fetch replies for comment {comment_id}: {e}")
                break
            if not data:
                if self._last_api_fail_reason:
                    self._set_comment_failure_reason(self._last_api_fail_reason)
                else:
                    self._set_comment_failure_reason("empty_response")
                break

            status_code = int(data.get("status_code", 0) or 0)
            if status_code != 0:
                self._set_comment_failure_reason(f"reply_status_{status_code}")
                logger.warning(
                    "TikTok replies API returned non-zero status (comment_id=%s status_code=%s status_msg=%s)",
                    comment_id,
                    status_code,
                    data.get("status_msg", ""),
                )
                break
            pages_fetched += 1

            # Parse reply comments
            for reply_data in data.get("comments", []):
                if max_replies is not None and len(replies) >= max_replies:
                    _record_cap("max_replies_per_comment", limit=max_replies)
                    break
                reply = self._parse_comment(reply_data, video_id, post_url, is_reply=True, parent_id=comment_id)
                replies.append(reply)
            _emit_progress("fetch_tiktok_comment_replies_page", cursor=cursor, replies_fetched=len(replies))
            if max_replies is not None and len(replies) >= max_replies:
                _record_cap("max_replies_per_comment", limit=max_replies)
                break

            # Check for more pages
            if not data.get("has_more", False):
                break
            if max_pages is not None and pages_fetched >= max_pages:
                _record_cap("max_reply_pages", limit=max_pages)
                break
            cursor = data.get("cursor", 0)
            if not cursor:
                break

        return replies

    def fetch_comments_concurrent(
        self,
        video_ids: list[tuple[str, str | None]],
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
        max_workers: int | None = None,
        max_replies_per_comment: int | None = None,
        max_reply_pages: int | None = None,
        reply_fetch_deadline_seconds: float | None = None,
    ) -> dict[str, list["TikTokComment"]]:
        """Fetch comments for multiple videos concurrently.

        Args:
            video_ids: List of (video_id, username) tuples
            max_comments: Max comments per video
            fetch_replies: Whether to fetch reply chains
            delay: Base delay between requests
            fast_mode: Use aggressive rate limiting
            max_workers: Concurrency level (default from env or 3)
            max_replies_per_comment: Maximum replies to fetch for each top-level comment
            max_reply_pages: Maximum reply API pages to fetch for each top-level comment
            reply_fetch_deadline_seconds: Shared reply-fetch deadline for each video

        Returns:
            Dict mapping video_id -> list of comments
        """
        if max_workers is None:
            max_workers = int(os.getenv("SOCIAL_TIKTOK_COMMENT_CONCURRENCY", "3"))
        max_workers = max(1, min(max_workers, 8))

        if len(video_ids) <= 1 or max_workers <= 1:
            result: dict[str, list[TikTokComment]] = {}
            for vid, uname in video_ids:
                result[vid] = self.fetch_comments(
                    vid,
                    username=uname,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=delay,
                    fast_mode=fast_mode,
                    max_replies_per_comment=max_replies_per_comment,
                    max_reply_pages=max_reply_pages,
                    reply_fetch_deadline_seconds=reply_fetch_deadline_seconds,
                )
            return result

        rate_lock = threading.Lock()
        original_rate_limit = self._rate_limit

        def _synchronized_rate_limit(d: float, *, fast_mode: bool = False):
            with rate_lock:
                original_rate_limit(d, fast_mode=fast_mode)

        def _fetch_one(vid: str, uname: str | None) -> tuple[str, list[TikTokComment]]:
            self._rate_limit = _synchronized_rate_limit  # type: ignore[assignment]
            try:
                comments = self.fetch_comments(
                    vid,
                    username=uname,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=delay,
                    fast_mode=fast_mode,
                    max_replies_per_comment=max_replies_per_comment,
                    max_reply_pages=max_reply_pages,
                    reply_fetch_deadline_seconds=reply_fetch_deadline_seconds,
                )
                return vid, comments
            except Exception as exc:
                logger.error("Concurrent TikTok comment fetch failed for %s: %s", vid, exc)
                return vid, []

        logger.info(
            "Fetching TikTok comments for %d videos concurrently (workers=%d, fast=%s)",
            len(video_ids),
            max_workers,
            fast_mode,
        )
        results: dict[str, list[TikTokComment]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_one, vid, uname): vid for vid, uname in video_ids}
            completed = 0
            for future in as_completed(futures):
                vid, comments = future.result()
                results[vid] = comments
                completed += 1
                if completed % 10 == 0:
                    logger.info("TikTok concurrent comments: %d/%d done", completed, len(video_ids))

        self._rate_limit = original_rate_limit  # type: ignore[assignment]
        logger.info(
            "TikTok concurrent comment fetch complete: %d videos, %d total comments",
            len(results),
            sum(len(v) for v in results.values()),
        )
        return results

    def _parse_comment(
        self,
        data: dict,
        video_id: str,
        post_url: str,
        is_reply: bool = False,
        parent_id: str | None = None,
    ) -> TikTokComment:
        """Parse comment data into TikTokComment object."""
        created_at = self._coerce_timestamp(
            data.get("create_time") or data.get("createTime") or data.get("createTimeISO") or data.get("timestamp")
        )
        user = _dict_or_empty(data.get("user"))
        resolved_post_url = str(post_url or data.get("videoWebUrl") or "").strip()
        resolved_video_id = str(
            video_id or data.get("aweme_id") or data.get("awemeId") or data.get("item_id") or data.get("itemId") or ""
        ).strip()
        if not resolved_video_id:
            resolved_video_id = self._extract_video_id_from_url(resolved_post_url)
        comment_id = str(data.get("cid") or data.get("id") or "").strip()
        parent_source_comment_id = (
            str(data.get("parentId") or data.get("parent_id") or data.get("parentCid") or parent_id or "").strip()
            or None
        )
        aweme_id = str(data.get("awemeId") or data.get("aweme_id") or resolved_video_id or "").strip() or None
        comment_language = str(data.get("commentLanguage") or data.get("comment_language") or "").strip() or None
        is_author_liked_raw = data.get("isAuthorLiked")
        is_author_liked = bool(is_author_liked_raw) if isinstance(is_author_liked_raw, (bool, int)) else None
        media_urls = self._extract_comment_media_urls(data, user=user)

        parsed_replies: list[TikTokComment] = []
        for row in data.get("replies") or []:
            if not isinstance(row, dict):
                continue
            parsed_replies.append(
                self._parse_comment(
                    row,
                    resolved_video_id,
                    resolved_post_url,
                    is_reply=True,
                    parent_id=comment_id or parent_id,
                )
            )

        reply_count = int(
            data.get("reply_comment_total")
            or data.get("replyCommentTotal")
            or data.get("replyCount")
            or data.get("repliesCount")
            or 0
        )
        if parsed_replies and reply_count <= 0:
            reply_count = len(parsed_replies)

        profile_username = (
            str(
                user.get("unique_id")
                or user.get("uniqueId")
                or data.get("uniqueId")
                or data.get("ownerUsername")
                or user.get("username")
                or ""
            )
            .strip()
            .lstrip("@")
        )

        return TikTokComment(
            comment_id=comment_id,
            text=str(data.get("text") or ""),
            username=profile_username,
            user_id=str(user.get("uid") or data.get("uid") or user.get("id") or ""),
            nickname=str(user.get("nickname") or user.get("nickName") or data.get("nickname") or ""),
            created_at=created_at,
            date_time=(datetime.fromtimestamp(created_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if created_at else ""),
            likes=int(data.get("digg_count") or data.get("diggCount") or data.get("likesCount") or 0),
            is_reply=is_reply,
            parent_comment_id=parent_id,
            reply_count=reply_count,
            replies=parsed_replies,
            avatar_thumbnail_url=(
                str(
                    data.get("avatarThumbnail")
                    or user.get("avatar_thumb")
                    or user.get("avatar_thumb_url")
                    or user.get("avatarLarger")
                    or user.get("avatar_larger")
                    or ""
                ).strip()
                or None
            ),
            comment_language=comment_language,
            is_author_liked=is_author_liked,
            aweme_id=aweme_id,
            parent_source_comment_id=parent_source_comment_id,
            user_url=(
                str(user.get("url") or user.get("profileUrl") or "").strip()
                or (f"https://www.tiktok.com/@{profile_username}" if profile_username else None)
            ),
            user_bio=(str(user.get("bio") or user.get("signature") or "").strip() or None),
            user_avatar_url=self._pick_best_avatar_url(
                user.get("avatarLarger"),
                user.get("avatar_larger"),
                user.get("originalAvatarUrl"),
                user.get("avatarThumb"),
                user.get("avatar_thumb"),
                user.get("avatar_thumb_url"),
                user.get("avatarUrl"),
                user.get("avatar"),
                data.get("avatarThumbnail"),
                data.get("avatarUrl"),
                data.get("avatar"),
            ),
            user_region=(str(user.get("region") or "").strip() or None),
            user_language=(str(user.get("language") or user.get("lang") or "").strip() or None),
            media_urls=media_urls,
            video_id=resolved_video_id,
            post_url=resolved_post_url,
        )

    def _extract_media_urls(self, item: dict) -> list[str]:
        """Extract playable video URLs from a post."""
        urls: list[str] = []

        video = item.get("video") if isinstance(item.get("video"), dict) else {}
        video_meta = item.get("videoMeta") if isinstance(item.get("videoMeta"), dict) else {}

        for payload in (video, video_meta):
            if not payload:
                continue
            best_bitrate_url = self._extract_best_bitrate_video_url(payload)
            if best_bitrate_url:
                urls.append(best_bitrate_url)
            for key in ("playAddr", "downloadAddr", "playUrl", "url"):
                candidate = self._extract_url_value(payload.get(key))
                if candidate and candidate not in urls:
                    urls.append(candidate)

        for row in item.get("mediaUrls") or []:
            candidate = self._extract_url_value(row)
            if candidate and candidate not in urls:
                urls.append(candidate)

        deduped: list[str] = []
        for url in urls:
            normalized = str(url or "").strip()
            if normalized and normalized not in deduped:
                deduped.append(normalized)
        return deduped[:1]

    def _extract_comment_media_urls(self, item: dict[str, Any], *, user: dict[str, Any] | None = None) -> list[str]:
        if not isinstance(item, dict):
            return []

        avatar_candidates = {
            str(value or "").strip()
            for value in (
                item.get("avatarThumbnail"),
                (user or {}).get("avatarThumbnail"),
                (user or {}).get("avatar_thumb"),
                (user or {}).get("avatar_thumb_url"),
                (user or {}).get("avatarUrl"),
                (user or {}).get("avatar"),
                (user or {}).get("originalAvatarUrl"),
                (user or {}).get("avatarLarger"),
                (user or {}).get("avatar_larger"),
            )
            if str(value or "").strip()
        }

        candidates: list[str] = []
        media_keys = (
            "media",
            "medias",
            "mediaUrls",
            "media_urls",
            "images",
            "image",
            "videos",
            "video",
            "gif",
            "gifs",
            "attachments",
            "attachment",
            "stickers",
            "sticker",
            "resource",
            "resources",
        )
        for key in media_keys:
            self._collect_candidate_urls(item.get(key), candidates)

        text = str(item.get("text") or "")
        for matched in URL_RE.findall(text):
            cleaned = matched.rstrip(".,!?;:)]}\"'")
            if cleaned:
                candidates.append(cleaned)

        deduped: list[str] = []
        for candidate in candidates:
            normalized = str(candidate or "").strip()
            if not normalized or normalized in deduped:
                continue
            if normalized in avatar_candidates or self._looks_like_avatar_url(normalized):
                continue
            if not self._is_allowed_comment_media_url(normalized):
                continue
            deduped.append(normalized)
        return deduped

    def _collect_candidate_urls(self, value: Any, collector: list[str]) -> None:
        if value is None:
            return
        if isinstance(value, str):
            normalized = value.strip()
            if normalized.startswith("http://") or normalized.startswith("https://"):
                collector.append(normalized)
            return
        if isinstance(value, list):
            for item in value:
                self._collect_candidate_urls(item, collector)
            return
        if isinstance(value, dict):
            for key in (
                "url",
                "src",
                "uri",
                "image",
                "imageUrl",
                "image_url",
                "gifUrl",
                "gif_url",
                "videoUrl",
                "video_url",
                "playAddr",
                "downloadAddr",
                "play_url",
                "download_url",
                "url_list",
                "UrlList",
            ):
                self._collect_candidate_urls(value.get(key), collector)
            return

    @staticmethod
    def _is_allowed_comment_media_url(url: str) -> bool:
        parsed = urlparse(url)
        host = str(parsed.netloc or "").lower()
        path = str(parsed.path or "").lower()
        if "tiktokcdn" not in host:
            return False
        if any(path.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".mov", ".webm")):
            return True
        return any(token in path for token in ("image-origin", "image", "gif", "video"))

    @staticmethod
    def _looks_like_avatar_url(url: str) -> bool:
        parsed = urlparse(url)
        path = str(parsed.path or "").lower()
        return any(token in path for token in ("avt-", "/avatar", "profile_pic", "profile"))

    @staticmethod
    def _avatar_url_quality_score(url: str) -> tuple[int, int]:
        normalized = str(url or "").strip().lower()
        if not normalized:
            return (-1, -1)
        score = 0
        if any(
            token in normalized
            for token in ("avatarlarger", "avatar_larger", "originalavatarurl", "cropcenter:1080", "profile_pic")
        ):
            score += 40
        if any(token in normalized for token in ("avatarthumbnail", "avatar_thumb", "thumbnail", "small", "tiny")):
            score -= 25
        max_dim = 0
        for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
            max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
        for match in re.finditer(r"cropcenter:(\d{2,4}):(\d{2,4})", normalized):
            max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
        return (score + min(max_dim, 4096), len(normalized))

    def _pick_best_avatar_url(self, *candidates: Any) -> str | None:
        best_url: str | None = None
        best_score: tuple[int, int] = (-1, -1)
        for candidate in candidates:
            value = str(candidate or "").strip()
            if not value.startswith(("http://", "https://")):
                continue
            score = self._avatar_url_quality_score(value)
            if score > best_score:
                best_score = score
                best_url = value
        return best_url

    @staticmethod
    def _extract_best_bitrate_video_url(video: dict[str, Any]) -> str | None:
        rows: list[tuple[tuple[int, int, int], str]] = []
        for entry in video.get("bitrateInfo") or []:
            if not isinstance(entry, dict):
                continue
            play_addr = entry.get("PlayAddr")
            if not isinstance(play_addr, dict):
                continue
            url_list = play_addr.get("UrlList")
            if not isinstance(url_list, list) or not url_list:
                continue
            url = str(url_list[0] or "").strip()
            if not url:
                continue
            height = int(play_addr.get("Height") or 0)
            width = int(play_addr.get("Width") or 0)
            bitrate = int(entry.get("Bitrate") or 0)
            rows.append(((max(height, width), bitrate, int(play_addr.get("DataSize") or 0)), url))
        rows.sort(reverse=True)
        return rows[0][1] if rows else None

    @staticmethod
    def _extract_url_value(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            for item in value:
                candidate = TikTokScraper._extract_url_value(item)
                if candidate:
                    return candidate
            return ""
        if isinstance(value, dict):
            for key in ("url", "playAddr", "downloadAddr"):
                candidate = TikTokScraper._extract_url_value(value.get(key))
                if candidate:
                    return candidate
            for key in ("url_list", "UrlList"):
                candidate = TikTokScraper._extract_url_value(value.get(key))
                if candidate:
                    return candidate
        return ""

    @staticmethod
    def _extract_thumbnail_url(item: dict[str, Any]) -> str | None:
        video = item.get("video") if isinstance(item.get("video"), dict) else {}
        video_meta = item.get("videoMeta") if isinstance(item.get("videoMeta"), dict) else {}

        for payload, keys in (
            (video, ("cover", "dynamicCover")),
            (video_meta, ("coverUrl", "originalCoverUrl", "cover", "dynamicCover")),
        ):
            if not payload:
                continue
            for key in keys:
                candidate = TikTokScraper._extract_url_value(payload.get(key))
                if candidate:
                    return candidate

        for key in ("coverUrl", "originalCoverUrl", "thumbnail", "thumbnailUrl"):
            candidate = TikTokScraper._extract_url_value(item.get(key))
            if candidate:
                return candidate
        return None

    def _fetch_oembed_thumbnail(self, video_url: str) -> str | None:
        """Fetch thumbnail URL from TikTok's public oembed endpoint."""
        try:
            resp = self.session.get(
                "https://www.tiktok.com/oembed",
                params={"url": video_url},
                timeout=10,
            )
            if resp.status_code == 200:
                self._record_endpoint_response(endpoint="fetch_oembed_thumbnail", response=resp)
                data = resp.json()
                return data.get("thumbnail_url") or None
        except Exception:
            pass
        return None
