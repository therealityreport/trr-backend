"""
YouTube scraper module for fetching videos from channels.

Supports:
- Fetching videos from a YouTube channel
- Filtering by keywords in title/description
- Filtering by date range
- Fetching comments and replies from videos
- Both API-based and web scraping approaches
"""

import json
import logging
import os
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from html import unescape
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Timeout
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, value)


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


YOUTUBE_CHANNEL_SURFACES = ("videos", "shorts", "posts")


def _normalize_youtube_surfaces(value: Any) -> tuple[str, ...]:
    if value is None or value == "":
        return YOUTUBE_CHANNEL_SURFACES
    if isinstance(value, str):
        raw_items = re.split(r"[,|]", value)
    elif isinstance(value, Iterable):
        raw_items = list(value)
    else:
        raw_items = [value]

    normalized: list[str] = []
    for item in raw_items:
        surface = str(item or "").strip().lower()
        if surface in {"video", "channel_videos"}:
            surface = "videos"
        elif surface in {"short", "reels"}:
            surface = "shorts"
        elif surface in {"community", "community_posts", "post"}:
            surface = "posts"
        if surface not in YOUTUBE_CHANNEL_SURFACES or surface in normalized:
            continue
        normalized.append(surface)
    return tuple(normalized or YOUTUBE_CHANNEL_SURFACES)


@dataclass
class YouTubeScrapeConfig:
    """Configuration for a YouTube scrape operation."""

    channel_handle: str = ""  # Channel handle like "bravo" (without @)
    keywords: list[str] = field(default_factory=list)  # Keywords to filter by (e.g., "RHOSLC", "Salt Lake City")
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 2.0
    max_results: int | None = None  # None = no limit
    max_pages: int | None = None  # continuation page limit
    enforce_keyword_filter: bool = True
    allow_ytdlp_search_supplement: bool = True
    allow_ytdlp_video_enrichment: bool = True
    surfaces: tuple[str, ...] | list[str] | str | None = YOUTUBE_CHANNEL_SURFACES
    fetch_post_schema_org: bool = True
    source_type: str = "account"
    playlist_id: str | None = None
    playlist_url: str | None = None

    # Performance tuning
    fast_mode: bool = False
    """When True, uses aggressive rate-limiting tiers for faster scraping."""

    fetch_comment_replies: bool = True
    """When False, only fetch top-level comments and skip reply chains."""

    # Metadata for tracking
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def __post_init__(self):
        """Apply fast_mode overrides when enabled."""
        self.surfaces = _normalize_youtube_surfaces(self.surfaces)
        if self.fast_mode:
            # Use a lower base delay unless explicitly overridden
            if self.delay_seconds == 2.0:  # Only override if at default
                self.delay_seconds = 0.5
            logger.info(
                "YouTubeScrapeConfig fast_mode enabled: delay=%.2fs",
                self.delay_seconds,
            )
        if self.date_start is not None and self.date_start.tzinfo is None:
            self.date_start = self.date_start.replace(tzinfo=UTC)
        if self.date_end is not None and self.date_end.tzinfo is None:
            self.date_end = self.date_end.replace(tzinfo=UTC)

    @property
    def start_timestamp(self) -> float:
        return self.date_start.timestamp() if self.date_start else 0

    @property
    def end_timestamp(self) -> float:
        if self.date_end:
            # Use end of day so the entire date is included
            return self.date_end.replace(hour=23, minute=59, second=59).timestamp()
        return datetime.now(UTC).timestamp()

    def matches_keywords(self, text: str) -> bool:
        """Check if text contains any of the configured keywords."""
        if not self.enforce_keyword_filter:
            return True
        if not self.keywords:
            return True  # No filter = match all
        text_lower = text.lower()
        for keyword in self.keywords:
            # Match keyword with or without # prefix
            kw_clean = keyword.lower().lstrip("#")
            if kw_clean in text_lower or f"#{kw_clean}" in text_lower:
                return True
        return False

    def is_in_date_range(self, timestamp: float) -> bool | None:
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
class YouTubeComment:
    """Represents a single YouTube comment with reply support."""

    comment_id: str
    text: str
    author: str
    author_channel_id: str
    likes: int
    created_at: int
    date_time: str
    is_reply: bool
    parent_comment_id: str | None
    reply_count: int
    replies: list["YouTubeComment"] = field(default_factory=list)

    # Video reference
    video_id: str = ""
    video_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested replies
        result["replies"] = [r.to_dict() if hasattr(r, "to_dict") else r for r in self.replies]
        return result


@dataclass
class YouTubeVideo:
    """Represents a single YouTube video with extracted data."""

    video_id: str
    title: str
    description: str
    date_time: str
    published_at: int  # Unix timestamp
    channel_id: str
    channel_title: str
    duration: str  # ISO 8601 duration (e.g., "PT5M30S")
    duration_seconds: int
    views: int
    likes: int
    comments: int
    url: str
    thumbnail_url: str
    tags: list[str]
    keywords_matched: list[str]
    user_avatar_url: str | None = None
    is_short: bool = False
    source_surface: str = "videos"
    published_text: str = ""
    media_urls: list[str] = field(default_factory=list)
    schema_org: dict[str, Any] = field(default_factory=dict)

    # Comments (populated when fetch_comments is called)
    comment_list: list[YouTubeComment] = field(default_factory=list)

    # Optional tracking metadata
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested comments
        result["comment_list"] = [c.to_dict() if hasattr(c, "to_dict") else c for c in self.comment_list]
        return result


class YouTubeScraper:
    """YouTube scraper for fetching videos from channels."""

    # YouTube endpoints
    CHANNEL_SEARCH_URL = "https://www.youtube.com/results"
    CHANNEL_VIDEOS_URL = "https://www.youtube.com/@{handle}/videos"
    CHANNEL_SHORTS_URL = "https://www.youtube.com/@{handle}/shorts"
    CHANNEL_POSTS_URL = "https://www.youtube.com/@{handle}/posts"
    CHANNEL_ABOUT_URL = "https://www.youtube.com/@{handle}/about"
    VIDEO_WATCH_URL = "https://www.youtube.com/watch?v={video_id}"
    PLAYER_RESPONSE_MARKERS = (
        "ytInitialPlayerResponse =",
        "var ytInitialPlayerResponse =",
        "window['ytInitialPlayerResponse'] =",
        'window["ytInitialPlayerResponse"] =',
    )

    @staticmethod
    def _pick_largest_thumbnail_url(payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        thumbnails = payload.get("thumbnails")
        if not isinstance(thumbnails, list):
            return None
        best_url: str | None = None
        best_size = -1
        for item in thumbnails:
            if not isinstance(item, dict):
                continue
            candidate = str(item.get("url") or "").strip()
            if not candidate:
                continue
            width = int(item.get("width") or 0)
            height = int(item.get("height") or 0)
            size = max(width, height)
            if size >= best_size:
                best_size = size
                best_url = candidate
        return best_url

    @staticmethod
    def _normalize_channel_avatar_url(candidate: Any, *, require_yt3_host: bool = False) -> str | None:
        value = str(candidate or "").strip()
        if not value:
            return None
        parsed = urlparse(value)
        host = parsed.netloc.lower()
        is_yt3 = host == "yt3.googleusercontent.com" or host.endswith(".yt3.googleusercontent.com")
        if require_yt3_host and not is_yt3:
            return None
        if is_yt3:
            value = re.sub(r"=s\d+(-[^?]*)?", r"=s1024\1", value)
        return value

    def _extract_channel_header_avatar_from_data(self, data: dict[str, Any]) -> str | None:
        if not isinstance(data, dict):
            return None
        header = data.get("header")
        if not isinstance(header, dict):
            return None

        target_nodes: list[dict[str, Any]] = []
        stack: list[Any] = [header]
        visited = 0
        while stack and visited < 600:
            node = stack.pop()
            visited += 1
            if isinstance(node, dict):
                for key, value in node.items():
                    key_lower = str(key or "").strip().lower()
                    if (
                        key_lower in {"c4tabbedheaderrenderer", "pageheaderrenderer", "pageheaderviewmodel"}
                        or "pageheader" in key_lower
                    ) and isinstance(value, dict):
                        target_nodes.append(value)
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            elif isinstance(node, list):
                stack.extend(node)

        def _collect_urls(payload: Any) -> list[str]:
            urls: list[str] = []
            walk_stack: list[Any] = [payload]
            walked = 0
            while walk_stack and walked < 1000:
                current = walk_stack.pop()
                walked += 1
                if isinstance(current, dict):
                    direct_url = str(current.get("url") or "").strip()
                    if direct_url:
                        urls.append(direct_url)
                    thumb_url = self._pick_largest_thumbnail_url(current)
                    if thumb_url:
                        urls.append(thumb_url)
                    for nested in current.values():
                        if isinstance(nested, (dict, list)):
                            walk_stack.append(nested)
                elif isinstance(current, list):
                    walk_stack.extend(current)
            return urls

        candidates: list[str] = []
        for node in target_nodes or [header]:
            candidates.extend(_collect_urls(node))
        for candidate in candidates:
            normalized = self._normalize_channel_avatar_url(candidate, require_yt3_host=True)
            if normalized:
                return normalized
        return None

    def _extract_channel_title_from_data(self, data: dict[str, Any]) -> str | None:
        if not isinstance(data, dict):
            return None

        metadata = (
            data.get("metadata", {}).get("channelMetadataRenderer", {})
            if isinstance(data.get("metadata"), dict)
            else {}
        )
        if isinstance(metadata, dict):
            title = str(metadata.get("title") or "").strip()
            if title:
                return title

        stack: list[Any] = [data.get("header")]
        visited = 0
        while stack and visited < 600:
            node = stack.pop()
            visited += 1
            if isinstance(node, dict):
                for key in ("title", "pageTitle", "channelName"):
                    value = str(node.get(key) or "").strip()
                    if value:
                        return value
                for nested in node.values():
                    if isinstance(nested, (dict, list)):
                        stack.append(nested)
            elif isinstance(node, list):
                stack.extend(node)

        return None

    def _extract_channel_avatar_from_data(self, data: dict[str, Any]) -> str | None:
        if not isinstance(data, dict):
            return None

        metadata = (
            data.get("metadata", {}).get("channelMetadataRenderer", {})
            if isinstance(data.get("metadata"), dict)
            else {}
        )
        candidates: list[Any] = []
        if isinstance(metadata, dict):
            candidates.extend(
                [
                    self._pick_largest_thumbnail_url(metadata.get("avatar")),
                    metadata.get("avatarUrl"),
                    metadata.get("avatar"),
                ]
            )

        candidates.append(self._extract_channel_header_avatar_from_data(data))
        for candidate in candidates:
            normalized = self._normalize_channel_avatar_url(candidate, require_yt3_host=True)
            if normalized:
                return normalized
        return None

    def _extract_channel_avatar_from_renderer(
        self,
        renderer: dict[str, Any],
        *,
        fallback_channel_avatar_url: str | None = None,
    ) -> str | None:
        candidates: list[str] = []
        byline_runs = renderer.get("shortBylineText", {}).get("runs", [])
        if isinstance(byline_runs, list) and byline_runs:
            endpoint = byline_runs[0].get("navigationEndpoint", {})
            browse = endpoint.get("browseEndpoint", {}) if isinstance(endpoint, dict) else {}
            thumbnails = browse.get("thumbnail") if isinstance(browse, dict) else {}
            candidate = self._pick_largest_thumbnail_url(thumbnails)
            if candidate:
                candidates.append(candidate)
        channel_thumbs = renderer.get("channelThumbnailSupportedRenderers", {})
        if isinstance(channel_thumbs, dict):
            for key in (
                "channelThumbnailWithLinkRenderer",
                "channelThumbnailRenderer",
            ):
                sub = channel_thumbs.get(key, {})
                if not isinstance(sub, dict):
                    continue
                candidate = self._pick_largest_thumbnail_url(sub.get("thumbnail"))
                if candidate:
                    candidates.append(candidate)
        for candidate in candidates:
            value = self._normalize_channel_avatar_url(candidate)
            if value:
                return value
        return self._normalize_channel_avatar_url(fallback_channel_avatar_url, require_yt3_host=True)

    COMMENT_API_URL = "https://www.youtube.com/youtubei/v1/next"
    YTINITAL_DATA_PATTERN = re.compile(r"var ytInitialData = ({.*?});", re.DOTALL)
    SCHEMA_ORG_JSON_LD_PATTERN = re.compile(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        re.IGNORECASE | re.DOTALL,
    )
    PUBLISHED_DATE_PATTERNS = (
        re.compile(r'"(?:datePublished|uploadDate|publishDate)"\s*:\s*"([^"]+)"', re.IGNORECASE),
        re.compile(r'itemprop="(?:datePublished|uploadDate)"\s+content="([^"]+)"', re.IGNORECASE),
    )
    PLAYER_MICROFORMAT_PATTERNS = (
        re.compile(
            r'"playerMicroformatRenderer"\s*:\s*\{.*?"(?:publishDate|uploadDate)"\s*:\s*"([^"]+)"',
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r'"microformat"\s*:\s*\{.*?"(?:publishDate|uploadDate)"\s*:\s*"([^"]+)"',
            re.IGNORECASE | re.DOTALL,
        ),
    )
    DATE_ONLY_PREFIX_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")
    SHORTS_LIKE_COUNT_PATTERNS = (
        re.compile(
            r'<[^>]+\bid=(["\'])button-bar\1[^>]*>.*?<reel-action-bar-view-model\b[^>]*>.*?'
            r"<like-button-view-model\b[^>]*>.*?<toggle-button-view-model\b[^>]*>.*?"
            r"<button-view-model\b[^>]*>.*?<label\b[^>]*>.*?<div\b[^>]*>.*?"
            r"<span\b[^>]*>(?P<count>[^<]+)</span>",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"<like-button-view-model\b[^>]*>.*?<toggle-button-view-model\b[^>]*>.*?"
            r"<button-view-model\b[^>]*>.*?<label\b[^>]*>.*?<div\b[^>]*>.*?"
            r"<span\b[^>]*>(?P<count>[^<]+)</span>",
            re.IGNORECASE | re.DOTALL,
        ),
    )

    # Client context for API requests
    INNERTUBE_CONTEXT = {
        "client": {
            "hl": "en",
            "gl": "US",
            "clientName": "WEB",
            "clientVersion": "2.20240101.00.00",
        }
    }

    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.5
    REQUEST_TIMEOUT_SECONDS = (10, 45)
    CONTINUATION_REQUEST_TIMEOUT_SECONDS = (
        _env_int("SOCIAL_YOUTUBE_CONTINUATION_CONNECT_TIMEOUT_SECONDS", 6),
        _env_int("SOCIAL_YOUTUBE_CONTINUATION_READ_TIMEOUT_SECONDS", 10),
    )
    CONTINUATION_REQUEST_TOTAL_TIMEOUT_SECONDS = _env_int(
        "SOCIAL_YOUTUBE_CONTINUATION_TOTAL_TIMEOUT_SECONDS",
        20,
    )
    PRE_WINDOW_PAGE_CAP = _env_int("SOCIAL_YOUTUBE_PRE_WINDOW_PAGE_CAP", 12)
    INITIAL_DATE_WINDOW_NO_HIT_PAGE_CAP = _env_int("SOCIAL_YOUTUBE_INITIAL_DATE_WINDOW_NO_HIT_PAGE_CAP", 8)
    DATE_WINDOW_NO_HIT_PAGE_CAP = _env_int("SOCIAL_YOUTUBE_DATE_WINDOW_NO_HIT_PAGE_CAP", 1)
    YTDLP_SEARCH_TIMEOUT_SECONDS = _env_int("SOCIAL_YOUTUBE_YTDLP_TIMEOUT_SECONDS", 120)
    YTDLP_ENRICH_MAX_VIDEOS = _env_int("SOCIAL_YOUTUBE_YTDLP_ENRICH_MAX_VIDEOS", 300)
    COMMENT_CONTINUATION_RETRY_ATTEMPTS = _env_int("SOCIAL_YOUTUBE_COMMENT_CONTINUATION_RETRY_ATTEMPTS", 3)
    COMMENT_CONTINUATION_RETRY_BACKOFF_SECONDS = _env_int(
        "SOCIAL_YOUTUBE_COMMENT_CONTINUATION_RETRY_BACKOFF_SECONDS",
        1,
    )
    TRANSCRIPT_FETCH_TIMEOUT_SECONDS = _env_int("SOCIAL_YOUTUBE_TRANSCRIPT_FETCH_TIMEOUT_SECONDS", 45)

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.session = self._create_session()
        self._request_count = 0
        self._last_429_at: float = 0.0
        self._consecutive_success: int = 0
        self.last_retrieval_meta: dict[str, Any] = {}
        self.last_comment_fetch_reason: str | None = None
        self._last_channel_continuation_error: str | None = None
        self.comments_auth_failed = False
        self._last_transport = "requests"
        self._fallback_chain: list[str] = []
        self._last_stop_reason: str | None = None
        self._last_retryable = False
        self._last_complete = False
        self._last_source_mode = "scraper"
        self._precise_publish_ts_cache: dict[str, int] = {}
        self._post_schema_org_cache: dict[str, dict[str, Any]] = {}
        self._precise_publish_attempts = 0
        self._precise_publish_successes = 0
        self._precise_publish_failures = 0
        self._shorts_precise_publish_attempts = 0
        self._shorts_precise_publish_successes = 0
        self._shorts_precise_publish_failures = 0

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        return {
            "request_count": int(getattr(self, "_request_count", 0) or 0),
            "transport": str(getattr(self, "_last_transport", "requests") or "requests"),
            "fallback_chain": list(getattr(self, "_fallback_chain", []) or []),
            "stop_reason": getattr(self, "_last_stop_reason", None),
            "retryable": bool(getattr(self, "_last_retryable", False)),
            "complete": bool(getattr(self, "_last_complete", False)),
            "source_mode": str(getattr(self, "_last_source_mode", "scraper") or "scraper"),
        }

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
            "forbidden",
            "unauthorized",
        )
        return any(marker in value for marker in markers)

    def _set_comment_failure_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self.last_comment_fetch_reason = normalized
        if self._is_auth_related_failure(normalized):
            self.comments_auth_failed = True

    @staticmethod
    def _is_retryable_comment_failure(reason: str | None) -> bool:
        value = str(reason or "").strip().lower()
        if not value:
            return False
        if value in {"request_error", "continuation_fetch_failed"}:
            return True
        if value.startswith("http_429") or value.startswith("http_5"):
            return True
        return False

    def _create_session(self) -> requests.Session:
        """Create a session with retry logic."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.RETRY_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _continuation_request_timeout(self) -> Timeout:
        connect_timeout, read_timeout = self.CONTINUATION_REQUEST_TIMEOUT_SECONDS
        return Timeout(
            total=max(1, self.CONTINUATION_REQUEST_TOTAL_TIMEOUT_SECONDS),
            connect=max(1, connect_timeout),
            read=max(1, read_timeout),
        )

    def _get_headers(self) -> dict:
        """Get request headers."""
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
        }

    def _rate_limit(self, delay: float, *, fast_mode: bool = False):
        """Apply adaptive rate limiting between requests.

        Standard mode: starts at 50% of the base delay.
        Fast mode: uses aggressive tiers that ramp down with consecutive successes.
        Both modes: double delay for 60s after any 429 response.
        """
        if self._request_count > 0:
            now = time.monotonic()
            if self._last_429_at and (now - self._last_429_at) < 60.0:
                effective_delay = delay * 2.0
            elif fast_mode:
                # Aggressive tiers: ramp down as we prove the session is healthy
                if self._consecutive_success >= 20:
                    effective_delay = delay * 0.15  # e.g. 0.5 * 0.15 = 0.075s
                elif self._consecutive_success >= 5:
                    effective_delay = delay * 0.25  # e.g. 0.5 * 0.25 = 0.125s
                else:
                    effective_delay = delay * 0.5  # e.g. 0.5 * 0.5 = 0.25s
            else:
                effective_delay = delay * 0.5
            logger.debug(
                "Rate limiting: waiting %.3fs (base=%ss, fast=%s, streak=%s)",
                effective_delay,
                delay,
                fast_mode,
                self._consecutive_success,
            )
            time.sleep(effective_delay)
        self._request_count += 1

    def _track_response_status(self, status_code: int) -> None:
        """Track response status for adaptive rate limiting."""
        if status_code == 429:
            self._last_429_at = time.monotonic()
            self._consecutive_success = 0
        elif 200 <= status_code < 400:
            self._consecutive_success += 1

    def _parse_duration(self, duration_str: str) -> int:
        """Parse ISO 8601 duration to seconds."""
        if not duration_str:
            return 0
        # Handle PT format (e.g., PT5M30S, PT1H2M3S)
        pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
        match = re.match(pattern, duration_str)
        if not match:
            return 0
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds

    def _parse_timestamp(self, date_str: str) -> int:
        """Parse YouTube date string to Unix timestamp."""
        try:
            # Try ISO format first
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
        # Try other formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return int(dt.timestamp())
            except (ValueError, TypeError):
                continue
        return 0

    def _extract_ytinital_data(self, html: str) -> dict | None:
        """Extract ytInitialData from YouTube page HTML."""
        import json

        match = self.YTINITAL_DATA_PATTERN.search(html)
        if not match:
            # Try alternative pattern
            alt_pattern = re.compile(r"ytInitialData\s*=\s*({.*?});</script>", re.DOTALL)
            match = alt_pattern.search(html)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        return None

    @staticmethod
    def _normalize_handle(value: str | None) -> str:
        if not value:
            return ""
        normalized = str(value).strip().lower()
        if not normalized:
            return ""
        normalized = normalized.lstrip("@")
        if "/" in normalized:
            normalized = normalized.split("/", 1)[0]
        return normalized

    def _extract_handle_from_url(self, value: str | None) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if raw.startswith("/"):
            raw = f"https://www.youtube.com{raw}"
        match = re.search(r"/@([a-zA-Z0-9._-]+)", raw)
        if match:
            return self._normalize_handle(match.group(1))
        return ""

    def _renderer_owner_candidates(self, renderer: dict) -> set[str]:
        candidates: set[str] = set()
        if not isinstance(renderer, dict):
            return candidates

        candidate_paths = [
            renderer.get("ownerText", {}).get("runs", []),
            renderer.get("authorText", {}).get("runs", []),
            renderer.get("shortBylineText", {}).get("runs", []),
            renderer.get("longBylineText", {}).get("runs", []),
            renderer.get("headline", {}).get("runs", []),
            renderer.get("title", {}).get("runs", []),
        ]
        for runs in candidate_paths:
            if not isinstance(runs, list):
                continue
            for run in runs:
                if not isinstance(run, dict):
                    continue
                text = str(run.get("text") or "").strip()
                if text.startswith("@"):
                    handle = self._normalize_handle(text)
                    if handle:
                        candidates.add(handle)
                endpoint = run.get("navigationEndpoint", {})
                browse = endpoint.get("browseEndpoint", {}) if isinstance(endpoint, dict) else {}
                canonical = str(browse.get("canonicalBaseUrl") or "") if isinstance(browse, dict) else ""
                handle_from_canonical = self._extract_handle_from_url(canonical)
                if handle_from_canonical:
                    candidates.add(handle_from_canonical)
                command_meta = endpoint.get("commandMetadata", {}) if isinstance(endpoint, dict) else {}
                web_cmd = command_meta.get("webCommandMetadata", {}) if isinstance(command_meta, dict) else {}
                handle_from_url = self._extract_handle_from_url(web_cmd.get("url"))
                if handle_from_url:
                    candidates.add(handle_from_url)
        return candidates

    def _renderer_matches_owner(self, renderer: dict, target_handle: str) -> bool:
        normalized_target = self._normalize_handle(target_handle)
        if not normalized_target:
            return True
        candidates = self._renderer_owner_candidates(renderer)
        if not candidates:
            return True
        return normalized_target in candidates

    def _extract_channel_identity_from_data(self, data: dict, fallback_handle: str) -> tuple[str, str]:
        canonical_handle = self._normalize_handle(fallback_handle)
        channel_id = ""

        metadata = data.get("metadata", {}).get("channelMetadataRenderer", {}) if isinstance(data, dict) else {}
        if isinstance(metadata, dict):
            channel_id = str(metadata.get("externalId") or metadata.get("channelId") or "").strip()
            vanity_url = str(metadata.get("vanityChannelUrl") or metadata.get("channelUrl") or "").strip()
            handle_from_meta = self._extract_handle_from_url(vanity_url)
            if handle_from_meta:
                canonical_handle = handle_from_meta

        header = data.get("header", {}) if isinstance(data, dict) else {}
        tabbed_header = header.get("c4TabbedHeaderRenderer", {}) if isinstance(header, dict) else {}
        if isinstance(tabbed_header, dict):
            channel_id_from_header = str(tabbed_header.get("channelId") or "").strip()
            if channel_id_from_header:
                channel_id = channel_id_from_header

        return canonical_handle, channel_id

    @staticmethod
    def _extract_text(payload: Any) -> str:
        if isinstance(payload, str):
            return payload.strip()
        if not isinstance(payload, dict):
            return ""
        simple_text = str(payload.get("simpleText") or payload.get("content") or payload.get("text") or "").strip()
        if simple_text:
            return simple_text
        runs = payload.get("runs")
        if isinstance(runs, list):
            joined = "".join(str(item.get("text") or "") for item in runs if isinstance(item, dict)).strip()
            if joined:
                return joined
        return ""

    @staticmethod
    def _parse_compact_count_text(payload: Any) -> int | None:
        text = YouTubeScraper._extract_text(payload).lower().replace(",", "")
        if not text:
            return None
        match = re.search(r"(\d+(?:\.\d+)?)\s*([kmb]?)", text)
        if not match:
            return None
        value = float(match.group(1))
        suffix = match.group(2)
        multiplier = 1
        if suffix == "k":
            multiplier = 1000
        elif suffix == "m":
            multiplier = 1000000
        elif suffix == "b":
            multiplier = 1000000000
        return int(value * multiplier)

    def _normalize_youtube_url(self, value: Any) -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.startswith("/"):
            return f"https://www.youtube.com{raw}"
        return raw

    def _find_about_channel_view_model(self, data: Any) -> dict[str, Any] | None:
        stack: list[Any] = [data]
        visited = 0
        while stack and visited < 2000:
            node = stack.pop()
            visited += 1
            if isinstance(node, dict):
                candidate = node.get("aboutChannelViewModel")
                if isinstance(candidate, dict):
                    return candidate
                for nested in node.values():
                    if isinstance(nested, (dict, list)):
                        stack.append(nested)
            elif isinstance(node, list):
                stack.extend(node)
        return None

    def _extract_channel_about_snapshot_from_data(
        self,
        data: dict[str, Any] | None,
        fallback_handle: str,
    ) -> dict[str, Any]:
        if not isinstance(data, dict):
            return {}

        canonical_handle, channel_id = self._extract_channel_identity_from_data(data, fallback_handle)
        about = self._find_about_channel_view_model(data) or {}
        metadata = (
            data.get("metadata", {}).get("channelMetadataRenderer", {})
            if isinstance(data.get("metadata"), dict)
            else {}
        )

        profile_url = (
            self._normalize_youtube_url(about.get("canonicalChannelUrl"))
            or self._normalize_youtube_url(about.get("channelUrl"))
            or self._normalize_youtube_url(metadata.get("vanityChannelUrl"))
            or self._normalize_youtube_url(metadata.get("channelUrl"))
        )
        if not profile_url and canonical_handle:
            profile_url = f"https://www.youtube.com/@{canonical_handle}"

        snapshot = {
            "username": canonical_handle or self._normalize_handle(fallback_handle) or None,
            "display_name": self._extract_text(about.get("title")) or self._extract_channel_title_from_data(data),
            "bio": self._extract_text(about.get("description")) or None,
            "avatar_url": self._extract_channel_avatar_from_data(data),
            "profile_url": profile_url,
            "follower_count": self._parse_compact_count_text(about.get("subscriberCountText")),
            "total_posts": self._parse_compact_count_text(about.get("videoCountText")),
            "channel_id": channel_id or None,
        }
        return {key: value for key, value in snapshot.items() if value is not None}

    def resolve_channel_identity(self, handle: str, delay: float = 0.5) -> dict[str, str | None]:
        normalized_handle = self._normalize_handle(handle)
        data = self.fetch_channel_videos(normalized_handle, delay, surface="videos")
        if not data:
            data = self.fetch_channel_videos(normalized_handle, delay, surface="shorts")
        if not data:
            return {"canonical_handle": normalized_handle, "channel_id": None}
        canonical_handle, channel_id = self._extract_channel_identity_from_data(data, normalized_handle)
        return {
            "canonical_handle": canonical_handle or normalized_handle,
            "channel_id": channel_id or None,
        }

    def resolve_channel_about_snapshot(self, handle: str, delay: float = 0.5) -> dict[str, Any]:
        normalized_handle = self._normalize_handle(handle)
        data = self.fetch_channel_about(normalized_handle, delay)
        return self._extract_channel_about_snapshot_from_data(data, normalized_handle)

    @staticmethod
    def _shorts_lockup_to_renderer(model: dict) -> dict:
        """Convert a shortsLockupViewModel into a videoRenderer-like dict.

        YouTube replaced ``reelItemRenderer`` with ``shortsLockupViewModel``
        in 2025.  This shim extracts the available fields so that the existing
        ``_parse_video_renderer`` path can handle shorts transparently.
        """
        reel_ep = model.get("onTap", {}).get("innertubeCommand", {}).get("reelWatchEndpoint", {})
        video_id = reel_ep.get("videoId", "")
        if not video_id:
            entity_id = model.get("entityId", "")
            if entity_id.startswith("shorts-shelf-item-"):
                video_id = entity_id[len("shorts-shelf-item-") :]

        # Parse title + views from overlay or accessibilityText
        overlay = model.get("overlayMetadata", {})
        primary = overlay.get("primaryText", {})
        title = primary.get("content", "") if isinstance(primary, dict) else ""

        # Fallback to accessibilityText if overlay has no title
        view_text = ""
        if not title:
            a11y = model.get("accessibilityText", "")
            if a11y:
                cleaned = re.sub(r"\s*-\s*play Short$", "", a11y, flags=re.IGNORECASE)
                parts = cleaned.rsplit(",", 1)
                if len(parts) == 2 and "view" in parts[1].lower():
                    title = parts[0].strip()
                    view_text = parts[1].strip()
                else:
                    title = cleaned.strip()

        # Overlay metadata has compact view text (e.g. "3.1K views") which
        # the parser handles better than the a11y "3.1 thousand views" form.
        secondary = overlay.get("secondaryText", {})
        overlay_views = secondary.get("content", "") if isinstance(secondary, dict) else ""
        if overlay_views:
            view_text = overlay_views

        # Thumbnail
        thumb_vm = model.get("thumbnailViewModel", {})
        thumb_url = ""
        if isinstance(thumb_vm, dict):
            image = thumb_vm.get("image", {})
            sources = image.get("sources", []) if isinstance(image, dict) else []
            if isinstance(sources, list) and sources:
                thumb_url = str(sources[-1].get("url", ""))
        if not thumb_url:
            reel_thumbs = reel_ep.get("thumbnail", {}).get("thumbnails", [])
            if reel_thumbs:
                thumb_url = str(reel_thumbs[-1].get("url", ""))

        url = f"/shorts/{video_id}" if video_id else ""

        return {
            "videoId": video_id,
            "title": {"simpleText": title},
            "descriptionSnippet": {"runs": [{"text": title}]} if title else {},
            "viewCountText": {"simpleText": view_text},
            "thumbnail": {"thumbnails": [{"url": thumb_url}]} if thumb_url else {},
            "navigationEndpoint": {
                "commandMetadata": {
                    "webCommandMetadata": {"url": url},
                },
            },
        }

    def _extract_renderer_url(self, renderer: dict) -> str:
        title_runs = renderer.get("title", {}).get("runs", []) if isinstance(renderer, dict) else []
        headline_runs = renderer.get("headline", {}).get("runs", []) if isinstance(renderer, dict) else []
        title_endpoint = (
            title_runs[0].get("navigationEndpoint") if isinstance(title_runs, list) and title_runs else None
        )
        headline_endpoint = (
            headline_runs[0].get("navigationEndpoint") if isinstance(headline_runs, list) and headline_runs else None
        )

        def _extract_from_endpoint(endpoint: dict) -> str:
            if not isinstance(endpoint, dict):
                return ""
            command_metadata = endpoint.get("commandMetadata", {})
            if isinstance(command_metadata, dict):
                web_command = command_metadata.get("webCommandMetadata", {})
                if isinstance(web_command, dict):
                    candidate = str(web_command.get("url") or "").strip()
                    if candidate:
                        return candidate
            watch_endpoint = endpoint.get("watchEndpoint", {})
            if isinstance(watch_endpoint, dict):
                video_id = str(watch_endpoint.get("videoId") or "").strip()
                if video_id:
                    return f"/watch?v={video_id}"
            return ""

        endpoint_candidates = [
            renderer.get("navigationEndpoint"),
            renderer.get("onTap", {}).get("innertubeCommand"),
            title_endpoint,
            headline_endpoint,
        ]
        for endpoint in endpoint_candidates:
            candidate = _extract_from_endpoint(endpoint if isinstance(endpoint, dict) else {})
            if candidate:
                return candidate
        return ""

    def _canonical_video_url(self, *, video_id: str, surface: str, renderer_url: str) -> str:
        raw = str(renderer_url or "").strip()
        if raw:
            if raw.startswith("/"):
                raw = f"https://www.youtube.com{raw}"
            if "/shorts/" in raw:
                return raw
            if "/watch" in raw or "youtu.be/" in raw:
                return raw
        if surface == "shorts":
            return f"https://www.youtube.com/shorts/{video_id}"
        return f"https://www.youtube.com/watch?v={video_id}"

    @staticmethod
    def _duration_iso_from_seconds(value: Any) -> str:
        try:
            seconds = max(0, int(float(value or 0)))
        except (TypeError, ValueError):
            seconds = 0
        return f"PT{seconds}S" if seconds > 0 else ""

    @staticmethod
    def _timestamp_from_ytdlp(payload: dict[str, Any]) -> int:
        for key in ("timestamp", "release_timestamp", "modified_timestamp"):
            try:
                value = int(payload.get(key) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                return value
        upload_date = str(payload.get("upload_date") or "").strip()
        if re.fullmatch(r"\d{8}", upload_date):
            try:
                return int(datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=UTC).timestamp())
            except ValueError:
                return 0
        return 0

    @staticmethod
    def _extract_video_id_from_url(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        parsed = urlparse(raw if "://" in raw else f"https://www.youtube.com/{raw.lstrip('/')}")
        query_id = parse_qs(parsed.query).get("v", [""])[0]
        if query_id:
            return query_id
        for pattern in (r"/shorts/([A-Za-z0-9_-]{6,})", r"youtu\.be/([A-Za-z0-9_-]{6,})"):
            match = re.search(pattern, raw)
            if match:
                return match.group(1)
        return ""

    @staticmethod
    def extract_playlist_id(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if "://" in raw or raw.lower().startswith("www."):
            parsed = urlparse(raw if "://" in raw else f"https://{raw}")
            query_id = parse_qs(parsed.query).get("list", [""])[0]
            if query_id:
                return query_id.strip()
        direct = raw.split("?", 1)[0].split("#", 1)[0].strip()
        if re.fullmatch(r"(?:PL|UU|LL|FL|RD|OLAK5uy_)[A-Za-z0-9_-]{8,}", direct):
            return direct
        match = re.search(r"(?:list=|/playlist/)([A-Za-z0-9_-]{10,})", raw)
        return match.group(1).strip() if match else ""

    @classmethod
    def _playlist_url(cls, *, playlist_id: str | None, playlist_url: str | None) -> tuple[str, str]:
        resolved_id = cls.extract_playlist_id(playlist_id) or cls.extract_playlist_id(playlist_url)
        if not resolved_id:
            return "", ""
        raw_url = str(playlist_url or "").strip()
        if raw_url.startswith(("http://", "https://")) and cls.extract_playlist_id(raw_url) == resolved_id:
            return resolved_id, raw_url
        return resolved_id, f"https://www.youtube.com/playlist?list={resolved_id}"

    def _video_from_ytdlp_payload(
        self,
        payload: dict[str, Any],
        config: YouTubeScrapeConfig,
        *,
        source_surface: str = "videos",
        keyword_hint: str | None = None,
    ) -> YouTubeVideo | None:
        video_id = str(payload.get("id") or payload.get("display_id") or "").strip() or self._extract_video_id_from_url(
            payload.get("webpage_url") or payload.get("url")
        )
        if not video_id:
            return None
        title = str(payload.get("title") or "").strip()
        description = str(payload.get("description") or payload.get("fulltitle") or "").strip()
        combined_text = f"{title} {description}".strip()
        if not config.matches_keywords(combined_text):
            return None

        timestamp = self._timestamp_from_ytdlp(payload)
        if timestamp > 0:
            in_range = config.is_in_date_range(timestamp)
            if in_range is None or in_range is False:
                return None
        date_time = datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""
        duration_seconds = _safe_int(payload.get("duration"))
        webpage_url = str(payload.get("webpage_url") or payload.get("original_url") or payload.get("url") or "")
        is_short = "/shorts/" in webpage_url
        if not is_short and 0 < duration_seconds <= 60:
            is_short = True
        surface = "shorts" if is_short else source_surface
        if surface not in {"videos", "shorts", "search"}:
            surface = "shorts" if is_short else "videos"
        thumbnail_url = str(payload.get("thumbnail") or "").strip()
        thumbnails = payload.get("thumbnails")
        if not thumbnail_url and isinstance(thumbnails, list) and thumbnails:
            thumbnail_url = str((thumbnails[-1] or {}).get("url") or "").strip()

        keywords_matched: list[str] = []
        for keyword in config.keywords:
            cleaned = str(keyword or "").strip().lower().lstrip("#")
            if cleaned and cleaned in combined_text.lower():
                keywords_matched.append(keyword)
        if keyword_hint and keyword_hint not in keywords_matched:
            keywords_matched.append(keyword_hint)

        if is_short:
            description = description or title
            title = "" if source_surface != "search" else title

        return YouTubeVideo(
            video_id=video_id,
            title=title,
            description=description,
            date_time=date_time,
            published_at=timestamp,
            channel_id=str(payload.get("channel_id") or "").strip(),
            channel_title=str(payload.get("channel") or payload.get("uploader") or "").strip(),
            duration=self._duration_iso_from_seconds(duration_seconds),
            duration_seconds=duration_seconds,
            views=_safe_int(payload.get("view_count")),
            likes=_safe_int(payload.get("like_count")),
            comments=_safe_int(payload.get("comment_count")),
            url=self._canonical_video_url(video_id=video_id, surface=surface, renderer_url=webpage_url),
            thumbnail_url=thumbnail_url,
            tags=list(payload.get("tags") or []),
            keywords_matched=keywords_matched,
            user_avatar_url=self._normalize_channel_avatar_url(
                payload.get("uploader_avatar")
                or payload.get("channel_thumbnail")
                or payload.get("channelAvatarUrl")
                or payload.get("author_avatar_url")
                or None
            ),
            is_short=is_short,
            source_surface=surface,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _parse_video_renderer(
        self,
        renderer: dict,
        config: YouTubeScrapeConfig,
        *,
        surface: str = "videos",
        fallback_channel_avatar_url: str | None = None,
    ) -> YouTubeVideo | None:
        """Parse a video renderer from YouTube data."""
        video_id = renderer.get("videoId", "")
        if not video_id:
            navigation_url = self._extract_renderer_url(renderer)
            short_match = re.search(r"/shorts/([A-Za-z0-9_-]{6,})", navigation_url)
            watch_match = re.search(r"[?&]v=([A-Za-z0-9_-]{6,})", navigation_url)
            if short_match:
                video_id = short_match.group(1)
            elif watch_match:
                video_id = watch_match.group(1)
        if not video_id:
            return None

        # Extract title
        title_container = renderer.get("title", {}) or renderer.get("headline", {}) or {}
        title_runs = title_container.get("runs", []) if isinstance(title_container, dict) else []
        title = title_runs[0].get("text", "") if title_runs else ""
        if not title and isinstance(title_container, dict):
            title = str(title_container.get("simpleText") or "")

        # Extract description
        desc_runs = renderer.get("descriptionSnippet", {}).get("runs", [])
        description = "".join(r.get("text", "") for r in desc_runs)
        if not description:
            desc_runs = renderer.get("detailedMetadataSnippets", [{}])[0].get("snippetText", {}).get("runs", [])
            if isinstance(desc_runs, list):
                description = "".join(str(r.get("text") or "") for r in desc_runs if isinstance(r, dict))

        # Extract view count
        view_count_text = renderer.get("viewCountText", {})
        view_text = view_count_text.get("simpleText") or ""
        if not view_text:
            runs = view_count_text.get("runs", [])
            if isinstance(runs, list):
                view_text = "".join(str(item.get("text", "")) for item in runs if isinstance(item, dict))
        views = self._parse_view_count(view_text)

        # Extract published time
        published_container = renderer.get("publishedTimeText", {}) or {}
        published_text = published_container.get("simpleText", "")
        if not published_text:
            runs = published_container.get("runs", [])
            if isinstance(runs, list) and runs:
                published_text = "".join(str(item.get("text", "")) for item in runs if isinstance(item, dict))
        published_at = self._estimate_publish_date(published_text)

        # Duration
        duration_text = renderer.get("lengthText", {}).get("simpleText", "")
        duration_seconds = self._parse_duration_text(duration_text)

        # Thumbnail
        thumbnails = renderer.get("thumbnail", {}).get("thumbnails", [])
        thumbnail_url = thumbnails[-1].get("url", "") if thumbnails else ""

        # Channel info
        channel_info = renderer.get("ownerText", {}).get("runs", [{}])[0]
        channel_title = channel_info.get("text", "") or renderer.get("channelName", "")
        if not channel_title:
            short_byline = renderer.get("shortBylineText", {}).get("runs", [])
            if isinstance(short_byline, list) and short_byline:
                channel_title = str(short_byline[0].get("text") or "")
        channel_avatar_url = self._extract_channel_avatar_from_renderer(
            renderer,
            fallback_channel_avatar_url=fallback_channel_avatar_url,
        )

        # Find matched keywords
        combined_text = f"{title} {description}".lower()
        keywords_matched = []
        for kw in config.keywords:
            kw_clean = kw.lower().lstrip("#")
            if kw_clean in combined_text:
                keywords_matched.append(kw)

        renderer_url = self._extract_renderer_url(renderer)
        canonical_url = self._canonical_video_url(video_id=video_id, surface=surface, renderer_url=renderer_url)
        is_short = surface == "shorts" or "/shorts/" in canonical_url
        if is_short:
            description = description or title
            title = ""

        return YouTubeVideo(
            video_id=video_id,
            title=title,
            description=description,
            date_time=datetime.fromtimestamp(published_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
            if published_at
            else "",
            published_at=published_at,
            channel_id="",
            channel_title=channel_title,
            duration=duration_text,
            duration_seconds=duration_seconds,
            views=views,
            likes=0,  # Not available in search results
            comments=0,  # Not available in search results
            url=canonical_url,
            thumbnail_url=thumbnail_url,
            tags=[],  # Not available in search results
            keywords_matched=keywords_matched,
            user_avatar_url=channel_avatar_url,
            is_short=is_short,
            source_surface=surface,
            published_text=published_text,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    @classmethod
    def _iter_schema_org_json_ld(cls, html: str) -> Iterable[dict[str, Any]]:
        def _walk(value: Any) -> Iterable[dict[str, Any]]:
            if isinstance(value, dict):
                yield value
                graph = value.get("@graph")
                if isinstance(graph, list):
                    for item in graph:
                        yield from _walk(item)
                for item in value.values():
                    if isinstance(item, (dict, list)):
                        yield from _walk(item)
            elif isinstance(value, list):
                for item in value:
                    yield from _walk(item)

        for match in cls.SCHEMA_ORG_JSON_LD_PATTERN.finditer(str(html or "")):
            raw = unescape(str(match.group(1) or "")).strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            yield from _walk(payload)

    @staticmethod
    def _schema_type_matches(value: Any, *expected: str) -> bool:
        expected_lower = {item.lower() for item in expected}
        values = value if isinstance(value, list) else [value]
        for item in values:
            normalized = str(item or "").strip().split("/")[-1].lower()
            if normalized in expected_lower:
                return True
        return False

    @staticmethod
    def _schema_image_urls(value: Any) -> list[str]:
        urls: list[str] = []

        def _add(candidate: Any) -> None:
            url = str(candidate or "").strip()
            if url.startswith(("http://", "https://")) and url not in urls:
                urls.append(url)

        def _walk(node: Any) -> None:
            if isinstance(node, str):
                _add(node)
            elif isinstance(node, list):
                for item in node:
                    _walk(item)
            elif isinstance(node, dict):
                for key in ("url", "contentUrl", "thumbnailUrl"):
                    if key in node:
                        _walk(node.get(key))
                if "image" in node:
                    _walk(node.get("image"))

        _walk(value)
        return urls

    def _extract_schema_org_post_metadata(self, html: str, *, post_url: str | None = None) -> dict[str, Any]:
        for item in self._iter_schema_org_json_ld(html):
            if not self._schema_type_matches(
                item.get("@type"),
                "DiscussionForumPosting",
                "SocialMediaPosting",
                "BlogPosting",
                "Posting",
            ):
                continue
            schema_url = str(item.get("url") or post_url or "").strip()
            published_raw = str(item.get("datePublished") or item.get("uploadDate") or "").strip()
            published_at = self._parse_timestamp(published_raw) if published_raw else 0
            author_raw = item.get("author")
            author = author_raw if isinstance(author_raw, dict) else {}
            return {
                "schema_type": item.get("@type"),
                "url": schema_url or None,
                "datePublished": published_raw or None,
                "published_at": published_at or None,
                "date_time": datetime.fromtimestamp(published_at, tz=UTC).isoformat() if published_at else None,
                "headline": str(item.get("headline") or "").strip() or None,
                "text": str(item.get("text") or item.get("articleBody") or item.get("description") or "").strip()
                or None,
                "image_urls": self._schema_image_urls(item.get("image")),
                "author_name": str(author.get("name") or "").strip() or None,
                "author_url": str(author.get("url") or "").strip() or None,
            }
        return {}

    def _fetch_post_schema_org_metadata(
        self,
        post_url: str,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
    ) -> dict[str, Any]:
        normalized_url = str(post_url or "").strip()
        if not normalized_url:
            return {}
        cached = getattr(self, "_post_schema_org_cache", {}).get(normalized_url)
        if cached is not None:
            return dict(cached)

        self._rate_limit(delay, fast_mode=fast_mode)
        try:
            response = self.session.get(
                normalized_url,
                headers=self._get_headers(),
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            self._track_response_status(response.status_code)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code:
                self._track_response_status(status_code)
            logger.debug("Failed to fetch YouTube post schema.org metadata for %s: %s", normalized_url, exc)
            return {}
        metadata = self._extract_schema_org_post_metadata(response.text or "", post_url=normalized_url)
        self._post_schema_org_cache[normalized_url] = dict(metadata)
        return metadata

    def _post_id_from_renderer(self, renderer: dict[str, Any]) -> str:
        if not isinstance(renderer, dict):
            return ""
        for key in ("postId", "backstagePostId", "entityId", "id"):
            candidate = str(renderer.get(key) or "").strip()
            if re.fullmatch(r"[A-Za-z0-9_-]{8,}", candidate):
                return candidate
        stack: list[Any] = [renderer]
        visited = 0
        while stack and visited < 2000:
            node = stack.pop()
            visited += 1
            if isinstance(node, str):
                match = re.search(r"/post/([A-Za-z0-9_-]{8,})", node)
                if match:
                    return match.group(1)
            elif isinstance(node, dict):
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
        return ""

    def _post_url_from_renderer(self, renderer: dict[str, Any], post_id: str) -> str:
        stack: list[Any] = [renderer]
        visited = 0
        while stack and visited < 2000:
            node = stack.pop()
            visited += 1
            if isinstance(node, str):
                match = re.search(r"((?:https://www\.youtube\.com)?/post/[A-Za-z0-9_-]{8,})", node)
                if match:
                    return self._normalize_youtube_url(match.group(1)) or ""
            elif isinstance(node, dict):
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
        return f"https://www.youtube.com/post/{post_id}" if post_id else ""

    def _extract_post_image_urls(self, renderer: dict[str, Any]) -> list[str]:
        urls: list[str] = []

        def _add(candidate: Any) -> None:
            url = str(candidate or "").strip()
            if not url.startswith(("http://", "https://")):
                return
            normalized = url.lower()
            if "youtube.com/post/" in normalized or "/@" in normalized:
                return
            if not any(host in normalized for host in ("yt3.ggpht.com", "googleusercontent.com", "i.ytimg.com")):
                return
            if url not in urls:
                urls.append(url)

        def _walk(node: Any, parent_key: str = "") -> None:
            if isinstance(node, str):
                if parent_key.lower() in {"url", "contenturl", "thumbnailurl"}:
                    _add(node)
            elif isinstance(node, list):
                for item in node:
                    _walk(item, parent_key=parent_key)
            elif isinstance(node, dict):
                thumb = self._pick_largest_thumbnail_url(node)
                if thumb:
                    _add(thumb)
                for key, value in node.items():
                    _walk(value, parent_key=str(key))

        _walk(renderer)
        return urls

    @staticmethod
    def _extract_hashtags(text: str) -> list[str]:
        tags: list[str] = []
        for match in re.finditer(r"#([A-Za-z0-9_]+)", str(text or "")):
            tag = f"#{match.group(1)}"
            if tag not in tags:
                tags.append(tag)
        return tags

    def _parse_post_renderer(
        self,
        renderer: dict[str, Any],
        config: YouTubeScrapeConfig,
        *,
        fallback_channel_avatar_url: str | None = None,
    ) -> YouTubeVideo | None:
        post_id = self._post_id_from_renderer(renderer)
        post_url = self._post_url_from_renderer(renderer, post_id)
        if not post_id and post_url:
            match = re.search(r"/post/([A-Za-z0-9_-]{8,})", post_url)
            post_id = match.group(1) if match else ""
        if not post_id:
            return None

        content_text = (
            self._extract_text(renderer.get("contentText"))
            or self._extract_text(renderer.get("message"))
            or self._extract_text(renderer.get("text"))
            or self._extract_text(renderer.get("title"))
        )
        published_text = self._extract_text(renderer.get("publishedTimeText"))
        published_at = self._estimate_publish_date(published_text)
        schema_meta: dict[str, Any] = {}
        if bool(getattr(config, "fetch_post_schema_org", True)) and post_url:
            schema_meta = self._fetch_post_schema_org_metadata(
                post_url,
                min(max(float(config.delay_seconds or 0.0) * 0.25, 0.05), 0.35),
                fast_mode=config.fast_mode,
            )
            schema_ts = _safe_int(schema_meta.get("published_at"))
            if schema_ts > 0:
                published_at = schema_ts
            content_text = str(schema_meta.get("text") or content_text or "").strip()
            post_url = str(schema_meta.get("url") or post_url or "").strip()

        headline = str(schema_meta.get("headline") or "").strip()
        title = headline or (content_text[:96].strip() if content_text else "YouTube community post")
        media_urls = []
        for candidate in list(schema_meta.get("image_urls") or []) + self._extract_post_image_urls(renderer):
            candidate_url = str(candidate or "").strip()
            if candidate_url and candidate_url not in media_urls:
                media_urls.append(candidate_url)
        thumbnail_url = media_urls[0] if media_urls else ""
        likes = self._parse_compact_count_text(renderer.get("voteCount")) or 0
        comments = self._parse_compact_count_text(renderer.get("replyCount")) or 0
        channel_title = (
            self._extract_text(renderer.get("authorText"))
            or self._extract_text(renderer.get("ownerText"))
            or str(schema_meta.get("author_name") or "")
        )
        channel_avatar_url = self._extract_channel_avatar_from_renderer(
            renderer,
            fallback_channel_avatar_url=fallback_channel_avatar_url,
        )
        combined_text = f"{title} {content_text}".lower()
        keywords_matched = []
        for kw in config.keywords:
            kw_clean = kw.lower().lstrip("#")
            if kw_clean in combined_text:
                keywords_matched.append(kw)

        return YouTubeVideo(
            video_id=post_id,
            title=title,
            description=content_text,
            date_time=datetime.fromtimestamp(published_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
            if published_at
            else "",
            published_at=published_at,
            channel_id="",
            channel_title=channel_title,
            duration="",
            duration_seconds=0,
            views=0,
            likes=likes,
            comments=comments,
            url=post_url or f"https://www.youtube.com/post/{post_id}",
            thumbnail_url=thumbnail_url,
            tags=self._extract_hashtags(content_text),
            keywords_matched=keywords_matched,
            user_avatar_url=channel_avatar_url,
            is_short=False,
            source_surface="posts",
            published_text=published_text,
            media_urls=media_urls,
            schema_org=schema_meta,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _parse_view_count(self, view_text: str) -> int:
        """Parse view count from text like '1.2M views' or '1,234 views'."""
        if not view_text:
            return 0
        view_text = view_text.lower().replace(",", "").replace(" views", "").replace(" view", "")
        multiplier = 1
        if "k" in view_text:
            multiplier = 1000
            view_text = view_text.replace("k", "")
        elif "m" in view_text:
            multiplier = 1000000
            view_text = view_text.replace("m", "")
        elif "b" in view_text:
            multiplier = 1000000000
            view_text = view_text.replace("b", "")
        try:
            return int(float(view_text) * multiplier)
        except ValueError:
            return 0

    def _parse_duration_text(self, duration_text: str) -> int:
        """Parse duration from text like '5:30' or '1:02:03'."""
        if not duration_text:
            return 0
        parts = duration_text.split(":")
        try:
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            else:
                return int(parts[0])
        except ValueError:
            return 0

    def _estimate_publish_date(self, published_text: str) -> int:
        """Estimate publish date from relative text like '2 days ago'."""
        if not published_text:
            return 0
        now = datetime.now(tz=UTC)
        text = published_text.lower().replace("streamed", "").replace("premiered", "")
        text = re.sub(r"\s+", " ", text).strip(" .")
        text = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text)

        if text in {"today", "just now"}:
            return int(now.timestamp())
        if text == "yesterday":
            return int(now.timestamp() - 86400)

        # Parse absolute dates when YouTube returns full day precision.
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"):
            try:
                parsed = datetime.strptime(text, fmt).replace(tzinfo=UTC)
                return int(parsed.timestamp())
            except ValueError:
                continue

        # Parse relative time
        patterns = [
            (r"(\d+)\s*second", 1),
            (r"(\d+)\s*minute", 60),
            (r"(\d+)\s*hour", 3600),
            (r"(\d+)\s*day", 86400),
            (r"(\d+)\s*week", 604800),
            (r"(\d+)\s*month", 2592000),
            (r"(\d+)\s*year", 31536000),
        ]

        for pattern, multiplier in patterns:
            match = re.search(pattern, text)
            if match:
                offset = int(match.group(1)) * multiplier
                return int(now.timestamp() - offset)

        return 0

    def _is_low_precision_publish_text(self, published_text: str) -> bool:
        text = (published_text or "").lower()
        return "month" in text or "year" in text

    def _parse_precise_publish_candidate(self, candidate: str) -> int:
        value = str(candidate or "").strip()
        if not value:
            return 0

        ts = self._parse_timestamp(value)
        if ts > 0:
            return ts

        match = self.DATE_ONLY_PREFIX_PATTERN.search(value)
        if not match:
            return 0
        try:
            parsed = datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=UTC)
            return int(parsed.timestamp())
        except ValueError:
            return 0

    def _extract_precise_publish_timestamp_from_html(self, body: str) -> int:
        ts = 0
        for pattern in self.PUBLISHED_DATE_PATTERNS:
            for match in pattern.finditer(body):
                ts = self._parse_precise_publish_candidate(match.group(1))
                if ts > 0:
                    return ts
        for pattern in self.PLAYER_MICROFORMAT_PATTERNS:
            for match in pattern.finditer(body):
                ts = self._parse_precise_publish_candidate(match.group(1))
                if ts > 0:
                    return ts
        return 0

    def _fetch_precise_publish_timestamp(self, video_id: str, delay: float = 2.0, *, fast_mode: bool = False) -> int:
        """Fetch exact upload date from watch-page and Shorts-page metadata."""
        cached = self._precise_publish_ts_cache.get(video_id)
        if cached is not None:
            return cached

        ts = 0
        for url in (
            self.VIDEO_WATCH_URL.format(video_id=video_id),
            f"https://www.youtube.com/shorts/{video_id}",
        ):
            self._rate_limit(delay, fast_mode=fast_mode)
            try:
                response = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                )
                self._track_response_status(response.status_code)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                status_code = getattr(getattr(e, "response", None), "status_code", None)
                if status_code:
                    self._track_response_status(status_code)
                logger.debug("Failed to fetch precise publish timestamp for %s via %s: %s", video_id, url, e)
                continue
            ts = self._extract_precise_publish_timestamp_from_html(response.text or "")
            if ts > 0:
                break
        self._precise_publish_ts_cache[video_id] = ts
        return ts

    def _extract_shorts_like_count_from_html(self, body: str) -> int:
        html = str(body or "")
        if not html:
            return 0
        for pattern in self.SHORTS_LIKE_COUNT_PATTERNS:
            match = pattern.search(html)
            if not match:
                continue
            count_text = unescape(str(match.group("count") or "")).strip()
            likes = self._parse_like_count(count_text)
            if likes > 0:
                return likes
        return 0

    def _fetch_shorts_like_count(self, video_id: str, delay: float = 2.0, *, fast_mode: bool = False) -> int:
        normalized_video_id = str(video_id or "").strip()
        if not normalized_video_id:
            return 0
        self._rate_limit(delay, fast_mode=fast_mode)
        try:
            response = self.session.get(
                f"https://www.youtube.com/shorts/{normalized_video_id}",
                headers=self._get_headers(),
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            self._track_response_status(response.status_code)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code:
                self._track_response_status(status_code)
            logger.debug("Failed to fetch Shorts like count for %s: %s", normalized_video_id, exc)
            return 0
        return self._extract_shorts_like_count_from_html(response.text or "")

    @staticmethod
    def _extract_json_object_after_marker(text: str, marker: str) -> str | None:
        marker_idx = text.find(marker)
        if marker_idx < 0:
            return None
        start = text.find("{", marker_idx + len(marker))
        if start < 0:
            return None

        depth = 0
        in_string = False
        escaped = False
        for idx in range(start, len(text)):
            char = text[idx]
            if in_string:
                if escaped:
                    escaped = False
                    continue
                if char == "\\":
                    escaped = True
                    continue
                if char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
                continue
            if char == "{":
                depth += 1
                continue
            if char == "}":
                depth -= 1
                if depth == 0:
                    return text[start : idx + 1]
        return None

    @classmethod
    def _extract_player_response_from_html(cls, html: str) -> dict[str, Any] | None:
        for marker in cls.PLAYER_RESPONSE_MARKERS:
            payload = cls._extract_json_object_after_marker(html, marker)
            if not payload:
                continue
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                return data
        return None

    def _build_caption_candidates_from_source(
        self,
        *,
        source_name: str,
        source_payload: dict[str, Any],
        is_auto: bool,
        preferred_languages: list[str],
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for language, tracks in source_payload.items():
            if not isinstance(tracks, list):
                continue
            for track in tracks:
                if not isinstance(track, dict):
                    continue
                url = str(track.get("url") or "").strip()
                ext = str(track.get("ext") or "").strip().lower()
                if not url:
                    continue
                if ext and ext not in {"json3", "srv3", "vtt", "ttml"}:
                    continue
                score = self._caption_track_score(
                    language=str(language or ""),
                    ext=ext or "",
                    is_auto=is_auto,
                    preferred_languages=preferred_languages,
                )
                candidates.append(
                    {
                        "url": url,
                        "ext": ext or "vtt",
                        "language": str(language or ""),
                        "source": source_name,
                        "score": score,
                    }
                )
        return candidates

    def _caption_candidates_from_player_response(
        self,
        player_response: dict[str, Any],
        *,
        preferred_languages: list[str],
    ) -> list[dict[str, Any]]:
        captions = player_response.get("captions")
        if not isinstance(captions, dict):
            return []
        tracklist = captions.get("playerCaptionsTracklistRenderer")
        if not isinstance(tracklist, dict):
            return []
        caption_tracks = tracklist.get("captionTracks")
        if not isinstance(caption_tracks, list):
            return []

        candidates: list[dict[str, Any]] = []
        for track in caption_tracks:
            if not isinstance(track, dict):
                continue
            url = str(track.get("baseUrl") or track.get("url") or "").strip()
            language = str(track.get("languageCode") or "").strip()
            if not url:
                continue
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            ext = str((query.get("fmt") or query.get("format") or ["vtt"])[0] or "vtt").strip().lower()
            if ext and ext not in {"json3", "srv3", "vtt", "ttml"}:
                ext = "vtt"
            is_auto = str(track.get("kind") or "").strip().lower() == "asr"
            score = self._caption_track_score(
                language=language,
                ext=ext,
                is_auto=is_auto,
                preferred_languages=preferred_languages,
            )
            candidates.append(
                {
                    "url": url,
                    "ext": ext,
                    "language": language or None,
                    "source": "auto_captions" if is_auto else "manual_captions",
                    "score": score,
                }
            )
        return candidates

    def _refine_video_publish_timestamp_if_needed(
        self,
        video: YouTubeVideo,
        config: YouTubeScrapeConfig,
        current_in_range: bool | None,
    ) -> bool | None:
        if not (config.date_start or config.date_end):
            return current_in_range
        if not video.video_id:
            return current_in_range
        if self._video_surface(video) == "posts":
            return current_in_range

        needs_refine = video.published_at <= 0
        if not needs_refine and self._is_low_precision_publish_text(video.published_text):
            needs_refine = True
        if not needs_refine:
            return current_in_range

        is_short = bool(getattr(video, "is_short", False)) or self._video_surface(video) == "shorts"
        self._precise_publish_attempts += 1
        if is_short:
            self._shorts_precise_publish_attempts += 1
        precise_delay = min(max(float(config.delay_seconds or 0.0) * 0.25, 0.05), 0.35)
        precise_ts = self._fetch_precise_publish_timestamp(video.video_id, precise_delay, fast_mode=config.fast_mode)
        if precise_ts <= 0:
            self._precise_publish_failures += 1
            if is_short:
                self._shorts_precise_publish_failures += 1
            return current_in_range

        self._precise_publish_successes += 1
        if is_short:
            self._shorts_precise_publish_successes += 1
        if precise_ts != video.published_at:
            video.published_at = precise_ts
            video.date_time = datetime.fromtimestamp(precise_ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        return config.is_in_date_range(video.published_at)

    def _channel_surface_url(self, handle: str, surface: str) -> str:
        normalized_surface = str(surface or "videos").strip().lower()
        if normalized_surface == "shorts":
            return self.CHANNEL_SHORTS_URL.format(handle=handle)
        if normalized_surface == "posts":
            return self.CHANNEL_POSTS_URL.format(handle=handle)
        return self.CHANNEL_VIDEOS_URL.format(handle=handle)

    def _channel_about_url(self, handle: str) -> str:
        return self.CHANNEL_ABOUT_URL.format(handle=handle)

    def fetch_channel_videos(
        self,
        handle: str,
        delay: float = 2.0,
        surface: str = "videos",
        *,
        fast_mode: bool = False,
    ) -> dict | None:
        """Fetch videos, shorts, or community posts from a YouTube channel page."""
        self._rate_limit(delay, fast_mode=fast_mode)

        url = self._channel_surface_url(handle, surface)
        headers = self._get_headers()

        try:
            response = self.session.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT_SECONDS)
            self._track_response_status(response.status_code)
            response.raise_for_status()
            return self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code:
                self._track_response_status(status_code)
            logger.error("Failed to fetch channel %s surface=%s: %s", f"@{handle}", surface, e)
            return None

    def fetch_channel_about(self, handle: str, delay: float = 2.0, *, fast_mode: bool = False) -> dict | None:
        self._rate_limit(delay, fast_mode=fast_mode)

        url = self._channel_about_url(handle)
        headers = self._get_headers()

        try:
            response = self.session.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT_SECONDS)
            self._track_response_status(response.status_code)
            response.raise_for_status()
            return self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code:
                self._track_response_status(status_code)
            logger.error("Failed to fetch channel about for %s: %s", f"@{handle}", e)
            return None

    def _fetch_continuation(
        self, continuation_token: str, delay: float = 2.0, *, fast_mode: bool = False
    ) -> dict | None:
        """Fetch next page of channel videos using continuation token."""
        import json

        self._rate_limit(delay, fast_mode=fast_mode)
        self._last_channel_continuation_error = None

        payload = {
            "context": self.INNERTUBE_CONTEXT,
            "continuation": continuation_token,
        }
        headers = self._get_headers()
        headers["content-type"] = "application/json"

        browse_url = "https://www.youtube.com/youtubei/v1/browse"
        try:
            # Channel continuation pages run on the hot path for bounded admin
            # backfills. Bypass the session retry adapter here so a single slow
            # YouTube continuation cannot hold a Modal worker for minutes.
            response = requests.post(
                browse_url,
                headers=headers,
                data=json.dumps(payload),
                # requests accepts urllib3 Timeout objects at runtime; the stubs
                # only admit float/tuple, so widen for the type checker.
                timeout=cast("Any", self._continuation_request_timeout()),
            )
            self._track_response_status(response.status_code)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code:
                self._track_response_status(status_code)
            self._last_channel_continuation_error = "request_error"
            logger.error(f"Continuation fetch failed: {e}")
            return None

    def _extract_channel_continuation_token(self, data: dict) -> str | None:
        """Extract continuation token from channel page data."""
        contents = data.get("contents", {})
        tabs = contents.get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
        for tab in tabs:
            tab_content = tab.get("tabRenderer", {}).get("content", {})
            rich_grid = tab_content.get("richGridRenderer", {})
            for item in rich_grid.get("contents", []):
                cont = item.get("continuationItemRenderer", {})
                if cont:
                    endpoint = cont.get("continuationEndpoint", {})
                    return endpoint.get("continuationCommand", {}).get("token")
        return None

    def _extract_continuation_videos_and_token(self, data: dict) -> tuple[list[dict], str | None]:
        """Extract videos and next continuation token from a continuation response."""
        renderers = []
        next_token = None

        on_response = data.get("onResponseReceivedActions", [])
        for action in on_response:
            append_items = action.get("appendContinuationItemsAction", {})
            for item in append_items.get("continuationItems", []):
                content = item.get("richItemRenderer", {}).get("content", {})
                video_renderer = content.get("videoRenderer", {})
                if video_renderer:
                    renderers.append(video_renderer)
                reel_renderer = content.get("reelItemRenderer", {})
                if reel_renderer:
                    renderers.append(reel_renderer)
                shorts_lockup = content.get("shortsLockupViewModel", {})
                if shorts_lockup:
                    renderers.append(self._shorts_lockup_to_renderer(shorts_lockup))
                post_renderer = content.get("backstagePostRenderer", {})
                if post_renderer:
                    renderers.append(post_renderer)
                post_thread = content.get("backstagePostThreadRenderer", {})
                if post_thread:
                    thread_renderer = post_thread.get("post", {}).get("backstagePostRenderer", {})
                    if thread_renderer:
                        renderers.append(thread_renderer)
                cont = item.get("continuationItemRenderer", {})
                if cont:
                    endpoint = cont.get("continuationEndpoint", {})
                    next_token = endpoint.get("continuationCommand", {}).get("token")

        return renderers, next_token

    def search_channel_videos(
        self, handle: str, query: str, delay: float = 2.0, *, fast_mode: bool = False
    ) -> dict | None:
        """Search for videos from a specific channel with a query."""
        self._rate_limit(delay, fast_mode=fast_mode)

        # YouTube search with channel filter
        search_query = f"{query} site:youtube.com/@{handle}"
        params = {"search_query": search_query}
        headers = self._get_headers()

        try:
            response = self.session.get(
                self.CHANNEL_SEARCH_URL,
                params=params,
                headers=headers,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            self._track_response_status(response.status_code)
            response.raise_for_status()
            return self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code:
                self._track_response_status(status_code)
            logger.error(f"Search failed: {e}")
            return None

    def _iter_video_renderers(self, data: dict):
        """Iterate through video renderers in YouTube data."""
        # Navigate through the nested structure
        contents = data.get("contents", {})

        # Try channel page structure
        tabs = contents.get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
        for tab in tabs:
            tab_content = tab.get("tabRenderer", {}).get("content", {})
            rich_grid = tab_content.get("richGridRenderer", {})
            for item in rich_grid.get("contents", []):
                content = item.get("richItemRenderer", {}).get("content", {})
                video_renderer = content.get("videoRenderer", {})
                if video_renderer:
                    yield video_renderer
                reel_renderer = content.get("reelItemRenderer", {})
                if reel_renderer:
                    yield reel_renderer
                shorts_lockup = content.get("shortsLockupViewModel", {})
                if shorts_lockup:
                    yield self._shorts_lockup_to_renderer(shorts_lockup)

        # Try search results structure
        primary_contents = contents.get("twoColumnSearchResultsRenderer", {}).get("primaryContents", {})
        section_list = primary_contents.get("sectionListRenderer", {})
        for section in section_list.get("contents", []):
            items = section.get("itemSectionRenderer", {}).get("contents", [])
            for item in items:
                video_renderer = item.get("videoRenderer", {})
                if video_renderer:
                    yield video_renderer

    def _iter_post_renderers(self, data: dict):
        """Iterate through community post renderers in YouTube data."""
        stack: list[Any] = [data]
        seen_ids: set[str] = set()
        visited = 0
        while stack and visited < 12000:
            node = stack.pop()
            visited += 1
            if isinstance(node, dict):
                renderer = node.get("backstagePostRenderer")
                if isinstance(renderer, dict):
                    post_id = self._post_id_from_renderer(renderer)
                    dedupe_key = post_id or str(id(renderer))
                    if dedupe_key not in seen_ids:
                        seen_ids.add(dedupe_key)
                        yield renderer
                post_thread = node.get("backstagePostThreadRenderer")
                if isinstance(post_thread, dict):
                    thread_renderer = post_thread.get("post", {}).get("backstagePostRenderer", {})
                    if isinstance(thread_renderer, dict):
                        post_id = self._post_id_from_renderer(thread_renderer)
                        dedupe_key = post_id or str(id(thread_renderer))
                        if dedupe_key not in seen_ids:
                            seen_ids.add(dedupe_key)
                            yield thread_renderer
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)

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
            logger.debug("YouTube scrape progress callback raised", exc_info=True)

    @staticmethod
    def _video_surface(video: YouTubeVideo) -> str:
        surface = str(getattr(video, "source_surface", "") or "").strip().lower()
        if surface in set(YOUTUBE_CHANNEL_SURFACES):
            return surface
        if bool(getattr(video, "is_short", False)):
            return "shorts"
        url = str(getattr(video, "url", "") or "").lower()
        if "/post/" in url:
            return "posts"
        return "shorts" if "/shorts/" in url else "videos"

    def _apply_surface_guaranteed_limit(
        self,
        videos: list[YouTubeVideo],
        *,
        max_results: int | None,
    ) -> tuple[list[YouTubeVideo], bool, int | None]:
        if max_results is None:
            return videos, False, None
        requested_limit = max(0, int(max_results))
        if requested_limit <= 0:
            return [], False, 0

        surfaces_present = {self._video_surface(video) for video in videos if self._video_surface(video)}
        guaranteed_surfaces = [surface for surface in YOUTUBE_CHANNEL_SURFACES if surface in surfaces_present]
        effective_limit = max(requested_limit, len(guaranteed_surfaces)) if guaranteed_surfaces else requested_limit
        if len(videos) <= effective_limit:
            return videos, effective_limit != requested_limit, effective_limit

        indexed_videos = list(enumerate(videos))

        def _sort_key(item: tuple[int, YouTubeVideo]) -> tuple[int, str, int]:
            idx, candidate = item
            published_at = int(getattr(candidate, "published_at", 0) or 0)
            video_id = str(getattr(candidate, "video_id", "") or "")
            return (-published_at, video_id, idx)

        sorted_items = sorted(indexed_videos, key=_sort_key)
        if len(guaranteed_surfaces) <= 1:
            limited = [video for _, video in sorted_items[:effective_limit]]
            return limited, False, effective_limit

        selected_indices: set[int] = set()
        selected_items: list[tuple[int, YouTubeVideo]] = []
        for surface in guaranteed_surfaces:
            for item in sorted_items:
                idx, candidate = item
                if idx in selected_indices:
                    continue
                if self._video_surface(candidate) != surface:
                    continue
                selected_indices.add(idx)
                selected_items.append(item)
                break

        for item in sorted_items:
            idx, _candidate = item
            if len(selected_items) >= effective_limit:
                break
            if idx in selected_indices:
                continue
            selected_indices.add(idx)
            selected_items.append(item)

        selected_items = sorted(selected_items, key=_sort_key)
        limited = [video for _, video in selected_items[:effective_limit]]
        return limited, effective_limit != requested_limit, effective_limit

    def _parse_ytdlp_playlist_stdout(self, stdout: str) -> dict[str, Any]:
        raw = str(stdout or "").strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else {}
        except json.JSONDecodeError:
            entries: list[dict[str, Any]] = []
            for line in raw.splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    entries.append(item)
            return {"entries": entries} if entries else {}

    def _scrape_playlist_via_ytdlp(
        self,
        config: YouTubeScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[YouTubeVideo]:
        playlist_id, playlist_url = self._playlist_url(
            playlist_id=config.playlist_id,
            playlist_url=config.playlist_url,
        )
        if not playlist_id:
            raise ValueError("YouTube playlist sources require a playlist id or playlist URL")
        logger.info("Starting YouTube playlist scrape for %s", playlist_id)
        self._last_transport = "yt-dlp"
        self._fallback_chain = ["yt_dlp_playlist"]
        self._last_stop_reason = None
        self._last_retryable = False
        self._last_complete = False
        self._last_source_mode = "playlist"
        if not shutil.which("yt-dlp"):
            self.last_retrieval_meta = {
                "retrieval_mode": "playlist_ytdlp",
                "playlist_id": playlist_id,
                "playlist_url": playlist_url,
                "pages_scanned": 0,
                "posts_checked": 0,
                "matched_posts": 0,
                "error_code": "youtube_ytdlp_unavailable",
                "error_class": "YouTubeYtDlpUnavailable",
                "retryable": True,
            }
            self._last_stop_reason = "ytdlp_unavailable"
            self._last_retryable = True
            return []

        self._emit_progress(
            progress_cb,
            phase="scrape_playlist_ytdlp_start",
            pages_scanned=0,
            posts_checked=0,
            matched_posts=0,
        )
        cmd = [
            "yt-dlp",
            "--dump-single-json",
            "--skip-download",
            "--ignore-errors",
            playlist_url,
        ]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=max(60, int(self.YTDLP_SEARCH_TIMEOUT_SECONDS) * 3),
                check=False,
            )
        except subprocess.TimeoutExpired:
            self.last_retrieval_meta = {
                "retrieval_mode": "playlist_ytdlp",
                "playlist_id": playlist_id,
                "playlist_url": playlist_url,
                "pages_scanned": 1,
                "posts_checked": 0,
                "matched_posts": 0,
                "error_code": "youtube_playlist_ytdlp_timeout",
                "error_class": "YouTubePlaylistYtDlpTimeout",
                "retryable": True,
            }
            self._last_stop_reason = "ytdlp_timeout"
            self._last_retryable = True
            return []

        payload = self._parse_ytdlp_playlist_stdout(proc.stdout)
        entries = payload.get("entries") if isinstance(payload, dict) else None
        entry_list = [entry for entry in (entries or []) if isinstance(entry, dict)]
        if not entry_list and payload.get("id"):
            entry_list = [payload]

        posts_checked = len(entry_list)
        matched: list[YouTubeVideo] = []
        seen_ids: set[str] = set()
        for entry in entry_list:
            video = self._video_from_ytdlp_payload(entry, config, source_surface="videos")
            if not video or video.video_id in seen_ids:
                continue
            seen_ids.add(video.video_id)
            matched.append(video)

        enrichment_meta = self._maybe_enrich_videos_via_ytdlp(
            matched,
            config,
            delay=config.delay_seconds,
            fast_mode=config.fast_mode,
        )
        surface_cap_override_applied = False
        effective_result_cap: int | None = None
        if config.max_results:
            matched, surface_cap_override_applied, effective_result_cap = self._apply_surface_guaranteed_limit(
                matched,
                max_results=config.max_results,
            )
        matched.sort(key=lambda video: int(video.published_at or 0), reverse=True)
        playlist_title = str(payload.get("title") or payload.get("playlist_title") or "").strip() or None
        channel_id = str(payload.get("channel_id") or payload.get("uploader_id") or "").strip() or None
        channel_title = str(payload.get("channel") or payload.get("uploader") or "").strip() or None
        total_posts = _safe_int(payload.get("playlist_count")) or posts_checked or len(matched)
        self._last_stop_reason = "complete" if proc.returncode == 0 else "ytdlp_nonzero_exit"
        self._last_retryable = proc.returncode != 0 and not matched
        self._last_complete = proc.returncode == 0 or bool(matched)
        self.last_retrieval_meta = {
            "retrieval_mode": "playlist_ytdlp",
            "playlist_id": playlist_id,
            "playlist_url": playlist_url,
            "playlist_title": playlist_title,
            "pages_scanned": 1,
            "posts_checked": posts_checked,
            "matched_posts": len(matched),
            "checked_renderers": posts_checked,
            "total_posts": total_posts,
            "first_page_count": posts_checked,
            "first_page_counts": {
                "videos": sum(1 for video in matched if self._video_surface(video) != "shorts"),
                "shorts": sum(1 for video in matched if self._video_surface(video) == "shorts"),
            },
            "shorts_candidates_found": sum(1 for video in matched if self._video_surface(video) == "shorts"),
            "surface_cap_override_applied": bool(surface_cap_override_applied),
            "requested_max_results": int(config.max_results) if config.max_results is not None else None,
            "effective_max_results": effective_result_cap,
            "canonical_handle": playlist_id.lower(),
            "canonical_channel_id": channel_id,
            "resolved_channel_title": channel_title,
            "resolved_channel_avatar_url": None,
            "ytdlp_returncode": proc.returncode,
            "yt_dlp_enrichment": enrichment_meta,
        }
        if proc.returncode != 0 and not matched:
            self.last_retrieval_meta["error_code"] = "youtube_playlist_ytdlp_failed"
            self.last_retrieval_meta["error_class"] = "YouTubePlaylistYtDlpFailed"
            self.last_retrieval_meta["retryable"] = True
            stderr = str(proc.stderr or "").strip()
            if stderr:
                self.last_retrieval_meta["error_message"] = stderr[-500:]

        self._emit_progress(
            progress_cb,
            phase="scrape_complete",
            pages_scanned=1,
            posts_checked=posts_checked,
            matched_posts=len(matched),
        )
        logger.info("Playlist scrape complete: found %d videos for %s", len(matched), playlist_id)
        return matched

    def _scrape_channel_via_ytdlp(
        self,
        config: YouTubeScrapeConfig,
        *,
        canonical_handle: str | None = None,
        canonical_channel_id: str | None = None,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[list[YouTubeVideo], dict[str, Any]]:
        handle = (canonical_handle or config.channel_handle or "").strip().lstrip("@")
        normalized_handle = self._normalize_handle(handle)
        if not normalized_handle or not shutil.which("yt-dlp"):
            return [], {
                "used": False,
                "available": bool(shutil.which("yt-dlp")),
                "posts_checked": 0,
                "matched_posts": 0,
            }

        requested_limit = int(config.max_results or 100)
        playlist_end = max(1, min(requested_limit, 200))
        matched: list[YouTubeVideo] = []
        seen_ids: set[str] = set()
        posts_checked = 0
        requested_surfaces = _normalize_youtube_surfaces(config.surfaces)
        fallback_surfaces = [surface for surface in requested_surfaces if surface in {"videos", "shorts"}]
        surfaces_checked: dict[str, int] = dict.fromkeys(requested_surfaces, 0)
        errors: list[str] = []

        for surface in fallback_surfaces:
            channel_url = f"https://www.youtube.com/@{normalized_handle}/{surface}"
            cmd = [
                "yt-dlp",
                "--dump-json",
                "--skip-download",
                "--ignore-errors",
                "--playlist-end",
                str(playlist_end),
                channel_url,
            ]
            logger.info("Channel page was empty; trying yt-dlp %s fallback for @%s", surface, normalized_handle)
            try:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=max(60, int(self.YTDLP_SEARCH_TIMEOUT_SECONDS) * 3),
                    check=False,
                )
            except subprocess.TimeoutExpired:
                errors.append(f"{surface}:timeout")
                continue

            payload = self._parse_ytdlp_playlist_stdout(proc.stdout)
            entries = payload.get("entries") if isinstance(payload, dict) else None
            entry_list = [entry for entry in (entries or []) if isinstance(entry, dict)]
            if not entry_list and isinstance(payload, dict) and payload.get("id"):
                entry_list = [payload]
            posts_checked += len(entry_list)
            surfaces_checked[surface] = len(entry_list)

            if proc.returncode != 0 and not entry_list:
                stderr = str(proc.stderr or "").strip()
                errors.append(f"{surface}:{stderr[-160:] or proc.returncode}")
                continue

            for entry in entry_list:
                owner_candidates = self._extract_ytdlp_owner_candidates(entry)
                if owner_candidates and not self._ytdlp_entry_matches_owner(
                    entry,
                    target_handle=normalized_handle,
                    target_channel_id=canonical_channel_id,
                ):
                    continue
                video = self._video_from_ytdlp_payload(entry, config, source_surface=surface)
                if not video or video.video_id in seen_ids:
                    continue
                seen_ids.add(video.video_id)
                matched.append(video)

            self._emit_progress(
                progress_cb,
                phase="scrape_ytdlp_channel_fallback",
                pages_scanned=sum(1 for count in surfaces_checked.values() if count > 0),
                posts_checked=posts_checked,
                matched_posts=len(matched),
            )

        matched.sort(key=lambda video: int(video.published_at or 0), reverse=True)
        if config.max_results:
            matched, _override_applied, _effective_limit = self._apply_surface_guaranteed_limit(
                matched,
                max_results=config.max_results,
            )
        return matched, {
            "used": True,
            "available": True,
            "posts_checked": posts_checked,
            "matched_posts": len(matched),
            "surfaces_checked": surfaces_checked,
            "unsupported_surfaces": [surface for surface in requested_surfaces if surface not in {"videos", "shorts"}],
            "errors": errors,
        }

    def scrape(
        self,
        config: YouTubeScrapeConfig,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[YouTubeVideo]:
        """
        Scrape videos from a YouTube channel with filtering.

        Always fetches from the channel page directly (not search) to ensure
        correct channel filtering, then applies keyword matching locally.
        Supports pagination via continuation tokens.

        Args:
            config: YouTubeScrapeConfig with channel handle, keywords, date range, etc.

        Returns:
            List of YouTubeVideo objects matching the filters.
        """
        playlist_id, _playlist_url = self._playlist_url(
            playlist_id=config.playlist_id,
            playlist_url=config.playlist_url,
        )
        if playlist_id or str(config.source_type or "").strip().lower() == "playlist":
            return self._scrape_playlist_via_ytdlp(config, progress_cb=progress_cb)

        handle = (config.channel_handle or "").lstrip("@")
        if not handle:
            raise ValueError("YouTube account scrapes require a channel handle")
        logger.info(f"Starting YouTube scrape for @{handle}")
        if config.keywords:
            logger.info(f"Filtering by keywords: {config.keywords}")
        if config.date_start or config.date_end:
            logger.info(f"Date range: {config.date_start} to {config.date_end}")

        requested_surfaces = _normalize_youtube_surfaces(config.surfaces)
        videos = []
        continuation_pages_by_surface: dict[str, int] = dict.fromkeys(requested_surfaces, 0)
        surface_pages_scanned: dict[str, int] = dict.fromkeys(requested_surfaces, 0)
        checked_renderers = 0
        timestamp_unknown_count = 0
        in_range_hits = 0
        no_hit_pages = 0
        pre_window_pages = 0
        after_window_pages = 0
        ownership_filtered = 0
        scan_capped_reason: str | None = None
        continuation_failure_reason: str | None = None
        continuation_failure_count = 0
        first_page_counts: dict[str, int] = dict.fromkeys(requested_surfaces, 0)
        canonical_handle = self._normalize_handle(handle)
        canonical_channel_id = ""
        resolved_channel_title: str | None = None
        resolved_channel_avatar_url: str | None = None
        self._precise_publish_attempts = 0
        self._precise_publish_successes = 0
        self._precise_publish_failures = 0
        self._shorts_precise_publish_attempts = 0
        self._shorts_precise_publish_successes = 0
        self._shorts_precise_publish_failures = 0
        surface_cap_override_applied = False
        effective_result_cap: int | None = None
        initial_surfaces_processed: set[str] = set()

        def _total_pages_scanned() -> int:
            return int(sum(surface_pages_scanned.values()))

        def _total_continuation_pages_scanned() -> int:
            return int(sum(continuation_pages_by_surface.values()))

        def _bounded_date_window_scan() -> bool:
            return bool(config.date_start or config.date_end)

        def _continuation_page_cap_reached(surface: str) -> bool:
            if not config.max_pages:
                return False
            if _bounded_date_window_scan():
                return _total_continuation_pages_scanned() >= config.max_pages
            return int(continuation_pages_by_surface.get(surface, 0) or 0) >= config.max_pages

        def _unique_collection_state() -> tuple[int, set[str]]:
            seen_ids: set[str] = set()
            surfaces_present: set[str] = set()
            unique_count = 0
            for candidate in videos:
                video_id = str(getattr(candidate, "video_id", "") or "")
                if not video_id or video_id in seen_ids:
                    continue
                seen_ids.add(video_id)
                unique_count += 1
                surfaces_present.add(self._video_surface(candidate))
            return unique_count, surfaces_present

        def _requested_max_results() -> int | None:
            if not config.max_results:
                return None
            return max(0, int(config.max_results))

        def _current_surface_has_enough_for_next_surface() -> bool:
            requested_limit = _requested_max_results()
            if requested_limit is None:
                return False
            if requested_limit <= 0:
                return True
            if not (set(requested_surfaces) - initial_surfaces_processed):
                return False
            unique_count, _surfaces_present = _unique_collection_state()
            return unique_count >= requested_limit

        def _collection_satisfies_max_results() -> bool:
            requested_limit = _requested_max_results()
            if requested_limit is None:
                return False
            if requested_limit <= 0:
                return True
            if set(requested_surfaces) - initial_surfaces_processed:
                return False
            unique_count, surfaces_present = _unique_collection_state()
            guaranteed_surface_count = len([surface for surface in requested_surfaces if surface in surfaces_present])
            effective_limit = max(requested_limit, guaranteed_surface_count)
            return unique_count >= effective_limit

        for surface in requested_surfaces:
            logger.info("Fetching %s from @%s channel page...", surface, handle)
            surface_no_hit_pages = 0
            surface_pre_window_pages = 0
            surface_after_window_pages = 0
            initial_ownership_counter = [0]
            data = self.fetch_channel_videos(handle, config.delay_seconds, surface=surface, fast_mode=config.fast_mode)
            if not data:
                logger.warning("Failed to fetch channel page for @%s (surface=%s)", handle, surface)
                continue
            resolved_handle, resolved_channel_id = self._extract_channel_identity_from_data(data, handle)
            if resolved_handle:
                canonical_handle = resolved_handle
            if resolved_channel_id:
                canonical_channel_id = resolved_channel_id
            if not resolved_channel_title:
                resolved_channel_title = self._extract_channel_title_from_data(data)
            if not resolved_channel_avatar_url:
                resolved_channel_avatar_url = self._extract_channel_avatar_from_data(data)

            initial_result = self._process_video_data(
                data,
                config,
                surface=surface,
                target_handle=canonical_handle or handle,
                ownership_filtered_counter=initial_ownership_counter,
                return_stats=True,
            )
            if isinstance(initial_result, tuple):
                initial_page_videos, initial_stats = initial_result
            else:
                initial_page_videos = initial_result
                initial_stats = {
                    "checked_renderers": len(initial_page_videos),
                    "before_window_items": 0,
                    "after_window_items": 0,
                    "window_candidate_items": len(initial_page_videos),
                    "timestamp_unknown": 0,
                    "in_range_hits": 0,
                }
            ownership_filtered += initial_ownership_counter[0]
            checked_renderers += int(initial_stats.get("checked_renderers") or 0)
            timestamp_unknown_count += int(initial_stats.get("timestamp_unknown") or 0)
            in_range_hits += int(initial_stats.get("in_range_hits") or 0)
            if config.date_start or config.date_end:
                before_only = (
                    bool(initial_stats.get("before_window_items"))
                    and not bool(initial_stats.get("window_candidate_items"))
                    and not bool(initial_stats.get("after_window_items"))
                )
                after_only = (
                    bool(initial_stats.get("after_window_items"))
                    and not bool(initial_stats.get("window_candidate_items"))
                    and not bool(initial_stats.get("before_window_items"))
                )
                if before_only:
                    pre_window_pages += 1
                    surface_pre_window_pages += 1
                else:
                    surface_pre_window_pages = 0
                if after_only:
                    after_window_pages += 1
                    surface_after_window_pages += 1
                else:
                    surface_after_window_pages = 0
            surface_pages_scanned[surface] = max(1, int(surface_pages_scanned.get(surface, 0) or 0))
            first_page_counts[surface] = len(initial_page_videos)
            videos.extend(initial_page_videos)
            initial_surfaces_processed.add(surface)
            self._emit_progress(
                progress_cb,
                phase="scrape_initial_page" if surface == "videos" else f"scrape_initial_page_{surface}",
                pages_scanned=_total_pages_scanned(),
                posts_checked=checked_renderers,
                matched_posts=len(videos),
            )

            continuation_token = self._extract_channel_continuation_token(data)
            page_num = 1

            while continuation_token:
                if _current_surface_has_enough_for_next_surface():
                    logger.info(
                        "Reached max results collection target (%s); trying next surface before %s continuation",
                        config.max_results,
                        surface,
                    )
                    break
                if _collection_satisfies_max_results():
                    logger.info(
                        "Reached max results collection target (%s); skipping %s continuation",
                        config.max_results,
                        surface,
                    )
                    break
                if _continuation_page_cap_reached(surface):
                    logger.info("Reached max continuation pages limit (%s)", config.max_pages)
                    break

                page_num += 1
                continuation_pages_by_surface[surface] += 1
                surface_pages_scanned[surface] = max(
                    int(surface_pages_scanned.get(surface, 0) or 0),
                    page_num,
                )
                logger.info(f"Fetching {surface} page {page_num}...")
                cont_data = self._fetch_continuation(
                    continuation_token, config.delay_seconds, fast_mode=config.fast_mode
                )
                if not cont_data:
                    continuation_failure_count += 1
                    continuation_failure_reason = self._last_channel_continuation_error or "request_error"
                    break

                renderers, continuation_token = self._extract_continuation_videos_and_token(cont_data)
                if not renderers:
                    logger.info("No more videos in continuation")
                    break

                page_ownership_counter = [0]
                page_videos, page_stats = self._process_renderer_batch(
                    renderers,
                    config,
                    surface=surface,
                    target_handle=canonical_handle or handle,
                    ownership_filtered_counter=page_ownership_counter,
                )
                page_hits = len(page_videos)
                ownership_filtered += page_ownership_counter[0]
                checked_renderers += int(page_stats.get("checked_renderers") or 0)
                in_range_hits += int(page_stats.get("in_range_hits") or 0)
                timestamp_unknown_count += int(page_stats.get("timestamp_unknown") or 0)

                # Treat pages with only before-window and/or undated items as
                # "before window" for capping.  Without this, shorts pages full
                # of undated items (timestamp_unknown) never count toward the
                # pre_window_page_cap and the scraper paginates indefinitely.
                _has_window = bool(page_stats.get("window_candidate_items"))
                _has_after = bool(page_stats.get("after_window_items"))
                _has_before = bool(page_stats.get("before_window_items"))
                _has_unknown = bool(page_stats.get("timestamp_unknown"))
                page_before_only = (_has_before or _has_unknown) and not _has_window and not _has_after
                page_after_only = (
                    bool(page_stats.get("after_window_items"))
                    and not bool(page_stats.get("window_candidate_items"))
                    and not bool(page_stats.get("before_window_items"))
                )

                if page_before_only:
                    surface_pre_window_pages += 1
                    pre_window_pages += 1
                else:
                    surface_pre_window_pages = 0
                if page_after_only:
                    surface_after_window_pages += 1
                    after_window_pages += 1
                else:
                    surface_after_window_pages = 0

                videos.extend(page_videos)
                self._emit_progress(
                    progress_cb,
                    phase="scrape_continuation_page",
                    pages_scanned=_total_pages_scanned(),
                    posts_checked=checked_renderers,
                    matched_posts=len(videos),
                )
                logger.info(f"Page {page_num}: {len(page_videos)} matches, {len(videos)} total")
                if page_hits == 0:
                    if config.date_start or config.date_end:
                        if page_before_only:
                            if surface_pre_window_pages >= self.PRE_WINDOW_PAGE_CAP:
                                scan_capped_reason = "pre_window_cap"
                                self._emit_progress(
                                    progress_cb,
                                    phase="scrape_pre_window_cap",
                                    pages_scanned=_total_pages_scanned(),
                                    posts_checked=checked_renderers,
                                    matched_posts=len(videos),
                                )
                                logger.info(
                                    "Stopping %s continuation crawl after pre-window cap (%d pages)",
                                    surface,
                                    self.PRE_WINDOW_PAGE_CAP,
                                )
                                break
                            continue
                        if page_after_only:
                            # Keep paging: still traversing newer content toward the requested window.
                            continue
                    surface_no_hit_pages += 1
                    no_hit_pages += 1
                    # Before/after-window pages can be noisy.
                    # Give a wider no-hit runway until we get an in-range hit.
                    no_hit_threshold = (
                        self.INITIAL_DATE_WINDOW_NO_HIT_PAGE_CAP
                        if (config.date_start or config.date_end) and in_range_hits == 0
                        else self.DATE_WINDOW_NO_HIT_PAGE_CAP
                    )
                    if surface_no_hit_pages >= no_hit_threshold and (config.date_start or config.date_end):
                        logger.info(
                            "Stopping %s continuation crawl after %d no-hit pages",
                            surface,
                            surface_no_hit_pages,
                        )
                        break
                else:
                    surface_no_hit_pages = 0
                if _collection_satisfies_max_results():
                    logger.info(
                        "Reached max results collection target (%s); stopping %s continuation",
                        config.max_results,
                        surface,
                    )
                    break

            if _collection_satisfies_max_results():
                logger.info(
                    "Reached max results collection target (%s); skipping remaining surfaces",
                    config.max_results,
                )
                break
            if (
                config.max_pages
                and _bounded_date_window_scan()
                and _total_continuation_pages_scanned() >= config.max_pages
            ):
                scan_capped_reason = scan_capped_reason or "max_pages"
                logger.info(
                    "Reached bounded-window continuation page limit (%s); skipping remaining surfaces",
                    config.max_pages,
                )
                break

        # Deduplicate by video_id
        seen = set()
        unique_videos = []
        for video in videos:
            if video.video_id not in seen:
                seen.add(video.video_id)
                unique_videos.append(video)

        if config.max_results:
            unique_videos, surface_cap_override_applied, effective_result_cap = self._apply_surface_guaranteed_limit(
                unique_videos,
                max_results=config.max_results,
            )

        # Enrich small channel-page result sets with likes/comments/tags via yt-dlp.
        # Large catalog backfills must persist page results promptly; per-video
        # yt-dlp enrichment would turn a 12k-video channel scrape into hours.
        enrichment_meta = self._maybe_enrich_videos_via_ytdlp(
            unique_videos,
            config,
            delay=config.delay_seconds,
            fast_mode=config.fast_mode,
        )

        channel_ytdlp_fallback_meta: dict[str, Any] = {
            "used": False,
            "available": bool(shutil.which("yt-dlp")),
            "posts_checked": 0,
            "matched_posts": 0,
        }
        should_try_channel_ytdlp_fallback = len(unique_videos) == 0 and bool(config.allow_ytdlp_search_supplement)
        if (
            should_try_channel_ytdlp_fallback
            and (config.date_start or config.date_end)
            and checked_renderers > 0
            and continuation_failure_count == 0
        ):
            should_try_channel_ytdlp_fallback = False
            channel_ytdlp_fallback_meta["skip_reason"] = "bounded_window_no_hits_after_channel_scan"
        if should_try_channel_ytdlp_fallback:
            fallback_videos, channel_ytdlp_fallback_meta = self._scrape_channel_via_ytdlp(
                config,
                canonical_handle=canonical_handle or handle,
                canonical_channel_id=canonical_channel_id or None,
                progress_cb=progress_cb,
            )
            if fallback_videos:
                unique_videos = fallback_videos
                checked_renderers = max(checked_renderers, int(channel_ytdlp_fallback_meta.get("posts_checked") or 0))
                in_range_hits = max(in_range_hits, len(unique_videos))
                enrichment_meta = self._maybe_enrich_videos_via_ytdlp(
                    unique_videos,
                    config,
                    delay=config.delay_seconds,
                    fast_mode=config.fast_mode,
                )

        # Supplement with yt-dlp when channel browsing found no matches or was capped.
        supplement_needed = len(unique_videos) == 0 or scan_capped_reason is not None
        should_supplement = supplement_needed and bool(config.allow_ytdlp_search_supplement)
        if should_supplement and config.keywords and shutil.which("yt-dlp"):
            logger.info(f"Channel browsing found only {len(unique_videos)} videos; supplementing with yt-dlp search...")
            search_videos = self._search_via_ytdlp(
                config,
                canonical_handle=canonical_handle or handle,
                canonical_channel_id=canonical_channel_id or None,
            )
            existing_ids = {v.video_id for v in unique_videos}
            added = 0
            for sv in search_videos:
                if sv.video_id not in existing_ids:
                    unique_videos.append(sv)
                    existing_ids.add(sv.video_id)
                    added += 1
            if added:
                logger.info(f"yt-dlp search added {added} additional videos (total: {len(unique_videos)})")
                checked_renderers = max(checked_renderers, len(unique_videos))
                self._emit_progress(
                    progress_cb,
                    phase="scrape_ytdlp_fallback",
                    pages_scanned=max(1, _total_pages_scanned()),
                    posts_checked=checked_renderers,
                    matched_posts=len(unique_videos),
                )

        if should_supplement and config.max_results:
            unique_videos, surface_cap_override_applied, effective_result_cap = self._apply_surface_guaranteed_limit(
                unique_videos,
                max_results=config.max_results,
            )

        for video in unique_videos:
            try:
                channel_id = str(video.channel_id or "").strip()
            except AttributeError:
                channel_id = ""
            if not channel_id and canonical_channel_id:
                video.channel_id = canonical_channel_id

            try:
                channel_title = str(video.channel_title or "").strip()
            except AttributeError:
                channel_title = ""
            if not channel_title and resolved_channel_title:
                video.channel_title = resolved_channel_title

            try:
                user_avatar_url = str(video.user_avatar_url or "").strip()
            except AttributeError:
                user_avatar_url = ""
            if not user_avatar_url and resolved_channel_avatar_url:
                video.user_avatar_url = resolved_channel_avatar_url

        logger.info(f"Scrape complete: found {len(unique_videos)} videos")
        self._emit_progress(
            progress_cb,
            phase="scrape_complete",
            pages_scanned=max(1, _total_pages_scanned()),
            posts_checked=checked_renderers,
            matched_posts=len(unique_videos),
        )
        continuation_pages_total = int(_total_continuation_pages_scanned())
        fallback_chain = ["channel_page_json"]
        if continuation_pages_total > 0:
            fallback_chain.append("continuation")
        if bool(channel_ytdlp_fallback_meta.get("used")):
            fallback_chain.append("yt_dlp_channel")
        if should_supplement:
            fallback_chain.append("yt_dlp_enrichment")
        self._last_transport = "channel_page_json"
        self._fallback_chain = fallback_chain
        self._last_stop_reason = "continuation_fetch_failed" if continuation_failure_count > 0 else "complete"
        self._last_retryable = continuation_failure_count > 0
        self._last_complete = continuation_failure_count == 0
        self._last_source_mode = "hybrid" if should_supplement else "scraper"
        self.last_retrieval_meta = {
            "retrieval_mode": "channel_continuation",
            "fallback_chain": list(fallback_chain),
            "requested_surfaces": list(requested_surfaces),
            "continuation_pages": continuation_pages_total,
            "continuation_pages_by_surface": dict(continuation_pages_by_surface),
            "pages_scanned": max(1, _total_pages_scanned()),
            "posts_checked": checked_renderers,
            "matched_posts": len(unique_videos),
            "checked_renderers": checked_renderers,
            "timestamp_unknown_count": timestamp_unknown_count,
            "in_range_hits": in_range_hits,
            "pre_window_pages": pre_window_pages,
            "before_window_pages": pre_window_pages,
            "after_window_pages": after_window_pages,
            "pre_window_page_cap": self.PRE_WINDOW_PAGE_CAP,
            "first_page_count": int(first_page_counts.get("videos", 0)),
            "first_page_counts": first_page_counts,
            "ownership_filtered": ownership_filtered,
            "scan_capped_reason": scan_capped_reason,
            "surface_pages_scanned": dict(surface_pages_scanned),
            "videos_pages_scanned": int(surface_pages_scanned.get("videos", 0)),
            "shorts_pages_scanned": int(surface_pages_scanned.get("shorts", 0)),
            "posts_pages_scanned": int(surface_pages_scanned.get("posts", 0)),
            "surface_cap_override_applied": bool(surface_cap_override_applied),
            "requested_max_results": int(config.max_results) if config.max_results is not None else None,
            "effective_max_results": effective_result_cap,
            "yt_dlp_supplement_needed": bool(supplement_needed),
            "yt_dlp_supplement_enabled": bool(config.allow_ytdlp_search_supplement),
            "yt_dlp_channel_fallback_used": bool(channel_ytdlp_fallback_meta.get("used")),
            "yt_dlp_channel_fallback_available": bool(channel_ytdlp_fallback_meta.get("available")),
            "yt_dlp_channel_fallback_posts_checked": int(channel_ytdlp_fallback_meta.get("posts_checked") or 0),
            "yt_dlp_channel_fallback_matched_posts": int(channel_ytdlp_fallback_meta.get("matched_posts") or 0),
            "yt_dlp_channel_fallback_surfaces_checked": dict(channel_ytdlp_fallback_meta.get("surfaces_checked") or {}),
            "yt_dlp_channel_fallback_errors": list(channel_ytdlp_fallback_meta.get("errors") or []),
            "yt_dlp_channel_fallback_skip_reason": channel_ytdlp_fallback_meta.get("skip_reason"),
            "yt_dlp_enrichment": enrichment_meta,
            "continuation_failure_reason": continuation_failure_reason,
            "continuation_failure_count": continuation_failure_count,
            "precise_publish_attempts": self._precise_publish_attempts,
            "precise_publish_successes": self._precise_publish_successes,
            "precise_publish_failures": self._precise_publish_failures,
            "shorts_candidates_found": sum(1 for video in unique_videos if self._video_surface(video) == "shorts"),
            "community_posts_found": sum(1 for video in unique_videos if self._video_surface(video) == "posts"),
            "shorts_precise_publish_attempts": self._shorts_precise_publish_attempts,
            "shorts_precise_publish_successes": self._shorts_precise_publish_successes,
            "shorts_precise_publish_failures": self._shorts_precise_publish_failures,
            "canonical_handle": canonical_handle or handle,
            "canonical_channel_id": canonical_channel_id or None,
            "resolved_channel_title": resolved_channel_title,
            "resolved_channel_avatar_url": resolved_channel_avatar_url,
        }
        if continuation_failure_count > 0:
            self.last_retrieval_meta["error_code"] = "youtube_continuation_fetch_failed"
            self.last_retrieval_meta["retryable"] = True
            self.last_retrieval_meta["error_class"] = "YouTubeContinuationFetchError"
        return unique_videos

    def _videos_needing_ytdlp_enrichment(self, videos: list[YouTubeVideo]) -> list[YouTubeVideo]:
        return [
            v
            for v in videos
            if isinstance(v, YouTubeVideo)
            and self._video_surface(v) != "posts"
            and ((v.likes == 0 and v.comments == 0) or int(getattr(v, "duration_seconds", 0) or 0) <= 0)
        ]

    def _maybe_enrich_videos_via_ytdlp(
        self,
        videos: list[YouTubeVideo],
        config: YouTubeScrapeConfig,
        *,
        delay: float = 1.0,
        fast_mode: bool = False,
    ) -> dict[str, Any]:
        needs_enrichment = self._videos_needing_ytdlp_enrichment(videos)
        max_videos = max(1, int(self.YTDLP_ENRICH_MAX_VIDEOS or 1))
        meta: dict[str, Any] = {
            "available": bool(shutil.which("yt-dlp")),
            "attempted": False,
            "video_count": len(videos),
            "needs_enrichment_count": len(needs_enrichment),
            "max_videos": max_videos,
            "skipped_count": 0,
            "skip_reason": None,
        }
        if not needs_enrichment:
            meta["skip_reason"] = "nothing_missing"
            return meta
        if not bool(config.allow_ytdlp_video_enrichment):
            meta["skip_reason"] = "disabled_by_config"
            meta["skipped_count"] = len(needs_enrichment)
            return meta
        if not meta["available"]:
            meta["skip_reason"] = "yt_dlp_unavailable"
            meta["skipped_count"] = len(needs_enrichment)
            return meta
        if len(needs_enrichment) > max_videos:
            meta["skip_reason"] = "video_count_exceeds_limit"
            meta["skipped_count"] = len(needs_enrichment)
            logger.info(
                "Skipping yt-dlp enrichment for %d YouTube videos; limit=%d. Run detail refresh for per-video metrics.",
                len(needs_enrichment),
                max_videos,
            )
            return meta
        self._enrich_videos_via_ytdlp(videos, delay=delay, fast_mode=fast_mode)
        meta["attempted"] = True
        return meta

    def _enrich_videos_via_ytdlp(
        self,
        videos: list[YouTubeVideo],
        delay: float = 1.0,
        *,
        fast_mode: bool = False,
    ) -> None:
        """Enrich videos with likes, comments, tags, and duration via yt-dlp.

        Enriches videos missing core metrics and/or duration metadata.
        Mutates videos in place.
        """
        if not shutil.which("yt-dlp"):
            logger.debug("yt-dlp not available; skipping enrichment")
            return

        needs_enrichment = self._videos_needing_ytdlp_enrichment(videos)
        if not needs_enrichment:
            return

        logger.info(f"Enriching {len(needs_enrichment)} videos with metrics via yt-dlp...")
        enriched = 0
        for i, video in enumerate(needs_enrichment, 1):
            url = f"https://www.youtube.com/watch?v={video.video_id}"
            try:
                proc = subprocess.run(
                    [
                        "yt-dlp",
                        "--dump-single-json",
                        "--no-playlist",
                        "--skip-download",
                        url,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=self.YTDLP_SEARCH_TIMEOUT_SECONDS,
                    check=False,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"yt-dlp enrichment failed for {video.video_id}: {exc}")
                continue

            if proc.returncode != 0:
                logger.warning(f"yt-dlp enrichment non-zero exit for {video.video_id}")
                continue

            try:
                data = json.loads(proc.stdout or "{}")
            except json.JSONDecodeError:
                continue

            resolved_like_count = data.get("like_count", 0) or 0
            is_short = bool(getattr(video, "is_short", False)) or self._video_surface(video) == "shorts"
            if resolved_like_count <= 0 and is_short:
                fallback_delay = min(max(float(delay or 0.0) * 0.25, 0.05), 0.35)
                resolved_like_count = self._fetch_shorts_like_count(
                    video.video_id, delay=fallback_delay, fast_mode=fast_mode
                )
            video.likes = resolved_like_count
            video.comments = data.get("comment_count", 0) or 0
            video.views = data.get("view_count", video.views) or video.views
            resolved_duration_seconds = int(data.get("duration", 0) or 0)
            if resolved_duration_seconds > 0:
                video.duration_seconds = resolved_duration_seconds
                if not str(video.duration or "").strip():
                    video.duration = f"PT{resolved_duration_seconds}S"
            if not video.channel_id:
                video.channel_id = data.get("channel_id", "") or ""
            if not video.tags:
                video.tags = data.get("tags", []) or []
            if not video.description:
                video.description = data.get("description", "") or ""
            enriched += 1
            logger.info(
                f"  [{i}/{len(needs_enrichment)}] {video.video_id}: "
                f"{video.views:,} views, {video.likes:,} likes, {video.comments:,} comments"
            )

            if delay and i < len(needs_enrichment):
                time.sleep(delay)

        logger.info(f"Enrichment complete: {enriched}/{len(needs_enrichment)} videos enriched")

    def _extract_ytdlp_owner_candidates(self, payload: dict[str, Any]) -> set[str]:
        candidates: set[str] = set()
        for key in ("uploader_id", "channel_url", "uploader_url", "webpage_url"):
            handle = self._extract_handle_from_url(str(payload.get(key) or ""))
            if handle:
                candidates.add(handle)
        for key in ("channel", "uploader"):
            raw = str(payload.get(key) or "").strip()
            if not raw:
                continue
            normalized = self._normalize_handle(raw)
            if normalized:
                candidates.add(normalized)
        return candidates

    def _ytdlp_entry_matches_owner(
        self,
        payload: dict[str, Any],
        *,
        target_handle: str,
        target_channel_id: str | None,
    ) -> bool:
        normalized_target_handle = self._normalize_handle(target_handle)
        normalized_target_channel_id = str(target_channel_id or "").strip()
        entry_channel_id = str(payload.get("channel_id") or "").strip()
        if normalized_target_channel_id and entry_channel_id:
            return entry_channel_id == normalized_target_channel_id
        if normalized_target_channel_id and not entry_channel_id:
            return False

        if not normalized_target_handle:
            return not normalized_target_channel_id

        candidates = self._extract_ytdlp_owner_candidates(payload)
        if not candidates:
            return False
        return normalized_target_handle in candidates

    def _search_via_ytdlp(
        self,
        config: YouTubeScrapeConfig,
        *,
        canonical_handle: str | None = None,
        canonical_channel_id: str | None = None,
    ) -> list[YouTubeVideo]:
        """
        Search YouTube via yt-dlp to find videos by keyword.

        Uses ytsearchN: prefix to search YouTube, then filters results
        by channel and date range. Much faster than paginating a busy
        channel's entire video list.
        """
        handle = config.channel_handle.lstrip("@")
        target_handle = self._normalize_handle(canonical_handle or handle)
        search_terms = [kw for kw in (config.keywords or []) if len(kw) <= 40]
        if not search_terms:
            return []

        has_date_window = bool(config.date_start or config.date_end)
        search_prefix = "ytsearchdate" if has_date_window else "ytsearch"
        search_limit = 200 if has_date_window else 50
        search_queries: list[str] = []
        for term in search_terms[:4]:
            q = f"{term} {handle}".strip()
            if q and q not in search_queries:
                search_queries.append(q)
        # Keep one broader channel-biased query so we can recover posts
        # whose titles miss strict keyword matches in YouTube ranking.
        if handle and handle not in search_queries:
            search_queries.append(handle)

        all_videos: list[YouTubeVideo] = []
        seen_ids: set[str] = set()

        for query in search_queries:
            cmd = [
                "yt-dlp",
                "--dump-json",
                "--skip-download",
                "--ignore-errors",
                f"{search_prefix}{search_limit}:{query}",
            ]
            logger.info(f"yt-dlp searching YouTube: '{query}'")

            try:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=max(30, int(self.YTDLP_SEARCH_TIMEOUT_SECONDS)),
                )
            except subprocess.TimeoutExpired:
                logger.warning(f"yt-dlp search timed out for '{query}'")
                continue

            for line in proc.stdout.strip().splitlines():
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if not self._ytdlp_entry_matches_owner(
                    data,
                    target_handle=target_handle,
                    target_channel_id=canonical_channel_id,
                ):
                    continue

                vid_id = data.get("id", "")
                if not vid_id or vid_id in seen_ids:
                    continue

                # Filter by date range
                ts = data.get("timestamp") or 0
                if ts and config.date_start:
                    if ts < config.start_timestamp:
                        continue
                if ts and config.date_end:
                    if ts > config.end_timestamp:
                        continue

                seen_ids.add(vid_id)
                dt_str = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
                webpage_url = str(data.get("webpage_url") or data.get("url") or "")
                is_short = "/shorts/" in webpage_url

                video = YouTubeVideo(
                    video_id=vid_id,
                    title=data.get("title", ""),
                    description=data.get("description", "") or "",
                    date_time=dt_str,
                    published_at=ts,
                    channel_id=data.get("channel_id", ""),
                    channel_title=data.get("channel", "") or data.get("uploader", ""),
                    duration=f"PT{data.get('duration', 0)}S",
                    duration_seconds=data.get("duration", 0) or 0,
                    views=data.get("view_count", 0) or 0,
                    likes=data.get("like_count", 0) or 0,
                    comments=data.get("comment_count", 0) or 0,
                    url=self._canonical_video_url(
                        video_id=vid_id,
                        surface="shorts" if is_short else "videos",
                        renderer_url=webpage_url,
                    ),
                    thumbnail_url=(data.get("thumbnails") or [{}])[0].get("url", ""),
                    tags=data.get("tags", []) or [],
                    keywords_matched=[query],
                    user_avatar_url=self._normalize_channel_avatar_url(
                        data.get("uploader_avatar")
                        or data.get("channel_thumbnail")
                        or data.get("channelAvatarUrl")
                        or data.get("author_avatar_url")
                        or None
                    ),
                    is_short=is_short,
                    source_surface="search",
                    show_id=config.show_id,
                    season_number=config.season_number,
                    person_id=config.person_id,
                )
                all_videos.append(video)
                logger.info(f"yt-dlp found: {vid_id} - {video.title[:50]}... ({dt_str})")

        logger.info(f"yt-dlp search total: {len(all_videos)} videos from channel @{target_handle}")
        return all_videos

    def _process_renderer_batch(
        self,
        renderers: Iterable[dict[str, Any]],
        config: YouTubeScrapeConfig,
        *,
        surface: str = "videos",
        target_handle: str | None = None,
        ownership_filtered_counter: list[int] | None = None,
        fallback_channel_avatar_url: str | None = None,
    ) -> tuple[list[YouTubeVideo], dict[str, int]]:
        videos: list[YouTubeVideo] = []
        stats: dict[str, int] = {
            "checked_renderers": 0,
            "before_window_items": 0,
            "after_window_items": 0,
            "window_candidate_items": 0,
            "timestamp_unknown": 0,
            "in_range_hits": 0,
        }
        for renderer in renderers:
            if target_handle and not self._renderer_matches_owner(renderer, target_handle):
                if ownership_filtered_counter is not None:
                    ownership_filtered_counter[0] += 1
                continue
            stats["checked_renderers"] += 1
            if surface == "posts":
                video = self._parse_post_renderer(
                    renderer,
                    config,
                    fallback_channel_avatar_url=fallback_channel_avatar_url,
                )
            else:
                video = self._parse_video_renderer(
                    renderer,
                    config,
                    surface=surface,
                    fallback_channel_avatar_url=fallback_channel_avatar_url,
                )
            if not video:
                continue
            in_range: bool | None = None
            if video.published_at > 0:
                in_range = config.is_in_date_range(video.published_at)
            in_range = self._refine_video_publish_timestamp_if_needed(video, config, in_range)
            if video.published_at > 0:
                if in_range is None:  # Before range
                    stats["before_window_items"] += 1
                    continue
                if in_range is False:  # After range
                    stats["after_window_items"] += 1
                    continue
                stats["window_candidate_items"] += 1
                stats["in_range_hits"] += 1
            elif config.date_start or config.date_end:
                # For bounded Shorts windows, skip rows we cannot place into a
                # concrete time period after exact-date recovery.
                stats["timestamp_unknown"] += 1
                if bool(getattr(video, "is_short", False)) or surface == "shorts":
                    stats["shorts_undated_skipped"] = int(stats.get("shorts_undated_skipped") or 0) + 1
                    continue
                stats["window_candidate_items"] += 1

            combined_text = f"{video.title} {video.description}"
            if config.matches_keywords(combined_text):
                videos.append(video)
                title_short = video.title[:50] + "..." if len(video.title) > 50 else video.title
                logger.info(f"Found: {video.video_id} - {title_short} ({video.date_time})")

        return videos, stats

    def _process_video_data(
        self,
        data: dict,
        config: YouTubeScrapeConfig,
        *,
        surface: str = "videos",
        target_handle: str | None = None,
        ownership_filtered_counter: list[int] | None = None,
        return_stats: bool = False,
    ) -> list[YouTubeVideo] | tuple[list[YouTubeVideo], dict[str, int]]:
        """Process video data and apply filters."""
        fallback_channel_avatar_url = self._extract_channel_header_avatar_from_data(data)
        renderers = self._iter_post_renderers(data) if surface == "posts" else self._iter_video_renderers(data)
        videos, stats = self._process_renderer_batch(
            renderers,
            config,
            surface=surface,
            target_handle=target_handle,
            ownership_filtered_counter=ownership_filtered_counter,
            fallback_channel_avatar_url=fallback_channel_avatar_url,
        )
        if return_stats:
            return videos, stats
        return videos

    def fetch_comments(
        self,
        video_id: str,
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
    ) -> list[YouTubeComment]:
        """
        Fetch comments for a YouTube video including replies.

        Args:
            video_id: The YouTube video ID
            max_comments: Maximum number of top-level comments to fetch
            fetch_replies: Whether to fetch replies to comments
            delay: Delay between API requests
            fast_mode: When True, uses aggressive rate-limiting tiers

        Returns:
            List of YouTubeComment objects with nested replies
        """
        self.last_comment_fetch_reason = None
        self.comments_auth_failed = False
        watch_video_url = f"https://www.youtube.com/watch?v={video_id}"
        shorts_video_url = f"https://www.youtube.com/shorts/{video_id}"
        video_url = watch_video_url
        logger.info(f"Fetching comments for video {video_id}")

        continuation_token = None
        bootstrap_failures: list[str] = []
        for bootstrap_url in (watch_video_url, shorts_video_url):
            self._rate_limit(delay, fast_mode=fast_mode)
            try:
                response = self.session.get(
                    bootstrap_url,
                    headers=self._get_headers(),
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                )
                self._track_response_status(response.status_code)
                response.raise_for_status()
            except requests.exceptions.RequestException as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code:
                    self._track_response_status(status_code)
                bootstrap_failures.append("request_error")
                logger.warning("Failed to fetch YouTube comment bootstrap page (%s): %s", bootstrap_url, exc)
                continue

            yt_data = self._extract_ytinital_data(response.text)
            if not yt_data:
                bootstrap_failures.append("parse_error")
                logger.warning("Could not extract ytInitialData from %s", bootstrap_url)
                continue

            continuation_token = self._extract_comment_continuation(yt_data)
            if continuation_token:
                video_url = bootstrap_url
                break
            bootstrap_failures.append("comments_unavailable")

        if not continuation_token:
            if "request_error" in bootstrap_failures:
                self._set_comment_failure_reason("request_error")
            elif "parse_error" in bootstrap_failures:
                self._set_comment_failure_reason("parse_error")
            else:
                self._set_comment_failure_reason("comments_unavailable")
            logger.warning("No comment continuation token found - video may have comments disabled")
            return []

        comments = []
        comments_fetched = 0
        continuation_retry_count = 0

        while continuation_token:
            self._rate_limit(delay, fast_mode=fast_mode)

            # Fetch comments using continuation
            comment_data = self._fetch_comment_continuation(continuation_token, delay)
            if not comment_data:
                retry_reason = str(self.last_comment_fetch_reason or "").strip()
                if self._is_retryable_comment_failure(retry_reason):
                    if continuation_retry_count < self.COMMENT_CONTINUATION_RETRY_ATTEMPTS:
                        continuation_retry_count += 1
                        backoff_seconds = float(
                            self.COMMENT_CONTINUATION_RETRY_BACKOFF_SECONDS * continuation_retry_count
                        )
                        logger.info(
                            "Retrying comment continuation for %s (%d/%d, reason=%s, backoff=%ss)",
                            video_id,
                            continuation_retry_count,
                            self.COMMENT_CONTINUATION_RETRY_ATTEMPTS,
                            retry_reason,
                            int(backoff_seconds),
                        )
                        time.sleep(backoff_seconds)
                        continue
                if not self.last_comment_fetch_reason:
                    self._set_comment_failure_reason("continuation_fetch_failed")
                break
            continuation_retry_count = 0

            # Parse comments from response
            entity_index = self._build_comment_entity_index(comment_data)
            items, next_continuation = self._parse_comment_response(comment_data)
            parsed_on_page = 0

            for item in items:
                comment = self._parse_comment_thread(
                    item,
                    video_id,
                    video_url,
                    fetch_replies,
                    delay,
                    entity_index=entity_index,
                    fast_mode=fast_mode,
                )
                if comment:
                    comments.append(comment)
                    comments_fetched += 1
                    parsed_on_page += 1
                    logger.info(f"  Comment {comment.comment_id}: {comment.likes} likes, {comment.reply_count} replies")

                if max_comments and comments_fetched >= max_comments:
                    break

            logger.info(f"Fetched {len(comments)} comments so far...")

            if max_comments and comments_fetched >= max_comments:
                break
            # Guard against schema drift / continuation loops that never yield parsable comments.
            if not items and not next_continuation:
                break
            if continuation_token == next_continuation:
                logger.warning("YouTube comment continuation token did not advance; stopping pagination")
                break
            if parsed_on_page == 0 and not next_continuation:
                break
            continuation_token = next_continuation

        logger.info(f"Total: {len(comments)} comments fetched for video {video_id}")
        return comments

    @staticmethod
    def _parse_vtt_timestamp(value: str) -> float:
        text = str(value or "").strip()
        if not text:
            return 0.0
        parts = text.split(":")
        if len(parts) == 3:
            hours = int(parts[0] or 0)
            minutes = int(parts[1] or 0)
            seconds = float(parts[2] or 0)
            return float(hours * 3600 + minutes * 60 + seconds)
        if len(parts) == 2:
            minutes = int(parts[0] or 0)
            seconds = float(parts[1] or 0)
            return float(minutes * 60 + seconds)
        return float(text)

    @classmethod
    def _parse_vtt_transcript_segments(cls, vtt_text: str) -> list[dict[str, Any]]:
        segments: list[dict[str, Any]] = []
        blocks = re.split(r"\n\s*\n", str(vtt_text or "").replace("\r", "\n"))
        for block in blocks:
            lines = [line.strip() for line in block.split("\n") if line.strip()]
            if not lines:
                continue
            time_line = ""
            text_lines: list[str] = []
            for line in lines:
                if "-->" in line and not time_line:
                    time_line = line
                elif time_line:
                    text_lines.append(line)
            if not time_line:
                continue
            parts = [part.strip() for part in time_line.split("-->")]
            if len(parts) != 2:
                continue
            start_seconds = cls._parse_vtt_timestamp(parts[0].replace(",", "."))
            end_seconds = cls._parse_vtt_timestamp(parts[1].replace(",", ".").split(" ", 1)[0])
            text = " ".join(text_lines).strip()
            text = re.sub(r"<[^>]+>", "", text).strip()
            if not text:
                continue
            segments.append(
                {
                    "start_seconds": round(max(0.0, start_seconds), 3),
                    "end_seconds": round(max(start_seconds, end_seconds), 3),
                    "text": text,
                }
            )
        return segments

    @staticmethod
    def _parse_json3_transcript_segments(payload: dict[str, Any]) -> list[dict[str, Any]]:
        segments: list[dict[str, Any]] = []
        events = payload.get("events")
        if not isinstance(events, list):
            return segments
        for event in events:
            if not isinstance(event, dict):
                continue
            start_ms = int(event.get("tStartMs") or 0)
            duration_ms = int(event.get("dDurationMs") or 0)
            segs = event.get("segs")
            if not isinstance(segs, list):
                continue
            parts: list[str] = []
            for seg in segs:
                if not isinstance(seg, dict):
                    continue
                text = str(seg.get("utf8") or "").replace("\n", " ").strip()
                if text:
                    parts.append(text)
            text_value = " ".join(parts).strip()
            if not text_value:
                continue
            start_seconds = max(0.0, float(start_ms) / 1000.0)
            end_seconds = max(start_seconds, start_seconds + max(0.0, float(duration_ms) / 1000.0))
            segments.append(
                {
                    "start_seconds": round(start_seconds, 3),
                    "end_seconds": round(end_seconds, 3),
                    "text": text_value,
                }
            )
        return segments

    @staticmethod
    def _caption_track_score(
        *,
        language: str,
        ext: str,
        is_auto: bool,
        preferred_languages: list[str],
    ) -> tuple[int, int, int]:
        language_value = str(language or "").strip().lower()
        ext_value = str(ext or "").strip().lower()
        pref_score = 0
        for idx, pref in enumerate(preferred_languages):
            pref_value = str(pref or "").strip().lower()
            if not pref_value:
                continue
            if language_value == pref_value:
                pref_score = max(pref_score, 100 - idx)
                break
            if language_value.startswith(pref_value):
                pref_score = max(pref_score, 80 - idx)
                break
        ext_score_map = {"json3": 30, "srv3": 25, "vtt": 20, "ttml": 15}
        ext_score = ext_score_map.get(ext_value, 5)
        manual_score = 20 if not is_auto else 0
        return pref_score, manual_score, ext_score

    def fetch_transcript(
        self,
        video_id: str,
        *,
        preferred_languages: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_video_id = str(video_id or "").strip()
        if not normalized_video_id:
            return {
                "text": "",
                "segments": [],
                "language": None,
                "source": None,
                "error": "missing_video_id",
            }
        languages = [str(item).strip() for item in (preferred_languages or ["en-US", "en"]) if str(item).strip()]
        if not languages:
            languages = ["en-US", "en"]
        watch_url = f"https://www.youtube.com/watch?v={normalized_video_id}"
        candidates: list[dict[str, Any]] = []
        for page_url in (watch_url, f"https://www.youtube.com/shorts/{normalized_video_id}"):
            try:
                response = self.session.get(
                    page_url,
                    headers=self._get_headers(),
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
            except Exception:
                continue
            player_response = self._extract_player_response_from_html(str(response.text or ""))
            if not isinstance(player_response, dict):
                continue
            candidates = self._caption_candidates_from_player_response(
                player_response,
                preferred_languages=languages,
            )
            if candidates:
                break

        if not candidates and shutil.which("yt-dlp"):
            try:
                proc = subprocess.run(
                    [
                        "yt-dlp",
                        "--dump-single-json",
                        "--no-playlist",
                        "--skip-download",
                        watch_url,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=max(10, int(self.YTDLP_SEARCH_TIMEOUT_SECONDS)),
                    check=False,
                )
            except Exception as exc:  # noqa: BLE001
                return {
                    "text": "",
                    "segments": [],
                    "language": None,
                    "source": None,
                    "error": f"yt_dlp_exception:{exc.__class__.__name__}",
                }

            if proc.returncode != 0:
                return {
                    "text": "",
                    "segments": [],
                    "language": None,
                    "source": None,
                    "error": "yt_dlp_failed",
                }

            try:
                payload = json.loads(proc.stdout or "{}")
            except json.JSONDecodeError:
                return {
                    "text": "",
                    "segments": [],
                    "language": None,
                    "source": None,
                    "error": "yt_dlp_parse_failed",
                }

            subtitles = payload.get("subtitles") if isinstance(payload.get("subtitles"), dict) else {}
            auto_captions = (
                payload.get("automatic_captions") if isinstance(payload.get("automatic_captions"), dict) else {}
            )
            for source_name, source_payload, is_auto in (
                ("manual_captions", subtitles, False),
                ("auto_captions", auto_captions, True),
            ):
                if not isinstance(source_payload, dict):
                    continue
                candidates.extend(
                    self._build_caption_candidates_from_source(
                        source_name=source_name,
                        source_payload=source_payload,
                        is_auto=is_auto,
                        preferred_languages=languages,
                    )
                )
        if not candidates:
            return {
                "text": "",
                "segments": [],
                "language": None,
                "source": None,
                "error": "captions_unavailable",
            }

        candidates.sort(key=lambda item: item["score"], reverse=True)
        selected = candidates[0]
        try:
            response = self.session.get(
                str(selected["url"]),
                headers=self._get_headers(),
                timeout=(10, max(10, int(self.TRANSCRIPT_FETCH_TIMEOUT_SECONDS))),
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            return {
                "text": "",
                "segments": [],
                "language": selected.get("language"),
                "source": selected.get("source"),
                "error": f"caption_download_failed:{exc.__class__.__name__}",
            }

        segments: list[dict[str, Any]] = []
        try:
            if str(selected.get("ext") or "").lower() in {"json3", "srv3"}:
                transcript_payload = response.json()
                if isinstance(transcript_payload, dict):
                    segments = self._parse_json3_transcript_segments(transcript_payload)
            else:
                segments = self._parse_vtt_transcript_segments(str(response.text or ""))
        except Exception as exc:  # noqa: BLE001
            return {
                "text": "",
                "segments": [],
                "language": selected.get("language"),
                "source": selected.get("source"),
                "error": f"caption_parse_failed:{exc.__class__.__name__}",
            }

        if not segments:
            return {
                "text": "",
                "segments": [],
                "language": selected.get("language"),
                "source": selected.get("source"),
                "error": "caption_empty",
            }

        transcript_text = " ".join(str(item.get("text") or "").strip() for item in segments if item.get("text")).strip()
        return {
            "text": transcript_text,
            "segments": segments,
            "language": selected.get("language"),
            "source": selected.get("source"),
            "error": None,
        }

    def _build_comment_entity_index(self, data: dict) -> dict[str, dict]:
        """Build an index of commentId -> commentEntityPayload from framework updates."""
        index: dict[str, dict] = {}
        mutations = data.get("frameworkUpdates", {}).get("entityBatchUpdate", {}).get("mutations", [])
        for mutation in mutations:
            payload = mutation.get("payload", {})
            entity = payload.get("commentEntityPayload", {})
            props = entity.get("properties", {})
            comment_id = props.get("commentId")
            if isinstance(comment_id, str) and comment_id:
                index[comment_id] = entity
        return index

    @staticmethod
    def _extract_token_from_continuation_item(item: dict[str, Any]) -> str | None:
        continuation_renderer = item.get("continuationItemRenderer", {})
        if isinstance(continuation_renderer, dict):
            endpoint = continuation_renderer.get("continuationEndpoint", {})
            if isinstance(endpoint, dict):
                command = endpoint.get("continuationCommand", {})
                if isinstance(command, dict):
                    token = str(command.get("token") or "").strip()
                    if token:
                        return token
        return None

    def _find_continuation_token(self, node: Any, *, comment_biased: bool = False) -> str | None:
        best: tuple[int, int, str] | None = None
        visit_index = 0

        def _score(path_text: str) -> int:
            score = 0
            lowered = path_text.lower()
            if "comment" in lowered:
                score += 6
            if "engagement" in lowered:
                score += 2
            if "continuationitemrenderer" in lowered:
                score += 1
            return score

        def _consider(token_value: str | None, *, path_parts: list[str]) -> None:
            nonlocal best, visit_index
            token = str(token_value or "").strip()
            if not token:
                return
            path_text = ".".join(path_parts)
            score = _score(path_text)
            if comment_biased and score <= 0:
                return
            visit_index += 1
            candidate = (score, -visit_index, token)
            if best is None or candidate > best:
                best = candidate

        def _walk(value: Any, path_parts: list[str]) -> None:
            if isinstance(value, dict):
                command = value.get("continuationCommand", {})
                if isinstance(command, dict):
                    _consider(command.get("token"), path_parts=path_parts + ["continuationCommand", "token"])
                next_data = value.get("nextContinuationData", {})
                if isinstance(next_data, dict):
                    _consider(
                        next_data.get("continuation"),
                        path_parts=path_parts + ["nextContinuationData", "continuation"],
                    )
                for key, child in value.items():
                    _walk(child, path_parts + [str(key)])
            elif isinstance(value, list):
                for idx, child in enumerate(value):
                    _walk(child, path_parts + [str(idx)])

        _walk(node, [])
        return best[2] if best else None

    @staticmethod
    def _comment_response_containers(data: dict[str, Any]) -> list[dict[str, Any]]:
        containers: list[dict[str, Any]] = []
        for key in ("onResponseReceivedEndpoints", "onResponseReceivedActions"):
            payload = data.get(key, [])
            if not isinstance(payload, list):
                continue
            for item in payload:
                if isinstance(item, dict):
                    containers.append(item)
        return containers

    def _extract_comment_continuation(self, yt_data: dict) -> str | None:
        """Extract the continuation token for comments from ytInitialData."""
        try:
            # Navigate to comments section
            contents = yt_data.get("contents", {})
            two_col = contents.get("twoColumnWatchNextResults", {})
            results = two_col.get("results", {}).get("results", {}).get("contents", [])

            for item in results:
                # Look for item section with comments
                item_section = item.get("itemSectionRenderer", {})
                section_contents = item_section.get("contents", [])

                for section_item in section_contents:
                    continuation_renderer = section_item.get("continuationItemRenderer", {})
                    if continuation_renderer:
                        endpoint = continuation_renderer.get("continuationEndpoint", {})
                        return endpoint.get("continuationCommand", {}).get("token")

                    # Also check for message renderer (comments disabled message)
                    message_renderer = section_item.get("messageRenderer", {})
                    if message_renderer:
                        text = message_renderer.get("text", {}).get("runs", [{}])[0].get("text", "")
                        if "disabled" in text.lower():
                            logger.warning(f"Comments appear to be disabled: {text}")
                            return None
        except (KeyError, TypeError, IndexError):
            pass

        engagement_panels = yt_data.get("engagementPanels", [])
        token = self._find_continuation_token(engagement_panels, comment_biased=True)
        if token:
            return token

        # Fallback for modern watch/shorts layouts that move continuation under different branches.
        token = self._find_continuation_token(yt_data, comment_biased=True)
        if token:
            return token

        return None

    def _fetch_comment_continuation(self, continuation_token: str, delay: float = 2.0) -> dict | None:
        """Fetch comments using a continuation token."""
        import json

        payload = {
            "context": self.INNERTUBE_CONTEXT,
            "continuation": continuation_token,
        }

        headers = self._get_headers()
        headers["content-type"] = "application/json"

        try:
            response = self.session.post(
                self.COMMENT_API_URL,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except ValueError:
            self._set_comment_failure_reason("parse_error")
            logger.error("Failed to decode YouTube comment continuation payload as JSON")
            return None
        except requests.exceptions.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code in {401, 403}:
                self._set_comment_failure_reason(f"http_{status_code}_auth")
            elif status_code is not None:
                self._set_comment_failure_reason(f"http_{status_code}")
            else:
                self._set_comment_failure_reason("request_error")
            logger.error(f"Failed to fetch comment continuation: {e}")
            return None

    def _parse_comment_response(self, data: dict) -> tuple[list[dict], str | None]:
        """Parse comment response and extract items and next continuation."""
        items = []
        next_continuation = None

        try:
            # Look for continuation contents
            on_response = self._comment_response_containers(data)
            for endpoint in on_response:
                # Reload continuation (initial load)
                reload_items = endpoint.get("reloadContinuationItemsCommand", {})
                if reload_items:
                    for item in reload_items.get("continuationItems", []):
                        if "commentThreadRenderer" in item or "commentViewModel" in item:
                            items.append(item)
                        elif "continuationItemRenderer" in item:
                            next_continuation = self._extract_token_from_continuation_item(item) or next_continuation

                # Append continuation (subsequent loads)
                append_items = endpoint.get("appendContinuationItemsAction", {})
                if append_items:
                    for item in append_items.get("continuationItems", []):
                        if "commentThreadRenderer" in item or "commentViewModel" in item:
                            items.append(item)
                        elif "continuationItemRenderer" in item:
                            next_continuation = self._extract_token_from_continuation_item(item) or next_continuation
        except (KeyError, TypeError):
            pass

        return items, next_continuation

    def _parse_comment_thread(
        self,
        item: dict,
        video_id: str,
        video_url: str,
        fetch_replies: bool = True,
        delay: float = 2.0,
        entity_index: dict[str, dict] | None = None,
        *,
        fast_mode: bool = False,
    ) -> YouTubeComment | None:
        """Parse a comment thread into YouTubeComment."""
        try:
            thread = item.get("commentThreadRenderer", {})
            comment_renderer = thread.get("comment", {}).get("commentRenderer", {})
            if comment_renderer:
                comment = self._parse_comment_renderer(comment_renderer, video_id, video_url)
            else:
                comment_vm_container = thread.get("commentViewModel", {})
                if isinstance(comment_vm_container, dict):
                    comment_vm = comment_vm_container.get("commentViewModel", comment_vm_container)
                else:
                    comment_vm = {}
                comment = self._parse_comment_view_model(
                    comment_vm,
                    entity_index or {},
                    video_id,
                    video_url,
                )
            if not comment:
                return None

            # Fetch replies if requested
            if fetch_replies and comment.reply_count > 0:
                replies_renderer = thread.get("replies", {}).get("commentRepliesRenderer", {})
                reply_continuation = None

                # Get reply continuation token
                for cont in replies_renderer.get("contents", []):
                    cont_renderer = cont.get("continuationItemRenderer", {})
                    if cont_renderer:
                        endpoint = cont_renderer.get("continuationEndpoint", {})
                        reply_continuation = endpoint.get("continuationCommand", {}).get("token")
                        break

                if reply_continuation:
                    replies = self._fetch_comment_replies(
                        reply_continuation,
                        video_id,
                        video_url,
                        comment.comment_id,
                        delay,
                        fast_mode=fast_mode,
                    )
                    comment.replies = replies

            return comment
        except (KeyError, TypeError):
            return None

    def _parse_comment_view_model(
        self,
        comment_view_model: dict,
        entity_index: dict[str, dict],
        video_id: str,
        video_url: str,
        is_reply: bool = False,
        parent_id: str | None = None,
    ) -> YouTubeComment | None:
        """Parse YouTube's modern commentViewModel schema into YouTubeComment."""
        comment_id = comment_view_model.get("commentId", "")
        if not comment_id:
            return None

        entity = entity_index.get(comment_id, {})
        properties = entity.get("properties", {})
        author_data = entity.get("author", {})
        toolbar = entity.get("toolbar", {})

        text = properties.get("content", {}).get("content", "") or ""
        author = author_data.get("displayName", "") or ""
        author_channel_id = author_data.get("channelId", "") or ""
        likes_text = toolbar.get("likeCountNotliked") or toolbar.get("likeCountLiked") or "0"
        likes = self._parse_like_count(str(likes_text))
        reply_count_text = toolbar.get("replyCount") or "0"
        reply_count = self._parse_like_count(str(reply_count_text))
        published_text = str(properties.get("publishedTime", "") or "")
        created_at = self._estimate_publish_date(published_text)

        return YouTubeComment(
            comment_id=comment_id,
            text=text,
            author=author,
            author_channel_id=author_channel_id,
            likes=likes,
            created_at=created_at,
            date_time=datetime.fromtimestamp(created_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if created_at else "",
            is_reply=is_reply,
            parent_comment_id=parent_id,
            reply_count=reply_count,
            video_id=video_id,
            video_url=video_url,
        )

    def _parse_comment_renderer(
        self,
        renderer: dict,
        video_id: str,
        video_url: str,
        is_reply: bool = False,
        parent_id: str | None = None,
    ) -> YouTubeComment | None:
        """Parse a comment renderer into YouTubeComment."""
        try:
            comment_id = renderer.get("commentId", "")
            if not comment_id:
                return None

            # Extract text
            content_text = renderer.get("contentText", {})
            text_parts = content_text.get("runs", [])
            text = "".join(part.get("text", "") for part in text_parts)

            # Extract author info
            author = renderer.get("authorText", {}).get("simpleText", "")
            author_endpoint = renderer.get("authorEndpoint", {}).get("browseEndpoint", {})
            author_channel_id = author_endpoint.get("browseId", "")

            # Extract like count
            vote_count = renderer.get("voteCount", {}).get("simpleText", "0")
            likes = self._parse_like_count(vote_count)

            # Extract reply count
            reply_count = renderer.get("replyCount", 0)

            # Extract timestamp
            published_text = renderer.get("publishedTimeText", {}).get("runs", [{}])[0].get("text", "")
            created_at = self._estimate_publish_date(published_text)

            return YouTubeComment(
                comment_id=comment_id,
                text=text,
                author=author,
                author_channel_id=author_channel_id,
                likes=likes,
                created_at=created_at,
                date_time=datetime.fromtimestamp(created_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
                if created_at
                else "",
                is_reply=is_reply,
                parent_comment_id=parent_id,
                reply_count=reply_count,
                video_id=video_id,
                video_url=video_url,
            )
        except (KeyError, TypeError):
            return None

    def _parse_like_count(self, like_text: str) -> int:
        """Parse like count from text like '1.2K' or '1,234'."""
        if not like_text:
            return 0
        like_text = like_text.lower().replace(",", "")
        multiplier = 1
        if "k" in like_text:
            multiplier = 1000
            like_text = like_text.replace("k", "")
        elif "m" in like_text:
            multiplier = 1000000
            like_text = like_text.replace("m", "")
        try:
            return int(float(like_text) * multiplier)
        except ValueError:
            return 0

    def _fetch_comment_replies(
        self,
        continuation_token: str,
        video_id: str,
        video_url: str,
        parent_id: str,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
    ) -> list[YouTubeComment]:
        """Fetch replies to a comment."""
        replies = []
        seen_tokens: set[str] = set()
        max_reply_pages = 200
        pages = 0

        while continuation_token:
            if continuation_token in seen_tokens:
                break
            seen_tokens.add(continuation_token)
            pages += 1
            if pages > max_reply_pages:
                logger.warning("[youtube_reply_pagination_capped] pages=%s parent_id=%s", pages, parent_id)
                break

            self._rate_limit(delay, fast_mode=fast_mode)
            data = self._fetch_comment_continuation(continuation_token, delay)
            if not data:
                break

            next_continuation = None
            entity_index = self._build_comment_entity_index(data)
            try:
                on_response = self._comment_response_containers(data)
                for endpoint in on_response:
                    append_items = endpoint.get("appendContinuationItemsAction", {}).get("continuationItems", [])
                    reload_items = endpoint.get("reloadContinuationItemsCommand", {}).get("continuationItems", [])
                    for item in [*append_items, *reload_items]:
                        comment_renderer = item.get("commentRenderer", {})
                        if comment_renderer:
                            reply = self._parse_comment_renderer(
                                comment_renderer,
                                video_id,
                                video_url,
                                is_reply=True,
                                parent_id=parent_id,
                            )
                            if reply:
                                replies.append(reply)
                        else:
                            reply_vm_container = item.get("commentViewModel", {})
                            if isinstance(reply_vm_container, dict):
                                reply_vm = reply_vm_container.get("commentViewModel", reply_vm_container)
                            else:
                                reply_vm = {}
                            reply = self._parse_comment_view_model(
                                reply_vm,
                                entity_index,
                                video_id,
                                video_url,
                                is_reply=True,
                                parent_id=parent_id,
                            )
                            if reply:
                                replies.append(reply)

                        # Check for more replies
                        next_continuation = self._extract_token_from_continuation_item(item) or next_continuation
            except (KeyError, TypeError):
                break

            if not next_continuation or next_continuation == continuation_token:
                break
            continuation_token = next_continuation

        return replies
