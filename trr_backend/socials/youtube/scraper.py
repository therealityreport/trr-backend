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
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
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


@dataclass
class YouTubeScrapeConfig:
    """Configuration for a YouTube scrape operation."""

    channel_handle: str  # Channel handle like "bravo" (without @)
    keywords: list[str] = field(default_factory=list)  # Keywords to filter by (e.g., "RHOSLC", "Salt Lake City")
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 2.0
    max_results: int | None = None  # None = no limit
    max_pages: int | None = None  # continuation page limit

    # Metadata for tracking
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    @property
    def start_timestamp(self) -> float:
        return self.date_start.timestamp() if self.date_start else 0

    @property
    def end_timestamp(self) -> float:
        if self.date_end:
            # Use end of day so the entire date is included
            return self.date_end.replace(hour=23, minute=59, second=59).timestamp()
        return datetime.now().timestamp()

    def matches_keywords(self, text: str) -> bool:
        """Check if text contains any of the configured keywords."""
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
    is_short: bool = False
    source_surface: str = "videos"
    published_text: str = ""

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
    VIDEO_WATCH_URL = "https://www.youtube.com/watch?v={video_id}"
    COMMENT_API_URL = "https://www.youtube.com/youtubei/v1/next"
    YTINITAL_DATA_PATTERN = re.compile(r"var ytInitialData = ({.*?});", re.DOTALL)
    PUBLISHED_DATE_PATTERNS = (
        re.compile(r'"(?:datePublished|uploadDate|publishDate)"\s*:\s*"([^"]+)"', re.IGNORECASE),
        re.compile(r'itemprop="(?:datePublished|uploadDate)"\s+content="([^"]+)"', re.IGNORECASE),
    )
    DATE_ONLY_PREFIX_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")

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
    PRE_WINDOW_PAGE_CAP = _env_int("SOCIAL_YOUTUBE_PRE_WINDOW_PAGE_CAP", 12)
    YTDLP_SEARCH_TIMEOUT_SECONDS = _env_int("SOCIAL_YOUTUBE_YTDLP_TIMEOUT_SECONDS", 120)

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.session = self._create_session()
        self._request_count = 0
        self.last_retrieval_meta: dict[str, Any] = {}
        self.last_comment_fetch_reason: str | None = None
        self.comments_auth_failed = False
        self._precise_publish_ts_cache: dict[str, int] = {}
        self._precise_publish_attempts = 0
        self._precise_publish_successes = 0
        self._precise_publish_failures = 0

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

    def _rate_limit(self, delay: float):
        """Apply rate limiting between requests."""
        if self._request_count > 0:
            logger.debug(f"Rate limiting: waiting {delay}s")
            time.sleep(delay)
        self._request_count += 1

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

    @staticmethod
    def _shorts_lockup_to_renderer(model: dict) -> dict:
        """Convert a shortsLockupViewModel into a videoRenderer-like dict.

        YouTube replaced ``reelItemRenderer`` with ``shortsLockupViewModel``
        in 2025.  This shim extracts the available fields so that the existing
        ``_parse_video_renderer`` path can handle shorts transparently.
        """
        reel_ep = (
            model.get("onTap", {})
            .get("innertubeCommand", {})
            .get("reelWatchEndpoint", {})
        )
        video_id = reel_ep.get("videoId", "")
        if not video_id:
            entity_id = model.get("entityId", "")
            if entity_id.startswith("shorts-shelf-item-"):
                video_id = entity_id[len("shorts-shelf-item-"):]

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

    def _parse_video_renderer(
        self,
        renderer: dict,
        config: YouTubeScrapeConfig,
        *,
        surface: str = "videos",
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
        view_text = renderer.get("viewCountText", {}).get("simpleText", "0")
        if not view_text:
            runs = renderer.get("viewCountText", {}).get("runs", [])
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

        return YouTubeVideo(
            video_id=video_id,
            title=title,
            description=description,
            date_time=datetime.fromtimestamp(published_at).strftime("%Y-%m-%d %H:%M:%S") if published_at else "",
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
            is_short=is_short,
            source_surface=surface,
            published_text=published_text,
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

    def _fetch_precise_publish_timestamp(self, video_id: str, delay: float = 2.0) -> int:
        """Fetch exact upload date from watch-page metadata."""
        cached = self._precise_publish_ts_cache.get(video_id)
        if cached is not None:
            return cached

        self._rate_limit(delay)
        url = self.VIDEO_WATCH_URL.format(video_id=video_id)
        try:
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.debug("Failed to fetch precise publish timestamp for %s: %s", video_id, e)
            self._precise_publish_ts_cache[video_id] = 0
            return 0

        body = response.text or ""
        ts = 0
        for pattern in self.PUBLISHED_DATE_PATTERNS:
            for match in pattern.finditer(body):
                ts = self._parse_precise_publish_candidate(match.group(1))
                if ts > 0:
                    break
            if ts > 0:
                break

        self._precise_publish_ts_cache[video_id] = ts
        return ts

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

        needs_refine = video.published_at <= 0
        if not needs_refine and self._is_low_precision_publish_text(video.published_text):
            needs_refine = True
        if not needs_refine:
            return current_in_range

        self._precise_publish_attempts += 1
        precise_delay = min(max(float(config.delay_seconds or 0.0) * 0.25, 0.05), 0.35)
        precise_ts = self._fetch_precise_publish_timestamp(video.video_id, precise_delay)
        if precise_ts <= 0:
            self._precise_publish_failures += 1
            return current_in_range

        self._precise_publish_successes += 1
        if precise_ts != video.published_at:
            video.published_at = precise_ts
            video.date_time = datetime.fromtimestamp(precise_ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        return config.is_in_date_range(video.published_at)

    def _channel_surface_url(self, handle: str, surface: str) -> str:
        normalized_surface = str(surface or "videos").strip().lower()
        if normalized_surface == "shorts":
            return self.CHANNEL_SHORTS_URL.format(handle=handle)
        return self.CHANNEL_VIDEOS_URL.format(handle=handle)

    def fetch_channel_videos(
        self,
        handle: str,
        delay: float = 2.0,
        surface: str = "videos",
    ) -> dict | None:
        """Fetch videos or shorts from a YouTube channel page."""
        self._rate_limit(delay)

        url = self._channel_surface_url(handle, surface)
        headers = self._get_headers()

        try:
            response = self.session.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to fetch channel %s surface=%s: %s", f"@{handle}", surface, e)
            return None

    def _fetch_continuation(self, continuation_token: str, delay: float = 2.0) -> dict | None:
        """Fetch next page of channel videos using continuation token."""
        import json

        self._rate_limit(delay)

        payload = {
            "context": self.INNERTUBE_CONTEXT,
            "continuation": continuation_token,
        }
        headers = self._get_headers()
        headers["content-type"] = "application/json"

        browse_url = "https://www.youtube.com/youtubei/v1/browse"
        try:
            response = self.session.post(
                browse_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
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
                cont = item.get("continuationItemRenderer", {})
                if cont:
                    endpoint = cont.get("continuationEndpoint", {})
                    next_token = endpoint.get("continuationCommand", {}).get("token")

        return renderers, next_token

    def search_channel_videos(self, handle: str, query: str, delay: float = 2.0) -> dict | None:
        """Search for videos from a specific channel with a query."""
        self._rate_limit(delay)

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
            response.raise_for_status()
            return self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
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
        handle = config.channel_handle.lstrip("@")
        logger.info(f"Starting YouTube scrape for @{handle}")
        if config.keywords:
            logger.info(f"Filtering by keywords: {config.keywords}")
        if config.date_start or config.date_end:
            logger.info(f"Date range: {config.date_start} to {config.date_end}")

        videos = []
        continuation_pages = 0
        timestamp_unknown_count = 0
        in_range_hits = 0
        no_hit_pages = 0
        pre_window_pages = 0
        ownership_filtered = 0
        scan_capped_reason: str | None = None
        first_page_counts: dict[str, int] = {"videos": 0, "shorts": 0}
        canonical_handle = self._normalize_handle(handle)
        canonical_channel_id = ""
        self._precise_publish_attempts = 0
        self._precise_publish_successes = 0
        self._precise_publish_failures = 0

        for surface in ("videos", "shorts"):
            logger.info("Fetching %s from @%s channel page...", surface, handle)
            surface_no_hit_pages = 0
            surface_pre_window_pages = 0
            ownership_counter = [0]
            data = self.fetch_channel_videos(handle, config.delay_seconds, surface=surface)
            if not data:
                logger.warning("Failed to fetch channel page for @%s (surface=%s)", handle, surface)
                continue
            resolved_handle, resolved_channel_id = self._extract_channel_identity_from_data(data, handle)
            if resolved_handle:
                canonical_handle = resolved_handle
            if resolved_channel_id:
                canonical_channel_id = resolved_channel_id

            initial_page_videos = self._process_video_data(
                data,
                config,
                surface=surface,
                target_handle=canonical_handle or handle,
                ownership_filtered_counter=ownership_counter,
            )
            ownership_filtered += ownership_counter[0]
            first_page_counts[surface] = len(initial_page_videos)
            videos.extend(initial_page_videos)
            self._emit_progress(
                progress_cb,
                phase="scrape_initial_page" if surface == "videos" else "scrape_initial_page_shorts",
                pages_scanned=1 if surface == "videos" else 0,
                posts_checked=len(videos),
                matched_posts=len(videos),
            )

            continuation_token = self._extract_channel_continuation_token(data)
            page_num = 1

            while continuation_token:
                if config.max_pages and continuation_pages >= config.max_pages:
                    logger.info("Reached max continuation pages limit (%s)", config.max_pages)
                    break
                if config.max_results and len(videos) >= config.max_results:
                    break

                page_num += 1
                continuation_pages += 1
                logger.info(f"Fetching {surface} page {page_num}...")
                cont_data = self._fetch_continuation(continuation_token, config.delay_seconds)
                if not cont_data:
                    break

                renderers, continuation_token = self._extract_continuation_videos_and_token(cont_data)
                if not renderers:
                    logger.info("No more videos in continuation")
                    break

                page_videos = []
                page_hits = 0
                page_window_candidates = False
                for renderer in renderers:
                    if not self._renderer_matches_owner(renderer, canonical_handle or handle):
                        ownership_filtered += 1
                        continue
                    video = self._parse_video_renderer(renderer, config, surface=surface)
                    if not video:
                        continue

                    in_range: bool | None = None
                    if video.published_at > 0:
                        in_range = config.is_in_date_range(video.published_at)
                    in_range = self._refine_video_publish_timestamp_if_needed(video, config, in_range)

                    if video.published_at > 0:
                        if in_range is None:  # Before range - keep scanning; channel ordering is not always strict.
                            page_window_candidates = True
                            continue
                        if in_range is False:  # After range - skip
                            continue
                        page_window_candidates = True
                        in_range_hits += 1
                    elif config.date_start or config.date_end:
                        # Unknown timestamps are increasingly common for dynamic renderers.
                        # Still allow keyword matching but track the count.
                        timestamp_unknown_count += 1
                        page_window_candidates = True

                    combined_text = f"{video.title} {video.description}"
                    if config.matches_keywords(combined_text):
                        page_videos.append(video)
                        page_hits += 1
                        title_short = video.title[:50] + "..." if len(video.title) > 50 else video.title
                        logger.info(f"Found: {video.video_id} - {title_short} ({video.date_time})")

                videos.extend(page_videos)
                self._emit_progress(
                    progress_cb,
                    phase="scrape_continuation_page",
                    pages_scanned=page_num,
                    posts_checked=len(videos),
                    matched_posts=len(videos),
                )
                logger.info(f"Page {page_num}: {len(page_videos)} matches, {len(videos)} total")
                if page_hits == 0:
                    if (config.date_start or config.date_end) and not page_window_candidates:
                        surface_pre_window_pages += 1
                        pre_window_pages += 1
                        if surface_pre_window_pages >= self.PRE_WINDOW_PAGE_CAP:
                            scan_capped_reason = "pre_window_cap"
                            self._emit_progress(
                                progress_cb,
                                phase="scrape_pre_window_cap",
                                pages_scanned=page_num,
                                posts_checked=len(videos),
                                matched_posts=len(videos),
                            )
                            logger.info(
                                "Stopping %s continuation crawl after pre-window cap (%d pages)",
                                surface,
                                self.PRE_WINDOW_PAGE_CAP,
                            )
                            break
                    else:
                        surface_no_hit_pages += 1
                        no_hit_pages += 1
                        # Before/after-window pages can be noisy.
                        # Give a wider no-hit runway until we get an in-range hit.
                        no_hit_threshold = 25 if (config.date_start or config.date_end) and in_range_hits == 0 else 5
                        if surface_no_hit_pages >= no_hit_threshold and (config.date_start or config.date_end):
                            logger.info("Stopping %s continuation crawl after %d no-hit pages", surface, surface_no_hit_pages)
                            break
                else:
                    surface_no_hit_pages = 0

        # Deduplicate by video_id
        seen = set()
        unique_videos = []
        for video in videos:
            if video.video_id not in seen:
                seen.add(video.video_id)
                unique_videos.append(video)

        # Enrich channel-page results with likes/comments/tags via yt-dlp.
        self._enrich_videos_via_ytdlp(unique_videos, delay=config.delay_seconds)

        # Supplement with yt-dlp when channel browsing found no matches or was capped.
        should_supplement = len(unique_videos) == 0 or scan_capped_reason is not None
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
                self._emit_progress(
                    progress_cb,
                    phase="scrape_ytdlp_fallback",
                    pages_scanned=continuation_pages + 1,
                    posts_checked=len(unique_videos),
                    matched_posts=len(unique_videos),
                )

        if config.max_results:
            unique_videos = unique_videos[: config.max_results]

        logger.info(f"Scrape complete: found {len(unique_videos)} videos")
        self._emit_progress(
            progress_cb,
            phase="scrape_complete",
            pages_scanned=max(1, continuation_pages + 1),
            posts_checked=len(unique_videos),
            matched_posts=len(unique_videos),
        )
        self.last_retrieval_meta = {
            "retrieval_mode": "channel_continuation",
            "continuation_pages": continuation_pages,
            "timestamp_unknown_count": timestamp_unknown_count,
            "in_range_hits": in_range_hits,
            "pre_window_pages": pre_window_pages,
            "pre_window_page_cap": self.PRE_WINDOW_PAGE_CAP,
            "first_page_count": int(first_page_counts.get("videos", 0)),
            "first_page_counts": first_page_counts,
            "ownership_filtered": ownership_filtered,
            "scan_capped_reason": scan_capped_reason,
            "precise_publish_attempts": self._precise_publish_attempts,
            "precise_publish_successes": self._precise_publish_successes,
            "precise_publish_failures": self._precise_publish_failures,
            "canonical_handle": canonical_handle or handle,
            "canonical_channel_id": canonical_channel_id or None,
        }
        return unique_videos

    def _enrich_videos_via_ytdlp(
        self,
        videos: list[YouTubeVideo],
        delay: float = 1.0,
    ) -> None:
        """Enrich videos with likes, comments, tags via yt-dlp --dump-single-json.

        Only enriches videos that are missing metrics (likes == 0 and comments == 0).
        Mutates videos in place.
        """
        if not shutil.which("yt-dlp"):
            logger.debug("yt-dlp not available; skipping enrichment")
            return

        needs_enrichment = [
            v for v in videos
            if isinstance(v, YouTubeVideo) and v.likes == 0 and v.comments == 0
        ]
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

            video.likes = data.get("like_count", 0) or 0
            video.comments = data.get("comment_count", 0) or 0
            video.views = data.get("view_count", video.views) or video.views
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
                    is_short=is_short,
                    source_surface="search",
                    show_id=config.show_id,
                    season_number=config.season_number,
                )
                all_videos.append(video)
                logger.info(f"yt-dlp found: {vid_id} - {video.title[:50]}... ({dt_str})")

        logger.info(f"yt-dlp search total: {len(all_videos)} videos from channel @{target_handle}")
        return all_videos

    def _process_video_data(
        self,
        data: dict,
        config: YouTubeScrapeConfig,
        *,
        surface: str = "videos",
        target_handle: str | None = None,
        ownership_filtered_counter: list[int] | None = None,
    ) -> list[YouTubeVideo]:
        """Process video data and apply filters."""
        videos = []
        for renderer in self._iter_video_renderers(data):
            if target_handle and not self._renderer_matches_owner(renderer, target_handle):
                if ownership_filtered_counter is not None:
                    ownership_filtered_counter[0] += 1
                continue
            video = self._parse_video_renderer(renderer, config, surface=surface)
            if not video:
                continue

            # Check date range if publish date is known
            in_range: bool | None = None
            if video.published_at > 0:
                in_range = config.is_in_date_range(video.published_at)
            in_range = self._refine_video_publish_timestamp_if_needed(video, config, in_range)
            if video.published_at > 0:
                if in_range is None:  # Before range
                    continue
                if in_range is False:  # After range
                    continue

            # Check keyword filter
            combined_text = f"{video.title} {video.description}"
            if config.matches_keywords(combined_text):
                videos.append(video)
                title_short = video.title[:50] + "..." if len(video.title) > 50 else video.title
                logger.info(f"Found: {video.video_id} - {title_short} ({video.date_time})")

        return videos

    def fetch_comments(
        self,
        video_id: str,
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
    ) -> list[YouTubeComment]:
        """
        Fetch comments for a YouTube video including replies.

        Args:
            video_id: The YouTube video ID
            max_comments: Maximum number of top-level comments to fetch
            fetch_replies: Whether to fetch replies to comments
            delay: Delay between API requests

        Returns:
            List of YouTubeComment objects with nested replies
        """
        self.last_comment_fetch_reason = None
        self.comments_auth_failed = False
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        logger.info(f"Fetching comments for video {video_id}")

        # First, get the video page to extract the comment continuation token
        self._rate_limit(delay)
        try:
            response = self.session.get(
                video_url,
                headers=self._get_headers(),
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            yt_data = self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
            self._set_comment_failure_reason("request_error")
            logger.error(f"Failed to fetch video page: {e}")
            return []

        if not yt_data:
            self._set_comment_failure_reason("parse_error")
            logger.error("Could not extract ytInitialData from video page")
            return []

        # Extract comment section continuation token
        continuation_token = self._extract_comment_continuation(yt_data)
        if not continuation_token:
            self._set_comment_failure_reason("comments_unavailable")
            logger.warning("No comment continuation token found - video may have comments disabled")
            return []

        comments = []
        comments_fetched = 0

        while continuation_token:
            self._rate_limit(delay)

            # Fetch comments using continuation
            comment_data = self._fetch_comment_continuation(continuation_token, delay)
            if not comment_data:
                if not self.last_comment_fetch_reason:
                    self._set_comment_failure_reason("continuation_fetch_failed")
                break

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
            on_response = data.get("onResponseReceivedEndpoints", [])
            for endpoint in on_response:
                # Reload continuation (initial load)
                reload_items = endpoint.get("reloadContinuationItemsCommand", {})
                if reload_items:
                    for item in reload_items.get("continuationItems", []):
                        if "commentThreadRenderer" in item:
                            items.append(item)
                        elif "continuationItemRenderer" in item:
                            cont = item.get("continuationItemRenderer", {})
                            endpoint_data = cont.get("continuationEndpoint", {})
                            next_continuation = endpoint_data.get("continuationCommand", {}).get("token")

                # Append continuation (subsequent loads)
                append_items = endpoint.get("appendContinuationItemsAction", {})
                if append_items:
                    for item in append_items.get("continuationItems", []):
                        if "commentThreadRenderer" in item:
                            items.append(item)
                        elif "continuationItemRenderer" in item:
                            cont = item.get("continuationItemRenderer", {})
                            endpoint_data = cont.get("continuationEndpoint", {})
                            next_continuation = endpoint_data.get("continuationCommand", {}).get("token")
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
    ) -> YouTubeComment | None:
        """Parse a comment thread into YouTubeComment."""
        try:
            thread = item.get("commentThreadRenderer", {})
            comment_renderer = thread.get("comment", {}).get("commentRenderer", {})
            if comment_renderer:
                comment = self._parse_comment_renderer(comment_renderer, video_id, video_url)
            else:
                comment_vm = thread.get("commentViewModel", {}).get("commentViewModel", {})
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
                        reply_continuation, video_id, video_url, comment.comment_id, delay
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
            date_time=datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S") if created_at else "",
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
                date_time=datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S") if created_at else "",
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
    ) -> list[YouTubeComment]:
        """Fetch replies to a comment."""
        replies = []

        while continuation_token:
            self._rate_limit(delay)
            data = self._fetch_comment_continuation(continuation_token, delay)
            if not data:
                break

            next_continuation = None
            entity_index = self._build_comment_entity_index(data)
            try:
                on_response = data.get("onResponseReceivedEndpoints", [])
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
                            reply_vm = item.get("commentViewModel", {})
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
                        cont_renderer = item.get("continuationItemRenderer", {})
                        if cont_renderer:
                            ep = cont_renderer.get("continuationEndpoint", {})
                            next_continuation = ep.get("continuationCommand", {}).get("token")
            except (KeyError, TypeError):
                break

            continuation_token = next_continuation

        return replies
