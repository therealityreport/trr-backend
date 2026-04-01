"""Facebook page/reels/photos scraper with compliant public-first behavior."""

from __future__ import annotations

import base64
import hashlib
import html
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_REEL_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/reel/([0-9]+)", re.IGNORECASE)
_PAGE_REELS_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/([^/?#]+)/reels/?", re.IGNORECASE)
_PAGE_PHOTOS_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/([^/?#]+)/photos/?", re.IGNORECASE)
_PAGE_POST_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/([^/?#]+)/posts/([A-Za-z0-9._-]+)", re.IGNORECASE)
_GROUP_POST_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/groups/([0-9]+)/posts/([0-9]+)", re.IGNORECASE)
_POST_ID_RE = re.compile(r'"post_id":"?([0-9]{6,})"?')
_OG_URL_RE = re.compile(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']', re.IGNORECASE)
_OG_TITLE_RE = re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.IGNORECASE)
_OG_DESC_RE = re.compile(
    r'<meta[^>]+property=["\'](?:og:description|description)["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_OG_IMAGE_RE = re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.IGNORECASE)
_PUBLISHED_TIME_RE = re.compile(
    r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_RELATIVE_REEL_HREF_RE = re.compile(r'href=["\'](/reel/([0-9]+)[^"\']*)["\']', re.IGNORECASE)
_RELATIVE_VIDEO_HREF_RE = re.compile(
    r'href=["\'](/[^"\']+/videos/([0-9]+)[^"\']*)["\']',
    re.IGNORECASE,
)
_RELATIVE_GROUP_POST_HREF_RE = re.compile(
    r'href=["\'](/groups/[0-9]+/posts/[0-9]+[^"\']*)["\']',
    re.IGNORECASE,
)
_PHOTO_FBID_HREF_RE = re.compile(
    r'href="(/photo/\?fbid=([0-9]+)[^"]*)"',
    re.IGNORECASE,
)
_PERMALINK_URL_JSON_RE = re.compile(
    r'"permalink_url":"((?:[^"\\]|\\.)*)"',
)
_SHARE_URL_RE = re.compile(
    r"https://(?:www\.)?facebook\.com/share/(?:v|p|r)/([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)
_VIDEO_URL_RE = re.compile(
    r"https://(?:www\.)?facebook\.com/([^/?#]+)/videos/([0-9]+)",
    re.IGNORECASE,
)
_TRACKING_QUERY_PREFIXES = ("utm_",)
_TRACKING_QUERY_KEYS = {"fbclid", "__tn__", "__cft__", "ref", "refsrc"}

# Regex patterns for extracting engagement metrics from Facebook SSR JSON blobs
_FB_FEEDBACK_BLOCK_RE = re.compile(
    r'"feedback":\{"id":"[^"]+","comment_rendering_instance":\{"comments":\{"total_count":(\d+)\}\}',
)
_FB_REACTION_COUNT_RE = re.compile(r'"reaction_count"\s*:\s*\{\s*"count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_REACTION_I18N_RE = re.compile(r'"i18n_reaction_count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_TOTAL_COMMENT_COUNT_RE = re.compile(r'"total_comment_count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_TOTAL_COMMENT_COUNT_REDUCED_RE = re.compile(
    r'"total_comment_count_reduced"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?'
)
_FB_COMMENT_COUNT_RE = re.compile(
    r'"comment_count"\s*:\s*\{[^{}]*?"count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?[^{}]*\}'
)
_FB_VIDEO_VIEW_COUNT_RE = re.compile(r'"video_view_count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_VIDEO_VIEW_COUNT_REDUCED_RE = re.compile(r'"video_view_count_reduced"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_PLAY_COUNT_RE = re.compile(r'"play_count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_PLAY_COUNT_REDUCED_RE = re.compile(r'"play_count_reduced"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
# 2026-03: Facebook SPA renders view counts in og:title as "69K views · 8.5K reactions | ..."
_FB_OG_TITLE_VIEWS_RE = re.compile(r"([\d,.]+[KkMmBb]?)\s*views", re.IGNORECASE)
_FB_SHARE_COUNT_RE = re.compile(r'"share_count"\s*:\s*\{\s*"count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_RESHARE_COUNT_RE = re.compile(r'"reshare_count"\s*:\s*\{\s*"count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?')
_FB_SHARE_COUNT_REDUCED_RE = re.compile(
    r'"share_count"\s*:\s*\{[^{}]*"count_reduced"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?[^{}]*\}'
)
_FB_RESHARE_COUNT_REDUCED_RE = re.compile(
    r'"reshare_count"\s*:\s*\{[^{}]*"count_reduced"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?[^{}]*\}'
)
_FB_TOP_REACTIONS_RE = re.compile(
    r'"top_reactions"\s*:\s*\{(?:"count"\s*:\s*[0-9A-Za-z]+,\s*)?"edges"\s*:\s*\[([^\]]*)\]',
    re.IGNORECASE | re.DOTALL,
)
_FB_REACTION_EDGE_RE = re.compile(
    r'"(?:localized_name|name)"\s*:\s*"([^\"]+)"(?:[^{}]*\{[^{}]*\})?[^}]*?'
    r'"(?:reaction_count|count)"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?',
    re.IGNORECASE | re.DOTALL,
)
# 2026-03 Facebook SSR format: engagement inside unified_reactors and flat
# share_count_reduced in the feedback block (reaction_count is gone).
_FB_UNIFIED_REACTORS_COUNT_RE = re.compile(
    r'"unified_reactors"\s*:\s*\{\s*"count"\s*:\s*"?([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"?'
)
_FB_FLAT_SHARE_COUNT_REDUCED_RE = re.compile(r'"share_count_reduced"\s*:\s*"([0-9]+(?:\.[0-9]+)?[KkMmBb]?)"')

# JSON-based patterns for authenticated SPA responses (no OG meta tags)
_FB_MESSAGE_TEXT_RE = re.compile(r'"message":\{"text":"((?:[^"\\]|\\.)*)"')
_FB_PERMALINK_URL_RE = re.compile(r'"permalink_url":"((?:[^"\\]|\\.)*)"')
# Post creation timestamp embedded in Facebook's JSON data (unix epoch seconds).
# Facebook rarely includes <meta article:published_time>, but almost always
# embeds "creation_time":<epoch> in inline script JSON for the primary post.
_FB_CREATION_TIME_RE = re.compile(r'"creation_time"\s*:\s*(\d{10})')
_FB_DURATION_MS_RE = re.compile(
    r'"(?:playable_duration_in_ms|playable_duration_ms|video_duration_ms)"\s*:\s*([0-9]{2,})'
)
_FB_DURATION_SECONDS_RE = re.compile(
    r'"(?:duration_in_sec|duration_seconds|playable_duration(?:_in_seconds)?)"\s*:\s*([0-9]+(?:\.[0-9]+)?)'
)
_FB_OWNER_PAGE_NAME_RE = re.compile(r'"owner_as_page":\{[^}]*"name":"((?:[^"\\]|\\.)*)"')
_FB_OWNER_PROFILE_PICTURE_URI_RE = re.compile(
    r'"(?:owner_as_page|owner)":\{[^{}]*?"profile_picture":\{[^{}]*?"uri":"((?:[^"\\]|\\.)*)"',
)
_FB_PROFILE_PICTURE_URI_RE = re.compile(
    r'"profile_picture":\{[^{}]*?"uri":"((?:[^"\\]|\\.)*)"',
)
_FB_OWNER_PROFILE_PIC_URL_RE = re.compile(
    r'"(?:owner_as_page|owner)":\{[^{}]*?"(?:profile_pic_url|profilePicUrl|profile_image_url)":"((?:[^"\\]|\\.)*)"',
)
_FB_INSTAGRAM_URL_RE = re.compile(
    (
        r"https?://(?:l\.facebook\.com/l\.php\?(?:[^\"' >]*[?&](?:u|url)=)?|www\.)?"
        r"instagram\.com/(?:p|reel|tv)/[A-Za-z0-9_-]+/?(?:\?[^\"' >]*)?"
    ),
    re.IGNORECASE,
)
_FB_INSTAGRAM_EMBEDDED_URL_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/(?:p|reel|tv)/[A-Za-z0-9_-]+/?(?:\?[^\"' >]*)?",
    re.IGNORECASE,
)
_FB_SHARE_COUNT_TEXT_RE = re.compile(r"^\s*\d[\d,.KMBkmb]*\s+shares?\s*$")
_FB_PROFILE_NAME_BLOCK_RE = re.compile(r'data-ad-rendering-role="profile_name"', re.IGNORECASE)
_FB_STORY_MESSAGE_RE = re.compile(
    r'data-ad-rendering-role="story_message"[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_FB_PROFILE_LINK_TEXT_RE = re.compile(
    r'href="([^"]+)"[^>]*>\s*(?:<[^>]+>)*\s*([^<][^<]{1,200}?)\s*(?:</[^>]+>)*\s*</a>',
    re.IGNORECASE | re.DOTALL,
)
_FB_PRIVACY_LABEL_RE = re.compile(r"<title>(Shared with [^<]+)</title>", re.IGNORECASE)
_FB_IMAGE_SRC_RE = re.compile(r'<img[^>]+src="([^"]+)"', re.IGNORECASE)
_FB_POST_URL_IN_SEGMENT_RE = re.compile(
    r'href="([^"]*(?:/posts/|/groups/[0-9]+/posts/|/reel/|/videos/)[^"]*)"',
    re.IGNORECASE,
)


def _strip_tags(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_caption_for_match(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(str(value or "")).strip()).casefold()


@dataclass
class FacebookScrapeConfig:
    page_handle: str
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 1.25
    max_pages: int | None = 1
    include_feed: bool = True
    include_reels: bool = True
    include_photos: bool = True

    # Performance tuning
    fast_mode: bool = False
    """When True, uses aggressive rate-limiting tiers for faster scraping."""

    fetch_comment_replies: bool = True
    """When False, only fetch top-level comments and skip reply chains."""

    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None
    max_scroll_iterations: int = 50
    max_scrape_seconds: float = 600.0
    """Overall wall-clock timeout for the entire scrape() call (default: 10 min)."""

    def __post_init__(self):
        """Apply fast_mode overrides when enabled."""
        if self.fast_mode:
            # Use a lower base delay unless explicitly overridden
            if self.delay_seconds == 1.25:  # Only override if at default
                self.delay_seconds = 0.5
            logger.info(
                "FacebookScrapeConfig fast_mode enabled: delay=%.2fs",
                self.delay_seconds,
            )

    @property
    def normalized_handle(self) -> str:
        return str(self.page_handle or "").strip().lstrip("@")

    def in_date_window(self, value: datetime | None) -> bool:
        if value is None:
            return self.date_start is None and self.date_end is None
        if self.date_start and value < self.date_start:
            return False
        if self.date_end and value > self.date_end:
            return False
        return True


@dataclass
class FacebookSearchConfig:
    query: str
    search_url: str | None = None
    profile_url: str | None = None
    date_start: datetime | None = None
    date_end: datetime | None = None
    max_posts: int = 25
    include_share_details: bool = False
    include_comments: bool = False
    max_comments: int = 100
    max_shares: int = 100
    allow_cross_platform_media_fallback: bool = True
    delay_seconds: float = 1.25
    max_scroll_iterations: int = 25

    @property
    def normalized_query(self) -> str:
        return str(self.query or "").strip()

    def in_date_window(self, value: datetime | None) -> bool:
        if value is None:
            return self.date_start is None and self.date_end is None
        if self.date_start and value < self.date_start:
            return False
        if self.date_end and value > self.date_end:
            return False
        return True


@dataclass
class FacebookShare:
    sharer_name: str
    profile_url: str | None = None
    post_url: str | None = None
    caption_snippet: str | None = None
    posted_at: int | None = None
    privacy_label: str | None = None
    media_preview_urls: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FacebookMediaProvenance:
    platform: str = "facebook"
    matched_by: str = "native"
    fallback_used: bool = False
    source_url: str | None = None
    candidate_urls: list[str] = field(default_factory=list)
    attempts: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _canonicalize_facebook_post_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or "").strip().lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = "/" + "/".join(segment for segment in parsed.path.split("/") if segment)
    query_items: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        normalized_key = str(key or "").strip().lower()
        if not normalized_key:
            continue
        if normalized_key in _TRACKING_QUERY_KEYS:
            continue
        if any(normalized_key.startswith(prefix) for prefix in _TRACKING_QUERY_PREFIXES):
            continue
        query_items.append((normalized_key, str(value or "").strip()))
    query_items.sort()
    query = urlencode(query_items, doseq=True)
    return urlunparse((scheme, netloc, path, "", query, ""))


def _deterministic_fallback_post_id(url: str) -> str:
    canonical = _canonicalize_facebook_post_url(url)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]  # noqa: S324
    return f"fb_{digest}"


@dataclass
class FacebookComment:
    comment_id: str
    username: str
    text: str
    likes: int = 0
    created_at: int | None = None
    is_reply: bool = False
    reply_count: int = 0
    parent_source_comment_id: str | None = None
    media_urls: list[str] = field(default_factory=list)
    replies: list[FacebookComment] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["replies"] = [reply.to_dict() for reply in self.replies]
        return payload


@dataclass
class FacebookPost:
    post_id: str
    username: str
    post_type: str
    caption: str
    media_urls: list[str]
    thumbnail_url: str | None
    user_avatar_url: str | None = None
    duration_seconds: float | None = None
    likes: int = 0
    comments: int = 0
    shares: int = 0
    views: int = 0
    posted_at: int | None = None
    url: str = ""
    reactions: dict[str, int] = field(default_factory=dict)
    share_details: list[FacebookShare] = field(default_factory=list)
    media_provenance: FacebookMediaProvenance = field(default_factory=FacebookMediaProvenance)
    raw_data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FacebookScraper:
    BASE_URL = "https://www.facebook.com"

    def __init__(self, *, cookies: dict[str, str] | None = None):
        self.cookies = cookies or {}
        self.session = self._create_session()
        self.last_retrieval_meta: dict[str, Any] = {}
        self.last_comment_fetch_reason: str | None = None
        self.comments_auth_failed = False
        self._request_count = 0
        self._last_429_at: float = 0.0
        self._consecutive_success: int = 0

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    _USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )

    def _headers(self, *, referer: str | None = None, document: bool = True) -> dict[str, str]:
        headers: dict[str, str] = {
            "accept-language": "en-US,en;q=0.9",
            "user-agent": self._USER_AGENT,
            # Browser fingerprint headers required by Meta for full SSR payload
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }
        if document:
            headers["accept"] = (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            )
            headers["sec-fetch-dest"] = "document"
            headers["sec-fetch-mode"] = "navigate"
            headers["sec-fetch-site"] = "none"
            headers["sec-fetch-user"] = "?1"
            headers["upgrade-insecure-requests"] = "1"
            headers["cache-control"] = "max-age=0"
        else:
            headers["accept"] = "*/*"
        if referer:
            headers["referer"] = referer
        return headers

    def _rate_limit(self, delay: float, *, fast_mode: bool = False) -> None:
        """Apply adaptive rate limiting between requests.

        Standard mode: uses the base delay as-is.
        Fast mode: uses aggressive tiers that ramp down with consecutive successes.
        Both modes: double delay for 60s after any 429 response.
        """
        if self._request_count > 0 and delay > 0:
            now = time.monotonic()
            if self._last_429_at and (now - self._last_429_at) < 60.0:
                effective_delay = delay * 2.0
            elif fast_mode:
                # Aggressive tiers: ramp down as we prove the session is healthy
                if self._consecutive_success >= 20:
                    effective_delay = delay * 0.15
                elif self._consecutive_success >= 5:
                    effective_delay = delay * 0.25
                else:
                    effective_delay = delay * 0.5
            else:
                effective_delay = delay
            logger.debug(
                "Rate limiting: waiting %.3fs (base=%.2fs, fast=%s, streak=%d)",
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

    def _playwright_cookie_list(self) -> list[dict[str, Any]]:
        cookies: list[dict[str, Any]] = []
        for name, value in (self.cookies or {}).items():
            key = str(name or "").strip()
            if not key:
                continue
            cookies.append(
                {
                    "name": key,
                    "value": str(value or ""),
                    "domain": ".facebook.com",
                    "path": "/",
                    "secure": True,
                    "httpOnly": False,
                }
            )
        return cookies

    def _new_playwright_context(self, playwright: Any, *, referer: str | None = None) -> Any:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=self._headers(referer=referer).get("user-agent", ""),
            locale="en-US",
        )
        cookies = self._playwright_cookie_list()
        if cookies:
            try:
                context.add_cookies(cookies)
            except Exception:  # noqa: BLE001
                logger.debug("[facebook] failed to load cookies into playwright context", exc_info=True)
        return browser, context

    def _fetch_html(
        self, url: str, *, delay_seconds: float, referer: str | None = None, fast_mode: bool = False
    ) -> str:
        self._rate_limit(delay_seconds, fast_mode=fast_mode)
        try:
            response = self.session.get(
                url,
                timeout=(10, 45),
                headers=self._headers(referer=referer),
                cookies=self.cookies,
            )
            self._track_response_status(response.status_code)
            response.raise_for_status()
            return response.text or ""
        except Exception as exc:  # noqa: BLE001
            if not self._playwright_fallback_enabled():
                raise
            # Facebook often returns 400 to non-browser clients. Fallback keeps
            # compliant, public-only behavior while using a real browser renderer.
            logger.info("[facebook] requests fetch failed for %s (%s); trying playwright fallback", url, exc)
            return self._fetch_html_with_playwright(url, delay_seconds=delay_seconds, referer=referer)

    @staticmethod
    def _playwright_fallback_enabled() -> bool:
        raw = (os.getenv("SOCIAL_FACEBOOK_PLAYWRIGHT_FALLBACK", "true") or "").strip().lower()
        return raw not in {"0", "false", "off", "no"}

    def _fetch_html_with_playwright(
        self,
        url: str,
        *,
        delay_seconds: float,
        referer: str | None = None,
        wait_for_spa: bool = False,
        skip_cookies: bool = False,
    ) -> str:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Playwright fallback requested but playwright is unavailable") from exc

        with sync_playwright() as playwright:
            if skip_cookies:
                # Unauthenticated context: Facebook's SPA render includes view
                # counts in og:title when no session cookies are present.
                browser = playwright.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=self._headers(referer=referer).get("user-agent", ""),
                    locale="en-US",
                )
            else:
                browser, context = self._new_playwright_context(playwright, referer=referer)
            page = context.new_page()
            if wait_for_spa:
                # SPA enrichment: wait for network to settle so async payloads
                # containing view/play counts are loaded into the DOM.
                page.goto(url, wait_until="networkidle", timeout=60_000)
                page.wait_for_timeout(3000)
            else:
                page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                if delay_seconds > 0:
                    page.wait_for_timeout(max(500, int(delay_seconds * 1000)))
            try:
                page.keyboard.press("Escape")
            except Exception:  # noqa: BLE001
                pass
            html_text = page.content() or ""
            browser.close()
            return html_text

    @staticmethod
    def _build_search_filters_param(date_start: datetime | None, date_end: datetime | None) -> str | None:
        if date_start is None and date_end is None:
            return None
        start = date_start or date_end
        end = date_end or date_start
        if start is None or end is None:
            return None
        inner = {
            "start_year": start.strftime("%Y"),
            "start_month": start.strftime("%Y-%-m"),
            "end_year": end.strftime("%Y"),
            "end_month": end.strftime("%Y-%-m"),
            "start_day": start.strftime("%Y-%-m-%-d"),
            "end_day": end.strftime("%Y-%-m-%-d"),
        }
        outer = {
            "rp_creation_time:0": json.dumps(
                {"name": "creation_time", "args": json.dumps(inner, separators=(",", ":"))},
                separators=(",", ":"),
            )
        }
        return base64.b64encode(json.dumps(outer, separators=(",", ":")).encode("utf-8")).decode("ascii")

    @staticmethod
    def _build_search_url(config: FacebookSearchConfig) -> str:
        if config.search_url:
            parsed = urlparse(config.search_url)
            params = dict(parse_qsl(parsed.query, keep_blank_values=True))
            params["q"] = config.normalized_query
            filters = FacebookScraper._build_search_filters_param(config.date_start, config.date_end)
            if filters:
                params["filters"] = filters
            return urlunparse(parsed._replace(query=urlencode(params)))

        base = str(config.profile_url or "").rstrip("/")
        if not base:
            raise ValueError("profile_url or search_url is required")
        if not base.endswith("/search"):
            if not base.endswith("/search/"):
                base = f"{base}/search"
        params = {"q": config.normalized_query}
        filters = FacebookScraper._build_search_filters_param(config.date_start, config.date_end)
        if filters:
            params["filters"] = filters
        return f"{base}?{urlencode(params)}"

    @staticmethod
    def _normalize_url_candidate(url: str) -> str:
        raw = html.unescape(str(url or "").strip())
        if not raw:
            return ""
        if raw.startswith("/"):
            return f"https://www.facebook.com{raw}"
        return raw

    @staticmethod
    def _extract_instagram_candidate_urls(text: str) -> list[str]:
        candidates: list[str] = []
        for match in _FB_INSTAGRAM_EMBEDDED_URL_RE.finditer(text or ""):
            url = str(match.group(0) or "").strip()
            if url:
                candidates.append(url.split("&fbclid=", 1)[0])
        for match in re.finditer(r"https?://l\.facebook\.com/l\.php\?[^\"' >]+", text or "", re.IGNORECASE):
            raw = html.unescape(str(match.group(0) or ""))
            parsed = urlparse(raw)
            query = parse_qs(parsed.query)
            target = ""
            for key in ("u", "url"):
                if query.get(key):
                    target = unquote(str(query[key][0] or ""))
                    break
            if _FB_INSTAGRAM_EMBEDDED_URL_RE.search(target):
                candidates.append(target.split("&fbclid=", 1)[0])
        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            normalized = candidate.rstrip("/")
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(candidate)
        return deduped

    @staticmethod
    def _extract_duration_seconds(html_text: str) -> float | None:
        if ms_match := _FB_DURATION_MS_RE.search(html_text):
            try:
                return round(int(ms_match.group(1)) / 1000.0, 3)
            except ValueError:
                return None
        if sec_match := _FB_DURATION_SECONDS_RE.search(html_text):
            try:
                return round(float(sec_match.group(1)), 3)
            except ValueError:
                return None
        return None

    @staticmethod
    def _post_types_match(facebook_post_type: str, instagram_media_type: str | None) -> bool:
        fb = str(facebook_post_type or "").strip().lower()
        ig = str(instagram_media_type or "").strip().lower()
        if not fb or not ig:
            return False
        if fb == ig:
            return True
        if fb == "reel" and ig in {"video", "reel"}:
            return True
        if fb == "photo" and ig in {"image", "photo"}:
            return True
        if fb == "feed" and ig in {"image", "photo", "video"}:
            return True
        return False

    @staticmethod
    def _same_calendar_day(first_epoch: int | None, second_dt: datetime | None) -> bool:
        if first_epoch is None or second_dt is None:
            return False
        first_dt = datetime.fromtimestamp(int(first_epoch), tz=UTC)
        second_utc = second_dt.astimezone(UTC) if second_dt.tzinfo else second_dt.replace(tzinfo=UTC)
        return first_dt.date() == second_utc.date()

    @staticmethod
    def _extract_share_details_from_html(dialog_html: str, *, max_shares: int) -> list[FacebookShare]:
        shares: list[FacebookShare] = []
        segments = _FB_PROFILE_NAME_BLOCK_RE.split(dialog_html or "")
        for segment in segments[1:]:
            if len(shares) >= max_shares:
                break
            links = _FB_PROFILE_LINK_TEXT_RE.findall(segment)
            if not links:
                continue
            profile_url: str | None = None
            sharer_name = ""
            for href, text in links:
                normalized_href = FacebookScraper._normalize_url_candidate(href)
                cleaned_text = _strip_tags(text)
                if "facebook.com" in normalized_href and cleaned_text and not sharer_name:
                    profile_url = normalized_href
                    sharer_name = cleaned_text
                    break
            if not sharer_name:
                continue
            post_url = None
            if post_match := _FB_POST_URL_IN_SEGMENT_RE.search(segment):
                post_url = FacebookScraper._normalize_url_candidate(post_match.group(1))
            caption = None
            if msg_match := _FB_STORY_MESSAGE_RE.search(segment):
                caption = _strip_tags(msg_match.group(1)) or None
            privacy = None
            if privacy_match := _FB_PRIVACY_LABEL_RE.search(segment):
                privacy = _strip_tags(privacy_match.group(1)) or None
            preview_urls = []
            for img_url in _FB_IMAGE_SRC_RE.findall(segment):
                if "scontent" in img_url or "fbcdn" in img_url:
                    preview_urls.append(html.unescape(img_url))
            deduped_previews: list[str] = []
            seen_preview: set[str] = set()
            for url in preview_urls:
                if url in seen_preview:
                    continue
                seen_preview.add(url)
                deduped_previews.append(url)
            shares.append(
                FacebookShare(
                    sharer_name=sharer_name,
                    profile_url=profile_url,
                    post_url=post_url,
                    caption_snippet=caption,
                    posted_at=None,
                    privacy_label=privacy,
                    media_preview_urls=deduped_previews[:4],
                )
            )
        return shares

    def _resolve_cross_platform_media_fallback(
        self,
        *,
        post: FacebookPost,
        html_text: str,
        allow_fallback: bool,
    ) -> None:
        if not allow_fallback or post.media_urls:
            return
        candidates = self._extract_instagram_candidate_urls(html_text)
        if not candidates:
            return
        attempts: list[dict[str, Any]] = []
        try:
            from trr_backend.socials.instagram import InstagramScraper, resolve_instagram_media
        except Exception:  # noqa: BLE001
            return

        ig_scraper = InstagramScraper()
        normalized_caption = _normalize_caption_for_match(post.caption)
        for candidate in candidates:
            try:
                resolution = resolve_instagram_media(
                    candidate,
                    session=ig_scraper.session,
                    timeout=ig_scraper.request_timeout,
                    headers=ig_scraper._get_headers(candidate),
                    cookies=ig_scraper.cookies,
                    fetch_post_info=ig_scraper.fetch_post_info,
                )
            except Exception as exc:  # noqa: BLE001
                attempts.append({"candidate_url": candidate, "matched": False, "reason": f"resolution_failed:{exc}"})
                continue
            media_item = getattr(resolution, "metadata", None)
            caption = ""
            if media_item is not None and hasattr(media_item, "raw_media"):
                caption = ig_scraper._extract_caption(getattr(media_item, "raw_media", {}) or {})
            caption_match = normalized_caption and _normalize_caption_for_match(caption) == normalized_caption
            day_match = self._same_calendar_day(post.posted_at, getattr(media_item, "taken_at", None))
            type_match = self._post_types_match(post.post_type, getattr(resolution, "media_type", None))
            duration_match = False
            duration_seconds = getattr(media_item, "duration_seconds", None)
            if post.duration_seconds is not None and duration_seconds is not None:
                try:
                    duration_match = abs(float(post.duration_seconds) - float(duration_seconds)) <= 2.0
                except (TypeError, ValueError):
                    duration_match = False
            attempts.append(
                {
                    "candidate_url": candidate,
                    "matched": bool(caption_match and day_match and (type_match or duration_match)),
                    "caption_match": caption_match,
                    "same_day": day_match,
                    "type_match": type_match,
                    "duration_match": duration_match,
                    "resolution_source": getattr(resolution, "source", None),
                }
            )
            if not (caption_match and day_match and (type_match or duration_match)):
                continue
            if resolution.media_urls:
                post.media_urls = list(resolution.media_urls)
            if not post.thumbnail_url:
                post.thumbnail_url = resolution.thumbnail_url
            post.media_provenance = FacebookMediaProvenance(
                platform="instagram",
                matched_by="caption+same_day+type_or_duration",
                fallback_used=True,
                source_url=candidate,
                candidate_urls=candidates,
                attempts=attempts + list(getattr(resolution, "attempts", []) or []),
            )
            post.raw_data["media_provenance"] = post.media_provenance.to_dict()
            return
        if attempts:
            post.media_provenance = FacebookMediaProvenance(
                platform="facebook",
                matched_by="native_unavailable",
                fallback_used=False,
                source_url=post.url,
                candidate_urls=candidates,
                attempts=attempts,
            )
            post.raw_data["media_provenance"] = post.media_provenance.to_dict()

    def _discover_search_post_urls(self, config: FacebookSearchConfig) -> list[str]:
        if not self._playwright_fallback_enabled():
            raise RuntimeError("Playwright is required for Facebook search scraping")
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Playwright unavailable for Facebook search scraping") from exc

        search_url = self._build_search_url(config)
        discovered: list[str] = []
        seen: set[str] = set()

        with sync_playwright() as playwright:
            browser, context = self._new_playwright_context(playwright, referer=config.profile_url or search_url)
            page = context.new_page()
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=45_000)
                page.wait_for_timeout(max(750, int(config.delay_seconds * 1000)))
                stagnant_cycles = 0
                for _ in range(max(1, config.max_scroll_iterations)):
                    html_text = page.content() or ""
                    candidates = self._extract_post_urls(html_text, handle="")
                    new_count = 0
                    for candidate_url, _kind in candidates:
                        normalized = self._normalize_url_candidate(candidate_url)
                        if not normalized or normalized in seen:
                            continue
                        seen.add(normalized)
                        discovered.append(normalized)
                        new_count += 1
                        if len(discovered) >= max(1, config.max_posts):
                            break
                    if len(discovered) >= max(1, config.max_posts):
                        break
                    if new_count == 0:
                        stagnant_cycles += 1
                        if stagnant_cycles >= 3:
                            break
                    else:
                        stagnant_cycles = 0
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(max(750, int(config.delay_seconds * 1000)))
            finally:
                context.close()
                browser.close()

        self.last_retrieval_meta = {
            "source": "facebook_search",
            "search_url": search_url,
            "posts_discovered": len(discovered),
            "cookies_supplied": bool(self.cookies),
        }
        return discovered[: max(1, config.max_posts)]

    def _scrape_share_details(self, post_url: str, *, max_shares: int, delay_seconds: float) -> list[FacebookShare]:
        if max_shares <= 0 or not self._playwright_fallback_enabled():
            return []
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception:  # noqa: BLE001
            return []

        with sync_playwright() as playwright:
            browser, context = self._new_playwright_context(playwright, referer=post_url)
            page = context.new_page()
            try:
                page.goto(post_url, wait_until="domcontentloaded", timeout=45_000)
                page.wait_for_timeout(max(750, int(delay_seconds * 1000)))
                share_locator = page.locator('[role="button"], [role="link"]').filter(
                    has_text=re.compile(r"\bshares?\b", re.I)
                )
                if share_locator.count() == 0:
                    return []
                share_locator.first.click(timeout=5_000)
                dialog = page.locator('[role="dialog"][aria-label="People who shared this"]')
                dialog.wait_for(timeout=5_000)
                stable_cycles = 0
                prior_count = -1
                collected: list[FacebookShare] = []
                for _ in range(15):
                    dialog_html = dialog.inner_html(timeout=5_000)
                    collected = self._extract_share_details_from_html(dialog_html, max_shares=max_shares)
                    if len(collected) >= max_shares:
                        break
                    if len(collected) == prior_count:
                        stable_cycles += 1
                        if stable_cycles >= 3:
                            break
                    else:
                        stable_cycles = 0
                    prior_count = len(collected)
                    page.evaluate(
                        """() => {
                            const dialog = document.querySelector(
                              '[role="dialog"][aria-label="People who shared this"]'
                            );
                            if (!dialog) return;
                            const candidates = Array.from(dialog.querySelectorAll('div'))
                              .filter((el) => el.scrollHeight > el.clientHeight + 20);
                            const target = candidates.sort((a, b) => b.scrollHeight - a.scrollHeight)[0];
                            if (target) target.scrollTop = target.scrollHeight;
                        }"""
                    )
                    page.wait_for_timeout(max(500, int(delay_seconds * 1000)))
                return collected[:max_shares]
            except PlaywrightTimeoutError:
                return []
            finally:
                context.close()
                browser.close()

    def _scrape_feed_with_scroll(
        self,
        handle: str,
        config: FacebookScrapeConfig,
    ) -> list[tuple[str, str]]:
        """Scroll feed page with Playwright to discover post URLs beyond initial render.

        Uses two discovery channels:
        1. DOM scraping: extracts URLs from rendered HTML after each scroll.
        2. XHR interception: captures permalink_url values from Facebook's
           GraphQL responses as the page lazy-loads feed content.
        """
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Playwright unavailable for scroll scraping") from exc

        url = f"{self.BASE_URL}/{quote(handle)}"
        all_post_urls: list[tuple[str, str]] = []
        seen: set[str] = set()

        # Collect permalink URLs from intercepted XHR responses.
        xhr_permalinks: list[str] = []

        def _on_response(response):
            try:
                if "/api/graphql" not in response.url:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct and "text" not in ct:
                    return
                body = response.text()
                for m in _PERMALINK_URL_JSON_RE.finditer(body):
                    raw = m.group(1).replace("\\/", "/")
                    if "facebook.com" in raw and raw not in seen:
                        xhr_permalinks.append(raw)
            except Exception:  # noqa: BLE001
                pass

        with sync_playwright() as playwright:
            browser, context = self._new_playwright_context(playwright)
            page = context.new_page()
            page.on("response", _on_response)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                if config.delay_seconds > 0:
                    page.wait_for_timeout(max(500, int(config.delay_seconds * 1000)))
                try:
                    page.keyboard.press("Escape")
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(500)

                stagnant_cycles = 0
                for _ in range(config.max_scroll_iterations):
                    html_text = page.content() or ""
                    candidates = self._extract_post_urls(html_text, handle=handle)

                    # Also drain XHR-intercepted permalinks into candidates.
                    while xhr_permalinks:
                        purl = xhr_permalinks.pop(0)
                        kind = "reel" if "/reel/" in purl else "photo" if "/photo" in purl else "feed"
                        candidates.append((purl, kind))

                    new_count = 0
                    for candidate_url, kind in candidates:
                        if candidate_url not in seen:
                            seen.add(candidate_url)
                            all_post_urls.append((candidate_url, kind))
                            new_count += 1

                    if new_count == 0:
                        stagnant_cycles += 1
                        if stagnant_cycles >= 3:
                            logger.info(
                                "[facebook] scroll stagnation after %d cycles, %d URLs discovered",
                                _ + 1,
                                len(all_post_urls),
                            )
                            break
                    else:
                        stagnant_cycles = 0

                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    if config.delay_seconds > 0:
                        page.wait_for_timeout(max(500, int(config.delay_seconds * 1000)))
            finally:
                context.close()
                browser.close()

        logger.info("[facebook] scroll scrape discovered %d post URLs for %s", len(all_post_urls), handle)
        return all_post_urls

    def _scrape_surface_with_scroll(
        self,
        surface_url: str,
        *,
        handle: str,
        config: FacebookScrapeConfig,
    ) -> list[tuple[str, str]]:
        """Render a surface page (reels, photos) with Playwright and scroll to discover post URLs.

        Facebook surface pages (e.g. /BravoTV/reels/) are SPA shells with
        ssrEnabled:false — the initial HTML contains no reel/video URLs.  This
        method launches Playwright, waits for client-side content to render,
        then scrolls to discover additional post URLs.
        """
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Playwright unavailable for surface scroll") from exc

        all_post_urls: list[tuple[str, str]] = []
        seen: set[str] = set()

        with sync_playwright() as playwright:
            browser, context = self._new_playwright_context(playwright)
            page = context.new_page()
            try:
                page.goto(surface_url, wait_until="domcontentloaded", timeout=45_000)
                # Wait longer than the standard fetch for SPA content to render.
                page.wait_for_timeout(3000)
                try:
                    page.keyboard.press("Escape")
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(500)

                max_scroll = min(config.max_scroll_iterations, 15)
                stagnant_cycles = 0
                for cycle in range(max_scroll):
                    html_text = page.content() or ""
                    candidates = self._extract_post_urls(html_text, handle=handle)
                    new_count = 0
                    for candidate_url, kind in candidates:
                        if candidate_url not in seen:
                            seen.add(candidate_url)
                            all_post_urls.append((candidate_url, kind))
                            new_count += 1

                    if new_count == 0:
                        stagnant_cycles += 1
                        if stagnant_cycles >= 3:
                            logger.info(
                                "[facebook] surface scroll stagnation after %d cycles, %d URLs",
                                cycle + 1,
                                len(all_post_urls),
                            )
                            break
                    else:
                        stagnant_cycles = 0

                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(max(1000, int(config.delay_seconds * 1000)))
            finally:
                context.close()
                browser.close()

        logger.info("[facebook] surface scroll discovered %d post URLs from %s", len(all_post_urls), surface_url)
        return all_post_urls

    @staticmethod
    def _has_primary_post_signals(html_text: str) -> bool:
        if not html_text:
            return False
        return bool(
            _OG_TITLE_RE.search(html_text)
            or _OG_DESC_RE.search(html_text)
            or _PUBLISHED_TIME_RE.search(html_text)
            or _FB_CREATION_TIME_RE.search(html_text)
            or _FB_MESSAGE_TEXT_RE.search(html_text)
            or _FB_FEEDBACK_BLOCK_RE.search(html_text)
            or FacebookScraper._FB_OWNER_PAGE_NAME_RE.search(html_text)
            or FacebookScraper._FB_OWNER_NAME_RE.search(html_text)
        )

    @staticmethod
    def _first_group(pattern: re.Pattern[str], text: str) -> str:
        match = pattern.search(text)
        if not match:
            return ""
        return html.unescape(str(match.group(1) or "")).strip()

    @staticmethod
    def _coerce_engagement_count(raw: object) -> int:
        if raw is None:
            return 0
        if isinstance(raw, bool):
            return int(raw)
        if isinstance(raw, dict):
            for key in ("count", "count_reduced", "total_count", "value"):
                if key in raw:
                    value = FacebookScraper._coerce_engagement_count(raw.get(key))
                    if value:
                        return value
            return 0
        if isinstance(raw, int):
            return max(raw, 0)
        if isinstance(raw, float):
            return int(max(raw, 0))

        value = html.unescape(str(raw)).strip().replace(",", "")
        if not value:
            return 0

        match = re.match(r"([0-9]+(?:\.[0-9]+)?)([KkMmBb]?)", value)
        if not match:
            return 0

        amount = float(match.group(1))
        suffix = match.group(2).upper()
        if suffix == "K":
            amount *= 1_000
        elif suffix == "M":
            amount *= 1_000_000
        elif suffix == "B":
            amount *= 1_000_000_000
        return int(amount)

    @staticmethod
    def _extract_reactions_from_edges(edges_text: str) -> dict[str, int]:
        if not edges_text:
            return {}

        reactions: dict[str, int] = {}

        try:
            edges = json.loads(f"[{edges_text}]")
        except Exception:  # noqa: BLE001
            edges = None

        if isinstance(edges, list):
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                node = edge.get("node")
                name = ""
                if isinstance(node, dict):
                    name = str(node.get("localized_name") or "").strip()
                    if not name:
                        name = str(node.get("name") or "").strip()
                if not name:
                    name = str(edge.get("localized_name") or "").strip()
                if not name:
                    name = str(edge.get("name") or "").strip()
                if not name:
                    continue
                count = FacebookScraper._coerce_engagement_count(
                    edge.get("reaction_count") if edge.get("reaction_count") is not None else edge.get("count")
                )
                if not count:
                    count = FacebookScraper._coerce_engagement_count(edge.get("i18n_reaction_count"))
                if not count:
                    count = FacebookScraper._coerce_engagement_count(
                        edge.get("reaction_count_count") if edge.get("reaction_count_count") is not None else None
                    )
                if count:
                    reactions[name] = count
            if reactions:
                return reactions

        if reactions:
            return reactions

        for edge_match in _FB_REACTION_EDGE_RE.finditer(edges_text):
            name = edge_match.group(1)
            count = FacebookScraper._coerce_engagement_count(edge_match.group(2))
            if name and count:
                reactions[name] = count

        return reactions

    @staticmethod
    def _to_epoch(value: str) -> int | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return int(dt.timestamp())
        except ValueError:
            return None

    def _surface_urls(self, handle: str, config: FacebookScrapeConfig) -> list[str]:
        base = f"{self.BASE_URL}/{quote(handle)}"
        urls: list[str] = []
        if config.include_feed:
            urls.append(base)
        if config.include_reels:
            urls.append(f"{base}/reels")
        if config.include_photos:
            urls.append(f"{base}/photos")
        return urls

    def _extract_post_urls(self, page_html: str, *, handle: str) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for match in _REEL_URL_RE.finditer(page_html):
            source = f"{self.BASE_URL}/reel/{match.group(1)}"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "reel"))
        for match in _PAGE_POST_URL_RE.finditer(page_html):
            source = f"{self.BASE_URL}/{match.group(1)}/posts/{match.group(2)}"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "feed"))
        for match in _GROUP_POST_URL_RE.finditer(page_html):
            source = f"{self.BASE_URL}/groups/{match.group(1)}/posts/{match.group(2)}"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "feed"))
        for match in _RELATIVE_REEL_HREF_RE.finditer(page_html):
            source = f"{self.BASE_URL}/reel/{match.group(2)}"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "reel"))
        for match in _RELATIVE_VIDEO_HREF_RE.finditer(page_html):
            source = f"{self.BASE_URL}{match.group(1)}"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "reel"))
        for match in _RELATIVE_GROUP_POST_HREF_RE.finditer(page_html):
            source = f"{self.BASE_URL}{match.group(1)}"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "feed"))
        # Photo posts: /photo/?fbid=NNN — Facebook embeds page-timeline photos
        # as /photo/?fbid=NNN&set=pb.PAGE_ID... links in the scroll DOM.
        for match in _PHOTO_FBID_HREF_RE.finditer(page_html):
            fbid = match.group(2)
            source = f"{self.BASE_URL}/photo/?fbid={fbid}"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "photo"))
        # Share URLs: facebook.com/share/v/... , .../share/p/... , .../share/r/...
        for match in _SHARE_URL_RE.finditer(page_html):
            source = match.group(0)
            if source in seen:
                continue
            seen.add(source)
            # Determine type: v/ → video/reel, r/ → reel, p/ → feed post
            segment = source.rsplit("/", 2)[-2] if source.count("/") >= 2 else ""
            post_type = "reel" if segment in ("v", "r") else "feed"
            pairs.append((source, post_type))
        # Direct video URLs: facebook.com/PAGE/videos/NNN
        for match in _VIDEO_URL_RE.finditer(page_html):
            source = match.group(0)
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "reel"))
        # Inline JSON permalink_url: Facebook embeds post permalinks in script
        # data that are not visible as <a href> but contain real post URLs.
        for match in _PERMALINK_URL_JSON_RE.finditer(page_html):
            raw = match.group(1).replace("\\/", "/")
            if "facebook.com" not in raw:
                continue
            post_type = "reel" if "/reel/" in raw else "photo" if "/photo" in raw else "feed"
            if raw not in seen:
                seen.add(raw)
                pairs.append((raw, post_type))
        if not pairs:
            for og_match in _OG_URL_RE.finditer(page_html):
                candidate = str(og_match.group(1) or "").strip()
                if "facebook.com" not in candidate:
                    continue
                post_type = "reel" if "/reel/" in candidate else "photo" if "/photos" in candidate else "feed"
                if candidate not in seen:
                    seen.add(candidate)
                    pairs.append((candidate, post_type))
        return pairs

    @staticmethod
    def _extract_engagement(html_text: str) -> dict[str, Any]:
        """Extract engagement metrics from Facebook SSR JSON embedded in HTML.

        Facebook embeds structured JSON data in its server-rendered HTML that
        contains engagement counts: reaction totals, comment counts, view/play
        counts, share counts, and per-reaction breakdowns.

        In authenticated responses, these metrics are often split across
        multiple feedback blocks rather than one combined block, so we first
        try the local context around the comment_rendering_instance block,
        then fall back to full-HTML scanning for any metrics still at zero.
        """
        engagement: dict[str, Any] = {
            "reaction_count": 0,
            "comment_count": 0,
            "share_count": 0,
            "view_count": 0,
            "play_count": 0,
            "reactions": {},
        }

        # Find the primary feedback block (the one with comment_rendering_instance)
        fb_match = _FB_FEEDBACK_BLOCK_RE.search(html_text)
        if fb_match:
            engagement["comment_count"] = int(fb_match.group(1))
            # Extract the broader context around this feedback block for related fields
            ctx_start = fb_match.start()
            ctx_end = min(len(html_text), ctx_start + 5000)
            ctx = html_text[ctx_start:ctx_end]

            rc_match = _FB_REACTION_COUNT_RE.search(ctx)
            if rc_match:
                engagement["reaction_count"] = FacebookScraper._coerce_engagement_count(rc_match.group(1))
            elif rc_match_alt := _FB_REACTION_I18N_RE.search(ctx):
                engagement["reaction_count"] = FacebookScraper._coerce_engagement_count(rc_match_alt.group(1))

            vc_match = _FB_VIDEO_VIEW_COUNT_RE.search(ctx)
            if vc_match:
                engagement["view_count"] = FacebookScraper._coerce_engagement_count(vc_match.group(1))
            else:
                vc_match = _FB_VIDEO_VIEW_COUNT_REDUCED_RE.search(ctx)
                if vc_match:
                    engagement["view_count"] = FacebookScraper._coerce_engagement_count(vc_match.group(1))

            pc_match = _FB_PLAY_COUNT_RE.search(ctx)
            if pc_match:
                engagement["play_count"] = FacebookScraper._coerce_engagement_count(pc_match.group(1))
            else:
                pc_match = _FB_PLAY_COUNT_REDUCED_RE.search(ctx)
                if pc_match:
                    engagement["play_count"] = FacebookScraper._coerce_engagement_count(pc_match.group(1))

            sc_match = _FB_SHARE_COUNT_RE.search(ctx)
            if sc_match:
                engagement["share_count"] = FacebookScraper._coerce_engagement_count(sc_match.group(1))
            else:
                sc_match = _FB_SHARE_COUNT_REDUCED_RE.search(ctx)
                if sc_match:
                    engagement["share_count"] = FacebookScraper._coerce_engagement_count(sc_match.group(1))

            rsc_match = _FB_RESHARE_COUNT_RE.search(ctx)
            if rsc_match and engagement["share_count"] == 0:
                engagement["share_count"] = FacebookScraper._coerce_engagement_count(rsc_match.group(1))
            elif engagement["share_count"] == 0:
                rsc_match = _FB_RESHARE_COUNT_REDUCED_RE.search(ctx)
                if rsc_match:
                    engagement["share_count"] = FacebookScraper._coerce_engagement_count(rsc_match.group(1))

            # Extract per-reaction breakdown (Like, Love, Haha, Wow, Sad, Angry, Care)
            tr_match = _FB_TOP_REACTIONS_RE.search(ctx)
            if tr_match:
                engagement["reactions"].update(FacebookScraper._extract_reactions_from_edges(tr_match.group(1)))

        # Fall back to full-HTML scanning for any metrics still at zero.
        # Authenticated responses split feedback data across multiple blocks,
        # so the 5000-char window above may miss some metrics.
        if engagement["comment_count"] == 0:
            tc_match = _FB_TOTAL_COMMENT_COUNT_RE.search(html_text)
            if tc_match:
                engagement["comment_count"] = FacebookScraper._coerce_engagement_count(tc_match.group(1))
            if engagement["comment_count"] == 0:
                tr_match = _FB_TOTAL_COMMENT_COUNT_REDUCED_RE.search(html_text)
                if tr_match:
                    engagement["comment_count"] = FacebookScraper._coerce_engagement_count(tr_match.group(1))
            if engagement["comment_count"] == 0:
                cc_match = _FB_COMMENT_COUNT_RE.search(html_text)
                if cc_match:
                    engagement["comment_count"] = FacebookScraper._coerce_engagement_count(cc_match.group(1))
            if engagement["comment_count"] == 0:
                fb2 = _FB_FEEDBACK_BLOCK_RE.search(html_text)
                if fb2:
                    engagement["comment_count"] = int(fb2.group(1))

        if engagement["reaction_count"] == 0:
            rc_match = _FB_REACTION_COUNT_RE.search(html_text)
            if rc_match:
                engagement["reaction_count"] = FacebookScraper._coerce_engagement_count(rc_match.group(1))
            else:
                rc_match_alt = _FB_REACTION_I18N_RE.search(html_text)
                if rc_match_alt:
                    engagement["reaction_count"] = FacebookScraper._coerce_engagement_count(rc_match_alt.group(1))
        # 2026-03: Facebook replaced reaction_count with unified_reactors.count
        if engagement["reaction_count"] == 0:
            ur_match = _FB_UNIFIED_REACTORS_COUNT_RE.search(html_text)
            if ur_match:
                engagement["reaction_count"] = FacebookScraper._coerce_engagement_count(ur_match.group(1))

        if engagement["view_count"] == 0:
            vc_match = _FB_VIDEO_VIEW_COUNT_RE.search(html_text)
            if vc_match:
                engagement["view_count"] = FacebookScraper._coerce_engagement_count(vc_match.group(1))
            else:
                vc_match = _FB_VIDEO_VIEW_COUNT_REDUCED_RE.search(html_text)
                if vc_match:
                    engagement["view_count"] = FacebookScraper._coerce_engagement_count(vc_match.group(1))

        if engagement["play_count"] == 0:
            pc_match = _FB_PLAY_COUNT_RE.search(html_text)
            if pc_match:
                engagement["play_count"] = FacebookScraper._coerce_engagement_count(pc_match.group(1))
            else:
                pc_match = _FB_PLAY_COUNT_REDUCED_RE.search(html_text)
                if pc_match:
                    engagement["play_count"] = FacebookScraper._coerce_engagement_count(pc_match.group(1))

        # 2026-03: SPA-rendered pages embed views in og:title as "69K views · ..."
        if engagement["view_count"] == 0 and engagement["play_count"] == 0:
            og_title_match = _OG_TITLE_RE.search(html_text)
            if og_title_match:
                title_views = _FB_OG_TITLE_VIEWS_RE.search(og_title_match.group(1))
                if title_views:
                    engagement["view_count"] = FacebookScraper._coerce_engagement_count(title_views.group(1))

        if engagement["share_count"] == 0:
            sc_match = _FB_SHARE_COUNT_RE.search(html_text)
            if sc_match:
                engagement["share_count"] = FacebookScraper._coerce_engagement_count(sc_match.group(1))
            else:
                sc_match = _FB_SHARE_COUNT_REDUCED_RE.search(html_text)
                if sc_match:
                    engagement["share_count"] = FacebookScraper._coerce_engagement_count(sc_match.group(1))
        # 2026-03: Facebook now emits flat "share_count_reduced":"N" in feedback
        if engagement["share_count"] == 0:
            fsc_match = _FB_FLAT_SHARE_COUNT_REDUCED_RE.search(html_text)
            if fsc_match:
                engagement["share_count"] = FacebookScraper._coerce_engagement_count(fsc_match.group(1))

        if not engagement["reactions"]:
            # In authenticated responses, the post's top_reactions may not be
            # the first match in the HTML (comment reactions appear first but
            # lack localized_name).  Search near the reaction_count match
            # for the post-level top_reactions block.
            rc_match = _FB_REACTION_COUNT_RE.search(html_text)
            if rc_match:
                ctx_start = max(0, rc_match.start() - 200)
                ctx_end = min(len(html_text), rc_match.end() + 5000)
                ctx = html_text[ctx_start:ctx_end]
                tr_match = _FB_TOP_REACTIONS_RE.search(ctx)
                if tr_match:
                    engagement["reactions"].update(FacebookScraper._extract_reactions_from_edges(tr_match.group(1)))
            # Final fallback: try all top_reactions blocks until one has localized_name
            if not engagement["reactions"]:
                for tr_match in _FB_TOP_REACTIONS_RE.finditer(html_text):
                    engagement["reactions"].update(FacebookScraper._extract_reactions_from_edges(tr_match.group(1)))
                    if engagement["reactions"]:
                        break

        return engagement

    def _build_post_from_html(self, *, url: str, html_text: str, username: str, post_type_hint: str) -> FacebookPost:
        og_url = self._first_group(_OG_URL_RE, html_text) or url
        post_id = self._first_group(_POST_ID_RE, html_text)

        # In authenticated SPA responses, OG tags are absent. Fall back to
        # JSON-embedded permalink_url and message text.
        permalink_match = _FB_PERMALINK_URL_RE.search(html_text)
        if permalink_match and not self._first_group(_OG_URL_RE, html_text):
            raw_permalink = permalink_match.group(1).replace("\\/", "/")
            og_url = raw_permalink

        if not post_id:
            parsed = urlparse(og_url)
            pieces = [piece for piece in parsed.path.split("/") if piece]
            if pieces and pieces[-1].isdigit():
                post_id = pieces[-1]
            else:
                # Handle /photo/?fbid=NNN query-string IDs.
                qs = parse_qs(parsed.query)
                fbid_vals = qs.get("fbid", [])
                if fbid_vals and fbid_vals[0].isdigit():
                    post_id = fbid_vals[0]
                else:
                    post_id = parsed.path.strip("/") or _deterministic_fallback_post_id(og_url or url)
        title = self._first_group(_OG_TITLE_RE, html_text)
        desc = self._first_group(_OG_DESC_RE, html_text)
        image_url = self._first_group(_OG_IMAGE_RE, html_text)
        published_iso = self._first_group(_PUBLISHED_TIME_RE, html_text)
        posted_at = self._to_epoch(published_iso)
        # Fallback: extract creation_time from Facebook's inline JSON data.
        # Facebook rarely emits the article:published_time meta tag, but its
        # script payloads almost always contain "creation_time":<unix_epoch>.
        creation_time_epoch: int | None = None
        if posted_at is None:
            ct_match = _FB_CREATION_TIME_RE.search(html_text)
            if ct_match:
                creation_time_epoch = int(ct_match.group(1))
                posted_at = creation_time_epoch
        caption = desc or title

        # Authenticated SPA fallback: extract caption from JSON message text
        if not caption:
            msg_match = _FB_MESSAGE_TEXT_RE.search(html_text)
            if msg_match:
                raw_msg = msg_match.group(1)
                try:
                    caption = html.unescape(json.loads(f'"{raw_msg}"'))
                except Exception:  # noqa: BLE001
                    caption = html.unescape(raw_msg.replace("\\n", "\n"))

        media_urls = [image_url] if image_url else []
        post_type = post_type_hint
        if "/reel/" in og_url or "/videos/" in og_url:
            post_type = "reel"
        elif "/photos" in og_url:
            post_type = "photo"

        engagement = self._extract_engagement(html_text)
        user_avatar_url = self._extract_owner_avatar_url(html_text)
        duration_seconds = self._extract_duration_seconds(html_text)
        media_provenance = FacebookMediaProvenance(
            platform="facebook",
            matched_by="native",
            fallback_used=False,
            source_url=og_url,
            candidate_urls=[],
            attempts=[],
        )

        return FacebookPost(
            post_id=post_id,
            username=username,
            post_type=post_type or "feed",
            caption=caption,
            media_urls=media_urls,
            thumbnail_url=image_url or None,
            user_avatar_url=user_avatar_url,
            duration_seconds=duration_seconds,
            likes=engagement["reaction_count"],
            comments=engagement["comment_count"],
            shares=engagement["share_count"],
            views=engagement["view_count"] or engagement["play_count"],
            posted_at=posted_at,
            url=og_url,
            reactions=engagement["reactions"],
            media_provenance=media_provenance,
            raw_data={
                "og_url": og_url,
                "og_title": title,
                "og_description": desc,
                "published_time": published_iso or None,
                "creation_time_epoch": creation_time_epoch,
                "source": "public_ssr_engagement",
                "play_count": engagement["play_count"],
                "video_view_count": engagement["view_count"],
                "user_avatar_url": user_avatar_url,
                "duration_seconds": duration_seconds,
                "media_provenance": media_provenance.to_dict(),
            },
        )

    # Regex for extracting owner/page name from SSR JSON.
    # owner_as_page is preferred (page name), then owner with name field.
    _FB_OWNER_PAGE_NAME_RE = re.compile(r'"owner_as_page":\{[^}]*"name":"((?:[^"\\]|\\.)*)"')
    _FB_OWNER_NAME_RE = re.compile(r'"owner":\{[^}]*"name":"((?:[^"\\]|\\.)*)"')

    @staticmethod
    def _avatar_quality_score(url: str) -> tuple[int, int]:
        normalized = str(url or "").strip().lower()
        if not normalized:
            return (-1, -1)
        score = 0
        if any(token in normalized for token in ("profile_picture", "profile_pic", "profile_image")):
            score += 25
        if any(
            token in normalized for token in ("thumb", "thumbnail", "small", "tiny", "s50x50", "s96x96", "s160x160")
        ):
            score -= 20
        max_dim = 0
        for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
            max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
        return (score + min(max_dim, 4096), len(normalized))

    def _extract_owner_avatar_url(self, html_text: str) -> str | None:
        candidates: list[str] = []
        for pattern in (_FB_OWNER_PROFILE_PICTURE_URI_RE, _FB_OWNER_PROFILE_PIC_URL_RE, _FB_PROFILE_PICTURE_URI_RE):
            for match in pattern.finditer(html_text):
                raw_value = str(match.group(1) or "").strip()
                if not raw_value:
                    continue
                try:
                    candidate = str(json.loads(f'"{raw_value}"') or "").strip()
                except Exception:  # noqa: BLE001
                    candidate = raw_value.replace("\\/", "/").strip()
                if candidate.startswith(("http://", "https://")):
                    candidates.append(candidate)

        best: str | None = None
        best_score: tuple[int, int] = (-1, -1)
        for candidate in candidates:
            score = self._avatar_quality_score(candidate)
            if score > best_score:
                best_score = score
                best = candidate
        return best

    def scrape_post(
        self,
        post_url: str,
        *,
        delay_seconds: float = 1.25,
        fetch_comment_list: bool = False,
        max_comments: int = 100,
        fetch_share_list: bool = False,
        max_shares: int = 100,
        allow_cross_platform_media_fallback: bool = True,
        fast_mode: bool = False,
    ) -> tuple[FacebookPost | None, list[FacebookComment]]:
        """Scrape a single Facebook post URL for engagement metrics and comments.

        Supports standard post URLs, reel URLs, video URLs, and /share/v/ short
        links.  Returns (post, comments) where post includes full engagement
        data (reactions, comment count, share count, view count) and comments
        is populated when *fetch_comment_list* is True.
        """
        try:
            html_text = self._fetch_html(post_url, delay_seconds=delay_seconds, fast_mode=fast_mode)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[facebook] scrape_post failed for %s: %s", post_url, exc)
            return None, []

        # Resolve username: prefer owner_as_page name, then owner name, fall back to URL
        og_url = self._first_group(_OG_URL_RE, html_text) or post_url
        owner_match = self._FB_OWNER_PAGE_NAME_RE.search(html_text) or self._FB_OWNER_NAME_RE.search(html_text)
        if owner_match:
            username = html.unescape(owner_match.group(1))
        else:
            parsed = urlparse(og_url)
            path_parts = [p for p in parsed.path.split("/") if p and p not in ("reel", "share", "v", "p", "r")]
            username = path_parts[0] if path_parts else ""

        # Determine post type from resolved URL
        post_type = "feed"
        if "/reel/" in og_url:
            post_type = "reel"
        elif "/videos/" in og_url:
            post_type = "reel"
        elif "/photos" in og_url:
            post_type = "photo"

        post = self._build_post_from_html(
            url=post_url,
            html_text=html_text,
            username=username,
            post_type_hint=post_type,
        )
        self._resolve_cross_platform_media_fallback(
            post=post,
            html_text=html_text,
            allow_fallback=allow_cross_platform_media_fallback,
        )

        comments: list[FacebookComment] = []
        if fetch_comment_list:
            comments = self._extract_comments_from_ssr(html_text, max_comments=max_comments)
        if fetch_share_list:
            post.share_details = self._scrape_share_details(
                post.url or post_url,
                max_shares=max_shares,
                delay_seconds=delay_seconds,
            )
            post.raw_data["share_details"] = [share.to_dict() for share in post.share_details]

        return post, comments

    def search_posts(self, config: FacebookSearchConfig) -> list[FacebookPost]:
        if not config.normalized_query:
            return []

        candidate_urls = self._discover_search_post_urls(config)
        posts: list[FacebookPost] = []
        seen_ids: set[str] = set()
        checked = 0
        for candidate_url in candidate_urls:
            checked += 1
            post, comments = self.scrape_post(
                candidate_url,
                delay_seconds=config.delay_seconds,
                fetch_comment_list=config.include_comments,
                max_comments=config.max_comments,
                fetch_share_list=config.include_share_details,
                max_shares=config.max_shares,
                allow_cross_platform_media_fallback=config.allow_cross_platform_media_fallback,
            )
            if post is None or not post.post_id or post.post_id in seen_ids:
                continue
            posted_dt = datetime.fromtimestamp(post.posted_at, tz=UTC) if isinstance(post.posted_at, int) else None
            if not config.in_date_window(posted_dt):
                continue
            if comments:
                post.raw_data["comments_preview"] = [comment.to_dict() for comment in comments]
            seen_ids.add(post.post_id)
            posts.append(post)
            if len(posts) >= max(1, config.max_posts):
                break

        self.last_retrieval_meta = {
            **dict(self.last_retrieval_meta or {}),
            "query": config.normalized_query,
            "posts_checked": checked,
            "matched_posts": len(posts),
            "include_share_details": config.include_share_details,
            "include_comments": config.include_comments,
        }
        return posts

    def scrape(
        self,
        config: FacebookScrapeConfig,
        *,
        progress_cb: Any | None = None,
    ) -> list[FacebookPost]:
        handle = config.normalized_handle
        if not handle:
            return []

        posts: list[FacebookPost] = []
        seen_ids: set[str] = set()
        pages_scanned = 0
        posts_checked = 0
        matched_posts = 0
        surface_fetch_failures = 0
        candidate_fetch_failures = 0
        scrape_start = time.monotonic()
        timed_out = False

        def _check_timeout() -> bool:
            nonlocal timed_out
            if time.monotonic() - scrape_start > config.max_scrape_seconds:
                timed_out = True
                logger.warning(
                    "[facebook] scrape timed out after %.0fs (limit: %.0fs)",
                    time.monotonic() - scrape_start,
                    config.max_scrape_seconds,
                )
            return timed_out

        # When a date window is specified and Playwright is available, use
        # scroll-based pagination on the feed to reach older posts.
        feed_candidates_from_scroll: list[tuple[str, str]] | None = None
        if config.include_feed and config.date_start is not None and self._playwright_fallback_enabled():
            try:
                feed_candidates_from_scroll = self._scrape_feed_with_scroll(handle, config)
                pages_scanned += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("[facebook] scroll scrape failed for %s: %s — falling back to static fetch", handle, exc)
                surface_fetch_failures += 1

        # Process scroll-discovered feed candidates first.
        if feed_candidates_from_scroll:
            feed_url = f"{self.BASE_URL}/{handle}"
            for candidate_url, kind in feed_candidates_from_scroll:
                if _check_timeout():
                    break
                posts_checked += 1
                try:
                    post_html = self._fetch_html(
                        candidate_url, delay_seconds=config.delay_seconds, referer=feed_url, fast_mode=config.fast_mode
                    )
                except Exception:  # noqa: BLE001
                    candidate_fetch_failures += 1
                    continue
                if (
                    (config.date_start is not None or config.date_end is not None)
                    and not self._has_primary_post_signals(post_html)
                    and self._playwright_fallback_enabled()
                ):
                    try:
                        post_html = self._fetch_html_with_playwright(
                            candidate_url,
                            delay_seconds=config.delay_seconds,
                            referer=feed_url,
                        )
                    except Exception:  # noqa: BLE001
                        continue
                post = self._build_post_from_html(
                    url=candidate_url,
                    html_text=post_html,
                    username=handle,
                    post_type_hint=kind,
                )
                # Playwright enrichment: Facebook SSR omits view/play counts for
                # video content.  Re-fetch with SPA rendering to extract from og:title.
                if (
                    post.views == 0
                    and post.likes > 0
                    and (
                        "/reel/" in candidate_url or "/videos/" in candidate_url or post.post_type in ("reel", "video")
                    )
                    and self._playwright_fallback_enabled()
                ):
                    try:
                        pw_html = self._fetch_html_with_playwright(
                            candidate_url,
                            delay_seconds=config.delay_seconds,
                            referer=feed_url,
                            wait_for_spa=True,
                            skip_cookies=True,
                        )
                        pw_eng = self._extract_engagement(pw_html)
                        enriched_views = pw_eng["view_count"] or pw_eng["play_count"]
                        if enriched_views:
                            post.views = enriched_views
                            if post.raw_data:
                                post.raw_data["video_view_count"] = pw_eng["view_count"]
                                post.raw_data["play_count"] = pw_eng["play_count"]
                                post.raw_data["views_enriched_via_playwright"] = True
                    except Exception:  # noqa: BLE001
                        pass
                if not post.post_id or post.post_id in seen_ids:
                    continue
                posted_dt = datetime.fromtimestamp(post.posted_at, tz=UTC) if isinstance(post.posted_at, int) else None
                if not config.in_date_window(posted_dt):
                    continue
                seen_ids.add(post.post_id)
                matched_posts += 1
                posts.append(post)
                if progress_cb:
                    progress_cb(
                        {
                            "phase": "scrape_posts",
                            "pages_scanned": pages_scanned,
                            "posts_checked": posts_checked,
                            "matched_posts": matched_posts,
                        }
                    )

        # Process remaining surfaces (reels, photos — and feed as fallback
        # if scroll was not attempted or failed).
        surface_urls = self._surface_urls(handle, config)
        if feed_candidates_from_scroll is not None:
            feed_url = f"{self.BASE_URL}/{handle}"
            surface_urls = [u for u in surface_urls if u != feed_url]

        for surface_url in surface_urls:
            if _check_timeout():
                break
            if config.max_pages is not None and pages_scanned >= max(1, int(config.max_pages)):
                break
            try:
                html_text = self._fetch_html(
                    surface_url, delay_seconds=config.delay_seconds, fast_mode=config.fast_mode
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("[facebook] failed to fetch %s: %s", surface_url, exc)
                surface_fetch_failures += 1
                continue
            pages_scanned += 1
            candidates = self._extract_post_urls(html_text, handle=handle)

            # Facebook surfaces like /reels/ and /photos serve SPA shells with
            # ssrEnabled:false — no content in the initial HTML.  When requests
            # returns an empty shell, fall back to Playwright scroll to discover
            # post URLs from the rendered DOM.
            if not candidates and self._playwright_fallback_enabled():
                try:
                    candidates = self._scrape_surface_with_scroll(
                        surface_url,
                        handle=handle,
                        config=config,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "[facebook] playwright surface scroll failed for %s: %s",
                        surface_url,
                        exc,
                    )

            for candidate_url, kind in candidates:
                if _check_timeout():
                    break
                if config.max_pages is not None and posts_checked >= max(1, int(config.max_pages)) * 100:
                    break
                posts_checked += 1
                try:
                    post_html = self._fetch_html(
                        candidate_url,
                        delay_seconds=config.delay_seconds,
                        referer=surface_url,
                        fast_mode=config.fast_mode,
                    )
                except Exception:  # noqa: BLE001
                    candidate_fetch_failures += 1
                    continue
                if (
                    (config.date_start is not None or config.date_end is not None)
                    and not self._has_primary_post_signals(post_html)
                    and self._playwright_fallback_enabled()
                ):
                    try:
                        post_html = self._fetch_html_with_playwright(
                            candidate_url,
                            delay_seconds=config.delay_seconds,
                            referer=surface_url,
                        )
                    except Exception:  # noqa: BLE001
                        continue
                post = self._build_post_from_html(
                    url=candidate_url,
                    html_text=post_html,
                    username=handle,
                    post_type_hint=kind,
                )
                # Playwright enrichment: Facebook SSR omits view/play counts for
                # video content.  Re-fetch with SPA rendering to extract from og:title.
                if (
                    post.views == 0
                    and post.likes > 0
                    and (
                        "/reel/" in candidate_url or "/videos/" in candidate_url or post.post_type in ("reel", "video")
                    )
                    and self._playwright_fallback_enabled()
                ):
                    try:
                        pw_html = self._fetch_html_with_playwright(
                            candidate_url,
                            delay_seconds=config.delay_seconds,
                            referer=surface_url,
                            wait_for_spa=True,
                            skip_cookies=True,
                        )
                        pw_eng = self._extract_engagement(pw_html)
                        enriched_views = pw_eng["view_count"] or pw_eng["play_count"]
                        if enriched_views:
                            post.views = enriched_views
                            if post.raw_data:
                                post.raw_data["video_view_count"] = pw_eng["view_count"]
                                post.raw_data["play_count"] = pw_eng["play_count"]
                                post.raw_data["views_enriched_via_playwright"] = True
                    except Exception:  # noqa: BLE001
                        pass  # keep post with 0 views rather than losing it
                if not post.post_id or post.post_id in seen_ids:
                    continue
                posted_dt = datetime.fromtimestamp(post.posted_at, tz=UTC) if isinstance(post.posted_at, int) else None
                if not config.in_date_window(posted_dt):
                    continue
                seen_ids.add(post.post_id)
                matched_posts += 1
                posts.append(post)
                if progress_cb:
                    progress_cb(
                        {
                            "phase": "scrape_posts",
                            "pages_scanned": pages_scanned,
                            "posts_checked": posts_checked,
                            "matched_posts": matched_posts,
                        }
                    )

        scrape_elapsed = time.monotonic() - scrape_start
        self.last_retrieval_meta = {
            "source": "scroll_and_static" if feed_candidates_from_scroll is not None else "public_meta_fallback",
            "pages_scanned": pages_scanned,
            "posts_checked": posts_checked,
            "matched_posts": matched_posts,
            "cookies_supplied": bool(self.cookies),
            "surface_fetch_failures": surface_fetch_failures,
            "candidate_fetch_failures": candidate_fetch_failures,
            "scrape_elapsed_seconds": round(scrape_elapsed, 1),
            "timed_out": timed_out,
        }
        if matched_posts == 0 and (surface_fetch_failures > 0 or candidate_fetch_failures > 0):
            self.last_retrieval_meta["error_code"] = "facebook_catalog_fetch_failed"
            self.last_retrieval_meta["retryable"] = True
            self.last_retrieval_meta["error_class"] = "FacebookCatalogFetchError"
        return posts

    # Regex for extracting comments from Facebook SSR HTML payloads
    _FB_COMMENT_FBID_RE = re.compile(r'"legacy_fbid":"(\d+)"')
    _FB_COMMENT_BODY_RE = re.compile(r'"body":\{"text":"((?:[^"\\]|\\.)*)"')
    _FB_COMMENT_AUTHOR_RE = re.compile(r'"author":\{[^}]*"name":"((?:[^"\\]|\\.)*)"')
    _FB_COMMENT_REACTORS_RE = re.compile(r'"reactors":\{"count_reduced":"(\d+)"')
    _FB_COMMENT_DEPTH_RE = re.compile(r'"depth":(\d+)')
    _FB_COMMENT_CREATED_RE = re.compile(r'"created_time":(\d{10})')

    def fetch_comments(
        self,
        post_url_or_id: str,
        *,
        max_comments: int = 0,
        fetch_replies: bool = True,
        delay_seconds: float = 1.25,
        fast_mode: bool = False,
    ) -> list[FacebookComment]:
        """Extract comments from Facebook post SSR HTML.

        Meta's SSR response includes a limited set of visible comments embedded
        in the page's JSON preloader data.  This method fetches the post page
        with browser-fingerprint headers and extracts whatever comments are
        present.  Without authenticated cookies, typically 2-5 comments are
        returned by Meta's server.
        """
        self.last_comment_fetch_reason = None
        self.comments_auth_failed = not bool(self.cookies)

        if max_comments == 0:
            return []

        post_url = post_url_or_id if post_url_or_id.startswith("http") else f"{self.BASE_URL}/reel/{post_url_or_id}"

        try:
            html_text = self._fetch_html(post_url, delay_seconds=delay_seconds, fast_mode=fast_mode)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[facebook] comment fetch failed for %s: %s", post_url, exc)
            self.last_comment_fetch_reason = "facebook_fetch_failed"
            return []

        comments = self._extract_comments_from_ssr(html_text, max_comments=max_comments)
        self.last_comment_fetch_reason = "facebook_ssr_comments_ok" if comments else "facebook_no_ssr_comments"
        return comments

    def _extract_comments_from_ssr(self, html_text: str, *, max_comments: int = 100) -> list[FacebookComment]:
        """Parse comment data from Facebook SSR HTML preloader JSON."""
        comments: list[FacebookComment] = []
        seen_ids: set[str] = set()

        for m in self._FB_COMMENT_FBID_RE.finditer(html_text):
            if len(comments) >= max_comments:
                break

            fbid = m.group(1)
            if fbid in seen_ids:
                continue

            # Grab surrounding context (comment block)
            start = max(0, m.start() - 1000)
            end = min(len(html_text), m.end() + 3000)
            ctx = html_text[start:end]

            # Must have a body.text to be a real comment (not just a post or UI element)
            body_match = self._FB_COMMENT_BODY_RE.search(ctx)
            if not body_match:
                continue

            seen_ids.add(fbid)
            raw_body = body_match.group(1)
            try:
                # Decode JSON unicode escapes (\uXXXX) including surrogate pairs
                body_text = html.unescape(json.loads(f'"{raw_body}"'))
            except Exception:  # noqa: BLE001
                body_text = html.unescape(raw_body.replace("\\n", "\n"))

            author_match = self._FB_COMMENT_AUTHOR_RE.search(ctx)
            author = html.unescape(author_match.group(1)) if author_match else ""

            reactors_match = self._FB_COMMENT_REACTORS_RE.search(ctx)
            likes = int(reactors_match.group(1)) if reactors_match else 0

            depth_match = self._FB_COMMENT_DEPTH_RE.search(ctx)
            depth = int(depth_match.group(1)) if depth_match else 0

            created_match = self._FB_COMMENT_CREATED_RE.search(ctx)
            created_at = int(created_match.group(1)) if created_match else None

            comments.append(
                FacebookComment(
                    comment_id=fbid,
                    username=author,
                    text=body_text,
                    likes=likes,
                    created_at=created_at,
                    is_reply=(depth > 0),
                    reply_count=0,
                )
            )

        return comments
