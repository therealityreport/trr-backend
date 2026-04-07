"""
Instagram scraper module for fetching posts from public profiles.

Supports:
- Fetching posts from any public Instagram profile
- Filtering by hashtags (case-insensitive)
- Filtering by date range
- Both authenticated (full pagination) and public (limited) modes
"""

import json
import logging
import math
import os
import random
import re
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager
from trr_backend.socials.instagram.permalink_metadata import _shortcode_to_media_id as permalink_shortcode_to_media_id

logger = logging.getLogger(__name__)
_INSTAGRAM_BROWSER_SESSIONS = AccountBrowserSessionManager(
    platform="instagram",
    cookie_domains=(".instagram.com",),
)


@dataclass
class ScrapeConfig:
    """Configuration for an Instagram scrape operation."""

    username: str
    hashtags: list[str] = field(default_factory=list)
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 2.0
    max_pages: int | None = None  # None = no limit
    no_match_page_limit: int | None = None  # None = use scraper default/env

    # Performance tuning
    fast_mode: bool = False
    """When True, uses aggressive rate-limiting tiers, larger page size, reduced
    context warming, disabled browser fallback, and fewer retry attempts."""

    scrape_mode: str = "graphql"
    """Scraping strategy: 'graphql' (default, direct API), 'browser_intercept'
    (headless Playwright scroll + response interception, Sort-Feed style),
    or 'auto' (graphql first, browser_intercept fallback)."""

    fetch_comment_replies: bool = True
    """When False, only fetch top-level comments and skip reply chains.
    Useful for bulk scrapes where replies aren't needed immediately."""

    max_scrape_seconds: float = 420.0
    """Overall wall-clock timeout for the entire scrape() call (default: 7 min)."""

    require_auth: bool = False
    """When True, scrape() returns early with an error meta if cookies are
    missing or invalid instead of silently falling back to profile_info mode."""

    # Metadata for tracking
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def __post_init__(self):
        """Apply fast_mode overrides when enabled."""
        if self.fast_mode:
            # Use a lower base delay unless explicitly overridden
            if self.delay_seconds == 2.0:  # Only override if at default
                self.delay_seconds = 0.5
            logger.info(
                "ScrapeConfig fast_mode enabled: delay=%.2fs, scrape_mode=%s, comment_replies=%s",
                self.delay_seconds,
                self.scrape_mode,
                self.fetch_comment_replies,
            )

    @property
    def start_timestamp(self) -> float:
        return self.date_start.timestamp() if self.date_start else 0

    @property
    def end_timestamp(self) -> float:
        return self.date_end.timestamp() if self.date_end else datetime.now(UTC).timestamp()

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
class InstagramUserDetail:
    """Rich user object extracted from tagged users, collaborators, and post owners."""

    username: str
    user_id: str | None = None
    full_name: str | None = None
    is_verified: bool | None = None
    profile_pic_url: str | None = None
    profile_pic_url_hd: str | None = None
    tag_x: float | None = None
    tag_y: float | None = None
    tag_position_source: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class InstagramComment:
    """Represents a single Instagram comment with reply support."""

    comment_id: str
    text: str
    username: str
    user_id: str
    created_at: int
    date_time: str
    likes: int
    is_reply: bool
    parent_comment_id: str | None  # ID of parent comment if this is a reply
    reply_count: int
    replies: list["InstagramComment"] = field(default_factory=list)
    media_urls: list[str] = field(default_factory=list)
    owner_profile_pic_url: str | None = None
    owner_is_verified: bool | None = None

    # Post reference
    post_shortcode: str = ""
    post_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested replies
        result["replies"] = [r.to_dict() if hasattr(r, "to_dict") else r for r in self.replies]
        return result


@dataclass
class InstagramPost:
    """Represents a single Instagram post with extracted data."""

    shortcode: str
    post_type: str  # image, video, carousel, reel
    date_time: str
    taken_at: int
    caption: str
    profile_tags: list[str]
    sponsored: bool
    likes: int
    comments: int
    video_views: int
    url: str
    pk: str
    username: str
    video_views_observed: int | None = None
    video_views_source: str | None = None
    video_views_raw_candidates: list[dict[str, Any]] = field(default_factory=list)

    # Media URLs
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    hashtags: list[str] = field(default_factory=list)
    mentions: list[str] = field(default_factory=list)
    collaborators: list[str] = field(default_factory=list)

    # Comments (populated when fetch_comments is called)
    comment_list: list[InstagramComment] = field(default_factory=list)

    # Optional tracking metadata
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    # Rich user detail objects
    tagged_users_detail: list[InstagramUserDetail] = field(default_factory=list)
    collaborators_detail: list[InstagramUserDetail] = field(default_factory=list)
    owner_detail: InstagramUserDetail | None = None

    # Additional metadata
    product_type: str | None = None
    video_play_count: int | None = None
    alt_text: str | None = None
    width: int | None = None
    height: int | None = None
    is_comments_disabled: bool | None = None
    music_info: dict[str, Any] | None = None
    audio_url: str | None = None
    video_duration: float | None = None
    child_posts_data: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested comments
        result["comment_list"] = [c.to_dict() if hasattr(c, "to_dict") else c for c in self.comment_list]
        return result


class InstagramScraper:
    """Instagram profile scraper with support for authenticated and public modes."""

    PROFILE_INFO_URL = "https://www.instagram.com/api/v1/users/web_profile_info/"
    GRAPHQL_URL = "https://www.instagram.com/graphql/query"
    POST_INFO_URL = "https://www.instagram.com/api/v1/media/{media_id}/info/"
    COMMENTS_URL = "https://www.instagram.com/api/v1/media/{media_id}/comments/"
    COMMENT_REPLIES_URL = "https://www.instagram.com/api/v1/media/{media_id}/comments/{comment_id}/child_comments/"
    PROFILE_POSTS_DOC_IDS = (
        # Current doc_id observed in live web requests.
        "25645538101792896",
        # Legacy doc_ids still accepted by the public web GraphQL endpoint.
        "26035927152742158",
        "33944389991841132",
    )
    WEB_X_ASBD_ID = "359341"
    PROFILE_POSTS_PAGE_SIZE = int(os.getenv("SOCIAL_INSTAGRAM_PAGE_SIZE", "33"))
    PROFILE_POSTS_FAST_PAGE_SIZE = 50  # Used in fast_mode for fewer pagination requests
    _PROFILE_PAGE_LSD_RE = re.compile(r'"LSD",\[\],\{"token":"(?P<token>[^"]+)"\}')
    _PROFILE_PAGE_BLOKS_VERSION_RE = re.compile(r"bloks_version[^0-9a-fA-F]+(?P<token>[0-9a-fA-F]{32,})")
    _PROFILE_PAGE_SPIN_R_RE = re.compile(r'"__spin_r":(?P<token>\d+)')
    _PROFILE_PAGE_SPIN_B_RE = re.compile(r'"__spin_b":"(?P<token>[^"]+)"')
    _PROFILE_PAGE_SPIN_T_RE = re.compile(r'"__spin_t":(?P<token>\d+)')
    _PROFILE_PAGE_HSI_RE = re.compile(r'"hsi":"?(?P<token>\d+)"?')
    _PROFILE_PAGE_HS_RE = re.compile(r'"(?:haste_session|__hs)":"(?P<token>[^"]+)"')

    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.5
    REQUEST_CONNECT_TIMEOUT_SECONDS = 10
    REQUEST_READ_TIMEOUT_SECONDS = 45
    METRICS_REQUEST_READ_TIMEOUT_SECONDS = 20
    DEFAULT_NO_MATCH_PAGE_LIMIT = 40
    DEFAULT_METRICS_MAX_PAGES = 250
    DEFAULT_METRICS_TIMEOUT_SECONDS = 420
    _VIEW_COUNT_FIELD_PRIORITY = (
        "video_view_count",
        "videoViewCount",
        "view_count",
        "play_count",
        "video_play_count",
        "videoPlayCount",
        "playCount",
        "viewCount",
    )
    _VIEW_COUNT_FIELD_PRIORITY_LOWER = {
        "video_view_count",
        "videoviewcount",
        "view_count",
        "play_count",
        "video_play_count",
        "videoplaycount",
        "playcount",
        "viewcount",
    }
    _VIEW_COUNT_TEXT_FIELDS = (
        "accessibility_caption",
        "accessibilityCaption",
        "caption",
        "text",
        "overlayText",
        "overlay_text",
    )

    def __init__(
        self,
        cookies: dict | None = None,
        session: requests.Session | None = None,
        *,
        browser_account_id: str | None = None,
        browser_session_manager: AccountBrowserSessionManager | None = None,
    ):
        self.cookies = cookies or {}
        self.session = session if session is not None else self._create_session()
        self.browser_account_id = str(browser_account_id or "").strip() or None
        self._browser_session_manager = browser_session_manager or _INSTAGRAM_BROWSER_SESSIONS
        self._request_count = 0
        self._last_429_at: float = 0.0
        self._consecutive_success: int = 0
        self._rate_lock = threading.Lock()
        self._profile_page_context_cache: dict[str, dict[str, str]] = {}
        self.last_retrieval_meta: dict[str, Any] = {}
        self.comments_auth_failed = False
        self.last_comment_fetch_reason: str | None = None
        self.last_post_info_fetch_reason: str | None = None
        self.request_timeout = (
            self.REQUEST_CONNECT_TIMEOUT_SECONDS,
            self.REQUEST_READ_TIMEOUT_SECONDS,
        )

    def _resolved_browser_account_id(self, fallback_account_id: str | None = None) -> str:
        return self._browser_session_manager.resolve_account_id(
            self.browser_account_id,
            fallback_account_id=fallback_account_id,
        )

    # ── Cookie / auth helpers ────────────────────────────────────────────

    def _validate_cookies(self) -> dict[str, Any]:
        """Structural check that cookies contain the minimum fields for auth.

        Does NOT hit the API — Instagram returns 400 for fresh sessions
        before context warming, so we just check key presence.
        """
        result: dict[str, Any] = {
            "valid": False,
            "reason": None,
            "cookies_present": bool(self.cookies),
            "sessionid_present": bool(self.cookies.get("sessionid")),
        }
        sessionid = str(self.cookies.get("sessionid") or "").strip()
        if not sessionid:
            result["reason"] = "no_sessionid"
            return result

        csrftoken = str(self.cookies.get("csrftoken") or "").strip()
        ds_user_id = str(self.cookies.get("ds_user_id") or "").strip()
        result["has_csrftoken"] = bool(csrftoken)
        result["has_ds_user_id"] = bool(ds_user_id)
        if csrftoken and ds_user_id:
            result["valid"] = True
        elif not csrftoken and not ds_user_id:
            result["reason"] = "missing_csrftoken_and_ds_user_id"
        elif not csrftoken:
            result["reason"] = "missing_csrftoken"
        else:
            result["reason"] = "missing_ds_user_id"
        return result

    def _try_auto_refresh_cookies(self) -> dict[str, Any]:
        """Refresh cookies via Playwright login using env-var credentials.

        Env vars: INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD,
        SOCIAL_INSTAGRAM_COOKIES_FILE (default: data/instagram_cookies.json).

        Disabled by default to avoid triggering Instagram security checks.
        Set SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH=true to enable.
        """
        auto_refresh_enabled = (os.getenv("SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH") or "").strip().lower()
        if auto_refresh_enabled not in {"1", "true", "yes", "on"}:
            return {"refreshed": False, "reason": "auto_refresh_disabled"}

        from .cookie_refresh import refresh_instagram_cookies

        ig_user = (os.getenv("INSTAGRAM_USERNAME") or "").strip()
        ig_pass = (os.getenv("INSTAGRAM_PASSWORD") or "").strip()
        if not ig_user or not ig_pass:
            return {"refreshed": False, "reason": "no_credentials_in_env"}

        cookie_file = (os.getenv("SOCIAL_INSTAGRAM_COOKIES_FILE") or "").strip() or "data/instagram_cookies.json"
        cookie_path = Path(cookie_file)
        if not cookie_path.is_absolute():
            project_root = Path(__file__).resolve().parent.parent.parent.parent
            cookie_path = project_root / cookie_file

        logger.info("[instagram] attempting cookie auto-refresh via Playwright login (%s)", ig_user)
        validation_username = str(self.browser_account_id or ig_user).strip().lstrip("@")

        def _validate_refreshed_cookies(cookies: dict[str, str]) -> tuple[bool, str | None]:
            validator_scraper = InstagramScraper(
                cookies=cookies,
                browser_account_id=validation_username or None,
            )
            payload = validator_scraper.fetch_posts_graphql(
                validation_username or ig_user,
                delay=0.0,
                request_timeout=(10, 20),
            )
            connection = (payload or {}).get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
            if connection.get("edges"):
                return True, None
            error_code = str(validator_scraper.last_retrieval_meta.get("error_code") or "").strip().lower()
            error_message = str(validator_scraper.last_retrieval_meta.get("error_message") or "").strip().lower()
            if error_code == "instagram_graphql_checkpoint_required" or error_message == "checkpoint_required":
                return False, "checkpoint_required"
            return False, "graphql_validation_failed"

        try:
            fresh_cookies = refresh_instagram_cookies(
                username=ig_user,
                password=ig_pass,
                cookie_file=str(cookie_path),
                account_id=self._resolved_browser_account_id(ig_user),
                headless=True,
                timeout_seconds=90,
                validation_username=validation_username or ig_user,
                validator=_validate_refreshed_cookies,
            )
            if not fresh_cookies or not fresh_cookies.get("sessionid"):
                return {"refreshed": False, "reason": "refresh_returned_no_sessionid"}

            self.cookies = fresh_cookies
            for k, v in fresh_cookies.items():
                self.session.cookies.set(k, v, domain=".instagram.com")
            self._profile_page_context_cache.clear()
            logger.info("[instagram] cookie auto-refresh succeeded — sessionid=%s…", fresh_cookies["sessionid"][:8])
            return {"refreshed": True, "reason": None, "cookie_file": str(cookie_path)}
        except Exception as exc:  # noqa: BLE001
            logger.warning("[instagram] cookie auto-refresh failed: %s", exc)
            return {"refreshed": False, "reason": f"refresh_error: {exc}"}

    @staticmethod
    def _is_local_environment() -> bool:
        """True when running on local machine, not on Modal workers."""
        return not (os.getenv("MODAL_TASK_ID") or os.getenv("MODAL_ENVIRONMENT"))

    @staticmethod
    def _chrome_browser_headless() -> bool:
        """Resolve headed vs headless mode.

        SOCIAL_INSTAGRAM_BROWSER_MODE=headed   → False (show browser)
        SOCIAL_INSTAGRAM_BROWSER_MODE=headless → True  (background)
        Default: headed locally, headless on Modal.
        """
        mode = (os.getenv("SOCIAL_INSTAGRAM_BROWSER_MODE") or "").strip().lower()
        if mode == "headless":
            return True
        if mode == "headed":
            return False
        # Default: headed locally so you can watch / intervene
        return not InstagramScraper._is_local_environment()

    def _try_interactive_login(self) -> dict[str, Any]:
        """Open Chrome with the user's profile for Instagram auth.

        Auto-enabled when running locally (no Modal env detected).
        Set SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN=false to disable.
        Set SOCIAL_INSTAGRAM_BROWSER_MODE=headed|headless to control visibility.
        """
        # Explicitly disabled?
        explicit = (os.getenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN") or "").strip().lower()
        if explicit in {"0", "false", "no", "off"}:
            return {"refreshed": False, "reason": "interactive_login_disabled"}

        # Never run on Modal workers
        if not self._is_local_environment():
            return {"refreshed": False, "reason": "running_on_modal"}

        chrome_profile = (
            (os.getenv("SOCIAL_INSTAGRAM_CHROME_PROFILE") or "").strip()
            or "entertainmentdatagroup@gmail.com"
        )
        cookie_file = (os.getenv("SOCIAL_INSTAGRAM_COOKIES_FILE") or "").strip() or "data/instagram_cookies.json"
        validation_username = str(self.browser_account_id or "").strip().lstrip("@") or None
        headless = self._chrome_browser_headless()

        try:
            from .cookie_refresh import interactive_chrome_login

            fresh_cookies = interactive_chrome_login(
                chrome_profile_name=chrome_profile,
                cookie_file=cookie_file,
                timeout_seconds=300,
                validation_username=validation_username,
                headless=headless,
            )
            if not fresh_cookies or not fresh_cookies.get("sessionid"):
                return {"refreshed": False, "reason": "interactive_login_no_sessionid"}

            self.cookies = fresh_cookies
            for k, v in fresh_cookies.items():
                self.session.cookies.set(k, v, domain=".instagram.com")
            self._profile_page_context_cache.clear()
            logger.info("[instagram] interactive login succeeded — sessionid=%s…", fresh_cookies["sessionid"][:8])
            return {"refreshed": True, "reason": None, "method": "interactive_chrome"}
        except Exception as exc:  # noqa: BLE001
            logger.warning("[instagram] interactive login failed: %s", exc)
            return {"refreshed": False, "reason": f"interactive_login_error: {exc}"}

    def _profile_posts_doc_ids(self) -> list[str]:
        override = (os.getenv("INSTAGRAM_PROFILE_POSTS_DOC_ID") or "").strip()
        ids: list[str] = []
        if override:
            ids.append(override)
        for doc_id in self.PROFILE_POSTS_DOC_IDS:
            if doc_id not in ids:
                ids.append(doc_id)
        return ids

    def _resolve_graphql_cursor_retry_attempts(self, cursor: str | None) -> int:
        default_attempts = 3 if cursor else 2
        raw = (os.getenv("SOCIAL_INSTAGRAM_CURSOR_RETRY_ATTEMPTS") or "").strip()
        if not raw:
            return default_attempts
        try:
            return max(1, min(int(raw), 5))
        except ValueError:
            return default_attempts

    def _resolve_graphql_retry_backoff_seconds(self, cursor: str | None, attempt_index: int) -> float:
        raw = (os.getenv("SOCIAL_INSTAGRAM_CURSOR_RETRY_BACKOFF_SECONDS") or "").strip()
        if raw:
            try:
                base = max(0.0, min(float(raw), 30.0))
            except ValueError:
                base = 1.5 if cursor else 0.75
        else:
            base = 1.5 if cursor else 0.75
        if base <= 0:
            return 0.0
        computed = min(base * max(1, attempt_index + 1), 30.0)
        return random.uniform(computed * 0.5, computed)

    @staticmethod
    def _playwright_graphql_fallback_enabled() -> bool:
        raw = (os.getenv("SOCIAL_INSTAGRAM_BROWSER_GRAPHQL_FALLBACK") or "").strip().lower()
        if raw:
            return raw not in {"0", "false", "off", "no"}
        return True

    def _browser_cookie_payload(self) -> list[dict[str, Any]]:
        cookies: list[dict[str, Any]] = []
        for name, value in self._request_cookies().items():
            if not str(name or "").strip() or not str(value or "").strip():
                continue
            cookies.append(
                {
                    "name": str(name),
                    "value": str(value),
                    "domain": ".instagram.com",
                    "path": "/",
                    "secure": True,
                }
            )
        return cookies

    def _fetch_posts_graphql_with_browser(
        self,
        username: str,
        cursor: str | None = None,
        *,
        request_timeout: tuple[int, int] | float | None = None,
    ) -> dict[str, Any] | None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            self.last_retrieval_meta.update(
                {
                    "error_code": "instagram_graphql_browser_unavailable",
                    "error_class": type(exc).__name__,
                    "retryable": True,
                    "graphql_cursor": str(cursor or "").strip() or None,
                }
            )
            return None

        timeout = request_timeout or self.request_timeout
        if isinstance(timeout, tuple):
            timeout_ms = int(max(timeout) * 1000)
        else:
            timeout_ms = int(float(timeout) * 1000)
        timeout_ms = max(15_000, min(timeout_ms, 90_000))
        doc_ids = self._profile_posts_doc_ids()
        user_agent = self._get_headers().get("user-agent", "Mozilla/5.0")

        with sync_playwright() as playwright:
            with self._browser_session_manager.account_context(
                playwright=playwright,
                account_id=self._resolved_browser_account_id(username),
                headless=True,
                viewport={"width": 1280, "height": 1400},
                user_agent=user_agent,
                seed_cookies=self._request_cookies(),
            ) as browser_session:
                context = browser_session.context
                page = context.new_page()
                page.goto(
                    f"https://www.instagram.com/{username}/",
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                page.wait_for_timeout(1_500)
                if cursor:
                    captured_payload: dict[str, Any] | None = None
                    captured_failure: dict[str, Any] | None = None

                    def _handle_response(response: Any) -> None:
                        nonlocal captured_payload, captured_failure
                        if captured_payload is not None:
                            return
                        try:
                            if "/graphql/query" not in str(response.url or ""):
                                return
                            request = response.request
                            post_data = str(request.post_data or "")
                            if cursor and cursor not in post_data:
                                return
                            payload = response.json()
                            connection = payload.get("data", {}).get(
                                "xdt_api__v1__feed__user_timeline_graphql_connection", {}
                            )
                            edges = connection.get("edges") or []
                            count_value = self._coerce_int(connection.get("count"), default=0)
                            if response.ok and connection and (edges or count_value == 0):
                                captured_payload = payload
                                return
                            captured_failure = {
                                "status": int(getattr(response, "status", 0) or 0) or None,
                                "payload": payload if isinstance(payload, dict) else None,
                            }
                        except Exception:  # noqa: BLE001
                            return

                    page.on("response", _handle_response)
                    max_scroll_attempts = 4
                    scroll_attempt = 0
                    while scroll_attempt < max_scroll_attempts and captured_payload is None:
                        scroll_attempt += 1
                        page.mouse.wheel(0, 6000)
                        page.wait_for_timeout(1_000)
                    result = {"ok": bool(captured_payload), "payload": captured_payload, "failure": captured_failure}
                else:
                    result = page.evaluate(
                        """
                    async ({ username, cursor, docIds, count, fallbackAsbdId }) => {
                      const html = document.documentElement.outerHTML || "";
                      const capture = (pattern) => {
                        const match = html.match(pattern);
                        if (!match) return null;
                        return match.groups?.token ?? match[1] ?? null;
                      };
                      const lsd = capture(/"LSD",\\[\\],\\{"token":"(?<token>[^"]+)"/);
                      const spinR = capture(/"__spin_r":(?<token>\\d+)/);
                      const spinB = capture(/"__spin_b":"(?<token>[^"]+)"/);
                      const spinT = capture(/"__spin_t":(?<token>\\d+)/);
                      const hsi = capture(/"hsi":"?(?<token>\\d+)"?/);
                      const hs = capture(/"(?:haste_session|__hs)":"(?<token>[^"]+)"/);
                      const cookies = Object.fromEntries(
                        document.cookie.split("; ").filter(Boolean).map((part) => {
                          const idx = part.indexOf("=");
                          if (idx <= 0) return [part, ""];
                          return [part.slice(0, idx), part.slice(idx + 1)];
                        })
                      );
                      const viewerId = cookies.ds_user_id || "0";
                      const asbdId = fallbackAsbdId || "359341";
                      const dpr = String(window.devicePixelRatio || cookies.dpr || "1");
                      const runtime = {};
                      if (lsd) {
                        runtime.lsd = lsd;
                        runtime.jazoest = `2${Array.from(lsd).reduce((sum, ch) => sum + ch.charCodeAt(0), 0)}`;
                      }
                      if (spinR) runtime.__spin_r = spinR;
                      if (spinB) runtime.__spin_b = spinB;
                      if (spinT) runtime.__spin_t = spinT;
                      if (hsi) runtime.__hsi = hsi;
                      if (hs) runtime.__hs = hs;
                      if (dpr) runtime.dpr = dpr;
                      let lastFailure = null;
                      for (const docId of docIds) {
                        const form = new URLSearchParams({
                          av: viewerId,
                          __d: "www",
                          __user: viewerId,
                          __a: "1",
                          __req: "1",
                          __comet_req: "7",
                          fb_api_caller_class: "RelayModern",
                          fb_api_req_friendly_name: "PolarisProfilePostsTabContentQuery_connection",
                          variables: JSON.stringify({
                            after: cursor,
                            before: null,
                            data: {
                              count,
                              include_reel_media_seen_timestamp: true,
                              include_relationship_info: true,
                              latest_besties_reel_media: true,
                              latest_reel_media: true
                            },
                            first: count,
                            last: null,
                            username
                          }),
                          server_timestamps: "true",
                          doc_id: docId,
                          ...runtime
                        });
                        const response = await fetch("/graphql/query", {
                          method: "POST",
                          credentials: "include",
                          headers: {
                            "content-type": "application/x-www-form-urlencoded",
                            "x-fb-friendly-name": "PolarisProfilePostsTabContentQuery_connection",
                            "x-asbd-id": asbdId,
                            ...(lsd ? { "x-fb-lsd": lsd } : {})
                          },
                          body: form.toString()
                        });
                        const text = await response.text();
                        let payload = null;
                        try {
                          payload = JSON.parse(text);
                        } catch (error) {
                          payload = null;
                        }
                        const connection = payload?.data?.xdt_api__v1__feed__user_timeline_graphql_connection;
                        const edges = Array.isArray(connection?.edges) ? connection.edges : [];
                        const countValue = Number(connection?.count || 0);
                        if (response.ok && connection && (edges.length > 0 || countValue === 0)) {
                          return { ok: true, payload, runtime };
                        }
                        lastFailure = {
                          status: response.status,
                          payload,
                          text
                        };
                      }
                      return { ok: false, failure: lastFailure, runtime };
                    }
                    """,
                        {
                            "username": username,
                            "cursor": str(cursor or "").strip() or None,
                            "docIds": doc_ids,
                            "count": self.PROFILE_POSTS_PAGE_SIZE,
                            "fallbackAsbdId": str(os.getenv("INSTAGRAM_WEB_X_ASBD_ID") or self.WEB_X_ASBD_ID),
                        },
                    )
                runtime = dict((result or {}).get("runtime") or {})
                if runtime:
                    self._profile_page_context_cache[username] = {
                        "lsd": str(runtime.get("lsd") or "").strip(),
                        "spin_r": str(runtime.get("__spin_r") or "").strip(),
                        "spin_b": str(runtime.get("__spin_b") or "").strip(),
                        "spin_t": str(runtime.get("__spin_t") or "").strip(),
                        "hsi": str(runtime.get("__hsi") or "").strip(),
                        "hs": str(runtime.get("__hs") or "").strip(),
                    }
                for cookie in context.cookies():
                    name = str(cookie.get("name") or "").strip()
                    value = str(cookie.get("value") or "")
                    if name and value:
                        self.session.cookies.set(name, value)
                if result and result.get("ok") and result.get("payload"):
                    self.last_retrieval_meta["graphql_cursor"] = str(cursor or "").strip() or None
                    self.last_retrieval_meta["retrieval_mode"] = "graphql_playwright"
                    self.last_retrieval_meta["transport"] = "playwright"
                    self.last_retrieval_meta["retrieval_transport"] = "playwright"
                    return dict(result["payload"])
                failure = dict((result or {}).get("failure") or {})
                failure_payload = failure.get("payload") if isinstance(failure.get("payload"), dict) else {}
                failure_message = str(failure_payload.get("message") or "").strip().lower() or None
                status_code = failure.get("status")
                try:
                    status_code = int(status_code) if status_code is not None else None
                except (TypeError, ValueError):
                    status_code = None
                if cursor:
                    error_code = "instagram_graphql_cursor_request_failed"
                    if status_code == 401:
                        error_code = "instagram_graphql_cursor_unauthorized"
                    elif status_code == 403:
                        error_code = "instagram_graphql_cursor_forbidden"
                    elif status_code == 429:
                        error_code = "instagram_graphql_cursor_rate_limited"
                    elif status_code == 400 and failure_message == "checkpoint_required":
                        error_code = "instagram_graphql_checkpoint_required"
                else:
                    error_code = "instagram_graphql_initial_request_failed"
                    if status_code == 400 and failure_message == "checkpoint_required":
                        error_code = "instagram_graphql_checkpoint_required"
                auth_fatal = error_code in {
                    "instagram_graphql_cursor_unauthorized",
                    "instagram_graphql_cursor_forbidden",
                    "instagram_graphql_checkpoint_required",
                }
                self.last_retrieval_meta.update(
                    {
                        "error_code": error_code,
                        "error_class": "PlaywrightGraphQLFailure",
                        "error_status_code": status_code,
                        "error_message": failure_message,
                        "retryable": not auth_fatal,
                        "graphql_cursor": str(cursor or "").strip() or None,
                        "transport": "playwright",
                        "retrieval_transport": "playwright",
                    }
                )
                return None

    def _reset_request_session(self) -> None:
        preserved_cookies = self._request_cookies()
        self.session = self._create_session()
        for key, value in preserved_cookies.items():
            if value is None:
                continue
            self.session.cookies.set(str(key), str(value))

    def _graphql_request_error_details(
        self,
        *,
        cursor: str | None,
        error: requests.exceptions.RequestException | None,
    ) -> dict[str, Any]:
        response = getattr(error, "response", None)
        status_code = None
        try:
            status_code = int(getattr(response, "status_code", 0) or 0) or None
        except (TypeError, ValueError):
            status_code = None
        error_message = self._graphql_error_response_message(response)
        if cursor:
            if status_code == 401:
                error_code = "instagram_graphql_cursor_unauthorized"
            elif status_code == 403:
                error_code = "instagram_graphql_cursor_forbidden"
            elif status_code == 429:
                error_code = "instagram_graphql_cursor_rate_limited"
            elif status_code == 400 and error_message == "checkpoint_required":
                error_code = "instagram_graphql_checkpoint_required"
            else:
                error_code = "instagram_graphql_cursor_request_failed"
        else:
            if status_code == 400 and error_message == "checkpoint_required":
                error_code = "instagram_graphql_checkpoint_required"
            else:
                error_code = "instagram_graphql_initial_request_failed"
        auth_fatal = error_code in {
            "instagram_graphql_cursor_unauthorized",
            "instagram_graphql_cursor_forbidden",
            "instagram_graphql_checkpoint_required",
        }
        return {
            "error_code": error_code,
            "error_class": error.__class__.__name__ if error is not None else "RequestException",
            "error_status_code": status_code,
            "error_message": error_message,
            "retryable": not auth_fatal,
            "graphql_cursor": str(cursor or "").strip() or None,
        }

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

    def _resolve_no_match_page_limit(self, config: ScrapeConfig) -> int:
        if config.no_match_page_limit is not None:
            try:
                return max(0, int(config.no_match_page_limit))
            except (TypeError, ValueError):
                return 0

        raw = (os.getenv("SOCIAL_INSTAGRAM_NO_MATCH_PAGE_LIMIT") or "").strip() or (
            os.getenv("SOCIAL_NO_MATCH_PAGE_LIMIT") or ""
        ).strip()
        if raw:
            try:
                return max(0, int(raw))
            except ValueError:
                return 0

        if config.date_start or config.date_end:
            return self.DEFAULT_NO_MATCH_PAGE_LIMIT
        return 0

    def _resolve_metrics_max_pages(self, config: ScrapeConfig) -> int:
        if config.max_pages is not None:
            try:
                return max(1, int(config.max_pages))
            except (TypeError, ValueError):
                return self.DEFAULT_METRICS_MAX_PAGES
        raw = (os.getenv("SOCIAL_INSTAGRAM_METRICS_MAX_PAGES") or "").strip()
        if raw:
            try:
                return max(1, int(raw))
            except ValueError:
                return self.DEFAULT_METRICS_MAX_PAGES
        return self.DEFAULT_METRICS_MAX_PAGES

    def _resolve_metrics_timeout_seconds(self) -> int:
        raw = (os.getenv("SOCIAL_INSTAGRAM_METRICS_TIMEOUT_SECONDS") or "").strip()
        if raw:
            try:
                return max(30, int(raw))
            except ValueError:
                return self.DEFAULT_METRICS_TIMEOUT_SECONDS
        return self.DEFAULT_METRICS_TIMEOUT_SECONDS

    def _get(self, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self.request_timeout)
        response = self.session.get(url, **kwargs)
        self._track_response_status(response.status_code)
        return response

    def _post(self, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self.request_timeout)
        response = self.session.post(url, **kwargs)
        self._track_response_status(response.status_code)
        return response

    def _resolve_metrics_request_timeout(self) -> tuple[int, int]:
        read_raw = (os.getenv("SOCIAL_INSTAGRAM_METRICS_READ_TIMEOUT_SECONDS") or "").strip()
        if read_raw:
            try:
                read_timeout = max(5, int(read_raw))
            except ValueError:
                read_timeout = self.METRICS_REQUEST_READ_TIMEOUT_SECONDS
        else:
            read_timeout = self.METRICS_REQUEST_READ_TIMEOUT_SECONDS
        connect_timeout = max(3, int(self.REQUEST_CONNECT_TIMEOUT_SECONDS))
        return (connect_timeout, read_timeout)

    def _request_cookies(self) -> dict[str, str]:
        """Merge any fresh session cookies back into the request cookie payload."""
        merged = {str(key): str(value) for key, value in (self.cookies or {}).items() if value is not None}
        session_cookies = self.session.cookies.get_dict()
        for key, value in session_cookies.items():
            if value is None:
                continue
            merged[str(key)] = str(value)
        self.cookies = merged
        return merged

    def _get_headers(self, referer: str | None = None) -> dict:
        """Get request headers."""
        request_cookies = self._request_cookies()
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://www.instagram.com",
            "referer": referer or "https://www.instagram.com/",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "x-ig-app-id": "936619743392459",
            "x-requested-with": "XMLHttpRequest",
        }
        if request_cookies.get("csrftoken"):
            headers["x-csrftoken"] = request_cookies["csrftoken"]
        return headers

    @staticmethod
    def _graphql_error_response_message(response: requests.Response | None) -> str | None:
        if response is None:
            return None
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            message = str(payload.get("message") or "").strip().lower()
            if message:
                return message
        text = str(getattr(response, "text", "") or "").strip()
        if not text:
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return None
        if isinstance(payload, dict):
            message = str(payload.get("message") or "").strip().lower()
            if message:
                return message
        return None

    def _extract_profile_page_context(self, html: str) -> dict[str, str]:
        context: dict[str, str] = {}
        if not html:
            return context
        lsd_match = self._PROFILE_PAGE_LSD_RE.search(html)
        if lsd_match:
            context["lsd"] = str(lsd_match.group("token") or "").strip()
        bloks_match = self._PROFILE_PAGE_BLOKS_VERSION_RE.search(html)
        if bloks_match:
            context["bloks_version"] = str(bloks_match.group("token") or "").strip()
        spin_r_match = self._PROFILE_PAGE_SPIN_R_RE.search(html)
        if spin_r_match:
            context["spin_r"] = str(spin_r_match.group("token") or "").strip()
        spin_b_match = self._PROFILE_PAGE_SPIN_B_RE.search(html)
        if spin_b_match:
            context["spin_b"] = str(spin_b_match.group("token") or "").strip()
        spin_t_match = self._PROFILE_PAGE_SPIN_T_RE.search(html)
        if spin_t_match:
            context["spin_t"] = str(spin_t_match.group("token") or "").strip()
        hsi_match = self._PROFILE_PAGE_HSI_RE.search(html)
        if hsi_match:
            context["hsi"] = str(hsi_match.group("token") or "").strip()
        hs_match = self._PROFILE_PAGE_HS_RE.search(html)
        if hs_match:
            context["hs"] = str(hs_match.group("token") or "").strip()
        return {key: value for key, value in context.items() if value}

    @staticmethod
    def _jazoest_for_token(token: str | None) -> str | None:
        raw = str(token or "").strip()
        if not raw:
            return None
        return f"2{sum(ord(char) for char in raw)}"

    def _graphql_form_runtime_fields(
        self,
        *,
        page_context: dict[str, str],
        request_cookies: dict[str, str],
    ) -> dict[str, str]:
        runtime_fields: dict[str, str] = {}
        lsd_token = str(page_context.get("lsd") or request_cookies.get("lsd") or "").strip()
        if lsd_token:
            runtime_fields["lsd"] = lsd_token
            jazoest = self._jazoest_for_token(lsd_token)
            if jazoest:
                runtime_fields["jazoest"] = jazoest
        for context_key, field_key in (
            ("spin_r", "__spin_r"),
            ("spin_b", "__spin_b"),
            ("spin_t", "__spin_t"),
            ("hsi", "__hsi"),
            ("hs", "__hs"),
        ):
            value = str(page_context.get(context_key) or "").strip()
            if value:
                runtime_fields[field_key] = value
        dpr = str(request_cookies.get("dpr") or "").strip()
        if dpr:
            runtime_fields["dpr"] = dpr
        return runtime_fields

    def _warm_profile_request_context(
        self,
        username: str,
        *,
        timeout: tuple[int, int] | float | None = None,
        force: bool = False,
    ) -> dict[str, str]:
        cached = self._profile_page_context_cache.get(username) or {}
        if cached and not force:
            return dict(cached)

        warm_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://www.instagram.com/",
            "user-agent": self._get_headers().get("user-agent", "Mozilla/5.0"),
        }
        try:
            response = self._get(
                f"https://www.instagram.com/{username}/",
                headers=warm_headers,
                cookies=self._request_cookies(),
                timeout=timeout or self.request_timeout,
            )
        except requests.exceptions.RequestException:
            logger.debug("Failed warming Instagram profile context for %s", username, exc_info=True)
            return dict(cached)

        context = self._extract_profile_page_context(response.text or "")
        if response.cookies:
            self._request_cookies()
        if context:
            self._profile_page_context_cache[username] = dict(context)
        return dict(context or cached)

    def _rate_limit(self, delay: float, *, fast_mode: bool = False):
        """Apply adaptive rate limiting between requests.

        Standard mode: starts at 50% of the base delay.
        Fast mode: uses aggressive tiers that ramp down with consecutive successes.
        Both modes: double delay for 60s after any 429 response.
        Thread-safe via _rate_lock to support concurrent partition runners.
        """
        with self._rate_lock:
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
                elif self._consecutive_success >= 20:
                    effective_delay = delay * 0.5
                else:
                    effective_delay = delay * 0.75
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
        with self._rate_lock:
            if status_code == 429:
                self._last_429_at = time.monotonic()
                self._consecutive_success = 0
            elif 200 <= status_code < 400:
                self._consecutive_success += 1

    @staticmethod
    def _normalize_tag_coord(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        coord: float | None = None
        if isinstance(value, (int, float)):
            coord = float(value)
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                coord = float(text)
            except ValueError:
                return None
        if coord is None or not math.isfinite(coord):
            return None
        return round(min(1.0, max(0.0, coord)), 4)

    def _normalized_tag_position(
        self,
        x_value: Any,
        y_value: Any,
        *,
        source: str,
    ) -> tuple[float, float, str] | None:
        x = self._normalize_tag_coord(x_value)
        y = self._normalize_tag_coord(y_value)
        if x is None or y is None:
            return None
        return (x, y, source)

    def _tag_position_from_array(self, value: Any, *, source: str) -> tuple[float, float, str] | None:
        if not isinstance(value, (list, tuple)) or len(value) < 2:
            return None
        return self._normalized_tag_position(value[0], value[1], source=source)

    def _tag_position_from_object(self, value: Any, *, source: str) -> tuple[float, float, str] | None:
        if not isinstance(value, dict):
            return None
        return self._normalized_tag_position(
            value.get("x", value.get("left")),
            value.get("y", value.get("top")),
            source=source,
        )

    def _extract_rest_tag_position(self, tagged: dict[str, Any]) -> tuple[float, float, str] | None:
        position = tagged.get("position")
        return self._tag_position_from_array(
            position, source="rest_usertags.position_array"
        ) or self._tag_position_from_object(position, source="rest_usertags.position_object")

    def _extract_graphql_tag_position(
        self,
        edge: dict[str, Any],
        edge_node: dict[str, Any],
    ) -> tuple[float, float, str] | None:
        return (
            self._tag_position_from_array(edge_node.get("position"), source="graphql_node.position_array")
            or self._tag_position_from_object(edge_node.get("position"), source="graphql_node.position_object")
            or self._normalized_tag_position(edge_node.get("x"), edge_node.get("y"), source="graphql_node.xy")
            or self._tag_position_from_array(edge.get("position"), source="graphql_edge.position_array")
            or self._tag_position_from_object(edge.get("position"), source="graphql_edge.position_object")
            or self._normalized_tag_position(edge.get("x"), edge.get("y"), source="graphql_edge.xy")
        )

    @staticmethod
    def _coerce_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _coerce_timestamp(value: Any) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return 0
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return 0
            return int(parsed.timestamp())
        return 0

    @staticmethod
    def _parse_compact_count(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            try:
                return max(0, int(float(value)))
            except (TypeError, ValueError):
                return None
        raw = str(value or "").strip()
        if not raw:
            return None
        normalized = raw.replace(",", "").strip()
        match = re.match(r"^(?P<num>\d+(?:\.\d+)?)\s*(?P<suffix>[KMBkmb])?$", normalized)
        if not match:
            return None
        base = float(match.group("num"))
        suffix = str(match.group("suffix") or "").lower()
        multiplier = 1
        if suffix == "k":
            multiplier = 1_000
        elif suffix == "m":
            multiplier = 1_000_000
        elif suffix == "b":
            multiplier = 1_000_000_000
        return max(0, int(base * multiplier))

    @classmethod
    def _extract_count_from_text_metric(cls, value: Any) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        match = re.search(
            r"(?P<count>\d[\d,]*(?:\.\d+)?)\s*(?P<suffix>[KMBkmb])?\s*(?:views?|plays?)\b",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        compact = f"{match.group('count')}{match.group('suffix') or ''}"
        return cls._parse_compact_count(compact)

    @classmethod
    def _is_candidate_view_metric_key(cls, key: str) -> bool:
        normalized = str(key or "").strip()
        if not normalized:
            return False
        lowered = normalized.lower()
        if lowered in cls._VIEW_COUNT_FIELD_PRIORITY_LOWER:
            return True
        return lowered.endswith("_view_count") or lowered.endswith("_play_count")

    @staticmethod
    def _normalize_metric_raw_value(value: Any) -> Any:
        if isinstance(value, (int, float, bool)) or value is None:
            return value
        if isinstance(value, str):
            compact = value.strip()
            if len(compact) <= 120:
                return compact
            return compact[:117] + "..."
        return str(value)[:120]

    def _collect_view_candidates(
        self,
        node: Any,
        *,
        prefix: str = "",
        depth: int = 0,
        out: list[tuple[str, Any]] | None = None,
    ) -> list[tuple[str, Any]]:
        candidates = out if out is not None else []
        if depth > 5:
            return candidates
        if isinstance(node, dict):
            for key, value in node.items():
                key_text = str(key or "").strip()
                path = f"{prefix}.{key_text}" if prefix else key_text
                if self._is_candidate_view_metric_key(key_text):
                    candidates.append((path, value))
                if key_text in self._VIEW_COUNT_TEXT_FIELDS and isinstance(value, str):
                    text_count = self._extract_count_from_text_metric(value)
                    if text_count is not None:
                        candidates.append((f"{path}:text_metric", text_count))
                if isinstance(value, (dict, list)):
                    self._collect_view_candidates(
                        value,
                        prefix=path,
                        depth=depth + 1,
                        out=candidates,
                    )
            return candidates
        if isinstance(node, list):
            for idx, item in enumerate(node):
                path = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
                self._collect_view_candidates(item, prefix=path, depth=depth + 1, out=candidates)
        return candidates

    def _extract_video_views(self, node: dict[str, Any]) -> tuple[int | None, str | None, list[dict[str, Any]]]:
        raw_candidates: list[dict[str, Any]] = []

        def _record(source: str, raw: Any, parsed: int | None) -> None:
            raw_candidates.append(
                {
                    "source": source,
                    "raw": self._normalize_metric_raw_value(raw),
                    "parsed": parsed,
                }
            )

        for key in self._VIEW_COUNT_FIELD_PRIORITY:
            if key not in node:
                continue
            raw_value = node.get(key)
            parsed = self._parse_compact_count(raw_value)
            _record(f"node.{key}", raw_value, parsed)
            if parsed is not None:
                return parsed, f"node.{key}", raw_candidates[:10]

        for text_key in self._VIEW_COUNT_TEXT_FIELDS:
            if text_key not in node:
                continue
            raw_text = node.get(text_key)
            parsed_text = self._extract_count_from_text_metric(raw_text)
            _record(f"node.{text_key}:text_metric", raw_text, parsed_text)
            if parsed_text is not None:
                return parsed_text, f"node.{text_key}:text_metric", raw_candidates[:10]

        for source_path, raw_value in self._collect_view_candidates(node):
            parsed = self._parse_compact_count(raw_value)
            _record(source_path, raw_value, parsed)
            if parsed is not None:
                return parsed, source_path, raw_candidates[:10]

        return None, None, raw_candidates[:10]

    @staticmethod
    def _dedupe_preserve_order(values: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for value in values:
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(value)
        return out

    @staticmethod
    def _normalize_handle_token(value: Any) -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        raw = raw.lstrip("@#")
        match = re.search(r"[A-Za-z0-9._]+", raw)
        if not match:
            return None
        token = match.group(0).rstrip(".,:;!?)]}>'\"")
        return token or None

    def _normalize_mention(self, value: Any) -> str | None:
        token = self._normalize_handle_token(value)
        if not token:
            return None
        return f"@{token}"

    def _normalize_hashtag(self, value: Any) -> str | None:
        token = self._normalize_handle_token(value)
        if not token:
            return None
        cleaned = token.replace(".", "")
        if not cleaned:
            return None
        return cleaned

    def _determine_post_type(self, node: dict) -> str:
        """Determine post type from node data."""
        typename = node.get("__typename", "")
        if typename == "GraphSidecar":
            return "carousel"
        if typename == "GraphVideo":
            if node.get("product_type") == "clips":
                return "reel"
            return "video"
        if typename == "GraphImage":
            return "image"

        actor_product_type = str(node.get("productType") or node.get("product_type") or "").strip().lower()
        if actor_product_type == "clips":
            return "reel"

        actor_type = str(node.get("type") or node.get("media_type_name") or "").strip().lower()
        if actor_type in {"reel", "clips"}:
            return "reel"
        if actor_type in {"carousel", "sidecar"}:
            return "carousel"
        if actor_type == "video":
            return "video"
        if actor_type == "image":
            return "image"

        # Fallback to media_type (REST API)
        media_type = node.get("media_type")
        product_type = node.get("product_type", "")
        if product_type == "clips":
            return "reel"
        if node.get("carousel_media") or node.get("carousel_media_count"):
            return "carousel"
        if media_type == 1:
            return "image"
        if media_type == 2:
            return "video"
        return "image"

    def _extract_caption(self, node: dict) -> str:
        """Extract caption text from various response formats."""
        # GraphQL format
        edge_caption = node.get("edge_media_to_caption", {})
        if edge_caption:
            edges = edge_caption.get("edges", [])
            if edges:
                return edges[0].get("node", {}).get("text", "")

        # REST API format
        caption = node.get("caption")
        if isinstance(caption, dict):
            return caption.get("text", "")
        if isinstance(caption, str):
            return caption
        return ""

    def _extract_profile_tags(self, node: dict) -> list[str]:
        """Extract tagged usernames from post."""
        tags = []

        # GraphQL format
        edge_tags = node.get("edge_media_to_tagged_user", {})
        if edge_tags:
            for edge in edge_tags.get("edges", []):
                username = edge.get("node", {}).get("user", {}).get("username")
                if username:
                    tags.append(username)

        # REST API format
        usertags = node.get("usertags", {})
        if usertags and isinstance(usertags, dict):
            for tag in usertags.get("in", []):
                username = tag.get("user", {}).get("username")
                if username and username not in tags:
                    tags.append(username)

        tagged_users = node.get("taggedUsers")
        if isinstance(tagged_users, list):
            for tagged in tagged_users:
                username = None
                if isinstance(tagged, dict):
                    username = tagged.get("username") or tagged.get("user", {}).get("username")
                if not username:
                    continue
                normalized = self._normalize_handle_token(username)
                if normalized:
                    tags.append(normalized)

        return self._dedupe_preserve_order(tags)

    def _extract_collaborators(self, node: dict) -> list[str]:
        collaborators: list[str] = []
        for key in ("coauthor_producers", "invited_coauthor_producers", "coauthorProducers"):
            values = node.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                username = value
                if isinstance(value, dict):
                    username = value.get("username") or value.get("user", {}).get("username")
                normalized = self._normalize_handle_token(username)
                if normalized:
                    collaborators.append(normalized)
        return self._dedupe_preserve_order(collaborators)

    @staticmethod
    def _profile_pic_quality_score(value: str) -> tuple[int, int]:
        normalized = str(value or "").strip().lower()
        if not normalized:
            return (-1, -1)
        score = 0
        if "profile_pic" in normalized:
            score += 25
        if "profile_pic_url_hd" in normalized or "profile_pic_hd" in normalized:
            score += 30
        if any(
            token in normalized for token in ("thumb", "thumbnail", "s32x32", "s50x50", "s64x64", "s96x96", "s150x150")
        ):
            score -= 20
        max_dim = 0
        for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
            max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
        for match in re.finditer(r"profile_pic[^0-9]{0,8}(\d{2,4})", normalized):
            max_dim = max(max_dim, int(match.group(1)))
        return (score + min(max_dim, 4096), len(normalized))

    def _pick_best_profile_pic_url(self, *candidates: Any) -> str | None:
        best: str | None = None
        best_score: tuple[int, int] = (-1, -1)
        for candidate in candidates:
            value = str(candidate or "").strip()
            if not value.startswith(("http://", "https://")):
                continue
            score = self._profile_pic_quality_score(value)
            if score > best_score:
                best_score = score
                best = value
        return best

    def _extract_profile_avatar_from_profile_payload(self, data: dict[str, Any] | None) -> str | None:
        if not isinstance(data, dict):
            return None
        user = data.get("data", {}).get("user", {}) if isinstance(data.get("data"), dict) else {}
        if not isinstance(user, dict):
            return None
        return self._pick_best_profile_pic_url(
            user.get("profile_pic_url_hd") or user.get("profilePicUrlHd"),
            user.get("profile_pic_url") or user.get("profilePicUrl"),
        )

    def _backfill_post_owner_profile_pic(
        self,
        posts: list["InstagramPost"],
        *,
        profile_pic_url: str | None,
    ) -> int:
        resolved_profile_pic = str(profile_pic_url or "").strip() or None
        if not resolved_profile_pic:
            return 0

        updated = 0
        for post in posts:
            if not getattr(post, "owner_profile_pic_url", None):
                post.owner_profile_pic_url = resolved_profile_pic
                updated += 1
            owner_detail = getattr(post, "owner_detail", None)
            if owner_detail and not getattr(owner_detail, "profile_pic_url", None):
                owner_detail.profile_pic_url = resolved_profile_pic
        return updated

    def _extract_tagged_users_detail(self, node: dict) -> list[InstagramUserDetail]:
        """Extract full tagged user objects from post."""
        details: list[InstagramUserDetail] = []
        seen: set[str] = set()

        def _add(
            username: str | None,
            user_id: Any = None,
            full_name: str | None = None,
            is_verified: Any = None,
            profile_pic_url: str | None = None,
            profile_pic_url_hd: str | None = None,
            tag_position: tuple[float, float, str] | None = None,
        ) -> None:
            normalized = self._normalize_handle_token(username)
            if not normalized or normalized.lower() in seen:
                return
            seen.add(normalized.lower())
            tag_x = tag_position[0] if tag_position is not None else None
            tag_y = tag_position[1] if tag_position is not None else None
            tag_position_source = tag_position[2] if tag_position is not None else None
            details.append(
                InstagramUserDetail(
                    username=normalized,
                    user_id=str(user_id) if user_id else None,
                    full_name=full_name or None,
                    is_verified=bool(is_verified) if is_verified is not None else None,
                    profile_pic_url=self._pick_best_profile_pic_url(profile_pic_url_hd, profile_pic_url),
                    profile_pic_url_hd=str(profile_pic_url_hd or "").strip() or None,
                    tag_x=tag_x,
                    tag_y=tag_y,
                    tag_position_source=tag_position_source,
                )
            )

        # GraphQL format
        edge_tags = node.get("edge_media_to_tagged_user", {})
        if edge_tags:
            for edge in edge_tags.get("edges", []):
                if not isinstance(edge, dict):
                    continue
                edge_node = edge.get("node", {})
                if not isinstance(edge_node, dict):
                    edge_node = {}
                user = edge_node.get("user", {}) if isinstance(edge_node.get("user"), dict) else {}
                _add(
                    user.get("username"),
                    user.get("id") or user.get("pk"),
                    user.get("full_name"),
                    user.get("is_verified"),
                    user.get("profile_pic_url"),
                    user.get("profile_pic_url_hd") or user.get("profilePicUrlHd"),
                    tag_position=self._extract_graphql_tag_position(edge, edge_node),
                )

        # REST API format
        usertags = node.get("usertags", {})
        if usertags and isinstance(usertags, dict):
            for tag in usertags.get("in", []):
                if not isinstance(tag, dict):
                    continue
                user = tag.get("user", {})
                _add(
                    user.get("username"),
                    user.get("pk") or user.get("id"),
                    user.get("full_name"),
                    user.get("is_verified"),
                    user.get("profile_pic_url"),
                    user.get("profile_pic_url_hd") or user.get("profilePicUrlHd"),
                    tag_position=self._extract_rest_tag_position(tag),
                )

        # Actor-style format
        tagged_users = node.get("taggedUsers")
        if isinstance(tagged_users, list):
            for tagged in tagged_users:
                if not isinstance(tagged, dict):
                    continue
                user = tagged if "username" in tagged else tagged.get("user", {})
                _add(
                    user.get("username"),
                    user.get("id") or user.get("pk"),
                    user.get("full_name") or user.get("fullName"),
                    user.get("is_verified") or user.get("isVerified"),
                    user.get("profile_pic_url") or user.get("profilePicUrl"),
                    user.get("profile_pic_url_hd") or user.get("profilePicUrlHd"),
                    tag_position=(
                        self._tag_position_from_array(
                            tagged.get("position"),
                            source="actor_tagged_users.position_array",
                        )
                        or self._tag_position_from_object(
                            tagged.get("position"),
                            source="actor_tagged_users.position_object",
                        )
                        or self._normalized_tag_position(
                            tagged.get("x"),
                            tagged.get("y"),
                            source="actor_tagged_users.xy",
                        )
                    ),
                )

        return details

    def _extract_collaborators_detail(self, node: dict) -> list[InstagramUserDetail]:
        """Extract full collaborator user objects from post."""
        details: list[InstagramUserDetail] = []
        seen: set[str] = set()

        for key in ("coauthor_producers", "invited_coauthor_producers", "coauthorProducers"):
            values = node.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                if not isinstance(value, dict):
                    continue
                user = value if "username" in value else value.get("user", {})
                normalized = self._normalize_handle_token(user.get("username"))
                if not normalized or normalized.lower() in seen:
                    continue
                seen.add(normalized.lower())
                details.append(
                    InstagramUserDetail(
                        username=normalized,
                        user_id=str(user.get("id") or user.get("pk") or "") or None,
                        full_name=user.get("full_name") or user.get("fullName") or None,
                        is_verified=bool(user.get("is_verified") or user.get("isVerified"))
                        if (user.get("is_verified") is not None or user.get("isVerified") is not None)
                        else None,
                        profile_pic_url=self._pick_best_profile_pic_url(
                            user.get("profile_pic_url_hd") or user.get("profilePicUrlHd"),
                            user.get("profile_pic_url") or user.get("profilePicUrl"),
                        ),
                        profile_pic_url_hd=str(user.get("profile_pic_url_hd") or user.get("profilePicUrlHd") or "")
                        or None,
                    )
                )

        return details

    def _extract_owner_detail(self, node: dict) -> InstagramUserDetail | None:
        """Extract post owner/author detail."""
        owner = node.get("owner", {}) if isinstance(node.get("owner"), dict) else {}
        username = str(node.get("ownerUsername") or owner.get("username") or "").strip()
        if not username:
            return None
        return InstagramUserDetail(
            username=username,
            user_id=str(owner.get("id") or owner.get("pk") or node.get("ownerId") or "") or None,
            full_name=str(owner.get("full_name") or node.get("ownerFullName") or "").strip() or None,
            is_verified=bool(owner.get("is_verified")) if "is_verified" in owner else None,
            profile_pic_url=self._pick_best_profile_pic_url(
                owner.get("profile_pic_url_hd") or owner.get("profilePicUrlHd"),
                owner.get("profile_pic_url"),
                node.get("ownerProfilePicUrlHd"),
                node.get("ownerProfilePicUrl"),
            ),
            profile_pic_url_hd=str(
                owner.get("profile_pic_url_hd")
                or owner.get("profilePicUrlHd")
                or node.get("ownerProfilePicUrlHd")
                or ""
            ).strip()
            or None,
        )

    def _extract_child_posts_data(self, node: dict) -> list[dict[str, Any]]:
        """Extract carousel child post details."""
        children: list[dict[str, Any]] = []

        # Actor-style format (childPosts)
        child_posts = node.get("childPosts")
        if isinstance(child_posts, list):
            for index, child in enumerate(child_posts):
                if not isinstance(child, dict):
                    continue
                dims = child.get("dimensions", {})
                child_tagged_users = [detail.to_dict() for detail in self._extract_tagged_users_detail(child)]
                children.append(
                    {
                        "slide_index": index,
                        "type": child.get("type") or child.get("__typename"),
                        "display_url": child.get("displayUrl") or child.get("display_url"),
                        "video_url": child.get("videoUrl") or child.get("video_url"),
                        "width": self._coerce_int(
                            dims.get("width") if isinstance(dims, dict) else child.get("original_width"), 0
                        )
                        or None,
                        "height": self._coerce_int(
                            dims.get("height") if isinstance(dims, dict) else child.get("original_height"), 0
                        )
                        or None,
                        "alt": child.get("alt") or child.get("accessibility_caption"),
                        "tagged_users_detail": child_tagged_users,
                    }
                )
            if children:
                return children

        # REST API format (carousel_media)
        carousel = node.get("carousel_media")
        if isinstance(carousel, list):
            for index, item in enumerate(carousel):
                if not isinstance(item, dict):
                    continue
                display_url = None
                video_url = None
                if item.get("image_versions2"):
                    candidates = item["image_versions2"].get("candidates", [])
                    if candidates:
                        display_url = candidates[0].get("url")
                if item.get("video_versions"):
                    versions = item["video_versions"]
                    if versions:
                        video_url = versions[0].get("url")
                child_tagged_users = [detail.to_dict() for detail in self._extract_tagged_users_detail(item)]
                children.append(
                    {
                        "slide_index": index,
                        "type": "Video" if video_url else "Image",
                        "display_url": display_url,
                        "video_url": video_url,
                        "width": self._coerce_int(item.get("original_width"), 0) or None,
                        "height": self._coerce_int(item.get("original_height"), 0) or None,
                        "alt": item.get("accessibility_caption"),
                        "tagged_users_detail": child_tagged_users,
                    }
                )
            if children:
                return children

        # GraphQL format (edge_sidecar_to_children)
        sidecar = node.get("edge_sidecar_to_children") or {}
        for index, edge in enumerate(sidecar.get("edges", [])):
            child = edge.get("node", {})
            dims = child.get("dimensions", {})
            child_tagged_users = [detail.to_dict() for detail in self._extract_tagged_users_detail(child)]
            children.append(
                {
                    "slide_index": index,
                    "type": child.get("__typename", "").replace("Graph", ""),
                    "display_url": child.get("display_url"),
                    "video_url": child.get("video_url"),
                    "width": self._coerce_int(dims.get("width"), 0) or None,
                    "height": self._coerce_int(dims.get("height"), 0) or None,
                    "alt": child.get("accessibility_caption"),
                    "tagged_users_detail": child_tagged_users,
                }
            )

        return children

    def _extract_additional_post_fields(self, node: dict) -> dict[str, Any]:
        """Extract supplementary post metadata not covered by primary fields."""
        result: dict[str, Any] = {}

        # product_type (e.g., "clips" for reels, "feed" for feed posts)
        result["product_type"] = str(node.get("productType") or node.get("product_type") or "").strip() or None

        # video_play_count (distinct from video_view_count)
        vpc = node.get("videoPlayCount") or node.get("video_play_count")
        result["video_play_count"] = self._coerce_int(vpc, 0) if vpc is not None else None

        # alt text
        result["alt_text"] = (
            str(node.get("accessibilityCaption") or node.get("accessibility_caption") or node.get("alt") or "").strip()
            or None
        )

        # dimensions
        dims = node.get("dimensions")
        if isinstance(dims, dict):
            result["width"] = self._coerce_int(dims.get("width"), 0) or None
            result["height"] = self._coerce_int(dims.get("height"), 0) or None
        else:
            w = node.get("dimensionsWidth") or node.get("original_width")
            h = node.get("dimensionsHeight") or node.get("original_height")
            result["width"] = self._coerce_int(w, 0) or None if w is not None else None
            result["height"] = self._coerce_int(h, 0) or None if h is not None else None

        # is_comments_disabled
        icd = node.get("comments_disabled")
        if icd is None:
            icd = node.get("isCommentsDisabled")
        result["is_comments_disabled"] = bool(icd) if icd is not None else None

        # music_info
        music = node.get("musicInfo") or node.get("music_info")
        result["music_info"] = music if isinstance(music, dict) else None

        # audio_url (direct MP4 link to audio track, separate from video)
        audio_url = node.get("audioUrl") or node.get("audio_url")
        result["audio_url"] = str(audio_url).strip() if audio_url else None

        # video_duration
        vd = node.get("videoDuration") or node.get("video_duration")
        if vd is not None:
            try:
                result["video_duration"] = float(vd)
            except (TypeError, ValueError):
                result["video_duration"] = None
        else:
            result["video_duration"] = None

        return result

    def _extract_hashtags(self, node: dict, caption: str) -> list[str]:
        tags: list[str] = []
        if isinstance(node.get("hashtags"), list):
            for item in node.get("hashtags", []):
                value = item.get("name") if isinstance(item, dict) else item
                normalized = self._normalize_hashtag(value)
                if normalized:
                    tags.append(normalized)
        if caption:
            for item in re.findall(r"(?<![\w.])#([A-Za-z0-9_]+)", caption):
                normalized = self._normalize_hashtag(item)
                if normalized:
                    tags.append(normalized)
        return self._dedupe_preserve_order(tags)

    def _extract_mentions(self, node: dict, caption: str) -> list[str]:
        mentions: list[str] = []
        if isinstance(node.get("mentions"), list):
            for item in node.get("mentions", []):
                value = item.get("username") if isinstance(item, dict) else item
                normalized = self._normalize_mention(value)
                if normalized:
                    mentions.append(normalized)
        if caption:
            for item in re.findall(r"(?<![\w.])@([A-Za-z0-9_.]+)", caption):
                normalized = self._normalize_mention(item)
                if normalized:
                    mentions.append(normalized)
        return self._dedupe_preserve_order(mentions)

    def _extract_like_count(self, node: dict) -> int:
        """Extract like count."""
        if "like_count" in node:
            return self._coerce_int(node.get("like_count"), 0)
        if "likesCount" in node:
            return self._coerce_int(node.get("likesCount"), 0)
        edge_liked = node.get("edge_liked_by") or node.get("edge_media_preview_like")
        if edge_liked:
            return self._coerce_int(edge_liked.get("count"), 0)
        return 0

    def _extract_comment_count(self, node: dict) -> int:
        """Extract comment count."""
        if "comment_count" in node:
            return self._coerce_int(node.get("comment_count"), 0)
        if "commentsCount" in node:
            return self._coerce_int(node.get("commentsCount"), 0)
        edge_comments = node.get("edge_media_to_comment")
        if edge_comments:
            return self._coerce_int(edge_comments.get("count"), 0)
        return 0

    def _extract_timestamp(self, node: dict) -> int:
        """Extract Unix timestamp."""
        return self._coerce_timestamp(node.get("taken_at_timestamp") or node.get("taken_at") or node.get("timestamp"))

    def _extract_shortcode(self, node: dict) -> str:
        """Extract shortcode."""
        return str(node.get("shortcode") or node.get("shortCode") or node.get("code", ""))

    def _parse_post_node(self, node: dict, config: ScrapeConfig) -> InstagramPost:
        """Parse a post node into InstagramPost."""
        shortcode = self._extract_shortcode(node)
        taken_at = self._extract_timestamp(node)
        caption = self._extract_caption(node)
        media_urls = self._extract_media_urls(node)
        mentions = self._extract_mentions(node, caption)
        extras = self._extract_additional_post_fields(node)
        post_type = self._determine_post_type(node)
        video_views_observed, video_views_source, video_views_raw_candidates = self._extract_video_views(node)
        route = "reel" if post_type == "reel" else "p"

        return InstagramPost(
            shortcode=shortcode,
            post_type=post_type,
            date_time=datetime.fromtimestamp(taken_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if taken_at else "",
            taken_at=taken_at,
            caption=caption,
            profile_tags=self._extract_profile_tags(node),
            sponsored=bool(node.get("is_paid_partnership")),
            likes=self._extract_like_count(node),
            comments=self._extract_comment_count(node),
            video_views=video_views_observed or 0,
            url=f"https://www.instagram.com/{route}/{shortcode}/" if shortcode else "",
            pk=str(node.get("pk") or node.get("id", "")),
            username=str(node.get("ownerUsername") or node.get("owner", {}).get("username") or config.username),
            video_views_observed=video_views_observed,
            video_views_source=video_views_source,
            video_views_raw_candidates=video_views_raw_candidates,
            media_urls=media_urls,
            thumbnail_url=self._extract_thumbnail_url(node, media_urls),
            hashtags=self._extract_hashtags(node, caption),
            mentions=mentions,
            collaborators=self._extract_collaborators(node),
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
            tagged_users_detail=self._extract_tagged_users_detail(node),
            collaborators_detail=self._extract_collaborators_detail(node),
            owner_detail=self._extract_owner_detail(node),
            product_type=extras.get("product_type"),
            video_play_count=extras.get("video_play_count"),
            alt_text=extras.get("alt_text"),
            width=extras.get("width"),
            height=extras.get("height"),
            is_comments_disabled=extras.get("is_comments_disabled"),
            music_info=extras.get("music_info"),
            audio_url=extras.get("audio_url"),
            video_duration=extras.get("video_duration"),
            child_posts_data=self._extract_child_posts_data(node),
        )

    def fetch_profile_info(
        self,
        username: str,
        delay: float = 2.0,
        *,
        request_timeout: tuple[int, int] | float | None = None,
    ) -> dict | None:
        """Fetch profile info using public API (limited to ~12 posts)."""
        self._rate_limit(delay)
        url = f"{self.PROFILE_INFO_URL}?username={username}"
        timeout = request_timeout or self.request_timeout
        if not self.session.cookies.get("csrftoken"):
            self._warm_profile_request_context(username, timeout=timeout)

        attempts_remaining = 2
        last_error: requests.exceptions.RequestException | None = None
        while attempts_remaining > 0:
            attempts_remaining -= 1
            try:
                response = self._get(
                    url,
                    headers=self._get_headers(f"https://www.instagram.com/{username}/"),
                    cookies=self._request_cookies(),
                    timeout=timeout,
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                last_error = e
                if attempts_remaining <= 0:
                    break
                self._warm_profile_request_context(username, timeout=timeout, force=True)
        logger.error("Failed to fetch profile info for %s: %s", username, last_error)
        return None

    def fetch_posts_graphql(
        self,
        username: str,
        cursor: str | None = None,
        delay: float = 2.0,
        *,
        request_timeout: tuple[int, int] | float | None = None,
        fast_mode: bool = False,
        allow_browser_fallback: bool = True,
        page_size: int | None = None,
    ) -> dict | None:
        """Fetch posts using GraphQL (requires auth for full access)."""
        self._rate_limit(delay, fast_mode=fast_mode)
        for key in (
            "error_code",
            "error_class",
            "error_status_code",
            "error_message",
            "retryable",
            "graphql_cursor",
            "transport",
            "retrieval_transport",
        ):
            self.last_retrieval_meta.pop(key, None)
        timeout = request_timeout or self.request_timeout
        # Always allow at least 2 attempts for the initial page (cursor=None)
        # to survive a single transient failure, even in fast_mode.
        if fast_mode:
            attempt_limit = 2 if not cursor else 1
        else:
            attempt_limit = self._resolve_graphql_cursor_retry_attempts(cursor)
        last_error: requests.exceptions.RequestException | None = None
        for attempt_index in range(attempt_limit):
            # In fast_mode, only warm context on the very first page (no cursor).
            # Subsequent pages reuse the cached context to avoid extra HTTP requests.
            should_warm = not fast_mode or (not cursor and attempt_index == 0)
            page_context = (
                self._warm_profile_request_context(
                    username,
                    timeout=timeout,  # use full timeout — first warm sets critical tokens
                    force=(bool(cursor) or attempt_index > 0) if should_warm else False,
                )
                if should_warm
                else self._profile_page_context_cache.get(username, {})
            )
            request_cookies = self._request_cookies()
            viewer_id = str(request_cookies.get("ds_user_id") or self.cookies.get("ds_user_id") or "0")

            resolved_page_size = page_size or (
                self.PROFILE_POSTS_FAST_PAGE_SIZE if fast_mode else self.PROFILE_POSTS_PAGE_SIZE
            )
            variables = {
                "after": cursor,
                "before": None,
                "data": {
                    "count": resolved_page_size,
                    "include_reel_media_seen_timestamp": True,
                    "include_relationship_info": True,
                    "latest_besties_reel_media": True,
                    "latest_reel_media": True,
                },
                "first": resolved_page_size,
                "last": None,
                "username": username,
            }

            data = {
                "av": viewer_id,
                "__d": "www",
                "__user": viewer_id,
                "__a": "1",
                "__req": "1",
                "__comet_req": "7",
                "fb_api_caller_class": "RelayModern",
                "fb_api_req_friendly_name": "PolarisProfilePostsTabContentQuery_connection",
                "variables": json.dumps(variables),
                "server_timestamps": "true",
            }
            data.update(self._graphql_form_runtime_fields(page_context=page_context, request_cookies=request_cookies))

            headers = self._get_headers(f"https://www.instagram.com/{username}/")
            headers["content-type"] = "application/x-www-form-urlencoded"
            headers["x-fb-friendly-name"] = "PolarisProfilePostsTabContentQuery_connection"
            headers["x-asbd-id"] = str(os.getenv("INSTAGRAM_WEB_X_ASBD_ID") or self.WEB_X_ASBD_ID)
            lsd_token = str(page_context.get("lsd") or request_cookies.get("lsd") or "").strip()
            if lsd_token:
                headers["x-fb-lsd"] = lsd_token
            bloks_version = str(
                os.getenv("INSTAGRAM_WEB_BLOKS_VERSION_ID") or page_context.get("bloks_version") or ""
            ).strip()
            if bloks_version:
                headers["x-bloks-version-id"] = bloks_version

            saw_request_error = False
            for doc_id in self._profile_posts_doc_ids():
                data["doc_id"] = doc_id
                try:
                    response = self._post(
                        self.GRAPHQL_URL,
                        data=data,
                        headers=headers,
                        cookies=request_cookies,
                        timeout=timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    connection = payload.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
                    if connection:
                        self.last_retrieval_meta["graphql_cursor"] = str(cursor or "").strip() or None
                        self.last_retrieval_meta["retrieval_mode"] = "graphql_requests_enriched"
                        self.last_retrieval_meta["transport"] = "requests_enriched"
                        self.last_retrieval_meta["retrieval_transport"] = "requests_enriched"
                        return payload
                    logger.warning("Instagram GraphQL doc_id %s returned no connection data; trying fallback", doc_id)
                except requests.exceptions.RequestException as e:
                    last_error = e
                    saw_request_error = True
                    status_code = getattr(getattr(e, "response", None), "status_code", None)
                    if status_code in (401, 403):
                        logger.warning("GraphQL auth error %s for doc_id %s — skipping remaining doc_ids", status_code, doc_id)
                        break
                    logger.warning("GraphQL request failed for doc_id %s: %s", doc_id, e)
                    continue

            if attempt_index + 1 < attempt_limit and saw_request_error:
                self._reset_request_session()
                backoff_seconds = self._resolve_graphql_retry_backoff_seconds(cursor, attempt_index)
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds)
                logger.warning(
                    "Instagram GraphQL %s page failed for @%s; refreshing profile context and retrying (%d/%d)",
                    "cursor" if cursor else "initial",
                    username,
                    attempt_index + 1,
                    attempt_limit,
                )
                continue
            break
        if last_error is not None and cursor:
            self.last_retrieval_meta.update(self._graphql_request_error_details(cursor=cursor, error=last_error))
            self.last_retrieval_meta["transport"] = "requests_enriched"
            self.last_retrieval_meta["retrieval_transport"] = "requests_enriched"
            if self.last_retrieval_meta.get("error_code") in {
                "instagram_graphql_cursor_unauthorized",
                "instagram_graphql_cursor_forbidden",
                "instagram_graphql_cursor_rate_limited",
            }:
                self._reset_request_session()
                self._profile_page_context_cache.pop(username, None)
            logger.warning("Instagram GraphQL exhausted cursor retries for @%s after cursor=%s", username, cursor)
        elif last_error is not None:
            self.last_retrieval_meta.update(self._graphql_request_error_details(cursor=cursor, error=last_error))
            self.last_retrieval_meta["transport"] = "requests_enriched"
            self.last_retrieval_meta["retrieval_transport"] = "requests_enriched"
        auth_recoverable_errors = {
            "instagram_graphql_checkpoint_required",
            "instagram_graphql_cursor_unauthorized",
            "instagram_graphql_cursor_forbidden",
        }
        unrecoverable_fallback_errors = {
            "instagram_graphql_cursor_rate_limited",
            # NOTE: auth errors (403/401/checkpoint) are NOT included here because
            # the Playwright browser fallback CAN recover from them — the browser
            # has additional context (Origin, Referer, JS headers) that HTTP lacks.
        }
        # ── Lightweight session rotation before escalating to interactive login ──
        _auto_rotation_attempted = getattr(self, "_auto_rotation_attempted_this_scrape", False)
        if (
            self.last_retrieval_meta.get("error_code") in auth_recoverable_errors
            and not _auto_rotation_attempted
        ):
            logger.warning(
                "GraphQL auth error for @%s (%s) — attempting auto cookie refresh before interactive login",
                username,
                self.last_retrieval_meta.get("error_code"),
            )
            self._auto_rotation_attempted_this_scrape = True
            refresh_result = self._try_auto_refresh_cookies()
            if refresh_result.get("refreshed"):
                self._reset_request_session()
                self._profile_page_context_cache.pop(username, None)
                retry_payload = self.fetch_posts_graphql(
                    username,
                    cursor=cursor,
                    delay=delay,
                    request_timeout=request_timeout,
                    fast_mode=fast_mode,
                    allow_browser_fallback=allow_browser_fallback,
                    page_size=page_size,
                )
                if retry_payload is not None:
                    self._auto_rotation_attempted_this_scrape = False
                    return retry_payload

        # If auth failed and we're running locally, open Chrome for manual re-auth (once per scrape)
        _interactive_already_attempted = getattr(self, "_interactive_login_attempted_this_scrape", False)
        if (
            self.last_retrieval_meta.get("error_code") in auth_recoverable_errors
            and self._is_local_environment()
            and not _interactive_already_attempted
        ):
            logger.warning(
                "Instagram auth failed for @%s (%s) — attempting interactive Chrome login",
                username,
                self.last_retrieval_meta.get("error_code"),
            )
            self._interactive_login_attempted_this_scrape = True
            interactive_result = self._try_interactive_login()
            if interactive_result.get("refreshed"):
                # Got fresh cookies — retry the failed request once
                self._reset_request_session()
                self._profile_page_context_cache.pop(username, None)
                retry_payload = self.fetch_posts_graphql(
                    username,
                    cursor=cursor,
                    delay=delay,
                    request_timeout=request_timeout,
                    fast_mode=fast_mode,
                    allow_browser_fallback=False,
                    page_size=page_size,
                )
                if retry_payload is not None:
                    # Reset the guard on success — cookies are working again
                    self._interactive_login_attempted_this_scrape = False
                    return retry_payload
                logger.warning(
                    "Instagram GraphQL still failing for @%s after interactive login — falling through to browser fallback",
                    username,
                )
        elif _interactive_already_attempted and self.last_retrieval_meta.get("error_code") in auth_recoverable_errors:
            logger.warning(
                "Instagram GraphQL 403 for @%s persists after interactive login — falling through to browser fallback",
                username,
            )
        if self.last_retrieval_meta.get("error_code") in unrecoverable_fallback_errors:
            logger.warning(
                "Skipping Instagram GraphQL Playwright fallback for @%s because %s is not browser-recoverable",
                username,
                self.last_retrieval_meta.get("error_code"),
            )
            return None
        if allow_browser_fallback and self._playwright_graphql_fallback_enabled():
            fallback_payload = self._fetch_posts_graphql_with_browser(
                username,
                cursor,
                request_timeout=timeout,
            )
            if fallback_payload is not None:
                return fallback_payload
        return None

    def _iter_posts_from_profile_info(self, data: dict) -> Iterator[tuple[dict, dict]]:
        """Iterate posts from profile info response."""
        user = data.get("data", {}).get("user", {})
        timeline = user.get("edge_owner_to_timeline_media", {})
        edges = timeline.get("edges", [])
        page_info = timeline.get("page_info", {})

        for edge in edges:
            yield edge.get("node", {}), page_info

    def _iter_posts_from_graphql(self, data: dict) -> Iterator[tuple[dict, dict]]:
        """Iterate posts from GraphQL response."""
        connection = data.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
        edges = connection.get("edges", [])
        page_info = connection.get("page_info", {})

        for edge in edges:
            yield edge.get("node", {}), page_info

    def _extract_profile_total_posts(self, data: dict[str, Any], *, source: str) -> int | None:
        if source == "profile_info":
            user = data.get("data", {}).get("user", {})
            timeline = user.get("edge_owner_to_timeline_media", {})
            total_posts = self._coerce_int(timeline.get("count"), default=0)
            return total_posts if total_posts > 0 else None
        connection = data.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
        total_posts = self._coerce_int(connection.get("count"), default=0)
        return total_posts if total_posts > 0 else None

    def _shortcode_to_media_id(self, shortcode: str) -> str:
        """Convert Instagram shortcode to media ID."""
        return permalink_shortcode_to_media_id(shortcode)

    def fetch_post_info(self, shortcode: str, delay: float = 2.0) -> dict | None:
        """Fetch detailed post info including media URLs."""
        self._rate_limit(delay)
        self.last_post_info_fetch_reason = None
        try:
            media_id = self._shortcode_to_media_id(shortcode)
        except (ValueError, IndexError):
            logger.error(f"Invalid shortcode '{shortcode}' — skipping post info fetch")
            self.last_post_info_fetch_reason = "invalid_shortcode"
            return None
        url = self.POST_INFO_URL.format(media_id=media_id)
        headers = self._get_headers(f"https://www.instagram.com/p/{shortcode}/")

        def _fallback_from_permalink() -> dict | None:
            from trr_backend.socials.instagram.permalink_metadata import fetch_permalink_media_item

            try:
                media_item = fetch_permalink_media_item(
                    shortcode,
                    session=self.session,
                    timeout=self.request_timeout,
                    headers=headers,
                    cookies=self._request_cookies(),
                )
            except Exception:
                media_item = None
            if isinstance(media_item, dict):
                self.last_post_info_fetch_reason = "fallback_permalink_media_item"
                return {"items": [media_item]}
            self.last_post_info_fetch_reason = self.last_post_info_fetch_reason or "node_not_found"
            return None

        try:
            response = self._get(url, headers=headers, cookies=self._request_cookies())
            response.raise_for_status()
            content_type = str(response.headers.get("content-type") or "").lower()
            if "text/html" in content_type:
                self.last_post_info_fetch_reason = "html_challenge_or_auth_required"
                return _fallback_from_permalink()
            try:
                payload = response.json()
            except ValueError:
                self.last_post_info_fetch_reason = "non_json_response"
                return _fallback_from_permalink()
            return payload
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch post info for {shortcode}: {e}")
            self.last_post_info_fetch_reason = "request_error"
            return _fallback_from_permalink()

    def fetch_comments(
        self,
        shortcode: str,
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
    ) -> list[InstagramComment]:
        """
        Fetch comments for a post including replies.

        Args:
            shortcode: Post shortcode (from URL like /p/DUBSkVeEp4c/)
            max_comments: Maximum number of top-level comments to fetch
            fetch_replies: Whether to fetch replies to comments
            delay: Delay between API requests
            fast_mode: Use aggressive rate limiting tiers

        Returns:
            List of InstagramComment objects with nested replies
        """
        self.last_comment_fetch_reason = None
        self.comments_auth_failed = False
        try:
            media_id = self._shortcode_to_media_id(shortcode)
        except (ValueError, IndexError):
            logger.error(f"Invalid shortcode '{shortcode}' — skipping comment fetch")
            self.last_comment_fetch_reason = "invalid_shortcode"
            return []
        post_url = f"https://www.instagram.com/p/{shortcode}/"
        logger.info(f"Fetching comments for {shortcode} (media_id: {media_id})")

        comments = []
        cursor = None
        comments_fetched = 0

        while True:
            response: requests.Response | None = None
            self._rate_limit(delay, fast_mode=fast_mode)
            url = self.COMMENTS_URL.format(media_id=media_id)
            params = {"can_support_threading": "true", "permalink_enabled": "false"}
            if cursor:
                params["min_id"] = cursor

            headers = self._get_headers(post_url)

            try:
                response = self._get(url, params=params, headers=headers, cookies=self._request_cookies())
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                if "text/html" in content_type:
                    if not self.comments_auth_failed:
                        logger.error(
                            "Instagram returned HTML instead of JSON for comments on %s "
                            "(session cookie likely expired — re-export cookies to fix)",
                            shortcode,
                        )
                    self.comments_auth_failed = True
                    self.last_comment_fetch_reason = "html_challenge_or_auth_required"
                    break
                try:
                    data = response.json()
                except ValueError:
                    self.last_comment_fetch_reason = "non_json_response"
                    logger.error(
                        "Instagram comments endpoint returned a non-JSON payload for %s (status=%s content-type=%s)",
                        shortcode,
                        response.status_code,
                        content_type or "unknown",
                    )
                    break
                status_value = str(data.get("status") or "").strip().lower() if isinstance(data, dict) else ""
                if status_value and status_value != "ok":
                    self.last_comment_fetch_reason = "api_status_fail"
                    message = str(data.get("message") or data.get("error_message") or "").strip().lower()
                    if status_value in {"fail", "login_required", "checkpoint_required", "challenge_required"} or (
                        "challenge" in message or "login" in message or "checkpoint" in message
                    ):
                        self.comments_auth_failed = True
                    logger.error(
                        "Instagram comments endpoint returned status=%s for %s (message=%s)",
                        status_value,
                        shortcode,
                        str(data.get("message") or data.get("error_message") or ""),
                    )
                    break
            except requests.exceptions.RequestException as e:
                self.last_comment_fetch_reason = "request_error"
                logger.error(
                    "Failed to fetch comments for %s (status=%s): %s",
                    shortcode,
                    getattr(response, "status_code", "?") if response is not None else "?",
                    e,
                )
                break

            # Parse comments
            if isinstance(data, list):
                comment_rows = data
            elif isinstance(data, dict):
                comment_rows = data.get("comments", [])
            else:
                comment_rows = []
            for comment_data in comment_rows:
                comment = self._parse_comment(comment_data, shortcode, post_url)
                comments.append(comment)
                comments_fetched += 1

                # Fetch replies if requested and comment has replies
                if fetch_replies and comment.reply_count > 0 and not comment.replies:
                    replies = self._fetch_comment_replies(
                        media_id,
                        comment.comment_id,
                        shortcode,
                        post_url,
                        delay,
                        fast_mode=fast_mode,
                    )
                    comment.replies = replies
                    logger.info(f"  Comment {comment.comment_id}: {comment.reply_count} replies fetched")

                if max_comments and comments_fetched >= max_comments:
                    break

            logger.info(f"Fetched {len(comments)} comments so far...")

            # Check for more pages.
            if max_comments and comments_fetched >= max_comments:
                break
            has_more = bool(data.get("has_more_comments", False)) if isinstance(data, dict) else False
            # Instagram often reports `has_more_comments=false` while still providing
            # `has_more_headload_comments=true` with a valid `next_min_id`.
            if isinstance(data, dict):
                has_more = has_more or bool(data.get("has_more_headload_comments", False))
            if not has_more:
                break
            next_cursor = (data.get("next_min_id") or data.get("next_max_id")) if isinstance(data, dict) else None
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

        logger.info(f"Total: {len(comments)} comments fetched for {shortcode}")
        return comments

    def _fetch_comment_replies(
        self,
        media_id: str,
        comment_id: str,
        shortcode: str,
        post_url: str,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
    ) -> list[InstagramComment]:
        """Fetch replies to a specific comment."""
        replies = []
        cursor = None

        while True:
            response: requests.Response | None = None
            self._rate_limit(delay, fast_mode=fast_mode)
            url = self.COMMENT_REPLIES_URL.format(media_id=media_id, comment_id=comment_id)
            params = {}
            if cursor:
                params["min_id"] = cursor

            headers = self._get_headers(post_url)

            try:
                response = self._get(url, params=params, headers=headers, cookies=self._request_cookies())
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                if "text/html" in content_type:
                    logger.error(
                        "Instagram returned HTML for replies on comment %s (session cookie likely expired)",
                        comment_id,
                    )
                    self.comments_auth_failed = True
                    self.last_comment_fetch_reason = "html_challenge_or_auth_required"
                    break
                try:
                    data = response.json()
                except ValueError:
                    self.last_comment_fetch_reason = "non_json_response"
                    logger.error(
                        "Instagram replies endpoint returned a non-JSON payload for comment %s "
                        "(status=%s content-type=%s)",
                        comment_id,
                        response.status_code,
                        content_type or "unknown",
                    )
                    break
                status_value = str(data.get("status") or "").strip().lower() if isinstance(data, dict) else ""
                if status_value and status_value != "ok":
                    self.last_comment_fetch_reason = "api_status_fail"
                    message = str(data.get("message") or data.get("error_message") or "").strip().lower()
                    if status_value in {"fail", "login_required", "checkpoint_required", "challenge_required"} or (
                        "challenge" in message or "login" in message or "checkpoint" in message
                    ):
                        self.comments_auth_failed = True
                    logger.error(
                        "Instagram replies endpoint returned status=%s for comment %s (message=%s)",
                        status_value,
                        comment_id,
                        str(data.get("message") or data.get("error_message") or ""),
                    )
                    break
            except requests.exceptions.RequestException as e:
                self.last_comment_fetch_reason = "request_error"
                logger.error(f"Failed to fetch replies for comment {comment_id}: {e}")
                break

            # Parse child comments (replies)
            if isinstance(data, dict):
                reply_rows = data.get("child_comments", [])
                if not reply_rows and isinstance(data.get("replies"), list):
                    reply_rows = data.get("replies", [])
            elif isinstance(data, list):
                reply_rows = data
            else:
                reply_rows = []
            for reply_data in reply_rows:
                reply = self._parse_comment(reply_data, shortcode, post_url, is_reply=True, parent_id=comment_id)
                replies.append(reply)

            # Check for more pages
            has_more_tail = bool(data.get("has_more_tail_child_comments", False)) if isinstance(data, dict) else False
            if not has_more_tail:
                break
            cursor = data.get("next_min_child_cursor") if isinstance(data, dict) else None
            if not cursor:
                break

        return replies

    def fetch_comments_concurrent(
        self,
        shortcodes: list[str],
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
        max_workers: int | None = None,
    ) -> dict[str, list["InstagramComment"]]:
        """Fetch comments for multiple posts concurrently.

        Returns a dict mapping shortcode -> list of comments.
        Uses a ThreadPoolExecutor for parallel fetching while coordinating
        rate limiting across threads via a shared lock.

        Args:
            shortcodes: List of post shortcodes to fetch comments for
            max_comments: Max comments per post
            fetch_replies: Whether to fetch reply chains
            delay: Base delay between requests
            fast_mode: Use aggressive rate limiting
            max_workers: Concurrency level (default from env or 3)
        """
        if max_workers is None:
            max_workers = int(os.getenv("SOCIAL_INSTAGRAM_COMMENT_CONCURRENCY", "3"))
        max_workers = max(1, min(max_workers, 8))  # Clamp to 1-8

        if len(shortcodes) <= 1 or max_workers <= 1:
            # Sequential fallback for single items or concurrency=1
            result: dict[str, list[InstagramComment]] = {}
            for sc in shortcodes:
                result[sc] = self.fetch_comments(
                    sc,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=delay,
                    fast_mode=fast_mode,
                )
            return result

        # Use a lock to serialize _rate_limit sleep calls so threads don't
        # all sleep independently (which would be slower than sequential).
        rate_lock = threading.Lock()
        original_rate_limit = self._rate_limit

        def _synchronized_rate_limit(d: float, *, fast_mode: bool = False):
            with rate_lock:
                original_rate_limit(d, fast_mode=fast_mode)

        results: dict[str, list[InstagramComment]] = {}

        def _fetch_one(shortcode: str) -> tuple[str, list[InstagramComment]]:
            # Temporarily patch _rate_limit with the synchronized version
            self._rate_limit = _synchronized_rate_limit  # type: ignore[assignment]
            try:
                comments = self.fetch_comments(
                    shortcode,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=delay,
                    fast_mode=fast_mode,
                )
                return shortcode, comments
            except Exception as exc:
                logger.error("Concurrent comment fetch failed for %s: %s", shortcode, exc)
                return shortcode, []

        logger.info(
            "Fetching comments for %d posts concurrently (workers=%d, fast=%s)",
            len(shortcodes),
            max_workers,
            fast_mode,
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_one, sc): sc for sc in shortcodes}
            completed = 0
            for future in as_completed(futures):
                shortcode, comments = future.result()
                results[shortcode] = comments
                completed += 1
                if completed % 10 == 0:
                    logger.info(
                        "Concurrent comments: %d/%d posts done",
                        completed,
                        len(shortcodes),
                    )

        # Restore original _rate_limit
        self._rate_limit = original_rate_limit  # type: ignore[assignment]
        logger.info(
            "Concurrent comment fetch complete: %d posts, %d total comments",
            len(results),
            sum(len(v) for v in results.values()),
        )
        return results

    def _parse_comment(
        self,
        data: dict,
        shortcode: str,
        post_url: str,
        is_reply: bool = False,
        parent_id: str | None = None,
    ) -> InstagramComment:
        """Parse comment data into InstagramComment object."""
        user = data.get("user", {}) if isinstance(data.get("user"), dict) else {}
        owner = data.get("owner", {}) if isinstance(data.get("owner"), dict) else {}
        created_at = self._coerce_timestamp(data.get("created_at") or data.get("timestamp"))
        username = data.get("ownerUsername") or owner.get("username") or user.get("username") or ""
        user_id = data.get("ownerId") or owner.get("id") or user.get("pk") or user.get("id") or ""
        likes = data.get("comment_like_count")
        if likes is None:
            likes = data.get("likesCount")
        if likes is None:
            likes = data.get("like_count")
        reply_count = data.get("child_comment_count")
        if reply_count is None:
            reply_count = data.get("repliesCount")

        comment = InstagramComment(
            comment_id=str(data.get("pk") or data.get("id") or ""),
            text=str(data.get("text") or ""),
            username=str(username or ""),
            user_id=str(user_id or ""),
            created_at=created_at,
            date_time=datetime.fromtimestamp(created_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if created_at else "",
            likes=self._coerce_int(likes, 0),
            is_reply=is_reply,
            parent_comment_id=parent_id,
            reply_count=self._coerce_int(reply_count, 0),
            media_urls=self._extract_comment_media_urls(data),
            owner_profile_pic_url=self._pick_best_profile_pic_url(
                data.get("ownerProfilePicUrlHd"),
                data.get("ownerProfilePicUrl"),
                owner.get("profile_pic_url_hd") or owner.get("profilePicUrlHd"),
                owner.get("profile_pic_url"),
                user.get("profile_pic_url_hd") or user.get("profilePicUrlHd"),
                user.get("profile_pic_url"),
            ),
            owner_is_verified=(
                bool(owner.get("is_verified"))
                if "is_verified" in owner
                else bool(user.get("is_verified"))
                if "is_verified" in user
                else None
            ),
            post_shortcode=shortcode,
            post_url=post_url,
        )
        nested_replies = data.get("replies")
        if isinstance(nested_replies, list):
            comment.replies = [
                self._parse_comment(reply, shortcode, post_url, is_reply=True, parent_id=comment.comment_id)
                for reply in nested_replies
                if isinstance(reply, dict)
            ]
            if comment.reply_count <= 0:
                comment.reply_count = len(comment.replies)
        return comment

    def _extract_comment_media_urls(self, data: dict[str, Any]) -> list[str]:
        """Extract media URLs from Instagram comment payloads when present."""
        urls: list[str] = []

        def _append(candidate: Any) -> None:
            value = str(candidate or "").strip()
            if value and value.startswith(("http://", "https://")) and value not in urls:
                urls.append(value)

        explicit_media_urls = data.get("media_urls")
        if isinstance(explicit_media_urls, list):
            for candidate in explicit_media_urls:
                _append(candidate)

        def _collect_media_node(node: Any) -> None:
            if isinstance(node, list):
                for item in node:
                    _collect_media_node(item)
                return
            if not isinstance(node, dict):
                return

            for candidate in self._extract_media_urls(node):
                _append(candidate)

            for key in (
                "url",
                "display_url",
                "displayUrl",
                "video_url",
                "videoUrl",
                "image_url",
                "imageUrl",
                "thumbnail_url",
                "thumbnailUrl",
            ):
                _append(node.get(key))

            for nested_key in (
                "media",
                "media_versions",
                "attachment",
                "attachments",
                "content",
                "preview",
                "image_versions2",
                "video_versions",
                "carousel_media",
            ):
                nested = node.get(nested_key)
                if isinstance(nested, (dict, list)):
                    _collect_media_node(nested)

        for key in ("media", "attachment", "attachments", "content", "preview", "clip"):
            nested = data.get(key)
            if isinstance(nested, (dict, list)):
                _collect_media_node(nested)

        if isinstance(data.get("image_versions2"), dict) or isinstance(data.get("video_versions"), list):
            _collect_media_node(data)

        return self._dedupe_preserve_order(urls)

    @staticmethod
    def _first_valid_http_url(candidates: list[Any]) -> str | None:
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value.startswith(("http://", "https://")):
                return value
        return None

    def _extract_primary_image_url(self, node: dict[str, Any]) -> str | None:
        candidates: list[Any] = []

        image_versions = node.get("image_versions2")
        if isinstance(image_versions, dict):
            versions = image_versions.get("candidates")
            if isinstance(versions, list) and versions:
                first = versions[0] if isinstance(versions[0], dict) else {}
                candidates.append(first.get("url"))

        candidates.extend([node.get("display_url"), node.get("displayUrl")])

        images = node.get("images")
        if isinstance(images, list):
            for image in images:
                if isinstance(image, str):
                    candidates.append(image)
                    continue
                if isinstance(image, dict):
                    candidates.extend([image.get("url"), image.get("displayUrl"), image.get("display_url")])

        return self._first_valid_http_url(candidates)

    def _extract_primary_video_url(self, node: dict[str, Any]) -> str | None:
        candidates: list[Any] = []

        versions = node.get("video_versions")
        if isinstance(versions, list) and versions:
            first = versions[0] if isinstance(versions[0], dict) else {}
            candidates.append(first.get("url"))

        candidates.extend([node.get("video_url"), node.get("videoUrl")])
        return self._first_valid_http_url(candidates)

    def _extract_carousel_media_urls(self, node: dict[str, Any]) -> list[str]:
        urls: list[str] = []

        def _append(candidate: str | None) -> None:
            if candidate and candidate not in urls:
                urls.append(candidate)

        carousel = node.get("carousel_media")
        if isinstance(carousel, list):
            for item in carousel:
                if not isinstance(item, dict):
                    continue
                _append(self._extract_primary_video_url(item) or self._extract_primary_image_url(item))

        sidecar = node.get("edge_sidecar_to_children")
        if isinstance(sidecar, dict):
            edges = sidecar.get("edges")
            if isinstance(edges, list):
                for edge in edges:
                    child = edge.get("node", {}) if isinstance(edge, dict) else {}
                    if not isinstance(child, dict):
                        continue
                    _append(self._extract_primary_video_url(child) or self._extract_primary_image_url(child))

        child_posts = node.get("childPosts")
        if isinstance(child_posts, list):
            for child in child_posts:
                if not isinstance(child, dict):
                    continue
                _append(self._extract_primary_video_url(child) or self._extract_primary_image_url(child))

        return urls

    def _extract_thumbnail_url(self, node: dict[str, Any], media_urls: list[str]) -> str | None:
        thumbnail = self._extract_primary_image_url(node)
        if thumbnail:
            return thumbnail
        return media_urls[0] if media_urls else None

    def _extract_media_urls(self, node: dict) -> list[str]:
        """Extract canonical media URLs from a post.

        Single-image and single-video posts store one primary media URL.
        Real carousel posts keep one primary media URL per slide.
        """
        post_type = self._determine_post_type(node)
        if post_type == "carousel":
            return self._extract_carousel_media_urls(node)

        primary_video_url = self._extract_primary_video_url(node)
        primary_image_url = self._extract_primary_image_url(node)
        primary_url = primary_video_url if post_type in {"video", "reel"} else (primary_image_url or primary_video_url)
        if not primary_url:
            primary_url = primary_video_url or primary_image_url
        return [primary_url] if primary_url else []

    def _emit_progress(
        self,
        progress_cb: Callable[[dict[str, Any]], None] | None,
        *,
        phase: str,
        pages_scanned: int,
        posts_checked: int,
        matched_posts: int,
        total_posts: int | None = None,
    ) -> None:
        if not progress_cb:
            return
        try:
            payload = {
                "phase": phase,
                "pages_scanned": max(0, int(pages_scanned)),
                "posts_checked": max(0, int(posts_checked)),
                "matched_posts": max(0, int(matched_posts)),
            }
            if total_posts is not None:
                payload["total_posts"] = max(0, int(total_posts))
            progress_cb(payload)
        except Exception:
            logger.debug("Instagram scrape progress callback raised", exc_info=True)

    def _metrics_entry_from_node(self, node: dict[str, Any]) -> dict[str, Any]:
        views_observed, views_source, raw_candidates = self._extract_video_views(node)
        return {
            "likes": self._extract_like_count(node),
            "comments": self._extract_comment_count(node),
            "views_observed": views_observed,
            "views_source": views_source,
            "views_raw_candidates": raw_candidates,
            "post_type": self._determine_post_type(node),
        }

    def _merge_metrics_entry(self, existing: dict[str, Any] | None, incoming: dict[str, Any]) -> dict[str, Any]:
        if not existing:
            return dict(incoming)
        merged = dict(existing)
        merged["likes"] = max(self._coerce_int(existing.get("likes"), 0), self._coerce_int(incoming.get("likes"), 0))
        merged["comments"] = max(
            self._coerce_int(existing.get("comments"), 0),
            self._coerce_int(incoming.get("comments"), 0),
        )
        existing_views = existing.get("views_observed")
        incoming_views = incoming.get("views_observed")
        if incoming_views is not None:
            if existing_views is None:
                merged["views_observed"] = incoming_views
                merged["views_source"] = incoming.get("views_source")
                merged["views_raw_candidates"] = incoming.get("views_raw_candidates") or []
            else:
                merged["views_observed"] = max(self._coerce_int(existing_views, 0), self._coerce_int(incoming_views, 0))
                if self._coerce_int(incoming_views, 0) >= self._coerce_int(existing_views, 0):
                    merged["views_source"] = incoming.get("views_source")
                    merged["views_raw_candidates"] = incoming.get("views_raw_candidates") or []
        return merged

    def scrape_metrics_index(
        self,
        config: ScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, dict[str, Any]]:
        metrics_index: dict[str, dict[str, Any]] = {}
        posts_checked = 0
        pages_scanned = 0
        no_match_pages = 0
        no_match_page_limit = self._resolve_no_match_page_limit(config)
        max_pages_limit = self._resolve_metrics_max_pages(config)
        timeout_deadline = time.monotonic() + float(self._resolve_metrics_timeout_seconds())
        seen_cursors: set[str] = set()
        metrics_request_timeout = self._resolve_metrics_request_timeout()

        def _process_node(node: dict[str, Any]) -> bool:
            nonlocal posts_checked
            posts_checked += 1
            timestamp = self._extract_timestamp(node)
            in_range = config.is_in_date_range(timestamp)
            if in_range is None:
                return False
            if in_range is False:
                return True
            caption = self._extract_caption(node)
            if not config.matches_hashtags(caption):
                return True
            shortcode = self._extract_shortcode(node)
            if not shortcode:
                return True
            entry = self._metrics_entry_from_node(node)
            metrics_index[shortcode] = self._merge_metrics_entry(metrics_index.get(shortcode), entry)
            return True

        has_auth = bool(self.cookies.get("sessionid"))
        if not has_auth:
            data = self.fetch_profile_info(
                config.username,
                config.delay_seconds,
                request_timeout=metrics_request_timeout,
            )
            if not data:
                return metrics_index
            pages_scanned = 1
            for node, _ in self._iter_posts_from_profile_info(data):
                keep_going = _process_node(node)
                if not keep_going:
                    break
            self._emit_progress(
                progress_cb,
                phase="metrics_profile_page",
                pages_scanned=pages_scanned,
                posts_checked=posts_checked,
                matched_posts=len(metrics_index),
            )
            return metrics_index

        cursor: str | None = None
        while True:
            pages_scanned += 1
            if pages_scanned > max_pages_limit:
                break
            if time.monotonic() >= timeout_deadline:
                logger.warning(
                    "Instagram metrics index timeout reached for @%s after %d pages",
                    config.username,
                    pages_scanned - 1,
                )
                break
            data = self.fetch_posts_graphql(
                config.username,
                cursor,
                config.delay_seconds,
                request_timeout=metrics_request_timeout,
            )
            if not data:
                break
            page_info: dict[str, Any] = {}
            page_matches = 0
            for node, pi in self._iter_posts_from_graphql(data):
                page_info = pi
                keep_going = _process_node(node)
                if not keep_going:
                    self._emit_progress(
                        progress_cb,
                        phase="metrics_graphql_page",
                        pages_scanned=pages_scanned,
                        posts_checked=posts_checked,
                        matched_posts=len(metrics_index),
                    )
                    return metrics_index
                shortcode = self._extract_shortcode(node)
                if shortcode and shortcode in metrics_index:
                    page_matches += 1
            if no_match_page_limit > 0 and page_matches == 0 and (config.date_start or config.date_end):
                no_match_pages += 1
                if no_match_pages >= no_match_page_limit:
                    break
            elif page_matches > 0:
                no_match_pages = 0
            self._emit_progress(
                progress_cb,
                phase="metrics_graphql_page",
                pages_scanned=pages_scanned,
                posts_checked=posts_checked,
                matched_posts=len(metrics_index),
            )
            has_next = bool(page_info.get("has_next_page"))
            next_cursor = str(page_info.get("end_cursor") or "").strip() or None
            if next_cursor and next_cursor in seen_cursors:
                logger.warning(
                    "Instagram metrics index detected repeating cursor for @%s; stopping pagination",
                    config.username,
                )
                break
            cursor = next_cursor
            if cursor:
                seen_cursors.add(cursor)
            if not has_next or not cursor:
                break
        return metrics_index

    def scrape(
        self,
        config: ScrapeConfig,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[InstagramPost]:
        """
        Scrape posts from an Instagram profile with filtering.

        Args:
            config: ScrapeConfig with username, hashtags, date range, etc.

        Returns:
            List of InstagramPost objects matching the filters.
        """
        scrape_start = time.monotonic()

        logger.info(
            "Starting scrape for @%s (mode=%s, fast=%s)",
            config.username,
            config.scrape_mode,
            config.fast_mode,
        )
        if config.hashtags:
            logger.info("Filtering by hashtags: %s", config.hashtags)
        if config.date_start or config.date_end:
            logger.info("Date range: %s to %s", config.date_start, config.date_end)

        has_auth = bool(self.cookies.get("sessionid"))

        # ── Auth gate (with auto-refresh) ─────────────────────────────
        auto_refresh_result: dict[str, Any] | None = None

        if not has_auth:
            auto_refresh_result = self._try_auto_refresh_cookies()
            if auto_refresh_result.get("refreshed"):
                has_auth = True
            else:
                interactive_result = self._try_interactive_login()
                if interactive_result.get("refreshed"):
                    auto_refresh_result = interactive_result
                    has_auth = True
            if not has_auth and config.require_auth:
                logger.warning(
                    "[instagram] scrape aborted for @%s: no sessionid, auto-refresh failed (%s)",
                    config.username,
                    auto_refresh_result.get("reason"),
                )
                self.last_retrieval_meta = self._auth_failure_meta(
                    scrape_start,
                    mode="auth_required",
                    code="instagram_auth_required",
                    cls="InstagramAuthRequired",
                    auto_refresh=auto_refresh_result,
                )
                return []

        if has_auth and config.require_auth:
            auth_check = self._validate_cookies()
            if not auth_check["valid"]:
                auto_refresh_result = self._try_auto_refresh_cookies()
                if auto_refresh_result.get("refreshed"):
                    auth_check = self._validate_cookies()
                if not auth_check["valid"]:
                    interactive_result = self._try_interactive_login()
                    if interactive_result.get("refreshed"):
                        auto_refresh_result = interactive_result
                        auth_check = self._validate_cookies()
                if not auth_check["valid"]:
                    logger.warning(
                        "[instagram] scrape aborted for @%s: cookies invalid (%s)",
                        config.username,
                        auth_check.get("reason"),
                    )
                    self.last_retrieval_meta = self._auth_failure_meta(
                        scrape_start,
                        mode="auth_validation_failed",
                        code="instagram_auth_invalid",
                        cls="InstagramAuthInvalid",
                        auto_refresh=auto_refresh_result,
                        validation_reason=auth_check.get("reason"),
                    )
                    return []

        # ── Route to the requested scrape mode ─────────────────────────
        if config.scrape_mode == "browser_intercept" and has_auth:
            posts = self._scrape_browser_intercept(config, progress_cb=progress_cb)
            self._finalize_scrape_meta(scrape_start, config)
            return posts

        if config.scrape_mode == "auto" and has_auth:
            posts = self._scrape_graphql(config, progress_cb=progress_cb)
            if not posts and self.last_retrieval_meta.get("initial_page_failed"):
                logger.warning(
                    "Instagram GraphQL initial page failed for @%s; trying browser_intercept mode",
                    config.username,
                )
                posts = self._scrape_browser_intercept(config, progress_cb=progress_cb)
                if posts:
                    self._finalize_scrape_meta(scrape_start, config)
                    return posts
                # Final fallback to profile-info mode
                posts = self._scrape_profile_info(config, progress_cb=progress_cb)
                self._finalize_scrape_meta(scrape_start, config)
                return posts
            self._finalize_scrape_meta(scrape_start, config)
            return posts

        # Default: graphql mode (original behavior)
        if has_auth:
            posts = self._scrape_graphql(config, progress_cb=progress_cb)
            # If the very first authenticated page fails, degrade gracefully to profile-info mode.
            if not posts and self.last_retrieval_meta.get("initial_page_failed"):
                fallback_reason = self.last_retrieval_meta.get("fallback_reason") or "graphql_initial_page_failed"
                logger.warning(
                    "Instagram GraphQL initial page failed for @%s; falling back to profile-info mode (%s)",
                    config.username,
                    fallback_reason,
                )
                posts = self._scrape_profile_info(config, progress_cb=progress_cb)
                self.last_retrieval_meta["retrieval_mode"] = "profile_info_fallback"
                self.last_retrieval_meta["fallback_reason"] = fallback_reason
                self.last_retrieval_meta["first_page_count"] = len(posts)
            self._finalize_scrape_meta(scrape_start, config)
            return posts
        posts = self._scrape_profile_info(config, progress_cb=progress_cb)
        self._finalize_scrape_meta(scrape_start, config)
        return posts

    def _finalize_scrape_meta(self, scrape_start: float, config: ScrapeConfig) -> None:
        """Append wall-clock timing and timeout status to retrieval meta."""
        elapsed = time.monotonic() - scrape_start
        timed_out = elapsed >= config.max_scrape_seconds
        self.last_retrieval_meta["scrape_elapsed_seconds"] = round(elapsed, 1)
        self.last_retrieval_meta["timed_out"] = timed_out
        if timed_out:
            logger.warning(
                "[instagram] scrape timed out after %.0fs (limit: %.0fs)",
                elapsed,
                config.max_scrape_seconds,
            )

    def _auth_failure_meta(
        self,
        scrape_start: float,
        *,
        mode: str,
        code: str,
        cls: str,
        auto_refresh: dict[str, Any] | None,
        validation_reason: str | None = None,
    ) -> dict[str, Any]:
        """Build a retrieval-meta dict for an auth-gate abort."""
        meta: dict[str, Any] = {
            "retrieval_mode": mode,
            "error_code": code,
            "error_class": cls,
            "retryable": False,
            "cookies_valid": False,
            "cookies_present": bool(self.cookies),
            "sessionid_present": bool(self.cookies.get("sessionid")),
            "auto_refresh_attempted": bool(auto_refresh),
            "auto_refresh_reason": (auto_refresh or {}).get("reason"),
            "scrape_elapsed_seconds": round(time.monotonic() - scrape_start, 1),
            "timed_out": False,
        }
        if validation_reason:
            meta["cookies_validation_reason"] = validation_reason
        return meta

    def _scrape_profile_info(
        self,
        config: ScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[InstagramPost]:
        """Scrape using public profile info API (limited results)."""
        logger.info("Using profile info API (unauthenticated, limited to ~12 posts)")

        data = self.fetch_profile_info(config.username, config.delay_seconds)
        if not data:
            return []

        posts = []
        posts_checked = 0
        total_posts = self._extract_profile_total_posts(data, source="profile_info")
        for node, _ in self._iter_posts_from_profile_info(data):
            posts_checked += 1
            timestamp = self._extract_timestamp(node)

            # Check date range
            in_range = config.is_in_date_range(timestamp)
            if in_range is None:  # Before range
                break
            if in_range is False:  # After range
                continue

            # Check hashtag filter
            caption = self._extract_caption(node)
            if config.matches_hashtags(caption):
                post = self._parse_post_node(node, config)
                posts.append(post)
                logger.info("Found: %s (%s)", post.shortcode, post.date_time)
            self._emit_progress(
                progress_cb,
                phase="scrape_profile_page",
                pages_scanned=1,
                posts_checked=posts_checked,
                matched_posts=len(posts),
                total_posts=total_posts,
            )

        profile_avatar_backfilled_posts = self._backfill_post_owner_profile_pic(
            posts,
            profile_pic_url=self._extract_profile_avatar_from_profile_payload(data),
        )
        logger.info("Scrape complete: %d posts found", len(posts))
        self.last_retrieval_meta = {
            "retrieval_mode": "profile_info",
            "first_page_count": len(posts),
            "fallback_reason": None,
            "initial_page_failed": False,
            "profile_avatar_backfilled_posts": profile_avatar_backfilled_posts,
        }
        if total_posts:
            self.last_retrieval_meta["total_posts"] = total_posts
        return posts

    def _scrape_graphql(
        self,
        config: ScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[InstagramPost]:
        """Scrape using GraphQL API with full pagination."""
        logger.info("Using GraphQL API (authenticated, full pagination)")
        _t0 = time.monotonic()

        posts = []
        cursor = None
        page_num = 0
        posts_checked = 0
        total_posts: int | None = None
        profile_info_total_posts: int | None = None
        reached_date_limit = False
        initial_page_failed = False
        failure_reason: str | None = None
        stop_reason: str | None = None
        no_match_pages = 0
        no_match_page_limit = self._resolve_no_match_page_limit(config)
        seen_cursors: set[str] = set()
        self._pagination_session_rotated = False
        self._auto_rotation_attempted_this_scrape = False
        rotation_attempts = 0
        rotation_successes = 0

        profile_info_data = self.fetch_profile_info(config.username, config.delay_seconds)
        if profile_info_data:
            profile_info_total_posts = self._extract_profile_total_posts(profile_info_data, source="profile_info")
            if profile_info_total_posts is not None:
                total_posts = profile_info_total_posts

        while not reached_date_limit:
            page_num += 1
            if config.max_pages and page_num > config.max_pages:
                logger.info("Reached max pages limit (%s)", config.max_pages)
                stop_reason = "max_pages_reached"
                break
            if time.monotonic() - _t0 > config.max_scrape_seconds:
                logger.warning(
                    "[instagram] graphql pagination timed out after %.0fs (limit: %.0fs)",
                    time.monotonic() - _t0,
                    config.max_scrape_seconds,
                )
                stop_reason = "timeout"
                break

            logger.info("Fetching page %d%s", page_num, " [fast]" if config.fast_mode else "")
            data = self.fetch_posts_graphql(
                config.username,
                cursor,
                config.delay_seconds,
                fast_mode=config.fast_mode,
            )
            if not data:
                # Attempt one session rotation on auth failure before giving up.
                # Uses interactive Chrome login (pop-up) when running locally —
                # no SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH env var needed.
                error_code = self.last_retrieval_meta.get("error_code", "")
                if (
                    error_code in {
                        "instagram_graphql_cursor_unauthorized",
                        "instagram_graphql_cursor_forbidden",
                    }
                    and not self._pagination_session_rotated
                    and self._is_local_environment()
                ):
                    self._pagination_session_rotated = True  # guard before attempt — don't retry even if refresh fails
                    logger.warning(
                        "Auth failure mid-pagination on page %d (%s) — opening Chrome for session refresh",
                        page_num, error_code,
                    )
                    rotation_attempts += 1
                    refresh = self._try_interactive_login()
                    if refresh.get("refreshed"):
                        rotation_successes += 1
                        self._reset_request_session()
                        self._profile_page_context_cache.pop(config.username, None)
                        page_num -= 1  # retry same page with fresh cookies
                        continue
                if page_num == 1:
                    initial_page_failed = True
                    failure_reason = "graphql_empty_or_error"
                stop_reason = "graphql_empty_or_error"
                break

            if total_posts is None:
                total_posts = self._extract_profile_total_posts(data, source="graphql") or profile_info_total_posts

            page_info = {}
            posts_on_page = 0
            page_matches = 0

            for node, pi in self._iter_posts_from_graphql(data):
                page_info = pi
                posts_checked += 1
                posts_on_page += 1

                timestamp = self._extract_timestamp(node)

                # Check date range
                in_range = config.is_in_date_range(timestamp)
                if in_range is None:  # Before range
                    reached_date_limit = True
                    stop_reason = "date_start_reached"
                    break
                if in_range is False:  # After range
                    continue

                # Check hashtag filter
                caption = self._extract_caption(node)
                if config.matches_hashtags(caption):
                    post = self._parse_post_node(node, config)
                    posts.append(post)
                    page_matches += 1
                    logger.info(
                        "Found #%d: %s (%s) - %s - %s likes",
                        len(posts),
                        post.shortcode,
                        post.date_time,
                        getattr(post, "post_type", "?"),
                        getattr(post, "likes", 0),
                    )

            self._emit_progress(
                progress_cb,
                phase="scrape_graphql_page",
                pages_scanned=page_num,
                posts_checked=posts_checked,
                matched_posts=len(posts),
                total_posts=total_posts,
            )

            if posts_on_page == 0:
                logger.info("No more posts found")
                stop_reason = "no_more_posts"
                break

            if no_match_page_limit > 0 and page_matches == 0 and (config.date_start or config.date_end):
                no_match_pages += 1
                if no_match_pages >= no_match_page_limit:
                    logger.info(
                        "Stopping GraphQL crawl after %d consecutive no-match pages (limit=%d)",
                        no_match_pages,
                        no_match_page_limit,
                    )
                    stop_reason = "no_match_page_limit_reached"
                    break
            elif page_matches > 0:
                no_match_pages = 0

            # Get next page
            has_next = page_info.get("has_next_page", False)
            next_cursor = str(page_info.get("end_cursor") or "").strip() or None
            if next_cursor and next_cursor in seen_cursors:
                logger.warning(
                    "Instagram GraphQL pagination detected repeating cursor for @%s; stopping pagination",
                    config.username,
                )
                stop_reason = "repeating_cursor"
                break
            cursor = next_cursor
            if cursor:
                seen_cursors.add(cursor)
            if not has_next or not cursor:
                logger.info("No more pages available")
                stop_reason = "no_more_pages"
                break

            logger.info("Page %d: checked %d posts, %d matches total", page_num, posts_on_page, len(posts))

            # Proactive cookie health check every 20 pages
            if page_num % 20 == 0:
                cookie_check = self._validate_cookies()
                if not cookie_check["valid"]:
                    logger.warning(
                        "[instagram] cookies expired mid-pagination at page %d — attempting refresh",
                        page_num,
                    )
                    rotation_attempts += 1
                    refresh = self._try_auto_refresh_cookies()
                    if refresh.get("refreshed"):
                        rotation_successes += 1
                        self._reset_request_session()
                        self._profile_page_context_cache.pop(config.username, None)

        logger.info("Scrape complete: checked %d posts, found %d matches", posts_checked, len(posts))
        profile_avatar_backfilled_posts = 0
        if posts and any(not getattr(post, "owner_profile_pic_url", None) for post in posts):
            profile_data = profile_info_data or self.fetch_profile_info(config.username, config.delay_seconds)
            profile_avatar_backfilled_posts = self._backfill_post_owner_profile_pic(
                posts,
                profile_pic_url=self._extract_profile_avatar_from_profile_payload(profile_data),
            )
        self.last_retrieval_meta = {
            "retrieval_mode": "graphql",
            "first_page_count": len(posts) if page_num <= 1 else min(len(posts), 12),
            "fallback_reason": failure_reason,
            "initial_page_failed": initial_page_failed,
            "pages_scanned": page_num,
            "posts_checked": posts_checked,
            "stop_reason": stop_reason,
            "no_match_pages": no_match_pages,
            "no_match_page_limit": no_match_page_limit,
            "profile_avatar_backfilled_posts": profile_avatar_backfilled_posts,
            "rotation_attempts": rotation_attempts,
            "rotation_successes": rotation_successes,
        }
        if total_posts:
            self.last_retrieval_meta["total_posts"] = total_posts
        return posts

    def _scrape_browser_intercept(
        self,
        config: ScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[InstagramPost]:
        """Scrape by scrolling a headless browser and intercepting GraphQL responses.

        Replicates the Sort Feed Chrome extension technique: navigate to the profile,
        auto-scroll, and capture post data from the GraphQL responses Instagram sends
        during its own infinite-scroll pagination.  Zero rate-limiting overhead because
        requests originate from a real browser session.
        """
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            logger.error("Playwright not available for browser_intercept mode: %s", exc)
            self.last_retrieval_meta.update(
                {
                    "retrieval_mode": "browser_intercept",
                    "error_code": "playwright_unavailable",
                    "error_class": type(exc).__name__,
                }
            )
            return []

        logger.info(
            "Starting browser_intercept scrape for @%s (Sort Feed technique)",
            config.username,
        )

        posts: list[InstagramPost] = []
        seen_pks: set[str] = set()
        reached_date_limit = False
        no_new_data_scrolls = 0
        max_no_new_data_scrolls = 5
        scroll_count = 0
        total_posts: int | None = None

        user_agent = self._get_headers().get("user-agent", "Mozilla/5.0")
        timeout_ms = 60_000
        max_posts = config.max_pages * 50 if config.max_pages else 10_000

        with sync_playwright() as playwright:
            try:
                with self._browser_session_manager.account_context(
                    playwright=playwright,
                    account_id=self._resolved_browser_account_id(config.username),
                    headless=True,
                    viewport={"width": 1280, "height": 900},
                    user_agent=user_agent,
                    seed_cookies=self._request_cookies(),
                ) as browser_session:
                    context = browser_session.context
                    page = context.new_page()

                    # Navigate to profile
                    page.goto(
                        f"https://www.instagram.com/{config.username}/",
                        wait_until="domcontentloaded",
                        timeout=timeout_ms,
                    )
                    page.wait_for_timeout(2_000)

                    # Register response interceptor for ALL GraphQL paginated responses
                    def _handle_graphql_response(response: Any) -> None:
                        nonlocal total_posts, reached_date_limit, no_new_data_scrolls
                        try:
                            resp_url = str(response.url or "")
                            if "/graphql" not in resp_url and "/api/graphql" not in resp_url:
                                return
                            if not response.ok:
                                return
                            payload = response.json()
                            data_obj = payload.get("data", {})
                            # Try the known connection key first, then scan for
                            # any key containing "timeline" or "feed" as a
                            # fallback in case Instagram renamed the field.
                            connection = data_obj.get("xdt_api__v1__feed__user_timeline_graphql_connection")
                            if not connection:
                                for key in data_obj:
                                    if "timeline" in key.lower() or "feed" in key.lower():
                                        candidate = data_obj[key]
                                        if isinstance(candidate, dict) and (
                                            "edges" in candidate or "page_info" in candidate
                                        ):
                                            connection = candidate
                                            logger.info("browser_intercept: matched alt connection key '%s'", key)
                                            break
                            if not connection:
                                return
                            edges = connection.get("edges") or []
                            if not edges:
                                return

                            # Extract total post count if available
                            if total_posts is None:
                                count_val = self._coerce_int(connection.get("count"), default=0)
                                if count_val > 0:
                                    total_posts = count_val

                            new_posts_this_batch = 0
                            for edge in edges:
                                node = edge.get("node", {})
                                pk = str(node.get("pk") or node.get("id", ""))
                                if not pk or pk in seen_pks:
                                    continue
                                seen_pks.add(pk)

                                # Check date range
                                timestamp = self._extract_timestamp(node)
                                in_range = config.is_in_date_range(timestamp)
                                if in_range is None:  # Before date range — stop
                                    reached_date_limit = True
                                    return
                                if in_range is False:  # After date range — skip
                                    continue

                                # Check hashtag filter
                                caption = self._extract_caption(node)
                                if config.matches_hashtags(caption):
                                    post = self._parse_post_node(node, config)
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
                                    posts_checked=len(seen_pks),
                                    matched_posts=len(posts),
                                    total_posts=total_posts,
                                )
                        except Exception:  # noqa: BLE001
                            logger.debug("browser_intercept: failed to parse GraphQL response", exc_info=True)

                    page.on("response", _handle_graphql_response)

                    # Auto-scroll loop
                    scroll_interval_ms = 600  # Sort Feed-style fast scrolling
                    _scroll_t0 = time.monotonic()
                    while (
                        not reached_date_limit
                        and no_new_data_scrolls < max_no_new_data_scrolls
                        and (time.monotonic() - _scroll_t0) < config.max_scrape_seconds
                    ):
                        if len(posts) >= max_posts:
                            logger.info("browser_intercept: reached max posts (%d)", max_posts)
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

                        if scroll_count % 20 == 0:
                            logger.info(
                                "browser_intercept: scrolled %d times, %d posts collected",
                                scroll_count,
                                len(posts),
                            )
            except Exception as exc:
                logger.error("browser_intercept failed for @%s: %s", config.username, exc)
                self.last_retrieval_meta = {
                    "retrieval_mode": "browser_intercept",
                    "error_code": "browser_intercept_error",
                    "error_class": type(exc).__name__,
                    "error_message": str(exc),
                    "posts_checked": len(seen_pks),
                    "pages_scanned": scroll_count,
                }
                if total_posts:
                    self.last_retrieval_meta["total_posts"] = total_posts
                return posts

        _scroll_elapsed = time.monotonic() - _scroll_t0 if "_scroll_t0" in dir() else 0
        scroll_timed_out = _scroll_elapsed >= config.max_scrape_seconds
        if scroll_timed_out:
            logger.warning(
                "browser_intercept: scroll loop timed out after %.0fs (limit: %.0fs), %d posts collected",
                _scroll_elapsed, config.max_scrape_seconds, len(posts),
            )
        stop_reason = (
            "date_start_reached"
            if reached_date_limit
            else "max_posts_reached"
            if len(posts) >= max_posts
            else "timeout"
            if scroll_timed_out
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
        self.last_retrieval_meta = {
            "retrieval_mode": "browser_intercept",
            "posts_checked": len(seen_pks),
            "pages_scanned": scroll_count,
            "stop_reason": stop_reason,
        }
        if total_posts:
            self.last_retrieval_meta["total_posts"] = total_posts
        return posts


def load_cookies_from_file(filepath: str) -> dict:
    """Load Instagram cookies from a JSON file."""
    with open(filepath) as f:
        cookies = json.load(f)
    # Remove comment fields
    return {k: v for k, v in cookies.items() if not k.startswith("_")}
