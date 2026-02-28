"""Meta Threads scraper with GraphQL API extraction and OG-tag fallback."""

from __future__ import annotations

import html
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_POST_URL_RE = re.compile(r"https://(?:www\.)?threads\.com/@([A-Za-z0-9._]+)/post/([A-Za-z0-9_-]+)", re.IGNORECASE)
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
_LIKES_RE = re.compile(r'"like_count"\s*:\s*([0-9]+)')
_REPLIES_RE = re.compile(r'"reply_count"\s*:\s*([0-9]+)')
_REPOSTS_RE = re.compile(r'"repost_count"\s*:\s*([0-9]+)')
_QUOTES_RE = re.compile(r'"quote_count"\s*:\s*([0-9]+)')

# Threads GraphQL API constants
_THREADS_GRAPHQL_URL = "https://www.threads.com/graphql/query"
_THREADS_IG_APP_ID = "238260118697367"
_THREADS_PROFILE_POSTS_DOC_ID = "25806288305730982"
_THREADS_DEFAULT_PAGE_SIZE = 12

# Relay internal provider flags required by the GraphQL query
_RELAY_PROVIDER_DEFAULTS: dict[str, bool] = {
    "__relay_internal__pv__BarcelonaIsLoggedInrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasProfileSelfReplyContextrelayprovider": True,
    "__relay_internal__pv__BarcelonaIsReplyApprovalEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasDearAlgoConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasEventBadgerelayprovider": False,
    "__relay_internal__pv__BarcelonaIsReplyApprovalsConsumptionEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaIsSearchDiscoveryEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPodcastConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunitiesrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasGameScoreSharerelayprovider": True,
    "__relay_internal__pv__BarcelonaHasSelfThreadCountrelayprovider": True,
    "__relay_internal__pv__IsTagIndicatorEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasDeepDiverelayprovider": False,
    "__relay_internal__pv__BarcelonaHasGhostPostConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasSpoilerStylingInforelayprovider": True,
    "__relay_internal__pv__BarcelonaHasGhostPostEmojiActivationrelayprovider": False,
    "__relay_internal__pv__BarcelonaOptionalCookiesEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasDearAlgoWebProductionrelayprovider": True,
    "__relay_internal__pv__BarcelonaQuotedPostUFIEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasTopicTagsrelayprovider": True,
    "__relay_internal__pv__BarcelonaIsCrawlerrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasDisplayNamesrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunityTopContributorsrelayprovider": False,
    "__relay_internal__pv__BarcelonaCanSeeSponsoredContentrelayprovider": False,
    "__relay_internal__pv__BarcelonaShouldShowFediverseM075Featuresrelayprovider": True,
    "__relay_internal__pv__BarcelonaImplicitTrendsGKrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsInternalUserrelayprovider": False,
}

# Regex patterns for extracting tokens from Threads page HTML
_DTSG_RE = re.compile(r'"DTSGInitialData"[^}]*?"token"\s*:\s*"([^"]+)"')
_LSD_RE = re.compile(r'"LSD"[^}]*?"token"\s*:\s*"([^"]+)"')
_USER_ID_RE = re.compile(r'BarcelonaProfileThreadsTabDirectQueryRelayPreloader[^"]*","queryID":"(\d+)","variables":\{"[^"]*"[^}]*"userID"\s*:\s*"(\d+)"')
_USER_ID_SIMPLE_RE = re.compile(r'"userID"\s*:\s*"(\d+)"')
_JAZOEST_RE = re.compile(r"jazoest[=:](\d+)")


@dataclass
class ThreadsScrapeConfig:
    username: str
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 1.0
    max_pages: int | None = 1

    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    @property
    def normalized_username(self) -> str:
        return str(self.username or "").strip().lstrip("@")

    def in_date_window(self, value: datetime | None) -> bool:
        if value is None:
            return True
        if self.date_start and value < self.date_start:
            return False
        if self.date_end and value > self.date_end:
            return False
        return True


@dataclass
class ThreadsComment:
    comment_id: str
    username: str
    text: str
    likes: int = 0
    created_at: int | None = None
    is_reply: bool = True
    reply_count: int = 0
    parent_source_comment_id: str | None = None
    media_urls: list[str] = field(default_factory=list)
    replies: list[ThreadsComment] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["replies"] = [reply.to_dict() for reply in self.replies]
        return payload


@dataclass
class ThreadsPost:
    post_id: str
    username: str
    text: str
    media_urls: list[str]
    thumbnail_url: str | None
    likes: int = 0
    replies: int = 0
    reposts: int = 0
    quotes: int = 0
    views: int = 0
    posted_at: int | None = None
    url: str = ""
    raw_data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class _PageTokens:
    """CSRF tokens and session values extracted from a Threads page load."""

    fb_dtsg: str
    lsd: str
    jazoest: str
    user_id: str | None = None


class ThreadsScraper:
    BASE_URL = "https://www.threads.com"

    def __init__(self, *, cookies: dict[str, str] | None = None):
        self.cookies = cookies or {}
        self.session = self._create_session()
        self.last_retrieval_meta: dict[str, Any] = {}
        self.last_comment_fetch_reason: str | None = None
        self.comments_auth_failed = False
        self._request_count = 0
        self._page_tokens: _PageTokens | None = None

    # ------------------------------------------------------------------
    # Session & HTTP helpers
    # ------------------------------------------------------------------

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
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

    def _headers(self, *, referer: str | None = None, document: bool = False) -> dict[str, str]:
        headers: dict[str, str] = {
            "accept-language": "en-US,en;q=0.9",
            "user-agent": self._USER_AGENT,
            # Browser fingerprint headers required by Meta to serve full SSR payload
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-ch-ua-platform-version": '"26.4.0"',
            "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Google Chrome";v="145.0.7632.117", "Chromium";v="145.0.7632.117"',
            "sec-ch-ua-model": '""',
            "sec-ch-prefers-color-scheme": "light",
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

    def _graphql_headers(self, *, tokens: _PageTokens, referer: str | None = None) -> dict[str, str]:
        headers = self._headers(referer=referer)
        headers.update(
            {
                "content-type": "application/x-www-form-urlencoded",
                "x-ig-app-id": _THREADS_IG_APP_ID,
                "x-fb-lsd": tokens.lsd,
                "x-csrftoken": tokens.fb_dtsg.split(":")[0] if ":" in tokens.fb_dtsg else self.cookies.get("csrftoken", ""),
                "x-fb-friendly-name": "BarcelonaProfileThreadsTabDirectQuery",
            }
        )
        csrf = self.cookies.get("csrftoken", "")
        if csrf:
            headers["x-csrftoken"] = csrf
        return headers

    def _rate_limit(self, delay_seconds: float) -> None:
        if self._request_count > 0 and delay_seconds > 0:
            time.sleep(delay_seconds)
        self._request_count += 1

    def _fetch_html(
        self,
        url: str,
        *,
        delay_seconds: float,
        referer: str | None = None,
        document: bool = False,
    ) -> str:
        self._rate_limit(delay_seconds)
        response = self.session.get(
            url,
            timeout=(10, 45),
            headers=self._headers(referer=referer, document=document),
            cookies=self.cookies,
        )
        response.raise_for_status()
        return response.text or ""

    # ------------------------------------------------------------------
    # GraphQL API helpers
    # ------------------------------------------------------------------

    def _extract_page_tokens(self, page_html: str) -> _PageTokens | None:
        """Extract fb_dtsg, lsd, jazoest, and userID from page HTML."""
        dtsg_match = _DTSG_RE.search(page_html)
        lsd_match = _LSD_RE.search(page_html)
        if not dtsg_match or not lsd_match:
            logger.debug("[threads] could not extract page tokens from HTML")
            return None
        jazoest_match = _JAZOEST_RE.search(page_html)

        # Extract userID from the profile preloader (most reliable)
        user_id: str | None = None
        preloader_match = _USER_ID_RE.search(page_html)
        if preloader_match:
            user_id = preloader_match.group(2)
        else:
            # Fallback: find userID in profile page data context
            # Skip the first match which is often the viewer's app ID
            for m in _USER_ID_SIMPLE_RE.finditer(page_html):
                candidate = m.group(1)
                # Skip the viewer app ID (17841...)
                if candidate.startswith("17841"):
                    continue
                user_id = candidate
                break

        return _PageTokens(
            fb_dtsg=dtsg_match.group(1),
            lsd=lsd_match.group(1),
            jazoest=jazoest_match.group(1) if jazoest_match else "26474",
            user_id=user_id,
        )

    def _graphql_query(
        self,
        *,
        tokens: _PageTokens,
        doc_id: str,
        variables: dict[str, Any],
        friendly_name: str,
        referer: str | None = None,
        delay_seconds: float = 1.0,
    ) -> dict[str, Any]:
        """Execute a Threads GraphQL query and return parsed JSON."""
        self._rate_limit(delay_seconds)

        body = {
            "av": "17841449018491289",
            "__user": "0",
            "__a": "1",
            "__comet_req": "29",
            "fb_dtsg": tokens.fb_dtsg,
            "jazoest": tokens.jazoest,
            "lsd": tokens.lsd,
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": friendly_name,
            "server_timestamps": "true",
            "variables": json.dumps(variables),
            "doc_id": doc_id,
        }

        headers = self._graphql_headers(tokens=tokens, referer=referer)
        headers["x-fb-friendly-name"] = friendly_name

        response = self.session.post(
            _THREADS_GRAPHQL_URL,
            data=body,
            timeout=(10, 60),
            headers=headers,
            cookies=self.cookies,
        )
        response.raise_for_status()
        return response.json()

    def _fetch_profile_posts_page(
        self,
        *,
        tokens: _PageTokens,
        user_id: str,
        cursor: str | None = None,
        page_size: int = _THREADS_DEFAULT_PAGE_SIZE,
        referer: str | None = None,
        delay_seconds: float = 1.0,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """Fetch one page of profile posts via GraphQL.

        Returns (raw_edges, next_cursor, has_next_page).
        """
        variables: dict[str, Any] = {
            "userID": user_id,
            "first": page_size,
            "allow_page_info_for_lox_user": False,
            "skipGhostPosts": False,
            **_RELAY_PROVIDER_DEFAULTS,
        }
        if cursor:
            variables["after"] = cursor

        result = self._graphql_query(
            tokens=tokens,
            doc_id=_THREADS_PROFILE_POSTS_DOC_ID,
            variables=variables,
            friendly_name="BarcelonaProfileThreadsTabDirectQuery",
            referer=referer,
            delay_seconds=delay_seconds,
        )

        edges = (result.get("data") or {}).get("mediaData", {}).get("edges", [])
        page_info = (result.get("data") or {}).get("mediaData", {}).get("page_info", {})
        next_cursor = page_info.get("end_cursor")
        has_next = bool(page_info.get("has_next_page"))

        return edges, next_cursor, has_next

    # ------------------------------------------------------------------
    # Post parsing (GraphQL response → ThreadsPost)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_media_urls(post_data: dict[str, Any]) -> list[str]:
        """Extract all media URLs from a GraphQL post object."""
        urls: list[str] = []
        seen: set[str] = set()

        def _add(url: str) -> None:
            if url and url not in seen:
                seen.add(url)
                urls.append(url)

        media_type = post_data.get("media_type")

        # Video posts (media_type 2)
        for v in post_data.get("video_versions") or []:
            _add(str(v.get("url") or ""))

        # Image posts (media_type 1) or video thumbnail
        for c in (post_data.get("image_versions2") or {}).get("candidates") or []:
            _add(str(c.get("url") or ""))

        # Carousel posts (media_type 19)
        if media_type == 19:
            for item in post_data.get("carousel_media") or []:
                for v in item.get("video_versions") or []:
                    _add(str(v.get("url") or ""))
                for c in (item.get("image_versions2") or {}).get("candidates") or []:
                    _add(str(c.get("url") or ""))

        return urls

    @staticmethod
    def _extract_thumbnail(post_data: dict[str, Any]) -> str | None:
        """Extract the best thumbnail URL from a post."""
        candidates = (post_data.get("image_versions2") or {}).get("candidates") or []
        if candidates:
            return str(candidates[0].get("url") or "") or None
        return None

    @staticmethod
    def _extract_text(post_data: dict[str, Any]) -> str:
        """Extract post text from text_fragments or caption."""
        tpa = post_data.get("text_post_app_info") or {}
        fragments = (tpa.get("text_fragments") or {}).get("fragments") or []
        text_parts = [str(f.get("plaintext") or "") for f in fragments]
        text = "".join(text_parts).strip()
        if text:
            return text
        caption = post_data.get("caption") or {}
        return str(caption.get("text") or "").strip()

    def _build_post_from_graphql(self, edge: dict[str, Any], *, username: str) -> ThreadsPost | None:
        """Convert a GraphQL edge to a ThreadsPost."""
        thread_items = (edge.get("node") or {}).get("thread_items") or []
        if not thread_items:
            return None
        post_data = thread_items[0].get("post") or {}
        if not post_data:
            return None

        pk = str(post_data.get("pk") or "")
        code = str(post_data.get("code") or "")
        post_user = post_data.get("user") or {}
        post_username = str(post_user.get("username") or username)

        tpa = post_data.get("text_post_app_info") or {}

        media_urls = self._extract_media_urls(post_data)
        thumbnail_url = self._extract_thumbnail(post_data)
        text = self._extract_text(post_data)

        taken_at = post_data.get("taken_at")
        posted_at = int(taken_at) if taken_at is not None else None

        post_url = post_data.get("canonical_url") or ""
        if not post_url and code:
            post_url = f"{self.BASE_URL}/@{post_username}/post/{code}"

        return ThreadsPost(
            post_id=code or pk,
            username=post_username,
            text=text,
            media_urls=media_urls,
            thumbnail_url=thumbnail_url,
            likes=int(post_data.get("like_count") or 0),
            replies=int(tpa.get("direct_reply_count") or 0),
            reposts=int(tpa.get("repost_count") or 0),
            quotes=int(tpa.get("quote_count") or 0),
            views=0,
            posted_at=posted_at,
            url=post_url,
            raw_data={
                "pk": pk,
                "code": code,
                "media_type": post_data.get("media_type"),
                "is_verified": post_user.get("is_verified"),
                "full_name": post_user.get("full_name"),
                "reshare_count": int(tpa.get("reshare_count") or 0),
                "is_reply": tpa.get("is_reply", False),
                "reply_control": tpa.get("reply_control"),
                "source": "threads_graphql_api",
            },
        )

    # ------------------------------------------------------------------
    # OG-tag fallback (legacy public scraping)
    # ------------------------------------------------------------------

    @staticmethod
    def _first_group(pattern: re.Pattern[str], text: str) -> str:
        match = pattern.search(text)
        if not match:
            return ""
        return html.unescape(str(match.group(1) or "")).strip()

    @staticmethod
    def _to_int(value: str) -> int:
        raw = str(value or "").strip()
        if not raw:
            return 0
        try:
            return max(0, int(raw))
        except ValueError:
            return 0

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

    def _extract_post_urls(self, page_html: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for match in _POST_URL_RE.finditer(page_html):
            url = f"{self.BASE_URL}/@{match.group(1)}/post/{match.group(2)}"
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls

    @staticmethod
    def _playwright_discovery_enabled() -> bool:
        raw = (os.getenv("SOCIAL_THREADS_PLAYWRIGHT_DISCOVERY", "true") or "").strip().lower()
        return raw not in {"0", "false", "off", "no"}

    def _discover_posts_with_playwright(
        self,
        *,
        username: str,
        profile_url: str,
        delay_seconds: float,
    ) -> list[dict[str, str]]:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            logger.debug("[threads] playwright unavailable for discovery: %s", exc)
            return []

        discovered: list[dict[str, str]] = []
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=self._headers().get("user-agent", ""),
                locale="en-US",
            )
            page = context.new_page()
            page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
            if delay_seconds > 0:
                page.wait_for_timeout(max(750, int(delay_seconds * 1000)))
            rows = page.evaluate(
                """
                (username) => {
                  const token = `/@${username}/post/`;
                  const links = Array.from(document.querySelectorAll('a[href]'));
                  const seen = new Set();
                  const out = [];
                  for (const link of links) {
                    const href = (link.getAttribute('href') || '').trim();
                    if (!href || !href.includes(token)) continue;
                    const normalizedHref = href.split('?')[0];
                    if (seen.has(normalizedHref)) continue;
                    seen.add(normalizedHref);
                    const article = link.closest('article') || link.closest('[role="article"]') || link.parentElement;
                    let preview = '';
                    if (article) {
                      const candidates = Array.from(article.querySelectorAll('div,span'))
                        .map(el => (el.textContent || '').trim())
                        .filter(Boolean)
                        .filter(text => text.length >= 24)
                        .filter(text => !/^\\d+[smhdwy]$/i.test(text))
                        .filter(text => !/^(Like|Comment|Repost|Share)\\b/i.test(text));
                      candidates.sort((a, b) => b.length - a.length);
                      preview = candidates[0] || '';
                    }
                    out.push({ href: normalizedHref, preview });
                  }
                  return out;
                }
                """,
                username,
            )
            browser.close()

        for row in rows:
            href = str((row or {}).get("href") or "").strip()
            if not href:
                continue
            normalized = href
            if normalized.startswith("/"):
                normalized = f"{self.BASE_URL}{normalized}"
            elif normalized.startswith("http://"):
                normalized = f"https://{normalized[len('http://'):]}"
            if not normalized.startswith("https://www.threads.com/"):
                continue
            normalized = normalized.split("?", 1)[0]
            discovered.append({"url": normalized, "preview": str((row or {}).get("preview") or "").strip()})
        return discovered

    def _build_post_from_html(self, *, url: str, html_text: str, username: str) -> ThreadsPost:
        og_url = self._first_group(_OG_URL_RE, html_text) or url
        post_id = ""
        match = _POST_URL_RE.search(og_url)
        if match:
            post_id = match.group(2)
        if not post_id:
            parsed = urlparse(og_url)
            pieces = [piece for piece in parsed.path.split("/") if piece]
            post_id = pieces[-1] if pieces else f"{username}-{abs(hash(url))}"

        title = self._first_group(_OG_TITLE_RE, html_text)
        desc = self._first_group(_OG_DESC_RE, html_text)
        image_url = self._first_group(_OG_IMAGE_RE, html_text)
        published_iso = self._first_group(_PUBLISHED_TIME_RE, html_text)
        posted_at = self._to_epoch(published_iso)
        text = desc or title
        likes = self._to_int(self._first_group(_LIKES_RE, html_text))
        replies_count = self._to_int(self._first_group(_REPLIES_RE, html_text))
        reposts = self._to_int(self._first_group(_REPOSTS_RE, html_text))
        quotes = self._to_int(self._first_group(_QUOTES_RE, html_text))
        media_urls = [image_url] if image_url else []
        return ThreadsPost(
            post_id=post_id,
            username=username,
            text=text,
            media_urls=media_urls,
            thumbnail_url=image_url or None,
            likes=likes,
            replies=replies_count,
            reposts=reposts,
            quotes=quotes,
            posted_at=posted_at,
            url=og_url,
            raw_data={
                "og_url": og_url,
                "og_title": title,
                "og_description": desc,
                "published_time": published_iso or None,
                "source": "public_meta_fallback",
            },
        )

    # ------------------------------------------------------------------
    # Main scrape entry point
    # ------------------------------------------------------------------

    def _scrape_via_graphql(
        self,
        config: ThreadsScrapeConfig,
        *,
        page_html: str,
        profile_url: str,
        progress_cb: Any | None = None,
    ) -> list[ThreadsPost] | None:
        """Attempt GraphQL-based scraping. Returns None if tokens unavailable."""
        tokens = self._extract_page_tokens(page_html)
        if not tokens:
            logger.info("[threads] page tokens not found, falling back to OG extraction")
            return None

        user_id = tokens.user_id
        if not user_id:
            logger.info("[threads] user_id not found in page, falling back to OG extraction")
            return None

        self._page_tokens = tokens
        username = config.normalized_username
        max_post_limit = max(1, int(config.max_pages or 1)) * 100

        posts: list[ThreadsPost] = []
        seen_ids: set[str] = set()
        cursor: str | None = None
        pages_scanned = 0
        posts_checked = 0
        matched_posts = 0
        stop_pagination = False

        while not stop_pagination:
            pages_scanned += 1
            try:
                edges, next_cursor, has_next = self._fetch_profile_posts_page(
                    tokens=tokens,
                    user_id=user_id,
                    cursor=cursor,
                    referer=profile_url,
                    delay_seconds=config.delay_seconds,
                )
            except Exception:
                logger.warning("[threads] GraphQL page fetch failed at page %d", pages_scanned, exc_info=True)
                break

            if not edges:
                break

            for edge in edges:
                posts_checked += 1
                post = self._build_post_from_graphql(edge, username=username)
                if not post or not post.post_id:
                    continue
                if post.post_id in seen_ids:
                    continue

                posted_dt = datetime.fromtimestamp(post.posted_at, tz=UTC) if isinstance(post.posted_at, int) else None

                # If we have a date_end and post is newer than it, skip but keep paginating
                if posted_dt and config.date_end and posted_dt > config.date_end:
                    continue

                # If we have a date_start and post is older than it, stop pagination
                if posted_dt and config.date_start and posted_dt < config.date_start:
                    stop_pagination = True
                    break

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

                if matched_posts >= max_post_limit:
                    stop_pagination = True
                    break

            cursor = next_cursor
            if not has_next or not cursor:
                break

        self.last_retrieval_meta = {
            "source": "threads_graphql_api",
            "pages_scanned": pages_scanned,
            "posts_checked": posts_checked,
            "matched_posts": matched_posts,
            "cookies_supplied": bool(self.cookies),
            "user_id": user_id,
        }
        return posts

    def _scrape_via_fallback(
        self,
        config: ThreadsScrapeConfig,
        *,
        page_html: str,
        profile_url: str,
        progress_cb: Any | None = None,
    ) -> list[ThreadsPost]:
        """OG-tag / Playwright fallback scraping (legacy path)."""
        username = config.normalized_username
        pages_scanned = 1
        posts_checked = 0
        matched_posts = 0
        posts: list[ThreadsPost] = []
        seen_ids: set[str] = set()
        preview_by_url: dict[str, str] = {}

        candidate_urls = self._extract_post_urls(page_html)
        source = "public_meta_fallback"
        if not candidate_urls and self._playwright_discovery_enabled():
            discovered = self._discover_posts_with_playwright(
                username=username,
                profile_url=profile_url,
                delay_seconds=config.delay_seconds,
            )
            candidate_urls = [
                str(item.get("url") or "").strip()
                for item in discovered
                if str(item.get("url") or "").strip()
            ]
            preview_by_url = {
                str(item.get("url") or "").strip(): str(item.get("preview") or "").strip()
                for item in discovered
                if str(item.get("url") or "").strip()
            }
            if candidate_urls:
                source = "playwright_profile_discovery"

        for candidate_url in candidate_urls:
            posts_checked += 1
            try:
                post_html = self._fetch_html(candidate_url, delay_seconds=config.delay_seconds, referer=profile_url)
            except Exception:
                continue
            post = self._build_post_from_html(url=candidate_url, html_text=post_html, username=username)
            preview_text = preview_by_url.get(candidate_url, "")
            if not post.text and preview_text:
                post.text = preview_text
                post.raw_data["profile_preview"] = preview_text
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
            if config.max_pages is not None and matched_posts >= max(1, int(config.max_pages)) * 100:
                break

        self.last_retrieval_meta = {
            "source": source,
            "pages_scanned": pages_scanned,
            "posts_checked": posts_checked,
            "matched_posts": matched_posts,
            "cookies_supplied": bool(self.cookies),
        }
        return posts

    def scrape(
        self,
        config: ThreadsScrapeConfig,
        *,
        progress_cb: Any | None = None,
    ) -> list[ThreadsPost]:
        username = config.normalized_username
        if not username:
            return []
        profile_url = f"{self.BASE_URL}/@{quote(username)}"

        # Fetch profile page HTML (needed for both paths).
        # document=True adds sec-ch-ua / sec-fetch-* headers that trigger Meta's
        # full SSR payload (with preloader data containing userID + tokens).
        page_html = self._fetch_html(profile_url, delay_seconds=config.delay_seconds, document=True)

        # Try GraphQL API first (requires auth cookies)
        if self.cookies:
            graphql_posts = self._scrape_via_graphql(
                config,
                page_html=page_html,
                profile_url=profile_url,
                progress_cb=progress_cb,
            )
            if graphql_posts is not None:
                logger.info(
                    "[threads] GraphQL scrape for @%s: %d posts found",
                    username,
                    len(graphql_posts),
                )
                return graphql_posts

        # Fall back to OG-tag / Playwright extraction
        logger.info("[threads] using OG-tag fallback scrape for @%s", username)
        return self._scrape_via_fallback(
            config,
            page_html=page_html,
            profile_url=profile_url,
            progress_cb=progress_cb,
        )

    # Instagram mobile UA needed for the text_feed REST API
    _MOBILE_USER_AGENT = (
        "Instagram 517.0.0.0.57 Android (33/13; 420dpi; 1080x2400; "
        "samsung; SM-S918B; e3q; qcom; en_US; 595849780)"
    )

    def fetch_comments(
        self,
        post_url_or_id: str,
        *,
        max_comments: int = 0,
        fetch_replies: bool = True,
        delay_seconds: float = 1.0,
    ) -> list[ThreadsComment]:
        """Fetch comments/replies for a Threads post via the Instagram REST API.

        Uses ``i.instagram.com/api/v1/text_feed/{pk}/replies/`` with cursor-based
        pagination.  The *post_url_or_id* can be the numeric media pk, or a
        full ``https://www.threads.com/…/post/CODE`` URL.  If a URL or
        shortcode is provided, the scraper fetches the post page to resolve
        the numeric pk first.
        """
        self.last_comment_fetch_reason = None
        self.comments_auth_failed = not bool(self.cookies)

        if max_comments == 0:
            return []
        if not self.cookies:
            self.last_comment_fetch_reason = "threads_no_cookies"
            return []

        # Resolve numeric pk
        post_pk = self._resolve_post_pk(post_url_or_id, delay_seconds=delay_seconds)
        if not post_pk:
            self.last_comment_fetch_reason = "threads_pk_resolve_failed"
            return []

        # Paginate replies via Instagram REST API
        comments: list[ThreadsComment] = []
        paging_token: str | None = None
        page = 0
        max_pages = 50  # safety cap

        while len(comments) < max_comments and page < max_pages:
            page += 1
            self._rate_limit(delay_seconds)
            try:
                batch, paging_token, has_more = self._fetch_replies_page(
                    post_pk, paging_token=paging_token
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[threads] replies page %d failed for pk=%s: %s",
                    page, post_pk, exc,
                )
                self.last_comment_fetch_reason = "threads_replies_page_error"
                break

            comments.extend(batch)
            logger.debug(
                "[threads] replies page %d: %d new (%d total) for pk=%s",
                page, len(batch), len(comments), post_pk,
            )

            if not has_more or not paging_token:
                break

        # Trim to max_comments
        if len(comments) > max_comments:
            comments = comments[:max_comments]

        self.last_comment_fetch_reason = "threads_replies_ok" if comments else "threads_no_replies"
        return comments

    def _resolve_post_pk(self, post_url_or_id: str, *, delay_seconds: float) -> str | None:
        """Resolve a Threads post URL, shortcode, or pk to the numeric pk."""
        raw = str(post_url_or_id or "").strip()
        if not raw:
            return None

        # Already a numeric pk
        if raw.isdigit() and len(raw) >= 10:
            return raw

        # URL — fetch the post page and extract pk from preloader data
        if raw.startswith("http"):
            target_url = raw
        else:
            # Assume shortcode
            target_url = f"{self.BASE_URL}/post/{raw}"

        try:
            self._rate_limit(delay_seconds)
            html_text = self._fetch_html(target_url, delay_seconds=0, document=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[threads] failed to fetch post page %s: %s", target_url, exc)
            return None

        pk_match = re.search(r'"post_id"\s*:\s*"(\d+)"', html_text)
        if pk_match:
            return pk_match.group(1)

        # Fallback: look for pk in preloader
        pk_match2 = re.search(r'"pk"\s*:\s*"(\d{15,})"', html_text)
        if pk_match2:
            return pk_match2.group(1)

        logger.warning("[threads] could not resolve pk from %s", target_url)
        return None

    def _fetch_replies_page(
        self,
        post_pk: str,
        *,
        paging_token: str | None = None,
    ) -> tuple[list[ThreadsComment], str | None, bool]:
        """Fetch one page of replies from the Instagram text_feed REST API.

        Returns ``(comments, next_paging_token, has_more)``.
        """
        url = f"https://i.instagram.com/api/v1/text_feed/{post_pk}/replies/"
        if paging_token:
            url += f"?paging_token={paging_token}"

        headers = {
            "user-agent": self._MOBILE_USER_AGENT,
            "x-ig-app-id": _THREADS_IG_APP_ID,
            "x-csrftoken": self.cookies.get("csrftoken", ""),
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
        }

        resp = self.session.get(url, headers=headers, cookies=self.cookies, timeout=(10, 45))
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "ok":
            raise RuntimeError(f"text_feed API error: {data.get('status')}")

        comments: list[ThreadsComment] = []
        for thread in data.get("reply_threads", []):
            for item in thread.get("thread_items", []):
                comment = self._parse_reply_item(item)
                if comment:
                    comments.append(comment)

        next_token = (data.get("paging_tokens") or {}).get("downwards")
        has_more = bool(data.get("downwards_thread_will_continue", False))
        return comments, next_token, has_more

    @staticmethod
    def _parse_reply_item(item: dict[str, Any]) -> ThreadsComment | None:
        """Parse a single thread_item from the text_feed replies response."""
        post = item.get("post")
        if not post:
            return None

        pk = str(post.get("pk", ""))
        if not pk:
            return None

        user = post.get("user") or {}
        caption = post.get("caption") or {}
        tpai = post.get("text_post_app_info") or {}
        reply_to = item.get("reply_to_author") or {}

        # Extract media URLs from image_versions2/video_versions
        media_urls: list[str] = []
        image_versions = post.get("image_versions2") or {}
        candidates = image_versions.get("candidates") or []
        if candidates:
            # Pick the highest resolution
            best = max(candidates, key=lambda c: (c.get("width", 0) * c.get("height", 0)))
            if best.get("url"):
                media_urls.append(best["url"])
        for vid in post.get("video_versions") or []:
            if vid.get("url"):
                media_urls.append(vid["url"])
                break  # just the first video variant

        return ThreadsComment(
            comment_id=pk,
            username=user.get("username", ""),
            text=caption.get("text", ""),
            likes=int(post.get("like_count", 0)),
            created_at=post.get("taken_at"),
            is_reply=True,
            reply_count=int(tpai.get("direct_reply_count", 0)),
            parent_source_comment_id=reply_to.get("username"),
            media_urls=media_urls,
        )
