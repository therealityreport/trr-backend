"""Facebook page/reels/photos scraper with compliant public-first behavior."""

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

_REEL_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/reel/([0-9]+)", re.IGNORECASE)
_PAGE_REELS_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/([^/?#]+)/reels/?", re.IGNORECASE)
_PAGE_PHOTOS_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/([^/?#]+)/photos/?", re.IGNORECASE)
_PAGE_POST_URL_RE = re.compile(r"https://(?:www\.)?facebook\.com/([^/?#]+)/posts/([A-Za-z0-9._-]+)", re.IGNORECASE)
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

    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    @property
    def normalized_handle(self) -> str:
        return str(self.page_handle or "").strip().lstrip("@")

    def in_date_window(self, value: datetime | None) -> bool:
        if value is None:
            return True
        if self.date_start and value < self.date_start:
            return False
        if self.date_end and value > self.date_end:
            return False
        return True


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
    likes: int = 0
    comments: int = 0
    shares: int = 0
    views: int = 0
    posted_at: int | None = None
    url: str = ""
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

    def _rate_limit(self, delay_seconds: float) -> None:
        if self._request_count > 0 and delay_seconds > 0:
            time.sleep(delay_seconds)
        self._request_count += 1

    def _fetch_html(self, url: str, *, delay_seconds: float, referer: str | None = None) -> str:
        self._rate_limit(delay_seconds)
        try:
            response = self.session.get(
                url,
                timeout=(10, 45),
                headers=self._headers(referer=referer),
                cookies=self.cookies,
            )
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

    def _fetch_html_with_playwright(self, url: str, *, delay_seconds: float, referer: str | None = None) -> str:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Playwright fallback requested but playwright is unavailable") from exc

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=self._headers(referer=referer).get("user-agent", ""),
                locale="en-US",
            )
            page = context.new_page()
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
    def _first_group(pattern: re.Pattern[str], text: str) -> str:
        match = pattern.search(text)
        if not match:
            return ""
        return html.unescape(str(match.group(1) or "")).strip()

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
        for match in _PAGE_PHOTOS_URL_RE.finditer(page_html):
            source = f"{self.BASE_URL}/{match.group(1)}/photos"
            if source in seen:
                continue
            seen.add(source)
            pairs.append((source, "photo"))
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

    def _build_post_from_html(self, *, url: str, html_text: str, username: str, post_type_hint: str) -> FacebookPost:
        og_url = self._first_group(_OG_URL_RE, html_text) or url
        post_id = self._first_group(_POST_ID_RE, html_text)
        if not post_id:
            parsed = urlparse(og_url)
            pieces = [piece for piece in parsed.path.split("/") if piece]
            if pieces and pieces[-1].isdigit():
                post_id = pieces[-1]
            else:
                post_id = parsed.path.strip("/") or f"{username}-{abs(hash(url))}"
        title = self._first_group(_OG_TITLE_RE, html_text)
        desc = self._first_group(_OG_DESC_RE, html_text)
        image_url = self._first_group(_OG_IMAGE_RE, html_text)
        published_iso = self._first_group(_PUBLISHED_TIME_RE, html_text)
        posted_at = self._to_epoch(published_iso)
        caption = desc or title
        media_urls = [image_url] if image_url else []
        post_type = post_type_hint
        if "/reel/" in og_url:
            post_type = "reel"
        elif "/photos" in og_url:
            post_type = "photo"
        return FacebookPost(
            post_id=post_id,
            username=username,
            post_type=post_type or "feed",
            caption=caption,
            media_urls=media_urls,
            thumbnail_url=image_url or None,
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

        for surface_url in self._surface_urls(handle, config):
            if config.max_pages is not None and pages_scanned >= max(1, int(config.max_pages)):
                break
            try:
                html_text = self._fetch_html(surface_url, delay_seconds=config.delay_seconds)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[facebook] failed to fetch %s: %s", surface_url, exc)
                continue
            pages_scanned += 1
            candidates = self._extract_post_urls(html_text, handle=handle)
            for candidate_url, kind in candidates:
                if config.max_pages is not None and posts_checked >= max(1, int(config.max_pages)) * 100:
                    break
                posts_checked += 1
                try:
                    post_html = self._fetch_html(candidate_url, delay_seconds=config.delay_seconds, referer=surface_url)
                except Exception:
                    continue
                post = self._build_post_from_html(
                    url=candidate_url,
                    html_text=post_html,
                    username=handle,
                    post_type_hint=kind,
                )
                if not post.post_id or post.post_id in seen_ids:
                    continue
                posted_dt = (
                    datetime.fromtimestamp(post.posted_at, tz=UTC) if isinstance(post.posted_at, int) else None
                )
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

        self.last_retrieval_meta = {
            "source": "public_meta_fallback",
            "pages_scanned": pages_scanned,
            "posts_checked": posts_checked,
            "matched_posts": matched_posts,
            "cookies_supplied": bool(self.cookies),
        }
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

        post_url = (
            post_url_or_id
            if post_url_or_id.startswith("http")
            else f"{self.BASE_URL}/reel/{post_url_or_id}"
        )

        try:
            html_text = self._fetch_html(post_url, delay_seconds=delay_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[facebook] comment fetch failed for %s: %s", post_url, exc)
            self.last_comment_fetch_reason = "facebook_fetch_failed"
            return []

        comments = self._extract_comments_from_ssr(html_text, max_comments=max_comments)
        self.last_comment_fetch_reason = (
            "facebook_ssr_comments_ok" if comments else "facebook_no_ssr_comments"
        )
        return comments

    def _extract_comments_from_ssr(
        self, html_text: str, *, max_comments: int = 100
    ) -> list[FacebookComment]:
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
                body_text = html.unescape(
                    json.loads(f'"{raw_body}"')
                )
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
