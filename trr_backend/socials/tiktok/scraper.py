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
import re
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class TikTokScrapeConfig:
    """Configuration for a TikTok scrape operation."""

    username: str
    hashtags: list[str] = field(default_factory=list)
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 2.0
    max_pages: int | None = None  # None = no limit

    # Metadata for tracking
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    @property
    def start_timestamp(self) -> float:
        return self.date_start.timestamp() if self.date_start else 0

    @property
    def end_timestamp(self) -> float:
        return self.date_end.timestamp() if self.date_end else datetime.now().timestamp()

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
    views: int
    url: str
    username: str
    author_nickname: str
    duration: int  # seconds
    music_title: str
    music_author: str

    # Media URLs
    media_urls: list[str] = field(default_factory=list)

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

    def __init__(self, cookies: dict | None = None):
        self.cookies = cookies or {}
        self.session = self._create_session()
        self._request_count = 0
        self.last_retrieval_meta: dict[str, Any] = {}
        self._last_api_fail_reason: str | None = None

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

    def _rate_limit(self, delay: float):
        """Apply rate limiting between requests."""
        if self._request_count > 0:
            logger.debug(f"Rate limiting: waiting {delay}s")
            time.sleep(delay)
        self._request_count += 1

    def _safe_response_json(self, response: requests.Response) -> dict | None:
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
            logger.warning(
                "TikTok returned non-JSON response (status=%s, content-type=%s, reason=%s)",
                response.status_code,
                content_type or "unknown",
                self._last_api_fail_reason,
            )
            return None

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
                netscape = Path(json_path).with_name(
                    Path(json_path).stem + "_netscape.txt"
                )
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
            response = self.session.get(url, headers=headers, cookies=self.cookies)
            response.raise_for_status()
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
            "--playlist-end", str(max_videos),
            url,
        ]

        # Format date boundaries for Python-side filtering
        ds_str = date_start.strftime("%Y%m%d") if date_start else None
        de_str = date_end.strftime("%Y%m%d") if date_end else None

        logger.info(f"yt-dlp listing videos for @{username} (max {max_videos})...")
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120
            )
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
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30
            )
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

        return TikTokPost(
            video_id=video_id,
            date_time=(
                datetime.fromtimestamp(create_time).strftime("%Y-%m-%d %H:%M:%S")
                if create_time
                else ""
            ),
            create_time=create_time,
            description=description,
            hashtags=self._extract_hashtags(description),
            mentions=self._extract_mentions(description),
            likes=data.get("like_count", 0) or 0,
            comments=data.get("comment_count", 0) or 0,
            shares=data.get("repost_count", 0) or 0,
            views=data.get("view_count", 0) or 0,
            url=f"https://www.tiktok.com/@{username}/video/{video_id}" if video_id else "",
            username=username,
            author_nickname=data.get("channel", ""),
            duration=data.get("duration", 0) or 0,
            music_title=data.get("track", ""),
            music_author=data.get("artist", ""),
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _scrape_via_ytdlp(
        self,
        config: TikTokScrapeConfig,
        *,
        max_videos_hint: int | None = None,
        max_posts_hint: int | None = None,
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
            return []

        logger.info("Attempting yt-dlp bulk fallback scraper...")

        # Estimate how many videos to fetch based on date range.
        # @bravotv posts ~10 videos/day; add generous buffer.
        max_videos = 500
        if config.date_start:
            days_back = (datetime.now(tz=UTC) - config.date_start).days
            max_videos = max(500, min(5000, days_back * 15))
        if max_videos_hint is not None:
            max_videos = min(max_videos, max(50, max_videos_hint))

        url = f"https://www.tiktok.com/@{config.username}"
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--dump-json",
            "--playlist-end", str(max_videos),
        ]

        # Pass cookies if a Netscape-format file exists
        cookie_file = self._find_ytdlp_cookie_file()
        if cookie_file:
            cmd.extend(["--cookies", cookie_file])
            logger.info(f"yt-dlp using cookies from {cookie_file}")

        cmd.append(url)

        logger.info(f"yt-dlp bulk listing @{config.username} (up to {max_videos} videos)...")
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True,
                timeout=max(120, max_videos // 3),
            )
        except subprocess.TimeoutExpired:
            logger.warning("yt-dlp bulk listing timed out")
            return []

        posts: list[TikTokPost] = []
        total = 0
        for line in proc.stdout.strip().splitlines():
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1

            ts = data.get("timestamp", 0) or 0
            in_range = config.is_in_date_range(ts)
            if in_range is None:
                break  # Before date range, stop
            if in_range is False:
                continue  # After date range, skip

            description = data.get("title", "") or data.get("description", "") or ""
            if not config.matches_hashtags(description):
                continue

            post = self._parse_ytdlp_metadata(data, config)
            posts.append(post)
            logger.info(
                f"Found #{len(posts)}: {post.video_id} ({post.date_time}) "
                f"- {post.views:,} views"
            )

            if max_posts_hint and len(posts) >= max_posts_hint:
                break

        logger.info(f"yt-dlp bulk: scanned {total} videos, found {len(posts)} matches")
        return posts

    def _extract_hashtags(self, text: str) -> list[str]:
        """Extract hashtags from text."""
        return re.findall(r"#(\w+)", text)

    def _extract_mentions(self, text: str) -> list[str]:
        """Extract @mentions from text."""
        return re.findall(r"@(\w+)", text)

    def _parse_post_item(self, item: dict, config: TikTokScrapeConfig) -> TikTokPost:
        """Parse a post item into TikTokPost."""
        video_id = item.get("id", "")
        create_time = item.get("createTime", 0)
        description = item.get("desc", "")

        # Author info
        author = item.get("author", {})
        username = author.get("uniqueId", config.username)
        nickname = author.get("nickname", "")

        # Stats
        stats = item.get("stats", {})

        # Music info
        music = item.get("music", {})

        # Video info
        video = item.get("video", {})
        duration = video.get("duration", 0)

        return TikTokPost(
            video_id=video_id,
            date_time=datetime.fromtimestamp(create_time).strftime("%Y-%m-%d %H:%M:%S") if create_time else "",
            create_time=create_time,
            description=description,
            hashtags=self._extract_hashtags(description),
            mentions=self._extract_mentions(description),
            likes=stats.get("diggCount", 0),
            comments=stats.get("commentCount", 0),
            shares=stats.get("shareCount", 0),
            views=stats.get("playCount", 0),
            url=f"https://www.tiktok.com/@{username}/video/{video_id}" if video_id else "",
            username=username,
            author_nickname=nickname,
            duration=duration,
            music_title=music.get("title", ""),
            music_author=music.get("authorName", ""),
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def fetch_user_detail(self, username: str, delay: float = 2.0) -> dict | None:
        """Fetch user detail to get secUid needed for post list."""
        self._rate_limit(delay)

        params = {"uniqueId": username}
        headers = self._get_headers(f"https://www.tiktok.com/@{username}")

        try:
            response = self.session.get(self.USER_DETAIL_URL, params=params, headers=headers, cookies=self.cookies)
            response.raise_for_status()
            parsed = self._safe_response_json(response)
            if parsed is None:
                return None
            return parsed
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch user detail for @{username}: {e}")
            self._last_api_fail_reason = "request_error"
            return None

    def fetch_posts(self, username: str, sec_uid: str, cursor: int = 0, delay: float = 2.0) -> dict | None:
        """Fetch posts from user's profile."""
        self._rate_limit(delay)

        params = {
            "secUid": sec_uid,
            "count": 30,
            "cursor": cursor,
        }
        headers = self._get_headers(f"https://www.tiktok.com/@{username}")

        try:
            response = self.session.get(self.USER_POST_URL, params=params, headers=headers, cookies=self.cookies)
            response.raise_for_status()
            parsed = self._safe_response_json(response)
            if parsed is None:
                return None
            return parsed
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch posts: {e}")
            self._last_api_fail_reason = "request_error"
            return None

    def scrape(self, config: TikTokScrapeConfig) -> list[TikTokPost]:
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

        # Get user detail first to get secUid
        user_data = self.fetch_user_detail(config.username, config.delay_seconds)

        sec_uid = None
        use_api = False

        if user_data and user_data.get("userInfo", {}).get("user", {}).get("secUid"):
            user_info = user_data.get("userInfo", {})
            user = user_info.get("user", {})
            sec_uid = user.get("secUid")
            use_api = True
            logger.info(f"Found user: {user.get('nickname')} (@{config.username})")
        else:
            logger.warning(
                f"API returned no data for @{config.username}; "
                "falling back to HTML scraping"
            )

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
                    use_api = True  # Can try API for pagination now

        # Process HTML-extracted posts first
        posts = []
        existing_ids: set[str] = set()
        posts_checked = 0
        reached_date_limit = False
        pages_scanned = 0

        for item in html_posts:
            posts_checked += 1
            create_time = int(item.get("createTime", 0) or 0)

            in_range = config.is_in_date_range(create_time)
            if in_range is None:
                reached_date_limit = True
                break
            if in_range is False:
                continue

            description = item.get("desc", "")
            if config.matches_hashtags(description):
                post = self._parse_post_item(item, config)
                if post.video_id and post.video_id not in existing_ids:
                    posts.append(post)
                    existing_ids.add(post.video_id)
                logger.info(f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views")

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
                data = self.fetch_posts(config.username, sec_uid, cursor, config.delay_seconds)
                if not data:
                    break

                items = data.get("itemList", [])
                if not items:
                    logger.info("No more posts found")
                    break

                for item in items:
                    posts_checked += 1
                    create_time = int(item.get("createTime", 0) or 0)

                    in_range = config.is_in_date_range(create_time)
                    if in_range is None:
                        reached_date_limit = True
                        break
                    if in_range is False:
                        continue

                    description = item.get("desc", "")
                    if config.matches_hashtags(description):
                        post = self._parse_post_item(item, config)
                        # Avoid duplicates from HTML extraction / prior pages.
                        if post.video_id and post.video_id not in existing_ids:
                            posts.append(post)
                            existing_ids.add(post.video_id)
                            logger.info(
                                f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views"
                            )

                has_more = data.get("hasMore", False)
                cursor = data.get("cursor", 0)
                if not has_more or not cursor:
                    logger.info("No more pages available")
                    break

                logger.info(f"Page {page_num}: checked {len(items)} posts, {len(posts)} matches total")

        logger.info(f"Scrape complete: checked {posts_checked} posts, found {len(posts)} matches")

        # Final fallback: try yt-dlp bulk mode if no posts found
        if not posts and self._has_ytdlp():
            logger.info("API/HTML scraping returned no posts; trying yt-dlp bulk fallback...")
            max_videos_hint = (config.max_pages or 10) * 30
            posts = self._scrape_via_ytdlp(
                config,
                max_videos_hint=max_videos_hint,
                max_posts_hint=config.max_pages * 30 if config.max_pages else None,
            )
            existing_ids = {p.video_id for p in posts if p.video_id}

        # Supplement: if yt-dlp found posts but we have very few, try to get more
        if posts and len(posts) < 5 and self._has_ytdlp():
            logger.info(f"Only {len(posts)} posts found, trying yt-dlp to supplement...")
            max_videos_hint = (config.max_pages or 10) * 30
            ytdlp_posts = self._scrape_via_ytdlp(
                config,
                max_videos_hint=max_videos_hint,
                max_posts_hint=config.max_pages * 30 if config.max_pages else None,
            )
            for p in ytdlp_posts:
                if p.video_id and p.video_id not in existing_ids:
                    posts.append(p)
                    existing_ids.add(p.video_id)

        self.last_retrieval_meta = {
            "retrieval_mode": (
                "api"
                if user_data and user_data.get("userInfo", {}).get("user", {}).get("secUid")
                else ("html" if html_posts else "ytdlp_fallback" if posts else "none")
            ),
            "api_fail_reason": self._last_api_fail_reason,
            "pages_scanned": pages_scanned,
            "videos_scanned": posts_checked,
            "first_page_count": len(posts[:30]),
        }
        return posts

    def fetch_comments(
        self,
        video_id: str,
        username: str | None = None,
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
    ) -> list[TikTokComment]:
        """
        Fetch comments for a TikTok video including replies.

        Args:
            video_id: The video ID (aweme_id)
            username: Username for building the post URL (optional)
            max_comments: Maximum number of top-level comments to fetch
            fetch_replies: Whether to fetch replies to comments
            delay: Delay between API requests

        Returns:
            List of TikTokComment objects with nested replies
        """
        post_url = f"https://www.tiktok.com/@{username}/video/{video_id}" if username else ""
        logger.info(f"Fetching comments for video {video_id}")

        comments = []
        cursor = 0
        comments_fetched = 0

        while True:
            self._rate_limit(delay)

            params = {
                "aweme_id": video_id,
                "count": 50,
                "cursor": cursor,
            }
            headers = self._get_headers(post_url or "https://www.tiktok.com/")

            try:
                response = self.session.get(self.COMMENTS_URL, params=params, headers=headers, cookies=self.cookies)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch comments: {e}")
                break

            # Parse comments
            for comment_data in data.get("comments", []):
                comment = self._parse_comment(comment_data, video_id, post_url)
                comments.append(comment)
                comments_fetched += 1

                # Fetch replies if requested and comment has replies
                if fetch_replies and comment.reply_count > 0:
                    replies = self._fetch_comment_replies(video_id, comment.comment_id, post_url, delay)
                    comment.replies = replies
                    logger.info(f"  Comment {comment.comment_id}: {len(replies)} replies fetched")

                if max_comments and comments_fetched >= max_comments:
                    break

            logger.info(f"Fetched {len(comments)} comments so far...")

            # Check for more pages
            if max_comments and comments_fetched >= max_comments:
                break
            if not data.get("has_more", False):
                break
            cursor = data.get("cursor", 0)
            if not cursor:
                break

        logger.info(f"Total: {len(comments)} comments fetched for video {video_id}")
        return comments

    def _fetch_comment_replies(
        self,
        video_id: str,
        comment_id: str,
        post_url: str,
        delay: float = 2.0,
    ) -> list[TikTokComment]:
        """Fetch replies to a specific comment."""
        replies = []
        cursor = 0

        while True:
            self._rate_limit(delay)

            params = {
                "item_id": video_id,
                "comment_id": comment_id,
                "count": 50,
                "cursor": cursor,
            }
            headers = self._get_headers(post_url or "https://www.tiktok.com/")

            try:
                response = self.session.get(
                    self.COMMENT_REPLIES_URL, params=params, headers=headers, cookies=self.cookies
                )
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch replies for comment {comment_id}: {e}")
                break

            # Parse reply comments
            for reply_data in data.get("comments", []):
                reply = self._parse_comment(reply_data, video_id, post_url, is_reply=True, parent_id=comment_id)
                replies.append(reply)

            # Check for more pages
            if not data.get("has_more", False):
                break
            cursor = data.get("cursor", 0)
            if not cursor:
                break

        return replies

    def _parse_comment(
        self,
        data: dict,
        video_id: str,
        post_url: str,
        is_reply: bool = False,
        parent_id: str | None = None,
    ) -> TikTokComment:
        """Parse comment data into TikTokComment object."""
        created_at = data.get("create_time", 0)
        user = data.get("user", {})

        return TikTokComment(
            comment_id=str(data.get("cid", "")),
            text=data.get("text", ""),
            username=user.get("unique_id", ""),
            user_id=str(user.get("uid", "")),
            nickname=user.get("nickname", ""),
            created_at=created_at,
            date_time=(datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S") if created_at else ""),
            likes=data.get("digg_count", 0),
            is_reply=is_reply,
            parent_comment_id=parent_id,
            reply_count=data.get("reply_comment_total", 0),
            video_id=video_id,
            post_url=post_url,
        )

    def _extract_media_urls(self, item: dict) -> list[str]:
        """Extract all media URLs from a post."""
        urls = []

        # Video URL
        video = item.get("video", {})
        if video:
            # Play URL
            play_addr = video.get("playAddr")
            if play_addr:
                urls.append(play_addr)
            # Download URL
            download_addr = video.get("downloadAddr")
            if download_addr and download_addr not in urls:
                urls.append(download_addr)

        # Cover/thumbnail
        cover = video.get("cover") if video else None
        if cover and cover not in urls:
            urls.append(cover)

        # Dynamic cover (animated thumbnail)
        dynamic_cover = video.get("dynamicCover") if video else None
        if dynamic_cover and dynamic_cover not in urls:
            urls.append(dynamic_cover)

        return urls
