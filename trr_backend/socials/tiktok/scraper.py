"""
TikTok scraper module for fetching posts from public profiles.

Supports:
- Fetching posts from any public TikTok profile
- Filtering by hashtags (case-insensitive)
- Filtering by date range
- Fetching comments and replies with like counts
"""

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
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

    def _extract_hashtags(self, text: str) -> list[str]:
        """Extract hashtags from text."""
        import re

        return re.findall(r"#(\w+)", text)

    def _extract_mentions(self, text: str) -> list[str]:
        """Extract @mentions from text."""
        import re

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
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch user detail for @{username}: {e}")
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
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch posts: {e}")
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

        # Get user detail first to get secUid
        user_data = self.fetch_user_detail(config.username, config.delay_seconds)
        if not user_data:
            logger.error(f"Failed to get user detail for @{config.username}")
            return []

        user_info = user_data.get("userInfo", {})
        user = user_info.get("user", {})
        sec_uid = user.get("secUid")

        if not sec_uid:
            logger.error(f"Could not get secUid for @{config.username}")
            return []

        logger.info(f"Found user: {user.get('nickname')} (@{config.username})")

        # Fetch posts
        posts = []
        cursor = 0
        page_num = 0
        posts_checked = 0
        reached_date_limit = False

        while not reached_date_limit:
            page_num += 1
            if config.max_pages and page_num > config.max_pages:
                logger.info(f"Reached max pages limit ({config.max_pages})")
                break

            logger.info(f"Fetching page {page_num}...")
            data = self.fetch_posts(config.username, sec_uid, cursor, config.delay_seconds)
            if not data:
                break

            items = data.get("itemList", [])
            if not items:
                logger.info("No more posts found")
                break

            for item in items:
                posts_checked += 1
                create_time = item.get("createTime", 0)

                # Check date range
                in_range = config.is_in_date_range(create_time)
                if in_range is None:  # Before range
                    reached_date_limit = True
                    break
                if in_range is False:  # After range
                    continue

                # Check hashtag filter
                description = item.get("desc", "")
                if config.matches_hashtags(description):
                    post = self._parse_post_item(item, config)
                    posts.append(post)
                    logger.info(f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views")

            # Check for more pages
            has_more = data.get("hasMore", False)
            cursor = data.get("cursor", 0)
            if not has_more or not cursor:
                logger.info("No more pages available")
                break

            logger.info(f"Page {page_num}: checked {len(items)} posts, {len(posts)} matches total")

        logger.info(f"Scrape complete: checked {posts_checked} posts, found {len(posts)} matches")
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
