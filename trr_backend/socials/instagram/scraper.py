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
import os
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class ScrapeConfig:
    """Configuration for an Instagram scrape operation."""

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

    # Media URLs
    media_urls: list[str] = field(default_factory=list)

    # Comments (populated when fetch_comments is called)
    comment_list: list[InstagramComment] = field(default_factory=list)

    # Optional tracking metadata
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

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
        "26035927152742158",
        # Backward fallback used by older sessions.
        "33944389991841132",
    )

    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.5

    def __init__(self, cookies: dict | None = None):
        self.cookies = cookies or {}
        self.session = self._create_session()
        self._request_count = 0
        self.last_retrieval_meta: dict[str, Any] = {}

    def _profile_posts_doc_ids(self) -> list[str]:
        override = (os.getenv("INSTAGRAM_PROFILE_POSTS_DOC_ID") or "").strip()
        ids: list[str] = []
        if override:
            ids.append(override)
        for doc_id in self.PROFILE_POSTS_DOC_IDS:
            if doc_id not in ids:
                ids.append(doc_id)
        return ids

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
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://www.instagram.com",
            "referer": referer or "https://www.instagram.com/",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
            "x-ig-app-id": "936619743392459",
            "x-requested-with": "XMLHttpRequest",
        }
        if self.cookies.get("csrftoken"):
            headers["x-csrftoken"] = self.cookies["csrftoken"]
        return headers

    def _rate_limit(self, delay: float):
        """Apply rate limiting between requests."""
        if self._request_count > 0:
            logger.debug(f"Rate limiting: waiting {delay}s")
            time.sleep(delay)
        self._request_count += 1

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

        return tags

    def _extract_like_count(self, node: dict) -> int:
        """Extract like count."""
        if "like_count" in node:
            return node.get("like_count", 0)
        edge_liked = node.get("edge_liked_by") or node.get("edge_media_preview_like")
        if edge_liked:
            return edge_liked.get("count", 0)
        return 0

    def _extract_comment_count(self, node: dict) -> int:
        """Extract comment count."""
        if "comment_count" in node:
            return node.get("comment_count", 0)
        edge_comments = node.get("edge_media_to_comment")
        if edge_comments:
            return edge_comments.get("count", 0)
        return 0

    def _extract_timestamp(self, node: dict) -> int:
        """Extract Unix timestamp."""
        return node.get("taken_at_timestamp") or node.get("taken_at", 0)

    def _extract_shortcode(self, node: dict) -> str:
        """Extract shortcode."""
        return node.get("shortcode") or node.get("code", "")

    def _parse_post_node(self, node: dict, config: ScrapeConfig) -> InstagramPost:
        """Parse a post node into InstagramPost."""
        shortcode = self._extract_shortcode(node)
        taken_at = self._extract_timestamp(node)

        return InstagramPost(
            shortcode=shortcode,
            post_type=self._determine_post_type(node),
            date_time=datetime.fromtimestamp(taken_at).strftime("%Y-%m-%d %H:%M:%S") if taken_at else "",
            taken_at=taken_at,
            caption=self._extract_caption(node),
            profile_tags=self._extract_profile_tags(node),
            sponsored=bool(node.get("is_paid_partnership")),
            likes=self._extract_like_count(node),
            comments=self._extract_comment_count(node),
            video_views=node.get("video_view_count", 0),
            url=f"https://www.instagram.com/p/{shortcode}/" if shortcode else "",
            pk=str(node.get("pk") or node.get("id", "")),
            username=config.username,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def fetch_profile_info(self, username: str, delay: float = 2.0) -> dict | None:
        """Fetch profile info using public API (limited to ~12 posts)."""
        self._rate_limit(delay)
        url = f"{self.PROFILE_INFO_URL}?username={username}"
        headers = self._get_headers(f"https://www.instagram.com/{username}/")

        try:
            response = self.session.get(url, headers=headers, cookies=self.cookies)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch profile info for {username}: {e}")
            return None

    def fetch_posts_graphql(self, username: str, cursor: str | None = None, delay: float = 2.0) -> dict | None:
        """Fetch posts using GraphQL (requires auth for full access)."""
        self._rate_limit(delay)

        variables = {
            "after": cursor,
            "before": None,
            "data": {
                "count": 12,
                "include_reel_media_seen_timestamp": True,
                "include_relationship_info": True,
                "latest_besties_reel_media": True,
                "latest_reel_media": True,
            },
            "first": 12,
            "last": None,
            "username": username,
        }

        data = {
            "av": self.cookies.get("ds_user_id", "17841454077505205"),
            "__d": "www",
            "__user": self.cookies.get("ds_user_id", "0"),
            "__a": "1",
            "__req": "1",
            "__comet_req": "7",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "PolarisProfilePostsTabContentQuery_connection",
            "variables": json.dumps(variables),
            "server_timestamps": "true",
        }

        headers = self._get_headers(f"https://www.instagram.com/{username}/")
        headers["content-type"] = "application/x-www-form-urlencoded"
        headers["x-fb-friendly-name"] = "PolarisProfilePostsTabContentQuery_connection"
        if self.cookies.get("lsd"):
            headers["x-fb-lsd"] = self.cookies["lsd"]

        for doc_id in self._profile_posts_doc_ids():
            data["doc_id"] = doc_id
            try:
                response = self.session.post(self.GRAPHQL_URL, data=data, headers=headers, cookies=self.cookies)
                response.raise_for_status()
                payload = response.json()
                connection = payload.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
                if connection:
                    return payload
                logger.warning("Instagram GraphQL doc_id %s returned no connection data; trying fallback", doc_id)
            except requests.exceptions.RequestException as e:
                logger.warning("GraphQL request failed for doc_id %s: %s", doc_id, e)
                continue
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

    def _shortcode_to_media_id(self, shortcode: str) -> str:
        """Convert Instagram shortcode to media ID."""
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        media_id = 0
        for char in shortcode:
            media_id = media_id * 64 + alphabet.index(char)
        return str(media_id)

    def fetch_post_info(self, shortcode: str, delay: float = 2.0) -> dict | None:
        """Fetch detailed post info including media URLs."""
        self._rate_limit(delay)
        media_id = self._shortcode_to_media_id(shortcode)
        url = self.POST_INFO_URL.format(media_id=media_id)
        headers = self._get_headers(f"https://www.instagram.com/p/{shortcode}/")

        try:
            response = self.session.get(url, headers=headers, cookies=self.cookies)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch post info for {shortcode}: {e}")
            return None

    def fetch_comments(
        self,
        shortcode: str,
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
    ) -> list[InstagramComment]:
        """
        Fetch comments for a post including replies.

        Args:
            shortcode: Post shortcode (from URL like /p/DUBSkVeEp4c/)
            max_comments: Maximum number of top-level comments to fetch
            fetch_replies: Whether to fetch replies to comments
            delay: Delay between API requests

        Returns:
            List of InstagramComment objects with nested replies
        """
        media_id = self._shortcode_to_media_id(shortcode)
        post_url = f"https://www.instagram.com/p/{shortcode}/"
        logger.info(f"Fetching comments for {shortcode} (media_id: {media_id})")

        comments = []
        cursor = None
        comments_fetched = 0

        while True:
            self._rate_limit(delay)
            url = self.COMMENTS_URL.format(media_id=media_id)
            params = {"can_support_threading": "true", "permalink_enabled": "false"}
            if cursor:
                params["min_id"] = cursor

            headers = self._get_headers(post_url)

            try:
                response = self.session.get(url, params=params, headers=headers, cookies=self.cookies)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch comments: {e}")
                break

            # Parse comments
            for comment_data in data.get("comments", []):
                comment = self._parse_comment(comment_data, shortcode, post_url)
                comments.append(comment)
                comments_fetched += 1

                # Fetch replies if requested and comment has replies
                if fetch_replies and comment.reply_count > 0:
                    replies = self._fetch_comment_replies(media_id, comment.comment_id, shortcode, post_url, delay)
                    comment.replies = replies
                    logger.info(f"  Comment {comment.comment_id}: {comment.reply_count} replies fetched")

                if max_comments and comments_fetched >= max_comments:
                    break

            logger.info(f"Fetched {len(comments)} comments so far...")

            # Check for more pages
            if max_comments and comments_fetched >= max_comments:
                break
            if not data.get("has_more_comments", False):
                break
            cursor = data.get("next_min_id")
            if not cursor:
                break

        logger.info(f"Total: {len(comments)} comments fetched for {shortcode}")
        return comments

    def _fetch_comment_replies(
        self,
        media_id: str,
        comment_id: str,
        shortcode: str,
        post_url: str,
        delay: float = 2.0,
    ) -> list[InstagramComment]:
        """Fetch replies to a specific comment."""
        replies = []
        cursor = None

        while True:
            self._rate_limit(delay)
            url = self.COMMENT_REPLIES_URL.format(media_id=media_id, comment_id=comment_id)
            params = {}
            if cursor:
                params["min_id"] = cursor

            headers = self._get_headers(post_url)

            try:
                response = self.session.get(url, params=params, headers=headers, cookies=self.cookies)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch replies for comment {comment_id}: {e}")
                break

            # Parse child comments (replies)
            for reply_data in data.get("child_comments", []):
                reply = self._parse_comment(reply_data, shortcode, post_url, is_reply=True, parent_id=comment_id)
                replies.append(reply)

            # Check for more pages
            if not data.get("has_more_tail_child_comments", False):
                break
            cursor = data.get("next_min_child_cursor")
            if not cursor:
                break

        return replies

    def _parse_comment(
        self,
        data: dict,
        shortcode: str,
        post_url: str,
        is_reply: bool = False,
        parent_id: str | None = None,
    ) -> InstagramComment:
        """Parse comment data into InstagramComment object."""
        created_at = data.get("created_at", 0)
        user = data.get("user", {})

        return InstagramComment(
            comment_id=str(data.get("pk", "")),
            text=data.get("text", ""),
            username=user.get("username", ""),
            user_id=str(user.get("pk", "")),
            created_at=created_at,
            date_time=datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S") if created_at else "",
            likes=data.get("comment_like_count", 0),
            is_reply=is_reply,
            parent_comment_id=parent_id,
            reply_count=data.get("child_comment_count", 0),
            post_shortcode=shortcode,
            post_url=post_url,
        )

    def _extract_media_urls(self, node: dict) -> list[str]:
        """Extract all media URLs from a post."""
        urls = []

        # Single image/video
        if node.get("image_versions2"):
            candidates = node["image_versions2"].get("candidates", [])
            if candidates:
                urls.append(candidates[0].get("url", ""))

        if node.get("video_versions"):
            versions = node.get("video_versions", [])
            if versions:
                urls.append(versions[0].get("url", ""))

        # Carousel media
        carousel = node.get("carousel_media", [])
        for item in carousel:
            if item.get("image_versions2"):
                candidates = item["image_versions2"].get("candidates", [])
                if candidates:
                    urls.append(candidates[0].get("url", ""))
            if item.get("video_versions"):
                versions = item.get("video_versions", [])
                if versions:
                    urls.append(versions[0].get("url", ""))

        # GraphQL format
        display_url = node.get("display_url")
        if display_url and display_url not in urls:
            urls.append(display_url)

        video_url = node.get("video_url")
        if video_url and video_url not in urls:
            urls.append(video_url)

        # Sidecar (carousel) in GraphQL format
        sidecar = node.get("edge_sidecar_to_children", {})
        for edge in sidecar.get("edges", []):
            child = edge.get("node", {})
            if child.get("display_url"):
                urls.append(child["display_url"])
            if child.get("video_url"):
                urls.append(child["video_url"])

        return urls

    def scrape(self, config: ScrapeConfig) -> list[InstagramPost]:
        """
        Scrape posts from an Instagram profile with filtering.

        Args:
            config: ScrapeConfig with username, hashtags, date range, etc.

        Returns:
            List of InstagramPost objects matching the filters.
        """
        logger.info(f"Starting scrape for @{config.username}")
        if config.hashtags:
            logger.info(f"Filtering by hashtags: {config.hashtags}")
        if config.date_start or config.date_end:
            logger.info(f"Date range: {config.date_start} to {config.date_end}")

        # Determine scrape mode
        has_auth = bool(self.cookies.get("sessionid"))
        if has_auth:
            posts = self._scrape_graphql(config)
            # If the very first authenticated page fails, degrade gracefully to profile-info mode.
            if not posts and self.last_retrieval_meta.get("initial_page_failed"):
                fallback_reason = self.last_retrieval_meta.get("fallback_reason") or "graphql_initial_page_failed"
                logger.warning(
                    "Instagram GraphQL initial page failed for @%s; falling back to profile-info mode (%s)",
                    config.username,
                    fallback_reason,
                )
                posts = self._scrape_profile_info(config)
                self.last_retrieval_meta["retrieval_mode"] = "profile_info_fallback"
                self.last_retrieval_meta["fallback_reason"] = fallback_reason
                self.last_retrieval_meta["first_page_count"] = len(posts)
            return posts
        return self._scrape_profile_info(config)

    def _scrape_profile_info(self, config: ScrapeConfig) -> list[InstagramPost]:
        """Scrape using public profile info API (limited results)."""
        logger.info("Using profile info API (unauthenticated, limited to ~12 posts)")

        data = self.fetch_profile_info(config.username, config.delay_seconds)
        if not data:
            return []

        posts = []
        for node, _ in self._iter_posts_from_profile_info(data):
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
                logger.info(f"Found: {post.shortcode} ({post.date_time})")

        logger.info(f"Scrape complete: {len(posts)} posts found")
        self.last_retrieval_meta = {
            "retrieval_mode": "profile_info",
            "first_page_count": len(posts),
            "fallback_reason": None,
            "initial_page_failed": False,
        }
        return posts

    def _scrape_graphql(self, config: ScrapeConfig) -> list[InstagramPost]:
        """Scrape using GraphQL API with full pagination."""
        logger.info("Using GraphQL API (authenticated, full pagination)")

        posts = []
        cursor = None
        page_num = 0
        posts_checked = 0
        reached_date_limit = False
        initial_page_failed = False
        failure_reason: str | None = None

        while not reached_date_limit:
            page_num += 1
            if config.max_pages and page_num > config.max_pages:
                logger.info(f"Reached max pages limit ({config.max_pages})")
                break

            logger.info(f"Fetching page {page_num}...")
            data = self.fetch_posts_graphql(config.username, cursor, config.delay_seconds)
            if not data:
                if page_num == 1:
                    initial_page_failed = True
                    failure_reason = "graphql_empty_or_error"
                break

            page_info = {}
            posts_on_page = 0

            for node, pi in self._iter_posts_from_graphql(data):
                page_info = pi
                posts_checked += 1
                posts_on_page += 1

                timestamp = self._extract_timestamp(node)

                # Check date range
                in_range = config.is_in_date_range(timestamp)
                if in_range is None:  # Before range
                    reached_date_limit = True
                    break
                if in_range is False:  # After range
                    continue

                # Check hashtag filter
                caption = self._extract_caption(node)
                if config.matches_hashtags(caption):
                    post = self._parse_post_node(node, config)
                    posts.append(post)
                    logger.info(
                        f"Found #{len(posts)}: {post.shortcode} ({post.date_time}) "
                        f"- {post.post_type} - {post.likes:,} likes"
                    )

            if posts_on_page == 0:
                logger.info("No more posts found")
                break

            # Get next page
            has_next = page_info.get("has_next_page", False)
            cursor = page_info.get("end_cursor")
            if not has_next or not cursor:
                logger.info("No more pages available")
                break

            logger.info(f"Page {page_num}: checked {posts_on_page} posts, {len(posts)} matches total")

        logger.info(f"Scrape complete: checked {posts_checked} posts, found {len(posts)} matches")
        self.last_retrieval_meta = {
            "retrieval_mode": "graphql",
            "first_page_count": len(posts) if page_num <= 1 else min(len(posts), 12),
            "fallback_reason": failure_reason,
            "initial_page_failed": initial_page_failed,
            "pages_scanned": page_num,
            "posts_checked": posts_checked,
        }
        return posts


def load_cookies_from_file(filepath: str) -> dict:
    """Load Instagram cookies from a JSON file."""
    with open(filepath) as f:
        cookies = json.load(f)
    # Remove comment fields
    return {k: v for k, v in cookies.items() if not k.startswith("_")}
