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
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
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
    no_match_page_limit: int | None = None  # None = use scraper default/env

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
class InstagramUserDetail:
    """Rich user object extracted from tagged users, collaborators, and post owners."""

    username: str
    user_id: str | None = None
    full_name: str | None = None
    is_verified: bool | None = None
    profile_pic_url: str | None = None

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
    owner: dict[str, Any] | None = None
    replies: list["InstagramComment"] = field(default_factory=list)
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
    user_id: str | None = None

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
    content_type: str | None = None
    video_play_count: int | None = None
    alt_text: str | None = None
    width: int | None = None
    height: int | None = None
    is_comments_disabled: bool | None = None
    music_info: dict[str, Any] | None = None
    video_duration: float | None = None
    location: dict[str, Any] | None = None
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
        "26035927152742158",
        # Backward fallback used by older sessions.
        "33944389991841132",
    )

    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.5
    REQUEST_CONNECT_TIMEOUT_SECONDS = 10
    REQUEST_READ_TIMEOUT_SECONDS = 45
    DEFAULT_NO_MATCH_PAGE_LIMIT = 40

    def __init__(self, cookies: dict | None = None):
        self.cookies = cookies or {}
        self.session = self._create_session()
        self._request_count = 0
        self.last_retrieval_meta: dict[str, Any] = {}
        self.comments_auth_failed = False
        self.last_comment_fetch_reason: str | None = None
        self.request_timeout = (
            self.REQUEST_CONNECT_TIMEOUT_SECONDS,
            self.REQUEST_READ_TIMEOUT_SECONDS,
        )

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

    def _get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.session.get(url, timeout=self.request_timeout, **kwargs)

    def _post(self, url: str, **kwargs: Any) -> requests.Response:
        return self.session.post(url, timeout=self.request_timeout, **kwargs)

    def _get_headers(self, referer: str | None = None) -> dict:
        """Get request headers."""
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
        if self.cookies.get("csrftoken"):
            headers["x-csrftoken"] = self.cookies["csrftoken"]
        return headers

    def _rate_limit(self, delay: float):
        """Apply rate limiting between requests."""
        if self._request_count > 0:
            logger.debug(f"Rate limiting: waiting {delay}s")
            time.sleep(delay)
        self._request_count += 1

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
        ) -> None:
            normalized = self._normalize_handle_token(username)
            if not normalized or normalized.lower() in seen:
                return
            seen.add(normalized.lower())
            details.append(
                InstagramUserDetail(
                    username=normalized,
                    user_id=str(user_id) if user_id else None,
                    full_name=full_name or None,
                    is_verified=bool(is_verified) if is_verified is not None else None,
                    profile_pic_url=str(profile_pic_url) if profile_pic_url else None,
                )
            )

        # GraphQL format
        edge_tags = node.get("edge_media_to_tagged_user", {})
        if edge_tags:
            for edge in edge_tags.get("edges", []):
                user = edge.get("node", {}).get("user", {})
                _add(
                    user.get("username"),
                    user.get("id") or user.get("pk"),
                    user.get("full_name"),
                    user.get("is_verified"),
                    user.get("profile_pic_url"),
                )

        # REST API format
        usertags = node.get("usertags", {})
        if usertags and isinstance(usertags, dict):
            for tag in usertags.get("in", []):
                user = tag.get("user", {})
                _add(
                    user.get("username"),
                    user.get("pk") or user.get("id"),
                    user.get("full_name"),
                    user.get("is_verified"),
                    user.get("profile_pic_url"),
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
                        profile_pic_url=str(user.get("profile_pic_url") or user.get("profilePicUrl") or "") or None,
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
            profile_pic_url=str(owner.get("profile_pic_url") or node.get("ownerProfilePicUrl") or "").strip() or None,
        )

    def _extract_child_posts_data(self, node: dict) -> list[dict[str, Any]]:
        """Extract carousel child post details."""
        children: list[dict[str, Any]] = []

        # Actor-style format (childPosts)
        child_posts = node.get("childPosts")
        if isinstance(child_posts, list):
            for child in child_posts:
                if not isinstance(child, dict):
                    continue
                dims = child.get("dimensions", {})
                children.append(
                    {
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
                    }
                )
            if children:
                return children

        # REST API format (carousel_media)
        carousel = node.get("carousel_media")
        if isinstance(carousel, list):
            for item in carousel:
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
                children.append(
                    {
                        "type": "Video" if video_url else "Image",
                        "display_url": display_url,
                        "video_url": video_url,
                        "width": self._coerce_int(item.get("original_width"), 0) or None,
                        "height": self._coerce_int(item.get("original_height"), 0) or None,
                        "alt": item.get("accessibility_caption"),
                    }
                )
            if children:
                return children

        # GraphQL format (edge_sidecar_to_children)
        sidecar = node.get("edge_sidecar_to_children") or {}
        for edge in sidecar.get("edges", []):
            child = edge.get("node", {})
            dims = child.get("dimensions", {})
            children.append(
                {
                    "type": child.get("__typename", "").replace("Graph", ""),
                    "display_url": child.get("display_url"),
                    "video_url": child.get("video_url"),
                    "width": self._coerce_int(dims.get("width"), 0) or None,
                    "height": self._coerce_int(dims.get("height"), 0) or None,
                    "alt": child.get("accessibility_caption"),
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

        # video_duration
        vd = node.get("videoDuration") or node.get("video_duration")
        if vd is not None:
            try:
                result["video_duration"] = float(vd)
            except (TypeError, ValueError):
                result["video_duration"] = None
        else:
            result["video_duration"] = None

        # content_type
        content_type = node.get("type") or node.get("media_type_name") or node.get("content_type") or ""
        result["content_type"] = str(content_type).strip() or None

        # location metadata
        location = node.get("location")
        if isinstance(location, dict):
            result["location"] = dict(location)
        elif location is None or location == "":
            result["location"] = None
        else:
            result["location"] = {"name": str(location)}

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
        owner_detail = self._extract_owner_detail(node)
        owner_user_id = owner_detail.user_id if owner_detail and hasattr(owner_detail, "user_id") else None

        return InstagramPost(
            shortcode=shortcode,
            post_type=self._determine_post_type(node),
            date_time=datetime.fromtimestamp(taken_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if taken_at else "",
            taken_at=taken_at,
            caption=caption,
            profile_tags=self._extract_profile_tags(node),
            sponsored=bool(node.get("is_paid_partnership")),
            likes=self._extract_like_count(node),
            comments=self._extract_comment_count(node),
            video_views=self._coerce_int(node.get("video_view_count") or node.get("videoViewCount"), 0),
            url=f"https://www.instagram.com/p/{shortcode}/" if shortcode else "",
            pk=str(node.get("pk") or node.get("id", "")),
            username=str(node.get("ownerUsername") or node.get("owner", {}).get("username") or config.username),
            user_id=owner_user_id,
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
            owner_detail=owner_detail,
            product_type=extras.get("product_type"),
            content_type=extras.get("content_type"),
            video_play_count=extras.get("video_play_count"),
            alt_text=extras.get("alt_text"),
            width=extras.get("width"),
            height=extras.get("height"),
            is_comments_disabled=extras.get("is_comments_disabled"),
            music_info=extras.get("music_info"),
            video_duration=extras.get("video_duration"),
            location=extras.get("location"),
            child_posts_data=self._extract_child_posts_data(node),
        )

    def fetch_profile_info(self, username: str, delay: float = 2.0) -> dict | None:
        """Fetch profile info using public API (limited to ~12 posts)."""
        self._rate_limit(delay)
        url = f"{self.PROFILE_INFO_URL}?username={username}"
        headers = self._get_headers(f"https://www.instagram.com/{username}/")

        try:
            response = self._get(url, headers=headers, cookies=self.cookies)
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
                response = self._post(self.GRAPHQL_URL, data=data, headers=headers, cookies=self.cookies)
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
        try:
            media_id = self._shortcode_to_media_id(shortcode)
        except (ValueError, IndexError):
            logger.error(f"Invalid shortcode '{shortcode}' — skipping post info fetch")
            return None
        url = self.POST_INFO_URL.format(media_id=media_id)
        headers = self._get_headers(f"https://www.instagram.com/p/{shortcode}/")

        try:
            response = self._get(url, headers=headers, cookies=self.cookies)
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
            self._rate_limit(delay)
            url = self.COMMENTS_URL.format(media_id=media_id)
            params = {"can_support_threading": "true", "permalink_enabled": "false"}
            if cursor:
                params["min_id"] = cursor

            headers = self._get_headers(post_url)

            try:
                response = self._get(url, params=params, headers=headers, cookies=self.cookies)
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
                    replies = self._fetch_comment_replies(media_id, comment.comment_id, shortcode, post_url, delay)
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
    ) -> list[InstagramComment]:
        """Fetch replies to a specific comment."""
        replies = []
        cursor = None

        while True:
            response: requests.Response | None = None
            self._rate_limit(delay)
            url = self.COMMENT_REPLIES_URL.format(media_id=media_id, comment_id=comment_id)
            params = {}
            if cursor:
                params["min_id"] = cursor

            headers = self._get_headers(post_url)

            try:
                response = self._get(url, params=params, headers=headers, cookies=self.cookies)
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
            owner=owner or None,
            owner_profile_pic_url=str(
                data.get("ownerProfilePicUrl") or owner.get("profile_pic_url") or user.get("profile_pic_url") or ""
            )
            or None,
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
        carousel = node.get("carousel_media") or []
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
        actor_display_url = node.get("displayUrl")
        if actor_display_url and actor_display_url not in urls:
            urls.append(actor_display_url)

        video_url = node.get("video_url")
        if video_url and video_url not in urls:
            urls.append(video_url)
        actor_video_url = node.get("videoUrl")
        if actor_video_url and actor_video_url not in urls:
            urls.append(actor_video_url)

        # Sidecar (carousel) in GraphQL format
        sidecar = node.get("edge_sidecar_to_children") or {}
        for edge in sidecar.get("edges", []):
            child = edge.get("node", {})
            if child.get("display_url"):
                urls.append(child["display_url"])
            if child.get("video_url"):
                urls.append(child["video_url"])

        images = node.get("images")
        if isinstance(images, list):
            for image in images:
                if isinstance(image, str):
                    if image and image not in urls:
                        urls.append(image)
                    continue
                if not isinstance(image, dict):
                    continue
                candidate = image.get("url") or image.get("displayUrl") or image.get("display_url")
                if candidate and candidate not in urls:
                    urls.append(candidate)

        child_posts = node.get("childPosts")
        if isinstance(child_posts, list):
            for child in child_posts:
                if not isinstance(child, dict):
                    continue
                candidate_display = child.get("displayUrl") or child.get("display_url")
                candidate_video = child.get("videoUrl") or child.get("video_url")
                if candidate_display and candidate_display not in urls:
                    urls.append(candidate_display)
                if candidate_video and candidate_video not in urls:
                    urls.append(candidate_video)

        deduped: list[str] = []
        for url in urls:
            normalized = str(url or "").strip()
            if normalized and normalized not in deduped:
                deduped.append(normalized)
        return deduped

    @staticmethod
    def _looks_like_video_url(url: str) -> bool:
        normalized = str(url or "").strip().lower()
        if not normalized:
            return False
        return any(
            marker in normalized
            for marker in (
                ".mp4",
                ".m3u8",
                ".mov",
                ".webm",
                "mime_type=video",
                "/video/tos/",
                "_video_dashinit",
            )
        )

    def _extract_thumbnail_url(self, node: dict, media_urls: list[str]) -> str | None:
        """Select an image thumbnail URL without falling back to video media URLs."""
        candidates: list[str] = []

        image_versions = node.get("image_versions2")
        if isinstance(image_versions, dict):
            version_candidates = image_versions.get("candidates")
            if isinstance(version_candidates, list) and version_candidates:
                first = version_candidates[0]
                if isinstance(first, dict):
                    candidate = str(first.get("url") or "").strip()
                    if candidate:
                        candidates.append(candidate)

        for key in ("display_url", "displayUrl", "thumbnail_src"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())

        images = node.get("images")
        if isinstance(images, list):
            for image in images:
                if isinstance(image, str) and image.strip():
                    candidates.append(image.strip())
                    continue
                if not isinstance(image, dict):
                    continue
                candidate = image.get("url") or image.get("displayUrl") or image.get("display_url")
                if isinstance(candidate, str) and candidate.strip():
                    candidates.append(candidate.strip())

        child_posts = node.get("childPosts")
        if isinstance(child_posts, list):
            for child in child_posts:
                if not isinstance(child, dict):
                    continue
                candidate = child.get("displayUrl") or child.get("display_url")
                if isinstance(candidate, str) and candidate.strip():
                    candidates.append(candidate.strip())

        sidecar = node.get("edge_sidecar_to_children") or {}
        if isinstance(sidecar, dict):
            for edge in sidecar.get("edges", []):
                if not isinstance(edge, dict):
                    continue
                child = edge.get("node")
                if not isinstance(child, dict):
                    continue
                candidate = child.get("display_url")
                if isinstance(candidate, str) and candidate.strip():
                    candidates.append(candidate.strip())

        for candidate in candidates:
            if candidate and not self._looks_like_video_url(candidate):
                return candidate

        for media_url in media_urls:
            if media_url and not self._looks_like_video_url(media_url):
                return media_url
        return None

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
            logger.debug("Instagram scrape progress callback raised", exc_info=True)

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
        logger.info(f"Starting scrape for @{config.username}")
        if config.hashtags:
            logger.info(f"Filtering by hashtags: {config.hashtags}")
        if config.date_start or config.date_end:
            logger.info(f"Date range: {config.date_start} to {config.date_end}")

        # Determine scrape mode
        has_auth = bool(self.cookies.get("sessionid"))
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
            return posts
        return self._scrape_profile_info(config, progress_cb=progress_cb)

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
                logger.info(f"Found: {post.shortcode} ({post.date_time})")
            self._emit_progress(
                progress_cb,
                phase="scrape_profile_page",
                pages_scanned=1,
                posts_checked=posts_checked,
                matched_posts=len(posts),
            )

        logger.info(f"Scrape complete: {len(posts)} posts found")
        self.last_retrieval_meta = {
            "retrieval_mode": "profile_info",
            "first_page_count": len(posts),
            "fallback_reason": None,
            "initial_page_failed": False,
        }
        return posts

    def _scrape_graphql(
        self,
        config: ScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[InstagramPost]:
        """Scrape using GraphQL API with full pagination."""
        logger.info("Using GraphQL API (authenticated, full pagination)")

        posts = []
        cursor = None
        page_num = 0
        posts_checked = 0
        reached_date_limit = False
        initial_page_failed = False
        failure_reason: str | None = None
        stop_reason: str | None = None
        no_match_pages = 0
        no_match_page_limit = self._resolve_no_match_page_limit(config)

        while not reached_date_limit:
            page_num += 1
            if config.max_pages and page_num > config.max_pages:
                logger.info(f"Reached max pages limit ({config.max_pages})")
                stop_reason = "max_pages_reached"
                break

            logger.info(f"Fetching page {page_num}...")
            data = self.fetch_posts_graphql(config.username, cursor, config.delay_seconds)
            if not data:
                if page_num == 1:
                    initial_page_failed = True
                    failure_reason = "graphql_empty_or_error"
                stop_reason = "graphql_empty_or_error"
                break

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
                        f"Found #{len(posts)}: {post.shortcode} ({post.date_time}) "
                        f"- {post.post_type} - {post.likes:,} likes"
                    )

            self._emit_progress(
                progress_cb,
                phase="scrape_graphql_page",
                pages_scanned=page_num,
                posts_checked=posts_checked,
                matched_posts=len(posts),
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
            cursor = page_info.get("end_cursor")
            if not has_next or not cursor:
                logger.info("No more pages available")
                stop_reason = "no_more_pages"
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
            "stop_reason": stop_reason,
            "no_match_pages": no_match_pages,
            "no_match_page_limit": no_match_page_limit,
        }
        return posts


def load_cookies_from_file(filepath: str) -> dict:
    """Load Instagram cookies from a JSON file."""
    with open(filepath) as f:
        cookies = json.load(f)
    # Remove comment fields
    return {k: v for k, v in cookies.items() if not k.startswith("_")}
