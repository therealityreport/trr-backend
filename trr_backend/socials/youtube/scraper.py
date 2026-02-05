"""
YouTube scraper module for fetching videos from channels.

Supports:
- Fetching videos from a YouTube channel
- Filtering by keywords in title/description
- Filtering by date range
- Fetching comments and replies from videos
- Both API-based and web scraping approaches
"""

import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class YouTubeScrapeConfig:
    """Configuration for a YouTube scrape operation."""

    channel_handle: str  # Channel handle like "bravo" (without @)
    keywords: list[str] = field(default_factory=list)  # Keywords to filter by (e.g., "RHOSLC", "Salt Lake City")
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 2.0
    max_results: int | None = None  # None = no limit

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
    VIDEO_WATCH_URL = "https://www.youtube.com/watch?v={video_id}"
    COMMENT_API_URL = "https://www.youtube.com/youtubei/v1/next"
    YTINITAL_DATA_PATTERN = re.compile(r"var ytInitialData = ({.*?});", re.DOTALL)

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

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
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

    def _parse_video_renderer(self, renderer: dict, config: YouTubeScrapeConfig) -> YouTubeVideo | None:
        """Parse a video renderer from YouTube data."""
        video_id = renderer.get("videoId", "")
        if not video_id:
            return None

        # Extract title
        title_runs = renderer.get("title", {}).get("runs", [])
        title = title_runs[0].get("text", "") if title_runs else ""

        # Extract description
        desc_runs = renderer.get("descriptionSnippet", {}).get("runs", [])
        description = "".join(r.get("text", "") for r in desc_runs)

        # Extract view count
        view_text = renderer.get("viewCountText", {}).get("simpleText", "0")
        views = self._parse_view_count(view_text)

        # Extract published time
        published_text = renderer.get("publishedTimeText", {}).get("simpleText", "")
        published_at = self._estimate_publish_date(published_text)

        # Duration
        duration_text = renderer.get("lengthText", {}).get("simpleText", "")
        duration_seconds = self._parse_duration_text(duration_text)

        # Thumbnail
        thumbnails = renderer.get("thumbnail", {}).get("thumbnails", [])
        thumbnail_url = thumbnails[-1].get("url", "") if thumbnails else ""

        # Channel info
        channel_info = renderer.get("ownerText", {}).get("runs", [{}])[0]
        channel_title = channel_info.get("text", "")

        # Find matched keywords
        combined_text = f"{title} {description}".lower()
        keywords_matched = []
        for kw in config.keywords:
            kw_clean = kw.lower().lstrip("#")
            if kw_clean in combined_text:
                keywords_matched.append(kw)

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
            url=f"https://www.youtube.com/watch?v={video_id}",
            thumbnail_url=thumbnail_url,
            tags=[],  # Not available in search results
            keywords_matched=keywords_matched,
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
        now = datetime.now()
        text = published_text.lower()

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

        return int(now.timestamp())

    def fetch_channel_videos(self, handle: str, delay: float = 2.0) -> dict | None:
        """Fetch videos from a YouTube channel page."""
        self._rate_limit(delay)

        url = self.CHANNEL_VIDEOS_URL.format(handle=handle)
        headers = self._get_headers()

        try:
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            return self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch channel videos for @{handle}: {e}")
            return None

    def search_channel_videos(self, handle: str, query: str, delay: float = 2.0) -> dict | None:
        """Search for videos from a specific channel with a query."""
        self._rate_limit(delay)

        # YouTube search with channel filter
        search_query = f"{query} site:youtube.com/@{handle}"
        params = {"search_query": search_query}
        headers = self._get_headers()

        try:
            response = self.session.get(self.CHANNEL_SEARCH_URL, params=params, headers=headers)
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
                video_renderer = item.get("richItemRenderer", {}).get("content", {}).get("videoRenderer", {})
                if video_renderer:
                    yield video_renderer

        # Try search results structure
        primary_contents = contents.get("twoColumnSearchResultsRenderer", {}).get("primaryContents", {})
        section_list = primary_contents.get("sectionListRenderer", {})
        for section in section_list.get("contents", []):
            items = section.get("itemSectionRenderer", {}).get("contents", [])
            for item in items:
                video_renderer = item.get("videoRenderer", {})
                if video_renderer:
                    yield video_renderer

    def scrape(self, config: YouTubeScrapeConfig) -> list[YouTubeVideo]:
        """
        Scrape videos from a YouTube channel with filtering.

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

        # If keywords are specified, search for each keyword
        if config.keywords:
            for keyword in config.keywords:
                logger.info(f"Searching for '{keyword}' on @{handle}...")
                data = self.search_channel_videos(handle, keyword, config.delay_seconds)
                if data:
                    videos.extend(self._process_video_data(data, config))

                if config.max_results and len(videos) >= config.max_results:
                    break
        else:
            # Fetch all channel videos
            logger.info(f"Fetching all videos from @{handle}...")
            data = self.fetch_channel_videos(handle, config.delay_seconds)
            if data:
                videos.extend(self._process_video_data(data, config))

        # Deduplicate by video_id
        seen = set()
        unique_videos = []
        for video in videos:
            if video.video_id not in seen:
                seen.add(video.video_id)
                unique_videos.append(video)

        # Apply max_results limit
        if config.max_results:
            unique_videos = unique_videos[: config.max_results]

        logger.info(f"Scrape complete: found {len(unique_videos)} videos")
        return unique_videos

    def _process_video_data(self, data: dict, config: YouTubeScrapeConfig) -> list[YouTubeVideo]:
        """Process video data and apply filters."""
        videos = []
        for renderer in self._iter_video_renderers(data):
            video = self._parse_video_renderer(renderer, config)
            if not video:
                continue

            # Check date range if publish date is known
            if video.published_at > 0:
                in_range = config.is_in_date_range(video.published_at)
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
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        logger.info(f"Fetching comments for video {video_id}")

        # First, get the video page to extract the comment continuation token
        self._rate_limit(delay)
        try:
            response = self.session.get(video_url, headers=self._get_headers())
            response.raise_for_status()
            yt_data = self._extract_ytinital_data(response.text)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch video page: {e}")
            return []

        if not yt_data:
            logger.error("Could not extract ytInitialData from video page")
            return []

        # Extract comment section continuation token
        continuation_token = self._extract_comment_continuation(yt_data)
        if not continuation_token:
            logger.warning("No comment continuation token found - video may have comments disabled")
            return []

        comments = []
        comments_fetched = 0

        while continuation_token:
            self._rate_limit(delay)

            # Fetch comments using continuation
            comment_data = self._fetch_comment_continuation(continuation_token, delay)
            if not comment_data:
                break

            # Parse comments from response
            items, next_continuation = self._parse_comment_response(comment_data)

            for item in items:
                comment = self._parse_comment_thread(item, video_id, video_url, fetch_replies, delay)
                if comment:
                    comments.append(comment)
                    comments_fetched += 1
                    logger.info(f"  Comment {comment.comment_id}: {comment.likes} likes, {comment.reply_count} replies")

                if max_comments and comments_fetched >= max_comments:
                    break

            logger.info(f"Fetched {len(comments)} comments so far...")

            if max_comments and comments_fetched >= max_comments:
                break
            continuation_token = next_continuation

        logger.info(f"Total: {len(comments)} comments fetched for video {video_id}")
        return comments

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
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
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
    ) -> YouTubeComment | None:
        """Parse a comment thread into YouTubeComment."""
        try:
            thread = item.get("commentThreadRenderer", {})
            comment_renderer = thread.get("comment", {}).get("commentRenderer", {})
            if not comment_renderer:
                return None

            comment = self._parse_comment_renderer(comment_renderer, video_id, video_url)
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
            try:
                on_response = data.get("onResponseReceivedEndpoints", [])
                for endpoint in on_response:
                    append_items = endpoint.get("appendContinuationItemsAction", {})
                    if append_items:
                        for item in append_items.get("continuationItems", []):
                            comment_renderer = item.get("commentRenderer", {})
                            if comment_renderer:
                                reply = self._parse_comment_renderer(
                                    comment_renderer, video_id, video_url, is_reply=True, parent_id=parent_id
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
