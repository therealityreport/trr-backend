"""
Twitter/X scraper module for searching tweets.

Supports:
- Searching tweets by hashtag or phrase
- Filtering by date range
- Including/excluding replies
- Fetching tweet replies/comments
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

ADVANCED_QUERY_HINT_RE = re.compile(
    r'(^|\s)(from:|to:|since:|until:|filter:|-filter:)|\bOR\b|\bAND\b|[()"]',
    re.IGNORECASE,
)


@dataclass
class TwitterScrapeConfig:
    """Configuration for a Twitter scrape operation."""

    query: str  # Search term (hashtag like "RHOSLC" or phrase)
    date_start: datetime
    date_end: datetime
    include_replies: bool = False
    include_links: bool = True
    delay_seconds: float = 2.0
    max_pages: int | None = None  # None = no limit

    # Metadata for tracking
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def build_search_query(self) -> str:
        """Build Twitter advanced search query string."""
        parts = []

        normalized_query = self.query.strip()

        # Add the main query (supports raw advanced query passthrough).
        if ADVANCED_QUERY_HINT_RE.search(normalized_query):
            parts.append(normalized_query)
        elif normalized_query.startswith("#"):
            parts.append(normalized_query)
        else:
            # Search for exact phrase or hashtag
            parts.append(f'"{normalized_query}" OR #{normalized_query}')

        # Add date filters
        parts.append(f"since:{self.date_start.strftime('%Y-%m-%d')}")
        parts.append(f"until:{self.date_end.strftime('%Y-%m-%d')}")

        # Add filters
        if not self.include_replies:
            parts.append("-filter:replies")
        if not self.include_links:
            parts.append("-filter:links")

        return " ".join(parts)


@dataclass
class Tweet:
    """Represents a single tweet with extracted data."""

    tweet_id: str
    date_time: str
    created_at: int  # Unix timestamp
    text: str
    hashtags: list[str]
    mentions: list[str]
    likes: int
    retweets: int
    replies: int
    quotes: int
    views: int
    url: str
    username: str
    display_name: str
    user_verified: bool
    is_reply: bool
    is_retweet: bool
    is_quote: bool
    reply_to_tweet_id: str | None = None
    quoted_tweet_id: str | None = None
    media_urls: list[str] = field(default_factory=list)

    # Optional tracking metadata
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class TwitterScraper:
    """Twitter/X scraper for searching tweets."""

    # Twitter GraphQL endpoints
    SEARCH_TIMELINE_URL = "https://x.com/i/api/graphql/UN1i3zUiCWa-6r-Uaho4fw/SearchTimeline"
    TWEET_DETAIL_URL = "https://x.com/i/api/graphql/VWFGPVAGkZMGRKGe3GFFnA/TweetDetail"

    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.5

    # Required features for GraphQL queries
    FEATURES = {
        "rweb_tipjar_consumption_enabled": True,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "articles_preview_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "creator_subscriptions_quote_tweet_preview_enabled": False,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "rweb_video_timestamps_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_enhance_cards_enabled": False,
    }

    def __init__(self, cookies: dict | None = None, bearer_token: str | None = None):
        self.cookies = cookies or {}
        self.bearer_token = bearer_token
        self.session = self._create_session()
        self._request_count = 0
        self._guest_token: str | None = None

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
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "authorization": f"Bearer {self.bearer_token}" if self.bearer_token else "",
            "content-type": "application/json",
            "origin": "https://x.com",
            "referer": "https://x.com/search",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
            "x-twitter-active-user": "yes",
            "x-twitter-client-language": "en",
        }
        if self.cookies.get("ct0"):
            headers["x-csrf-token"] = self.cookies["ct0"]
        if self._guest_token:
            headers["x-guest-token"] = self._guest_token
        return headers

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

    def _parse_tweet_result(self, result: dict, config: TwitterScrapeConfig) -> Tweet | None:
        """Parse a tweet result from GraphQL response."""
        # Handle different result types
        if result.get("__typename") == "TweetWithVisibilityResults":
            result = result.get("tweet", {})

        tweet = result.get("legacy", {})
        if not tweet:
            return None

        user_result = result.get("core", {}).get("user_results", {}).get("result", {})
        user = user_result.get("legacy", {})

        tweet_id = tweet.get("id_str", "")
        username = user.get("screen_name", "")

        # Parse created_at
        created_at_str = tweet.get("created_at", "")
        try:
            created_at_dt = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
            created_at = int(created_at_dt.timestamp())
            date_time = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            created_at = 0
            date_time = ""

        # Extract media URLs
        media_urls = []
        entities = tweet.get("extended_entities", {}) or tweet.get("entities", {})
        for media in entities.get("media", []):
            media_url = media.get("media_url_https") or media.get("media_url")
            if media_url:
                media_urls.append(media_url)

        # Get engagement metrics
        views_data = result.get("views", {})
        views = int(views_data.get("count", 0)) if views_data.get("count") else 0

        text = tweet.get("full_text", "") or tweet.get("text", "")

        return Tweet(
            tweet_id=tweet_id,
            date_time=date_time,
            created_at=created_at,
            text=text,
            hashtags=self._extract_hashtags(text),
            mentions=self._extract_mentions(text),
            likes=tweet.get("favorite_count", 0),
            retweets=tweet.get("retweet_count", 0),
            replies=tweet.get("reply_count", 0),
            quotes=tweet.get("quote_count", 0),
            views=views,
            url=f"https://x.com/{username}/status/{tweet_id}" if tweet_id and username else "",
            username=username,
            display_name=user.get("name", ""),
            user_verified=user_result.get("is_blue_verified", False),
            is_reply=bool(tweet.get("in_reply_to_status_id_str")),
            is_retweet=bool(tweet.get("retweeted_status_result")),
            is_quote=bool(result.get("quoted_status_result")),
            reply_to_tweet_id=tweet.get("in_reply_to_status_id_str"),
            quoted_tweet_id=result.get("quoted_status_result", {}).get("result", {}).get("legacy", {}).get("id_str"),
            media_urls=media_urls,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _fetch_search(self, query: str, cursor: str | None = None, delay: float = 2.0) -> dict | None:
        """Fetch search results."""
        import json
        import urllib.parse

        self._rate_limit(delay)

        variables = {
            "rawQuery": query,
            "count": 20,
            "querySource": "typed_query",
            "product": "Latest",
        }
        if cursor:
            variables["cursor"] = cursor

        params = {
            "variables": json.dumps(variables),
            "features": json.dumps(self.FEATURES),
        }

        url = f"{self.SEARCH_TIMELINE_URL}?{urllib.parse.urlencode(params)}"
        headers = self._get_headers()

        try:
            response = self.session.get(url, headers=headers, cookies=self.cookies)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Search request failed: {e}")
            return None

    def fetch_tweet_replies(self, tweet_id: str, delay: float = 2.0) -> list[Tweet]:
        """Fetch replies to a specific tweet."""
        import json
        import urllib.parse

        self._rate_limit(delay)

        variables = {
            "focalTweetId": tweet_id,
            "with_rux_injections": False,
            "rankingMode": "Relevance",
            "includePromotedContent": False,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
        }

        params = {
            "variables": json.dumps(variables),
            "features": json.dumps(self.FEATURES),
        }

        url = f"{self.TWEET_DETAIL_URL}?{urllib.parse.urlencode(params)}"
        headers = self._get_headers()

        try:
            response = self.session.get(url, headers=headers, cookies=self.cookies)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Tweet detail request failed: {e}")
            return []

        # Parse replies from response
        replies = []
        instructions = data.get("data", {}).get("threaded_conversation_with_injections_v2", {}).get("instructions", [])

        for instruction in instructions:
            if instruction.get("type") != "TimelineAddEntries":
                continue

            for entry in instruction.get("entries", []):
                # Skip the focal tweet and cursor entries
                if not entry.get("entryId", "").startswith("conversationthread-"):
                    continue

                items = entry.get("content", {}).get("items", [])
                for item in items:
                    tweet_result = (
                        item.get("item", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                    )
                    if tweet_result:
                        config = TwitterScrapeConfig(query="", date_start=datetime.now(), date_end=datetime.now())
                        tweet = self._parse_tweet_result(tweet_result, config)
                        if tweet and tweet.tweet_id != tweet_id:
                            replies.append(tweet)

        return replies

    def scrape(self, config: TwitterScrapeConfig) -> list[Tweet]:
        """
        Scrape tweets matching the search criteria.

        Args:
            config: TwitterScrapeConfig with query, date range, etc.

        Returns:
            List of Tweet objects matching the search.
        """
        search_query = config.build_search_query()
        logger.info(f"Starting Twitter search: {search_query}")

        tweets = []
        cursor = None
        page_num = 0

        while True:
            page_num += 1
            if config.max_pages and page_num > config.max_pages:
                logger.info(f"Reached max pages limit ({config.max_pages})")
                break

            logger.info(f"Fetching page {page_num}...")
            data = self._fetch_search(search_query, cursor, config.delay_seconds)
            if not data:
                break

            # Parse search results
            search_data = data.get("data", {}).get("search_by_raw_query", {})
            timeline = search_data.get("search_timeline", {}).get("timeline", {})
            instructions = timeline.get("instructions", [])

            tweets_on_page = 0
            next_cursor = None

            for instruction in instructions:
                if instruction.get("type") == "TimelineAddEntries":
                    for entry in instruction.get("entries", []):
                        entry_id = entry.get("entryId", "")

                        # Handle cursor entries
                        if entry_id.startswith("cursor-bottom-"):
                            next_cursor = entry.get("content", {}).get("value")
                            continue
                        if entry_id.startswith("cursor-top-"):
                            continue

                        # Handle tweet entries
                        if entry_id.startswith("tweet-"):
                            tweet_result = (
                                entry.get("content", {})
                                .get("itemContent", {})
                                .get("tweet_results", {})
                                .get("result", {})
                            )
                            if tweet_result:
                                tweet = self._parse_tweet_result(tweet_result, config)
                                if tweet:
                                    tweets_on_page += 1
                                    tweets.append(tweet)
                                    logger.info(
                                        f"Found #{len(tweets)}: @{tweet.username} ({tweet.date_time}) "
                                        f"- {tweet.likes:,} likes, {tweet.retweets:,} RTs"
                                    )

            if tweets_on_page == 0:
                logger.info("No more tweets found")
                break

            logger.info(f"Page {page_num}: found {tweets_on_page} tweets, {len(tweets)} total")

            # Get next page
            cursor = next_cursor
            if not cursor:
                logger.info("No more pages available")
                break

        logger.info(f"Search complete: found {len(tweets)} tweets")
        return tweets
