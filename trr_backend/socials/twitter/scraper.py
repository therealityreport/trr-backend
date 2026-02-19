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

        # Note: -filter:replies and -filter:links operators cause 404 errors
        # from Twitter's current API. Skip them; replies/links are filtered
        # client-side when needed.

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

    # Base URL for GraphQL endpoints (hash is discovered dynamically)
    GRAPHQL_BASE_URL = "https://x.com/i/api/graphql"
    GUEST_ACTIVATE_URL = "https://api.twitter.com/1.1/guest/activate.json"
    MAIN_PAGE_URL = "https://x.com"

    # Fallback hashes (updated periodically, auto-discovered at runtime)
    _FALLBACK_SEARCH_HASH = "cGK-Qeg1XJc2sZ6kgQw_Iw"
    _FALLBACK_DETAIL_HASH = "VWFGPVAGkZMGRKGe3GFFnA"

    # Public bearer token used by Twitter's web app (not a secret).
    PUBLIC_BEARER_TOKEN = (
        "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
        "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
    )

    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.5
    REQUEST_TIMEOUT_SECONDS = (10, 45)

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
    # TweetDetail has stricter feature requirements than SearchTimeline.
    TWEET_DETAIL_FEATURE_OVERRIDES = {
        "post_ctas_fetch_enabled": False,
        "responsive_web_grok_annotations_enabled": False,
        "responsive_web_grok_analysis_button_from_backend": False,
        "responsive_web_grok_imagine_annotation_enabled": False,
        "responsive_web_grok_community_note_auto_translation_is_enabled": False,
        "responsive_web_profile_redirect_enabled": False,
        "responsive_web_grok_show_grok_translated_post": False,
        "responsive_web_grok_share_attachment_enabled": False,
        "profile_label_improvements_pcf_label_in_post_enabled": False,
        "premium_content_api_read_enabled": False,
        "responsive_web_grok_analyze_post_followups_enabled": False,
        "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
        "responsive_web_grok_image_annotation_enabled": False,
        "rweb_video_screen_enabled": False,
        "responsive_web_jetfuel_frame": False,
    }

    def __init__(
        self,
        cookies: dict | None = None,
        bearer_token: str | None = None,
        twikit_credentials: dict | None = None,
    ):
        self.cookies = cookies or {}
        self.bearer_token = bearer_token or self.PUBLIC_BEARER_TOKEN
        self.session = self._create_session()
        self._request_count = 0
        self._guest_token: str | None = None
        self._search_hash: str | None = None
        # twikit credentials: {"username": ..., "email": ..., "password": ...}
        self._twikit_credentials = twikit_credentials
        self._detail_hash: str | None = None
        self._last_graphql_status_code: int | None = None
        self.last_retrieval_meta: dict[str, Any] = {}

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

    def _activate_guest_token(self) -> bool:
        """Activate a guest token for unauthenticated access."""
        if self._guest_token:
            return True
        headers = {
            "authorization": f"Bearer {self.bearer_token}",
            "origin": "https://x.com",
            "referer": "https://x.com/",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
        }
        try:
            response = self.session.post(
                self.GUEST_ACTIVATE_URL,
                headers=headers,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            data = response.json()
            self._guest_token = data.get("guest_token")
            if self._guest_token:
                logger.info("Activated guest token for Twitter API access")
                return True
            logger.error("Guest token activation returned no token")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to activate guest token: {e}")
            return False

    def _ensure_auth(self):
        """Ensure we have some form of authentication (cookies or guest token)."""
        if self.cookies.get("ct0"):
            return  # Have cookie-based auth
        self._activate_guest_token()

    def _discover_graphql_hashes(self):
        """Discover current GraphQL operation hashes from Twitter's JS bundle."""
        if self._search_hash and self._detail_hash:
            return

        try:
            headers = {
                "user-agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/144.0.0.0 Safari/537.36"
                ),
            }
            resp = self.session.get(
                self.MAIN_PAGE_URL,
                headers=headers,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()

            # Find candidate JS bundles where operation names are defined.
            js_urls = re.findall(
                r'src="(https://abs\.twimg\.com/responsive-web/client-web/[^"]+\.js)"',
                resp.text,
            )
            js_urls = list(dict.fromkeys(js_urls))
            if not js_urls:
                raise ValueError("Could not find main JS bundle URL")

            for js_url in js_urls[:8]:
                js_resp = self.session.get(
                    js_url,
                    headers=headers,
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                )
                js_resp.raise_for_status()
                js_text = js_resp.text

                if not self._search_hash:
                    match = re.search(
                        r'queryId:"([a-zA-Z0-9_-]+)",operationName:"SearchTimeline"',
                        js_text,
                    ) or re.search(
                        r'operationName:"SearchTimeline",queryId:"([a-zA-Z0-9_-]+)"',
                        js_text,
                    )
                    if match:
                        self._search_hash = match.group(1)
                        logger.info("Discovered SearchTimeline hash from %s", js_url)

                if not self._detail_hash:
                    match = re.search(
                        r'queryId:"([a-zA-Z0-9_-]+)",operationName:"TweetDetail"',
                        js_text,
                    ) or re.search(
                        r'operationName:"TweetDetail",queryId:"([a-zA-Z0-9_-]+)"',
                        js_text,
                    )
                    if match:
                        self._detail_hash = match.group(1)
                        logger.info("Discovered TweetDetail hash from %s", js_url)

                if self._search_hash and self._detail_hash:
                    break

        except Exception as e:
            logger.warning(f"Failed to discover GraphQL hashes: {e}; using fallbacks")

        # Use fallbacks if discovery failed
        if not self._search_hash:
            self._search_hash = self._FALLBACK_SEARCH_HASH
        if not self._detail_hash:
            self._detail_hash = self._FALLBACK_DETAIL_HASH

    @property
    def _search_timeline_url(self) -> str:
        self._discover_graphql_hashes()
        return f"{self.GRAPHQL_BASE_URL}/{self._search_hash}/SearchTimeline"

    @property
    def _tweet_detail_url(self) -> str:
        self._discover_graphql_hashes()
        return f"{self.GRAPHQL_BASE_URL}/{self._detail_hash}/TweetDetail"

    def _rate_limit(self, delay: float):
        """Apply rate limiting between requests."""
        if self._request_count > 0:
            logger.debug(f"Rate limiting: waiting {delay}s")
            time.sleep(delay)
        self._request_count += 1

    # Syndication endpoints (public, no auth required)
    SYNDICATION_TIMELINE_URL = "https://syndication.twitter.com/srv/timeline-profile/screen-name/{username}"

    def _extract_hashtags(self, text: str) -> list[str]:
        """Extract hashtags from text."""
        return re.findall(r"#(\w+)", text)

    def _extract_mentions(self, text: str) -> list[str]:
        """Extract @mentions from text."""
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
        user_core = user_result.get("core", {})

        tweet_id = tweet.get("id_str", "")
        username = user.get("screen_name", "") or user_core.get("screen_name", "")
        display_name = user.get("name", "") or user_core.get("name", "")
        user_verified = bool(user_result.get("is_blue_verified") or user.get("verified"))

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
            display_name=display_name,
            user_verified=user_verified,
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

    def _parse_syndication_tweet(self, tweet_data: dict, config: TwitterScrapeConfig) -> Tweet | None:
        """Parse a tweet from the syndication __NEXT_DATA__ response."""
        tweet_id = tweet_data.get("id_str", "")
        if not tweet_id:
            return None

        created_at_str = tweet_data.get("created_at", "")
        try:
            created_at_dt = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
            created_at = int(created_at_dt.timestamp())
            date_time = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            created_at = 0
            date_time = ""

        user = tweet_data.get("user", {})
        username = user.get("screen_name", "")
        text = tweet_data.get("full_text", "") or tweet_data.get("text", "")

        # Extract media
        media_urls = []
        entities = tweet_data.get("extended_entities", {}) or tweet_data.get("entities", {})
        for media in (entities.get("media", []) if isinstance(entities, dict) else []):
            media_url = media.get("media_url_https") or media.get("media_url")
            if media_url:
                media_urls.append(media_url)

        return Tweet(
            tweet_id=tweet_id,
            date_time=date_time,
            created_at=created_at,
            text=text,
            hashtags=self._extract_hashtags(text),
            mentions=self._extract_mentions(text),
            likes=tweet_data.get("favorite_count", 0),
            retweets=tweet_data.get("retweet_count", 0),
            replies=tweet_data.get("reply_count", 0),
            quotes=tweet_data.get("quote_count", 0),
            views=0,
            url=f"https://x.com/{username}/status/{tweet_id}" if tweet_id and username else "",
            username=username,
            display_name=user.get("name", ""),
            user_verified=bool(user.get("verified") or user.get("is_blue_verified")),
            is_reply=bool(tweet_data.get("in_reply_to_status_id_str")),
            is_retweet=bool(tweet_data.get("retweeted_status")),
            is_quote=bool(tweet_data.get("quoted_status")),
            reply_to_tweet_id=tweet_data.get("in_reply_to_status_id_str"),
            quoted_tweet_id=None,
            media_urls=media_urls,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _scrape_syndication(self, username: str, config: TwitterScrapeConfig) -> list[Tweet]:
        """Scrape tweets via the public syndication API (no auth required)."""
        import json

        logger.info(f"Using syndication API for @{username}")
        self._rate_limit(config.delay_seconds)

        url = self.SYNDICATION_TIMELINE_URL.format(username=username)
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
            response = self.session.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Syndication request failed for @{username}: {e}")
            return []

        # Extract __NEXT_DATA__ from the HTML
        match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            response.text,
            re.DOTALL,
        )
        if not match:
            logger.error("Could not extract __NEXT_DATA__ from syndication response")
            return []

        try:
            next_data = json.loads(match.group(1))
        except json.JSONDecodeError:
            logger.error("Failed to parse __NEXT_DATA__ JSON")
            return []

        entries = (
            next_data.get("props", {})
            .get("pageProps", {})
            .get("timeline", {})
            .get("entries", [])
        )

        tweets = []
        for entry in entries:
            tweet_data = entry.get("content", {}).get("tweet", {})
            if not tweet_data:
                continue

            tweet = self._parse_syndication_tweet(tweet_data, config)
            if not tweet:
                continue

            # Check date range
            if tweet.created_at > 0:
                if tweet.created_at < config.date_start.timestamp():
                    continue
                if tweet.created_at > config.date_end.timestamp():
                    continue

            tweets.append(tweet)
            logger.info(
                f"Found #{len(tweets)}: @{tweet.username} ({tweet.date_time}) "
                f"- {tweet.likes:,} likes, {tweet.retweets:,} RTs"
            )

        logger.info(f"Syndication scrape: found {len(tweets)} tweets for @{username}")
        return tweets

    def _scrape_via_twikit(self, config: TwitterScrapeConfig) -> list[Tweet]:
        """
        Scrape tweets using the twikit library (requires credentials or cookies).

        Supports two auth modes:
        1. Cookies: dict with 'auth_token' and 'ct0' keys (preferred, avoids Cloudflare)
        2. Login: dict with 'username', 'email', 'password' keys

        Falls back gracefully if twikit is not installed or credentials
        are missing.
        """
        if not self._twikit_credentials:
            return []

        try:
            from twikit import Client as TwikitClient  # noqa: F811
        except ImportError:
            logger.warning("twikit not installed; skipping twikit fallback")
            return []

        import asyncio

        async def _search() -> list[Tweet]:
            client = TwikitClient("en-US")

            # Prefer cookie-based auth (bypasses Cloudflare)
            auth_token = self._twikit_credentials.get("auth_token", "")
            ct0 = self._twikit_credentials.get("ct0", "")

            if auth_token and ct0:
                logger.info("twikit: using cookie-based auth")
                client.set_cookies({"auth_token": auth_token, "ct0": ct0})
            else:
                username = self._twikit_credentials.get("username", "")
                email = self._twikit_credentials.get("email", "")
                password = self._twikit_credentials.get("password", "")

                try:
                    await client.login(
                        auth_info_1=email or username,
                        auth_info_2=username,
                        password=password,
                    )
                except Exception as exc:
                    logger.error(f"twikit login failed: {exc}")
                    return []

            search_query = config.build_search_query()
            logger.info(f"twikit search: {search_query}")

            tweets: list[Tweet] = []
            cursor = None
            page = 0

            while True:
                page += 1
                if config.max_pages and page > config.max_pages:
                    break

                try:
                    if cursor:
                        results = await cursor.next()
                    else:
                        results = await client.search_tweet(
                            search_query, "Latest", count=20
                        )
                except Exception as exc:
                    # Retry once on first page (transient 404s from Twitter)
                    if page == 1:
                        logger.warning(
                            f"twikit search page 1 failed ({exc}); retrying in 5s..."
                        )
                        await asyncio.sleep(5)
                        try:
                            results = await client.search_tweet(
                                search_query, "Latest", count=20
                            )
                        except Exception as retry_exc:
                            logger.error(f"twikit retry also failed: {retry_exc}")
                            break
                    else:
                        logger.error(f"twikit search page {page} failed: {exc}")
                        break

                if not results:
                    break

                for t in results:
                    try:
                        created_at_dt = datetime.strptime(
                            t.created_at, "%a %b %d %H:%M:%S %z %Y"
                        )
                        created_at = int(created_at_dt.timestamp())
                        date_time = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        created_at = 0
                        date_time = ""

                    text = t.full_text or t.text or ""

                    tweet = Tweet(
                        tweet_id=str(t.id),
                        date_time=date_time,
                        created_at=created_at,
                        text=text,
                        hashtags=self._extract_hashtags(text),
                        mentions=self._extract_mentions(text),
                        likes=t.favorite_count or 0,
                        retweets=t.retweet_count or 0,
                        replies=t.reply_count or 0,
                        quotes=t.quote_count or 0,
                        views=t.view_count or 0,
                        url=f"https://x.com/{t.user.screen_name}/status/{t.id}"
                        if t.user
                        else "",
                        username=t.user.screen_name if t.user else "",
                        display_name=t.user.name if t.user else "",
                        user_verified=bool(
                            getattr(t.user, "is_blue_verified", False)
                        )
                        if t.user
                        else False,
                        is_reply=bool(t.in_reply_to),
                        is_retweet=bool(getattr(t, "retweeted_tweet", None)),
                        is_quote=bool(getattr(t, "quoted_tweet", None)),
                        reply_to_tweet_id=str(t.in_reply_to)
                        if t.in_reply_to
                        else None,
                        quoted_tweet_id=None,
                        media_urls=[],
                        show_id=config.show_id,
                        season_number=config.season_number,
                        person_id=config.person_id,
                    )
                    tweets.append(tweet)
                    logger.info(
                        f"Found #{len(tweets)}: @{tweet.username} ({tweet.date_time}) "
                        f"- {tweet.likes:,} likes, {tweet.retweets:,} RTs"
                    )

                cursor = results
                time.sleep(config.delay_seconds)

            return tweets

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import nest_asyncio

                nest_asyncio.apply()
            return loop.run_until_complete(_search())
        except RuntimeError:
            return asyncio.run(_search())

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

        url = f"{self._search_timeline_url}?{urllib.parse.urlencode(params)}"
        headers = self._get_headers()

        try:
            response = self.session.get(
                url,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            self._last_graphql_status_code = response.status_code
            return response.json()
        except requests.exceptions.RequestException as e:
            self._last_graphql_status_code = getattr(getattr(e, "response", None), "status_code", None)
            logger.error(f"Search request failed: {e}")
            return None

    def _extract_required_feature_flags(self, response: requests.Response) -> list[str]:
        """Parse Twitter validation errors and extract missing feature-flag names."""
        try:
            payload = response.json()
        except ValueError:
            return []
        flags: list[str] = []
        for error in payload.get("errors", []) or []:
            message = str(error.get("message", "") or "")
            marker = "cannot be null:"
            idx = message.find(marker)
            if idx < 0:
                continue
            suffix = message[idx + len(marker) :]
            for raw_name in suffix.split(","):
                name = raw_name.strip().strip(".")
                if not name:
                    continue
                if re.fullmatch(r"[A-Za-z0-9_]+", name):
                    flags.append(name)
        return list(dict.fromkeys(flags))

    def fetch_tweet_replies(self, tweet_id: str, delay: float = 2.0) -> list[Tweet]:
        """Fetch replies to a specific tweet."""
        import json
        import urllib.parse

        self._ensure_auth()
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

        features = dict(self.FEATURES)
        features.update(self.TWEET_DETAIL_FEATURE_OVERRIDES)
        headers = self._get_headers()

        def _request(detail_features: dict[str, bool]) -> requests.Response:
            params = {
                "variables": json.dumps(variables),
                "features": json.dumps(detail_features),
            }
            url = f"{self._tweet_detail_url}?{urllib.parse.urlencode(params)}"
            return self.session.get(
                url,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )

        try:
            response = _request(features)
            if response.status_code == 404:
                # Hashes rotate frequently; force one rediscovery and retry.
                self._detail_hash = None
                self._discover_graphql_hashes()
                response = _request(features)
            if response.status_code == 400:
                # Twitter frequently adds required flags. Auto-apply once when signaled.
                missing_flags = self._extract_required_feature_flags(response)
                if missing_flags:
                    logger.info("TweetDetail requires %d additional feature flags; retrying", len(missing_flags))
                    for flag in missing_flags:
                        features.setdefault(flag, False)
                    response = _request(features)
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

        Tries GraphQL search first (requires auth cookies). If that fails,
        falls back to the syndication API (public, no auth) for ``from:``
        queries.

        Args:
            config: TwitterScrapeConfig with query, date range, etc.

        Returns:
            List of Tweet objects matching the search.
        """
        self._ensure_auth()

        search_query = config.build_search_query()
        logger.info(f"Starting Twitter search: {search_query}")

        tweets: list[Tweet] = []
        graphql_404_count = 0
        fallback_triggered = False
        retrieval_mode = "graphql"
        cursor = None
        page_num = 0
        graphql_failed = False

        from_match = re.search(r"(?:^|\s)from:(\w+)", config.query)
        if from_match and (self._twikit_credentials or not self.cookies.get("ct0")):
            fallback_triggered = True
            if self._twikit_credentials:
                logger.info("Using twikit as primary mode for from: query")
                tweets = self._scrape_via_twikit(config)
                retrieval_mode = "twikit"
            if not tweets:
                username = from_match.group(1)
                tweets = self._scrape_syndication(username, config)
                retrieval_mode = "syndication"
            if tweets:
                self.last_retrieval_meta = {
                    "retrieval_mode": retrieval_mode,
                    "graphql_404_count": graphql_404_count,
                    "fallback_triggered": fallback_triggered,
                    "tweet_count": len(tweets),
                }
                return tweets

        while True:
            page_num += 1
            if config.max_pages and page_num > config.max_pages:
                logger.info(f"Reached max pages limit ({config.max_pages})")
                break

            logger.info(f"Fetching page {page_num}...")
            data = self._fetch_search(search_query, cursor, config.delay_seconds)
            if not data:
                if self._last_graphql_status_code == 404 and graphql_404_count < 1:
                    graphql_404_count += 1
                    # Hashes likely rotated; force rediscovery once.
                    self._search_hash = None
                    self._detail_hash = None
                    logger.warning("GraphQL returned 404; retrying after hash rediscovery")
                    continue
                graphql_failed = True
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

        # Fallback chain when GraphQL fails
        if graphql_failed and not tweets:
            fallback_triggered = True
            # 1. Try twikit (authenticated search via credentials)
            if self._twikit_credentials:
                import time

                # Brief delay after GraphQL failure to avoid Twitter rate limits
                time.sleep(3)
                logger.info("GraphQL failed; trying twikit search...")
                tweets = self._scrape_via_twikit(config)
                retrieval_mode = "twikit"

            # 2. Try syndication API for from: queries (public, no auth)
            if not tweets:
                from_match = re.search(r"from:(\w+)", config.query)
                if from_match:
                    username = from_match.group(1)
                    logger.info(
                        f"Falling back to syndication API for @{username}"
                    )
                    tweets = self._scrape_syndication(username, config)
                    retrieval_mode = "syndication"
                elif not self._twikit_credentials:
                    logger.warning(
                        "Twitter requires authentication for search. "
                        "Set SOCIAL_TWITTER_COOKIES_JSON, TWITTER_COOKIES_FILE, "
                        "or TWIKIT_USERNAME + TWIKIT_PASSWORD env vars."
                    )

        logger.info(f"Search complete: found {len(tweets)} tweets")
        self.last_retrieval_meta = {
            "retrieval_mode": retrieval_mode,
            "graphql_404_count": graphql_404_count,
            "fallback_triggered": fallback_triggered,
            "tweet_count": len(tweets),
        }
        return tweets
