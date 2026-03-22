"""
Twitter/X scraper module for searching tweets.

Supports:
- Searching tweets by hashtag or phrase
- Filtering by date range
- Including/excluding replies
- Fetching tweet replies/comments
"""

import logging
import os
import re
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

ADVANCED_QUERY_HINT_RE = re.compile(
    r'(^|\s)(from:|to:|since:|until:|filter:|-filter:)|\bOR\b|\bAND\b|[()"]',
    re.IGNORECASE,
)
MEDIA_URL_EXTENSION_RE = re.compile(r"\.(?:jpg|jpeg|png|gif|webp|bmp|mp4|m4v|mov|webm)(?:$|[?#])", re.IGNORECASE)


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

    # Performance tuning
    fast_mode: bool = False
    """When True, uses aggressive rate-limiting tiers for faster scraping."""

    fetch_comment_replies: bool = True
    """When False, skip reply fetching for bulk scrapes."""

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
                "TwitterScrapeConfig fast_mode enabled: delay=%.2fs",
                self.delay_seconds,
            )

    def build_search_query(self) -> str:
        """Build Twitter advanced search query string."""
        parts = []

        normalized_query = self.query.strip()

        # Add the main query (supports raw advanced query passthrough).
        if ADVANCED_QUERY_HINT_RE.search(normalized_query):
            parts.append(normalized_query)
        elif normalized_query.startswith("#") or normalized_query.startswith("@"):
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
    hosted_media_urls: list[str] = field(default_factory=list)
    link_preview_media_count: int = 0
    user_id: str | None = None
    user_profile_url: str | None = None
    user_avatar_url: str | None = None

    # Optional tracking metadata
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def mirror_tweet_media(tweets: list[Tweet]) -> dict[str, list[str]]:
    from trr_backend.media.s3_mirror import get_s3_bucket, get_s3_client, mirror_urls_to_s3

    mirrored_by_tweet_id: dict[str, list[str]] = {}
    if not tweets:
        return mirrored_by_tweet_id

    try:
        s3_client = get_s3_client()
        bucket = get_s3_bucket()
    except Exception:
        logger.warning("Twitter media mirroring unavailable: failed to initialize S3 client", exc_info=True)
        for tweet in tweets:
            tweet.hosted_media_urls = []
            mirrored_by_tweet_id[str(tweet.tweet_id)] = []
        return mirrored_by_tweet_id

    for tweet in tweets:
        try:
            source_urls: list[str] = []
            seen_source_urls: set[str] = set()
            for value in tweet.media_urls or []:
                candidate = str(value or "").strip()
                if not candidate or candidate in seen_source_urls:
                    continue
                seen_source_urls.add(candidate)
                source_urls.append(candidate)

            if not source_urls:
                tweet.hosted_media_urls = []
                mirrored_by_tweet_id[str(tweet.tweet_id)] = []
                continue

            _tweet_url = f"https://x.com/i/status/{tweet.tweet_id}" if tweet.tweet_id else None
            results = mirror_urls_to_s3(
                source_urls,
                s3_client=s3_client,
                bucket=bucket,
                tweet_url=_tweet_url,
            )
            hosted_urls: list[str] = []
            seen_hosted_urls: set[str] = set()
            for result in results:
                hosted_url = str(result.hosted_url or "").strip()
                if not hosted_url or hosted_url in seen_hosted_urls:
                    continue
                if result.status not in {"mirrored", "skipped"}:
                    continue
                seen_hosted_urls.add(hosted_url)
                hosted_urls.append(hosted_url)

            tweet.hosted_media_urls = hosted_urls
            mirrored_by_tweet_id[str(tweet.tweet_id)] = hosted_urls
        except Exception:
            logger.warning("Twitter media mirroring failed for tweet_id=%s", tweet.tweet_id, exc_info=True)
            tweet.hosted_media_urls = []
            mirrored_by_tweet_id[str(tweet.tweet_id)] = []

    return mirrored_by_tweet_id


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
        "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
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
    QUOTE_SEARCH_FEATURE_OVERRIDES = {
        "rweb_video_screen_enabled": False,
        "profile_label_improvements_pcf_label_in_post_enabled": True,
        "responsive_web_profile_redirect_enabled": False,
        "rweb_tipjar_consumption_enabled": False,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "premium_content_api_read_enabled": False,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
        "responsive_web_grok_analyze_post_followups_enabled": True,
        "responsive_web_jetfuel_frame": True,
        "responsive_web_grok_share_attachment_enabled": True,
        "responsive_web_grok_annotations_enabled": True,
        "articles_preview_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "content_disclosure_indicator_enabled": True,
        "content_disclosure_ai_generated_indicator_enabled": True,
        "responsive_web_grok_show_grok_translated_post": True,
        "responsive_web_grok_analysis_button_from_backend": True,
        "post_ctas_fetch_enabled": True,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_grok_image_annotation_enabled": True,
        "responsive_web_grok_imagine_annotation_enabled": True,
        "responsive_web_grok_community_note_auto_translation_is_enabled": False,
        "responsive_web_enhance_cards_enabled": False,
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
        self._last_429_at: float = 0.0
        self._consecutive_success: int = 0
        self._guest_token: str | None = None
        self._search_hash: str | None = None
        # twikit credentials: {"username": ..., "email": ..., "password": ...}
        self._twikit_credentials = twikit_credentials
        self._detail_hash: str | None = None
        self._last_graphql_status_code: int | None = None
        self.last_retrieval_meta: dict[str, Any] = {}
        self.last_reply_fetch_reason: str | None = None
        self.last_quote_fetch_reason: str | None = None
        self.last_quote_fetch_meta: dict[str, Any] = {}
        self._quote_search_timeline_unavailable = False
        self._last_twikit_search_error: str | None = None
        self._last_playwright_search_error: str | None = None
        self.comments_auth_failed = False

    @staticmethod
    def _is_auth_related_failure(reason: str | None) -> bool:
        value = str(reason or "").strip().lower()
        if not value:
            return False
        markers = (
            "auth",
            "login",
            "challenge",
            "forbidden",
            "unauthorized",
            "http_401",
            "http_403",
        )
        return any(marker in value for marker in markers)

    def _set_reply_failure_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self.last_reply_fetch_reason = normalized
        if self._is_auth_related_failure(normalized):
            self.comments_auth_failed = True

    def _set_quote_failure_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self.last_quote_fetch_reason = normalized
        if self._is_auth_related_failure(normalized):
            self.comments_auth_failed = True

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

    def _rate_limit(self, delay: float, *, fast_mode: bool = False):
        """Apply adaptive rate limiting between requests.

        Standard mode: starts at 50% of the base delay.
        Fast mode: uses aggressive tiers that ramp down with consecutive successes.
        Both modes: double delay for 60s after any 429 response.
        """
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
                    effective_delay = delay * 0.5   # e.g. 0.5 * 0.5 = 0.25s
            elif self._consecutive_success >= 20:
                effective_delay = delay * 0.5
            else:
                effective_delay = delay * 0.5
            logger.debug(f"Rate limiting: waiting {effective_delay:.3f}s (base={delay}s, fast={fast_mode}, streak={self._consecutive_success})")
            time.sleep(effective_delay)
        self._request_count += 1

    def _track_response_status(self, status_code: int) -> None:
        """Track response status for adaptive rate limiting."""
        if status_code == 429:
            self._last_429_at = time.monotonic()
            self._consecutive_success = 0
        elif 200 <= status_code < 400:
            self._consecutive_success += 1

    # Syndication endpoints (public, no auth required)
    SYNDICATION_TIMELINE_URL = "https://syndication.twitter.com/srv/timeline-profile/screen-name/{username}"
    SYNDICATION_TWEET_RESULT_URL = "https://cdn.syndication.twimg.com/tweet-result?id={tweet_id}&lang=en&token={token}"

    def _extract_hashtags(self, text: str) -> list[str]:
        """Extract hashtags from text."""
        return re.findall(r"#(\w+)", text)

    def _extract_mentions(self, text: str) -> list[str]:
        """Extract @mentions from text."""
        return re.findall(r"@(\w+)", text)

    @staticmethod
    def _looks_like_media_url(url: str) -> bool:
        candidate = str(url or "").strip()
        if not candidate.startswith(("http://", "https://")):
            return False
        parsed = urlparse(candidate)
        host = (parsed.netloc or "").lower()
        path = parsed.path or ""
        if any(marker in host for marker in ("twimg.com", "pbs.twimg.com", "video.twimg.com", "ytimg.com")):
            return True
        if MEDIA_URL_EXTENSION_RE.search(path):
            return True
        query = parsed.query.lower()
        return "format=" in query and "name=" in query

    def _collect_url_like_strings(self, payload: Any, out: set[str], *, depth: int = 0) -> None:
        if depth > 6:
            return
        if isinstance(payload, dict):
            for key, value in payload.items():
                key_lower = str(key).lower()
                if isinstance(value, str):
                    normalized = value.strip()
                    if normalized.startswith(("http://", "https://")) and (
                        "url" in key_lower or key_lower.endswith("_uri") or "http" in normalized
                    ):
                        out.add(normalized)
                    continue
                if isinstance(value, (dict, list)):
                    self._collect_url_like_strings(value, out, depth=depth + 1)
            return
        if isinstance(payload, list):
            for item in payload:
                self._collect_url_like_strings(item, out, depth=depth + 1)

    def _extract_media_urls_from_tweet(
        self,
        *,
        tweet_payload: dict[str, Any],
        result_payload: dict[str, Any] | None,
    ) -> tuple[list[str], int]:
        media_urls: list[str] = []
        seen_urls: set[str] = set()
        link_preview_count = 0

        def _append_media(url: str, *, from_preview: bool) -> None:
            nonlocal link_preview_count
            normalized = str(url or "").strip()
            if not normalized or normalized in seen_urls:
                return
            seen_urls.add(normalized)
            media_urls.append(normalized)
            if from_preview:
                link_preview_count += 1

        entities = tweet_payload.get("extended_entities", {}) or tweet_payload.get("entities", {})
        for media in entities.get("media", []) if isinstance(entities, dict) else []:
            media_type = str(media.get("type") or "").strip().lower()
            if media_type in {"video", "animated_gif"}:
                variants = (media.get("video_info") or {}).get("variants") or []
                mp4_variants: list[tuple[int, str]] = []
                for variant in variants:
                    if not isinstance(variant, dict):
                        continue
                    content_type = str(variant.get("content_type") or "").strip().lower()
                    if content_type != "video/mp4":
                        continue
                    variant_url = str(variant.get("url") or "").strip()
                    if not variant_url:
                        continue
                    bitrate = int(variant.get("bitrate") or 0)
                    mp4_variants.append((bitrate, variant_url))
                if mp4_variants:
                    mp4_variants.sort(key=lambda item: item[0], reverse=True)
                    _append_media(mp4_variants[0][1], from_preview=False)
                    continue

            media_url = media.get("media_url_https") or media.get("media_url")
            if isinstance(media_url, str) and media_url.strip():
                _append_media(media_url, from_preview=False)

        candidate_urls: set[str] = set()
        urls_entities = tweet_payload.get("entities", {}).get("urls", [])
        if isinstance(urls_entities, list):
            self._collect_url_like_strings(urls_entities, candidate_urls)
        if isinstance(result_payload, dict):
            card = result_payload.get("card")
            if isinstance(card, dict):
                self._collect_url_like_strings(card, candidate_urls)
        for candidate in sorted(candidate_urls):
            if self._looks_like_media_url(candidate):
                _append_media(candidate, from_preview=True)

        return media_urls, link_preview_count

    def _extract_media_urls_from_syndication_result(self, payload: dict[str, Any]) -> list[str]:
        media_urls: list[str] = []
        seen_urls: set[str] = set()

        def _append(url: str) -> None:
            normalized = str(url or "").strip()
            if not normalized or normalized in seen_urls:
                return
            seen_urls.add(normalized)
            media_urls.append(normalized)

        def _best_mp4_from_variants(variants: Any) -> str | None:
            if not isinstance(variants, list):
                return None
            best_url: str | None = None
            best_size = -1
            for variant in variants:
                if not isinstance(variant, dict):
                    continue
                variant_type = str(variant.get("type") or variant.get("content_type") or "").strip().lower()
                if variant_type != "video/mp4":
                    continue
                src = str(variant.get("src") or variant.get("url") or "").strip()
                if not src:
                    continue
                size = int(variant.get("bitrate") or 0)
                if size <= 0:
                    match = re.search(r"/(\\d+)x(\\d+)/", src)
                    if match:
                        size = int(match.group(1)) * int(match.group(2))
                if size >= best_size:
                    best_size = size
                    best_url = src
            return best_url

        direct_video = payload.get("video")
        if isinstance(direct_video, dict):
            best_video = _best_mp4_from_variants(direct_video.get("variants"))
            if best_video:
                _append(best_video)

        media_details = payload.get("mediaDetails")
        if isinstance(media_details, list):
            for media in media_details:
                if not isinstance(media, dict):
                    continue
                best_video = _best_mp4_from_variants((media.get("video_info") or {}).get("variants"))
                if best_video:
                    _append(best_video)
                media_url = str(media.get("media_url_https") or media.get("media_url") or "").strip()
                if media_url:
                    _append(media_url)

        photos = payload.get("photos")
        if isinstance(photos, list):
            for photo in photos:
                if not isinstance(photo, dict):
                    continue
                photo_url = str(photo.get("url") or photo.get("src") or "").strip()
                if photo_url:
                    _append(photo_url)

        return media_urls

    @staticmethod
    def _normalize_text_url_token(value: str) -> str:
        token = str(value or "").strip()
        return token.rstrip(".,!?:;)]}\"'")

    def _expanded_url_is_media(self, value: str) -> bool:
        normalized = self._normalize_text_url_token(value).lower()
        if not normalized:
            return False
        if self._looks_like_media_url(normalized):
            return True
        if normalized.startswith("https://pic.twitter.com/") or normalized.startswith("http://pic.twitter.com/"):
            return True
        return bool(re.search(r"https?://(?:x|twitter)\.com/[^/]+/status/\d+/(?:photo|video|gif)/\d+", normalized))

    def _extract_media_text_tokens(self, tweet_payload: dict[str, Any]) -> set[str]:
        tokens: set[str] = set()
        entity_groups: list[dict[str, Any]] = []
        entities = tweet_payload.get("entities")
        extended_entities = tweet_payload.get("extended_entities")
        if isinstance(entities, dict):
            entity_groups.append(entities)
        if isinstance(extended_entities, dict):
            entity_groups.append(extended_entities)

        for entity_group in entity_groups:
            media_entries = entity_group.get("media", [])
            if isinstance(media_entries, list):
                for media in media_entries:
                    if not isinstance(media, dict):
                        continue
                    for key in ("url", "expanded_url", "display_url", "media_url_https", "media_url"):
                        value = self._normalize_text_url_token(str(media.get(key) or ""))
                        if value.startswith(("http://", "https://")):
                            tokens.add(value)

            url_entries = entity_group.get("urls", [])
            if not isinstance(url_entries, list):
                continue
            for url_entry in url_entries:
                if not isinstance(url_entry, dict):
                    continue
                url_token = self._normalize_text_url_token(str(url_entry.get("url") or ""))
                expanded = self._normalize_text_url_token(str(url_entry.get("expanded_url") or ""))
                media_marker = self._normalize_text_url_token(str(url_entry.get("media_url_https") or ""))
                if url_token.startswith(("http://", "https://")) and (
                    self._expanded_url_is_media(expanded) or self._expanded_url_is_media(media_marker)
                ):
                    tokens.add(url_token)
        return tokens

    def _strip_media_url_text(
        self,
        *,
        text: str,
        tweet_payload: dict[str, Any],
        result_payload: dict[str, Any] | None,
        media_urls: list[str] | None = None,
    ) -> str:
        raw_text = str(text or "").strip()
        if not raw_text:
            return ""
        media_url_candidates = list(media_urls or [])
        if not media_url_candidates:
            media_url_candidates, _ = self._extract_media_urls_from_tweet(
                tweet_payload=tweet_payload,
                result_payload=result_payload,
            )
        removal_tokens = {self._normalize_text_url_token(url) for url in media_url_candidates}
        removal_tokens.update(self._extract_media_text_tokens(tweet_payload))
        if not removal_tokens:
            return raw_text
        cleaned_tokens: list[str] = []
        for token in raw_text.split():
            normalized_token = self._normalize_text_url_token(token)
            if normalized_token in removal_tokens:
                continue
            cleaned_tokens.append(token)
        return re.sub(r"\s+", " ", " ".join(cleaned_tokens)).strip()

    def fetch_public_tweet_summary(self, tweet_id: str, delay: float = 0.0) -> dict[str, Any] | None:
        """Fetch public tweet metadata from syndication endpoint.

        This endpoint is unauthenticated and helps recover core metadata
        (username/avatar/reply count/media) when GraphQL detail access is blocked.
        """
        normalized_id = str(tweet_id or "").strip()
        if not normalized_id:
            return None
        if delay > 0:
            self._rate_limit(delay)
        token = str(int(time.time()))
        url = self.SYNDICATION_TWEET_RESULT_URL.format(tweet_id=normalized_id, token=token)
        headers = {
            "accept": "application/json,text/plain,*/*",
            "origin": "https://x.com",
            "referer": f"https://x.com/i/status/{normalized_id}",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
        }
        try:
            response = self.session.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT_SECONDS)
            self._track_response_status(response.status_code)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None

        user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
        username = str(user.get("screen_name") or "").strip()
        profile_url = f"https://x.com/{username}" if username else None
        avatar_url = str(user.get("profile_image_url_https") or user.get("profile_image_url") or "").strip() or None

        def _as_int(value: Any) -> int:
            try:
                return int(value)
            except Exception:
                return 0

        media_urls = self._extract_media_urls_from_syndication_result(payload)
        tweet_text = self._strip_media_url_text(
            text=str(payload.get("text") or ""),
            tweet_payload=payload,
            result_payload=payload,
            media_urls=media_urls,
        )
        return {
            "tweet_id": normalized_id,
            "username": username,
            "display_name": str(user.get("name") or "").strip() or None,
            "text": tweet_text or None,
            "url": f"https://x.com/{username}/status/{normalized_id}"
            if username
            else f"https://x.com/i/status/{normalized_id}",
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "user_id": str(user.get("id_str") or user.get("id") or "").strip() or None,
            "user_profile_url": profile_url,
            "user_avatar_url": avatar_url,
            "likes": _as_int(payload.get("favorite_count")),
            "replies": _as_int(payload.get("conversation_count") or payload.get("reply_count")),
            "retweets": _as_int(payload.get("retweet_count")),
            "quotes": _as_int(payload.get("quote_count")),
            "views": _as_int(payload.get("view_count") or payload.get("views")),
            "bookmarks": _as_int(payload.get("bookmark_count")),
            "media_urls": media_urls,
        }

    def fetch_tweet_detail_summary(self, tweet_id: str, delay: float = 0.0) -> dict[str, Any] | None:
        """Fetch root tweet metadata from TweetDetail GraphQL."""
        import json
        import urllib.parse

        normalized_id = str(tweet_id or "").strip()
        if not normalized_id:
            return None

        self._ensure_auth()
        if delay > 0:
            self._rate_limit(delay)

        variables = {
            "focalTweetId": normalized_id,
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
                self._detail_hash = None
                self._discover_graphql_hashes()
                response = _request(features)
            if response.status_code == 400:
                missing_flags = self._extract_required_feature_flags(response)
                if missing_flags:
                    logger.info("TweetDetail requires %d additional feature flags; retrying", len(missing_flags))
                    for flag in missing_flags:
                        features.setdefault(flag, False)
                    response = _request(features)
            self._track_response_status(response.status_code)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None:
                self._track_response_status(status)
            logger.debug("Tweet detail summary request failed for %s: %s", normalized_id, exc)
            return None
        except Exception:
            logger.debug("Tweet detail summary parse failed for %s", normalized_id, exc_info=True)
            return None

        if not isinstance(data, dict):
            return None

        root_result: dict[str, Any] | None = None
        instructions = data.get("data", {}).get("threaded_conversation_with_injections_v2", {}).get("instructions", [])
        for instruction in instructions:
            if instruction.get("type") != "TimelineAddEntries":
                continue
            for entry in instruction.get("entries", []):
                tweet_result = (
                    entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                )
                if not isinstance(tweet_result, dict):
                    continue
                rest_id = str(tweet_result.get("rest_id") or "").strip()
                legacy_id = str(tweet_result.get("legacy", {}).get("id_str") or "").strip()
                if (
                    entry.get("entryId") == f"tweet-{normalized_id}"
                    or rest_id == normalized_id
                    or legacy_id == normalized_id
                ):
                    root_result = tweet_result
                    break
            if root_result:
                break

        if not root_result:
            return None
        if root_result.get("__typename") == "TweetWithVisibilityResults":
            root_result = root_result.get("tweet", {})
        if not isinstance(root_result, dict):
            return None

        legacy = root_result.get("legacy", {}) if isinstance(root_result.get("legacy"), dict) else {}
        if not legacy:
            return None

        user_result = root_result.get("core", {}).get("user_results", {}).get("result", {})
        user = user_result.get("legacy", {}) if isinstance(user_result.get("legacy"), dict) else {}
        user_core = user_result.get("core", {}) if isinstance(user_result.get("core"), dict) else {}
        username = str(user.get("screen_name") or user_core.get("screen_name") or "").strip()

        def _as_int(value: Any) -> int:
            try:
                return int(value)
            except Exception:
                return 0

        views_data = root_result.get("views", {}) if isinstance(root_result.get("views"), dict) else {}
        media_urls, _ = self._extract_media_urls_from_tweet(
            tweet_payload=legacy,
            result_payload=root_result,
        )
        text = self._strip_media_url_text(
            text=str(legacy.get("full_text") or legacy.get("text") or ""),
            tweet_payload=legacy,
            result_payload=root_result,
            media_urls=media_urls,
        )
        return {
            "tweet_id": normalized_id,
            "username": username,
            "display_name": str(user.get("name") or user_core.get("name") or "").strip() or None,
            "text": text or None,
            "url": f"https://x.com/{username}/status/{normalized_id}"
            if username
            else f"https://x.com/i/status/{normalized_id}",
            "created_at": str(legacy.get("created_at") or "").strip() or None,
            "user_id": str(user_result.get("rest_id") or user.get("id_str") or user_core.get("id_str") or "").strip()
            or None,
            "user_profile_url": f"https://x.com/{username}" if username else None,
            "user_avatar_url": (
                str(user.get("profile_image_url_https") or user.get("profile_image_url") or "").strip() or None
            ),
            "likes": _as_int(legacy.get("favorite_count")),
            "replies": _as_int(legacy.get("reply_count")),
            "retweets": _as_int(legacy.get("retweet_count")),
            "quotes": _as_int(legacy.get("quote_count")),
            "views": _as_int(views_data.get("count")),
            "bookmarks": _as_int(legacy.get("bookmark_count")),
            "media_urls": media_urls,
        }

    @staticmethod
    def _normalize_optional_tweet_id(value: Any) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    def _resolve_quote_markers(self, *, result: dict[str, Any], tweet: dict[str, Any]) -> tuple[bool, str | None]:
        quoted_result = result.get("quoted_status_result")
        quoted_result_id = self._normalize_optional_tweet_id(
            quoted_result.get("result", {}).get("legacy", {}).get("id_str") if isinstance(quoted_result, dict) else None
        )
        legacy_quoted_id = self._normalize_optional_tweet_id(
            tweet.get("quoted_status_id_str") or tweet.get("quoted_status_id")
        )
        is_quote = bool(quoted_result or tweet.get("is_quote_status") or quoted_result_id or legacy_quoted_id)
        return is_quote, quoted_result_id or legacy_quoted_id

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
        user_id = str(user_result.get("rest_id") or user.get("id_str") or user_core.get("id_str") or "").strip() or None
        user_avatar_url = (
            str(user.get("profile_image_url_https") or user.get("profile_image_url") or "").strip() or None
        )
        user_profile_url = f"https://x.com/{username}" if username else None

        # Parse created_at
        created_at_str = tweet.get("created_at", "")
        try:
            created_at_dt = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
            created_at = int(created_at_dt.timestamp())
            date_time = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            created_at = 0
            date_time = ""

        media_urls, link_preview_media_count = self._extract_media_urls_from_tweet(
            tweet_payload=tweet,
            result_payload=result if isinstance(result, dict) else None,
        )

        # Get engagement metrics
        views_data = result.get("views", {})
        views = int(views_data.get("count", 0)) if views_data.get("count") else 0

        text = self._strip_media_url_text(
            text=str(tweet.get("full_text", "") or tweet.get("text", "")),
            tweet_payload=tweet,
            result_payload=result,
            media_urls=media_urls,
        )
        is_quote, quoted_tweet_id = self._resolve_quote_markers(result=result, tweet=tweet)

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
            is_quote=is_quote,
            reply_to_tweet_id=tweet.get("in_reply_to_status_id_str"),
            quoted_tweet_id=quoted_tweet_id,
            media_urls=media_urls,
            link_preview_media_count=link_preview_media_count,
            user_id=user_id,
            user_profile_url=user_profile_url,
            user_avatar_url=user_avatar_url,
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
        user_id = str(user.get("id_str") or user.get("id") or "").strip() or None
        user_avatar_url = (
            str(user.get("profile_image_url_https") or user.get("profile_image_url") or "").strip() or None
        )
        user_profile_url = f"https://x.com/{username}" if username else None
        media_urls, link_preview_media_count = self._extract_media_urls_from_tweet(
            tweet_payload=tweet_data if isinstance(tweet_data, dict) else {},
            result_payload=tweet_data if isinstance(tweet_data, dict) else None,
        )
        text = self._strip_media_url_text(
            text=str(tweet_data.get("full_text", "") or tweet_data.get("text", "")),
            tweet_payload=tweet_data if isinstance(tweet_data, dict) else {},
            result_payload=tweet_data if isinstance(tweet_data, dict) else None,
            media_urls=media_urls,
        )
        quoted_tweet_id = self._normalize_optional_tweet_id(
            tweet_data.get("quoted_status_id_str") or tweet_data.get("quoted_status_id")
        )
        is_quote = bool(tweet_data.get("quoted_status") or tweet_data.get("is_quote_status") or quoted_tweet_id)

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
            is_quote=is_quote,
            reply_to_tweet_id=tweet_data.get("in_reply_to_status_id_str"),
            quoted_tweet_id=quoted_tweet_id,
            media_urls=media_urls,
            link_preview_media_count=link_preview_media_count,
            user_id=user_id,
            user_profile_url=user_profile_url,
            user_avatar_url=user_avatar_url,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _parse_twikit_tweet(self, raw_tweet: Any, config: TwitterScrapeConfig) -> Tweet | None:
        """Normalize a twikit tweet object to the shared Tweet model."""
        tweet_id = str(getattr(raw_tweet, "id", "") or "").strip()
        if not tweet_id:
            return None

        created_at = 0
        date_time = ""
        created_at_raw = str(getattr(raw_tweet, "created_at", "") or "").strip()
        if created_at_raw:
            try:
                created_at_dt = datetime.strptime(created_at_raw, "%a %b %d %H:%M:%S %z %Y")
                created_at = int(created_at_dt.timestamp())
                date_time = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                pass

        user = getattr(raw_tweet, "user", None)
        username = str(getattr(user, "screen_name", "") or "").strip() if user else ""
        display_name = str(getattr(user, "name", "") or "").strip() if user else ""
        raw_data = getattr(raw_tweet, "_data", {}) if isinstance(getattr(raw_tweet, "_data", {}), dict) else {}
        legacy = getattr(raw_tweet, "_legacy", {}) if isinstance(getattr(raw_tweet, "_legacy", {}), dict) else {}
        text = str(
            getattr(raw_tweet, "full_text", "")
            or getattr(raw_tweet, "text", "")
            or legacy.get("full_text")
            or legacy.get("text")
            or ""
        )
        in_reply_to = (
            str(
                getattr(raw_tweet, "in_reply_to", "")
                or legacy.get("in_reply_to_status_id_str")
                or legacy.get("in_reply_to_status_id")
                or ""
            ).strip()
            or None
        )
        quoted_obj = getattr(raw_tweet, "quoted_tweet", None)
        quoted_tweet_id = (
            str(getattr(quoted_obj, "id", "") or "").strip()
            or str(getattr(raw_tweet, "quoted_tweet_id", "") or "").strip()
            or str(legacy.get("quoted_status_id_str") or legacy.get("quoted_status_id") or "").strip()
            or None
        )
        is_quote = bool(quoted_obj or quoted_tweet_id)
        media_urls, link_preview_media_count = self._extract_media_urls_from_tweet(
            tweet_payload=legacy,
            result_payload=raw_data or None,
        )
        text = self._strip_media_url_text(
            text=text,
            tweet_payload=legacy,
            result_payload=raw_data or None,
            media_urls=media_urls,
        )

        def _as_int(value: Any) -> int:
            try:
                return int(value)
            except Exception:
                return 0

        views_data = raw_data.get("views", {}) if isinstance(raw_data.get("views"), dict) else {}
        is_retweet = bool(
            legacy.get("retweeted_status_result")
            or legacy.get("retweeted_status_id_str")
            or raw_data.get("retweeted_status_result")
            or raw_data.get("retweeted_tweet_result")
            or str(getattr(raw_tweet, "retweeted_tweet_id", "") or "").strip()
        )

        return Tweet(
            tweet_id=tweet_id,
            date_time=date_time,
            created_at=created_at,
            text=text,
            hashtags=self._extract_hashtags(text),
            mentions=self._extract_mentions(text),
            likes=_as_int(getattr(raw_tweet, "favorite_count", 0) or legacy.get("favorite_count")),
            retweets=_as_int(getattr(raw_tweet, "retweet_count", 0) or legacy.get("retweet_count")),
            replies=_as_int(getattr(raw_tweet, "reply_count", 0) or legacy.get("reply_count")),
            quotes=_as_int(getattr(raw_tweet, "quote_count", 0) or legacy.get("quote_count")),
            views=_as_int(getattr(raw_tweet, "view_count", 0) or views_data.get("count")),
            url=f"https://x.com/{username}/status/{tweet_id}" if username else "",
            username=username,
            display_name=display_name,
            user_verified=bool(getattr(user, "is_blue_verified", False) or getattr(user, "verified", False))
            if user
            else False,
            is_reply=bool(in_reply_to),
            is_retweet=is_retweet,
            is_quote=is_quote,
            reply_to_tweet_id=in_reply_to,
            quoted_tweet_id=quoted_tweet_id,
            media_urls=media_urls,
            link_preview_media_count=link_preview_media_count,
            user_id=str(getattr(user, "id", "") or "").strip() or None if user else None,
            user_profile_url=f"https://x.com/{username}" if username else None,
            user_avatar_url=(
                str(getattr(user, "profile_image_url_https", "") or "").strip()
                or str(getattr(user, "profile_image_url", "") or "").strip()
                or None
            )
            if user
            else None,
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _search_tweets_via_twikit(self, *, query: str, max_pages: int, delay: float) -> list[Any]:
        """
        Best-effort twikit search helper used as fallback when GraphQL quote/reply
        endpoints fail in compliant mode.
        """
        if not self._twikit_credentials:
            return []
        self._last_twikit_search_error = None
        try:
            from twikit import Client as TwikitClient  # noqa: F811
        except ImportError:
            logger.warning("twikit not installed; skipping twikit search fallback")
            self._last_twikit_search_error = "twikit_unavailable"
            return []

        import asyncio

        def _error_reason(exc: Exception) -> str:
            lowered = str(exc).lower()
            if "429" in lowered or "rate" in lowered:
                return "rate_limited"
            return "twikit_page_error"

        async def _run_search() -> list[Any]:
            client = TwikitClient("en-US")
            auth_token = str(self._twikit_credentials.get("auth_token", "") or "").strip()
            ct0 = str(self._twikit_credentials.get("ct0", "") or "").strip()
            if auth_token and ct0:
                client.set_cookies({"auth_token": auth_token, "ct0": ct0})
            else:
                username = str(self._twikit_credentials.get("username", "") or "").strip()
                email = str(self._twikit_credentials.get("email", "") or "").strip()
                password = str(self._twikit_credentials.get("password", "") or "").strip()
                if not username or not password:
                    return []
                await client.login(
                    auth_info_1=email or username,
                    auth_info_2=username,
                    password=password,
                )

            collected: list[Any] = []
            cursor = None
            page = 0
            while True:
                page += 1
                if max_pages and page > max_pages:
                    break
                try:
                    if cursor:
                        results = await cursor.next()
                    else:
                        results = await client.search_tweet(query, "Latest", count=20)
                except Exception as exc:
                    reason = _error_reason(exc)
                    if collected:
                        logger.warning(
                            "twikit search halted for query=%s page=%d after partial fetch (%s)",
                            query,
                            page,
                            reason,
                        )
                        self._last_twikit_search_error = reason
                        break
                    raise
                if not results:
                    break
                collected.extend(list(results))
                cursor = results
                if delay > 0:
                    await asyncio.sleep(delay)
            return collected

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import nest_asyncio

                nest_asyncio.apply()
            return loop.run_until_complete(_run_search())
        except RuntimeError:
            return asyncio.run(_run_search())
        except Exception as exc:
            self._last_twikit_search_error = _error_reason(exc)
            logger.exception("twikit fallback search failed for query=%s", query)
            return []

    def _fetch_tweet_replies_via_twikit(self, *, tweet_id: str, max_pages: int = 5, delay: float = 0.5) -> list[Tweet]:
        query = f"conversation_id:{tweet_id}"
        raw_results = self._search_tweets_via_twikit(query=query, max_pages=max_pages, delay=delay)
        if not raw_results:
            if self._last_twikit_search_error:
                self._set_reply_failure_reason(self._last_twikit_search_error)
            return []
        config = TwitterScrapeConfig(query="", date_start=datetime.now(), date_end=datetime.now())
        replies: list[Tweet] = []
        seen_ids: set[str] = set()
        for raw in raw_results:
            tweet = self._parse_twikit_tweet(raw, config)
            if not tweet or tweet.tweet_id == tweet_id:
                continue
            if tweet.is_quote:
                continue
            if not str(tweet.reply_to_tweet_id or "").strip():
                continue
            if tweet.tweet_id in seen_ids:
                continue
            seen_ids.add(tweet.tweet_id)
            replies.append(tweet)
        return replies

    def _fetch_tweet_quotes_via_twikit(self, *, tweet_id: str, max_pages: int = 5, delay: float = 0.5) -> list[Tweet]:
        query = f"quoted_tweet_id:{tweet_id}"
        raw_results = self._search_tweets_via_twikit(query=query, max_pages=max_pages, delay=delay)
        if not raw_results:
            if self._last_twikit_search_error:
                self._set_quote_failure_reason(self._last_twikit_search_error)
            return []
        config = TwitterScrapeConfig(query="", date_start=datetime.now(), date_end=datetime.now())
        quotes: list[Tweet] = []
        seen_ids: set[str] = set()
        for raw in raw_results:
            tweet = self._parse_twikit_tweet(raw, config)
            if not tweet or tweet.tweet_id == tweet_id:
                continue
            if not tweet.is_quote and not tweet.quoted_tweet_id:
                tweet.is_quote = True
            if not tweet.quoted_tweet_id:
                tweet.quoted_tweet_id = tweet_id
            if tweet.quoted_tweet_id and tweet.quoted_tweet_id != tweet_id:
                continue
            tweet.is_quote = True
            if tweet.tweet_id in seen_ids:
                continue
            seen_ids.add(tweet.tweet_id)
            quotes.append(tweet)
        return quotes

    def _scrape_syndication(self, username: str, config: TwitterScrapeConfig) -> list[Tweet]:
        """Scrape tweets via the public syndication API (no auth required)."""
        import json

        logger.info(f"Using syndication API for @{username}")
        self._rate_limit(config.delay_seconds, fast_mode=config.fast_mode)

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
            self._track_response_status(response.status_code)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None:
                self._track_response_status(status)
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

        entries = next_data.get("props", {}).get("pageProps", {}).get("timeline", {}).get("entries", [])

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

    def _fetch_search_via_playwright(
        self,
        *,
        query: str,
        config: TwitterScrapeConfig,
        max_pages: int = 5,
        delay: float = 0.5,
    ) -> list[Tweet]:
        auth_token = str(self.cookies.get("auth_token") or "").strip()
        csrf_token = str(self.cookies.get("ct0") or "").strip()
        self._last_playwright_search_error = None
        if not auth_token or not csrf_token:
            self._last_playwright_search_error = "playwright_missing_auth_cookie"
            return []

        try:
            from playwright.async_api import async_playwright
        except Exception:
            self._last_playwright_search_error = "playwright_unavailable"
            return []

        payloads: list[dict[str, Any]] = []

        async def _capture_payloads() -> list[dict[str, Any]]:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/145.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1280, "height": 2800},
                )
                await context.add_cookies(
                    [
                        {
                            "name": "auth_token",
                            "value": auth_token,
                            "domain": ".x.com",
                            "path": "/",
                            "httpOnly": True,
                            "secure": True,
                            "sameSite": "Lax",
                        },
                        {
                            "name": "ct0",
                            "value": csrf_token,
                            "domain": ".x.com",
                            "path": "/",
                            "httpOnly": False,
                            "secure": True,
                            "sameSite": "Lax",
                        },
                    ]
                )
                page = await context.new_page()

                async def _on_response(response: Any) -> None:
                    response_url = str(response.url or "")
                    if "/SearchTimeline?" not in response_url:
                        return
                    if int(response.status) != 200:
                        return
                    try:
                        response_payload = await response.json()
                    except Exception:
                        return
                    if isinstance(response_payload, dict):
                        payloads.append(response_payload)

                page.on("response", _on_response)
                await page.goto(
                    f"https://x.com/search?q={quote(query, safe='')}&src=typed_query&f=live",
                    wait_until="domcontentloaded",
                    timeout=45000,
                )
                await page.wait_for_timeout(4000)

                max_scrolls = max(8, min(max_pages * 6, 80))
                wait_ms = max(int(delay * 1000), 1200)
                stagnant_cycles = 0
                payload_count = len(payloads)
                for _ in range(max_scrolls):
                    await page.keyboard.press("End")
                    await page.mouse.wheel(0, 12000)
                    await page.wait_for_timeout(wait_ms)
                    if len(payloads) == payload_count:
                        stagnant_cycles += 1
                    else:
                        stagnant_cycles = 0
                        payload_count = len(payloads)
                    if len(payloads) >= max_pages and stagnant_cycles >= 3:
                        break
                    if stagnant_cycles >= 10:
                        break

                await page.close()
                await context.close()
                await browser.close()
            return payloads

        try:
            import asyncio

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import nest_asyncio

                    nest_asyncio.apply()
                captured_payloads = loop.run_until_complete(_capture_payloads())
            except RuntimeError:
                captured_payloads = asyncio.run(_capture_payloads())
        except Exception:
            logger.error("Playwright search fallback failed for query=%s", query, exc_info=True)
            self._last_playwright_search_error = "playwright_error"
            return []

        if not captured_payloads:
            self._last_playwright_search_error = "playwright_no_search_payload"
            return []

        tweets: list[Tweet] = []
        seen_ids: set[str] = set()
        for payload in captured_payloads:
            instructions = (
                payload.get("data", {})
                .get("search_by_raw_query", {})
                .get("search_timeline", {})
                .get("timeline", {})
                .get("instructions", [])
            )
            for instruction in instructions:
                if instruction.get("type") != "TimelineAddEntries":
                    continue
                for entry in instruction.get("entries", []):
                    entry_id = str(entry.get("entryId") or "")
                    if not entry_id.startswith("tweet-"):
                        continue
                    tweet_result = (
                        entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                    )
                    if not tweet_result:
                        continue
                    tweet = self._parse_tweet_result(tweet_result, config)
                    if not tweet or not str(tweet.tweet_id or "").strip() or tweet.tweet_id in seen_ids:
                        continue
                    seen_ids.add(tweet.tweet_id)
                    tweets.append(tweet)

        if not tweets:
            self._last_playwright_search_error = "playwright_no_tweet_entries"
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
                        results = await client.search_tweet(search_query, "Latest", count=20)
                except Exception as exc:
                    # Retry once on first page (transient 404s from Twitter)
                    if page == 1:
                        logger.warning(f"twikit search page 1 failed ({exc}); retrying in 5s...")
                        await asyncio.sleep(5)
                        try:
                            results = await client.search_tweet(search_query, "Latest", count=20)
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
                        created_at_dt = datetime.strptime(t.created_at, "%a %b %d %H:%M:%S %z %Y")
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
                        url=f"https://x.com/{t.user.screen_name}/status/{t.id}" if t.user else "",
                        username=t.user.screen_name if t.user else "",
                        display_name=t.user.name if t.user else "",
                        user_verified=bool(getattr(t.user, "is_blue_verified", False)) if t.user else False,
                        is_reply=bool(t.in_reply_to),
                        is_retweet=bool(getattr(t, "retweeted_tweet", None)),
                        is_quote=bool(getattr(t, "quoted_tweet", None) or getattr(t, "quoted_tweet_id", None)),
                        reply_to_tweet_id=str(t.in_reply_to) if t.in_reply_to else None,
                        quoted_tweet_id=(
                            str(getattr(getattr(t, "quoted_tweet", None), "id", "") or "").strip()
                            or str(getattr(t, "quoted_tweet_id", "") or "").strip()
                            or None
                        ),
                        media_urls=[],
                        user_id=str(getattr(t.user, "id", "") or "").strip() or None if t.user else None,
                        user_profile_url=(
                            f"https://x.com/{t.user.screen_name}"
                            if t.user and getattr(t.user, "screen_name", "")
                            else None
                        ),
                        user_avatar_url=(
                            str(getattr(t.user, "profile_image_url_https", "") or "").strip()
                            or str(getattr(t.user, "profile_image_url", "") or "").strip()
                            or None
                        )
                        if t.user
                        else None,
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

    def _fetch_search(
        self,
        query: str,
        cursor: str | None = None,
        delay: float = 2.0,
        *,
        query_source: str = "typed_query",
        product: str = "Latest",
        variable_overrides: dict[str, Any] | None = None,
        feature_overrides: dict[str, bool] | None = None,
        referer: str | None = None,
        extra_headers: dict[str, str] | None = None,
        fast_mode: bool = False,
    ) -> dict | None:
        """Fetch search results."""
        import json
        import urllib.parse

        self._rate_limit(delay, fast_mode=fast_mode)

        variables = {
            "rawQuery": query,
            "count": 20,
            "querySource": str(query_source or "typed_query"),
            "product": str(product or "Latest"),
        }
        if cursor:
            variables["cursor"] = cursor
        if variable_overrides:
            for key, value in variable_overrides.items():
                if value is None:
                    continue
                variables[str(key)] = value

        features = dict(self.FEATURES)
        if feature_overrides:
            features.update({str(k): bool(v) for k, v in feature_overrides.items()})

        params = {
            "variables": json.dumps(variables),
            "features": json.dumps(features),
        }

        url = f"{self._search_timeline_url}?{urllib.parse.urlencode(params)}"
        headers = self._get_headers()
        if referer:
            headers["referer"] = str(referer)
        if extra_headers:
            for key, value in extra_headers.items():
                key_norm = str(key or "").strip()
                value_norm = str(value or "").strip()
                if not key_norm or not value_norm:
                    continue
                headers[key_norm] = value_norm

        try:
            response = self.session.get(
                url,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            self._track_response_status(response.status_code)
            response.raise_for_status()
            self._last_graphql_status_code = response.status_code
            return response.json()
        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None:
                self._track_response_status(status)
            self._last_graphql_status_code = status
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

    def fetch_tweet_replies(
        self,
        tweet_id: str,
        delay: float = 2.0,
        *,
        search_max_pages: int = 20,
        twikit_max_pages: int = 5,
    ) -> list[Tweet]:
        """Fetch replies to a specific tweet."""
        import json
        import urllib.parse

        self.last_reply_fetch_reason = None
        self.comments_auth_failed = False
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
            self._track_response_status(response.status_code)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code is not None:
                self._track_response_status(status_code)
                self._set_reply_failure_reason(f"http_{status_code}")
            else:
                self._set_reply_failure_reason("request_error")
            logger.error(f"Tweet detail request failed: {e}")
            fallback_replies = self._fetch_tweet_replies_via_search(
                tweet_id=tweet_id,
                delay=delay,
                max_pages=search_max_pages,
            )
            if not fallback_replies and self._twikit_credentials:
                fallback_replies = self._fetch_tweet_replies_via_twikit(
                    tweet_id=tweet_id,
                    max_pages=twikit_max_pages,
                    delay=max(delay, 0.2),
                )
            if fallback_replies:
                self.last_reply_fetch_reason = None
            return fallback_replies
        if not isinstance(data, dict):
            self._set_reply_failure_reason("parse_error")
            fallback_replies = self._fetch_tweet_replies_via_search(
                tweet_id=tweet_id,
                delay=delay,
                max_pages=search_max_pages,
            )
            if not fallback_replies and self._twikit_credentials:
                fallback_replies = self._fetch_tweet_replies_via_twikit(
                    tweet_id=tweet_id,
                    max_pages=twikit_max_pages,
                    delay=max(delay, 0.2),
                )
            if fallback_replies:
                self.last_reply_fetch_reason = None
            return fallback_replies
        if data.get("errors"):
            self._set_reply_failure_reason("api_errors")

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

        # Always supplement with SearchTimeline results for better coverage
        seen_ids = {r.tweet_id for r in replies}
        try:
            search_replies = self._fetch_tweet_replies_via_search(
                tweet_id=tweet_id,
                delay=delay,
                max_pages=search_max_pages,
            )
            for sr in search_replies:
                if sr.tweet_id not in seen_ids:
                    seen_ids.add(sr.tweet_id)
                    replies.append(sr)
        except Exception:
            pass  # SearchTimeline supplement is best-effort

        # Twikit as final supplement if still no results
        if not replies and self._twikit_credentials:
            try:
                twikit_replies = self._fetch_tweet_replies_via_twikit(
                    tweet_id=tweet_id,
                    max_pages=twikit_max_pages,
                    delay=max(delay, 0.2),
                )
                for tr in twikit_replies:
                    if tr.tweet_id not in seen_ids:
                        seen_ids.add(tr.tweet_id)
                        replies.append(tr)
            except Exception:
                pass

        return replies

    def _fetch_tweet_replies_via_search(self, *, tweet_id: str, delay: float, max_pages: int = 20) -> list[Tweet]:
        """Fallback reply fetch using SearchTimeline conversation query."""
        self._ensure_auth()
        query = f"conversation_id:{tweet_id}"
        cursor: str | None = None
        page_num = 0
        replies: list[Tweet] = []
        seen_ids: set[str] = set()

        while True:
            page_num += 1
            if max_pages and page_num > max_pages:
                break

            data = self._fetch_search(query, cursor, delay)
            if not data:
                if self._last_graphql_status_code == 404 and page_num <= 1:
                    self._search_hash = None
                    self._detail_hash = None
                    self._discover_graphql_hashes()
                    data = self._fetch_search(query, cursor, delay)
                    if not data:
                        break
                else:
                    break

            timeline = (
                data.get("data", {}).get("search_by_raw_query", {}).get("search_timeline", {}).get("timeline", {})
            )
            instructions = timeline.get("instructions", [])
            page_count = 0
            next_cursor: str | None = None

            for instruction in instructions:
                if instruction.get("type") != "TimelineAddEntries":
                    continue
                for entry in instruction.get("entries", []):
                    entry_id = str(entry.get("entryId") or "")
                    if entry_id.startswith("cursor-bottom-"):
                        next_cursor = entry.get("content", {}).get("value")
                        continue
                    if entry_id.startswith("cursor-top-") or not entry_id.startswith("tweet-"):
                        continue

                    tweet_result = (
                        entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                    )
                    if not tweet_result:
                        continue
                    config = TwitterScrapeConfig(query="", date_start=datetime.now(), date_end=datetime.now())
                    tweet = self._parse_tweet_result(tweet_result, config)
                    if not tweet or tweet.tweet_id == tweet_id:
                        continue
                    if tweet.is_quote:
                        continue
                    reply_to_id = str(tweet.reply_to_tweet_id or "").strip()
                    if not reply_to_id:
                        continue
                    if tweet.tweet_id in seen_ids:
                        continue
                    seen_ids.add(tweet.tweet_id)
                    replies.append(tweet)
                    page_count += 1

            if not next_cursor:
                break
            if page_count == 0:
                break
            cursor = next_cursor

        return replies

    def _fetch_quotes_via_tweet_detail(self, tweet_id: str, delay: float) -> list[Tweet]:
        """Fetch quote tweets using TweetDetail timeline entries."""
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
                self._detail_hash = None
                self._discover_graphql_hashes()
                response = _request(features)
            if response.status_code == 400:
                missing_flags = self._extract_required_feature_flags(response)
                if missing_flags:
                    logger.info("TweetDetail requires %d additional feature flags; retrying", len(missing_flags))
                    for flag in missing_flags:
                        features.setdefault(flag, False)
                    response = _request(features)
            self._track_response_status(response.status_code)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code is not None:
                self._track_response_status(status_code)
                self._set_quote_failure_reason(f"http_{status_code}")
            else:
                self._set_quote_failure_reason("request_error")
            logger.error("Tweet detail quote request failed: %s", exc)
            return []

        if not isinstance(data, dict):
            self._set_quote_failure_reason("parse_error")
            return []
        if data.get("errors"):
            self._set_quote_failure_reason("api_errors")

        quotes: list[Tweet] = []
        seen_ids: set[str] = set()
        instructions = data.get("data", {}).get("threaded_conversation_with_injections_v2", {}).get("instructions", [])
        for instruction in instructions:
            if instruction.get("type") != "TimelineAddEntries":
                continue
            for entry in instruction.get("entries", []):
                entry_id = str(entry.get("entryId") or "")
                if not entry_id.startswith("tweet-"):
                    continue
                tweet_result = (
                    entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                )
                if not tweet_result:
                    continue
                config = TwitterScrapeConfig(query="", date_start=datetime.now(), date_end=datetime.now())
                tweet = self._parse_tweet_result(tweet_result, config)
                if not tweet or tweet.tweet_id == tweet_id:
                    continue
                if not tweet.is_quote or not tweet.quoted_tweet_id:
                    legacy = tweet_result.get("legacy", {}) if isinstance(tweet_result, dict) else {}
                    legacy_is_quote, legacy_quote_id = self._resolve_quote_markers(
                        result=tweet_result if isinstance(tweet_result, dict) else {},
                        tweet=legacy if isinstance(legacy, dict) else {},
                    )
                    if not tweet.is_quote and legacy_is_quote:
                        tweet.is_quote = True
                    if not tweet.quoted_tweet_id and legacy_quote_id:
                        tweet.quoted_tweet_id = legacy_quote_id
                if tweet.quoted_tweet_id and tweet.quoted_tweet_id != tweet_id:
                    continue
                if not tweet.is_quote and not tweet.quoted_tweet_id:
                    continue
                if not tweet.quoted_tweet_id:
                    tweet.quoted_tweet_id = tweet_id
                tweet.is_quote = True
                if tweet.tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet.tweet_id)
                quotes.append(tweet)
        if not quotes and not self.last_quote_fetch_reason:
            self._set_quote_failure_reason("tweet_detail_no_quote_entries")
        return quotes

    def _extract_quote_tweets_from_search_payload(
        self,
        *,
        payload: dict[str, Any],
        tweet_id: str,
        seen_ids: set[str],
    ) -> tuple[list[Tweet], str | None]:
        timeline = payload.get("data", {}).get("search_by_raw_query", {}).get("search_timeline", {}).get("timeline", {})
        instructions = timeline.get("instructions", [])
        next_cursor: str | None = None
        quotes: list[Tweet] = []
        for instruction in instructions:
            if instruction.get("type") != "TimelineAddEntries":
                continue
            for entry in instruction.get("entries", []):
                entry_id = str(entry.get("entryId") or "")
                if entry_id.startswith("cursor-bottom-"):
                    next_cursor = entry.get("content", {}).get("value")
                    continue
                if entry_id.startswith("cursor-top-") or not entry_id.startswith("tweet-"):
                    continue

                tweet_result = (
                    entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                )
                if not tweet_result:
                    continue
                config = TwitterScrapeConfig(query="", date_start=datetime.now(), date_end=datetime.now())
                tweet = self._parse_tweet_result(tweet_result, config)
                if not tweet or tweet.tweet_id == tweet_id:
                    continue
                if not tweet.is_quote or not tweet.quoted_tweet_id:
                    legacy = tweet_result.get("legacy", {}) if isinstance(tweet_result, dict) else {}
                    legacy_is_quote, legacy_quote_id = self._resolve_quote_markers(
                        result=tweet_result if isinstance(tweet_result, dict) else {},
                        tweet=legacy if isinstance(legacy, dict) else {},
                    )
                    if not tweet.is_quote and legacy_is_quote:
                        tweet.is_quote = True
                    if not tweet.quoted_tweet_id and legacy_quote_id:
                        tweet.quoted_tweet_id = legacy_quote_id
                if tweet.quoted_tweet_id and tweet.quoted_tweet_id != tweet_id:
                    continue
                if not tweet.is_quote and not tweet.quoted_tweet_id:
                    tweet.is_quote = True
                if not tweet.quoted_tweet_id:
                    tweet.quoted_tweet_id = tweet_id
                tweet.is_quote = True
                if tweet.tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet.tweet_id)
                quotes.append(tweet)
        return quotes, next_cursor

    def _fetch_tweet_quotes_via_playwright(
        self, *, tweet_id: str, max_pages: int = 5, delay: float = 0.5
    ) -> list[Tweet]:
        auth_token = str(self.cookies.get("auth_token") or "").strip()
        csrf_token = str(self.cookies.get("ct0") or "").strip()
        if not auth_token or not csrf_token:
            self._set_quote_failure_reason("playwright_missing_auth_cookie")
            return []

        try:
            from playwright.async_api import async_playwright
        except Exception:
            self._set_quote_failure_reason("playwright_unavailable")
            return []

        encoded_marker = f"quoted_tweet_id%3A{tweet_id}"
        raw_marker = f"quoted_tweet_id:{tweet_id}"
        payloads: list[dict[str, Any]] = []

        async def _capture_payloads() -> list[dict[str, Any]]:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/145.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1280, "height": 2800},
                )
                await context.add_cookies(
                    [
                        {
                            "name": "auth_token",
                            "value": auth_token,
                            "domain": ".x.com",
                            "path": "/",
                            "httpOnly": True,
                            "secure": True,
                            "sameSite": "Lax",
                        },
                        {
                            "name": "ct0",
                            "value": csrf_token,
                            "domain": ".x.com",
                            "path": "/",
                            "httpOnly": False,
                            "secure": True,
                            "sameSite": "Lax",
                        },
                    ]
                )
                page = await context.new_page()

                async def _on_response(response: Any) -> None:
                    response_url = str(response.url or "")
                    if "/SearchTimeline?" not in response_url:
                        return
                    if encoded_marker not in response_url and raw_marker not in response_url:
                        return
                    if int(response.status) != 200:
                        return
                    try:
                        response_payload = await response.json()
                    except Exception:
                        return
                    if isinstance(response_payload, dict):
                        payloads.append(response_payload)

                page.on("response", _on_response)
                await page.goto(
                    f"https://x.com/i/status/{tweet_id}/retweets/with_comments",
                    wait_until="domcontentloaded",
                    timeout=45000,
                )
                await page.wait_for_timeout(4000)

                max_scrolls = max(8, min(max_pages * 6, 80))
                wait_ms = max(int(delay * 1000), 1200)
                stagnant_cycles = 0
                payload_count = len(payloads)
                for _ in range(max_scrolls):
                    await page.keyboard.press("End")
                    await page.mouse.wheel(0, 12000)
                    await page.wait_for_timeout(wait_ms)
                    if len(payloads) == payload_count:
                        stagnant_cycles += 1
                    else:
                        stagnant_cycles = 0
                        payload_count = len(payloads)
                    if len(payloads) >= max_pages and stagnant_cycles >= 3:
                        break
                    if stagnant_cycles >= 10:
                        break

                await page.close()
                await context.close()
                await browser.close()
            return payloads

        try:
            import asyncio

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import nest_asyncio

                    nest_asyncio.apply()
                captured_payloads = loop.run_until_complete(_capture_payloads())
            except RuntimeError:
                captured_payloads = asyncio.run(_capture_payloads())
        except Exception:
            logger.error("Playwright quote fallback failed for tweet_id=%s", tweet_id, exc_info=True)
            self._set_quote_failure_reason("playwright_error")
            return []

        if not captured_payloads:
            self._set_quote_failure_reason("playwright_no_search_payload")
            return []

        quotes: list[Tweet] = []
        seen_ids: set[str] = set()
        for payload in captured_payloads:
            parsed_quotes, _ = self._extract_quote_tweets_from_search_payload(
                payload=payload,
                tweet_id=tweet_id,
                seen_ids=seen_ids,
            )
            quotes.extend(parsed_quotes)

        if not quotes and not self.last_quote_fetch_reason:
            self._set_quote_failure_reason("playwright_no_quote_entries")
        return quotes

    def _fetch_tweet_quotes_via_search(self, *, tweet_id: str, delay: float, max_pages: int = 5) -> list[Tweet]:
        """Fetch quote tweets using SearchTimeline with quoted_tweet_id operator."""
        if self._quote_search_timeline_unavailable:
            self._set_quote_failure_reason("http_404")
            return []
        self._ensure_auth()
        query = f"quoted_tweet_id:{tweet_id}"
        cursor: str | None = None
        page_num = 0
        quotes: list[Tweet] = []
        seen_ids: set[str] = set()

        while True:
            page_num += 1
            if max_pages and page_num > max_pages:
                break

            extra_headers: dict[str, str] = {}
            if self.cookies.get("auth_token"):
                extra_headers["x-twitter-auth-type"] = "OAuth2Session"
            transaction_id = str(os.getenv("TWITTER_X_CLIENT_TRANSACTION_ID") or "").strip()
            if transaction_id:
                extra_headers["x-client-transaction-id"] = transaction_id

            data = self._fetch_search(
                query,
                cursor,
                delay,
                query_source="tdqt",
                product="Latest",
                variable_overrides={"withGrokTranslatedBio": False},
                feature_overrides=self.QUOTE_SEARCH_FEATURE_OVERRIDES,
                referer=f"https://x.com/i/status/{tweet_id}/retweets/with_comments",
                extra_headers=(extra_headers or None),
            )
            if not data:
                if self._last_graphql_status_code == 404 and page_num <= 1:
                    self._search_hash = None
                    self._detail_hash = None
                    self._discover_graphql_hashes()
                    data = self._fetch_search(
                        query,
                        cursor,
                        delay,
                        query_source="tdqt",
                        product="Latest",
                        variable_overrides={"withGrokTranslatedBio": False},
                        feature_overrides=self.QUOTE_SEARCH_FEATURE_OVERRIDES,
                        referer=f"https://x.com/i/status/{tweet_id}/retweets/with_comments",
                        extra_headers=(extra_headers or None),
                    )
                    if not data:
                        if self._last_graphql_status_code == 404:
                            self._quote_search_timeline_unavailable = True
                        self._set_quote_failure_reason(
                            f"http_{int(self._last_graphql_status_code)}"
                            if self._last_graphql_status_code
                            else "search_error"
                        )
                        break
                else:
                    if self._last_graphql_status_code == 404:
                        self._quote_search_timeline_unavailable = True
                    self._set_quote_failure_reason(
                        f"http_{int(self._last_graphql_status_code)}"
                        if self._last_graphql_status_code
                        else "search_error"
                    )
                    break
            if not isinstance(data, dict):
                self._set_quote_failure_reason("parse_error")
                break
            if data.get("errors"):
                self._set_quote_failure_reason("api_errors")
            page_quotes, next_cursor = self._extract_quote_tweets_from_search_payload(
                payload=data,
                tweet_id=tweet_id,
                seen_ids=seen_ids,
            )
            quotes.extend(page_quotes)
            page_count = len(page_quotes)

            if not next_cursor:
                break
            if page_count == 0:
                break
            cursor = next_cursor
        return quotes

    def fetch_tweet_quotes(self, tweet_id: str, delay: float = 2.0, max_pages: int = 5) -> list[Tweet]:
        """Fetch quote tweets for a specific root tweet."""
        self.last_quote_fetch_reason = None
        attempts: list[dict[str, Any]] = []
        self.last_quote_fetch_meta = {
            "tweet_id": str(tweet_id or "").strip(),
            "attempts": attempts,
            "source_used": None,
            "failure_reason": None,
        }
        self._ensure_auth()

        first_failure_reason: str | None = None

        def _record_attempt(source: str, *, count: int, failure_reason: str | None, skipped: bool = False) -> None:
            attempts.append(
                {
                    "source": source,
                    "count": int(count),
                    "failure_reason": failure_reason,
                    "skipped": bool(skipped),
                }
            )

        # Primary: SearchTimeline with quoted_tweet_id (has pagination, finds all quote types).
        self.last_quote_fetch_reason = None
        search_quotes = self._fetch_tweet_quotes_via_search(tweet_id=tweet_id, delay=delay, max_pages=max_pages)
        search_reason = None if search_quotes else self.last_quote_fetch_reason
        _record_attempt("search_timeline", count=len(search_quotes), failure_reason=search_reason)
        if search_quotes:
            self.last_quote_fetch_reason = None
            self.last_quote_fetch_meta["source_used"] = "search_timeline"
            self.last_quote_fetch_meta["failure_reason"] = None
            return search_quotes
        if search_reason:
            first_failure_reason = search_reason

        # Fallback: TweetDetail conversation thread (no pagination, reply-quotes only).
        detail_quotes = self._fetch_quotes_via_tweet_detail(tweet_id=tweet_id, delay=delay)
        detail_reason = None if detail_quotes else self.last_quote_fetch_reason
        _record_attempt("tweet_detail", count=len(detail_quotes), failure_reason=detail_reason)
        if detail_quotes:
            self.last_quote_fetch_reason = None
            self.last_quote_fetch_meta["source_used"] = "tweet_detail"
            self.last_quote_fetch_meta["failure_reason"] = None
            return detail_quotes
        if detail_reason and not first_failure_reason:
            first_failure_reason = detail_reason

        # Tertiary: twikit authenticated search.
        if self._twikit_credentials:
            self.last_quote_fetch_reason = None
            twikit_quotes = self._fetch_tweet_quotes_via_twikit(
                tweet_id=tweet_id,
                max_pages=max_pages,
                delay=max(delay, 0.2),
            )
            twikit_reason = None if twikit_quotes else self.last_quote_fetch_reason
            _record_attempt("twikit", count=len(twikit_quotes), failure_reason=twikit_reason)
            if twikit_quotes:
                self.last_quote_fetch_reason = None
                self.last_quote_fetch_meta["source_used"] = "twikit"
                self.last_quote_fetch_meta["failure_reason"] = None
                return twikit_quotes
            if twikit_reason and not first_failure_reason:
                first_failure_reason = twikit_reason
        else:
            _record_attempt("twikit", count=0, failure_reason="no_twikit_credentials", skipped=True)

        # Last resort: browser-based fallback that captures SearchTimeline payloads from /retweets/with_comments.
        self.last_quote_fetch_reason = None
        playwright_quotes = self._fetch_tweet_quotes_via_playwright(
            tweet_id=tweet_id,
            max_pages=max_pages,
            delay=max(delay, 0.2),
        )
        playwright_reason = None if playwright_quotes else self.last_quote_fetch_reason
        _record_attempt("playwright", count=len(playwright_quotes), failure_reason=playwright_reason)
        if playwright_quotes:
            self.last_quote_fetch_reason = None
            self.last_quote_fetch_meta["source_used"] = "playwright"
            self.last_quote_fetch_meta["failure_reason"] = None
            return playwright_quotes
        if playwright_reason and not first_failure_reason:
            first_failure_reason = playwright_reason

        self.last_quote_fetch_reason = first_failure_reason
        self.last_quote_fetch_meta["failure_reason"] = self.last_quote_fetch_reason
        return []

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
            logger.debug("Twitter scrape progress callback raised", exc_info=True)

    @staticmethod
    def _window_bound_timestamp(value: datetime) -> int:
        if value.tzinfo is None:
            return int(value.replace(tzinfo=UTC).timestamp())
        return int(value.timestamp())

    def _tweet_within_window(self, *, tweet: Tweet, start_ts: int, end_ts: int) -> bool:
        created_at = int(getattr(tweet, "created_at", 0) or 0)
        if created_at <= 0:
            return False
        return start_ts <= created_at <= end_ts

    def _clamp_tweets_to_window(self, *, tweets: list[Tweet], start_ts: int, end_ts: int) -> list[Tweet]:
        return [tweet for tweet in tweets if self._tweet_within_window(tweet=tweet, start_ts=start_ts, end_ts=end_ts)]

    def scrape(
        self,
        config: TwitterScrapeConfig,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[Tweet]:
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
        window_start_ts = self._window_bound_timestamp(config.date_start)
        window_end_ts = self._window_bound_timestamp(config.date_end)

        tweets: list[Tweet] = []
        graphql_404_count = 0
        fallback_triggered = False
        retrieval_mode = "graphql"
        fallback_attempts: list[str] = []
        cursor = None
        page_num = 0
        graphql_failed = False
        posts_checked_total = 0
        filtered_out_of_window = 0
        older_only_page_streak = 0
        older_only_page_limit = 2
        stop_reason = "complete"

        from_match = re.search(r"(?:^|\s)from:(\w+)", config.query)

        while True:
            page_num += 1
            if config.max_pages and page_num > config.max_pages:
                logger.info(f"Reached max pages limit ({config.max_pages})")
                stop_reason = "max_pages_reached"
                break

            logger.info(f"Fetching page {page_num}...")
            data = self._fetch_search(search_query, cursor, config.delay_seconds, fast_mode=config.fast_mode)
            if not data:
                if self._last_graphql_status_code == 404 and graphql_404_count < 1:
                    graphql_404_count += 1
                    # Hashes likely rotated; force rediscovery once.
                    self._search_hash = None
                    self._detail_hash = None
                    logger.warning("GraphQL returned 404; retrying after hash rediscovery")
                    continue
                graphql_failed = True
                stop_reason = "graphql_fetch_failed"
                break

            # Parse search results
            search_data = data.get("data", {}).get("search_by_raw_query", {})
            timeline = search_data.get("search_timeline", {}).get("timeline", {})
            instructions = timeline.get("instructions", [])

            tweets_on_page = 0
            checked_on_page = 0
            older_than_window_on_page = 0
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
                                    checked_on_page += 1
                                    posts_checked_total += 1
                                    if not self._tweet_within_window(
                                        tweet=tweet,
                                        start_ts=window_start_ts,
                                        end_ts=window_end_ts,
                                    ):
                                        filtered_out_of_window += 1
                                        if int(getattr(tweet, "created_at", 0) or 0) < window_start_ts:
                                            older_than_window_on_page += 1
                                        continue
                                    tweets_on_page += 1
                                    tweets.append(tweet)
                                    logger.info(
                                        f"Found #{len(tweets)}: @{tweet.username} ({tweet.date_time}) "
                                        f"- {tweet.likes:,} likes, {tweet.retweets:,} RTs"
                                    )

            if checked_on_page == 0:
                logger.info("No tweet entries found on page %d; stopping pagination", page_num)
                stop_reason = "no_tweet_entries"
                break

            logger.info(f"Page {page_num}: found {tweets_on_page} tweets, {len(tweets)} total")
            self._emit_progress(
                progress_cb,
                phase="scrape_graphql_page",
                pages_scanned=page_num,
                posts_checked=posts_checked_total,
                matched_posts=len(tweets),
            )

            if tweets_on_page == 0 and older_than_window_on_page == checked_on_page:
                older_only_page_streak += 1
                if older_only_page_streak >= older_only_page_limit:
                    logger.info(
                        "Stopping pagination after %d page(s) fully older than window start",
                        older_only_page_streak,
                    )
                    stop_reason = "older_than_window_repeated"
                    break
            else:
                older_only_page_streak = 0

            # Get next page
            cursor = next_cursor
            if not cursor:
                logger.info("No more pages available")
                stop_reason = "no_cursor"
                break

        # Fallback chain:
        # 1) GraphQL SearchTimeline always first
        # 2) twikit next (if configured)
        # 3) syndication last resort (from: queries only)
        if not tweets and from_match:
            fallback_triggered = True
            if self._twikit_credentials:
                fallback_attempts.append("twikit")
                import time

                time.sleep(3)
                logger.info("GraphQL yielded no in-window results; trying twikit search fallback...")
                twikit_tweets = self._scrape_via_twikit(config)
                tweets = self._clamp_tweets_to_window(
                    tweets=twikit_tweets,
                    start_ts=window_start_ts,
                    end_ts=window_end_ts,
                )
                if tweets:
                    retrieval_mode = "twikit"
            if not tweets:
                username = from_match.group(1)
                fallback_attempts.append("syndication")
                logger.info(f"Falling back to syndication API for @{username}")
                syndication_tweets = self._scrape_syndication(username, config)
                tweets = self._clamp_tweets_to_window(
                    tweets=syndication_tweets,
                    start_ts=window_start_ts,
                    end_ts=window_end_ts,
                )
                if tweets:
                    retrieval_mode = "syndication"
            if not tweets:
                fallback_attempts.append("playwright")
                logger.info("Primary Twitter search paths empty; trying Playwright search fallback...")
                playwright_tweets = self._fetch_search_via_playwright(
                    query=search_query,
                    config=config,
                    max_pages=config.max_pages or 5,
                    delay=max(config.delay_seconds, 0.2),
                )
                tweets = self._clamp_tweets_to_window(
                    tweets=playwright_tweets,
                    start_ts=window_start_ts,
                    end_ts=window_end_ts,
                )
                if tweets:
                    retrieval_mode = "playwright"
        elif graphql_failed and not tweets:
            fallback_triggered = True
            if self._twikit_credentials:
                fallback_attempts.append("twikit")
                import time

                time.sleep(3)
                logger.info("GraphQL failed; trying twikit search fallback...")
                twikit_tweets = self._scrape_via_twikit(config)
                tweets = self._clamp_tweets_to_window(
                    tweets=twikit_tweets,
                    start_ts=window_start_ts,
                    end_ts=window_end_ts,
                )
                if tweets:
                    retrieval_mode = "twikit"
            if not tweets:
                fallback_attempts.append("playwright")
                logger.info("GraphQL failed; trying Playwright search fallback...")
                playwright_tweets = self._fetch_search_via_playwright(
                    query=search_query,
                    config=config,
                    max_pages=config.max_pages or 5,
                    delay=max(config.delay_seconds, 0.2),
                )
                tweets = self._clamp_tweets_to_window(
                    tweets=playwright_tweets,
                    start_ts=window_start_ts,
                    end_ts=window_end_ts,
                )
                if tweets:
                    retrieval_mode = "playwright"
            if not tweets and not self._twikit_credentials:
                logger.warning(
                    "Twitter requires authentication for search. "
                    "Set SOCIAL_TWITTER_COOKIES_JSON, TWITTER_COOKIES_FILE, "
                    "or TWIKIT_USERNAME + TWIKIT_PASSWORD env vars."
                )

        logger.info("Search complete: found %d tweets (%d checked)", len(tweets), posts_checked_total)
        self._emit_progress(
            progress_cb,
            phase="scrape_complete",
            pages_scanned=page_num,
            posts_checked=posts_checked_total,
            matched_posts=len(tweets),
        )
        self.last_retrieval_meta = {
            "retrieval_mode": retrieval_mode,
            "search_query": search_query,
            "window_start": config.date_start.isoformat(),
            "window_end": config.date_end.isoformat(),
            "from_query": bool(from_match),
            "fast_mode": config.fast_mode,
            "graphql_404_count": graphql_404_count,
            "graphql_failed": graphql_failed,
            "fallback_triggered": fallback_triggered,
            "fallback_attempts": fallback_attempts,
            "playwright_failure_reason": self._last_playwright_search_error,
            "pages_scanned": page_num,
            "posts_checked": posts_checked_total,
            "filtered_out_of_window": filtered_out_of_window,
            "stop_reason": stop_reason,
            "tweet_count": len(tweets),
        }
        return tweets
