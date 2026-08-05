# ruff: noqa: F401, F403, F405, UP037
"""Direct TikTok, Twitter, YouTube, Facebook, and Threads scrape routes."""

from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from .social_landing import *

router = APIRouter()


class TikTokScrapeRequest(BaseModel):
    """Request to scrape TikTok posts."""

    username: str = Field(..., description="TikTok username to scrape (without @)")
    hashtags: list[str] = Field(..., description="Hashtags to filter by (without #)")
    date_start: datetime = Field(..., description="Start date for filtering")
    date_end: datetime = Field(..., description="End date for filtering")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=None, ge=1, le=500, description="Maximum pages to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")


class TikTokPostResponse(BaseModel):
    """Single TikTok post in response."""

    video_id: str
    date_time: str
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
    duration: int
    music_title: str
    music_author: str


class TikTokScrapeResponse(BaseModel):
    """Response from TikTok scrape operation."""

    success: bool
    username: str
    posts_found: int
    posts: list[TikTokPostResponse]
    filters_applied: dict
    diagnostics: dict[str, Any] | None = None
    error: str | None = None


def _build_tiktok_scrape_diagnostics(retrieval_meta: dict[str, Any]) -> dict[str, Any] | None:
    allowed_keys = (
        "retrieval_mode",
        "http_client",
        "fallback_chain",
        "stop_reason",
        "error_code",
        "risk_state",
        "operator_summary",
        "operator_action",
        "triage_bucket",
        "profile_enrichment_status",
    )
    diagnostics = {key: retrieval_meta[key] for key in allowed_keys if key in retrieval_meta}
    return diagnostics or None


class TwitterSearchRequest(BaseModel):
    """Request to search Twitter/X for tweets."""

    query: str = Field(..., description="Search query (hashtag or phrase, e.g., RHOSLC or #RHOSLC)")
    date_start: datetime = Field(..., description="Start date for search")
    date_end: datetime = Field(..., description="End date for search")
    include_replies: bool = Field(default=False, description="Include reply tweets in results")
    include_links: bool = Field(default=True, description="Include tweets with links")
    mirror_to_s3: bool = Field(default=False, description="Mirror discovered media URLs to S3")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=None, ge=1, le=100, description="Maximum pages to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")

    # Persistence options
    persist: bool = Field(default=False, description="Upsert results to social.twitter_tweets")
    scrape_query: str | None = Field(
        default=None,
        description="Label stored on persisted rows; defaults to query value when omitted",
    )


class TweetResponse(BaseModel):
    """Single tweet in response."""

    tweet_id: str
    date_time: str
    text: str
    hashtags: list[str]
    mentions: list[str]
    likes: int
    retweets: int
    replies: int
    quotes: int
    views: int
    bookmarks: int = 0
    shares: int = 0
    url: str
    username: str
    display_name: str
    user_verified: bool
    is_reply: bool
    is_retweet: bool
    is_quote: bool
    thread_root_tweet_id: str | None = None
    thread_position: int | None = None
    is_thread_part: bool = False
    twitter_context_role: str | None = None
    media_urls: list[str]
    hosted_media_urls: list[str] = Field(default_factory=list)


class TwitterSearchResponse(BaseModel):
    """Response from Twitter search operation."""

    success: bool
    query: str
    tweets_found: int
    tweets: list[TweetResponse]
    search_query_used: str
    filters_applied: dict
    retrieval_meta: dict | None = None
    complete: bool = False
    persist_summary: dict | None = None
    scrape_run_id: str | None = None
    error: str | None = None


class TweetRepliesRequest(BaseModel):
    """Request to fetch replies for a tweet."""

    tweet_id: str = Field(..., description="Tweet ID to fetch replies for")
    mirror_to_s3: bool = Field(default=False, description="Mirror discovered media URLs to S3")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    search_max_pages: int | None = Field(default=None, ge=1, le=100, description="Maximum search fallback pages")
    twikit_max_pages: int | None = Field(default=None, ge=1, le=100, description="Maximum twikit fallback pages")


class TweetRepliesResponse(BaseModel):
    """Response from tweet replies operation."""

    success: bool
    tweet_id: str
    replies_found: int
    replies: list[TweetResponse]
    error: str | None = None


class TweetQuotesRequest(BaseModel):
    """Request to fetch quote tweets for a tweet."""

    tweet_id: str = Field(..., description="Tweet ID to fetch quote tweets for")
    mirror_to_s3: bool = Field(default=False, description="Mirror discovered media URLs to S3")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int = Field(default=60, ge=1, le=100, description="Maximum search pages for quote fallbacks")


class TweetQuotesResponse(BaseModel):
    """Response from tweet quotes operation."""

    success: bool
    tweet_id: str
    quotes_found: int
    quotes: list[TweetResponse]
    source_used: str | None = None
    failure_reason: str | None = None
    error: str | None = None


def _tweet_to_response(tweet: Any) -> TweetResponse:
    return TweetResponse(
        tweet_id=tweet.tweet_id,
        date_time=tweet.date_time,
        text=tweet.text,
        hashtags=tweet.hashtags,
        mentions=tweet.mentions,
        likes=tweet.likes,
        retweets=tweet.retweets,
        replies=tweet.replies,
        quotes=tweet.quotes,
        views=tweet.views,
        bookmarks=getattr(tweet, "bookmarks", 0) or 0,
        shares=getattr(tweet, "shares", 0) or getattr(tweet, "retweets", 0) or 0,
        url=tweet.url,
        username=tweet.username,
        display_name=tweet.display_name,
        user_verified=tweet.user_verified,
        is_reply=tweet.is_reply,
        is_retweet=tweet.is_retweet,
        is_quote=tweet.is_quote,
        thread_root_tweet_id=getattr(tweet, "thread_root_tweet_id", None),
        thread_position=getattr(tweet, "thread_position", None),
        is_thread_part=bool(getattr(tweet, "is_thread_part", False)),
        twitter_context_role=getattr(tweet, "twitter_context_role", None),
        media_urls=tweet.media_urls,
        hosted_media_urls=getattr(tweet, "hosted_media_urls", []) or [],
    )


class YouTubeScrapeRequest(BaseModel):
    """Request to scrape YouTube channel videos."""

    channel_handle: str = Field(default="", description="YouTube channel handle (without @)")
    source_type: Literal["account", "playlist"] = Field(default="account", description="YouTube source mode")
    playlist_id: str | None = Field(default=None, description="YouTube playlist ID when source_type is playlist")
    playlist_url: str | None = Field(default=None, description="YouTube playlist URL when source_type is playlist")
    keywords: list[str] = Field(..., description="Keywords to filter by (e.g., RHOSLC, 'Salt Lake City')")
    date_start: datetime = Field(..., description="Start date for filtering")
    date_end: datetime = Field(..., description="End date for filtering")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_results: int | None = Field(default=None, ge=1, le=500, description="Maximum videos to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")

    @model_validator(mode="after")
    def validate_source(self) -> YouTubeScrapeRequest:
        if self.source_type == "playlist":
            if not (str(self.playlist_id or "").strip() or str(self.playlist_url or "").strip()):
                raise ValueError("playlist_id or playlist_url is required when source_type is playlist")
            return self
        if not str(self.channel_handle or "").strip():
            raise ValueError("channel_handle is required when source_type is account")
        return self


class YouTubeVideoResponse(BaseModel):
    """Single YouTube video in response."""

    video_id: str
    title: str
    description: str
    date_time: str
    channel_title: str
    duration: str
    duration_seconds: int
    views: int
    likes: int
    comments: int
    url: str
    thumbnail_url: str
    keywords_matched: list[str]


class YouTubeScrapeResponse(BaseModel):
    """Response from YouTube scrape operation."""

    success: bool
    channel_handle: str
    videos_found: int
    videos: list[YouTubeVideoResponse]
    filters_applied: dict
    error: str | None = None


class FacebookScrapeRequest(BaseModel):
    page_handle: str = Field(..., description="Facebook page handle (without leading /)")
    hashtags: list[str] = Field(default_factory=list, description="Optional hashtag filter (without #)")
    keywords: list[str] = Field(default_factory=list, description="Optional keyword filter")
    date_start: datetime | None = Field(default=None, description="Optional start date for filtering")
    date_end: datetime | None = Field(default=None, description="Optional end date for filtering")
    delay_seconds: float = Field(default=1.25, ge=0.25, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=1, ge=1, le=100, description="Maximum discovery pages")


class FacebookMediaProvenanceResponse(BaseModel):
    platform: str
    matched_by: str
    fallback_used: bool


class FacebookShareResponse(BaseModel):
    sharer_name: str
    profile_url: str | None = None
    post_url: str | None = None
    caption_snippet: str | None = None
    posted_at: str | None = None
    privacy_label: str | None = None
    media_preview_urls: list[str] = Field(default_factory=list)


class FacebookPostResponse(BaseModel):
    post_id: str
    post_type: str
    username: str
    caption: str
    likes: int
    comments: int
    shares: int
    views: int
    url: str
    thumbnail_url: str | None = None
    media_urls: list[str] = Field(default_factory=list)
    posted_at: str | None = None
    reactions: dict[str, int] = Field(
        default_factory=dict,
        description="Per-reaction breakdown (Like, Love, Haha, etc.)",
    )
    share_details: list[FacebookShareResponse] = Field(default_factory=list)
    media_provenance: FacebookMediaProvenanceResponse | None = None


def _facebook_post_response(post: Any) -> FacebookPostResponse:
    raw_media_provenance = getattr(post, "media_provenance", None)
    if hasattr(raw_media_provenance, "to_dict"):
        media_provenance = dict(raw_media_provenance.to_dict() or {})
    elif isinstance(raw_media_provenance, dict):
        media_provenance = dict(raw_media_provenance or {})
    else:
        media_provenance = {}
    share_details = []
    for share in getattr(post, "share_details", []) or []:
        posted_at = getattr(share, "posted_at", None)
        share_details.append(
            FacebookShareResponse(
                sharer_name=str(getattr(share, "sharer_name", "") or ""),
                profile_url=str(getattr(share, "profile_url", "") or "") or None,
                post_url=str(getattr(share, "post_url", "") or "") or None,
                caption_snippet=str(getattr(share, "caption_snippet", "") or "") or None,
                posted_at=(
                    datetime.fromtimestamp(int(posted_at), tz=UTC).isoformat() if posted_at is not None else None
                ),
                privacy_label=str(getattr(share, "privacy_label", "") or "") or None,
                media_preview_urls=[
                    str(url) for url in (getattr(share, "media_preview_urls", []) or []) if str(url).strip()
                ],
            )
        )
    return FacebookPostResponse(
        post_id=str(getattr(post, "post_id", "") or ""),
        post_type=str(getattr(post, "post_type", "feed") or "feed"),
        username=str(getattr(post, "username", "") or ""),
        caption=str(getattr(post, "caption", "") or ""),
        likes=int(getattr(post, "likes", 0) or 0),
        comments=int(getattr(post, "comments", 0) or 0),
        shares=int(getattr(post, "shares", 0) or 0),
        views=int(getattr(post, "views", 0) or 0),
        url=str(getattr(post, "url", "") or ""),
        thumbnail_url=str(getattr(post, "thumbnail_url", "") or "") or None,
        media_urls=[str(url) for url in (getattr(post, "media_urls", []) or []) if str(url)],
        posted_at=(
            datetime.fromtimestamp(int(post.posted_at), tz=UTC).isoformat()
            if getattr(post, "posted_at", None) is not None
            else None
        ),
        reactions=dict(getattr(post, "reactions", {}) or {}),
        share_details=share_details,
        media_provenance=(
            FacebookMediaProvenanceResponse(
                platform=str(media_provenance.get("platform") or "facebook"),
                matched_by=str(media_provenance.get("matched_by") or "native"),
                fallback_used=bool(media_provenance.get("fallback_used", False)),
            )
            if media_provenance
            else None
        ),
    )


class FacebookScrapeResponse(BaseModel):
    success: bool
    page_handle: str
    posts_found: int
    posts: list[FacebookPostResponse]
    filters_applied: dict
    retrieval_meta: dict | None = None
    error: str | None = None


class FacebookSearchPostsRequest(BaseModel):
    search_url: str | None = Field(default=None, description="Direct Facebook search URL")
    profile_url: str | None = Field(default=None, description="Facebook profile/page URL used to build search URL")
    query: str = Field(..., description="Search query such as a hashtag or phrase")
    date_start: datetime | None = Field(default=None, description="Optional start date for filtering")
    date_end: datetime | None = Field(default=None, description="Optional end date for filtering")
    max_posts: int = Field(default=25, ge=1, le=100, description="Maximum posts to return")
    include_share_details: bool = Field(default=False, description="Also fetch people who shared the post")
    include_comments: bool = Field(default=False, description="Also fetch visible comments for each post")
    max_comments: int = Field(default=100, ge=0, le=1000, description="Max comments per post")
    max_shares: int = Field(default=100, ge=0, le=500, description="Max share-detail rows per post")
    allow_cross_platform_media_fallback: bool = Field(
        default=True,
        description="Allow strict Instagram media fallback when Facebook media is unavailable",
    )
    delay_seconds: float = Field(default=1.25, ge=0.25, le=10.0, description="Delay between requests")

    @model_validator(mode="after")
    def validate_search_source(self) -> FacebookSearchPostsRequest:
        if not str(self.query or "").strip():
            raise ValueError("query is required")
        if not str(self.search_url or "").strip() and not str(self.profile_url or "").strip():
            raise ValueError("search_url or profile_url is required")
        return self


class FacebookSearchPostsResponse(BaseModel):
    success: bool
    query: str
    posts_found: int
    posts: list[FacebookPostResponse]
    retrieval_meta: dict | None = None
    error: str | None = None


class FacebookPostScrapeRequest(BaseModel):
    post_url: str = Field(..., description="Facebook post/video/reel URL (supports /share/v/ short links)")
    fetch_comments: bool = Field(default=True, description="Also extract comments from the page")
    max_comments: int = Field(default=100, ge=0, le=1000, description="Max comments to extract")
    fetch_shares: bool = Field(default=False, description="Also extract people who shared the post")
    max_shares: int = Field(default=100, ge=0, le=500, description="Max share-detail rows to extract")
    allow_cross_platform_media_fallback: bool = Field(
        default=True,
        description="Allow strict Instagram media fallback when Facebook media is unavailable",
    )


class FacebookCommentResponse(BaseModel):
    comment_id: str
    username: str
    text: str
    likes: int = 0
    created_at: int | None = None
    is_reply: bool = False
    reply_count: int = 0


class FacebookPostScrapeResponse(BaseModel):
    success: bool
    post: FacebookPostResponse | None = None
    comments: list[FacebookCommentResponse] = Field(default_factory=list)
    comments_found: int = 0
    shares_found: int = 0
    error: str | None = None


class ThreadsScrapeRequest(BaseModel):
    username: str = Field(..., description="Threads username (without @)")
    hashtags: list[str] = Field(default_factory=list, description="Optional hashtag filter (without #)")
    keywords: list[str] = Field(default_factory=list, description="Optional keyword filter")
    date_start: datetime | None = Field(default=None, description="Optional start date")
    date_end: datetime | None = Field(default=None, description="Optional end date")
    delay_seconds: float = Field(default=1.0, ge=0.25, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=1, ge=1, le=100, description="Maximum profile pages to inspect")


class ThreadsPostResponse(BaseModel):
    post_id: str
    username: str
    text: str
    likes: int
    replies: int
    reposts: int
    quotes: int
    views: int
    url: str
    thumbnail_url: str | None = None
    media_urls: list[str] = Field(default_factory=list)
    posted_at: str | None = None


class ThreadsScrapeResponse(BaseModel):
    success: bool
    username: str
    posts_found: int
    posts: list[ThreadsPostResponse]
    filters_applied: dict
    retrieval_meta: dict | None = None
    error: str | None = None


@router.post("/tiktok/scrape", response_model=TikTokScrapeResponse)
async def scrape_tiktok(
    request: TikTokScrapeRequest,
    user: InternalAdminUser,
) -> TikTokScrapeResponse:
    """
    Scrape TikTok posts from a profile with optional filtering.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.control_plane.runtime import _load_tiktok_cookies
    from trr_backend.socials.tiktok.direct_scrape import scrape_tiktok as run_tiktok_scrape

    logger.info(f"TikTok scrape requested by {user.get('email')} for @{request.username}")

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="tiktok", surface=surface, loader=_load_tiktok_cookies)

    return TikTokScrapeResponse(**run_tiktok_scrape(request, load_cookies=_load_cookies, logger=logger))


@router.get("/tiktok/preview/{username}")
async def preview_tiktok_profile(
    username: str,
    user: InternalAdminUser,
) -> dict:
    """
    Preview basic info about a TikTok profile.

    Returns profile metadata without full scraping.
    Useful for validating usernames before configuring scrape jobs.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.control_plane.runtime import _load_tiktok_cookies
    from trr_backend.socials.tiktok.direct_scrape import preview_tiktok_profile as run_tiktok_preview

    logger.info(f"TikTok preview requested by {user.get('email')} for @{username}")

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="tiktok", surface=surface, loader=_load_tiktok_cookies)

    return run_tiktok_preview(username, load_cookies=_load_cookies, logger=logger)


@router.post("/twitter/search", response_model=TwitterSearchResponse)
async def search_twitter(
    request: TwitterSearchRequest,
    user: InternalAdminUser,
) -> TwitterSearchResponse:
    """
    Search Twitter/X for tweets matching a query (hashtag or phrase).

    Uses Twitter advanced search syntax to filter by date range.
    Example: searching for "RHOSLC" from 2026-01-01 to 2026-01-11.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.control_plane.runtime import (
        _load_twikit_credentials,
        _load_twitter_auth,
    )
    from trr_backend.socials.twitter.direct_scrape import search_twitter as run_twitter_search

    logger.info(f"Twitter search requested by {user.get('email')} for query: {request.query}")

    def _load_auth() -> tuple[Any, Any, Any]:
        twitter_cookies, twitter_bearer = _load_twitter_auth()
        twikit_creds = _load_twikit_credentials(twitter_cookies)
        return twitter_cookies, twitter_bearer, twikit_creds

    return TwitterSearchResponse(
        **run_twitter_search(
            request,
            load_auth=_load_auth,
            persist_search=persist_standalone_twitter_search,
            logger=logger,
        )
    )


@router.post("/twitter/replies", response_model=TweetRepliesResponse)
async def fetch_tweet_replies(
    request: TweetRepliesRequest,
    user: InternalAdminUser,
) -> TweetRepliesResponse:
    """
    Fetch replies/comments for a specific tweet.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.control_plane.runtime import (
        _load_twikit_credentials,
        _load_twitter_auth,
    )
    from trr_backend.socials.twitter.direct_scrape import fetch_tweet_replies as run_tweet_replies

    logger.info(f"Twitter replies requested by {user.get('email')} for tweet: {request.tweet_id}")

    def _load_auth() -> tuple[Any, Any, Any]:
        twitter_cookies, twitter_bearer = _load_twitter_auth()
        twikit_creds = _load_twikit_credentials(twitter_cookies)
        return twitter_cookies, twitter_bearer, twikit_creds

    return TweetRepliesResponse(**run_tweet_replies(request, load_auth=_load_auth, logger=logger))


@router.post("/twitter/quotes", response_model=TweetQuotesResponse)
async def fetch_tweet_quotes(
    request: TweetQuotesRequest,
    user: InternalAdminUser,
) -> TweetQuotesResponse:
    """
    Fetch quote tweets for a specific tweet.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.control_plane.runtime import (
        _load_twikit_credentials,
        _load_twitter_auth,
    )
    from trr_backend.socials.twitter.direct_scrape import fetch_tweet_quotes as run_tweet_quotes

    logger.info(f"Twitter quotes requested by {user.get('email')} for tweet: {request.tweet_id}")

    def _load_auth() -> tuple[Any, Any, Any]:
        twitter_cookies, twitter_bearer = _load_twitter_auth()
        twikit_creds = _load_twikit_credentials(twitter_cookies)
        return twitter_cookies, twitter_bearer, twikit_creds

    return TweetQuotesResponse(**run_tweet_quotes(request, load_auth=_load_auth, logger=logger))


@router.post("/youtube/scrape", response_model=YouTubeScrapeResponse)
async def scrape_youtube(
    request: YouTubeScrapeRequest,
    user: InternalAdminUser,
) -> YouTubeScrapeResponse:
    """
    Scrape YouTube channel videos with keyword filtering.

    Searches for videos from a specific channel that match the given keywords
    and fall within the date range.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.youtube.direct_scrape import scrape_youtube as run_youtube_scrape

    logger.info(f"YouTube scrape requested by {user.get('email')} for @{request.channel_handle}")

    return YouTubeScrapeResponse(**run_youtube_scrape(request, logger=logger))


@router.post("/facebook/scrape", response_model=FacebookScrapeResponse)
async def scrape_facebook(
    request: FacebookScrapeRequest,
    user: InternalAdminUser,
) -> FacebookScrapeResponse:
    from trr_backend.socials.control_plane.runtime import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import scrape_facebook as run_facebook_scrape

    logger.info("Facebook scrape requested by %s for %s", user.get("email"), request.page_handle)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return FacebookScrapeResponse(**run_facebook_scrape(request, load_cookies=_load_cookies, logger=logger))


@router.post("/facebook/search-posts", response_model=FacebookSearchPostsResponse)
async def search_facebook_posts(
    request: FacebookSearchPostsRequest,
    user: InternalAdminUser,
) -> FacebookSearchPostsResponse:
    from trr_backend.socials.control_plane.runtime import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import search_facebook_posts as run_facebook_search_posts

    logger.info("Facebook search requested by %s for query=%s", user.get("email"), request.query)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return FacebookSearchPostsResponse(**run_facebook_search_posts(request, load_cookies=_load_cookies, logger=logger))


@router.get("/facebook/preview/{page_handle}")
async def preview_facebook_page(page_handle: str, user: InternalAdminUser) -> dict:
    from trr_backend.socials.control_plane.runtime import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import preview_facebook_page as run_facebook_preview

    logger.info("Facebook preview requested by %s for %s", user.get("email"), page_handle)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return run_facebook_preview(page_handle, load_cookies=_load_cookies, logger=logger)


@router.post("/facebook/scrape-post", response_model=FacebookPostScrapeResponse)
async def scrape_facebook_post(
    request: FacebookPostScrapeRequest,
    user: InternalAdminUser,
) -> FacebookPostScrapeResponse:
    from trr_backend.socials.control_plane.runtime import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import scrape_facebook_post as run_facebook_post_scrape

    logger.info("Facebook post scrape requested by %s for %s", user.get("email"), request.post_url)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return FacebookPostScrapeResponse(**run_facebook_post_scrape(request, load_cookies=_load_cookies, logger=logger))


@router.post("/threads/scrape", response_model=ThreadsScrapeResponse)
async def scrape_threads(
    request: ThreadsScrapeRequest,
    user: InternalAdminUser,
) -> ThreadsScrapeResponse:
    from trr_backend.socials.control_plane.runtime import _load_threads_cookies
    from trr_backend.socials.threads.direct_scrape import scrape_threads as run_threads_scrape

    logger.info("Threads scrape requested by %s for @%s", user.get("email"), request.username)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="threads", surface=surface, loader=_load_threads_cookies)

    return ThreadsScrapeResponse(**run_threads_scrape(request, load_cookies=_load_cookies, logger=logger))


@router.get("/threads/preview/{username}")
async def preview_threads_profile(username: str, user: InternalAdminUser) -> dict:
    from trr_backend.socials.control_plane.runtime import _load_threads_cookies
    from trr_backend.socials.threads.direct_scrape import preview_threads_profile as run_threads_preview

    logger.info("Threads preview requested by %s for @%s", user.get("email"), username)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="threads", surface=surface, loader=_load_threads_cookies)

    return run_threads_preview(username, load_cookies=_load_cookies, logger=logger)


__all__ = [name for name in globals() if not name.startswith("__")]
