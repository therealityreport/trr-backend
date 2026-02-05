"""
API endpoints for social media scraping and analytics.

Provides endpoints to:
1. Scrape Instagram posts from profiles with filtering
2. Scrape TikTok posts from profiles with filtering
3. Search Twitter/X for tweets by hashtag/phrase
4. Scrape YouTube channel videos by keywords
5. Configure social accounts for shows/seasons/cast members
6. Retrieve cached social media data
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from api.auth import AdminUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/socials", tags=["admin-socials"])


# Request/Response Models


class InstagramScrapeRequest(BaseModel):
    """Request to scrape Instagram posts."""

    username: str = Field(..., description="Instagram username to scrape (without @)")
    hashtags: list[str] = Field(..., description="Hashtags to filter by (without #)")
    date_start: datetime = Field(..., description="Start date for filtering")
    date_end: datetime = Field(..., description="End date for filtering")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=None, ge=1, le=500, description="Maximum pages to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")


class InstagramPostResponse(BaseModel):
    """Single Instagram post in response."""

    shortcode: str
    post_type: str
    date_time: str
    caption: str
    profile_tags: list[str]
    sponsored: bool
    likes: int
    comments: int
    video_views: int
    url: str
    username: str


class InstagramScrapeResponse(BaseModel):
    """Response from Instagram scrape operation."""

    success: bool
    username: str
    posts_found: int
    posts: list[InstagramPostResponse]
    filters_applied: dict
    error: str | None = None


class SocialAccountConfig(BaseModel):
    """Configuration for a social account to track."""

    platform: Literal["instagram", "tiktok", "twitter"]
    username: str
    hashtags: list[str] = Field(default=[])
    entity_type: Literal["show", "season", "person"]
    show_id: UUID | None = None
    season_number: int | None = None
    person_id: UUID | None = None


# Endpoints


@router.post("/instagram/scrape", response_model=InstagramScrapeResponse)
async def scrape_instagram(
    request: InstagramScrapeRequest,
    user: AdminUser,
) -> InstagramScrapeResponse:
    """
    Scrape Instagram posts from a profile with optional filtering.

    This is a synchronous endpoint that returns results immediately.
    For large scrapes, consider using the async version.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.instagram import InstagramScraper, ScrapeConfig

    logger.info(f"Instagram scrape requested by {user.get('email')} for @{request.username}")

    config = ScrapeConfig(
        username=request.username,
        hashtags=request.hashtags,
        date_start=request.date_start,
        date_end=request.date_end,
        delay_seconds=request.delay_seconds,
        max_pages=request.max_pages,
        show_id=request.show_id,
        season_number=request.season_number,
        person_id=request.person_id,
    )

    try:
        # Note: In production, cookies would come from secure storage (env vars or secrets manager)
        # For now, running without auth (limited results)
        scraper = InstagramScraper(cookies={})
        posts = scraper.scrape(config)

        return InstagramScrapeResponse(
            success=True,
            username=request.username,
            posts_found=len(posts),
            posts=[
                InstagramPostResponse(
                    shortcode=p.shortcode,
                    post_type=p.post_type,
                    date_time=p.date_time,
                    caption=p.caption,
                    profile_tags=p.profile_tags,
                    sponsored=p.sponsored,
                    likes=p.likes,
                    comments=p.comments,
                    video_views=p.video_views,
                    url=p.url,
                    username=p.username,
                )
                for p in posts
            ],
            filters_applied={
                "hashtags": request.hashtags,
                "date_start": request.date_start.isoformat() if request.date_start else None,
                "date_end": request.date_end.isoformat() if request.date_end else None,
            },
        )
    except Exception as e:
        logger.error(f"Instagram scrape failed: {e}", exc_info=True)
        return InstagramScrapeResponse(
            success=False,
            username=request.username,
            posts_found=0,
            posts=[],
            filters_applied={},
            error=str(e),
        )


@router.post("/instagram/scrape/async")
async def scrape_instagram_async(
    request: InstagramScrapeRequest,
    background_tasks: BackgroundTasks,
    user: AdminUser,
) -> dict:
    """
    Start an async Instagram scrape operation.

    Returns immediately with a job ID. Results can be polled or will be
    stored in the database when complete.

    Requires admin access (allowlist only).
    """
    import uuid

    job_id = str(uuid.uuid4())

    # TODO: Implement async scraping with job tracking
    # For now, return a placeholder response
    logger.info(f"Async Instagram scrape requested by {user.get('email')} - job {job_id}")

    return {
        "job_id": job_id,
        "status": "queued",
        "message": "Async scraping not yet implemented. Use /instagram/scrape for synchronous scraping.",
    }


@router.get("/instagram/preview/{username}")
async def preview_instagram_profile(
    username: str,
    user: AdminUser,
) -> dict:
    """
    Preview basic info about an Instagram profile.

    Returns profile metadata and recent post count without full scraping.
    Useful for validating usernames before configuring scrape jobs.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.instagram import InstagramScraper

    logger.info(f"Instagram preview requested by {user.get('email')} for @{username}")

    try:
        scraper = InstagramScraper(cookies={})
        data = scraper.fetch_profile_info(username, delay=0)

        if not data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        user_data = data.get("data", {}).get("user", {})
        if not user_data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        timeline = user_data.get("edge_owner_to_timeline_media", {})

        return {
            "username": user_data.get("username"),
            "full_name": user_data.get("full_name"),
            "biography": user_data.get("biography"),
            "is_verified": user_data.get("is_verified", False),
            "is_private": user_data.get("is_private", False),
            "followers": user_data.get("edge_followed_by", {}).get("count", 0),
            "following": user_data.get("edge_follow", {}).get("count", 0),
            "post_count": timeline.get("count", 0),
            "profile_pic_url": user_data.get("profile_pic_url_hd") or user_data.get("profile_pic_url"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Instagram preview failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


# TikTok Models


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
    error: str | None = None


# TikTok Endpoints


@router.post("/tiktok/scrape", response_model=TikTokScrapeResponse)
async def scrape_tiktok(
    request: TikTokScrapeRequest,
    user: AdminUser,
) -> TikTokScrapeResponse:
    """
    Scrape TikTok posts from a profile with optional filtering.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper

    logger.info(f"TikTok scrape requested by {user.get('email')} for @{request.username}")

    config = TikTokScrapeConfig(
        username=request.username,
        hashtags=request.hashtags,
        date_start=request.date_start,
        date_end=request.date_end,
        delay_seconds=request.delay_seconds,
        max_pages=request.max_pages,
        show_id=request.show_id,
        season_number=request.season_number,
        person_id=request.person_id,
    )

    try:
        scraper = TikTokScraper()
        posts = scraper.scrape(config)

        return TikTokScrapeResponse(
            success=True,
            username=request.username,
            posts_found=len(posts),
            posts=[
                TikTokPostResponse(
                    video_id=p.video_id,
                    date_time=p.date_time,
                    description=p.description,
                    hashtags=p.hashtags,
                    mentions=p.mentions,
                    likes=p.likes,
                    comments=p.comments,
                    shares=p.shares,
                    views=p.views,
                    url=p.url,
                    username=p.username,
                    author_nickname=p.author_nickname,
                    duration=p.duration,
                    music_title=p.music_title,
                    music_author=p.music_author,
                )
                for p in posts
            ],
            filters_applied={
                "hashtags": request.hashtags,
                "date_start": request.date_start.isoformat(),
                "date_end": request.date_end.isoformat(),
            },
        )
    except Exception as e:
        logger.error(f"TikTok scrape failed: {e}", exc_info=True)
        return TikTokScrapeResponse(
            success=False,
            username=request.username,
            posts_found=0,
            posts=[],
            filters_applied={},
            error=str(e),
        )


@router.get("/tiktok/preview/{username}")
async def preview_tiktok_profile(
    username: str,
    user: AdminUser,
) -> dict:
    """
    Preview basic info about a TikTok profile.

    Returns profile metadata without full scraping.
    Useful for validating usernames before configuring scrape jobs.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.tiktok import TikTokScraper

    logger.info(f"TikTok preview requested by {user.get('email')} for @{username}")

    try:
        scraper = TikTokScraper()
        data = scraper.fetch_user_detail(username, delay=0)

        if not data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        user_info = data.get("userInfo", {})
        user_data = user_info.get("user", {})
        stats = user_info.get("stats", {})

        if not user_data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        return {
            "username": user_data.get("uniqueId"),
            "nickname": user_data.get("nickname"),
            "bio": user_data.get("signature"),
            "is_verified": user_data.get("verified", False),
            "is_private": user_data.get("privateAccount", False),
            "followers": stats.get("followerCount", 0),
            "following": stats.get("followingCount", 0),
            "likes": stats.get("heart", 0),
            "video_count": stats.get("videoCount", 0),
            "profile_pic_url": user_data.get("avatarLarger") or user_data.get("avatarMedium"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"TikTok preview failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


# Twitter/X Models


class TwitterSearchRequest(BaseModel):
    """Request to search Twitter/X for tweets."""

    query: str = Field(..., description="Search query (hashtag or phrase, e.g., RHOSLC or #RHOSLC)")
    date_start: datetime = Field(..., description="Start date for search")
    date_end: datetime = Field(..., description="End date for search")
    include_replies: bool = Field(default=False, description="Include reply tweets in results")
    include_links: bool = Field(default=True, description="Include tweets with links")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=None, ge=1, le=100, description="Maximum pages to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")


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
    url: str
    username: str
    display_name: str
    user_verified: bool
    is_reply: bool
    is_retweet: bool
    is_quote: bool
    media_urls: list[str]


class TwitterSearchResponse(BaseModel):
    """Response from Twitter search operation."""

    success: bool
    query: str
    tweets_found: int
    tweets: list[TweetResponse]
    search_query_used: str
    filters_applied: dict
    error: str | None = None


class TweetRepliesRequest(BaseModel):
    """Request to fetch replies for a tweet."""

    tweet_id: str = Field(..., description="Tweet ID to fetch replies for")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")


# Twitter/X Endpoints


@router.post("/twitter/search", response_model=TwitterSearchResponse)
async def search_twitter(
    request: TwitterSearchRequest,
    user: AdminUser,
) -> TwitterSearchResponse:
    """
    Search Twitter/X for tweets matching a query (hashtag or phrase).

    Uses Twitter advanced search syntax to filter by date range.
    Example: searching for "RHOSLC" from 2026-01-01 to 2026-01-11.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.twitter import TwitterScrapeConfig, TwitterScraper

    logger.info(f"Twitter search requested by {user.get('email')} for query: {request.query}")

    config = TwitterScrapeConfig(
        query=request.query,
        date_start=request.date_start,
        date_end=request.date_end,
        include_replies=request.include_replies,
        include_links=request.include_links,
        delay_seconds=request.delay_seconds,
        max_pages=request.max_pages,
        show_id=request.show_id,
        season_number=request.season_number,
        person_id=request.person_id,
    )

    try:
        # Note: In production, cookies/bearer token would come from secure storage
        scraper = TwitterScraper()
        tweets = scraper.scrape(config)

        return TwitterSearchResponse(
            success=True,
            query=request.query,
            tweets_found=len(tweets),
            tweets=[
                TweetResponse(
                    tweet_id=t.tweet_id,
                    date_time=t.date_time,
                    text=t.text,
                    hashtags=t.hashtags,
                    mentions=t.mentions,
                    likes=t.likes,
                    retweets=t.retweets,
                    replies=t.replies,
                    quotes=t.quotes,
                    views=t.views,
                    url=t.url,
                    username=t.username,
                    display_name=t.display_name,
                    user_verified=t.user_verified,
                    is_reply=t.is_reply,
                    is_retweet=t.is_retweet,
                    is_quote=t.is_quote,
                    media_urls=t.media_urls,
                )
                for t in tweets
            ],
            search_query_used=config.build_search_query(),
            filters_applied={
                "query": request.query,
                "date_start": request.date_start.isoformat(),
                "date_end": request.date_end.isoformat(),
                "include_replies": request.include_replies,
                "include_links": request.include_links,
            },
        )
    except Exception as e:
        logger.error(f"Twitter search failed: {e}", exc_info=True)
        return TwitterSearchResponse(
            success=False,
            query=request.query,
            tweets_found=0,
            tweets=[],
            search_query_used=config.build_search_query(),
            filters_applied={},
            error=str(e),
        )


@router.post("/twitter/replies")
async def fetch_tweet_replies(
    request: TweetRepliesRequest,
    user: AdminUser,
) -> dict:
    """
    Fetch replies/comments for a specific tweet.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.twitter import TwitterScraper

    logger.info(f"Twitter replies requested by {user.get('email')} for tweet: {request.tweet_id}")

    try:
        scraper = TwitterScraper()
        replies = scraper.fetch_tweet_replies(request.tweet_id, request.delay_seconds)

        return {
            "success": True,
            "tweet_id": request.tweet_id,
            "replies_found": len(replies),
            "replies": [
                {
                    "tweet_id": r.tweet_id,
                    "date_time": r.date_time,
                    "text": r.text,
                    "username": r.username,
                    "likes": r.likes,
                    "url": r.url,
                }
                for r in replies
            ],
        }
    except Exception as e:
        logger.error(f"Twitter replies fetch failed: {e}", exc_info=True)
        return {
            "success": False,
            "tweet_id": request.tweet_id,
            "replies_found": 0,
            "replies": [],
            "error": str(e),
        }


# YouTube Models


class YouTubeScrapeRequest(BaseModel):
    """Request to scrape YouTube channel videos."""

    channel_handle: str = Field(..., description="YouTube channel handle (without @)")
    keywords: list[str] = Field(..., description="Keywords to filter by (e.g., RHOSLC, 'Salt Lake City')")
    date_start: datetime = Field(..., description="Start date for filtering")
    date_end: datetime = Field(..., description="End date for filtering")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_results: int | None = Field(default=None, ge=1, le=500, description="Maximum videos to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")


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


# YouTube Endpoints


@router.post("/youtube/scrape", response_model=YouTubeScrapeResponse)
async def scrape_youtube(
    request: YouTubeScrapeRequest,
    user: AdminUser,
) -> YouTubeScrapeResponse:
    """
    Scrape YouTube channel videos with keyword filtering.

    Searches for videos from a specific channel that match the given keywords
    and fall within the date range.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.youtube import YouTubeScrapeConfig, YouTubeScraper

    logger.info(f"YouTube scrape requested by {user.get('email')} for @{request.channel_handle}")

    config = YouTubeScrapeConfig(
        channel_handle=request.channel_handle,
        keywords=request.keywords,
        date_start=request.date_start,
        date_end=request.date_end,
        delay_seconds=request.delay_seconds,
        max_results=request.max_results,
        show_id=request.show_id,
        season_number=request.season_number,
        person_id=request.person_id,
    )

    try:
        scraper = YouTubeScraper()
        videos = scraper.scrape(config)

        return YouTubeScrapeResponse(
            success=True,
            channel_handle=request.channel_handle,
            videos_found=len(videos),
            videos=[
                YouTubeVideoResponse(
                    video_id=v.video_id,
                    title=v.title,
                    description=v.description[:500] if v.description else "",
                    date_time=v.date_time,
                    channel_title=v.channel_title,
                    duration=v.duration,
                    duration_seconds=v.duration_seconds,
                    views=v.views,
                    likes=v.likes,
                    comments=v.comments,
                    url=v.url,
                    thumbnail_url=v.thumbnail_url,
                    keywords_matched=v.keywords_matched,
                )
                for v in videos
            ],
            filters_applied={
                "keywords": request.keywords,
                "date_start": request.date_start.isoformat(),
                "date_end": request.date_end.isoformat(),
            },
        )
    except Exception as e:
        logger.error(f"YouTube scrape failed: {e}", exc_info=True)
        return YouTubeScrapeResponse(
            success=False,
            channel_handle=request.channel_handle,
            videos_found=0,
            videos=[],
            filters_applied={},
            error=str(e),
        )
