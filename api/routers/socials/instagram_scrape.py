# ruff: noqa: F401, F403, F405, I001, UP037
"""Direct Instagram scrape and preview routes."""
from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from .social_landing import *

router = APIRouter()

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
    allow_inline_dev_fallback: bool = Field(default=False)

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

    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]
    username: str
    hashtags: list[str] = Field(default=[])
    entity_type: Literal["show", "season", "person"]
    show_id: UUID | None = None
    season_number: int | None = None
    person_id: UUID | None = None

@router.post("/instagram/scrape", response_model=InstagramScrapeResponse)
async def scrape_instagram(
    request: InstagramScrapeRequest,
    user: InternalAdminUser,
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
        from trr_backend.socials.instagram.auth_runtime import _load_instagram_cookies

        cookies = _load_social_auth_or_503(platform="instagram", surface="scrape", loader=_load_instagram_cookies)
        scraper = InstagramScraper(cookies=cookies)
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
    except HTTPException:
        raise
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
    user: InternalAdminUser,
) -> dict:
    """
    Start an async Instagram scrape operation.

    Returns immediately with a job ID. Results can be polled or will be
    stored in the database when complete.

    Requires admin access (allowlist only).
    """
    from trr_backend.db import pg
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.control_plane.worker_health import (
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )
    from trr_backend.socials.control_plane import (
        execute_run,
        ingest_season,
    )

    if request.show_id is None or request.season_number is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "BAD_REQUEST",
                "message": "show_id and season_number are required for async ingest",
            },
        )

    season_row = pg.fetch_one(
        """
        select id::text as season_id
        from core.seasons
        where show_id = %s::uuid and season_number = %s
        limit 1
        """,
        [str(request.show_id), int(request.season_number)],
    )
    if not season_row or not str(season_row.get("season_id") or "").strip():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "SEASON_NOT_FOUND",
                "message": "No season found for the provided show_id and season_number",
            },
        )

    season_id = str(season_row.get("season_id") or "").strip()
    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    used_inline_fallback = False
    worker_health: dict[str, Any] | None = None
    if queue_enabled:
        try:
            worker_health = await run_in_threadpool(assert_worker_available_when_queue_enabled)
        except SocialWorkerUnavailableError as exc:
            worker_health = exc.worker_health
            if request.allow_inline_dev_fallback and _is_local_or_dev_runtime() and not remote_plane_enforced:
                queue_enabled = False
                used_inline_fallback = True
            else:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": (
                            "SOCIAL_REMOTE_JOB_PLANE_ENFORCED" if remote_plane_enforced else "SOCIAL_WORKER_UNAVAILABLE"
                        ),
                        "message": _remote_worker_unavailable_message(exc) if remote_plane_enforced else str(exc),
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
    elif not remote_plane_enforced:
        used_inline_fallback = bool(request.allow_inline_dev_fallback)
    else:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_REMOTE_JOB_PLANE_ENFORCED",
                "message": (
                    "Social ingest remote-worker ownership is enforced "
                    "(TRR_JOB_PLANE_MODE=remote or TRR_LONG_JOB_ENFORCE_REMOTE=1)."
                ),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
            },
        )

    try:
        run_payload = ingest_season(
            season_id,
            platforms=["instagram"],
            accounts_override=[request.username],
            hashtags_override=request.hashtags or [],
            keywords_override=[],
            source_scope="network",
            max_posts_per_target=0,
            max_comments_per_post=0,
            max_replies_per_post=0,
            fetch_replies=False,
            ingest_mode="posts_only",
            sync_strategy="incremental",
            comment_refresh_policy="balanced",
            comment_anchor_source_ids=None,
            date_start=request.date_start,
            date_end=request.date_end,
            initiated_by=user.get("email"),
            inline_worker_id=None if queue_enabled else "api-background",
        )
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc

    run_id = str(run_payload.get("run_id") or "").strip()
    if not run_id:
        raise HTTPException(
            status_code=500,
            detail={"code": "INGEST_RUN_NOT_CREATED", "message": "Failed to create async Instagram ingest run"},
        )

    if not queue_enabled:
        background_tasks.add_task(execute_run, run_id, worker_id="api-background:instagram", platform="instagram")

    execution_mode, execution_mode_canonical, execution_mode_legacy = _resolve_social_execution_modes(
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
    )
    logger.info("Async Instagram scrape requested by %s - run %s", user.get("email"), run_id)
    response_payload = {
        "job_id": run_id,
        "run_id": run_id,
        "season_id": season_id,
        "status": "queued" if queue_enabled else "started",
        "execution_mode": execution_mode,
        "execution_mode_canonical": execution_mode_canonical,
        "execution_mode_legacy": execution_mode_legacy,
        "execution_owner": execution_owner_label(),
        "execution_backend_canonical": execution_metadata()["execution_backend_canonical"],
        "execution_mode_deprecation": _social_execution_mode_deprecation_payload(),
        "jobs_url": f"/api/v1/admin/socials/seasons/{season_id}/ingest/jobs?run_id={run_id}",
        "runs_url": f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs?run_id={run_id}",
        "message": (
            "Async Instagram ingest run queued. Poll /ingest/jobs with run_id for progress."
            if execution_mode == "queued"
            else "Async Instagram ingest run started inline. Poll /ingest/jobs with run_id for progress."
        ),
    }
    if used_inline_fallback and worker_health is not None:
        response_payload["worker_health"] = worker_health
    return response_payload

@router.get("/instagram/preview/{username}")
async def preview_instagram_profile(
    username: str,
    user: InternalAdminUser,
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
        raise HTTPException(
            status_code=500,
            detail="Instagram preview failed",
            headers={"x-error-code": "SOCIAL_PREVIEW_FAILED"},
        ) from e

__all__ = [name for name in globals() if not name.startswith("__")]
