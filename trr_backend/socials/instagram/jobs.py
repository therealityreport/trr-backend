"""Instagram claimed-job handlers."""

from __future__ import annotations

from typing import Any

from trr_backend.socials.pipelines.job_handler_types import FunctionPlatformJobHandler, PlatformJobHandler
from trr_backend.socials.pipelines.shared_job_executor import execute_shared_claimed_job

INSTAGRAM_COMMENTS_SCRAPLING_STAGE = "comments_scrapling"
INSTAGRAM_POSTS_SCRAPLING_STAGE = "posts_scrapling"
INSTAGRAM_PROFILE_SNAPSHOT_STAGE = "instagram_profile_snapshot"
INSTAGRAM_PROFILE_FOLLOWING_STAGE = "instagram_profile_following"


def _run_comments_scrapling(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.socials.instagram.comments_scrapling.job_runner import run_instagram_comments_scrapling_job

    return run_instagram_comments_scrapling_job(job, worker_id=worker_id)


def _run_posts_scrapling(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job

    return run_instagram_posts_scrapling_job(job, worker_id=worker_id)


def _run_shared_instagram_stage(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    return execute_shared_claimed_job(job, worker_id=worker_id)


def instagram_job_handlers() -> tuple[PlatformJobHandler, ...]:
    return (
        FunctionPlatformJobHandler("instagram", INSTAGRAM_COMMENTS_SCRAPLING_STAGE, _run_comments_scrapling),
        FunctionPlatformJobHandler("instagram", INSTAGRAM_POSTS_SCRAPLING_STAGE, _run_posts_scrapling),
        FunctionPlatformJobHandler("instagram", INSTAGRAM_PROFILE_SNAPSHOT_STAGE, _run_shared_instagram_stage),
        FunctionPlatformJobHandler("instagram", INSTAGRAM_PROFILE_FOLLOWING_STAGE, _run_shared_instagram_stage),
    )


__all__ = [
    "INSTAGRAM_COMMENTS_SCRAPLING_STAGE",
    "INSTAGRAM_POSTS_SCRAPLING_STAGE",
    "INSTAGRAM_PROFILE_FOLLOWING_STAGE",
    "INSTAGRAM_PROFILE_SNAPSHOT_STAGE",
    "instagram_job_handlers",
]
