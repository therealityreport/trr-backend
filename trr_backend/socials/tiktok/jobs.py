"""TikTok claimed-job handlers."""

from __future__ import annotations

from typing import Any

from trr_backend.socials.pipelines.job_handler_types import FunctionPlatformJobHandler, PlatformJobHandler

TIKTOK_POSTS_SCRAPLING_STAGE = "tiktok_posts_scrapling"


def _run_posts_scrapling(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.socials.tiktok.posts_scrapling.job_runner import run_tiktok_posts_scrapling_job

    return run_tiktok_posts_scrapling_job(job, worker_id=worker_id)


def tiktok_job_handlers() -> tuple[PlatformJobHandler, ...]:
    return (FunctionPlatformJobHandler("tiktok", TIKTOK_POSTS_SCRAPLING_STAGE, _run_posts_scrapling),)


__all__ = [
    "TIKTOK_POSTS_SCRAPLING_STAGE",
    "tiktok_job_handlers",
]
