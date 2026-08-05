"""Threads claimed-job handlers."""

from __future__ import annotations

from typing import Any

from trr_backend.socials.pipelines.job_handler_types import FunctionPlatformJobHandler, PlatformJobHandler

THREADS_POSTS_SCRAPLING_STAGE = "threads_posts_scrapling"


def _run_posts_scrapling(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.socials.threads.posts_scrapling.job_runner import run_threads_posts_scrapling_job

    return run_threads_posts_scrapling_job(job, worker_id=worker_id)


def threads_job_handlers() -> tuple[PlatformJobHandler, ...]:
    return (FunctionPlatformJobHandler("threads", THREADS_POSTS_SCRAPLING_STAGE, _run_posts_scrapling),)


__all__ = [
    "THREADS_POSTS_SCRAPLING_STAGE",
    "threads_job_handlers",
]
