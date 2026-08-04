"""Platform job handler registry for claimed social jobs."""

from __future__ import annotations

from trr_backend.socials.pipelines.job_handler_types import (
    FunctionPlatformJobHandler,
    PlatformJobHandler,
    _normalize_key,
)


def registered_platform_job_handlers() -> tuple[PlatformJobHandler, ...]:
    from trr_backend.socials.instagram.comments_scrapling.job_runner import (
        run_instagram_comments_scrapling_job,
    )
    from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job
    from trr_backend.socials.pipelines.shared_job_executor import execute_shared_claimed_job
    from trr_backend.socials.threads.posts_scrapling.job_runner import run_threads_posts_scrapling_job
    from trr_backend.socials.tiktok.posts_scrapling.job_runner import run_tiktok_posts_scrapling_job

    return (
        FunctionPlatformJobHandler("instagram", "comments_scrapling", run_instagram_comments_scrapling_job),
        FunctionPlatformJobHandler("instagram", "posts_scrapling", run_instagram_posts_scrapling_job),
        FunctionPlatformJobHandler("instagram", "instagram_profile_snapshot", execute_shared_claimed_job),
        FunctionPlatformJobHandler("instagram", "instagram_profile_following", execute_shared_claimed_job),
        FunctionPlatformJobHandler("tiktok", "tiktok_posts_scrapling", run_tiktok_posts_scrapling_job),
        FunctionPlatformJobHandler("threads", "threads_posts_scrapling", run_threads_posts_scrapling_job),
    )


def resolve_platform_job_handler(platform: str, stage: str) -> PlatformJobHandler | None:
    normalized_platform = _normalize_key(platform)
    normalized_stage = _normalize_key(stage)
    matches = [
        handler
        for handler in registered_platform_job_handlers()
        if handler.supports(normalized_platform, normalized_stage)
    ]
    if len(matches) > 1:
        raise RuntimeError(f"multiple_social_job_handlers:{normalized_platform}:{normalized_stage}")
    return matches[0] if matches else None


__all__ = [
    "FunctionPlatformJobHandler",
    "PlatformJobHandler",
    "registered_platform_job_handlers",
    "resolve_platform_job_handler",
]
