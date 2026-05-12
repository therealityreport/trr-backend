"""Meta Threads scraping module."""

from __future__ import annotations

from typing import Any

# Compatibility exports remain during the platform cleanup so direct scrape,
# posts_catalog, and posts_scrapling callers can migrate independently.
from .crawlee_adapter import run_stage_with_crawlee
from .direct_scrape import post_to_payload, preview_threads_profile, scrape_threads
from .media_resolver import ThreadsMediaResolution, resolve_threads_media
from .scraper import ThreadsComment, ThreadsPost, ThreadsScrapeConfig, ThreadsScraper

__all__ = [
    "post_to_payload",
    "preview_threads_profile",
    "scrape_threads",
    "ThreadsScraper",
    "ThreadsScrapeConfig",
    "ThreadsPost",
    "ThreadsComment",
    "ThreadsMediaResolution",
    "resolve_threads_media",
    "run_stage_with_crawlee",
    "run_threads_posts_scrapling_job",
]


def __getattr__(name: str) -> Any:
    if name == "run_threads_posts_scrapling_job":
        from .posts_scrapling.job_runner import run_threads_posts_scrapling_job

        return run_threads_posts_scrapling_job
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
