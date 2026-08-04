"""Threads session adapter for the posts Scrapling lane.

Delegates through the configured canonical Threads cookie-loader port. Cookie
selection, paired validation, fallback, and refresh remain in social_season_analytics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trr_backend.socials.pipelines.threads_cookie_loader import (
    load_threads_cookies as _load_threads_cookies,
)
from trr_backend.socials.scrapling_transport import cookies_to_scrapling


@dataclass(slots=True)
class ThreadsPostsScraplingSession:
    raw_cookies: dict[str, str]
    cookies: list[dict[str, Any]]
    cookie_source: str


def _cookies_to_scrapling(cookies: dict[str, str]) -> list[dict[str, Any]]:
    return cookies_to_scrapling(cookies, ".threads.com")


def resolve_threads_posts_session() -> ThreadsPostsScraplingSession:
    cookies = _load_threads_cookies()
    if not cookies:
        raise RuntimeError("No Threads cookies found via canonical loader.")
    return ThreadsPostsScraplingSession(
        raw_cookies=cookies,
        cookies=_cookies_to_scrapling(cookies),
        cookie_source="canonical",
    )
