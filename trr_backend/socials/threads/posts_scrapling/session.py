"""Threads session adapter for the posts Scrapling lane.

Delegates to the canonical Threads cookie loader in social_season_analytics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trr_backend.socials.scrapling_transport import cookies_to_scrapling


@dataclass(slots=True)
class ThreadsPostsScraplingSession:
    raw_cookies: dict[str, str]
    cookies: list[dict[str, Any]]
    cookie_source: str


def _cookies_to_scrapling(cookies: dict[str, str]) -> list[dict[str, Any]]:
    return cookies_to_scrapling(cookies, ".threads.com")


def _load_threads_cookies() -> dict[str, str]:
    from trr_backend.socials.social_season_analytics_impl import _load_threads_cookies as _canonical_load

    return _canonical_load()


def resolve_threads_posts_session() -> ThreadsPostsScraplingSession:
    cookies = _load_threads_cookies()
    if not cookies:
        raise RuntimeError("No Threads cookies found via canonical loader.")
    return ThreadsPostsScraplingSession(
        raw_cookies=cookies,
        cookies=_cookies_to_scrapling(cookies),
        cookie_source="canonical",
    )
