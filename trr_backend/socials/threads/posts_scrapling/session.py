"""Threads session adapter for the posts Scrapling lane.

Delegates to the canonical Threads cookie loader in social_season_analytics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ThreadsPostsScraplingSession:
    raw_cookies: dict[str, str]
    cookies: list[dict[str, Any]]
    cookie_source: str


def _cookies_to_scrapling(cookies: dict[str, str]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for name, value in (cookies or {}).items():
        cookie_name = str(name or "").strip()
        cookie_value = str(value or "").strip()
        if not (cookie_name and cookie_value):
            continue
        payload.append(
            {
                "name": cookie_name,
                "value": cookie_value,
                "domain": ".threads.com",
                "path": "/",
            }
        )
    return payload


def _load_threads_cookies() -> dict[str, str]:
    from trr_backend.repositories.social_season_analytics import _load_threads_cookies as _canonical_load

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
