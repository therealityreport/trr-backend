"""TikTok cookie resolution for the posts Scrapling lane.

Delegates to the canonical _load_tiktok_cookies() from
social_season_analytics which handles env vars, file loading,
cookie validation, and auto-refresh.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class TikTokPostsScraplingSession:
    raw_cookies: dict[str, str]
    cookies: list[dict[str, Any]]  # Scrapling format
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
                "domain": ".tiktok.com",
                "path": "/",
            }
        )
    return payload


def _load_tiktok_cookies() -> dict[str, str]:
    """Import and call the canonical TikTok cookie loader from the repo.

    Lazy import to avoid loading the 10k+ line social_season_analytics
    module until needed, and to make the function patchable in tests.
    """
    from trr_backend.repositories.social_season_analytics import (
        _load_tiktok_cookies as _canonical_load,
    )

    return _canonical_load()


def resolve_tiktok_posts_session() -> TikTokPostsScraplingSession:
    """Resolve TikTok cookies via canonical loader and convert to Scrapling format."""
    cookies = _load_tiktok_cookies()
    if not cookies:
        raise RuntimeError("No TikTok cookies found via canonical loader. Check env vars or cookie files.")
    return TikTokPostsScraplingSession(
        raw_cookies=cookies,
        cookies=_cookies_to_scrapling(cookies),
        cookie_source="canonical",
    )
