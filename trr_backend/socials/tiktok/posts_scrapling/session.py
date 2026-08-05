"""TikTok cookie resolution for the posts Scrapling lane.

Delegates through the configured canonical TikTok cookie-loader port. Cookie
selection, validation, and refresh remain owned by social_season_analytics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trr_backend.socials.pipelines.tiktok_cookie_loader import (
    load_tiktok_cookies as _load_tiktok_cookies,
)


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
