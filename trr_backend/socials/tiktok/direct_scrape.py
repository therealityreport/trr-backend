"""Route-facing TikTok direct preview and scrape orchestration.

This module owns the direct admin preview/scrape interface. It intentionally
does not import the TikTok ``posts_scrapling`` claimed-job lane or the shared
social season analytics repository. TikTok comments remain outside this direct
backend interface.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from fastapi import HTTPException

from trr_backend.socials.tiktok.normalizer import (
    post_to_route_response,
    profile_preview_to_route_response,
)
from trr_backend.socials.tiktok.parser import extract_profile_preview_sections

logger = logging.getLogger(__name__)

_SAFE_DIAGNOSTIC_KEYS = (
    "retrieval_mode",
    "http_client",
    "fallback_chain",
    "stop_reason",
    "error_code",
    "risk_state",
    "operator_summary",
    "operator_action",
    "triage_bucket",
    "profile_enrichment_status",
)


def build_scrape_diagnostics(retrieval_meta: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the route-visible safe subset of TikTok retrieval diagnostics."""

    diagnostics = {key: retrieval_meta[key] for key in _SAFE_DIAGNOSTIC_KEYS if key in retrieval_meta}
    return diagnostics or None


def _request_value(request: Any, name: str) -> Any:
    if isinstance(request, Mapping):
        return request[name]
    return getattr(request, name)


def _request_optional_value(request: Any, name: str, default: Any = None) -> Any:
    if isinstance(request, Mapping):
        return request.get(name, default)
    return getattr(request, name, default)


def _build_scrape_config(request: Any) -> Any:
    from trr_backend.socials import tiktok as tiktok_module

    return tiktok_module.TikTokScrapeConfig(
        username=_request_value(request, "username"),
        hashtags=_request_value(request, "hashtags"),
        date_start=_request_value(request, "date_start"),
        date_end=_request_value(request, "date_end"),
        delay_seconds=_request_optional_value(request, "delay_seconds", 2.0),
        max_pages=_request_optional_value(request, "max_pages"),
        show_id=_request_optional_value(request, "show_id"),
        season_number=_request_optional_value(request, "season_number"),
        person_id=_request_optional_value(request, "person_id"),
    )


def scrape_tiktok(
    request: Any,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Scrape TikTok posts and return the current admin route response payload."""

    active_logger = logger or globals()["logger"]
    username = _request_value(request, "username")
    config = _build_scrape_config(request)

    try:
        from trr_backend.socials import tiktok as tiktok_module

        tiktok_cookies = load_cookies("scrape")
        scraper = tiktok_module.TikTokScraper(cookies=tiktok_cookies)
        posts = scraper.scrape(config)
        diagnostics = build_scrape_diagnostics(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

        return {
            "success": True,
            "username": username,
            "posts_found": len(posts),
            "posts": [post_to_route_response(post) for post in posts],
            "filters_applied": {
                "hashtags": _request_value(request, "hashtags"),
                "date_start": _request_value(request, "date_start").isoformat(),
                "date_end": _request_value(request, "date_end").isoformat(),
            },
            "diagnostics": diagnostics,
        }
    except HTTPException:
        raise
    except Exception as exc:
        active_logger.error("TikTok scrape failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "username": username,
            "posts_found": 0,
            "posts": [],
            "filters_applied": {},
            "error": str(exc),
        }


def preview_tiktok_profile(
    username: str,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Preview TikTok profile metadata and return the current route payload."""

    active_logger = logger or globals()["logger"]

    try:
        from trr_backend.socials import tiktok as tiktok_module

        tiktok_cookies = load_cookies("preview")
        scraper = tiktok_module.TikTokScraper(cookies=tiktok_cookies)
        data = scraper.fetch_user_detail(username, delay=0)

        if not data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        user_data, stats = extract_profile_preview_sections(data)
        if not user_data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        return profile_preview_to_route_response(user_data, stats)
    except HTTPException:
        raise
    except Exception as exc:
        active_logger.error("TikTok preview failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
