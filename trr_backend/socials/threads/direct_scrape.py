"""Direct admin scrape and preview helpers for Meta Threads.

This module owns only the direct admin scrape/preview interface. Shared-account
catalog persistence stays in ``posts_catalog`` and claimed-job Scraping stays in
``posts_scrapling``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException

_logger = logging.getLogger(__name__)


def post_to_payload(post: Any) -> dict[str, Any]:
    """Shape a Threads scraper post for the existing direct scrape response."""
    posted_at = getattr(post, "posted_at", None)
    return {
        "post_id": str(getattr(post, "post_id", "") or ""),
        "username": str(getattr(post, "username", "") or ""),
        "text": str(getattr(post, "text", "") or ""),
        "likes": int(getattr(post, "likes", 0) or 0),
        "replies": int(getattr(post, "replies", 0) or 0),
        "reposts": int(getattr(post, "reposts", 0) or 0),
        "quotes": int(getattr(post, "quotes", 0) or 0),
        "views": int(getattr(post, "views", 0) or 0),
        "url": str(getattr(post, "url", "") or ""),
        "thumbnail_url": str(getattr(post, "thumbnail_url", "") or "") or None,
        "media_urls": [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url or "").strip()],
        "posted_at": datetime.fromtimestamp(int(posted_at), tz=UTC).isoformat() if posted_at is not None else None,
    }


def _post_matches_filters(post: Any, *, hashtags: list[str], keywords: list[str]) -> bool:
    text = str(getattr(post, "text", "") or "").lower()
    if hashtags and not any(f"#{tag}" in text for tag in hashtags):
        return False
    if keywords and not any(term in text for term in keywords):
        return False
    return True


def scrape_threads(
    request: Any,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run the direct Threads profile scrape and return the stable route payload."""
    log = logger or _logger
    username = getattr(request, "username", "")

    try:
        from trr_backend.socials.threads import ThreadsScrapeConfig, ThreadsScraper

        scraper = ThreadsScraper(cookies=load_cookies("scrape"))
        config = ThreadsScrapeConfig(
            username=username,
            date_start=getattr(request, "date_start", None),
            date_end=getattr(request, "date_end", None),
            delay_seconds=getattr(request, "delay_seconds", 1.0),
            max_pages=getattr(request, "max_pages", 1),
        )
        posts = scraper.scrape(config)

        request_hashtags = list(getattr(request, "hashtags", []) or [])
        request_keywords = list(getattr(request, "keywords", []) or [])
        lowered_hashtags = [str(tag).strip().lower().lstrip("#") for tag in request_hashtags if str(tag).strip()]
        lowered_keywords = [str(keyword).strip().lower() for keyword in request_keywords if str(keyword).strip()]
        filtered = [
            post for post in posts if _post_matches_filters(post, hashtags=lowered_hashtags, keywords=lowered_keywords)
        ]

        date_start = getattr(request, "date_start", None)
        date_end = getattr(request, "date_end", None)
        return {
            "success": True,
            "username": username,
            "posts_found": len(filtered),
            "posts": [post_to_payload(post) for post in filtered],
            "filters_applied": {
                "hashtags": request_hashtags,
                "keywords": request_keywords,
                "date_start": date_start.isoformat() if date_start else None,
                "date_end": date_end.isoformat() if date_end else None,
            },
            "retrieval_meta": dict(getattr(scraper, "last_retrieval_meta", {}) or {}),
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("Threads scrape failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "username": username,
            "posts_found": 0,
            "posts": [],
            "filters_applied": {},
            "error": str(exc),
        }


def preview_threads_profile(
    username: str,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run the direct Threads preview and return the stable route payload."""
    log = logger or _logger

    try:
        from trr_backend.socials.threads import ThreadsScrapeConfig, ThreadsScraper

        scraper = ThreadsScraper(cookies=load_cookies("preview"))
        posts = scraper.scrape(ThreadsScrapeConfig(username=username, max_pages=1))
        latest = posts[0] if posts else None
        return {
            "username": username,
            "posts_discovered": len(posts),
            "latest_post": {
                "post_id": getattr(latest, "post_id", None) if latest else None,
                "url": getattr(latest, "url", None) if latest else None,
                "text": getattr(latest, "text", None) if latest else None,
            },
            "retrieval_meta": dict(getattr(scraper, "last_retrieval_meta", {}) or {}),
        }
    except Exception as exc:  # noqa: BLE001
        log.error("Threads preview failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


__all__ = ["post_to_payload", "preview_threads_profile", "scrape_threads"]
