"""Direct admin scrape interface for YouTube.

This module owns the platform-local request-to-payload adapter for the
`/youtube/scrape` route. YouTube API access, Crawlee stage execution, and media
URL resolving remain distinct adapters in this package; direct scraping does
not introduce a shared Scrapling abstraction or new comment behavior.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException

_DEFAULT_LOGGER = logging.getLogger(__name__)


def video_to_payload(video: Any) -> dict[str, Any]:
    """Return the route payload shape for a scraped YouTube video."""
    return {
        "video_id": video.video_id,
        "title": video.title,
        "description": video.description[:500] if video.description else "",
        "date_time": video.date_time,
        "channel_title": video.channel_title,
        "duration": video.duration,
        "duration_seconds": video.duration_seconds,
        "views": video.views,
        "likes": video.likes,
        "comments": video.comments,
        "url": video.url,
        "thumbnail_url": video.thumbnail_url,
        "keywords_matched": video.keywords_matched,
    }


def scrape_youtube(request: Any, *, logger: logging.Logger | None = None) -> dict[str, Any]:
    """Scrape YouTube videos and return the existing route response shape."""
    from trr_backend.socials.youtube.scraper import YouTubeScrapeConfig, YouTubeScraper

    active_logger = logger or _DEFAULT_LOGGER
    config_kwargs = {
        "channel_handle": request.channel_handle,
        "keywords": request.keywords,
        "date_start": request.date_start,
        "date_end": request.date_end,
        "delay_seconds": request.delay_seconds,
        "max_results": request.max_results,
        "show_id": request.show_id,
        "season_number": request.season_number,
        "person_id": request.person_id,
    }
    for attr in ("source_type", "playlist_id", "playlist_url"):
        value = getattr(request, attr, None)
        if value:
            config_kwargs[attr] = value
    config = YouTubeScrapeConfig(
        **config_kwargs,
    )

    try:
        scraper = YouTubeScraper()
        videos = scraper.scrape(config)

        return {
            "success": True,
            "channel_handle": request.channel_handle,
            "videos_found": len(videos),
            "videos": [video_to_payload(video) for video in videos],
            "filters_applied": {
                "keywords": request.keywords,
                "date_start": request.date_start.isoformat(),
                "date_end": request.date_end.isoformat(),
            },
            "error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:
        active_logger.error("YouTube scrape failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "channel_handle": request.channel_handle,
            "videos_found": 0,
            "videos": [],
            "filters_applied": {},
            "error": str(exc),
        }
