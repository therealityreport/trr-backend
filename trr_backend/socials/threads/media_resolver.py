"""Threads media URL resolver for media mirroring.

Follows the same pattern as youtube/media_resolver.py and tiktok/media_resolver.py.
Threads media URLs are directly available from the GraphQL API response, so
this resolver primarily handles URL selection (best quality) and validation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class ThreadsMediaResolution:
    """Result of resolving media URLs for a Threads post."""

    source: str | None = None
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    media_type: str = "unknown"
    attempts: list[dict[str, Any]] = field(default_factory=list)


def _create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _probe_media_url(url: str, *, session: requests.Session, timeout: tuple[int, int] = (5, 15)) -> bool:
    """Verify a media URL is accessible via HEAD request."""
    try:
        resp = session.head(url, timeout=timeout, allow_redirects=True)
        return resp.status_code == 200
    except Exception:
        return False


def _pick_best_image(candidates: list[dict[str, Any]]) -> str | None:
    """Pick the highest-resolution image from candidates list."""
    if not candidates:
        return None
    # Candidates are typically sorted by quality; first is best
    for c in candidates:
        url = str(c.get("url") or "").strip()
        if url:
            return url
    return None


def _pick_best_video(versions: list[dict[str, Any]]) -> str | None:
    """Pick the best-quality video URL from versions list."""
    if not versions:
        return None
    # Sort by resolution (width * height) descending
    sorted_versions = sorted(
        versions,
        key=lambda v: (v.get("width") or 0) * (v.get("height") or 0),
        reverse=True,
    )
    for v in sorted_versions:
        url = str(v.get("url") or "").strip()
        if url:
            return url
    return None


def resolve_threads_media(
    post_data: dict[str, Any],
    *,
    session: requests.Session | None = None,
    validate_urls: bool = False,
    timeout: tuple[int, int] = (5, 15),
) -> ThreadsMediaResolution:
    """Resolve media URLs for a Threads post.

    Args:
        post_data: Raw post data from the GraphQL API (the ``post`` object
            inside ``thread_items[0]``).
        session: Optional requests session for URL validation.
        validate_urls: If True, probe each URL with a HEAD request.
        timeout: Timeout for validation requests.

    Returns:
        ThreadsMediaResolution with resolved URLs and attempt audit trail.
    """
    resolution = ThreadsMediaResolution()
    media_type_code = post_data.get("media_type")

    if session is None and validate_urls:
        session = _create_session()

    # Determine media type string
    if media_type_code == 1:
        resolution.media_type = "image"
    elif media_type_code == 2:
        resolution.media_type = "video"
    elif media_type_code == 19:
        resolution.media_type = "carousel"
    elif media_type_code == 8:
        resolution.media_type = "album"
    else:
        resolution.media_type = f"unknown_{media_type_code}"

    # ------------------------------------------------------------------
    # Attempt 1: Extract from GraphQL post data
    # ------------------------------------------------------------------
    attempt: dict[str, Any] = {
        "source": "threads_graphql_post_data",
        "success": False,
        "reason_code": None,
        "selected_url_count": 0,
        "error_type": None,
        "error_message": None,
    }

    try:
        urls: list[str] = []
        seen: set[str] = set()

        def _add(url: str) -> None:
            if url and url not in seen:
                seen.add(url)
                urls.append(url)

        # Video URLs
        best_video = _pick_best_video(post_data.get("video_versions") or [])
        if best_video:
            _add(best_video)

        # Image URLs (also serves as video thumbnail)
        image_candidates = (post_data.get("image_versions2") or {}).get("candidates") or []
        best_image = _pick_best_image(image_candidates)
        if best_image:
            _add(best_image)
            if not resolution.thumbnail_url:
                resolution.thumbnail_url = best_image

        # Carousel items
        for carousel_item in post_data.get("carousel_media") or []:
            item_video = _pick_best_video(carousel_item.get("video_versions") or [])
            if item_video:
                _add(item_video)
            item_image = _pick_best_image(
                (carousel_item.get("image_versions2") or {}).get("candidates") or []
            )
            if item_image:
                _add(item_image)

        if urls:
            # Optionally validate URLs
            if validate_urls and session:
                validated = []
                for url in urls:
                    if _probe_media_url(url, session=session, timeout=timeout):
                        validated.append(url)
                    else:
                        logger.debug("[threads] media URL probe failed: %s", url[:80])
                urls = validated

            if urls:
                resolution.media_urls = urls
                resolution.source = "threads_graphql_post_data"
                attempt["success"] = True
                attempt["selected_url_count"] = len(urls)
            else:
                attempt["reason_code"] = "threads_media_urls_not_accessible"
        else:
            attempt["reason_code"] = "threads_no_media_urls_in_post"

    except Exception as exc:
        attempt["error_type"] = type(exc).__name__
        attempt["error_message"] = str(exc)[:240]
        attempt["reason_code"] = "threads_media_extraction_error"

    resolution.attempts.append(attempt)

    # ------------------------------------------------------------------
    # Attempt 2: Fall back to OG image if available
    # ------------------------------------------------------------------
    if not resolution.media_urls:
        og_attempt: dict[str, Any] = {
            "source": "threads_raw_data_fallback",
            "success": False,
            "reason_code": None,
            "selected_url_count": 0,
            "error_type": None,
            "error_message": None,
        }

        raw_data = post_data.get("raw_data") or {}
        # Check if there's an og_image in raw_data (from OG-tag fallback scraping)
        thumbnail = raw_data.get("og_image") or raw_data.get("thumbnail_url")
        if thumbnail:
            resolution.media_urls = [thumbnail]
            resolution.thumbnail_url = resolution.thumbnail_url or thumbnail
            resolution.source = "threads_raw_data_fallback"
            og_attempt["success"] = True
            og_attempt["selected_url_count"] = 1
        else:
            og_attempt["reason_code"] = "threads_no_fallback_media"

        resolution.attempts.append(og_attempt)

    return resolution
