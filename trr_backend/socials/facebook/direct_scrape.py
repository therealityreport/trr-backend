"""Route-facing Facebook direct scrape orchestration.

This module intentionally keeps posts catalog ownership out of the direct admin
scrape path. Operation functions import scraper classes from the
``trr_backend.socials.facebook.scraper`` leaf so the package root can retain its
compatibility exports without creating an import cycle.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException

_LOGGER = logging.getLogger(__name__)


def _get_value(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def _timestamp_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value), tz=UTC).isoformat()


def _media_provenance_payload(raw_media_provenance: Any) -> dict[str, Any] | None:
    if hasattr(raw_media_provenance, "to_dict"):
        media_provenance = dict(raw_media_provenance.to_dict() or {})
    elif isinstance(raw_media_provenance, Mapping):
        media_provenance = dict(raw_media_provenance or {})
    else:
        media_provenance = {}

    if not media_provenance:
        return None
    return {
        "platform": str(media_provenance.get("platform") or "facebook"),
        "matched_by": str(media_provenance.get("matched_by") or "native"),
        "fallback_used": bool(media_provenance.get("fallback_used", False)),
    }


def _load_cookies(load_cookies: Callable[[str], Any], surface: str) -> Any:
    return load_cookies(surface)


def post_to_payload(post: Any) -> dict[str, Any]:
    """Build the stable Facebook post response payload used by admin routes."""

    share_details = []
    for share in _get_value(post, "share_details", []) or []:
        posted_at = _get_value(share, "posted_at")
        share_details.append(
            {
                "sharer_name": str(_get_value(share, "sharer_name", "") or ""),
                "profile_url": str(_get_value(share, "profile_url", "") or "") or None,
                "post_url": str(_get_value(share, "post_url", "") or "") or None,
                "caption_snippet": str(_get_value(share, "caption_snippet", "") or "") or None,
                "posted_at": _timestamp_to_iso(posted_at),
                "privacy_label": str(_get_value(share, "privacy_label", "") or "") or None,
                "media_preview_urls": [
                    str(url).strip()
                    for url in (_get_value(share, "media_preview_urls", []) or [])
                    if str(url or "").strip()
                ],
            }
        )

    return {
        "post_id": str(_get_value(post, "post_id", "") or ""),
        "post_type": str(_get_value(post, "post_type", "feed") or "feed"),
        "username": str(_get_value(post, "username", "") or ""),
        "caption": str(_get_value(post, "caption", "") or ""),
        "likes": int(_get_value(post, "likes", 0) or 0),
        "comments": int(_get_value(post, "comments", 0) or 0),
        "shares": int(_get_value(post, "shares", 0) or 0),
        "views": int(_get_value(post, "views", 0) or 0),
        "url": str(_get_value(post, "url", "") or ""),
        "thumbnail_url": str(_get_value(post, "thumbnail_url", "") or "") or None,
        "media_urls": [
            str(url).strip() for url in (_get_value(post, "media_urls", []) or []) if str(url or "").strip()
        ],
        "posted_at": _timestamp_to_iso(_get_value(post, "posted_at")),
        "reactions": dict(_get_value(post, "reactions", {}) or {}),
        "share_details": share_details,
        "media_provenance": _media_provenance_payload(_get_value(post, "media_provenance")),
    }


def comment_to_payload(comment: Any) -> dict[str, Any]:
    """Build the stable Facebook comment response payload used by admin routes."""

    return {
        "comment_id": str(_get_value(comment, "comment_id", "") or ""),
        "username": str(_get_value(comment, "username", "") or ""),
        "text": str(_get_value(comment, "text", "") or ""),
        "likes": int(_get_value(comment, "likes", 0) or 0),
        "created_at": _get_value(comment, "created_at"),
        "is_reply": bool(_get_value(comment, "is_reply", False)),
        "reply_count": int(_get_value(comment, "reply_count", 0) or 0),
    }


def scrape_facebook(
    request: Any,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    from trr_backend.socials.facebook.scraper import FacebookScrapeConfig, FacebookScraper

    log = logger or _LOGGER
    page_handle = _get_value(request, "page_handle")
    log.info("Facebook scrape requested for %s", page_handle)
    try:
        scraper = FacebookScraper(cookies=_load_cookies(load_cookies, "scrape"))
        config = FacebookScrapeConfig(
            page_handle=page_handle,
            date_start=_get_value(request, "date_start"),
            date_end=_get_value(request, "date_end"),
            delay_seconds=_get_value(request, "delay_seconds"),
            max_pages=_get_value(request, "max_pages"),
            include_feed=True,
            include_reels=True,
            include_photos=True,
        )
        posts = scraper.scrape(config)
        hashtags = list(_get_value(request, "hashtags", []) or [])
        keywords = list(_get_value(request, "keywords", []) or [])
        lowered_hashtags = [str(tag).strip().lower().lstrip("#") for tag in hashtags if str(tag).strip()]
        lowered_keywords = [str(keyword).strip().lower() for keyword in keywords if str(keyword).strip()]

        def _matches(post: Any) -> bool:
            text = str(_get_value(post, "caption", "") or "").lower()
            if lowered_hashtags and not any(f"#{tag}" in text for tag in lowered_hashtags):
                return False
            if lowered_keywords and not any(term in text for term in lowered_keywords):
                return False
            return True

        filtered = [post for post in posts if _matches(post)]
        date_start = _get_value(request, "date_start")
        date_end = _get_value(request, "date_end")
        return {
            "success": True,
            "page_handle": page_handle,
            "posts_found": len(filtered),
            "posts": [post_to_payload(post) for post in filtered],
            "filters_applied": {
                "hashtags": hashtags,
                "keywords": keywords,
                "date_start": date_start.isoformat() if date_start else None,
                "date_end": date_end.isoformat() if date_end else None,
            },
            "retrieval_meta": dict(_get_value(scraper, "last_retrieval_meta", {}) or {}),
            "error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("Facebook scrape failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "page_handle": page_handle,
            "posts_found": 0,
            "posts": [],
            "filters_applied": {},
            "retrieval_meta": None,
            "error": str(exc),
        }


def search_facebook_posts(
    request: Any,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    from trr_backend.socials.facebook.scraper import FacebookScraper, FacebookSearchConfig

    log = logger or _LOGGER
    query = _get_value(request, "query")
    log.info("Facebook search requested for query=%s", query)
    try:
        scraper = FacebookScraper(cookies=_load_cookies(load_cookies, "search_posts"))
        config = FacebookSearchConfig(
            search_url=_get_value(request, "search_url"),
            profile_url=_get_value(request, "profile_url"),
            query=query,
            date_start=_get_value(request, "date_start"),
            date_end=_get_value(request, "date_end"),
            max_posts=_get_value(request, "max_posts"),
            include_share_details=_get_value(request, "include_share_details"),
            include_comments=_get_value(request, "include_comments"),
            max_comments=_get_value(request, "max_comments"),
            max_shares=_get_value(request, "max_shares"),
            allow_cross_platform_media_fallback=_get_value(request, "allow_cross_platform_media_fallback"),
            delay_seconds=_get_value(request, "delay_seconds"),
        )
        posts = scraper.search_posts(config)
        return {
            "success": True,
            "query": query,
            "posts_found": len(posts),
            "posts": [post_to_payload(post) for post in posts],
            "retrieval_meta": dict(_get_value(scraper, "last_retrieval_meta", {}) or {}),
            "error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("Facebook search failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "query": query,
            "posts_found": 0,
            "posts": [],
            "retrieval_meta": None,
            "error": str(exc),
        }


def preview_facebook_page(
    page_handle: str,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    from trr_backend.socials.facebook.scraper import FacebookScrapeConfig, FacebookScraper

    log = logger or _LOGGER
    log.info("Facebook preview requested for %s", page_handle)
    try:
        scraper = FacebookScraper(cookies=_load_cookies(load_cookies, "preview"))
        posts = scraper.scrape(FacebookScrapeConfig(page_handle=page_handle, max_pages=1))
        latest = posts[0] if posts else None
        return {
            "page_handle": page_handle,
            "posts_discovered": len(posts),
            "latest_post": {
                "post_id": _get_value(latest, "post_id") if latest else None,
                "post_type": _get_value(latest, "post_type") if latest else None,
                "url": _get_value(latest, "url") if latest else None,
                "caption": _get_value(latest, "caption") if latest else None,
            },
            "retrieval_meta": dict(_get_value(scraper, "last_retrieval_meta", {}) or {}),
        }
    except Exception as exc:  # noqa: BLE001
        log.error("Facebook preview failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def scrape_facebook_post(
    request: Any,
    *,
    load_cookies: Callable[[str], Any],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    from trr_backend.socials.facebook.scraper import FacebookScraper

    log = logger or _LOGGER
    post_url = _get_value(request, "post_url")
    log.info("Facebook post scrape requested for %s", post_url)
    try:
        scraper = FacebookScraper(cookies=_load_cookies(load_cookies, "scrape_post"))
        post, comments = scraper.scrape_post(
            post_url,
            fetch_comment_list=_get_value(request, "fetch_comments"),
            max_comments=_get_value(request, "max_comments"),
            fetch_share_list=_get_value(request, "fetch_shares"),
            max_shares=_get_value(request, "max_shares"),
            allow_cross_platform_media_fallback=_get_value(request, "allow_cross_platform_media_fallback"),
        )
        if post is None:
            return {
                "success": False,
                "post": None,
                "comments": [],
                "comments_found": 0,
                "shares_found": 0,
                "error": "Failed to fetch post",
            }

        comment_payloads = [comment_to_payload(comment) for comment in comments]
        return {
            "success": True,
            "post": post_to_payload(post),
            "comments": comment_payloads,
            "comments_found": len(comment_payloads),
            "shares_found": len(_get_value(post, "share_details", []) or []),
            "error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("Facebook post scrape failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "post": None,
            "comments": [],
            "comments_found": 0,
            "shares_found": 0,
            "error": str(exc),
        }
