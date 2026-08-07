"""PicDetective reverse image search integration.

Calls the PicDetective API to find visually matching images across the web.
Used to find larger, unwatermarked versions of Getty editorial images on
syndication sites (Glamour, Yahoo, Vogue, Daily Mail, etc.).

API: GET https://picdetective.com/api/search?url=<encoded>&search_type=exact_matches
Returns JSON with exact_matches[] containing title, link, source, thumbnail, image{src,width,height}.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

PICDETECTIVE_API_BASE = "https://picdetective.com/api"
DEFAULT_MIN_WIDTH = 1080
DEFAULT_LIMIT = 5
DEFAULT_TIMEOUT_SECONDS = 30
EXCLUDED_DOMAINS = frozenset({"gettyimages.com", "gettyimages.co.uk"})

_DEFAULT_HEADERS = {
    "accept": "application/json",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}


@dataclass(frozen=True)
class ReverseImageCandidate:
    """A candidate image found via reverse image search."""

    title: str
    source_domain: str
    page_url: str
    thumbnail_b64: str | None
    width: int | None
    height: int | None


def _extract_domain(url: str) -> str:
    """Extract clean domain from a URL, stripping www. prefix."""
    try:
        hostname = urlparse(url).hostname or ""
        return re.sub(r"^www\.", "", hostname.lower())
    except Exception:
        return ""


def _is_excluded_domain(domain: str) -> bool:
    """Check if domain is in the exclusion list (e.g., gettyimages.com)."""
    return any(domain.endswith(excluded) for excluded in EXCLUDED_DOMAINS)


def _parse_int(value: Any) -> int | None:
    """Safely parse a value to int, handling strings with commas like '2,560'."""
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if cleaned.isdigit():
            return int(cleaned)
    return None


def parse_search_response(
    data: dict[str, Any],
    *,
    min_width: int = DEFAULT_MIN_WIDTH,
    limit: int = DEFAULT_LIMIT,
    exclude_domains: frozenset[str] = EXCLUDED_DOMAINS,
) -> list[ReverseImageCandidate]:
    """Parse PicDetective API response into filtered, sorted candidates."""
    matches = data.get("exact_matches")
    if not isinstance(matches, list):
        return []

    candidates: list[ReverseImageCandidate] = []
    for match in matches:
        if not isinstance(match, dict):
            continue

        page_url = str(match.get("link") or match.get("url") or "").strip()
        if not page_url:
            continue

        domain = _extract_domain(page_url)
        if not domain or _is_excluded_domain(domain):
            continue

        raw_image = match.get("image")
        image = raw_image if isinstance(raw_image, dict) else {}
        width = _parse_int(image.get("width"))
        height = _parse_int(image.get("height"))

        if min_width and (width is None or width < min_width):
            continue

        candidates.append(
            ReverseImageCandidate(
                title=str(match.get("title") or "").strip(),
                source_domain=domain,
                page_url=page_url,
                thumbnail_b64=str(match.get("thumbnail") or "").strip() or None,
                width=width,
                height=height,
            )
        )

    candidates.sort(
        key=lambda c: (c.width or 0) * (c.height or 0),
        reverse=True,
    )
    return candidates[:limit]


def search_by_image_url(
    image_url: str,
    *,
    min_width: int = DEFAULT_MIN_WIDTH,
    limit: int = DEFAULT_LIMIT,
) -> list[ReverseImageCandidate]:
    """Search PicDetective for visually matching images.

    Args:
        image_url: The source image URL (typically a Getty preview URL with auth params).
        min_width: Minimum width in pixels to include in results.
        limit: Maximum number of candidates to return.

    Returns:
        List of ReverseImageCandidate sorted by resolution descending.
    """
    cleaned_url = str(image_url or "").strip()
    if not cleaned_url:
        return []

    api_url = f"{PICDETECTIVE_API_BASE}/search"
    try:
        response = requests.get(
            api_url,
            params={"url": cleaned_url, "search_type": "exact_matches"},
            headers=_DEFAULT_HEADERS,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        logger.warning("PicDetective search failed for %s: %s", cleaned_url[:80], exc)
        return []

    return parse_search_response(data, min_width=min_width, limit=limit)
