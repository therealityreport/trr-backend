"""
URL-based image scraper for extracting highest-resolution images from web pages.

This module provides functionality to:
1. Fetch HTML from a URL with proper headers
2. Parse HTML to extract all images
3. Resolve srcset attributes to find highest resolution versions
4. Return structured image candidates for preview/import
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Regex patterns for srcset parsing (from imdb_images.py)
_SRCSET_DESC_RE = re.compile(r"^\d+(?:\.\d+)?[wx]$")
_SIZE_RE = re.compile(r"U[XY](\d+)", re.IGNORECASE)

# Common image URL dimension patterns
_WIDTH_PARAM_RE = re.compile(r"[?&]w=(\d+)", re.IGNORECASE)
_HEIGHT_PARAM_RE = re.compile(r"[?&]h=(\d+)", re.IGNORECASE)
_RESIZE_RE = re.compile(r"/(\d+)x(\d+)/", re.IGNORECASE)
_WP_RESIZE_RE = re.compile(r"-(\d+)x(\d+)\.", re.IGNORECASE)

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

# Common thumbnail/icon patterns to skip
_SKIP_PATTERNS = [
    r"/favicon",
    r"/logo",
    r"/icon",
    r"/sprite",
    r"/widget",
    r"/button",
    r"/social",
    r"/share",
    r"/avatar",
    r"\.svg$",
    r"\.gif$",
    r"data:image",
    r"googletagmanager",
    r"analytics",
    r"tracking",
]
_SKIP_RE = re.compile("|".join(_SKIP_PATTERNS), re.IGNORECASE)

# Image file extensions for direct URL detection
_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".avif", ".bmp", ".tiff", ".tif"}

# Image content types for direct URL detection
_IMAGE_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/avif",
    "image/bmp",
    "image/tiff",
}


@dataclass
class ImageCandidate:
    """Represents an image candidate extracted from a web page."""

    id: str
    original_url: str
    best_url: str
    width: int | None = None
    height: int | None = None
    alt_text: str | None = None
    context: str | None = None
    thumbnail_url: str | None = None
    source_element: str | None = None  # 'img', 'picture', 'figure'

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "original_url": self.original_url,
            "best_url": self.best_url,
            "width": self.width,
            "height": self.height,
            "alt_text": self.alt_text,
            "context": self.context,
            "thumbnail_url": self.thumbnail_url or self.best_url,
            "source_element": self.source_element,
        }


@dataclass
class ScrapeResult:
    """Result of scraping a URL for images."""

    url: str
    page_title: str | None
    domain: str
    images: list[ImageCandidate] = field(default_factory=list)
    total_found: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "page_title": self.page_title,
            "domain": self.domain,
            "images": [img.to_dict() for img in self.images],
            "total_found": self.total_found,
            "error": self.error,
        }


def fetch_page_html(
    url: str,
    *,
    timeout: float = 30.0,
    headers: dict[str, str] | None = None,
) -> tuple[str, str | None]:
    """
    Fetch HTML content from a URL.

    Returns:
        Tuple of (html_content, page_title)
    """
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}

    resp = requests.get(url, headers=merged_headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    page_title = title_tag.get_text(strip=True) if title_tag else None

    return html, page_title


def _split_srcset(srcset: str) -> list[str]:
    """Split srcset attribute handling URLs with commas."""
    raw = srcset or ""
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    length = len(raw)

    while i < length:
        ch = raw[i]
        if ch == ",":
            j = i + 1
            while j < length and raw[j].isspace():
                j += 1
            lookahead = raw[j : j + 8].lower()
            if (
                lookahead.startswith("http://")
                or lookahead.startswith("https://")
                or lookahead.startswith("//")
            ):
                part = "".join(buf).strip()
                if part:
                    parts.append(part)
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_srcset(srcset: str) -> list[tuple[str, str | None]]:
    """Parse srcset into list of (url, descriptor) tuples."""
    candidates: list[tuple[str, str | None]] = []

    for part in _split_srcset(srcset):
        tokens = part.replace("\n", " ").split()
        if not tokens:
            continue

        url = tokens[0].strip().rstrip(",")
        if not (
            url.startswith("http://") or url.startswith("https://") or url.startswith("//")
        ):
            continue

        descriptor: str | None = None
        if len(tokens) > 1:
            candidate = tokens[1].strip().rstrip(",")
            if _SRCSET_DESC_RE.match(candidate):
                descriptor = candidate

        candidates.append((url, descriptor))

    return candidates


def _extract_width_from_url(url: str) -> int:
    """Extract width from URL patterns (UX/UY params, w param, resize patterns)."""
    # IMDb/Amazon style UX/UY params
    matches = [int(val) for val in _SIZE_RE.findall(url) if val.isdigit()]
    if matches:
        return max(matches)

    # WordPress style -WxH. pattern
    wp_match = _WP_RESIZE_RE.search(url)
    if wp_match:
        return int(wp_match.group(1))

    # URL resize path /WxH/
    resize_match = _RESIZE_RE.search(url)
    if resize_match:
        return int(resize_match.group(1))

    # Query param w=
    w_match = _WIDTH_PARAM_RE.search(url)
    if w_match:
        return int(w_match.group(1))

    return 0


def _candidate_width(url: str, descriptor: str | None) -> int:
    """Calculate width score for a srcset candidate."""
    url_score = _extract_width_from_url(url)

    if descriptor:
        if descriptor.endswith("w"):
            try:
                return int(descriptor[:-1])
            except ValueError:
                return url_score
        if descriptor.endswith("x"):
            try:
                scale = float(descriptor[:-1])
            except ValueError:
                scale = 0.0
            return url_score or int(scale * 1000)

    return url_score


def _pick_best_url(srcset: str | None, src: str | None) -> tuple[str | None, int]:
    """Select the highest resolution URL from srcset or fall back to src."""
    candidates = _parse_srcset(srcset or "")

    if candidates:
        scored: list[tuple[int, str]] = []
        for url, desc in candidates:
            scored.append((_candidate_width(url, desc), url))
        scored.sort(key=lambda item: item[0], reverse=True)

        if scored:
            return scored[0][1], scored[0][0]
        return candidates[-1][0], 0

    if src:
        return src, _extract_width_from_url(src)

    return None, 0


def _normalize_url(url: str | None, base_url: str) -> str | None:
    """Normalize a URL, handling protocol-relative and relative URLs."""
    if not url:
        return None

    trimmed = url.strip()
    if not trimmed:
        return None

    # Skip data URLs and invalid patterns
    if trimmed.startswith("data:"):
        return None

    # Protocol-relative URL
    if trimmed.startswith("//"):
        return f"https:{trimmed}"

    # Relative URL
    if not trimmed.startswith("http://") and not trimmed.startswith("https://"):
        return urljoin(base_url, trimmed)

    return trimmed


def _should_skip_url(url: str) -> bool:
    """Check if URL should be skipped (icons, logos, tracking, etc.)."""
    return bool(_SKIP_RE.search(url))


def _is_direct_image_url(url: str) -> bool:
    """
    Check if a URL appears to be a direct image URL by extension.

    This is a quick heuristic check - the actual Content-Type is verified
    when fetching the URL.
    """
    parsed = urlparse(url)
    path_lower = parsed.path.lower()

    # Check file extension (strip query params)
    for ext in _IMAGE_EXTENSIONS:
        if path_lower.endswith(ext):
            return True

    return False


def _fetch_direct_image_info(url: str, timeout: float = 30.0) -> ImageCandidate | None:
    """
    Fetch a direct image URL and return it as an ImageCandidate.

    Does a HEAD request first to verify it's an image, then returns
    the URL as a single candidate.
    """
    headers = {**_DEFAULT_HEADERS}

    try:
        # Do a HEAD request to check Content-Type without downloading
        resp = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()

        # Verify it's actually an image
        if content_type not in _IMAGE_CONTENT_TYPES:
            # Maybe the extension was misleading - not an image
            return None

        # Try to get dimensions from Content-Length (can't get actual dimensions without downloading)
        # We'll leave width/height as None since we can't determine without full download

        # Extract filename for alt text
        parsed = urlparse(url)
        path = parsed.path
        filename = path.split("/")[-1] if path else None
        if filename:
            # Remove extension for cleaner alt text
            filename = filename.rsplit(".", 1)[0] if "." in filename else filename
            # Replace common separators with spaces
            filename = filename.replace("-", " ").replace("_", " ")

        return ImageCandidate(
            id=str(uuid.uuid4()),
            original_url=url,
            best_url=url,
            width=None,
            height=None,
            alt_text=filename,
            context="Direct image URL",
            thumbnail_url=url,
            source_element="direct",
        )

    except requests.RequestException:
        return None


def _get_nearby_text(element) -> str | None:
    """Extract nearby text that might be a caption."""
    # Check for figcaption in parent figure
    parent = element.find_parent("figure")
    if parent:
        figcaption = parent.find("figcaption")
        if figcaption:
            return figcaption.get_text(strip=True)[:200]

    # Check for nearby paragraph or span
    next_sibling = element.find_next_sibling()
    if next_sibling and next_sibling.name in ("p", "span", "div"):
        text = next_sibling.get_text(strip=True)
        if text and len(text) < 300:
            return text[:200]

    return None


def _try_upgrade_wordpress_url(url: str) -> str:
    """Try to get full-size WordPress image by removing resize suffix."""
    # WordPress pattern: image-300x200.jpg -> image.jpg
    wp_match = _WP_RESIZE_RE.search(url)
    if wp_match:
        # Remove the -WxH part
        return _WP_RESIZE_RE.sub(".", url)

    # Try removing common size query params
    if "?w=" in url or "&w=" in url:
        # Remove w= param to get original size
        parsed = urlparse(url)
        if parsed.query:
            params = [p for p in parsed.query.split("&") if not p.startswith("w=")]
            new_query = "&".join(params)
            return url.split("?")[0] + ("?" + new_query if new_query else "")

    return url


def extract_images_from_html(
    html: str,
    base_url: str,
    *,
    min_width: int = 200,
    limit: int = 50,
) -> list[ImageCandidate]:
    """
    Extract image candidates from HTML content.

    Args:
        html: HTML content to parse
        base_url: Base URL for resolving relative URLs
        min_width: Minimum width (from URL/srcset) to include
        limit: Maximum number of images to return

    Returns:
        List of ImageCandidate objects
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[ImageCandidate] = []
    seen_urls: set[str] = set()

    # Find all img elements
    for img in soup.find_all("img"):
        src = img.get("src")
        srcset = img.get("srcset")
        data_src = img.get("data-src")
        data_srcset = img.get("data-srcset")

        # Try data-* attributes first (lazy loading)
        best_url, width = _pick_best_url(data_srcset, data_src)
        if not best_url:
            best_url, width = _pick_best_url(srcset, src)

        if not best_url:
            continue

        best_url = _normalize_url(best_url, base_url)
        if not best_url:
            continue

        if _should_skip_url(best_url):
            continue

        # Try to upgrade to full-size version
        upgraded_url = _try_upgrade_wordpress_url(best_url)
        if upgraded_url != best_url:
            # Check if upgraded URL has higher width
            upgraded_width = _extract_width_from_url(upgraded_url)
            if upgraded_width >= width or upgraded_width == 0:
                best_url = upgraded_url
                width = upgraded_width if upgraded_width > 0 else width

        # Skip if below minimum width (when we can determine width)
        if width > 0 and width < min_width:
            continue

        # Deduplicate by URL
        if best_url in seen_urls:
            continue
        seen_urls.add(best_url)

        # Extract metadata
        alt_text = img.get("alt") or img.get("title")
        if alt_text:
            alt_text = str(alt_text).strip()[:200]

        context = _get_nearby_text(img)

        # Get original src for thumbnail
        original_url = _normalize_url(src or data_src, base_url)
        thumbnail_url = original_url if original_url else best_url

        # Parse width/height attributes
        attr_width = img.get("width")
        attr_height = img.get("height")
        parsed_width = int(attr_width) if attr_width and str(attr_width).isdigit() else None
        parsed_height = int(attr_height) if attr_height and str(attr_height).isdigit() else None

        # Use extracted width if available, else attribute
        final_width = width if width > 0 else parsed_width
        final_height = parsed_height

        candidates.append(
            ImageCandidate(
                id=str(uuid.uuid4()),
                original_url=original_url or best_url,
                best_url=best_url,
                width=final_width,
                height=final_height,
                alt_text=alt_text,
                context=context,
                thumbnail_url=thumbnail_url,
                source_element="img",
            )
        )

        if len(candidates) >= limit:
            break

    # Also check picture elements with source tags
    for picture in soup.find_all("picture"):
        # Find the highest resolution source
        best_source_url = None
        best_source_width = 0

        for source in picture.find_all("source"):
            srcset = source.get("srcset")
            if srcset:
                url, width = _pick_best_url(srcset, None)
                if url and width > best_source_width:
                    best_source_url = url
                    best_source_width = width

        # Fall back to img inside picture
        img = picture.find("img")
        if img:
            src = img.get("src")
            srcset = img.get("srcset")
            url, width = _pick_best_url(srcset, src)
            if url and width > best_source_width:
                best_source_url = url
                best_source_width = width

        if not best_source_url:
            continue

        best_source_url = _normalize_url(best_source_url, base_url)
        if not best_source_url or _should_skip_url(best_source_url):
            continue

        if best_source_url in seen_urls:
            continue
        seen_urls.add(best_source_url)

        if best_source_width > 0 and best_source_width < min_width:
            continue

        alt_text = None
        if img:
            alt_text = img.get("alt") or img.get("title")
            if alt_text:
                alt_text = str(alt_text).strip()[:200]

        context = _get_nearby_text(picture)

        candidates.append(
            ImageCandidate(
                id=str(uuid.uuid4()),
                original_url=best_source_url,
                best_url=best_source_url,
                width=best_source_width if best_source_width > 0 else None,
                height=None,
                alt_text=alt_text,
                context=context,
                thumbnail_url=best_source_url,
                source_element="picture",
            )
        )

        if len(candidates) >= limit:
            break

    return candidates


def scrape_url_for_images(
    url: str,
    *,
    min_width: int = 200,
    limit: int = 50,
) -> ScrapeResult:
    """
    Scrape a URL for images and return structured results.

    Supports two modes:
    1. Direct image URL: If the URL points directly to an image file (e.g., .jpg, .png, .webp),
       returns that image as a single candidate.
    2. Web page URL: Parses HTML to extract all images from the page.

    Args:
        url: URL to scrape (either a web page or direct image URL)
        min_width: Minimum image width to include (only applies to web page mode)
        limit: Maximum number of images

    Returns:
        ScrapeResult with images and metadata
    """
    parsed = urlparse(url)
    domain = parsed.netloc

    try:
        # Check if this is a direct image URL first
        if _is_direct_image_url(url):
            candidate = _fetch_direct_image_info(url)
            if candidate:
                return ScrapeResult(
                    url=url,
                    page_title=None,
                    domain=domain,
                    images=[candidate],
                    total_found=1,
                    error=None,
                )
            # If HEAD request failed or wasn't an image, fall through to HTML parsing
            # (maybe the extension was misleading)

        # Standard web page scraping
        html, page_title = fetch_page_html(url)
        images = extract_images_from_html(html, url, min_width=min_width, limit=limit)

        return ScrapeResult(
            url=url,
            page_title=page_title,
            domain=domain,
            images=images,
            total_found=len(images),
            error=None,
        )
    except requests.RequestException as exc:
        return ScrapeResult(
            url=url,
            page_title=None,
            domain=domain,
            images=[],
            total_found=0,
            error=str(exc),
        )
    except Exception as exc:
        return ScrapeResult(
            url=url,
            page_title=None,
            domain=domain,
            images=[],
            total_found=0,
            error=f"Unexpected error: {exc}",
        )


def download_and_hash_image(
    url: str,
    *,
    referer: str | None = None,
    timeout: float = 30.0,
) -> tuple[bytes, str, str]:
    """
    Download an image and compute its SHA256 hash.

    Args:
        url: Image URL to download
        referer: Optional referer header
        timeout: Request timeout

    Returns:
        Tuple of (image_bytes, sha256_hash, content_type)

    Raises:
        RuntimeError: If download fails or response is empty
    """
    headers = {**_DEFAULT_HEADERS}
    if referer:
        headers["referer"] = referer

    resp = requests.get(url, headers=headers, timeout=timeout, stream=True)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "image/jpeg")
    data = resp.content

    if not data:
        raise RuntimeError("Empty image response")

    sha256 = hashlib.sha256(data).hexdigest()

    return data, sha256, content_type
