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
import ipaddress
import json
import os
import re
import socket
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlparse

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
_EONLINE_RESIZE_RE = re.compile(r"/rs_(\d+)x(\d+)-", re.IGNORECASE)
_MSN_ARTICLE_ID_RE = re.compile(r"/ar-([A-Za-z0-9]+)(?:[/?#]|$)")
_MSN_LOCALE_RE = re.compile(r"^[a-z]{2}-[a-z]{2}$", re.IGNORECASE)
_OFFICIAL_BIO_SUFFIX_RE = re.compile(r"[’']s\s+official\s+bio\b.*$", re.IGNORECASE)
_OFFICIAL_BIO_PHRASE_RE = re.compile(r"\bofficial\s+bio\b.*$", re.IGNORECASE)

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

# CDN hostname → origin referer domain mapping
# Some CDNs block requests without a valid referer from the origin domain
_CDN_REFERER_MAP = {
    "akns-images.eonline.com": "https://www.eonline.com/",
    "images.eonline.com": "https://www.eonline.com/",
}

# Image content types for direct URL detection
_IMAGE_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/avif",
    "image/bmp",
    "image/tiff",
}
_IMAGE_DOWNLOAD_CHUNK_SIZE_BYTES = 64 * 1024
_LOCAL_HOSTNAMES = {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}


def _normalize_content_type(value: str | None) -> str:
    return (value or "").split(";", 1)[0].strip().lower()


def _disallowed_image_content_type(value: str | None) -> bool:
    content_type = _normalize_content_type(value)
    if not content_type:
        return False
    if content_type in {"text/html", "application/xhtml+xml", "application/json", "text/json"}:
        return True
    return content_type.endswith("+json")


def _looks_like_svg(data: bytes) -> bool:
    if not data:
        return False
    head = data.lstrip()[:4096].lower()
    if head.startswith(b"<svg"):
        return True
    return head.startswith(b"<?xml") and b"<svg" in head


def _sniff_image_content_type(data: bytes) -> str | None:
    if not data:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"
    if data.startswith(b"RIFF") and len(data) >= 12 and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"BM"):
        return "image/bmp"
    if data.startswith(b"II*\x00") or data.startswith(b"MM\x00*"):
        return "image/tiff"
    if len(data) >= 16 and data[4:8] == b"ftyp" and (b"avif" in data[8:32] or b"avis" in data[8:32]):
        return "image/avif"
    if _looks_like_svg(data):
        return "image/svg+xml"
    return None


def _looks_like_html_or_json_payload(data: bytes) -> bool:
    if not data:
        return False
    head = data.lstrip()[:512].lower()
    if not head:
        return False
    if head.startswith((b"<!doctype html", b"<html", b"<head", b"<body", b"<script")) or b"<html" in head[:128]:
        return True
    if head.startswith((b"{", b"[")):
        return True
    return False


def _is_public_ip_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value.split("%", 1)[0])
    except ValueError:
        return False
    return address.is_global


@lru_cache(maxsize=2048)
def _resolve_host_addresses(hostname: str, port: int) -> tuple[str, ...]:
    infos = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    addresses = sorted({str(info[4][0]).split("%", 1)[0] for info in infos if info and info[4]})
    return tuple(addresses)


def _public_image_url_error(value: str | None) -> str | None:
    source_url = str(value or "").strip()
    parsed = urlparse(source_url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return "invalid_source_url"

    hostname = parsed.hostname.strip().strip("[]").lower()
    if not hostname or hostname in _LOCAL_HOSTNAMES or hostname.endswith(".localhost") or hostname.endswith(".local"):
        return "private_network_url"

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address is not None:
        return None if address.is_global else "private_network_url"

    port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    try:
        addresses = _resolve_host_addresses(hostname, port)
    except (OSError, socket.gaierror):
        return "unresolvable_source_url"
    if not addresses:
        return "unresolvable_source_url"
    if any(not _is_public_ip_address(address) for address in addresses):
        return "private_network_url"
    return None


def _parse_response_content_length(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = int(str(value).strip())
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


@dataclass
class ImageCandidate:
    """Represents an image candidate extracted from a web page."""

    id: str
    original_url: str
    best_url: str
    width: int | None = None
    height: int | None = None
    bytes: int | None = None
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
            "bytes": self.bytes,
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
    page_published_at: str | None = None
    images: list[ImageCandidate] = field(default_factory=list)
    total_found: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "page_title": self.page_title,
            "page_published_at": self.page_published_at,
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


def extract_page_published_at(html: str) -> str | None:
    """
    Best-effort extraction of a page publish timestamp.

    Prefer JSON-LD `datePublished` (common for galleries/article pages),
    falling back to meta tags when present.
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD scripts (e.g. E! Online galleries expose `datePublished`).
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        def iter_nodes(obj: Any):
            if isinstance(obj, dict):
                yield obj
                graph = obj.get("@graph")
                if isinstance(graph, list):
                    for node in graph:
                        yield from iter_nodes(node)
            elif isinstance(obj, list):
                for node in obj:
                    yield from iter_nodes(node)

        for node in iter_nodes(payload):
            value = node.get("datePublished") or node.get("dateCreated") or node.get("uploadDate")
            if isinstance(value, str) and value.strip():
                return value.strip()

    # Meta tags
    meta_candidates = [
        ("property", "article:published_time"),
        ("name", "article:published_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "date"),
    ]
    for attr, key in meta_candidates:
        tag = soup.find("meta", attrs={attr: key})
        if tag and tag.get("content"):
            value = str(tag.get("content")).strip()
            if value:
                return value

    return None


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
            if lookahead.startswith("http://") or lookahead.startswith("https://") or lookahead.startswith("//"):
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
        if not (url.startswith("http://") or url.startswith("https://") or url.startswith("//")):
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

    # E! Online pattern: /rs_634x707-...
    eonline_match = _EONLINE_RESIZE_RE.search(url)
    if eonline_match:
        return int(eonline_match.group(1))

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


def _derive_referer_for_url(url: str) -> str:
    """
    Derive appropriate Referer header for a URL.

    For known CDN URLs, returns the origin domain.
    For regular URLs, returns the URL's own origin.
    """
    parsed = urlparse(url)
    hostname = parsed.netloc.lower()

    # Check known CDN mappings
    if hostname in _CDN_REFERER_MAP:
        return _CDN_REFERER_MAP[hostname]

    # Default: use the URL's own origin as referer
    return f"{parsed.scheme}://{parsed.netloc}/"


def _fetch_direct_image_info(
    url: str,
    timeout: float = 30.0,
    referer: str | None = None,
) -> ImageCandidate | None:
    """
    Fetch a direct image URL and return it as an ImageCandidate.

    Does a HEAD request first to verify it's an image, then returns
    the URL as a single candidate.
    """
    headers = {**_DEFAULT_HEADERS}
    if referer:
        headers["referer"] = referer

    try:
        # Do a HEAD request to check Content-Type without downloading
        resp = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()

        # Verify it's actually an image
        if content_type not in _IMAGE_CONTENT_TYPES:
            # Maybe the extension was misleading - not an image
            return None

        # Content-Length is useful for preview display, even if we can't infer dimensions.
        content_length = _parse_content_length(resp.headers.get("Content-Length"))

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
            bytes=content_length,
            alt_text=filename,
            context="Direct image URL",
            thumbnail_url=url,
            source_element="direct",
        )

    except requests.RequestException:
        return None


def _parse_content_length(value: str | None) -> int | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = int(raw)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _fetch_content_length(
    url: str,
    *,
    timeout: float = 3.0,
    referer: str | None = None,
) -> int | None:
    """
    Best-effort HEAD request to get Content-Length for display in preview.
    Returns None if not available or HEAD is blocked.
    """
    headers = {**_DEFAULT_HEADERS}
    if referer:
        headers["referer"] = referer
    try:
        resp = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        if resp.status_code >= 400:
            return None
        return _parse_content_length(resp.headers.get("Content-Length"))
    except requests.RequestException:
        return None


def _populate_candidate_content_lengths(
    candidates: list[ImageCandidate],
    *,
    max_heads: int,
    timeout_s: float,
    max_workers: int,
) -> None:
    """
    Populate candidate.bytes using best-effort HEAD requests (Content-Length).
    Mutates the candidates list in place.
    """
    if not candidates or max_heads <= 0:
        return

    targets = [c for c in candidates if c.bytes is None][:max_heads]
    if not targets:
        return

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        future_map = {
            executor.submit(
                _fetch_content_length,
                candidate.best_url,
                timeout=timeout_s,
                referer=_derive_referer_for_url(candidate.best_url),
            ): candidate
            for candidate in targets
        }

        for future in as_completed(future_map):
            candidate = future_map[future]
            try:
                candidate.bytes = future.result()
            except Exception:
                candidate.bytes = None


def _get_nearby_text(element) -> str | None:
    """Extract nearby text that might be a caption (best-effort, page-dependent)."""
    max_len = 2000

    # Check for figcaption in parent figure
    parent = element.find_parent("figure")
    if parent:
        sibling_context = _extract_sibling_context(parent)
        title_text = None
        title_tag = parent.find(["h1", "h2", "h3"])
        if title_tag:
            title_text = _normalize_heading_for_name(title_tag.get_text(separator=" ", strip=True))

        figcaption = parent.find("figcaption")
        if figcaption:
            caption_text = _normalize_text(figcaption.get_text(separator="\n", strip=True))
            parts = [p for p in (title_text, caption_text) if p]
            combined = "\n".join(parts).strip()
            if sibling_context and len(sibling_context) > len(combined) + 20:
                return sibling_context[:max_len]
            return combined[:max_len] if combined else None
        if title_text:
            if sibling_context and len(sibling_context) > len(title_text) + 20:
                return sibling_context[:max_len]
            return title_text[:max_len]
        if sibling_context:
            return sibling_context[:max_len]

    # Check for nearby paragraph or span
    next_sibling = element.find_next_sibling()
    if next_sibling and next_sibling.name in ("p", "span", "div"):
        text = _normalize_text(next_sibling.get_text(separator="\n", strip=True))
        if text:
            # Prefer richer heading+bio context when available.
            probe = element
            for _ in range(8):
                richer = _extract_sibling_context(probe)
                if richer and len(richer) > len(text) + 20:
                    return richer[:max_len]
                probe = getattr(probe, "parent", None)
                if probe is None or not getattr(probe, "name", None):
                    break
            if len(text) <= max_len:
                return text

    # Article body pattern: heading + image + 1-2 bio paragraphs (e.g. Bravo cast bios).
    best_context: str | None = None
    best_score = -1
    probe = element
    for _ in range(8):
        context = _extract_sibling_context(probe)
        if context:
            score = len(context) + (200 if "\n" in context else 0)
            if score > best_score:
                best_score = score
                best_context = context
        probe = getattr(probe, "parent", None)
        if probe is None or not getattr(probe, "name", None):
            break

    if best_context:
        return best_context[:max_len]

    return None


def _normalize_text(value: str | None) -> str:
    """Collapse whitespace for stable context text."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _strip_html_text(value: str | None) -> str:
    """Convert a tiny HTML snippet into plain text."""
    return _normalize_text(BeautifulSoup(value or "", "html.parser").get_text(" ", strip=True))


def _normalize_heading_for_name(value: str | None) -> str | None:
    """Trim boilerplate from heading strings used as cast names."""
    text = _normalize_text(value)
    if not text:
        return None

    text = _OFFICIAL_BIO_SUFFIX_RE.sub("", text).strip(" \t\r\n-:")
    text = _OFFICIAL_BIO_PHRASE_RE.sub("", text).strip(" \t\r\n-:")
    return text[:200] if text else None


def _extract_sibling_context(block) -> str | None:
    """
    Extract context from nearby siblings:
    - nearest previous heading (name line)
    - next 1-2 paragraphs (bio/caption lines)
    """
    heading_text: str | None = None

    sibling = getattr(block, "previous_sibling", None)
    scan_count = 0
    while sibling is not None and scan_count < 12:
        name = getattr(sibling, "name", None)
        if name in ("h1", "h2", "h3"):
            heading_text = _normalize_heading_for_name(sibling.get_text(separator=" ", strip=True))
            break
        if name in ("img", "picture", "figure"):
            break

        if name in ("div", "section", "article"):
            nested_heading = sibling.find(["h1", "h2", "h3"])
            if nested_heading:
                heading_text = _normalize_heading_for_name(nested_heading.get_text(separator=" ", strip=True))
                break

        sibling = getattr(sibling, "previous_sibling", None)
        scan_count += 1

    paragraphs: list[str] = []
    sibling = getattr(block, "next_sibling", None)
    scan_count = 0
    while sibling is not None and scan_count < 16:
        name = getattr(sibling, "name", None)
        if name in ("h1", "h2", "h3", "img", "picture", "figure"):
            break

        candidate_text = None
        if name == "p":
            candidate_text = sibling.get_text(separator=" ", strip=True)
        elif name in ("div", "section", "article"):
            nested_p = sibling.find("p")
            if nested_p:
                candidate_text = nested_p.get_text(separator=" ", strip=True)

        text = _normalize_text(candidate_text)
        if text and not text.lower().startswith("related:"):
            paragraphs.append(text)
            if len(paragraphs) >= 2:
                break

        sibling = getattr(sibling, "next_sibling", None)
        scan_count += 1

    if not heading_text and not paragraphs:
        return None

    parts = [heading_text] if heading_text else []
    parts.extend(paragraphs)
    return "\n".join([part for part in parts if part]).strip() or None


def _is_domain(hostname: str, root: str) -> bool:
    """True when hostname is exactly root or any subdomain of root."""
    host = (hostname or "").split(":", 1)[0].lower()
    root = root.lower()
    return host == root or host.endswith(f".{root}")


def _extract_msn_article_id(url: str) -> str | None:
    """Extract MSN article id from paths like `/.../ar-AA1TfjXH`."""
    parsed = urlparse(url)
    path = parsed.path or ""
    match = _MSN_ARTICLE_ID_RE.search(path)
    if not match:
        return None
    return match.group(1)


def _extract_msn_locale(url: str) -> str:
    """Extract locale from MSN URL path, defaulting to en-us."""
    parsed = urlparse(url)
    parts = [p for p in (parsed.path or "").split("/") if p]
    if parts and _MSN_LOCALE_RE.match(parts[0]):
        return parts[0].lower()
    return "en-us"


def _safe_int(value: Any) -> int | None:
    """Best-effort integer parsing."""
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _fetch_msn_detail_payload(url: str, *, timeout: float = 30.0) -> dict[str, Any] | None:
    """
    Fetch MSN article detail payload from assets API.

    Returns None when URL doesn't contain a recognized article id.
    Raises RequestException for network/http issues.
    """
    article_id = _extract_msn_article_id(url)
    if not article_id:
        return None

    locale = _extract_msn_locale(url)
    api_url = f"https://assets.msn.com/content/view/v2/Detail/{locale}/{article_id}"

    headers = {
        **_DEFAULT_HEADERS,
        "accept": "application/json,text/plain,*/*",
    }
    response = requests.get(api_url, headers=headers, timeout=timeout)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        return None
    return payload


def _extract_msn_context_by_cms_id(body_html: str) -> dict[str, str]:
    """Map MSN `cmsId` values to `Name\\nBio` context snippets."""
    if not body_html:
        return {}

    soup = BeautifulSoup(body_html, "html.parser")
    contexts: dict[str, str] = {}

    for img in soup.find_all("img"):
        cms_id = _normalize_text(img.get("data-document-id"))
        if not cms_id:
            continue

        name: str | None = None
        heading = img.find_previous(["h1", "h2", "h3"])
        if heading:
            name = _normalize_heading_for_name(heading.get_text(separator=" ", strip=True))

        bio_lines: list[str] = []
        sibling = img.next_sibling
        scan_count = 0
        while sibling is not None and scan_count < 20:
            tag_name = getattr(sibling, "name", None)
            if tag_name in ("h1", "h2", "h3", "img"):
                break
            if tag_name == "p":
                text = _normalize_text(sibling.get_text(separator=" ", strip=True))
                if text and not text.lower().startswith("related:"):
                    bio_lines.append(text)
                    if len(bio_lines) >= 2:
                        break
            sibling = sibling.next_sibling
            scan_count += 1

        parts: list[str] = []
        if name:
            parts.append(name)
        parts.extend(bio_lines)
        if parts:
            contexts[cms_id] = "\n".join(parts)[:2000]

    return contexts


def _extract_msn_images_from_payload(
    payload: dict[str, Any],
    source_url: str,
    *,
    min_width: int,
    limit: int,
) -> list[ImageCandidate]:
    """Build image candidates from MSN detail payload."""
    resources_raw = payload.get("imageResources")
    if not isinstance(resources_raw, list):
        resources_raw = []

    resources: list[dict[str, Any]] = [item for item in resources_raw if isinstance(item, dict)]
    if not resources:
        return []

    body_html = payload.get("body")
    context_by_cms = _extract_msn_context_by_cms_id(body_html if isinstance(body_html, str) else "")
    fallback_context = "\n".join(
        [text for text in (_strip_html_text(payload.get("title")), _strip_html_text(payload.get("abstract"))) if text]
    )

    # Keep best resource (largest width) per cmsId.
    best_by_cms: dict[str, dict[str, Any]] = {}
    for resource in resources:
        cms_id = _normalize_text(str(resource.get("cmsId") or ""))
        if not cms_id:
            continue
        width = _safe_int(resource.get("width")) or 0
        existing = best_by_cms.get(cms_id)
        existing_width = _safe_int(existing.get("width")) if isinstance(existing, dict) else 0
        if not existing or width >= (existing_width or 0):
            best_by_cms[cms_id] = resource

    candidates: list[ImageCandidate] = []
    seen_urls: set[str] = set()
    used_cms_ids: set[str] = set()

    def add_candidate(resource: dict[str, Any], cms_id: str | None) -> None:
        if len(candidates) >= limit:
            return

        raw_url = resource.get("url")
        best_url = _normalize_url(str(raw_url), source_url) if raw_url else None
        if not best_url or _should_skip_url(best_url) or best_url in seen_urls:
            return

        width = _safe_int(resource.get("width"))
        if width and width < min_width:
            return

        seen_urls.add(best_url)
        if cms_id:
            used_cms_ids.add(cms_id)

        caption_text = _strip_html_text(resource.get("caption"))
        title_text = _strip_html_text(resource.get("title"))
        alt_text = (title_text or caption_text)[:200] if (title_text or caption_text) else None
        context = context_by_cms.get(cms_id or "", None) or (fallback_context or None)

        candidates.append(
            ImageCandidate(
                id=str(uuid.uuid4()),
                original_url=best_url,
                best_url=best_url,
                width=width,
                height=_safe_int(resource.get("height")),
                alt_text=alt_text,
                context=context,
                thumbnail_url=best_url,
                source_element="msn-api",
            )
        )

    # Preserve in-article order first (matches cast order in announcement pages).
    if isinstance(body_html, str) and body_html.strip():
        body_soup = BeautifulSoup(body_html, "html.parser")
        for img in body_soup.find_all("img"):
            cms_id = _normalize_text(img.get("data-document-id"))
            if not cms_id:
                continue
            resource = best_by_cms.get(cms_id)
            if resource:
                add_candidate(resource, cms_id)
            if len(candidates) >= limit:
                return candidates

    # Add any additional resources not referenced in body order.
    for resource in resources:
        cms_id = _normalize_text(str(resource.get("cmsId") or "")) or None
        if cms_id and cms_id in used_cms_ids:
            continue
        add_candidate(resource, cms_id)
        if len(candidates) >= limit:
            break

    return candidates


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
        parent_link = img.find_parent("a")
        if parent_link:
            parent_href = parent_link.get("href") or ""
            if parent_link.get("data-pin-do") == "buttonPin" or "pinterest.com/pin/create/button" in parent_href:
                # Skip Pinterest button icons embedded in share links
                continue

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

    # Extract Pinterest share-link media URLs (common on galleries like E! Online)
    for link in soup.find_all("a", href=True):
        href = link.get("href")
        if not href:
            continue

        is_pinterest_button = link.get("data-pin-do") == "buttonPin" or "pinterest.com/pin/create/button" in href
        if not is_pinterest_button:
            continue

        parsed = urlparse(href)
        query = parse_qs(parsed.query)
        media_values = query.get("media") or []
        if not media_values:
            continue

        description_values = query.get("description") or []
        description = unquote(description_values[0]) if description_values else None

        for media_url in media_values:
            normalized = _normalize_url(unquote(media_url), base_url)
            if not normalized or _should_skip_url(normalized):
                continue

            if normalized in seen_urls:
                continue
            seen_urls.add(normalized)

            width = _extract_width_from_url(normalized)
            if width > 0 and width < min_width:
                continue

            alt_text = description.strip()[:200] if description else None
            context = _get_nearby_text(link)

            candidates.append(
                ImageCandidate(
                    id=str(uuid.uuid4()),
                    original_url=normalized,
                    best_url=normalized,
                    width=width if width > 0 else None,
                    height=None,
                    alt_text=alt_text,
                    context=context,
                    thumbnail_url=normalized,
                    source_element="pinterest-link",
                )
            )

            if len(candidates) >= limit:
                break

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
            referer = _derive_referer_for_url(url)
            candidate = _fetch_direct_image_info(url, referer=referer)
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
        page_published_at = extract_page_published_at(html)
        images = extract_images_from_html(html, url, min_width=min_width, limit=limit)

        # MSN article pages render content client-side; use their detail API payload
        # to recover cast photos + bios in preview/import flows.
        if _is_domain(domain, "msn.com"):
            try:
                payload = _fetch_msn_detail_payload(url, timeout=20.0)
            except requests.RequestException:
                payload = None

            if payload:
                msn_images = _extract_msn_images_from_payload(
                    payload,
                    url,
                    min_width=min_width,
                    limit=limit,
                )
                if msn_images:
                    images = msn_images

                payload_title = _strip_html_text(payload.get("title"))
                if payload_title:
                    page_title = payload_title

                published = payload.get("publishedDateTime")
                if isinstance(published, str) and published.strip():
                    page_published_at = published.strip()

        # Best-effort: fetch Content-Length for preview display (do not fail scrape if blocked).
        try:
            max_heads = int(os.getenv("SCRAPE_PREVIEW_HEAD_MAX", "30"))
        except ValueError:
            max_heads = 30
        try:
            timeout_s = float(os.getenv("SCRAPE_PREVIEW_HEAD_TIMEOUT_S", "3"))
        except ValueError:
            timeout_s = 3.0
        try:
            max_workers = int(os.getenv("SCRAPE_PREVIEW_HEAD_WORKERS", "8"))
        except ValueError:
            max_workers = 8

        _populate_candidate_content_lengths(
            images,
            max_heads=max_heads,
            timeout_s=timeout_s,
            max_workers=max_workers,
        )

        return ScrapeResult(
            url=url,
            page_title=page_title,
            page_published_at=page_published_at,
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
    max_bytes: int = 25 * 1024 * 1024,
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
    source_url = str(url or "").strip()
    url_error = _public_image_url_error(source_url)
    if url_error:
        raise RuntimeError(url_error)
    try:
        max_bytes_limit = int(max_bytes)
    except (TypeError, ValueError):
        max_bytes_limit = 0
    if max_bytes_limit <= 0:
        raise RuntimeError("invalid_max_bytes")

    headers = {**_DEFAULT_HEADERS}
    if referer:
        headers["referer"] = referer

    resp = requests.get(source_url, headers=headers, timeout=timeout, stream=True)
    try:
        resp.raise_for_status()
        content_type = _normalize_content_type(resp.headers.get("Content-Type")) or None
        if _disallowed_image_content_type(content_type):
            raise RuntimeError("asset_wrong_content_type")
        content_length = _parse_response_content_length(resp.headers.get("Content-Length"))
        if content_length is not None and content_length > max_bytes_limit:
            raise RuntimeError("asset_too_large")

        chunks: list[bytes] = []
        size_bytes = 0
        iter_content = getattr(resp, "iter_content", None)
        if callable(iter_content):
            for chunk in iter_content(chunk_size=_IMAGE_DOWNLOAD_CHUNK_SIZE_BYTES):
                if not chunk:
                    continue
                size_bytes += len(chunk)
                if size_bytes > max_bytes_limit:
                    raise RuntimeError("asset_too_large")
                chunks.append(chunk)
        if chunks:
            data = b"".join(chunks)
        else:
            data = getattr(resp, "content", b"") or b""
            if len(data) > max_bytes_limit:
                raise RuntimeError("asset_too_large")
    finally:
        close = getattr(resp, "close", None)
        if callable(close):
            close()

    if not data:
        raise RuntimeError("Empty image response")
    if _looks_like_html_or_json_payload(data):
        raise RuntimeError("asset_wrong_content_type")

    sniffed_content_type = _sniff_image_content_type(data[:4096])
    if not sniffed_content_type:
        raise RuntimeError(f"Non-image response content-type: {content_type}")
    if content_type != sniffed_content_type:
        content_type = sniffed_content_type

    sha256 = hashlib.sha256(data).hexdigest()

    return data, sha256, content_type
