from __future__ import annotations

import gzip
import json
import re
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote, unquote, urlencode, urlparse

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from bs4 import BeautifulSoup

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

_NOT_FOUND_MARKERS = (
    "this page does not exist",
    "page does not exist",
    "there is currently no text in this page",
    "oops! we can't find this page",
    "page not found",
    "special:badtitle",
)

_ORDINAL_SUFFIX_RE = re.compile(r"(\d+)(st|nd|rd|th)", re.IGNORECASE)


@dataclass(frozen=True)
class FandomInfoboxResult:
    source: str
    url: str
    title: str | None
    full_name: str | None
    birth_date: str | None
    gender: str | None
    resides_in: str | None
    infobox: dict[str, str]


@dataclass(frozen=True)
class FandomPageFetchResult:
    url: str
    status_code: int | None
    html: str | None
    error: str | None


@dataclass(frozen=True)
class FandomSourceRecord:
    url: str
    fetched_at: str
    fields: list[str]


@dataclass(frozen=True)
class FandomGalleryImage:
    """A single image from a Fandom gallery page."""

    url: str
    thumb_url: str | None
    caption: str | None
    source_page_url: str


@dataclass(frozen=True)
class FandomGalleryResult:
    """Result of parsing a Fandom gallery page."""

    source: str
    url: str
    person_name: str
    images: list[FandomGalleryImage]
    error: str | None


def build_real_housewives_wiki_url_from_name(name: str) -> str:
    safe_name = re.sub(r"\s+", "_", (name or "").strip())
    return f"https://real-housewives.fandom.com/wiki/{quote(safe_name)}"


def _merge_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    merged = {**_DEFAULT_HEADERS, **(headers or {})}
    if not any(key.lower() == "accept-encoding" for key in merged):
        merged["accept-encoding"] = "gzip"
    return merged


def _parse_charset(content_type: str | None) -> str | None:
    if not content_type:
        return None
    match = re.search(r"charset=([^\s;]+)", content_type, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip("\"'")


def _decode_bytes(data: bytes, content_type: str | None) -> str:
    charset = _parse_charset(content_type) or "utf-8"
    try:
        return data.decode(charset)
    except (LookupError, UnicodeDecodeError):
        return data.decode("utf-8", errors="replace")


def _maybe_decompress(data: bytes, content_encoding: str | None) -> bytes:
    if content_encoding and "gzip" in content_encoding.lower():
        try:
            return gzip.decompress(data)
        except OSError:
            return data
    return data


def fetch_html(
    url: str,
    *,
    timeout: float = 15.0,
    headers: Mapping[str, str] | None = None,
) -> tuple[int | None, str | None, str | None]:
    merged_headers = _merge_headers(headers)
    if requests is not None:
        try:
            resp = requests.get(url, headers=merged_headers, timeout=timeout)
        except requests.RequestException as exc:
            return None, None, str(exc)
        data = resp.content or b""
        text = _decode_bytes(data, resp.headers.get("content-type"))
        return resp.status_code, text, None

    request = urllib.request.Request(url, headers=merged_headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            data = resp.read() or b""
            data = _maybe_decompress(data, resp.headers.get("Content-Encoding"))
            text = _decode_bytes(data, resp.headers.get("Content-Type"))
            return resp.getcode(), text, None
    except urllib.error.HTTPError as exc:
        data = exc.read() or b""
        data = _maybe_decompress(data, exc.headers.get("Content-Encoding"))
        text = _decode_bytes(data, exc.headers.get("Content-Type"))
        return exc.code, text, str(exc)
    except urllib.error.URLError as exc:
        return None, None, str(exc)


def _normalize_infobox_value(value: str) -> str:
    if not value:
        return ""
    lines = []
    for line in value.splitlines():
        cleaned = re.sub(r"\s+", " ", line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def _normalize_single_line(value: str) -> str:
    if not value:
        return ""
    parts = [re.sub(r"\s+", " ", line).strip() for line in value.splitlines() if line.strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    cleaned: list[str] = []
    last_idx = len(parts) - 1
    for idx, part in enumerate(parts):
        text = part.rstrip(",") if idx < last_idx else part
        if text:
            cleaned.append(text)
    return ", ".join(cleaned).strip()


def _normalize_birthdate(value: str) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if "(" in raw:
        raw = raw.split("(", 1)[0].strip()
    raw = _ORDINAL_SUFFIX_RE.sub(r"\1", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return None

    # Try ISO first.
    try:
        return datetime.fromisoformat(raw).date().isoformat()
    except ValueError:
        pass

    for fmt in (
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d %Y",
        "%b %d %Y",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _extract_infobox_entries(soup: BeautifulSoup) -> dict[str, str]:
    infobox = soup.select_one("aside.portable-infobox") or soup.select_one(".portable-infobox")
    if infobox is None:
        return {}

    entries: dict[str, str] = {}
    for item in infobox.select(".pi-item.pi-data"):
        key = (item.get("data-source") or "").strip()
        if not key:
            label = item.select_one(".pi-data-label")
            key = (label.get_text(" ", strip=True) if label else "").strip()
        if not key:
            continue

        value_node = item.select_one(".pi-data-value")
        if value_node is None:
            continue
        for br in value_node.find_all("br"):
            br.replace_with("\n")
        value_text = _normalize_infobox_value(value_node.get_text("\n", strip=True))
        if not value_text:
            continue

        entries[key] = value_text

    return entries


def parse_fandom_infobox_html(html: str, *, url: str) -> FandomInfoboxResult:
    soup = BeautifulSoup(html or "", "html.parser")

    title = None
    title_el = soup.select_one("span.mw-page-title-main")
    if title_el is not None:
        title = title_el.get_text(" ", strip=True) or None
    if title is None and soup.title is not None:
        title = soup.title.get_text(" ", strip=True) or None

    infobox = _extract_infobox_entries(soup)

    full_name = None
    birth_date = None
    gender = None
    resides_in = None

    for key, value in infobox.items():
        normalized_key = key.strip().casefold()
        if normalized_key == "full name":
            full_name = _normalize_single_line(value)
        elif normalized_key in {"birthdate", "birth date"}:
            birth_date = _normalize_birthdate(value)
        elif normalized_key == "gender":
            gender = _normalize_single_line(value)
        elif normalized_key == "resides in":
            resides_in = _normalize_single_line(value)

    return FandomInfoboxResult(
        source="fandom",
        url=url,
        title=title,
        full_name=full_name,
        birth_date=birth_date,
        gender=gender,
        resides_in=resides_in,
        infobox=infobox,
    )


def is_fandom_page_missing(html: str | None, status_code: int | None) -> bool:
    if status_code == 404:
        return True
    if not html:
        return True
    lower_html = html.casefold()
    if any(marker in lower_html for marker in _NOT_FOUND_MARKERS):
        return True

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    return "not found" in title.casefold()


def fetch_fandom_page(
    url: str,
    *,
    extra_headers: Mapping[str, str] | None = None,
    timeout_seconds: float = 30.0,
    max_retries: int = 2,
    backoff_seconds: float = 1.0,
) -> FandomPageFetchResult:
    last_error: str | None = None
    for attempt in range(max_retries + 1):
        status, html, error = fetch_html(url, timeout=timeout_seconds, headers=extra_headers)
        if status in {429, 503} and attempt < max_retries:
            time.sleep(backoff_seconds * (2**attempt))
            continue
        if status is None and error and attempt < max_retries:
            last_error = error
            time.sleep(backoff_seconds * (2**attempt))
            continue
        if error:
            last_error = error
        return FandomPageFetchResult(url=url, status_code=status, html=html, error=error)

    return FandomPageFetchResult(url=url, status_code=None, html=None, error=last_error)


def search_real_housewives_wiki(
    name: str,
    *,
    timeout_seconds: float = 20.0,
) -> str | None:
    headers = {"accept": "application/json"}

    query = (name or "").strip()
    if not query:
        return None

    rest_url = "https://real-housewives.fandom.com/rest.php/v1/search"
    rest_query_url = f"{rest_url}?{urlencode({'query': query, 'limit': 1})}"
    status, body, _ = fetch_html(rest_query_url, timeout=timeout_seconds, headers=headers)
    if status == 200 and body:
        try:
            payload = json.loads(body)
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            items = payload.get("items") or payload.get("results")
            if isinstance(items, list) and items:
                item = items[0]
                if isinstance(item, dict):
                    url = item.get("url")
                    if isinstance(url, str) and url.strip():
                        return url.strip()
                    title = item.get("title")
                    if isinstance(title, str) and title.strip():
                        return build_real_housewives_wiki_url_from_name(title)

    api_url = "https://real-housewives.fandom.com/api.php"
    api_query_url = f"{api_url}?{urlencode({'action': 'query', 'list': 'search', 'srsearch': query, 'format': 'json'})}"
    status, body, _ = fetch_html(api_query_url, timeout=timeout_seconds, headers=headers)
    if status != 200 or not body:
        return None
    try:
        payload = json.loads(body)
    except ValueError:
        return None

    query_block = payload.get("query") if isinstance(payload, dict) else None
    if not isinstance(query_block, dict):
        return None
    results = query_block.get("search")
    if not isinstance(results, list) or not results:
        return None

    first = results[0]
    if not isinstance(first, dict):
        return None

    title = first.get("title")
    if isinstance(title, str) and title.strip():
        return build_real_housewives_wiki_url_from_name(title)
    return None


def build_fandom_source_record(result: FandomInfoboxResult, *, fetched_at: str) -> FandomSourceRecord:
    fields = sorted(result.infobox.keys())
    return FandomSourceRecord(url=result.url, fetched_at=fetched_at, fields=fields)


def build_real_housewives_gallery_url_from_name(name: str) -> str:
    """Build a gallery page URL from a person's name."""
    safe_name = re.sub(r"\s+", "_", (name or "").strip())
    return f"https://real-housewives.fandom.com/wiki/{quote(safe_name)}/Gallery"


def _extract_full_image_url(thumb_url: str) -> str:
    """
    Convert a Fandom thumbnail URL to the full-size image URL.

    Fandom thumbnail URLs look like:
    https://static.wikia.nocookie.net/.../revision/latest/scale-to-width-down/185?cb=...

    Full URLs look like:
    https://static.wikia.nocookie.net/.../revision/latest?cb=...
    """
    if not thumb_url:
        return thumb_url
    # Remove scale-to-width-down or other resize parameters
    url = re.sub(r"/scale-to-width-down/\d+", "", thumb_url)
    url = re.sub(r"/scale-to-height-down/\d+", "", url)
    url = re.sub(r"/scale-to-width/\d+", "", url)
    url = re.sub(r"/smart/width/\d+/height/\d+", "", url)
    url = re.sub(r"/window-crop/width/\d+/x-offset/\d+/y-offset/\d+/window-width/\d+/window-height/\d+", "", url)
    return url


def parse_fandom_gallery_html(html: str, *, url: str, person_name: str) -> FandomGalleryResult:
    """
    Parse a Fandom gallery page and extract all image URLs.

    Gallery pages typically have images in:
    - .wikia-gallery elements
    - .gallery elements
    - .pi-image-collection (infobox galleries)
    - Standard gallery markup
    """
    if not html:
        return FandomGalleryResult(
            source="fandom",
            url=url,
            person_name=person_name,
            images=[],
            error="Empty HTML response",
        )

    soup = BeautifulSoup(html, "html.parser")
    images: list[FandomGalleryImage] = []
    seen_urls: set[str] = set()

    # Method 1: Look for gallery items (.wikia-gallery-item, .gallery-image-wrapper, etc.)
    gallery_selectors = [
        ".wikia-gallery-item",
        ".gallery-image-wrapper",
        ".gallerybox",
        ".image-thumbnail",
        ".lightbox-caption",
    ]

    for selector in gallery_selectors:
        for item in soup.select(selector):
            img = item.select_one("img")
            if not img:
                continue

            # Get the image URL (prefer data-src for lazy-loaded images)
            thumb = img.get("data-src") or img.get("src") or ""
            if not thumb or "data:image" in thumb:
                continue

            full_url = _extract_full_image_url(thumb)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Try to get caption
            caption = None
            caption_el = item.select_one(".lightbox-caption") or item.select_one(".gallerytext")
            if caption_el:
                caption = caption_el.get_text(" ", strip=True) or None

            images.append(
                FandomGalleryImage(
                    url=full_url,
                    thumb_url=thumb if thumb != full_url else None,
                    caption=caption,
                    source_page_url=url,
                )
            )

    # Method 2: Look for images inside gallery tags
    for gallery in soup.select(".gallery, .wikia-gallery"):
        for img in gallery.select("img"):
            thumb = img.get("data-src") or img.get("src") or ""
            if not thumb or "data:image" in thumb:
                continue

            full_url = _extract_full_image_url(thumb)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Get alt text as caption fallback
            caption = img.get("alt") or img.get("title")
            if caption and caption.lower() in ("image", "photo", "gallery"):
                caption = None

            images.append(
                FandomGalleryImage(
                    url=full_url,
                    thumb_url=thumb if thumb != full_url else None,
                    caption=caption,
                    source_page_url=url,
                )
            )

    # Method 3: Look for any article images (broader search)
    article = soup.select_one(".mw-parser-output") or soup.select_one("#mw-content-text")
    if article:
        for img in article.select("img"):
            thumb = img.get("data-src") or img.get("src") or ""
            if not thumb or "data:image" in thumb:
                continue
            # Skip tiny images (icons, etc.)
            width = img.get("width") or img.get("data-image-width")
            if width:
                try:
                    if int(width) < 100:
                        continue
                except ValueError:
                    pass

            full_url = _extract_full_image_url(thumb)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            caption = img.get("alt") or img.get("title")
            if caption and caption.lower() in ("image", "photo", "gallery"):
                caption = None

            images.append(
                FandomGalleryImage(
                    url=full_url,
                    thumb_url=thumb if thumb != full_url else None,
                    caption=caption,
                    source_page_url=url,
                )
            )

    return FandomGalleryResult(
        source="fandom",
        url=url,
        person_name=person_name,
        images=images,
        error=None,
    )


def _is_challenge_page(html: str) -> bool:
    """Check if the HTML is a Cloudflare/anti-bot challenge page."""
    if not html:
        return False
    text = html.lower()
    return (
        ("client challenge" in text and "loading-error" in text)
        or ("cloudflare" in text and "challenge" in text)
        or (len(html) < 5000 and "/_fs-ch-" in html)
    )


def _fetch_fandom_page_via_api(
    page_url: str,
    *,
    timeout_seconds: float = 30.0,
) -> tuple[str | None, str | None]:
    """
    Fetch a Fandom page using the MediaWiki API.

    Returns:
        (html, error) tuple
    """
    try:
        parsed = urlparse(page_url)
        # Extract page name from URL
        if "/wiki/" in parsed.path:
            page = parsed.path.split("/wiki/")[-1]
        else:
            page = parsed.path.rsplit("/", 1)[-1]
        page = unquote(page)

        api_url = f"{parsed.scheme}://{parsed.netloc}/api.php?action=parse&page={quote(page)}&prop=text&format=json"

        headers = {**_DEFAULT_HEADERS, "accept": "application/json"}
        api_req = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(api_req, timeout=timeout_seconds) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))

        if "error" in data:
            error_info = data.get("error", {})
            return None, error_info.get("info", "API error")

        html = data.get("parse", {}).get("text", {}).get("*", "")
        return html, None

    except urllib.error.HTTPError as exc:
        return None, f"HTTP {exc.code}: {exc.reason}"
    except urllib.error.URLError as exc:
        return None, str(exc)
    except Exception as exc:
        return None, str(exc)


def fetch_fandom_gallery(
    name: str,
    *,
    timeout_seconds: float = 30.0,
    max_retries: int = 2,
) -> FandomGalleryResult:
    """
    Fetch and parse a Fandom gallery page for a person.

    Uses the MediaWiki API to bypass anti-bot challenges, with fallback to
    direct page fetch if needed.

    Args:
        name: Person's name (e.g., "Lisa Barlow")
        timeout_seconds: Request timeout
        max_retries: Number of retries for transient errors

    Returns:
        FandomGalleryResult with extracted images
    """
    url = build_real_housewives_gallery_url_from_name(name)

    # Try MediaWiki API first (more reliable, bypasses challenges)
    html, api_error = _fetch_fandom_page_via_api(url, timeout_seconds=timeout_seconds)

    if html:
        # Check if API returned "page doesn't exist" error in the HTML
        if is_fandom_page_missing(html, 200):
            return FandomGalleryResult(
                source="fandom",
                url=url,
                person_name=name,
                images=[],
                error="Gallery page not found",
            )
        return parse_fandom_gallery_html(html, url=url, person_name=name)

    # Fallback to direct page fetch
    result = fetch_fandom_page(url, timeout_seconds=timeout_seconds, max_retries=max_retries)

    if result.error and result.status_code is None:
        # Return API error if we have one, otherwise the fetch error
        return FandomGalleryResult(
            source="fandom",
            url=url,
            person_name=name,
            images=[],
            error=api_error or result.error,
        )

    if is_fandom_page_missing(result.html, result.status_code):
        return FandomGalleryResult(
            source="fandom",
            url=url,
            person_name=name,
            images=[],
            error="Gallery page not found",
        )

    # Check if we got a challenge page
    if _is_challenge_page(result.html or ""):
        return FandomGalleryResult(
            source="fandom",
            url=url,
            person_name=name,
            images=[],
            error=api_error or "Anti-bot challenge page - API fetch also failed",
        )

    return parse_fandom_gallery_html(result.html or "", url=url, person_name=name)
