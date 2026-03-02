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
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlencode, urljoin, urlparse

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
_FANDOM_SPECIAL_PAGE_PREFIXES = (
    "special:",
    "file:",
    "category:",
    "template:",
    "user:",
    "help:",
    "forum:",
    "talk:",
    "message wall:",
)

_ORDINAL_SUFFIX_RE = re.compile(r"(\d+)(st|nd|rd|th)", re.IGNORECASE)
_FANDOM_DOMAIN_RE = re.compile(r"^[a-z0-9-]+(?:\.[a-z0-9-]+)*\.fandom\.com$")
_DEFAULT_FANDOM_ALLOWLIST_PATH = Path(__file__).with_name("fandom_community_allowlist.txt")


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
    file_page_url: str | None = None
    section_label: str | None = None
    width: int | None = None
    height: int | None = None


@dataclass(frozen=True)
class FandomGalleryResult:
    """Result of parsing a Fandom gallery page."""

    source: str
    url: str
    person_name: str
    images: list[FandomGalleryImage]
    error: str | None


@dataclass(frozen=True)
class FandomFileResult:
    """Result of parsing a Fandom file page."""

    url: str
    file_url: str | None
    width: int | None
    height: int | None
    mime_type: str | None
    created_at: str | None
    error: str | None


def build_real_housewives_wiki_url_from_name(name: str) -> str:
    safe_name = re.sub(r"\s+", "_", (name or "").strip())
    return f"https://real-housewives.fandom.com/wiki/{quote(safe_name)}"


def _normalize_fandom_community_domain(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    host = parsed.netloc if parsed.netloc else parsed.path
    host = host.split("/", 1)[0].strip().lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host or not _FANDOM_DOMAIN_RE.fullmatch(host):
        return None
    return host


def normalize_fandom_community_domain(value: str | None) -> str | None:
    return _normalize_fandom_community_domain(value)


@lru_cache(maxsize=16)
def _load_fandom_community_allowlist_from_path(path: str) -> tuple[str, ...]:
    allowlist_path = Path(path)
    if not allowlist_path.exists():
        return ()
    domains: list[str] = []
    for line in allowlist_path.read_text(encoding="utf-8").splitlines():
        cleaned = line.split("#", 1)[0].strip()
        if not cleaned:
            continue
        normalized = _normalize_fandom_community_domain(cleaned)
        if normalized and normalized not in domains:
            domains.append(normalized)
    return tuple(domains)


@lru_cache(maxsize=1)
def _load_fandom_community_allowlist_from_db() -> tuple[str, ...]:
    try:
        from trr_backend.db import pg
    except Exception:  # noqa: BLE001
        return ()

    try:
        rows = pg.fetch_all(
            """
            SELECT domain
            FROM core.fandom_community_allowlist
            WHERE is_active = true
            ORDER BY domain
            """
        )
    except Exception as exc:  # noqa: BLE001
        message = str(exc).lower()
        if "fandom_community_allowlist" in message and ("does not exist" in message or "undefined table" in message):
            return ()
        return ()

    domains: list[str] = []
    for row in rows:
        normalized = _normalize_fandom_community_domain(str(row.get("domain") or ""))
        if normalized and normalized not in domains:
            domains.append(normalized)
    return tuple(domains)


def refresh_fandom_community_allowlist_cache() -> None:
    _load_fandom_community_allowlist_from_path.cache_clear()
    _load_fandom_community_allowlist_from_db.cache_clear()


def load_fandom_community_allowlist_with_source(path: str | None = None) -> tuple[tuple[str, ...], str]:
    db_allowlist = _load_fandom_community_allowlist_from_db()
    if db_allowlist:
        return db_allowlist, "database"
    resolved_path = str(Path(path).resolve()) if path else str(_DEFAULT_FANDOM_ALLOWLIST_PATH.resolve())
    return _load_fandom_community_allowlist_from_path(resolved_path), "file"


def load_fandom_community_allowlist(path: str | None = None) -> tuple[str, ...]:
    allowlist, _ = load_fandom_community_allowlist_with_source(path)
    return allowlist


def build_fandom_wiki_url_from_name(name: str, community_domain: str) -> str | None:
    normalized_domain = _normalize_fandom_community_domain(community_domain)
    if not normalized_domain:
        return None
    safe_name = re.sub(r"\s+", "_", (name or "").strip())
    if not safe_name:
        return None
    return f"https://{normalized_domain}/wiki/{quote(safe_name)}"


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


def _extract_fandom_search_result_candidates_from_html(
    html: str,
    *,
    community_domain: str,
    limit: int,
) -> list[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    base_url = f"https://{community_domain}"
    candidates: list[str] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        candidate = str(urljoin(base_url, href)).strip()
        parsed = urlparse(candidate)
        host = str(parsed.hostname or "").strip().lower()
        if host != community_domain:
            continue
        path = unquote(parsed.path or "")
        if "/wiki/" not in path:
            continue
        slug = path.split("/wiki/", 1)[1].split("/", 1)[0].strip()
        if not slug:
            continue
        lower_slug = slug.casefold()
        if any(lower_slug.startswith(prefix) for prefix in _FANDOM_SPECIAL_PAGE_PREFIXES):
            continue
        normalized = f"{parsed.scheme}://{parsed.netloc}/wiki/{quote(slug, safe='()_:-')}"
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        candidates.append(normalized)
        if len(candidates) >= limit:
            break
    return candidates


def _fandom_allpages_prefix_candidates(query: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", str(query or "").strip())
    if not normalized:
        return []
    slug = normalized.replace(" ", "_")
    compact = re.sub(r"[^A-Za-z0-9_()'-]+", "", slug).strip("_")
    parts = [part for part in re.split(r"[_\s]+", normalized) if part]

    candidates: list[str] = []
    for raw in (
        slug,
        compact,
        "_".join(parts[:3]) if parts else "",
        "_".join(parts[:2]) if parts else "",
        parts[0] if parts else "",
    ):
        value = str(raw or "").strip()
        if not value:
            continue
        if value not in candidates:
            candidates.append(value)
    return candidates[:5]


def _search_fandom_allpages_candidates(
    query: str,
    *,
    community_domain: str,
    timeout_seconds: float,
    limit: int,
) -> list[str]:
    headers = {"accept": "application/json"}
    domain = _normalize_fandom_community_domain(community_domain)
    if not domain:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    for prefix in _fandom_allpages_prefix_candidates(query):
        params = {
            "action": "query",
            "list": "allpages",
            "apprefix": prefix,
            "aplimit": limit,
            "format": "json",
        }
        api_query_url = (
            f"https://{domain}/api.php?"
            f"{urlencode(params)}"
        )
        status, body, _ = fetch_html(api_query_url, timeout=timeout_seconds, headers=headers)
        if status != 200 or not body:
            continue
        try:
            payload = json.loads(body)
        except ValueError:
            continue
        query_block = payload.get("query") if isinstance(payload, dict) else None
        if not isinstance(query_block, dict):
            continue
        pages = query_block.get("allpages")
        if not isinstance(pages, list):
            continue
        for page in pages:
            if not isinstance(page, dict):
                continue
            title = str(page.get("title") or "").strip()
            if not title:
                continue
            lower_title = title.casefold()
            if any(lower_title.startswith(prefix_value) for prefix_value in _FANDOM_SPECIAL_PAGE_PREFIXES):
                continue
            candidate = build_fandom_wiki_url_from_name(title, domain)
            if not candidate:
                continue
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
            if len(candidates) >= limit:
                return candidates[:limit]
    return candidates[:limit]


def search_fandom_community_wiki_candidates(
    name: str,
    *,
    community_domain: str,
    timeout_seconds: float = 20.0,
    max_results: int = 5,
) -> list[str]:
    headers = {"accept": "application/json"}

    query = (name or "").strip()
    domain = _normalize_fandom_community_domain(community_domain)
    if not domain:
        return []
    if not query:
        return []

    limit = max(1, min(int(max_results or 1), 20))
    candidates: list[str] = []
    seen: set[str] = set()

    def add_candidate(raw_url: str | None) -> None:
        candidate = str(raw_url or "").strip()
        if not candidate:
            return
        key = candidate.lower()
        if key in seen:
            return
        seen.add(key)
        candidates.append(candidate)
        if len(candidates) > limit:
            del candidates[limit:]

    rest_url = f"https://{domain}/rest.php/v1/search"
    rest_query_url = f"{rest_url}?{urlencode({'query': query, 'limit': limit})}"
    status, body, _ = fetch_html(rest_query_url, timeout=timeout_seconds, headers=headers)
    if status == 200 and body:
        try:
            payload = json.loads(body)
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            items = payload.get("items") or payload.get("results")
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    url = item.get("url")
                    if isinstance(url, str) and url.strip():
                        add_candidate(url.strip())
                        if len(candidates) >= limit:
                            return candidates[:limit]
                        continue
                    title = item.get("title")
                    if isinstance(title, str) and title.strip():
                        add_candidate(build_fandom_wiki_url_from_name(title, domain))
                        if len(candidates) >= limit:
                            return candidates[:limit]

    api_url = f"https://{domain}/api.php"
    api_query_url = (
        f"{api_url}?"
        f"{urlencode({'action': 'query', 'list': 'search', 'srsearch': query, 'srlimit': limit, 'format': 'json'})}"
    )
    status, body, _ = fetch_html(api_query_url, timeout=timeout_seconds, headers=headers)
    if status != 200 or not body:
        return candidates[:limit]
    try:
        payload = json.loads(body)
    except ValueError:
        return candidates[:limit]

    query_block = payload.get("query") if isinstance(payload, dict) else None
    if not isinstance(query_block, dict):
        return candidates[:limit]
    results = query_block.get("search")
    if not isinstance(results, list) or not results:
        return candidates[:limit]
    for result in results:
        if not isinstance(result, dict):
            continue
        title = result.get("title")
        if isinstance(title, str) and title.strip():
            add_candidate(build_fandom_wiki_url_from_name(title, domain))
            if len(candidates) >= limit:
                break

    if len(candidates) < limit:
        special_search_url = (
            f"https://{domain}/wiki/Special:Search?"
            f"{urlencode({'scope': 'internal', 'navigationSearch': 'true', 'query': query})}"
        )
        status, body, _ = fetch_html(special_search_url, timeout=timeout_seconds, headers=headers)
        if status == 200 and body:
            for candidate in _extract_fandom_search_result_candidates_from_html(
                body,
                community_domain=domain,
                limit=limit,
            ):
                add_candidate(candidate)
                if len(candidates) >= limit:
                    break

    if len(candidates) < limit:
        for candidate in _search_fandom_allpages_candidates(
            query,
            community_domain=domain,
            timeout_seconds=timeout_seconds,
            limit=limit,
        ):
            add_candidate(candidate)
            if len(candidates) >= limit:
                break
    return candidates[:limit]


def search_fandom_community_wiki(
    name: str,
    *,
    community_domain: str,
    timeout_seconds: float = 20.0,
) -> str | None:
    candidates = search_fandom_community_wiki_candidates(
        name,
        community_domain=community_domain,
        timeout_seconds=timeout_seconds,
        max_results=1,
    )
    return candidates[0] if candidates else None


def search_allowlisted_fandom_wikis(
    name: str,
    *,
    allowlist: list[str] | tuple[str, ...] | None = None,
    timeout_seconds: float = 20.0,
    max_results: int = 3,
) -> list[str]:
    query = (name or "").strip()
    if not query:
        return []
    raw_allowlist = allowlist if allowlist is not None else load_fandom_community_allowlist()
    domains: list[str] = []
    for value in raw_allowlist:
        normalized = _normalize_fandom_community_domain(value)
        if normalized and normalized not in domains:
            domains.append(normalized)

    matches: list[str] = []
    for domain in domains:
        candidate = search_fandom_community_wiki(
            query,
            community_domain=domain,
            timeout_seconds=timeout_seconds,
        )
        if not candidate or candidate in matches:
            continue
        matches.append(candidate)
        if len(matches) >= max_results:
            break
    return matches


def search_real_housewives_wiki(
    name: str,
    *,
    timeout_seconds: float = 20.0,
) -> str | None:
    return search_fandom_community_wiki(
        name,
        community_domain="real-housewives.fandom.com",
        timeout_seconds=timeout_seconds,
    )


def is_allowlisted_fandom_domain(
    url_or_domain: str,
    *,
    allowlist: list[str] | tuple[str, ...] | None = None,
) -> bool:
    normalized = _normalize_fandom_community_domain(url_or_domain)
    if not normalized:
        parsed = urlparse(str(url_or_domain or "").strip())
        normalized = _normalize_fandom_community_domain(parsed.netloc or parsed.path)
    if not normalized:
        return False
    raw_allowlist = allowlist if allowlist is not None else load_fandom_community_allowlist()
    normalized_allowlist = {
        domain for domain in (_normalize_fandom_community_domain(item) for item in raw_allowlist) if domain
    }
    return normalized in normalized_allowlist


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
    url = thumb_url.strip()
    if not url:
        return url
    if url.startswith("//"):
        url = f"https:{url}"
    if url.lower().startswith("/wiki/file:") or url.lower().startswith("/wiki/file%3a"):
        return _fandom_file_to_special_url(url)
    if url.startswith("/"):
        url = f"https://real-housewives.fandom.com{url}"
    if "/wiki/file:" in url.lower() or "/wiki/file%3a" in url.lower():
        return _fandom_file_to_special_url(url)
    # Remove scale-to-width-down or other resize parameters
    url = re.sub(r"/scale-to-width-down/\d+", "", url)
    url = re.sub(r"/scale-to-height-down/\d+", "", url)
    url = re.sub(r"/scale-to-width/\d+", "", url)
    url = re.sub(r"/smart/width/\d+/height/\d+", "", url)
    url = re.sub(r"/window-crop/width/\d+/x-offset/\d+/y-offset/\d+/window-width/\d+/window-height/\d+", "", url)
    return url


def _normalize_fandom_url(url: str | None) -> str | None:
    if not url:
        return None
    cleaned = url.strip()
    if not cleaned:
        return None
    if cleaned.startswith("//"):
        return f"https:{cleaned}"
    if cleaned.startswith("/"):
        return f"https://real-housewives.fandom.com{cleaned}"
    return cleaned


def _looks_like_file_page_url(url: str | None) -> bool:
    if not url:
        return False
    lower = url.lower()
    return "/wiki/file:" in lower or "/wiki/file%3a" in lower


def _extract_file_page_url(node) -> str | None:
    if node is None:
        return None
    candidate = None
    if getattr(node, "name", None) == "a" and node.get("href"):
        candidate = node.get("href")
    else:
        link = node.find("a", href=True)
        if link:
            candidate = link.get("href")
        if not candidate:
            parent_link = node.find_parent("a", href=True)
            if parent_link:
                candidate = parent_link.get("href")
    if not candidate:
        data_href = node.get("data-href") if hasattr(node, "get") else None
        if data_href:
            candidate = data_href
    if not _looks_like_file_page_url(candidate):
        return None
    return _normalize_fandom_url(candidate)


def _fandom_file_to_special_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    path = parsed.path or ""
    path_lower = path.lower()
    # Check for both literal colon and URL-encoded colon (%3A)
    if "file:" not in path_lower and "file%3a" not in path_lower:
        return raw_url
    # Normalize URL-encoded colons for splitting
    normalized_path = path.replace("%3A", ":").replace("%3a", ":")
    if "File:" in normalized_path:
        file_part = normalized_path.split("File:", 1)[1].lstrip("/")
    else:
        file_part = normalized_path.split("file:", 1)[1].lstrip("/")
    if not file_part:
        return raw_url
    file_part = quote(unquote(file_part), safe="")
    return f"https://real-housewives.fandom.com/wiki/Special:FilePath/{file_part}"


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

    def _normalize_heading(text: str | None) -> str | None:
        if not text:
            return None
        cleaned = re.sub(r"\s+", " ", text).strip()
        if not cleaned:
            return None
        lowered = cleaned.lower()
        if lowered in ("gallery", "contents"):
            return None
        return cleaned

    def _find_section_label(node) -> str | None:
        heading = node.find_previous(["h2", "h3", "h4"])
        if not heading:
            return None
        return _normalize_heading(heading.get_text(" ", strip=True))

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

            section_label = _find_section_label(item)
            width = _parse_int(img.get("data-image-width") or img.get("width"))
            height = _parse_int(img.get("data-image-height") or img.get("height"))
            file_page_url = _extract_file_page_url(item) or _extract_file_page_url(img)
            images.append(
                FandomGalleryImage(
                    url=full_url,
                    thumb_url=thumb if thumb != full_url else None,
                    caption=caption,
                    source_page_url=url,
                    file_page_url=file_page_url,
                    section_label=section_label,
                    width=width,
                    height=height,
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

            section_label = _find_section_label(img)
            width = _parse_int(img.get("data-image-width") or img.get("width"))
            height = _parse_int(img.get("data-image-height") or img.get("height"))
            file_page_url = _extract_file_page_url(img)
            images.append(
                FandomGalleryImage(
                    url=full_url,
                    thumb_url=thumb if thumb != full_url else None,
                    caption=caption,
                    source_page_url=url,
                    file_page_url=file_page_url,
                    section_label=section_label,
                    width=width,
                    height=height,
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

            section_label = _find_section_label(img)
            width = _parse_int(img.get("data-image-width") or img.get("width"))
            height = _parse_int(img.get("data-image-height") or img.get("height"))
            file_page_url = _extract_file_page_url(img)
            images.append(
                FandomGalleryImage(
                    url=full_url,
                    thumb_url=thumb if thumb != full_url else None,
                    caption=caption,
                    source_page_url=url,
                    file_page_url=file_page_url,
                    section_label=section_label,
                    width=width,
                    height=height,
                )
            )

    return FandomGalleryResult(
        source="fandom",
        url=url,
        person_name=person_name,
        images=images,
        error=None,
    )


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_fandom_file_html(html: str, *, url: str) -> FandomFileResult:
    if not html:
        return FandomFileResult(
            url=url,
            file_url=None,
            width=None,
            height=None,
            mime_type=None,
            created_at=None,
            error="Empty HTML response",
        )

    soup = BeautifulSoup(html, "html.parser")

    file_url = None
    width = None
    height = None
    mime_type = None

    link = soup.select_one("div.fullMedia a") or soup.select_one("a#file")
    if link and link.get("href"):
        file_url = _normalize_fandom_url(link.get("href"))

    if not file_url:
        meta_image = soup.select_one("meta[property='og:image']")
        if meta_image and meta_image.get("content"):
            file_url = _normalize_fandom_url(meta_image.get("content"))

    meta_width = soup.select_one("meta[property='og:image:width']")
    meta_height = soup.select_one("meta[property='og:image:height']")
    width = _parse_int(meta_width.get("content") if meta_width else None)
    height = _parse_int(meta_height.get("content") if meta_height else None)

    full_media = soup.select_one("div.fullMedia") or soup.select_one("div.fullmedia")
    if full_media:
        text = full_media.get_text(" ", strip=True)
        match = re.search(r"([0-9][0-9,]*)\\s*[×x]\\s*([0-9][0-9,]*)\\s*pixels", text)
        if match and (width is None or height is None):
            width = width or _parse_int(match.group(1))
            height = height or _parse_int(match.group(2))
        mime_match = re.search(r"MIME\\s+type:\\s*([a-z0-9/+.\\-]+)", text, re.IGNORECASE)
        if mime_match:
            mime_type = mime_match.group(1).lower()

    return FandomFileResult(
        url=url,
        file_url=file_url,
        width=width,
        height=height,
        mime_type=mime_type,
        created_at=None,
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


def _extract_file_title_from_url(file_page_url: str) -> str | None:
    parsed = urlparse(file_page_url)
    path = parsed.path or ""
    if "/wiki/" in path:
        title = path.split("/wiki/")[-1]
    else:
        title = path.rsplit("/", 1)[-1]
    title = unquote(title)
    if title.lower().startswith("special:filepath"):
        name = title.split(":", 1)[-1].strip()
        if name:
            return f"File:{name}"
        return None
    if not title.lower().startswith("file:"):
        return f"File:{title}"
    return title


def _fetch_fandom_file_info_via_api(
    file_page_url: str,
    *,
    timeout_seconds: float = 20.0,
) -> tuple[dict[str, Any] | None, str | None]:
    title = _extract_file_title_from_url(file_page_url)
    if not title:
        return None, "Could not extract file title"
    parsed = urlparse(file_page_url)
    api_url = (
        f"{parsed.scheme}://{parsed.netloc}/api.php?action=query&titles={quote(title)}"
        "&prop=imageinfo&iiprop=url|size|mime|timestamp&format=json"
    )
    headers = {**_DEFAULT_HEADERS, "accept": "application/json"}
    api_req = urllib.request.Request(api_url, headers=headers)
    try:
        with urllib.request.urlopen(api_req, timeout=timeout_seconds) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        return None, f"HTTP {exc.code}: {exc.reason}"
    except urllib.error.URLError as exc:
        return None, str(exc)
    except Exception as exc:
        return None, str(exc)

    pages = data.get("query", {}).get("pages", {}) if isinstance(data, dict) else {}
    if not isinstance(pages, dict) or not pages:
        return None, "No pages in API response"
    page = next(iter(pages.values()))
    if not isinstance(page, dict):
        return None, "Invalid page data"
    infos = page.get("imageinfo")
    if not isinstance(infos, list) or not infos:
        return None, "Missing imageinfo"
    return infos[0], None


def fetch_fandom_file_metadata(
    file_page_url: str,
    *,
    timeout_seconds: float = 20.0,
) -> FandomFileResult:
    """
    Fetch and parse a Fandom file page to resolve the original image and dimensions.
    """
    api_info, api_error = _fetch_fandom_file_info_via_api(file_page_url, timeout_seconds=timeout_seconds)
    if api_info:
        return FandomFileResult(
            url=file_page_url,
            file_url=_normalize_fandom_url(api_info.get("url")),
            width=api_info.get("width") if isinstance(api_info.get("width"), int) else None,
            height=api_info.get("height") if isinstance(api_info.get("height"), int) else None,
            mime_type=api_info.get("mime") if isinstance(api_info.get("mime"), str) else None,
            created_at=api_info.get("timestamp") if isinstance(api_info.get("timestamp"), str) else None,
            error=None,
        )

    status_code, html, error = fetch_html(file_page_url, timeout=timeout_seconds)
    if html and _is_challenge_page(html):
        html, api_error = _fetch_fandom_page_via_api(file_page_url, timeout_seconds=timeout_seconds)
        if api_error and not error:
            error = api_error

    if not html:
        return FandomFileResult(
            url=file_page_url,
            file_url=None,
            width=None,
            height=None,
            mime_type=None,
            created_at=None,
            error=error or f"HTTP {status_code}" if status_code else (error or "No HTML"),
        )

    result = parse_fandom_file_html(html, url=file_page_url)
    if result.file_url is None and error:
        return FandomFileResult(
            url=file_page_url,
            file_url=None,
            width=result.width,
            height=result.height,
            mime_type=result.mime_type,
            created_at=result.created_at,
            error=error,
        )
    return result


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
