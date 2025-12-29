from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import gzip
import json
import re
import time
from typing import Mapping
from urllib.parse import quote, urlencode
import urllib.error
import urllib.request

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
