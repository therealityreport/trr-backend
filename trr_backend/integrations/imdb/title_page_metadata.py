from __future__ import annotations

import gzip
import json
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from bs4 import BeautifulSoup

_IMDB_TITLE_ID_RE = re.compile(r"^tt\d+$", re.IGNORECASE)

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

_TAG_NORMALIZATION = {
    "Dating Reality TV": "Reality TV Dating",
}


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


def fetch_imdb_title_html(
    imdb_id: str,
    *,
    timeout_seconds: float = 30.0,
    headers: Mapping[str, str] | None = None,
) -> str:
    imdb_id = str(imdb_id or "").strip()
    if not _IMDB_TITLE_ID_RE.match(imdb_id):
        raise ValueError(f"Invalid IMDb id: {imdb_id!r}")
    url = f"https://www.imdb.com/title/{quote(imdb_id)}/"
    status, html, error = fetch_html(url, timeout=timeout_seconds, headers=headers)
    if error and status is None:
        raise RuntimeError(f"IMDb request failed: {error}")
    if status != 200:
        snippet = (html or "")[:200]
        raise RuntimeError(f"IMDb request failed with HTTP {status}: {snippet}")
    return html or ""


def _clean_title(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("- IMDb"):
        text = text[: -len("- IMDb")].strip()
    return text or None


def _extract_jsonld_blocks(soup: BeautifulSoup) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = node.string or node.get_text(strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    blocks.append(item)
        elif isinstance(payload, dict):
            blocks.append(payload)
    return blocks


def _pick_primary_jsonld(blocks: list[dict[str, Any]]) -> dict[str, Any] | None:
    for block in blocks:
        kind = block.get("@type")
        if isinstance(kind, list):
            kind_values = {str(k) for k in kind}
        else:
            kind_values = {str(kind)} if kind is not None else set()
        if any(value in kind_values for value in ("TVSeries", "TVMiniSeries", "Movie", "TVSpecial", "TVEpisode")):
            return block
    return blocks[0] if blocks else None


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(_coerce_list(item))
        return out
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return []


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _normalize_tags(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        mapped = _TAG_NORMALIZATION.get(value, value)
        if mapped:
            normalized.append(mapped)
    return _dedupe_preserve_order(normalized)


def _extract_interests(soup: BeautifulSoup) -> list[str]:
    container = soup.select_one('[data-testid="interests"]')
    if not container:
        return []
    chip_texts = [
        node.get_text(" ", strip=True) for node in container.select(".ipc-chip__text") if node.get_text(strip=True)
    ]
    if not chip_texts:
        chip_texts = [
            node.get_text(" ", strip=True) for node in container.select("a, span") if node.get_text(strip=True)
        ]
    return _dedupe_preserve_order([text for text in chip_texts if text])


def _extract_plot_text(soup: BeautifulSoup) -> str | None:
    plot = soup.select_one('[data-testid^="plot"]')
    if plot is not None:
        text = plot.get_text(" ", strip=True)
        if text:
            return text
    return None


def _extract_meta_description(soup: BeautifulSoup) -> str | None:
    meta = soup.find("meta", attrs={"name": "description"})
    if meta is None:
        return None
    content = meta.get("content")
    return content.strip() if isinstance(content, str) and content.strip() else None


def _extract_canonical_url(soup: BeautifulSoup, imdb_id: str) -> str:
    link = soup.find("link", attrs={"rel": "canonical"})
    if link is not None:
        href = link.get("href")
        if isinstance(href, str) and href.strip():
            return href.strip()
    return f"https://www.imdb.com/title/{imdb_id}/"


def _extract_runtime_minutes_from_og(soup: BeautifulSoup) -> int | None:
    meta = soup.find("meta", attrs={"property": "og:description"})
    if meta is None:
        return None
    content = meta.get("content")
    if not isinstance(content, str):
        return None
    match = re.search(r"\b(\d+)m\b", content)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _extract_total_episodes(soup: BeautifulSoup) -> int | None:
    for node in soup.select('[data-testid="episodes-header"]'):
        text = node.get_text(" ", strip=True)
        match = re.search(r"Episodes\s*(\d+)", text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    for node in soup.find_all(string=re.compile(r"Episodes\s*\d+", re.IGNORECASE)):
        match = re.search(r"Episodes\s*(\d+)", str(node), re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _extract_total_seasons(soup: BeautifulSoup) -> int | None:
    for node in soup.select('[data-testid="browse-episodes-season"]'):
        text = node.get_text(" ", strip=True)
        match = re.search(r"(\d+)\s+seasons", text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    for node in soup.find_all(string=re.compile(r"\d+\s+seasons", re.IGNORECASE)):
        match = re.search(r"(\d+)\s+seasons", str(node), re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _extract_trailer(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    trailer = payload.get("trailer")
    if isinstance(trailer, list) and trailer:
        trailer = trailer[0]
    if not isinstance(trailer, Mapping):
        return None
    return {
        "url": trailer.get("url"),
        "embed_url": trailer.get("embedUrl") or trailer.get("embed_url"),
        "thumbnail_url": trailer.get("thumbnailUrl") or trailer.get("thumbnail_url"),
        "upload_date": trailer.get("uploadDate") or trailer.get("upload_date"),
        "duration": trailer.get("duration"),
    }


def parse_imdb_title_html(html: str, *, imdb_id: str) -> dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    imdb_id = str(imdb_id or "").strip()

    jsonld_blocks = _extract_jsonld_blocks(soup)
    primary = _pick_primary_jsonld(jsonld_blocks) or {}

    title = None
    name = primary.get("name")
    if isinstance(name, str) and name.strip():
        title = name.strip()
    if not title:
        title = _clean_title(soup.title.get_text(" ", strip=True) if soup.title else None)

    description = None
    desc_value = primary.get("description")
    if isinstance(desc_value, str) and desc_value.strip():
        description = desc_value.strip()
    if not description:
        description = _extract_plot_text(soup)
    if not description:
        description = _extract_meta_description(soup)

    interests_raw = _extract_interests(soup)
    tags = _normalize_tags(interests_raw)

    genres = _dedupe_preserve_order(_coerce_list(primary.get("genre")))

    keywords = []
    raw_keywords = primary.get("keywords")
    if isinstance(raw_keywords, str):
        keywords = [item.strip() for item in raw_keywords.split(",") if item.strip()]

    aggregate_rating = primary.get("aggregateRating")
    rating_value = None
    rating_count = None
    if isinstance(aggregate_rating, Mapping):
        rating_value = _coerce_float(aggregate_rating.get("ratingValue"))
        rating_count = _coerce_int(aggregate_rating.get("ratingCount"))

    image = primary.get("image")
    poster_image_url = None
    if isinstance(image, str) and image.strip():
        poster_image_url = image.strip()
    elif isinstance(image, list) and image:
        first = image[0]
        if isinstance(first, str) and first.strip():
            poster_image_url = first.strip()

    if not poster_image_url:
        og_image = soup.find("meta", attrs={"property": "og:image"})
        if og_image is not None:
            content = og_image.get("content")
            if isinstance(content, str) and content.strip():
                poster_image_url = content.strip()

    result = {
        "imdb_id": imdb_id,
        "imdb_url": _extract_canonical_url(soup, imdb_id),
        "title": title,
        "description": description,
        "tags": tags,
        "tags_raw": interests_raw,
        "genres": genres,
        "content_rating": primary.get("contentRating"),
        "keywords": keywords,
        "aggregate_rating_value": rating_value,
        "aggregate_rating_count": rating_count,
        "poster_image_url": poster_image_url,
        "date_published": primary.get("datePublished"),
        "runtime_minutes": _extract_runtime_minutes_from_og(soup),
        "trailer": _extract_trailer(primary),
        "total_episodes": _extract_total_episodes(soup),
        "total_seasons": _extract_total_seasons(soup),
    }

    return result
