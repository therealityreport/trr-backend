from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

PEACOCK_BASE_URL = "https://www.peacocktv.com"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_LOVE_ISLAND_USA_SEASON_8_CAST_PAGE_URL = "https://www.peacocktv.com/blog/love-island-usa-season-8-cast"

_DEFAULT_CAST_PAGE_URLS: dict[tuple[str, int | None], str] = {
    ("love-island-usa", None): DEFAULT_LOVE_ISLAND_USA_SEASON_8_CAST_PAGE_URL,
    ("love-island-usa", 8): DEFAULT_LOVE_ISLAND_USA_SEASON_8_CAST_PAGE_URL,
}
_FILENAME_STOP_TOKENS = {
    "pea",
    "lis8",
    "casaamor",
    "characterportrait",
    "titlesocial",
    "1080x1350",
    "0",
    "1",
}


def _slugify(value: str | None) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().casefold())
    return re.sub(r"-{2,}", "-", cleaned).strip("-")


def _clean_url(value: str | None, *, base_url: str) -> str | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    return urljoin(base_url, cleaned)


def full_size_image_url(value: str | None, *, base_url: str = PEACOCK_BASE_URL) -> str | None:
    cleaned = _clean_url(value, base_url=base_url)
    if not cleaned:
        return None
    return re.sub(r"(/sites/peacock/files)/styles/[^/]+/public/", r"\1/", cleaned)


def _filename_from_url(value: str | None) -> str | None:
    parsed = urlparse(str(value or "").strip())
    filename = parsed.path.rsplit("/", 1)[-1].strip()
    return filename or None


def _dimensions_from_filename(filename: str | None) -> tuple[int | None, int | None]:
    match = re.search(r"(?<!\d)(\d{3,5})x(\d{3,5})(?!\d)", str(filename or ""))
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _name_candidates_from_filename(filename: str | None) -> list[str]:
    stem = re.sub(r"\.[a-z0-9]+$", "", str(filename or "").strip(), flags=re.IGNORECASE)
    tokens = [
        token
        for token in re.split(r"[^a-z0-9]+", stem.casefold())
        if token and token not in _FILENAME_STOP_TOKENS and not token.isdigit()
    ]
    candidates: list[str] = []
    seen: set[str] = set()
    for token in reversed(tokens):
        if token in seen:
            continue
        seen.add(token)
        candidates.append(token.title())
    return candidates


def _iter_jsonld_images(soup: BeautifulSoup, *, base_url: str) -> list[str]:
    image_urls: list[str] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, Mapping):
                continue
            images = node.get("image")
            if isinstance(images, str):
                images = [images]
            if not isinstance(images, list):
                continue
            for image in images:
                cleaned = _clean_url(str(image), base_url=base_url)
                if cleaned:
                    image_urls.append(cleaned)
    return image_urls


def _iter_img_tag_rows(soup: BeautifulSoup, *, base_url: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for image in soup.find_all("img"):
        attrs = image.attrs if isinstance(image.attrs, dict) else {}
        raw_url = attrs.get("src") or attrs.get("data-src") or attrs.get("data-lazy-src")
        cleaned = _clean_url(str(raw_url), base_url=base_url)
        if not cleaned:
            continue
        rows.append(
            {
                "preview_image_url": cleaned,
                "alt_text": str(attrs.get("alt") or "").strip() or None,
            }
        )
    return rows


def _image_is_peacock_content(value: str | None) -> bool:
    parsed = urlparse(str(value or "").strip())
    return (
        parsed.netloc.endswith("peacocktv.com")
        and "/sites/peacock/files/" in parsed.path
        and bool(re.search(r"\.(jpe?g|png|webp)$", parsed.path, re.IGNORECASE))
    )


def _asset_matches_person(asset: Mapping[str, Any], person_name: str | None) -> bool:
    tokens = [token for token in re.split(r"[^a-z0-9]+", str(person_name or "").casefold()) if token]
    if not tokens:
        return True
    full_name = " ".join(tokens)
    haystack = " ".join(
        str(asset.get(key) or "") for key in ("alt_text", "caption", "file_name", "preview_image_url", "image_url")
    ).casefold()
    slug_haystack = _slugify(haystack)
    if full_name in haystack or "-".join(tokens) in slug_haystack:
        return True
    return bool(tokens[0] and re.search(rf"\b{re.escape(tokens[0])}\b", haystack))


def resolve_cast_page_url(
    *,
    show_name: str | None,
    season: int | None = None,
    page_url: str | None = None,
) -> str | None:
    explicit = str(page_url or os.environ.get("PEACOCK_CAST_PAGE_URL") or "").strip()
    if explicit:
        return explicit
    show_slug = _slugify(show_name)
    if not show_slug:
        return None
    env_key = f"PEACOCK_CAST_PAGE_URL_{show_slug.upper().replace('-', '_')}"
    if season is not None:
        season_env = os.environ.get(f"{env_key}_SEASON_{int(season)}")
        if season_env:
            return season_env.strip()
    if os.environ.get(env_key):
        return str(os.environ[env_key]).strip()
    return _DEFAULT_CAST_PAGE_URLS.get((show_slug, season)) or _DEFAULT_CAST_PAGE_URLS.get((show_slug, None))


def extract_cast_images_from_html(html: str, *, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    base_url = page_url or PEACOCK_BASE_URL
    jsonld_urls = _iter_jsonld_images(soup, base_url=base_url)
    rows_by_original_url: dict[str, dict[str, Any]] = {}

    def upsert_image(preview_url: str, *, alt_text: str | None = None, source: str) -> None:
        if not _image_is_peacock_content(preview_url):
            return
        original_url = full_size_image_url(preview_url, base_url=base_url)
        if not original_url:
            return
        filename = _filename_from_url(original_url)
        width, height = _dimensions_from_filename(filename)
        source_image_id = f"peacock-blog:{hashlib.sha1(original_url.encode('utf-8')).hexdigest()[:16]}"
        existing = rows_by_original_url.get(original_url)
        if existing:
            if alt_text and not existing.get("alt_text"):
                existing["alt_text"] = alt_text
                existing["caption"] = alt_text
            existing.setdefault("extracted_from", []).append(source)
            return
        rows_by_original_url[original_url] = {
            "source_image_id": source_image_id,
            "image_url": original_url,
            "original_image_url": original_url,
            "preview_image_url": preview_url,
            "source_url": original_url,
            "source_page_url": page_url,
            "file_name": filename,
            "width": width,
            "height": height,
            "alt_text": alt_text,
            "caption": alt_text,
            "name_candidates": _name_candidates_from_filename(filename),
            "source_provider": "Peacock",
            "source_label": "Peacock Blog",
            "source_variant": "peacock_cast_page",
            "extracted_from": [source],
        }

    for row in _iter_img_tag_rows(soup, base_url=base_url):
        upsert_image(
            str(row.get("preview_image_url") or ""),
            alt_text=str(row.get("alt_text") or "").strip() or None,
            source="img",
        )
    for image_url in jsonld_urls:
        upsert_image(image_url, source="json_ld")
    return list(rows_by_original_url.values())


def fetch_cast_images(
    *,
    page_url: str,
    client: httpx.Client | None = None,
) -> list[dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0"}
    if client is not None:
        response = client.get(page_url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS, follow_redirects=True)
        response.raise_for_status()
        return extract_cast_images_from_html(response.text, page_url=str(response.url))
    with httpx.Client(timeout=DEFAULT_TIMEOUT_SECONDS, follow_redirects=True, headers=headers) as owned_client:
        response = owned_client.get(page_url)
        response.raise_for_status()
        return extract_cast_images_from_html(response.text, page_url=str(response.url))


def collect_cast_images(
    *,
    show_name: str | None,
    season: int | None = None,
    person_name: str | None = None,
    limit: int = 100,
    page_url: str | None = None,
) -> list[dict[str, Any]]:
    resolved_page_url = resolve_cast_page_url(show_name=show_name, season=season, page_url=page_url)
    if not resolved_page_url:
        return []
    rows = fetch_cast_images(page_url=resolved_page_url)
    filtered = [row for row in rows if _asset_matches_person(row, person_name)]
    if person_name:
        for row in filtered:
            row["people_names"] = [person_name]
    return filtered[: max(1, int(limit or 1))]
