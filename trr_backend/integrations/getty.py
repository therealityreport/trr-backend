from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import quote

from bs4 import BeautifulSoup
from requests import Session
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

GettyProgressCallback = Callable[[int, int, str], None]

BASE_URL = "https://www.gettyimages.com"
DEFAULT_TIMEOUT_SECONDS = 20
MAX_DETAIL_CANDIDATES = 6
DEFAULT_SEARCH_PAGE_SIZE = 60
MAX_SEARCH_PAGES = 10
DEFAULT_DETAIL_BATCH_SIZE = 25
DEFAULT_DETAIL_MAX_WORKERS = 8

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}


def _session(session: Session | None = None) -> Session:
    return session or Session()


def resolve_asset_by_object_name(object_name: str, *, session: Session | None = None) -> dict[str, Any] | None:
    cleaned = str(object_name or "").strip()
    if not cleaned:
        return None

    detail_urls = _search_detail_urls(cleaned, session=session)
    for detail_url in detail_urls:
        detail = fetch_asset_detail(detail_url, session=session)
        if not detail:
            continue
        parsed_name = str(detail.get("object_name") or "").strip()
        if parsed_name.casefold() == cleaned.casefold():
            return detail
    return None


def search_editorial_assets(
    phrase: str,
    *,
    limit: int = 50,
    session: Session | None = None,
    progress_cb: GettyProgressCallback | None = None,
    detail_batch_size: int = DEFAULT_DETAIL_BATCH_SIZE,
    detail_max_workers: int = DEFAULT_DETAIL_MAX_WORKERS,
) -> list[dict[str, Any]]:
    cleaned = str(phrase or "").strip()
    if not cleaned:
        return []

    detail_urls = _search_detail_urls_for_phrase(cleaned, limit=limit, session=session)
    total = min(len(detail_urls), max(0, int(limit)))
    if progress_cb:
        progress_cb(0, total, f"Getty search found {total} candidate asset pages.")
    results: list[dict[str, Any]] = []
    safe_batch_size = max(1, int(detail_batch_size or DEFAULT_DETAIL_BATCH_SIZE))
    safe_max_workers = max(1, int(detail_max_workers or DEFAULT_DETAIL_MAX_WORKERS))

    for batch_start in range(0, total, safe_batch_size):
        batch_urls = detail_urls[batch_start : batch_start + safe_batch_size]
        batch_end = batch_start + len(batch_urls)
        if progress_cb:
            progress_cb(batch_start, total, f"Fetching Getty assets {batch_start + 1}-{batch_end}/{total}...")

        with ThreadPoolExecutor(max_workers=min(safe_max_workers, len(batch_urls))) as executor:
            batch_details = list(executor.map(fetch_asset_detail, batch_urls))

        for offset, (detail_url, detail) in enumerate(zip(batch_urls, batch_details, strict=False), start=1):
            index = batch_start + offset
            if not detail:
                if progress_cb:
                    progress_cb(index, total, f"Getty asset {index}/{total} did not return detail.")
                continue
            results.append(detail)
            if progress_cb:
                object_name = str(detail.get("object_name") or "").strip()
                label = object_name or str(detail.get("editorial_id") or detail_url)
                progress_cb(index, total, f"Fetched Getty asset {index}/{total}: {label}")
            if len(results) >= limit:
                return results
    return results


def _search_detail_urls(object_name: str, *, session: Session | None = None) -> list[str]:
    query = quote(object_name)
    search_url = f"{BASE_URL}/search/2/image?family=editorial&phrase={query}&sort=newest"
    client = _session(session)
    try:
        response = client.get(search_url, headers=_DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT_SECONDS)
        response.raise_for_status()
    except RequestException as exc:
        logger.warning("Getty search failed for %s: %s", object_name, exc)
        return []

    html = response.text
    matches = re.findall(r'href="(/detail/(?:news-photo|photo)/[^"]+/\d+)"', html)
    deduped: list[str] = []
    seen: set[str] = set()
    for match in matches:
        absolute = f"{BASE_URL}{match}"
        if absolute in seen:
            continue
        seen.add(absolute)
        deduped.append(absolute)
        if len(deduped) >= MAX_DETAIL_CANDIDATES:
            break
    return deduped


def _search_detail_urls_for_phrase(phrase: str, *, limit: int, session: Session | None = None) -> list[str]:
    query = quote(phrase)
    client = _session(session)
    deduped: list[str] = []
    seen: set[str] = set()

    for page in range(1, MAX_SEARCH_PAGES + 1):
        page_suffix = "" if page == 1 else f"&page={page}"
        search_url = (
            f"{BASE_URL}/search/2/image?groupbyevent=false&family=editorial&phrase={query}&sort=newest{page_suffix}"
        )
        try:
            response = client.get(search_url, headers=_DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
        except RequestException as exc:
            logger.warning("Getty search failed for %s page=%s: %s", phrase, page, exc)
            break

        matches = re.findall(r'href="(/detail/(?:news-photo|photo)/[^"]+/\d+)"', response.text)
        page_new = 0
        for match in matches:
            absolute = f"{BASE_URL}{match}"
            if absolute in seen:
                continue
            seen.add(absolute)
            deduped.append(absolute)
            page_new += 1
            if len(deduped) >= limit:
                return deduped
        if page_new == 0 or len(matches) < DEFAULT_SEARCH_PAGE_SIZE:
            break
    return deduped


def fetch_asset_detail(detail_url: str, *, session: Session | None = None) -> dict[str, Any] | None:
    client = _session(session)
    try:
        response = client.get(detail_url, headers=_DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT_SECONDS)
        response.raise_for_status()
    except RequestException as exc:
        logger.warning("Getty detail fetch failed for %s: %s", detail_url, exc)
        return None

    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    asset_json = _extract_asset_detail_json(soup)
    object_name = _extract_object_name(html, asset_json)
    editorial_id = _extract_editorial_id(detail_url, asset_json)

    result: dict[str, Any] = {
        "detail_url": detail_url,
        "editorial_id": editorial_id,
        "object_name": object_name,
        "asset": asset_json,
        "people": _extract_specific_people(asset_json),
    }
    result["title"] = _first_present(asset_json, "title", "headline")
    result["caption"] = _first_present(asset_json, "caption", "caption_plain")
    result["credit"] = _first_present(asset_json, "credit", "creditLine")
    result["collection"] = _first_present(asset_json, "collection", "collectionName")
    result["license_type"] = _first_present(asset_json, "licenseType", "license_type")
    result["date_created"] = _first_present(asset_json, "dateCreated", "date_created")
    result["upload_date"] = _first_present(asset_json, "uploadDate", "upload_date")
    result["keywords"] = asset_json.get("keywords") if isinstance(asset_json.get("keywords"), list) else []
    return result


def _extract_asset_detail_json(soup: BeautifulSoup) -> dict[str, Any]:
    script = soup.find("script", attrs={"data-component": "AssetDetail"})
    if script is None or not script.string:
        return {}
    try:
        payload = json.loads(script.string)
    except json.JSONDecodeError:
        return {}

    asset = payload.get("asset")
    if isinstance(asset, dict):
        return asset
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_object_name(html: str, asset_json: dict[str, Any]) -> str | None:
    direct = _first_present(asset_json, "objectName", "object_name")
    if direct:
        return str(direct)
    match = re.search(r"Object name.*?>\s*([^<]+?)\s*<", html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def _extract_editorial_id(detail_url: str, asset_json: dict[str, Any]) -> str | None:
    direct = _first_present(asset_json, "editorialId", "editorial_id", "id", "assetId")
    if direct:
        return str(direct)
    match = re.search(r"/(\d+)(?:\?|$)", detail_url)
    if match:
        return match.group(1)
    return None


def _extract_specific_people(asset_json: dict[str, Any]) -> list[dict[str, Any]]:
    keywords = asset_json.get("keywords")
    if not isinstance(keywords, list):
        return []
    people: list[dict[str, Any]] = []
    for keyword in keywords:
        if not isinstance(keyword, dict):
            continue
        if str(keyword.get("type") or "").strip() != "SpecificPeople":
            continue
        people.append(
            {
                "id": keyword.get("id"),
                "text": keyword.get("text"),
                "weight": keyword.get("weight"),
                "parent_keyword": keyword.get("parent_keyword"),
            }
        )
    return people


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None
