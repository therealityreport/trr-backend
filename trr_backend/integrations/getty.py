from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlencode

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
    query_params: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    cleaned = str(phrase or "").strip()
    if not cleaned:
        return []

    candidates = _search_asset_candidates_for_phrase(cleaned, limit=limit, session=session, query_params=query_params)
    candidates = _merge_grouped_event_metadata(
        cleaned,
        candidates,
        session=session,
        query_params=query_params,
        limit=limit,
    )
    total = min(len(candidates), max(0, int(limit)))
    if progress_cb:
        progress_cb(0, total, f"Getty search found {total} candidate asset pages.")
    results: list[dict[str, Any]] = []
    safe_batch_size = max(1, int(detail_batch_size or DEFAULT_DETAIL_BATCH_SIZE))
    safe_max_workers = max(1, int(detail_max_workers or DEFAULT_DETAIL_MAX_WORKERS))

    for batch_start in range(0, total, safe_batch_size):
        batch_candidates = candidates[batch_start : batch_start + safe_batch_size]
        batch_urls = [str(candidate.get("detail_url") or "").strip() for candidate in batch_candidates]
        batch_end = batch_start + len(batch_candidates)
        if progress_cb:
            progress_cb(batch_start, total, f"Fetching Getty assets {batch_start + 1}-{batch_end}/{total}...")

        with ThreadPoolExecutor(max_workers=min(safe_max_workers, len(batch_urls))) as executor:
            batch_details = list(executor.map(fetch_asset_detail, batch_urls))

        for offset, (candidate, detail_url, detail) in enumerate(
            zip(batch_candidates, batch_urls, batch_details, strict=False),
            start=1,
        ):
            index = batch_start + offset
            if not detail:
                if progress_cb:
                    progress_cb(index, total, f"Getty asset {index}/{total} did not return detail.")
                continue
            results.append(_merge_search_candidate_with_detail(candidate, detail))
            if progress_cb:
                object_name = str(detail.get("object_name") or "").strip()
                label = object_name or str(detail.get("editorial_id") or detail_url)
                progress_cb(index, total, f"Fetched Getty asset {index}/{total}: {label}")
            if len(results) >= limit:
                return results
    return results


def _merge_grouped_event_metadata(
    phrase: str,
    candidates: list[dict[str, Any]],
    *,
    session: Session | None = None,
    query_params: dict[str, str] | None = None,
    limit: int,
) -> list[dict[str, Any]]:
    if not candidates:
        return candidates

    grouped_query_params = dict(query_params or {})
    grouped_query_params["groupbyevent"] = "true"
    grouped_candidates = _search_asset_candidates_for_phrase(
        phrase,
        limit=limit,
        session=session,
        query_params=grouped_query_params,
    )
    if not grouped_candidates:
        return candidates

    by_detail_url = {
        str(candidate.get("detail_url") or "").strip(): candidate
        for candidate in grouped_candidates
        if str(candidate.get("detail_url") or "").strip()
    }
    by_editorial_id = {
        str(candidate.get("editorial_id") or "").strip(): candidate
        for candidate in grouped_candidates
        if str(candidate.get("editorial_id") or "").strip()
    }
    by_object_name = {
        str(candidate.get("object_name") or "").strip().casefold(): candidate
        for candidate in grouped_candidates
        if str(candidate.get("object_name") or "").strip()
    }

    merged_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        detail_url = str(candidate.get("detail_url") or "").strip()
        editorial_id = str(candidate.get("editorial_id") or "").strip()
        object_name = str(candidate.get("object_name") or "").strip().casefold()
        grouped_candidate = (
            by_detail_url.get(detail_url)
            or by_editorial_id.get(editorial_id)
            or by_object_name.get(object_name)
        )
        if grouped_candidate:
            merged_candidates.append(_merge_search_candidate_with_detail(grouped_candidate, candidate))
        else:
            merged_candidates.append(candidate)
    return merged_candidates


def _merge_search_candidate_with_detail(candidate: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    merged = dict(candidate)
    merged.update(detail)
    if "detail_url" not in merged or not merged["detail_url"]:
        merged["detail_url"] = candidate.get("detail_url")
    if "event_name" not in merged and candidate.get("event_name"):
        merged["event_name"] = candidate.get("event_name")
    if "event_id" not in merged and candidate.get("event_id"):
        merged["event_id"] = candidate.get("event_id")
    if "event_url_slug" not in merged and candidate.get("event_url_slug"):
        merged["event_url_slug"] = candidate.get("event_url_slug")
    if "search_title" not in merged and candidate.get("search_title"):
        merged["search_title"] = candidate.get("search_title")
    if "search_caption" not in merged and candidate.get("search_caption"):
        merged["search_caption"] = candidate.get("search_caption")
    if "grouped_image_count" not in merged and candidate.get("grouped_image_count") is not None:
        merged["grouped_image_count"] = candidate.get("grouped_image_count")
    if "getty_event_group_title" not in merged and candidate.get("event_name"):
        merged["getty_event_group_title"] = candidate.get("event_name")
    return merged


def _build_search_url(
    phrase: str,
    *,
    page: int | None = None,
    query_params: dict[str, str] | None = None,
) -> str:
    params = {
        "groupbyevent": "false",
        "family": "editorial",
        "phrase": phrase,
        "sort": "newest",
    }
    if isinstance(query_params, dict):
        for key, value in query_params.items():
            cleaned_key = str(key or "").strip()
            cleaned_value = str(value or "").strip()
            if cleaned_key and cleaned_value:
                params[cleaned_key] = cleaned_value
    if page and page > 1:
        params["page"] = str(page)
    return f"{BASE_URL}/search/2/image?{urlencode(params)}"


def _search_detail_urls(object_name: str, *, session: Session | None = None) -> list[str]:
    search_url = _build_search_url(object_name)
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


def _search_detail_urls_for_phrase(
    phrase: str,
    *,
    limit: int,
    session: Session | None = None,
    query_params: dict[str, str] | None = None,
) -> list[str]:
    return [
        str(candidate.get("detail_url") or "").strip()
        for candidate in _search_asset_candidates_for_phrase(
            phrase,
            limit=limit,
            session=session,
            query_params=query_params,
        )
    ]


def _search_asset_candidates_for_phrase(
    phrase: str,
    *,
    limit: int,
    session: Session | None = None,
    query_params: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    client = _session(session)
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()

    for page in range(1, MAX_SEARCH_PAGES + 1):
        search_url = _build_search_url(phrase, page=page, query_params=query_params)
        try:
            response = client.get(search_url, headers=_DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
        except RequestException as exc:
            logger.warning("Getty search failed for %s page=%s: %s", phrase, page, exc)
            break

        page_candidates = _extract_search_asset_candidates(response.text)
        if not page_candidates:
            page_candidates = [{"detail_url": url} for url in _extract_detail_urls_from_html(response.text)]

        page_new = 0
        for candidate in page_candidates:
            absolute = str(candidate.get("detail_url") or "").strip()
            if not absolute or absolute in seen:
                continue
            seen.add(absolute)
            deduped.append(candidate)
            page_new += 1
            if len(deduped) >= limit:
                return deduped
        if page_new == 0 or len(page_candidates) < DEFAULT_SEARCH_PAGE_SIZE:
            break
    return deduped


def _extract_detail_urls_from_html(html: str) -> list[str]:
    matches = re.findall(r'href="(/detail/(?:news-photo|photo)/[^"]+/\d+)"', html)
    return [f"{BASE_URL}{match}" for match in matches]


def _extract_search_asset_candidates(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        script_text = script.string or script.get_text()
        if not script_text:
            continue
        data_component = str(script.get("data-component") or "").strip().lower()
        script_id = str(script.get("id") or "").strip()
        if data_component == "search" or script_id.startswith("Search_"):
            candidates = _extract_search_asset_candidates_from_payload_text(script_text)
            if candidates:
                return candidates

    for script in soup.find_all("script"):
        script_text = script.string or script.get_text()
        if not script_text:
            continue
        normalized = script_text.strip()
        if '"search"' not in normalized and '"searchItems"' not in normalized and '"landingUrl"' not in normalized:
            continue
        candidates = _extract_search_asset_candidates_from_payload_text(normalized)
        if candidates:
            return candidates

    inline_payload_match = re.search(r"(\{\"search\":.+?\})\s*</script>", html, flags=re.DOTALL)
    if inline_payload_match:
        candidates = _extract_search_asset_candidates_from_payload_text(inline_payload_match.group(1))
        if candidates:
            return candidates

    return []


def _extract_search_asset_candidates_from_payload_text(payload_text: str) -> list[dict[str, Any]]:
    payload = _parse_embedded_json_payload(payload_text)
    if not isinstance(payload, dict):
        return []
    return _extract_search_asset_candidates_from_payload(payload)


def _parse_embedded_json_payload(payload_text: str) -> dict[str, Any] | None:
    normalized = str(payload_text or "").strip()
    if not normalized:
        return None
    try:
        payload = json.loads(normalized)
    except json.JSONDecodeError:
        start = normalized.find("{")
        end = normalized.rfind("}")
        if start == -1 or end <= start:
            return None
        try:
            payload = json.loads(normalized[start : end + 1])
        except json.JSONDecodeError:
            return None
    return payload if isinstance(payload, dict) else None


def _extract_search_asset_candidates_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    search_payload = payload.get("search")
    if isinstance(search_payload, dict):
        gallery = search_payload.get("gallery")
        if isinstance(gallery, dict) and isinstance(gallery.get("assets"), list):
            assets = gallery.get("assets")
        else:
            assets = search_payload.get("searchItems") or search_payload.get("assets") or search_payload.get("items")
    else:
        assets = payload.get("searchItems") or payload.get("assets") or payload.get("items")
    if not isinstance(assets, list):
        return []

    candidates: list[dict[str, Any]] = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        landing_url = str(asset.get("landingUrl") or "").strip()
        detail_url = f"{BASE_URL}{landing_url}" if landing_url.startswith("/") else landing_url
        if not detail_url:
            continue
        candidate = {
            "detail_url": detail_url,
            "event_name": str(asset.get("eventName") or "").strip() or None,
            "event_id": str(asset.get("eventId") or "").strip() or None,
            "event_url_slug": str(asset.get("eventUrlSlug") or "").strip() or None,
            "search_title": str(asset.get("title") or asset.get("shortTitle") or "").strip() or None,
            "search_caption": str(asset.get("caption") or "").strip() or None,
            "grouped_image_count": asset.get("collapsedImageCount"),
        }
        editorial_id = str(asset.get("id") or asset.get("editorialId") or asset.get("assetId") or "").strip()
        if editorial_id:
            candidate["editorial_id"] = editorial_id
        object_name = str(asset.get("objectName") or "").strip()
        if object_name:
            candidate["object_name"] = object_name
        candidates.append(candidate)
    return candidates


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
    result["event_name"] = _first_present(asset_json, "eventName", "event_name")
    result["event_id"] = _first_present(asset_json, "eventId", "event_id")
    result["event_url_slug"] = _first_present(asset_json, "eventUrlSlug", "event_url_slug")
    result["getty_event_group_title"] = result["event_name"]
    result["thumb_url"] = _first_present(asset_json, "thumbUrl")
    result["comp_url"] = _first_present(asset_json, "compUrl")
    result["preview_image_url"] = _first_present(
        asset_json,
        "downloadableCompUrl",
        "galleryHighResCompUrl",
        "highResCompUrl",
        "galleryComp1024Url",
        "compUrl",
        "mainImageUrl",
        "thumbUrl",
    )
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
