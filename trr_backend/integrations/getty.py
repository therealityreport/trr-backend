from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from bs4 import BeautifulSoup
from requests import Session
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

GettyProgressCallback = Callable[[int, int, str], None]

BASE_URL = "https://www.gettyimages.com"
DEFAULT_TIMEOUT_SECONDS = 20
MAX_DETAIL_CANDIDATES = 6
DEFAULT_SEARCH_PAGE_SIZE = 60
MAX_SEARCH_PAGES = 100
DEFAULT_DETAIL_BATCH_SIZE = 25
DEFAULT_DETAIL_MAX_WORKERS = 8
DEFAULT_EVENT_DETAIL_SAMPLE_LIMIT = 6

_DETAIL_SECTION_STOP_MARKERS = {
    "More images from this event",
    "Similar images",
    "Related searches",
    "CONTENT",
    "SOLUTIONS",
    "TOOLS & SERVICES",
    "COMPANY",
    "Royalty-free",
    "Creative Video",
    "Editorial",
    "Archive",
    "Custom Content",
    "Creative Collections",
    "Contributor support",
    "Apply to be a contributor",
}
_DETAIL_FIELD_LABELS = {
    "Restrictions:": "restrictions",
    "Credit:": "credit_display",
    "Editorial #:": "editorial_number",
    "Collection:": "collection_display",
    "Date created:": "date_created_display",
    "Upload date:": "upload_date_display",
    "License type:": "license_type_display",
    "Release info:": "release_info",
    "Source:": "source_display",
    "Object name:": "object_name_display",
    "Max file size:": "max_file_size",
}
_PEOPLE_COUNT_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}
_PEOPLE_OVERLAY_RE = re.compile(r"\bPeople:\s*([^\n|]+)", flags=re.IGNORECASE)
_PICTURED_RE = re.compile(r"\bPictured:\s*([^.;]+)", flags=re.IGNORECASE)
_ONE_LETTER_EDIT_ALLOWED_CONTEXT_RE = re.compile(
    r"\b(watch what happens live|wwhl|episode\s+\d+|season\s+\d+)\b",
    flags=re.IGNORECASE,
)
_GETTY_PERSON_MATCH_DENYLIST = (
    {
        "person_name": "Brandi Glanville",
        "event_fragment": "Hilary Roberts Birthday Celebration And Red Songbird Foundation Launch Party",
        "title_fragment": "Brandi Glanville Photos and High-Res Pictures",
        "deny_reason": "hilary_roberts_event_false_positive",
    },
)

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
    # limit <= 0 means unlimited — process all candidates
    total = len(candidates) if limit <= 0 else min(len(candidates), max(0, int(limit)))
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
            if limit > 0 and len(results) >= limit:
                return results
    return results


def search_grouped_events(
    phrase: str,
    *,
    limit: int = 50,
    session: Session | None = None,
    progress_cb: GettyProgressCallback | None = None,
    query_params: dict[str, str] | None = None,
    person_name: str | None = None,
    person_match_required: bool = False,
    minimum_grouped_image_count: int | None = None,
    event_detail_sample_limit: int = DEFAULT_EVENT_DETAIL_SAMPLE_LIMIT,
    source_query_scope: str | None = None,
    full_scan_person_assets: bool = False,
) -> list[dict[str, Any]]:
    cleaned = str(phrase or "").strip()
    if not cleaned:
        return []

    grouped_query_params = dict(query_params or {})
    grouped_query_params["groupbyevent"] = "true"
    candidates = _search_asset_candidates_for_phrase(
        cleaned,
        limit=limit,
        session=session,
        query_params=grouped_query_params,
    )
    # limit <= 0 means unlimited — process all candidates
    total = len(candidates) if limit <= 0 else min(len(candidates), max(0, int(limit)))
    if progress_cb:
        progress_cb(0, total, f"Getty grouped-event search found {total} candidate event pages.")

    results: list[dict[str, Any]] = []
    seen_event_urls: set[str] = set()
    minimum_image_count = max(0, int(minimum_grouped_image_count or 0))

    for index, candidate in enumerate(candidates[:total], start=1):
        event_url = str(candidate.get("detail_url") or "").strip()
        if not event_url or event_url in seen_event_urls:
            continue
        seen_event_urls.add(event_url)
        if progress_cb:
            progress_cb(index - 1, total, f"Fetching Getty event {index}/{total}: {event_url}")
        detail = fetch_asset_detail(event_url, session=session)
        representative_asset = _merge_search_candidate_with_detail(candidate, detail) if detail else dict(candidate)
        matched_asset = None
        if person_name and _asset_matches_person(representative_asset, person_name):
            matched_asset = representative_asset
        grouped_event = {
            "event_url": event_url,
            "event_asset_count_scanned": 1 if representative_asset else 0,
            "representative_asset": representative_asset,
            "matched_asset": matched_asset,
            "asset_samples": [_summarize_grouped_event_asset(representative_asset)] if representative_asset else [],
        }
        merged = _merge_grouped_event_candidate_with_page(
            candidate,
            grouped_event,
            source_query_scope=source_query_scope,
        )
        if full_scan_person_assets and person_name:
            scan_result = scan_event_page_for_person(
                event_url,
                person_name=person_name,
                session=session,
                progress_cb=progress_cb,
            )
            if scan_result and scan_result.get("matched_assets"):
                merged["matched_assets_list"] = scan_result["matched_assets"]
                merged["person_image_count"] = scan_result["person_image_count"]
                merged["event_asset_count_scanned"] = scan_result["total_scanned"]
                merged["matched_asset"] = scan_result["matched_assets"][0]
                merged["representative_asset"] = scan_result["representative_asset"]
            elif scan_result:
                merged["matched_assets_list"] = []
                merged["person_image_count"] = 0
                merged["event_asset_count_scanned"] = scan_result["total_scanned"]
        if person_match_required and not merged.get("matched_asset"):
            if progress_cb:
                progress_cb(
                    index,
                    total,
                    f"Skipped Getty event {index}/{total}: no match for {person_name or 'person'}",
                )
            continue
        grouped_image_count = merged.get("grouped_image_count")
        try:
            parsed_grouped_image_count = int(grouped_image_count)
        except (TypeError, ValueError):
            parsed_grouped_image_count = 0
        if minimum_image_count and parsed_grouped_image_count < minimum_image_count:
            if progress_cb:
                progress_cb(
                    index,
                    total,
                    (
                        f"Skipped Getty event {index}/{total}: grouped image count "
                        f"{parsed_grouped_image_count} below minimum {minimum_image_count}"
                    ),
                )
            continue
        results.append(merged)
        if progress_cb:
            event_name = str(merged.get("event_name") or merged.get("search_title") or event_url)
            progress_cb(index, total, f"Fetched Getty event {index}/{total}: {event_name}")
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
            by_detail_url.get(detail_url) or by_editorial_id.get(editorial_id) or by_object_name.get(object_name)
        )
        if grouped_candidate:
            merged_candidates.append(_merge_search_candidate_with_detail(grouped_candidate, candidate))
        else:
            merged_candidates.append(candidate)
    return merged_candidates


def _merge_grouped_event_candidate_with_page(
    candidate: dict[str, Any],
    grouped_event: dict[str, Any] | None,
    *,
    source_query_scope: str | None = None,
) -> dict[str, Any]:
    merged = dict(candidate)
    if isinstance(grouped_event, dict):
        merged.update(grouped_event)
    event_url = str(candidate.get("detail_url") or "").strip()
    if event_url:
        merged["event_url"] = event_url
    if source_query_scope:
        merged["source_query_scope"] = source_query_scope
    if "representative_asset" not in merged:
        merged["representative_asset"] = None
    if "matched_asset" not in merged:
        merged["matched_asset"] = None
    return merged


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
    if "event_date" not in merged and candidate.get("event_date"):
        merged["event_date"] = candidate.get("event_date")
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


def fetch_grouped_event_page(
    event_url: str,
    *,
    session: Session | None = None,
    person_name: str | None = None,
    detail_limit: int = DEFAULT_EVENT_DETAIL_SAMPLE_LIMIT,
) -> dict[str, Any] | None:
    cleaned_url = str(event_url or "").strip()
    if not cleaned_url:
        return None
    client = _session(session)
    try:
        response = client.get(cleaned_url, headers=_DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT_SECONDS)
        response.raise_for_status()
    except RequestException as exc:
        logger.warning("Getty grouped event fetch failed for %s: %s", cleaned_url, exc)
        return None

    event_candidates = _extract_search_asset_candidates(response.text)
    representative_asset: dict[str, Any] | None = None
    matched_asset: dict[str, Any] | None = None
    scanned_assets: list[dict[str, Any]] = []
    safe_limit = max(1, int(detail_limit or DEFAULT_EVENT_DETAIL_SAMPLE_LIMIT))

    for candidate in event_candidates[:safe_limit]:
        detail_url = str(candidate.get("detail_url") or "").strip()
        if not detail_url:
            continue
        detail = fetch_asset_detail(detail_url, session=client)
        if not detail:
            continue
        merged_asset = _merge_search_candidate_with_detail(candidate, detail)
        scanned_assets.append(_summarize_grouped_event_asset(merged_asset))
        if representative_asset is None:
            representative_asset = merged_asset
        if person_name and matched_asset is None:
            match_details = describe_asset_person_match(merged_asset, person_name)
            if match_details.get("matched"):
                matched_asset = {**merged_asset, "person_match": match_details}

    return {
        "event_url": cleaned_url,
        "event_asset_count_scanned": len(scanned_assets),
        "representative_asset": matched_asset or representative_asset,
        "matched_asset": matched_asset,
        "asset_samples": scanned_assets,
    }


DEFAULT_EVENT_SCAN_LIMIT = 200


def _parse_event_url(event_url: str) -> tuple[str, dict[str, str]]:
    """Extract a search phrase and query params from a Getty event page URL.

    Event URLs look like:
        https://www.gettyimages.com/photos/bravocon-2023?eventid=99999

    Returns (phrase, query_params) suitable for ``_search_asset_candidates_for_phrase``.
    """
    parsed = urlparse(event_url)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    phrase = path_parts[-1].replace("-", " ") if path_parts else ""
    qs = parse_qs(parsed.query)
    query_params: dict[str, str] = {}
    for key, values in qs.items():
        cleaned_key = str(key).strip().lower()
        if cleaned_key and values:
            query_params[cleaned_key] = str(values[0]).strip()
    return phrase, query_params


def scan_event_page_for_person(
    event_url: str,
    *,
    person_name: str,
    session: Session | None = None,
    scan_limit: int = DEFAULT_EVENT_SCAN_LIMIT,
    progress_cb: GettyProgressCallback | None = None,
) -> dict[str, Any] | None:
    """Paginate through a Getty event page and return ALL assets matching the person.

    Unlike ``fetch_grouped_event_page()`` which returns one representative,
    this scans up to *scan_limit* assets and returns every person match.
    """
    cleaned_url = str(event_url or "").strip()
    normalized_person = _normalize_name(person_name)
    if not cleaned_url or not normalized_person:
        return None

    phrase, query_params = _parse_event_url(cleaned_url)
    if not phrase:
        return None

    all_candidates = _search_asset_candidates_for_phrase(
        phrase,
        limit=max(1, int(scan_limit)),
        session=session,
        query_params=query_params,
    )
    if not all_candidates:
        return {
            "event_url": cleaned_url,
            "total_scanned": 0,
            "person_image_count": 0,
            "matched_assets": [],
            "representative_asset": None,
        }

    safe_limit = max(1, int(scan_limit))
    candidates_to_scan = all_candidates[:safe_limit]
    total = len(candidates_to_scan)

    matched_assets: list[dict[str, Any]] = []
    all_scanned: list[dict[str, Any]] = []

    # Fetch details in parallel for performance
    detail_urls = [str(c.get("detail_url") or "").strip() for c in candidates_to_scan]
    valid_pairs = [(c, u) for c, u in zip(candidates_to_scan, detail_urls) if u]
    if valid_pairs:
        batch_candidates, batch_urls = zip(*valid_pairs)
        workers = min(DEFAULT_DETAIL_MAX_WORKERS, len(batch_urls))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            batch_details = list(executor.map(fetch_asset_detail, batch_urls))
        for index, (candidate, detail) in enumerate(zip(batch_candidates, batch_details), start=1):
            if not detail:
                continue
            merged = _merge_search_candidate_with_detail(candidate, detail)
            all_scanned.append(merged)
            match_details = describe_asset_person_match(merged, person_name)
            if match_details.get("matched"):
                matched_assets.append({**merged, "person_match": match_details})
            if progress_cb:
                progress_cb(index, total, f"Scanned {index}/{total}, {len(matched_assets)} matches so far")

    return {
        "event_url": cleaned_url,
        "total_scanned": len(all_scanned),
        "person_image_count": len(matched_assets),
        "matched_assets": matched_assets,
        "representative_asset": matched_assets[0] if matched_assets else (all_scanned[0] if all_scanned else None),
    }


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
            if limit > 0 and len(deduped) >= limit:
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
            "event_date": (
                str(
                    asset.get("eventDate")
                    or asset.get("eventDateDisplay")
                    or asset.get("eventDateText")
                    or asset.get("displayDate")
                    or ""
                ).strip()
                or None
            ),
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
        overlay_people = _extract_people_overlay_names(asset)
        if overlay_people:
            candidate["search_people_overlay_names"] = overlay_people
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
    detail_fields = _extract_detail_section_fields(soup)
    keyword_texts = _extract_keyword_texts(asset_json)
    people_count = _infer_people_count(keyword_texts)
    overlay_people = _extract_people_overlay_names(asset_json)

    result: dict[str, Any] = {
        "detail_url": detail_url,
        "editorial_id": editorial_id,
        "object_name": object_name,
        "asset": asset_json,
        "people": _extract_specific_people(asset_json),
        "details": detail_fields,
    }
    result["title"] = _first_present(asset_json, "title", "headline")
    result["caption"] = _first_present(asset_json, "caption", "caption_plain")
    result["credit"] = detail_fields.get("credit_display") or _first_present(asset_json, "credit", "creditLine")
    result["collection"] = detail_fields.get("collection_display") or _first_present(
        asset_json, "collection", "collectionName"
    )
    result["license_type"] = detail_fields.get("license_type_display") or _first_present(
        asset_json, "licenseType", "license_type"
    )
    result["date_created"] = detail_fields.get("date_created_display") or _first_present(
        asset_json, "dateCreated", "date_created"
    )
    result["upload_date"] = detail_fields.get("upload_date_display") or _first_present(
        asset_json, "uploadDate", "upload_date"
    )
    result["event_name"] = _first_present(asset_json, "eventName", "event_name")
    result["event_id"] = _first_present(asset_json, "eventId", "event_id")
    result["event_url_slug"] = _first_present(asset_json, "eventUrlSlug", "event_url_slug")
    result["event_date"] = _first_present(
        asset_json,
        "eventDate",
        "event_date",
        "dateCreated",
        "date_created",
    )
    result["getty_event_group_title"] = result["event_name"]
    image_urls = _extract_best_image_urls(asset_json)
    max_file_size = detail_fields.get("max_file_size")
    result["thumb_url"] = image_urls.get("thumbUrl") or _first_present(asset_json, "thumbUrl")
    result["comp_url"] = image_urls.get("compUrl") or _first_present(asset_json, "compUrl")
    result["original_image_url"] = _select_best_original_image_url(image_urls, max_file_size=max_file_size)
    result["preview_image_url"] = _select_best_preview_image_url(image_urls) or result["original_image_url"]
    result["keywords"] = asset_json.get("keywords") if isinstance(asset_json.get("keywords"), list) else []
    result["keyword_texts"] = keyword_texts
    result["people_overlay_names"] = overlay_people
    result["restrictions"] = detail_fields.get("restrictions")
    result["release_info"] = detail_fields.get("release_info")
    result["source"] = detail_fields.get("source_display") or _first_present(asset_json, "source")
    result["max_file_size"] = max_file_size
    result["editorial_number"] = detail_fields.get("editorial_number") or editorial_id
    result["object_name_display"] = detail_fields.get("object_name_display") or object_name
    result["people_count"] = people_count
    result["people_count_source"] = "auto" if people_count is not None else None
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


def _extract_best_image_urls(asset_json: dict[str, Any]) -> dict[str, str]:
    """Extract all available image URLs from asset JSON, including nested displaySizes."""
    urls: dict[str, str] = {}
    for key in (
        "downloadableCompUrl",
        "galleryHighResCompUrl",
        "highResCompUrl",
        "galleryComp1024Url",
        "compUrl",
        "mainImageUrl",
        "thumbUrl",
    ):
        value = str(asset_json.get(key) or "").strip()
        if value:
            urls[key] = value

    display_sizes = asset_json.get("displaySizes")
    if isinstance(display_sizes, list):
        for entry in display_sizes:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name") or "").strip().lower()
            uri = str(entry.get("uri") or entry.get("url") or "").strip()
            if uri:
                if name == "high_res_comp" and "highResCompUrl" not in urls:
                    urls["highResCompUrl"] = uri
                elif name == "comp" and "compUrl" not in urls:
                    urls["compUrl"] = uri
                elif name == "thumb" and "thumbUrl" not in urls:
                    urls["thumbUrl"] = uri
                elif name == "preview" and "previewUrl" not in urls:
                    urls["previewUrl"] = uri

    return urls


def _parse_max_file_dimensions(value: str | None) -> tuple[int | None, int | None]:
    text = str(value or "").strip()
    if not text:
        return None, None
    match = re.search(r"(\d{2,5})\s*x\s*(\d{2,5})", text, flags=re.IGNORECASE)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _parse_image_url_dimensions(url: str | None) -> tuple[int | None, int | None]:
    cleaned = str(url or "").strip()
    if not cleaned:
        return None, None
    parsed = urlparse(cleaned)
    query = parse_qs(parsed.query)
    raw_size = next(iter(query.get("s", [])), "").strip()
    size_match = re.search(r"(\d{2,5})x(\d{2,5})", raw_size, flags=re.IGNORECASE)
    if size_match:
        return int(size_match.group(1)), int(size_match.group(2))
    width = next((value for value in query.get("w", []) if value.isdigit()), "")
    height = next((value for value in query.get("h", []) if value.isdigit()), "")
    if width and height:
        return int(width), int(height)
    size_match = re.search(r"(\d{2,5})x(\d{2,5})", cleaned, flags=re.IGNORECASE)
    if size_match:
        return int(size_match.group(1)), int(size_match.group(2))
    return None, None


def _is_significantly_smaller_than_expected(
    dimensions: tuple[int | None, int | None],
    expected_dimensions: tuple[int | None, int | None],
) -> bool:
    width, height = dimensions
    expected_width, expected_height = expected_dimensions
    if not width or not height or not expected_width or not expected_height:
        return False
    area = width * height
    expected_area = expected_width * expected_height
    longest_side = max(width, height)
    expected_longest_side = max(expected_width, expected_height)
    return area < (expected_area * 0.25) or longest_side < (expected_longest_side * 0.45)


def _score_original_image_url_candidate(
    key: str,
    url: str,
    *,
    expected_dimensions: tuple[int | None, int | None],
) -> tuple[int, int, int]:
    tier = {
        "downloadableCompUrl": 80,
        "galleryHighResCompUrl": 75,
        "highResCompUrl": 70,
        "galleryComp1024Url": 60,
        "mainImageUrl": 50,
        "compUrl": 45,
        "previewUrl": 35,
        "thumbUrl": 10,
    }.get(key, 0)
    width, height = _parse_image_url_dimensions(url)
    area = (width or 0) * (height or 0)
    if area > 0 and _is_significantly_smaller_than_expected((width, height), expected_dimensions):
        signal = 0
    elif area > 0:
        signal = 2
    elif tier >= 70:
        signal = 1
    else:
        signal = 0
    return signal, area, tier


def _select_best_original_image_url(image_urls: dict[str, str], *, max_file_size: str | None = None) -> str | None:
    expected_dimensions = _parse_max_file_dimensions(max_file_size)
    candidates = [
        (key, url)
        for key in (
            "downloadableCompUrl",
            "galleryHighResCompUrl",
            "highResCompUrl",
            "galleryComp1024Url",
            "mainImageUrl",
            "compUrl",
            "previewUrl",
            "thumbUrl",
        )
        if (url := str(image_urls.get(key) or "").strip())
    ]
    if not candidates:
        return None
    best_key, best_url = max(
        candidates,
        key=lambda item: _score_original_image_url_candidate(
            item[0],
            item[1],
            expected_dimensions=expected_dimensions,
        ),
    )
    if best_key == "previewUrl" and len(candidates) > 1:
        non_preview = [candidate for candidate in candidates if candidate[0] != "previewUrl"]
        if non_preview:
            _, best_url = max(
                non_preview,
                key=lambda item: _score_original_image_url_candidate(
                    item[0],
                    item[1],
                    expected_dimensions=expected_dimensions,
                ),
            )
    return best_url


def _select_best_preview_image_url(image_urls: dict[str, str]) -> str | None:
    for key in (
        "galleryComp1024Url",
        "compUrl",
        "previewUrl",
        "mainImageUrl",
        "thumbUrl",
        "downloadableCompUrl",
        "galleryHighResCompUrl",
        "highResCompUrl",
    ):
        value = str(image_urls.get(key) or "").strip()
        if value:
            return value
    return None


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


def _extract_people_overlay_names(payload: Any) -> list[str]:
    matches: list[str] = []

    def _visit(value: Any, *, depth: int = 0) -> None:
        if depth > 3:
            return
        if isinstance(value, str):
            for match in _PEOPLE_OVERLAY_RE.finditer(value):
                raw_names = match.group(1).strip()
                if not raw_names:
                    continue
                matches.extend(_split_people_name_list(raw_names))
            return
        if isinstance(value, dict):
            for nested in value.values():
                _visit(nested, depth=depth + 1)
            return
        if isinstance(value, list):
            for nested in value:
                _visit(nested, depth=depth + 1)

    _visit(payload)
    return _dedupe_names(matches)


def _extract_detail_section_fields(soup: BeautifulSoup) -> dict[str, str]:
    strings = [value.strip() for value in soup.stripped_strings if value and value.strip()]
    results: dict[str, str] = {}
    for index, value in enumerate(strings):
        field_name = _DETAIL_FIELD_LABELS.get(value)
        if not field_name:
            continue
        collected: list[str] = []
        cursor = index + 1
        while cursor < len(strings):
            current = strings[cursor]
            if current in _DETAIL_FIELD_LABELS or current in _DETAIL_SECTION_STOP_MARKERS:
                break
            collected.append(current)
            cursor += 1
        cleaned = " ".join(part.strip() for part in collected if part.strip()).strip()
        if cleaned:
            if field_name == "object_name_display" and " " in cleaned:
                first_token = cleaned.split()[0]
                if re.search(r"\.\w{2,4}$", first_token):
                    cleaned = first_token
            results[field_name] = cleaned
    return results


def _extract_keyword_texts(asset_json: dict[str, Any]) -> list[str]:
    keywords = asset_json.get("keywords")
    if not isinstance(keywords, list):
        return []
    results: list[str] = []
    for entry in keywords:
        if isinstance(entry, dict):
            text = str(entry.get("text") or "").strip()
        else:
            text = str(entry or "").strip()
        if text:
            results.append(text)
    return results


def _infer_people_count(keyword_texts: list[str]) -> int | None:
    for raw_value in keyword_texts:
        lowered = str(raw_value or "").strip().lower()
        if not lowered:
            continue
        word_match = re.fullmatch(r"([a-z]+)\s+(?:people|person)", lowered)
        if word_match:
            return _PEOPLE_COUNT_WORDS.get(word_match.group(1))
        number_match = re.fullmatch(r"(\d+)\s+(?:people|person)", lowered)
        if number_match:
            return int(number_match.group(1))
    return None


def _normalize_name(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().casefold())


def _normalize_alpha_only(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().casefold())


def _dedupe_names(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = " ".join(str(value or "").split()).strip()
        if not cleaned:
            continue
        normalized = _normalize_name(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(cleaned)
    return deduped


def _split_people_name_list(value: str | None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    cleaned = re.sub(r"\((?:l|r|c|far left|far right|left|right|center)[^)]*\)", " ", raw, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:l-r|r-l|left to right)\b", " ", cleaned, flags=re.IGNORECASE)
    parts = re.split(r"\s*(?:,|/|;|&|\band\b)\s*", cleaned)
    return _dedupe_names(parts)


def _extract_pictured_names(*values: str | None) -> list[str]:
    matches: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        for match in _PICTURED_RE.finditer(text):
            matches.extend(_split_people_name_list(match.group(1)))
    return _dedupe_names(matches)


@lru_cache(maxsize=2048)
def _has_single_character_edit(first: str, second: str) -> bool:
    if first == second:
        return False
    if abs(len(first) - len(second)) > 1:
        return False
    if len(first) > len(second):
        first, second = second, first
    index_first = 0
    index_second = 0
    edits = 0
    while index_first < len(first) and index_second < len(second):
        if first[index_first] == second[index_second]:
            index_first += 1
            index_second += 1
            continue
        edits += 1
        if edits > 1:
            return False
        if len(first) == len(second):
            index_first += 1
            index_second += 1
        else:
            index_second += 1
    if index_second < len(second) or index_first < len(first):
        edits += 1
    return edits == 1


def _names_match_with_one_letter_tolerance(expected: str, candidate: str) -> bool:
    expected_tokens = _normalize_name(expected).split()
    candidate_tokens = _normalize_name(candidate).split()
    if len(expected_tokens) < 2 or len(expected_tokens) != len(candidate_tokens):
        return False
    mismatch_count = 0
    for left, right in zip(expected_tokens, candidate_tokens, strict=False):
        if left == right:
            continue
        if mismatch_count > 0:
            return False
        if not _has_single_character_edit(_normalize_alpha_only(left), _normalize_alpha_only(right)):
            return False
        mismatch_count += 1
    return mismatch_count == 1


def _asset_matches_denylist(asset: dict[str, Any], person_name: str) -> str | None:
    normalized_person = _normalize_name(person_name)
    title = str(asset.get("title") or asset.get("search_title") or "").strip()
    event_name = str(asset.get("event_name") or "").strip()
    for entry in _GETTY_PERSON_MATCH_DENYLIST:
        if _normalize_name(entry["person_name"]) != normalized_person:
            continue
        if entry["event_fragment"].casefold() not in event_name.casefold():
            continue
        title_fragment = str(entry.get("title_fragment") or "").strip()
        if title_fragment and title_fragment.casefold() not in title.casefold():
            continue
        return str(entry["deny_reason"]).strip() or "known_exception"
    return None


def describe_asset_person_match(asset: dict[str, Any], person_name: str) -> dict[str, Any]:
    normalized_person = _normalize_name(person_name)
    if not normalized_person:
        return {"matched": False, "reason": None, "matched_name": None}

    deny_reason = _asset_matches_denylist(asset, person_name)
    if deny_reason:
        return {
            "matched": False,
            "reason": "known_exception",
            "matched_name": None,
            "deny_reason": deny_reason,
        }

    overlay_people = _dedupe_names(
        [
            *[
                str(value).strip()
                for value in (asset.get("people_overlay_names") or asset.get("search_people_overlay_names") or [])
                if isinstance(value, str) and str(value).strip()
            ],
            *[
                str(entry).strip()
                for entry in _extract_people_overlay_names(
                    asset.get("asset") if isinstance(asset.get("asset"), dict) else {}
                )
            ],
        ]
    )
    for name in overlay_people:
        if _normalize_name(name) == normalized_person:
            return {
                "matched": True,
                "reason": "solo_overlay",
                "matched_name": name,
                "name_source": "people_overlay",
            }

    specific_people = [
        str(person.get("text") or "").strip()
        for person in asset.get("people") or []
        if isinstance(person, dict) and str(person.get("text") or "").strip()
    ]
    for name in _dedupe_names(specific_people):
        if _normalize_name(name) == normalized_person:
            return {
                "matched": True,
                "reason": "specific_people",
                "matched_name": name,
                "name_source": "specific_people",
            }

    caption_values = [
        str(asset.get("caption") or "").strip() or None,
        str(asset.get("search_caption") or "").strip() or None,
    ]
    pictured_names = _extract_pictured_names(*caption_values)
    for name in pictured_names:
        if _normalize_name(name) == normalized_person:
            return {
                "matched": True,
                "reason": "caption",
                "matched_name": name,
                "name_source": "pictured_list",
            }

    strong_context_values = [
        str(asset.get("caption") or "").strip(),
        str(asset.get("search_caption") or "").strip(),
        str(asset.get("title") or "").strip(),
        str(asset.get("search_title") or "").strip(),
        str(asset.get("event_name") or "").strip(),
    ]
    for value in strong_context_values:
        normalized_value = _normalize_name(value)
        if normalized_person and normalized_person in normalized_value:
            reason = "caption" if value in caption_values else "keyword_title"
            return {
                "matched": True,
                "reason": reason,
                "matched_name": person_name,
                "name_source": "text_match",
            }

    strong_typo_context = any(
        _ONE_LETTER_EDIT_ALLOWED_CONTEXT_RE.search(value) for value in strong_context_values if value
    )
    if strong_typo_context:
        for name in pictured_names:
            if _names_match_with_one_letter_tolerance(person_name, name):
                return {
                    "matched": True,
                    "reason": "caption_typo",
                    "matched_name": name,
                    "name_source": "pictured_list_typo",
                }

    for keyword in _extract_keyword_texts(asset.get("asset") if isinstance(asset.get("asset"), dict) else {}):
        normalized_keyword = _normalize_name(keyword)
        if normalized_keyword == normalized_person or normalized_person in normalized_keyword:
            return {
                "matched": True,
                "reason": "keyword_title",
                "matched_name": keyword,
                "name_source": "keyword",
            }

    return {"matched": False, "reason": None, "matched_name": None}


def _asset_matches_person(asset: dict[str, Any], person_name: str) -> bool:
    return bool(describe_asset_person_match(asset, person_name).get("matched"))


def _summarize_grouped_event_asset(asset: dict[str, Any]) -> dict[str, Any]:
    preview_url = (
        str(asset.get("preview_image_url") or asset.get("comp_url") or asset.get("thumb_url") or "").strip() or None
    )
    original_image_url = (
        str(asset.get("original_image_url") or asset.get("preview_image_url") or asset.get("comp_url") or "").strip()
        or None
    )
    return {
        "detail_url": str(asset.get("detail_url") or "").strip() or None,
        "editorial_id": str(asset.get("editorial_id") or "").strip() or None,
        "object_name": str(asset.get("object_name") or "").strip() or None,
        "caption": str(asset.get("caption") or "").strip() or None,
        "original_image_url": original_image_url,
        "preview_image_url": preview_url,
    }


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None
