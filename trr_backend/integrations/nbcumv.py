from __future__ import annotations

import io
import json
import logging
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from functools import lru_cache
from typing import Any

from PIL import ExifTags, Image, ImageOps
from requests import Session
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

DEFAULT_APPSYNC_API_KEY = ""
APPSYNC_URL = os.environ.get(
    "NBCUMV_APPSYNC_URL",
    "https://bfg5dqxssngazhtsf6uo7bzdvm.appsync-api.us-west-2.amazonaws.com/graphql",
)
APPSYNC_API_KEY = os.environ.get("NBCUMV_APPSYNC_API_KEY", DEFAULT_APPSYNC_API_KEY).strip()
BATCH_DOWNLOAD_URL = os.environ.get(
    "NBCUMV_BATCH_DOWNLOAD_URL",
    "https://or1ukny4rd.execute-api.us-west-2.amazonaws.com/v1",
)
CLOUDSEARCH_URL = os.environ.get(
    "NBCUMV_CLOUDSEARCH_URL",
    "https://jrh818qk4k.execute-api.us-west-2.amazonaws.com/v1/",
)
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_PAGE_SIZE = 100
MAX_FALLBACK_SCAN_PAGES = 100
CLOUDSEARCH_PAGE_SIZE = 100

_IMAGE_FIELDS = """
id
lbx_id
lbx_filename
created
modified
liveDate
location
lbx_airdateText
lbx_caption
lbx_credit
lbx_copyright
lbx_endDate
lbx_episodeTitle
lbx_fileSize
lbx_headline
lbx_height
lbx_keywords
lbx_liveDate
lbx_metadataFilename
lbx_nupNumber
lbx_photographer
lbx_programCategory
lbx_resolutionX
lbx_resolutionY
lbx_season
lbx_seasonNumber
lbx_showTitle
lbx_specialInstructions
lbx_type
lbx_width
divisionIds
eventIds
networkIds
showIds
pubStatus
"""

_IPTC_DATASET_NAMES: dict[tuple[int, int], str] = {
    (2, 5): "object_name",
    (2, 15): "category",
    (2, 20): "supplemental_categories",
    (2, 25): "keywords",
    (2, 40): "special_instructions",
    (2, 55): "date_created",
    (2, 60): "time_created",
    (2, 80): "byline",
    (2, 85): "byline_title",
    (2, 90): "city",
    (2, 92): "sublocation",
    (2, 95): "province_state",
    (2, 101): "country",
    (2, 103): "original_transmission_reference",
    (2, 105): "headline",
    (2, 110): "credit",
    (2, 115): "source",
    (2, 116): "copyright_notice",
    (2, 118): "contact",
    (2, 120): "caption_abstract",
    (2, 122): "writer_editor",
}


@dataclass(frozen=True)
class SearchFilters:
    filename: str | None = None
    lbx_id: str | None = None
    show_id: str | None = None
    search_text: str | None = None
    nup_prefix: str | None = None
    show_name: str | None = None
    meta_type: str | None = None
    season: str | None = None
    episode: str | None = None
    network: str | None = None
    created_start: str | None = None
    created_end: str | None = None
    live_date_start: str | None = None
    live_date_end: str | None = None
    search_caption: str | None = None
    limit: int = 25


def _normalize_title(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text.endswith(", the"):
        text = f"the {text[:-5].strip()}"
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _filename_key(value: str | None) -> str:
    return str(value or "").strip().lower()


def _filename_stem(value: str | None) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    return re.sub(r"\.[a-z0-9]+$", "", cleaned, flags=re.IGNORECASE)


def _normalize_nup_identifier(value: str | None) -> str:
    stem = _filename_stem(value).upper()
    if not stem:
        return ""
    parts = stem.split("_")
    if len(parts) != 3 or parts[0] != "NUP":
        return stem
    frame = parts[2]
    if frame.isdigit():
        frame = str(int(frame))
    return f"{parts[0]}_{parts[1]}_{frame}"


def _nup_set_prefix(value: str | None) -> str | None:
    normalized = _normalize_nup_identifier(value)
    if not normalized:
        return None
    parts = normalized.split("_")
    if len(parts) != 3 or parts[0] != "NUP":
        return None
    return f"{parts[0]}_{parts[1]}"


def _filenames_match(left: str | None, right: str | None) -> bool:
    left_cleaned = str(left or "").strip()
    right_cleaned = str(right or "").strip()
    if not left_cleaned or not right_cleaned:
        return False
    if _filename_key(left_cleaned) == _filename_key(right_cleaned):
        return True
    if _filename_key(_filename_stem(left_cleaned)) == _filename_key(_filename_stem(right_cleaned)):
        return True
    left_nup = _normalize_nup_identifier(left_cleaned)
    right_nup = _normalize_nup_identifier(right_cleaned)
    return bool(left_nup and right_nup and left_nup == right_nup)


def _cloudsearch_query_candidates(filename: str | None) -> list[str]:
    cleaned = str(filename or "").strip()
    if not cleaned:
        return []
    stem = _filename_stem(cleaned)
    raw_candidates = [cleaned]
    if stem and stem.casefold() != cleaned.casefold():
        raw_candidates.append(stem)
    if stem and "." not in cleaned:
        raw_candidates.extend([f"{stem}.JPG", f"{stem}.jpg"])
    candidates: list[str] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        normalized = candidate.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(candidate)
    return candidates


def _session(session: Session | None = None) -> Session:
    return session or Session()


def _graphql_request(query: str, *, session: Session | None = None) -> dict[str, Any]:
    if not APPSYNC_API_KEY:
        raise RuntimeError("NBCUMV_APPSYNC_API_KEY is required for NBCUMV GraphQL requests")
    client = _session(session)
    try:
        response = client.post(
            APPSYNC_URL,
            json={"query": query},
            headers={
                "Content-Type": "application/json",
                "x-api-key": APPSYNC_API_KEY,
            },
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"NBCUMV GraphQL request failed: {exc}") from exc

    payload = response.json()
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        message = "; ".join(str(item.get("message") or item) for item in errors)
        raise RuntimeError(f"NBCUMV GraphQL error: {message}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("NBCUMV GraphQL response was missing data")
    return data


def _cloudsearch_request(
    query: str,
    *,
    fq: str | None = None,
    size: int = CLOUDSEARCH_PAGE_SIZE,
    start: int = 0,
    session: Session | None = None,
) -> dict[str, Any]:
    client = _session(session)
    try:
        params = {
            "q": query,
            "size": max(1, min(CLOUDSEARCH_PAGE_SIZE, int(size or CLOUDSEARCH_PAGE_SIZE))),
            "start": max(0, int(start or 0)),
            "return": "_all_fields",
        }
        if isinstance(fq, str) and fq.strip():
            params["fq"] = fq.strip()
        response = client.get(CLOUDSEARCH_URL, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
        response.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"NBCUMV CloudSearch request failed: {exc}") from exc

    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("NBCUMV CloudSearch response was malformed")
    return payload


def _json_graphql(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True)


def _render_graphql_input(value: Any) -> str:
    if isinstance(value, dict):
        parts = [f"{key}: {_render_graphql_input(inner)}" for key, inner in value.items() if inner is not None]
        return "{ " + ", ".join(parts) + " }"
    if isinstance(value, list):
        return "[" + ", ".join(_render_graphql_input(item) for item in value) + "]"
    return _json_graphql(value)


def _build_search_filter(filters: SearchFilters) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if filters.filename:
        payload["lbx_filename"] = {"eq": filters.filename}
    if filters.show_id:
        payload["showIds"] = {"contains": filters.show_id}
    if filters.live_date_start and filters.live_date_end:
        payload["liveDate"] = {
            "between": [_iso_day_start(filters.live_date_start), _iso_day_end(filters.live_date_end)]
        }
    return payload


def _caption_matches(item: dict[str, Any], search_caption: str | None) -> bool:
    needle = str(search_caption or "").strip().casefold()
    if not needle:
        return True
    for field in ("lbx_caption", "lbx_headline", "lbx_keywords"):
        value = str(item.get(field) or "").casefold()
        if needle in value:
            return True
    return False


def _iso_day_start(day: str) -> str:
    value = str(day or "").strip()
    if not value:
        raise ValueError("Date value is required")
    if "T" in value:
        return value
    return f"{value}T00:00:00.000Z"


def _iso_day_end(day: str) -> str:
    value = str(day or "").strip()
    if not value:
        raise ValueError("Date value is required")
    if "T" in value:
        return value
    return f"{value}T23:59:59.999Z"


def _cloudsearch_field_strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    cleaned = str(value or "").strip()
    return [cleaned] if cleaned else []


def _cloudsearch_field_first(value: Any) -> str | None:
    values = _cloudsearch_field_strings(value)
    return values[0] if values else None


def _parse_optional_int(value: Any) -> int | None:
    if isinstance(value, int) and value >= 0:
        return value
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    try:
        parsed = int(cleaned)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _hidden_tags_for_status(status: str | None, existing_tags: list[str] | None = None) -> list[str]:
    tags = [str(tag).strip() for tag in (existing_tags or []) if str(tag).strip()]
    hidden = str(status or "").strip() == "0"
    deduped: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if tag.casefold() == "hidden":
            continue
        lowered = tag.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(tag)
    if hidden:
        deduped.append("HIDDEN")
    return deduped


def _coerce_cloudsearch_text(value: Any) -> str | None:
    cleaned = _cloudsearch_field_first(value)
    return cleaned if cleaned else None


def _cloudsearch_hit_to_image(hit: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(hit, dict):
        return None
    fields = hit.get("fields")
    if not isinstance(fields, dict):
        return None
    if str(fields.get("type") or "").strip().lower() != "image":
        return None
    filename = str(fields.get("title") or "").strip()
    lbx_id = str(fields.get("item_number") or "").strip()
    if not filename or not lbx_id:
        return None
    show_ids = _cloudsearch_field_strings(fields.get("show_ids"))
    show_titles = _cloudsearch_field_strings(fields.get("shows"))
    networks = _cloudsearch_field_strings(fields.get("networks"))
    meta_types = _cloudsearch_field_strings(fields.get("meta_types"))
    status = _coerce_cloudsearch_text(fields.get("status")) or _coerce_cloudsearch_text(fields.get("pubStatus"))
    tags = _hidden_tags_for_status(status)
    return {
        "id": str(fields.get("id") or hit.get("id") or "").strip() or None,
        "lbx_id": lbx_id,
        "lbx_filename": filename,
        "created": str(fields.get("created") or "").strip() or None,
        "liveDate": str(fields.get("live_date") or "").strip() or None,
        "location": str(fields.get("thumbnail") or "").strip() or None,
        "lbx_fileSize": _parse_optional_int(fields.get("filesize")),
        "lbx_caption": str(fields.get("description") or "").strip() or None,
        "lbx_headline": str(fields.get("headline") or "").strip() or None,
        "lbx_showTitle": show_titles[0] if show_titles else None,
        "lbx_episodeTitle": _coerce_cloudsearch_text(fields.get("episode_title")),
        "lbx_episodeNumber": _coerce_cloudsearch_text(fields.get("episode_number")),
        "lbx_season": _coerce_cloudsearch_text(fields.get("season")),
        "lbx_seasonNumber": (
            _parse_optional_int(fields.get("season_number")) or _parse_optional_int(fields.get("season"))
        ),
        "lbx_type": _coerce_cloudsearch_text(fields.get("meta_type")) or (meta_types[0] if meta_types else None),
        "lbx_keywords": _coerce_cloudsearch_text(fields.get("keywords")),
        "status": status,
        "is_hidden": status == "0",
        "tags": tags,
        "metaTypes": meta_types,
        "networkTitles": networks,
        "showIds": show_ids,
        "showTitles": show_titles,
        "cloudsearch_fields": fields,
        "cloudsearch_hit_id": str(hit.get("id") or "").strip() or None,
    }


def _annotate_nup_group_counts(images: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts_by_nup_set: dict[str, int] = {}
    for image in images:
        prefix = _nup_set_prefix(image.get("lbx_filename"))
        if not prefix:
            continue
        counts_by_nup_set[prefix] = counts_by_nup_set.get(prefix, 0) + 1
    for image in images:
        prefix = _nup_set_prefix(image.get("lbx_filename"))
        if prefix:
            image["nup_set"] = prefix
            image["grouped_image_count"] = counts_by_nup_set.get(prefix, 1)
    return images


def _annotate_grouped_image_counts(images: list[dict[str, Any]], *, person_match_source: str) -> list[dict[str, Any]]:
    _annotate_nup_group_counts(images)
    for image in images:
        image["person_match_source"] = person_match_source
    return images


def _cloudsearch_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def _expand_show_name_aliases(show_name: str | None) -> list[str]:
    cleaned = str(show_name or "").strip()
    if not cleaned:
        return []
    variants = [cleaned]
    lowered = cleaned.casefold()
    if "ex-wives club" in lowered and "ex-wives clubs" not in lowered:
        variants.append(re.sub(r"ex-wives club\b", "Ex-Wives Clubs", cleaned, flags=re.IGNORECASE))
    if "ex-wives clubs" in lowered:
        variants.append(re.sub(r"ex-wives clubs\b", "Ex-Wives Club", cleaned, flags=re.IGNORECASE))
    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        key = variant.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(variant)
    return deduped


def _build_cloudsearch_filter_value(field: str, values: list[str]) -> str | None:
    cleaned_values = [str(value).strip() for value in values if str(value).strip()]
    if not cleaned_values:
        return None
    if len(cleaned_values) == 1:
        return f"{field}:'{_cloudsearch_escape(cleaned_values[0])}'"
    inner = " ".join(f"{field}:'{_cloudsearch_escape(value)}'" for value in cleaned_values)
    return f"(or {inner})"


def _build_cloudsearch_fq(filters: SearchFilters) -> str:
    # NBCUMV Data Quality Notes:
    # - RHUGT S2 show name typo: "Ex-Wives Clubs" in headline, but "Ex-Wives Club" in shows.
    # - NUP_195460_02015 caption misspells Tamra as "Tamra Gunvalson".
    # - NUP_195530 is mislabeled as "Season 1" while containing Season 2 cast imagery.
    # - RHUGT S2 images can be status:0 only; NEVER filter CloudSearch by status.
    clauses = ["type:'image'"]
    show_name_clause = _build_cloudsearch_filter_value("shows", _expand_show_name_aliases(filters.show_name))
    if show_name_clause:
        clauses.append(show_name_clause)
    show_id_clause = _build_cloudsearch_filter_value(
        "show_ids",
        [str(filters.show_id).strip()] if str(filters.show_id or "").strip() else [],
    )
    if show_id_clause:
        clauses.append(show_id_clause)
    meta_type_clause = _build_cloudsearch_filter_value(
        "meta_types",
        [str(filters.meta_type).strip()] if str(filters.meta_type or "").strip() else [],
    )
    if meta_type_clause:
        clauses.append(meta_type_clause)
    network_clause = _build_cloudsearch_filter_value(
        "networks",
        [str(filters.network).strip()] if str(filters.network or "").strip() else [],
    )
    if network_clause:
        clauses.append(network_clause)
    return f"(and {' '.join(clauses)})"


def _resolve_cloudsearch_queries(filters: SearchFilters) -> list[str]:
    if filters.nup_prefix:
        return [str(filters.nup_prefix).strip()]
    if filters.search_text:
        return [str(filters.search_text).strip()]
    if filters.lbx_id:
        return [str(filters.lbx_id).strip()]
    if filters.filename:
        return _cloudsearch_query_candidates(filters.filename)
    return ["*"]


def _matches_cloudsearch_text_filter(candidate: str | None, needle: str | None) -> bool:
    normalized_candidate = str(candidate or "").casefold()
    normalized_needle = str(needle or "").strip().casefold()
    if not normalized_needle:
        return True
    return normalized_needle in normalized_candidate


def _match_date_range(value: str | None, start: str | None, end: str | None) -> bool:
    if not start and not end:
        return True
    candidate = str(value or "").strip()
    if not candidate:
        return False
    try:
        parsed_candidate = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return False
    if start:
        parsed_start = datetime.fromisoformat(_iso_day_start(start).replace("Z", "+00:00"))
        if parsed_candidate < parsed_start:
            return False
    if end:
        parsed_end = datetime.fromisoformat(_iso_day_end(end).replace("Z", "+00:00"))
        if parsed_candidate > parsed_end:
            return False
    return True


def _match_season_filter(image: dict[str, Any], season: str | None) -> bool:
    needle = str(season or "").strip()
    if not needle:
        return True
    explicit = image.get("lbx_seasonNumber") or image.get("lbx_season")
    if str(explicit or "").strip() == needle:
        return True
    haystack = " ".join(
        str(image.get(key) or "").strip()
        for key in ("lbx_headline", "lbx_caption", "lbx_showTitle")
        if str(image.get(key) or "").strip()
    )
    return bool(re.search(rf"\bseason\s+{re.escape(needle)}\b", haystack, re.IGNORECASE))


def _match_episode_filter(image: dict[str, Any], episode: str | None) -> bool:
    needle = str(episode or "").strip()
    if not needle:
        return True
    explicit = image.get("lbx_episodeNumber")
    if str(explicit or "").strip() == needle:
        return True
    for key in ("lbx_episodeTitle", "lbx_headline", "lbx_caption"):
        if _matches_cloudsearch_text_filter(image.get(key), needle):
            return True
    return False


def _apply_cloudsearch_post_filters(images: list[dict[str, Any]], filters: SearchFilters) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for image in images:
        if filters.show_id:
            show_ids = [str(item).strip() for item in image.get("showIds") or [] if str(item).strip()]
            if str(filters.show_id).strip() not in show_ids:
                continue
        if filters.show_name:
            expanded_names = _expand_show_name_aliases(filters.show_name)
            if expanded_names and not any(
                _matches_cloudsearch_text_filter(image.get("lbx_showTitle"), candidate)
                or any(
                    _matches_cloudsearch_text_filter(show_title, candidate)
                    for show_title in image.get("showTitles") or []
                )
                for candidate in expanded_names
            ):
                continue
        if filters.meta_type:
            meta_types = [str(item).strip() for item in image.get("metaTypes") or [] if str(item).strip()]
            if meta_types and not any(
                str(filters.meta_type).strip().casefold() == item.casefold() for item in meta_types
            ):
                continue
        if filters.network:
            networks = [str(item).strip() for item in image.get("networkTitles") or [] if str(item).strip()]
            if networks and not any(str(filters.network).strip().casefold() == item.casefold() for item in networks):
                continue
        if not _caption_matches(image, filters.search_caption):
            continue
        if not _match_date_range(image.get("created"), filters.created_start, filters.created_end):
            continue
        if not _match_date_range(image.get("liveDate"), filters.live_date_start, filters.live_date_end):
            continue
        if not _match_season_filter(image, filters.season):
            continue
        if not _match_episode_filter(image, filters.episode):
            continue
        filtered.append(image)
    return filtered


def search_cloudsearch_images(filters: SearchFilters, *, session: Session | None = None) -> list[dict[str, Any]]:
    limit = max(1, int(filters.limit or 25))
    fq = _build_cloudsearch_fq(filters)
    queries = _resolve_cloudsearch_queries(filters)
    collected: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for query in queries:
        start = 0
        total_found: int | None = None
        while len(collected) < limit:
            payload = _cloudsearch_request(
                query,
                fq=fq,
                size=min(CLOUDSEARCH_PAGE_SIZE, limit - len(collected)),
                start=start,
                session=session,
            )
            hits = payload.get("hits")
            if not isinstance(hits, dict):
                break
            total_found = _parse_optional_int(hits.get("found"))
            raw_hits = hits.get("hit") or []
            if not isinstance(raw_hits, list) or not raw_hits:
                break
            page_images: list[dict[str, Any]] = []
            for hit in raw_hits:
                image = _cloudsearch_hit_to_image(hit)
                if image is None:
                    continue
                dedupe_key = str(image.get("lbx_id") or "").strip() or str(image.get("lbx_filename") or "").strip()
                if dedupe_key and dedupe_key in seen_keys:
                    continue
                if dedupe_key:
                    seen_keys.add(dedupe_key)
                page_images.append(image)
            for image in _apply_cloudsearch_post_filters(page_images, filters):
                collected.append(image)
                if len(collected) >= limit:
                    break
            start += len(raw_hits)
            if total_found is not None and start >= total_found:
                break
        if len(collected) >= limit:
            break
    return _annotate_nup_group_counts(collected[:limit])


def search_person_images(
    person_name: str,
    *,
    show_id: str | None = None,
    limit: int = DEFAULT_PAGE_SIZE,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    normalized_person_name = str(person_name or "").strip()
    if not normalized_person_name:
        return []
    capped_limit = max(1, int(limit or DEFAULT_PAGE_SIZE))
    collected = search_cloudsearch_images(
        SearchFilters(
            search_text=normalized_person_name,
            show_id=show_id,
            limit=capped_limit,
        ),
        session=session,
    )
    return _annotate_grouped_image_counts(collected[:capped_limit], person_match_source="cloudsearch")


def discover_person_show_titles(
    person_name: str,
    *,
    limit: int = DEFAULT_PAGE_SIZE,
    session: Session | None = None,
) -> list[str]:
    discovered_titles: list[str] = []
    seen_titles: set[str] = set()
    for image in search_person_images(person_name, limit=max(1, int(limit or DEFAULT_PAGE_SIZE)), session=session):
        title = str(image.get("lbx_showTitle") or "").strip()
        if not title:
            continue
        normalized_title = _normalize_title(title)
        if not normalized_title or normalized_title in seen_titles:
            continue
        seen_titles.add(normalized_title)
        discovered_titles.append(title)
    return discovered_titles


def search_person_show_catalog(
    person_name: str,
    *,
    show_id: str,
    limit: int = DEFAULT_PAGE_SIZE,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    normalized_person_name = str(person_name or "").strip()
    normalized_show_id = str(show_id or "").strip()
    if not normalized_person_name or not normalized_show_id:
        return []

    capped_limit = max(1, int(limit or DEFAULT_PAGE_SIZE))
    matches = search_cloudsearch_images(
        SearchFilters(
            search_text=normalized_person_name,
            show_id=normalized_show_id,
            search_caption=normalized_person_name,
            limit=capped_limit,
        ),
        session=session,
    )
    return _annotate_grouped_image_counts(matches, person_match_source="show_catalog")


def search_images(filters: SearchFilters, *, session: Session | None = None) -> list[dict[str, Any]]:
    items = search_cloudsearch_images(filters, session=session)
    if filters.lbx_id and not any(
        str(item.get("lbx_id") or "").strip() == str(filters.lbx_id).strip() for item in items
    ):
        matched = _scan_for_lbx_id(str(filters.lbx_id).strip(), session=session)
        if matched is not None:
            items = [matched]
    return items[: max(1, int(filters.limit or 25))]


def _list_show_images_uncached(
    show_id: str,
    *,
    session: Session | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    normalized_show_id = str(show_id or "").strip()
    if not normalized_show_id:
        return []
    max_items = max(1, int(limit)) if limit is not None else 5000
    show_title = None
    for item in list_all_shows():
        if str(item.get("id") or "").strip() == normalized_show_id:
            show_title = str(item.get("title") or "").strip() or None
            break
    return search_cloudsearch_images(
        SearchFilters(show_id=normalized_show_id, show_name=show_title, limit=max_items),
        session=session,
    )


@lru_cache(maxsize=64)
def _list_show_images_cached(show_id: str) -> tuple[dict[str, Any], ...]:
    return tuple(_list_show_images_uncached(show_id))


def list_show_images(show_id: str, *, session: Session | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    normalized_show_id = str(show_id or "").strip()
    if not normalized_show_id:
        return []
    if session is None and limit is None:
        return [dict(item) for item in _list_show_images_cached(normalized_show_id)]
    return _list_show_images_uncached(normalized_show_id, session=session, limit=limit)


def build_show_image_index(
    show_id: str,
    *,
    session: Session | None = None,
) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for image in list_show_images(show_id, session=session):
        filename = _filename_key(image.get("lbx_filename"))
        if filename and filename not in index:
            index[filename] = image
    return index


def find_show_image_by_filename(
    show_id: str,
    filename: str | None,
    *,
    session: Session | None = None,
) -> dict[str, Any] | None:
    key = _filename_key(filename)
    if not key:
        return None
    index = build_show_image_index(show_id, session=session)
    direct = index.get(key)
    if direct is not None:
        return direct
    for candidate in index.values():
        if _filenames_match(candidate.get("lbx_filename"), filename):
            return candidate
    return None


@lru_cache(maxsize=1)
def list_all_shows() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    next_token: str | None = None
    while True:
        token_expr = _json_graphql(next_token) if next_token else "null"
        query = f"""
        query {{
          listShows(
            limit: {DEFAULT_PAGE_SIZE}
            nextToken: {token_expr}
          ) {{
            items {{
              id
              title
              slug
              networkIds
            }}
            nextToken
          }}
        }}
        """
        payload = _graphql_request(query).get("listShows") or {}
        page_items = payload.get("items") or []
        items.extend(page_items)
        next_token = payload.get("nextToken")
        if not next_token or not page_items:
            break
    return items


def resolve_show_by_title(title: str | None) -> dict[str, Any] | None:
    normalized = _normalize_title(title)
    if not normalized:
        return None

    exact_matches: list[dict[str, Any]] = []
    fallback_matches: list[dict[str, Any]] = []
    for item in list_all_shows():
        item_title = str(item.get("title") or "")
        normalized_item = _normalize_title(item_title)
        if not normalized_item:
            continue
        if normalized_item == normalized:
            exact_matches.append(item)
            continue
        if normalized_item.startswith(normalized) or normalized.startswith(normalized_item):
            fallback_matches.append(item)

    if exact_matches:
        return exact_matches[0]
    if fallback_matches:
        return fallback_matches[0]
    return None


def _scan_for_lbx_id(lbx_id: str, *, session: Session | None = None) -> dict[str, Any] | None:
    next_token: str | None = None
    for _ in range(MAX_FALLBACK_SCAN_PAGES):
        token_expr = _json_graphql(next_token) if next_token else "null"
        query = f"""
        query {{
          searchImages(
            limit: {DEFAULT_PAGE_SIZE}
            nextToken: {token_expr}
          ) {{
            items {{ {_IMAGE_FIELDS} }}
            nextToken
          }}
        }}
        """
        payload = _graphql_request(query, session=session).get("searchImages") or {}
        for item in payload.get("items") or []:
            if str(item.get("lbx_id") or "").strip() == lbx_id:
                return item
        next_token = payload.get("nextToken")
        if not next_token:
            break
    return None


def _search_cloudsearch_images_by_filename(
    filename: str,
    *,
    show_id: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    return search_cloudsearch_images(
        SearchFilters(filename=filename, show_id=show_id, limit=10),
        session=session,
    )


def fetch_image_by_identity(
    *,
    filename: str | None = None,
    lbx_id: str | None = None,
    show_id: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    if filename:
        if show_id:
            image = find_show_image_by_filename(str(show_id), filename, session=session)
            if image:
                return image
        items = search_images(SearchFilters(filename=filename, limit=1), session=session)
        if items:
            return items[0]
        cloudsearch_candidates = _search_cloudsearch_images_by_filename(
            str(filename),
            show_id=show_id,
            session=session,
        )
        for candidate in cloudsearch_candidates:
            if _filenames_match(candidate.get("lbx_filename"), filename):
                return candidate
    if lbx_id:
        return _scan_for_lbx_id(str(lbx_id).strip(), session=session)
    return None


def request_hires_zip_url(*, lbx_id: str, filename: str, session: Session | None = None) -> str:
    payload = {
        "images": [
            {
                "filename": filename,
                "id": str(lbx_id),
                "resolution": "hiRes",
            }
        ]
    }
    client = _session(session)
    try:
        response = client.post(
            BATCH_DOWNLOAD_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/zip",
            },
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"NBCUMV batch download request failed: {exc}") from exc

    result = response.json()
    location = result.get("location")
    if not isinstance(location, str) or not location.strip():
        raise RuntimeError("NBCUMV batch download response was missing location")
    return location


def download_hires_image(*, lbx_id: str, filename: str, session: Session | None = None) -> tuple[bytes, str | None]:
    zip_url = request_hires_zip_url(lbx_id=lbx_id, filename=filename, session=session)
    client = _session(session)
    try:
        response = client.get(zip_url, timeout=DEFAULT_TIMEOUT_SECONDS)
        response.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"NBCUMV ZIP download failed: {exc}") from exc

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            with archive.open(member) as handle:
                data = handle.read()
            return data, _guess_content_type(member.filename)
    raise RuntimeError("NBCUMV ZIP archive did not contain an image file")


def _guess_content_type(filename: str) -> str | None:
    lowered = str(filename or "").lower()
    if lowered.endswith(".jpg") or lowered.endswith(".jpeg"):
        return "image/jpeg"
    if lowered.endswith(".png"):
        return "image/png"
    if lowered.endswith(".webp"):
        return "image/webp"
    return None


def extract_embedded_metadata(image_bytes: bytes) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "exif": {},
        "xmp": {},
        "iptc": {},
    }
    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            image = ImageOps.exif_transpose(image)
            metadata["dimensions"] = {
                "width": int(image.width),
                "height": int(image.height),
                "mode": str(image.mode),
                "format": str(image.format or ""),
            }
            metadata["exif"] = _extract_exif_dict(image)
            raw_xmp = _extract_raw_xmp(image_bytes)
            metadata["xmp_raw"] = raw_xmp
            if hasattr(image, "getxmp"):
                try:
                    metadata["xmp"] = _json_safe_value(image.getxmp() or {})
                except Exception:
                    metadata["xmp"] = {}
            if "icc_profile" in image.info:
                metadata["icc_profile_bytes"] = len(image.info.get("icc_profile") or b"")
    except Exception as exc:
        logger.warning("Failed to extract embedded metadata from NBCUMV image: %s", exc)
        metadata["error"] = str(exc)

    metadata["iptc"] = _extract_iptc_dict(image_bytes)
    metadata["extracted_at"] = datetime.now(UTC).isoformat()
    sanitized = _json_safe_value(metadata)
    return sanitized if isinstance(sanitized, dict) else metadata


def _json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", value)
    if isinstance(value, bytes):
        try:
            decoded = value.decode("utf-8", errors="replace")
        except Exception:
            return value.hex()
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", decoded)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Fraction):
        as_float = float(value)
        return int(as_float) if as_float.is_integer() else as_float
    if hasattr(value, "numerator") and hasattr(value, "denominator"):
        try:
            as_float = float(value)
        except Exception:
            return str(value)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(value, dict):
        return {str(key): _json_safe_value(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return str(value)


def _extract_exif_dict(image: Image.Image) -> dict[str, Any]:
    exif = image.getexif()
    result: dict[str, Any] = {}
    for tag_id, value in exif.items():
        tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
        result[tag_name] = _json_safe_value(value)
    return result


def _extract_raw_xmp(image_bytes: bytes) -> str | None:
    match = re.search(rb"<x:xmpmeta[\s\S]*?</x:xmpmeta>", image_bytes)
    if not match:
        return None
    try:
        return match.group(0).decode("utf-8", errors="replace")
    except Exception:
        return None


def _extract_iptc_dict(image_bytes: bytes) -> dict[str, Any]:
    photoshop_signature = b"Photoshop 3.0\x00"
    start = image_bytes.find(photoshop_signature)
    if start < 0:
        return {}

    offset = start + len(photoshop_signature)
    resources: dict[str, Any] = {}

    while offset < len(image_bytes) - 12:
        if image_bytes[offset : offset + 4] != b"8BIM":
            offset += 1
            continue

        offset += 4
        resource_id = int.from_bytes(image_bytes[offset : offset + 2], "big")
        offset += 2

        name_length = image_bytes[offset]
        offset += 1
        offset += name_length
        if (1 + name_length) % 2 == 1:
            offset += 1

        size = int.from_bytes(image_bytes[offset : offset + 4], "big")
        offset += 4
        data = image_bytes[offset : offset + size]
        offset += size
        if size % 2 == 1:
            offset += 1

        if resource_id != 0x0404:
            continue

        for key, value in _parse_iptc_dataset_block(data).items():
            resources[key] = value

    return resources


def _parse_iptc_dataset_block(data: bytes) -> dict[str, Any]:
    index = 0
    parsed: dict[str, Any] = {}
    while index + 5 <= len(data):
        if data[index] != 0x1C:
            index += 1
            continue
        record = data[index + 1]
        dataset = data[index + 2]
        size = int.from_bytes(data[index + 3 : index + 5], "big")
        index += 5
        value_bytes = data[index : index + size]
        index += size

        key = _IPTC_DATASET_NAMES.get((record, dataset), f"{record}:{dataset}")
        value = value_bytes.decode("utf-8", errors="replace").strip()
        if not value:
            continue
        current = parsed.get(key)
        if current is None:
            parsed[key] = value
        elif isinstance(current, list):
            current.append(value)
        else:
            parsed[key] = [current, value]
    return parsed
