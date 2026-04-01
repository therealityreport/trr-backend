from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.ingestion.cast_photo_sources import (
    fetch_fandom_gallery_cast_photos,
    fetch_imdb_cast_photos,
    fetch_tmdb_cast_photos,
)
from trr_backend.integrations import getty, nbcumv
from trr_backend.integrations.bravo_jsonapi import (
    fetch_gallery_assets,
    fetch_person_galleries,
    fetch_show_galleries,
    find_person_uuid,
    find_show_node,
)
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    build_shared_media_s3_key,
    get_object_storage_bucket,
    get_object_storage_client,
    guess_ext_from_content_type,
    mirror_url_to_s3,
    upload_bytes_to_s3,
)

SourceSelection = str
ProgressCallback = Callable[[str], None]

GETTY_FAMILY_ARTIFACTS = ("getty", "nbcumv", "bravo")
PERSON_SOURCE_FAMILIES = ("getty", "imdb", "tmdb", "fandom")
SHOW_SOURCE_FAMILIES = ("getty",)
ACTION_STOP_WORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}


def _emit(progress_cb: ProgressCallback | None, message: str) -> None:
    if progress_cb:
        progress_cb(message)


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return cleaned.strip("-") or "unknown"


def _json_default(value: Any) -> Any:
    if isinstance(value, (set, tuple)):
        return list(value)
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False, default=_json_default) + "\n")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _normalize_sources(sources: Sequence[str] | str | None, *, mode: str) -> list[str]:
    if sources is None:
        return ["all"]
    if isinstance(sources, str):
        raw_items = [part.strip().lower() for part in sources.split(",") if part.strip()]
    else:
        raw_items = [str(part).strip().lower() for part in sources if str(part).strip()]
    if not raw_items:
        raw_items = ["all"]

    allowed = set(PERSON_SOURCE_FAMILIES if mode == "person" else SHOW_SOURCE_FAMILIES) | {"all"}
    normalized: list[str] = []
    for item in raw_items:
        if item in allowed and item not in normalized:
            normalized.append(item)
    return normalized or ["all"]


def _selected_source_families(selection: Sequence[str], *, mode: str) -> list[str]:
    allowed = list(PERSON_SOURCE_FAMILIES if mode == "person" else SHOW_SOURCE_FAMILIES)
    if "all" in selection:
        return allowed
    return [item for item in selection if item in allowed]


def _refreshed_artifacts(selection: Sequence[str], *, mode: str) -> list[str]:
    artifacts: list[str] = []
    for family in _selected_source_families(selection, mode=mode):
        if family == "getty":
            for artifact in GETTY_FAMILY_ARTIFACTS:
                if artifact not in artifacts:
                    artifacts.append(artifact)
        elif family not in artifacts:
            artifacts.append(family)
    return artifacts


def _normalize_nup_key(filename: str | None) -> str | None:
    stem = re.sub(r"\.[a-z0-9]+$", "", str(filename or "").strip(), flags=re.IGNORECASE).upper()
    if not stem:
        return None
    match = re.match(r"^(NUP)_(\d+)_([0-9]+)", stem)
    if not match:
        return stem
    frame = str(int(match.group(3)))
    return f"{match.group(1)}_{match.group(2)}_{frame}"


def _nup_set_from_key(nup_key: str | None) -> str | None:
    parts = str(nup_key or "").split("_")
    if len(parts) != 3 or parts[0] != "NUP":
        return None
    return f"{parts[0]}_{parts[1]}"


def _file_name_from_url(url: str | None) -> str | None:
    cleaned = str(url or "").strip()
    if not cleaned:
        return None
    parsed = urlparse(cleaned)
    name = parsed.path.rsplit("/", 1)[-1].strip()
    return name or None


def _parse_getty_id(value: str | None) -> str | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    match = re.search(r"gettyimages-(\d+)", cleaned, re.IGNORECASE)
    if match:
        return match.group(1)
    if cleaned.isdigit():
        return cleaned
    return None


def _split_caption_people(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    pictured_match = re.search(r"Pictured(?:\s*\([^)]+\))?:\s*(.+?)(?:--|$)", text, re.IGNORECASE)
    if not pictured_match:
        return []
    pictured = re.sub(r"^\([^)]*\)\s*", "", pictured_match.group(1)).strip()
    names = [part.strip(" .,!") for part in pictured.split(",") if part.strip(" .,!")]
    deduped: list[str] = []
    seen: set[str] = set()
    for name in names:
        key = name.casefold()
        if key and key not in seen:
            seen.add(key)
            deduped.append(name)
    return deduped


def _collect_known_people_names(raw_payloads: Mapping[str, Any]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    getty_rows = raw_payloads.get("getty") if isinstance(raw_payloads.get("getty"), list) else []
    for asset in getty_rows:
        people = asset.get("people") if isinstance(asset, dict) else None
        if isinstance(people, list):
            for entry in people:
                candidate = str(entry.get("text") or "").strip() if isinstance(entry, dict) else str(entry).strip()
                if " - " in candidate:
                    candidate = candidate.split(" - ", 1)[0].strip()
                if candidate and candidate.casefold() not in seen:
                    seen.add(candidate.casefold())
                    names.append(candidate)
        for candidate in _split_caption_people(asset.get("caption") if isinstance(asset, dict) else None):
            if candidate.casefold() not in seen:
                seen.add(candidate.casefold())
                names.append(candidate)
    nbcumv_rows = raw_payloads.get("nbcumv") if isinstance(raw_payloads.get("nbcumv"), list) else []
    for asset in nbcumv_rows:
        for candidate in _split_caption_people(asset.get("lbx_caption") if isinstance(asset, dict) else None):
            if candidate.casefold() not in seen:
                seen.add(candidate.casefold())
                names.append(candidate)
    bravo_rows = raw_payloads.get("bravo") if isinstance(raw_payloads.get("bravo"), list) else []
    for asset in bravo_rows:
        people_names = asset.get("gallery_people_names") if isinstance(asset, dict) else None
        if isinstance(people_names, list):
            for candidate in people_names:
                name = str(candidate).strip()
                if name and name.casefold() not in seen:
                    seen.add(name.casefold())
                    names.append(name)
    return names


def _extract_people_from_text(text: str | None, *, known_people: Sequence[str]) -> list[str]:
    extracted = _split_caption_people(text)
    if extracted:
        return extracted
    haystack = str(text or "").strip().casefold()
    if not haystack:
        return []
    matches: list[str] = []
    for candidate in known_people:
        clean = str(candidate).strip()
        if clean and clean.casefold() in haystack:
            matches.append(clean)
    deduped: list[str] = []
    seen: set[str] = set()
    for match in matches:
        key = match.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(match)
    return deduped


def _caption_keywords(text: str | None, *, known_people: Sequence[str]) -> set[str]:
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", str(text or "").strip().lower())
    tokens = [token for token in cleaned.split() if len(token) > 2 and token not in ACTION_STOP_WORDS]
    for person in known_people:
        for token in str(person).lower().split():
            tokens = [candidate for candidate in tokens if candidate != token]
    return set(tokens)


def _parse_iso_date(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
    return match.group(1) if match else None


def _parse_season_from_text(value: str | None) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"\bseason\s+(\d+)\b", text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _best_text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, dict):
            value = value.get("value")
        text = str(value or "").strip()
        if text:
            return text
    return None


def _absolute_path_url(path: str | None) -> str | None:
    cleaned = str(path or "").strip()
    if not cleaned:
        return None
    return cleaned if cleaned.startswith("http") else f"https://www.bravotv.com{cleaned}"


def _extract_bravo_image_people_names(
    asset: Mapping[str, Any],
    *,
    known_people: Sequence[str],
) -> list[str]:
    unique_first_name_map: dict[str, str] = {}
    duplicate_first_names: set[str] = set()
    for candidate in known_people:
        clean = str(candidate).strip()
        first_name = clean.split(" ", 1)[0].strip()
        normalized_first = first_name.casefold()
        if not normalized_first:
            continue
        if normalized_first in unique_first_name_map:
            duplicate_first_names.add(normalized_first)
        else:
            unique_first_name_map[normalized_first] = clean
    for duplicate_first_name in duplicate_first_names:
        unique_first_name_map.pop(duplicate_first_name, None)

    text_candidates = [
        _best_text(asset.get("field_caption")),
        _best_text(asset.get("field_image_description")),
        _best_text(asset.get("field_media_image_alt")),
    ]
    for text in text_candidates:
        extracted = _extract_people_from_text(text, known_people=known_people)
        if extracted:
            return extracted
        haystack = str(text or "").strip().casefold()
        if not haystack:
            continue
        first_name_matches = [
            full_name
            for first_name, full_name in unique_first_name_map.items()
            if re.search(rf"\b{re.escape(first_name)}\b", haystack)
        ]
        if first_name_matches:
            return first_name_matches
    return []


def _resolve_bravo_source_page_url(asset: Mapping[str, Any]) -> str | None:
    explicit = _best_text(asset.get("source_page_url"))
    if explicit:
        return explicit
    gallery_path = _best_text(asset.get("gallery_path"))
    gallery_url = _absolute_path_url(gallery_path)
    gallery_item_id = _best_text(asset.get("gallery_item_id"))
    if gallery_url and gallery_item_id:
        return f"{gallery_url}#{gallery_item_id}"
    return gallery_url


def _normalize_getty_record(asset: dict[str, Any], *, known_people: Sequence[str]) -> dict[str, Any]:
    object_name = _best_text(asset.get("object_name"), asset.get("object_name_display"))
    people_names = [
        str(entry.get("text") or "").split(" - ", 1)[0].strip()
        for entry in (asset.get("people") or [])
        if isinstance(entry, dict) and str(entry.get("text") or "").strip()
    ]
    if not people_names:
        people_names = _extract_people_from_text(asset.get("caption"), known_people=known_people)
    keywords = [str(value).strip() for value in (asset.get("keyword_texts") or []) if str(value).strip()]
    detail_url = _best_text(asset.get("detail_url"))
    image_urls = getty._extract_best_image_urls(asset)
    max_file_size = _best_text(asset.get("max_file_size"), asset.get("maxFileSize"))
    original_url = _best_text(
        asset.get("getty_original_image_url"), asset.get("original_image_url")
    ) or getty._select_best_original_image_url(image_urls, max_file_size=max_file_size)
    preview_url = (
        _best_text(asset.get("getty_preview_image_url"), asset.get("preview_image_url"))
        or getty._select_best_preview_image_url(image_urls)
        or original_url
    )
    variant_seed_url = original_url or preview_url or _best_text(asset.get("thumb_url"), asset.get("thumbUrl"))
    getty_variants = getty.build_getty_url_variants(variant_seed_url)
    fallback_large_url = getty_variants.get("full_res")
    canonical_large_url = original_url or fallback_large_url or preview_url
    thumb_clean_url = (
        _best_text(asset.get("getty_thumb_clean_url"), asset.get("thumb_url"))
        or getty_variants.get("thumb_clean")
        or preview_url
        or canonical_large_url
    )
    full_res_clean_url = getty_variants.get("full_res_clean")
    full_res_url = getty_variants.get("full_res") or canonical_large_url
    season_number = None
    for keyword in keywords:
        season_number = _parse_season_from_text(keyword)
        if season_number is not None:
            break
    if season_number is None:
        season_number = _parse_season_from_text(asset.get("event_name"))
    dimensions = asset.get("assetDimensions") if isinstance(asset.get("assetDimensions"), dict) else {}
    return {
        "source": "getty",
        "source_id": _best_text(asset.get("editorial_id"), detail_url),
        "bridge_key": _normalize_nup_key(object_name),
        "nup_filename": object_name,
        "nup_set": _nup_set_from_key(_normalize_nup_key(object_name)),
        "getty_editorial_id": _parse_getty_id(asset.get("editorial_id")),
        "caption": _best_text(asset.get("caption")),
        "bravo_caption": None,
        "people_names": people_names,
        "photographer": _best_text(asset.get("credit"), asset.get("byline")),
        "show_name": _best_text(asset.get("event_name"), asset.get("search_title"), asset.get("title")),
        "season_number": season_number,
        "episode_title": None,
        "air_date": _parse_iso_date(_best_text(asset.get("date_created"), asset.get("event_date"))),
        "keywords": keywords,
        "source_url": canonical_large_url,
        "preview_image_url": preview_url,
        "thumb_url": thumb_clean_url,
        "getty_original_image_url": original_url or canonical_large_url,
        "getty_full_res_url": full_res_url,
        "getty_full_res_clean_url": full_res_clean_url,
        "getty_thumb_clean_url": thumb_clean_url,
        "getty_preview_image_url": preview_url,
        "source_page_url": detail_url,
        "width": dimensions.get("width"),
        "height": dimensions.get("height"),
        "raw": asset,
    }


def _normalize_nbcumv_record(asset: dict[str, Any], *, known_people: Sequence[str]) -> dict[str, Any]:
    filename = _best_text(asset.get("lbx_filename"))
    keywords = (
        [str(value).strip() for value in (asset.get("lbx_keywords") or []) if str(value).strip()]
        if isinstance(asset.get("lbx_keywords"), list)
        else []
    )
    return {
        "source": "nbcumv",
        "source_id": _best_text(asset.get("lbx_id"), asset.get("id"), filename),
        "bridge_key": _normalize_nup_key(filename),
        "nup_filename": filename,
        "nup_set": _nup_set_from_key(_normalize_nup_key(filename)),
        "getty_editorial_id": _parse_getty_id(filename),
        "caption": _best_text(asset.get("lbx_caption")),
        "bravo_caption": None,
        "people_names": _extract_people_from_text(asset.get("lbx_caption"), known_people=known_people),
        "photographer": _best_text(asset.get("lbx_photographer"), asset.get("lbx_credit")),
        "show_name": _best_text(asset.get("lbx_showTitle")),
        "season_number": asset.get("lbx_seasonNumber") or asset.get("lbx_season"),
        "episode_title": _best_text(asset.get("lbx_episodeTitle")),
        "air_date": _parse_iso_date(_best_text(asset.get("liveDate"), asset.get("created"))),
        "keywords": keywords,
        "source_url": _best_text(asset.get("location")),
        "source_page_url": None,
        "width": asset.get("lbx_width") or asset.get("lbx_resolutionX"),
        "height": asset.get("lbx_height") or asset.get("lbx_resolutionY"),
        "raw": asset,
    }


def _normalize_bravo_record(asset: dict[str, Any], *, known_people: Sequence[str]) -> dict[str, Any]:
    file_name = _best_text(asset.get("file_name"), _file_name_from_url(asset.get("file_url")))
    caption = _best_text(asset.get("field_caption"))
    people_names = _extract_bravo_image_people_names(asset, known_people=known_people)
    return {
        "source": "bravo",
        "source_id": _best_text(asset.get("media_uuid"), asset.get("file_uuid"), file_name),
        "bridge_key": _normalize_nup_key(file_name),
        "nup_filename": file_name,
        "nup_set": _nup_set_from_key(_normalize_nup_key(file_name)),
        "getty_editorial_id": _parse_getty_id(file_name),
        "caption": caption,
        "bravo_caption": caption,
        "people_names": people_names,
        "photographer": _best_text(asset.get("field_credit")),
        "show_name": _best_text(asset.get("gallery_show_name")),
        "season_number": asset.get("season_number"),
        "episode_title": _best_text(asset.get("gallery_title")),
        "air_date": _parse_iso_date(_best_text(asset.get("gallery_published_date"), asset.get("gallery_created"))),
        "keywords": [],
        "source_url": _best_text(asset.get("file_url")),
        "source_page_url": _resolve_bravo_source_page_url(asset),
        "width": None,
        "height": None,
        "raw": asset,
    }


def _normalize_candidate_records(raw_payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    known_people = _collect_known_people_names(raw_payloads)
    normalized: list[dict[str, Any]] = []
    for asset in raw_payloads.get("getty") if isinstance(raw_payloads.get("getty"), list) else []:
        if isinstance(asset, dict):
            normalized.append(_normalize_getty_record(asset, known_people=known_people))
    for asset in raw_payloads.get("nbcumv") if isinstance(raw_payloads.get("nbcumv"), list) else []:
        if isinstance(asset, dict):
            normalized.append(_normalize_nbcumv_record(asset, known_people=known_people))
    for asset in raw_payloads.get("bravo") if isinstance(raw_payloads.get("bravo"), list) else []:
        if isinstance(asset, dict):
            normalized.append(_normalize_bravo_record(asset, known_people=known_people))
    return normalized


def _caption_match_score(left: dict[str, Any], right: dict[str, Any], *, known_people: Sequence[str]) -> int:
    left_people = {str(value).strip().casefold() for value in (left.get("people_names") or []) if str(value).strip()}
    right_people = {str(value).strip().casefold() for value in (right.get("people_names") or []) if str(value).strip()}
    overlap = len(left_people.intersection(right_people))
    left_keywords = _caption_keywords(left.get("caption"), known_people=known_people)
    right_keywords = _caption_keywords(right.get("caption"), known_people=known_people)
    keyword_overlap = len(left_keywords.intersection(right_keywords))
    score = overlap * 3 + keyword_overlap
    if (
        left.get("show_name")
        and right.get("show_name")
        and str(left.get("show_name")).casefold() == str(right.get("show_name")).casefold()
    ):
        score += 1
    if (
        left.get("season_number")
        and right.get("season_number")
        and left.get("season_number") == right.get("season_number")
    ):
        score += 1
    if left.get("air_date") and right.get("air_date") and left.get("air_date") == right.get("air_date"):
        score += 1
    return score


def _merge_group_records(group_id: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    per_source = {record["source"]: record for record in records}
    getty_record = per_source.get("getty", {})
    nbcumv_record = per_source.get("nbcumv", {})
    bravo_record = per_source.get("bravo", {})
    persons = (
        list(getty_record.get("people_names") or [])
        or list(nbcumv_record.get("people_names") or [])
        or list(bravo_record.get("people_names") or [])
    )
    keywords = []
    for record in records:
        for keyword in record.get("keywords") or []:
            text = str(keyword).strip()
            if text and text not in keywords:
                keywords.append(text)
    return {
        "id": group_id,
        "sources": [record["source"] for record in records],
        "per_source": per_source,
        "nup_filename": _best_text(
            nbcumv_record.get("nup_filename"),
            getty_record.get("nup_filename"),
            bravo_record.get("nup_filename"),
        ),
        "nup_set": _best_text(
            nbcumv_record.get("nup_set"),
            getty_record.get("nup_set"),
            bravo_record.get("nup_set"),
        ),
        "getty_editorial_id": _best_text(
            getty_record.get("getty_editorial_id"),
            bravo_record.get("getty_editorial_id"),
        ),
        "persons_pictured": persons,
        "photographer": _best_text(
            getty_record.get("photographer"),
            bravo_record.get("photographer"),
            nbcumv_record.get("photographer"),
        ),
        "caption": _best_text(getty_record.get("caption"), nbcumv_record.get("caption"), bravo_record.get("caption")),
        "bravo_caption": _best_text(bravo_record.get("bravo_caption")),
        "episode_title": _best_text(nbcumv_record.get("episode_title"), bravo_record.get("episode_title")),
        "season_number": nbcumv_record.get("season_number")
        or bravo_record.get("season_number")
        or getty_record.get("season_number"),
        "air_date": _best_text(
            nbcumv_record.get("air_date"),
            getty_record.get("air_date"),
            bravo_record.get("air_date"),
        ),
        "show_name": _best_text(
            nbcumv_record.get("show_name"),
            bravo_record.get("show_name"),
            getty_record.get("show_name"),
        ),
        "keywords": keywords,
    }


def build_bridge_and_catalog(raw_payloads: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records = _normalize_candidate_records(raw_payloads)
    known_people = _collect_known_people_names(raw_payloads)
    consumed: set[str] = set()
    bridge_rows: list[dict[str, Any]] = []
    merged_catalog: list[dict[str, Any]] = []
    group_counter = 1

    def _record_key(record: dict[str, Any]) -> str:
        return f"{record['source']}:{record.get('source_id')}"

    exact_nup_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record.get("bridge_key"):
            exact_nup_groups[str(record["bridge_key"])].append(record)
    for bridge_key, group_records in sorted(exact_nup_groups.items()):
        if len(group_records) < 2:
            continue
        keys = [_record_key(record) for record in group_records]
        consumed.update(keys)
        group_id = f"bridge-{group_counter:05d}"
        group_counter += 1
        merged = _merge_group_records(group_id, group_records)
        merged["bridge_strategy"] = "A_nup_filename"
        merged["match_confidence"] = "high"
        bridge_rows.append(
            {
                "group_id": group_id,
                "strategy": "A_nup_filename",
                "confidence": "high",
                "bridge_key": bridge_key,
                "records": keys,
            }
        )
        merged_catalog.append(merged)

    exact_getty_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        record_key = _record_key(record)
        if record_key in consumed:
            continue
        if record.get("getty_editorial_id"):
            exact_getty_groups[str(record["getty_editorial_id"])].append(record)
    for editorial_id, group_records in sorted(exact_getty_groups.items()):
        if len(group_records) < 2:
            continue
        keys = [_record_key(record) for record in group_records]
        consumed.update(keys)
        group_id = f"bridge-{group_counter:05d}"
        group_counter += 1
        merged = _merge_group_records(group_id, group_records)
        merged["bridge_strategy"] = "B_getty_id"
        merged["match_confidence"] = "high"
        bridge_rows.append(
            {
                "group_id": group_id,
                "strategy": "B_getty_id",
                "confidence": "high",
                "bridge_key": editorial_id,
                "records": keys,
            }
        )
        merged_catalog.append(merged)

    remaining_getty = [
        record for record in records if record["source"] == "getty" and _record_key(record) not in consumed
    ]
    remaining_bravo = [
        record for record in records if record["source"] == "bravo" and _record_key(record) not in consumed
    ]
    for getty_record in remaining_getty:
        best_score = 0
        best_candidate: dict[str, Any] | None = None
        second_best = 0
        for bravo_record in remaining_bravo:
            score = _caption_match_score(getty_record, bravo_record, known_people=known_people)
            if score > best_score:
                second_best = best_score
                best_score = score
                best_candidate = bravo_record
            elif score > second_best:
                second_best = score
        getty_key = _record_key(getty_record)
        if best_candidate and best_score >= 5 and best_score >= second_best + 2:
            bravo_key = _record_key(best_candidate)
            consumed.add(getty_key)
            consumed.add(bravo_key)
            group_id = f"bridge-{group_counter:05d}"
            group_counter += 1
            merged = _merge_group_records(group_id, [getty_record, best_candidate])
            merged["bridge_strategy"] = "E_caption_semantic"
            merged["match_confidence"] = "medium"
            bridge_rows.append(
                {
                    "group_id": group_id,
                    "strategy": "E_caption_semantic",
                    "confidence": "medium",
                    "score": best_score,
                    "records": [getty_key, bravo_key],
                }
            )
            merged_catalog.append(merged)
        elif best_candidate and best_score >= 3:
            bridge_rows.append(
                {
                    "group_id": None,
                    "strategy": "manual_review",
                    "confidence": "needs_review",
                    "score": best_score,
                    "records": [getty_key, _record_key(best_candidate)],
                    "reason": "caption_match_ambiguous",
                }
            )

    for record in records:
        record_key = _record_key(record)
        if record_key in consumed:
            continue
        group_id = f"bridge-{group_counter:05d}"
        group_counter += 1
        merged = _merge_group_records(group_id, [record])
        merged["bridge_strategy"] = "source_only"
        merged["match_confidence"] = "source_only"
        merged_catalog.append(merged)
        bridge_rows.append(
            {
                "group_id": group_id,
                "strategy": "source_only",
                "confidence": "source_only",
                "records": [record_key],
            }
        )

    return bridge_rows, merged_catalog


def _build_google_reverse_image_search_url(image_url: str | None) -> str | None:
    cleaned = str(image_url or "").strip()
    if not cleaned:
        return None
    return f"https://www.google.com/searchbyimage?image_url={quote(cleaned, safe='')}"


def _upload_bytes(data: bytes, *, content_type: str | None) -> dict[str, Any]:
    content_type_value = content_type or "application/octet-stream"
    sha256 = hashlib.sha256(data).hexdigest()
    ext = guess_ext_from_content_type(content_type_value)
    key = build_shared_media_s3_key(sha256, ext)
    client = get_object_storage_client()
    bucket = get_object_storage_bucket()
    etag, bytes_len = upload_bytes_to_s3(
        client,
        bucket=bucket,
        key=key,
        data=data,
        content_type=content_type_value,
    )
    return {
        "hosted_bucket": bucket,
        "hosted_key": key,
        "hosted_url": build_hosted_url(key),
        "hosted_sha256": sha256,
        "hosted_content_type": content_type_value,
        "hosted_bytes": bytes_len,
        "hosted_etag": etag,
        "hosted_at": datetime.now(UTC).isoformat(),
    }


def acquire_best_image(record: dict[str, Any]) -> dict[str, Any]:
    per_source = record.get("per_source") if isinstance(record.get("per_source"), dict) else {}
    nbcumv_record = per_source.get("nbcumv") if isinstance(per_source, dict) else None
    if isinstance(nbcumv_record, dict):
        raw = nbcumv_record.get("raw") if isinstance(nbcumv_record.get("raw"), dict) else {}
        lbx_id = str(raw.get("lbx_id") or "").strip()
        filename = str(raw.get("lbx_filename") or "").strip()
        if lbx_id and filename:
            try:
                image_bytes, content_type = nbcumv.download_hires_image(lbx_id=lbx_id, filename=filename)
                uploaded = _upload_bytes(image_bytes, content_type=content_type)
                return {
                    "status": "uploaded",
                    "source": "nbcumv_hires",
                    "watermarked": False,
                    **uploaded,
                }
            except Exception as exc:
                return {
                    "status": "failed",
                    "source": "nbcumv_hires",
                    "watermarked": False,
                    "error": str(exc),
                }

    bravo_record = per_source.get("bravo") if isinstance(per_source, dict) else None
    if isinstance(bravo_record, dict):
        source_url = str(bravo_record.get("source_url") or "").strip()
        if source_url:
            result = mirror_url_to_s3(source_url)
            return {
                "status": result.status,
                "source": "bravo_cdn",
                "watermarked": False,
                "hosted_url": result.hosted_url,
                "hosted_key": result.hosted_key,
                "hosted_sha256": result.sha256,
                "hosted_content_type": result.content_type,
                "hosted_bytes": result.size_bytes,
                "error": result.error,
            }

    getty_record = per_source.get("getty") if isinstance(per_source, dict) else None
    if isinstance(getty_record, dict):
        source_url = str(getty_record.get("source_url") or "").strip() or None
        preview_url = str(getty_record.get("preview_image_url") or "").strip() or source_url
        thumb_url = str(getty_record.get("thumb_url") or "").strip() or None
        source_page_url = getty_record.get("source_page_url")
        if source_url:
            result = mirror_url_to_s3(source_url)
            if result.status != "failed" and result.hosted_url:
                acquisition: dict[str, Any] = {
                    "status": "uploaded",
                    "source": "getty",
                    "watermarked": "w=gi" in source_url,
                    "source_url": source_url,
                    "preview_source_url": preview_url,
                    "source_page_url": source_page_url,
                    "hosted_url": result.hosted_url,
                    "hosted_key": result.hosted_key,
                    "hosted_sha256": result.sha256,
                    "hosted_content_type": result.content_type,
                    "hosted_bytes": result.size_bytes,
                }
                if thumb_url:
                    thumb_result = mirror_url_to_s3(thumb_url)
                    if thumb_result.status != "failed" and thumb_result.hosted_url:
                        acquisition.update(
                            {
                                "hosted_thumb_url": thumb_result.hosted_url,
                                "hosted_thumb_key": thumb_result.hosted_key,
                                "hosted_thumb_sha256": thumb_result.sha256,
                                "hosted_thumb_content_type": thumb_result.content_type,
                                "hosted_thumb_bytes": thumb_result.size_bytes,
                            }
                        )
                return acquisition
        return {
            "status": "referenced_only",
            "source": "getty_preview",
            "watermarked": True,
            "source_url": preview_url,
            "source_page_url": source_page_url,
            "google_reverse_image_search_url": _build_google_reverse_image_search_url(preview_url),
        }

    return {"status": "unavailable", "source": None, "watermarked": None}


def _normalize_external_ids(person_name: str) -> dict[str, Any]:
    try:
        db = create_supabase_admin_client()
        response = (
            db.schema("core").table("people").select("id,name,external_ids").eq("name", person_name).limit(1).execute()
        )
    except Exception:
        return {}
    rows = response.data or []
    if not rows:
        return {}
    row = rows[0] if isinstance(rows[0], dict) else {}
    external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
    return {
        "person_id": row.get("id"),
        "imdb_id": str(external_ids.get("imdb") or "").strip() or None,
        "tmdb_id": int(external_ids.get("tmdb")) if str(external_ids.get("tmdb") or "").strip().isdigit() else None,
    }


def _collect_getty_person(person_name: str, *, limit: int) -> list[dict[str, Any]]:
    assets = getty.search_editorial_assets(f"{person_name} Bravo", limit=limit)
    if not assets:
        assets = getty.search_editorial_assets(person_name, limit=limit, query_params={"sort": "best"})
    return assets


def _collect_getty_show(show_name: str, *, season: int | None, episode: int | None, limit: int) -> list[dict[str, Any]]:
    phrase_parts = [show_name]
    if season is not None:
        phrase_parts.append(f"Season {season}")
    if episode is not None:
        phrase_parts.append(f"Episode {episode}")
    phrase = " ".join(phrase_parts)
    assets = getty.search_editorial_assets(phrase, limit=limit)
    if not assets and season is not None:
        assets = getty.search_editorial_assets(f"{show_name} Bravo", limit=limit)
    return assets


def _filter_nbcumv_show_assets(
    assets: list[dict[str, Any]],
    *,
    season: int | None,
    episode: int | None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for asset in assets:
        if season is not None:
            season_value = asset.get("lbx_seasonNumber") or asset.get("lbx_season")
            try:
                if int(season_value) != int(season):
                    continue
            except (TypeError, ValueError):
                continue
        if episode is not None:
            episode_title = str(asset.get("lbx_episodeTitle") or "").strip()
            if episode_title and not re.search(rf"\b{int(episode)}\b", episode_title):
                continue
        filtered.append(asset)
    return filtered


def _collect_nbcumv_person(person_name: str, *, show_name: str | None, limit: int) -> list[dict[str, Any]]:
    show_rows: list[dict[str, Any]] = []
    if show_name:
        resolved = nbcumv.resolve_show_by_title(show_name)
        if resolved:
            show_rows.append(resolved)
    else:
        discovered_titles = nbcumv.discover_person_show_titles(person_name, limit=max(10, limit))
        for title in discovered_titles:
            resolved = nbcumv.resolve_show_by_title(title)
            if resolved:
                show_rows.append(resolved)
    deduped_by_id: dict[str, dict[str, Any]] = {}
    for show in show_rows:
        show_id = str(show.get("id") or "").strip()
        if show_id and show_id not in deduped_by_id:
            deduped_by_id[show_id] = show
    results: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for show in deduped_by_id.values():
        show_id = str(show.get("id") or "").strip()
        if not show_id:
            continue
        assets = nbcumv.search_images(
            nbcumv.SearchFilters(show_id=show_id, search_caption=person_name, limit=min(100, max(1, limit)))
        )
        for asset in assets:
            lbx_id = str(asset.get("lbx_id") or "").strip()
            if lbx_id and lbx_id not in seen_ids:
                seen_ids.add(lbx_id)
                results.append(asset)
            if len(results) >= limit:
                return results[:limit]
    return results[:limit]


def _collect_nbcumv_show(
    show_name: str,
    *,
    season: int | None,
    episode: int | None,
    limit: int,
) -> list[dict[str, Any]]:
    resolved = nbcumv.resolve_show_by_title(show_name)
    if not resolved:
        return []
    show_id = str(resolved.get("id") or "").strip()
    if not show_id:
        return []
    assets = nbcumv.list_show_images(show_id, limit=max(limit, 100))
    return _filter_nbcumv_show_assets(assets, season=season, episode=episode)[:limit]


def _collect_bravo_person(person_name: str, *, limit: int, show_name: str | None = None) -> list[dict[str, Any]]:
    client = httpx.Client(timeout=30, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
    try:
        person_uuid = find_person_uuid(person_name, client=client)
        if not person_uuid:
            return []
        galleries = fetch_person_galleries(person_uuid, client=client, limit=limit)
        rows: list[dict[str, Any]] = []
        for gallery in galleries:
            rows.extend(fetch_gallery_assets(gallery, client=client))
            if len(rows) >= limit:
                break
        if show_name:
            normalized_show = show_name.casefold()
            rows = [
                row
                for row in rows
                if normalized_show in str(row.get("gallery_show_name") or row.get("gallery_title") or "").casefold()
            ]
        return rows[:limit]
    finally:
        client.close()


def _collect_bravo_show(show_name: str, *, season: int | None, limit: int) -> list[dict[str, Any]]:
    client = httpx.Client(timeout=30, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
    try:
        show = find_show_node(show_name, client=client)
        if not show or show.get("nid") is None:
            return []
        galleries = fetch_show_galleries(show["nid"], client=client, limit=limit)
        rows: list[dict[str, Any]] = []
        for gallery in galleries:
            gallery_rows = fetch_gallery_assets(gallery, client=client)
            if season is not None:
                gallery_rows = [row for row in gallery_rows if row.get("season_number") == season]
            rows.extend(gallery_rows)
            if len(rows) >= limit:
                break
        return rows[:limit]
    finally:
        client.close()


def _collect_person_supplemental_sources(
    *,
    person_name: str,
    person_id: str | None,
    imdb_id: str | None,
    tmdb_id: int | None,
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    person_lookup = _normalize_external_ids(person_name)
    resolved_person_id = str(person_id or person_lookup.get("person_id") or "00000000-0000-0000-0000-000000000000")
    imdb_person_id = imdb_id or person_lookup.get("imdb_id")
    tmdb_person_id = tmdb_id or person_lookup.get("tmdb_id")
    return {
        "imdb": fetch_imdb_cast_photos(imdb_person_id, resolved_person_id, limit=limit) if imdb_person_id else [],
        "tmdb": fetch_tmdb_cast_photos(
            tmdb_person_id,
            resolved_person_id,
            imdb_person_id=imdb_person_id,
            limit=limit,
        )
        if tmdb_person_id
        else [],
        "fandom": fetch_fandom_gallery_cast_photos(
            person_name,
            resolved_person_id,
            imdb_person_id=imdb_person_id,
            limit=limit,
        ),
    }


def _mirror_supplemental_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mirrored: list[dict[str, Any]] = []
    for row in rows:
        source_url = str(row.get("image_url") or row.get("url") or "").strip()
        if not source_url:
            mirrored.append(dict(row))
            continue
        mirror_result = mirror_url_to_s3(source_url)
        enriched = dict(row)
        enriched["hosted"] = {
            "status": mirror_result.status,
            "hosted_url": mirror_result.hosted_url,
            "hosted_key": mirror_result.hosted_key,
            "hosted_sha256": mirror_result.sha256,
            "hosted_content_type": mirror_result.content_type,
            "hosted_bytes": mirror_result.size_bytes,
            "error": mirror_result.error,
        }
        mirrored.append(enriched)
    return mirrored


def _assign_to_people(merged_catalog: list[dict[str, Any]], *, output_dir: Path) -> dict[str, Any]:
    galleries: dict[str, list[dict[str, Any]]] = defaultdict(list)
    total_identified = 0
    multi_person = 0
    for record in merged_catalog:
        people = [str(value).strip() for value in (record.get("persons_pictured") or []) if str(value).strip()]
        record["persons_identified"] = people
        if people:
            total_identified += 1
        if len(people) > 1:
            multi_person += 1
        if not people:
            galleries["_unidentified"].append(record)
            continue
        for person in people:
            galleries[_slugify(person)].append(record)

    by_person_root = output_dir / "by_person"
    for slug, records in galleries.items():
        _write_json(by_person_root / slug / "catalog.json", records)

    top_people = sorted(
        (
            {"name": slug.replace("-", " ").title(), "image_count": len(records)}
            for slug, records in galleries.items()
            if slug != "_unidentified"
        ),
        key=lambda item: item["image_count"],
        reverse=True,
    )[:25]
    total_images = len(merged_catalog)
    for item in top_people:
        item["pct"] = f"{(item['image_count'] / total_images * 100):.1f}%" if total_images else "0.0%"
    return {
        "total_images": total_images,
        "persons_identified": len(top_people),
        "images_with_persons": total_identified,
        "images_unidentified": len(galleries.get("_unidentified", [])),
        "top_persons": top_people,
        "multi_person_images": multi_person,
        "solo_images": max(0, total_identified - multi_person),
    }


def _build_source_distribution_report(
    merged_catalog: list[dict[str, Any]],
    bridge_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    source_counts: dict[str, int] = defaultdict(int)
    strategy_counts: dict[str, int] = defaultdict(int)
    watermarked = 0
    uploaded = 0
    for record in merged_catalog:
        for source in record.get("sources") or []:
            source_counts[str(source)] += 1
        strategy_counts[str(record.get("bridge_strategy") or "unknown")] += 1
        acquisition = record.get("acquisition") if isinstance(record.get("acquisition"), dict) else {}
        if acquisition.get("watermarked") is True:
            watermarked += 1
        if acquisition.get("status") == "uploaded":
            uploaded += 1
    manual_review = [row for row in bridge_rows if str(row.get("strategy") or "").strip().lower() == "manual_review"]
    return {
        "total_unique_images": len(merged_catalog),
        "sources": dict(sorted(source_counts.items())),
        "bridge_strategies_used": dict(sorted(strategy_counts.items())),
        "uploaded_assets": uploaded,
        "watermarked_references": watermarked,
        "manual_review_count": len(manual_review),
    }


def _render_source_distribution_text(report: dict[str, Any]) -> str:
    lines = ["Source Distribution", ""]
    lines.append(f"Total unique images: {report.get('total_unique_images', 0)}")
    lines.append(f"Uploaded assets: {report.get('uploaded_assets', 0)}")
    lines.append(f"Watermarked references kept: {report.get('watermarked_references', 0)}")
    lines.append(f"Manual review rows: {report.get('manual_review_count', 0)}")
    lines.append("")
    lines.append("Sources:")
    for source, count in (report.get("sources") or {}).items():
        lines.append(f"- {source}: {count}")
    lines.append("")
    lines.append("Bridge strategies:")
    for strategy, count in (report.get("bridge_strategies_used") or {}).items():
        lines.append(f"- {strategy}: {count}")
    return "\n".join(lines)


def run_get_images_pipeline(
    *,
    person_name: str | None = None,
    person_id: str | None = None,
    show_name: str | None = None,
    season: int | None = None,
    episode: int | None = None,
    output_dir: str | os.PathLike[str],
    sources: Sequence[str] | str | None = None,
    getty_limit: int = 200,
    nbcumv_limit: int = 300,
    bravo_limit: int = 300,
    supplemental_limit: int = 100,
    imdb_id: str | None = None,
    tmdb_id: int | None = None,
    force_all: bool = False,
    getty_prefetched_assets: list[dict[str, Any]] | None = None,
    getty_prefetched_events: list[dict[str, Any]] | None = None,
    getty_prefetched_queries: list[dict[str, Any]] | None = None,
    getty_prefetch_mode: str | None = None,
    getty_prefetch_auth_mode: str | None = None,
    getty_prefetch_auth_warning: str | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict[str, Any]:
    mode = "person" if person_name else "show"
    if mode == "person" and not person_name:
        raise ValueError("person_name is required for person mode")
    if mode == "show" and not show_name:
        raise ValueError("show_name is required for show mode")

    selected_sources = _normalize_sources(sources, mode=mode)
    selected_families = _selected_source_families(selected_sources, mode=mode)
    refreshed_artifacts = _refreshed_artifacts(selected_sources, mode=mode)

    output_root = Path(output_dir)
    raw_dir = output_root / "raw"
    reports_dir = output_root / "reports"
    raw_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "mode": mode,
        "person": person_name,
        "show": show_name,
        "season": season,
        "episode": episode,
        "selected_sources": list(selected_sources),
        "selected_source_families": list(selected_families),
        "refreshed_artifacts": list(refreshed_artifacts),
        "reused_caches": [],
        "executed_at": datetime.now(UTC).isoformat(),
        "counts": {},
        "notes": [],
    }
    normalized_getty_prefetch_mode = str(getty_prefetch_mode or "").strip().lower() or None
    if getty_prefetch_auth_mode:
        manifest["getty_prefetch_auth_mode"] = getty_prefetch_auth_mode
    if getty_prefetch_auth_warning:
        manifest["getty_prefetch_auth_warning"] = getty_prefetch_auth_warning

    raw_payloads: dict[str, Any] = {}

    def _load_or_collect(name: str, collector: Callable[[], Any]) -> Any:
        artifact_path = raw_dir / f"{name}.json"
        should_refresh = force_all or name in refreshed_artifacts or not artifact_path.exists()
        if should_refresh:
            _emit(progress_cb, f"Collecting {name}...")
            payload = collector()
            _write_json(artifact_path, payload)
        else:
            manifest["reused_caches"].append(name)
            _emit(progress_cb, f"Reusing cached {name} artifact.")
            payload = _read_json(artifact_path)
        raw_payloads[name] = payload
        manifest["counts"][name] = len(payload) if isinstance(payload, list) else 0
        return payload

    if "getty" in selected_families:
        if isinstance(getty_prefetched_events, list):
            _write_json(raw_dir / "getty_prefetched_events.json", getty_prefetched_events)
            manifest["counts"]["getty_prefetched_events"] = len(getty_prefetched_events)
        if isinstance(getty_prefetched_queries, list):
            _write_json(raw_dir / "getty_prefetched_queries.json", getty_prefetched_queries)
            manifest["counts"]["getty_prefetched_queries"] = len(getty_prefetched_queries)
        if mode == "person":
            if isinstance(getty_prefetched_assets, list):
                prefetch_mode_label = normalized_getty_prefetch_mode or "full"
                manifest["notes"].append(
                    "getty_prefetched_assets supplied; "
                    f"skipping live Getty search ({prefetch_mode_label})."
                )
                _load_or_collect("getty", lambda: list(getty_prefetched_assets))
            else:
                _load_or_collect("getty", lambda: _collect_getty_person(person_name or "", limit=getty_limit))
            _load_or_collect(
                "nbcumv",
                lambda: _collect_nbcumv_person(person_name or "", show_name=show_name, limit=nbcumv_limit),
            )
            _load_or_collect(
                "bravo",
                lambda: _collect_bravo_person(person_name or "", limit=bravo_limit, show_name=show_name),
            )
        else:
            if isinstance(getty_prefetched_assets, list):
                prefetch_mode_label = normalized_getty_prefetch_mode or "full"
                manifest["notes"].append(
                    "getty_prefetched_assets supplied; "
                    f"skipping live Getty search ({prefetch_mode_label})."
                )
                _load_or_collect("getty", lambda: list(getty_prefetched_assets))
            else:
                _load_or_collect(
                    "getty",
                    lambda: _collect_getty_show(show_name or "", season=season, episode=episode, limit=getty_limit),
                )
            _load_or_collect(
                "nbcumv",
                lambda: _collect_nbcumv_show(show_name or "", season=season, episode=episode, limit=nbcumv_limit),
            )
            _load_or_collect("bravo", lambda: _collect_bravo_show(show_name or "", season=season, limit=bravo_limit))

    if mode == "person":
        if "imdb" in selected_families:
            _load_or_collect(
                "imdb",
                lambda: _collect_person_supplemental_sources(
                    person_name=person_name or "",
                    person_id=person_id,
                    imdb_id=imdb_id,
                    tmdb_id=tmdb_id,
                    limit=supplemental_limit,
                )["imdb"],
            )
            if not raw_payloads.get("imdb"):
                manifest["notes"].append("IMDb skipped or empty. Provide an IMDb external ID if lookup is unavailable.")
        if "tmdb" in selected_families:
            _load_or_collect(
                "tmdb",
                lambda: _collect_person_supplemental_sources(
                    person_name=person_name or "",
                    person_id=person_id,
                    imdb_id=imdb_id,
                    tmdb_id=tmdb_id,
                    limit=supplemental_limit,
                )["tmdb"],
            )
            if not raw_payloads.get("tmdb"):
                manifest["notes"].append("TMDb skipped or empty. Provide a TMDb external ID if lookup is unavailable.")
        if "fandom" in selected_families:
            _load_or_collect(
                "fandom",
                lambda: _collect_person_supplemental_sources(
                    person_name=person_name or "",
                    person_id=person_id,
                    imdb_id=imdb_id,
                    tmdb_id=tmdb_id,
                    limit=supplemental_limit,
                )["fandom"],
            )
            if not raw_payloads.get("fandom"):
                manifest["notes"].append("Fandom skipped or empty. No matching gallery card images were collected.")
    elif any(source in selected_families for source in ("imdb", "tmdb")):
        manifest["notes"].append("IMDb/TMDb selection is person-mode only and was ignored for this show run.")

    bridge_rows: list[dict[str, Any]] = []
    merged_catalog: list[dict[str, Any]] = []
    if any(name in raw_payloads for name in GETTY_FAMILY_ARTIFACTS):
        bridge_rows, merged_catalog = build_bridge_and_catalog(raw_payloads)
        for record in merged_catalog:
            record["acquisition"] = acquire_best_image(record)

    supplemental_catalog: dict[str, Any] = {}
    if mode == "person":
        if isinstance(raw_payloads.get("imdb"), list):
            supplemental_catalog["imdb"] = _mirror_supplemental_rows(raw_payloads["imdb"])
        if isinstance(raw_payloads.get("tmdb"), list):
            supplemental_catalog["tmdb"] = _mirror_supplemental_rows(raw_payloads["tmdb"])
        if isinstance(raw_payloads.get("fandom"), list):
            supplemental_catalog["fandom"] = _mirror_supplemental_rows(raw_payloads["fandom"])

    source_distribution = _build_source_distribution_report(merged_catalog, bridge_rows)
    _write_json(output_root / "bridge_table.json", bridge_rows)
    _write_json(output_root / "merged_catalog.json", merged_catalog)
    _write_json(reports_dir / "source_distribution.json", source_distribution)
    _write_text(reports_dir / "source_distribution.txt", _render_source_distribution_text(source_distribution))

    if supplemental_catalog:
        _write_json(output_root / "supplemental_cast_photos.json", supplemental_catalog)

    show_summary: dict[str, Any] = {
        "mode": mode,
        "person": person_name,
        "show": show_name,
        "season": season,
        "episode": episode,
        "total_merged_records": len(merged_catalog),
        "supplemental_sources": {key: len(value) for key, value in supplemental_catalog.items()},
    }

    if mode == "show":
        assignment = _assign_to_people(merged_catalog, output_dir=output_root)
        assignment.update({"show": show_name, "season": season})
        _write_json(reports_dir / "person_assignment.json", assignment)
        show_summary.update(assignment)

    _write_json(output_root / "show_summary.json", show_summary)
    _write_json(output_root / "run_manifest.json", manifest)

    return {
        "manifest": manifest,
        "bridge_table_path": str(output_root / "bridge_table.json"),
        "merged_catalog_path": str(output_root / "merged_catalog.json"),
        "reports_path": str(reports_dir),
        "supplemental_catalog_path": str(output_root / "supplemental_cast_photos.json")
        if supplemental_catalog
        else None,
        "show_summary_path": str(output_root / "show_summary.json"),
    }
