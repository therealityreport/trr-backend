"""
Unified cast photo source fetchers.

Each fetcher returns normalized rows ready for upsert into core.cast_photos.
All rows include:
- person_id (UUID)
- source (imdb|tmdb|fandom|fandom-gallery)
- url (non-null)
- url_path
- image_url_canonical (for deduplication)
- source-specific fields
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

try:
    import requests
except ImportError:
    requests = None


def _canonical_url(url: str) -> str:
    """Create a canonical URL for deduplication (remove query params, lowercase)."""
    if "?" in url:
        url = url.split("?")[0]
    return url.lower().strip()


def _url_path_with_query(raw_url: str | None) -> str | None:
    """Extract path (with query) from a URL."""
    if not raw_url:
        return None
    parsed = urlparse(raw_url)
    if not parsed.path:
        return None
    if parsed.query:
        return f"{parsed.path}?{parsed.query}"
    return parsed.path


def _url_hash(url: str) -> str:
    """Generate a short hash of a URL for source_image_id."""
    return hashlib.sha256(_canonical_url(url).encode()).hexdigest()[:16]


def _normalize_fandom_section_label(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"[_-]+", " ", value).strip()
    if not cleaned:
        return None
    if cleaned.lower() == cleaned:
        return cleaned.title()
    return cleaned


def _extract_season_number(*values: str | None) -> int | None:
    for value in values:
        if not value:
            continue
        match = re.search(r"\b(?:season|s)\s*([0-9]{1,2})\b", value, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
        match = re.search(r"\b([0-9]{1,2})\b", value)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None


def _infer_fandom_section_tag(*values: str | None) -> str | None:
    text = " ".join([v for v in values if v]).strip().lower()
    if not text:
        return None
    if "confessional" in text:
        return "CONFESSIONAL"
    if "intro" in text or "tagline" in text or "opening" in text:
        return "INTRO"
    if "reunion" in text:
        return "REUNION"
    if "promo" in text or "promotional" in text:
        return "PROMO"
    if "episode" in text or "still" in text:
        return "EPISODE STILL"
    return "OTHER"


_REAL_HOUSEWIVES_SHOW_PATTERN = re.compile(
    r"(The\s+Real\s+Housewives\s+of\s+[A-Za-z][A-Za-z '&.-]*?)"
    r"(?=\s+(?:Season|S)\s*[0-9]{1,2}\b|\s+(?:Episode|Ep)\s*[0-9]{1,3}\b|\s+(?:Reunion|Confessional|Promo|Promotional|Tagline|Intro|Opening)\b|$)",
    re.IGNORECASE,
)

_REAL_HOUSEWIVES_CODE_BY_LOCATION: dict[str, str] = {
    "orange county": "RHOC",
    "new york city": "RHONY",
    "new jersey": "RHONJ",
    "atlanta": "RHOA",
    "beverly hills": "RHOBH",
    "potomac": "RHOP",
    "dallas": "RHOD",
    "miami": "RHOM",
    "salt lake city": "RHOSLC",
    "washington d.c.": "RHODC",
    "washington dc": "RHODC",
    "dubai": "RHODubai",
}

_REAL_HOUSEWIVES_CODE_PATTERN = re.compile(r"\bRHO(?:SLC|BH|NY|NJ|OC|DC|A|P|D|M)\b", re.IGNORECASE)


def _extract_show_title(*values: str | None) -> str | None:
    for value in values:
        if not value:
            continue
        normalized = " ".join(value.split())
        if not normalized:
            continue
        match = _REAL_HOUSEWIVES_SHOW_PATTERN.search(normalized)
        if match:
            return match.group(1).strip()
    return None


def _extract_show_code_from_text(*values: str | None) -> str | None:
    for value in values:
        if not value:
            continue
        match = _REAL_HOUSEWIVES_CODE_PATTERN.search(value)
        if match:
            return match.group(0).upper()
    return None


def _derive_show_short_code(show_title: str | None, *values: str | None) -> str | None:
    text_code = _extract_show_code_from_text(*values)
    if text_code:
        return text_code
    if not show_title:
        return None
    normalized = " ".join(show_title.split()).strip().lower()
    prefix = "the real housewives of "
    if not normalized.startswith(prefix):
        return None
    location = normalized[len(prefix) :].strip()
    if not location:
        return None
    return _REAL_HOUSEWIVES_CODE_BY_LOCATION.get(location)


def _extract_episode_number(*values: str | None) -> int | None:
    for value in values:
        if not value:
            continue
        match = re.search(r"\b(?:episode|ep|e)\s*([0-9]{1,3})\b", value, re.IGNORECASE)
        if not match:
            continue
        try:
            return int(match.group(1))
        except ValueError:
            continue
    return None


def _build_fandom_metadata(
    *,
    section_tag: str | None,
    section_label: str | None,
    source_variant: str | None = None,
    source_page_url: str | None = None,
) -> dict[str, Any] | None:
    metadata: dict[str, Any] = {}
    if section_tag:
        metadata["fandom_section_tag"] = section_tag
    if section_label:
        metadata["fandom_section_label"] = section_label
    if source_variant:
        metadata["source_variant"] = source_variant
    if source_page_url:
        metadata["source_page_url"] = source_page_url
    return metadata or None


# ---------------------------------------------------------------------------
# IMDb Source
# ---------------------------------------------------------------------------


def _normalize_imdb_id_set(values: set[str] | None) -> set[str]:
    if not values:
        return set()
    normalized: set[str] = set()
    for value in values:
        token = str(value or "").strip().lower()
        if token:
            normalized.add(token)
    return normalized


def _normalize_keyword_list(values: list[str] | None) -> list[str]:
    if not values:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _imdb_row_people_count(row: dict[str, Any]) -> int | None:
    people_ids = row.get("people_imdb_ids")
    if isinstance(people_ids, list):
        count = len([item for item in people_ids if isinstance(item, str) and item.strip()])
        if count > 0:
            return count
    people_names = row.get("people_names")
    if isinstance(people_names, list):
        count = len([item for item in people_names if isinstance(item, str) and item.strip()])
        if count > 0:
            return count
    return None


def _imdb_row_matches_filters(
    row: dict[str, Any],
    *,
    allowed_title_imdb_ids: set[str],
    allowed_title_keywords: list[str],
) -> bool:
    if not allowed_title_imdb_ids and not allowed_title_keywords:
        return True

    title_ids = row.get("title_imdb_ids")
    if isinstance(title_ids, list):
        for raw_id in title_ids:
            candidate = str(raw_id or "").strip().lower()
            if candidate and candidate in allowed_title_imdb_ids:
                return True

    if allowed_title_keywords:
        title_names = row.get("title_names") if isinstance(row.get("title_names"), list) else []
        caption = row.get("caption")
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        source_page_title = metadata.get("source_page_title") if isinstance(metadata, dict) else None
        haystack = " ".join(
            [
                *(str(value) for value in title_names if isinstance(value, str)),
                str(caption) if isinstance(caption, str) else "",
                str(source_page_title) if isinstance(source_page_title, str) else "",
            ]
        ).lower()
        if haystack:
            for keyword in allowed_title_keywords:
                if keyword in haystack:
                    return True

    return False


def _imdb_row_priority(row: dict[str, Any]) -> tuple[int, int]:
    people_count = _imdb_row_people_count(row)
    if people_count == 1:
        return (0, 1)
    if isinstance(people_count, int) and people_count > 1:
        return (2, people_count)
    return (1, 99)


def _normalize_person_name(value: str | None) -> str:
    if not isinstance(value, str):
        return ""
    normalized = re.sub(r"\s+", " ", value).strip().lower()
    normalized = re.sub(r"[^a-z0-9 ]+", "", normalized)
    return normalized


def _default_imdb_diagnostics() -> dict[str, int]:
    return {
        "imdb_pages_scanned": 0,
        "imdb_candidates_seen": 0,
        "imdb_kept": 0,
        "imdb_filtered_type": 0,
        "imdb_filtered_people": 0,
        "imdb_filtered_episode": 0,
        "imdb_filtered_other": 0,
    }


def fetch_imdb_cast_photos(
    imdb_person_id: str,
    person_id: str | UUID,
    *,
    limit: int = 50,
    allowed_title_imdb_ids: set[str] | None = None,
    allowed_title_keywords: list[str] | None = None,
    prioritize_solo_people: bool = False,
    strict_types: set[str] | None = None,
    target_person_imdb_id: str | None = None,
    target_person_name: str | None = None,
    allowed_cast_imdb_ids: set[str] | None = None,
    allowed_cast_names: set[str] | None = None,
    allowed_episode_imdb_ids: set[str] | None = None,
    strict_mode_enabled: bool = False,
    imdb_diagnostics: dict[str, int] | None = None,
    session: requests.Session | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from IMDb person gallery.

    Args:
        imdb_person_id: IMDb person ID (nm...)
        person_id: core.people UUID
        limit: Max photos to fetch
        allowed_title_imdb_ids: Optional IMDb title ID filter (show/episode IDs)
        allowed_title_keywords: Optional case-insensitive title/caption keywords
        prioritize_solo_people: Rank single-person images first before applying limit
        strict_types: Optional allowed IMDb image types (e.g., {"event", "still_frame"})
        target_person_imdb_id: IMDb ID of person whose gallery is being refreshed
        target_person_name: Full name of person whose gallery is being refreshed
        allowed_cast_imdb_ids: IMDb IDs of allowed cast members for strict filtering
        allowed_cast_names: Full names of allowed cast members for strict filtering
        allowed_episode_imdb_ids: Episode IMDb IDs allowed for still-frame fallback
        strict_mode_enabled: Enable strict Traitors-focused filtering rules
        imdb_diagnostics: Optional mutable diagnostics dict to populate in-place
        session: Optional requests session for connection reuse
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.integrations.imdb.person_gallery import (
        fetch_imdb_person_mediaindex_html,
        fetch_imdb_person_mediaindex_page,
        fetch_imdb_person_mediaviewer_html,
        parse_imdb_person_mediaindex_payload,
        parse_imdb_person_mediaindex_state,
        parse_imdb_person_mediaviewer_details,
    )

    try:
        media_html = fetch_imdb_person_mediaindex_html(imdb_person_id, session=session)
        images, page_info = parse_imdb_person_mediaindex_state(media_html, imdb_person_id)
    except Exception as exc:
        if verbose:
            print(f"  WARN IMDb mediaindex {imdb_person_id}: {exc}")
        return []

    diagnostics = _default_imdb_diagnostics()
    if isinstance(imdb_diagnostics, dict):
        diagnostics.update({key: int(imdb_diagnostics.get(key, 0) or 0) for key in diagnostics})

    if not images:
        if isinstance(imdb_diagnostics, dict):
            imdb_diagnostics.update(diagnostics)
        return []

    normalized_title_ids = _normalize_imdb_id_set(allowed_title_imdb_ids)
    normalized_keywords = _normalize_keyword_list(allowed_title_keywords)
    strict_enabled = bool(strict_mode_enabled)
    should_expand_scan = bool(normalized_title_ids or normalized_keywords or prioritize_solo_people or strict_enabled)
    pages_fetched = 1
    if should_expand_scan:
        max_pages = 3
        if limit:
            max_pages = min(10, max(3, (int(limit) + 49) // 50 + 2))
        cursor = page_info.get("end_cursor")
        has_next = bool(page_info.get("has_next_page"))
        seen_keys = {
            str(image.get("source_image_id") or image.get("viewer_id") or "").strip().casefold()
            for image in images
            if str(image.get("source_image_id") or image.get("viewer_id") or "").strip()
        }

        while has_next and cursor and pages_fetched < max_pages:
            try:
                payload = fetch_imdb_person_mediaindex_page(
                    imdb_person_id,
                    after_cursor=cursor,
                    first=50,
                    session=session,
                )
                next_images, next_page_info = parse_imdb_person_mediaindex_payload(payload, imdb_person_id)
            except Exception as exc:  # noqa: BLE001
                if verbose:
                    print(f"  WARN IMDb mediaindex page {imdb_person_id} after={cursor[:24]}...: {exc}")
                break

            pages_fetched += 1
            for image in next_images:
                key = str(image.get("source_image_id") or image.get("viewer_id") or "").strip().casefold()
                if not key or key in seen_keys:
                    continue
                seen_keys.add(key)
                images.append(image)
            has_next = bool(next_page_info.get("has_next_page"))
            next_cursor = next_page_info.get("end_cursor")
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

    diagnostics["imdb_pages_scanned"] = pages_fetched

    if limit and not should_expand_scan:
        images = images[:limit]
    mediaindex_url_path = f"/name/{imdb_person_id}/mediaindex/"
    rows: list[dict[str, Any]] = []
    strict_types_normalized = {str(v or "").strip().lower() for v in (strict_types or set()) if str(v or "").strip()}
    target_person_imdb_id_norm = str(target_person_imdb_id or "").strip().lower() or None
    target_person_name_norm = _normalize_person_name(target_person_name)
    allowed_cast_imdb_ids_norm = {
        str(v or "").strip().lower() for v in (allowed_cast_imdb_ids or set()) if str(v or "").strip()
    }
    if target_person_imdb_id_norm:
        allowed_cast_imdb_ids_norm.add(target_person_imdb_id_norm)
    allowed_cast_names_norm = {
        _normalize_person_name(str(v or "").strip()) for v in (allowed_cast_names or set()) if str(v or "").strip()
    }
    allowed_cast_names_norm = {v for v in allowed_cast_names_norm if v}
    if target_person_name_norm:
        allowed_cast_names_norm.add(target_person_name_norm)
    allowed_episode_imdb_ids_norm = {
        str(v or "").strip().lower() for v in (allowed_episode_imdb_ids or set()) if str(v or "").strip()
    }
    for image in images:
        viewer_id = image.get("viewer_id")
        details: dict[str, Any] = {}

        if viewer_id:
            try:
                viewer_html = fetch_imdb_person_mediaviewer_html(imdb_person_id, viewer_id, session=session)
                details = parse_imdb_person_mediaviewer_details(viewer_html, viewer_id=viewer_id)
            except Exception as exc:
                if verbose:
                    print(f"    WARN mediaviewer {viewer_id}: {exc}")

        # Choose best URL (prefer details if higher resolution)
        url = image.get("url")
        width = image.get("width")
        height = image.get("height")
        url_path = image.get("url_path")

        if details.get("url") and details.get("width"):
            if width is None or details["width"] >= width:
                url = details["url"]
                width = details["width"]
                height = details.get("height")
                url_path = details.get("url_path")

        if not url:
            continue

        source_image_id = str(image.get("source_image_id") or "").strip()
        if not source_image_id:
            continue

        diagnostics["imdb_candidates_seen"] += 1
        image_type_raw = image.get("image_type")
        if not (isinstance(image_type_raw, str) and image_type_raw.strip()):
            image_type_raw = details.get("image_type")
        image_type = (
            str(image_type_raw).strip().lower()
            if isinstance(image_type_raw, str) and image_type_raw.strip()
            else None
        )

        tags: dict[str, Any] = {}
        people_ids = details.get("people_imdb_ids") or []
        people_names = details.get("people_names") or []
        people: list[dict[str, Any]] = []
        for idx, imdb_id in enumerate(people_ids):
            name = people_names[idx] if idx < len(people_names) else None
            people.append({"imdb_id": imdb_id, "name": name})
        if people:
            tags["people"] = people

        title_ids = details.get("title_imdb_ids") or []
        title_names = details.get("title_names") or []
        titles: list[dict[str, Any]] = []
        for idx, imdb_id in enumerate(title_ids):
            title = title_names[idx] if idx < len(title_names) else None
            titles.append({"imdb_id": imdb_id, "title": title})
        if titles:
            tags["titles"] = titles

        caption = details.get("caption")
        if not caption:
            image_caption = image.get("caption")
            caption = image_caption if isinstance(image_caption, str) and image_caption.strip() else None
        if caption:
            tags["caption_plain"] = caption

        metadata: dict[str, Any] | None = None
        if tags:
            metadata = {"tags": tags}

        primary_title: str | None = None
        if isinstance(title_names, list):
            for title_name in title_names:
                if isinstance(title_name, str) and title_name.strip():
                    primary_title = title_name.strip()
                    break
        if primary_title is None and isinstance(caption, str) and caption.strip():
            caption_match = re.search(r"\bin\s+(.+?)\s*\((\d{4})\)\s*$", caption.strip(), re.IGNORECASE)
            if caption_match:
                inferred_title = caption_match.group(1).strip(" \"'.,")
                if inferred_title:
                    primary_title = inferred_title

        source_page_url = None
        if viewer_id:
            source_page_url = f"https://www.imdb.com/name/{imdb_person_id}/mediaviewer/{viewer_id}/"
        else:
            source_page_url = f"https://www.imdb.com/name/{imdb_person_id}/mediaindex/"

        if metadata is None:
            metadata = {}
        metadata["source_variant"] = "imdb_person_gallery"
        metadata["source_logo"] = "IMDb"
        metadata["source_page_url"] = source_page_url
        metadata["source_file_url"] = url
        metadata["source_image_url"] = url
        metadata["imdb_person_id"] = imdb_person_id
        if image_type:
            metadata["imdb_image_type"] = image_type
        if viewer_id:
            metadata["imdb_viewer_id"] = viewer_id
        if primary_title:
            metadata["source_page_title"] = primary_title
            metadata["asset_name"] = primary_title
            metadata["name"] = primary_title
        metadata["imdb_metadata_refreshed_at"] = datetime.now(UTC).isoformat()

        if strict_enabled:
            type_ok = image_type in strict_types_normalized if strict_types_normalized else True
            if not type_ok:
                diagnostics["imdb_filtered_type"] += 1
                continue

            people_ids_norm = {
                str(value or "").strip().lower() for value in people_ids if isinstance(value, str) and value.strip()
            }
            people_names_norm = {
                _normalize_person_name(str(value or "").strip())
                for value in people_names
                if isinstance(value, str) and str(value or "").strip()
            }
            people_names_norm = {value for value in people_names_norm if value}
            title_ids_norm = {
                str(value or "").strip().lower() for value in title_ids if isinstance(value, str) and value.strip()
            }

            solo_self_ok = False
            if target_person_imdb_id_norm and people_ids_norm:
                solo_self_ok = len(people_ids_norm) == 1 and target_person_imdb_id_norm in people_ids_norm
            elif target_person_name_norm and people_names_norm:
                solo_self_ok = len(people_names_norm) == 1 and target_person_name_norm in people_names_norm

            cast_group_ok = False
            if target_person_imdb_id_norm and people_ids_norm and allowed_cast_imdb_ids_norm:
                cast_group_ok = (
                    len(people_ids_norm) >= 2
                    and target_person_imdb_id_norm in people_ids_norm
                    and people_ids_norm.issubset(allowed_cast_imdb_ids_norm)
                )
            elif target_person_name_norm and people_names_norm and allowed_cast_names_norm:
                cast_group_ok = (
                    len(people_names_norm) >= 2
                    and target_person_name_norm in people_names_norm
                    and people_names_norm.issubset(allowed_cast_names_norm)
                )

            episode_still_ok = bool(
                image_type == "still_frame"
                and title_ids_norm
                and allowed_episode_imdb_ids_norm
                and bool(title_ids_norm.intersection(allowed_episode_imdb_ids_norm))
            )

            if solo_self_ok:
                metadata["imdb_filter_reason"] = "solo_self"
            elif cast_group_ok:
                metadata["imdb_filter_reason"] = "traitors_cast_group"
            elif episode_still_ok:
                metadata["imdb_filter_reason"] = "episode_still_frame"
            else:
                if image_type == "still_frame":
                    diagnostics["imdb_filtered_episode"] += 1
                else:
                    diagnostics["imdb_filtered_people"] += 1
                continue
            metadata["imdb_filter_scope"] = "traitors_strict"

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "imdb",
                "source_image_id": source_image_id,
                "viewer_id": viewer_id,
                "mediaindex_url_path": mediaindex_url_path,
                "mediaviewer_url_path": image.get("mediaviewer_url_path"),
                "url": url,
                "url_path": url_path,
                "image_url": url,
                "image_url_canonical": _canonical_url(url),
                "width": width,
                "height": height,
                "caption": caption,
                "gallery_index": details.get("gallery_index"),
                "gallery_total": details.get("gallery_total"),
                "people_imdb_ids": details.get("people_imdb_ids"),
                "people_names": details.get("people_names"),
                "title_imdb_ids": details.get("title_imdb_ids"),
                "title_names": details.get("title_names"),
                "source_page_url": source_page_url,
                "metadata": metadata,
                "fetched_at": datetime.now(UTC).isoformat(),
            }
        )

    if (normalized_title_ids or normalized_keywords) and not strict_enabled:
        before_count = len(rows)
        rows = [
            row
            for row in rows
            if _imdb_row_matches_filters(
                row,
                allowed_title_imdb_ids=normalized_title_ids,
                allowed_title_keywords=normalized_keywords,
            )
        ]
        diagnostics["imdb_filtered_other"] += max(0, before_count - len(rows))
    if strict_enabled:
        reason_rank = {"solo_self": 0, "traitors_cast_group": 1, "episode_still_frame": 2}

        def _strict_priority(row: dict[str, Any]) -> tuple[int, int, int]:
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            reason = str(metadata.get("imdb_filter_reason") or "").strip().lower()
            rank = reason_rank.get(reason, 3)
            people_count = _imdb_row_people_count(row)
            people_rank = people_count if isinstance(people_count, int) and people_count >= 0 else 99
            gallery_index = row.get("gallery_index")
            gallery_rank = gallery_index if isinstance(gallery_index, int) and gallery_index >= 0 else 999999
            return rank, people_rank, gallery_rank

        rows.sort(key=_strict_priority)
    elif prioritize_solo_people:
        rows.sort(key=_imdb_row_priority)
    if limit:
        rows = rows[:limit]
    diagnostics["imdb_kept"] = len(rows)
    if isinstance(imdb_diagnostics, dict):
        imdb_diagnostics.update(diagnostics)
    return rows


# ---------------------------------------------------------------------------
# TMDb Source
# ---------------------------------------------------------------------------


def fetch_tmdb_cast_photos(
    tmdb_person_id: int,
    person_id: str | UUID,
    imdb_person_id: str | None = None,
    *,
    limit: int = 50,
    api_key: str | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from TMDb person profile images.

    Args:
        tmdb_person_id: TMDb person ID (integer)
        person_id: core.people UUID
        imdb_person_id: Optional IMDb ID for cross-reference
        limit: Max photos to fetch
        api_key: TMDb API key (or from env)
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.ingestion.tmdb_person_images import (
        build_tmdb_image_url,
        fetch_tmdb_person_profile_images,
    )

    try:
        images = fetch_tmdb_person_profile_images(tmdb_person_id, api_key=api_key)
    except Exception as exc:
        if verbose:
            print(f"  WARN TMDb person images {tmdb_person_id}: {exc}")
        return []

    if not images:
        return []

    images = images[:limit] if limit else images
    rows: list[dict[str, Any]] = []
    now = datetime.now(UTC).isoformat()

    for idx, img in enumerate(images):
        file_path = img.get("file_path")
        if not file_path:
            continue

        full_url = build_tmdb_image_url(file_path)

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "tmdb",
                "source_image_id": file_path,
                "url": full_url,
                "url_path": file_path,
                "image_url": full_url,
                "image_url_canonical": full_url,
                "width": img.get("width"),
                "height": img.get("height"),
                "position": idx + 1,
                "gallery_index": idx + 1,
                "gallery_total": len(images),
                "fetched_at": now,
                "metadata": {
                    "tmdb_person_id": tmdb_person_id,
                    "aspect_ratio": img.get("aspect_ratio"),
                    "vote_average": img.get("vote_average"),
                    "vote_count": img.get("vote_count"),
                },
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Fandom Person Page Source
# ---------------------------------------------------------------------------


def fetch_fandom_person_cast_photos(
    person_name: str,
    person_id: str | UUID,
    imdb_person_id: str | None = None,
    *,
    limit: int = 50,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from Fandom person wiki page.

    Args:
        person_name: Person name for wiki page lookup
        person_id: core.people UUID
        imdb_person_id: Optional IMDb ID for cross-reference
        limit: Max photos to fetch
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.ingestion.fandom_person_scraper import (
        fetch_fandom_person_html,
        parse_fandom_person_html,
    )

    # Build Fandom wiki URL from name
    wiki_name = person_name.replace(" ", "_")
    url = f"https://real-housewives.fandom.com/wiki/{wiki_name}"

    try:
        html, source_page_url = fetch_fandom_person_html(url)
        metadata, photos = parse_fandom_person_html(html, source_url=source_page_url)
        result = {"photos": photos, **metadata}
    except Exception as exc:
        if verbose:
            print(f"  WARN Fandom person {person_name}: {exc}")
        return []

    photos = result.get("photos", [])
    if not photos:
        return []

    photos = photos[:limit] if limit else photos
    rows: list[dict[str, Any]] = []
    now = datetime.now(UTC).isoformat()
    page_title = str(result.get("page_title") or "").strip() or None
    default_show_title = str(result.get("installment") or "").strip() or None

    for photo in photos:
        image_url = photo.get("url") or photo.get("image_url")
        if not image_url:
            continue

        section_label = _normalize_fandom_section_label(photo.get("context_section"))
        caption_text = str(photo.get("caption") or "").strip() or None
        alt_text = str(photo.get("alt_text") or "").strip() or None
        section_tag = _infer_fandom_section_tag(
            photo.get("context_type"),
            section_label,
            caption_text,
            alt_text,
        )
        metadata = _build_fandom_metadata(
            section_tag=section_tag,
            section_label=section_label,
            source_page_url=source_page_url,
        )
        if metadata is None:
            metadata = {}
        if section_tag:
            metadata.setdefault("content_type", section_tag)

        season_value = photo.get("season")
        season_number = season_value if isinstance(season_value, int) else _extract_season_number(
            section_label, caption_text, alt_text
        )
        if isinstance(season_number, int):
            metadata.setdefault("season_number", season_number)

        episode_number = _extract_episode_number(section_label, caption_text, alt_text)
        if isinstance(episode_number, int):
            metadata.setdefault("episode_number", episode_number)

        show_title = _extract_show_title(section_label, caption_text, alt_text) or default_show_title
        if show_title:
            metadata.setdefault("show_name", show_title)
            metadata.setdefault("show_title", show_title)

        show_short_code = _derive_show_short_code(show_title, section_label, caption_text, alt_text)
        if show_short_code:
            metadata.setdefault("show_short_code", show_short_code)

        if page_title:
            metadata.setdefault("source_page_title", page_title)
            metadata.setdefault("fandom_page_title", page_title)
            metadata.setdefault("person_name", page_title)

        if show_title and isinstance(season_number, int):
            metadata.setdefault("asset_name", f"{show_title} Season {season_number}")
            metadata.setdefault("name", f"{show_title} Season {season_number}")
        elif show_title:
            metadata.setdefault("asset_name", show_title)
            metadata.setdefault("name", show_title)
        elif page_title:
            metadata.setdefault("asset_name", page_title)
            metadata.setdefault("name", page_title)

        tags = metadata.get("tags")
        if not isinstance(tags, dict):
            tags = {}
        if page_title:
            tags.setdefault("people", [{"name": page_title}])
        if show_title:
            tags.setdefault("titles", [{"title": show_title}])
        if tags:
            metadata["tags"] = tags

        # Ensure url and url_path are never null
        url_value = image_url
        url_path = photo.get("url_path") or _url_path_with_query(image_url) or image_url
        title_names = [show_title] if show_title else None
        people_names = [page_title] if page_title else None

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "fandom",
                "source_image_id": f"fandom-person-{_url_hash(image_url)}",
                "source_page_url": source_page_url,
                "url": url_value,
                "url_path": url_path,
                "image_url": image_url,
                "thumb_url": photo.get("thumb_url"),
                "image_url_canonical": _canonical_url(image_url),
                "width": photo.get("width"),
                "height": photo.get("height"),
                "caption": caption_text or alt_text,
                "context_section": photo.get("context_section"),
                "context_type": photo.get("context_type"),
                "season": season_number,
                "position": photo.get("position"),
                "people_names": people_names,
                "title_names": title_names,
                "metadata": metadata,
                "fetched_at": now,
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Fandom Gallery Source
# ---------------------------------------------------------------------------


def fetch_fandom_gallery_cast_photos(
    person_name: str,
    person_id: str | UUID,
    imdb_person_id: str | None = None,
    *,
    limit: int = 50,
    resolve_file_pages: bool = True,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from Fandom gallery page.

    Args:
        person_name: Person name for gallery page lookup
        person_id: core.people UUID
        imdb_person_id: Optional IMDb ID for cross-reference
        limit: Max photos to fetch
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.integrations.fandom import fetch_fandom_file_metadata, fetch_fandom_gallery

    try:
        gallery = fetch_fandom_gallery(person_name)
    except Exception as exc:
        if verbose:
            print(f"  WARN Fandom gallery {person_name}: {exc}")
        return []

    if gallery.error:
        if verbose:
            print(f"  WARN Fandom gallery {person_name}: {gallery.error}")
        return []

    if not gallery.images:
        return []

    images = gallery.images[:limit] if limit else gallery.images
    rows: list[dict[str, Any]] = []
    now = datetime.now(UTC).isoformat()
    file_meta_cache: dict[str, Any] = {}

    for image in images:
        image_url = image.url
        if not image_url:
            continue

        section_label = _normalize_fandom_section_label(image.section_label)
        section_tag = _infer_fandom_section_tag(section_label, image.caption)
        season = _extract_season_number(section_label, image.caption)
        file_page_url = image.file_page_url
        file_meta = None
        if resolve_file_pages and file_page_url:
            cached = file_meta_cache.get(file_page_url)
            if cached is None:
                try:
                    cached = fetch_fandom_file_metadata(file_page_url)
                except Exception as exc:
                    if verbose:
                        print(f"  WARN Fandom file {file_page_url}: {exc}")
                    cached = None
                file_meta_cache[file_page_url] = cached
            file_meta = cached

        resolved_url = image_url
        resolved_width = image.width
        resolved_height = image.height
        resolved_mime = None
        resolved_created_at = None
        if file_meta:
            resolved_url = file_meta.file_url or resolved_url
            resolved_width = file_meta.width or resolved_width
            resolved_height = file_meta.height or resolved_height
            resolved_mime = file_meta.mime_type
            resolved_created_at = file_meta.created_at

        metadata = _build_fandom_metadata(
            section_tag=section_tag,
            section_label=section_label,
            source_variant="fandom_gallery",
            source_page_url=file_page_url or image.source_page_url,
        )
        if metadata is None:
            metadata = {}
        if image.source_page_url:
            metadata.setdefault("source_gallery_url", image.source_page_url)
        if file_page_url:
            metadata.setdefault("source_file_url", file_page_url)
        if season:
            metadata.setdefault("season_number", season)
        if resolved_width:
            metadata["image_width"] = resolved_width
        if resolved_height:
            metadata["image_height"] = resolved_height
        if resolved_mime:
            metadata["image_mime_type"] = resolved_mime
        if resolved_created_at:
            metadata["source_created_at"] = resolved_created_at

        # Ensure url and url_path are never null
        url_value = resolved_url or image_url
        url_path = _url_path_with_query(url_value) or url_value

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "fandom",
                "source_image_id": f"fandom-gallery-{_url_hash(url_value)}",
                "source_page_url": file_page_url or image.source_page_url or gallery.url,
                "url": url_value,
                "url_path": url_path,
                "image_url": url_value,
                "thumb_url": image.thumb_url,
                "image_url_canonical": _canonical_url(url_value),
                "caption": image.caption,
                "season": season,
                "width": resolved_width,
                "height": resolved_height,
                "metadata": metadata,
                "fetched_at": now,
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Unified Multi-Source Fetcher
# ---------------------------------------------------------------------------


def fetch_all_cast_photos(
    person_id: str | UUID,
    *,
    imdb_person_id: str | None = None,
    tmdb_person_id: int | None = None,
    person_name: str | None = None,
    sources: list[str] | None = None,
    limit_per_source: int = 50,
    imdb_allowed_title_imdb_ids: set[str] | None = None,
    imdb_allowed_title_keywords: list[str] | None = None,
    imdb_prioritize_solo_people: bool = False,
    imdb_strict_types: set[str] | None = None,
    imdb_target_person_imdb_id: str | None = None,
    imdb_target_person_name: str | None = None,
    imdb_allowed_cast_imdb_ids: set[str] | None = None,
    imdb_allowed_cast_names: set[str] | None = None,
    imdb_allowed_episode_imdb_ids: set[str] | None = None,
    imdb_strict_mode_enabled: bool = False,
    imdb_diagnostics: dict[str, int] | None = None,
    session: requests.Session | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from all enabled sources.

    Args:
        person_id: core.people UUID
        imdb_person_id: IMDb person ID (required for imdb source)
        tmdb_person_id: TMDb person ID (required for tmdb source)
        person_name: Person name (required for fandom sources)
        sources: List of sources to fetch from. Default: all available
        limit_per_source: Max photos per source
        imdb_allowed_title_imdb_ids: Optional IMDb title ID filter for person gallery rows
        imdb_allowed_title_keywords: Optional text filter for person gallery rows
        imdb_prioritize_solo_people: Rank single-person IMDb rows first before limit
        imdb_strict_types: Optional allowed IMDb image types for strict mode
        imdb_target_person_imdb_id: IMDb ID of person whose gallery is being refreshed
        imdb_target_person_name: Name of person whose gallery is being refreshed
        imdb_allowed_cast_imdb_ids: Allowed show-cast IMDb IDs for strict mode
        imdb_allowed_cast_names: Allowed show-cast names for strict mode
        imdb_allowed_episode_imdb_ids: Allowed episode IMDb IDs for still-frame fallback
        imdb_strict_mode_enabled: Enable strict IMDb filtering mode
        imdb_diagnostics: Optional mutable diagnostics dict populated by IMDb fetcher
        session: Optional requests session
        verbose: Print progress

    Returns:
        Combined list of photo dicts from all sources
    """
    all_sources = {"imdb", "tmdb", "fandom", "fandom-gallery"}
    enabled = set(sources) if sources else all_sources

    all_rows: list[dict[str, Any]] = []

    # IMDb source
    if "imdb" in enabled and imdb_person_id:
        if verbose:
            print(f"  Fetching IMDb photos for {imdb_person_id}...")
        rows = fetch_imdb_cast_photos(
            imdb_person_id,
            person_id,
            limit=limit_per_source,
            allowed_title_imdb_ids=imdb_allowed_title_imdb_ids,
            allowed_title_keywords=imdb_allowed_title_keywords,
            prioritize_solo_people=imdb_prioritize_solo_people,
            strict_types=imdb_strict_types,
            target_person_imdb_id=imdb_target_person_imdb_id,
            target_person_name=imdb_target_person_name,
            allowed_cast_imdb_ids=imdb_allowed_cast_imdb_ids,
            allowed_cast_names=imdb_allowed_cast_names,
            allowed_episode_imdb_ids=imdb_allowed_episode_imdb_ids,
            strict_mode_enabled=imdb_strict_mode_enabled,
            imdb_diagnostics=imdb_diagnostics,
            session=session,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} IMDb photos")
        all_rows.extend(rows)

    # TMDb source
    if "tmdb" in enabled and tmdb_person_id:
        if verbose:
            print(f"  Fetching TMDb photos for {tmdb_person_id}...")
        rows = fetch_tmdb_cast_photos(
            tmdb_person_id,
            person_id,
            imdb_person_id=imdb_person_id,
            limit=limit_per_source,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} TMDb photos")
        all_rows.extend(rows)

    # Fandom person page source
    if "fandom" in enabled and person_name:
        if verbose:
            print(f"  Fetching Fandom person photos for {person_name}...")
        rows = fetch_fandom_person_cast_photos(
            person_name,
            person_id,
            imdb_person_id=imdb_person_id,
            limit=limit_per_source,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} Fandom person photos")
        all_rows.extend(rows)

    # Fandom gallery source
    if "fandom-gallery" in enabled and person_name:
        if verbose:
            print(f"  Fetching Fandom gallery photos for {person_name}...")
        rows = fetch_fandom_gallery_cast_photos(
            person_name,
            person_id,
            imdb_person_id=imdb_person_id,
            limit=limit_per_source,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} Fandom gallery photos")
        all_rows.extend(rows)

    return all_rows
