"""Service composition for public cast and credit reads."""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal
from urllib.parse import urljoin

import requests

from trr_backend.integrations.imdb import name_filmography
from trr_backend.repositories import core_cast_credit_reads as repository

ShowCastView = Literal["membership", "episode_evidence", "archive_only"]
SeasonCastView = Literal["membership", "episode_counts"]
PhotoFallback = Literal["none", "bravo"]

_EMPTY_PHOTO = {
    "url": None,
    "thumbnail_focus_x": None,
    "thumbnail_focus_y": None,
    "thumbnail_zoom": None,
    "thumbnail_crop_mode": None,
}
_BRAVO_MAX_LIVE_FETCHES = 4
_BRAVO_MAX_HTML_BYTES = 1_000_000
_BRAVO_TIMEOUT = (1.0, 2.0)
_BRAVO_IMAGE_CACHE: dict[str, str | None] = {}
_META_TAG_RE = re.compile(r"<meta\s+[^>]*>", re.IGNORECASE)
_META_ATTR_RE = re.compile(r"([a-zA-Z_:.-]+)\s*=\s*[\"']([^\"']*)[\"']")

logger = logging.getLogger(__name__)


def _person_ids(rows: list[dict[str, Any]]) -> list[str]:
    return list(dict.fromkeys(str(row.get("person_id") or "") for row in rows if row.get("person_id")))


def _extract_bravo_image(html: str, profile_url: str) -> str | None:
    for tag in _META_TAG_RE.findall(html):
        attributes = {key.casefold(): value.strip() for key, value in _META_ATTR_RE.findall(tag)}
        marker = (attributes.get("property") or attributes.get("name") or "").casefold()
        if marker not in {"og:image", "twitter:image", "twitter:image:src"}:
            continue
        candidate = attributes.get("content")
        if not candidate:
            continue
        resolved = urljoin(profile_url, candidate)
        if not resolved.casefold().endswith((".mp4", ".mov", ".m3u8", ".webm", ".mp3", ".pdf", ".html")):
            return resolved
    return None


def _fetch_bravo_profile_image(profile_url: str) -> str | None:
    if profile_url in _BRAVO_IMAGE_CACHE:
        return _BRAVO_IMAGE_CACHE[profile_url]

    response: Any | None = None
    try:
        response = requests.get(
            profile_url,
            headers={
                "user-agent": "Mozilla/5.0",
                "accept-language": "en-US,en;q=0.9",
            },
            timeout=_BRAVO_TIMEOUT,
            stream=True,
        )
        if response.status_code != 200:
            _BRAVO_IMAGE_CACHE[profile_url] = None
            return None
        body = bytearray()
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            remaining = _BRAVO_MAX_HTML_BYTES - len(body)
            if remaining <= 0:
                break
            body.extend(chunk[:remaining])
            if len(body) >= _BRAVO_MAX_HTML_BYTES:
                break
        image_url = _extract_bravo_image(body.decode("utf-8", errors="replace"), profile_url)
        _BRAVO_IMAGE_CACHE[profile_url] = image_url
        return image_url
    except Exception:  # noqa: BLE001 - an optional remote image must never fail cast reads.
        _BRAVO_IMAGE_CACHE[profile_url] = None
        return None
    finally:
        if response is not None:
            response.close()


def _apply_bravo_fallback(
    person_ids: list[str],
    photos: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], int]:
    unresolved = [person_id for person_id in person_ids if person_id not in photos]
    if not unresolved:
        return photos, 0
    candidates, query_count = repository.get_bravo_photo_candidates(unresolved)
    pending: list[tuple[str, str]] = []
    for person_id in unresolved:
        candidate = candidates.get(person_id, {})
        image_url = candidate.get("image_url")
        if isinstance(image_url, str) and image_url.strip():
            photos[person_id] = {
                "url": image_url.strip(),
                **{key: value for key, value in _EMPTY_PHOTO.items() if key != "url"},
            }
            continue
        profile_url = candidate.get("profile_url")
        if isinstance(profile_url, str) and profile_url.strip():
            pending.append((person_id, profile_url.strip()))

    bounded_pending = pending[:_BRAVO_MAX_LIVE_FETCHES]
    if bounded_pending:
        with ThreadPoolExecutor(max_workers=min(len(bounded_pending), _BRAVO_MAX_LIVE_FETCHES)) as executor:
            image_urls = list(executor.map(lambda entry: _fetch_bravo_profile_image(entry[1]), bounded_pending))
        for (person_id, _profile_url), image_url in zip(bounded_pending, image_urls, strict=True):
            if image_url and person_id not in photos:
                photos[person_id] = {
                    "url": image_url,
                    **{key: value for key, value in _EMPTY_PHOTO.items() if key != "url"},
                }
    return photos, query_count


def get_show_cast(
    show_id: str,
    *,
    view: ShowCastView,
    limit: int | None = None,
    offset: int | None = None,
    include_photos: bool = True,
    photo_fallback: PhotoFallback = "none",
) -> tuple[list[dict[str, Any]], int]:
    """Compose one of the app-compatible show-cast projections."""

    rows, query_count = repository.list_show_cast(
        show_id,
        view=view,
        limit=limit,
        offset=offset,
    )
    if not rows:
        return [], query_count

    person_ids = _person_ids(rows)
    people, people_queries = repository.get_people_by_ids(person_ids)
    query_count += people_queries

    totals: dict[str, dict[str, Any]] = {}
    if view == "episode_evidence":
        totals, totals_queries = repository.get_show_cast_episode_totals(show_id, person_ids)
        query_count += totals_queries
    elif view == "archive_only":
        totals, totals_queries = repository.get_show_cast_archive_totals(show_id, person_ids)
        query_count += totals_queries

    photos: dict[str, dict[str, Any]] = {}
    if include_photos:
        photos, photo_queries = repository.get_preferred_cast_photos(person_ids)
        query_count += photo_queries
        if photo_fallback == "bravo":
            photos, bravo_queries = _apply_bravo_fallback(person_ids, photos)
            query_count += bravo_queries

    payload: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        eligible_total = row.pop("eligible_total_episodes", None)
        person_id = str(row.get("person_id") or "")
        person = people.get(person_id, {})
        evidence = totals.get(person_id, {})
        photo = photos.get(person_id, _EMPTY_PHOTO)
        full_name = person.get("full_name") or row.get("cast_member_name")
        if view in {"episode_evidence", "archive_only"}:
            full_name = full_name or evidence.get("person_name")

        shaped = {
            **row,
            "full_name": full_name,
            "known_for": person.get("known_for"),
            "photo_url": photo.get("url"),
            "thumbnail_focus_x": photo.get("thumbnail_focus_x"),
            "thumbnail_focus_y": photo.get("thumbnail_focus_y"),
            "thumbnail_zoom": photo.get("thumbnail_zoom"),
            "thumbnail_crop_mode": photo.get("thumbnail_crop_mode"),
        }
        if view == "episode_evidence":
            shaped["total_episodes"] = evidence.get("total_episodes", eligible_total)
            shaped["archive_episode_count"] = evidence.get("archive_episodes")
        elif view == "archive_only":
            shaped["total_episodes"] = 0
            shaped["archive_episode_count"] = evidence.get("archive_episodes")
        payload.append(shaped)

    return payload, query_count


def get_season_cast(
    season_id: str,
    *,
    view: SeasonCastView,
    limit: int | None = None,
    offset: int | None = None,
    include_archive_only: bool = False,
    photo_fallback: PhotoFallback = "none",
) -> tuple[list[dict[str, Any]], int]:
    """Compose one of the current app's season-cast projections."""

    season, query_count = repository.get_season_context(season_id)
    if season is None:
        return [], query_count
    show_id = str(season.get("show_id") or "")
    season_number = int(season.get("season_number") or 0)

    if view == "membership":
        rows, membership_queries = repository.list_season_membership(
            show_id,
            season_number,
            limit=limit,
            offset=offset,
        )
        query_count += membership_queries
        if not rows:
            return [], query_count
        person_ids = _person_ids(rows)
        photos, photo_queries = repository.get_preferred_cast_photos(
            person_ids,
            season_number=season_number,
        )
        query_count += photo_queries
        if photo_fallback == "bravo":
            photos, bravo_queries = _apply_bravo_fallback(person_ids, photos)
            query_count += bravo_queries
        return [
            {
                **row,
                "photo_url": photos.get(str(row.get("person_id") or ""), _EMPTY_PHOTO).get("url"),
                "thumbnail_focus_x": photos.get(str(row.get("person_id") or ""), _EMPTY_PHOTO).get("thumbnail_focus_x"),
                "thumbnail_focus_y": photos.get(str(row.get("person_id") or ""), _EMPTY_PHOTO).get("thumbnail_focus_y"),
                "thumbnail_zoom": photos.get(str(row.get("person_id") or ""), _EMPTY_PHOTO).get("thumbnail_zoom"),
                "thumbnail_crop_mode": photos.get(str(row.get("person_id") or ""), _EMPTY_PHOTO).get(
                    "thumbnail_crop_mode"
                ),
            }
            for row in rows
        ], query_count

    if view != "episode_counts":  # pragma: no cover - guarded by router parsing.
        raise ValueError(f"Unsupported season cast view: {view}")

    counts, evidence, count_queries = repository.list_season_episode_counts(
        show_id,
        season_id,
        season_number,
        limit=limit,
        offset=offset,
    )
    query_count += count_queries

    normalized_counts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_row in counts:
        row = dict(raw_row)
        person_id = str(row.get("person_id") or "")
        if not person_id:
            continue
        seen.add(person_id)
        person_evidence = evidence.get(person_id)
        if person_evidence is not None:
            row["episodes_in_season"] = person_evidence["regular_episodes_in_season"]
        normalized_counts.append(row)

    if include_archive_only:
        for person_id, person_evidence in evidence.items():
            if (
                person_id not in seen
                and person_evidence["regular_episodes_in_season"] <= 0
                and person_evidence["archive_episodes_in_season"] > 0
            ):
                normalized_counts.append({"person_id": person_id, "episodes_in_season": 0})

    filtered_counts: list[dict[str, Any]] = []
    for row in normalized_counts:
        person_id = str(row.get("person_id") or "")
        person_evidence = evidence.get(person_id, {})
        regular = int(person_evidence.get("regular_episodes_in_season", row.get("episodes_in_season") or 0))
        archive = int(person_evidence.get("archive_episodes_in_season", 0))
        if regular > 0 or (include_archive_only and archive > 0):
            filtered_counts.append({**row, "episodes_in_season": regular})

    if not filtered_counts:
        return [], query_count

    person_ids = _person_ids(filtered_counts)
    totals, totals_queries = repository.get_season_membership_totals(show_id, person_ids)
    query_count += totals_queries
    people, people_queries = repository.get_people_by_ids(person_ids)
    query_count += people_queries
    photos, photo_queries = repository.get_preferred_cast_photos(
        person_ids,
        season_number=season_number,
    )
    query_count += photo_queries
    if photo_fallback == "bravo":
        photos, bravo_queries = _apply_bravo_fallback(person_ids, photos)
        query_count += bravo_queries

    payload: list[dict[str, Any]] = []
    for row in filtered_counts:
        person_id = str(row.get("person_id") or "")
        regular = int(row.get("episodes_in_season") or 0)
        archive = int(evidence.get(person_id, {}).get("archive_episodes_in_season", 0))
        photo = photos.get(person_id, _EMPTY_PHOTO)
        payload.append(
            {
                "person_id": person_id,
                "person_name": people.get(person_id, {}).get("full_name")
                or totals.get(person_id, {}).get("person_name"),
                "episodes_in_season": regular,
                # This intentionally mirrors the current app mapping, which exposes
                # the season-regular count here rather than the lifetime total query.
                "total_episodes": regular,
                "photo_url": photo.get("url"),
                "thumbnail_focus_x": photo.get("thumbnail_focus_x"),
                "thumbnail_focus_y": photo.get("thumbnail_focus_y"),
                "thumbnail_zoom": photo.get("thumbnail_zoom"),
                "thumbnail_crop_mode": photo.get("thumbnail_crop_mode"),
                "archive_episodes_in_season": archive,
            }
        )
    return payload, query_count


def get_person_credits(
    person_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[dict[str, Any], int]:
    """Return local-first credits with soft IMDb-only enrichment, then paginate."""

    normalized_limit, normalized_offset = repository.normalize_pagination(limit, offset)
    local_credits, query_count = repository.list_local_person_credits(person_id)
    imdb_person_id, imdb_id_queries = repository.get_person_imdb_id(person_id)
    query_count += imdb_id_queries

    imdb_credits: list[dict[str, str]] = []
    if imdb_person_id:
        try:
            imdb_credits = name_filmography.fetch_name_filmography(imdb_person_id)
        except Exception as error:  # noqa: BLE001 - enrichment must never fail local credits.
            logger.warning("IMDb name-filmography enrichment failed: %s", error)

    show_by_imdb_id: dict[str, dict[str, str]] = {}
    if imdb_credits:
        show_by_imdb_id, mapping_queries = repository.map_imdb_titles(
            [credit["imdb_title_id"] for credit in imdb_credits]
        )
        query_count += mapping_queries

    local_imdb_ids = {
        str(credit.get("external_imdb_id") or "").strip().casefold()
        for credit in local_credits
        if str(credit.get("external_imdb_id") or "").strip()
    }
    imdb_only: list[dict[str, Any]] = []
    for credit in imdb_credits:
        imdb_title_id = str(credit.get("imdb_title_id") or "").strip().casefold()
        if not imdb_title_id or imdb_title_id in local_imdb_ids:
            continue
        mapped_show = show_by_imdb_id.get(imdb_title_id)
        imdb_only.append(
            {
                "id": f"imdb-{person_id}-{imdb_title_id}",
                "show_id": mapped_show.get("show_id") if mapped_show else None,
                "person_id": person_id,
                "show_name": mapped_show.get("show_name") if mapped_show else credit.get("show_name"),
                "role": None,
                "billing_order": None,
                "credit_category": "Self",
                "source_type": "imdb_name_fullcredits",
                "external_imdb_id": imdb_title_id,
                "external_url": credit.get("external_url"),
                "metadata": None,
            }
        )
    imdb_only.sort(key=lambda credit: (str(credit.get("show_name") or "").casefold(), str(credit.get("id"))))

    combined = [*local_credits, *imdb_only]
    curated_ids, curated_queries = repository.list_curated_cast_show_ids(person_id)
    query_count += curated_queries
    return {
        "credits": combined[normalized_offset : normalized_offset + normalized_limit],
        "curated_cast_show_ids": sorted(set(curated_ids)),
        "total_count": len(combined),
    }, query_count


def get_person_episode_credits(
    person_id: str,
    *,
    show_id: str | None = None,
    include_archive_footage: bool = False,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[dict[str, Any], int]:
    """Return the ordered episode evidence with a bounded response page."""

    normalized_limit, normalized_offset = repository.normalize_pagination(limit, offset)
    rows, query_count = repository.list_person_episode_credits(
        person_id,
        show_id=show_id,
        include_archive_footage=include_archive_footage,
    )
    return {
        "episode_credits": rows[normalized_offset : normalized_offset + normalized_limit],
        "total_count": len(rows),
    }, query_count
