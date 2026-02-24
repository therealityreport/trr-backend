"""Admin endpoints for Bravo sync/import workflows."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Literal
from urllib.parse import quote, unquote, urlparse
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.ingestion.fandom_person_scraper import fetch_fandom_person_html, parse_fandom_person_html
from trr_backend.ingestion.show_cast_matrix_scraper import is_missing_fandom_page
from trr_backend.integrations.fandom import is_allowlisted_fandom_domain, load_fandom_community_allowlist
from trr_backend.repositories.cast_fandom import upsert_cast_fandom
from trr_backend.scraping.bravo_parser import (
    parse_bravo_show_bundle,
    probe_bravo_person_url_candidates,
    resolve_page_featured_image_url,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-bravo"])

_BRAVO_SOURCE_ID = "bravo"
_BRAVO_VARIANT = "default"
_BRAVO_PEOPLE_BASE_URL = "https://www.bravotv.com/people"
_BRAVO_PROBE_STATE_KEY = "bravo_probe_state"
_BRAVO_PROBE_STATE_NA = "na"
_BRAVO_PROBE_REASON_KEY = "bravo_probe_reason"
_BRAVO_PROBE_REASON_MISSING = "missing"
_BRAVO_PROBE_CHECKED_AT_KEY = "bravo_probe_checked_at"
_BRAVO_PROBE_SOURCE_KEY = "bravo_probe_source"
_BRAVO_PROBE_SOURCE_VALUE = "bravo_import_commit"
_FANDOM_PROBE_STATE_KEY = "fandom_probe_state"
_FANDOM_PROBE_STATE_NA = "na"
_FANDOM_PROBE_REASON_KEY = "fandom_probe_reason"
_FANDOM_PROBE_REASON_MISSING = "missing"
_FANDOM_PROBE_CHECKED_AT_KEY = "fandom_probe_checked_at"
_FANDOM_PROBE_SOURCE_KEY = "fandom_probe_source"
_FANDOM_PROBE_SOURCE_VALUE = "bravo_import_commit"
_DEFAULT_FANDOM_COMMUNITY_DOMAIN = "real-housewives.fandom.com"
_BRAVO_CAST_ONLY_PREVIEW_WORKER_LIMIT = 3
_BRAVO_SLOW_CANDIDATE_WARN_MS = 3500
_BRAVO_PREVIEW_HEARTBEAT_SECONDS = 5.0
_BRAVO_PREVIEW_SIGNATURE_VERSION = "v1"
_BRAVO_VIDEO_THUMBNAIL_CONTEXT_SECTION = "bravo_video"
_BRAVO_VIDEO_THUMBNAIL_CONTEXT_TYPE = "thumbnail"
_BRAVO_VIDEO_THUMBNAIL_ASSET_NAME = "Bravo video thumbnail"
_CAST_ANNOUNCEMENT_RE = re.compile(
    r"\b(cast|friend\s*[- ]?of|full\s*[- ]?time|housewife|joins|joined|returning|returns)\b",
    re.IGNORECASE,
)


class BravoPreviewRequest(BaseModel):
    show_url: HttpUrl
    include_people: bool = True
    include_videos: bool = True
    include_news: bool = True
    person_url_candidates: list[HttpUrl] = Field(default_factory=list)
    cast_only: bool = False
    season_number: int | None = Field(default=None, ge=1, le=200)


BravoImageKind = Literal[
    "poster",
    "backdrop",
    "logo",
    "episode_still",
    "cast",
    "promo",
    "intro",
    "reunion",
    "other",
]


class BravoSelectedShowImage(BaseModel):
    url: HttpUrl
    kind: BravoImageKind = "promo"


class BravoCommitRequest(BaseModel):
    show_url: HttpUrl
    selected_show_images: list[BravoSelectedShowImage] = Field(default_factory=list)
    selected_show_image_urls: list[HttpUrl] = Field(default_factory=list)
    description_override: str | None = None
    airs_override: str | None = None
    person_url_mappings: dict[str, UUID] | None = None
    person_url_candidates: list[HttpUrl] = Field(default_factory=list)
    cast_only: bool = False
    season_number: int | None = Field(default=None, ge=1, le=200)
    sync_cast_matrix: bool = True
    preview_result: dict[str, Any] | None = None
    preview_signature: str | None = None


class BravoVideoThumbnailSyncRequest(BaseModel):
    force: bool = False


def _to_iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_bravo_source(db: SupabaseAdminClient) -> None:
    exists_response = db.schema("core").table("sources").select("id").eq("id", _BRAVO_SOURCE_ID).limit(1).execute()
    if getattr(exists_response, "error", None):
        message = getattr(exists_response.error, "message", str(exists_response.error))
        raise HTTPException(status_code=502, detail=f"Failed to verify bravo source row: {message}")
    if exists_response.data:
        return

    insert_response = (
        db.schema("core")
        .table("sources")
        .insert(
            {
                "id": _BRAVO_SOURCE_ID,
                "category": "vendor",
                # DbSession serializes Python lists as JSON; pass PostgreSQL array literal text.
                "aliases": "{bravotv}",
            }
        )
        .execute()
    )
    if getattr(insert_response, "error", None):
        message = getattr(insert_response.error, "message", str(insert_response.error))
        duplicate = "duplicate key value" in message.lower() or "already exists" in message.lower()
        if duplicate:
            return
        raise HTTPException(status_code=502, detail=f"Failed to ensure bravo source row: {message}")


def _payload_sha(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _slugify(value: str | None) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned


def _person_slug_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    for i, part in enumerate(parts):
        if part.lower() == "people" and i + 1 < len(parts):
            slug = unquote(parts[i + 1]).strip().lower()
            return slug or None
    return None


def _show_exists(db: SupabaseAdminClient, show_id: str) -> bool:
    response = db.schema("core").table("shows").select("id").eq("id", show_id).limit(1).execute()
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Database error checking show")
    return bool(response.data)


def _assert_show_sync_ready_for_bravo(db: SupabaseAdminClient, show_id: str) -> None:
    missing: list[str] = []

    seasons_response = db.schema("core").table("seasons").select("id").eq("show_id", show_id).limit(1).execute()
    if getattr(seasons_response, "error", None):
        raise HTTPException(status_code=502, detail="Database error checking seasons sync status")
    if not seasons_response.data:
        missing.append("seasons")

    episodes_response = db.schema("core").table("episodes").select("id").eq("show_id", show_id).limit(1).execute()
    if getattr(episodes_response, "error", None):
        raise HTTPException(status_code=502, detail="Database error checking episodes sync status")
    if not episodes_response.data:
        missing.append("episodes")

    cast_ready = False
    try:
        cast_response = (
            db.schema("core")
            .table("v_person_show_seasons")
            .select("person_id")
            .eq("show_id", show_id)
            .gt("total_episodes", 0)
            .limit(1)
            .execute()
        )
        cast_ready = bool(cast_response.data) and not getattr(cast_response, "error", None)
    except Exception:
        cast_ready = False
    if not cast_ready:
        try:
            fallback_response = (
                db.schema("core").table("show_cast").select("person_id").eq("show_id", show_id).limit(1).execute()
            )
            cast_ready = bool(fallback_response.data) and not getattr(fallback_response, "error", None)
        except Exception:
            cast_ready = False
    if not cast_ready:
        missing.append("cast")

    if missing:
        raise HTTPException(
            status_code=409,
            detail=f"Sync seasons, episodes, and cast before Bravo import (missing: {', '.join(missing)}).",
        )


def _build_show_cast_index(db: SupabaseAdminClient, show_id: str) -> list[dict[str, str]]:
    rows: list[dict[str, Any]] = []

    primary = (
        db.schema("core")
        .table("v_person_show_seasons")
        .select("person_id, person_name")
        .eq("show_id", show_id)
        .limit(1000)
        .execute()
    )
    if not getattr(primary, "error", None) and primary.data:
        rows = primary.data
    else:
        fallback = (
            db.schema("core")
            .table("v_show_cast")
            .select("person_id, cast_member_name")
            .eq("show_id", show_id)
            .limit(1000)
            .execute()
        )
        if getattr(fallback, "error", None):
            return []
        rows = fallback.data or []

    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        person_name = str(row.get("person_name") or row.get("cast_member_name") or "").strip()
        if not person_id or person_id in seen:
            continue
        seen.add(person_id)
        out.append({"person_id": person_id, "person_name": person_name})
    return out


def _build_cast_candidate_person_urls(show_cast: list[dict[str, str]]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for row in show_cast:
        slug = _slugify(row.get("person_name"))
        if not slug:
            continue
        url = f"{_BRAVO_PEOPLE_BASE_URL}/{slug}"
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _normalize_fandom_domain(value: str | None) -> str | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    parsed = urlparse(raw)
    host = parsed.netloc or parsed.path
    host = host.strip().lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _normalize_fandom_url(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return None
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _extract_fandom_domain_from_url(value: str | None) -> str | None:
    normalized_url = _normalize_fandom_url(value)
    if not normalized_url:
        return None
    return _normalize_fandom_domain(urlparse(normalized_url).netloc)


def _build_fandom_person_candidate_url(*, domain: str, person_name: str | None) -> str | None:
    cleaned_name = str(person_name or "").strip()
    if not cleaned_name:
        return None
    title = re.sub(r"\s+", "_", cleaned_name)
    if not title:
        return None
    return f"https://{domain}/wiki/{quote(title)}"


def _load_fandom_probe_domains(
    db: SupabaseAdminClient,
    *,
    show_id: str,
) -> list[str]:
    allowlist = load_fandom_community_allowlist()
    rows = (
        db.schema("core")
        .table("entity_links")
        .select("url, entity_type, season_number, status")
        .eq("show_id", show_id)
        .in_("entity_type", ["show", "season"])
        .in_("link_kind", ["fandom", "wikia"])
        .limit(500)
        .execute()
    )
    domains: list[str] = []
    seen_domains: set[str] = set()
    if not getattr(rows, "error", None):
        for row in rows.data or []:
            status = str(row.get("status") or "").strip().lower()
            if status == "rejected":
                continue
            domain = _extract_fandom_domain_from_url(str(row.get("url") or "").strip())
            if not domain:
                continue
            if not is_allowlisted_fandom_domain(domain, allowlist=allowlist):
                continue
            if domain in seen_domains:
                continue
            seen_domains.add(domain)
            domains.append(domain)

    fallback_domain = _DEFAULT_FANDOM_COMMUNITY_DOMAIN
    if not domains and is_allowlisted_fandom_domain(fallback_domain, allowlist=allowlist):
        domains.append(fallback_domain)
    return domains


def _build_cast_candidate_fandom_urls(
    show_cast: list[dict[str, str]],
    *,
    community_domains: list[str],
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    urls: list[str] = []
    candidate_url_to_person_id: dict[str, str] = {}
    candidate_name_by_url: dict[str, str] = {}
    seen_urls: set[str] = set()
    for row in show_cast:
        person_id = str(row.get("person_id") or "").strip()
        person_name = str(row.get("person_name") or "").strip()
        if not person_id or not person_name:
            continue
        for domain in community_domains:
            candidate_url = _build_fandom_person_candidate_url(domain=domain, person_name=person_name)
            normalized_candidate_url = _normalize_fandom_url(candidate_url)
            if not normalized_candidate_url or normalized_candidate_url in seen_urls:
                continue
            seen_urls.add(normalized_candidate_url)
            urls.append(candidate_url or normalized_candidate_url)
            candidate_url_to_person_id[normalized_candidate_url] = person_id
            candidate_name_by_url[normalized_candidate_url] = person_name
    return urls, candidate_url_to_person_id, candidate_name_by_url


def _extract_person_name_from_fandom_url(url: str | None) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if "/wiki/" not in parsed.path:
        return None
    slug = parsed.path.split("/wiki/", 1)[1].split("/", 1)[0]
    if not slug:
        return None
    return unquote(slug).replace("_", " ").strip() or None


def _normalize_name_token(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _fandom_name_matches(expected_name: str | None, candidate_name: str | None) -> bool:
    expected = _normalize_name_token(expected_name)
    candidate = _normalize_name_token(candidate_name)
    if not expected or not candidate:
        return False
    if expected == candidate:
        return True
    expected_parts = expected.split()
    candidate_parts = candidate.split()
    if not expected_parts or not candidate_parts:
        return False
    expected_last = expected_parts[-1]
    candidate_last = candidate_parts[-1]
    expected_first = expected_parts[0]
    candidate_first = candidate_parts[0]
    if expected_last != candidate_last:
        return False
    return (
        expected_first == candidate_first
        or expected_first.startswith(candidate_first)
        or candidate_first.startswith(expected_first)
    )


def _parse_fandom_preview_person(
    *,
    cast_fandom: dict[str, Any],
    photos: list[dict[str, Any]],
    page_url: str,
) -> dict[str, Any]:
    name = str(cast_fandom.get("full_name") or cast_fandom.get("page_title") or "").strip() or None
    bio = str(cast_fandom.get("casting_summary") or cast_fandom.get("summary") or "").strip() or None
    hero_image_url: str | None = None
    for photo in photos:
        if not isinstance(photo, dict):
            continue
        context_type = str(photo.get("context_type") or "").strip().lower()
        context_section = str(photo.get("context_section") or "").strip().lower()
        if context_type == "hero" or context_section == "infobox":
            hero_image_url = str(photo.get("image_url_canonical") or photo.get("image_url") or "").strip() or None
            if hero_image_url:
                break
    if not hero_image_url:
        for photo in photos:
            if not isinstance(photo, dict):
                continue
            candidate = str(photo.get("image_url_canonical") or photo.get("image_url") or "").strip()
            if candidate:
                hero_image_url = candidate
                break
    return {
        "canonical_url": page_url,
        "name": name,
        "bio": bio,
        "hero_image_url": hero_image_url,
        "social_links": {},
    }


def _select_fandom_profile_image_url(photos: list[dict[str, Any]]) -> str | None:
    for photo in photos:
        if not isinstance(photo, dict):
            continue
        context_type = str(photo.get("context_type") or "").strip().lower()
        context_section = str(photo.get("context_section") or "").strip().lower()
        if context_type == "hero" or context_section == "infobox":
            candidate = str(photo.get("image_url_canonical") or photo.get("image_url") or "").strip()
            if candidate:
                return candidate
    for photo in photos:
        if not isinstance(photo, dict):
            continue
        candidate = str(photo.get("image_url_canonical") or photo.get("image_url") or "").strip()
        if candidate:
            return candidate
    return None


def _probe_single_fandom_candidate(
    candidate_url: str,
    *,
    expected_name: str | None,
) -> dict[str, Any]:
    normalized_candidate_url = _normalize_fandom_url(candidate_url) or candidate_url
    try:
        html, final_url = fetch_fandom_person_html(candidate_url)
    except Exception as exc:  # noqa: BLE001
        return {
            "candidate_url": normalized_candidate_url,
            "url": normalized_candidate_url,
            "status": "error",
            "error": str(exc) or "request_failed",
        }

    resolved_url = _normalize_fandom_url(final_url) or normalized_candidate_url
    if is_missing_fandom_page(html, resolved_url):
        return {
            "candidate_url": normalized_candidate_url,
            "url": resolved_url,
            "status": "missing",
        }

    try:
        cast_fandom, photos = parse_fandom_person_html(html, source_url=resolved_url)
    except Exception as exc:  # noqa: BLE001
        return {
            "candidate_url": normalized_candidate_url,
            "url": resolved_url,
            "status": "error",
            "error": str(exc) or "parse_failed",
        }

    if not isinstance(cast_fandom, dict) or not cast_fandom:
        return {
            "candidate_url": normalized_candidate_url,
            "url": resolved_url,
            "status": "missing",
        }

    page_owner_name = _extract_person_name_from_fandom_url(resolved_url)
    full_name = str(cast_fandom.get("full_name") or "").strip()
    page_title = str(cast_fandom.get("page_title") or "").strip()
    if expected_name and not any(
        _fandom_name_matches(expected_name, candidate_name)
        for candidate_name in (page_owner_name, full_name, page_title)
        if candidate_name
    ):
        return {
            "candidate_url": normalized_candidate_url,
            "url": resolved_url,
            "status": "missing",
            "error": "person_name_mismatch",
        }

    preview_person = _parse_fandom_preview_person(
        cast_fandom=cast_fandom,
        photos=photos if isinstance(photos, list) else [],
        page_url=resolved_url,
    )
    return {
        "candidate_url": normalized_candidate_url,
        "url": resolved_url,
        "status": "ok",
        "person": preview_person,
        "cast_fandom": cast_fandom,
        "photos": photos if isinstance(photos, list) else [],
    }


def _probe_fandom_person_url_candidates(
    candidate_urls: list[str],
    *,
    candidate_name_by_url: dict[str, str],
    max_people: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw_candidate_url in candidate_urls[: max(0, max_people)]:
        candidate_url = str(raw_candidate_url).strip()
        if not candidate_url:
            continue
        normalized_candidate_url = _normalize_fandom_url(candidate_url) or candidate_url
        probe = _probe_single_fandom_candidate(
            candidate_url,
            expected_name=candidate_name_by_url.get(normalized_candidate_url, ""),
        )
        status = str(probe.get("status") or "").strip().lower()
        if status not in {"ok", "missing", "error"}:
            continue
        result: dict[str, Any] = {
            "candidate_url": normalized_candidate_url,
            "url": str(probe.get("url") or normalized_candidate_url).strip() or normalized_candidate_url,
            "status": status,
        }
        name_value = candidate_name_by_url.get(normalized_candidate_url, "")
        if name_value:
            result["name"] = name_value
        error_value = str(probe.get("error") or "").strip()
        if error_value:
            result["error"] = error_value
        person = probe.get("person") if isinstance(probe.get("person"), dict) else None
        if person:
            result["person"] = person
        cast_fandom = probe.get("cast_fandom") if isinstance(probe.get("cast_fandom"), dict) else None
        if cast_fandom:
            result["cast_fandom"] = cast_fandom
        photos = probe.get("photos") if isinstance(probe.get("photos"), list) else None
        if photos:
            result["photos"] = photos
        out.append(result)
    return out


def _merge_person_url_candidates(*groups: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for raw in group:
            value = str(raw).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
    return out


def _sse_event(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"


def _sse_error_stream(payload: dict[str, Any]) -> StreamingResponse:
    def _iter() -> Any:
        yield _sse_event("error", payload)

    return StreamingResponse(_iter(), media_type="text/event-stream")


def _probe_single_bravo_candidate(
    candidate_url: str,
    *,
    include_related_content: bool,
    hydrate_related_dates: bool,
) -> dict[str, Any]:
    for probe in probe_bravo_person_url_candidates(
        [candidate_url],
        max_people=1,
        include_related_content=include_related_content,
        hydrate_related_dates=hydrate_related_dates,
    ):
        return probe
    return {
        "candidate_url": candidate_url,
        "url": candidate_url,
        "status": "error",
        "error": "probe_failed_no_result",
    }


def _summarize_candidate_results(person_candidate_results: list[dict[str, Any]]) -> dict[str, int]:
    tested = len(person_candidate_results)
    valid = sum(
        1
        for result in person_candidate_results
        if isinstance(result, dict) and str(result.get("status") or "").strip().lower() == "ok"
    )
    missing = sum(
        1
        for result in person_candidate_results
        if isinstance(result, dict) and str(result.get("status") or "").strip().lower() == "missing"
    )
    errors = sum(
        1
        for result in person_candidate_results
        if isinstance(result, dict) and str(result.get("status") or "").strip().lower() == "error"
    )
    return {
        "tested": tested,
        "valid": valid,
        "missing": missing,
        "errors": errors,
    }


def _normalize_person_url(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return None
    path = parsed.path.rstrip("/")
    if not path:
        path = "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _normalize_show_url(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return None
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _normalize_candidate_url_set(urls: list[str]) -> set[str]:
    normalized: set[str] = set()
    for raw in urls:
        value = _normalize_person_url(raw)
        if value:
            normalized.add(value)
    return normalized


def _build_preview_signature(
    *,
    show_url: str,
    cast_only: bool,
    season_number: int | None,
    candidate_urls: list[str],
    fandom_candidate_urls: list[str],
) -> str:
    normalized_show_url = _normalize_show_url(show_url) or str(show_url or "").strip()
    normalized_candidate_urls = sorted(_normalize_candidate_url_set(candidate_urls))
    normalized_fandom_urls = sorted(
        {
            value
            for value in (_normalize_fandom_url(raw) for raw in fandom_candidate_urls)
            if isinstance(value, str) and value.strip()
        }
    )
    payload = {
        "v": _BRAVO_PREVIEW_SIGNATURE_VERSION,
        "show_url": normalized_show_url,
        "cast_only": bool(cast_only),
        "season_number": season_number,
        "candidate_urls": normalized_candidate_urls,
        "fandom_candidate_urls": normalized_fandom_urls,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _stable_show_image_candidate_id(url: str) -> str:
    normalized = _normalize_show_url(url) or str(url or "").strip()
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
    return f"bravo-show-{digest}"


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _build_bundle_from_preview_result(preview_result: dict[str, Any]) -> dict[str, Any]:
    show_value = preview_result.get("show")
    people_value = preview_result.get("people")
    videos_value = preview_result.get("videos")
    news_value = preview_result.get("news")
    image_candidates_value = preview_result.get("image_candidates")
    discovered_person_urls_value = preview_result.get("discovered_person_urls")
    person_candidate_results_value = preview_result.get("person_candidate_results")
    fandom_candidate_results_value = preview_result.get("fandom_candidate_results")
    fandom_domains_used_value = preview_result.get("fandom_domains_used")

    return {
        "show": show_value if isinstance(show_value, dict) else {},
        "people": people_value if isinstance(people_value, list) else [],
        "videos": videos_value if isinstance(videos_value, list) else [],
        "news": news_value if isinstance(news_value, list) else [],
        "image_candidates": image_candidates_value if isinstance(image_candidates_value, list) else [],
        "discovered_person_urls": (
            discovered_person_urls_value if isinstance(discovered_person_urls_value, list) else []
        ),
        "person_candidate_results": (
            person_candidate_results_value if isinstance(person_candidate_results_value, list) else []
        ),
        "fandom_candidate_results": (
            fandom_candidate_results_value if isinstance(fandom_candidate_results_value, list) else []
        ),
        "fandom_domains_used": fandom_domains_used_value if isinstance(fandom_domains_used_value, list) else [],
        "raw": preview_result.get("raw") if isinstance(preview_result.get("raw"), dict) else preview_result,
    }


def _extract_preview_candidate_urls(preview_result: dict[str, Any]) -> list[str]:
    explicit = preview_result.get("cast_candidate_urls_tested")
    if isinstance(explicit, list):
        values = [str(item).strip() for item in explicit if str(item).strip()]
        if values:
            return values
    candidate_results = preview_result.get("person_candidate_results")
    if isinstance(candidate_results, list):
        urls: list[str] = []
        for row in candidate_results:
            if not isinstance(row, dict):
                continue
            url = str(row.get("url") or "").strip()
            if url:
                urls.append(url)
        if urls:
            return urls
    return []


def _extract_preview_fandom_candidate_urls(preview_result: dict[str, Any]) -> list[str]:
    explicit = preview_result.get("fandom_candidate_urls_tested")
    if isinstance(explicit, list):
        values = [str(item).strip() for item in explicit if str(item).strip()]
        if values:
            return values
    candidate_results = preview_result.get("fandom_candidate_results")
    if isinstance(candidate_results, list):
        urls: list[str] = []
        for row in candidate_results:
            if not isinstance(row, dict):
                continue
            candidate_url = str(row.get("candidate_url") or "").strip()
            if candidate_url:
                urls.append(candidate_url)
                continue
            url = str(row.get("url") or "").strip()
            if url:
                urls.append(url)
        if urls:
            return urls
    return []


def _validate_cast_only_preview_reuse_or_raise(
    *,
    preview_result: dict[str, Any],
    show_url: str,
    season_number: int | None,
    expected_candidate_urls: list[str],
    expected_fandom_candidate_urls: list[str] | None = None,
    expected_preview_signature: str | None = None,
    request_preview_signature: str | None = None,
) -> None:
    preview_show = preview_result.get("show") if isinstance(preview_result.get("show"), dict) else {}
    preview_show_url = (
        str(preview_result.get("show_url") or "").strip() or str(preview_show.get("canonical_url") or "").strip()
    )
    normalized_preview_show_url = _normalize_show_url(preview_show_url)
    normalized_request_show_url = _normalize_show_url(show_url)
    if (
        not normalized_preview_show_url
        or not normalized_request_show_url
        or normalized_preview_show_url != normalized_request_show_url
    ):
        raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")

    expected_set = _normalize_candidate_url_set(expected_candidate_urls)
    preview_set = _normalize_candidate_url_set(_extract_preview_candidate_urls(preview_result))
    if expected_set != preview_set:
        raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")

    if expected_fandom_candidate_urls is not None:
        expected_fandom_set = {
            value for value in (_normalize_fandom_url(raw) for raw in expected_fandom_candidate_urls) if value
        }
        preview_fandom_urls = _extract_preview_fandom_candidate_urls(preview_result)
        preview_fandom_set = {value for value in (_normalize_fandom_url(raw) for raw in preview_fandom_urls) if value}
        if expected_fandom_set != preview_fandom_set:
            raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")

    preview_season_number = _coerce_optional_int(preview_result.get("season_filter"))
    if preview_season_number != season_number:
        raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")

    preview_cast_only = preview_result.get("cast_only")
    if preview_cast_only is not None and bool(preview_cast_only) is False:
        raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")

    preview_signature = str(preview_result.get("preview_signature") or "").strip()
    if expected_preview_signature and preview_signature and preview_signature != expected_preview_signature:
        raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")
    if (
        request_preview_signature
        and expected_preview_signature
        and request_preview_signature != expected_preview_signature
    ):
        raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")


def _is_bravo_profile_na_marker(status: str, metadata: dict[str, Any] | None) -> bool:
    if status != "rejected":
        return False
    probe_state = str((metadata or {}).get(_BRAVO_PROBE_STATE_KEY) or "").strip().lower()
    return probe_state == _BRAVO_PROBE_STATE_NA


def _load_bravo_profile_link_state_by_person_id(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    cast_person_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not cast_person_ids:
        return {}
    response = (
        db.schema("core")
        .table("entity_links")
        .select("entity_id, url, status, metadata")
        .eq("show_id", show_id)
        .eq("entity_type", "person")
        .eq("link_kind", "bravo_profile")
        .in_("entity_id", cast_person_ids)
        .limit(5000)
        .execute()
    )
    if getattr(response, "error", None):
        return {}

    by_person_id: dict[str, dict[str, Any]] = {}
    for row in response.data or []:
        person_id = str(row.get("entity_id") or "").strip()
        if not person_id:
            continue
        status = str(row.get("status") or "pending").strip().lower()
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        normalized_url = _normalize_person_url(str(row.get("url") or ""))
        state = by_person_id.setdefault(
            person_id,
            {
                "has_non_rejected": False,
                "has_na": False,
                "url_keys": set(),
            },
        )
        if status != "rejected":
            state["has_non_rejected"] = True
        if _is_bravo_profile_na_marker(status, metadata):
            state["has_na"] = True
        if normalized_url:
            state["url_keys"].add(normalized_url)
    return by_person_id


def _build_cast_person_url_lookup(show_cast: list[dict[str, str]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for row in show_cast:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id:
            continue
        slug = _slugify(row.get("person_name"))
        if not slug:
            continue
        normalized_url = _normalize_person_url(f"{_BRAVO_PEOPLE_BASE_URL}/{slug}")
        if normalized_url and normalized_url not in lookup:
            lookup[normalized_url] = person_id
    return lookup


def _filter_explicit_person_url_candidates(
    explicit_candidate_urls: list[str],
    *,
    cast_url_lookup: dict[str, str],
    link_state_by_person_id: dict[str, dict[str, Any]],
    suppress_link_state: bool = True,
) -> list[str]:
    filtered: list[str] = []
    seen: set[str] = set()
    for raw_url in explicit_candidate_urls:
        value = str(raw_url).strip()
        normalized = _normalize_person_url(value)
        if not value or not normalized or normalized in seen:
            continue
        person_id = cast_url_lookup.get(normalized)
        if suppress_link_state and person_id:
            state = link_state_by_person_id.get(person_id) or {}
            if bool(state.get("has_non_rejected")) or bool(state.get("has_na")):
                continue
        seen.add(normalized)
        filtered.append(value)
    return filtered


def _build_eligible_cast_candidate_person_urls(
    show_cast: list[dict[str, str]],
    *,
    link_state_by_person_id: dict[str, dict[str, Any]],
) -> tuple[list[str], dict[str, str]]:
    urls: list[str] = []
    url_to_person_id: dict[str, str] = {}
    seen: set[str] = set()
    for row in show_cast:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id:
            continue
        state = link_state_by_person_id.get(person_id) or {}
        if bool(state.get("has_non_rejected")) or bool(state.get("has_na")):
            continue
        slug = _slugify(row.get("person_name"))
        if not slug:
            continue
        url = f"{_BRAVO_PEOPLE_BASE_URL}/{slug}"
        normalized = _normalize_person_url(url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(url)
        url_to_person_id[normalized] = person_id
    return urls, url_to_person_id


def _persist_missing_bravo_profile_markers(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    actor: str,
    missing_candidate_urls: list[str],
    candidate_url_to_person_id: dict[str, str],
    cast_person_name_by_id: dict[str, str],
    link_state_by_person_id: dict[str, dict[str, Any]],
) -> int:
    from api.routers import admin_show_links

    marked = 0
    seen_urls: set[str] = set()
    for raw_url in missing_candidate_urls:
        normalized_url = _normalize_person_url(raw_url)
        if not normalized_url or normalized_url in seen_urls:
            continue
        seen_urls.add(normalized_url)

        person_id = candidate_url_to_person_id.get(normalized_url)
        if not person_id:
            continue

        state = link_state_by_person_id.get(person_id) or {}
        if bool(state.get("has_na")):
            continue

        person_name = cast_person_name_by_id.get(person_id, "").strip()
        metadata = {
            _BRAVO_PROBE_STATE_KEY: _BRAVO_PROBE_STATE_NA,
            _BRAVO_PROBE_REASON_KEY: _BRAVO_PROBE_REASON_MISSING,
            _BRAVO_PROBE_CHECKED_AT_KEY: _to_iso_now(),
            _BRAVO_PROBE_SOURCE_KEY: _BRAVO_PROBE_SOURCE_VALUE,
        }
        admin_show_links._upsert_link(
            db,
            show_id=show_id,
            entity_type="person",
            entity_id=person_id,
            link_group="official",
            link_kind="bravo_profile",
            url=normalized_url,
            label=f"{person_name} Bravo profile (N/A)" if person_name else "Bravo profile (N/A)",
            season_number=0,
            status="rejected",
            confidence=0.95,
            source=_BRAVO_PROBE_SOURCE_VALUE,
            discovered_by=_BRAVO_PROBE_SOURCE_VALUE,
            metadata=metadata,
            actor=actor,
        )
        marked += 1
        state["has_na"] = True
        link_state_by_person_id[person_id] = state

    return marked


def _is_fandom_profile_na_marker(status: str, metadata: dict[str, Any] | None) -> bool:
    if status != "rejected":
        return False
    probe_state = str((metadata or {}).get(_FANDOM_PROBE_STATE_KEY) or "").strip().lower()
    return probe_state == _FANDOM_PROBE_STATE_NA


def _load_fandom_link_state_by_person_id(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    cast_person_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not cast_person_ids:
        return {}
    response = (
        db.schema("core")
        .table("entity_links")
        .select("entity_id, url, status, metadata")
        .eq("show_id", show_id)
        .eq("entity_type", "person")
        .in_("link_kind", ["fandom", "wikia"])
        .in_("entity_id", cast_person_ids)
        .limit(5000)
        .execute()
    )
    if getattr(response, "error", None):
        return {}

    by_person_id: dict[str, dict[str, Any]] = {}
    for row in response.data or []:
        person_id = str(row.get("entity_id") or "").strip()
        if not person_id:
            continue
        status = str(row.get("status") or "pending").strip().lower()
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        normalized_url = _normalize_fandom_url(str(row.get("url") or ""))
        state = by_person_id.setdefault(
            person_id,
            {
                "has_non_rejected": False,
                "has_na": False,
                "url_keys": set(),
            },
        )
        if status != "rejected":
            state["has_non_rejected"] = True
        if _is_fandom_profile_na_marker(status, metadata):
            state["has_na"] = True
        if normalized_url:
            state["url_keys"].add(normalized_url)
    return by_person_id


def _persist_valid_fandom_profile_links(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    actor: str,
    fandom_candidate_results: list[dict[str, Any]],
    candidate_url_to_person_id: dict[str, str],
    cast_person_name_by_id: dict[str, str],
    link_state_by_person_id: dict[str, dict[str, Any]],
) -> int:
    from api.routers import admin_show_links

    upserted = 0
    seen_rows: set[tuple[str, str]] = set()
    for result in fandom_candidate_results:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        if status != "ok":
            continue
        candidate_url = _normalize_fandom_url(str(result.get("candidate_url") or "").strip())
        resolved_url = _normalize_fandom_url(str(result.get("url") or "").strip())
        if not candidate_url or not resolved_url:
            continue
        person_id = candidate_url_to_person_id.get(candidate_url)
        if not person_id:
            continue
        key = (person_id, resolved_url)
        if key in seen_rows:
            continue
        seen_rows.add(key)
        person_name = cast_person_name_by_id.get(person_id, "").strip()
        admin_show_links._upsert_link(
            db,
            show_id=show_id,
            entity_type="person",
            entity_id=person_id,
            link_group="knowledge",
            link_kind="fandom",
            url=resolved_url,
            label=f"{person_name} Fandom page" if person_name else "Fandom page",
            season_number=0,
            status="approved",
            confidence=0.95,
            source="bravo_fandom_probe",
            discovered_by="bravo_fandom_probe",
            metadata={
                _FANDOM_PROBE_CHECKED_AT_KEY: _to_iso_now(),
                _FANDOM_PROBE_SOURCE_KEY: _FANDOM_PROBE_SOURCE_VALUE,
            },
            actor=actor,
        )
        state = link_state_by_person_id.setdefault(person_id, {"has_non_rejected": False, "has_na": False})
        state["has_non_rejected"] = True
        upserted += 1
    return upserted


def _persist_missing_fandom_profile_markers(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    actor: str,
    fandom_candidate_results: list[dict[str, Any]],
    candidate_url_to_person_id: dict[str, str],
    cast_person_name_by_id: dict[str, str],
    link_state_by_person_id: dict[str, dict[str, Any]],
) -> int:
    from api.routers import admin_show_links

    marked = 0
    seen_rows: set[tuple[str, str]] = set()
    for result in fandom_candidate_results:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        if status != "missing":
            continue
        candidate_url = _normalize_fandom_url(str(result.get("candidate_url") or "").strip())
        marker_url = _normalize_fandom_url(str(result.get("url") or "").strip()) or candidate_url
        if not candidate_url or not marker_url:
            continue
        person_id = candidate_url_to_person_id.get(candidate_url)
        if not person_id:
            continue
        dedupe_key = (person_id, marker_url)
        if dedupe_key in seen_rows:
            continue
        seen_rows.add(dedupe_key)
        state = link_state_by_person_id.get(person_id) or {}
        existing_urls = state.get("url_keys") if isinstance(state.get("url_keys"), set) else set()
        if marker_url in existing_urls:
            continue
        person_name = cast_person_name_by_id.get(person_id, "").strip()
        admin_show_links._upsert_link(
            db,
            show_id=show_id,
            entity_type="person",
            entity_id=person_id,
            link_group="knowledge",
            link_kind="fandom",
            url=marker_url,
            label=f"{person_name} Fandom page (N/A)" if person_name else "Fandom page (N/A)",
            season_number=0,
            status="rejected",
            confidence=0.95,
            source=_FANDOM_PROBE_SOURCE_VALUE,
            discovered_by=_FANDOM_PROBE_SOURCE_VALUE,
            metadata={
                _FANDOM_PROBE_STATE_KEY: _FANDOM_PROBE_STATE_NA,
                _FANDOM_PROBE_REASON_KEY: _FANDOM_PROBE_REASON_MISSING,
                _FANDOM_PROBE_CHECKED_AT_KEY: _to_iso_now(),
                _FANDOM_PROBE_SOURCE_KEY: _FANDOM_PROBE_SOURCE_VALUE,
            },
            actor=actor,
        )
        state["has_na"] = True
        if not isinstance(state.get("url_keys"), set):
            state["url_keys"] = set()
        state["url_keys"].add(marker_url)
        link_state_by_person_id[person_id] = state
        marked += 1
    return marked


def _resolve_person_url_map(
    discovered_person_urls: list[str],
    show_cast: list[dict[str, str]],
    explicit: dict[str, str] | None = None,
) -> dict[str, str]:
    explicit = explicit or {}

    cast_by_slug: dict[str, str] = {}
    for row in show_cast:
        slug = _slugify(row.get("person_name"))
        if slug and slug not in cast_by_slug:
            cast_by_slug[slug] = row["person_id"]

    resolved: dict[str, str] = {}
    for person_url in discovered_person_urls:
        canonical = str(person_url).strip()
        if not canonical:
            continue

        explicit_id = explicit.get(canonical)
        if explicit_id:
            resolved[canonical] = explicit_id
            continue

        slug = _person_slug_from_url(canonical)
        if not slug:
            continue

        if slug in cast_by_slug:
            resolved[canonical] = cast_by_slug[slug]
            continue

        for cast_slug, person_id in cast_by_slug.items():
            if slug == cast_slug or slug in cast_slug or cast_slug in slug:
                resolved[canonical] = person_id
                break

    return resolved


def _infer_person_tags(
    text: str | None,
    item_url: str | None,
    people_refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    haystack = f"{text or ''} {item_url or ''}".lower()
    tags: list[dict[str, Any]] = []
    seen: set[str] = set()

    for ref in people_refs:
        person_id = str(ref.get("person_id") or "").strip()
        person_name = str(ref.get("person_name") or "").strip()
        person_url = str(ref.get("person_url") or "").strip() or None

        slug = _person_slug_from_url(person_url) if person_url else None
        name_hit = bool(person_name and person_name.lower() in haystack)
        slug_hit = bool(slug and slug in haystack)

        if not name_hit and not slug_hit:
            continue

        dedupe_key = person_id or person_name.lower() or (slug or "")
        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        tags.append(
            {
                "person_id": person_id or None,
                "person_name": person_name or None,
                "person_url": person_url,
            }
        )

    return tags


def _merge_person_tags(
    left: list[dict[str, Any]] | None,
    right: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_tag in [*(left or []), *(right or [])]:
        if not isinstance(raw_tag, dict):
            continue
        person_id = str(raw_tag.get("person_id") or "").strip() or None
        person_url = str(raw_tag.get("person_url") or "").strip() or None
        person_name = str(raw_tag.get("person_name") or "").strip() or None
        dedupe_key = person_id or person_url or (person_name.lower() if person_name else "")
        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(
            {
                "person_id": person_id,
                "person_name": person_name,
                "person_url": person_url,
            }
        )
    return out


def _is_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True


def _dedupe_items(
    items: list[dict[str, Any]],
    key: str,
    *,
    merge_person_tags: bool = False,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for item in items:
        raw = item.get(key)
        if not isinstance(raw, str):
            continue
        normalized = raw.strip()
        if not normalized:
            continue
        existing = by_key.get(normalized)
        if existing is None:
            copied = dict(item)
            copied[key] = normalized
            by_key[normalized] = copied
            out.append(copied)
            continue
        if merge_person_tags:
            existing["person_tags"] = _merge_person_tags(
                existing.get("person_tags") if isinstance(existing.get("person_tags"), list) else [],
                item.get("person_tags") if isinstance(item.get("person_tags"), list) else [],
            )
        for field in (
            "title",
            "runtime",
            "kicker",
            "headline",
            "season_number",
            "published_at",
            "image_url",
            "original_image_url",
            "hosted_image_url",
            "media_asset_id",
            "thumbnail_sync_status",
            "thumbnail_sync_error",
        ):
            if _is_non_empty_value(existing.get(field)):
                continue
            if _is_non_empty_value(item.get(field)):
                existing[field] = item.get(field)
    return out


def _upsert_show_snapshot(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    payload: dict[str, Any],
    status: str = "success",
    error: str | None = None,
) -> dict[str, Any]:
    _ensure_bravo_source(db)
    fetched_at = _to_iso_now()
    payload_sha = _payload_sha(payload)

    latest_row = {
        "show_id": show_id,
        "source_id": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "fetched_at": fetched_at,
        "fetch_method": "admin_import_bravo",
        "status": status,
        "error": error,
        "payload": payload,
        "payload_sha256": payload_sha,
    }

    latest_resp = (
        db.schema("core")
        .table("show_source_latest")
        .upsert(latest_row, on_conflict="show_id,source_id,variant")
        .execute()
    )
    if getattr(latest_resp, "error", None):
        message = getattr(latest_resp.error, "message", str(latest_resp.error))
        logger.error("Failed to persist show bravo snapshot for show_id=%s: %s", show_id, message)
        raise HTTPException(status_code=502, detail=f"Failed to persist show bravo snapshot: {message}")

    history_row = {
        "show_id": show_id,
        "source_id": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "fetched_at": fetched_at,
        "fetch_method": "admin_import_bravo",
        "status": status,
        "error": error,
        "payload": payload,
        "payload_sha256": payload_sha,
    }
    history_resp = db.schema("core").table("show_source_history").insert(history_row).execute()
    if getattr(history_resp, "error", None):
        logger.warning("Failed to persist show bravo snapshot history: %s", history_resp.error)

    return {
        "show_id": show_id,
        "source_id": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "fetched_at": fetched_at,
        "payload_sha256": payload_sha,
    }


def _upsert_person_snapshot(
    db: SupabaseAdminClient,
    *,
    person_id: str,
    payload: dict[str, Any],
    status: str = "success",
    error: str | None = None,
) -> dict[str, Any]:
    _ensure_bravo_source(db)
    fetched_at = _to_iso_now()
    payload_sha = _payload_sha(payload)

    latest_row = {
        "person_id": person_id,
        "source_id": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "fetched_at": fetched_at,
        "fetch_method": "admin_import_bravo",
        "status": status,
        "error": error,
        "payload": payload,
        "payload_sha256": payload_sha,
    }
    latest_resp = (
        db.schema("core")
        .table("person_source_latest")
        .upsert(latest_row, on_conflict="person_id,source_id,variant")
        .execute()
    )
    if getattr(latest_resp, "error", None):
        message = getattr(latest_resp.error, "message", str(latest_resp.error))
        logger.error("Failed to persist bravo person snapshot for person_id=%s: %s", person_id, message)
        raise HTTPException(status_code=502, detail=f"Failed to persist person snapshot for {person_id}: {message}")

    history_row = {
        "person_id": person_id,
        "source_id": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "fetched_at": fetched_at,
        "fetch_method": "admin_import_bravo",
        "status": status,
        "error": error,
        "payload": payload,
        "payload_sha256": payload_sha,
    }
    history_resp = db.schema("core").table("person_source_history").insert(history_row).execute()
    if getattr(history_resp, "error", None):
        logger.warning("Failed to persist person bravo snapshot history: %s", history_resp.error)

    return {
        "person_id": person_id,
        "source_id": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "fetched_at": fetched_at,
        "payload_sha256": payload_sha,
    }


def _merge_source_value(field: Any, source: str, value: str | None) -> dict[str, Any]:
    out = dict(field) if isinstance(field, dict) else {}
    if isinstance(value, str) and value.strip():
        out[source] = value.strip()
    return out


_SOCIAL_PLATFORMS = {"instagram", "twitter", "facebook", "tiktok", "youtube"}


def _normalize_social_handle(platform: str, value: str) -> str | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        parsed = urlparse(cleaned)
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            return None
        if platform == "youtube":
            first = parts[0]
            if first.lower() in {"channel", "user", "c"} and len(parts) > 1:
                handle = parts[1].strip()
                return handle or None
            if first.lower() in {"channel", "user", "c"}:
                return None
            handle = first.strip()
            return handle or None
        return parts[0].lstrip("@").strip() or None

    if platform == "youtube":
        handle = cleaned.strip("/").strip()
        if handle.lower() in {"user", "channel", "c"}:
            return None
        return handle or None
    return cleaned.lstrip("@").strip("/") or None


def _build_social_url(platform: str, handle: str) -> str | None:
    normalized = handle.strip()
    if not normalized:
        return None
    if platform == "instagram":
        return f"https://www.instagram.com/{normalized.lstrip('@')}"
    if platform == "twitter":
        return f"https://x.com/{normalized.lstrip('@')}"
    if platform == "facebook":
        return f"https://www.facebook.com/{normalized.lstrip('@')}"
    if platform == "tiktok":
        tiktok_handle = normalized if normalized.startswith("@") else f"@{normalized.lstrip('@')}"
        return f"https://www.tiktok.com/{tiktok_handle}"
    if platform == "youtube":
        if normalized.startswith("@"):
            return f"https://www.youtube.com/{normalized}"
        if normalized.upper().startswith("UC"):
            return f"https://www.youtube.com/channel/{normalized}"
        return f"https://www.youtube.com/@{normalized.lstrip('@')}"
    return None


def _normalize_social_external_ids(incoming: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_value in incoming.items():
        if not isinstance(raw_value, str) or not raw_value.strip():
            continue
        key = str(raw_key).strip().lower()
        value = raw_value.strip()

        platform: str | None = None
        if key in _SOCIAL_PLATFORMS:
            platform = key
        elif key.endswith("_id") and key[:-3] in _SOCIAL_PLATFORMS:
            platform = key[:-3]
        elif key.endswith("_url") and key[:-4] in _SOCIAL_PLATFORMS:
            platform = key[:-4]

        if platform is None:
            out[key] = value
            continue

        handle = _normalize_social_handle(platform, value)
        if handle:
            out[platform] = handle
            out[f"{platform}_id"] = handle
            resolved_url = _build_social_url(platform, handle)
            if resolved_url:
                out[f"{platform}_url"] = resolved_url

    return out


def _has_existing_external_id(out: dict[str, Any], key: str) -> bool:
    if key in _SOCIAL_PLATFORMS or (key.endswith("_id") and key[:-3] in _SOCIAL_PLATFORMS):
        platform = key[:-3] if key.endswith("_id") else key
        for candidate in (platform, f"{platform}_id"):
            existing = out.get(candidate)
            if isinstance(existing, str) and existing.strip():
                return True
        return False

    if key.endswith("_url") and key[:-4] in _SOCIAL_PLATFORMS:
        existing = out.get(key)
        return isinstance(existing, str) and existing.strip()

    existing = out.get(key)
    return isinstance(existing, str) and existing.strip()


def _merge_external_ids_fill_missing(existing: Any, incoming: dict[str, str]) -> dict[str, Any]:
    out = dict(existing) if isinstance(existing, dict) else {}
    normalized_incoming = _normalize_social_external_ids(incoming)
    for key, value in normalized_incoming.items():
        if not isinstance(value, str) or not value.strip():
            continue
        if _has_existing_external_id(out, key):
            continue
        out[key] = value.strip()

    # Keep legacy + canonical social keys in sync for downstream consumers.
    for platform in _SOCIAL_PLATFORMS:
        legacy = out.get(platform)
        canonical = out.get(f"{platform}_id")
        if isinstance(legacy, str) and legacy.strip() and not (isinstance(canonical, str) and canonical.strip()):
            out[f"{platform}_id"] = legacy.strip()
        if isinstance(canonical, str) and canonical.strip() and not (isinstance(legacy, str) and legacy.strip()):
            out[platform] = canonical.strip()
    return out


def _persist_show_description(db: SupabaseAdminClient, show_id: str, description: str | None) -> None:
    if not isinstance(description, str) or not description.strip():
        return
    response = db.schema("core").table("shows").update({"description": description.strip()}).eq("id", show_id).execute()
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Failed to update show description")


def _persist_season_overview(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    season_id: str,
    overview: str | None,
) -> None:
    if not isinstance(overview, str) or not overview.strip():
        return
    response = (
        db.schema("core")
        .table("seasons")
        .update({"overview": overview.strip()})
        .eq("id", season_id)
        .eq("show_id", show_id)
        .execute()
    )
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Failed to update season overview")


def _persist_person_profile(
    db: SupabaseAdminClient,
    *,
    person_id: str,
    person_url: str,
    bio: str | None,
    hero_image_url: str | None,
    social_links: dict[str, str],
    source: str = "bravo",
) -> None:
    response = (
        db.schema("core")
        .table("people")
        .select("id, biography, homepage, profile_image_url, external_ids")
        .eq("id", person_id)
        .limit(1)
        .execute()
    )
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to read person {person_id}")
    if not response.data:
        return

    row = response.data[0]
    payload = {
        "biography": _merge_source_value(row.get("biography"), source, bio),
        "homepage": _merge_source_value(row.get("homepage"), source, person_url),
        "profile_image_url": _merge_source_value(row.get("profile_image_url"), source, hero_image_url),
        "external_ids": _merge_external_ids_fill_missing(row.get("external_ids"), social_links),
    }

    update_resp = db.schema("core").table("people").update(payload).eq("id", person_id).execute()
    if getattr(update_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to update person {person_id}")


def _import_person_profile_image(
    *,
    db: SupabaseAdminClient,
    admin_user: dict[str, Any],
    show_id: str,
    season_id: str | None,
    season_number: int | None,
    person_id: str,
    person_url: str,
    image_url: str,
    person_name: str | None,
    source_label: str,
    context_section: str,
) -> dict[str, Any]:
    from api.routers.admin_scrape import ImportImageItem, ImportRequest, import_images

    asset_label = f"{source_label} profile picture"
    if season_number is not None:
        import_request = ImportRequest(
            entity_type="season",
            show_id=UUID(show_id),
            season_id=UUID(season_id) if season_id else None,
            season_number=season_number,
            source_url=person_url,
            images=[
                ImportImageItem(
                    candidate_id=f"{source_label.lower()}-person-hero-{person_id}",
                    url=image_url,
                    caption=f"{asset_label}{f' ({person_name})' if person_name else ''}",
                    kind="promo",
                    person_ids=[UUID(person_id)],
                    context_section=context_section,
                    context_type="profile_picture",
                    asset_name=asset_label,
                )
            ],
        )
    else:
        import_request = ImportRequest(
            entity_type="person",
            person_id=UUID(person_id),
            source_url=person_url,
            images=[
                ImportImageItem(
                    candidate_id=f"{source_label.lower()}-person-hero-{person_id}",
                    url=image_url,
                    caption=f"{asset_label}{f' ({person_name})' if person_name else ''}",
                    kind="promo",
                    context_section=context_section,
                    context_type="profile_picture",
                    asset_name=asset_label,
                )
            ],
        )
    import_result = import_images(import_request, db, admin_user)
    asset_ids: list[str] = []
    hosted_urls: list[str] = []
    for raw_asset in list(getattr(import_result, "assets", []) or []):
        if isinstance(raw_asset, dict):
            asset_id = raw_asset.get("id")
            hosted_url = raw_asset.get("hosted_url")
        else:
            asset_id = getattr(raw_asset, "id", None)
            hosted_url = getattr(raw_asset, "hosted_url", None)
        asset_id_str = str(asset_id or "").strip()
        if asset_id_str and asset_id_str not in asset_ids:
            asset_ids.append(asset_id_str)
        hosted_url_str = str(hosted_url or "").strip()
        if hosted_url_str and hosted_url_str not in hosted_urls:
            hosted_urls.append(hosted_url_str)
    return {
        "imported": int(import_result.imported),
        "skipped": int(import_result.skipped_duplicates),
        "errors": list(import_result.errors),
        "asset_ids": asset_ids,
        "hosted_urls": hosted_urls,
        "primary_hosted_url": hosted_urls[0] if hosted_urls else None,
    }


def _import_bravo_person_image(
    *,
    db: SupabaseAdminClient,
    admin_user: dict[str, Any],
    show_id: str,
    season_id: str | None,
    season_number: int | None,
    person_id: str,
    person_url: str,
    hero_image_url: str,
    person_name: str | None,
) -> dict[str, Any]:
    return _import_person_profile_image(
        db=db,
        admin_user=admin_user,
        show_id=show_id,
        season_id=season_id,
        season_number=season_number,
        person_id=person_id,
        person_url=person_url,
        image_url=hero_image_url,
        person_name=person_name,
        source_label="Bravo",
        context_section="bravo_profile",
    )


def _import_fandom_person_image(
    *,
    db: SupabaseAdminClient,
    admin_user: dict[str, Any],
    show_id: str,
    season_id: str | None,
    season_number: int | None,
    person_id: str,
    person_url: str,
    image_url: str,
    person_name: str | None,
) -> dict[str, Any]:
    return _import_person_profile_image(
        db=db,
        admin_user=admin_user,
        show_id=show_id,
        season_id=season_id,
        season_number=season_number,
        person_id=person_id,
        person_url=person_url,
        image_url=image_url,
        person_name=person_name,
        source_label="Fandom",
        context_section="fandom_profile",
    )


def _default_profile_thumbnail_crop() -> dict[str, Any]:
    return {
        "x": 50,
        "y": 32,
        "zoom": 1,
        "mode": "auto",
    }


def _build_profile_link_context(
    *,
    existing: Any,
    person_url: str,
    season_number: int | None,
    context_section: str,
) -> dict[str, Any]:
    context = dict(existing) if isinstance(existing, dict) else {}
    context.setdefault("context_section", context_section)
    context.setdefault("context_type", "profile_picture")
    if isinstance(person_url, str) and person_url.strip():
        context.setdefault("source_url", person_url.strip())
    if isinstance(season_number, int):
        context["season_number"] = int(season_number)

    crop = context.get("thumbnail_crop")
    if not isinstance(crop, dict):
        context["thumbnail_crop"] = _default_profile_thumbnail_crop()
    return context


def _promote_profile_media_link(
    *,
    db: SupabaseAdminClient,
    person_id: str,
    person_url: str,
    media_asset_id: str,
    season_number: int | None,
    context_section: str,
) -> None:
    from trr_backend.media.user_uploads import set_primary_media_link

    media_asset_id_str = str(media_asset_id or "").strip()
    if not media_asset_id_str:
        return

    gallery_existing_resp = (
        db.schema("core")
        .table("media_links")
        .select("id, context, position")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "gallery")
        .eq("media_asset_id", media_asset_id_str)
        .limit(1)
        .execute()
    )
    if getattr(gallery_existing_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to load gallery media link for person {person_id}")
    gallery_existing = gallery_existing_resp.data[0] if gallery_existing_resp.data else {}
    gallery_context = _build_profile_link_context(
        existing=gallery_existing.get("context"),
        person_url=person_url,
        season_number=season_number,
        context_section=context_section,
    )
    gallery_position = gallery_existing.get("position") if isinstance(gallery_existing, dict) else None
    if not isinstance(gallery_position, int):
        gallery_position = 0
    gallery_upsert_resp = (
        db.schema("core")
        .table("media_links")
        .upsert(
            {
                "entity_type": "person",
                "entity_id": person_id,
                "media_asset_id": media_asset_id_str,
                "kind": "gallery",
                "position": gallery_position,
                "is_primary": False,
                "context": gallery_context,
            },
            on_conflict="entity_type,entity_id,kind,media_asset_id",
        )
        .execute()
    )
    if getattr(gallery_upsert_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to upsert gallery media link for person {person_id}")

    profile_existing_resp = (
        db.schema("core")
        .table("media_links")
        .select("id, context")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "profile")
        .eq("media_asset_id", media_asset_id_str)
        .limit(1)
        .execute()
    )
    if getattr(profile_existing_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to load profile media link for person {person_id}")
    profile_existing = profile_existing_resp.data[0] if profile_existing_resp.data else {}
    profile_context = _build_profile_link_context(
        existing=profile_existing.get("context"),
        person_url=person_url,
        season_number=season_number,
        context_section=context_section,
    )
    profile_upsert_resp = (
        db.schema("core")
        .table("media_links")
        .upsert(
            {
                "entity_type": "person",
                "entity_id": person_id,
                "media_asset_id": media_asset_id_str,
                "kind": "profile",
                "position": 0,
                "is_primary": True,
                "context": profile_context,
            },
            on_conflict="entity_type,entity_id,kind,media_asset_id",
        )
        .execute()
    )
    if getattr(profile_upsert_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to upsert profile media link for person {person_id}")

    profile_link_resp = (
        db.schema("core")
        .table("media_links")
        .select("id")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "profile")
        .eq("media_asset_id", media_asset_id_str)
        .limit(1)
        .execute()
    )
    if getattr(profile_link_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to resolve profile media link for person {person_id}")
    if not profile_link_resp.data:
        raise HTTPException(status_code=502, detail=f"Profile media link missing for person {person_id}")
    profile_link_id = str(profile_link_resp.data[0].get("id") or "").strip()
    if not profile_link_id:
        raise HTTPException(status_code=502, detail=f"Profile media link id missing for person {person_id}")

    try:
        set_primary_media_link(
            db,
            entity_type="person",
            entity_id=person_id,
            kind="profile",
            media_link_id=profile_link_id,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=502,
            detail=f"Failed to set primary profile media link for person {person_id}",
        ) from exc


def _promote_bravo_profile_media_link(
    *,
    db: SupabaseAdminClient,
    person_id: str,
    person_url: str,
    media_asset_id: str,
    season_number: int | None,
) -> None:
    _promote_profile_media_link(
        db=db,
        person_id=person_id,
        person_url=person_url,
        media_asset_id=media_asset_id,
        season_number=season_number,
        context_section="bravo_profile",
    )


def _promote_fandom_profile_media_link(
    *,
    db: SupabaseAdminClient,
    person_id: str,
    person_url: str,
    media_asset_id: str,
    season_number: int | None,
) -> None:
    _promote_profile_media_link(
        db=db,
        person_id=person_id,
        person_url=person_url,
        media_asset_id=media_asset_id,
        season_number=season_number,
        context_section="fandom_profile",
    )


def _fetch_show_snapshot(db: SupabaseAdminClient, show_id: str) -> dict[str, Any]:
    response = (
        db.schema("core")
        .table("show_source_latest")
        .select("show_id, source_id, variant, fetched_at, payload, payload_sha256")
        .eq("show_id", show_id)
        .eq("source_id", _BRAVO_SOURCE_ID)
        .eq("variant", _BRAVO_VARIANT)
        .limit(1)
        .execute()
    )
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Failed to load bravo snapshot")
    if not response.data:
        raise HTTPException(status_code=404, detail="No persisted Bravo snapshot for this show")
    return response.data[0]


def _resolve_season_id(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    season_number: int,
) -> str | None:
    response = (
        db.schema("core")
        .table("seasons")
        .select("id")
        .eq("show_id", show_id)
        .eq("season_number", season_number)
        .limit(1)
        .execute()
    )
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Failed to resolve season for Bravo import")
    if not response.data:
        return None
    return str(response.data[0].get("id") or "").strip() or None


def _fetch_person_snapshots(db: SupabaseAdminClient, person_ids: list[str]) -> list[dict[str, Any]]:
    if not person_ids:
        return []
    response = (
        db.schema("core")
        .table("person_source_latest")
        .select("person_id, fetched_at, payload")
        .eq("source_id", _BRAVO_SOURCE_ID)
        .eq("variant", _BRAVO_VARIANT)
        .in_("person_id", person_ids)
        .limit(1000)
        .execute()
    )
    if getattr(response, "error", None):
        return []
    return response.data or []


def _video_item_needs_thumbnail_sync(item: dict[str, Any]) -> bool:
    hosted_image_url = str(item.get("hosted_image_url") or "").strip()
    image_url = str(item.get("image_url") or "").strip()
    status = str(item.get("thumbnail_sync_status") or "").strip().lower()
    if hosted_image_url:
        return False
    if status == "synced" and image_url:
        return False
    return True


def _sync_bravo_video_thumbnails(
    *,
    db: SupabaseAdminClient,
    admin_user: AdminUser,
    show_id: str,
    normalized: dict[str, Any],
    force: bool = False,
    refresh_from_clip_metadata: bool = True,
) -> dict[str, Any]:
    from api.routers.admin_scrape import ImportImageItem, ImportRequest, import_images

    attempted = 0
    imported = 0
    skipped = 0
    synced = 0
    failed = 0
    missing_source = 0
    refreshed_from_clip = 0
    errors: list[str] = []
    candidate_counter = 0

    for list_key in ("videos_show", "videos_person"):
        video_items = normalized.get(list_key) if isinstance(normalized.get(list_key), list) else []
        for item in video_items:
            if not isinstance(item, dict):
                continue
            if not force and not _video_item_needs_thumbnail_sync(item):
                continue

            clip_url = str(item.get("clip_url") or "").strip()
            image_url = str(item.get("image_url") or "").strip()
            original_image_url = str(item.get("original_image_url") or "").strip()
            previous_image_url = image_url

            if refresh_from_clip_metadata and clip_url:
                try:
                    featured_image_url = resolve_page_featured_image_url(clip_url)
                except Exception:  # noqa: BLE001
                    featured_image_url = None
                if isinstance(featured_image_url, str) and featured_image_url.strip():
                    featured_image_url = featured_image_url.strip()
                    if featured_image_url != image_url:
                        if image_url and not original_image_url:
                            original_image_url = image_url
                        image_url = featured_image_url
                        refreshed_from_clip += 1

            source_image_url = image_url or original_image_url
            if source_image_url and not original_image_url:
                original_image_url = source_image_url
            item["original_image_url"] = original_image_url or None

            if not source_image_url or not clip_url:
                item["thumbnail_sync_status"] = "missing_source"
                item["thumbnail_sync_error"] = (
                    "Missing source thumbnail URL for Bravo video mirroring"
                    if not source_image_url
                    else "Missing clip URL for Bravo video mirroring"
                )
                missing_source += 1
                continue

            attempted += 1
            candidate_counter += 1

            try:
                import_request = ImportRequest(
                    entity_type="show",
                    show_id=UUID(show_id),
                    source_url=clip_url,
                    images=[
                        ImportImageItem(
                            candidate_id=f"bravo-video-thumbnail-{candidate_counter}",
                            url=source_image_url,
                            caption=str(item.get("title") or "").strip()[:160] or _BRAVO_VIDEO_THUMBNAIL_ASSET_NAME,
                            kind="promo",
                            context_section=_BRAVO_VIDEO_THUMBNAIL_CONTEXT_SECTION,
                            context_type=_BRAVO_VIDEO_THUMBNAIL_CONTEXT_TYPE,
                            source_logo="bravo",
                            asset_name=_BRAVO_VIDEO_THUMBNAIL_ASSET_NAME,
                        )
                    ],
                )
                import_result = import_images(import_request, db, admin_user)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                error_message = f"{clip_url}: {exc}"
                errors.append(error_message)
                item["thumbnail_sync_status"] = "failed"
                item["thumbnail_sync_error"] = str(exc)
                item["image_url"] = image_url or previous_image_url or source_image_url
                continue

            imported += int(import_result.imported)
            skipped += int(import_result.skipped_duplicates)
            if import_result.errors:
                errors.extend([f"{clip_url}: {str(err)}" for err in import_result.errors])

            first_asset = import_result.assets[0] if import_result.assets else None
            if isinstance(first_asset, dict):
                hosted_url = str(first_asset.get("hosted_url") or "").strip()
                media_asset_id = str(first_asset.get("id") or "").strip() or None
            else:
                hosted_url = str(getattr(first_asset, "hosted_url", "") or "").strip()
                media_asset_id = str(getattr(first_asset, "id", "") or "").strip() or None

            if not hosted_url:
                failed += 1
                error_message = "; ".join([str(err) for err in import_result.errors]) or (
                    "Media import returned no hosted URL"
                )
                item["thumbnail_sync_status"] = "failed"
                item["thumbnail_sync_error"] = error_message
                item["image_url"] = image_url or previous_image_url or source_image_url
                continue

            synced += 1
            item["hosted_image_url"] = hosted_url
            item["image_url"] = hosted_url
            item["media_asset_id"] = media_asset_id
            item["thumbnail_sync_status"] = "synced"
            item["thumbnail_sync_error"] = None
            if not item.get("original_image_url"):
                item["original_image_url"] = source_image_url

    return {
        "attempted": attempted,
        "synced": synced,
        "failed": failed,
        "missing_source": missing_source,
        "imported": imported,
        "skipped": skipped,
        "refreshed_from_clip": refreshed_from_clip,
        "remaining": max(0, attempted - synced),
        "errors": errors,
    }


def _extract_videos_from_snapshot(
    snapshot_payload: dict[str, Any],
    *,
    merge_person_sources: bool,
    db: SupabaseAdminClient,
) -> list[dict[str, Any]]:
    normalized = snapshot_payload.get("normalized") if isinstance(snapshot_payload, dict) else {}
    if not isinstance(normalized, dict):
        normalized = {}

    videos_show = normalized.get("videos_show") if isinstance(normalized.get("videos_show"), list) else []
    videos_person = normalized.get("videos_person") if isinstance(normalized.get("videos_person"), list) else []

    # Most recent Bravo show snapshots already embed person video/news payloads.
    # Avoid reloading person_source_latest on every read unless we need a fallback
    # for older snapshots that do not include embedded person items.
    if merge_person_sources and len(videos_person) == 0:
        people = normalized.get("people") if isinstance(normalized.get("people"), list) else []
        person_ids = [
            str(item.get("person_id"))
            for item in people
            if isinstance(item, dict) and isinstance(item.get("person_id"), str)
        ]
        for row in _fetch_person_snapshots(db, person_ids):
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            person_norm = payload.get("normalized") if isinstance(payload, dict) else {}
            if isinstance(person_norm, dict):
                person_videos = person_norm.get("videos")
                if isinstance(person_videos, list):
                    videos_person.extend(person_videos)

    deduped = _dedupe_items([*videos_show, *videos_person], "clip_url", merge_person_tags=True)
    for item in deduped:
        if not isinstance(item, dict):
            continue
        hosted_image_url = str(item.get("hosted_image_url") or "").strip()
        current_image_url = str(item.get("image_url") or "").strip()
        if hosted_image_url:
            if current_image_url and not str(item.get("original_image_url") or "").strip():
                item["original_image_url"] = current_image_url
            item["image_url"] = hosted_image_url
            item["thumbnail_sync_status"] = "synced"
    return deduped


def _extract_news_from_snapshot(
    snapshot_payload: dict[str, Any],
    *,
    db: SupabaseAdminClient,
) -> list[dict[str, Any]]:
    normalized = snapshot_payload.get("normalized") if isinstance(snapshot_payload, dict) else {}
    if not isinstance(normalized, dict):
        normalized = {}

    news_show = normalized.get("news_show") if isinstance(normalized.get("news_show"), list) else []
    news_person = normalized.get("news_person") if isinstance(normalized.get("news_person"), list) else []

    if len(news_person) == 0:
        people = normalized.get("people") if isinstance(normalized.get("people"), list) else []
        person_ids = [
            str(item.get("person_id"))
            for item in people
            if isinstance(item, dict) and isinstance(item.get("person_id"), str)
        ]
        for row in _fetch_person_snapshots(db, person_ids):
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            person_norm = payload.get("normalized") if isinstance(payload, dict) else {}
            if isinstance(person_norm, dict):
                person_news = person_norm.get("news")
                if isinstance(person_news, list):
                    news_person.extend(person_news)

    return _dedupe_items([*news_show, *news_person], "article_url", merge_person_tags=True)


def _normalize_bundle_for_show(
    bundle: dict[str, Any],
    *,
    people_refs: list[dict[str, Any]],
) -> dict[str, Any]:
    videos_show: list[dict[str, Any]] = []
    for raw_video in bundle.get("videos") or []:
        if not isinstance(raw_video, dict):
            continue
        title = raw_video.get("title") if isinstance(raw_video.get("title"), str) else None
        clip_url = raw_video.get("clip_url") if isinstance(raw_video.get("clip_url"), str) else None
        if not clip_url:
            continue
        videos_show.append(
            {
                "title": title,
                "runtime": raw_video.get("runtime"),
                "kicker": raw_video.get("kicker"),
                "image_url": raw_video.get("image_url"),
                "original_image_url": raw_video.get("original_image_url") or raw_video.get("image_url"),
                "hosted_image_url": raw_video.get("hosted_image_url"),
                "media_asset_id": raw_video.get("media_asset_id"),
                "thumbnail_sync_status": raw_video.get("thumbnail_sync_status"),
                "thumbnail_sync_error": raw_video.get("thumbnail_sync_error"),
                "clip_url": clip_url,
                "season_number": raw_video.get("season_number"),
                "published_at": raw_video.get("published_at"),
                "person_tags": _infer_person_tags(title, clip_url, people_refs),
            }
        )

    news_show: list[dict[str, Any]] = []
    for raw_news in bundle.get("news") or []:
        if not isinstance(raw_news, dict):
            continue
        headline = raw_news.get("headline") if isinstance(raw_news.get("headline"), str) else None
        article_url = raw_news.get("article_url") if isinstance(raw_news.get("article_url"), str) else None
        if not article_url:
            continue
        news_show.append(
            {
                "headline": headline,
                "image_url": raw_news.get("image_url"),
                "article_url": article_url,
                "published_at": raw_news.get("published_at"),
                "person_tags": _infer_person_tags(headline, article_url, people_refs),
            }
        )

    videos_person: list[dict[str, Any]] = []
    news_person: list[dict[str, Any]] = []

    normalized_people: list[dict[str, Any]] = []
    for person in bundle.get("people") or []:
        if not isinstance(person, dict):
            continue
        person_url = person.get("canonical_url") if isinstance(person.get("canonical_url"), str) else None
        person_id = None
        person_name = person.get("name") if isinstance(person.get("name"), str) else None
        for ref in people_refs:
            if ref.get("person_url") == person_url:
                person_id = ref.get("person_id")
                person_name = ref.get("person_name") or person_name
                break

        normalized_people.append(
            {
                "person_id": person_id,
                "person_name": person_name,
                "person_url": person_url,
            }
        )

        base_tag = {
            "person_id": person_id,
            "person_name": person_name,
            "person_url": person_url,
        }

        for video in person.get("videos") or []:
            if not isinstance(video, dict):
                continue
            clip_url = video.get("clip_url") if isinstance(video.get("clip_url"), str) else None
            if not clip_url:
                continue
            videos_person.append(
                {
                    "title": video.get("title"),
                    "runtime": video.get("runtime"),
                    "kicker": video.get("kicker"),
                    "image_url": video.get("image_url"),
                    "original_image_url": video.get("original_image_url") or video.get("image_url"),
                    "hosted_image_url": video.get("hosted_image_url"),
                    "media_asset_id": video.get("media_asset_id"),
                    "thumbnail_sync_status": video.get("thumbnail_sync_status"),
                    "thumbnail_sync_error": video.get("thumbnail_sync_error"),
                    "clip_url": clip_url,
                    "season_number": video.get("season_number"),
                    "published_at": video.get("published_at"),
                    "person_tags": [base_tag],
                }
            )

        for news in person.get("news") or []:
            if not isinstance(news, dict):
                continue
            article_url = news.get("article_url") if isinstance(news.get("article_url"), str) else None
            if not article_url:
                continue
            news_person.append(
                {
                    "headline": news.get("headline"),
                    "image_url": news.get("image_url"),
                    "article_url": article_url,
                    "published_at": news.get("published_at"),
                    "person_tags": [base_tag],
                }
            )

    return {
        "show": bundle.get("show") or {},
        "image_candidates": bundle.get("image_candidates") or [],
        "discovered_person_urls": bundle.get("discovered_person_urls") or [],
        "people": normalized_people,
        "videos_show": _dedupe_items(videos_show, "clip_url", merge_person_tags=True),
        "videos_person": _dedupe_items(videos_person, "clip_url", merge_person_tags=True),
        "news_show": _dedupe_items(news_show, "article_url", merge_person_tags=True),
        "news_person": _dedupe_items(news_person, "article_url", merge_person_tags=True),
    }


def _filter_bundle_by_season(bundle: dict[str, Any], season_number: int | None) -> dict[str, Any]:
    if season_number is None:
        return bundle

    filtered_bundle = dict(bundle)
    videos = bundle.get("videos") if isinstance(bundle.get("videos"), list) else []
    filtered_bundle["videos"] = [
        item
        for item in videos
        if isinstance(item, dict)
        and isinstance(item.get("season_number"), int)
        and int(item.get("season_number")) == season_number
    ]

    people = bundle.get("people") if isinstance(bundle.get("people"), list) else []
    filtered_people: list[dict[str, Any]] = []
    for person in people:
        if not isinstance(person, dict):
            continue
        person_copy = dict(person)
        person_videos = person.get("videos") if isinstance(person.get("videos"), list) else []
        person_copy["videos"] = [
            item
            for item in person_videos
            if isinstance(item, dict)
            and isinstance(item.get("season_number"), int)
            and int(item.get("season_number")) == season_number
        ]
        filtered_people.append(person_copy)
    filtered_bundle["people"] = filtered_people
    return filtered_bundle


def _is_cast_announcement(text: str | None) -> bool:
    if not isinstance(text, str) or not text.strip():
        return False
    return bool(_CAST_ANNOUNCEMENT_RE.search(text))


def _role_labels_from_cast_announcement(text: str | None) -> list[str]:
    if not _is_cast_announcement(text):
        return []
    lowered = (text or "").lower()
    labels: list[str] = []
    if re.search(r"\b(friend\s*[- ]?of)\b", lowered, re.IGNORECASE):
        labels.append("Friend-Of")
    if re.search(r"\b(full\s*[- ]?time|housewife|househusband)\b", lowered, re.IGNORECASE):
        labels.append("Full-Time")
    if not labels:
        labels.append("Cast")
    return labels


def _normalize_role_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower().strip()).strip("_")


def _get_or_create_show_role_id(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    role_label: str,
    actor: str,
    role_id_cache: dict[str, str],
) -> str | None:
    normalized = _normalize_role_name(role_label)
    if not normalized:
        return None
    if normalized in role_id_cache:
        return role_id_cache[normalized]

    row = {
        "show_id": show_id,
        "name": role_label.strip(),
        "normalized_name": normalized,
        "sort_order": 0,
        "is_active": True,
        "created_by": actor,
        "updated_by": actor,
    }
    upsert_response = (
        db.schema("core").table("show_role_catalog").upsert(row, on_conflict="show_id,normalized_name").execute()
    )
    rows = get_list_result(upsert_response, "upserting show role from bravo sync")
    if rows and rows[0].get("id"):
        role_id = str(rows[0]["id"])
        role_id_cache[normalized] = role_id
        return role_id

    existing = (
        db.schema("core")
        .table("show_role_catalog")
        .select("id")
        .eq("show_id", show_id)
        .eq("normalized_name", normalized)
        .limit(1)
        .execute()
    )
    existing_rows = get_list_result(existing, "fetching existing show role from bravo sync")
    if existing_rows and existing_rows[0].get("id"):
        role_id = str(existing_rows[0]["id"])
        role_id_cache[normalized] = role_id
        return role_id
    return None


def _persist_discovered_links_from_bravo_sync(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    actor: str,
) -> dict[str, int]:
    from api.routers import admin_show_links

    discovered = admin_show_links._discover_show_links(show_id)
    discovered.extend(admin_show_links._discover_season_links(show_id))
    discovered.extend(admin_show_links._discover_people_links(show_id))

    upserted = 0
    skipped_invalid_url = 0
    skipped_person_source_non_approved = 0
    for row in discovered:
        url = str(row.get("url") or "").strip()
        parsed = urlparse(url)
        if not url or not parsed.scheme.startswith("http"):
            skipped_invalid_url += 1
            continue
        entity_type = str(row.get("entity_type") or "show").strip().lower()
        link_kind = admin_show_links._normalize_link_kind(str(row.get("link_kind") or "other").strip().lower())
        row_status = str(row.get("status") or "").strip().lower()
        is_person_source = entity_type == "person" and link_kind in admin_show_links._PERSON_SOURCE_LINK_KINDS
        if is_person_source and row_status != "approved":
            skipped_person_source_non_approved += 1
            continue
        status = (
            "approved"
            if is_person_source
            else (row_status if row_status in {"pending", "approved", "rejected"} else "pending")
        )
        confidence_raw = row.get("confidence")
        if isinstance(confidence_raw, (int, float)):
            confidence = float(confidence_raw)
        else:
            confidence = (
                0.95
                if status == "approved"
                else (0.75 if str(row.get("link_group") or "") == "cast_announcements" else 0.65)
            )
        admin_show_links._upsert_link(
            db,
            show_id=show_id,
            entity_type=entity_type,
            entity_id=str(row.get("entity_id") or show_id),
            link_group=str(row.get("link_group") or "other"),
            link_kind=link_kind,
            url=url,
            label=(str(row.get("label")) if row.get("label") else None),
            season_number=int(row.get("season_number") or 0),
            status=status,
            confidence=confidence,
            source=(str(row.get("source")) if row.get("source") else "bravo_sync"),
            discovered_by="bravo_sync",
            metadata=(row.get("metadata") if isinstance(row.get("metadata"), dict) else {}),
            actor=actor,
        )
        upserted += 1
    return {
        "upserted": upserted,
        "skipped_invalid_url": skipped_invalid_url,
        "skipped_person_source_non_approved": skipped_person_source_non_approved,
    }


def _persist_cast_role_suggestions_from_bravo_sync(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    normalized_bundle: dict[str, Any],
    fallback_season_number: int | None,
    actor: str,
) -> dict[str, int]:
    news_items = normalized_bundle.get("news_show") if isinstance(normalized_bundle.get("news_show"), list) else []
    if not news_items:
        return {"role_suggestions": 0, "role_assignments": 0, "announcement_people": 0}

    role_id_cache: dict[str, str] = {}
    season_id_cache: dict[int, str | None] = {}
    seen_people: set[str] = set()
    role_suggestions = 0
    role_assignments = 0

    for item in news_items:
        if not isinstance(item, dict):
            continue
        headline = str(item.get("headline") or "").strip()
        if not _is_cast_announcement(headline):
            continue
        role_labels = _role_labels_from_cast_announcement(headline)
        if not role_labels:
            continue

        season_number = (
            int(item.get("season_number"))
            if isinstance(item.get("season_number"), int)
            else int(fallback_season_number or 0)
        )
        if season_number not in season_id_cache:
            season_id_cache[season_number] = (
                _resolve_season_id(db, show_id=show_id, season_number=season_number) if season_number > 0 else None
            )
        season_id = season_id_cache[season_number]
        article_url = str(item.get("article_url") or "").strip() or None

        person_tags = item.get("person_tags") if isinstance(item.get("person_tags"), list) else []
        for raw_tag in person_tags:
            if not isinstance(raw_tag, dict):
                continue
            person_id = str(raw_tag.get("person_id") or "").strip()
            if not person_id:
                continue
            seen_people.add(person_id)
            for role_label in role_labels:
                role_id = _get_or_create_show_role_id(
                    db,
                    show_id=show_id,
                    role_label=role_label,
                    actor=actor,
                    role_id_cache=role_id_cache,
                )
                if not role_id:
                    continue
                role_suggestions += 1
                assignment = {
                    "show_id": show_id,
                    "person_id": person_id,
                    "season_id": season_id,
                    "season_number": max(0, season_number),
                    "role_id": role_id,
                    "source": "bravo_cast_announcement",
                    "confidence": 0.8,
                    "metadata": {
                        "headline": headline or None,
                        "article_url": article_url,
                    },
                    "created_by": actor,
                    "updated_by": actor,
                }
                assignment_response = (
                    db.schema("core")
                    .table("show_cast_role_assignments")
                    .upsert(assignment, on_conflict="show_id,person_id,season_number,role_id")
                    .execute()
                )
                get_list_result(assignment_response, "upserting cast role suggestion from bravo sync")
                role_assignments += 1

    return {
        "role_suggestions": role_suggestions,
        "role_assignments": role_assignments,
        "announcement_people": len(seen_people),
    }


@router.post("/{show_id}/import-bravo/preview")
def preview_bravo_import(
    show_id: UUID,
    payload: BravoPreviewRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")
    _assert_show_sync_ready_for_bravo(db, show_id_str)
    show_cast = _build_show_cast_index(db, show_id_str)
    cast_person_ids = sorted({str(row.get("person_id") or "").strip() for row in show_cast if row.get("person_id")})
    cast_person_name_by_id = {
        str(row.get("person_id") or "").strip(): str(row.get("person_name") or "").strip()
        for row in show_cast
        if row.get("person_id")
    }
    link_state_by_person_id = _load_bravo_profile_link_state_by_person_id(
        db,
        show_id=show_id_str,
        cast_person_ids=cast_person_ids,
    )
    cast_url_lookup = _build_cast_person_url_lookup(show_cast)
    if payload.cast_only:
        cast_candidate_urls = _build_cast_candidate_person_urls(show_cast)
    else:
        cast_candidate_urls, _eligible_url_to_person_id = _build_eligible_cast_candidate_person_urls(
            show_cast,
            link_state_by_person_id=link_state_by_person_id,
        )
    explicit_candidate_urls = [str(url) for url in payload.person_url_candidates]
    filtered_explicit_candidate_urls = _filter_explicit_person_url_candidates(
        explicit_candidate_urls,
        cast_url_lookup=cast_url_lookup,
        link_state_by_person_id=link_state_by_person_id,
        suppress_link_state=not payload.cast_only,
    )
    person_url_candidates = _merge_person_url_candidates(filtered_explicit_candidate_urls, cast_candidate_urls)
    fandom_domains = _load_fandom_probe_domains(db, show_id=show_id_str)
    fandom_candidate_urls, _fandom_candidate_url_to_person_id, fandom_candidate_name_by_url = (
        _build_cast_candidate_fandom_urls(show_cast, community_domains=fandom_domains)
    )

    bundle = parse_bravo_show_bundle(
        str(payload.show_url),
        include_people=payload.include_people,
        include_videos=payload.include_videos,
        include_news=payload.include_news,
        person_url_candidates=person_url_candidates,
        max_people=max(40, len(person_url_candidates)),
        candidate_people_only=payload.cast_only,
        include_person_related_content=not payload.cast_only,
        hydrate_person_related_dates=not payload.cast_only,
    )
    bundle = _filter_bundle_by_season(bundle, payload.season_number)
    person_candidate_results = (
        bundle.get("person_candidate_results") if isinstance(bundle.get("person_candidate_results"), list) else []
    )
    summary = _summarize_candidate_results(person_candidate_results)
    fandom_candidate_results = (
        _probe_fandom_person_url_candidates(
            fandom_candidate_urls,
            candidate_name_by_url=fandom_candidate_name_by_url,
            max_people=max(40, len(fandom_candidate_urls)),
        )
        if payload.include_people and fandom_candidate_urls
        else []
    )
    fandom_summary = _summarize_candidate_results(fandom_candidate_results)
    fandom_people = [
        result.get("person")
        for result in fandom_candidate_results
        if isinstance(result, dict)
        and str(result.get("status") or "").strip().lower() == "ok"
        and isinstance(result.get("person"), dict)
    ]
    preview_signature = _build_preview_signature(
        show_url=str(payload.show_url),
        cast_only=bool(payload.cast_only),
        season_number=payload.season_number,
        candidate_urls=person_url_candidates,
        fandom_candidate_urls=fandom_candidate_urls,
    )

    return {
        "show": bundle.get("show") or {},
        "people": bundle.get("people") or [],
        "fandom_people": fandom_people,
        "videos": bundle.get("videos") or [],
        "news": bundle.get("news") or [],
        "image_candidates": bundle.get("image_candidates") or [],
        "discovered_person_urls": bundle.get("discovered_person_urls") or [],
        "person_candidate_results": person_candidate_results,
        "bravo_candidates_tested": summary["tested"],
        "bravo_candidates_valid": summary["valid"],
        "bravo_candidates_missing": summary["missing"],
        "bravo_candidates_errors": summary["errors"],
        "fandom_domains_used": fandom_domains,
        "fandom_candidate_urls_tested": fandom_candidate_urls,
        "fandom_candidate_results": fandom_candidate_results,
        "fandom_candidates_tested": fandom_summary["tested"],
        "fandom_candidates_valid": fandom_summary["valid"],
        "fandom_candidates_missing": fandom_summary["missing"],
        "fandom_candidates_errors": fandom_summary["errors"],
        "skipped_existing_bravo_profiles": sum(
            1 for state in link_state_by_person_id.values() if bool(state.get("has_non_rejected"))
        ),
        "skipped_na_profiles": sum(1 for state in link_state_by_person_id.values() if bool(state.get("has_na"))),
        "cast_candidate_urls_tested": person_url_candidates,
        "cast_candidate_person_names": cast_person_name_by_id,
        "show_url": str(payload.show_url),
        "cast_only": payload.cast_only,
        "season_filter": payload.season_number,
        "preview_signature": preview_signature,
    }


@router.post("/{show_id}/import-bravo/preview/stream")
def preview_bravo_import_stream(
    show_id: UUID,
    payload: BravoPreviewRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StreamingResponse:
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        return _sse_error_stream(
            {
                "error": f"Show {show_id_str} not found",
                "status": 404,
            }
        )
    try:
        _assert_show_sync_ready_for_bravo(db, show_id_str)
    except HTTPException as exc:
        return _sse_error_stream(
            {
                "error": "Show is not ready for Bravo import",
                "detail": exc.detail,
                "status": exc.status_code,
            }
        )

    show_cast = _build_show_cast_index(db, show_id_str)
    cast_person_ids = sorted({str(row.get("person_id") or "").strip() for row in show_cast if row.get("person_id")})
    cast_person_name_by_id = {
        str(row.get("person_id") or "").strip(): str(row.get("person_name") or "").strip()
        for row in show_cast
        if row.get("person_id")
    }
    link_state_by_person_id = _load_bravo_profile_link_state_by_person_id(
        db,
        show_id=show_id_str,
        cast_person_ids=cast_person_ids,
    )
    cast_url_lookup = _build_cast_person_url_lookup(show_cast)
    if payload.cast_only:
        cast_candidate_urls = _build_cast_candidate_person_urls(show_cast)
    else:
        cast_candidate_urls, _eligible_url_to_person_id = _build_eligible_cast_candidate_person_urls(
            show_cast,
            link_state_by_person_id=link_state_by_person_id,
        )
    explicit_candidate_urls = [str(url) for url in payload.person_url_candidates]
    filtered_explicit_candidate_urls = _filter_explicit_person_url_candidates(
        explicit_candidate_urls,
        cast_url_lookup=cast_url_lookup,
        link_state_by_person_id=link_state_by_person_id,
        suppress_link_state=not payload.cast_only,
    )
    person_url_candidates = _merge_person_url_candidates(filtered_explicit_candidate_urls, cast_candidate_urls)
    fandom_domains = _load_fandom_probe_domains(db, show_id=show_id_str)
    fandom_candidate_urls, _fandom_candidate_url_to_person_id, fandom_candidate_name_by_url = (
        _build_cast_candidate_fandom_urls(show_cast, community_domains=fandom_domains)
    )
    max_people = max(40, len(person_url_candidates), len(fandom_candidate_urls))
    preview_signature = _build_preview_signature(
        show_url=str(payload.show_url),
        cast_only=bool(payload.cast_only),
        season_number=payload.season_number,
        candidate_urls=person_url_candidates,
        fandom_candidate_urls=fandom_candidate_urls,
    )

    candidate_name_by_url: dict[str, str] = {}
    for candidate_url in person_url_candidates:
        normalized = _normalize_person_url(candidate_url)
        if not normalized:
            continue
        person_id = cast_url_lookup.get(normalized)
        person_name = cast_person_name_by_id.get(person_id or "", "").strip()
        if person_name:
            candidate_name_by_url[normalized] = person_name

    candidate_rows = [
        {
            "url": candidate_url,
            "name": candidate_name_by_url.get(_normalize_person_url(candidate_url) or "", "") or None,
        }
        for candidate_url in person_url_candidates
    ]
    fandom_candidate_rows = [
        {
            "url": candidate_url,
            "name": fandom_candidate_name_by_url.get(_normalize_fandom_url(candidate_url) or "", "") or None,
        }
        for candidate_url in fandom_candidate_urls
    ]

    def event_stream() -> Any:
        try:
            yield _sse_event(
                "start",
                {
                    "candidates": candidate_rows,
                    "total": len(candidate_rows),
                    "source": "bravo",
                    "fandom_candidates": fandom_candidate_rows,
                    "fandom_total": len(fandom_candidate_rows),
                    "fandom_domains_used": fandom_domains,
                    "cast_only": payload.cast_only,
                    "preview_signature": preview_signature,
                },
            )

            base_bundle = parse_bravo_show_bundle(
                str(payload.show_url),
                include_people=False,
                include_videos=payload.include_videos,
                include_news=payload.include_news,
                person_url_candidates=person_url_candidates,
                max_people=max_people,
                candidate_people_only=payload.cast_only,
            )
            base_bundle = _filter_bundle_by_season(base_bundle, payload.season_number)

            people: list[dict[str, Any]] = []
            person_candidate_results: list[dict[str, Any]] = []
            candidate_elapsed_ms_values: list[int] = []
            fandom_people: list[dict[str, Any]] = []
            fandom_candidate_results: list[dict[str, Any]] = []

            if payload.include_people:
                candidate_sequence = person_url_candidates[: max(0, max_people)]
                if payload.cast_only and candidate_sequence:
                    max_workers = min(_BRAVO_CAST_ONLY_PREVIEW_WORKER_LIMIT, len(candidate_sequence))
                    started_at = perf_counter()
                    pending: dict[Future[dict[str, Any]], tuple[str, int, float]] = {}
                    next_candidate_idx = 0
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        while next_candidate_idx < len(candidate_sequence) and len(pending) < max_workers:
                            candidate_url = candidate_sequence[next_candidate_idx]
                            candidate_index = next_candidate_idx + 1
                            normalized_candidate_url = _normalize_person_url(candidate_url)
                            candidate_name = candidate_name_by_url.get(normalized_candidate_url or "", "")
                            future = executor.submit(
                                _probe_single_bravo_candidate,
                                candidate_url,
                                include_related_content=False,
                                hydrate_related_dates=False,
                            )
                            pending[future] = (candidate_url, candidate_index, perf_counter())
                            current_summary = _summarize_candidate_results(person_candidate_results)
                            yield _sse_event(
                                "progress",
                                {
                                    "source": "bravo",
                                    "url": candidate_url,
                                    "name": candidate_name or None,
                                    "status": "in_progress",
                                    "candidate_index": candidate_index,
                                    "bravo_candidates_tested": current_summary["tested"],
                                    "bravo_candidates_valid": current_summary["valid"],
                                    "bravo_candidates_missing": current_summary["missing"],
                                    "bravo_candidates_errors": current_summary["errors"],
                                    "live_counts": {
                                        "tested": current_summary["tested"],
                                        "valid": current_summary["valid"],
                                        "missing": current_summary["missing"],
                                        "errors": current_summary["errors"],
                                    },
                                },
                            )
                            next_candidate_idx += 1

                        while pending:
                            done, _ = wait(
                                list(pending.keys()),
                                timeout=_BRAVO_PREVIEW_HEARTBEAT_SECONDS,
                                return_when=FIRST_COMPLETED,
                            )
                            if not done:
                                current_summary = _summarize_candidate_results(person_candidate_results)
                                current_fandom_summary = _summarize_candidate_results(fandom_candidate_results)
                                yield _sse_event(
                                    "heartbeat",
                                    {
                                        "source": "bravo",
                                        "cast_only": payload.cast_only,
                                        "in_flight": len(pending),
                                        "bravo_candidates_tested": current_summary["tested"],
                                        "bravo_candidates_valid": current_summary["valid"],
                                        "bravo_candidates_missing": current_summary["missing"],
                                        "bravo_candidates_errors": current_summary["errors"],
                                        "fandom_candidates_tested": current_fandom_summary["tested"],
                                        "fandom_candidates_valid": current_fandom_summary["valid"],
                                        "fandom_candidates_missing": current_fandom_summary["missing"],
                                        "fandom_candidates_errors": current_fandom_summary["errors"],
                                    },
                                )
                                continue
                            for future in done:
                                submitted_candidate_url, candidate_index, candidate_started_at = pending.pop(future)
                                candidate_elapsed_ms = max(0, int((perf_counter() - candidate_started_at) * 1000))
                                candidate_elapsed_ms_values.append(candidate_elapsed_ms)
                                try:
                                    probe = future.result()
                                except Exception as exc:  # noqa: BLE001
                                    probe = {
                                        "candidate_url": submitted_candidate_url,
                                        "url": submitted_candidate_url,
                                        "status": "error",
                                        "error": str(exc),
                                    }
                                status = str(probe.get("status") or "").strip().lower()
                                if status not in {"ok", "missing", "error"}:
                                    status = "error"

                                candidate_url = str(
                                    probe.get("candidate_url") or probe.get("url") or submitted_candidate_url
                                ).strip()
                                result_url = str(probe.get("url") or candidate_url).strip()
                                if not candidate_url:
                                    continue

                                person = probe.get("person") if isinstance(probe.get("person"), dict) else None
                                if status == "ok" and person:
                                    people.append(person)

                                result: dict[str, Any] = {
                                    "url": result_url or candidate_url,
                                    "status": status,
                                }
                                error_value = str(probe.get("error") or "").strip()
                                if error_value:
                                    result["error"] = error_value
                                person_candidate_results.append(result)

                                summary = _summarize_candidate_results(person_candidate_results)
                                normalized_candidate_url = _normalize_person_url(candidate_url)
                                normalized_result_url = _normalize_person_url(result_url)
                                name = candidate_name_by_url.get(
                                    normalized_candidate_url or "",
                                    "",
                                ) or candidate_name_by_url.get(normalized_result_url or "", "")

                                progress_payload: dict[str, Any] = {
                                    "source": "bravo",
                                    "url": candidate_url,
                                    "name": name or None,
                                    "status": status,
                                    "candidate_index": candidate_index,
                                    "elapsed_ms": candidate_elapsed_ms,
                                    "bravo_candidates_tested": summary["tested"],
                                    "bravo_candidates_valid": summary["valid"],
                                    "bravo_candidates_missing": summary["missing"],
                                    "bravo_candidates_errors": summary["errors"],
                                    "live_counts": {
                                        "tested": summary["tested"],
                                        "valid": summary["valid"],
                                        "missing": summary["missing"],
                                        "errors": summary["errors"],
                                    },
                                }
                                if error_value:
                                    progress_payload["error"] = error_value
                                if person:
                                    progress_payload["person"] = person

                                if candidate_elapsed_ms >= _BRAVO_SLOW_CANDIDATE_WARN_MS:
                                    logger.warning(
                                        (
                                            "Slow cast-only bravo probe candidate "
                                            "show_id=%s candidate_index=%s elapsed_ms=%s url=%s"
                                        ),
                                        show_id_str,
                                        candidate_index,
                                        candidate_elapsed_ms,
                                        candidate_url,
                                    )

                                yield _sse_event("progress", progress_payload)

                                if next_candidate_idx < len(candidate_sequence):
                                    next_candidate_url = candidate_sequence[next_candidate_idx]
                                    next_candidate_index = next_candidate_idx + 1
                                    normalized_next_candidate_url = _normalize_person_url(next_candidate_url)
                                    next_candidate_name = candidate_name_by_url.get(
                                        normalized_next_candidate_url or "",
                                        "",
                                    )
                                    next_future = executor.submit(
                                        _probe_single_bravo_candidate,
                                        next_candidate_url,
                                        include_related_content=False,
                                        hydrate_related_dates=False,
                                    )
                                    pending[next_future] = (
                                        next_candidate_url,
                                        next_candidate_index,
                                        perf_counter(),
                                    )
                                    yield _sse_event(
                                        "progress",
                                        {
                                            "source": "bravo",
                                            "url": next_candidate_url,
                                            "name": next_candidate_name or None,
                                            "status": "in_progress",
                                            "candidate_index": next_candidate_index,
                                            "bravo_candidates_tested": summary["tested"],
                                            "bravo_candidates_valid": summary["valid"],
                                            "bravo_candidates_missing": summary["missing"],
                                            "bravo_candidates_errors": summary["errors"],
                                            "live_counts": {
                                                "tested": summary["tested"],
                                                "valid": summary["valid"],
                                                "missing": summary["missing"],
                                                "errors": summary["errors"],
                                            },
                                        },
                                    )
                                    next_candidate_idx += 1

                    if candidate_elapsed_ms_values:
                        sorted_elapsed_ms = sorted(candidate_elapsed_ms_values)
                        avg_elapsed_ms = int(sum(sorted_elapsed_ms) / len(sorted_elapsed_ms))
                        p95_index = int((len(sorted_elapsed_ms) - 1) * 0.95)
                        p95_elapsed_ms = sorted_elapsed_ms[p95_index]
                        logger.info(
                            (
                                "Cast-only bravo preview stream completed "
                                "show_id=%s candidates=%s elapsed_total_ms=%s "
                                "avg_candidate_ms=%s p95_candidate_ms=%s"
                            ),
                            show_id_str,
                            len(candidate_elapsed_ms_values),
                            int((perf_counter() - started_at) * 1000),
                            avg_elapsed_ms,
                            p95_elapsed_ms,
                        )
                else:
                    for probe in probe_bravo_person_url_candidates(
                        candidate_sequence,
                        max_people=max_people,
                        include_related_content=not payload.cast_only,
                        hydrate_related_dates=not payload.cast_only,
                    ):
                        status = str(probe.get("status") or "").strip().lower()
                        if status not in {"ok", "missing", "error"}:
                            continue

                        candidate_url = str(probe.get("candidate_url") or probe.get("url") or "").strip()
                        result_url = str(probe.get("url") or candidate_url).strip()
                        if not candidate_url:
                            continue

                        person = probe.get("person") if isinstance(probe.get("person"), dict) else None
                        if status == "ok" and person:
                            people.append(person)

                        result = {
                            "url": result_url or candidate_url,
                            "status": status,
                        }
                        error_value = str(probe.get("error") or "").strip()
                        if error_value:
                            result["error"] = error_value
                        person_candidate_results.append(result)

                        summary = _summarize_candidate_results(person_candidate_results)
                        normalized_candidate_url = _normalize_person_url(candidate_url)
                        normalized_result_url = _normalize_person_url(result_url)
                        name = candidate_name_by_url.get(
                            normalized_candidate_url or "",
                            "",
                        ) or candidate_name_by_url.get(normalized_result_url or "", "")

                        progress_payload: dict[str, Any] = {
                            "source": "bravo",
                            "url": candidate_url,
                            "name": name or None,
                            "status": status,
                            "bravo_candidates_tested": summary["tested"],
                            "bravo_candidates_valid": summary["valid"],
                            "bravo_candidates_missing": summary["missing"],
                            "bravo_candidates_errors": summary["errors"],
                            "live_counts": {
                                "tested": summary["tested"],
                                "valid": summary["valid"],
                                "missing": summary["missing"],
                                "errors": summary["errors"],
                            },
                        }
                        if error_value:
                            progress_payload["error"] = error_value
                        if person:
                            progress_payload["person"] = person

                        yield _sse_event("progress", progress_payload)

                if fandom_candidate_urls:
                    fandom_sequence = fandom_candidate_urls[: max(0, max_people)]
                    for candidate_index, candidate_url in enumerate(fandom_sequence, start=1):
                        normalized_candidate_url = _normalize_fandom_url(candidate_url) or candidate_url
                        candidate_name = fandom_candidate_name_by_url.get(normalized_candidate_url, "")
                        bravo_summary = _summarize_candidate_results(person_candidate_results)
                        fandom_summary = _summarize_candidate_results(fandom_candidate_results)
                        yield _sse_event(
                            "progress",
                            {
                                "source": "fandom",
                                "url": candidate_url,
                                "name": candidate_name or None,
                                "status": "in_progress",
                                "candidate_index": candidate_index,
                                "bravo_candidates_tested": bravo_summary["tested"],
                                "bravo_candidates_valid": bravo_summary["valid"],
                                "bravo_candidates_missing": bravo_summary["missing"],
                                "bravo_candidates_errors": bravo_summary["errors"],
                                "fandom_candidates_tested": fandom_summary["tested"],
                                "fandom_candidates_valid": fandom_summary["valid"],
                                "fandom_candidates_missing": fandom_summary["missing"],
                                "fandom_candidates_errors": fandom_summary["errors"],
                                "live_counts": {
                                    "tested": fandom_summary["tested"],
                                    "valid": fandom_summary["valid"],
                                    "missing": fandom_summary["missing"],
                                    "errors": fandom_summary["errors"],
                                },
                            },
                        )
                        candidate_started_at = perf_counter()
                        probe = _probe_single_fandom_candidate(
                            candidate_url,
                            expected_name=candidate_name or None,
                        )
                        candidate_elapsed_ms = max(0, int((perf_counter() - candidate_started_at) * 1000))
                        status = str(probe.get("status") or "").strip().lower()
                        if status not in {"ok", "missing", "error"}:
                            status = "error"
                        result_url = str(probe.get("url") or candidate_url).strip() or candidate_url
                        result: dict[str, Any] = {
                            "candidate_url": normalized_candidate_url,
                            "url": result_url,
                            "status": status,
                        }
                        if candidate_name:
                            result["name"] = candidate_name
                        error_value = str(probe.get("error") or "").strip()
                        if error_value:
                            result["error"] = error_value
                        person = probe.get("person") if isinstance(probe.get("person"), dict) else None
                        cast_fandom = probe.get("cast_fandom") if isinstance(probe.get("cast_fandom"), dict) else None
                        photos = probe.get("photos") if isinstance(probe.get("photos"), list) else None
                        if person:
                            result["person"] = person
                        if cast_fandom:
                            result["cast_fandom"] = cast_fandom
                        if photos:
                            result["photos"] = photos
                        fandom_candidate_results.append(result)
                        if status == "ok" and person:
                            fandom_people.append(person)

                        bravo_summary = _summarize_candidate_results(person_candidate_results)
                        fandom_summary = _summarize_candidate_results(fandom_candidate_results)
                        progress_payload: dict[str, Any] = {
                            "source": "fandom",
                            "url": candidate_url,
                            "name": candidate_name or None,
                            "status": status,
                            "candidate_index": candidate_index,
                            "elapsed_ms": candidate_elapsed_ms,
                            "bravo_candidates_tested": bravo_summary["tested"],
                            "bravo_candidates_valid": bravo_summary["valid"],
                            "bravo_candidates_missing": bravo_summary["missing"],
                            "bravo_candidates_errors": bravo_summary["errors"],
                            "fandom_candidates_tested": fandom_summary["tested"],
                            "fandom_candidates_valid": fandom_summary["valid"],
                            "fandom_candidates_missing": fandom_summary["missing"],
                            "fandom_candidates_errors": fandom_summary["errors"],
                            "live_counts": {
                                "tested": fandom_summary["tested"],
                                "valid": fandom_summary["valid"],
                                "missing": fandom_summary["missing"],
                                "errors": fandom_summary["errors"],
                            },
                        }
                        if error_value:
                            progress_payload["error"] = error_value
                        if person:
                            progress_payload["person"] = person
                        yield _sse_event("progress", progress_payload)

            summary = _summarize_candidate_results(person_candidate_results)
            fandom_summary = _summarize_candidate_results(fandom_candidate_results)
            resolved_person_urls = _merge_person_url_candidates(
                [str(person.get("canonical_url") or "").strip() for person in people if isinstance(person, dict)]
            )
            complete_payload = {
                "show": base_bundle.get("show") or {},
                "people": people,
                "fandom_people": fandom_people,
                "videos": base_bundle.get("videos") or [],
                "news": base_bundle.get("news") or [],
                "image_candidates": base_bundle.get("image_candidates") or [],
                "discovered_person_urls": resolved_person_urls if payload.include_people else person_url_candidates,
                "person_candidate_results": person_candidate_results,
                "bravo_candidates_tested": summary["tested"],
                "bravo_candidates_valid": summary["valid"],
                "bravo_candidates_missing": summary["missing"],
                "bravo_candidates_errors": summary["errors"],
                "fandom_domains_used": fandom_domains,
                "fandom_candidate_urls_tested": fandom_candidate_urls,
                "fandom_candidate_results": fandom_candidate_results,
                "fandom_candidates_tested": fandom_summary["tested"],
                "fandom_candidates_valid": fandom_summary["valid"],
                "fandom_candidates_missing": fandom_summary["missing"],
                "fandom_candidates_errors": fandom_summary["errors"],
                "skipped_existing_bravo_profiles": sum(
                    1 for state in link_state_by_person_id.values() if bool(state.get("has_non_rejected"))
                ),
                "skipped_na_profiles": sum(
                    1 for state in link_state_by_person_id.values() if bool(state.get("has_na"))
                ),
                "cast_candidate_urls_tested": person_url_candidates,
                "cast_candidate_person_names": cast_person_name_by_id,
                "show_url": str(payload.show_url),
                "cast_only": payload.cast_only,
                "season_filter": payload.season_number,
                "preview_signature": preview_signature,
            }
            yield _sse_event("complete", complete_payload)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Bravo preview stream failed for show %s", show_id_str)
            yield _sse_event(
                "error",
                {
                    "error": "Bravo preview stream failed",
                    "detail": str(exc),
                    "status": 500,
                },
            )

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/{show_id}/import-bravo/commit")
def commit_bravo_import(
    show_id: UUID,
    payload: BravoCommitRequest,
    db: SupabaseAdminClient = None,
    admin_user: AdminUser = None,
):
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")
    _assert_show_sync_ready_for_bravo(db, show_id_str)
    show_cast = _build_show_cast_index(db, show_id_str)
    cast_person_ids = sorted({str(row.get("person_id") or "").strip() for row in show_cast if row.get("person_id")})
    cast_person_name_by_id = {
        str(row.get("person_id") or "").strip(): str(row.get("person_name") or "").strip()
        for row in show_cast
        if row.get("person_id")
    }
    link_state_by_person_id = _load_bravo_profile_link_state_by_person_id(
        db,
        show_id=show_id_str,
        cast_person_ids=cast_person_ids,
    )
    cast_url_lookup = _build_cast_person_url_lookup(show_cast)
    if payload.cast_only:
        cast_candidate_urls = _build_cast_candidate_person_urls(show_cast)
        eligible_candidate_url_to_person_id = dict(cast_url_lookup)
    else:
        cast_candidate_urls, eligible_candidate_url_to_person_id = _build_eligible_cast_candidate_person_urls(
            show_cast,
            link_state_by_person_id=link_state_by_person_id,
        )
    explicit_candidate_urls = [str(url) for url in payload.person_url_candidates]
    filtered_explicit_candidate_urls = _filter_explicit_person_url_candidates(
        explicit_candidate_urls,
        cast_url_lookup=cast_url_lookup,
        link_state_by_person_id=link_state_by_person_id,
        suppress_link_state=not payload.cast_only,
    )
    person_url_candidates = _merge_person_url_candidates(filtered_explicit_candidate_urls, cast_candidate_urls)
    fandom_domains = _load_fandom_probe_domains(db, show_id=show_id_str)
    fandom_candidate_urls, fandom_candidate_url_to_person_id, fandom_candidate_name_by_url = (
        _build_cast_candidate_fandom_urls(show_cast, community_domains=fandom_domains)
    )
    expected_preview_signature = _build_preview_signature(
        show_url=str(payload.show_url),
        cast_only=bool(payload.cast_only),
        season_number=payload.season_number,
        candidate_urls=person_url_candidates,
        fandom_candidate_urls=fandom_candidate_urls,
    )
    request_preview_signature = str(payload.preview_signature or "").strip() or None
    if payload.cast_only and not request_preview_signature:
        raise HTTPException(status_code=422, detail="preview_signature is required for cast-only commit")
    fandom_link_state_by_person_id = _load_fandom_link_state_by_person_id(
        db,
        show_id=show_id_str,
        cast_person_ids=cast_person_ids,
    )

    if payload.cast_only and isinstance(payload.preview_result, dict):
        _validate_cast_only_preview_reuse_or_raise(
            preview_result=payload.preview_result,
            show_url=str(payload.show_url),
            season_number=payload.season_number,
            expected_candidate_urls=person_url_candidates,
            expected_fandom_candidate_urls=fandom_candidate_urls,
            expected_preview_signature=expected_preview_signature,
            request_preview_signature=request_preview_signature,
        )
        bundle = _build_bundle_from_preview_result(payload.preview_result)
    else:
        if payload.cast_only and request_preview_signature:
            if request_preview_signature != expected_preview_signature:
                raise HTTPException(status_code=409, detail="Preview stale. Re-run preview.")
        bundle = parse_bravo_show_bundle(
            str(payload.show_url),
            include_people=True,
            include_videos=not payload.cast_only,
            include_news=not payload.cast_only,
            person_url_candidates=person_url_candidates,
            max_people=max(40, len(person_url_candidates)),
            candidate_people_only=payload.cast_only,
            include_person_related_content=not payload.cast_only,
            hydrate_person_related_dates=not payload.cast_only,
        )
    bundle = _filter_bundle_by_season(bundle, payload.season_number)
    person_candidate_results = (
        bundle.get("person_candidate_results") if isinstance(bundle.get("person_candidate_results"), list) else []
    )
    summary = _summarize_candidate_results(person_candidate_results)
    bravo_candidates_tested = summary["tested"]
    bravo_candidates_valid = summary["valid"]
    bravo_candidates_missing = summary["missing"]
    fandom_candidate_results = (
        bundle.get("fandom_candidate_results") if isinstance(bundle.get("fandom_candidate_results"), list) else []
    )
    if not fandom_candidate_results:
        fandom_candidate_results = _probe_fandom_person_url_candidates(
            fandom_candidate_urls,
            candidate_name_by_url=fandom_candidate_name_by_url,
            max_people=max(40, len(fandom_candidate_urls)),
        )
    fandom_summary = _summarize_candidate_results(fandom_candidate_results)
    fandom_candidates_tested = fandom_summary["tested"]
    fandom_candidates_valid = fandom_summary["valid"]
    fandom_candidates_missing = fandom_summary["missing"]
    missing_candidate_urls = [
        str(result.get("url") or "").strip()
        for result in person_candidate_results
        if isinstance(result, dict) and str(result.get("status") or "").strip().lower() == "missing"
    ]
    season_id: str | None = None
    if payload.season_number is not None:
        season_id = _resolve_season_id(
            db,
            show_id=show_id_str,
            season_number=int(payload.season_number),
        )
        if not season_id:
            raise HTTPException(
                status_code=404,
                detail=f"Season {payload.season_number} was not found for show {show_id_str}",
            )

    explicit_map = {k: str(v) for k, v in (payload.person_url_mappings or {}).items()}
    person_url_map = _resolve_person_url_map(
        [str(url) for url in bundle.get("discovered_person_urls") or []],
        show_cast,
        explicit_map,
    )

    people_refs: list[dict[str, Any]] = []
    seen_people_ref_ids: set[str] = set()
    for person in bundle.get("people") or []:
        if not isinstance(person, dict):
            continue
        person_url = str(person.get("canonical_url") or "").strip()
        if not person_url:
            continue
        mapped_id = person_url_map.get(person_url)
        if mapped_id:
            seen_people_ref_ids.add(mapped_id)
        people_refs.append(
            {
                "person_id": mapped_id,
                "person_name": person.get("name"),
                "person_url": person_url,
            }
        )
    for cast_member in show_cast:
        person_id = str(cast_member.get("person_id") or "").strip()
        if not person_id or person_id in seen_people_ref_ids:
            continue
        seen_people_ref_ids.add(person_id)
        people_refs.append(
            {
                "person_id": person_id,
                "person_name": cast_member.get("person_name"),
                "person_url": None,
            }
        )

    normalized = _normalize_bundle_for_show(bundle, people_refs=people_refs)
    video_thumbnail_sync = _sync_bravo_video_thumbnails(
        db=db,
        admin_user=admin_user,
        show_id=show_id_str,
        normalized=normalized,
        force=False,
        refresh_from_clip_metadata=False,
    )

    show_description = (
        payload.description_override.strip()
        if isinstance(payload.description_override, str) and payload.description_override.strip()
        else str((normalized.get("show") or {}).get("description") or "").strip() or None
    )
    show_airs = (
        payload.airs_override.strip()
        if isinstance(payload.airs_override, str) and payload.airs_override.strip()
        else str((normalized.get("show") or {}).get("airs_text") or "").strip() or None
    )

    show_payload = {
        "source": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "show_id": show_id_str,
        "show_url": str(payload.show_url),
        "cast_only": payload.cast_only,
        "fetched_at": _to_iso_now(),
        "normalized": {
            **normalized,
            "show": {
                **(normalized.get("show") or {}),
                "description": show_description,
                "airs_text": show_airs,
            },
            "season_filter": payload.season_number,
            "fandom_domains_used": fandom_domains,
            "fandom_candidate_results": fandom_candidate_results,
            "video_thumbnail_sync": video_thumbnail_sync,
        },
        "raw": bundle.get("raw") or bundle,
        "person_url_map": person_url_map,
    }

    show_snapshot = _upsert_show_snapshot(db, show_id=show_id_str, payload=show_payload)
    if not payload.cast_only:
        if season_id:
            # Bravo copy is typically season-current marketing text; for season-targeted sync
            # persist it to the selected season overview instead of overwriting global show copy.
            _persist_season_overview(
                db,
                show_id=show_id_str,
                season_id=season_id,
                overview=show_description,
            )
        else:
            _persist_show_description(db, show_id_str, show_description)
    actor = str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin")
    bravo_na_marked = _persist_missing_bravo_profile_markers(
        db,
        show_id=show_id_str,
        actor=actor,
        missing_candidate_urls=missing_candidate_urls,
        candidate_url_to_person_id=eligible_candidate_url_to_person_id,
        cast_person_name_by_id=cast_person_name_by_id,
        link_state_by_person_id=link_state_by_person_id,
    )
    fandom_links_upserted = _persist_valid_fandom_profile_links(
        db,
        show_id=show_id_str,
        actor=actor,
        fandom_candidate_results=fandom_candidate_results,
        candidate_url_to_person_id=fandom_candidate_url_to_person_id,
        cast_person_name_by_id=cast_person_name_by_id,
        link_state_by_person_id=fandom_link_state_by_person_id,
    )
    fandom_na_marked = _persist_missing_fandom_profile_markers(
        db,
        show_id=show_id_str,
        actor=actor,
        fandom_candidate_results=fandom_candidate_results,
        candidate_url_to_person_id=fandom_candidate_url_to_person_id,
        cast_person_name_by_id=cast_person_name_by_id,
        link_state_by_person_id=fandom_link_state_by_person_id,
    )
    discovered_links = 0
    discovered_link_skips = {
        "invalid_url": 0,
        "person_source_non_approved": 0,
    }
    role_suggestion_stats = {
        "role_suggestions": 0,
        "role_assignments": 0,
        "announcement_people": 0,
    }
    if not payload.cast_only:
        discovered_link_stats = _persist_discovered_links_from_bravo_sync(
            db,
            show_id=show_id_str,
            actor=actor,
        )
        discovered_links = int(discovered_link_stats.get("upserted") or 0)
        discovered_link_skips = {
            "invalid_url": int(discovered_link_stats.get("skipped_invalid_url") or 0),
            "person_source_non_approved": int(discovered_link_stats.get("skipped_person_source_non_approved") or 0),
        }
        role_suggestion_stats = _persist_cast_role_suggestions_from_bravo_sync(
            db,
            show_id=show_id_str,
            normalized_bundle=normalized,
            fallback_season_number=payload.season_number,
            actor=actor,
        )
    cast_matrix_sync: dict[str, Any] | None = None
    cast_matrix_sync_error: str | None = None
    if payload.sync_cast_matrix and not payload.cast_only:
        try:
            from api.routers.admin_show_roles import CastMatrixSyncRequest, sync_cast_matrix_for_show

            cast_matrix_sync = sync_cast_matrix_for_show(
                show_id=show_id_str,
                payload=CastMatrixSyncRequest(
                    season_numbers=[int(payload.season_number)] if payload.season_number else [],
                    include_relationship_roles=True,
                    include_bravo_links=True,
                    include_bravo_images=True,
                    dry_run=False,
                ),
                db=db,
                admin_user=admin_user,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("cast-matrix sync failed for show_id=%s", show_id_str)
            cast_matrix_sync_error = str(exc)

    person_snapshots: list[dict[str, Any]] = []
    updated_people = 0
    unmatched_people: list[str] = []
    imported_person_images = 0
    skipped_person_images = 0
    fandom_profiles_upserted = 0
    fandom_fallback_images_imported = 0
    person_image_import_errors: list[str] = []
    profile_image_promoted_by_person_id: dict[str, bool] = {}
    fandom_profile_stage_done: set[str] = set()
    fandom_image_stage_done: set[str] = set()

    for person in bundle.get("people") or []:
        if not isinstance(person, dict):
            continue
        person_url = str(person.get("canonical_url") or "").strip()
        if not person_url:
            continue
        person_id = person_url_map.get(person_url)
        if not person_id:
            unmatched_people.append(person_url)
            continue
        profile_image_promoted_by_person_id.setdefault(person_id, False)

        person_payload = {
            "source": _BRAVO_SOURCE_ID,
            "variant": _BRAVO_VARIANT,
            "show_id": show_id_str,
            "person_id": person_id,
            "person_url": person_url,
            "fetched_at": _to_iso_now(),
            "normalized": {
                "person_id": person_id,
                "person_url": person_url,
                "name": person.get("name"),
                "bio": person.get("bio"),
                "hero_image_url": person.get("hero_image_url"),
                "social_links": person.get("social_links") or {},
                "videos": person.get("videos") or [],
                "news": person.get("news") or [],
            },
            "raw": person,
        }

        snapshot_meta = _upsert_person_snapshot(db, person_id=person_id, payload=person_payload)
        person_snapshots.append(snapshot_meta)

        social_links = person.get("social_links") if isinstance(person.get("social_links"), dict) else {}
        _persist_person_profile(
            db,
            person_id=person_id,
            person_url=person_url,
            bio=person.get("bio") if isinstance(person.get("bio"), str) else None,
            hero_image_url=(person.get("hero_image_url") if isinstance(person.get("hero_image_url"), str) else None),
            social_links={str(k): str(v) for k, v in social_links.items() if isinstance(v, str)},
        )

        hero_image_url = person.get("hero_image_url") if isinstance(person.get("hero_image_url"), str) else None
        if hero_image_url and hero_image_url.strip():
            try:
                person_import_result = _import_bravo_person_image(
                    db=db,
                    admin_user=admin_user,
                    show_id=show_id_str,
                    season_id=season_id,
                    season_number=payload.season_number,
                    person_id=person_id,
                    person_url=person_url,
                    hero_image_url=hero_image_url.strip(),
                    person_name=person.get("name") if isinstance(person.get("name"), str) else None,
                )
                for asset_id in person_import_result.get("asset_ids") or []:
                    if not isinstance(asset_id, str) or not asset_id.strip():
                        continue
                    _promote_bravo_profile_media_link(
                        db=db,
                        person_id=person_id,
                        person_url=person_url,
                        media_asset_id=asset_id.strip(),
                        season_number=payload.season_number,
                    )
                    profile_image_promoted_by_person_id[person_id] = True
                hosted_profile_url = person_import_result.get("primary_hosted_url")
                if isinstance(hosted_profile_url, str) and hosted_profile_url.strip():
                    _persist_person_profile(
                        db,
                        person_id=person_id,
                        person_url=person_url,
                        bio=person.get("bio") if isinstance(person.get("bio"), str) else None,
                        hero_image_url=hosted_profile_url.strip(),
                        social_links={str(k): str(v) for k, v in social_links.items() if isinstance(v, str)},
                    )
                    profile_image_promoted_by_person_id[person_id] = True
                imported_person_images += int(person_import_result.get("imported") or 0)
                skipped_person_images += int(person_import_result.get("skipped") or 0)
                person_image_import_errors.extend(
                    [
                        str(error)
                        for error in (person_import_result.get("errors") or [])
                        if isinstance(error, str) and error.strip()
                    ]
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("Bravo person image import failed for person_id=%s", person_id)
                person_image_import_errors.append(f"{person_id}: {exc}")
        updated_people += 1

    for result in fandom_candidate_results:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        if status != "ok":
            continue
        candidate_url = _normalize_fandom_url(str(result.get("candidate_url") or "").strip())
        if not candidate_url:
            continue
        person_id = fandom_candidate_url_to_person_id.get(candidate_url)
        if not person_id:
            continue
        person_url = _normalize_fandom_url(str(result.get("url") or "").strip()) or candidate_url
        person_name = cast_person_name_by_id.get(person_id) or (
            str((result.get("person") or {}).get("name") or "").strip()
            if isinstance(result.get("person"), dict)
            else ""
        )
        cast_fandom_payload = result.get("cast_fandom") if isinstance(result.get("cast_fandom"), dict) else None
        if cast_fandom_payload and person_id not in fandom_profile_stage_done:
            try:
                row = dict(cast_fandom_payload)
                row["person_id"] = person_id
                row["source"] = "fandom"
                upsert_cast_fandom(db, row)
                fandom_profiles_upserted += 1
                row_bio_raw = row.get("casting_summary") or row.get("summary")
                row_bio = str(row_bio_raw).strip() if isinstance(row_bio_raw, str) and row_bio_raw.strip() else None
                _persist_person_profile(
                    db,
                    person_id=person_id,
                    person_url=person_url,
                    bio=row_bio,
                    hero_image_url=None,
                    social_links={},
                    source="fandom",
                )
                fandom_profile_stage_done.add(person_id)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Fandom profile upsert failed for person_id=%s", person_id)
                person_image_import_errors.append(f"{person_id}: fandom_profile_upsert_failed: {exc}")

        if profile_image_promoted_by_person_id.get(person_id) or person_id in fandom_image_stage_done:
            continue
        photos = result.get("photos") if isinstance(result.get("photos"), list) else []
        fallback_image_url = _select_fandom_profile_image_url(photos)
        if not fallback_image_url:
            continue
        try:
            fandom_import_result = _import_fandom_person_image(
                db=db,
                admin_user=admin_user,
                show_id=show_id_str,
                season_id=season_id,
                season_number=payload.season_number,
                person_id=person_id,
                person_url=person_url,
                image_url=fallback_image_url,
                person_name=person_name or None,
            )
            for asset_id in fandom_import_result.get("asset_ids") or []:
                if not isinstance(asset_id, str) or not asset_id.strip():
                    continue
                _promote_fandom_profile_media_link(
                    db=db,
                    person_id=person_id,
                    person_url=person_url,
                    media_asset_id=asset_id.strip(),
                    season_number=payload.season_number,
                )
                profile_image_promoted_by_person_id[person_id] = True
                fandom_image_stage_done.add(person_id)
            hosted_profile_url = fandom_import_result.get("primary_hosted_url")
            if isinstance(hosted_profile_url, str) and hosted_profile_url.strip():
                _persist_person_profile(
                    db,
                    person_id=person_id,
                    person_url=person_url,
                    bio=None,
                    hero_image_url=hosted_profile_url.strip(),
                    social_links={},
                    source="fandom",
                )
                profile_image_promoted_by_person_id[person_id] = True
                fandom_image_stage_done.add(person_id)
            imported_person_images += int(fandom_import_result.get("imported") or 0)
            skipped_person_images += int(fandom_import_result.get("skipped") or 0)
            fandom_fallback_images_imported += int(fandom_import_result.get("imported") or 0)
            person_image_import_errors.extend(
                [
                    str(error)
                    for error in (fandom_import_result.get("errors") or [])
                    if isinstance(error, str) and error.strip()
                ]
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Fandom fallback image import failed for person_id=%s", person_id)
            person_image_import_errors.append(f"{person_id}: fandom_fallback_import_failed: {exc}")

    imported_show_images = 0
    imported_show_images_skipped = 0
    image_import_errors: list[str] = []

    selected_show_images: list[dict[str, str]] = []
    if not payload.cast_only:
        seen_urls: set[str] = set()
        if payload.selected_show_images:
            for selected in payload.selected_show_images:
                selected_url = str(selected.url).strip()
                if not selected_url or selected_url in seen_urls:
                    continue
                seen_urls.add(selected_url)
                selected_show_images.append(
                    {
                        "url": selected_url,
                        "kind": selected.kind,
                    }
                )
        elif payload.selected_show_image_urls:
            for selected_url in payload.selected_show_image_urls:
                normalized_url = str(selected_url).strip()
                if not normalized_url or normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)
                selected_show_images.append(
                    {
                        "url": normalized_url,
                        "kind": "promo",
                    }
                )

    if selected_show_images:
        try:
            # Reuse the existing scrape import pipeline for show-linked images.
            from api.routers.admin_scrape import ImportImageItem, ImportRequest, import_images

            import_request = ImportRequest(
                entity_type="show",
                show_id=UUID(show_id_str),
                source_url=str(payload.show_url),
                images=[
                    ImportImageItem(
                        candidate_id=_stable_show_image_candidate_id(image["url"]),
                        url=image["url"],
                        caption=f"Bravo import ({image['kind']})",
                        kind=image["kind"],
                    )
                    for image in selected_show_images
                ],
            )
            import_result = import_images(import_request, db, admin_user)
            imported_show_images = import_result.imported
            imported_show_images_skipped = import_result.skipped_duplicates
            image_import_errors = import_result.errors
        except Exception as exc:  # noqa: BLE001
            logger.exception("Bravo show image import failed")
            image_import_errors.append(str(exc))

    return {
        "show_snapshot": show_snapshot,
        "person_snapshots": person_snapshots,
        "cast_matrix_sync": cast_matrix_sync,
        "cast_matrix_sync_error": cast_matrix_sync_error,
        "video_thumbnail_sync": video_thumbnail_sync,
        "counts": {
            "show_videos": len(normalized.get("videos_show") or []),
            "show_news": len(normalized.get("news_show") or []),
            "person_videos": len(normalized.get("videos_person") or []),
            "person_news": len(normalized.get("news_person") or []),
            "video_thumbnail_attempted": int(video_thumbnail_sync.get("attempted") or 0),
            "video_thumbnail_synced": int(video_thumbnail_sync.get("synced") or 0),
            "video_thumbnail_failed": int(video_thumbnail_sync.get("failed") or 0),
            "video_thumbnail_missing_source": int(video_thumbnail_sync.get("missing_source") or 0),
            "people_updated": updated_people,
            "unmatched_people": len(unmatched_people),
            "imported_show_images": imported_show_images,
            "skipped_show_images": imported_show_images_skipped,
            "imported_person_images": imported_person_images,
            "skipped_person_images": skipped_person_images,
            "discovered_links": discovered_links,
            "discovered_links_skipped_invalid_url": discovered_link_skips["invalid_url"],
            "discovered_links_skipped_person_source_non_approved": discovered_link_skips[
                "person_source_non_approved"
            ],
            "role_suggestions": role_suggestion_stats.get("role_suggestions", 0),
            "role_assignments": role_suggestion_stats.get("role_assignments", 0),
            "announcement_people": role_suggestion_stats.get("announcement_people", 0),
            "cast_matrix_season_roles": int(
                ((cast_matrix_sync or {}).get("counts") or {}).get("season_role_assignments_upserted", 0)
            ),
            "cast_matrix_relationship_roles": int(
                ((cast_matrix_sync or {}).get("counts") or {}).get("relationship_role_assignments_upserted", 0)
            ),
            "cast_matrix_kid_roles": int(
                ((cast_matrix_sync or {}).get("counts") or {}).get("global_kid_assignments_upserted", 0)
            ),
            "cast_matrix_bravo_links": int(
                ((cast_matrix_sync or {}).get("counts") or {}).get("bravo_links_upserted", 0)
            ),
            "bravo_candidates_tested": bravo_candidates_tested,
            "bravo_candidates_valid": bravo_candidates_valid,
            "bravo_candidates_missing": bravo_candidates_missing,
            "bravo_na_marked": bravo_na_marked,
            "fandom_candidates_tested": fandom_candidates_tested,
            "fandom_candidates_valid": fandom_candidates_valid,
            "fandom_candidates_missing": fandom_candidates_missing,
            "fandom_candidates_errors": fandom_summary["errors"],
            "fandom_profiles_upserted": fandom_profiles_upserted,
            "fandom_links_upserted": fandom_links_upserted,
            "fandom_na_marked": fandom_na_marked,
            "fandom_fallback_images_imported": fandom_fallback_images_imported,
        },
        "unmatched_person_urls": unmatched_people,
        "person_candidate_results": person_candidate_results,
        "fandom_candidate_results": fandom_candidate_results,
        "fandom_domains_used": fandom_domains,
        "image_import_errors": image_import_errors,
        "person_image_import_errors": person_image_import_errors,
        "preview_signature": expected_preview_signature,
    }


@router.post("/{show_id}/bravo/videos/sync-thumbnails")
def sync_bravo_video_thumbnails(
    show_id: UUID,
    payload: BravoVideoThumbnailSyncRequest,
    db: SupabaseAdminClient = None,
    admin_user: AdminUser = None,
):
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")

    snapshot = _fetch_show_snapshot(db, show_id_str)
    snapshot_payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}
    if not isinstance(snapshot_payload, dict):
        raise HTTPException(status_code=409, detail="Bravo snapshot payload missing for this show")

    normalized = snapshot_payload.get("normalized") if isinstance(snapshot_payload.get("normalized"), dict) else {}
    if not isinstance(normalized, dict):
        raise HTTPException(status_code=409, detail="Bravo snapshot normalized payload missing for this show")

    video_thumbnail_sync = _sync_bravo_video_thumbnails(
        db=db,
        admin_user=admin_user,
        show_id=show_id_str,
        normalized=normalized,
        force=bool(payload.force),
        refresh_from_clip_metadata=True,
    )
    normalized["video_thumbnail_sync"] = {
        **video_thumbnail_sync,
        "forced": bool(payload.force),
        "synced_at": _to_iso_now(),
    }
    snapshot_payload["normalized"] = normalized

    updated_snapshot = _upsert_show_snapshot(db, show_id=show_id_str, payload=snapshot_payload)
    pending_remaining = sum(
        1
        for item in _extract_videos_from_snapshot(snapshot_payload, merge_person_sources=True, db=db)
        if isinstance(item, dict) and _video_item_needs_thumbnail_sync(item)
    )

    return {
        "show_snapshot": updated_snapshot,
        "video_thumbnail_sync": video_thumbnail_sync,
        "skipped": int(video_thumbnail_sync.get("attempted") or 0) == 0,
        "pending_remaining": pending_remaining,
    }


@router.get("/{show_id}/bravo/videos")
def get_bravo_videos(
    show_id: UUID,
    season_number: int | None = Query(default=None),
    person_id: UUID | None = Query(default=None),
    merge_person_sources: bool = Query(default=True),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")

    snapshot = _fetch_show_snapshot(db, show_id_str)
    payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}

    videos = _extract_videos_from_snapshot(payload, merge_person_sources=merge_person_sources, db=db)

    filtered = videos
    if season_number is not None:
        filtered = [
            item
            for item in filtered
            if isinstance(item.get("season_number"), int) and int(item.get("season_number")) == season_number
        ]

    if person_id is not None:
        person_id_str = str(person_id)
        filtered = [
            item
            for item in filtered
            if any(
                isinstance(tag, dict) and str(tag.get("person_id") or "") == person_id_str
                for tag in (item.get("person_tags") or [])
            )
        ]

    return {
        "videos": filtered,
        "count": len(filtered),
        "snapshot": {
            "show_id": snapshot.get("show_id"),
            "source_id": snapshot.get("source_id"),
            "variant": snapshot.get("variant"),
            "fetched_at": snapshot.get("fetched_at"),
            "payload_sha256": snapshot.get("payload_sha256"),
        },
    }


@router.get("/{show_id}/bravo/news")
def get_bravo_news(
    show_id: UUID,
    person_id: UUID | None = Query(default=None),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")

    snapshot = _fetch_show_snapshot(db, show_id_str)
    payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}

    news = _extract_news_from_snapshot(payload, db=db)

    filtered = news
    if person_id is not None:
        person_id_str = str(person_id)
        filtered = [
            item
            for item in filtered
            if any(
                isinstance(tag, dict) and str(tag.get("person_id") or "") == person_id_str
                for tag in (item.get("person_tags") or [])
            )
        ]

    return {
        "news": filtered,
        "count": len(filtered),
        "snapshot": {
            "show_id": snapshot.get("show_id"),
            "source_id": snapshot.get("source_id"),
            "variant": snapshot.get("variant"),
            "fetched_at": snapshot.get("fetched_at"),
            "payload_sha256": snapshot.get("payload_sha256"),
        },
    }
