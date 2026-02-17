"""Admin endpoints for Bravo sync/import workflows."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import unquote, urlparse
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, HttpUrl

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.scraping.bravo_parser import parse_bravo_show_bundle

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-bravo"])

_BRAVO_SOURCE_ID = "bravo"
_BRAVO_VARIANT = "default"
_CAST_ANNOUNCEMENT_RE = re.compile(
    r"\b(cast|friend\s*[- ]?of|full\s*[- ]?time|housewife|joins|joined|returning|returns)\b",
    re.IGNORECASE,
)


class BravoPreviewRequest(BaseModel):
    show_url: HttpUrl
    include_people: bool = True
    include_videos: bool = True
    include_news: bool = True
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
    season_number: int | None = Field(default=None, ge=1, le=200)
    sync_cast_matrix: bool = True


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
                db.schema("core")
                .table("show_cast")
                .select("person_id")
                .eq("show_id", show_id)
                .limit(1)
                .execute()
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
    if key in _SOCIAL_PLATFORMS or (
        key.endswith("_id") and key[:-3] in _SOCIAL_PLATFORMS
    ):
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
        if isinstance(legacy, str) and legacy.strip() and not (
            isinstance(canonical, str) and canonical.strip()
        ):
            out[f"{platform}_id"] = legacy.strip()
        if isinstance(canonical, str) and canonical.strip() and not (
            isinstance(legacy, str) and legacy.strip()
        ):
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
        "biography": _merge_source_value(row.get("biography"), "bravo", bio),
        "homepage": _merge_source_value(row.get("homepage"), "bravo", person_url),
        "profile_image_url": _merge_source_value(row.get("profile_image_url"), "bravo", hero_image_url),
        "external_ids": _merge_external_ids_fill_missing(row.get("external_ids"), social_links),
    }

    update_resp = db.schema("core").table("people").update(payload).eq("id", person_id).execute()
    if getattr(update_resp, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to update person {person_id}")


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
    from api.routers.admin_scrape import ImportImageItem, ImportRequest, import_images

    # Bravo person hero/profile images should be treated as season promos while also
    # linking into each person's gallery.
    if season_number is not None:
        import_request = ImportRequest(
            entity_type="season",
            show_id=UUID(show_id),
            season_id=UUID(season_id) if season_id else None,
            season_number=season_number,
            source_url=person_url,
            images=[
                ImportImageItem(
                    candidate_id=f"bravo-person-hero-{person_id}",
                    url=hero_image_url,
                    caption=f"Bravo profile picture{f' ({person_name})' if person_name else ''}",
                    kind="promo",
                    person_ids=[UUID(person_id)],
                    context_section="bravo_profile",
                    context_type="profile_picture",
                    asset_name="Bravo profile picture",
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
                    candidate_id=f"bravo-person-hero-{person_id}",
                    url=hero_image_url,
                    caption=f"Bravo profile picture{f' ({person_name})' if person_name else ''}",
                    kind="promo",
                    context_section="bravo_profile",
                    context_type="profile_picture",
                    asset_name="Bravo profile picture",
                )
            ],
        )
    import_result = import_images(import_request, db, admin_user)
    return {
        "imported": int(import_result.imported),
        "skipped": int(import_result.skipped_duplicates),
        "errors": list(import_result.errors),
    }


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

    return _dedupe_items([*videos_show, *videos_person], "clip_url", merge_person_tags=True)


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
        db.schema("core")
        .table("show_role_catalog")
        .upsert(row, on_conflict="show_id,normalized_name")
        .execute()
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


def _persist_pending_links_from_bravo_sync(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    actor: str,
) -> int:
    from api.routers import admin_show_links

    discovered = admin_show_links._discover_show_links(show_id)
    discovered.extend(admin_show_links._discover_season_links(show_id))
    discovered.extend(admin_show_links._discover_people_links(show_id))

    upserted = 0
    for row in discovered:
        url = str(row.get("url") or "").strip()
        parsed = urlparse(url)
        if not url or not parsed.scheme.startswith("http"):
            continue
        admin_show_links._upsert_link(
            db,
            show_id=show_id,
            entity_type=str(row.get("entity_type") or "show"),
            entity_id=str(row.get("entity_id") or show_id),
            link_group=str(row.get("link_group") or "other"),
            link_kind=str(row.get("link_kind") or "other"),
            url=url,
            label=(str(row.get("label")) if row.get("label") else None),
            season_number=int(row.get("season_number") or 0),
            status="pending",
            confidence=0.75 if str(row.get("link_group") or "") == "cast_announcements" else 0.65,
            source=(str(row.get("source")) if row.get("source") else "bravo_sync"),
            discovered_by="bravo_sync",
            metadata=(row.get("metadata") if isinstance(row.get("metadata"), dict) else {}),
            actor=actor,
        )
        upserted += 1
    return upserted


def _persist_cast_role_suggestions_from_bravo_sync(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    normalized_bundle: dict[str, Any],
    fallback_season_number: int | None,
    actor: str,
) -> dict[str, int]:
    news_items = (
        normalized_bundle.get("news_show")
        if isinstance(normalized_bundle.get("news_show"), list)
        else []
    )
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
                _resolve_season_id(db, show_id=show_id, season_number=season_number)
                if season_number > 0
                else None
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

    bundle = parse_bravo_show_bundle(
        str(payload.show_url),
        include_people=payload.include_people,
        include_videos=payload.include_videos,
        include_news=payload.include_news,
    )
    bundle = _filter_bundle_by_season(bundle, payload.season_number)

    return {
        "show": bundle.get("show") or {},
        "people": bundle.get("people") or [],
        "videos": bundle.get("videos") or [],
        "news": bundle.get("news") or [],
        "image_candidates": bundle.get("image_candidates") or [],
        "discovered_person_urls": bundle.get("discovered_person_urls") or [],
    }


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

    bundle = parse_bravo_show_bundle(
        str(payload.show_url),
        include_people=True,
        include_videos=True,
        include_news=True,
    )
    bundle = _filter_bundle_by_season(bundle, payload.season_number)
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

    show_cast = _build_show_cast_index(db, show_id_str)
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
        "fetched_at": _to_iso_now(),
        "normalized": {
            **normalized,
            "show": {
                **(normalized.get("show") or {}),
                "description": show_description,
                "airs_text": show_airs,
            },
            "season_filter": payload.season_number,
        },
        "raw": bundle.get("raw") or bundle,
        "person_url_map": person_url_map,
    }

    show_snapshot = _upsert_show_snapshot(db, show_id=show_id_str, payload=show_payload)
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
    actor = str(
        (admin_user or {}).get("email")
        or (admin_user or {}).get("id")
        or "admin"
    )
    discovered_links = _persist_pending_links_from_bravo_sync(
        db,
        show_id=show_id_str,
        actor=actor,
    )
    role_suggestion_stats = _persist_cast_role_suggestions_from_bravo_sync(
        db,
        show_id=show_id_str,
        normalized_bundle=normalized,
        fallback_season_number=payload.season_number,
        actor=actor,
    )
    cast_matrix_sync: dict[str, Any] | None = None
    cast_matrix_sync_error: str | None = None
    if payload.sync_cast_matrix:
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
    person_image_import_errors: list[str] = []

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
            hero_image_url=(
                person.get("hero_image_url") if isinstance(person.get("hero_image_url"), str) else None
            ),
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

    imported_show_images = 0
    imported_show_images_skipped = 0
    image_import_errors: list[str] = []

    selected_show_images: list[dict[str, str]] = []
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
                        candidate_id=f"bravo-{index + 1}",
                        url=image["url"],
                        caption=f"Bravo import ({image['kind']})",
                        kind=image["kind"],
                    )
                    for index, image in enumerate(selected_show_images)
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
        "counts": {
            "show_videos": len(normalized.get("videos_show") or []),
            "show_news": len(normalized.get("news_show") or []),
            "person_videos": len(normalized.get("videos_person") or []),
            "person_news": len(normalized.get("news_person") or []),
            "people_updated": updated_people,
            "unmatched_people": len(unmatched_people),
            "imported_show_images": imported_show_images,
            "skipped_show_images": imported_show_images_skipped,
            "imported_person_images": imported_person_images,
            "skipped_person_images": skipped_person_images,
            "discovered_links": discovered_links,
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
        },
        "unmatched_person_urls": unmatched_people,
        "image_import_errors": image_import_errors,
        "person_image_import_errors": person_image_import_errors,
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
