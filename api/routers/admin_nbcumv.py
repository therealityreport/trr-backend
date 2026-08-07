"""Admin endpoints for NBCUMV press photo preview/import."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.integrations import getty, nbcumv
from trr_backend.media.image_variants import generate_media_asset_variants
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    build_shared_media_s3_key,
    get_s3_bucket,
    get_s3_client,
    guess_ext_from_content_type,
    upload_bytes_to_s3,
)
from trr_backend.repositories.media_assets import asset_id_for
from trr_backend.repositories.web_scrape_images import create_media_link_for_entity

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/nbcumv", tags=["admin-nbcumv"])

_NBCUMV_SOURCE = "nbcumv"
_GETTY_SOURCE = "getty"


def _postgres_text_array_literal(values: list[str]) -> str:
    escaped: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned:
            continue
        escaped.append('"' + cleaned.replace("\\", "\\\\").replace('"', '\\"') + '"')
    return "{" + ",".join(escaped) + "}"


class NbcumvPreviewRequest(BaseModel):
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
    limit: int = Field(default=25, ge=1, le=100)

    @model_validator(mode="after")
    def validate_filters(self):
        has_any_filter = any(
            [
                self.filename,
                self.lbx_id,
                self.show_id,
                self.search_text,
                self.nup_prefix,
                self.show_name,
                self.meta_type,
                self.season,
                self.episode,
                self.network,
                self.created_start,
                self.created_end,
                self.live_date_start,
                self.live_date_end,
                self.search_caption,
            ]
        )
        if not has_any_filter:
            raise ValueError("Provide at least one NBCUMV filter")
        if bool(self.created_start) != bool(self.created_end):
            raise ValueError("created_start and created_end must both be provided")
        if bool(self.live_date_start) != bool(self.live_date_end):
            raise ValueError("live_date_start and live_date_end must both be provided")
        return self


class NbcumvImportItem(BaseModel):
    lbx_id: str
    lbx_filename: str
    location: str | None = None
    nbcumv_image: dict[str, Any] | None = None
    getty_asset: dict[str, Any] | None = None
    show_ids: list[str] = Field(default_factory=list)
    link_show_ids: list[UUID] = Field(default_factory=list)
    getty_detail_url: str | None = None
    gallery_bucket: dict[str, Any] | None = None
    person_ids: list[UUID] | None = None


class NbcumvImportRequest(BaseModel):
    items: list[NbcumvImportItem] = Field(min_length=1, max_length=100)
    assign_people: bool = True


def _ensure_sources(db: SupabaseAdminClient) -> None:
    rows = [
        {
            "id": _NBCUMV_SOURCE,
            "category": "vendor",
            "aliases": _postgres_text_array_literal(["nbcu", "nbcumv", "nbc media village"]),
        },
        {
            "id": _GETTY_SOURCE,
            "category": "vendor",
            "aliases": _postgres_text_array_literal(["getty images", "gettyimages"]),
        },
    ]
    response = db.schema("core").table("sources").upsert(rows, on_conflict="id").execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to ensure source rows: {response.error}")


def _normalize_person_name(value: str | None) -> str:
    cleaned = str(value or "").strip().lower()
    if not cleaned:
        return ""
    for marker in (" - television personality", " - tv personality"):
        if cleaned.endswith(marker):
            cleaned = cleaned[: -len(marker)]
    cleaned = cleaned.replace("&", " and ")
    cleaned = " ".join(cleaned.replace(",", " ").split())
    allowed = []
    for char in cleaned:
        if char.isalnum() or char in {" ", "'", "-", "."}:
            allowed.append(char)
    normalized = "".join(allowed).replace(".", "")
    return " ".join(normalized.split())


def _extract_caption_people(caption: str | None) -> list[str]:
    text = str(caption or "").strip()
    if not text:
        return []
    marker = "Pictured:"
    if marker not in text:
        return []
    people_text = text.split(marker, 1)[1]
    for stop in (" --", " (Photo", " (photo", ". Photo"):
        if stop in people_text:
            people_text = people_text.split(stop, 1)[0]
    people_text = people_text.strip()
    if not people_text:
        return []
    raw_parts: list[str] = []
    for chunk in people_text.replace(" & ", ", ").replace(" and ", ", ").split(","):
        cleaned = chunk.strip().strip(".")
        if cleaned:
            raw_parts.append(cleaned)
    return raw_parts


def _extract_keyword_people(keywords: Any) -> list[str]:
    if not isinstance(keywords, str):
        return []
    names: list[str] = []
    for token in keywords.replace(";", ",").split(","):
        cleaned = token.strip()
        if not cleaned:
            continue
        if len(cleaned.split()) < 2 or len(cleaned.split()) > 4:
            continue
        if not all(part[:1].isupper() for part in cleaned.split() if part[:1].isalpha()):
            continue
        names.append(cleaned)
    return names


def _extract_getty_people(getty_asset: dict[str, Any] | None) -> list[str]:
    if not getty_asset:
        return []
    people = getty_asset.get("people")
    if not isinstance(people, list):
        return []
    results: list[str] = []
    for entry in people:
        if not isinstance(entry, dict):
            continue
        label = str(entry.get("text") or "").strip()
        if not label:
            continue
        if " - " in label:
            label = label.split(" - ", 1)[0].strip()
        if label:
            results.append(label)
    return results


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned:
            continue
        lowered = cleaned.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(cleaned)
    return deduped


def _load_eligible_people_index(db: SupabaseAdminClient) -> dict[str, list[dict[str, str]]]:
    response = db.schema("core").table("v_person_show_seasons").select("person_id, person_name").limit(5000).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to load eligible people: {response.error}")
    rows = response.data or []

    people_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        person_name = str(row.get("person_name") or "").strip()
        if not person_id or not person_name:
            continue
        people_by_id.setdefault(person_id, {"person_id": person_id, "full_name": person_name, "aliases": set()})
        people_by_id[person_id]["aliases"].add(person_name)

    person_ids = list(people_by_id.keys())
    if person_ids:
        overrides = (
            db.schema("core")
            .table("people_overrides")
            .select("person_id, full_name_override")
            .in_("person_id", person_ids)
            .limit(max(1000, len(person_ids) * 2))
            .execute()
        )
        if not getattr(overrides, "error", None):
            for row in overrides.data or []:
                person_id = str(row.get("person_id") or "").strip()
                override = str(row.get("full_name_override") or "").strip()
                if person_id in people_by_id and override:
                    people_by_id[person_id]["aliases"].add(override)

    index: dict[str, list[dict[str, str]]] = {}
    for person in people_by_id.values():
        aliases = {person["full_name"], *person["aliases"]}
        for alias in aliases:
            normalized = _normalize_person_name(alias)
            if not normalized:
                continue
            bucket = index.setdefault(normalized, [])
            candidate = {
                "person_id": str(person["person_id"]),
                "full_name": str(person["full_name"]),
            }
            if all(existing["person_id"] != candidate["person_id"] for existing in bucket):
                bucket.append(candidate)

    return index


def _match_people_names(index: dict[str, list[dict[str, str]]], names: list[str]) -> dict[str, Any]:
    resolved: list[dict[str, str]] = []
    unmatched: list[str] = []
    ambiguous: list[str] = []
    seen_person_ids: set[str] = set()

    for name in _dedupe_strings(names):
        normalized = _normalize_person_name(name)
        if not normalized:
            continue
        matches = index.get(normalized, [])
        if len(matches) == 1:
            candidate = matches[0]
            if candidate["person_id"] not in seen_person_ids:
                seen_person_ids.add(candidate["person_id"])
                resolved.append(
                    {
                        "person_id": candidate["person_id"],
                        "full_name": candidate["full_name"],
                        "matched_name": name,
                    }
                )
        elif len(matches) > 1:
            ambiguous.append(name)
        else:
            unmatched.append(name)

    return {
        "resolved": resolved,
        "unmatched": _dedupe_strings(unmatched),
        "ambiguous": _dedupe_strings(ambiguous),
    }


def _existing_asset_by_nbcumv_id(db: SupabaseAdminClient, lbx_id: str) -> dict[str, Any] | None:
    response = (
        db.schema("core")
        .table("media_assets")
        .select("*")
        .eq("source", _NBCUMV_SOURCE)
        .eq("source_asset_id", str(lbx_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to query existing NBCUMV assets: {response.error}")
    data = response.data or []
    return data[0] if data else None


def _existing_person_links(db: SupabaseAdminClient, asset_id: str) -> list[str]:
    response = (
        db.schema("core")
        .table("media_links")
        .select("entity_id")
        .eq("entity_type", "person")
        .eq("kind", "gallery")
        .eq("media_asset_id", asset_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to query gallery links: {response.error}")
    return [str(row.get("entity_id")) for row in (response.data or []) if row.get("entity_id")]


def _existing_show_links(db: SupabaseAdminClient, asset_id: str) -> list[str]:
    response = (
        db.schema("core")
        .table("media_links")
        .select("entity_id")
        .eq("entity_type", "show")
        .eq("kind", "gallery")
        .eq("media_asset_id", asset_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to query show gallery links: {response.error}")
    return [str(row.get("entity_id")) for row in (response.data or []) if row.get("entity_id")]


def _extract_tagged_people(image: dict[str, Any], getty_asset: dict[str, Any] | None) -> list[str]:
    return _dedupe_strings(
        [
            *_extract_getty_people(getty_asset),
            *_extract_caption_people(image.get("lbx_caption")),
            *_extract_keyword_people(image.get("lbx_keywords")),
        ]
    )


def _hydrate_people(db: SupabaseAdminClient, person_ids: list[str]) -> list[dict[str, str]]:
    if not person_ids:
        return []
    response = db.schema("core").table("people").select("id, full_name").in_("id", person_ids).limit(500).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to hydrate people: {response.error}")
    rows = response.data or []
    return [
        {
            "person_id": str(row.get("id")),
            "full_name": str(row.get("full_name") or ""),
        }
        for row in rows
        if row.get("id")
    ]


def _merge_dict(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _first_getty_url(getty_asset: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = str(getty_asset.get(key) or "").strip()
        if value:
            return value
    return None


def _effective_nbcumv_tags(image: dict[str, Any], existing_tags: list[str] | None = None) -> list[str]:
    status = str(image.get("status") or "").strip() or None
    return nbcumv._hidden_tags_for_status(status, existing_tags)


def _parse_int_field(image: dict[str, Any], *keys: str) -> int | None:
    """Extract the first integer value from a dict by trying multiple keys."""
    for key in keys:
        raw = image.get(key)
        if isinstance(raw, int):
            return raw
        if isinstance(raw, str) and raw.strip().isdigit():
            return int(raw.strip())
    return None


def _parse_episode_number_from_caption(caption: str | None) -> int | None:
    """Extract episode number from NBC-style captions like 'Episode 20180'."""
    if not caption:
        return None
    match = re.search(r"Episode\s+(\d+)", caption, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _parse_season_from_tags(tags: list[str]) -> int | None:
    """Extract season number from Getty tags like 'Season 20'."""
    for tag in tags:
        match = re.search(r"\bSeason\s+(\d+)\b", str(tag), re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None


def _build_asset_metadata(
    *,
    image: dict[str, Any],
    getty_asset: dict[str, Any] | None,
    embedded_metadata: dict[str, Any] | None,
    tagged_people: list[str],
    resolved_people: list[dict[str, str]],
    unmatched_people: list[str],
    existing_metadata: dict[str, Any] | None = None,
    gallery_bucket: dict[str, Any] | None = None,
) -> dict[str, Any]:
    published_at = (
        (getty_asset or {}).get("date_created")
        or image.get("created")
        or image.get("liveDate")
        or image.get("lbx_liveDate")
    )
    getty = getty_asset or {}
    getty_details = dict(getty.get("details") or {}) if isinstance(getty.get("details"), dict) else {}
    getty_tags = list(getty.get("keyword_texts") or []) if isinstance(getty.get("keyword_texts"), list) else []
    existing_tags = (
        list(existing_metadata.get("tags") or [])
        if isinstance(existing_metadata, dict) and isinstance(existing_metadata.get("tags"), list)
        else []
    )
    effective_tags = _effective_nbcumv_tags(image, existing_tags)
    nbcumv_enriched = dict(image)
    # Derive `company` from lbx_copyright/lbx_credit for frontend extractNbcumvFields()
    if "company" not in nbcumv_enriched:
        company = str(nbcumv_enriched.get("lbx_copyright") or "").strip()
        if not company:
            credit = str(nbcumv_enriched.get("lbx_credit") or "").strip()
            if "/" in credit:
                company = credit.split("/", 1)[-1].strip()
        if company:
            nbcumv_enriched["company"] = company
    # Flatten showIds array to show_id string for frontend extractNbcumvFields()
    if "show_id" not in nbcumv_enriched:
        show_ids = nbcumv_enriched.get("showIds")
        if isinstance(show_ids, list) and show_ids:
            nbcumv_enriched["show_id"] = str(show_ids[0]).strip()
    # Map lbx_type to content_type for frontend extractNbcumvFields()
    if "content_type" not in nbcumv_enriched:
        lbx_type = str(nbcumv_enriched.get("lbx_type") or "").strip()
        if lbx_type:
            nbcumv_enriched["content_type"] = lbx_type
    payload = {
        "object_name": image.get("lbx_filename"),
        "resolved_people": resolved_people,
        "unmatched_people": unmatched_people,
        "tagged_people": tagged_people,
        "published_at": published_at,
        "source_page_url": getty.get("detail_url"),
        "status": str(image.get("status") or "").strip() or None,
        "is_hidden": bool(image.get("is_hidden")),
        "tags": effective_tags,
        "nbcumv": nbcumv_enriched,
        "getty": getty,
        "getty_details": getty_details,
        "getty_tags": getty_tags,
        "getty_event_title": str(getty.get("event_name") or "").strip() or None,
        "getty_event_url": str(getty.get("event_url") or "").strip() or None,
        "getty_event_id": str(getty.get("event_id") or "").strip() or None,
        "getty_event_slug": str(getty.get("event_url_slug") or "").strip() or None,
        "getty_event_date": str(getty.get("event_date") or "").strip() or None,
        "embedded_file": embedded_metadata or {},
    }
    getty_original_image_url = _first_getty_url(
        getty,
        "original_image_url",
        "galleryHighResCompUrl",
        "highResCompUrl",
        "largeMainImageURL",
        "defaultMainImageURL",
        "zoomedImageUrl",
        "galleryComp1024Url",
        "downloadableCompUrl",
        "preview_image_url",
        "compUrl",
        "mainImageUrl",
        "thumbUrl",
    )
    getty_preview_image_url = _first_getty_url(
        getty,
        "preview_image_url",
        "galleryComp1024Url",
        "compUrl",
        "mainImageUrl",
        "thumbUrl",
        "defaultMainImageURL",
        "zoomedImageUrl",
    )
    if getty_original_image_url:
        payload["getty_original_image_url"] = getty_original_image_url
    if getty_preview_image_url:
        payload["getty_preview_image_url"] = getty_preview_image_url
    if isinstance(getty.get("detail_url"), str) and str(getty.get("detail_url") or "").strip():
        payload["getty_detail_page_url"] = str(getty.get("detail_url") or "").strip()

    # --- Top-level keys the frontend reads directly ---

    # Show name/id (frontend reads metadata.show_name, not resolved_show_name)
    if isinstance(gallery_bucket, dict):
        if gallery_bucket.get("resolved_show_name"):
            payload["show_name"] = gallery_bucket["resolved_show_name"]
        if gallery_bucket.get("resolved_show_id"):
            payload["show_id"] = gallery_bucket["resolved_show_id"]

    # Season number from NBCUMV fields or Getty tags
    season_number = _parse_int_field(image, "lbx_seasonNumber", "lbx_season")
    if season_number is None and getty_tags:
        season_number = _parse_season_from_tags(getty_tags)
    if season_number is not None:
        payload["season_number"] = season_number

    # Episode number from NBCUMV or caption
    episode_number = _parse_int_field(image, "lbx_episodeNumber")
    if episode_number is None:
        episode_number = _parse_episode_number_from_caption(image.get("lbx_caption"))
    if episode_number is not None:
        payload["episode_number"] = episode_number

    # Episode title from NBCUMV
    episode_title = str(image.get("lbx_episodeTitle") or "").strip() or None
    if episode_title:
        payload["episode_title"] = episode_title

    # Created date (frontend reads created_at, not published_at)
    if published_at:
        payload["created_at"] = published_at

    # People count from Getty or tagged people
    people_count: int | None = None
    if isinstance(getty, dict) and getty:
        getty_pc = getty.get("people_count")
        if isinstance(getty_pc, int) and getty_pc >= 0:
            people_count = getty_pc
        elif isinstance(getty_pc, str) and getty_pc.strip().isdigit():
            people_count = int(getty_pc.strip())
        elif getty_tags:
            from trr_backend.integrations.getty import _infer_people_count

            people_count = _infer_people_count(getty_tags)
    if people_count is None and tagged_people:
        people_count = len(tagged_people)
    if people_count is not None:
        payload["people_count"] = people_count

    # People names (frontend reads people_names, not tagged_people)
    if tagged_people:
        payload["people_names"] = list(tagged_people)

    # Content type at top level (frontend reads metadata.content_type)
    nbcumv_ct = nbcumv_enriched.get("content_type") or nbcumv_enriched.get("lbx_type")
    if isinstance(nbcumv_ct, str) and nbcumv_ct.strip():
        payload["content_type"] = nbcumv_ct.strip()

    if isinstance(gallery_bucket, dict):
        payload["gallery_bucket"] = dict(gallery_bucket)
        payload.update(dict(gallery_bucket))
    return _merge_dict(existing_metadata or {}, payload)


def _build_person_link_context(
    *,
    image: dict[str, Any],
    getty_asset: dict[str, Any] | None,
    tagged_people: list[str],
    resolved_people: list[dict[str, str]],
    unmatched_people: list[str],
    gallery_bucket: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "source": _NBCUMV_SOURCE,
        "source_asset_id": str(image.get("lbx_id") or ""),
        "source_url": image.get("location"),
        "source_page_url": (getty_asset or {}).get("detail_url"),
        "object_name": image.get("lbx_filename"),
        "tagged_people": tagged_people,
        "resolved_people": resolved_people,
        "unmatched_people": unmatched_people,
        "show_ids": image.get("showIds") or [],
        "live_date": image.get("liveDate"),
        "created": image.get("created"),
    }
    if isinstance(gallery_bucket, dict):
        payload["gallery_bucket"] = dict(gallery_bucket)
        payload.update(dict(gallery_bucket))
    return payload


def _upsert_nbcumv_asset(
    db: SupabaseAdminClient,
    *,
    image: dict[str, Any],
    image_bytes: bytes | None,
    content_type: str | None,
    hosted_bucket: str | None,
    hosted_key: str | None,
    hosted_url: str | None,
    hosted_etag: str | None,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    asset_id = asset_id_for(
        _NBCUMV_SOURCE,
        source_asset_id=str(image.get("lbx_id") or ""),
        source_url=image.get("location"),
    )
    if asset_id is None:
        raise HTTPException(status_code=400, detail="NBCUMV image was missing lbx_id and location")
    existing = _existing_asset_by_nbcumv_id(db, str(image.get("lbx_id") or ""))

    dimensions: Any = (
        (metadata.get("embedded_file") or {}).get("dimensions")
        if isinstance(metadata.get("embedded_file"), dict)
        else {}
    )
    width: Any = dimensions.get("width") or image.get("lbx_width")
    height: Any = dimensions.get("height") or image.get("lbx_height")
    existing_sha256 = existing.get("sha256") if existing else None
    existing_content_type = existing.get("content_type") if existing else None
    existing_bytes = existing.get("bytes") if existing else None
    existing_hosted_sha256 = existing.get("hosted_sha256") if existing else None
    existing_hosted_content_type = existing.get("hosted_content_type") if existing else None
    existing_hosted_bytes = existing.get("hosted_bytes") if existing else None
    payload = {
        "media_type": "image",
        "source": _NBCUMV_SOURCE,
        "source_asset_id": str(image.get("lbx_id") or ""),
        "source_url": image.get("location"),
        "sha256": hashlib.sha256(image_bytes).hexdigest() if image_bytes is not None else existing_sha256,
        "content_type": content_type or existing_content_type,
        "bytes": len(image_bytes) if image_bytes is not None else existing_bytes,
        "width": int(width) if str(width or "").isdigit() else width,
        "height": int(height) if str(height or "").isdigit() else height,
        "caption": image.get("lbx_caption"),
        "hosted_bucket": hosted_bucket,
        "hosted_key": hosted_key,
        "hosted_url": hosted_url,
        "hosted_etag": hosted_etag,
        "hosted_at": now if hosted_url else existing.get("hosted_at") if existing else None,
        "hosted_sha256": hashlib.sha256(image_bytes).hexdigest() if image_bytes is not None else existing_hosted_sha256,
        "hosted_content_type": content_type or existing_hosted_content_type,
        "hosted_bytes": len(image_bytes) if image_bytes is not None else existing_hosted_bytes,
        "metadata": metadata,
        "fetched_at": now,
        "updated_at": now,
    }

    if existing:
        response = db.schema("core").table("media_assets").update(payload).eq("id", existing["id"]).execute()
    else:
        payload["id"] = str(asset_id)
        payload["created_at"] = now
        response = db.schema("core").table("media_assets").insert(payload).execute()

    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to upsert media asset: {response.error}")
    rows = response.data or []
    if not rows:
        raise HTTPException(status_code=502, detail="Media asset upsert returned no rows")
    return rows[0]


def _import_single_item(
    *,
    db: SupabaseAdminClient,
    item: NbcumvImportItem,
    assign_people: bool,
    people_index: dict[str, list[dict[str, str]]],
) -> dict[str, Any]:
    image = item.nbcumv_image if isinstance(item.nbcumv_image, dict) else None
    if image is None:
        image = nbcumv.fetch_image_by_identity(
            filename=item.lbx_filename,
            lbx_id=item.lbx_id,
            show_id=item.show_ids[0] if item.show_ids else None,
        )
    if image is None:
        raise HTTPException(status_code=404, detail=f"NBCUMV image not found: {item.lbx_filename}")
    if str(image.get("status") or "").strip() == "0":
        logger.info("Importing HIDDEN image (status:0): %s", image.get("lbx_filename"))

    getty_asset = dict(item.getty_asset) if isinstance(item.getty_asset, dict) else None
    if getty_asset and item.getty_detail_url and not str(getty_asset.get("detail_url") or "").strip():
        getty_asset["detail_url"] = item.getty_detail_url
    if getty_asset is None:
        try:
            getty_asset = (
                getty.fetch_asset_detail(item.getty_detail_url)
                if item.getty_detail_url
                else getty.resolve_asset_by_object_name(item.lbx_filename)
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Getty enrichment failed for %s: %s", item.lbx_filename, exc)
            getty_asset = None
    tagged_people = _extract_tagged_people(image, getty_asset)

    if assign_people:
        if item.person_ids:
            resolved_people = _hydrate_people(db, [str(person_id) for person_id in item.person_ids])
            unmatched_people: list[str] = []
        else:
            matches = _match_people_names(people_index, tagged_people)
            resolved_people = matches["resolved"]
            unmatched_people = _dedupe_strings([*matches["unmatched"], *matches["ambiguous"]])
    else:
        resolved_people = []
        unmatched_people = tagged_people

    existing_asset = _existing_asset_by_nbcumv_id(db, str(image.get("lbx_id") or ""))
    existing_metadata = dict(existing_asset.get("metadata") or {}) if existing_asset else {}
    needs_download = (
        existing_asset is None or not existing_asset.get("hosted_url") or "embedded_file" not in existing_metadata
    )

    image_bytes: bytes | None = None
    content_type: str | None = None
    embedded_metadata: dict[str, Any] | None = existing_metadata.get("embedded_file") if existing_metadata else None
    hosted_bucket = str(existing_asset.get("hosted_bucket") or "") if existing_asset else None
    hosted_key = str(existing_asset.get("hosted_key") or "") if existing_asset else None
    hosted_url = str(existing_asset.get("hosted_url") or "") if existing_asset else None
    hosted_etag = str(existing_asset.get("hosted_etag") or "") if existing_asset else None

    if needs_download:
        image_bytes, content_type = nbcumv.download_hires_image(
            lbx_id=str(image.get("lbx_id")),
            filename=str(image.get("lbx_filename")),
        )
        embedded_metadata = nbcumv.extract_embedded_metadata(image_bytes)
        content_type = content_type or "image/jpeg"
        ext = guess_ext_from_content_type(content_type or "image/jpeg")
        sha256 = hashlib.sha256(image_bytes).hexdigest()
        hosted_key = build_shared_media_s3_key(sha256, ext)
        hosted_bucket = get_s3_bucket()
        hosted_etag, _ = upload_bytes_to_s3(
            get_s3_client(),
            bucket=hosted_bucket,
            key=hosted_key,
            data=image_bytes,
            content_type=content_type,
        )
        hosted_url = build_hosted_url(hosted_key)

    metadata = _build_asset_metadata(
        image=image,
        getty_asset=getty_asset,
        embedded_metadata=embedded_metadata,
        tagged_people=tagged_people,
        resolved_people=resolved_people,
        unmatched_people=unmatched_people,
        existing_metadata=existing_metadata,
        gallery_bucket=item.gallery_bucket,
    )
    asset = _upsert_nbcumv_asset(
        db,
        image=image,
        image_bytes=image_bytes,
        content_type=content_type,
        hosted_bucket=hosted_bucket,
        hosted_key=hosted_key,
        hosted_url=hosted_url,
        hosted_etag=hosted_etag,
        metadata=metadata,
    )

    existing_person_ids = set(_existing_person_links(db, str(asset["id"])))
    created_person_ids: list[str] = []
    if assign_people:
        context = _build_person_link_context(
            image=image,
            getty_asset=getty_asset,
            tagged_people=tagged_people,
            resolved_people=resolved_people,
            unmatched_people=unmatched_people,
            gallery_bucket=item.gallery_bucket,
        )
        for person in resolved_people:
            person_id = str(person["person_id"])
            if person_id not in existing_person_ids:
                created_person_ids.append(person_id)
            create_media_link_for_entity(
                db,
                entity_type="person",
                entity_id=person_id,
                media_asset_id=str(asset["id"]),
                kind="gallery",
                position=0,
                context=context,
            )

    requested_show_ids = [str(show_id) for show_id in (item.link_show_ids or []) if str(show_id).strip()]
    existing_show_ids = set(_existing_show_links(db, str(asset["id"]))) if requested_show_ids else set()
    created_show_ids: list[str] = []
    for show_id in requested_show_ids:
        if show_id not in existing_show_ids:
            created_show_ids.append(show_id)
        create_media_link_for_entity(
            db,
            entity_type="show",
            entity_id=show_id,
            media_asset_id=str(asset["id"]),
            kind="gallery",
            position=0,
            context=_build_person_link_context(
                image=image,
                getty_asset=getty_asset,
                tagged_people=tagged_people,
                resolved_people=resolved_people,
                unmatched_people=unmatched_people,
                gallery_bucket=item.gallery_bucket,
            ),
        )

    try:
        generate_media_asset_variants(db, asset_id=str(asset["id"]), force=False)
    except Exception as exc:  # noqa: BLE001
        logger.warning("NBCUMV variant generation failed for asset %s: %s", asset.get("id"), exc)

    final_person_ids = _existing_person_links(db, str(asset["id"]))
    final_show_ids = _existing_show_links(db, str(asset["id"])) if requested_show_ids else []
    return {
        "lbx_id": str(image.get("lbx_id") or ""),
        "lbx_filename": image.get("lbx_filename"),
        "asset_id": asset.get("id"),
        "hosted_url": asset.get("hosted_url"),
        "created_person_ids": created_person_ids,
        "created_show_ids": created_show_ids,
        "person_ids": final_person_ids,
        "show_ids": final_show_ids,
        "unmatched_people": unmatched_people,
        "already_imported": bool(existing_asset and existing_asset.get("hosted_url")),
        "metadata_upgraded": bool(existing_asset and getty_asset),
    }


@router.post("/preview")
def preview_nbcumv_import(
    request: NbcumvPreviewRequest,
    db: SupabaseAdminClient,
    _: AdminUser = cast(AdminUser, None),
) -> dict[str, Any]:
    _ensure_sources(db)
    filters = nbcumv.SearchFilters(
        filename=request.filename,
        lbx_id=request.lbx_id,
        show_id=request.show_id,
        search_text=request.search_text,
        nup_prefix=request.nup_prefix,
        show_name=request.show_name,
        meta_type=request.meta_type,
        season=request.season,
        episode=request.episode,
        network=request.network,
        created_start=request.created_start,
        created_end=request.created_end,
        live_date_start=request.live_date_start,
        live_date_end=request.live_date_end,
        search_caption=request.search_caption,
        limit=request.limit,
    )
    images = nbcumv.search_images(filters)
    people_index = _load_eligible_people_index(db)

    preview_items: list[dict[str, Any]] = []
    for image in images:
        getty_asset = None
        getty_error = None
        try:
            getty_asset = getty.resolve_asset_by_object_name(str(image.get("lbx_filename") or ""))
        except Exception as exc:  # noqa: BLE001
            getty_error = str(exc)

        tagged_people = _extract_tagged_people(image, getty_asset)
        matches = _match_people_names(people_index, tagged_people)
        existing_asset = _existing_asset_by_nbcumv_id(db, str(image.get("lbx_id") or ""))
        existing_person_ids = _existing_person_links(db, str(existing_asset["id"])) if existing_asset else []
        effective_tags = _effective_nbcumv_tags(
            image,
            list((existing_asset or {}).get("metadata", {}).get("tags") or [])
            if isinstance((existing_asset or {}).get("metadata"), dict)
            else [],
        )

        preview_items.append(
            {
                "lbx_id": str(image.get("lbx_id") or ""),
                "lbx_filename": image.get("lbx_filename"),
                "location": image.get("location"),
                "status": str(image.get("status") or "").strip() or None,
                "is_hidden": bool(image.get("is_hidden")),
                "effective_tags": effective_tags,
                "show_ids": image.get("showIds") or [],
                "nbcumv": image,
                "getty": getty_asset,
                "getty_detail_url": (getty_asset or {}).get("detail_url"),
                "getty_error": getty_error,
                "tagged_people": tagged_people,
                "resolved_people": matches["resolved"],
                "person_ids": [match["person_id"] for match in matches["resolved"]],
                "unmatched_people": _dedupe_strings([*matches["unmatched"], *matches["ambiguous"]]),
                "already_imported": bool(existing_asset),
                "existing_asset_id": existing_asset.get("id") if existing_asset else None,
                "existing_person_ids": existing_person_ids,
                "import_ready": bool(image.get("lbx_id") and image.get("lbx_filename") and image.get("location")),
            }
        )

    return {
        "items": preview_items,
        "count": len(preview_items),
    }


@router.post("/import")
def import_nbcumv_assets(
    request: NbcumvImportRequest,
    db: SupabaseAdminClient,
    _: AdminUser = cast(AdminUser, None),
) -> dict[str, Any]:
    _ensure_sources(db)
    people_index = _load_eligible_people_index(db)

    results: list[dict[str, Any]] = []
    for item in request.items:
        results.append(
            _import_single_item(
                db=db,
                item=item,
                assign_people=request.assign_people,
                people_index=people_index,
            )
        )

    imported_asset_ids = [row["asset_id"] for row in results if row.get("asset_id")]
    created_person_links = sum(len(row.get("created_person_ids") or []) for row in results)
    created_show_links = sum(len(row.get("created_show_ids") or []) for row in results)
    skipped_duplicates = sum(1 for row in results if row.get("already_imported"))
    unresolved_names = _dedupe_strings(
        [name for row in results for name in (row.get("unmatched_people") or []) if isinstance(name, str)]
    )

    return {
        "imported_asset_ids": imported_asset_ids,
        "created_gallery_links": created_person_links + created_show_links,
        "created_person_gallery_links": created_person_links,
        "created_show_gallery_links": created_show_links,
        "skipped_duplicates": skipped_duplicates,
        "unresolved_names": unresolved_names,
        "results": results,
    }
