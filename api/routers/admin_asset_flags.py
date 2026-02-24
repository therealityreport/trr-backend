"""Admin endpoints for archiving/starring gallery assets across image tables.

We treat "archive" as a soft-delete:
- Mark archived_* columns so galleries can hide the asset by default.
- Best-effort delete the S3 mirror and clear hosted_* fields so it won't be re-used.

We treat "star" as a lightweight bookmark:
- Persist into the row's metadata JSON (starred/starred_at).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.s3_mirror import get_s3_bucket, get_s3_client

router = APIRouter(prefix="/admin/assets", tags=["admin-asset-flags"])

AssetOrigin = Literal["media_assets", "show_images", "season_images", "episode_images", "cast_photos"]


class ArchiveAssetRequest(BaseModel):
    origin: AssetOrigin
    asset_id: UUID
    reason: str | None = Field(default=None, description="Why this asset was archived.")


class ArchiveAssetResponse(BaseModel):
    origin: AssetOrigin
    asset_id: str
    archived_at: str
    s3_deleted: bool
    s3_error: str | None = None


class StarAssetRequest(BaseModel):
    origin: AssetOrigin
    asset_id: UUID
    starred: bool = True


class StarAssetResponse(BaseModel):
    origin: AssetOrigin
    asset_id: str
    starred: bool
    starred_at: str | None = None


class UpdateAssetContentTypeRequest(BaseModel):
    origin: AssetOrigin
    asset_id: UUID
    content_type: str = Field(..., description="Normalized content type, e.g. PROMO/CONFESSIONAL/REUNION.")


class UpdateAssetContentTypeResponse(BaseModel):
    origin: AssetOrigin
    asset_id: str
    content_type: str
    kind: str | None = None
    context_type: str | None = None
    updated_at: str


CONTENT_TYPE_ALIASES: dict[str, str] = {
    "PROMO": "PROMO",
    "PROMOS": "PROMO",
    "CONFESSIONAL": "CONFESSIONAL",
    "CONFESSIONALS": "CONFESSIONAL",
    "REUNION": "REUNION",
    "REUNIONS": "REUNION",
    "INTRO": "INTRO",
    "EPISODE STILL": "EPISODE STILL",
    "EPISODE_STILL": "EPISODE STILL",
    "EPISODE-STILL": "EPISODE STILL",
    "OTHER": "OTHER",
    "CAST PHOTO": "CAST PHOTOS",
    "CAST PHOTOS": "CAST PHOTOS",
    "PROFILE PICTURE": "PROFILE PICTURE",
    "PROFILE_PICTURE": "PROFILE PICTURE",
    "PROFILE-PICTURE": "PROFILE PICTURE",
    "PROFILE PHOTO": "PROFILE PICTURE",
    "PROFILE-PHOTO": "PROFILE PICTURE",
    "PROFILE_PHOTO": "PROFILE PICTURE",
    "PROFILE": "PROFILE PICTURE",
    "HEADSHOT": "PROFILE PICTURE",
    "BACKDROP": "BACKDROP",
    "POSTER": "POSTER",
    "LOGO": "LOGO",
}

CONTENT_TYPE_KIND_MAP: dict[str, str] = {
    "PROMO": "promo",
    "CONFESSIONAL": "confessional",
    "REUNION": "reunion",
    "INTRO": "intro",
    "EPISODE STILL": "episode_still",
    "OTHER": "other",
    "CAST PHOTOS": "cast",
    "PROFILE PICTURE": "profile_picture",
    "BACKDROP": "backdrop",
    "POSTER": "poster",
    "LOGO": "logo",
}

CONTENT_TYPE_CONTEXT_MAP: dict[str, str] = {
    "PROMO": "promo",
    "CONFESSIONAL": "confessional",
    "REUNION": "reunion",
    "INTRO": "intro",
    "EPISODE STILL": "episode still",
    "OTHER": "other",
    "CAST PHOTOS": "cast photos",
    "PROFILE PICTURE": "profile_picture",
    "BACKDROP": "backdrop",
    "POSTER": "poster",
    "LOGO": "logo",
}


def _normalize_content_type(value: str) -> str:
    normalized = " ".join(value.strip().replace("_", " ").replace("-", " ").split()).upper()
    resolved = CONTENT_TYPE_ALIASES.get(normalized)
    if not resolved:
        allowed = ", ".join(sorted(CONTENT_TYPE_KIND_MAP.keys()))
        raise HTTPException(status_code=400, detail=f"Unsupported content_type '{value}'. Allowed: {allowed}")
    return resolved


def _table_for_origin(origin: AssetOrigin) -> str:
    return origin


def _fetch_row(db: SupabaseAdminClient, *, origin: AssetOrigin, asset_id: str) -> dict:
    table = _table_for_origin(origin)
    response = (
        db.schema("core")
        .table(table)
        .select("id, hosted_bucket, hosted_key, hosted_url, metadata")
        .eq("id", asset_id)
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching asset")
    if not response.data:
        raise HTTPException(status_code=404, detail="Asset not found")
    return response.data[0]


def _fetch_content_row(db: SupabaseAdminClient, *, origin: AssetOrigin, asset_id: str) -> dict:
    table = _table_for_origin(origin)
    if origin in {"show_images", "season_images", "episode_images"}:
        select_fields = "id, metadata, kind"
    elif origin == "cast_photos":
        select_fields = "id, metadata, context_type"
    else:
        select_fields = "id, metadata"

    response = db.schema("core").table(table).select(select_fields).eq("id", asset_id).limit(1).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching asset")
    if not response.data:
        raise HTTPException(status_code=404, detail="Asset not found")
    return response.data[0]


def _clear_hosted_fields_payload(now_iso: str) -> dict:
    # These hosted_* columns exist across the core image tables and core.media_assets.
    # Setting them null ensures the archived asset won't render or be re-mirrored.
    return {
        "hosted_bucket": None,
        "hosted_key": None,
        "hosted_url": None,
        "hosted_sha256": None,
        "hosted_content_type": None,
        "hosted_bytes": None,
        "hosted_etag": None,
        "hosted_at": None,
        "updated_at": now_iso,
    }


@router.post("/archive", response_model=ArchiveAssetResponse)
def archive_asset(
    payload: ArchiveAssetRequest,
    db: SupabaseAdminClient = None,
    user: AdminUser = None,
) -> ArchiveAssetResponse:
    asset_id_str = str(payload.asset_id)
    row = _fetch_row(db, origin=payload.origin, asset_id=asset_id_str)

    hosted_bucket = row.get("hosted_bucket") or None
    hosted_key = row.get("hosted_key") or None
    s3_deleted = False
    s3_error: str | None = None

    if hosted_key:
        try:
            s3_client = get_s3_client()
            bucket = hosted_bucket or get_s3_bucket()
            s3_client.delete_object(Bucket=bucket, Key=hosted_key)
            s3_deleted = True
        except Exception as exc:  # noqa: BLE001
            # Best-effort only; we still archive even if S3 deletion fails.
            s3_error = str(exc)

    now_iso = datetime.now(UTC).isoformat()
    metadata_in = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    metadata_out = dict(metadata_in)
    metadata_out["archived_at"] = now_iso
    if payload.reason:
        metadata_out["archived_reason"] = payload.reason

    update_payload = {
        "archived_at": now_iso,
        "archived_by_firebase_uid": (user or {}).get("id"),
        "archived_reason": payload.reason,
        "metadata": metadata_out,
        **_clear_hosted_fields_payload(now_iso),
    }

    table = _table_for_origin(payload.origin)
    response = db.schema("core").table(table).update(update_payload).eq("id", asset_id_str).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error archiving asset")

    return ArchiveAssetResponse(
        origin=payload.origin,
        asset_id=asset_id_str,
        archived_at=now_iso,
        s3_deleted=s3_deleted,
        s3_error=s3_error,
    )


@router.post("/star", response_model=StarAssetResponse)
def star_asset(
    payload: StarAssetRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StarAssetResponse:
    asset_id_str = str(payload.asset_id)
    row = _fetch_row(db, origin=payload.origin, asset_id=asset_id_str)

    now_iso = datetime.now(UTC).isoformat()
    metadata_in = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    metadata_out = dict(metadata_in)
    metadata_out["starred"] = bool(payload.starred)
    if payload.starred:
        metadata_out["starred_at"] = now_iso
    else:
        metadata_out.pop("starred_at", None)

    table = _table_for_origin(payload.origin)
    response = (
        db.schema("core")
        .table(table)
        .update({"metadata": metadata_out, "updated_at": now_iso})
        .eq("id", asset_id_str)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error starring asset")

    return StarAssetResponse(
        origin=payload.origin,
        asset_id=asset_id_str,
        starred=bool(payload.starred),
        starred_at=now_iso if payload.starred else None,
    )


@router.post("/content-type", response_model=UpdateAssetContentTypeResponse)
def update_asset_content_type(
    payload: UpdateAssetContentTypeRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> UpdateAssetContentTypeResponse:
    asset_id_str = str(payload.asset_id)
    content_type = _normalize_content_type(payload.content_type)
    now_iso = datetime.now(UTC).isoformat()
    row = _fetch_content_row(db, origin=payload.origin, asset_id=asset_id_str)

    metadata_in = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    metadata_out = dict(metadata_in)
    metadata_out["fandom_section_tag"] = content_type
    metadata_out["content_type"] = content_type

    update_payload: dict[str, object] = {
        "metadata": metadata_out,
        "updated_at": now_iso,
    }

    kind_value = CONTENT_TYPE_KIND_MAP.get(content_type)
    context_type_value = CONTENT_TYPE_CONTEXT_MAP.get(content_type)

    if payload.origin in {"show_images", "season_images", "episode_images"} and kind_value:
        update_payload["kind"] = kind_value
    if payload.origin == "cast_photos" and context_type_value:
        update_payload["context_type"] = context_type_value

    table = _table_for_origin(payload.origin)
    response = db.schema("core").table(table).update(update_payload).eq("id", asset_id_str).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error updating content type")

    # Keep media-link gallery contexts aligned for media_assets so UI filters update immediately.
    if payload.origin == "media_assets" and context_type_value:
        links_response = (
            db.schema("core").table("media_links").select("id, context").eq("media_asset_id", asset_id_str).execute()
        )
        if not (hasattr(links_response, "error") and links_response.error):
            for link in links_response.data or []:
                context_in = link.get("context") if isinstance(link.get("context"), dict) else {}
                context_out = dict(context_in)
                context_out["fandom_section_tag"] = content_type
                context_out["context_type"] = context_type_value
                try:
                    db.schema("core").table("media_links").update({"context": context_out, "updated_at": now_iso}).eq(
                        "id", link["id"]
                    ).execute()
                except Exception:
                    continue

    return UpdateAssetContentTypeResponse(
        origin=payload.origin,
        asset_id=asset_id_str,
        content_type=content_type,
        kind=kind_value if payload.origin in {"show_images", "season_images", "episode_images"} else None,
        context_type=context_type_value if payload.origin in {"cast_photos", "media_assets"} else None,
        updated_at=now_iso,
    )
