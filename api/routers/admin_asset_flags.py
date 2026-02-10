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
