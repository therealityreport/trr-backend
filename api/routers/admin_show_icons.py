"""Admin endpoints for show icon upload/list/delete."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from api.auth import InternalAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.db import pg
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    build_icon_s3_key,
    get_s3_bucket,
    get_s3_client,
    upload_bytes_to_s3,
)

router = APIRouter(prefix="/admin", tags=["admin-show-icons"])

MAX_ICON_BYTES = 6 * 1024 * 1024
ALLOWED_IMAGE_TYPES = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
    "image/svg+xml",
}


class ShowIconRecord(BaseModel):
    id: str
    show_key: str
    filename: str
    s3_key: str
    hosted_url: str
    content_type: str
    size_bytes: int
    created_by: str | None = None
    created_at: datetime


class ListShowIconsResponse(BaseModel):
    icons: list[ShowIconRecord]


class DeleteShowIconResponse(BaseModel):
    id: str
    show_key: str
    deleted: bool
    s3_deleted: bool


def _validate_show_key(show_key: str) -> str:
    normalized = (show_key or "").strip().lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="show_key is required")
    return normalized


def _validate_content_type(content_type: str | None) -> str:
    normalized = (content_type or "").split(";", 1)[0].strip().lower()
    if normalized not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported content type: {content_type or 'unknown'}",
        )
    return normalized


def _insert_icon_record(_: SupabaseAdminClient, payload: dict[str, Any]) -> dict[str, Any]:
    rows = pg.execute_returning(
        """
        insert into public.show_icons (
          show_key,
          filename,
          s3_key,
          hosted_url,
          content_type,
          size_bytes,
          created_by,
          created_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s)
        returning
          id::text as id,
          show_key,
          filename,
          s3_key,
          hosted_url,
          content_type,
          size_bytes,
          created_by,
          created_at
        """,
        (
            payload["show_key"],
            payload["filename"],
            payload["s3_key"],
            payload["hosted_url"],
            payload["content_type"],
            payload["size_bytes"],
            payload.get("created_by"),
            payload["created_at"],
        ),
    )
    if not rows:
        raise HTTPException(status_code=502, detail="Database insert returned no rows")
    return rows[0]


def _list_icon_records(_: SupabaseAdminClient, show_key: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          id::text as id,
          show_key,
          filename,
          s3_key,
          hosted_url,
          content_type,
          size_bytes,
          created_by,
          created_at
        from public.show_icons
        where show_key = %s
        order by created_at desc
        """,
        (show_key,),
    )


def _get_icon_record(_: SupabaseAdminClient, show_key: str, icon_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        select
          id::text as id,
          show_key,
          filename,
          s3_key,
          hosted_url,
          content_type,
          size_bytes,
          created_by,
          created_at
        from public.show_icons
        where show_key = %s
          and id = %s::uuid
        limit 1
        """,
        (show_key, icon_id),
    )


def _delete_icon_record(_: SupabaseAdminClient, icon_id: str) -> None:
    pg.execute_returning(
        """
        delete from public.show_icons
        where id = %s::uuid
        returning id::text as id
        """,
        (icon_id,),
    )


@router.post("/shows/{show_key}/icons", response_model=ShowIconRecord)
async def upload_show_icon(
    show_key: str,
    file: UploadFile = File(...),
    db: SupabaseAdminClient = cast(SupabaseAdminClient, None),
    admin: InternalAdminUser = cast(InternalAdminUser, None),
) -> ShowIconRecord:
    normalized_show_key = _validate_show_key(show_key)
    content_type = _validate_content_type(file.content_type)
    filename = (file.filename or "icon").strip() or "icon"

    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Uploaded icon is empty")
    if len(payload) > MAX_ICON_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"Uploaded icon exceeds max size ({MAX_ICON_BYTES} bytes)",
        )

    key = build_icon_s3_key(normalized_show_key, filename)
    bucket = get_s3_bucket()
    _, size_bytes = upload_bytes_to_s3(
        get_s3_client(),
        bucket=bucket,
        key=key,
        data=payload,
        content_type=content_type,
    )
    hosted_url = build_hosted_url(key)
    created_by = admin.get("email") if isinstance(admin, dict) else None

    row = _insert_icon_record(
        db,
        {
            "show_key": normalized_show_key,
            "filename": filename,
            "s3_key": key,
            "hosted_url": hosted_url,
            "content_type": content_type,
            "size_bytes": size_bytes,
            "created_by": created_by,
            "created_at": datetime.now(UTC).isoformat(),
        },
    )
    return ShowIconRecord(**row)


@router.get("/shows/{show_key}/icons", response_model=ListShowIconsResponse)
def list_show_icons(
    show_key: str,
    db: SupabaseAdminClient = cast(SupabaseAdminClient, None),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> ListShowIconsResponse:
    normalized_show_key = _validate_show_key(show_key)
    rows = _list_icon_records(db, normalized_show_key)
    return ListShowIconsResponse(icons=[ShowIconRecord(**row) for row in rows])


@router.delete("/shows/{show_key}/icons/{icon_id}", response_model=DeleteShowIconResponse)
def delete_show_icon(
    show_key: str,
    icon_id: UUID,
    db: SupabaseAdminClient = cast(SupabaseAdminClient, None),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> DeleteShowIconResponse:
    normalized_show_key = _validate_show_key(show_key)
    icon_id_str = str(icon_id)
    row = _get_icon_record(db, normalized_show_key, icon_id_str)
    if not row:
        raise HTTPException(status_code=404, detail="Icon not found")

    s3_deleted = False
    s3_key = str(row.get("s3_key") or "").strip()
    if s3_key:
        get_s3_client().delete_object(Bucket=get_s3_bucket(), Key=s3_key)
        s3_deleted = True

    _delete_icon_record(db, icon_id_str)
    return DeleteShowIconResponse(
        id=icon_id_str,
        show_key=normalized_show_key,
        deleted=True,
        s3_deleted=s3_deleted,
    )
