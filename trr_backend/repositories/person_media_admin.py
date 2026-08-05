"""Backend-owned admin persistence for person cover photos and thumbnail crops."""

from __future__ import annotations

import json
from typing import Any, Literal

from trr_backend.db import pg

ThumbnailCropOrigin = Literal["cast_photos", "media_links"]


def get_cover_photo(person_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT
          person_id::text AS person_id,
          photo_id::text AS photo_id,
          photo_url,
          created_at,
          updated_at,
          created_by_firebase_uid
        FROM admin.person_cover_photos
        WHERE person_id = %s::uuid
        LIMIT 1
        """,
        [person_id],
    )
    return row, 1


def set_cover_photo(
    *,
    person_id: str,
    photo_id: str,
    photo_url: str,
    actor_uid: str,
) -> tuple[dict[str, Any], int]:
    rows = pg.execute_returning(
        """
        INSERT INTO admin.person_cover_photos (
          person_id,
          photo_id,
          photo_url,
          created_by_firebase_uid
        )
        VALUES (%s::uuid, %s, %s, %s)
        ON CONFLICT (person_id) DO UPDATE SET
          photo_id = EXCLUDED.photo_id,
          photo_url = EXCLUDED.photo_url,
          updated_at = NOW(),
          created_by_firebase_uid = EXCLUDED.created_by_firebase_uid
        RETURNING
          person_id::text AS person_id,
          photo_id::text AS photo_id,
          photo_url,
          created_at,
          updated_at,
          created_by_firebase_uid
        """,
        [person_id, photo_id, photo_url, actor_uid],
    )
    if not rows:
        raise RuntimeError("Failed to load the person cover photo after upsert")
    return rows[0], 1


def remove_cover_photo(person_id: str) -> tuple[bool, int]:
    rows = pg.execute_returning(
        """
        DELETE FROM admin.person_cover_photos
        WHERE person_id = %s::uuid
        RETURNING person_id::text AS person_id
        """,
        [person_id],
    )
    return bool(rows), 1


def _crop_result(
    *,
    origin: ThumbnailCropOrigin,
    row: dict[str, Any],
) -> dict[str, Any]:
    container_key = "metadata" if origin == "cast_photos" else "context"
    container = row.get(container_key)
    raw_crop = container.get("thumbnail_crop") if isinstance(container, dict) else None
    crop = raw_crop if isinstance(raw_crop, dict) else {}
    mode = crop.get("mode")
    return {
        "origin": origin,
        "photo_id": str(row.get("id") or ""),
        "person_id": str(row.get("person_id") or row.get("entity_id") or ""),
        "link_id": str(row.get("id") or "") if origin == "media_links" else None,
        "thumbnail_focus_x": crop.get("x") if isinstance(crop.get("x"), (int, float)) else None,
        "thumbnail_focus_y": crop.get("y") if isinstance(crop.get("y"), (int, float)) else None,
        "thumbnail_zoom": crop.get("zoom") if isinstance(crop.get("zoom"), (int, float)) else None,
        "thumbnail_crop_mode": mode if mode in {"manual", "auto"} else None,
    }


def update_thumbnail_crop(
    *,
    origin: ThumbnailCropOrigin,
    person_id: str,
    photo_id: str,
    crop: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, int]:
    crop_json = json.dumps(crop) if crop is not None else None
    if origin == "cast_photos":
        rows = pg.execute_returning(
            """
            WITH input AS (
              SELECT %s::uuid AS photo_id, %s::uuid AS person_id, %s::jsonb AS crop
            )
            UPDATE core.cast_photos AS cast_photo
            SET metadata = CASE
              WHEN input.crop IS NULL THEN COALESCE(metadata, '{}'::jsonb) - 'thumbnail_crop'
              ELSE jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{thumbnail_crop}',
                input.crop,
                true
              )
            END,
            updated_at = NOW()
            FROM input
            WHERE cast_photo.id = input.photo_id
              AND cast_photo.person_id = input.person_id
            RETURNING
              cast_photo.id::text AS id,
              cast_photo.person_id::text AS person_id,
              cast_photo.metadata
            """,
            [photo_id, person_id, crop_json],
        )
    else:
        rows = pg.execute_returning(
            """
            WITH input AS (
              SELECT %s::uuid AS link_id, %s::uuid AS person_id, %s::jsonb AS crop
            )
            UPDATE core.media_links AS media_link
            SET context = CASE
              WHEN input.crop IS NULL THEN COALESCE(context, '{}'::jsonb) - 'thumbnail_crop'
              ELSE jsonb_set(
                COALESCE(context, '{}'::jsonb),
                '{thumbnail_crop}',
                input.crop,
                true
              )
            END,
            updated_at = NOW()
            FROM input
            WHERE media_link.id = input.link_id
              AND media_link.entity_type = 'person'
              AND media_link.entity_id = input.person_id
              AND media_link.kind = 'gallery'
            RETURNING
              media_link.id::text AS id,
              media_link.entity_id::text AS entity_id,
              media_link.context
            """,
            [photo_id, person_id, crop_json],
        )
    if not rows:
        return None, 1
    return _crop_result(origin=origin, row=rows[0]), 1
