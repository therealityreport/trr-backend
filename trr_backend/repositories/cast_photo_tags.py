"""Repository functions for admin.cast_photo_people_tags."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import Json

from trr_backend.db import pg

logger = logging.getLogger(__name__)

TAG_FIELDS = (
    "cast_photo_id",
    "people_names",
    "people_ids",
    "people_count",
    "people_count_source",
    "detector",
    "created_at",
    "updated_at",
    "created_by_firebase_uid",
    "updated_by_firebase_uid",
)

MISSING_ADMIN_SCHEMA_OR_TABLE_CODES = {"3F000", "42P01"}


def is_missing_admin_schema_or_table(error: BaseException) -> bool:
    code = getattr(error, "pgcode", None)
    if not code:
        diag = getattr(error, "diag", None)
        code = getattr(diag, "sqlstate", None)
    return code in MISSING_ADMIN_SCHEMA_OR_TABLE_CODES


def _json_ready_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in TAG_FIELDS:
        value = row.get(field)
        if field == "cast_photo_id" and value is not None:
            out[field] = str(value)
        elif isinstance(value, datetime):
            out[field] = value.isoformat()
        else:
            out[field] = value
    return out


def list_tag_rows_by_photo_ids(photo_ids: list[str]) -> list[dict[str, Any]]:
    if not photo_ids:
        return []
    try:
        rows = pg.fetch_all(
            """
            SELECT
              cast_photo_id,
              people_names,
              people_ids,
              people_count,
              people_count_source,
              detector,
              created_at,
              updated_at,
              created_by_firebase_uid,
              updated_by_firebase_uid
            FROM admin.cast_photo_people_tags
            WHERE cast_photo_id = ANY(%s::uuid[])
            """,
            [photo_ids],
        )
    except Exception as exc:
        if is_missing_admin_schema_or_table(exc):
            logger.warning("Admin cast_photo_people_tags table unavailable: %s", exc)
            return []
        raise

    return [_json_ready_row(row) for row in rows]


def list_photo_ids_by_person_id(person_id: str) -> list[str]:
    if not person_id:
        return []
    try:
        rows = pg.fetch_all(
            """
            SELECT cast_photo_id
            FROM admin.cast_photo_people_tags
            WHERE people_ids @> ARRAY[%s]::text[]
            """,
            [person_id],
        )
    except Exception as exc:
        if is_missing_admin_schema_or_table(exc):
            logger.warning("Admin cast_photo_people_tags table unavailable: %s", exc)
            return []
        raise

    return [str(row["cast_photo_id"]) for row in rows if row.get("cast_photo_id")]


def upsert_cast_photo_tag_row(
    *,
    cast_photo_id: str,
    people_names: list[str] | None,
    people_ids: list[str] | None,
    people_count: int | None,
    people_count_source: str | None,
    detector: str | None = None,
    updated_by_firebase_uid: str | None = None,
    created_by_firebase_uid: str | None = None,
) -> dict[str, Any] | None:
    try:
        rows = pg.execute_returning(
            """
            INSERT INTO admin.cast_photo_people_tags (
              cast_photo_id,
              people_names,
              people_ids,
              people_count,
              people_count_source,
              detector,
              created_by_firebase_uid,
              updated_by_firebase_uid,
              updated_at
            ) VALUES (
              %s::uuid,
              %s::text[],
              %s::text[],
              %s::int,
              %s::text,
              %s::text,
              %s::text,
              %s::text,
              NOW()
            )
            ON CONFLICT (cast_photo_id) DO UPDATE SET
              people_names = EXCLUDED.people_names,
              people_ids = EXCLUDED.people_ids,
              people_count = EXCLUDED.people_count,
              people_count_source = EXCLUDED.people_count_source,
              detector = EXCLUDED.detector,
              updated_by_firebase_uid = EXCLUDED.updated_by_firebase_uid,
              updated_at = EXCLUDED.updated_at,
              created_by_firebase_uid = COALESCE(
                admin.cast_photo_people_tags.created_by_firebase_uid,
                EXCLUDED.created_by_firebase_uid
              )
            RETURNING
              cast_photo_id,
              people_names,
              people_ids,
              people_count,
              people_count_source,
              detector,
              created_at,
              updated_at,
              created_by_firebase_uid,
              updated_by_firebase_uid
            """,
            [
                cast_photo_id,
                people_names,
                people_ids,
                people_count,
                people_count_source,
                detector,
                created_by_firebase_uid,
                updated_by_firebase_uid,
            ],
        )
    except Exception as exc:
        if is_missing_admin_schema_or_table(exc):
            logger.warning("Admin cast_photo_people_tags table unavailable: %s", exc)
            return None
        raise

    return _json_ready_row(rows[0]) if rows else None


def set_cast_photo_face_boxes(cast_photo_id: str, face_boxes: list[Any] | None) -> bool:
    payload = Json(face_boxes) if face_boxes is not None else None
    pg.execute(
        """
        WITH payload AS (
          SELECT %s::uuid AS cast_photo_id, %s::jsonb AS face_boxes
        )
        UPDATE core.cast_photos AS cp
        SET metadata = CASE
            WHEN payload.face_boxes IS NULL
              THEN (COALESCE(cp.metadata, '{}'::jsonb) - 'face_boxes')
            ELSE jsonb_set(
              COALESCE(cp.metadata, '{}'::jsonb),
              '{face_boxes}',
              payload.face_boxes,
              true
            )
          END,
          updated_at = NOW()
        FROM payload
        WHERE cp.id = payload.cast_photo_id
        """,
        [cast_photo_id, payload],
    )
    return True


def get_tags_by_photo_ids(db: Any, photo_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not photo_ids:
        return {}
    try:
        response = (
            db.schema("admin")
            .table("cast_photo_people_tags")
            .select("cast_photo_id, people_names, people_ids, people_count, people_count_source, detector")
            .in_("cast_photo_id", photo_ids)
            .execute()
        )
    except Exception as exc:
        logger.warning("Failed to query cast_photo_people_tags: %s", exc)
        return {}

    if hasattr(response, "error") and response.error:
        logger.warning("Failed to query cast_photo_people_tags: %s", response.error)
        return {}

    rows = response.data or []
    return {row["cast_photo_id"]: row for row in rows}


def has_manual_tags(tag_row: dict[str, Any] | None) -> bool:
    if not tag_row:
        return False
    return (tag_row.get("people_count_source") or "").lower() == "manual"


def upsert_cast_photo_tags(
    db: Any,
    *,
    cast_photo_id: str,
    people_names: list[str] | None,
    people_ids: list[str] | None,
    people_count: int | None,
    people_count_source: str | None,
    detector: str | None = None,
    updated_by_firebase_uid: str | None = None,
    created_by_firebase_uid: str | None = None,
) -> dict[str, Any] | None:
    now = datetime.now(UTC).isoformat()
    row = {
        "cast_photo_id": cast_photo_id,
        "people_names": people_names,
        "people_ids": people_ids,
        "people_count": people_count,
        "people_count_source": people_count_source,
        "detector": detector,
        "updated_at": now,
        "updated_by_firebase_uid": updated_by_firebase_uid,
    }
    if created_by_firebase_uid:
        row["created_by_firebase_uid"] = created_by_firebase_uid

    try:
        response = db.schema("admin").table("cast_photo_people_tags").upsert(row, on_conflict="cast_photo_id").execute()
    except Exception as exc:
        logger.warning("Failed to upsert cast_photo_people_tags: %s", exc)
        return None

    if hasattr(response, "error") and response.error:
        logger.warning("Failed to upsert cast_photo_people_tags: %s", response.error)
        return None

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None
