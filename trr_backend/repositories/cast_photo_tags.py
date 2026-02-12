"""Repository functions for admin.cast_photo_people_tags."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


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
