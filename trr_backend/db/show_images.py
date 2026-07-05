from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

from trr_backend.db.session import DbSession


class ShowImagesError(RuntimeError):
    pass


def _stringify_temporal(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def _normalize_show_image_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    if not normalized.get("url_original") and normalized.get("url"):
        normalized["url_original"] = normalized.get("url")
    for field in ("fetched_at", "created_at", "updated_at"):
        if field in normalized:
            normalized[field] = _stringify_temporal(normalized.get(field))
    return normalized


def list_tmdb_show_images(
    db: DbSession,
    *,
    show_id: UUID | str,
    kind: str | None = None,
) -> list[dict[str, Any]]:
    """
    Return TMDb images for a show via `core.v_show_images_served_media_v2`.
    """

    query = (
        db.schema("core")
        .table("v_show_images_served_media_v2")
        .select("*")
        .eq("show_id", str(show_id))
        .eq("source", "tmdb")
    )
    if kind:
        query = query.eq("kind", kind)
    images_response = query.execute()
    if hasattr(images_response, "error") and images_response.error:
        raise ShowImagesError(f"Supabase error listing show images: {images_response.error}")

    data = images_response.data or []
    if not isinstance(data, list):
        return []
    return [_normalize_show_image_row(row) for row in data if isinstance(row, dict)]
