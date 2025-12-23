from __future__ import annotations

from typing import Any
from uuid import UUID

from supabase import Client


class ShowImagesError(RuntimeError):
    pass


def list_tmdb_show_images(
    db: Client,
    *,
    show_id: UUID | str,
    kind: str | None = None,
) -> list[dict[str, Any]]:
    """
    Return TMDb images for a show via `core.v_show_images` (includes show_name + url_original).

    This intentionally queries by `core.shows.tmdb_series_id`, not `core.show_images.show_id`, to avoid mismatches.
    """

    response = (
        db.schema("core")
        .table("shows")
        .select("tmdb_series_id")
        .eq("id", str(show_id))
        .single()
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise ShowImagesError(f"Supabase error reading show tmdb_series_id: {response.error}")
    show_row = response.data or {}
    if not isinstance(show_row, dict):
        raise ShowImagesError("Supabase returned unexpected show response shape.")

    tmdb_id = show_row.get("tmdb_series_id")
    if not isinstance(tmdb_id, int):
        return []

    query = (
        db.schema("core")
        .table("v_show_images")
        .select("*")
        .eq("tmdb_id", tmdb_id)
        .eq("source", "tmdb")
    )
    if kind:
        query = query.eq("kind", kind)
    images_response = query.execute()
    if hasattr(images_response, "error") and images_response.error:
        raise ShowImagesError(f"Supabase error listing show images: {images_response.error}")

    data = images_response.data or []
    return data if isinstance(data, list) else []
