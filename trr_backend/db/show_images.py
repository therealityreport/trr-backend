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
    return data if isinstance(data, list) else []
