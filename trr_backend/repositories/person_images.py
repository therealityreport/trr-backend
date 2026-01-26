"""Repository for person_images table operations."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any
from uuid import UUID

from trr_backend.db.supabase import call_rpc_with_cache_reload_hint
from trr_backend.repositories.media_assets import upsert_media_with_links

if TYPE_CHECKING:
    from supabase import Client
else:
    Client = Any


def upsert_person_images(
    db: Client,
    image_rows: list[dict[str, Any]],
    *,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Upsert person images into person_images.

    This function maps IMDb person IDs (nm...) to core.people UUIDs and
    upserts images into person_images using the RPC function.

    Args:
        db: Supabase client
        image_rows: List of image dicts with keys:
            - imdb_person_id (str): IMDb name ID (e.g., "nm0724202")
            - source (str): Image source (e.g., "imdb_graphql")
            - url (str): Full image URL
            - width (int | None): Image width in pixels
            - height (int | None): Image height in pixels
            - caption (str | None): Image caption text
        verbose: Print progress info

    Returns:
        List of upserted image rows from database

    Raises:
        ValueError: If any IMDb person ID cannot be resolved to a person_id

    Example:
        >>> images = [
        ...     {
        ...         "imdb_person_id": "nm0724202",
        ...         "source": "imdb_graphql",
        ...         "url": "https://m.media-amazon.com/images/M/...",
        ...         "width": 340,
        ...         "height": 238,
        ...         "caption": "Kyle Richards",
        ...     }
        ... ]
        >>> upserted = upsert_person_images(conn, images, verbose=True)
        >>> len(upserted)
        1
    """
    if not image_rows:
        if verbose:
            print("  No images to upsert")
        return []

    # Map IMDb IDs to person UUIDs
    imdb_ids = [row["imdb_person_id"] for row in image_rows]
    unique_imdb_ids = list(set(imdb_ids))

    if verbose:
        print(f"  Resolving {len(unique_imdb_ids)} unique IMDb person IDs to person_ids...")

    # Query core.people to resolve IMDb IDs to UUIDs
    response = (
        db.schema("core")
        .table("people")
        .select("id,external_ids")
        .in_("external_ids->>imdb", unique_imdb_ids)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error resolving person images: {response.error}")
    rows = response.data or []

    # Build mapping
    imdb_to_uuid: dict[str, UUID] = {}
    for row in rows if isinstance(rows, list) else []:
        person_id = row.get("id")
        imdb_id = str((row.get("external_ids") or {}).get("imdb") or "").strip()
        if person_id and imdb_id:
            imdb_to_uuid[imdb_id.lower()] = UUID(str(person_id))

    if verbose:
        print(f"  Resolved {len(imdb_to_uuid)} / {len(unique_imdb_ids)} IMDb IDs to person_ids")

    # Filter out images for people not in database
    resolved_images = []
    unresolved = []

    for img in image_rows:
        imdb_id = img["imdb_person_id"].lower()
        person_id = imdb_to_uuid.get(imdb_id)

        if person_id:
            resolved_images.append(
                {
                    "person_id": str(person_id),
                    "source": img["source"],
                    "url": img["url"],
                    "width": img.get("width"),
                    "height": img.get("height"),
                    "caption": img.get("caption"),
                    "is_primary": True,  # All GraphQL primaryImages are primary
                }
            )
        else:
            unresolved.append(imdb_id)

    if verbose and unresolved:
        print(f"  WARN: {len(unresolved)} IMDb IDs not found in core.people (will skip images)")
        if len(unresolved) <= 5:
            print(f"    Unresolved: {unresolved}")

    if not resolved_images:
        if verbose:
            print("  No images to upsert after ID resolution")
        return []

    # Call RPC function to upsert
    if verbose:
        print(f"  Upserting {len(resolved_images)} person images...")

    result = call_rpc_with_cache_reload_hint(
        db,
        schema="core",
        function_name="upsert_person_images",
        params={"rows": resolved_images},
    )

    if verbose:
        print(f"  ✓ Upserted {len(result or [])} person images")

    dual_write_enabled = os.getenv("ENABLE_MEDIA_DUAL_WRITE", "0").lower() in ("1", "true", "yes")
    if dual_write_enabled:
        try:
            upsert_media_with_links(db, resolved_images, entity_type="person")
        except Exception as exc:  # noqa: BLE001
            logging.warning("Media dual-write failed for person_images (non-blocking): %s", exc)

    # Convert to list of dicts
    return result or []
