"""Repository for core.person_images table operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from supabase import Client


def upsert_person_images(
    db: Client,
    image_rows: list[dict[str, Any]],
    *,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Upsert person images into core.person_images.

    This function maps IMDb person IDs (nm...) to core.people UUIDs and
    upserts images into core.person_images using the RPC function.

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
        >>> upserted = upsert_person_images(db, images, verbose=True)
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

    # Query core.people to resolve IMDb IDs to UUIDs using Supabase client
    response = (
        db.schema("core")
        .table("people")
        .select("id,external_ids")
        .in_("external_ids->>imdb", unique_imdb_ids)
        .execute()
    )

    # Build mapping
    imdb_to_uuid: dict[str, str] = {}
    for person in response.data:
        imdb_id = person.get("external_ids", {}).get("imdb")
        if imdb_id:
            imdb_to_uuid[imdb_id.lower()] = str(person["id"])

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
                    "person_id": person_id,
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

    # Call RPC function to upsert (in core schema)
    if verbose:
        print(f"  Upserting {len(resolved_images)} person images...")

    response = db.schema("core").rpc("upsert_person_images", {"rows": resolved_images}).execute()

    result_count = len(response.data) if response.data else 0
    if verbose:
        print(f"  ✓ Upserted {result_count} person images")

    return response.data if response.data else []
