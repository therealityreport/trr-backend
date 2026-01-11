"""Fetch and transform TMDb person images for cast_photos table."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from trr_backend.integrations.tmdb.client import fetch_person_images
from trr_backend.models.cast_photos import CastPhotoUpsert

TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/original"


def build_tmdb_image_url(file_path: str) -> str:
    """Build full TMDb image URL from file_path."""
    return f"{TMDB_IMAGE_BASE_URL}{file_path}"


def fetch_tmdb_person_profile_images(
    tmdb_person_id: int,
    *,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch profile images for a person from TMDb.

    Returns the list of profile image dicts from the API response.
    Each dict contains: file_path, width, height, aspect_ratio, vote_average, vote_count
    """
    response = fetch_person_images(tmdb_person_id, api_key=api_key)
    profiles = response.get("profiles")
    if not isinstance(profiles, list):
        return []
    return [p for p in profiles if isinstance(p, dict) and p.get("file_path")]


def build_tmdb_cast_photo_rows(
    person_id: UUID,
    tmdb_person_id: int,
    images: list[dict[str, Any]],
    *,
    imdb_person_id: str | None = None,
) -> list[CastPhotoUpsert]:
    """
    Transform TMDb profile images into CastPhotoUpsert rows.

    Args:
        person_id: UUID of the person in core.people table
        tmdb_person_id: TMDb person ID (integer)
        images: List of profile image dicts from TMDb API
        imdb_person_id: Optional IMDb person ID (nm...) if known

    Returns:
        List of CastPhotoUpsert objects ready for database upsert
    """
    now = datetime.now(UTC)
    rows: list[CastPhotoUpsert] = []

    for idx, img in enumerate(images):
        file_path = img.get("file_path")
        if not file_path:
            continue

        full_url = build_tmdb_image_url(file_path)
        width = img.get("width")
        height = img.get("height")

        row = CastPhotoUpsert(
            person_id=person_id,
            source="tmdb",
            imdb_person_id=imdb_person_id,
            source_image_id=file_path,  # Use file_path as unique ID within TMDb
            url=full_url,
            url_path=file_path,
            image_url=full_url,
            image_url_canonical=full_url,  # For deduplication
            width=int(width) if width is not None else None,
            height=int(height) if height is not None else None,
            position=idx + 1,
            gallery_index=idx + 1,
            gallery_total=len(images),
            fetched_at=now,
            updated_at=now,
            metadata={
                "tmdb_person_id": tmdb_person_id,
                "aspect_ratio": img.get("aspect_ratio"),
                "vote_average": img.get("vote_average"),
                "vote_count": img.get("vote_count"),
                "iso_639_1": img.get("iso_639_1"),
            },
        )
        rows.append(row)

    return rows
