"""Version-neutral person-media admin service."""

from __future__ import annotations

from typing import Any

from trr_backend.repositories import person_media_admin as person_media_repo
from trr_backend.repositories.person_media_admin import ThumbnailCropOrigin
from trr_backend.services.person_read_cache import invalidate_person_read_cache


def get_cover_photo(person_id: str) -> tuple[dict[str, Any] | None, int]:
    return person_media_repo.get_cover_photo(person_id)


def set_cover_photo(
    *,
    person_id: str,
    photo_id: str,
    photo_url: str,
    actor_uid: str,
) -> tuple[dict[str, Any], int]:
    result = person_media_repo.set_cover_photo(
        person_id=person_id,
        photo_id=photo_id,
        photo_url=photo_url,
        actor_uid=actor_uid,
    )
    invalidate_person_read_cache(person_id=person_id)
    return result


def remove_cover_photo(person_id: str) -> tuple[bool, int]:
    result = person_media_repo.remove_cover_photo(person_id)
    if result[0]:
        invalidate_person_read_cache(person_id=person_id)
    return result


def update_thumbnail_crop(
    *,
    origin: ThumbnailCropOrigin,
    person_id: str,
    photo_id: str,
    crop: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, int]:
    result = person_media_repo.update_thumbnail_crop(
        origin=origin,
        person_id=person_id,
        photo_id=photo_id,
        crop=crop,
    )
    if result[0] is not None:
        invalidate_person_read_cache(person_id=person_id)
    return result
