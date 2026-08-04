from __future__ import annotations

from datetime import UTC, datetime

import pytest

from trr_backend.services import person_media_admin
from trr_backend.services.person_read_cache import (
    cache_get,
    invalidate_person_read_cache,
    resolve_person_read_singleflight,
)

PERSON_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PHOTO_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ADMIN_UID = "signed-admin-uid"
COVER_KEY = f"person:{PERSON_ID}:cover-photo"
DETAIL_KEY = f"person:{PERSON_ID}:detail"
GALLERY_KEY = f"person:{PERSON_ID}:gallery:100:0:0:1:"


@pytest.fixture(autouse=True)
def clear_person_read_cache() -> None:
    invalidate_person_read_cache()
    yield
    invalidate_person_read_cache()


def _prime_person_read_cache() -> None:
    for key in (COVER_KEY, DETAIL_KEY, GALLERY_KEY):
        resolve_person_read_singleflight(
            cache_key=key,
            ttl_seconds=60,
            loader=lambda key=key: ({"cached": key}, 1),
        )
        assert cache_get(key) == {"cached": key}


def _assert_person_read_cache_invalidated() -> None:
    assert cache_get(COVER_KEY) is None
    assert cache_get(DETAIL_KEY) is None
    assert cache_get(GALLERY_KEY) is None


def _cover_photo() -> dict[str, object]:
    return {
        "person_id": PERSON_ID,
        "photo_id": PHOTO_ID,
        "photo_url": "https://cdn.example.com/person.jpg",
        "created_at": datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        "created_by_firebase_uid": ADMIN_UID,
    }


def test_set_cover_photo_invalidates_all_backend_person_reads_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prime_person_read_cache()
    monkeypatch.setattr(
        person_media_admin.person_media_repo,
        "set_cover_photo",
        lambda **_kwargs: (_cover_photo(), 1),
    )

    result = person_media_admin.set_cover_photo(
        person_id=PERSON_ID,
        photo_id=PHOTO_ID,
        photo_url="https://cdn.example.com/person.jpg",
        actor_uid=ADMIN_UID,
    )

    assert result == (_cover_photo(), 1)
    _assert_person_read_cache_invalidated()


def test_remove_cover_photo_invalidates_all_backend_person_reads_only_when_deleted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prime_person_read_cache()
    monkeypatch.setattr(person_media_admin.person_media_repo, "remove_cover_photo", lambda _person_id: (False, 1))

    assert person_media_admin.remove_cover_photo(PERSON_ID) == (False, 1)
    assert cache_get(COVER_KEY) is not None

    monkeypatch.setattr(person_media_admin.person_media_repo, "remove_cover_photo", lambda _person_id: (True, 1))
    assert person_media_admin.remove_cover_photo(PERSON_ID) == (True, 1)
    _assert_person_read_cache_invalidated()


def test_update_thumbnail_crop_invalidates_all_backend_person_reads_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prime_person_read_cache()
    crop_result = {
        "origin": "cast_photos",
        "photo_id": PHOTO_ID,
        "person_id": PERSON_ID,
        "link_id": None,
        "thumbnail_focus_x": 44.0,
        "thumbnail_focus_y": 26.0,
        "thumbnail_zoom": 1.2,
        "thumbnail_crop_mode": "manual",
    }
    monkeypatch.setattr(
        person_media_admin.person_media_repo,
        "update_thumbnail_crop",
        lambda **_kwargs: (crop_result, 1),
    )

    result = person_media_admin.update_thumbnail_crop(
        origin="cast_photos",
        person_id=PERSON_ID,
        photo_id=PHOTO_ID,
        crop={"x": 44, "y": 26, "zoom": 1.2, "mode": "manual"},
    )

    assert result == (crop_result, 1)
    _assert_person_read_cache_invalidated()


def test_update_thumbnail_crop_preserves_cache_when_owned_photo_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prime_person_read_cache()
    monkeypatch.setattr(
        person_media_admin.person_media_repo,
        "update_thumbnail_crop",
        lambda **_kwargs: (None, 1),
    )

    assert person_media_admin.update_thumbnail_crop(
        origin="cast_photos",
        person_id=PERSON_ID,
        photo_id=PHOTO_ID,
        crop=None,
    ) == (None, 1)
    assert cache_get(GALLERY_KEY) is not None
