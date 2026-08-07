from __future__ import annotations

from datetime import UTC, datetime

import pytest

from trr_backend.repositories import person_media_admin

PERSON_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PHOTO_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ADMIN_UID = "signed-admin-uid"


def _cover_photo_row() -> dict[str, object]:
    return {
        "person_id": PERSON_ID,
        "photo_id": PHOTO_ID,
        "photo_url": "https://cdn.example.com/person.jpg",
        "created_at": datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        "created_by_firebase_uid": ADMIN_UID,
    }


def test_get_cover_photo_reads_the_complete_backend_owned_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_one(sql: str, params: list[object]) -> dict[str, object]:
        calls.append((sql, params))
        return _cover_photo_row()

    monkeypatch.setattr(person_media_admin.pg, "fetch_one", fake_fetch_one)

    photo, query_count = person_media_admin.get_cover_photo(PERSON_ID)

    assert query_count == 1
    assert photo == _cover_photo_row()
    assert calls[0][1] == [PERSON_ID]
    assert "created_by_firebase_uid" in calls[0][0]
    assert "WHERE person_id = %s::uuid" in calls[0][0]


def test_set_cover_photo_uses_the_signed_actor_and_returns_the_upserted_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_execute_returning(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [_cover_photo_row()]

    monkeypatch.setattr(person_media_admin.pg, "execute_returning", fake_execute_returning)

    photo, query_count = person_media_admin.set_cover_photo(
        person_id=PERSON_ID,
        photo_id=PHOTO_ID,
        photo_url="https://cdn.example.com/person.jpg",
        actor_uid=ADMIN_UID,
    )

    assert query_count == 1
    assert photo == _cover_photo_row()
    assert calls[0][1] == [PERSON_ID, PHOTO_ID, "https://cdn.example.com/person.jpg", ADMIN_UID]
    assert "ON CONFLICT (person_id) DO UPDATE" in calls[0][0]
    assert "created_by_firebase_uid = EXCLUDED.created_by_firebase_uid" in calls[0][0]


def test_remove_cover_photo_reports_whether_a_row_was_deleted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        person_media_admin.pg,
        "execute_returning",
        lambda *_args, **_kwargs: [{"person_id": PERSON_ID}],
    )
    assert person_media_admin.remove_cover_photo(PERSON_ID) == (True, 1)

    monkeypatch.setattr(person_media_admin.pg, "execute_returning", lambda *_args, **_kwargs: [])
    assert person_media_admin.remove_cover_photo(PERSON_ID) == (False, 1)


@pytest.mark.parametrize(
    ("origin", "raw_row", "expected_link_id"),
    [
        (
            "cast_photos",
            {
                "id": PHOTO_ID,
                "person_id": PERSON_ID,
                "metadata": {"thumbnail_crop": {"x": 44, "y": 26, "zoom": 1.2, "mode": "manual"}},
            },
            None,
        ),
        (
            "media_links",
            {
                "id": PHOTO_ID,
                "entity_id": PERSON_ID,
                "context": {"thumbnail_crop": {"x": 50, "y": 30, "zoom": 1.1, "mode": "auto"}},
            },
            PHOTO_ID,
        ),
    ],
)
def test_update_thumbnail_crop_preserves_the_existing_flat_result_contract(
    monkeypatch: pytest.MonkeyPatch,
    origin: person_media_admin.ThumbnailCropOrigin,
    raw_row: dict[str, object],
    expected_link_id: str | None,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_execute_returning(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [raw_row]

    monkeypatch.setattr(person_media_admin.pg, "execute_returning", fake_execute_returning)
    crop = raw_row.get("metadata", raw_row.get("context"))
    crop_payload = crop["thumbnail_crop"] if isinstance(crop, dict) else None
    assert crop_payload is not None

    result, query_count = person_media_admin.update_thumbnail_crop(
        origin=origin,
        person_id=PERSON_ID,
        photo_id=PHOTO_ID,
        crop=crop_payload,
    )

    assert query_count == 1
    assert result is not None
    assert result["origin"] == origin
    assert result["photo_id"] == PHOTO_ID
    assert result["person_id"] == PERSON_ID
    assert result["link_id"] == expected_link_id
    assert result["thumbnail_crop_mode"] == crop_payload["mode"]
    assert calls[0][1][0:2] == [PHOTO_ID, PERSON_ID]
    assert isinstance(calls[0][1][2], str)


def test_update_thumbnail_crop_returns_none_when_the_owned_photo_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(person_media_admin.pg, "execute_returning", lambda *_args, **_kwargs: [])

    result, query_count = person_media_admin.update_thumbnail_crop(
        origin="cast_photos",
        person_id=PERSON_ID,
        photo_id=PHOTO_ID,
        crop=None,
    )

    assert result is None
    assert query_count == 1
