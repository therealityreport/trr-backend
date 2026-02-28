from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from api.routers import admin_person_images


class _FakeResponse:
    def __init__(self, data: list[dict[str, Any]] | None = None):
        self.data = data or []
        self.error = None


class _FakeDb:
    def __init__(self, rows_by_table: dict[str, list[dict[str, Any]]]):
        self._rows_by_table = rows_by_table
        self._table: str | None = None
        self._pending_update: dict[str, Any] | None = None
        self._filters: list[tuple[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []

    def schema(self, _name: str) -> _FakeDb:
        return self

    def table(self, name: str) -> _FakeDb:
        self._table = name
        self._pending_update = None
        self._filters = []
        return self

    def select(self, _fields: str) -> _FakeDb:
        return self

    def eq(self, field: str, value: Any) -> _FakeDb:
        self._filters.append((field, value))
        return self

    def in_(self, _field: str, _value: Any) -> _FakeDb:
        return self

    def update(self, payload: dict[str, Any]) -> _FakeDb:
        self._pending_update = payload
        return self

    def execute(self) -> _FakeResponse:
        table = self._table or ""
        if self._pending_update is not None:
            self.update_calls.append(
                {
                    "table": table,
                    "payload": self._pending_update,
                    "filters": list(self._filters),
                }
            )
            return _FakeResponse([self._pending_update])
        return _FakeResponse(self._rows_by_table.get(table, []))


def test_auto_count_cast_photos_persists_face_boxes_face_crops_and_auto_people(monkeypatch) -> None:
    photo_id = "11111111-1111-1111-1111-111111111111"
    db = _FakeDb(
        {
            "cast_photos": [
                {
                    "id": photo_id,
                    "person_id": "person-1",
                    "source": "imdb",
                    "hosted_url": "https://cdn.example.com/photo.jpg",
                    "url": "https://images.example.com/photo.jpg",
                    "image_url": "https://images.example.com/photo.jpg",
                    "thumb_url": None,
                    "source_page_url": "https://example.com/page",
                    "metadata": {},
                }
            ]
        }
    )
    upsert_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        "trr_backend.clients.screenalytics.is_screenalytics_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "trr_backend.clients.screenalytics.count_people",
        lambda _url: SimpleNamespace(
            people_count=2,
            detector="retinaface",
            detections=[
                SimpleNamespace(
                    kind="face",
                    x1=0.1,
                    y1=0.1,
                    x2=0.3,
                    y2=0.35,
                    confidence=0.95,
                    person_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    person_name="Alan Cumming",
                    match_similarity=0.92,
                    match_status="matched",
                    square_crop_bbox=[0.08, 0.06, 0.34, 0.32],
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.get_tags_by_photo_ids",
        lambda _db, _ids: {},
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.has_manual_tags",
        lambda _tag: False,
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.upsert_cast_photo_tags",
        lambda _db, **kwargs: upsert_calls.append(kwargs),
    )
    monkeypatch.setattr(
        admin_person_images,
        "generate_and_upload_face_crops",
        lambda **_kwargs: [
            {
                "index": 1,
                "x": 0.08,
                "y": 0.06,
                "width": 0.26,
                "height": 0.26,
                "variant_key": "face-crops/cast_photo/111/crop.jpg",
                "variant_url": "https://cdn.example.com/face-crops/cast.jpg",
                "size": 256,
            }
        ],
    )
    monkeypatch.setattr(admin_person_images, "_apply_auto_crop_payload", lambda _result: None)

    attempted, succeeded, failed = admin_person_images._auto_count_cast_photos(
        db,
        person_id="person-1",
        sources=["imdb"],
    )

    assert (attempted, succeeded, failed) == (1, 1, 0)
    assert len(upsert_calls) == 1
    assert upsert_calls[0]["people_ids"] == ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]
    assert upsert_calls[0]["people_names"] == ["Alan Cumming"]

    metadata_updates = [call for call in db.update_calls if call["table"] == "cast_photos"]
    assert metadata_updates
    metadata = metadata_updates[-1]["payload"]["metadata"]
    assert isinstance(metadata.get("face_boxes"), list)
    assert isinstance(metadata.get("face_crops"), list)
    assert metadata["face_boxes"][0]["person_id"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def test_auto_count_media_links_persists_face_boxes_face_crops_and_auto_people(monkeypatch) -> None:
    db = _FakeDb({"media_links": []})

    monkeypatch.setattr(
        "trr_backend.clients.screenalytics.is_screenalytics_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "trr_backend.clients.screenalytics.count_people",
        lambda _url: SimpleNamespace(
            people_count=2,
            detector="retinaface",
            detections=[
                SimpleNamespace(
                    kind="face",
                    x1=0.4,
                    y1=0.2,
                    x2=0.62,
                    y2=0.5,
                    confidence=0.91,
                    person_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    person_name="Darren Criss",
                    match_similarity=0.9,
                    match_status="matched",
                    square_crop_bbox=[0.36, 0.15, 0.66, 0.45],
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.media_links.has_manual_people_tags",
        lambda _ctx: False,
    )
    monkeypatch.setattr(
        "trr_backend.repositories.media_links.has_people_count",
        lambda _ctx: False,
    )
    monkeypatch.setattr(
        admin_person_images,
        "_fetch_person_media_link_rows",
        lambda _db, _person_id: [
            {
                "id": "link-1",
                "media_asset_id": "asset-1",
                "context": {},
                "source": "imdb",
                "source_url": "https://images.example.com/group.jpg",
                "hosted_url": "https://cdn.example.com/group.jpg",
                "metadata": {},
            }
        ],
    )
    monkeypatch.setattr(
        admin_person_images,
        "generate_and_upload_face_crops",
        lambda **_kwargs: [
            {
                "index": 1,
                "x": 0.36,
                "y": 0.15,
                "width": 0.3,
                "height": 0.3,
                "variant_key": "face-crops/media_asset/asset-1/crop.jpg",
                "variant_url": "https://cdn.example.com/face-crops/media.jpg",
                "size": 256,
            }
        ],
    )
    monkeypatch.setattr(admin_person_images, "_apply_auto_crop_payload", lambda _result: None)

    attempted, succeeded, failed = admin_person_images._auto_count_media_links(
        db,
        person_id="person-1",
    )

    assert (attempted, succeeded, failed) == (1, 1, 0)
    media_updates = [call for call in db.update_calls if call["table"] == "media_links"]
    assert media_updates
    context = media_updates[-1]["payload"]["context"]
    assert isinstance(context.get("face_boxes"), list)
    assert isinstance(context.get("face_crops"), list)
    assert context.get("people_ids") == ["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]
    assert context.get("people_names") == ["Darren Criss"]
