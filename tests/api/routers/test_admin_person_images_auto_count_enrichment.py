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
                    "person_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
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
        "trr_backend.services.person_images.detection.is_runtime_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.count_people_with_fallback",
        lambda _url, **_kwargs: SimpleNamespace(
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
    monkeypatch.setattr(admin_person_images, "_is_trr_show_eligible", lambda *_args, **_kwargs: True)
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
        person_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
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


def test_auto_count_cast_photos_force_recount_allows_identity_without_trr_show(monkeypatch) -> None:
    photo_id = "11111111-1111-1111-1111-111111111111"
    owner_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    db = _FakeDb(
        {
            "cast_photos": [
                {
                    "id": photo_id,
                    "person_id": owner_id,
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

    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.is_runtime_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.count_people_with_fallback",
        lambda _url, **_kwargs: SimpleNamespace(
            people_count=1,
            detector="retinaface",
            detections=[
                SimpleNamespace(
                    kind="face",
                    x1=0.1,
                    y1=0.1,
                    x2=0.3,
                    y2=0.35,
                    confidence=0.95,
                    person_id=owner_id,
                    person_name="Alan Cumming",
                    match_similarity=0.92,
                    match_status="matched",
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
        lambda _db, **_kwargs: None,
    )
    monkeypatch.setattr(admin_person_images, "_is_trr_show_eligible", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(admin_person_images, "generate_and_upload_face_crops", lambda **_kwargs: [])

    diagnostics = admin_person_images._empty_auto_count_diagnostics()
    attempted, succeeded, failed = admin_person_images._auto_count_cast_photos(
        db,
        person_id=owner_id,
        sources=["imdb"],
        force_recount=True,
        diagnostics=diagnostics,
    )

    assert (attempted, succeeded, failed) == (1, 1, 0)
    assert diagnostics["auto_identity_skipped_non_trr_show"] == 0
    metadata_updates = [call for call in db.update_calls if call["table"] == "cast_photos"]
    assert metadata_updates
    metadata = metadata_updates[-1]["payload"]["metadata"]
    assert metadata["face_boxes"][0]["person_id"] == owner_id
    assert metadata["face_boxes"][0]["match_status"] == "matched"


def test_auto_count_media_links_persists_face_boxes_face_crops_and_auto_people(monkeypatch) -> None:
    db = _FakeDb({"media_links": []})

    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.is_runtime_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.count_people_with_fallback",
        lambda _url, **_kwargs: SimpleNamespace(
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
    monkeypatch.setattr(admin_person_images, "_is_trr_show_eligible", lambda *_args, **_kwargs: True)
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
        person_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    )

    assert (attempted, succeeded, failed) == (1, 1, 0)
    media_updates = [call for call in db.update_calls if call["table"] == "media_links"]
    assert media_updates
    context = media_updates[-1]["payload"]["context"]
    assert isinstance(context.get("face_boxes"), list)
    assert isinstance(context.get("face_crops"), list)
    assert context.get("people_ids") == ["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]
    assert context.get("people_names") == ["Darren Criss"]


def test_auto_count_cast_photos_promotes_owner_by_similarity_and_assigns_remaining_tag(monkeypatch) -> None:
    photo_id = "11111111-1111-1111-1111-111111111111"
    owner_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    guest_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    db = _FakeDb(
        {
            "cast_photos": [
                {
                    "id": photo_id,
                    "person_id": owner_id,
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
    captured_candidates: list[list[str]] = []

    monkeypatch.setattr("trr_backend.services.person_images.detection.is_runtime_configured", lambda: True)

    def _fake_count_people(
        _url: str,
        *,
        candidate_person_ids: list[str] | None = None,
        **_kwargs: object,
    ) -> SimpleNamespace:
        captured_candidates.append(list(candidate_person_ids or []))
        return SimpleNamespace(
            people_count=2,
            detector="retinaface",
            detections=[
                SimpleNamespace(
                    kind="face",
                    x1=0.08,
                    y1=0.14,
                    x2=0.32,
                    y2=0.42,
                    confidence=0.92,
                    person_id=owner_id,
                    person_name="Alan Cumming",
                    match_similarity=0.86,
                ),
                SimpleNamespace(
                    kind="face",
                    x1=0.52,
                    y1=0.12,
                    x2=0.78,
                    y2=0.44,
                    confidence=0.95,
                    person_id=owner_id,
                    person_name="Alan Cumming",
                    match_similarity=0.94,
                ),
            ],
        )

    monkeypatch.setattr("trr_backend.services.person_images.detection.count_people_with_fallback", _fake_count_people)
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.get_tags_by_photo_ids",
        lambda _db, _ids: {
            photo_id: {
                "people_ids": [owner_id, guest_id],
                "people_names": ["Alan Cumming", "Susan Lucci"],
            }
        },
    )
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.has_manual_tags", lambda _tag: False)
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.upsert_cast_photo_tags", lambda *_a, **_k: None)
    monkeypatch.setattr(admin_person_images, "_is_trr_show_eligible", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(admin_person_images, "_apply_auto_crop_payload", lambda _result: None)
    monkeypatch.setattr(admin_person_images, "generate_and_upload_face_crops", lambda **_kwargs: [])

    attempted, succeeded, failed = admin_person_images._auto_count_cast_photos(
        db,
        person_id=owner_id,
        owner_person_name="Alan Cumming",
        sources=["imdb"],
    )

    assert (attempted, succeeded, failed) == (1, 1, 0)
    assert captured_candidates == [[owner_id, guest_id]]
    metadata_updates = [call for call in db.update_calls if call["table"] == "cast_photos"]
    metadata = metadata_updates[-1]["payload"]["metadata"]
    face_boxes = metadata.get("face_boxes") or []
    assert len(face_boxes) == 2
    by_x = sorted(face_boxes, key=lambda box: box.get("x", 0))
    left_face = by_x[0]
    right_face = by_x[1]
    assert left_face.get("person_id") == guest_id
    assert left_face.get("person_name") == "Susan Lucci"
    assert left_face.get("label_source") == "deterministic_tag_map"
    assert right_face.get("person_id") == owner_id
    assert right_face.get("person_name") == "Alan Cumming"
    assert right_face.get("label_source") in {"owner_similarity_seed", "lead_override"}


def test_auto_count_cast_photos_does_not_force_owner_to_best_unrelated_similarity(monkeypatch) -> None:
    photo_id = "11111111-1111-1111-1111-111111111111"
    owner_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    guest_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    db = _FakeDb(
        {
            "cast_photos": [
                {
                    "id": photo_id,
                    "person_id": owner_id,
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

    monkeypatch.setattr("trr_backend.services.person_images.detection.is_runtime_configured", lambda: True)
    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.count_people_with_fallback",
        lambda _url, **_kwargs: SimpleNamespace(
            people_count=2,
            detector="retinaface",
            detections=[
                SimpleNamespace(
                    kind="face",
                    x1=0.08,
                    y1=0.14,
                    x2=0.32,
                    y2=0.42,
                    confidence=0.92,
                    person_id=guest_id,
                    person_name="Susan Lucci",
                    match_similarity=0.95,
                    match_status="matched",
                ),
                SimpleNamespace(
                    kind="face",
                    x1=0.52,
                    y1=0.12,
                    x2=0.78,
                    y2=0.44,
                    confidence=0.91,
                    person_id=owner_id,
                    person_name="Alan Cumming",
                    match_similarity=0.84,
                    match_status="matched",
                ),
            ],
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.get_tags_by_photo_ids",
        lambda _db, _ids: {
            photo_id: {
                "people_ids": [owner_id, guest_id],
                "people_names": ["Alan Cumming", "Susan Lucci"],
            }
        },
    )
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.has_manual_tags", lambda _tag: False)
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.upsert_cast_photo_tags", lambda *_a, **_k: None)
    monkeypatch.setattr(admin_person_images, "_is_trr_show_eligible", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(admin_person_images, "_apply_auto_crop_payload", lambda _result: None)
    monkeypatch.setattr(admin_person_images, "generate_and_upload_face_crops", lambda **_kwargs: [])

    attempted, succeeded, failed = admin_person_images._auto_count_cast_photos(
        db,
        person_id=owner_id,
        owner_person_name="Alan Cumming",
        sources=["imdb"],
    )

    assert (attempted, succeeded, failed) == (1, 1, 0)
    metadata_updates = [call for call in db.update_calls if call["table"] == "cast_photos"]
    metadata = metadata_updates[-1]["payload"]["metadata"]
    face_boxes = metadata.get("face_boxes") or []
    by_x = sorted(face_boxes, key=lambda box: box.get("x", 0))
    left_face = by_x[0]
    right_face = by_x[1]
    assert left_face.get("person_id") == guest_id
    assert right_face.get("person_id") == owner_id
    assert right_face.get("person_name") == "Alan Cumming"


def test_auto_count_cast_photos_keeps_existing_thumbnail_crop_without_confident_owner_match(monkeypatch) -> None:
    photo_id = "11111111-1111-1111-1111-111111111111"
    owner_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    existing_crop = {
        "x": 61.2,
        "y": 44.0,
        "zoom": 1.15,
        "mode": "auto",
        "strategy": "legacy_auto",
    }
    db = _FakeDb(
        {
            "cast_photos": [
                {
                    "id": photo_id,
                    "person_id": owner_id,
                    "source": "imdb",
                    "hosted_url": "https://cdn.example.com/photo.jpg",
                    "url": "https://images.example.com/photo.jpg",
                    "image_url": "https://images.example.com/photo.jpg",
                    "thumb_url": None,
                    "source_page_url": "https://example.com/page",
                    "metadata": {"thumbnail_crop": existing_crop},
                }
            ]
        }
    )

    monkeypatch.setattr("trr_backend.services.person_images.detection.is_runtime_configured", lambda: True)
    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.count_people_with_fallback",
        lambda _url, **_kwargs: SimpleNamespace(
            people_count=1,
            detector="retinaface",
            detections=[
                SimpleNamespace(
                    kind="face",
                    x1=0.22,
                    y1=0.16,
                    x2=0.46,
                    y2=0.44,
                    confidence=0.92,
                    person_id=owner_id,
                    person_name="Alan Cumming",
                    match_similarity=0.35,
                    match_status="below_threshold",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.get_tags_by_photo_ids",
        lambda _db, _ids: {
            photo_id: {
                "people_ids": [owner_id],
                "people_names": ["Alan Cumming"],
            }
        },
    )
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.has_manual_tags", lambda _tag: False)
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.upsert_cast_photo_tags", lambda *_a, **_k: None)
    monkeypatch.setattr(admin_person_images, "_is_trr_show_eligible", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(admin_person_images, "_apply_auto_crop_payload", lambda _result: None)
    monkeypatch.setattr(admin_person_images, "generate_and_upload_face_crops", lambda **_kwargs: [])

    attempted, succeeded, failed = admin_person_images._auto_count_cast_photos(
        db,
        person_id=owner_id,
        owner_person_name="Alan Cumming",
        sources=["imdb"],
    )

    assert (attempted, succeeded, failed) == (1, 1, 0)
    metadata_updates = [call for call in db.update_calls if call["table"] == "cast_photos"]
    metadata = metadata_updates[-1]["payload"]["metadata"]
    assert metadata.get("thumbnail_crop") == existing_crop


def test_auto_count_cast_photos_backfills_face_metadata_when_people_count_already_exists(monkeypatch) -> None:
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
                    "metadata": {"legacy": True},
                }
            ]
        }
    )

    monkeypatch.setattr("trr_backend.services.person_images.detection.is_runtime_configured", lambda: True)
    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.count_people_with_fallback",
        lambda _url, **_kwargs: SimpleNamespace(
            people_count=1,
            detector="retinaface",
            detections=[
                SimpleNamespace(
                    kind="face",
                    x1=0.2,
                    y1=0.2,
                    x2=0.4,
                    y2=0.45,
                    confidence=0.93,
                    person_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    person_name="Alan Cumming",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.get_tags_by_photo_ids",
        lambda _db, _ids: {
            photo_id: {
                "people_count": 1,
                "people_ids": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
                "people_names": ["Alan Cumming"],
            }
        },
    )
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.has_manual_tags", lambda _tag: False)
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.upsert_cast_photo_tags", lambda *_a, **_k: None)
    monkeypatch.setattr(
        admin_person_images,
        "generate_and_upload_face_crops",
        lambda **_kwargs: [
            {
                "index": 1,
                "x": 0.18,
                "y": 0.16,
                "width": 0.28,
                "height": 0.28,
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
    metadata_updates = [call for call in db.update_calls if call["table"] == "cast_photos"]
    assert metadata_updates
    metadata = metadata_updates[-1]["payload"]["metadata"]
    assert isinstance(metadata.get("face_boxes"), list)
    assert isinstance(metadata.get("face_crops"), list)


def test_auto_count_cast_photos_generates_person_fallback_boxes_when_no_faces_detected(monkeypatch) -> None:
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

    monkeypatch.setattr("trr_backend.services.person_images.detection.is_runtime_configured", lambda: True)
    monkeypatch.setattr(
        "trr_backend.services.person_images.detection.count_people_with_fallback",
        lambda _url, **_kwargs: SimpleNamespace(
            people_count=2,
            detector="yolo",
            detections=[
                SimpleNamespace(kind="person", x1=0.05, y1=0.1, x2=0.35, y2=0.62, confidence=0.95),
                SimpleNamespace(kind="person", x1=0.48, y1=0.08, x2=0.86, y2=0.72, confidence=0.92),
            ],
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photo_tags.get_tags_by_photo_ids",
        lambda _db, _ids: {
            photo_id: {
                "people_ids": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
                "people_names": ["Alan Cumming"],
            }
        },
    )
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.has_manual_tags", lambda _tag: False)
    monkeypatch.setattr("trr_backend.repositories.cast_photo_tags.upsert_cast_photo_tags", lambda *_a, **_k: None)
    monkeypatch.setattr(admin_person_images, "_is_trr_show_eligible", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        admin_person_images,
        "generate_and_upload_face_crops",
        lambda **_kwargs: [
            {
                "index": 1,
                "x": 0.0,
                "y": 0.06,
                "width": 0.4,
                "height": 0.4,
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
    metadata_updates = [call for call in db.update_calls if call["table"] == "cast_photos"]
    metadata = metadata_updates[-1]["payload"]["metadata"]
    face_boxes = metadata.get("face_boxes") or []
    assert face_boxes
    assert face_boxes[0]["source_kind"] == "person_fallback"
    assert face_boxes[0]["fallback_reason"] == "no_faces_detected"
    assert face_boxes[0]["label_source"] == "deterministic_tag_map"
