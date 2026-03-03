from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from api.routers import admin_image_counts as counts


class _FakeResponse:
    def __init__(self, data):
        self.data = data
        self.error = None


class _FakeDb:
    def __init__(self, rows_by_table):
        self._rows_by_table = rows_by_table
        self._table = None
        self._pending_update = None

    def schema(self, _name):
        return self

    def table(self, name):
        self._table = name
        self._pending_update = None
        return self

    def select(self, _fields):
        return self

    def eq(self, _field, _value):
        return self

    def limit(self, _n):
        return self

    def update(self, payload):
        self._pending_update = payload
        return self

    def execute(self):
        row = self._rows_by_table.get(self._table)
        if self._pending_update is not None and row is not None:
            row.update(self._pending_update)
            return _FakeResponse([row])
        return _FakeResponse([row] if row else [])


@pytest.fixture(autouse=True)
def _stub_face_crop_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        counts,
        "generate_and_upload_face_crops",
        lambda **_kwargs: [],
    )


def test_auto_count_cast_photo_falls_back_to_original_url(monkeypatch) -> None:
    photo_id = uuid4()
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "hosted_url": None,
                "url": "https://example.com/original.jpg",
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=2,
            face_count=2,
            detector="simulated",
            model=None,
        ),
    )
    monkeypatch.setattr(counts, "get_tags_by_photo_ids", lambda _db, _ids: {})
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)

    out = counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert out.people_count == 2
    assert out.detector == "simulated"


def test_auto_count_cast_photo_retries_next_url_when_first_fails(monkeypatch) -> None:
    photo_id = uuid4()
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "source": "fandom",
                "hosted_url": None,
                "url": "https://real-housewives.fandom.com/wiki/Special:FilePath/good.jpeg",
                "image_url": "https://real-housewives.fandom.com/wiki/Special:FilePath/bad.jpeg",
                "thumb_url": None,
                "source_page_url": "https://real-housewives.fandom.com/wiki/Test",
            }
        }
    )

    calls: list[str] = []

    def fake_count_people(image_url, mode="faces_then_yolo"):  # noqa: ANN001
        calls.append(image_url)
        if "bad.jpeg" in image_url:
            raise counts.ScreenalyticsClientError("Failed to download source_url: 404")
        return SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
        )

    monkeypatch.setattr(counts, "count_people", fake_count_people)
    monkeypatch.setattr(counts, "get_tags_by_photo_ids", lambda _db, _ids: {})
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)

    out = counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert out.people_count == 1
    assert out.detector == "simulated"
    assert len(calls) >= 2
    assert any("Special:FilePath/good.jpeg" in url for url in calls)


def test_auto_count_media_asset_falls_back_to_source_url(monkeypatch) -> None:
    asset_id = uuid4()
    db = _FakeDb(
        {
            "media_assets": {
                "id": str(asset_id),
                "hosted_url": None,
                "source_url": "https://example.com/source.jpg",
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=3,
            face_count=0,
            detector="simulated",
            model=None,
        ),
    )
    monkeypatch.setattr(counts, "list_person_links_by_asset_id", lambda _db, _id: [{"context": {}}])
    monkeypatch.setattr(counts, "has_manual_people_tags", lambda _ctx: False)
    monkeypatch.setattr(counts, "has_people_count", lambda _ctx: False)
    monkeypatch.setattr(counts, "update_person_links_context", lambda *_args, **_kwargs: None)

    out = counts.auto_count_media_asset(asset_id=UUID(str(asset_id)), force=False, db=db, _=None)
    assert out.people_count == 3
    assert out.detector == "simulated"


def test_auto_count_media_asset_still_requires_any_link(monkeypatch) -> None:
    asset_id = uuid4()
    db = _FakeDb(
        {
            "media_assets": {
                "id": str(asset_id),
                "hosted_url": None,
                "source_url": "https://example.com/source.jpg",
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
        ),
    )
    monkeypatch.setattr(counts, "list_person_links_by_asset_id", lambda _db, _id: [])

    with pytest.raises(HTTPException) as excinfo:
        counts.auto_count_media_asset(asset_id=UUID(str(asset_id)), force=False, db=db, _=None)
    assert excinfo.value.status_code == 404


def test_auto_count_cast_photo_passes_owner_references_and_returns_references_used(monkeypatch) -> None:
    photo_id = uuid4()
    owner_id = str(uuid4())
    expected_references = [
        {
            "url": "https://example.com/ref-1.jpg",
            "media_asset_id": "asset-1",
            "link_id": "link-1",
            "rank": 1,
            "reasons": ["seeded", "solo"],
        }
    ]
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "person_id": owner_id,
                "hosted_url": "https://example.com/source.jpg",
                "url": "https://example.com/source.jpg",
                "metadata": {"show_name": "The Traitors"},
            }
        }
    )

    received_owner_refs: list[dict[str, object]] = []

    def _fake_count_people(image_url, **kwargs):  # noqa: ANN001
        nonlocal received_owner_refs
        owner_reference_images = kwargs.get("owner_reference_images")
        if isinstance(owner_reference_images, list):
            received_owner_refs = owner_reference_images
        return SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
            detections=[],
            reference_profile={"used": expected_references},
        )

    monkeypatch.setattr(counts, "count_people", _fake_count_people)
    monkeypatch.setattr(counts, "get_tags_by_photo_ids", lambda _db, _ids: {})
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        counts,
        "build_owner_tagging_reference_profile",
        lambda *_args, **_kwargs: {"used": expected_references},
    )
    monkeypatch.setattr(
        counts,
        "sync_owner_tagging_reference_usage",
        lambda _db, _person_id, *, used_references: used_references,
    )

    out = counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert received_owner_refs == expected_references
    assert out.references_used == expected_references


def test_auto_count_media_asset_passes_owner_references_and_returns_references_used(monkeypatch) -> None:
    asset_id = uuid4()
    owner_id = str(uuid4())
    expected_references = [
        {
            "url": "https://example.com/ref-owner.jpg",
            "media_asset_id": "asset-1",
            "link_id": "link-1",
            "rank": 1,
            "reasons": ["manual_upload", "solo"],
        }
    ]
    db = _FakeDb(
        {
            "media_assets": {
                "id": str(asset_id),
                "hosted_url": "https://example.com/source.jpg",
                "source_url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )
    links = [
        {
            "id": "link-1",
            "entity_type": "person",
            "kind": "gallery",
            "entity_id": owner_id,
            "media_asset_id": str(asset_id),
            "context": {"show_name": "The Traitors"},
        }
    ]

    received_owner_refs: list[dict[str, object]] = []

    def _fake_count_people(image_url, **kwargs):  # noqa: ANN001
        nonlocal received_owner_refs
        owner_reference_images = kwargs.get("owner_reference_images")
        if isinstance(owner_reference_images, list):
            received_owner_refs = owner_reference_images
        return SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
            detections=[],
            reference_profile={"used": expected_references},
        )

    monkeypatch.setattr(counts, "count_people", _fake_count_people)
    monkeypatch.setattr(counts, "list_person_links_by_asset_id", lambda _db, _id: links)
    monkeypatch.setattr(counts, "has_manual_people_tags", lambda _ctx: False)
    monkeypatch.setattr(counts, "has_people_count", lambda _ctx: False)
    monkeypatch.setattr(counts, "update_person_links_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        counts,
        "build_owner_tagging_reference_profile",
        lambda *_args, **_kwargs: {"used": expected_references},
    )
    monkeypatch.setattr(
        counts,
        "sync_owner_tagging_reference_usage",
        lambda _db, _person_id, *, used_references: used_references,
    )

    out = counts.auto_count_media_asset(asset_id=UUID(str(asset_id)), force=False, db=db, _=None)
    assert received_owner_refs == expected_references
    assert out.references_used == expected_references


def test_auto_count_cast_photo_passes_person_reference_images(monkeypatch) -> None:
    photo_id = uuid4()
    owner_id = str(uuid4())
    expected_person_reference_images = [
        {
            "person_id": str(uuid4()),
            "references": [
                {
                    "url": "https://example.com/ref-susan.jpg",
                    "url_candidates": [
                        "https://example.com/ref-susan.jpg",
                        "https://cdn.example.com/ref-susan.jpg",
                    ],
                    "source_url": "https://example.com/ref-susan.jpg",
                    "hosted_url": "https://cdn.example.com/ref-susan.jpg",
                    "link_id": "link-susan-1",
                    "media_asset_id": "asset-susan-1",
                    "rank": 1,
                    "reasons": ["seeded", "solo"],
                }
            ],
        }
    ]
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "person_id": owner_id,
                "hosted_url": "https://example.com/source.jpg",
                "url": "https://example.com/source.jpg",
                "metadata": {"show_name": "Watch What Happens Live with Andy Cohen"},
            }
        }
    )

    received_person_refs: list[dict[str, object]] = []

    def _fake_count_people(image_url, **kwargs):  # noqa: ANN001
        nonlocal received_person_refs
        refs = kwargs.get("person_reference_images")
        if isinstance(refs, list):
            received_person_refs = refs
        return SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
            detections=[],
            reference_profile={"used": []},
        )

    monkeypatch.setattr(counts, "count_people", _fake_count_people)
    monkeypatch.setattr(counts, "get_tags_by_photo_ids", lambda _db, _ids: {})
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        counts,
        "_resolve_runtime_person_reference_pools",
        lambda *_args, **_kwargs: expected_person_reference_images,
    )
    monkeypatch.setattr(
        counts,
        "build_owner_tagging_reference_profile",
        lambda *_args, **_kwargs: {"used": []},
    )

    counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert received_person_refs == expected_person_reference_images


def test_auto_count_media_asset_passes_person_reference_images(monkeypatch) -> None:
    asset_id = uuid4()
    owner_id = str(uuid4())
    expected_person_reference_images = [
        {
            "person_id": str(uuid4()),
            "references": [
                {
                    "url": "https://example.com/ref-guest.jpg",
                    "url_candidates": [
                        "https://example.com/ref-guest.jpg",
                        "https://cdn.example.com/ref-guest.jpg",
                    ],
                    "source_url": "https://example.com/ref-guest.jpg",
                    "hosted_url": "https://cdn.example.com/ref-guest.jpg",
                    "link_id": "link-guest-1",
                    "media_asset_id": "asset-guest-1",
                    "rank": 1,
                    "reasons": ["seeded", "solo"],
                }
            ],
        }
    ]
    db = _FakeDb(
        {
            "media_assets": {
                "id": str(asset_id),
                "hosted_url": "https://example.com/source.jpg",
                "source_url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )
    links = [
        {
            "id": "link-1",
            "entity_type": "person",
            "kind": "gallery",
            "entity_id": owner_id,
            "media_asset_id": str(asset_id),
            "context": {"show_name": "Watch What Happens Live with Andy Cohen"},
        }
    ]

    received_person_refs: list[dict[str, object]] = []

    def _fake_count_people(image_url, **kwargs):  # noqa: ANN001
        nonlocal received_person_refs
        refs = kwargs.get("person_reference_images")
        if isinstance(refs, list):
            received_person_refs = refs
        return SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
            detections=[],
            reference_profile={"used": []},
        )

    monkeypatch.setattr(counts, "count_people", _fake_count_people)
    monkeypatch.setattr(counts, "list_person_links_by_asset_id", lambda _db, _id: links)
    monkeypatch.setattr(counts, "has_manual_people_tags", lambda _ctx: False)
    monkeypatch.setattr(counts, "has_people_count", lambda _ctx: False)
    monkeypatch.setattr(counts, "update_person_links_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        counts,
        "_resolve_runtime_person_reference_pools",
        lambda *_args, **_kwargs: expected_person_reference_images,
    )
    monkeypatch.setattr(
        counts,
        "build_owner_tagging_reference_profile",
        lambda *_args, **_kwargs: {"used": []},
    )

    counts.auto_count_media_asset(asset_id=UUID(str(asset_id)), force=False, db=db, _=None)
    assert received_person_refs == expected_person_reference_images


def test_auto_count_cast_photo_force_enables_identity_even_when_not_trr_eligible(monkeypatch) -> None:
    photo_id = uuid4()
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "hosted_url": "https://example.com/source.jpg",
                "url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
            detections=[],
        ),
    )
    monkeypatch.setattr(counts, "get_tags_by_photo_ids", lambda _db, _ids: {})
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)
    monkeypatch.setattr(counts, "_is_trr_show_eligible", lambda *_args, **_kwargs: False)

    captured: dict[str, bool] = {}

    def _fake_build_detection_boxes(_result, **kwargs):  # noqa: ANN001
        captured["allow_identity_assignment"] = bool(kwargs.get("allow_identity_assignment"))
        return []

    monkeypatch.setattr(counts, "_build_detection_boxes", _fake_build_detection_boxes)

    counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=True, db=db, _=None)
    assert captured["allow_identity_assignment"] is True


def test_auto_count_media_asset_force_enables_identity_even_when_not_trr_eligible(monkeypatch) -> None:
    asset_id = uuid4()
    db = _FakeDb(
        {
            "media_assets": {
                "id": str(asset_id),
                "hosted_url": "https://example.com/source.jpg",
                "source_url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
            detections=[],
        ),
    )
    monkeypatch.setattr(
        counts,
        "list_person_links_by_asset_id",
        lambda _db, _id: [
            {
                "id": "link-1",
                "entity_type": "person",
                "kind": "gallery",
                "entity_id": str(uuid4()),
                "context": {},
            }
        ],
    )
    monkeypatch.setattr(counts, "has_manual_people_tags", lambda _ctx: False)
    monkeypatch.setattr(counts, "update_person_links_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(counts, "_is_trr_show_eligible", lambda *_args, **_kwargs: False)

    captured: dict[str, bool] = {}

    def _fake_build_detection_boxes(_result, **kwargs):  # noqa: ANN001
        captured["allow_identity_assignment"] = bool(kwargs.get("allow_identity_assignment"))
        return []

    monkeypatch.setattr(counts, "_build_detection_boxes", _fake_build_detection_boxes)

    counts.auto_count_media_asset(asset_id=UUID(str(asset_id)), force=True, db=db, _=None)
    assert captured["allow_identity_assignment"] is True


def test_auto_count_cast_photo_returns_face_boxes(monkeypatch) -> None:
    photo_id = uuid4()
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "hosted_url": "https://example.com/source.jpg",
                "url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=2,
            face_count=2,
            detector="simulated",
            model=None,
            detections=[
                SimpleNamespace(
                    x1=0.1,
                    y1=0.2,
                    x2=0.3,
                    y2=0.5,
                    confidence=0.92,
                    kind="face",
                ),
                SimpleNamespace(
                    x1=0.4,
                    y1=0.25,
                    x2=0.62,
                    y2=0.58,
                    confidence=0.88,
                    kind="face",
                ),
            ],
        ),
    )
    monkeypatch.setattr(counts, "get_tags_by_photo_ids", lambda _db, _ids: {})
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)
    monkeypatch.setattr(counts, "auto_thumbnail_crop", lambda _result: None)
    monkeypatch.setattr(counts, "face_centroid", lambda _result: None)

    out = counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert out.people_count == 2
    assert len(out.face_boxes) == 2
    assert out.face_boxes[0].x == 0.1
    assert out.face_boxes[0].width == 0.2


def test_auto_count_cast_photo_uses_person_fallback_boxes_when_faces_missing(monkeypatch) -> None:
    photo_id = uuid4()
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "hosted_url": "https://example.com/source.jpg",
                "url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=2,
            face_count=0,
            detector="yolo",
            model=None,
            detections=[
                SimpleNamespace(
                    x1=0.1,
                    y1=0.15,
                    x2=0.4,
                    y2=0.7,
                    confidence=0.9,
                    kind="person",
                ),
                SimpleNamespace(
                    x1=0.5,
                    y1=0.12,
                    x2=0.86,
                    y2=0.74,
                    confidence=0.88,
                    kind="person",
                ),
            ],
        ),
    )
    monkeypatch.setattr(
        counts,
        "get_tags_by_photo_ids",
        lambda _db, _ids: {str(photo_id): {"people_names": ["Alan Cumming"]}},
    )
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)
    monkeypatch.setattr(counts, "auto_thumbnail_crop", lambda _result: None)
    monkeypatch.setattr(counts, "face_centroid", lambda _result: None)

    out = counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert out.people_count == 2
    assert len(out.face_boxes) == 2
    assert out.face_boxes[0].source_kind == "person_fallback"
    assert out.face_boxes[0].fallback_reason == "no_faces_detected"


def test_build_detection_boxes_omits_identity_when_not_trr_eligible() -> None:
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
                person_id=str(uuid4()),
                person_name="Alan Cumming",
                label="Alan Cumming",
                match_similarity=0.95,
                match_status="matched",
                match_reason="matched",
                match_candidates=[{"person_id": str(uuid4()), "person_name": "Alan Cumming", "similarity": 0.95}],
            )
        ]
    )
    boxes = counts._build_detection_boxes(result, allow_identity_assignment=False)
    assert len(boxes) == 1
    assert boxes[0]["label_source"] == "generic"
    assert "person_id" not in boxes[0]
    assert "person_name" not in boxes[0]
    assert "match_similarity" not in boxes[0]
    assert "match_reason" not in boxes[0]
    assert "match_candidates" not in boxes[0]


def test_build_detection_boxes_includes_match_reason_and_candidates_when_allowed() -> None:
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
                person_id=str(uuid4()),
                person_name="Alan Cumming",
                label="Alan Cumming",
                match_similarity=0.81,
                match_status="below_threshold",
                match_reason="below_threshold",
                match_candidates=[
                    {"person_id": str(uuid4()), "person_name": "Susan Lucci", "similarity": 0.81},
                    {"person_id": str(uuid4()), "similarity": 0.66},
                ],
            )
        ]
    )
    boxes = counts._build_detection_boxes(result, allow_identity_assignment=True)
    assert len(boxes) == 1
    assert boxes[0]["match_reason"] == "below_threshold"
    assert isinstance(boxes[0]["match_candidates"], list)
    assert len(boxes[0]["match_candidates"]) == 2


def test_build_detection_boxes_backfills_person_name_from_tagged_people_ids() -> None:
    alan_id = "11111111-1111-1111-1111-111111111111"
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
                person_id=alan_id,
                match_similarity=0.81,
                match_status="matched",
                match_reason="matched",
                match_candidates=[{"person_id": alan_id, "similarity": 0.81}],
            )
        ],
    )

    boxes = counts._build_detection_boxes(
        result,
        allow_identity_assignment=True,
        tagged_people_ids=[alan_id],
        tagged_people_names=["Alan Cumming"],
    )
    assert len(boxes) == 1
    assert boxes[0]["person_name"] == "Alan Cumming"
    assert boxes[0]["label"] == "Alan Cumming"
    assert boxes[0]["match_candidates"][0]["person_name"] == "Alan Cumming"


def test_build_detection_boxes_person_fallback_skips_deterministic_mapping_when_not_eligible() -> None:
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.15,
                x2=0.4,
                y2=0.7,
                confidence=0.9,
                kind="person",
            )
        ]
    )
    boxes = counts._build_detection_boxes(
        result,
        tagged_people_names=["Alan Cumming"],
        allow_identity_assignment=False,
    )
    assert len(boxes) == 1
    assert boxes[0]["label_source"] == "generic"
    assert "person_name" not in boxes[0]


def test_build_detection_boxes_applies_best_effort_tag_assignment_when_tags_fewer_than_boxes() -> None:
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
            ),
            SimpleNamespace(
                x1=0.6,
                y1=0.2,
                x2=0.8,
                y2=0.5,
                confidence=0.91,
                kind="face",
            ),
        ]
    )
    boxes = counts._build_detection_boxes(
        result,
        tagged_people_names=["Alan Cumming"],
        allow_identity_assignment=True,
    )
    assert len(boxes) == 2
    assert boxes[0]["person_name"] == "Alan Cumming"
    assert boxes[0]["label_source"] == "best_effort_tag_map"
    assert boxes[0]["match_status"] == "matched"
    assert boxes[0]["match_reason"] == "best_effort_tag_map"
    assert "person_name" not in boxes[1]


def test_build_detection_boxes_promotes_single_face_deterministic_assignment_to_matched() -> None:
    owner_id = "11111111-1111-1111-1111-111111111111"
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.2,
                y1=0.15,
                x2=0.45,
                y2=0.6,
                confidence=0.84,
                kind="face",
                match_status="unassigned",
            )
        ],
    )
    boxes = counts._build_detection_boxes(
        result,
        allow_identity_assignment=True,
        tagged_people_ids=[owner_id],
        owner_person_id=owner_id,
        owner_person_name="Alan Cumming",
    )
    assert len(boxes) == 1
    assert boxes[0]["person_id"] == owner_id
    assert boxes[0]["person_name"] == "Alan Cumming"
    assert boxes[0]["label_source"] == "deterministic_tag_map"
    assert boxes[0]["match_status"] == "matched"
    assert boxes[0]["match_reason"] == "deterministic_tag_map"


def test_build_detection_boxes_applies_similarity_lead_override_before_hybrid_fallback() -> None:
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.91,
                kind="face",
                match_status="below_threshold",
                match_reason="below_threshold",
                match_similarity=0.76,
                match_candidates=[
                    {
                        "person_id": "11111111-1111-1111-1111-111111111111",
                        "person_name": "Alan Cumming",
                        "similarity": 0.76,
                    }
                ],
            ),
            SimpleNamespace(
                x1=0.6,
                y1=0.2,
                x2=0.8,
                y2=0.5,
                confidence=0.88,
                kind="face",
                match_status="below_threshold",
                match_reason="below_threshold",
                match_similarity=0.07,
                match_candidates=[
                    {
                        "person_id": "11111111-1111-1111-1111-111111111111",
                        "person_name": "Alan Cumming",
                        "similarity": 0.07,
                    }
                ],
            ),
        ]
    )

    boxes = counts._build_detection_boxes(
        result,
        allow_identity_assignment=True,
        tagged_people_ids=[
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
        ],
        tagged_people_names=["Alan Cumming", "Milo Ventimiglia"],
    )

    assert len(boxes) == 2
    by_x = sorted(boxes, key=lambda box: box.get("x", 0.0))
    lead_face = by_x[0]
    fallback_face = by_x[1]
    assert lead_face["person_name"] == "Alan Cumming"
    assert lead_face["label_source"] == "lead_override"
    assert lead_face["match_reason"] == "cross_face_lead_override"
    assert fallback_face["person_name"] == "Milo Ventimiglia"
    assert fallback_face["label_source"] == "deterministic_tag_map"


def test_auto_count_cast_photo_returns_owner_thumbnail_crop_when_confident_match(monkeypatch) -> None:
    photo_id = uuid4()
    owner_id = str(uuid4())
    db = _FakeDb(
        {
            "cast_photos": {
                "id": str(photo_id),
                "person_id": owner_id,
                "hosted_url": "https://example.com/source.jpg",
                "url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=1,
            face_count=1,
            detector="simulated",
            model=None,
            detections=[
                SimpleNamespace(
                    x1=0.2,
                    y1=0.2,
                    x2=0.4,
                    y2=0.45,
                    confidence=0.95,
                    kind="face",
                    person_id=owner_id,
                    person_name="Alan Cumming",
                    match_similarity=0.93,
                    match_status="matched",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        counts,
        "get_tags_by_photo_ids",
        lambda _db, _ids: {str(photo_id): {"people_ids": [owner_id], "people_names": ["Alan Cumming"]}},
    )
    monkeypatch.setattr(counts, "_is_trr_show_eligible", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)

    out = counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert out.thumbnail_crop is not None
    assert out.thumbnail_crop["x"] == 30.0
    assert out.thumbnail_crop["y"] == 35.5
    assert out.thumbnail_crop["zoom"] == 1.14
    assert out.thumbnail_crop["mode"] == "auto"


def test_auto_count_media_asset_returns_existing_context_crop_when_not_regenerated(monkeypatch) -> None:
    asset_id = uuid4()
    db = _FakeDb(
        {
            "media_assets": {
                "id": str(asset_id),
                "hosted_url": "https://example.com/source.jpg",
                "source_url": "https://example.com/source.jpg",
                "metadata": {},
            }
        }
    )

    monkeypatch.setattr(
        counts,
        "count_people",
        lambda image_url, mode="faces_then_yolo": SimpleNamespace(
            people_count=2,
            face_count=2,
            detector="simulated",
            model=None,
        ),
    )
    monkeypatch.setattr(
        counts,
        "list_person_links_by_asset_id",
        lambda _db, _id: [
            {
                "id": "link-1",
                "context": {
                    "thumbnail_crop": {"x": 52, "y": 33, "zoom": 1.08, "mode": "manual"},
                },
            }
        ],
    )
    monkeypatch.setattr(counts, "has_manual_people_tags", lambda _ctx: False)
    monkeypatch.setattr(counts, "update_person_links_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(counts, "auto_thumbnail_crop", lambda _result: None)
    monkeypatch.setattr(counts, "face_centroid", lambda _result: None)

    out = counts.auto_count_media_asset(asset_id=UUID(str(asset_id)), force=False, db=db, _=None)
    assert out.thumbnail_crop == {"x": 52.0, "y": 33.0, "zoom": 1.08, "mode": "manual"}
