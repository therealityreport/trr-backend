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


def test_auto_count_cast_photo_returns_thumbnail_crop(monkeypatch) -> None:
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
        ),
    )
    monkeypatch.setattr(counts, "get_tags_by_photo_ids", lambda _db, _ids: {})
    monkeypatch.setattr(counts, "has_manual_tags", lambda _row: False)
    monkeypatch.setattr(counts, "upsert_cast_photo_tags", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        counts,
        "auto_thumbnail_crop",
        lambda _result: {"x": 41.2, "y": 35.4, "zoom": 1.17, "mode": "auto", "strategy": "face_torso_v2"},
    )
    monkeypatch.setattr(counts, "face_centroid", lambda _result: None)

    out = counts.auto_count_cast_photo(photo_id=UUID(str(photo_id)), force=False, db=db, _=None)
    assert out.thumbnail_crop is not None
    assert out.thumbnail_crop["x"] == 41.2
    assert out.thumbnail_crop["y"] == 35.4
    assert out.thumbnail_crop["zoom"] == 1.17
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
