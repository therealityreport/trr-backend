from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from api.routers import admin_image_counts as counts


class _FakeResponse:
    def __init__(self, data):
        self.data = data
        self.error = None


class _FakeDb:
    def __init__(self, rows_by_table):
        self._rows_by_table = rows_by_table
        self._table = None

    def schema(self, _name):
        return self

    def table(self, name):
        self._table = name
        return self

    def select(self, _fields):
        return self

    def eq(self, _field, _value):
        return self

    def limit(self, _n):
        return self

    def execute(self):
        row = self._rows_by_table.get(self._table)
        return _FakeResponse([row] if row else [])


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

    with pytest.raises(Exception):
        counts.auto_count_media_asset(asset_id=UUID(str(asset_id)), force=False, db=db, _=None)

