from __future__ import annotations

from contextlib import nullcontext
from typing import Any

import pytest

from trr_backend.repositories import media_link_tags as repo


def test_normalize_people_dedupes_by_id_then_name() -> None:
    assert repo._normalize_people(
        [
            {"id": " person-1 ", "name": " Person One "},
            {"id": "person-1", "name": "Person One Duplicate"},
            {"name": "Person Two"},
            {"name": "person two"},
            {"id": "person-3", "name": ""},
            None,
        ]
    ) == [
        {"id": "person-1", "name": "Person One"},
        {"name": "Person Two"},
    ]


def test_normalize_face_boxes_clamps_values_and_skips_invalid_boxes() -> None:
    assert repo._normalize_face_boxes(
        [
            {
                "index": 4.8,
                "x": -1,
                "y": 0.25,
                "width": 2,
                "height": 0.5,
                "confidence": 1.3,
                "person_name": " Person One ",
                "label": " hero ",
            },
            {"x": 0.1, "y": 0.2, "width": 0, "height": 0.3},
        ]
    ) == [
        {
            "index": 4,
            "kind": "face",
            "x": 0,
            "y": 0.25,
            "width": 1,
            "height": 0.5,
            "confidence": 1,
            "person_name": "Person One",
            "label": "hero",
        }
    ]


class _FakeCursor:
    def __init__(self, *, link: dict[str, Any] | None) -> None:
        self.link = link
        self.person_gallery_selects = 0
        self.inserted_person_ids: list[str] = []
        self.updated: list[tuple[str, dict[str, Any]]] = []
        self._one: dict[str, Any] | None = None
        self._many: list[dict[str, Any]] = []

    def execute(self, sql: str, params: list[Any]) -> None:
        normalized = " ".join(sql.split()).lower()
        if "from core.media_links" in normalized and "where id = %s::uuid" in normalized:
            self._one = self.link
            self._many = []
            return
        if "from core.media_links" in normalized and "where media_asset_id = %s::uuid" in normalized:
            self.person_gallery_selects += 1
            if not self.link:
                self._many = []
                return
            base_link = dict(self.link)
            new_links = [
                {
                    **base_link,
                    "id": f"generated-{person_id}",
                    "entity_id": person_id,
                    "context": {},
                }
                for person_id in self.inserted_person_ids
            ]
            self._many = [base_link, *new_links]
            return
        if "insert into core.media_links" in normalized:
            self.inserted_person_ids.append(str(params[0]))
            return
        if "update core.media_links" in normalized:
            self.updated.append((str(params[1]), params[0].adapted))
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many


def test_sync_media_link_tags_updates_target_and_new_person_links(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor(
        link={
            "id": "link-1",
            "entity_type": "person",
            "entity_id": "person-1",
            "media_asset_id": "asset-1",
            "kind": "gallery",
            "position": None,
            "context": {
                "people_count": 4,
                "people_count_source": "manual",
            },
            "created_at": "2026-06-08T12:00:00+00:00",
        }
    )
    monkeypatch.setattr(repo.pg, "db_connection", lambda **_kwargs: nullcontext(object()))
    monkeypatch.setattr(repo.pg, "db_cursor", lambda **_kwargs: nullcontext(cursor))

    result = repo.sync_media_link_tags(
        "link-1",
        {
            "people": [
                {"id": "person-1", "name": "Person One"},
                {"id": "person-2", "name": "Person Two"},
            ],
            "face_boxes": [{"x": 0.1, "y": 0.2, "width": 0.3, "height": 0.4}],
        },
    )

    assert result == {
        "people_names": ["Person One", "Person Two"],
        "people_ids": ["person-1", "person-2"],
        "people_count": 4,
        "people_count_source": "manual",
        "face_boxes": [
            {
                "index": 1,
                "kind": "face",
                "x": 0.1,
                "y": 0.2,
                "width": 0.3,
                "height": 0.4,
                "confidence": None,
            }
        ],
    }
    assert cursor.inserted_person_ids == ["person-2"]
    assert {link_id for link_id, _context in cursor.updated} == {"link-1", "generated-person-2"}
    assert all(context["people_count_source"] == "manual" for _link_id, context in cursor.updated)


def test_sync_media_link_tags_raises_for_missing_link(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor(link=None)
    monkeypatch.setattr(repo.pg, "db_connection", lambda **_kwargs: nullcontext(object()))
    monkeypatch.setattr(repo.pg, "db_cursor", lambda **_kwargs: nullcontext(cursor))

    with pytest.raises(repo.MediaLinkTagsNotFoundError, match="Media link not found"):
        repo.sync_media_link_tags("missing-link", {"people": []})
