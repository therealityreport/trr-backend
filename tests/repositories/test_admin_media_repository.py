from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from trr_backend.repositories import admin_media

SHOW_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
IMAGE_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ENTITY_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
ASSET_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
LINK_ID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
ADMIN_UID = "signed-admin-uid"


def _media_link(*, context: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "id": LINK_ID,
        "entity_type": "season",
        "entity_id": ENTITY_ID,
        "media_asset_id": ASSET_ID,
        "kind": "gallery",
        "position": None,
        "context": context or {},
        "created_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    }


def test_validate_featured_image_uses_image_type_before_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_one(sql: str, params: list[object]):
        calls.append((sql, params))
        return {"kind": "poster", "image_type": "background"}

    monkeypatch.setattr(admin_media.pg, "fetch_one", fake_fetch_one)

    valid, query_count = admin_media.validate_show_featured_image(
        show_id=SHOW_ID,
        image_id=IMAGE_ID,
        expected_kind="backdrop",
    )

    assert valid is True
    assert query_count == 1
    assert "FROM core.show_images" in calls[0][0]
    assert calls[0][1] == [IMAGE_ID, SHOW_ID]


def test_get_image_uses_the_legacy_image_table_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_one(sql: str, params: list[object]):
        calls.append((sql, params))
        return {"id": IMAGE_ID, "season_id": ENTITY_ID, "source": "tmdb"}

    monkeypatch.setattr(admin_media.pg, "fetch_one", fake_fetch_one)

    image, query_count = admin_media.get_image("season", IMAGE_ID)

    assert image == {"id": IMAGE_ID, "season_id": ENTITY_ID, "source": "tmdb"}
    assert query_count == 1
    assert "FROM core.season_images" in calls[0][0]
    assert calls[0][1] == [IMAGE_ID]


def test_cross_type_reassignment_copies_archives_and_writes_both_audits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = {
        "id": IMAGE_ID,
        "person_id": SHOW_ID,
        "source": "getty",
        "url": "https://source.example/image.jpg",
        "hosted_url": "https://cdn.example/image.jpg",
        "caption": "Cast photo",
        "width": 1200,
        "height": 1600,
        "metadata": {"credit": "Getty"},
    }
    statements: list[tuple[str, list[object]]] = []
    monkeypatch.setattr(admin_media.pg, "fetch_one", lambda *_args, **_kwargs: source)

    def fake_execute(sql: str, params: list[object]) -> None:
        statements.append((sql, params))

    monkeypatch.setattr(admin_media.pg, "execute", fake_execute)

    query_count = admin_media.reassign_image(
        image_type="cast",
        image_id=IMAGE_ID,
        to_type="episode",
        to_entity_id=ENTITY_ID,
        mode="copy",
        actor_uid=ADMIN_UID,
    )

    assert query_count == 5
    assert "INSERT INTO core.episode_images" in statements[0][0]
    assert statements[0][1][-1] == ENTITY_ID
    assert "UPDATE core.cast_photos" in statements[1][0]
    audit_calls = [call for call in statements if "admin.image_audit_log" in call[0]]
    assert [call[1][2] for call in audit_calls] == ["archive", "copy_reassign"]
    assert all(call[1][3] == ADMIN_UID for call in audit_calls)
    copy_details = json.loads(str(audit_calls[-1][1][4]))
    assert copy_details == {
        "fromType": "cast",
        "fromEntityId": SHOW_ID,
        "toType": "episode",
        "toEntityId": ENTITY_ID,
    }


def test_create_existing_media_link_merges_new_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = _media_link(context={"people_count": 1, "kept": True})
    updated = _media_link(context={"people_count": 0, "kept": True})
    monkeypatch.setattr(admin_media.pg, "fetch_one", lambda *_args, **_kwargs: existing)
    calls: list[tuple[str, list[object]]] = []

    def fake_execute_returning(sql: str, params: list[object]):
        calls.append((sql, params))
        return [updated]

    monkeypatch.setattr(admin_media.pg, "execute_returning", fake_execute_returning)

    result, query_count = admin_media.create_media_link(
        media_asset_id=ASSET_ID,
        entity_type="season",
        entity_id=ENTITY_ID,
        kind="gallery",
        context={"people_count": 0},
    )

    assert result == {"link": updated, "already_exists": True}
    assert query_count == 2
    assert "UPDATE core.media_links" in calls[0][0]
    assert json.loads(str(calls[0][1][0])) == {"people_count": 0, "kept": True}


def test_create_new_media_link_preserves_the_legacy_insert_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inserted = _media_link(context={"source": "manual"})
    monkeypatch.setattr(admin_media.pg, "fetch_one", lambda *_args, **_kwargs: None)
    calls: list[tuple[str, list[object]]] = []

    def fake_execute_returning(sql: str, params: list[object]):
        calls.append((sql, params))
        return [inserted]

    monkeypatch.setattr(admin_media.pg, "execute_returning", fake_execute_returning)

    result, query_count = admin_media.create_media_link(
        media_asset_id=ASSET_ID,
        entity_type="season",
        entity_id=ENTITY_ID,
        kind="gallery",
        context={"source": "manual"},
    )

    assert result == {"link": inserted, "already_exists": False}
    assert query_count == 2
    assert "INSERT INTO core.media_links" in calls[0][0]
    assert calls[0][1][:4] == [ASSET_ID, "season", ENTITY_ID, "gallery"]


def test_update_media_link_context_preserves_unrelated_keys_and_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = _media_link(context={"people_count": 3, "kept": "yes"})
    updated = _media_link(context={"people_count": 0, "kept": "yes"})
    monkeypatch.setattr(admin_media.pg, "fetch_one", lambda *_args, **_kwargs: existing)
    calls: list[tuple[str, list[object]]] = []

    def fake_execute_returning(sql: str, params: list[object]):
        calls.append((sql, params))
        return [updated]

    monkeypatch.setattr(admin_media.pg, "execute_returning", fake_execute_returning)

    result, query_count = admin_media.update_media_link_context(
        LINK_ID,
        {"people_count": 0},
    )

    assert result == updated
    assert query_count == 2
    assert json.loads(str(calls[0][1][0])) == {"people_count": 0, "kept": "yes"}
