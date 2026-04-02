from __future__ import annotations

from psycopg2.extras import Json

from trr_backend.repositories import face_references


def test_sync_face_reference_image_upserts_enabled_row(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        face_references,
        "_media_link_image_row",
        lambda link_id: {
            "media_link_id": link_id,
            "person_id": "person-1",
            "media_asset_id": "asset-1",
            "source_url": "https://example.com/source.jpg",
            "hosted_url": "https://cdn.example.com/source.jpg",
            "hosted_sha256": "sha256-1",
        },
    )

    def _fake_execute_returning(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"media_link_id": "link-1", "embedding_status": "pending"}]

    monkeypatch.setattr(face_references.pg, "execute_returning", _fake_execute_returning)

    result = face_references.sync_face_reference_image(link_id="link-1", enabled=True)

    assert "INSERT INTO ml.face_reference_images" in str(captured["sql"])
    params = captured["params"]
    assert params[0] == "person-1"
    assert params[1] == "link-1"
    assert params[2] == "asset-1"
    assert params[3] == "https://example.com/source.jpg"
    assert params[4] == "https://cdn.example.com/source.jpg"
    assert params[5] == "sha256-1"
    assert isinstance(params[7], Json)
    assert params[7].adapted == {"source": "core.media_links.facebank_seed"}
    assert result == {"media_link_id": "link-1", "embedding_status": "pending"}


def test_sync_face_reference_image_disables_existing_row(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        face_references,
        "_media_link_image_row",
        lambda _link_id: {
            "media_link_id": "link-1",
            "person_id": "person-1",
            "media_asset_id": "asset-1",
        },
    )

    def _fake_execute_returning(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"media_link_id": "link-1", "embedding_status": "disabled", "is_active": False}]

    monkeypatch.setattr(face_references.pg, "execute_returning", _fake_execute_returning)

    result = face_references.sync_face_reference_image(link_id="link-1", enabled=False)

    assert "UPDATE ml.face_reference_images" in str(captured["sql"])
    assert captured["params"][1] == "link-1"
    assert result == {"media_link_id": "link-1", "embedding_status": "disabled", "is_active": False}


def test_list_active_face_reference_person_ids_filters_blank_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        face_references.pg,
        "fetch_all",
        lambda _sql, _params: [
            {"person_id": "person-1"},
            {"person_id": " person-2 "},
            {"person_id": ""},
            {},
        ],
    )

    result = face_references.list_active_face_reference_person_ids(["person-1", "person-2"])

    assert result == {"person-1", "person-2"}
