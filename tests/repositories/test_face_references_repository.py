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
    assert "review_status" in str(captured["sql"])
    params = captured["params"]
    assert params[0] == "person-1"
    assert params[1] == "link-1"
    assert params[2] == "asset-1"
    assert isinstance(params[3], Json)
    assert params[3].adapted == {"source": "core.media_links.facebank_seed", "enrollment": True}
    assert params[4] == "https://example.com/source.jpg"
    assert params[5] == "https://cdn.example.com/source.jpg"
    assert params[6] == "sha256-1"
    assert isinstance(params[8], Json)
    assert params[8].adapted == {"source": "core.media_links.facebank_seed", "enrollment": True}
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


def test_list_face_reference_images_orders_by_review_state(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "ref-1", "review_status": "approved"}]

    monkeypatch.setattr(face_references.pg, "fetch_all", _fake_fetch_all)

    result = face_references.list_face_reference_images(person_id="person-1", include_inactive=True)

    assert "FROM ml.face_reference_images AS fri" in str(captured["sql"])
    assert "ORDER BY" in str(captured["sql"])
    assert captured["params"] == ["person-1", True]
    assert result == [{"id": "ref-1", "review_status": "approved"}]


def test_resolve_face_reference_image_supports_legacy_bridge(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_one(sql: str, params: list[object]) -> dict[str, object]:
        captured["sql"] = sql
        captured["params"] = params
        return {"id": "ref-1", "legacy_screenalytics_face_bank_image_id": "legacy-1"}

    monkeypatch.setattr(face_references.pg, "fetch_one", _fake_fetch_one)

    result = face_references.resolve_face_reference_image(legacy_screenalytics_face_bank_image_id="legacy-1")

    assert "legacy_screenalytics_face_bank_image_id" in str(captured["sql"])
    assert captured["params"] == ["legacy-1"]
    assert result == {"id": "ref-1", "legacy_screenalytics_face_bank_image_id": "legacy-1"}


def test_set_face_reference_review_status_marks_duplicates_inactive(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute_returning(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "ref-1", "review_status": "duplicate", "approved": False}]

    monkeypatch.setattr(face_references.pg, "execute_returning", _fake_execute_returning)

    result = face_references.set_face_reference_review_status(
        reference_image_id="ref-1",
        review_status="duplicate",
        reviewed_by="admin@example.com",
        review_notes={"reason": "same image"},
        duplicate_of_reference_image_id="ref-2",
    )

    assert "duplicate_of_reference_image_id" in str(captured["sql"])
    assert captured["params"][0] is False
    assert captured["params"][1] == "duplicate"
    assert isinstance(captured["params"][2], Json)
    assert captured["params"][4] == "ref-2"
    assert result == {"id": "ref-1", "review_status": "duplicate", "approved": False}


def test_upsert_face_reference_embedding_serializes_vector(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute_returning(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "emb-1", "embedding_status": "ready"}]

    monkeypatch.setattr(face_references.pg, "execute_returning", _fake_execute_returning)

    result = face_references.upsert_face_reference_embedding(
        reference_image_id="ref-1",
        provider="insightface",
        model_name="antelopev2",
        model_version="faceanalysis-v1",
        embedding_status="ready",
        embedding=[1.0, 0.0, 0.0],
        metadata={"contract_key": "insightface:antelopev2:faceanalysis:normed_embedding:512d:l2_unit"},
        error_message=None,
    )

    assert "ml.face_reference_embeddings" in str(captured["sql"])
    assert captured["params"][5] == "[1.0000000000,0.0000000000,0.0000000000]"
    assert isinstance(captured["params"][6], Json)
    assert result == {"id": "emb-1", "embedding_status": "ready"}
