from __future__ import annotations

from trr_backend.repositories import face_references
from trr_backend.services import face_reference_embeddings as embeddings


def test_register_reference_image_persists_ready_insightface_seed(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        embeddings,
        "_build_embedding",
        lambda **_kwargs: {
            "status": "ready",
            "embedding": [1.0] + [0.0] * 511,
            "metadata": {"selected_face_index": 0},
            "error_message": None,
        },
    )
    monkeypatch.setattr(
        face_references,
        "upsert_face_reference_embedding",
        lambda **kwargs: captured.update(kwargs) or {"id": "embedding-1", **kwargs},
    )

    result = embeddings.register_reference_image(
        reference_image_id="ref-1",
        image_source="gallery.jpg",
        assigned_person_id="person-1",
    )

    assert result["embedding_status"] == "ready"
    assert captured["provider"] == "insightface"
    assert captured["metadata"]["contract_key"] == embeddings.FACE_REFERENCE_EMBEDDING_CONTRACT_KEY
    assert captured["metadata"]["selected_face_index"] == 0


def test_register_reference_image_queues_uncertain_gallery_seed(monkeypatch) -> None:
    review_calls: list[dict[str, object]] = []
    upsert_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        embeddings,
        "_build_embedding",
        lambda **_kwargs: {
            "status": "review",
            "review_reason": "multiple_faces_requires_human_selection",
            "embedding": None,
            "metadata": {"candidate_faces": [{"face_index": 0}, {"face_index": 1}]},
            "error_message": "select a face",
        },
    )
    monkeypatch.setattr(
        face_references,
        "queue_face_reference_builder_review",
        lambda **kwargs: review_calls.append(kwargs) or {"id": kwargs["reference_image_id"]},
    )
    monkeypatch.setattr(
        face_references,
        "upsert_face_reference_embedding",
        lambda **kwargs: upsert_calls.append(kwargs) or {"id": "embedding-1", **kwargs},
    )

    result = embeddings.register_reference_image(reference_image_id="ref-1", image_source="gallery.jpg")

    assert result["embedding_status"] == "pending"
    assert review_calls[0]["review_reason"] == "multiple_faces_requires_human_selection"
    assert upsert_calls[0]["embedding"] is None
    assert upsert_calls[0]["error_message"] == "select a face"
