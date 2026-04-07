"""Backend-owned DeepFace register/search/verify helpers for retained references."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from trr_backend.repositories import face_references

FACE_REFERENCE_EMBEDDING_PROVIDER = "deepface"
FACE_REFERENCE_EMBEDDING_MODEL_NAME = "ArcFace"
FACE_REFERENCE_EMBEDDING_MODEL_VERSION = "v1"
FACE_REFERENCE_EMBEDDING_DETECTOR = "retinaface"
FACE_REFERENCE_EMBEDDING_NORMALIZATION = "base"
FACE_REFERENCE_EMBEDDING_DIMENSIONS = 512
FACE_REFERENCE_EMBEDDING_CONTRACT_KEY = "deepface:arcface:retinaface:base:512d:l2_unit"


def _lazy_numpy():
    import numpy as np

    return np


def _lazy_deepface():
    from deepface import DeepFace

    return DeepFace


def _contract_metadata() -> dict[str, Any]:
    return {
        "contract_key": FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
        "provider": FACE_REFERENCE_EMBEDDING_PROVIDER,
        "model_name": FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        "model_version": FACE_REFERENCE_EMBEDDING_MODEL_VERSION,
        "detector_backend": FACE_REFERENCE_EMBEDDING_DETECTOR,
        "normalization": FACE_REFERENCE_EMBEDDING_NORMALIZATION,
        "dimensions": FACE_REFERENCE_EMBEDDING_DIMENSIONS,
        "unit_norm": True,
    }


def _normalize_embedding(values: Sequence[float]) -> list[float]:
    np = _lazy_numpy()
    arr = np.asarray(values, dtype=np.float32).reshape(-1)
    if getattr(arr, "size", 0) != FACE_REFERENCE_EMBEDDING_DIMENSIONS:
        raise ValueError(f"Expected {FACE_REFERENCE_EMBEDDING_DIMENSIONS}-d embedding, got {getattr(arr, 'size', 0)}")
    norm = float(np.linalg.norm(arr))
    if norm <= 1e-8:
        raise ValueError("Embedding norm is zero")
    normalized = arr / norm
    return [float(value) for value in normalized.tolist()]


def _represent_embedding(image_source: Any) -> list[float]:
    deepface = _lazy_deepface()
    response = deepface.represent(
        img_path=image_source,
        model_name=FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        detector_backend=FACE_REFERENCE_EMBEDDING_DETECTOR,
        normalization=FACE_REFERENCE_EMBEDDING_NORMALIZATION,
        enforce_detection=False,
    )
    if isinstance(response, dict):
        candidates = [response]
    else:
        candidates = list(response or [])
    if not candidates:
        raise ValueError("DeepFace.represent returned no embeddings")
    embedding = candidates[0].get("embedding")
    if not isinstance(embedding, Sequence):
        raise ValueError("DeepFace.represent returned an invalid embedding payload")
    return _normalize_embedding(embedding)


def register_reference_image(*, reference_image_id: str, image_source: Any) -> dict[str, Any]:
    metadata = _contract_metadata()
    try:
        embedding = _represent_embedding(image_source)
    except Exception as exc:  # noqa: BLE001
        failed = face_references.upsert_face_reference_embedding(
            reference_image_id=reference_image_id,
            provider=FACE_REFERENCE_EMBEDDING_PROVIDER,
            model_name=FACE_REFERENCE_EMBEDDING_MODEL_NAME,
            model_version=FACE_REFERENCE_EMBEDDING_MODEL_VERSION,
            embedding_status=face_references.FACE_REFERENCE_EMBEDDING_FAILED,
            embedding=None,
            metadata=metadata,
            error_message=str(exc),
        )
        if failed is None:
            raise
        return failed

    stored = face_references.upsert_face_reference_embedding(
        reference_image_id=reference_image_id,
        provider=FACE_REFERENCE_EMBEDDING_PROVIDER,
        model_name=FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        model_version=FACE_REFERENCE_EMBEDDING_MODEL_VERSION,
        embedding_status=face_references.FACE_REFERENCE_EMBEDDING_READY,
        embedding=embedding,
        metadata=metadata,
        error_message=None,
    )
    if stored is None:
        raise RuntimeError("Failed to persist face reference embedding")
    return stored


def search_reference_matches(
    *,
    image_source: Any | None = None,
    embedding: Sequence[float] | None = None,
    limit: int = 5,
    person_id: str | None = None,
) -> dict[str, Any]:
    normalized_embedding = (
        _normalize_embedding(embedding) if embedding is not None else _represent_embedding(image_source)
    )
    ann_matches = face_references.search_face_reference_matches(
        embedding=normalized_embedding,
        limit=limit,
        person_id=person_id,
        contract_key=FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
    )
    exact_matches = ann_matches
    if not exact_matches:
        exact_matches = face_references.search_face_reference_matches(
            embedding=normalized_embedding,
            limit=limit,
            person_id=person_id,
            contract_key=FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
        )
    return {
        "contract_key": FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
        "match_strategy": "ann_then_exact",
        "matches": exact_matches,
    }


def verify_reference_pair(*, left_image: Any, right_image: Any) -> dict[str, Any]:
    deepface = _lazy_deepface()
    response = deepface.verify(
        img1_path=left_image,
        img2_path=right_image,
        model_name=FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        detector_backend=FACE_REFERENCE_EMBEDDING_DETECTOR,
        normalization=FACE_REFERENCE_EMBEDDING_NORMALIZATION,
        enforce_detection=False,
    )
    return {
        "verified": bool(response.get("verified")),
        "distance": response.get("distance"),
        "threshold": response.get("threshold"),
        "model": FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        "provider": FACE_REFERENCE_EMBEDDING_PROVIDER,
        "contract_key": FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
        "raw": response,
    }
