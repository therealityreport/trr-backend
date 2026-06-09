"""Backend-owned cast-reference register/search/verify helpers."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Any

from trr_backend.repositories import face_references
from trr_backend.services import cast_reference_builder
from trr_backend.services.face_reference_contract import (
    FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
    FACE_REFERENCE_EMBEDDING_DETECTOR,
    FACE_REFERENCE_EMBEDDING_DIMENSIONS,
    FACE_REFERENCE_EMBEDDING_MODEL_NAME,
    FACE_REFERENCE_EMBEDDING_MODEL_VERSION,
    FACE_REFERENCE_EMBEDDING_NORMALIZATION,
    FACE_REFERENCE_EMBEDDING_PROVIDER,
)
from trr_backend.vision import cast_reference_face_matching


def _lazy_numpy():
    import numpy as np

    return np


def _contract_metadata() -> dict[str, Any]:
    return {
        **cast_reference_face_matching.contract_metadata(),
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
    return cast_reference_face_matching.normalize_embedding(values)


def _build_embedding(
    *,
    image_source: Any,
    assigned_person_id: str | None = None,
    selected_face_index: int | None = None,
) -> dict[str, Any]:
    return cast_reference_builder.build_cast_reference_seed(
        image_source=image_source,
        assigned_person_id=assigned_person_id,
        selected_face_index=selected_face_index,
    )


def register_reference_image(
    *,
    reference_image_id: str,
    image_source: Any,
    assigned_person_id: str | None = None,
    selected_face_index: int | None = None,
) -> dict[str, Any]:
    metadata = _contract_metadata()
    try:
        build_result = _build_embedding(
            image_source=image_source,
            assigned_person_id=assigned_person_id,
            selected_face_index=selected_face_index,
        )
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

    metadata = {**metadata, **dict(build_result.get("metadata") or {})}
    if build_result.get("status") != "ready":
        review_reason = str(build_result.get("review_reason") or "cast_reference_builder_review")
        face_references.queue_face_reference_builder_review(
            reference_image_id=reference_image_id,
            review_reason=review_reason,
            review_notes={"builder_error": build_result.get("error_message")},
            metadata=metadata,
        )
        pending = face_references.upsert_face_reference_embedding(
            reference_image_id=reference_image_id,
            provider=FACE_REFERENCE_EMBEDDING_PROVIDER,
            model_name=FACE_REFERENCE_EMBEDDING_MODEL_NAME,
            model_version=FACE_REFERENCE_EMBEDDING_MODEL_VERSION,
            embedding_status=face_references.FACE_REFERENCE_EMBEDDING_PENDING,
            embedding=None,
            metadata=metadata,
            error_message=str(build_result.get("error_message") or review_reason),
        )
        if pending is None:
            raise RuntimeError("Failed to persist face reference builder review state")
        return pending

    stored = face_references.upsert_face_reference_embedding(
        reference_image_id=reference_image_id,
        provider=FACE_REFERENCE_EMBEDDING_PROVIDER,
        model_name=FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        model_version=FACE_REFERENCE_EMBEDDING_MODEL_VERSION,
        embedding_status=face_references.FACE_REFERENCE_EMBEDDING_READY,
        embedding=build_result["embedding"],
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
        _normalize_embedding(embedding)
        if embedding is not None
        else _build_embedding(image_source=image_source).get("embedding")
    )
    if normalized_embedding is None:
        raise ValueError("Search image did not produce a ready screen-time face embedding")
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
    np = _lazy_numpy()
    left = _build_embedding(image_source=left_image)
    right = _build_embedding(image_source=right_image)
    if left.get("status") != "ready" or right.get("status") != "ready":
        return {
            "verified": False,
            "similarity": None,
            "threshold": _screen_time_verify_threshold(),
            "model": FACE_REFERENCE_EMBEDDING_MODEL_NAME,
            "provider": FACE_REFERENCE_EMBEDDING_PROVIDER,
            "contract_key": FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
            "left_status": left.get("status"),
            "right_status": right.get("status"),
            "left_review_reason": left.get("review_reason"),
            "right_review_reason": right.get("review_reason"),
        }
    similarity = float(
        np.dot(
            np.asarray(left["embedding"], dtype=np.float32),
            np.asarray(right["embedding"], dtype=np.float32),
        )
    )
    threshold = _screen_time_verify_threshold()
    return {
        "verified": similarity >= threshold,
        "similarity": round(max(0.0, min(1.0, similarity)), 4),
        "threshold": threshold,
        "model": FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        "provider": FACE_REFERENCE_EMBEDDING_PROVIDER,
        "contract_key": FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
    }


def _screen_time_verify_threshold() -> float:
    raw = str(os.getenv("SCREEN_TIME_FACE_VERIFY_SIMILARITY_MIN") or "0.65").strip()
    try:
        return float(raw)
    except ValueError:
        return 0.65
