"""Cast-reference face detection and embedding helpers."""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from typing import Any

from trr_backend.services.face_reference_contract import (
    FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
    FACE_REFERENCE_EMBEDDING_DIMENSIONS,
    FACE_REFERENCE_EMBEDDING_MODEL_NAME,
)

logger = logging.getLogger(__name__)

_face_analysis_model: object | None = None
_face_analysis_last_error: str | None = None
_face_analysis_provider_selected: str | None = None


class CastReferenceFaceRuntimeUnavailableError(RuntimeError):
    """Raised when the cast-reference face runtime is not available."""


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _lazy_numpy():
    import numpy as np

    return np


def contract_metadata() -> dict[str, Any]:
    return {
        "contract_key": FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
        "provider": "insightface",
        "model_name": FACE_REFERENCE_EMBEDDING_MODEL_NAME,
        "model_version": "faceanalysis-v1",
        "detector_backend": "insightface.FaceAnalysis",
        "normalization": "normed_embedding",
        "dimensions": FACE_REFERENCE_EMBEDDING_DIMENSIONS,
        "unit_norm": True,
    }


def normalize_embedding(values: Sequence[float]) -> list[float]:
    np = _lazy_numpy()
    arr = np.asarray(values, dtype=np.float32).reshape(-1)
    if getattr(arr, "size", 0) != FACE_REFERENCE_EMBEDDING_DIMENSIONS:
        raise ValueError(f"Expected {FACE_REFERENCE_EMBEDDING_DIMENSIONS}-d embedding, got {getattr(arr, 'size', 0)}")
    norm = float(np.linalg.norm(arr))
    if norm <= 1e-8:
        raise ValueError("Embedding norm is zero")
    return [float(value) for value in (arr / norm).tolist()]


def _face_model_profile_candidates() -> list[str]:
    configured = str(os.getenv("CAST_REFERENCE_INSIGHTFACE_PROFILE") or os.getenv("INSIGHTFACE_PROFILE") or "").strip()
    candidates = [configured or FACE_REFERENCE_EMBEDDING_MODEL_NAME]
    if _env_bool("CAST_REFERENCE_INSIGHTFACE_ALLOW_MODEL_FALLBACK", default=False):
        candidates.extend(["antelopev2", "buffalo_l", "buffalo_s"])
    deduped: list[str] = []
    for candidate in candidates:
        normalized = str(candidate or "").strip()
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def get_face_analysis_model() -> object | None:
    global _face_analysis_last_error, _face_analysis_model, _face_analysis_provider_selected

    if _face_analysis_model is not None:
        return _face_analysis_model
    if os.getenv("SCREENALYTICS_VISION_SIM") == "1":
        _face_analysis_last_error = "SCREENALYTICS_VISION_SIM=1 (forced simulated mode)"
        return None

    try:
        from insightface.app import FaceAnalysis
    except Exception as exc:  # noqa: BLE001
        _face_analysis_last_error = str(exc)
        return None

    det_size = (640, 640)
    for profile in _face_model_profile_candidates():
        try:
            model = FaceAnalysis(name=profile)
            model.prepare(ctx_id=-1, det_size=det_size)
            _face_analysis_model = model
            providers = getattr(model, "providers", None)
            if isinstance(providers, (list, tuple)) and providers:
                _face_analysis_provider_selected = str(providers[0] or "").strip() or None
            _face_analysis_last_error = None
            return _face_analysis_model
        except Exception as exc:  # noqa: BLE001
            _face_analysis_last_error = str(exc)
            logger.warning("Cast-reference face model failed profile=%s error=%s", profile, exc)
    return None


def detect_faces(image: Any) -> tuple[int, str | None, list]:
    model = get_face_analysis_model()
    if model is None:
        return 0, None, []
    try:
        faces = list(model.get(image) or [])
    except Exception as exc:  # noqa: BLE001
        raise CastReferenceFaceRuntimeUnavailableError(str(exc)) from exc
    return len(faces), FACE_REFERENCE_EMBEDDING_CONTRACT_KEY, faces


def extract_face_embedding(face: object) -> Any:
    np = _lazy_numpy()
    for key in ("normed_embedding", "embedding"):
        candidate = getattr(face, key, None)
        if candidate is None:
            continue
        arr = np.asarray(candidate, dtype=np.float32).reshape(-1)
        if getattr(arr, "size", 0) != FACE_REFERENCE_EMBEDDING_DIMENSIONS:
            continue
        normalized = normalize_embedding(arr)
        return np.asarray(normalized, dtype=np.float32)
    return None


def selected_provider() -> str | None:
    return _face_analysis_provider_selected


def last_error() -> str | None:
    return _face_analysis_last_error
