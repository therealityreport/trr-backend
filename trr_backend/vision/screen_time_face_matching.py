"""Screen-time-owned face detection and embedding helpers."""

from __future__ import annotations

import importlib.metadata
import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

SCREEN_TIME_FACE_PROVIDER = "deepface"
SCREEN_TIME_FACE_MODEL_NAME = str(os.getenv("SCREEN_TIME_DEEPFACE_MODEL") or "ArcFace").strip()
SCREEN_TIME_FACE_MODEL_VERSION = "deepface-represent-v1"
SCREEN_TIME_FACE_DETECTOR = str(os.getenv("SCREEN_TIME_DEEPFACE_DETECTOR") or "retinaface").strip()
SCREEN_TIME_FACE_DISTANCE_METRIC = str(os.getenv("SCREEN_TIME_DEEPFACE_DISTANCE_METRIC") or "cosine").strip()
SCREEN_TIME_FACE_NORMALIZATION = "l2_unit"
SCREEN_TIME_FACE_DIMENSIONS = 512
SCREEN_TIME_FACE_CONTRACT_KEY = (
    f"deepface:{SCREEN_TIME_FACE_MODEL_NAME}:"
    f"{SCREEN_TIME_FACE_DETECTOR}:{SCREEN_TIME_FACE_DISTANCE_METRIC}:"
    f"{SCREEN_TIME_FACE_DIMENSIONS}d:l2_unit"
)

_deepface_module: object | None = None
_deepface_last_error: str | None = None


@dataclass
class DeepFaceScreenTimeFace:
    bbox: Any
    det_score: float
    embedding: Any
    facial_area: dict[str, Any]
    raw: dict[str, Any]


class ScreenTimeFaceRuntimeUnavailableError(RuntimeError):
    """Raised when the screen-time face runtime is not available."""


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
        "contract_key": SCREEN_TIME_FACE_CONTRACT_KEY,
        "provider": SCREEN_TIME_FACE_PROVIDER,
        "model_name": SCREEN_TIME_FACE_MODEL_NAME,
        "model_version": SCREEN_TIME_FACE_MODEL_VERSION,
        "detector_backend": SCREEN_TIME_FACE_DETECTOR,
        "distance_metric": SCREEN_TIME_FACE_DISTANCE_METRIC,
        "normalization": SCREEN_TIME_FACE_NORMALIZATION,
        "dimensions": SCREEN_TIME_FACE_DIMENSIONS,
        "unit_norm": True,
        "deepface_version": _deepface_version(),
    }


def normalize_embedding(values: Sequence[float]) -> list[float]:
    np = _lazy_numpy()
    arr = np.asarray(values, dtype=np.float32).reshape(-1)
    if getattr(arr, "size", 0) != SCREEN_TIME_FACE_DIMENSIONS:
        raise ValueError(f"Expected {SCREEN_TIME_FACE_DIMENSIONS}-d embedding, got {getattr(arr, 'size', 0)}")
    norm = float(np.linalg.norm(arr))
    if norm <= 1e-8:
        raise ValueError("Embedding norm is zero")
    return [float(value) for value in (arr / norm).tolist()]


def _deepface_version() -> str | None:
    try:
        return importlib.metadata.version("deepface")
    except Exception:  # noqa: BLE001
        return None


def get_deepface_runtime() -> object | None:
    global _deepface_last_error, _deepface_module

    if _deepface_module is not None:
        return _deepface_module
    if os.getenv("SCREENALYTICS_VISION_SIM") == "1":
        _deepface_last_error = "SCREENALYTICS_VISION_SIM=1 (forced simulated mode)"
        return None

    try:
        from deepface import DeepFace
    except Exception as exc:  # noqa: BLE001
        _deepface_last_error = str(exc)
        return None

    _deepface_module = DeepFace
    _deepface_last_error = None
    return _deepface_module


def _coerce_deepface_bbox(raw_face: dict[str, Any]) -> tuple[list[float], dict[str, Any]] | None:
    facial_area = raw_face.get("facial_area")
    if not isinstance(facial_area, dict):
        facial_area = raw_face.get("region") if isinstance(raw_face.get("region"), dict) else None
    if not isinstance(facial_area, dict):
        return None
    x = float(facial_area.get("x") or 0.0)
    y = float(facial_area.get("y") or 0.0)
    w = float(facial_area.get("w") or facial_area.get("width") or 0.0)
    h = float(facial_area.get("h") or facial_area.get("height") or 0.0)
    if w <= 0 or h <= 0:
        return None
    bbox = [x, y, x + w, y + h]
    return bbox, {"x": x, "y": y, "w": w, "h": h}


def _coerce_deepface_confidence(raw_face: dict[str, Any]) -> float:
    for key in ("face_confidence", "confidence", "det_score"):
        value = raw_face.get(key)
        if isinstance(value, int | float):
            return max(0.0, min(1.0, float(value)))
    return 1.0


def _coerce_deepface_face(raw_face: dict[str, Any]) -> DeepFaceScreenTimeFace | None:
    embedding = raw_face.get("embedding")
    if embedding is None:
        return None
    bbox_result = _coerce_deepface_bbox(raw_face)
    if bbox_result is None:
        return None
    np = _lazy_numpy()
    bbox, facial_area = bbox_result
    return DeepFaceScreenTimeFace(
        bbox=np.asarray(bbox, dtype=np.float32),
        det_score=_coerce_deepface_confidence(raw_face),
        embedding=np.asarray(embedding, dtype=np.float32),
        facial_area=facial_area,
        raw=raw_face,
    )


def detect_faces(image: Any) -> tuple[int, str | None, list]:
    deepface = get_deepface_runtime()
    if deepface is None:
        return 0, None, []
    try:
        raw_faces = deepface.represent(
            img_path=image,
            model_name=SCREEN_TIME_FACE_MODEL_NAME,
            detector_backend=SCREEN_TIME_FACE_DETECTOR,
            enforce_detection=False,
            align=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise ScreenTimeFaceRuntimeUnavailableError(str(exc)) from exc
    if isinstance(raw_faces, dict):
        raw_faces = [raw_faces]
    faces = [_coerce_deepface_face(raw_face) for raw_face in list(raw_faces or []) if isinstance(raw_face, dict)]
    faces = [face for face in faces if face is not None]
    return len(faces), SCREEN_TIME_FACE_CONTRACT_KEY, faces


def extract_face_embedding(face: object) -> Any:
    np = _lazy_numpy()
    for key in ("embedding", "normed_embedding"):
        candidate = getattr(face, key, None)
        if candidate is None:
            continue
        arr = np.asarray(candidate, dtype=np.float32).reshape(-1)
        if getattr(arr, "size", 0) != SCREEN_TIME_FACE_DIMENSIONS:
            continue
        normalized = normalize_embedding(arr)
        return np.asarray(normalized, dtype=np.float32)
    return None


def selected_provider() -> str | None:
    return SCREEN_TIME_FACE_PROVIDER if _deepface_module is not None else None


def last_error() -> str | None:
    return _deepface_last_error


def filter_faces_for_screen_time(faces: list, *, image: Any) -> tuple[list, dict[int, dict[str, object]]]:
    from trr_backend.vision import people_count_engine

    return people_count_engine._adaptive_filter_faces(faces, image=image)


def match_faces_to_cast(
    faces: list,
    image: Any,
    *,
    candidate_person_ids: set[str] | None = None,
) -> list[dict[str, object]]:
    from trr_backend.services.face_reference_contract import FACE_REFERENCE_EMBEDDING_CONTRACT_KEY
    from trr_backend.vision import people_count_engine

    if SCREEN_TIME_FACE_CONTRACT_KEY != FACE_REFERENCE_EMBEDDING_CONTRACT_KEY:
        h, w = image.shape[:2]
        results: list[dict[str, object]] = []
        for face in faces:
            bbox = getattr(face, "bbox", None)
            if bbox is None or len(bbox) < 4:
                results.append(
                    {
                        "match_status": "unassigned",
                        "match_reason": "reference_contract_mismatch",
                    }
                )
                continue
            results.append(
                {
                    "square_crop_bbox": people_count_engine._square_crop_bbox(
                        [float(value) for value in bbox[:4]],
                        width=w,
                        height=h,
                        padding=0.35,
                    ),
                    "match_status": "unassigned",
                    "match_reason": "reference_contract_mismatch",
                    "face_contract_key": SCREEN_TIME_FACE_CONTRACT_KEY,
                    "reference_contract_key": FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
                }
            )
        return results

    return people_count_engine._match_faces_to_people(
        faces,
        image,
        candidate_person_ids=candidate_person_ids,
        threshold_prefix="SCREEN_TIME_FACE_MATCH",
    )


def normalize_screen_time_detections(
    faces: list,
    image: Any,
    *,
    identity_matches: list[dict[str, object]] | None = None,
    filter_diagnostics: dict[int, dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    from trr_backend.vision import people_count_engine

    return people_count_engine._normalize_face_detections(
        faces,
        image,
        identity_matches=identity_matches,
        filter_diagnostics=filter_diagnostics,
    )
