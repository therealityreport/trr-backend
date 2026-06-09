"""Build cast-member face references from gallery images."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import requests

from trr_backend.vision import cast_reference_face_matching


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _lazy_cv2():
    import cv2

    return cv2


def _lazy_numpy():
    import numpy as np

    return np


def _load_image(image_source: Any) -> Any:
    cv2 = _lazy_cv2()
    np = _lazy_numpy()
    if isinstance(image_source, (str, Path)):
        source = str(image_source)
        if source.startswith(("http://", "https://")):
            response = requests.get(source, timeout=20)
            response.raise_for_status()
            payload = response.content
        else:
            payload = Path(source).read_bytes()
        image = cv2.imdecode(np.frombuffer(payload, np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            raise ValueError("Failed to decode reference image")
        return image
    return image_source


def _face_bbox(face: object) -> list[float] | None:
    bbox = getattr(face, "bbox", None)
    if bbox is None or len(bbox) < 4:
        return None
    return [float(value) for value in bbox[:4]]


def _quality_for_face(face: object, image: Any) -> dict[str, Any]:
    cv2 = _lazy_cv2()
    h, w = image.shape[:2]
    bbox = _face_bbox(face)
    if bbox is None or w <= 0 or h <= 0:
        return {"passed": False, "reasons": ["missing_bbox"]}

    x1, y1, x2, y2 = bbox
    left = max(0, int(round(x1)))
    top = max(0, int(round(y1)))
    right = min(w, int(round(x2)))
    bottom = min(h, int(round(y2)))
    face_w = max(0, right - left)
    face_h = max(0, bottom - top)
    area_ratio = (face_w * face_h) / float(max(w * h, 1))
    det_score = float(getattr(face, "det_score", 0.0) or 0.0)

    reasons: list[str] = []
    min_side_px = _env_int("CAST_REFERENCE_MIN_FACE_SIDE_PX", 80)
    min_area_ratio = _env_float("CAST_REFERENCE_MIN_FACE_AREA_RATIO", 0.01)
    min_det_score = _env_float("CAST_REFERENCE_MIN_DETECTION_CONFIDENCE", 0.75)
    min_blur_score = _env_float("CAST_REFERENCE_MIN_BLUR_SCORE", 20.0)

    if face_w < min_side_px or face_h < min_side_px:
        reasons.append("face_too_small")
    if area_ratio < min_area_ratio:
        reasons.append("face_area_too_small")
    if det_score < min_det_score:
        reasons.append("low_detection_confidence")

    blur_score = None
    if face_w > 0 and face_h > 0:
        crop = image[top:bottom, left:right]
        if getattr(crop, "size", 0):
            gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
            blur_score = float(cv2.Laplacian(gray, cv2.CV_64F).var())
            if blur_score < min_blur_score:
                reasons.append("blurry_face")

    return {
        "passed": not reasons,
        "reasons": reasons,
        "bbox": bbox,
        "det_score": det_score,
        "face_width": face_w,
        "face_height": face_h,
        "face_area_ratio": round(area_ratio, 6),
        "blur_score": round(blur_score, 3) if blur_score is not None else None,
    }


def build_cast_reference_seed(
    *,
    image_source: Any,
    assigned_person_id: str | None = None,
    selected_face_index: int | None = None,
) -> dict[str, Any]:
    image = _load_image(image_source)
    image_height, image_width = image.shape[:2]
    raw_face_count, model_id, faces = cast_reference_face_matching.detect_faces(image)
    qualities = [_quality_for_face(face, image) for face in faces]
    candidate_faces = [
        {
            "face_index": index,
            **quality,
        }
        for index, quality in enumerate(qualities)
    ]

    base_metadata = {
        **cast_reference_face_matching.contract_metadata(),
        "builder": "cast_reference_builder",
        "source_role": "gallery_cast_reference",
        "assigned_person_id": str(assigned_person_id or "").strip() or None,
        "raw_face_count": raw_face_count,
        "model_id": model_id,
        "image_width": int(image_width),
        "image_height": int(image_height),
        "candidate_faces": candidate_faces,
    }

    if not faces:
        return {
            "status": "review",
            "review_reason": "no_faces_detected",
            "embedding": None,
            "metadata": base_metadata,
            "error_message": "No faces detected in gallery image",
        }

    if selected_face_index is None:
        if len(faces) != 1:
            return {
                "status": "review",
                "review_reason": "multiple_faces_requires_human_selection",
                "embedding": None,
                "metadata": base_metadata,
                "error_message": "Gallery image has multiple faces; select the cast member face before embedding",
            }
        selected_face_index = 0

    if selected_face_index < 0 or selected_face_index >= len(faces):
        return {
            "status": "review",
            "review_reason": "selected_face_index_invalid",
            "embedding": None,
            "metadata": base_metadata,
            "error_message": "Selected face index is not present in the gallery image",
        }

    selected_quality = qualities[selected_face_index]
    if not selected_quality.get("passed"):
        return {
            "status": "review",
            "review_reason": "selected_face_failed_quality_filter",
            "embedding": None,
            "metadata": {
                **base_metadata,
                "selected_face_index": selected_face_index,
                "selected_face_quality": selected_quality,
            },
            "error_message": "Selected gallery face failed quality filters",
        }

    embedding = cast_reference_face_matching.extract_face_embedding(faces[selected_face_index])
    if embedding is None:
        return {
            "status": "review",
            "review_reason": "selected_face_missing_embedding",
            "embedding": None,
            "metadata": {
                **base_metadata,
                "selected_face_index": selected_face_index,
                "selected_face_quality": selected_quality,
            },
            "error_message": "Selected gallery face did not produce an embedding",
        }

    normalized = cast_reference_face_matching.normalize_embedding(embedding)
    embedding_hash = hashlib.sha256(",".join(f"{value:.10f}" for value in normalized).encode("utf-8")).hexdigest()
    return {
        "status": "ready",
        "review_reason": None,
        "embedding": normalized,
        "metadata": {
            **base_metadata,
            "selected_face_index": selected_face_index,
            "selected_face_quality": selected_quality,
            "embedding_sha256": embedding_hash,
        },
        "error_message": None,
    }
