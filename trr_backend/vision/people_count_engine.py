"""Backend-owned people-count engine for admin image-analysis.

This ports the admin-facing people-count subset away from the standalone
Screenalytics HTTP service so the backend can execute the same work either
locally or inside a dedicated Modal function.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import UTC, datetime
from threading import Lock
from typing import Any

import requests

from trr_backend.db.pg import db_cursor
from trr_backend.services.face_reference_embeddings import FACE_REFERENCE_EMBEDDING_CONTRACT_KEY

logger = logging.getLogger(__name__)

DetectorMode = str

_retinaface_detector: object | None = None
_yolo_detector: object | None = None
_retinaface_profile_attempts: list[str] = []
_retinaface_profile_selected: str | None = None
_retinaface_provider_selected: str | None = None
_retinaface_last_error: str | None = None
_yolo_last_error: str | None = None
_FACE_MATCH_CACHE_LOCK = Lock()
_FACE_MATCH_CACHE: dict[str, object] = {"expires_at": 0.0, "entries": []}
_OWNER_REFERENCE_CACHE_LOCK = Lock()
_OWNER_REFERENCE_CACHE: dict[str, dict[str, object]] = {}


class VisionEngineError(RuntimeError):
    """Raised when the local vision engine cannot process a request."""


class VisionEngineUnavailableError(VisionEngineError):
    """Raised when the local vision engine is temporarily unavailable."""

    def __init__(self, message: str, *, retry_after_s: int = 0):
        super().__init__(message)
        self.retry_after_s = max(0, int(retry_after_s))


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _vision_unavailable_retry_after_seconds() -> int:
    return max(_env_int("VISION_UNAVAILABLE_RETRY_AFTER_SECONDS", 30), 1)


def _lazy_numpy():
    try:
        import numpy as np
    except Exception as exc:  # noqa: BLE001
        raise VisionEngineUnavailableError(
            "Vision runtime is missing numpy; use the dedicated Modal vision worker.",
            retry_after_s=_vision_unavailable_retry_after_seconds(),
        ) from exc
    return np


def _lazy_cv2():
    try:
        import cv2
    except Exception as exc:  # noqa: BLE001
        raise VisionEngineUnavailableError(
            "Vision runtime is missing OpenCV; use the dedicated Modal vision worker.",
            retry_after_s=_vision_unavailable_retry_after_seconds(),
        ) from exc
    return cv2


def _retinaface_profile_candidates() -> list[str]:
    configured = (os.getenv("INSIGHTFACE_PROFILE") or "").strip()
    candidates: list[str] = []
    for profile in [configured, "antelopev2", "buffalo_l", "buffalo_s"]:
        normalized = str(profile or "").strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def _get_retinaface_detector() -> object | None:
    global _retinaface_detector, _retinaface_last_error, _retinaface_profile_attempts
    global _retinaface_profile_selected, _retinaface_provider_selected

    if _retinaface_detector is not None:
        return _retinaface_detector

    if os.getenv("SCREENALYTICS_VISION_SIM") == "1":
        _retinaface_last_error = "SCREENALYTICS_VISION_SIM=1 (forced simulated mode)"
        _retinaface_profile_attempts = []
        _retinaface_profile_selected = None
        _retinaface_provider_selected = None
        return None

    try:
        from insightface.app import FaceAnalysis
    except Exception as exc:  # noqa: BLE001
        _retinaface_last_error = str(exc)
        return None

    det_size = (640, 640)
    candidate_profiles = _retinaface_profile_candidates()
    _retinaface_profile_attempts = list(candidate_profiles)
    _retinaface_profile_selected = None
    _retinaface_provider_selected = None
    _retinaface_last_error = None

    for profile in candidate_profiles:
        try:
            model = FaceAnalysis(name=profile)
            model.prepare(ctx_id=-1, det_size=det_size)
            _retinaface_detector = model
            _retinaface_profile_selected = profile
            providers = getattr(model, "providers", None)
            if isinstance(providers, (list, tuple)) and providers:
                provider = str(providers[0]).strip()
                _retinaface_provider_selected = provider or None
            return _retinaface_detector
        except Exception as exc:  # noqa: BLE001
            _retinaface_last_error = str(exc)
            logger.warning("RetinaFace profile failed profile=%s error=%s", profile, exc)

    return None


def _get_yolo_detector() -> object | None:
    global _yolo_detector, _yolo_last_error

    if _yolo_detector is not None:
        return _yolo_detector

    if os.getenv("SCREENALYTICS_VISION_SIM") == "1":
        _yolo_last_error = "SCREENALYTICS_VISION_SIM=1 (forced simulated mode)"
        return None

    try:
        from ultralytics import YOLO
    except Exception as exc:  # noqa: BLE001
        _yolo_last_error = str(exc)
        return None

    try:
        model_name = os.getenv("YOLO_MODEL", "yolov8n")
        _yolo_detector = YOLO(f"{model_name}.pt")
        _yolo_last_error = None
        return _yolo_detector
    except Exception as exc:  # noqa: BLE001
        _yolo_last_error = str(exc)
        logger.warning("Failed to load YOLO detector: %s", exc)
        return None


def _ensure_detectors_available(mode: DetectorMode) -> None:
    if os.getenv("SCREENALYTICS_VISION_SIM") == "1":
        return
    if mode in {"faces", "faces_then_yolo"} and _get_retinaface_detector() is None:
        raise VisionEngineUnavailableError(
            f"RetinaFace detector unavailable: {_retinaface_last_error or 'unknown error'}",
            retry_after_s=_vision_unavailable_retry_after_seconds(),
        )
    if mode in {"yolo", "faces_then_yolo"} and _get_yolo_detector() is None:
        raise VisionEngineUnavailableError(
            f"YOLO detector unavailable: {_yolo_last_error or 'unknown error'}",
            retry_after_s=_vision_unavailable_retry_after_seconds(),
        )


def _l2_normalize(vec: Any):
    np = _lazy_numpy()
    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    if arr.size == 0:
        return None
    norm = float(np.linalg.norm(arr))
    if norm <= 1e-8:
        return None
    return arr / norm


def _extract_face_embedding(face: object):
    np = _lazy_numpy()
    for key in ("normed_embedding", "embedding"):
        candidate = getattr(face, key, None)
        if candidate is None:
            continue
        normalized = _l2_normalize(np.asarray(candidate, dtype=np.float32))
        if normalized is not None:
            return normalized
    return None


def _coerce_embedding_vector(value: Any):
    np = _lazy_numpy()
    if value is None:
        return None
    try:
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return None
            if normalized.startswith("[") and normalized.endswith("]"):
                arr = np.fromstring(normalized[1:-1], sep=",", dtype=np.float32)
            else:
                arr = np.fromstring(normalized, sep=",", dtype=np.float32)
        else:
            arr = np.asarray(value, dtype=np.float32)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Failed to coerce retained face-reference embedding: %s", exc)
        return None
    if getattr(arr, "size", 0) == 0:
        return None
    return _l2_normalize(arr)


def _load_person_facebank_centroids() -> list[dict[str, object]]:
    np = _lazy_numpy()
    cache_ttl_seconds = max(_env_int("VISION_FACE_MATCH_CACHE_TTL_SECONDS", 300), 0)
    now = time.time()
    with _FACE_MATCH_CACHE_LOCK:
        expires_at = float(_FACE_MATCH_CACHE.get("expires_at") or 0.0)
        cached_entries = _FACE_MATCH_CACHE.get("entries")
        if expires_at > now and isinstance(cached_entries, list):
            return cached_entries

    try:
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT
                  fri.person_id::text AS person_id,
                  p.full_name AS person_name,
                  fre.embedding
                FROM ml.face_reference_images AS fri
                JOIN ml.face_reference_embeddings AS fre
                  ON fre.reference_image_id = fri.id
                JOIN core.people AS p
                  ON p.id = fri.person_id
                WHERE fri.approved = true
                  AND fri.is_active = true
                  AND fri.review_status = 'approved'
                  AND fre.embedding_status = 'ready'
                  AND fre.embedding IS NOT NULL
                  AND coalesce(fre.metadata->>'contract_key', '') = %s
                ORDER BY coalesce(fre.generated_at, fre.created_at) DESC
                LIMIT 5000
                """,
                [FACE_REFERENCE_EMBEDDING_CONTRACT_KEY],
            )
            rows = list(cur.fetchall() or [])
    except Exception as exc:  # noqa: BLE001
        logger.debug("Vision identity match cache load skipped: %s", exc)
        return []

    vectors_by_person: dict[str, list[Any]] = {}
    names_by_person: dict[str, str] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id:
            continue
        embedding = _coerce_embedding_vector(row.get("embedding"))
        if embedding is None:
            continue
        vectors_by_person.setdefault(person_id, []).append(embedding)
        person_name = str(row.get("person_name") or "").strip()
        if person_name and person_id not in names_by_person:
            names_by_person[person_id] = person_name

    entries: list[dict[str, object]] = []
    for person_id, vectors in vectors_by_person.items():
        if not vectors:
            continue
        centroid = _l2_normalize(np.mean(np.stack(vectors), axis=0))
        if centroid is None:
            continue
        entries.append(
            {
                "person_id": person_id,
                "person_name": names_by_person.get(person_id) or None,
                "embedding": centroid,
            }
        )

    with _FACE_MATCH_CACHE_LOCK:
        _FACE_MATCH_CACHE["entries"] = entries
        _FACE_MATCH_CACHE["expires_at"] = now + cache_ttl_seconds
    return entries


def _normalize_candidate_person_ids(value: list[str] | None) -> set[str]:
    if not isinstance(value, list):
        return set()
    out: set[str] = set()
    for entry in value:
        if isinstance(entry, str) and entry.strip():
            out.add(entry.strip())
    return out


def _normalize_owner_person_id(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _is_http_url(value: Any) -> bool:
    return isinstance(value, str) and value.strip().lower().startswith(("http://", "https://"))


def _normalize_reference_entry(entry: dict[str, object]) -> dict[str, object] | None:
    urls: list[str] = []
    seen: set[str] = set()
    raw_candidates = entry.get("url_candidates")
    if isinstance(raw_candidates, list):
        for raw in raw_candidates:
            if not isinstance(raw, str):
                continue
            normalized = raw.strip()
            if not _is_http_url(normalized):
                continue
            key = normalized.lower()
            if key in seen:
                continue
            urls.append(normalized)
            seen.add(key)
    for field in ("hosted_url", "url", "source_url"):
        raw = entry.get(field)
        if not isinstance(raw, str):
            continue
        normalized = raw.strip()
        if not _is_http_url(normalized):
            continue
        key = normalized.lower()
        if key in seen:
            continue
        urls.append(normalized)
        seen.add(key)
    if not urls:
        return None
    normalized_entry: dict[str, object] = {"url": urls[0], "url_candidates": urls}
    for field in ("media_asset_id", "link_id", "source_url", "hosted_url"):
        raw = entry.get(field)
        if isinstance(raw, str) and raw.strip():
            normalized_entry[field] = raw.strip()
    reasons = entry.get("reasons")
    if isinstance(reasons, list):
        cleaned = [str(item).strip() for item in reasons if isinstance(item, str) and str(item).strip()]
        if cleaned:
            normalized_entry["reasons"] = cleaned
    rank = entry.get("rank")
    if isinstance(rank, int):
        normalized_entry["rank"] = max(1, rank)
    return normalized_entry


def _normalize_owner_reference_images(value: list[dict[str, object]] | None) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for entry in value:
        if not isinstance(entry, dict):
            continue
        normalized_entry = _normalize_reference_entry(entry)
        if normalized_entry is None:
            continue
        signature = "|".join(str(url).strip().lower() for url in list(normalized_entry.get("url_candidates") or []))
        if signature and signature not in seen:
            seen.add(signature)
            normalized.append(normalized_entry)
    return normalized


def _normalize_person_reference_images(value: list[dict[str, object]] | None) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, object]] = []
    seen_person_ids: set[str] = set()
    for pool in value:
        if not isinstance(pool, dict):
            continue
        person_id_raw = pool.get("person_id")
        if not isinstance(person_id_raw, str):
            continue
        person_id = person_id_raw.strip()
        if not person_id or person_id in seen_person_ids:
            continue
        references_raw = pool.get("references")
        if not isinstance(references_raw, list):
            continue
        refs = _normalize_owner_reference_images([entry for entry in references_raw if isinstance(entry, dict)])
        if not refs:
            continue
        normalized_pool: dict[str, object] = {"person_id": person_id, "references": refs}
        person_name = pool.get("person_name")
        if isinstance(person_name, str) and person_name.strip():
            normalized_pool["person_name"] = person_name.strip()
        normalized.append(normalized_pool)
        seen_person_ids.add(person_id)
    return normalized


def _face_filter_thresholds() -> tuple[int, float]:
    return max(_env_int("VISION_FACE_MATCH_MIN_SIDE_PX", 56), 1), max(
        _env_float("VISION_FACE_MATCH_MIN_AREA_RATIO", 0.004),
        0.0,
    )


def _face_filter_hard_thresholds() -> tuple[int, float]:
    return max(_env_int("VISION_FACE_MATCH_HARD_MIN_SIDE_PX", 40), 1), max(
        _env_float("VISION_FACE_MATCH_HARD_MIN_AREA_RATIO", 0.002),
        0.0,
    )


def _face_filter_metrics(face: object, *, image: Any) -> dict[str, float] | None:
    bbox = getattr(face, "bbox", None)
    if bbox is None or len(bbox) < 4:
        return None
    h, w = image.shape[:2]
    if h <= 0 or w <= 0:
        return None
    image_area = float(max(h * w, 1))
    try:
        x1, y1, x2, y2 = [float(v) for v in bbox[:4]]
    except (TypeError, ValueError):
        return None
    face_w = max(0.0, x2 - x1)
    face_h = max(0.0, y2 - y1)
    return {
        "face_w": face_w,
        "face_h": face_h,
        "face_area_ratio": (face_w * face_h) / image_area,
    }


def _adaptive_filter_faces(faces: list, *, image: Any) -> tuple[list, dict[int, dict[str, object]]]:
    if not faces:
        return [], {}

    min_side_px, min_area_ratio = _face_filter_thresholds()
    hard_min_side_px, hard_min_area_ratio = _face_filter_hard_thresholds()
    hard_min_side_px = min(hard_min_side_px, min_side_px)
    hard_min_area_ratio = min(hard_min_area_ratio, min_area_ratio)
    rescue_min_side_px = max(_env_int("VISION_FACE_MATCH_RESCUE_MIN_SIDE_PX", 48), 1)
    rescue_min_confidence = max(0.0, min(1.0, _env_float("VISION_FACE_MATCH_RESCUE_MIN_CONFIDENCE", 0.70)))

    decisions_by_face: dict[int, dict[str, object]] = {}
    kept_faces: list = []
    rescue_candidates: list[tuple[float, float, int, object]] = []

    for face in faces:
        metrics = _face_filter_metrics(face, image=image)
        metrics_payload = metrics if isinstance(metrics, dict) else {}
        if not metrics:
            decisions_by_face[id(face)] = {"filter_decision": "filtered_tiny", "filter_metrics": metrics_payload}
            continue
        face_w = float(metrics["face_w"])
        face_h = float(metrics["face_h"])
        face_area_ratio = float(metrics["face_area_ratio"])
        det_score = float(getattr(face, "det_score", 0.0))
        is_hard_tiny = face_w < hard_min_side_px or face_h < hard_min_side_px or face_area_ratio < hard_min_area_ratio
        if is_hard_tiny:
            decisions_by_face[id(face)] = {"filter_decision": "filtered_tiny", "filter_metrics": metrics_payload}
            continue
        is_kept = face_w >= min_side_px and face_h >= min_side_px and face_area_ratio >= min_area_ratio
        if is_kept:
            kept_faces.append(face)
            decisions_by_face[id(face)] = {"filter_decision": "kept", "filter_metrics": metrics_payload}
            continue
        decisions_by_face[id(face)] = {"filter_decision": "filtered_tiny", "filter_metrics": metrics_payload}
        if face_w >= rescue_min_side_px and face_h >= rescue_min_side_px and det_score >= rescue_min_confidence:
            rescue_candidates.append((face_area_ratio, det_score, len(rescue_candidates), face))

    if len(kept_faces) < 2 and len(faces) >= 2 and rescue_candidates:
        rescue_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        for _area_ratio, _det_score, _order, face in rescue_candidates:
            if len(kept_faces) >= 2:
                break
            if face in kept_faces:
                continue
            kept_faces.append(face)
            decisions_by_face[id(face)] = {
                "filter_decision": "rescued_adaptive",
                "filter_metrics": decisions_by_face.get(id(face), {}).get("filter_metrics") or {},
            }

    return kept_faces, decisions_by_face


def _download_image(url: str, timeout: float = 10.0):
    np = _lazy_numpy()
    cv2 = _lazy_cv2()
    try:
        response = requests.get(
            str(url),
            timeout=timeout,
            headers={"User-Agent": "TRR-Backend-Vision/1.0"},
            stream=True,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise VisionEngineError(f"Failed to download image: {exc}") from exc
    image_data = response.content
    nparr = np.frombuffer(image_data, np.uint8)
    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if image is None:
        raise VisionEngineError("Failed to decode image")
    return image


def _detect_faces_retinaface(image: Any) -> tuple[int, str | None, list]:
    detector = _get_retinaface_detector()
    if detector is None:
        return 0, None, []
    try:
        faces = detector.get(image)
        profile = _retinaface_profile_selected or "retinaface"
        return len(faces), f"retinaface_{profile}", list(faces)
    except Exception as exc:  # noqa: BLE001
        logger.warning("RetinaFace detection failed: %s", exc)
        return 0, None, []


def _count_people_yolo(image: Any) -> tuple[int, str | None, list[tuple[list[float], float]]]:
    detector = _get_yolo_detector()
    if detector is None:
        return 0, None, []
    try:
        results = detector(
            image,
            conf=0.50,
            iou=0.45,
            classes=[0],
            verbose=False,
        )
        raw_boxes: list[tuple[list[float], float]] = []
        count = 0
        h, w = image.shape[:2]
        for result in results:
            if result.boxes is None:
                continue
            count += len(result.boxes)
            boxes = result.boxes
            try:
                xyxy = boxes.xyxy.cpu().tolist()
            except Exception:
                xyxy = boxes.xyxy.tolist()
            try:
                confs = boxes.conf.cpu().tolist()
            except Exception:
                confs = boxes.conf.tolist() if getattr(boxes, "conf", None) is not None else []
            for index, box in enumerate(xyxy):
                if not isinstance(box, list) or len(box) < 4:
                    continue
                confidence = float(confs[index]) if index < len(confs) else 0.0
                raw_boxes.append(
                    (
                        [
                            max(0.0, min(1.0, float(box[0]) / w)),
                            max(0.0, min(1.0, float(box[1]) / h)),
                            max(0.0, min(1.0, float(box[2]) / w)),
                            max(0.0, min(1.0, float(box[3]) / h)),
                        ],
                        confidence,
                    )
                )
        return count, os.getenv("YOLO_MODEL", "yolov8n"), raw_boxes
    except Exception as exc:  # noqa: BLE001
        logger.warning("YOLO detection failed: %s", exc)
        return 0, None, []


def _simulated_count(image: Any) -> tuple[int, int]:
    np = _lazy_numpy()
    cv2 = _lazy_cv2()
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    mean_brightness = np.mean(gray)
    face_count = int(mean_brightness / 50) % 5 + 1
    return face_count, face_count


def _reference_cache_key(person_id: str, references: list[dict[str, object]]) -> str:
    material = [person_id]
    for entry in references:
        urls = [str(url).strip() for url in list(entry.get("url_candidates") or []) if isinstance(url, str)]
        material.extend(urls)
    return hashlib.sha256("|".join(material).encode("utf-8")).hexdigest()


def _owner_reference_cache_ttl_seconds() -> int:
    return max(_env_int("VISION_OWNER_REFERENCE_CACHE_TTL_SECONDS", 300), 0)


def _select_reference_face(faces: list):
    if not faces:
        return None

    def _priority(face: object) -> tuple[float, float]:
        bbox = getattr(face, "bbox", None)
        area = 0.0
        if bbox is not None and len(bbox) >= 4:
            area = max(0.0, float(bbox[2]) - float(bbox[0])) * max(0.0, float(bbox[3]) - float(bbox[1]))
        return (float(getattr(face, "det_score", 0.0)), area)

    return max(faces, key=_priority)


def _build_reference_centroid(
    *,
    person_id: str,
    references: list[dict[str, object]],
):
    np = _lazy_numpy()
    min_reference_det_conf = max(0.0, min(1.0, _env_float("VISION_OWNER_REFERENCE_MIN_DET_CONF", 0.55)))
    single_face_embeddings: list[Any] = []
    single_face_used: list[dict[str, object]] = []
    multi_face_embeddings: list[Any] = []
    multi_face_used: list[dict[str, object]] = []
    skipped: list[dict[str, str]] = []

    for ref in references:
        urls = [str(url).strip() for url in list(ref.get("url_candidates") or []) if isinstance(url, str)]
        if not urls:
            continue
        resolved_url: str | None = None
        image = None
        for candidate_url in urls:
            try:
                image = _download_image(candidate_url)
                resolved_url = candidate_url
                break
            except Exception:  # noqa: BLE001
                continue
        if image is None or not resolved_url:
            skipped.append({"url": urls[0], "reason": "download_failed"})
            continue

        face_count, _model_id, faces = _detect_faces_retinaface(image)
        if face_count <= 0 or not faces:
            skipped.append({"url": resolved_url, "reason": "no_face_detected"})
            continue

        chosen_face = _select_reference_face(list(faces))
        if chosen_face is None:
            skipped.append({"url": resolved_url, "reason": "no_face_selected"})
            continue
        det_score = float(getattr(chosen_face, "det_score", 0.0))
        if det_score < min_reference_det_conf:
            skipped.append({"url": resolved_url, "reason": "low_detection_confidence"})
            continue
        embedding = _extract_face_embedding(chosen_face)
        if embedding is None:
            skipped.append({"url": resolved_url, "reason": "no_embedding"})
            continue

        used_entry: dict[str, object] = {
            "url": resolved_url,
            "rank": ref.get("rank") or (len(single_face_used) + len(multi_face_used) + 1),
        }
        for field in ("media_asset_id", "link_id", "source_url", "hosted_url", "reasons"):
            value = ref.get(field)
            if value:
                used_entry[field] = value
        if isinstance(ref.get("url_candidates"), list):
            used_entry["url_candidates"] = [u for u in ref.get("url_candidates") if isinstance(u, str)]

        if face_count > 1:
            reasons_list = used_entry.get("reasons")
            if isinstance(reasons_list, list):
                used_entry["reasons"] = [*reasons_list, "multi_face_best_selected"]
            else:
                used_entry["reasons"] = ["multi_face_best_selected"]
            multi_face_embeddings.append(embedding)
            multi_face_used.append(used_entry)
        else:
            single_face_embeddings.append(embedding)
            single_face_used.append(used_entry)

    embeddings = single_face_embeddings or multi_face_embeddings
    used = single_face_used or multi_face_used
    if not embeddings:
        return None, used, skipped

    centroid = _l2_normalize(np.mean(np.stack(embeddings), axis=0))
    return centroid, used, skipped


def _build_owner_reference_centroid_profile(
    *,
    owner_person_id: str | None,
    owner_reference_images: list[dict[str, object]] | None,
):
    normalized_owner_id = _normalize_owner_person_id(owner_person_id)
    normalized_refs = _normalize_owner_reference_images(owner_reference_images)
    if not normalized_owner_id or not normalized_refs:
        return None, None

    cache_key = _reference_cache_key(normalized_owner_id, normalized_refs)
    now_ts = time.time()
    ttl_seconds = _owner_reference_cache_ttl_seconds()
    with _OWNER_REFERENCE_CACHE_LOCK:
        cached = _OWNER_REFERENCE_CACHE.get(cache_key)
        if cached is not None:
            expires_at = float(cached.get("expires_at") or 0.0)
            if expires_at > now_ts:
                centroid = cached.get("centroid")
                profile = cached.get("profile")
                if isinstance(profile, dict):
                    profile_out = dict(profile)
                    profile_out["cache_hit"] = True
                    return centroid, profile_out

    centroid, used, skipped = _build_reference_centroid(person_id=normalized_owner_id, references=normalized_refs)
    profile: dict[str, object] = {
        "owner_person_id": normalized_owner_id,
        "requested": len(normalized_refs),
        "accepted": len(used),
        "used": used,
        "skipped": skipped,
        "cache_hit": False,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    with _OWNER_REFERENCE_CACHE_LOCK:
        _OWNER_REFERENCE_CACHE[cache_key] = {
            "centroid": centroid,
            "profile": profile,
            "expires_at": now_ts + ttl_seconds,
        }
    return centroid, profile


def _build_person_reference_centroids(
    person_reference_images: list[dict[str, object]] | None,
) -> dict[str, dict[str, object]]:
    pools = _normalize_person_reference_images(person_reference_images)
    if not pools:
        return {}
    out: dict[str, dict[str, object]] = {}
    for pool in pools:
        person_id = str(pool.get("person_id") or "").strip()
        person_name = str(pool.get("person_name") or "").strip() if isinstance(pool.get("person_name"), str) else None
        references = pool.get("references")
        if not person_id or not isinstance(references, list):
            continue
        centroid, used, _skipped = _build_reference_centroid(person_id=person_id, references=references)
        if centroid is None:
            continue
        out[person_id] = {
            "person_id": person_id,
            "person_name": person_name,
            "embedding": centroid,
            "used": used,
        }
    return out


def _square_crop_bbox(bbox: list[float], *, width: int, height: int, padding: float = 0.35) -> list[float] | None:
    if len(bbox) < 4 or width <= 0 or height <= 0:
        return None
    x1, y1, x2, y2 = [float(v) for v in bbox[:4]]
    face_w = max(x2 - x1, 1.0)
    face_h = max(y2 - y1, 1.0)
    side = max(face_w, face_h) * (1.0 + (2.0 * max(0.0, padding)))
    cx = (x1 + x2) / 2.0
    cy = (y1 + y2) / 2.0
    left = cx - (side / 2.0)
    top = cy - (side / 2.0)
    right = left + side
    bottom = top + side
    if left < 0:
        right -= left
        left = 0.0
    if top < 0:
        bottom -= top
        top = 0.0
    if right > float(width):
        shift = right - float(width)
        left -= shift
        right = float(width)
    if bottom > float(height):
        shift = bottom - float(height)
        top -= shift
        bottom = float(height)
    left = max(0.0, left)
    top = max(0.0, top)
    right = min(float(width), max(left + 1.0, right))
    bottom = min(float(height), max(top + 1.0, bottom))
    return [
        max(0.0, min(1.0, left / float(width))),
        max(0.0, min(1.0, top / float(height))),
        max(0.0, min(1.0, right / float(width))),
        max(0.0, min(1.0, bottom / float(height))),
    ]


def _match_faces_to_people(
    faces: list,
    image: Any,
    *,
    candidate_person_ids: set[str] | None = None,
    owner_person_id: str | None = None,
    owner_reference_centroid: Any = None,
    runtime_reference_centroids: dict[str, dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    np = _lazy_numpy()
    if not faces:
        return []

    min_similarity = _env_float("VISION_FACE_MATCH_SIMILARITY_MIN", 0.65)
    min_margin = _env_float("VISION_FACE_MATCH_MARGIN_MIN", 0.03)
    single_candidate_min_similarity = _env_float(
        "VISION_FACE_MATCH_SINGLE_CANDIDATE_MIN_SIMILARITY",
        min_similarity,
    )
    crop_padding = max(_env_float("VISION_FACE_CROP_PADDING", 0.35), 0.0)
    h, w = image.shape[:2]

    centroids_by_id: dict[str, dict[str, object]] = {}
    for entry in _load_person_facebank_centroids():
        person_id = str(entry.get("person_id") or "").strip()
        if person_id:
            centroids_by_id[person_id] = entry

    if isinstance(runtime_reference_centroids, dict):
        for person_id, entry in runtime_reference_centroids.items():
            normalized_id = str(person_id or "").strip()
            if normalized_id and isinstance(entry, dict) and entry.get("embedding") is not None:
                centroids_by_id[normalized_id] = {
                    "person_id": normalized_id,
                    "person_name": entry.get("person_name"),
                    "embedding": entry.get("embedding"),
                }

    normalized_owner_id = _normalize_owner_person_id(owner_person_id)
    if normalized_owner_id and owner_reference_centroid is not None:
        existing = centroids_by_id.get(normalized_owner_id, {})
        centroids_by_id[normalized_owner_id] = {
            "person_id": normalized_owner_id,
            "person_name": existing.get("person_name"),
            "embedding": owner_reference_centroid,
        }

    centroids = list(centroids_by_id.values())
    if candidate_person_ids:
        centroids = [entry for entry in centroids if str(entry.get("person_id") or "").strip() in candidate_person_ids]

    results: list[dict[str, object]] = []
    for face in faces:
        bbox = getattr(face, "bbox", None)
        if bbox is None or len(bbox) < 4:
            results.append({})
            continue
        match_payload: dict[str, object] = {
            "square_crop_bbox": _square_crop_bbox([float(v) for v in bbox[:4]], width=w, height=h, padding=crop_padding)
        }
        face_embedding = _extract_face_embedding(face)
        if face_embedding is None:
            match_payload["match_status"] = "no_embedding"
            match_payload["match_reason"] = "no_embedding"
            results.append(match_payload)
            continue
        scored: list[tuple[float, dict[str, object]]] = []
        for entry in centroids:
            centroid = entry.get("embedding")
            if centroid is None:
                continue
            similarity = float(np.dot(face_embedding, centroid))
            scored.append((similarity, entry))
        scored.sort(key=lambda item: item[0], reverse=True)

        top_candidates: list[dict[str, object]] = []
        for similarity, entry in scored[:3]:
            top_entry: dict[str, object] = {
                "person_id": str(entry.get("person_id") or "").strip(),
                "similarity": round(max(0.0, min(1.0, similarity)), 4),
            }
            if isinstance(entry.get("person_name"), str) and str(entry.get("person_name")).strip():
                top_entry["person_name"] = str(entry.get("person_name")).strip()
            top_candidates.append(top_entry)
        if top_candidates:
            match_payload["match_candidates"] = top_candidates

        if not scored:
            match_payload["match_status"] = "unassigned"
            match_payload["match_reason"] = "no_candidates"
            results.append(match_payload)
            continue

        best_similarity, best_entry = scored[0]
        second_similarity = scored[1][0] if len(scored) > 1 else None
        similarity_margin = best_similarity - second_similarity if second_similarity is not None else None
        matched_person_id = str(best_entry.get("person_id") or "").strip() or None
        matched_person_name = str(best_entry.get("person_name") or "").strip() or None

        if len(scored) == 1:
            if best_similarity >= single_candidate_min_similarity:
                match_payload.update(
                    {
                        "person_id": matched_person_id,
                        "person_name": matched_person_name,
                        "match_similarity": round(max(0.0, min(1.0, best_similarity)), 4),
                        "match_status": "matched",
                        "match_reason": "matched",
                    }
                )
            else:
                match_payload["match_status"] = "below_threshold"
                match_payload["match_reason"] = "below_threshold"
        elif best_similarity >= min_similarity and (similarity_margin is None or similarity_margin >= min_margin):
            match_payload.update(
                {
                    "person_id": matched_person_id,
                    "person_name": matched_person_name,
                    "match_similarity": round(max(0.0, min(1.0, best_similarity)), 4),
                    "match_status": "matched",
                    "match_reason": "matched",
                }
            )
        elif best_similarity >= min_similarity:
            match_payload["match_status"] = "ambiguous"
            match_payload["match_reason"] = "ambiguous"
        else:
            match_payload["match_status"] = "below_threshold"
            match_payload["match_reason"] = "below_threshold"
        results.append(match_payload)

    return results


def _normalize_face_detections(
    faces: list,
    image: Any,
    *,
    identity_matches: list[dict[str, object]] | None = None,
    filter_diagnostics: dict[int, dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    if not faces:
        return []
    h, w = image.shape[:2]
    if h == 0 or w == 0:
        return []
    detections: list[dict[str, object]] = []
    for index, face in enumerate(faces):
        bbox = getattr(face, "bbox", None)
        if bbox is None or len(bbox) < 4:
            continue
        match_info = identity_matches[index] if identity_matches and index < len(identity_matches) else {}
        filter_info = filter_diagnostics.get(id(face), {}) if isinstance(filter_diagnostics, dict) else {}
        payload: dict[str, object] = {
            "kind": "face",
            "bbox": [
                max(0.0, min(1.0, float(bbox[0]) / w)),
                max(0.0, min(1.0, float(bbox[1]) / h)),
                max(0.0, min(1.0, float(bbox[2]) / w)),
                max(0.0, min(1.0, float(bbox[3]) / h)),
            ],
            "confidence": float(getattr(face, "det_score", 0.0)),
        }
        for field in (
            "person_id",
            "person_name",
            "match_similarity",
            "match_status",
            "match_reason",
            "match_candidates",
            "square_crop_bbox",
        ):
            value = match_info.get(field)
            if value is not None:
                payload[field] = value
        if isinstance(filter_info.get("filter_decision"), str):
            payload["filter_decision"] = filter_info["filter_decision"]
        if isinstance(filter_info.get("filter_metrics"), dict) and filter_info.get("filter_metrics"):
            payload["filter_metrics"] = {
                key: float(value)
                for key, value in filter_info["filter_metrics"].items()
                if key in {"face_w", "face_h", "face_area_ratio"} and isinstance(value, (int, float))
            }
        detections.append(payload)
    return detections


def _normalize_person_detections(person_boxes: list[tuple[list[float], float]]) -> list[dict[str, object]]:
    detections: list[dict[str, object]] = []
    for bbox, confidence in person_boxes:
        detections.append({"kind": "person", "bbox": bbox, "confidence": float(confidence)})
    return detections


def _compute_people_count_dict(payload: dict[str, object]) -> dict[str, object]:
    image_url = str(payload.get("image_url") or "").strip()
    if not image_url:
        raise VisionEngineError("image_url is required")
    mode = str(payload.get("mode") or "faces_then_yolo").strip() or "faces_then_yolo"
    image = _download_image(image_url)

    if os.getenv("SCREENALYTICS_VISION_SIM") == "1":
        face_count, people_count = _simulated_count(image)
        return {
            "people_count": people_count,
            "face_count": face_count,
            "face_count_raw": face_count,
            "face_count_filtered": face_count,
            "face_filter_thresholds": {
                "min_side_px": _face_filter_thresholds()[0],
                "min_area_ratio": _face_filter_thresholds()[1],
            },
            "detector": "simulated",
            "model": "simulated",
            "detections": (
                [
                    {"kind": "face", "bbox": [0.3, 0.1, 0.7, 0.5], "confidence": 0.95},
                    {"kind": "person", "bbox": [0.2, 0.08, 0.8, 0.95], "confidence": 0.89},
                ]
                if face_count > 0
                else []
            ),
        }

    _ensure_detectors_available(mode)

    prefer_fast_pass = (
        bool(payload.get("prefer_fast_pass")) if isinstance(payload.get("prefer_fast_pass"), bool) else False
    )
    candidate_person_ids = _normalize_candidate_person_ids(
        payload.get("candidate_person_ids") if isinstance(payload.get("candidate_person_ids"), list) else None
    )
    owner_person_id = _normalize_owner_person_id(
        payload.get("owner_person_id") if isinstance(payload.get("owner_person_id"), str) else None
    )
    owner_reference_images = _normalize_owner_reference_images(
        payload.get("owner_reference_images") if isinstance(payload.get("owner_reference_images"), list) else None
    )
    person_reference_images = _normalize_person_reference_images(
        payload.get("person_reference_images") if isinstance(payload.get("person_reference_images"), list) else None
    )
    owner_reference_centroid = None
    reference_profile = None
    runtime_reference_centroids: dict[str, dict[str, object]] = {}
    if not prefer_fast_pass:
        owner_reference_centroid, reference_profile = _build_owner_reference_centroid_profile(
            owner_person_id=owner_person_id,
            owner_reference_images=owner_reference_images,
        )
        runtime_reference_centroids = _build_person_reference_centroids(person_reference_images)
        if owner_person_id and owner_reference_centroid is not None:
            runtime_reference_centroids[owner_person_id] = {
                "person_id": owner_person_id,
                "person_name": None,
                "embedding": owner_reference_centroid,
            }

    face_count = 0
    face_count_raw = 0
    face_count_filtered = 0
    people_count = 0
    detector_used = "none"
    model_used: str | None = None
    raw_faces: list = []
    filtered_faces: list = []
    face_filter_decisions: dict[int, dict[str, object]] = {}
    raw_person_boxes: list[tuple[list[float], float]] = []

    if mode == "faces":
        _face_count_detected, model_used, raw_faces = _detect_faces_retinaface(image)
        face_count_raw = len(raw_faces)
        filtered_faces, face_filter_decisions = _adaptive_filter_faces(raw_faces, image=image)
        face_count_filtered = len(filtered_faces)
        face_count = face_count_filtered
        people_count = face_count_filtered
        detector_used = "retinaface" if face_count_filtered > 0 else "none"
    elif mode == "yolo":
        people_count, model_used, raw_person_boxes = _count_people_yolo(image)
        detector_used = "yolo" if people_count > 0 else "none"
    else:
        _face_count_detected, model_used, raw_faces = _detect_faces_retinaface(image)
        face_count_raw = len(raw_faces)
        filtered_faces, face_filter_decisions = _adaptive_filter_faces(raw_faces, image=image)
        face_count_filtered = len(filtered_faces)
        face_count = face_count_filtered
        if face_count_filtered > 0:
            people_count = face_count_filtered
            detector_used = "retinaface"
            _, _yolo_model, raw_person_boxes = _count_people_yolo(image)
        else:
            people_count, model_used, raw_person_boxes = _count_people_yolo(image)
            detector_used = "yolo" if people_count > 0 else "none"

    identity_matches = _match_faces_to_people(
        filtered_faces,
        image,
        candidate_person_ids=candidate_person_ids or None,
        owner_person_id=owner_person_id,
        owner_reference_centroid=owner_reference_centroid,
        runtime_reference_centroids=runtime_reference_centroids,
    )
    detections = [
        *_normalize_face_detections(
            filtered_faces,
            image,
            identity_matches=identity_matches,
            filter_diagnostics=face_filter_decisions,
        ),
        *_normalize_person_detections(raw_person_boxes),
    ]
    return {
        "people_count": int(people_count),
        "face_count": int(face_count),
        "face_count_raw": int(face_count_raw),
        "face_count_filtered": int(face_count_filtered),
        "face_filter_thresholds": {
            "min_side_px": _face_filter_thresholds()[0],
            "min_area_ratio": _face_filter_thresholds()[1],
        },
        "detector": detector_used,
        "model": model_used,
        "detections": detections,
        "reference_profile": reference_profile,
    }


def compute_people_count(payload: dict[str, object]) -> dict[str, object]:
    return _compute_people_count_dict(payload)


def compute_people_count_batch(payload: dict[str, object]) -> dict[str, object]:
    images = payload.get("images")
    if not isinstance(images, list) or not images:
        return {"results": []}
    results: list[dict[str, object]] = []
    for entry in images:
        if not isinstance(entry, dict):
            results.append({"image_url": "", "result": None, "error": "invalid image payload"})
            continue
        image_url = str(entry.get("image_url") or "").strip()
        try:
            result = _compute_people_count_dict(entry)
            results.append({"image_url": image_url, "result": result, "error": None})
        except Exception as exc:  # noqa: BLE001
            results.append({"image_url": image_url, "result": None, "error": str(exc)})
    return {"results": results}
