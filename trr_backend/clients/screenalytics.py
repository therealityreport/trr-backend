"""Screenalytics client for people-count estimation."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Literal

import requests

DetectorMode = Literal["faces_then_yolo", "faces", "yolo"]

logger = logging.getLogger(__name__)
_UNAVAILABLE_LOCK = Lock()
_UNAVAILABLE_UNTIL_MONO = 0.0
_UNAVAILABLE_REASON: str | None = None


def _unavailable_cooldown_seconds() -> int:
    raw = os.getenv("SCREENALYTICS_UNAVAILABLE_COOLDOWN_SECONDS", "").strip()
    if raw:
        try:
            parsed = int(raw)
            if parsed > 0:
                return parsed
        except ValueError:
            pass
    return 300


def _mark_screenalytics_unavailable(reason: str) -> None:
    global _UNAVAILABLE_UNTIL_MONO, _UNAVAILABLE_REASON
    now = time.monotonic()
    cooldown = _unavailable_cooldown_seconds()
    with _UNAVAILABLE_LOCK:
        _UNAVAILABLE_UNTIL_MONO = max(_UNAVAILABLE_UNTIL_MONO, now + cooldown)
        _UNAVAILABLE_REASON = reason[:500]


def _clear_screenalytics_unavailable() -> None:
    global _UNAVAILABLE_UNTIL_MONO, _UNAVAILABLE_REASON
    with _UNAVAILABLE_LOCK:
        _UNAVAILABLE_UNTIL_MONO = 0.0
        _UNAVAILABLE_REASON = None


def get_screenalytics_unavailable_state() -> tuple[bool, int, str | None]:
    now = time.monotonic()
    with _UNAVAILABLE_LOCK:
        if _UNAVAILABLE_UNTIL_MONO <= now:
            return (False, 0, None)
        retry_after_s = int(max(1, round(_UNAVAILABLE_UNTIL_MONO - now)))
        return (True, retry_after_s, _UNAVAILABLE_REASON)


class ScreenalyticsClientError(RuntimeError):
    """Raised when Screenalytics requests fail."""


class ScreenalyticsUnavailableError(ScreenalyticsClientError):
    """Raised when Screenalytics is temporarily unavailable."""

    def __init__(self, message: str, *, retry_after_s: int = 0):
        super().__init__(message)
        self.retry_after_s = max(0, int(retry_after_s))


@dataclass
class FaceBbox:
    """A single detection bounding box (normalized 0-1)."""

    x1: float
    y1: float
    x2: float
    y2: float
    confidence: float
    kind: str = "face"
    person_id: str | None = None
    person_name: str | None = None
    label: str | None = None
    match_similarity: float | None = None
    match_status: str | None = None
    match_reason: str | None = None
    match_candidates: list[dict[str, object]] | None = None
    square_crop_bbox: list[float] | None = None


@dataclass
class PeopleCountResult:
    people_count: int
    face_count: int
    detector: str
    model: str | None = None
    detections: list[FaceBbox] = field(default_factory=list)
    reference_profile: dict[str, object] | None = None
    face_count_raw: int | None = None
    face_count_filtered: int | None = None


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _center_xy(det: FaceBbox) -> tuple[float, float]:
    return ((det.x1 + det.x2) / 2.0, (det.y1 + det.y2) / 2.0)


def _size_wh(det: FaceBbox) -> tuple[float, float]:
    return (max(0.0, det.x2 - det.x1), max(0.0, det.y2 - det.y1))


def _intersects(a: FaceBbox, b: FaceBbox) -> bool:
    return not (a.x2 <= b.x1 or b.x2 <= a.x1 or a.y2 <= b.y1 or b.y2 <= a.y1)


def _contains(container: FaceBbox, inner: FaceBbox) -> bool:
    return (
        container.x1 <= inner.x1 and container.y1 <= inner.y1 and container.x2 >= inner.x2 and container.y2 >= inner.y2
    )


def _face_priority(det: object) -> tuple[int, float, float]:
    """Priority key for selecting the best face detection.

    Prefers matched faces (by similarity) over unmatched faces, falling back to
    detection confidence when no match information is available.
    """
    status = str(getattr(det, "match_status", "") or "").lower()
    similarity = float(getattr(det, "match_similarity", 0.0) or 0.0)
    confidence = float(getattr(det, "confidence", 0.0) or 0.0)
    is_matched = 1 if status == "matched" else 0
    return (is_matched, similarity, confidence)


def _pick_best_person_for_face(face: FaceBbox, people: list[FaceBbox]) -> FaceBbox | None:
    if not people:
        return None

    matching = [p for p in people if _contains(p, face) or _intersects(p, face)]
    if matching:
        return max(
            matching,
            key=lambda p: (
                p.confidence,
                (p.x2 - p.x1) * (p.y2 - p.y1),
            ),
        )
    return max(
        people,
        key=lambda p: (
            p.confidence,
            (p.x2 - p.x1) * (p.y2 - p.y1),
        ),
    )


def _face_torso_focus(
    *,
    face: FaceBbox | None,
    person: FaceBbox | None,
) -> tuple[float, float, float]:
    """Return (focus_x, focus_y, target_visible_vertical_span) in normalized 0-1 space."""
    if face and person:
        face_cx, face_cy = _center_xy(face)
        person_cx, _ = _center_xy(person)
        _, face_h = _size_wh(face)
        _, person_h = _size_wh(person)
        person_anchor_y_ratio = _env_float("THUMBNAIL_FACE_TORSO_PERSON_ANCHOR_Y_RATIO", 0.44)
        face_weight_x = _env_float("THUMBNAIL_FACE_TORSO_FACE_WEIGHT_X", 0.55)
        face_weight_y = _env_float("THUMBNAIL_FACE_TORSO_FACE_WEIGHT_Y", 0.35)
        face_span_multiplier = _env_float("THUMBNAIL_FACE_TORSO_FACE_SPAN_MULTIPLIER", 3.3)
        person_span_multiplier = _env_float("THUMBNAIL_FACE_TORSO_PERSON_SPAN_MULTIPLIER", 0.88)
        min_span = _env_float("THUMBNAIL_FACE_TORSO_MIN_SPAN", 0.36)
        max_span = _env_float("THUMBNAIL_FACE_TORSO_MAX_SPAN", 0.80)

        person_anchor_y = person.y1 + (person_anchor_y_ratio * person_h)
        person_weight_x = max(0.0, 1.0 - face_weight_x)
        person_weight_y = max(0.0, 1.0 - face_weight_y)
        x = (face_weight_x * face_cx) + (person_weight_x * person_cx)
        y = (face_weight_y * face_cy) + (person_weight_y * person_anchor_y)
        target_span = _clamp(max(face_h * face_span_multiplier, person_h * person_span_multiplier), min_span, max_span)
        return (x, y, target_span)

    if face:
        face_cx, face_cy = _center_xy(face)
        _, face_h = _size_wh(face)
        x = face_cx
        y = face_cy + (_env_float("THUMBNAIL_FACE_ONLY_Y_SHIFT_MULTIPLIER", 0.22) * face_h)
        target_span = _clamp(
            face_h * _env_float("THUMBNAIL_FACE_ONLY_SPAN_MULTIPLIER", 3.7),
            _env_float("THUMBNAIL_FACE_ONLY_MIN_SPAN", 0.36),
            _env_float("THUMBNAIL_FACE_ONLY_MAX_SPAN", 0.78),
        )
        return (x, y, target_span)

    if person:
        person_cx, _ = _center_xy(person)
        _, person_h = _size_wh(person)
        x = person_cx
        y = person.y1 + (_env_float("THUMBNAIL_PERSON_ONLY_ANCHOR_Y_RATIO", 0.43) * person_h)
        target_span = _clamp(
            person_h * _env_float("THUMBNAIL_PERSON_ONLY_SPAN_MULTIPLIER", 0.90),
            _env_float("THUMBNAIL_PERSON_ONLY_MIN_SPAN", 0.42),
            _env_float("THUMBNAIL_PERSON_ONLY_MAX_SPAN", 0.84),
        )
        return (x, y, target_span)

    return (0.5, 0.36, 0.74)


def auto_thumbnail_crop(
    result: PeopleCountResult,
    *,
    strategy: str = "face_torso_v2",
) -> dict[str, float | str] | None:
    """Compute deterministic auto crop from available face/person detections."""
    detections = getattr(result, "detections", None) or []
    if not detections:
        return None

    faces = [det for det in detections if str(getattr(det, "kind", "face")).lower() == "face"]
    people = [det for det in detections if str(getattr(det, "kind", "")).lower() == "person"]

    best_face = max(faces, key=_face_priority) if faces else None
    best_person = _pick_best_person_for_face(best_face, people) if best_face else None
    if not best_person and people:
        best_person = max(
            people,
            key=lambda d: (
                d.confidence,
                (d.x2 - d.x1) * (d.y2 - d.y1),
            ),
        )

    focus_x, focus_y, target_span = _face_torso_focus(face=best_face, person=best_person)
    # At zoom=1, a 3:4 thumbnail typically shows ~75% of source image height.
    base_visible_vertical_span = _env_float("THUMBNAIL_BASE_VISIBLE_VERTICAL_SPAN", 0.75)
    zoom = base_visible_vertical_span / max(target_span, 0.01)
    zoom = _clamp(
        zoom,
        _env_float("THUMBNAIL_AUTO_ZOOM_MIN", 1.08),
        _env_float("THUMBNAIL_AUTO_ZOOM_MAX", 2.1),
    )

    return {
        "x": round(_clamp(focus_x, 0.0, 1.0) * 100.0, 1),
        "y": round(_clamp(focus_y, 0.0, 1.0) * 100.0, 1),
        "zoom": round(zoom, 2),
        "mode": "auto",
        "strategy": strategy,
    }


def face_centroid(result: PeopleCountResult) -> tuple[float, float] | None:
    """Return (x%, y%) centroid of the primary (highest-confidence) face, or None.

    Values are in the 0-100 range suitable for CSS object-position percentages.
    """
    detections = getattr(result, "detections", None)
    if not detections:
        return None
    face_detections = [d for d in detections if str(getattr(d, "kind", "face")).lower() == "face"]
    if not face_detections:
        return None
    best = max(face_detections, key=_face_priority)
    cx = ((best.x1 + best.x2) / 2) * 100
    cy = ((best.y1 + best.y2) / 2) * 100
    return (round(cx, 1), round(cy, 1))


def _base_url() -> str:
    return os.getenv("SCREENALYTICS_API_URL", "").strip().rstrip("/")


def _endpoint_candidates() -> list[str]:
    configured = os.getenv("SCREENALYTICS_API_PATH", "").strip()
    if configured:
        paths = [p.strip() for p in configured.split(",") if p.strip()]
        return paths
    return ["/vision/people-count", "/api/v1/vision/people-count", "/people-count"]


def _batch_endpoint_candidates() -> list[str]:
    configured = os.getenv("SCREENALYTICS_BATCH_API_PATH", "").strip()
    if configured:
        paths = [p.strip() for p in configured.split(",") if p.strip()]
        return paths
    return ["/vision/people-count/batch", "/api/v1/vision/people-count/batch"]


def is_screenalytics_configured() -> bool:
    return bool(os.getenv("SCREENALYTICS_API_URL", "").strip())


def _is_http_url(value: object) -> bool:
    return isinstance(value, str) and value.strip().lower().startswith(("http://", "https://"))


def _normalize_reference_urls(raw_entry: dict[str, object]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    raw_candidates = raw_entry.get("url_candidates")
    if isinstance(raw_candidates, list):
        for value in raw_candidates:
            if not isinstance(value, str):
                continue
            candidate = value.strip()
            canonical = candidate.lower()
            if not candidate or not _is_http_url(candidate) or canonical in seen:
                continue
            seen.add(canonical)
            out.append(candidate)
    for value in (raw_entry.get("source_url"), raw_entry.get("hosted_url"), raw_entry.get("url")):
        if not isinstance(value, str):
            continue
        candidate = value.strip()
        canonical = candidate.lower()
        if not candidate or not _is_http_url(candidate) or canonical in seen:
            continue
        seen.add(canonical)
        out.append(candidate)
    return out


def _normalize_reference_entry(raw_entry: dict[str, object]) -> dict[str, object] | None:
    urls = _normalize_reference_urls(raw_entry)
    if not urls:
        return None
    normalized_entry: dict[str, object] = {
        "url": urls[0],
        "url_candidates": urls,
    }
    media_asset_id = raw_entry.get("media_asset_id")
    if isinstance(media_asset_id, str) and media_asset_id.strip():
        normalized_entry["media_asset_id"] = media_asset_id.strip()
    link_id = raw_entry.get("link_id")
    if isinstance(link_id, str) and link_id.strip():
        normalized_entry["link_id"] = link_id.strip()
    rank = raw_entry.get("rank")
    if isinstance(rank, int):
        normalized_entry["rank"] = max(1, rank)
    reasons = raw_entry.get("reasons")
    if isinstance(reasons, list):
        normalized_reasons = [str(value).strip() for value in reasons if isinstance(value, str) and str(value).strip()]
        if normalized_reasons:
            normalized_entry["reasons"] = normalized_reasons
    source_url = raw_entry.get("source_url")
    if isinstance(source_url, str) and _is_http_url(source_url):
        normalized_entry["source_url"] = source_url.strip()
    hosted_url = raw_entry.get("hosted_url")
    if isinstance(hosted_url, str) and _is_http_url(hosted_url):
        normalized_entry["hosted_url"] = hosted_url.strip()
    return normalized_entry


def _normalize_owner_reference_images(
    owner_reference_images: list[dict[str, object]] | None,
) -> list[dict[str, object]]:
    normalized_reference_images: list[dict[str, object]] = []
    if not isinstance(owner_reference_images, list):
        return normalized_reference_images
    seen_signatures: set[str] = set()
    for raw_entry in owner_reference_images:
        if not isinstance(raw_entry, dict):
            continue
        normalized_entry = _normalize_reference_entry(raw_entry)
        if normalized_entry is None:
            continue
        signature = "|".join(str(url).strip().lower() for url in (normalized_entry.get("url_candidates") or []))
        if not signature or signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        normalized_reference_images.append(normalized_entry)
    return normalized_reference_images


def _normalize_person_reference_images(
    person_reference_images: list[dict[str, object]] | None,
) -> list[dict[str, object]]:
    normalized_person_reference_images: list[dict[str, object]] = []
    if not isinstance(person_reference_images, list):
        return normalized_person_reference_images
    seen_people: set[str] = set()
    for pool in person_reference_images:
        if not isinstance(pool, dict):
            continue
        person_id = pool.get("person_id")
        if not isinstance(person_id, str):
            continue
        normalized_person_id = person_id.strip()
        if not normalized_person_id or normalized_person_id in seen_people:
            continue
        references_raw = pool.get("references")
        if not isinstance(references_raw, list):
            continue
        seen_signatures: set[str] = set()
        normalized_refs: list[dict[str, object]] = []
        for raw_entry in references_raw:
            if not isinstance(raw_entry, dict):
                continue
            normalized_entry = _normalize_reference_entry(raw_entry)
            if normalized_entry is None:
                continue
            signature = "|".join(str(url).strip().lower() for url in (normalized_entry.get("url_candidates") or []))
            if not signature or signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            normalized_refs.append(normalized_entry)
        if not normalized_refs:
            continue
        normalized_pool: dict[str, object] = {"person_id": normalized_person_id, "references": normalized_refs}
        person_name = pool.get("person_name")
        if isinstance(person_name, str) and person_name.strip():
            normalized_pool["person_name"] = person_name.strip()
        normalized_person_reference_images.append(normalized_pool)
        seen_people.add(normalized_person_id)
    return normalized_person_reference_images


def _normalize_candidates(candidate_person_ids: list[str] | None) -> list[str]:
    normalized_candidates: list[str] = []
    if not isinstance(candidate_person_ids, list):
        return normalized_candidates
    seen_candidates: set[str] = set()
    for entry in candidate_person_ids:
        if not isinstance(entry, str):
            continue
        normalized = entry.strip()
        if not normalized or normalized in seen_candidates:
            continue
        seen_candidates.add(normalized)
        normalized_candidates.append(normalized)
    return normalized_candidates


def _build_people_count_payload(
    *,
    image_url: str,
    mode: DetectorMode,
    candidate_person_ids: list[str] | None,
    owner_person_id: str | None,
    owner_reference_images: list[dict[str, object]] | None,
    person_reference_images: list[dict[str, object]] | None,
    prefer_fast_pass: bool | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {"image_url": image_url, "mode": mode}
    normalized_candidates = _normalize_candidates(candidate_person_ids)
    if normalized_candidates:
        payload["candidate_person_ids"] = normalized_candidates
    normalized_owner_person_id = (
        str(owner_person_id).strip() if isinstance(owner_person_id, str) and owner_person_id.strip() else None
    )
    if normalized_owner_person_id:
        payload["owner_person_id"] = normalized_owner_person_id
    normalized_reference_images = _normalize_owner_reference_images(owner_reference_images)
    if normalized_reference_images:
        payload["owner_reference_images"] = normalized_reference_images
    normalized_person_reference_images = _normalize_person_reference_images(person_reference_images)
    if normalized_person_reference_images:
        payload["person_reference_images"] = normalized_person_reference_images
    if isinstance(prefer_fast_pass, bool):
        payload["prefer_fast_pass"] = prefer_fast_pass
    return payload


def _parse_people_count_data(data: dict[str, Any]) -> PeopleCountResult:
    people_count = data.get("people_count")
    face_count = data.get("face_count", 0)
    face_count_raw = data.get("face_count_raw")
    face_count_filtered = data.get("face_count_filtered")
    detector = data.get("detector", "unknown")
    model = data.get("model")

    if not isinstance(people_count, int):
        raise ScreenalyticsClientError("people_count missing from response")

    if not isinstance(face_count, int):
        face_count = 0
    if not isinstance(face_count_raw, int):
        face_count_raw = None
    if not isinstance(face_count_filtered, int):
        face_count_filtered = None

    if not isinstance(detector, str):
        detector = "unknown"

    detections: list[FaceBbox] = []
    raw_detections = data.get("detections")
    if isinstance(raw_detections, list):
        for det in raw_detections:
            if not isinstance(det, dict):
                continue
            bbox = det.get("bbox")
            conf = det.get("confidence", 0.0)
            kind = det.get("kind")
            person_id = det.get("person_id")
            person_name = det.get("person_name")
            label = det.get("label")
            match_similarity = det.get("match_similarity")
            match_status = det.get("match_status")
            match_reason = det.get("match_reason")
            match_candidates_raw = det.get("match_candidates")
            square_crop_bbox = det.get("square_crop_bbox")
            match_candidates: list[dict[str, object]] | None = None
            if isinstance(match_candidates_raw, list):
                normalized_candidates: list[dict[str, object]] = []
                for candidate in match_candidates_raw:
                    if not isinstance(candidate, dict):
                        continue
                    person_id_candidate = candidate.get("person_id")
                    person_name_candidate = candidate.get("person_name")
                    similarity_candidate = candidate.get("similarity")
                    if not isinstance(similarity_candidate, (int, float)):
                        continue
                    normalized_candidate: dict[str, object] = {
                        "similarity": round(max(0.0, min(1.0, float(similarity_candidate))), 4)
                    }
                    if isinstance(person_id_candidate, str) and person_id_candidate.strip():
                        normalized_candidate["person_id"] = person_id_candidate.strip()
                    if isinstance(person_name_candidate, str) and person_name_candidate.strip():
                        normalized_candidate["person_name"] = person_name_candidate.strip()
                    normalized_candidates.append(normalized_candidate)
                if normalized_candidates:
                    match_candidates = normalized_candidates
            if isinstance(bbox, list) and len(bbox) >= 4:
                try:
                    detections.append(
                        FaceBbox(
                            x1=float(bbox[0]),
                            y1=float(bbox[1]),
                            x2=float(bbox[2]),
                            y2=float(bbox[3]),
                            confidence=float(conf) if isinstance(conf, (int, float)) else 0.0,
                            kind=str(kind).lower() if isinstance(kind, str) else "face",
                            person_id=(
                                str(person_id).strip() if isinstance(person_id, str) and person_id.strip() else None
                            ),
                            person_name=(
                                str(person_name).strip()
                                if isinstance(person_name, str) and person_name.strip()
                                else None
                            ),
                            label=str(label).strip() if isinstance(label, str) and label.strip() else None,
                            match_similarity=(
                                float(match_similarity) if isinstance(match_similarity, (int, float)) else None
                            ),
                            match_status=(
                                str(match_status).strip()
                                if isinstance(match_status, str) and match_status.strip()
                                else None
                            ),
                            match_reason=(
                                str(match_reason).strip()
                                if isinstance(match_reason, str) and match_reason.strip()
                                else None
                            ),
                            match_candidates=match_candidates,
                            square_crop_bbox=(
                                [
                                    float(square_crop_bbox[0]),
                                    float(square_crop_bbox[1]),
                                    float(square_crop_bbox[2]),
                                    float(square_crop_bbox[3]),
                                ]
                                if isinstance(square_crop_bbox, list) and len(square_crop_bbox) >= 4
                                else None
                            ),
                        )
                    )
                except (ValueError, TypeError):
                    continue

    reference_profile_raw = data.get("reference_profile")
    reference_profile = reference_profile_raw if isinstance(reference_profile_raw, dict) else None

    return PeopleCountResult(
        people_count=people_count,
        face_count=face_count,
        detector=detector,
        model=model if isinstance(model, str) else None,
        detections=detections,
        reference_profile=reference_profile,
        face_count_raw=face_count_raw,
        face_count_filtered=face_count_filtered,
    )


def count_people(
    image_url: str,
    *,
    mode: DetectorMode = "faces_then_yolo",
    candidate_person_ids: list[str] | None = None,
    owner_person_id: str | None = None,
    owner_reference_images: list[dict[str, object]] | None = None,
    person_reference_images: list[dict[str, object]] | None = None,
    prefer_fast_pass: bool | None = None,
) -> PeopleCountResult:
    if not image_url:
        raise ScreenalyticsClientError("image_url is required")

    base = _base_url()
    if not base:
        raise ScreenalyticsClientError("SCREENALYTICS_API_URL is not configured")
    unavailable, retry_after_s, unavailable_reason = get_screenalytics_unavailable_state()
    if unavailable:
        reason_suffix = f": {unavailable_reason}" if unavailable_reason else ""
        raise ScreenalyticsUnavailableError(
            f"Screenalytics temporarily unavailable{reason_suffix}",
            retry_after_s=retry_after_s,
        )
    payload = _build_people_count_payload(
        image_url=image_url,
        mode=mode,
        candidate_person_ids=candidate_person_ids,
        owner_person_id=owner_person_id,
        owner_reference_images=owner_reference_images,
        person_reference_images=person_reference_images,
        prefer_fast_pass=prefer_fast_pass,
    )
    last_error: str | None = None

    response: requests.Response | None = None
    tried_urls: list[str] = []
    for path in _endpoint_candidates():
        url = f"{base}{path}"
        tried_urls.append(url)
        try:
            response = requests.post(url, json=payload, timeout=(3.05, 20))
        except requests.RequestException as exc:
            last_error = f"Screenalytics request failed: {exc}"
            continue

        if response.status_code == 404:
            last_error = f"Screenalytics error {response.status_code}: {response.text.strip()[:200] or 'unknown error'}"
            continue

        if response.status_code >= 400:
            detail = response.text.strip()[:200]
            if response.status_code >= 500:
                reason = f"Screenalytics error {response.status_code}: {detail or 'unknown error'}"
                _mark_screenalytics_unavailable(reason)
                unavailable, retry_after_s, _ = get_screenalytics_unavailable_state()
                raise ScreenalyticsUnavailableError(
                    reason,
                    retry_after_s=retry_after_s if unavailable else _unavailable_cooldown_seconds(),
                )
            raise ScreenalyticsClientError(f"Screenalytics error {response.status_code}: {detail or 'unknown error'}")
        break
    else:
        _mark_screenalytics_unavailable(last_error or "Screenalytics request failed")
        unavailable, retry_after_s, unavailable_reason = get_screenalytics_unavailable_state()
        logger.error(
            "Screenalytics people-count endpoint not found. Tried: %s",
            ", ".join(tried_urls),
        )
        raise ScreenalyticsUnavailableError(
            unavailable_reason or last_error or "Screenalytics request failed",
            retry_after_s=retry_after_s if unavailable else _unavailable_cooldown_seconds(),
        )

    if response is None:
        _mark_screenalytics_unavailable(last_error or "Screenalytics request failed")
        unavailable, retry_after_s, unavailable_reason = get_screenalytics_unavailable_state()
        raise ScreenalyticsUnavailableError(
            unavailable_reason or last_error or "Screenalytics request failed",
            retry_after_s=retry_after_s if unavailable else _unavailable_cooldown_seconds(),
        )

    _clear_screenalytics_unavailable()
    logger.info("Screenalytics people-count endpoint used: %s", response.url)

    try:
        data = response.json()
    except ValueError as exc:
        raise ScreenalyticsClientError("Screenalytics returned invalid JSON") from exc

    if not isinstance(data, dict):
        raise ScreenalyticsClientError("Screenalytics response was not an object")
    return _parse_people_count_data(data)


def count_people_batch(
    image_requests: list[dict[str, object]],
    *,
    mode: DetectorMode = "faces_then_yolo",
    prefer_fast_pass: bool | None = None,
) -> list[PeopleCountResult | None]:
    if not isinstance(image_requests, list) or not image_requests:
        return []
    base = _base_url()
    if not base:
        raise ScreenalyticsClientError("SCREENALYTICS_API_URL is not configured")
    unavailable, retry_after_s, unavailable_reason = get_screenalytics_unavailable_state()
    if unavailable:
        reason_suffix = f": {unavailable_reason}" if unavailable_reason else ""
        raise ScreenalyticsUnavailableError(
            f"Screenalytics temporarily unavailable{reason_suffix}",
            retry_after_s=retry_after_s,
        )

    normalized_images: list[dict[str, object]] = []
    for entry in image_requests:
        if not isinstance(entry, dict):
            normalized_images.append({})
            continue
        image_url = str(entry.get("image_url") or "").strip()
        if not image_url:
            normalized_images.append({})
            continue
        item_prefer_fast_pass = (
            entry.get("prefer_fast_pass") if isinstance(entry.get("prefer_fast_pass"), bool) else prefer_fast_pass
        )
        candidate_person_ids = entry.get("candidate_person_ids")
        owner_person_id = entry.get("owner_person_id")
        owner_reference_images = entry.get("owner_reference_images")
        person_reference_images = entry.get("person_reference_images")
        payload_item = _build_people_count_payload(
            image_url=image_url,
            mode=str(entry.get("mode") or mode),  # type: ignore[arg-type]
            candidate_person_ids=candidate_person_ids if isinstance(candidate_person_ids, list) else None,  # type: ignore[arg-type]
            owner_person_id=owner_person_id if isinstance(owner_person_id, str) else None,  # type: ignore[arg-type]
            owner_reference_images=(owner_reference_images if isinstance(owner_reference_images, list) else None),
            person_reference_images=(person_reference_images if isinstance(person_reference_images, list) else None),
            prefer_fast_pass=item_prefer_fast_pass,
        )
        normalized_images.append(payload_item)

    if not any(item.get("image_url") for item in normalized_images):
        return [None for _ in image_requests]

    payload = {
        "images": normalized_images,
        **({"prefer_fast_pass": prefer_fast_pass} if isinstance(prefer_fast_pass, bool) else {}),
    }
    response: requests.Response | None = None
    tried_urls: list[str] = []

    for path in _batch_endpoint_candidates():
        url = f"{base}{path}"
        tried_urls.append(url)
        try:
            response = requests.post(url, json=payload, timeout=(3.05, 45))
        except requests.RequestException as exc:
            logger.debug("Screenalytics batch request failed at %s: %s", url, exc)
            continue
        if response.status_code == 404:
            response = None
            continue
        if response.status_code >= 400:
            detail = response.text.strip()[:200]
            if response.status_code >= 500:
                reason = f"Screenalytics batch error {response.status_code}: {detail or 'unknown error'}"
                _mark_screenalytics_unavailable(reason)
                unavailable, retry_after_s, _ = get_screenalytics_unavailable_state()
                raise ScreenalyticsUnavailableError(
                    reason,
                    retry_after_s=retry_after_s if unavailable else _unavailable_cooldown_seconds(),
                )
            raise ScreenalyticsClientError(
                f"Screenalytics batch error {response.status_code}: {detail or 'unknown error'}"
            )
        break

    if response is None:
        # Batch endpoint unavailable: fallback to single endpoint for compatibility.
        results: list[PeopleCountResult | None] = []
        for entry in image_requests:
            if not isinstance(entry, dict):
                results.append(None)
                continue
            image_url = str(entry.get("image_url") or "").strip()
            if not image_url:
                results.append(None)
                continue
            try:
                item_prefer_fast_pass = (
                    entry.get("prefer_fast_pass")
                    if isinstance(entry.get("prefer_fast_pass"), bool)
                    else prefer_fast_pass
                )
                results.append(
                    count_people(
                        image_url,
                        mode=str(entry.get("mode") or mode),  # type: ignore[arg-type]
                        candidate_person_ids=(
                            entry.get("candidate_person_ids")
                            if isinstance(entry.get("candidate_person_ids"), list)
                            else None
                        ),
                        owner_person_id=(
                            entry.get("owner_person_id") if isinstance(entry.get("owner_person_id"), str) else None
                        ),
                        owner_reference_images=(
                            entry.get("owner_reference_images")
                            if isinstance(entry.get("owner_reference_images"), list)
                            else None
                        ),
                        person_reference_images=(
                            entry.get("person_reference_images")
                            if isinstance(entry.get("person_reference_images"), list)
                            else None
                        ),
                        prefer_fast_pass=item_prefer_fast_pass,
                    )
                )
            except ScreenalyticsClientError:
                results.append(None)
        return results

    _clear_screenalytics_unavailable()
    logger.info("Screenalytics people-count batch endpoint used: %s", response.url)
    try:
        data = response.json()
    except ValueError as exc:
        raise ScreenalyticsClientError("Screenalytics returned invalid JSON for batch response") from exc
    if not isinstance(data, dict):
        raise ScreenalyticsClientError("Screenalytics batch response was not an object")
    raw_results = data.get("results")
    if not isinstance(raw_results, list):
        raise ScreenalyticsClientError("Screenalytics batch response missing results")

    parsed_results: list[PeopleCountResult | None] = []
    for item in raw_results:
        if not isinstance(item, dict):
            parsed_results.append(None)
            continue
        result_payload = item.get("result") if isinstance(item.get("result"), dict) else item
        if isinstance(item.get("error"), str) and item.get("error"):
            parsed_results.append(None)
            continue
        if not isinstance(result_payload, dict):
            parsed_results.append(None)
            continue
        try:
            parsed_results.append(_parse_people_count_data(result_payload))
        except ScreenalyticsClientError:
            parsed_results.append(None)

    if len(parsed_results) < len(image_requests):
        parsed_results.extend([None] * (len(image_requests) - len(parsed_results)))
    elif len(parsed_results) > len(image_requests):
        parsed_results = parsed_results[: len(image_requests)]

    return parsed_results
