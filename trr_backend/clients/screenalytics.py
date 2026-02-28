"""Screenalytics client for people-count estimation."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Literal

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
    square_crop_bbox: list[float] | None = None


@dataclass
class PeopleCountResult:
    people_count: int
    face_count: int
    detector: str
    model: str | None = None
    detections: list[FaceBbox] = field(default_factory=list)


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


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
        person_anchor_y = person.y1 + (0.38 * person_h)
        x = (0.75 * face_cx) + (0.25 * person_cx)
        y = (0.65 * face_cy) + (0.35 * person_anchor_y)
        target_span = _clamp(max(face_h * 3.2, person_h * 0.78), 0.45, 0.86)
        return (x, y, target_span)

    if face:
        face_cx, face_cy = _center_xy(face)
        _, face_h = _size_wh(face)
        x = face_cx
        y = face_cy + (0.18 * face_h)
        target_span = _clamp(face_h * 3.5, 0.45, 0.84)
        return (x, y, target_span)

    if person:
        person_cx, _ = _center_xy(person)
        _, person_h = _size_wh(person)
        x = person_cx
        y = person.y1 + (0.35 * person_h)
        target_span = _clamp(person_h * 0.8, 0.50, 0.88)
        return (x, y, target_span)

    return (0.5, 0.32, 0.80)


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

    best_face = max(faces, key=lambda d: d.confidence) if faces else None
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
    # At zoom=1, a 4:5 thumbnail typically shows ~80% of source image height.
    base_visible_vertical_span = 0.8
    zoom = base_visible_vertical_span / max(target_span, 0.01)
    zoom = _clamp(zoom, 1.0, 1.6)

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
    best = max(face_detections, key=lambda d: d.confidence)
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


def is_screenalytics_configured() -> bool:
    return bool(os.getenv("SCREENALYTICS_API_URL", "").strip())


def count_people(image_url: str, *, mode: DetectorMode = "faces_then_yolo") -> PeopleCountResult:
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
    payload = {"image_url": image_url, "mode": mode}
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

    people_count = data.get("people_count")
    face_count = data.get("face_count", 0)
    detector = data.get("detector", "unknown")
    model = data.get("model")

    if not isinstance(people_count, int):
        raise ScreenalyticsClientError("people_count missing from response")

    if not isinstance(face_count, int):
        face_count = 0

    if not isinstance(detector, str):
        detector = "unknown"

    # Parse detections (face/person) if present.
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
            square_crop_bbox = det.get("square_crop_bbox")
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
                                str(person_id).strip()
                                if isinstance(person_id, str) and person_id.strip()
                                else None
                            ),
                            person_name=(
                                str(person_name).strip()
                                if isinstance(person_name, str) and person_name.strip()
                                else None
                            ),
                            label=str(label).strip() if isinstance(label, str) and label.strip() else None,
                            match_similarity=(
                                float(match_similarity)
                                if isinstance(match_similarity, (int, float))
                                else None
                            ),
                            match_status=(
                                str(match_status).strip()
                                if isinstance(match_status, str) and match_status.strip()
                                else None
                            ),
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

    return PeopleCountResult(
        people_count=people_count,
        face_count=face_count,
        detector=detector,
        model=model if isinstance(model, str) else None,
        detections=detections,
    )
