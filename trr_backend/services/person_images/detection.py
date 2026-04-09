"""Backend-owned detection and auto-crop adapter for person image workflows."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from trr_backend.clients import screenalytics

ScreenalyticsClientError = screenalytics.ScreenalyticsClientError
ScreenalyticsUnavailableError = screenalytics.ScreenalyticsUnavailableError

__all__ = [
    "ScreenalyticsClientError",
    "ScreenalyticsUnavailableError",
    "build_auto_thumbnail_crop_payload",
    "count_people_batch_with_fallback",
    "count_people_with_fallback",
    "get_unavailable_state",
    "is_runtime_configured",
]


def is_runtime_configured() -> bool:
    return screenalytics.is_screenalytics_configured()


def get_unavailable_state() -> tuple[bool, int, str | None]:
    return screenalytics.get_screenalytics_unavailable_state()


def build_auto_thumbnail_crop_payload(
    result: Any,
    *,
    fallback_strategy: str = "face_centroid_v1",
) -> dict[str, Any] | None:
    generated = screenalytics.auto_thumbnail_crop(result)
    if generated is not None:
        return {
            **generated,
            "generated_at": datetime.now(UTC).isoformat(),
        }
    centroid = screenalytics.face_centroid(result)
    if centroid is None:
        return None
    cx, cy = centroid
    return {
        "x": cx,
        "y": cy,
        "zoom": 1,
        "mode": "auto",
        "strategy": fallback_strategy,
        "generated_at": datetime.now(UTC).isoformat(),
    }


def count_people_with_fallback(
    image_url: str,
    *,
    candidate_person_ids: list[str] | None = None,
    owner_person_id: str | None = None,
    owner_reference_images: list[dict[str, object]] | None = None,
    person_reference_images: list[dict[str, object]] | None = None,
    prefer_fast_pass: bool = True,
) -> Any:
    if candidate_person_ids:
        try:
            return screenalytics.count_people(
                image_url,
                candidate_person_ids=candidate_person_ids,
                owner_person_id=owner_person_id,
                owner_reference_images=owner_reference_images,
                person_reference_images=person_reference_images,
                prefer_fast_pass=bool(prefer_fast_pass),
            )
        except TypeError:
            try:
                return screenalytics.count_people(image_url, candidate_person_ids=candidate_person_ids)
            except TypeError:
                return screenalytics.count_people(image_url)

    try:
        return screenalytics.count_people(
            image_url,
            owner_person_id=owner_person_id,
            owner_reference_images=owner_reference_images,
            person_reference_images=person_reference_images,
            prefer_fast_pass=bool(prefer_fast_pass),
        )
    except TypeError:
        return screenalytics.count_people(image_url)


def count_people_batch_with_fallback(
    image_requests: list[dict[str, object]],
    *,
    prefer_fast_pass: bool = True,
) -> list[Any]:
    try:
        return screenalytics.count_people_batch(image_requests, prefer_fast_pass=bool(prefer_fast_pass))
    except TypeError:
        return screenalytics.count_people_batch(image_requests)
