"""Compatibility shim for legacy screenalytics imports."""

from __future__ import annotations

from trr_backend.vision.people_count_service import (
    DetectorMode,
    FaceBbox,
    PeopleCountResult,
    PeopleCountServiceError,
    PeopleCountServiceUnavailableError,
    _invoke_people_count_batch_local,
    _invoke_people_count_local,
    _invoke_people_count_modal,
    auto_thumbnail_crop,
    count_people,
    count_people_batch,
    face_centroid,
    get_unavailable_state,
    is_runtime_configured,
)

ScreenalyticsClientError = PeopleCountServiceError
ScreenalyticsUnavailableError = PeopleCountServiceUnavailableError


def get_screenalytics_unavailable_state() -> tuple[bool, int, str | None]:
    return get_unavailable_state()


def is_screenalytics_configured() -> bool:
    return is_runtime_configured()


__all__ = [
    "DetectorMode",
    "FaceBbox",
    "PeopleCountResult",
    "PeopleCountServiceError",
    "PeopleCountServiceUnavailableError",
    "ScreenalyticsClientError",
    "ScreenalyticsUnavailableError",
    "_invoke_people_count_batch_local",
    "_invoke_people_count_local",
    "_invoke_people_count_modal",
    "auto_thumbnail_crop",
    "count_people",
    "count_people_batch",
    "face_centroid",
    "get_screenalytics_unavailable_state",
    "get_unavailable_state",
    "is_runtime_configured",
    "is_screenalytics_configured",
]
