"""Screenalytics client for people-count estimation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

import requests

DetectorMode = Literal["faces_then_yolo", "faces", "yolo"]


class ScreenalyticsClientError(RuntimeError):
    """Raised when Screenalytics requests fail."""


@dataclass
class PeopleCountResult:
    people_count: int
    face_count: int
    detector: str
    model: str | None = None


def _base_url() -> str:
    return os.getenv("SCREENALYTICS_API_URL", "http://127.0.0.1:8000").rstrip("/")


def count_people(image_url: str, *, mode: DetectorMode = "faces_then_yolo") -> PeopleCountResult:
    if not image_url:
        raise ScreenalyticsClientError("image_url is required")

    url = f"{_base_url()}/vision/people-count"
    payload = {"image_url": image_url, "mode": mode}

    try:
        response = requests.post(url, json=payload, timeout=(3.05, 20))
    except requests.RequestException as exc:
        raise ScreenalyticsClientError(f"Screenalytics request failed: {exc}") from exc

    if response.status_code >= 400:
        detail = response.text.strip()[:200]
        raise ScreenalyticsClientError(
            f"Screenalytics error {response.status_code}: {detail or 'unknown error'}"
        )

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

    return PeopleCountResult(
        people_count=people_count,
        face_count=face_count,
        detector=detector,
        model=model if isinstance(model, str) else None,
    )
