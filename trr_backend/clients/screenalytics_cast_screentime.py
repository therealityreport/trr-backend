"""Internal Screenalytics client for cast screentime dispatch."""

from __future__ import annotations

import os
from typing import Any

import requests


class ScreenalyticsCastScreentimeClientError(RuntimeError):
    """Raised when Screenalytics dispatch fails."""


def _base_url() -> str:
    raw = (os.getenv("SCREENALYTICS_API_URL") or "").strip().rstrip("/")
    if not raw:
        raise ScreenalyticsCastScreentimeClientError("SCREENALYTICS_API_URL is not configured")
    return raw


def _service_token() -> str:
    token = (os.getenv("SCREENALYTICS_SERVICE_TOKEN") or "").strip()
    if not token:
        raise ScreenalyticsCastScreentimeClientError("SCREENALYTICS_SERVICE_TOKEN is not configured")
    return token


def start_run(run_id: str) -> dict[str, Any]:
    return _post(
        f"/internal/cast-screentime/runs/{run_id}:start",
        {"run_id": run_id},
    )


def generate_segment_clip(
    run_id: str,
    *,
    segment_key: str,
    mode: str,
    duration_seconds: int | None = None,
    ttl_days: int = 7,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "run_id": run_id,
        "segment_key": segment_key,
        "mode": mode,
        "ttl_days": ttl_days,
    }
    if duration_seconds is not None:
        payload["duration_seconds"] = duration_seconds
    return _post(
        f"/internal/cast-screentime/runs/{run_id}:generate-clip",
        payload,
    )


def _post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(
        f"{_base_url()}{path}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {_service_token()}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=(5, 30),
    )
    if response.status_code >= 400:
        detail = response.text.strip() or response.reason
        raise ScreenalyticsCastScreentimeClientError(
            f"Screenalytics dispatch failed with {response.status_code}: {detail}"
        )
    payload = response.json()
    if not isinstance(payload, dict):
        raise ScreenalyticsCastScreentimeClientError("Screenalytics dispatch returned a non-object payload")
    return payload
