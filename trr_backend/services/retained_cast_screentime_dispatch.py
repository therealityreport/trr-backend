"""Retained dispatch boundary for backend-owned cast-screentime flows."""

from __future__ import annotations

from typing import Any

from trr_backend.services import retained_cast_screentime_runtime


class RetainedCastScreentimeDispatchError(RuntimeError):
    """Raised when retained screentime dispatch cannot start or fulfill work."""


def start_run(run_id: str) -> dict[str, Any]:
    try:
        return retained_cast_screentime_runtime.enqueue_run(run_id)
    except Exception as exc:  # noqa: BLE001
        raise RetainedCastScreentimeDispatchError(str(exc)) from exc


def generate_segment_clip(
    run_id: str,
    *,
    segment_key: str,
    mode: str,
    duration_seconds: int | None = None,
    ttl_days: int = 7,
) -> dict[str, Any]:
    try:
        return retained_cast_screentime_runtime.generate_segment_clip(
            run_id,
            segment_key=segment_key,
            mode=mode,
            duration_seconds=duration_seconds,
            ttl_days=ttl_days,
        )
    except Exception as exc:  # noqa: BLE001
        raise RetainedCastScreentimeDispatchError(str(exc)) from exc
