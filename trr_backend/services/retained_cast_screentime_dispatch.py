"""Transitional dispatch boundary for retained cast-screentime flows."""

from __future__ import annotations

from typing import Any

from trr_backend.clients import screenalytics_cast_screentime

RetainedCastScreentimeDispatchError = screenalytics_cast_screentime.ScreenalyticsCastScreentimeClientError


def start_run(run_id: str) -> dict[str, Any]:
    return screenalytics_cast_screentime.start_run(run_id)


def generate_segment_clip(
    run_id: str,
    *,
    segment_key: str,
    mode: str,
    duration_seconds: int | None = None,
    ttl_days: int = 7,
) -> dict[str, Any]:
    return screenalytics_cast_screentime.generate_segment_clip(
        run_id,
        segment_key=segment_key,
        mode=mode,
        duration_seconds=duration_seconds,
        ttl_days=ttl_days,
    )
