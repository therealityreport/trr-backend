"""Retained dispatch boundary for backend-owned cast-screentime flows."""

from __future__ import annotations

import os
from typing import Any

from trr_backend import modal_dispatch
from trr_backend.services import retained_cast_screentime_runtime


class RetainedCastScreentimeDispatchError(RuntimeError):
    """Raised when retained screentime dispatch cannot start or fulfill work."""


def start_run(run_id: str) -> dict[str, Any]:
    try:
        if _env_bool("CAST_SCREENTIME_BACKEND_SYNC", default=False):
            return retained_cast_screentime_runtime.enqueue_run(run_id)

        modal_result = modal_dispatch.dispatch_cast_screentime_run(run_id=run_id)
        if modal_result.get("dispatched"):
            call_id = str(modal_result.get("call_id") or "").strip()
            return {
                "run_id": run_id,
                "state": "queued",
                "job_id": f"modal:{call_id or run_id}",
                "mode": "modal",
                "dispatch": modal_result,
            }

        if modal_dispatch.modal_dispatch_enabled():
            reason = str(modal_result.get("reason_code") or modal_result.get("reason") or "modal_dispatch_failed")
            raise RetainedCastScreentimeDispatchError(reason)

        return retained_cast_screentime_runtime.enqueue_run(run_id)
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, RetainedCastScreentimeDispatchError):
            raise
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


def _env_bool(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default
