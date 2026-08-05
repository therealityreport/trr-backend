"""Import-neutral metadata shared by Instagram backfill control surfaces."""

from __future__ import annotations

from typing import Any

INSTAGRAM_BACKFILL_RUNBOOK_VERSION = "v4"
INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP = 2
INSTAGRAM_BACKFILL_CANARY_WORKER_CAP = 4
INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR = 25


def instagram_backfill_runbook_metadata(
    *,
    state: str = "active",
    cap4_canary_active: bool = False,
) -> dict[str, Any]:
    """Return the v4 runbook metadata shared by budget, launch, and progress."""

    return {
        "phase": "live_apply",
        "runbook_version": INSTAGRAM_BACKFILL_RUNBOOK_VERSION,
        "state": str(state or "active").strip().lower() or "active",
        "mandatory": True,
        "current_comments_cap": INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP,
        "binding_cap": INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP,
        "live_apply": {
            "mandatory": True,
            "binding_cap": INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP,
        },
        "speed_canary_optional": True,
        "speed_canary_cap": INSTAGRAM_BACKFILL_CANARY_WORKER_CAP,
        "cap4_canary": {
            "optional": True,
            "cap": INSTAGRAM_BACKFILL_CANARY_WORKER_CAP,
            "active": bool(cap4_canary_active),
            "mode": "active" if cap4_canary_active else "metadata_only",
        },
        "minimum_completed_comments_jobs": INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR,
        "minimum_sample_floor": INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR,
    }


__all__ = [
    "INSTAGRAM_BACKFILL_CANARY_WORKER_CAP",
    "INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP",
    "INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR",
    "INSTAGRAM_BACKFILL_RUNBOOK_VERSION",
    "instagram_backfill_runbook_metadata",
]
