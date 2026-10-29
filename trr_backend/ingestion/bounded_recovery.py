"""Durable run and generation binding for bounded Instagram recovery."""

from __future__ import annotations

import os
from datetime import UTC, datetime

from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.recovery_config import recovery_value

WINDOW_ENV = "TRR_INSTAGRAM_RECOVERY_WINDOW_ID"
GENERATION_ENV = "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION"
RUN_ENV = "TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID"
SOURCE_ENV = "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION"
MAX_WINDOW_SECONDS = 6 * 3600
USEFUL_PROGRESS_POLICY = {"mode": "useful_progress", "stall_seconds": 900, "max_duration_seconds": MAX_WINDOW_SECONDS}


def policy_duration(policy):
    """Retain old grants; seal the new stop policy and emergency bound together."""
    if policy is None:
        return 3600
    if not isinstance(policy, dict) or policy != USEFUL_PROGRESS_POLICY:
        raise ValueError("Unsupported recovery stop policy")
    if any(type(policy.get(key)) is not int for key in ("stall_seconds", "max_duration_seconds")):
        raise ValueError("Recovery time bounds must be integer seconds")
    return policy["max_duration_seconds"]


def window_key(run_id, generation):
    return f"bounded-window:{run_id}:{generation}"


def validate_window(run, window, now, *, environ=None):
    """An authorized generation cannot execute through an unbound worker."""
    env = os.environ if environ is None else environ
    bound_run = env.get(RUN_ENV)
    if env.get(WINDOW_ENV):
        if bound_run != str(run["run_id"]):
            raise LostOwnership("Bounded recovery rejected unrelated work")
        if str(run["generation"]) != env.get(GENERATION_ENV):
            raise LostOwnership("Bounded recovery generation changed")
        if not window:
            raise LostOwnership("Bounded recovery authorization is absent")
    if not window:
        return
    expected = window_key(run["run_id"], run["generation"])
    try:
        duration = policy_duration(window.get("stop_policy"))
    except ValueError as error:
        raise LostOwnership("Invalid recovery stop policy") from error
    if (
        window.get("window_id") != expected
        or window.get("generation") != run["generation"]
        or window.get("priority") != "comments_first"
        or window.get("duration_seconds") != duration
        or env.get(WINDOW_ENV) != expected
        or bound_run != str(run["run_id"])
        or env.get(GENERATION_ENV) != str(run["generation"])
        or recovery_value("RECOVERY_SOURCE_REVISION", environ=env) != window.get("source_revision")
    ):
        raise LostOwnership("Execution is not bound to this recovery window")
    deadline = datetime.fromisoformat(window["deadline_at"].replace("Z", "+00:00"))
    if deadline.tzinfo is None or now >= deadline:
        raise LostOwnership("Bounded recovery window expired")


def require_window(cur, run, *, environ=None):
    from trr_backend.ingestion.persistence import _row

    row = _row(
        cur,
        """select result,clock_timestamp() as database_now from social.instagram_ingestion_commands
        where account=%s and idempotency_key=%s""",
        (run["account"], window_key(run["run_id"], run["generation"])),
    )
    window = row["result"] if row else None
    validate_window(run, window, row["database_now"] if row else datetime.now(UTC), environ=environ)
    if window:
        if window.get("stop_policy"):
            from trr_backend.ingestion.bounded_recovery_phase import phase

            window = dict(window, phase=phase(cur, window))
        run["_bounded_window"] = window
    return window


def current_window_id():
    return os.getenv(WINDOW_ENV, "").strip()
