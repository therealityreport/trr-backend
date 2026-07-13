"""Read-only adaptive budget decisions for social control-plane lanes."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, Literal, cast

from trr_backend.socials.control_plane.backfill_health import get_backfill_health as _load_backfill_health
from trr_backend.socials.control_plane.queue_status import get_queue_status as _load_queue_status
from trr_backend.socials.instagram.auth_cooldown import get_active_cooldown as _load_active_cooldown

LaneBudgetState = Literal["normal", "reduced", "paused", "identity_blocked"]

STATE_NORMAL: LaneBudgetState = "normal"
STATE_REDUCED: LaneBudgetState = "reduced"
STATE_PAUSED: LaneBudgetState = "paused"
STATE_IDENTITY_BLOCKED: LaneBudgetState = "identity_blocked"
LANE_STATES: frozenset[str] = frozenset({STATE_NORMAL, STATE_REDUCED, STATE_PAUSED, STATE_IDENTITY_BLOCKED})

DEFAULT_LANE = "instagram_backfill"
DEFAULT_PLATFORM = "instagram"
DEFAULT_TTL_SECONDS = 300
INSTAGRAM_BACKFILL_RUNBOOK_VERSION = "v4"
INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP = 2
INSTAGRAM_BACKFILL_CANARY_WORKER_CAP = 4
INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR = 25
INSTAGRAM_DB_SESSION_WORKER_BUDGET_ENV = "SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET"
LEGACY_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET_ENV = "SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET"
DEFAULT_INSTAGRAM_DB_SESSION_WORKER_BUDGET = 10
INSTAGRAM_DB_SESSION_POOL_LIMIT_ENV = "SOCIAL_INSTAGRAM_DB_SESSION_POOL_LIMIT"
DEFAULT_INSTAGRAM_DB_SESSION_POOL_LIMIT = 15

ACTIVE_QUEUE_STATUSES: tuple[str, ...] = ("queued", "pending", "running", "retrying")

IDENTITY_BLOCKER_KINDS: frozenset[str] = frozenset(
    {
        "checkpoint",
        "challenge",
        "identity",
        "identity_blocked",
        "login_required",
        "reauth_required",
    }
)
IDENTITY_ERROR_MARKERS: tuple[str, ...] = (
    "checkpoint",
    "challenge",
    "identity",
    "login_required",
    "redirect_to_login",
    "two_step",
    "2fa",
)
PROXY_COOLDOWN_KINDS: frozenset[str] = frozenset(
    {
        "auth",
        "cooldown",
        "proxy",
        "proxy_cooldown",
        "rate_limit",
    }
)
PROXY_ERROR_MARKERS: tuple[str, ...] = (
    "proxy_budget_exhausted",
    "proxy_cooldown",
    "proxy_rate_limited",
    "rate_limit",
    "rate_limited",
    "http_429",
    "429",
)
WRITE_FAILURE_MARKERS: tuple[str, ...] = (
    "persist",
    "persistence",
    "write",
    "upsert",
    "silent_drop",
)

DEFAULT_LIMITS: dict[str, Any] = {
    "ttl_seconds": DEFAULT_TTL_SECONDS,
    "normal_max_concurrent_jobs": INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP,
    "reduced_max_concurrent_jobs": 1,
    "paused_max_concurrent_jobs": 0,
    "identity_blocked_max_concurrent_jobs": 0,
    "minimum_sample_floor": INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR,
    "queue_depth_reduced_threshold": 25,
    "queue_depth_paused_threshold": 100,
    "running_jobs_reduced_threshold": 2,
    "running_jobs_paused_threshold": 5,
    "recent_failures_reduced_threshold": 2,
    "recent_failures_paused_threshold": 6,
    "write_failures_reduced_threshold": 1,
    "write_failures_paused_threshold": 3,
    "stale_running_jobs_paused_threshold": 1,
    "auth_failure_rate_reduced_threshold": 0.10,
    "auth_failure_rate_paused_threshold": 0.35,
    "auth_failures_reduced_threshold": 2,
    "auth_failures_paused_threshold": 6,
    "proxy_gb_reduced_threshold": None,
    "proxy_gb_paused_threshold": None,
    "proxy_usd_reduced_threshold": None,
    "proxy_usd_paused_threshold": None,
}


def instagram_db_session_worker_budget() -> int:
    """Return the combined Instagram Modal-worker budget.

    The comments-only variable remains a compatibility fallback while deployed
    environments move to the canonical combined-worker name.
    """

    raw = str(
        os.getenv(INSTAGRAM_DB_SESSION_WORKER_BUDGET_ENV)
        or os.getenv(LEGACY_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET_ENV)
        or DEFAULT_INSTAGRAM_DB_SESSION_WORKER_BUDGET
    ).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_INSTAGRAM_DB_SESSION_WORKER_BUDGET


def instagram_db_session_pool_limit() -> int:
    raw = str(os.getenv(INSTAGRAM_DB_SESSION_POOL_LIMIT_ENV) or DEFAULT_INSTAGRAM_DB_SESSION_POOL_LIMIT).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_INSTAGRAM_DB_SESSION_POOL_LIMIT


def _instagram_db_session_pool_usage(*, requested_sessions: int) -> dict[str, Any]:
    """Prove requested session headroom with short-lived fresh reservations."""

    from trr_backend.db import pg

    limit = instagram_db_session_pool_limit()
    probe = pg.probe_fresh_session_capacity(requested_sessions=requested_sessions)
    available = bool(probe.get("available"))
    reason = str(probe.get("reason") or "").strip() or None
    return {
        "available": available,
        "source": "fresh_session_reservation",
        "limit": limit,
        "observed_sessions": None,
        "remaining_sessions": None,
        "at_capacity": reason == "session_pool_capacity",
        "application_name": "trr-backend:session-capacity-probe",
        "requested_sessions": _to_int(probe.get("requested_sessions")),
        "reserved_sessions": _to_int(probe.get("reserved_sessions")),
        "probe_reason": reason,
        "probe_target": _metadata_dict(probe.get("target")),
        "read_error": None if available else reason or str(probe.get("error") or "session_capacity_probe_failed"),
    }


def get_instagram_db_session_capacity(
    *,
    requested_workers: int = 0,
    raw_requested_workers: int | None = None,
    backend_effective_requested_workers: int | None = None,
    active_workers: int | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Return the canonical Instagram DB-session capacity snapshot."""

    budget = instagram_db_session_worker_budget()
    read_error: str | None = None
    active_db_job_ids: list[str] = []
    dispatched_unclaimed_job_ids: list[str] = []
    draining_remote_call_ids: list[str] = []
    nonterminal_remote_call_ids: list[str] = []
    session_pool_usage: dict[str, Any] = {
        "available": False,
        "source": "not_checked_active_worker_override",
        "limit": instagram_db_session_pool_limit(),
        "observed_sessions": None,
        "remaining_sessions": None,
        "at_capacity": None,
        "application_name": None,
        "read_error": None,
    }
    if active_workers is None:
        try:
            from trr_backend.db import pg
            from trr_backend.modal_dispatch import inspect_modal_function_call

            rows = pg.fetch_all(
                """
                select
                  id::text as job_id,
                  lower(coalesce(status, '')) as status,
                  nullif(trim(coalesce(worker_id, '')), '') as worker_id,
                  claimed_at,
                  nullif(metadata #>> '{dispatch,remote_invocation_id}', '') as remote_invocation_id,
                  lower(coalesce(metadata #>> '{dispatch,remote_invocation_status}', 'unknown'))
                    as remote_invocation_status
                from social.scrape_jobs
                where lower(coalesce(platform, '')) = 'instagram'
                  and lower(coalesce(config->>'stage', metadata->>'stage', job_type, '')) in (
                    'comments',
                    'comments_scrapling',
                    'shared_account_discovery',
                    'shared_account_posts'
                  )
                  and (
                    status in ('queued', 'pending', 'retrying', 'running', 'claimed', 'dispatched', 'processing')
                    or (
                      status in ('completed', 'failed', 'cancelled')
                      and coalesce(completed_at, heartbeat_at, started_at, claimed_at, created_at)
                        >= now() - interval '2 hours'
                      and nullif(metadata #>> '{dispatch,remote_invocation_id}', '') is not null
                      and lower(coalesce(metadata #>> '{dispatch,remote_invocation_status}', 'unknown'))
                        not in ('completed', 'failed', 'cancelled', 'terminated', 'error')
                    )
                  )
                """,
                conn=conn,
                pool_name="social_control",
            )
            inspections: dict[str, dict[str, Any]] = {}
            for row in rows:
                call_id = str(row.get("remote_invocation_id") or "").strip()
                if call_id and call_id not in inspections:
                    inspections[call_id] = inspect_modal_function_call(call_id)

            for row in rows:
                job_id = str(row.get("job_id") or "").strip()
                status = _normalize_text(row.get("status"))
                call_id = str(row.get("remote_invocation_id") or "").strip()
                inspection_status = _normalize_text(_metadata_dict(inspections.get(call_id)).get("status"))
                remote_nonterminal = bool(call_id) and inspection_status not in {"completed", "failed"}
                if remote_nonterminal and call_id not in nonterminal_remote_call_ids:
                    nonterminal_remote_call_ids.append(call_id)

                claimed = bool(str(row.get("worker_id") or "").strip() or row.get("claimed_at"))
                if status in {"running", "claimed", "processing"} or (status == "dispatched" and claimed):
                    if job_id:
                        active_db_job_ids.append(job_id)
                elif status in {"queued", "pending", "retrying", "dispatched"} and remote_nonterminal:
                    if job_id:
                        dispatched_unclaimed_job_ids.append(job_id)
                elif remote_nonterminal:
                    draining_remote_call_ids.append(call_id)
        except Exception as exc:  # noqa: BLE001 - diagnostics stay available during DB pressure
            read_error = f"{type(exc).__name__}: {exc}"
    else:
        active_db_job_ids = [f"active-worker-{index + 1}" for index in range(_to_int(active_workers))]

    active_db_jobs = len(active_db_job_ids)
    dispatched_unclaimed_jobs = len(dispatched_unclaimed_job_ids)
    draining_remote_calls = len(dict.fromkeys(draining_remote_call_ids))
    occupied = active_db_jobs + dispatched_unclaimed_jobs + draining_remote_calls
    effective_requested = _to_int(
        requested_workers if backend_effective_requested_workers is None else backend_effective_requested_workers
    )
    raw_requested = _to_int(effective_requested if raw_requested_workers is None else raw_requested_workers)
    if active_workers is None:
        session_pool_usage = _instagram_db_session_pool_usage(requested_sessions=effective_requested)
    remaining = max(0, budget - occupied)
    worker_budget_blocked = read_error is None and effective_requested > remaining
    session_pool_remaining = session_pool_usage.get("remaining_sessions")
    session_pool_blocked = bool(
        effective_requested > 0
        and (
            not session_pool_usage.get("available")
            or _to_int(session_pool_usage.get("reserved_sessions")) < effective_requested
        )
    )
    blocked = worker_budget_blocked or session_pool_blocked
    capacity_available = bool(read_error is None and (effective_requested == 0 or session_pool_usage.get("available")))
    capacity_read_error = read_error or (
        str(session_pool_usage.get("read_error") or "session_capacity_probe_failed")
        if effective_requested > 0 and not session_pool_usage.get("available")
        else None
    )
    block_reason = (
        "instagram_db_session_pool_capacity_exceeded"
        if session_pool_blocked and session_pool_usage.get("probe_reason") == "session_pool_capacity"
        else "instagram_db_session_pool_probe_failed"
        if session_pool_blocked
        else "instagram_db_session_worker_budget_exceeded"
        if worker_budget_blocked
        else None
    )
    return {
        "worker_budget": budget,
        "safe_combined_worker_limit": budget,
        "safe_limit": budget,
        "active_workers": occupied,
        "occupied_workers": occupied,
        "active_db_jobs": active_db_jobs,
        "active_db_job_ids": active_db_job_ids,
        "dispatched_unclaimed_jobs": dispatched_unclaimed_jobs,
        "dispatched_unclaimed_job_ids": dispatched_unclaimed_job_ids,
        "nonterminal_remote_calls": len(nonterminal_remote_call_ids),
        "nonterminal_remote_call_ids": nonterminal_remote_call_ids,
        "draining_remote_calls": draining_remote_calls,
        "draining_remote_call_ids": list(dict.fromkeys(draining_remote_call_ids)),
        "remaining_workers": remaining,
        "remaining_slots": remaining,
        "raw_requested_workers": raw_requested,
        "backend_effective_requested_workers": effective_requested,
        "requested_workers": effective_requested,
        "session_pool": {
            **session_pool_usage,
            "requested_sessions": effective_requested,
            "blocked": session_pool_blocked,
        },
        "session_pool_available": bool(session_pool_usage.get("available")),
        "session_pool_limit": _to_int(session_pool_usage.get("limit")),
        "session_pool_observed_sessions": session_pool_usage.get("observed_sessions"),
        "session_pool_remaining_sessions": session_pool_remaining,
        "session_pool_requested_sessions": effective_requested,
        "session_pool_reserved_sessions": _to_int(session_pool_usage.get("reserved_sessions")),
        "session_pool_at_capacity": session_pool_usage.get("at_capacity"),
        "session_pool_blocked": session_pool_blocked,
        "session_pool_read_error": session_pool_usage.get("read_error"),
        "blocked": blocked,
        "block_reason": block_reason,
        "available": capacity_available,
        "read_error": capacity_read_error,
    }


def instagram_backfill_runbook_metadata(*, state: str = "active", cap4_canary_active: bool = False) -> dict[str, Any]:
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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_account(value: Any) -> str | None:
    normalized = str(value or "").strip().lower().lstrip("@")
    return normalized or None


def _normalize_lane(value: Any) -> str:
    return _normalize_text(value) or DEFAULT_LANE


def _metadata_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return default


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _seconds_until(value: Any, now: datetime) -> int | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max(0, int((parsed - now).total_seconds()))


def _state_or_none(value: Any) -> LaneBudgetState | None:
    normalized = _normalize_text(value)
    if normalized in LANE_STATES:
        return cast(LaneBudgetState, normalized)
    return None


def _limit_number(value: Any, default: Any) -> Any:
    if default is None and (value is None or value == ""):
        return None
    if isinstance(default, int) and not isinstance(default, bool):
        return _to_int(value, default)
    if isinstance(default, float):
        return _to_float(value, default)
    if default is None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None
    return value


def _resolve_limits(benchmark_overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    limits = dict(DEFAULT_LIMITS)
    override_payload = _metadata_dict(benchmark_overrides)
    nested_limits = _metadata_dict(override_payload.get("limits"))
    cap4_canary_enabled = bool(override_payload.get("enable_cap4_canary") or nested_limits.get("enable_cap4_canary"))
    for source in (override_payload, nested_limits):
        for key, default in DEFAULT_LIMITS.items():
            if key in source:
                limits[key] = _limit_number(source.get(key), default)

    ttl_seconds = max(30, min(_to_int(limits.get("ttl_seconds"), DEFAULT_TTL_SECONDS), 3600))
    limits["ttl_seconds"] = ttl_seconds
    normal_cap_ceiling = (
        INSTAGRAM_BACKFILL_CANARY_WORKER_CAP if cap4_canary_enabled else INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP
    )
    limits["normal_max_concurrent_jobs"] = max(
        1,
        min(
            _to_int(limits.get("normal_max_concurrent_jobs"), INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP),
            normal_cap_ceiling,
        ),
    )
    if cap4_canary_enabled:
        limits["normal_max_concurrent_jobs"] = max(
            limits["normal_max_concurrent_jobs"],
            INSTAGRAM_BACKFILL_CANARY_WORKER_CAP,
        )
    limits["enable_cap4_canary"] = cap4_canary_enabled
    limits["minimum_sample_floor"] = max(
        INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR,
        _to_int(limits.get("minimum_sample_floor"), INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR),
    )
    return limits


def _effective_max_concurrent_jobs(state: LaneBudgetState, limits: Mapping[str, Any]) -> int:
    key = {
        STATE_NORMAL: "normal_max_concurrent_jobs",
        STATE_REDUCED: "reduced_max_concurrent_jobs",
        STATE_PAUSED: "paused_max_concurrent_jobs",
        STATE_IDENTITY_BLOCKED: "identity_blocked_max_concurrent_jobs",
    }[state]
    return _to_int(limits.get(key), 0)


def _decision_limits(state: LaneBudgetState, limits: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(limits)
    payload["effective_max_concurrent_jobs"] = _effective_max_concurrent_jobs(state, limits)
    payload["live_apply_binding_cap"] = INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP
    payload["cap4_canary_max_concurrent_jobs"] = INSTAGRAM_BACKFILL_CANARY_WORKER_CAP
    payload["cap4_canary_active"] = bool(limits.get("enable_cap4_canary") and state == STATE_NORMAL)
    payload["cap4_canary_metadata_only"] = not payload["cap4_canary_active"]
    return payload


def _matches_account(row: Mapping[str, Any], account: str | None, *, unknown_matches: bool) -> bool:
    if account is None:
        return True
    row_account = _normalize_account(
        row.get("account")
        or row.get("account_handle")
        or row.get("handle")
        or row.get("username")
        or row.get("target_account")
    )
    if row_account is None:
        return unknown_matches
    return row_account == account


def _matches_platform(row: Mapping[str, Any], platform: str) -> bool:
    row_platform = _normalize_text(row.get("platform"))
    return not row_platform or row_platform == platform


def _matches_lane(row: Mapping[str, Any], lane: str, *, unknown_matches: bool = True) -> bool:
    row_lane = _normalize_text(row.get("lane") or row.get("budget_lane") or row.get("stage") or row.get("job_type"))
    if not row_lane:
        return unknown_matches
    return row_lane == lane or row_lane in {"global", "*", "all"}


def _cooldown_is_active(cooldown: Mapping[str, Any], now: datetime) -> bool:
    active_value = cooldown.get("active")
    if active_value is False:
        return False
    until_value = cooldown.get("cooldown_until") or cooldown.get("until") or cooldown.get("expires_at")
    if until_value is None:
        return active_value is True or bool(cooldown)
    until = _parse_datetime(until_value)
    if until is None:
        return True
    return until > now


def _cooldown_matches(
    cooldown: Mapping[str, Any],
    *,
    platform: str,
    account: str | None,
    lane: str,
    now: datetime,
) -> bool:
    return (
        _cooldown_is_active(cooldown, now)
        and _matches_platform(cooldown, platform)
        and _matches_account(cooldown, account, unknown_matches=False)
        and _matches_lane(cooldown, lane)
    )


def _coerce_cooldown(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    to_metadata = getattr(value, "to_metadata", None)
    if callable(to_metadata):
        metadata = to_metadata()
        if isinstance(metadata, Mapping):
            return dict(metadata)
    return None


def _dedupe_cooldowns(cooldowns: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for cooldown in cooldowns:
        payload = dict(cooldown)
        key = (
            _normalize_text(payload.get("platform")),
            _normalize_account(payload.get("account_handle") or payload.get("account")) or "",
            str(payload.get("cooldown_until") or payload.get("until") or ""),
            _normalize_text(payload.get("blocker_kind")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
    return deduped


def _cooldowns_from_sources(
    *,
    supplied: Sequence[Mapping[str, Any]] | None,
    backfill_health: Mapping[str, Any],
    platform: str,
    account: str | None,
    include_live: bool,
    read_errors: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], str]:
    cooldowns: list[dict[str, Any]] = []
    source = "supplied" if supplied is not None else "backfill_health"
    if supplied is not None:
        cooldowns.extend(_list_of_dicts(supplied))
    else:
        cooldowns.extend(_list_of_dicts(backfill_health.get("cooldowns")))

    if include_live and supplied is None and account is not None:
        try:
            live_cooldown = _coerce_cooldown(_load_active_cooldown(platform, account))
        except Exception as exc:  # noqa: BLE001 - fail closed into evidence, not an exception
            read_errors.append({"source": "active_cooldown", "error": f"{type(exc).__name__}: {exc}"})
        else:
            if live_cooldown is not None:
                cooldowns.append(live_cooldown)
                source = "supplied+live" if supplied is not None else "backfill_health+live"
    return _dedupe_cooldowns(cooldowns), source


def _identity_blocker_from_worker_auth(backfill_health: Mapping[str, Any], platform: str) -> dict[str, Any] | None:
    if platform != "instagram":
        return None
    worker_auth = _metadata_dict(backfill_health.get("worker_auth"))
    if worker_auth.get("instagram_authenticated") is not False:
        return None
    return {
        "type": "worker_auth",
        "platform": platform,
        "instagram_authenticated": False,
        "reason": (
            worker_auth.get("instagram_auth_reason") or worker_auth.get("reason") or "instagram_identity_unavailable"
        ),
        "detail": worker_auth.get("instagram_auth_detail"),
    }


def _find_identity_blocker(
    *,
    cooldowns: Sequence[Mapping[str, Any]],
    backfill_health: Mapping[str, Any],
    platform: str,
    account: str | None,
    lane: str,
    now: datetime,
) -> dict[str, Any] | None:
    for cooldown in cooldowns:
        if not _cooldown_matches(cooldown, platform=platform, account=account, lane=lane, now=now):
            continue
        blocker_kind = _normalize_text(cooldown.get("blocker_kind"))
        error_code = _normalize_text(cooldown.get("last_error_code") or cooldown.get("error_code"))
        if blocker_kind in IDENTITY_BLOCKER_KINDS or any(marker in error_code for marker in IDENTITY_ERROR_MARKERS):
            return {"type": "active_cooldown", **dict(cooldown)}
    return _identity_blocker_from_worker_auth(backfill_health, platform)


def _extract_proxy_cooldown_entries(*payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for payload in payloads:
        for key in ("proxy_cooldown", "proxy_cooldowns", "active_proxy_cooldowns"):
            value = payload.get(key)
            if isinstance(value, Mapping):
                entries.append(dict(value))
            else:
                entries.extend(_list_of_dicts(value))
    return entries


def _failure_has_proxy_marker(failure: Mapping[str, Any]) -> bool:
    haystack = " ".join(
        _normalize_text(failure.get(key))
        for key in ("reason", "error_code", "last_error_code", "last_error_class", "error_message", "message")
    )
    return any(marker in haystack for marker in PROXY_ERROR_MARKERS)


def _find_proxy_cooldown(
    *,
    cooldowns: Sequence[Mapping[str, Any]],
    backfill_health: Mapping[str, Any],
    queue_status: Mapping[str, Any],
    benchmark_overrides: Mapping[str, Any],
    recent_failures: Sequence[Mapping[str, Any]],
    platform: str,
    account: str | None,
    lane: str,
    now: datetime,
) -> dict[str, Any] | None:
    for proxy_cooldown in _extract_proxy_cooldown_entries(backfill_health, queue_status, benchmark_overrides):
        if (
            _cooldown_is_active(proxy_cooldown, now)
            and _matches_platform(proxy_cooldown, platform)
            and _matches_account(proxy_cooldown, account, unknown_matches=True)
            and _matches_lane(proxy_cooldown, lane)
        ):
            return {"type": "proxy_cooldown", **proxy_cooldown}

    for cooldown in cooldowns:
        if not _cooldown_matches(cooldown, platform=platform, account=account, lane=lane, now=now):
            continue
        blocker_kind = _normalize_text(cooldown.get("blocker_kind"))
        error_code = _normalize_text(cooldown.get("last_error_code") or cooldown.get("error_code"))
        if blocker_kind in PROXY_COOLDOWN_KINDS or any(marker in error_code for marker in PROXY_ERROR_MARKERS):
            return {"type": "active_cooldown", **dict(cooldown)}

    for failure in recent_failures:
        if not (
            _matches_platform(failure, platform)
            and _matches_account(failure, account, unknown_matches=True)
            and _matches_lane(failure, lane)
        ):
            continue
        if not _failure_has_proxy_marker(failure):
            continue
        cooldown_until = failure.get("cooldown_until") or failure.get("next_available_at")
        if cooldown_until is not None and _seconds_until(cooldown_until, now) == 0:
            continue
        return {"type": "recent_failure", **dict(failure)}
    return None


def _looks_like_pause_record(value: Mapping[str, Any]) -> bool:
    return any(
        key in value
        for key in (
            "account",
            "account_handle",
            "handle",
            "lane",
            "budget_lane",
            "paused",
            "reason",
            "state",
        )
    )


def _pause_entries(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        if _looks_like_pause_record(value):
            return [dict(value)]
        entries: list[dict[str, Any]] = []
        for outer_key, nested in value.items():
            if isinstance(nested, Mapping):
                if _looks_like_pause_record(nested):
                    entry = dict(nested)
                    entry.setdefault("lane", outer_key)
                    entries.append(entry)
                    continue
                for inner_key, inner_value in nested.items():
                    if isinstance(inner_value, Mapping):
                        entry = dict(inner_value)
                    else:
                        entry = {"paused": bool(inner_value)}
                    entry.setdefault("lane", outer_key)
                    entry.setdefault("account", inner_key)
                    entries.append(entry)
            elif isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
                entries.extend({"lane": outer_key, "account": item, "paused": True} for item in nested)
            elif bool(nested):
                entries.append({"account": outer_key, "paused": True})
        return entries
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        entries = []
        for item in value:
            if isinstance(item, Mapping):
                entries.append(dict(item))
            else:
                entries.append({"account": item, "paused": True})
        return entries
    return []


def _find_account_lane_pause(
    *,
    backfill_health: Mapping[str, Any],
    queue_status: Mapping[str, Any],
    benchmark_overrides: Mapping[str, Any],
    lane: str,
    account: str | None,
) -> dict[str, Any] | None:
    for source_name, payload in (
        ("backfill_health", backfill_health),
        ("queue_status", queue_status),
        ("benchmark_overrides", benchmark_overrides),
    ):
        for key in ("account_lane_pauses", "lane_pauses", "account_pauses", "paused_accounts"):
            for entry in _pause_entries(payload.get(key)):
                paused_state = _state_or_none(entry.get("state"))
                paused = entry.get("paused")
                if paused is False or paused_state == STATE_NORMAL:
                    continue
                if paused is None and paused_state not in {STATE_PAUSED, STATE_IDENTITY_BLOCKED}:
                    paused = True
                if not bool(paused):
                    continue
                if not _matches_lane(entry, lane):
                    continue
                if not _matches_account(entry, account, unknown_matches=account is None):
                    continue
                return {"source": source_name, **entry}
    return None


def _recent_failures_from_sources(
    supplied: Sequence[Mapping[str, Any]] | None,
    queue_status: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    if supplied is not None:
        return _list_of_dicts(supplied), "supplied"
    queue = _metadata_dict(queue_status.get("queue"))
    return _list_of_dicts(queue.get("recent_failures")), "queue_status"


def _running_jobs_from_sources(
    supplied: Sequence[Mapping[str, Any]] | None,
    queue_status: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    if supplied is not None:
        return _list_of_dicts(supplied), "supplied"
    queue = _metadata_dict(queue_status.get("queue"))
    return _list_of_dicts(queue.get("running_jobs")), "queue_status"


def _stale_jobs_from_sources(
    supplied: Sequence[Mapping[str, Any]] | None,
    queue_status: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    if supplied is not None:
        return _list_of_dicts(supplied), "supplied"
    queue = _metadata_dict(queue_status.get("queue"))
    stale_jobs = _list_of_dicts(queue.get("stuck_jobs"))
    stale_claims = _metadata_dict(queue.get("stale_claims"))
    media_stale_claims = _metadata_dict(queue.get("media_stale_claims"))
    if stale_claims:
        stale_jobs.append({"kind": "stale_claims", **stale_claims})
    if media_stale_claims:
        stale_jobs.append({"kind": "media_stale_claims", **media_stale_claims})
    return stale_jobs, "queue_status"


def _queue_depth(backfill_health: Mapping[str, Any], queue_status: Mapping[str, Any]) -> int:
    health_queue = _metadata_dict(backfill_health.get("queue"))
    if "queue_depth" in health_queue:
        return _to_int(health_queue.get("queue_depth"))
    queue = _metadata_dict(queue_status.get("queue"))
    by_status = _metadata_dict(queue.get("by_status"))
    return sum(_to_int(by_status.get(status)) for status in ACTIVE_QUEUE_STATUSES)


def _queue_error(backfill_health: Mapping[str, Any], queue_status: Mapping[str, Any]) -> str | None:
    health_queue = _metadata_dict(backfill_health.get("queue"))
    queue = _metadata_dict(queue_status.get("queue"))
    return str(health_queue.get("error") or queue.get("error") or "").strip() or None


def _filtered_count(
    rows: Sequence[Mapping[str, Any]],
    *,
    platform: str,
    account: str | None,
    lane: str,
    total_keys: Sequence[str] = ("total",),
) -> int:
    count = 0
    for row in rows:
        if not (
            _matches_platform(row, platform)
            and _matches_account(row, account, unknown_matches=True)
            and _matches_lane(row, lane)
        ):
            continue
        row_total = 0
        has_total_key = False
        for key in total_keys:
            if key in row:
                has_total_key = True
            row_total = max(row_total, _to_int(row.get(key)))
        count += row_total if has_total_key else 1
    return count


def _count_recent_write_failures(
    failures: Sequence[Mapping[str, Any]],
    *,
    platform: str,
    account: str | None,
    lane: str,
    queue_status: Mapping[str, Any],
) -> int:
    count = 0
    for failure in failures:
        if not (
            _matches_platform(failure, platform)
            and _matches_account(failure, account, unknown_matches=True)
            and _matches_lane(failure, lane)
        ):
            continue
        haystack = " ".join(
            _normalize_text(failure.get(key))
            for key in ("reason", "error_code", "last_error_code", "last_error_class", "error_message", "message")
        )
        if any(marker in haystack for marker in WRITE_FAILURE_MARKERS):
            count += 1
    queue = _metadata_dict(queue_status.get("queue"))
    count += _to_int(queue.get("silent_drop_warnings_total"))
    return count


def _auth_failure_pressure(
    backfill_health: Mapping[str, Any],
    *,
    platform: str,
    account: str | None,
) -> dict[str, Any]:
    runs = _list_of_dicts(backfill_health.get("runs"))
    matching_runs = [
        run for run in runs if _matches_platform(run, platform) and _matches_account(run, account, unknown_matches=True)
    ]
    max_rate = 0.0
    total_failures = 0
    run_count = 0
    for run in matching_runs:
        run_count += 1
        max_rate = max(max_rate, _to_float(run.get("auth_failure_rate")))
        auth_failures = _metadata_dict(run.get("auth_failures"))
        total_failures += _to_int(auth_failures.get("auth_failures_total"))
    totals = _metadata_dict(backfill_health.get("totals"))
    if not matching_runs:
        total_failures = _to_int(totals.get("auth_failures_total"))
    return {
        "runs_considered": run_count,
        "auth_failure_rate_max": round(max_rate, 4),
        "auth_failures_total": total_failures,
    }


def _bandwidth_pressure(backfill_health: Mapping[str, Any]) -> dict[str, Any]:
    bandwidth = _metadata_dict(backfill_health.get("bandwidth"))
    return {
        "gb_total": _to_float(bandwidth.get("gb_total")),
        "derived_usd": _to_float(bandwidth.get("derived_usd")),
        "cost_available": bool(bandwidth.get("cost_available")),
    }


def _worker_health_pressure(backfill_health: Mapping[str, Any]) -> dict[str, Any]:
    worker_health = _metadata_dict(backfill_health.get("worker_health"))
    if not worker_health:
        return {}
    return {
        "healthy": worker_health.get("healthy"),
        "reason": worker_health.get("reason") or worker_health.get("error"),
        "healthy_workers": _to_int(worker_health.get("healthy_workers")),
        "active_workers": _to_int(worker_health.get("active_workers")),
    }


def _threshold_breached(value: float | int, threshold: Any) -> bool:
    if threshold is None:
        return False
    try:
        parsed = float(threshold)
    except (TypeError, ValueError):
        return False
    return parsed > 0 and float(value) >= parsed


def _lane_override_state(benchmark_overrides: Mapping[str, Any], lane: str) -> tuple[LaneBudgetState | None, Any]:
    direct_state = _state_or_none(benchmark_overrides.get("state"))
    if direct_state is not None:
        return direct_state, {"state": direct_state}

    for key in ("lane_states", "lane_budgets", "lanes"):
        lane_payload = benchmark_overrides.get(key)
        if not isinstance(lane_payload, Mapping):
            continue
        value = lane_payload.get(lane) or lane_payload.get("global") or lane_payload.get("*")
        if isinstance(value, Mapping):
            state = _state_or_none(value.get("state"))
            if state is not None:
                return state, dict(value)
        else:
            state = _state_or_none(value)
            if state is not None:
                return state, {key: value}
    return None, None


def _evaluate_global_pressure(
    *,
    backfill_health: Mapping[str, Any],
    queue_status: Mapping[str, Any],
    benchmark_overrides: Mapping[str, Any],
    recent_failures: Sequence[Mapping[str, Any]],
    running_jobs: Sequence[Mapping[str, Any]],
    stale_running_jobs: Sequence[Mapping[str, Any]],
    platform: str,
    account: str | None,
    lane: str,
    limits: Mapping[str, Any],
    read_errors: Sequence[Mapping[str, str]],
) -> tuple[LaneBudgetState | None, list[str], dict[str, Any]]:
    severe: list[str] = []
    moderate: list[str] = []

    override_state, override_evidence = _lane_override_state(benchmark_overrides, lane)
    if override_state in {STATE_PAUSED, STATE_IDENTITY_BLOCKED}:
        severe.append("benchmark_lane_state_paused")
    elif override_state == STATE_REDUCED:
        moderate.append("benchmark_lane_state_reduced")

    queue_depth = _queue_depth(backfill_health, queue_status)
    if _threshold_breached(queue_depth, limits.get("queue_depth_paused_threshold")):
        severe.append("queue_depth_paused_threshold")
    elif _threshold_breached(queue_depth, limits.get("queue_depth_reduced_threshold")):
        moderate.append("queue_depth_reduced_threshold")

    running_count = _filtered_count(
        running_jobs,
        platform=platform,
        account=account,
        lane=lane,
        total_keys=("total", "active", "running"),
    )
    if running_count == 0:
        queue = _metadata_dict(queue_status.get("queue"))
        by_status = _metadata_dict(queue.get("by_status"))
        running_count = _to_int(by_status.get("running"))
    if _threshold_breached(running_count, limits.get("running_jobs_paused_threshold")):
        severe.append("running_jobs_paused_threshold")
    elif _threshold_breached(running_count, limits.get("running_jobs_reduced_threshold")):
        moderate.append("running_jobs_reduced_threshold")

    stale_count = _filtered_count(
        stale_running_jobs,
        platform=platform,
        account=account,
        lane=lane,
        total_keys=("total", "stuck_jobs_total", "stale", "active"),
    )
    if _threshold_breached(stale_count, limits.get("stale_running_jobs_paused_threshold")):
        severe.append("stale_running_jobs_present")

    recent_failure_count = _filtered_count(
        recent_failures,
        platform=platform,
        account=account,
        lane=lane,
    )
    if _threshold_breached(recent_failure_count, limits.get("recent_failures_paused_threshold")):
        severe.append("recent_failures_paused_threshold")
    elif _threshold_breached(recent_failure_count, limits.get("recent_failures_reduced_threshold")):
        moderate.append("recent_failures_reduced_threshold")

    write_failure_count = _count_recent_write_failures(
        recent_failures,
        platform=platform,
        account=account,
        lane=lane,
        queue_status=queue_status,
    )
    if _threshold_breached(write_failure_count, limits.get("write_failures_paused_threshold")):
        severe.append("write_failures_paused_threshold")
    elif _threshold_breached(write_failure_count, limits.get("write_failures_reduced_threshold")):
        moderate.append("write_failures_reduced_threshold")

    auth_pressure = _auth_failure_pressure(backfill_health, platform=platform, account=account)
    if _threshold_breached(auth_pressure["auth_failure_rate_max"], limits.get("auth_failure_rate_paused_threshold")):
        severe.append("auth_failure_rate_paused_threshold")
    elif _threshold_breached(
        auth_pressure["auth_failure_rate_max"],
        limits.get("auth_failure_rate_reduced_threshold"),
    ):
        moderate.append("auth_failure_rate_reduced_threshold")
    if _threshold_breached(auth_pressure["auth_failures_total"], limits.get("auth_failures_paused_threshold")):
        severe.append("auth_failures_paused_threshold")
    elif _threshold_breached(auth_pressure["auth_failures_total"], limits.get("auth_failures_reduced_threshold")):
        moderate.append("auth_failures_reduced_threshold")

    bandwidth = _bandwidth_pressure(backfill_health)
    if _threshold_breached(bandwidth["gb_total"], limits.get("proxy_gb_paused_threshold")):
        severe.append("proxy_gb_paused_threshold")
    elif _threshold_breached(bandwidth["gb_total"], limits.get("proxy_gb_reduced_threshold")):
        moderate.append("proxy_gb_reduced_threshold")
    if _threshold_breached(bandwidth["derived_usd"], limits.get("proxy_usd_paused_threshold")):
        severe.append("proxy_usd_paused_threshold")
    elif _threshold_breached(bandwidth["derived_usd"], limits.get("proxy_usd_reduced_threshold")):
        moderate.append("proxy_usd_reduced_threshold")

    worker_health = _worker_health_pressure(backfill_health)
    if worker_health and worker_health.get("healthy") is False:
        severe.append("worker_health_unhealthy")

    queue_error = _queue_error(backfill_health, queue_status)
    if queue_error:
        moderate.append("queue_status_error")
    if read_errors:
        moderate.append("control_plane_read_error")

    evidence = {
        "queue_depth": queue_depth,
        "running_jobs_total": running_count,
        "stale_running_jobs_total": stale_count,
        "recent_failures_total": recent_failure_count,
        "write_failures_total": write_failure_count,
        "auth": auth_pressure,
        "bandwidth": bandwidth,
        "worker_health": worker_health,
        "queue_error": queue_error,
        "read_errors": [dict(item) for item in read_errors],
        "benchmark_override": override_evidence,
        "severe_reasons": severe,
        "moderate_reasons": moderate,
    }
    if severe:
        return STATE_PAUSED, severe, evidence
    if moderate:
        return STATE_REDUCED, moderate, evidence
    return None, [], evidence


def _load_live_backfill_health(read_errors: list[dict[str, str]]) -> tuple[dict[str, Any], str]:
    try:
        return dict(_load_backfill_health(include_terminal_runs=True)), "live"
    except Exception as exc:  # noqa: BLE001
        read_errors.append({"source": "backfill_health", "error": f"{type(exc).__name__}: {exc}"})
        return {}, "error"


def _load_live_queue_status(read_errors: list[dict[str, str]]) -> tuple[dict[str, Any], str]:
    try:
        return (
            dict(
                _load_queue_status(
                    include_recent_failures=True,
                    include_stuck_jobs=False,
                    include_runs_summary=True,
                    summary_only=True,
                    fresh=True,
                )
            ),
            "live_summary",
        )
    except Exception as exc:  # noqa: BLE001
        read_errors.append({"source": "queue_status", "error": f"{type(exc).__name__}: {exc}"})
        return {}, "error"


def build_budget_decision(
    *,
    lane: str = DEFAULT_LANE,
    account: str | None = None,
    platform: str = DEFAULT_PLATFORM,
    backfill_health: Mapping[str, Any] | None = None,
    queue_status: Mapping[str, Any] | None = None,
    active_cooldowns: Sequence[Mapping[str, Any]] | None = None,
    recent_failures: Sequence[Mapping[str, Any]] | None = None,
    stale_running_jobs: Sequence[Mapping[str, Any]] | None = None,
    running_jobs: Sequence[Mapping[str, Any]] | None = None,
    benchmark_overrides: Mapping[str, Any] | None = None,
    include_live: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the current budget state for a lane/account without writing state.

    Callers can pass already-fetched health and queue payloads for tests or batched
    control-plane reads. If a payload is omitted and ``include_live`` is true, the
    helper reads the existing health seams. The queue read intentionally uses the
    summary path so this budget decision remains side-effect free.
    """
    normalized_platform = _normalize_text(platform) or DEFAULT_PLATFORM
    normalized_lane = _normalize_lane(lane)
    normalized_account = _normalize_account(account)
    generated_at = now or datetime.now(UTC)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=UTC)
    generated_at = generated_at.astimezone(UTC)

    read_errors: list[dict[str, str]] = []
    if backfill_health is None:
        backfill_payload, backfill_source = (
            _load_live_backfill_health(read_errors) if include_live else ({}, "not_loaded")
        )
    else:
        backfill_payload = dict(backfill_health)
        backfill_source = "supplied"
    if queue_status is None:
        queue_payload, queue_source = _load_live_queue_status(read_errors) if include_live else ({}, "not_loaded")
    else:
        queue_payload = dict(queue_status)
        queue_source = "supplied"

    overrides = _metadata_dict(benchmark_overrides)
    limits = _resolve_limits(overrides)
    ttl_seconds = _to_int(limits.get("ttl_seconds"), DEFAULT_TTL_SECONDS)

    recent_failure_rows, recent_failure_source = _recent_failures_from_sources(recent_failures, queue_payload)
    running_job_rows, running_job_source = _running_jobs_from_sources(running_jobs, queue_payload)
    stale_job_rows, stale_job_source = _stale_jobs_from_sources(stale_running_jobs, queue_payload)
    cooldown_rows, cooldown_source = _cooldowns_from_sources(
        supplied=active_cooldowns,
        backfill_health=backfill_payload,
        platform=normalized_platform,
        account=normalized_account,
        include_live=include_live,
        read_errors=read_errors,
    )

    source_evidence = {
        "backfill_health": backfill_source,
        "queue_status": queue_source,
        "active_cooldowns": cooldown_source,
        "recent_failures": recent_failure_source,
        "running_jobs": running_job_source,
        "stale_running_jobs": stale_job_source,
    }

    identity_blocker = _find_identity_blocker(
        cooldowns=cooldown_rows,
        backfill_health=backfill_payload,
        platform=normalized_platform,
        account=normalized_account,
        lane=normalized_lane,
        now=generated_at,
    )
    if identity_blocker is not None:
        return _finalize_decision(
            state=STATE_IDENTITY_BLOCKED,
            lane=normalized_lane,
            account=normalized_account,
            platform=normalized_platform,
            reasons=["identity_blocked"],
            evidence={
                "identity_block": identity_blocker,
                "sources": source_evidence,
                "read_errors": read_errors,
            },
            limits=limits,
            generated_at=generated_at,
            ttl_seconds=ttl_seconds,
        )

    proxy_cooldown = _find_proxy_cooldown(
        cooldowns=cooldown_rows,
        backfill_health=backfill_payload,
        queue_status=queue_payload,
        benchmark_overrides=overrides,
        recent_failures=recent_failure_rows,
        platform=normalized_platform,
        account=normalized_account,
        lane=normalized_lane,
        now=generated_at,
    )
    if proxy_cooldown is not None:
        cooldown_ttl = _seconds_until(
            proxy_cooldown.get("cooldown_until") or proxy_cooldown.get("until") or proxy_cooldown.get("expires_at"),
            generated_at,
        )
        return _finalize_decision(
            state=STATE_PAUSED,
            lane=normalized_lane,
            account=normalized_account,
            platform=normalized_platform,
            reasons=["proxy_cooldown_active"],
            evidence={
                "proxy_cooldown": proxy_cooldown,
                "sources": source_evidence,
                "read_errors": read_errors,
            },
            limits=limits,
            generated_at=generated_at,
            ttl_seconds=min(ttl_seconds, max(30, cooldown_ttl)) if cooldown_ttl is not None else ttl_seconds,
        )

    account_lane_pause = _find_account_lane_pause(
        backfill_health=backfill_payload,
        queue_status=queue_payload,
        benchmark_overrides=overrides,
        lane=normalized_lane,
        account=normalized_account,
    )
    if account_lane_pause is not None:
        return _finalize_decision(
            state=STATE_PAUSED,
            lane=normalized_lane,
            account=normalized_account,
            platform=normalized_platform,
            reasons=["account_lane_paused"],
            evidence={
                "account_lane_pause": account_lane_pause,
                "sources": source_evidence,
                "read_errors": read_errors,
            },
            limits=limits,
            generated_at=generated_at,
            ttl_seconds=ttl_seconds,
        )

    pressure_state, pressure_reasons, pressure_evidence = _evaluate_global_pressure(
        backfill_health=backfill_payload,
        queue_status=queue_payload,
        benchmark_overrides=overrides,
        recent_failures=recent_failure_rows,
        running_jobs=running_job_rows,
        stale_running_jobs=stale_job_rows,
        platform=normalized_platform,
        account=normalized_account,
        lane=normalized_lane,
        limits=limits,
        read_errors=read_errors,
    )
    if pressure_state is not None:
        return _finalize_decision(
            state=pressure_state,
            lane=normalized_lane,
            account=normalized_account,
            platform=normalized_platform,
            reasons=pressure_reasons,
            evidence={
                "global_pressure": pressure_evidence,
                "sources": source_evidence,
            },
            limits=limits,
            generated_at=generated_at,
            ttl_seconds=ttl_seconds,
        )

    return _finalize_decision(
        state=STATE_NORMAL,
        lane=normalized_lane,
        account=normalized_account,
        platform=normalized_platform,
        reasons=["within_default_budget"],
        evidence={
            "global_pressure": pressure_evidence,
            "sources": source_evidence,
        },
        limits=limits,
        generated_at=generated_at,
        ttl_seconds=ttl_seconds,
    )


def _finalize_decision(
    *,
    state: LaneBudgetState,
    lane: str,
    account: str | None,
    platform: str,
    reasons: Sequence[str],
    evidence: Mapping[str, Any],
    limits: Mapping[str, Any],
    generated_at: datetime,
    ttl_seconds: int,
) -> dict[str, Any]:
    decision = {
        "state": state,
        "lane": lane,
        "account": account,
        "platform": platform,
        "reasons": list(reasons),
        "evidence": deepcopy(dict(evidence)),
        "limits": _decision_limits(state, limits),
        "generated_at": _iso(generated_at),
        "ttl_seconds": ttl_seconds,
    }
    if platform == DEFAULT_PLATFORM and lane == DEFAULT_LANE:
        decision["runbook_state"] = instagram_backfill_runbook_metadata(
            cap4_canary_active=bool(decision["limits"].get("cap4_canary_active")),
        )
    return decision


def get_budget_decision(**kwargs: Any) -> dict[str, Any]:
    """Compatibility alias for callers that prefer a getter-style API."""
    return build_budget_decision(**kwargs)


__all__ = [
    "DEFAULT_LANE",
    "DEFAULT_PLATFORM",
    "LANE_STATES",
    "LaneBudgetState",
    "INSTAGRAM_BACKFILL_CANARY_WORKER_CAP",
    "INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP",
    "INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR",
    "INSTAGRAM_BACKFILL_RUNBOOK_VERSION",
    "DEFAULT_INSTAGRAM_DB_SESSION_POOL_LIMIT",
    "DEFAULT_INSTAGRAM_DB_SESSION_WORKER_BUDGET",
    "INSTAGRAM_DB_SESSION_POOL_LIMIT_ENV",
    "INSTAGRAM_DB_SESSION_WORKER_BUDGET_ENV",
    "LEGACY_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET_ENV",
    "STATE_IDENTITY_BLOCKED",
    "STATE_NORMAL",
    "STATE_PAUSED",
    "STATE_REDUCED",
    "build_budget_decision",
    "get_budget_decision",
    "get_instagram_db_session_capacity",
    "instagram_db_session_pool_limit",
    "instagram_db_session_worker_budget",
    "instagram_backfill_runbook_metadata",
]
