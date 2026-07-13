# ruff: noqa: F821, UP037
"""Catalog launch orchestration for social account backfill runs."""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import os
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core
from trr_backend.socials.instagram.media_completion import build_media_completion_payload
from trr_backend.socials.instagram.snapshot_completion import (
    AD_FLAGS_PART,
    AUTHOR_AVATAR_PART,
    CANONICAL_POST_ROW_PART,
    COLLABORATORS_PART,
    COMMENT_MEDIA_PART,
    COMMENTS_PART,
    HOSTED_MEDIA_PART,
    LOCATION_PART,
    MEDIA_ASSETS_PART,
    MUSIC_PART,
    POST_DETAIL_PART,
    REPLIES_PART,
    TAGS_PART,
    build_snapshot_completion_summary,
)
from trr_backend.socials.pipelines.comments.instagram import (
    preview_social_account_comments_scrape as _preview_comments_scrape,
)
from trr_backend.socials.pipelines.comments.instagram import (
    start_social_account_comments_scrape as _start_comments_scrape,
)

_RESERVED_CORE_EXPORTS = {
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_core",
    "_IMPORTED_CORE_NAMES",
    "_LOCAL_ROOM_NAMES",
    "_RESERVED_CORE_EXPORTS",
    "_sync_core_overrides",
}
_IMPORTED_CORE_NAMES: set[str] = set()
for _name, _value in _core.__dict__.items():
    if _name in _RESERVED_CORE_EXPORTS:
        continue
    globals()[_name] = _value
    _IMPORTED_CORE_NAMES.add(_name)
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def _normalize_catalog_source_scope(value: str | None, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    if normalized == "bravo":
        return "network"
    if normalized in {"network", "creator", "community", "news"}:
        return normalized
    raise ValueError(f"Unsupported source scope: {value}")


def _catalog_comments_auth_metadata(result: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = _metadata_dict(result)
    if not payload:
        return {}
    if not payload.get("comments_auth_probe") and not payload.get("auth_repair_attempted"):
        return {}
    return {
        "auth_repair_attempted": bool(payload.get("auth_repair_attempted")),
        "auth_repair_status": str(payload.get("auth_repair_status") or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(payload.get("auth_repair_reason") or "").strip() or None,
        "comments_auth_probe": _metadata_dict(payload.get("comments_auth_probe")) or None,
    }


def _instagram_posts_launch_auth_check_enabled() -> bool:
    raw = str(os.getenv("SOCIAL_INSTAGRAM_POSTS_LAUNCH_AUTH_CHECK") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return not bool(os.getenv("PYTEST_CURRENT_TEST"))


def _posts_launch_auth_metadata(
    *,
    attempted: bool = False,
    status: str = "skipped",
    reason: str | None = None,
    probe: dict[str, Any] | None = None,
    repair_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "posts_auth_probe": probe or None,
        "auth_repair_attempted": bool(attempted),
        "auth_repair_status": str(status or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(reason or "").strip() or None,
        "auth_repair_result": repair_result or None,
    }


def _public_posts_launch_auth_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    data = _metadata_dict(metadata)
    if not data:
        return {}
    return {
        "posts_auth_probe": _metadata_dict(data.get("posts_auth_probe")) or None,
        "auth_repair_attempted": bool(data.get("auth_repair_attempted")),
        "auth_repair_status": str(data.get("auth_repair_status") or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(data.get("auth_repair_reason") or "").strip() or None,
    }


def _instagram_backfill_budget_decision(account_handle: str, *, enable_cap4_canary: bool = False) -> dict[str, Any]:
    from trr_backend.socials.control_plane.budget import build_budget_decision

    return build_budget_decision(
        lane="instagram_backfill",
        platform="instagram",
        account=account_handle,
        benchmark_overrides={"enable_cap4_canary": True} if enable_cap4_canary else None,
        include_live=False,
    )


def _instagram_backfill_runbook_state(
    *,
    state: str = "active",
    cap4_canary_active: bool = False,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.budget import instagram_backfill_runbook_metadata

    return instagram_backfill_runbook_metadata(state=state, cap4_canary_active=cap4_canary_active)


def _instagram_backfill_live_apply_worker_cap() -> int:
    from trr_backend.socials.control_plane.budget import INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP

    return INSTAGRAM_BACKFILL_LIVE_APPLY_WORKER_CAP


def _instagram_backfill_minimum_sample_floor() -> int:
    from trr_backend.socials.control_plane.budget import INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR

    return INSTAGRAM_BACKFILL_MINIMUM_SAMPLE_FLOOR


def _budget_runbook_state(decision: Mapping[str, Any] | None) -> dict[str, Any]:
    decision_payload = _metadata_dict(decision)
    stored = _metadata_dict(decision_payload.get("runbook_state"))
    if stored:
        return stored
    limits = _metadata_dict(decision_payload.get("limits"))
    cap4_active = bool(limits.get("cap4_canary_active"))
    return _instagram_backfill_runbook_state(
        state="cap4_canary" if cap4_active else "active",
        cap4_canary_active=cap4_active,
    )


def _budget_blocked_metadata(decision: Mapping[str, Any] | None) -> dict[str, Any]:
    decision_payload = _metadata_dict(decision)
    state = str(decision_payload.get("state") or "").strip().lower()
    if state not in {"paused", "identity_blocked"}:
        return {}
    reasons = [
        str(reason or "").strip() for reason in list(decision_payload.get("reasons") or []) if str(reason or "").strip()
    ]
    return {
        "state": state,
        "reason": reasons[0] if reasons else state,
        "reasons": reasons,
        "lane": str(decision_payload.get("lane") or "instagram_backfill").strip() or "instagram_backfill",
        "account": str(decision_payload.get("account") or "").strip() or None,
        "runbook_state": _budget_runbook_state(decision_payload),
    }


def _budget_max_concurrent_jobs(decision: Mapping[str, Any] | None) -> int | None:
    limits = _metadata_dict((decision or {}).get("limits"))
    if "effective_max_concurrent_jobs" not in limits:
        return None
    return _normalize_non_negative_int(limits.get("effective_max_concurrent_jobs"))


def _apply_budget_worker_limit(worker_count: int | None, decision: Mapping[str, Any] | None) -> int | None:
    max_concurrent = _budget_max_concurrent_jobs(decision)
    if max_concurrent is None or max_concurrent <= 0:
        return worker_count
    if worker_count is None:
        return max_concurrent
    return max(1, min(int(worker_count), max_concurrent))


def _instagram_backfill_healthy_worker_ceiling(budget_decision: Mapping[str, Any] | None = None) -> int:
    binding_cap = _instagram_backfill_live_apply_worker_cap()
    raw = str(os.getenv("TRR_INSTAGRAM_BACKFILL_HEALTHY_WORKERS") or "").strip()
    try:
        requested = int(raw) if raw else binding_cap
    except ValueError:
        requested = binding_cap
    limits = _metadata_dict(_metadata_dict(budget_decision).get("limits"))
    if limits.get("cap4_canary_active"):
        return max(1, _normalize_non_negative_int(limits.get("cap4_canary_max_concurrent_jobs")) or requested)
    return max(1, min(requested, binding_cap))


def _instagram_backfill_worker_plan(
    *,
    selected_tasks: Sequence[Any] | None,
    target_readiness: Mapping[str, Any] | None = None,
    budget_decision: Mapping[str, Any] | None = None,
    details_refresh_worker_count: int | None = None,
    comments_worker_count: int | None = None,
) -> dict[str, Any]:
    tasks = _normalize_optional_social_account_catalog_backfill_selected_tasks(selected_tasks) or []
    task_set = set(tasks)
    budget_state = str(_metadata_dict(budget_decision).get("state") or "").strip().lower() or "unknown"
    readiness = _metadata_dict(target_readiness)
    blockers = [
        str(reason or "").strip()
        for reason in [
            *list(readiness.get("blocker_reasons") or []),
            *list(readiness.get("comments_blocker_reasons") or []),
        ]
        if str(reason or "").strip()
    ]
    runbook_state = _budget_runbook_state(budget_decision)
    blocked_budget = _budget_blocked_metadata(budget_decision)
    safe_ceiling = _instagram_backfill_healthy_worker_ceiling(budget_decision)
    max_budget = _budget_max_concurrent_jobs(budget_decision)
    effective_ceiling = (
        0
        if blocked_budget
        else safe_ceiling
        if max_budget is None or max_budget <= 0
        else min(safe_ceiling, max_budget)
    )
    healthy = bool(not blocked_budget and budget_state == "normal" and not blockers and effective_ceiling > 1)
    requested_details = _normalize_non_negative_int(details_refresh_worker_count) or None
    requested_comments = _normalize_non_negative_int(comments_worker_count) or None

    details_workers = requested_details
    comments_workers = requested_comments
    reasons: list[str] = []
    if healthy and "post_details" in task_set and details_workers is None:
        details_workers = effective_ceiling
        reasons.append("healthy_post_detail_ramp")
    if healthy and "comments" in task_set and comments_workers is None:
        target_count = _normalize_non_negative_int(readiness.get("comments_target_source_ids_count"))
        comments_workers = min(effective_ceiling, target_count) if target_count > 0 else effective_ceiling
        reasons.append("healthy_comments_ramp")

    details_workers = _apply_budget_worker_limit(details_workers, budget_decision)
    comments_workers = _apply_budget_worker_limit(comments_workers, budget_decision)
    payload = {
        "state": "blocked_budget" if blocked_budget else "ramped" if reasons else "unchanged",
        "healthy": healthy,
        "budget_state": budget_state,
        "safe_ceiling": safe_ceiling,
        "effective_ceiling": effective_ceiling,
        "runbook_state": runbook_state,
        "requested_details_worker_count": requested_details,
        "requested_comments_worker_count": requested_comments,
        "details_refresh_worker_count": details_workers,
        "comments_worker_count": comments_workers,
        "reasons": (
            ["blocked_budget", *blocked_budget.get("reasons", [])]
            if blocked_budget
            else reasons or (["budget_not_normal"] if budget_state != "normal" else blockers or ["no_ramp_needed"])
        ),
    }
    if blocked_budget:
        payload["blocked_budget"] = blocked_budget
    return payload


def _worker_count_from_plan(plan: Mapping[str, Any] | None, key: str) -> int | None:
    value = _metadata_dict(plan).get(key)
    normalized = _normalize_non_negative_int(value)
    return normalized or None


def _instagram_db_session_capacity_for_launch(
    *,
    selected_tasks: Sequence[Any] | None,
    details_worker_count: int | None,
    comments_worker_count: int | None,
    raw_details_worker_count: int | None = None,
    raw_comments_worker_count: int | None = None,
    enforce: bool,
    conn: Any | None = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.budget import get_instagram_db_session_capacity

    tasks = set(_normalize_social_account_catalog_backfill_selected_tasks(selected_tasks))
    effective_requested_workers = 0
    raw_requested_workers = 0
    if "post_details" in tasks:
        effective_requested_workers += max(1, _normalize_non_negative_int(details_worker_count))
        raw_requested_workers += max(
            1,
            _normalize_non_negative_int(
                details_worker_count if raw_details_worker_count is None else raw_details_worker_count
            ),
        )
    if "comments" in tasks:
        effective_requested_workers += max(1, _normalize_non_negative_int(comments_worker_count))
        raw_requested_workers += max(
            1,
            _normalize_non_negative_int(
                comments_worker_count if raw_comments_worker_count is None else raw_comments_worker_count
            ),
        )
    capacity = get_instagram_db_session_capacity(
        requested_workers=effective_requested_workers,
        raw_requested_workers=raw_requested_workers,
        backend_effective_requested_workers=effective_requested_workers,
        conn=conn,
    )
    if enforce and (
        capacity.get("session_pool_blocked")
        or not capacity.get("available")
    ):
        reason = (
            "session_pool_capacity"
            if capacity.get("session_pool_blocked")
            or "emaxconnsession" in str(capacity.get("read_error") or "").lower()
            or "maxclientsinsessionmode" in str(capacity.get("read_error") or "").lower()
            else "database_unavailable"
        )
        raise pg.DatabaseServiceUnavailableError(
            str(capacity.get("read_error") or capacity.get("block_reason") or "Database capacity unavailable."),
            reason=reason,
        )
    if enforce and capacity.get("blocked"):
        if capacity.get("session_pool_blocked"):
            message = (
                "Instagram backfill needs "
                f"{capacity['requested_workers']} session-control slots but only "
                f"{capacity.get('session_pool_remaining_sessions') or 0} of "
                f"{capacity.get('session_pool_limit') or 0} observed Supabase session slots remain."
            )
        else:
            message = (
                "Instagram backfill needs "
                f"{capacity['requested_workers']} workers but only {capacity['remaining_workers']} "
                f"of {capacity['worker_budget']} DB-safe worker slots remain."
            )
        raise SocialIngestConflictError(
            "INSTAGRAM_DB_SESSION_WORKER_BUDGET_EXCEEDED",
            message,
            detail={"db_session_capacity": capacity},
        )
    return capacity


def get_instagram_catalog_launch_capacity(
    account_handle: str,
    *,
    selected_tasks: Sequence[Any] | None,
    details_refresh_worker_count: int | None = None,
    comments_worker_count: int | None = None,
    enable_cap4_canary: bool = False,
) -> dict[str, Any]:
    """Return the same capacity decision the backend launch gate will enforce."""

    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_tasks = _normalize_social_account_catalog_backfill_selected_tasks(selected_tasks)
    budget_decision = _instagram_backfill_budget_decision(
        normalized_account,
        enable_cap4_canary=enable_cap4_canary,
    )
    worker_plan = _instagram_backfill_worker_plan(
        selected_tasks=normalized_tasks,
        budget_decision=budget_decision,
        details_refresh_worker_count=details_refresh_worker_count,
        comments_worker_count=comments_worker_count,
    )
    capacity = _instagram_db_session_capacity_for_launch(
        selected_tasks=normalized_tasks,
        details_worker_count=_worker_count_from_plan(worker_plan, "details_refresh_worker_count"),
        comments_worker_count=_worker_count_from_plan(worker_plan, "comments_worker_count"),
        raw_details_worker_count=details_refresh_worker_count,
        raw_comments_worker_count=comments_worker_count,
        enforce=False,
    )
    selected_task_set = set(normalized_tasks)
    return {
        **capacity,
        "account_handle": normalized_account,
        "selected_tasks": normalized_tasks,
        "effective_details_worker_count": (
            _worker_count_from_plan(worker_plan, "details_refresh_worker_count") or 0
            if "post_details" in selected_task_set
            else 0
        ),
        "effective_comments_worker_count": (
            _worker_count_from_plan(worker_plan, "comments_worker_count") or 0
            if "comments" in selected_task_set
            else 0
        ),
        "adaptive_worker_plan": worker_plan,
    }


def _with_instagram_db_session_capacity(
    budget_decision: Mapping[str, Any] | None,
    capacity: Mapping[str, Any],
) -> dict[str, Any] | None:
    if budget_decision is None:
        return None
    return {**dict(budget_decision), "db_session_capacity": dict(capacity)}


def _instagram_db_session_admission_config(
    *,
    selected_tasks: Sequence[Any] | None,
    details_worker_count: int | None,
    comments_worker_count: int | None,
    raw_details_worker_count: int | None,
    raw_comments_worker_count: int | None,
    budget_decision: Mapping[str, Any] | None,
    conn: Any,
) -> dict[str, Any]:
    capacity = _instagram_db_session_capacity_for_launch(
        selected_tasks=selected_tasks,
        details_worker_count=details_worker_count,
        comments_worker_count=comments_worker_count,
        raw_details_worker_count=raw_details_worker_count,
        raw_comments_worker_count=raw_comments_worker_count,
        enforce=True,
        conn=conn,
    )
    admitted_budget = _with_instagram_db_session_capacity(budget_decision, capacity)
    return {
        "db_session_capacity": capacity,
        **({"budget_decision": admitted_budget} if admitted_budget else {}),
    }


def _catalog_launch_timing_payload(
    *,
    coverage_ms: float = 0.0,
    catalog_launch_ms: float = 0.0,
    comments_launch_ms: float = 0.0,
    launch_started_at: float,
    worker_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    total_ms = round((time_module.perf_counter() - launch_started_at) * 1000, 1)
    per_stage_ms = {
        "target_readiness": round(float(coverage_ms or 0.0), 1),
        "catalog_dispatch": round(float(catalog_launch_ms or 0.0), 1),
        "comments_dispatch": round(float(comments_launch_ms or 0.0), 1),
        "launch_total": total_ms,
    }
    return {
        "coverage_ms": per_stage_ms["target_readiness"],
        "catalog_launch_ms": per_stage_ms["catalog_dispatch"],
        "comments_launch_ms": per_stage_ms["comments_dispatch"],
        "total_ms": total_ms,
        "per_stage_ms": per_stage_ms,
        "worker_plan": _metadata_dict(worker_plan),
    }


_SNAPSHOT_PARTS_BY_TASK = {
    "post_details": (
        POST_DETAIL_PART,
        CANONICAL_POST_ROW_PART,
        MEDIA_ASSETS_PART,
        COLLABORATORS_PART,
        TAGS_PART,
        LOCATION_PART,
        MUSIC_PART,
        AD_FLAGS_PART,
    ),
    "comments": (COMMENTS_PART, REPLIES_PART),
    "media": (HOSTED_MEDIA_PART, COMMENT_MEDIA_PART, AUTHOR_AVATAR_PART),
}


def _initial_instagram_completion_metadata(
    *,
    account_handle: str,
    effective_selected_tasks: Sequence[Any] | None,
) -> dict[str, Any]:
    selected_tasks = _normalize_optional_social_account_catalog_backfill_selected_tasks(effective_selected_tasks)
    expected_parts: list[str] = []
    for task in selected_tasks:
        for part in _SNAPSHOT_PARTS_BY_TASK.get(task, ()):
            if part not in expected_parts:
                expected_parts.append(part)
    snapshot_summary = build_snapshot_completion_summary(
        expected_parts=expected_parts,
        deferred_parts={
            part: {"reason": "pending_backfill_dispatch", "account_handle": account_handle} for part in expected_parts
        },
        account_handle=account_handle,
        target_metadata={"selected_tasks": selected_tasks},
    ).to_metadata()
    media_completion = build_media_completion_payload()
    media_completion.update(
        {
            "status": "pending" if "media" in selected_tasks else "not_selected",
            "completed": "media" not in selected_tasks,
            "target": {"account_handle": account_handle},
        }
    )
    return {
        "snapshot_completion_summary": snapshot_summary,
        "media_completion": media_completion,
    }


def _posts_auth_probe_requires_manual_checkpoint(probe: Mapping[str, Any] | None) -> bool:
    reason = str(_metadata_dict(probe).get("reason") or "").strip().lower()
    return reason in {
        "checkpoint_required",
        "challenge_required",
        "instagram_graphql_checkpoint_required",
        "instagram_posts_warmup_auth_failed",
    }


def _posts_launch_probe_failure_status(*, reason: str | None, retryable: bool) -> str:
    normalized_reason = str(reason or "").strip().lower()
    if retryable or normalized_reason in {
        "requests_fallback_no_connection",
        "requests_fallback_unavailable",
        "requests_fallback_reverse_unsupported",
        "instagram_posts_warmup_transport_error",
        "transport_error",
        "transport_timeout",
    }:
        return "transport_blocked"
    return "fetch_blocked"


def _probe_instagram_posts_endpoint_for_launch(*, account_handle: str) -> dict[str, Any]:
    """Probe the profile-post GraphQL endpoint without persisting any posts."""

    async def _probe() -> dict[str, Any]:
        from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher
        from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy
        from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

        session = resolve_posts_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"posts_launch_auth_probe:{account_handle}",
        )
        cookie_fingerprint = _instagram_cookie_fingerprint(session.auth_session.cookies)[:16]
        validation_reason = str(getattr(session.auth_session, "validation_reason", "") or "").strip().lower()
        validation_category = str(getattr(session.auth_session, "validation_category", "") or "").strip().lower()
        if not bool(getattr(session.auth_session, "validated", False)) and (
            validation_reason in {"checkpoint_required", "challenge_required"}
            or validation_category in {"checkpoint_required", "challenge_required"}
        ):
            return {
                "mode": "profile_posts_endpoint",
                "account_handle": account_handle,
                "status": "auth_blocked",
                "result": "auth_blocked",
                "reason": "checkpoint_required",
                "retryable": False,
                "request_count": 0,
                "posts_seen": 0,
                "has_next_page": False,
                "doc_id_used": None,
                "profile_posts_doc_ids": None,
                "proxy_identity": None,
                "cookie_fingerprint": cookie_fingerprint,
                "cookie_fingerprint_algorithm": "sha256:16",
                "auth_source": str(session.auth_session.source or "").strip() or None,
                "auth_validation_category": validation_category or None,
            }
        proxy_session_key = str(session.browser_account_id or account_handle).strip().lower().lstrip("@")
        fetcher = InstagramPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=select_posts_proxy(session_key=proxy_session_key or account_handle),
            fast_mode=True,
            allow_requests_recovery=False,
        )
        try:
            await fetcher.warmup(account_handle)
            result = await fetcher.fetch_posts_page(account_handle, cursor=None)
            metadata = _metadata_dict(fetcher.runtime_metadata)
            if bool(result.auth_failed):
                status = "auth_blocked"
            elif bool(result.fetch_failed):
                status = _posts_launch_probe_failure_status(
                    reason=str(result.fetch_reason or "").strip() or None,
                    retryable=bool(result.retryable),
                )
            else:
                status = "valid"
            return {
                "mode": "profile_posts_endpoint",
                "account_handle": account_handle,
                "status": status,
                "result": status,
                "reason": str(result.fetch_reason or "").strip() or None,
                "retryable": bool(result.retryable),
                "request_count": int(result.request_count or metadata.get("request_count") or 0),
                "posts_seen": len(result.posts or []),
                "has_next_page": bool(result.has_next_page),
                "doc_id_used": _metadata_dict(metadata.get("profile_posts_doc_ids")).get("used"),
                "profile_posts_doc_ids": _metadata_dict(metadata.get("profile_posts_doc_ids")),
                "proxy_identity": _metadata_dict(metadata.get("proxy_identity")),
                "cookie_fingerprint": cookie_fingerprint,
                "cookie_fingerprint_algorithm": "sha256:16",
                "auth_source": str(session.auth_session.source or "").strip() or None,
            }
        finally:
            await fetcher.aclose()

    try:
        return asyncio.run(_probe())
    except Exception as exc:  # noqa: BLE001
        error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        if error_code in {
            "instagram_posts_warmup_auth_failed",
            "instagram_posts_warmup_no_cookies",
            "instagram_posts_cookie_bridge_failed",
            "instagram_posts_auth_failed",
        }:
            status = "auth_blocked"
            retryable = False
        elif error_code in {"instagram_posts_warmup_transport_error"}:
            status = "transport_blocked"
            retryable = True
        else:
            status = "transport_blocked"
            retryable = True
        return {
            "mode": "profile_posts_endpoint",
            "account_handle": account_handle,
            "status": status,
            "result": status,
            "reason": error_code or exc.__class__.__name__,
            "retryable": retryable,
            "exception_class": exc.__class__.__name__,
        }


def _ensure_instagram_posts_auth_ready_for_launch(*, account_handle: str) -> dict[str, Any]:
    if not _instagram_posts_launch_auth_check_enabled():
        return _posts_launch_auth_metadata()

    first_probe = _probe_instagram_posts_endpoint_for_launch(account_handle=account_handle)
    first_status = str(first_probe.get("status") or first_probe.get("result") or "").strip().lower()
    if first_status == "valid":
        return _posts_launch_auth_metadata(status="skipped", probe=first_probe)
    if first_status != "auth_blocked":
        return _posts_launch_auth_metadata(
            status="skipped",
            reason=str(first_probe.get("reason") or first_status or "posts_auth_probe_not_valid").strip() or None,
            probe=first_probe,
        )
    if _posts_auth_probe_requires_manual_checkpoint(first_probe):
        return _posts_launch_auth_metadata(
            attempted=False,
            status="failed",
            reason="checkpoint_required",
            probe=first_probe,
        )

    repair_result = refresh_platform_cookies_interactive(
        "instagram",
        headless=True,
        timeout_seconds=300,
        account_handle=account_handle,
    )
    repair_payload = _metadata_dict(repair_result)
    if not bool(repair_payload.get("success")):
        reason = str(repair_payload.get("reason") or "instagram_auth_repair_failed").strip().lower()
        return _posts_launch_auth_metadata(
            attempted=True,
            status="failed",
            reason=reason,
            probe=first_probe,
            repair_result=repair_payload,
        )

    second_probe = _probe_instagram_posts_endpoint_for_launch(account_handle=account_handle)
    second_status = str(second_probe.get("status") or second_probe.get("result") or "").strip().lower()
    if second_status == "valid":
        return _posts_launch_auth_metadata(
            attempted=True,
            status="succeeded",
            probe=second_probe,
            repair_result=repair_payload,
        )

    reason = str(second_probe.get("reason") or second_status or "posts_auth_probe_failed_after_repair").strip()
    return _posts_launch_auth_metadata(
        attempted=True,
        status="failed",
        reason=reason or "posts_auth_probe_failed_after_repair",
        probe=second_probe,
        repair_result=repair_payload,
    )


def _blocked_instagram_posts_launch_payload(
    *,
    run_id: str | None,
    account_handle: str,
    source_scope: str,
    launch_group_id: str,
    selected_tasks: Sequence[Any] | None,
    effective_selected_tasks: Sequence[Any] | None,
    posts_auth_metadata: Mapping[str, Any],
    timing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    public_auth_metadata = _public_posts_launch_auth_metadata(posts_auth_metadata)
    reason = str(public_auth_metadata.get("auth_repair_reason") or "posts_auth_blocked").strip().lower()
    normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(selected_tasks)
    normalized_effective_tasks = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        effective_selected_tasks
    )
    metadata_updates = {
        "launch_state": "blocked_auth",
        "launch_task_resolution_pending": False,
        "launch_completed_at": _iso(_now_utc()),
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": normalized_effective_tasks,
        "partial_scrape": True,
        "stop_reason": "checkpoint_required" if "checkpoint" in reason else "posts_auth_blocked",
        "blocked_reason": reason,
        **public_auth_metadata,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=normalized_effective_tasks,
            detail_status="blocked",
            comments_status="blocked" if "comments" in normalized_effective_tasks else "skipped",
            comments_blocker_reasons=["posts_auth_blocked"] if "comments" in normalized_effective_tasks else [],
            media_status="blocked" if "media" in normalized_effective_tasks else "skipped",
            enrichment_status="blocked",
            finalization_status="completed",
            timing=timing,
        ),
    }
    normalized_run_id = str(run_id or "").strip() or None
    if normalized_run_id:
        _merge_catalog_run_config(run_id=normalized_run_id, metadata_updates=metadata_updates)
        _set_run_status(normalized_run_id, "failed")
    return {
        "run_id": normalized_run_id,
        "status": "failed",
        "platform": "instagram",
        "account_handle": account_handle,
        "launch_group_id": launch_group_id,
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": normalized_effective_tasks,
        "catalog_run_id": normalized_run_id,
        "comments_run_id": None,
        "catalog_status": "failed",
        "comments_status": None,
        "catalog_bootstrap_required": None,
        "comments_deferred_until_catalog_complete": False,
        "attached_followups": {},
        "partial_scrape": True,
        "stop_reason": metadata_updates["stop_reason"],
        "blocked_reason": reason,
        **public_auth_metadata,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=normalized_effective_tasks,
            detail_status="blocked",
            comments_status="blocked" if "comments" in normalized_effective_tasks else "skipped",
            comments_blocker_reasons=["posts_auth_blocked"] if "comments" in normalized_effective_tasks else [],
            media_status="blocked" if "media" in normalized_effective_tasks else "skipped",
            enrichment_status="blocked",
            finalization_status="completed",
            timing=timing,
        ),
    }


def _instagram_catalog_backfill_force_detail_fetch_enabled() -> bool:
    raw = (os.getenv("SOCIAL_INSTAGRAM_CATALOG_BACKFILL_FORCE_DETAIL_FETCH") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return True


_CATALOG_STAGE_GRAPH_STAGES = (
    "target_readiness",
    "detail_refresh",
    "comments",
    "media",
    "enrichment",
    "finalization",
)


def _catalog_stage_entry(
    status: str,
    *,
    selected: bool = False,
    blocker_reasons: Sequence[Any] | None = None,
    timing_ms: float | None = None,
    **extra: Any,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "status": str(status or "pending").strip().lower() or "pending",
        "selected": bool(selected),
        "blocker_reasons": [
            str(reason or "").strip() for reason in list(blocker_reasons or []) if str(reason or "").strip()
        ],
    }
    if timing_ms is not None:
        entry["timing_ms"] = round(float(timing_ms or 0.0), 1)
    for key, value in extra.items():
        if value is not None:
            entry[key] = value
    return entry


def _catalog_stage_graph(
    *,
    selected_tasks: Sequence[Any] | None,
    effective_selected_tasks: Sequence[Any] | None,
    target_readiness: Mapping[str, Any] | None = None,
    detail_status: str | None = None,
    comments_status: str | None = None,
    comments_blocker_reasons: Sequence[Any] | None = None,
    media_status: str | None = None,
    enrichment_status: str | None = None,
    finalization_status: str | None = None,
) -> dict[str, dict[str, Any]]:
    effective_tasks = set(
        _normalize_optional_social_account_catalog_backfill_selected_tasks(effective_selected_tasks) or []
    )
    selected = set(_normalize_optional_social_account_catalog_backfill_selected_tasks(selected_tasks) or [])
    readiness = _metadata_dict(target_readiness)
    readiness_blockers = list(readiness.get("blocker_reasons") or [])
    comments_blockers = [
        str(reason or "").strip()
        for reason in list(comments_blocker_reasons or readiness.get("comments_blocker_reasons") or [])
        if str(reason or "").strip()
    ]
    graph = {
        "target_readiness": _catalog_stage_entry(
            str(readiness.get("status") or ("completed" if readiness else "pending")),
            selected=bool(effective_tasks & {"comments", "media", "post_details"}),
            blocker_reasons=readiness_blockers,
            timing_ms=readiness.get("timing_ms"),
            saved_source_ids_count=_normalize_non_negative_int(readiness.get("saved_source_ids_count")),
            commentable_target_count=_normalize_non_negative_int(readiness.get("commentable_target_count")),
            comments_target_source_ids_count=_normalize_non_negative_int(
                readiness.get("comments_target_source_ids_count")
            ),
            detail_gap_count=_normalize_non_negative_int(readiness.get("detail_gap_count")),
        ),
        "detail_refresh": _catalog_stage_entry(
            detail_status or ("pending" if "post_details" in effective_tasks else "skipped"),
            selected="post_details" in selected or "post_details" in effective_tasks,
        ),
        "comments": _catalog_stage_entry(
            comments_status or ("pending" if "comments" in effective_tasks else "skipped"),
            selected="comments" in selected or "comments" in effective_tasks,
            blocker_reasons=comments_blockers,
        ),
        "media": _catalog_stage_entry(
            media_status or ("pending" if "media" in effective_tasks else "skipped"),
            selected="media" in selected or "media" in effective_tasks,
        ),
        "enrichment": _catalog_stage_entry(
            enrichment_status or "pending",
            selected=bool(effective_tasks & {"post_details", "media"}),
        ),
        "finalization": _catalog_stage_entry(finalization_status or "pending", selected=True),
    }
    return {stage: graph[stage] for stage in _CATALOG_STAGE_GRAPH_STAGES}


def _comments_status_from_posts_stage(
    *,
    platform: str,
    effective_selected_tasks: Sequence[Any] | None,
    job_status: str | None,
) -> str:
    selected = set(_normalize_optional_social_account_catalog_backfill_selected_tasks(effective_selected_tasks) or [])
    if "comments" not in selected:
        return "skipped"
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform in {"tiktok", "twitter", "youtube", "threads"}:
        return str(job_status or "").strip().lower() or "pending"
    return "pending"


def _catalog_stage_graph_metadata(
    *,
    selected_tasks: Sequence[Any] | None,
    effective_selected_tasks: Sequence[Any] | None,
    target_readiness: Mapping[str, Any] | None = None,
    detail_status: str | None = None,
    comments_status: str | None = None,
    comments_blocker_reasons: Sequence[Any] | None = None,
    media_status: str | None = None,
    enrichment_status: str | None = None,
    finalization_status: str | None = None,
    timing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "pipeline_strategy": "stage_graph",
        "stage_graph": _catalog_stage_graph(
            selected_tasks=selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            target_readiness=target_readiness,
            detail_status=detail_status,
            comments_status=comments_status,
            comments_blocker_reasons=comments_blocker_reasons,
            media_status=media_status,
            enrichment_status=enrichment_status,
            finalization_status=finalization_status,
        ),
    }
    if target_readiness:
        metadata["target_readiness"] = _metadata_dict(target_readiness)
    timing_payload = _metadata_dict(timing)
    if timing_payload:
        metadata["timing"] = timing_payload
    return metadata


def _catalog_comments_blockers_from_error(exc: Exception) -> list[str]:
    code = str(getattr(exc, "code", "") or "").strip().upper()
    message = str(exc or "").strip().lower()
    if code == "SOCIAL_ACCOUNT_COMMENTS_NOTHING_TO_REFRESH":
        if "saved instagram posts" in message:
            return ["missing_source_ids"]
        return ["target_count_zero"]
    if code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED":
        if "checkpoint" in message:
            return ["checkpoint_required"]
        return ["auth_probe_failed"]
    return [code.lower() if code else "comments_launch_failed"]


def _comments_skip_str_list(value: Any) -> list[str]:
    """Coerce a config value into a defensive lowercased string list."""

    if value is None:
        return []
    if isinstance(value, str):
        items: Sequence[Any] = [value]
    elif isinstance(value, Mapping):
        items = list(value.keys())
    elif isinstance(value, Sequence):
        items = list(value)
    else:
        items = [value]
    return [text for text in (str(item or "").strip().lower() for item in items) if text]


def derive_comments_skip_reason(run_config: Mapping[str, Any]) -> dict[str, Any]:
    """Derive an honest, run-config-driven reason the comments stage was skipped.

    Returns a dict with keys ``reason``, ``detail`` and ``operator_action``. The
    precedence order is deliberate (first matching rule wins) and the function is
    defensive against missing/``None`` config keys. It never emits the literal
    string ``"manual checkpoint"`` -- the posts-auth checkpoint path describes the
    concrete operator remediation instead.
    """

    config = _metadata_dict(run_config)

    stage_graph = _metadata_dict(config.get("stage_graph"))
    comments_stage = _metadata_dict(stage_graph.get("comments"))
    effective_selected_tasks = _comments_skip_str_list(config.get("effective_selected_tasks"))
    comments_selected = bool(comments_stage.get("selected")) or "comments" in effective_selected_tasks

    target_readiness = _metadata_dict(config.get("target_readiness"))
    posts_auth_probe = _metadata_dict(config.get("posts_auth_probe"))
    stop_reason = str(config.get("stop_reason") or "").strip().lower()
    comments_blocker_reasons = _comments_skip_str_list(
        comments_stage.get("blocker_reasons")
        or target_readiness.get("comments_blocker_reasons")
        or config.get("comments_blocker_reasons")
    )

    # 1. Comments were never selected for this run.
    if not comments_selected:
        return {
            "reason": "comments_not_selected",
            "detail": "comments task not selected for this run",
            "operator_action": "Relaunch with the comments task selected to scrape comments.",
        }

    # 2. Posts/auth checkpoint blocked the run before comments could start.
    posts_auth_reason = str(posts_auth_probe.get("reason") or "").strip().lower()
    posts_auth_checkpoint = posts_auth_reason in {
        "checkpoint_required",
        "challenge_required",
        "instagram_graphql_checkpoint_required",
        "instagram_posts_warmup_auth_failed",
    }
    if (
        stop_reason == "checkpoint_required"
        or posts_auth_checkpoint
        or "posts_auth_blocked" in comments_blocker_reasons
        or "checkpoint_required" in comments_blocker_reasons
    ):
        return {
            "reason": "posts_auth_blocked",
            "detail": "checkpoint_required",
            "operator_action": (
                "Resolve the Instagram checkpoint in-app, re-login the account browser "
                "session, refresh cookies, then relaunch (no automated solver by design)."
            ),
        }

    # 3. A deferred comments follow-up is the owner of comments launch. Surface
    # that state before generic target-readiness blockers so operators do not
    # see "no targets" while a catalog-completion follow-up is pending/retryable.
    deferred_followup = _metadata_dict(config.get("deferred_comments_followup"))
    deferred_state = str(deferred_followup.get("state") or "").strip().lower()
    if (
        deferred_state in {"pending", "failed", "failed_exhausted"}
        or "comments_deferred_pending_discovery" in comments_blocker_reasons
        or "comments_deferred_pending_catalog_targets" in comments_blocker_reasons
    ):
        retryable = bool(deferred_followup.get("retryable"))
        if deferred_state == "failed_exhausted":
            return {
                "reason": "deferred_comments_followup_failed_exhausted",
                "detail": str(deferred_followup.get("error_message") or "retry budget exhausted").strip(),
                "operator_action": "Relaunch comments manually after checking the saved catalog targets.",
            }
        if deferred_state == "failed":
            return {
                "reason": "deferred_comments_followup_failed",
                "detail": str(deferred_followup.get("error_message") or "deferred follow-up launch failed").strip(),
                "operator_action": (
                    "Wait for the retry sweep or relaunch comments manually."
                    if retryable
                    else "Relaunch comments manually after fixing the recorded follow-up failure."
                ),
            }
        return {
            "reason": "comments_deferred_until_catalog_complete",
            "detail": "catalog completion follow-up owns comments launch",
            "operator_action": "Wait for catalog completion to start the deferred comments run.",
        }

    # 4. No eligible comment targets were available.
    can_start_comments = bool(target_readiness.get("can_start_comments"))
    commentable_target_count = _normalize_non_negative_int(target_readiness.get("commentable_target_count"))
    if not can_start_comments or commentable_target_count == 0:
        return {
            "reason": "no_commentable_targets",
            "detail": "no eligible comment targets available",
            "operator_action": "Backfill posts first so commentable targets exist, then relaunch comments.",
        }

    # 5. Public-first lane is healthy; an authenticated probe was not requested.
    if "strict_authenticated_probe_not_requested" in comments_blocker_reasons:
        return {
            "reason": "authenticated_comments_not_requested",
            "detail": "public lane healthy",
            "operator_action": (
                "Relaunch with the strict authenticated comments probe requested to run "
                "the authenticated comments lane."
            ),
        }

    # 6. Default: nothing is blocking; comments are running or already complete.
    return {
        "reason": "comments_running_or_complete",
        "detail": "comments stage running or already complete",
        "operator_action": "No action required; monitor the comments stage progress.",
    }


def build_instagram_backfill_target_readiness(
    account_handle: str,
    *,
    coverage: Mapping[str, Any] | None = None,
    refresh_policy: str = "stale_or_missing",
) -> dict[str, Any]:
    started_at = time_module.perf_counter()
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    coverage_payload = _metadata_dict(coverage)
    blocker_reasons: list[str] = []
    comments_blocker_reasons: list[str] = []
    try:
        target_counts = _room_callable(
            "_instagram_social_account_comments_target_counts",
            _instagram_social_account_comments_target_counts,
        )(normalized_account)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[catalog-launch] target readiness counts failed account=%s error=%s",
            normalized_account,
            exc,
        )
        target_counts = {}
        blocker_reasons.append("target_readiness_failed")
    try:
        preview = _room_callable(
            "preview_social_account_comments_scrape",
            _preview_comments_scrape,
        )(
            "instagram",
            normalized_account,
            mode="profile",
            refresh_policy=refresh_policy,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[catalog-launch] target readiness preview failed account=%s error=%s",
            normalized_account,
            exc,
        )
        preview = {}
        if "target_readiness_failed" not in blocker_reasons:
            blocker_reasons.append("target_readiness_failed")

    saved_source_ids_count = max(
        _normalize_non_negative_int(coverage_payload.get("materialized_posts")),
        _normalize_non_negative_int(coverage_payload.get("catalog_posts")),
        _normalize_non_negative_int(target_counts.get("available_posts")),
    )
    comments_target_source_ids_count = _normalize_non_negative_int(preview.get("target_source_ids_count"))
    commentable_target_count = _normalize_non_negative_int(target_counts.get("eligible_posts"))
    detail_gap_counts = _metadata_dict(coverage_payload.get("detail_gap_counts"))
    detail_gap_count = _normalize_non_negative_int(detail_gap_counts.get("posts_needing_detail_refresh"))
    if saved_source_ids_count <= 0:
        blocker_reasons.append("missing_source_ids")
        comments_blocker_reasons.append("missing_source_ids")
    if comments_target_source_ids_count <= 0:
        comments_blocker_reasons.append("target_count_zero")
    can_start_comments = saved_source_ids_count > 0 and comments_target_source_ids_count > 0
    status = "completed" if not blocker_reasons else "blocked"
    return {
        "status": status,
        "account_handle": normalized_account,
        "saved_source_ids_count": saved_source_ids_count,
        "commentable_target_count": commentable_target_count,
        "comments_target_source_ids_count": comments_target_source_ids_count,
        "sample_target_source_ids": _as_text_list(preview.get("sample_target_source_ids"))[
            : _instagram_backfill_minimum_sample_floor()
        ],
        "incomplete_comment_target_count": _normalize_non_negative_int(target_counts.get("missing_posts"))
        + _normalize_non_negative_int(target_counts.get("stale_posts")),
        "media_candidate_count": saved_source_ids_count,
        "detail_gap_count": detail_gap_count,
        "can_start_comments": can_start_comments,
        "blocker_reasons": list(dict.fromkeys(blocker_reasons)),
        "comments_blocker_reasons": list(dict.fromkeys(comments_blocker_reasons)),
        "refresh_policy": str(refresh_policy or "stale_or_missing").strip().lower() or "stale_or_missing",
        "timing_ms": round((time_module.perf_counter() - started_at) * 1000, 1),
        "comments_preview": {
            "comments_shard_count": _normalize_non_negative_int(preview.get("comments_shard_count")) or None,
            "comments_sharding_enabled": bool(preview.get("comments_sharding_enabled")),
            "recommended_comments_shard_count": _normalize_non_negative_int(
                preview.get("recommended_comments_shard_count")
            )
            or None,
            "target_priority": str(preview.get("target_priority") or "").strip() or None,
        },
    }


def start_social_account_catalog_backfill(
    platform: str,
    account_handle: str,
    *,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    inline_worker_id: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    resume_frontier_cursor: str | None = None,
    resume_frontier_snapshot: Mapping[str, Any] | None = None,
    catalog_action: str | None = None,
    catalog_action_scope: str | None = None,
    social_account_post_details_only: bool = False,
    details_refresh_skip_detail_fetch: bool | None = None,
    details_refresh_force_detail_fetch: bool | None = None,
    details_refresh_worker_count: int | None = None,
    comments_worker_count: int | None = None,
    comments_enable_media_followups: bool | None = None,
    details_refresh_skip_media_followups: bool | None = None,
    tiktok_comments_in_posts_stage: bool = False,
    tiktok_direct_comment_api_override: bool = False,
    twitter_comments_in_posts_stage: bool = False,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    selected_tasks: Sequence[Any] | None = None,
    effective_selected_tasks: Sequence[Any] | None = None,
    launch_group_id: str | None = None,
    existing_run_id: str | None = None,
    reserved_db_session_capacity: Mapping[str, Any] | None = None,
    enable_cap4_canary: bool = False,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    source_scope = _normalize_catalog_source_scope(source_scope)
    normalized_execution_preference = str(execution_preference or "auto").strip().lower() or "auto"
    if normalized_execution_preference not in {"auto", "prefer_local_inline"}:
        normalized_execution_preference = "auto"
    action_seed = resolve_social_account_catalog_action_seed(
        date_start=date_start,
        date_end=date_end,
        resume_frontier_cursor=resume_frontier_cursor,
        catalog_action=catalog_action,
        catalog_action_scope=catalog_action_scope,
    )
    normalized_date_start = action_seed["date_start"]
    normalized_date_end = action_seed["date_end"]
    normalized_resume_cursor = action_seed["resume_frontier_cursor"]
    normalized_catalog_action = action_seed["catalog_action"]
    normalized_catalog_action_scope = action_seed["catalog_action_scope"]
    # tp-1/tp-2: opt-in bounded-window skip-ahead. When launching a NEW bounded run
    # with no explicit resume cursor, seed it from a prior completed bounded run's
    # LOSSLESS resume cursor so an older-year run skips re-walking the already-scraped
    # newer region. Gated off by default; fail-safe because the resume cursor
    # re-fetches the straddling page (worst case is harmless overlap, never data loss).
    if (
        normalized_resume_cursor is None
        and normalized_date_start
        and normalized_date_end
        and _bounded_window_skip_ahead_enabled()
    ):
        _skip_candidate = _bounded_window_skip_ahead_candidate(
            normalized_platform,
            normalized_account,
            new_date_end=_coerce_dt(normalized_date_end),
        )
        if _skip_candidate:
            normalized_resume_cursor = _skip_candidate["resume_frontier_cursor"]
            resume_frontier_snapshot = {
                **dict(resume_frontier_snapshot or {}),
                "skip_ahead": True,
                "skip_ahead_from_date_start": _skip_candidate.get("candidate_date_start"),
                "skip_ahead_from_date_end": _skip_candidate.get("candidate_date_end"),
            }
    normalized_resume_snapshot = dict(resume_frontier_snapshot or {}) if normalized_resume_cursor else None
    if normalized_platform not in set(CATALOG_SUPPORTED_PLATFORMS):
        raise ValueError("Catalog backfill is not supported for this platform.")
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    budget_decision = (
        _instagram_backfill_budget_decision(normalized_account, enable_cap4_canary=enable_cap4_canary)
        if normalized_platform == "instagram"
        else None
    )
    adaptive_worker_plan = (
        _instagram_backfill_worker_plan(
            selected_tasks=effective_selected_tasks or selected_tasks,
            budget_decision=budget_decision,
            details_refresh_worker_count=details_refresh_worker_count,
            comments_worker_count=comments_worker_count,
        )
        if normalized_platform == "instagram"
        else {}
    )
    effective_details_worker_count = (
        _worker_count_from_plan(adaptive_worker_plan, "details_refresh_worker_count")
        if adaptive_worker_plan
        else _apply_budget_worker_limit(details_refresh_worker_count, budget_decision)
    )
    effective_comments_worker_count = (
        _worker_count_from_plan(adaptive_worker_plan, "comments_worker_count")
        if adaptive_worker_plan
        else _apply_budget_worker_limit(comments_worker_count, budget_decision)
    )
    run_id = str(existing_run_id or "").strip() or None
    reserved_here = run_id is None
    db_session_capacity = (
        _metadata_dict(reserved_db_session_capacity) or None
        if normalized_platform == "instagram" and not reserved_here
        else None
    )
    if db_session_capacity:
        budget_decision = _with_instagram_db_session_capacity(budget_decision, db_session_capacity)
    if run_id and not reserved_here and launch_group_id and _catalog_launch_parent_cancelled(run_id):
        return _catalog_launch_parent_result(_catalog_launch_parent_snapshot(run_id))
    if reserved_here:
        admission_callback = None
        if normalized_platform == "instagram":

            def admission_callback(lock_conn: Any) -> dict[str, Any]:
                return _instagram_db_session_admission_config(
                    selected_tasks=effective_selected_tasks or selected_tasks,
                    details_worker_count=effective_details_worker_count,
                    comments_worker_count=effective_comments_worker_count,
                    raw_details_worker_count=details_refresh_worker_count,
                    raw_comments_worker_count=comments_worker_count,
                    budget_decision=budget_decision,
                    conn=lock_conn,
                )
        initial_completion_metadata = (
            _initial_instagram_completion_metadata(
                account_handle=normalized_account,
                effective_selected_tasks=effective_selected_tasks or selected_tasks,
            )
            if normalized_platform == "instagram"
            else {}
        )
        reservation = _reserve_social_account_catalog_launch(
            platform=normalized_platform,
            account_handle=normalized_account,
            source_scope=source_scope,
            initiated_by=initiated_by,
            placeholder_config={
                **_build_social_account_catalog_launch_placeholder_config(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    source_scope=source_scope,
                    date_start=normalized_date_start,
                    date_end=normalized_date_end,
                    allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                    execution_preference=normalized_execution_preference,
                    launch_group_id=launch_group_id,
                    resume_frontier_cursor=normalized_resume_cursor,
                    catalog_action=normalized_catalog_action,
                    catalog_action_scope=normalized_catalog_action_scope,
                    comments_worker_count=effective_comments_worker_count,
                    comments_enable_media_followups=comments_enable_media_followups,
                    task_resolution_pending=False,
                    comment_anchor_source_ids=comment_anchor_source_ids,
                ),
                **({"enable_cap4_canary": True} if enable_cap4_canary else {}),
                **({"budget_decision": budget_decision} if budget_decision else {}),
                **({"db_session_capacity": db_session_capacity} if db_session_capacity else {}),
                **({"adaptive_worker_plan": adaptive_worker_plan} if adaptive_worker_plan else {}),
                **initial_completion_metadata,
                **_catalog_stage_graph_metadata(
                    selected_tasks=[],
                    effective_selected_tasks=[],
                    finalization_status="pending",
                ),
            },
            initial_status=_catalog_launch_initial_status(
                allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            ),
            admission_callback=admission_callback,
        )
        reservation_updates = _metadata_dict(reservation.get("config_updates"))
        db_session_capacity = _metadata_dict(reservation_updates.get("db_session_capacity")) or None
        admitted_budget = _metadata_dict(reservation_updates.get("budget_decision"))
        if admitted_budget:
            budget_decision = admitted_budget
        elif db_session_capacity:
            budget_decision = _with_instagram_db_session_capacity(budget_decision, db_session_capacity)
        run_id = str(reservation.get("run_id") or "").strip() or None
        if reservation.get("deduped"):
            return _catalog_launch_parent_result(reservation)
        logger.info(
            "[catalog-launch] kickoff_reserved platform=%s account=%s run_id=%s lock_wait_ms=%.1f lock_held_ms=%.1f",
            normalized_platform,
            normalized_account,
            run_id,
            float(reservation.get("lock_wait_ms") or 0.0),
            float(reservation.get("lock_held_ms") or 0.0),
        )

    try:
        if (
            is_queue_enabled()
            and not allow_local_dev_inline_bypass
            and _shared_account_catalog_requires_modal_executor(
                platform=normalized_platform,
                pipeline_ingest_mode=SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            )
        ):
            assert_worker_available_when_queue_enabled(
                required_execution_backend="modal",
                platform=normalized_platform,
                account_handle=normalized_account,
            )
        skip_implicit_frontier_resume = (
            normalized_platform == "instagram"
            and normalized_catalog_action == "backfill"
            and normalized_catalog_action_scope == "full_history"
        )
        if (
            not skip_implicit_frontier_resume
            and normalized_date_start is None
            and normalized_date_end is None
            and normalized_resume_cursor is None
        ):
            frontier = _latest_account_frontier(normalized_platform, normalized_account)
            next_cursor = str(frontier.get("next_cursor") or "").strip() or None
            if next_cursor and not frontier.get("exhausted"):
                normalized_resume_cursor = next_cursor
                normalized_resume_snapshot = {
                    "id": frontier.get("id"),
                    "run_id": frontier.get("run_id"),
                    "next_cursor": next_cursor,
                    "total_posts": frontier.get("total_posts"),
                    "posts_checked": frontier.get("posts_checked") or 0,
                    "posts_saved": frontier.get("posts_saved") or 0,
                    "pages_scanned": frontier.get("pages_scanned") or 0,
                    "last_transport": frontier.get("last_transport"),
                }
        ingest_kwargs = {
            "platforms": [normalized_platform],
            "source_scope": source_scope,
            "accounts_override": [normalized_account],
            "date_start": normalized_date_start,
            "date_end": normalized_date_end,
            "pipeline_ingest_mode": SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            "initiated_by": initiated_by,
            "inline_worker_id": inline_worker_id,
            "allow_local_dev_inline_bypass": allow_local_dev_inline_bypass,
            "execution_preference": normalized_execution_preference,
            "allow_ephemeral_accounts_override_sources": True,
            "resume_frontier_cursor": normalized_resume_cursor,
            "resume_frontier_snapshot": normalized_resume_snapshot,
            "catalog_action": normalized_catalog_action,
            "catalog_action_scope": normalized_catalog_action_scope,
            "social_account_post_details_only": social_account_post_details_only,
            "details_refresh_skip_detail_fetch": details_refresh_skip_detail_fetch,
            "details_refresh_force_detail_fetch": details_refresh_force_detail_fetch,
            "details_refresh_worker_count": effective_details_worker_count,
            "details_refresh_skip_media_followups": details_refresh_skip_media_followups,
            "tiktok_comments_in_posts_stage": tiktok_comments_in_posts_stage,
            "tiktok_direct_comment_api_override": tiktok_direct_comment_api_override,
            "selected_tasks": selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
            "comment_anchor_source_ids": comment_anchor_source_ids,
            "launch_group_id": launch_group_id,
            "existing_run_id": run_id,
            "defer_initial_dispatch": not reserved_here,
        }
        ingest_shared_account_args = getattr(
            getattr(ingest_shared_accounts, "__code__", None),
            "co_varnames",
            (),
        )
        if "comments_worker_count" in ingest_shared_account_args:
            ingest_kwargs["comments_worker_count"] = effective_comments_worker_count
        if "comments_enable_media_followups" in ingest_shared_account_args:
            ingest_kwargs["comments_enable_media_followups"] = comments_enable_media_followups
        if "twitter_comments_in_posts_stage" in getattr(
            getattr(ingest_shared_accounts, "__code__", None),
            "co_varnames",
            (),
        ):
            ingest_kwargs["twitter_comments_in_posts_stage"] = twitter_comments_in_posts_stage
        result = ingest_shared_accounts(**ingest_kwargs)
        if run_id and not reserved_here and launch_group_id:
            cancelled = _cancel_launch_group_if_parent_cancelled(
                run_id=run_id,
                platform=normalized_platform,
                account_handle=normalized_account,
            )
            if cancelled:
                return cancelled
        if run_id:
            _merge_catalog_run_config(
                run_id=run_id,
                metadata_updates={
                    "launch_state": "ready" if reserved_here else "finalizing",
                    "launch_task_resolution_pending": not reserved_here,
                    **({"launch_completed_at": _iso(_now_utc())} if reserved_here else {}),
                    **({"enable_cap4_canary": True} if enable_cap4_canary else {}),
                    **({"budget_decision": budget_decision} if budget_decision else {}),
                    **({"db_session_capacity": db_session_capacity} if db_session_capacity else {}),
                    **({"adaptive_worker_plan": adaptive_worker_plan} if adaptive_worker_plan else {}),
                    **_catalog_stage_graph_metadata(
                        selected_tasks=_normalize_optional_social_account_catalog_backfill_selected_tasks(
                            (result or {}).get("selected_tasks")
                        )
                        or [],
                        effective_selected_tasks=_normalize_optional_social_account_catalog_backfill_selected_tasks(
                            (result or {}).get("effective_selected_tasks")
                        )
                        or [],
                        finalization_status="completed",
                    ),
                },
            )
        if db_session_capacity:
            return {
                **dict(result or {}),
                "budget_decision": budget_decision,
                "db_session_capacity": db_session_capacity,
                "adaptive_worker_plan": adaptive_worker_plan,
            }
        return result
    except Exception as exc:  # noqa: BLE001
        if reserved_here and run_id:
            _record_social_account_catalog_launch_failure(
                run_id=run_id,
                error_message=str(exc),
            )
        raise


def begin_social_account_catalog_backfill_launch(
    platform: str,
    account_handle: str,
    *,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    selected_tasks: Sequence[Any] | None = None,
    details_refresh_worker_count: int | None = None,
    comments_worker_count: int | None = None,
    comments_enable_media_followups: bool | None = None,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    force_catalog_rediscovery: bool = False,
    enable_cap4_canary: bool = False,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if normalized_platform not in set(CATALOG_SUPPORTED_PLATFORMS):
        raise ValueError("Catalog backfill is not supported for this platform.")
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    normalized_execution_preference = str(execution_preference or "auto").strip().lower() or "auto"
    if normalized_execution_preference not in {"auto", "prefer_local_inline"}:
        normalized_execution_preference = "auto"
    action_seed = resolve_social_account_catalog_action_seed(
        date_start=date_start,
        date_end=date_end,
        catalog_action="backfill",
    )
    launch_group_id = str(uuid4())
    normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(selected_tasks)
    budget_decision = (
        _instagram_backfill_budget_decision(normalized_account, enable_cap4_canary=enable_cap4_canary)
        if normalized_platform == "instagram"
        else None
    )
    adaptive_worker_plan = (
        _instagram_backfill_worker_plan(
            selected_tasks=normalized_selected_tasks,
            budget_decision=budget_decision,
            details_refresh_worker_count=details_refresh_worker_count,
            comments_worker_count=comments_worker_count,
        )
        if normalized_platform == "instagram"
        else {}
    )
    effective_details_worker_count = (
        _worker_count_from_plan(adaptive_worker_plan, "details_refresh_worker_count")
        if adaptive_worker_plan
        else _apply_budget_worker_limit(details_refresh_worker_count, budget_decision)
    )
    effective_comments_worker_count = (
        _worker_count_from_plan(adaptive_worker_plan, "comments_worker_count")
        if adaptive_worker_plan
        else _apply_budget_worker_limit(comments_worker_count, budget_decision)
    )
    db_session_capacity: dict[str, Any] | None = None
    admission_callback = None
    if normalized_platform == "instagram":

        def admission_callback(lock_conn: Any) -> dict[str, Any]:
            return _instagram_db_session_admission_config(
                selected_tasks=normalized_selected_tasks,
                details_worker_count=effective_details_worker_count,
                comments_worker_count=effective_comments_worker_count,
                raw_details_worker_count=details_refresh_worker_count,
                raw_comments_worker_count=comments_worker_count,
                budget_decision=budget_decision,
                conn=lock_conn,
            )
    initial_completion_metadata = (
        _initial_instagram_completion_metadata(
            account_handle=normalized_account,
            effective_selected_tasks=normalized_selected_tasks,
        )
        if normalized_platform == "instagram"
        else {}
    )
    reservation = _reserve_social_account_catalog_launch(
        platform=normalized_platform,
        account_handle=normalized_account,
        source_scope=source_scope,
        initiated_by=initiated_by,
        placeholder_config={
            **_build_social_account_catalog_launch_placeholder_config(
                platform=normalized_platform,
                account_handle=normalized_account,
                source_scope=source_scope,
                date_start=action_seed["date_start"],
                date_end=action_seed["date_end"],
                allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                execution_preference=normalized_execution_preference,
                launch_group_id=launch_group_id,
                resume_frontier_cursor=action_seed["resume_frontier_cursor"],
                catalog_action=action_seed["catalog_action"],
                catalog_action_scope=action_seed["catalog_action_scope"],
                selected_tasks=normalized_selected_tasks,
                details_refresh_worker_count=effective_details_worker_count,
                comments_worker_count=effective_comments_worker_count,
                comments_enable_media_followups=comments_enable_media_followups,
                comment_anchor_source_ids=comment_anchor_source_ids,
                force_catalog_rediscovery=bool(force_catalog_rediscovery),
                task_resolution_pending=True,
            ),
            "launch_state": "reserved",
            **({"enable_cap4_canary": True} if enable_cap4_canary else {}),
            **({"budget_decision": budget_decision} if budget_decision else {}),
            **({"db_session_capacity": db_session_capacity} if db_session_capacity else {}),
            **({"adaptive_worker_plan": adaptive_worker_plan} if adaptive_worker_plan else {}),
            **initial_completion_metadata,
            **_catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=normalized_selected_tasks,
                finalization_status="pending",
            ),
        },
        initial_status=_catalog_launch_initial_status(
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
        ),
        admission_callback=admission_callback,
    )
    reservation_updates = _metadata_dict(reservation.get("config_updates"))
    db_session_capacity = _metadata_dict(reservation_updates.get("db_session_capacity")) or None
    admitted_budget = _metadata_dict(reservation_updates.get("budget_decision"))
    if admitted_budget:
        budget_decision = admitted_budget
    elif db_session_capacity:
        budget_decision = _with_instagram_db_session_capacity(budget_decision, db_session_capacity)
    run_id = str(reservation.get("run_id") or "").strip()
    if reservation.get("deduped"):
        return _catalog_launch_parent_result(reservation)
    initial_status = _catalog_launch_initial_status(
        allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
    )
    logger.info(
        (
            "[catalog-launch] kickoff_reserved platform=%s account=%s run_id=%s status=%s "
            "lock_wait_ms=%.1f lock_held_ms=%.1f"
        ),
        normalized_platform,
        normalized_account,
        run_id,
        initial_status,
        float(reservation.get("lock_wait_ms") or 0.0),
        float(reservation.get("lock_held_ms") or 0.0),
    )
    return {
        "run_id": run_id,
        "deduped": bool(reservation.get("deduped", False)),
        "status": initial_status,
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "launch_group_id": launch_group_id,
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": normalized_selected_tasks,
        "catalog_run_id": run_id,
        "comments_run_id": None,
        "catalog_status": initial_status,
        "comments_status": None,
        "catalog_bootstrap_required": None,
        "comments_deferred_until_catalog_complete": False,
        "post_details_skipped_reason": None,
        "launch_state": "reserved",
        "launch_task_resolution_pending": True,
        "attached_followups": {},
        "enable_cap4_canary": bool(enable_cap4_canary),
        "budget_decision": budget_decision,
        "db_session_capacity": db_session_capacity,
        "adaptive_worker_plan": adaptive_worker_plan,
        **initial_completion_metadata,
        "catalog_action": action_seed["catalog_action"],
        "catalog_action_scope": action_seed["catalog_action_scope"],
        "ingest_mode": SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=normalized_selected_tasks,
            finalization_status="pending",
        ),
    }


class CatalogLaunchTimeout(Exception):  # noqa: N818
    """Raised when catalog launch finalization exceeds its umbrella timeout (B2).

    Recoverable by design: callers leave the run in launch_state="finalizing" so the
    stale-finalizing recovery sweep re-drives it on a fresh worker. The underlying
    worker thread cannot be force-killed and is abandoned; its DB work stays bounded by
    the connection's statement_timeout.
    """

    def __init__(self, *, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(f"Catalog launch finalization exceeded {timeout_seconds:g}s timeout")


_CATALOG_FINALIZE_LAUNCH_TIMEOUT_DEFAULT_S = 100.0


def _catalog_launch_group_lock_key(launch_group_id: str) -> int:
    normalized = str(launch_group_id or "").strip().lower()
    return int(hashlib.md5(f"catalog-launch-group:{normalized}".encode()).hexdigest()[:15], 16) % (2**31)


@contextmanager
def _catalog_launch_group_transaction_lock(launch_group_id: str):
    """Try one transaction advisory lock for the full launch-owner lifetime."""

    with pg.db_connection(label="catalog-launch-group-owner") as conn:
        with pg.db_cursor(conn=conn, label="catalog-launch-group-owner") as cur:
            row = pg.fetch_one_with_cursor(
                cur,
                "select pg_try_advisory_xact_lock(%s) as locked",
                [_catalog_launch_group_lock_key(launch_group_id)],
            )
        yield bool((row or {}).get("locked"))


def _catalog_launch_parent_snapshot(run_id: str) -> dict[str, Any]:
    row = _load_catalog_run_row_by_id(run_id)
    config = _metadata_dict(row.get("config"))
    return {
        **dict(row or {}),
        "config": config,
        "launch_state": str(config.get("launch_state") or "").strip().lower() or None,
        "launch_group_id": str(config.get("launch_group_id") or "").strip() or None,
    }


def _catalog_launch_parent_cancelled(run_id: str) -> bool:
    parent = _catalog_launch_parent_snapshot(run_id)
    return str(parent.get("status") or "").strip().lower() in {"cancelled", "cancelling"} or str(
        parent.get("launch_state") or ""
    ).strip().lower() in {"cancelled", "cancelling"}


def _catalog_launch_parent_result(parent: Mapping[str, Any], *, owner_active: bool = False) -> dict[str, Any]:
    config = _metadata_dict(parent.get("config"))
    run_id = str(parent.get("run_id") or parent.get("id") or "").strip() or None
    launch_state = str(parent.get("launch_state") or config.get("launch_state") or "").strip().lower() or None
    return {
        "run_id": run_id,
        "catalog_run_id": run_id,
        "comments_run_id": str(config.get("comments_run_id") or "").strip() or None,
        "status": str(parent.get("status") or "").strip().lower() or None,
        "catalog_status": str(parent.get("status") or "").strip().lower() or None,
        "launch_group_id": str(config.get("launch_group_id") or "").strip() or None,
        "launch_state": launch_state,
        "launch_task_resolution_pending": launch_state in {"reserved", "pending", "finalizing"},
        "selected_tasks": _normalize_optional_social_account_catalog_backfill_selected_tasks(
            config.get("selected_tasks")
        ),
        "effective_selected_tasks": _normalize_optional_social_account_catalog_backfill_selected_tasks(
            config.get("effective_selected_tasks")
        ),
        "attached_followups": _metadata_dict(config.get("attached_followups")),
        "budget_decision": _metadata_dict(config.get("budget_decision")) or None,
        "db_session_capacity": _metadata_dict(config.get("db_session_capacity")) or None,
        "finalizer_owner_active": owner_active,
    }


def _cancel_launch_group_if_parent_cancelled(
    *,
    run_id: str | None,
    platform: str,
    account_handle: str,
) -> dict[str, Any] | None:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not _catalog_launch_parent_cancelled(normalized_run_id):
        return None
    try:
        from trr_backend.socials.control_plane.shared_accounts import cancel_social_account_catalog_run

        cancel_social_account_catalog_run(
            platform=platform,
            account_handle=account_handle,
            run_id=normalized_run_id,
            cancelled_by="catalog_parent_cancelled",
            reconcile_summary=False,
        )
    except Exception:  # noqa: BLE001 - parent cancellation still wins even if the drain audit fails
        logger.exception("[catalog-launch] launch_group_cancel_sweep_failed run_id=%s", normalized_run_id)
    return _catalog_launch_parent_result(_catalog_launch_parent_snapshot(normalized_run_id))


def _cas_catalog_launch_state(
    *,
    run_id: str,
    launch_group_id: str,
    from_states: Sequence[str],
    to_state: str,
    metadata_updates: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    updates = {"launch_state": to_state, **dict(metadata_updates or {})}
    row = pg.fetch_one(
        """
        update social.scrape_runs
        set config = coalesce(config, '{}'::jsonb) || %s::jsonb
        where id = %s::uuid
          and coalesce(config->>'launch_group_id', '') = %s
          and lower(coalesce(config->>'launch_state', 'reserved')) = any(%s::text[])
          and lower(coalesce(status, '')) not in ('cancelled', 'cancelling')
        returning id::text as id, id::text as run_id, status, config, summary
        """,
        [_json_dumps(updates), run_id, launch_group_id, [str(state).strip().lower() for state in from_states]],
    )
    return dict(row or {})


def _catalog_finalize_launch_timeout_seconds() -> float:
    """Umbrella timeout (seconds) for catalog launch finalization.

    Default 100s sits just under the 120s stale-finalizing recovery grace so a timed-out
    launch is re-driven on the next sweep. Set TRR_CATALOG_FINALIZE_LAUNCH_TIMEOUT_S=0 to
    disable the timeout entirely.
    """
    raw = str(os.getenv("TRR_CATALOG_FINALIZE_LAUNCH_TIMEOUT_S") or "").strip()
    if not raw:
        return _CATALOG_FINALIZE_LAUNCH_TIMEOUT_DEFAULT_S
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "[catalog-launch] invalid TRR_CATALOG_FINALIZE_LAUNCH_TIMEOUT_S=%r; using default %ss",
            raw,
            _CATALOG_FINALIZE_LAUNCH_TIMEOUT_DEFAULT_S,
        )
        return _CATALOG_FINALIZE_LAUNCH_TIMEOUT_DEFAULT_S
    return value if value > 0 else 0.0


def _run_catalog_launch_with_timeout(fn: Any, *, timeout_seconds: float) -> Any:
    """Run the (synchronous) catalog launch under an umbrella timeout.

    On timeout, raise CatalogLaunchTimeout and ABANDON the worker thread without waiting:
    the launch may be wedged on a slow probe, and blocking here would defeat the timeout
    and keep the recovery advisory lock held. ``shutdown(wait=False)`` never blocks; the
    abandoned thread's DB work is bounded by the connection statement_timeout.
    """
    if not timeout_seconds or timeout_seconds <= 0:
        return fn()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="catalog-finalize-launch")
    future = executor.submit(fn)
    try:
        return future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError as exc:
        raise CatalogLaunchTimeout(timeout_seconds=timeout_seconds) from exc
    finally:
        executor.shutdown(wait=False)


def finalize_social_account_catalog_backfill_launch(
    platform: str,
    account_handle: str,
    *,
    run_id: str,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    selected_tasks: Sequence[Any] | None = None,
    details_refresh_worker_count: int | None = None,
    comments_worker_count: int | None = None,
    comments_enable_media_followups: bool | None = None,
    launch_group_id: str | None = None,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    catalog_action: str | None = None,
    catalog_action_scope: str | None = None,
    force_catalog_rediscovery: bool = False,
    enable_cap4_canary: bool = False,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    started_at = time_module.perf_counter()
    initial_parent = {} if launch_group_id else _catalog_launch_parent_snapshot(run_id)
    normalized_launch_group_id = (
        str(launch_group_id or initial_parent.get("launch_group_id") or "").strip()
    )
    if not normalized_launch_group_id:
        raise ValueError("Catalog launch group is missing.")

    def _finalize_once() -> dict[str, Any]:
        with _catalog_launch_group_transaction_lock(normalized_launch_group_id) as acquired:
            parent = _catalog_launch_parent_snapshot(run_id)
            if not acquired:
                return _catalog_launch_parent_result(parent, owner_active=True)
            if _catalog_launch_parent_cancelled(run_id):
                return _catalog_launch_parent_result(parent)
            if str(parent.get("launch_state") or "") in {"ready", "completed_no_work"}:
                return _catalog_launch_parent_result(parent)

            transitioned = _cas_catalog_launch_state(
                run_id=run_id,
                launch_group_id=normalized_launch_group_id,
                from_states=("reserved", "pending", "finalizing"),
                to_state="finalizing",
                metadata_updates={
                    "launch_task_resolution_pending": True,
                    "launch_finalizing_started_at": _iso(_now_utc()),
                    **({"enable_cap4_canary": True} if enable_cap4_canary else {}),
                },
            )
            if not transitioned:
                return _catalog_launch_parent_result(_catalog_launch_parent_snapshot(run_id))

            _launch_callable = _room_callable(
                "launch_social_account_catalog_backfill",
                launch_social_account_catalog_backfill,
            )
            result = _launch_callable(
                normalized_platform,
                normalized_account,
                source_scope=source_scope,
                date_start=date_start,
                date_end=date_end,
                initiated_by=initiated_by,
                allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                execution_preference=execution_preference,
                selected_tasks=selected_tasks,
                details_refresh_worker_count=details_refresh_worker_count,
                comments_worker_count=comments_worker_count,
                comments_enable_media_followups=comments_enable_media_followups,
                comment_anchor_source_ids=comment_anchor_source_ids,
                existing_catalog_run_id=run_id,
                launch_group_id_override=normalized_launch_group_id,
                catalog_action=catalog_action,
                catalog_action_scope=catalog_action_scope,
                force_catalog_rediscovery=force_catalog_rediscovery,
                enable_cap4_canary=enable_cap4_canary,
                reserved_db_session_capacity=_metadata_dict(
                    _metadata_dict(parent.get("config")).get("db_session_capacity")
                )
                or None,
            )
            cancelled = _cancel_launch_group_if_parent_cancelled(
                run_id=run_id,
                platform=normalized_platform,
                account_handle=normalized_account,
            )
            if cancelled:
                return cancelled
            ready = _cas_catalog_launch_state(
                run_id=run_id,
                launch_group_id=normalized_launch_group_id,
                from_states=("finalizing",),
                to_state="ready",
                metadata_updates={
                    "launch_task_resolution_pending": False,
                    "launch_completed_at": _iso(_now_utc()),
                    "launch_finalize_timeout": False,
                },
            )
            if not ready:
                return _catalog_launch_parent_result(_catalog_launch_parent_snapshot(run_id))
            return {**dict(result or {}), "launch_state": "ready", "launch_task_resolution_pending": False}

    try:
        result = _run_catalog_launch_with_timeout(
            _finalize_once,
            timeout_seconds=_catalog_finalize_launch_timeout_seconds(),
        )
        logger.info(
            "[catalog-launch] finalize_complete platform=%s account=%s run_id=%s total_ms=%.1f comments_run_id=%s",
            normalized_platform,
            normalized_account,
            run_id,
            round((time_module.perf_counter() - started_at) * 1000, 1),
            str(result.get("comments_run_id") or "").strip() or None,
        )
        return result
    except CatalogLaunchTimeout as exc:
        # B2: do NOT hard-fail the run. Leave launch_state="finalizing" so the
        # stale-finalizing recovery sweep re-drives it on a fresh worker. Record only a
        # lightweight, observable marker. launch_finalizing_started_at is untouched, so
        # the 120s staleness clock keeps counting from this attempt.
        _merge_catalog_run_config(
            run_id=run_id,
            metadata_updates={
                "launch_finalize_timeout": True,
                "launch_finalize_timeout_at": _iso(_now_utc()),
                "launch_finalize_timeout_seconds": exc.timeout_seconds,
            },
        )
        logger.warning(
            "[catalog-launch] finalize_timeout platform=%s account=%s run_id=%s timeout_s=%s "
            "— left finalizing for recovery sweep",
            normalized_platform,
            normalized_account,
            run_id,
            exc.timeout_seconds,
        )
        return _catalog_launch_parent_result(_catalog_launch_parent_snapshot(run_id), owner_active=True)
    except Exception as exc:  # noqa: BLE001
        if not _catalog_launch_parent_cancelled(run_id):
            _record_social_account_catalog_launch_failure(
                run_id=run_id,
                error_message=str(exc),
            )
        logger.exception(
            "[catalog-launch] finalize_failed platform=%s account=%s run_id=%s",
            normalized_platform,
            normalized_account,
            run_id,
        )
        raise


def _force_catalog_rediscovery_env(platform: str, account_handle: str) -> bool:
    raw = (os.getenv("TRR_SOCIAL_FORCE_CATALOG_REDISCOVERY_ACCOUNTS") or "").strip()
    if not raw:
        return False
    if raw == "*":
        return True
    wanted = {p.strip().lower().lstrip("@") for p in raw.split(",") if p.strip()}
    return str(account_handle or "").strip().lower().lstrip("@") in wanted


def launch_social_account_catalog_backfill(
    platform: str,
    account_handle: str,
    *,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    inline_worker_id: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    selected_tasks: Sequence[Any] | None = None,
    details_refresh_worker_count: int | None = None,
    comments_worker_count: int | None = None,
    comments_enable_media_followups: bool | None = None,
    existing_catalog_run_id: str | None = None,
    launch_group_id_override: str | None = None,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    catalog_action: str | None = None,
    catalog_action_scope: str | None = None,
    force_catalog_rediscovery: bool = False,
    enable_cap4_canary: bool = False,
    reserved_db_session_capacity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    source_scope = _normalize_catalog_source_scope(source_scope)
    effective_force_catalog_rediscovery = bool(force_catalog_rediscovery) or _force_catalog_rediscovery_env(
        normalized_platform,
        normalized_account,
    )
    launch_started_at = time_module.perf_counter()
    coverage_ms = 0.0
    catalog_launch_ms = 0.0
    comments_launch_ms = 0.0
    normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(selected_tasks)
    normalized_comment_anchor_source_ids, _comment_anchor_overflow_platforms = _normalize_comment_anchor_source_ids(
        comment_anchor_source_ids,
        allowed_platforms={normalized_platform},
    )
    instagram_targeted_comment_source_ids = (
        sorted(normalized_comment_anchor_source_ids.get("instagram") or [])
        if normalized_platform == "instagram"
        else []
    )
    instagram_targeted_comments_only = bool(
        normalized_platform == "instagram"
        and instagram_targeted_comment_source_ids
        and set(normalized_selected_tasks) <= {"comments"}
    )
    normalized_execution_preference = str(execution_preference or "auto").strip().lower() or "auto"
    if normalized_execution_preference not in {"auto", "prefer_local_inline"}:
        normalized_execution_preference = "auto"
    action_seed = resolve_social_account_catalog_action_seed(
        date_start=date_start,
        date_end=date_end,
        catalog_action=catalog_action,
        catalog_action_scope=catalog_action_scope,
    )
    normalized_date_start = action_seed["date_start"]
    normalized_date_end = action_seed["date_end"]
    normalized_catalog_action = action_seed["catalog_action"]
    normalized_catalog_action_scope = action_seed["catalog_action_scope"]
    bounded_window_scope = normalized_catalog_action_scope
    budget_decision = (
        _instagram_backfill_budget_decision(normalized_account, enable_cap4_canary=enable_cap4_canary)
        if normalized_platform == "instagram"
        else None
    )
    effective_details_worker_count = _apply_budget_worker_limit(details_refresh_worker_count, budget_decision)
    effective_comments_worker_count = _apply_budget_worker_limit(comments_worker_count, budget_decision)
    adaptive_worker_plan: dict[str, Any] = {}
    if normalized_platform == "tiktok":
        launch_group_id = str(launch_group_id_override or uuid4())
        effective_selected_tasks = _effective_social_account_catalog_backfill_selected_tasks(
            normalized_platform,
            normalized_selected_tasks,
        )
        catalog_launch_started_at = time_module.perf_counter()
        catalog_result = _room_callable(
            "start_social_account_catalog_backfill",
            start_social_account_catalog_backfill,
        )(
            normalized_platform,
            normalized_account,
            source_scope=source_scope,
            date_start=normalized_date_start,
            date_end=normalized_date_end,
            initiated_by=initiated_by,
            inline_worker_id=inline_worker_id,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            catalog_action=normalized_catalog_action,
            catalog_action_scope=normalized_catalog_action_scope,
            details_refresh_skip_detail_fetch="post_details" not in effective_selected_tasks,
            details_refresh_skip_media_followups="media" not in effective_selected_tasks,
            tiktok_comments_in_posts_stage="comments" in effective_selected_tasks,
            tiktok_direct_comment_api_override=(
                "comments" in effective_selected_tasks and _tiktok_catalog_comment_override_enabled()
            ),
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            comment_anchor_source_ids=comment_anchor_source_ids,
            launch_group_id=launch_group_id,
            existing_run_id=existing_catalog_run_id,
        )
        catalog_launch_ms = round((time_module.perf_counter() - catalog_launch_started_at) * 1000, 1)
        catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip() or None
        if catalog_run_id:
            _merge_catalog_run_config(
                run_id=catalog_run_id,
                metadata_updates={
                    "selected_tasks": normalized_selected_tasks,
                    "effective_selected_tasks": effective_selected_tasks,
                    **_catalog_stage_graph_metadata(
                        selected_tasks=normalized_selected_tasks,
                        effective_selected_tasks=effective_selected_tasks,
                        detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        comments_status=_comments_status_from_posts_stage(
                            platform=normalized_platform,
                            effective_selected_tasks=effective_selected_tasks,
                            job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        ),
                        media_status=(
                            str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                            if "media" in effective_selected_tasks
                            else "skipped"
                        ),
                        finalization_status="completed",
                        timing={
                            "coverage_ms": coverage_ms,
                            "catalog_launch_ms": catalog_launch_ms,
                            "comments_launch_ms": comments_launch_ms,
                            "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                        },
                    ),
                },
            )
        logger.info(
            (
                "[catalog-launch] launch_complete platform=%s account=%s run_id=%s "
                "existing_run_id=%s coverage_ms=%.1f catalog_launch_ms=%.1f "
                "comments_launch_ms=%.1f total_ms=%.1f selected_tasks=%s "
                "effective_selected_tasks=%s"
            ),
            normalized_platform,
            normalized_account,
            catalog_run_id,
            str(existing_catalog_run_id or "").strip() or None,
            coverage_ms,
            catalog_launch_ms,
            comments_launch_ms,
            round((time_module.perf_counter() - launch_started_at) * 1000, 1),
            normalized_selected_tasks,
            effective_selected_tasks,
        )
        return {
            "run_id": catalog_run_id,
            "status": str((catalog_result or {}).get("status") or "").strip() or None,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "launch_group_id": launch_group_id,
            "selected_tasks": normalized_selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
            "post_details_skipped_reason": (
                "forced_for_comments"
                if "comments" in normalized_selected_tasks and "post_details" not in normalized_selected_tasks
                else None
            ),
            "catalog_run_id": catalog_run_id,
            "comments_run_id": None,
            "catalog_status": str((catalog_result or {}).get("status") or "").strip() or None,
            "comments_status": None,
            "catalog_action": normalized_catalog_action,
            "catalog_action_scope": normalized_catalog_action_scope,
            "catalog_bootstrap_required": False,
            "comments_deferred_until_catalog_complete": False,
            "attached_followups": {},
            **_catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                comments_status=_comments_status_from_posts_stage(
                    platform=normalized_platform,
                    effective_selected_tasks=effective_selected_tasks,
                    job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                ),
                media_status=(
                    str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                    if "media" in effective_selected_tasks
                    else "skipped"
                ),
                finalization_status="completed",
                timing={
                    "coverage_ms": coverage_ms,
                    "catalog_launch_ms": catalog_launch_ms,
                    "comments_launch_ms": comments_launch_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            ),
        }
    if normalized_platform != "instagram":
        launch_group_id = str(launch_group_id_override or uuid4())
        effective_selected_tasks = _effective_social_account_catalog_backfill_selected_tasks(
            normalized_platform,
            normalized_selected_tasks,
        )
        catalog_launch_started_at = time_module.perf_counter()
        catalog_result = _room_callable(
            "start_social_account_catalog_backfill",
            start_social_account_catalog_backfill,
        )(
            normalized_platform,
            normalized_account,
            source_scope=source_scope,
            date_start=normalized_date_start,
            date_end=normalized_date_end,
            initiated_by=initiated_by,
            inline_worker_id=inline_worker_id,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            catalog_action=normalized_catalog_action,
            catalog_action_scope=normalized_catalog_action_scope,
            social_account_post_details_only=effective_selected_tasks == ["post_details"],
            details_refresh_skip_detail_fetch="post_details" not in effective_selected_tasks,
            details_refresh_skip_media_followups="media" not in effective_selected_tasks,
            launch_group_id=launch_group_id,
            twitter_comments_in_posts_stage=(
                normalized_platform == "twitter" and "comments" in effective_selected_tasks
            ),
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            comment_anchor_source_ids=comment_anchor_source_ids,
            existing_run_id=existing_catalog_run_id,
        )
        catalog_launch_ms = round((time_module.perf_counter() - catalog_launch_started_at) * 1000, 1)
        catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip() or None
        if catalog_run_id:
            _merge_catalog_run_config(
                run_id=catalog_run_id,
                metadata_updates={
                    "selected_tasks": normalized_selected_tasks,
                    "effective_selected_tasks": effective_selected_tasks,
                    **_catalog_stage_graph_metadata(
                        selected_tasks=normalized_selected_tasks,
                        effective_selected_tasks=effective_selected_tasks,
                        detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        comments_status=_comments_status_from_posts_stage(
                            platform=normalized_platform,
                            effective_selected_tasks=effective_selected_tasks,
                            job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        ),
                        media_status=(
                            str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                            if "media" in effective_selected_tasks
                            else "skipped"
                        ),
                        finalization_status="completed",
                        timing={
                            "coverage_ms": coverage_ms,
                            "catalog_launch_ms": catalog_launch_ms,
                            "comments_launch_ms": comments_launch_ms,
                            "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                        },
                    ),
                },
            )
        logger.info(
            (
                "[catalog-launch] launch_complete platform=%s account=%s run_id=%s "
                "existing_run_id=%s coverage_ms=%.1f catalog_launch_ms=%.1f "
                "comments_launch_ms=%.1f total_ms=%.1f selected_tasks=%s "
                "effective_selected_tasks=%s"
            ),
            normalized_platform,
            normalized_account,
            catalog_run_id,
            str(existing_catalog_run_id or "").strip() or None,
            coverage_ms,
            catalog_launch_ms,
            comments_launch_ms,
            round((time_module.perf_counter() - launch_started_at) * 1000, 1),
            normalized_selected_tasks,
            effective_selected_tasks,
        )
        return {
            "run_id": catalog_run_id,
            "status": str((catalog_result or {}).get("status") or "").strip() or None,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "launch_group_id": launch_group_id,
            "selected_tasks": normalized_selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
            "post_details_skipped_reason": (
                "forced_for_comments"
                if "comments" in normalized_selected_tasks and "post_details" not in normalized_selected_tasks
                else None
            ),
            "catalog_run_id": catalog_run_id,
            "comments_run_id": None,
            "catalog_status": str((catalog_result or {}).get("status") or "").strip() or None,
            "comments_status": None,
            "catalog_action": normalized_catalog_action,
            "catalog_action_scope": normalized_catalog_action_scope,
            "catalog_bootstrap_required": False,
            "comments_deferred_until_catalog_complete": False,
            "attached_followups": {},
            **_catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                comments_status=_comments_status_from_posts_stage(
                    platform=normalized_platform,
                    effective_selected_tasks=effective_selected_tasks,
                    job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                ),
                media_status=(
                    str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                    if "media" in effective_selected_tasks
                    else "skipped"
                ),
                finalization_status="completed",
                timing={
                    "coverage_ms": coverage_ms,
                    "catalog_launch_ms": catalog_launch_ms,
                    "comments_launch_ms": comments_launch_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            ),
        }

    launch_group_id = str(launch_group_id_override or uuid4())
    if existing_catalog_run_id and _catalog_launch_parent_cancelled(existing_catalog_run_id):
        return _catalog_launch_parent_result(_catalog_launch_parent_snapshot(existing_catalog_run_id))
    catalog_result: dict[str, Any] | None = None
    comments_result: dict[str, Any] | None = None
    deferred_comments_followup: dict[str, Any] | None = None
    attached_followups: dict[str, dict[str, Any]] = {}
    post_details_skipped_reason: str | None = None
    target_readiness: dict[str, Any] | None = None
    comments_blocker_reasons: list[str] = []
    comments_started_before_detail_complete = False
    if normalized_platform == "instagram":
        coverage_started_at = time_module.perf_counter()
        use_fast_existing_posts_launch_state = bool(
            existing_catalog_run_id
            and bounded_window_scope == "full_history"
            and "post_details" in normalized_selected_tasks
            and not effective_force_catalog_rediscovery
        )
        if use_fast_existing_posts_launch_state:
            materialized_posts = _materialized_social_account_total_posts(
                "instagram",
                normalized_account,
                date_start=normalized_date_start,
                date_end=normalized_date_end,
            )
            coverage = {
                "platform": "instagram",
                "account_handle": normalized_account,
                "catalog_posts": materialized_posts,
                "materialized_posts": materialized_posts,
                "expected_total_posts": materialized_posts,
                "completion_target_posts": materialized_posts,
                "missing_catalog_posts": 0,
                "missing_materialized_posts": 0,
                "detail_gap_counts": {},
                "details_complete": False,
                "bootstrap_required": materialized_posts <= 0,
                "fast_launch_state": True,
            }
        else:
            coverage = _instagram_materialization_state(
                normalized_account,
                date_start=normalized_date_start,
                date_end=normalized_date_end,
            )
        coverage_ms = round((time_module.perf_counter() - coverage_started_at) * 1000, 1)
        requires_catalog_bootstrap = bool(coverage.get("bootstrap_required"))
        if instagram_targeted_comments_only:
            requires_catalog_bootstrap = False
        effective_selected_tasks = list(normalized_selected_tasks)
        stored_post_count = max(
            _normalize_non_negative_int(coverage.get("materialized_posts")),
            _normalize_non_negative_int(coverage.get("catalog_posts")),
        )
        if set(effective_selected_tasks) == {"post_details"} and stored_post_count > 0:
            requires_catalog_bootstrap = False
        if effective_force_catalog_rediscovery and not instagram_targeted_comments_only:
            requires_catalog_bootstrap = True
        if "comments" in effective_selected_tasks:
            if instagram_targeted_comment_source_ids:
                target_count = len(instagram_targeted_comment_source_ids)
                comments_shard_count = _instagram_comments_profile_shard_count(target_count)
                target_readiness = {
                    "status": "completed",
                    "account_handle": normalized_account,
                    "saved_source_ids_count": target_count,
                    "commentable_target_count": target_count,
                    "comments_target_source_ids_count": target_count,
                    "sample_target_source_ids": instagram_targeted_comment_source_ids[
                        : _instagram_backfill_minimum_sample_floor()
                    ],
                    "incomplete_comment_target_count": target_count,
                    "media_candidate_count": target_count,
                    "detail_gap_count": 0,
                    "can_start_comments": True,
                    "blocker_reasons": [],
                    "comments_blocker_reasons": [],
                    "refresh_policy": "explicit_targets",
                    "explicit_comment_anchor_source_ids": True,
                    "comments_preview": {
                        "comments_shard_count": comments_shard_count,
                        "comments_sharding_enabled": comments_shard_count > 1,
                        "recommended_comments_shard_count": _instagram_comments_recommended_shard_count(
                            target_count=target_count
                        ),
                        "target_priority": "explicit_anchor",
                    },
                    "timing_ms": coverage_ms,
                }
            elif use_fast_existing_posts_launch_state and not requires_catalog_bootstrap:
                materialized_count = _normalize_non_negative_int(coverage.get("materialized_posts"))
                comments_shard_count = _instagram_comments_profile_shard_count(materialized_count)
                target_readiness = {
                    "status": "completed",
                    "account_handle": normalized_account,
                    "saved_source_ids_count": materialized_count,
                    "commentable_target_count": materialized_count,
                    "comments_target_source_ids_count": materialized_count,
                    "incomplete_comment_target_count": materialized_count,
                    "media_candidate_count": materialized_count,
                    "detail_gap_count": 0,
                    "can_start_comments": materialized_count > 0,
                    "blocker_reasons": [],
                    "comments_blocker_reasons": [],
                    "refresh_policy": "stale_or_missing",
                    "comments_preview": {
                        "comments_shard_count": comments_shard_count,
                        "comments_sharding_enabled": comments_shard_count > 1,
                        "recommended_comments_shard_count": _instagram_comments_recommended_shard_count(
                            target_count=materialized_count
                        ),
                        "target_priority": "missing_first_recent",
                    },
                    "timing_ms": coverage_ms,
                }
            elif (
                normalized_catalog_action == "backfill"
                and bounded_window_scope == "bounded_window"
                and not requires_catalog_bootstrap
                and stored_post_count > 0
                and not instagram_targeted_comment_source_ids
                and any(task in effective_selected_tasks for task in ("post_details", "media"))
            ):
                comments_shard_count = _instagram_comments_profile_shard_count(stored_post_count)
                target_readiness = {
                    "status": "completed",
                    "account_handle": normalized_account,
                    "saved_source_ids_count": stored_post_count,
                    "commentable_target_count": stored_post_count,
                    "comments_target_source_ids_count": stored_post_count,
                    "incomplete_comment_target_count": stored_post_count,
                    "media_candidate_count": stored_post_count,
                    "detail_gap_count": _normalize_non_negative_int(
                        _metadata_dict(coverage.get("detail_gap_counts")).get("posts_needing_detail_refresh")
                    ),
                    "can_start_comments": True,
                    "blocker_reasons": [],
                    "comments_blocker_reasons": [],
                    "refresh_policy": "stale_or_missing",
                    "comments_preview": {
                        "comments_shard_count": comments_shard_count,
                        "comments_sharding_enabled": comments_shard_count > 1,
                        "recommended_comments_shard_count": _instagram_comments_recommended_shard_count(
                            target_count=stored_post_count
                        ),
                        "target_priority": "bounded_existing_posts",
                    },
                    "timing_ms": coverage_ms,
                }
            elif requires_catalog_bootstrap or (
                normalized_catalog_action == "backfill"
                and bounded_window_scope == "bounded_window"
                and not instagram_targeted_comment_source_ids
                and any(task in effective_selected_tasks for task in ("post_details", "media"))
            ):
                # Catalog-backed launches will stream or attach comment work after
                # post targets exist. Avoid the full comments preview here; on large
                # accounts it can consume the launch timeout before any jobs are queued.
                deferred_reason = (
                    "comments_deferred_pending_discovery"
                    if requires_catalog_bootstrap
                    else "comments_deferred_pending_catalog_targets"
                )
                target_readiness = {
                    "status": "deferred_until_catalog_complete",
                    "account_handle": normalized_account,
                    "saved_source_ids_count": max(
                        _normalize_non_negative_int(coverage.get("materialized_posts")),
                        _normalize_non_negative_int(coverage.get("catalog_posts")),
                    ),
                    "commentable_target_count": 0,
                    "comments_target_source_ids_count": 0,
                    "incomplete_comment_target_count": 0,
                    "media_candidate_count": 0,
                    "detail_gap_count": 0,
                    "can_start_comments": False,
                    "blocker_reasons": [],
                    "comments_blocker_reasons": [deferred_reason],
                    "refresh_policy": "stale_or_missing",
                    "comments_preview": {},
                    "timing_ms": coverage_ms,
                }
            else:
                target_readiness = build_instagram_backfill_target_readiness(
                    normalized_account,
                    coverage=coverage,
                    refresh_policy="stale_or_missing",
                )
            comments_blocker_reasons = [
                str(reason or "").strip()
                for reason in list(target_readiness.get("comments_blocker_reasons") or [])
                if str(reason or "").strip()
            ]
        else:
            target_readiness = {
                "status": "completed",
                "account_handle": normalized_account,
                "saved_source_ids_count": max(
                    _normalize_non_negative_int(coverage.get("materialized_posts")),
                    _normalize_non_negative_int(coverage.get("catalog_posts")),
                ),
                "commentable_target_count": 0,
                "comments_target_source_ids_count": 0,
                "incomplete_comment_target_count": 0,
                "media_candidate_count": max(
                    _normalize_non_negative_int(coverage.get("materialized_posts")),
                    _normalize_non_negative_int(coverage.get("catalog_posts")),
                ),
                "detail_gap_count": _normalize_non_negative_int(
                    _metadata_dict(coverage.get("detail_gap_counts")).get("posts_needing_detail_refresh")
                ),
                "can_start_comments": False,
                "blocker_reasons": [],
                "comments_blocker_reasons": [],
                "timing_ms": coverage_ms,
            }
    else:
        requires_catalog_bootstrap = False
        effective_selected_tasks = list(normalized_selected_tasks)
    if normalized_platform == "instagram":
        adaptive_worker_plan = _instagram_backfill_worker_plan(
            selected_tasks=effective_selected_tasks,
            target_readiness=target_readiness,
            budget_decision=budget_decision,
            details_refresh_worker_count=details_refresh_worker_count,
            comments_worker_count=comments_worker_count,
        )
        effective_details_worker_count = _worker_count_from_plan(
            adaptive_worker_plan,
            "details_refresh_worker_count",
        ) or _apply_budget_worker_limit(details_refresh_worker_count, budget_decision)
        effective_comments_worker_count = _worker_count_from_plan(
            adaptive_worker_plan,
            "comments_worker_count",
        ) or _apply_budget_worker_limit(comments_worker_count, budget_decision)
        db_session_capacity = (
            _metadata_dict(reserved_db_session_capacity) or None if existing_catalog_run_id else None
        )
        if db_session_capacity:
            budget_decision = _with_instagram_db_session_capacity(budget_decision, db_session_capacity)
    else:
        db_session_capacity = None
    catalog_tasks = (
        [task for task in effective_selected_tasks if task in ("post_details", "comments", "media")]
        if requires_catalog_bootstrap
        else [task for task in effective_selected_tasks if task in ("post_details", "media")]
    )
    catalog_selected = bool(catalog_tasks)
    catalog_details_refresh_only = catalog_selected and not requires_catalog_bootstrap
    bounded_existing_posts_comments_run = bool(
        normalized_platform == "instagram"
        and "comments" in effective_selected_tasks
        and normalized_catalog_action == "backfill"
        and bounded_window_scope == "bounded_window"
        and catalog_details_refresh_only
        and stored_post_count > 0
        and not instagram_targeted_comment_source_ids
    )
    comments_deferred_until_catalog_complete = False
    catalog_comments_streaming_enabled = False
    comments_reused_existing_run = False
    media_attachment_id = _catalog_media_attachment_id(launch_group_id) if "media" in effective_selected_tasks else None
    force_detail_fetch = (
        "post_details" in effective_selected_tasks and _instagram_catalog_backfill_force_detail_fetch_enabled()
    )
    posts_auth_metadata: dict[str, Any] = {}
    public_posts_auth_metadata: dict[str, Any] = {}
    if catalog_selected:
        posts_auth_metadata = _ensure_instagram_posts_auth_ready_for_launch(account_handle=normalized_account)
        public_posts_auth_metadata = _public_posts_launch_auth_metadata(posts_auth_metadata)
        if public_posts_auth_metadata.get("auth_repair_status") == "failed":
            return _blocked_instagram_posts_launch_payload(
                run_id=existing_catalog_run_id,
                account_handle=normalized_account,
                source_scope=source_scope,
                launch_group_id=launch_group_id,
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                posts_auth_metadata=posts_auth_metadata,
                timing={
                    "coverage_ms": coverage_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            )

    if not catalog_selected and not any(task in effective_selected_tasks for task in ("comments", "media")):
        no_work_payload = _complete_catalog_launch_no_work(
            run_id=existing_catalog_run_id,
            platform=normalized_platform,
            account_handle=normalized_account,
            launch_group_id=launch_group_id,
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            post_details_skipped_reason=post_details_skipped_reason,
        )
        no_work_payload.update(
            _catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                detail_status="skipped",
                comments_status="skipped",
                media_status="skipped",
                enrichment_status="skipped",
                finalization_status="completed",
                timing=_catalog_launch_timing_payload(
                    coverage_ms=coverage_ms,
                    launch_started_at=launch_started_at,
                    worker_plan=adaptive_worker_plan,
                ),
            )
        )
        if budget_decision:
            no_work_payload["budget_decision"] = budget_decision
        if db_session_capacity:
            no_work_payload["db_session_capacity"] = db_session_capacity
        if adaptive_worker_plan:
            no_work_payload["adaptive_worker_plan"] = adaptive_worker_plan
        logger.info(
            (
                "[catalog-launch] launch_complete_no_work platform=%s account=%s run_id=%s "
                "existing_run_id=%s coverage_ms=%.1f total_ms=%.1f selected_tasks=%s "
                "effective_selected_tasks=%s reason=%s"
            ),
            normalized_platform,
            normalized_account,
            no_work_payload.get("run_id"),
            str(existing_catalog_run_id or "").strip() or None,
            coverage_ms,
            round((time_module.perf_counter() - launch_started_at) * 1000, 1),
            normalized_selected_tasks,
            effective_selected_tasks,
            no_work_payload.get("no_work_reason"),
        )
        return no_work_payload

    if catalog_selected:
        if existing_catalog_run_id:
            cancelled = _cancel_launch_group_if_parent_cancelled(
                run_id=existing_catalog_run_id,
                platform=normalized_platform,
                account_handle=normalized_account,
            )
            if cancelled:
                return cancelled
        catalog_launch_started_at = time_module.perf_counter()
        catalog_result = _room_callable(
            "start_social_account_catalog_backfill",
            start_social_account_catalog_backfill,
        )(
            normalized_platform,
            normalized_account,
            source_scope=source_scope,
            date_start=normalized_date_start,
            date_end=normalized_date_end,
            initiated_by=initiated_by,
            inline_worker_id=inline_worker_id,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            catalog_action=normalized_catalog_action,
            catalog_action_scope=normalized_catalog_action_scope,
            social_account_post_details_only=catalog_details_refresh_only,
            details_refresh_skip_detail_fetch="post_details" not in catalog_tasks,
            details_refresh_force_detail_fetch=force_detail_fetch,
            details_refresh_worker_count=effective_details_worker_count,
            comments_worker_count=effective_comments_worker_count,
            details_refresh_skip_media_followups="media" not in catalog_tasks,
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            launch_group_id=launch_group_id,
            existing_run_id=existing_catalog_run_id,
            reserved_db_session_capacity=db_session_capacity,
            enable_cap4_canary=enable_cap4_canary,
        )
        catalog_launch_ms = round((time_module.perf_counter() - catalog_launch_started_at) * 1000, 1)
        admitted_capacity = _metadata_dict((catalog_result or {}).get("db_session_capacity"))
        if admitted_capacity:
            db_session_capacity = admitted_capacity
            budget_decision = _with_instagram_db_session_capacity(budget_decision, admitted_capacity)
        if existing_catalog_run_id:
            cancelled = _cancel_launch_group_if_parent_cancelled(
                run_id=existing_catalog_run_id,
                platform=normalized_platform,
                account_handle=normalized_account,
            )
            if cancelled:
                return cancelled
        if media_attachment_id:
            attached_followups["media"] = _build_attached_media_followup(
                attachment_id=media_attachment_id,
                source="catalog_media_mirror",
                status=str((catalog_result or {}).get("status") or "queued").strip().lower() or "queued",
            )

    effective_comments_enable_media_followups = (
        bool(comments_enable_media_followups)
        if comments_enable_media_followups is not None
        else "media" in effective_selected_tasks
    )
    comments_followup_target_filter = (
        "incomplete"
        if normalized_platform == "instagram"
        and normalized_catalog_action == "backfill"
        and not instagram_targeted_comment_source_ids
        else None
    )

    if "comments" in effective_selected_tasks:
        if existing_catalog_run_id and _catalog_launch_parent_cancelled(existing_catalog_run_id):
            return _catalog_launch_parent_result(_catalog_launch_parent_snapshot(existing_catalog_run_id))
        comments_launch_started_at = time_module.perf_counter()
        catalog_comments_streaming_enabled = bool(
            catalog_result is not None
            and normalized_platform == "instagram"
            and not bounded_existing_posts_comments_run
            and not bool(allow_local_dev_inline_bypass)
            and normalized_execution_preference != "prefer_local_inline"
        )
        defer_comments_until_catalog_complete = bool(
            catalog_result is not None
            and (bool(allow_local_dev_inline_bypass) or normalized_execution_preference == "prefer_local_inline")
        )
        if defer_comments_until_catalog_complete:
            comments_deferred_until_catalog_complete = True
            catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip()
            catalog_run_config = _metadata_dict((catalog_result or {}).get("config"))
            if catalog_run_id and not catalog_run_config:
                try:
                    catalog_run_row = _load_catalog_run_row_by_id(catalog_run_id)
                    catalog_run_config = _metadata_dict(catalog_run_row.get("config"))
                except pg.DatabaseServiceUnavailableError as exc:
                    logger.warning(
                        "Continuing catalog comments followup without loaded catalog run config: run_id=%s error=%s",
                        catalog_run_id,
                        exc,
                    )
            deferred_comments_followup = {
                "state": "pending",
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "source_scope": source_scope,
                "refresh_policy": "stale_or_missing",
                "target_filter": comments_followup_target_filter,
                "date_start": _iso(date_start) if date_start is not None else None,
                "date_end": _iso(date_end) if date_end is not None else None,
                "comments_enable_media_followups": effective_comments_enable_media_followups,
                "comments_worker_count": effective_comments_worker_count,
                "allow_local_dev_inline_bypass": bool(allow_local_dev_inline_bypass),
                "launch_group_id": launch_group_id,
                "runtime_version": _metadata_dict(catalog_run_config.get("required_runtime_version"))
                or dict(_resolve_effective_runtime_version(required_execution_backend="modal")),
                "created_by_runtime_version": _metadata_dict(catalog_run_config.get("created_by_runtime_version"))
                or dict(_resolve_runtime_version_stamp()),
            }
            attached_followups["comments"] = _build_attached_comments_followup(
                run_id=None,
                status="pending",
                source="deferred_after_catalog",
                state="pending",
            )
            if catalog_run_id and deferred_comments_followup:
                _merge_catalog_run_config(
                    run_id=catalog_run_id,
                    metadata_updates={
                        "selected_tasks": normalized_selected_tasks,
                        "effective_selected_tasks": effective_selected_tasks,
                        **({"enable_cap4_canary": True} if enable_cap4_canary else {}),
                        **({"budget_decision": budget_decision} if budget_decision else {}),
                        **({"db_session_capacity": db_session_capacity} if db_session_capacity else {}),
                        **({"adaptive_worker_plan": adaptive_worker_plan} if adaptive_worker_plan else {}),
                        **public_posts_auth_metadata,
                        "deferred_comments_followup": deferred_comments_followup,
                        "attached_followups": attached_followups,
                        **_catalog_stage_graph_metadata(
                            selected_tasks=normalized_selected_tasks,
                            effective_selected_tasks=effective_selected_tasks,
                            target_readiness=target_readiness,
                            detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                            comments_status="pending",
                            comments_blocker_reasons=comments_blocker_reasons,
                            media_status=(
                                str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                                if "media" in effective_selected_tasks
                                else "skipped"
                            ),
                            enrichment_status="pending",
                            finalization_status="pending",
                            timing=_catalog_launch_timing_payload(
                                coverage_ms=coverage_ms,
                                catalog_launch_ms=catalog_launch_ms,
                                comments_launch_ms=comments_launch_ms,
                                launch_started_at=launch_started_at,
                                worker_plan=adaptive_worker_plan,
                            ),
                        ),
                    },
                )
        elif catalog_comments_streaming_enabled:
            attached_followups["comments"] = _build_attached_comments_followup(
                run_id=None,
                status="pending",
                source="catalog_streaming",
                state="pending",
            )
        else:
            comments_source = "new_run"
            try:
                comments_result = _room_callable(
                    "start_social_account_comments_scrape",
                    _start_comments_scrape,
                )(
                    normalized_platform,
                    normalized_account,
                    mode="profile",
                    source_scope=source_scope,
                    max_posts=None,
                    max_comments_per_post=None,
                    refresh_policy="stale_or_missing",
                    target_filter=comments_followup_target_filter,
                    initiated_by=initiated_by,
                    inline_worker_id=None if catalog_result else inline_worker_id,
                    allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                    comments_enable_media_followups=effective_comments_enable_media_followups,
                    launch_group_id=launch_group_id,
                    skip_launch_auth_probe=bool(instagram_targeted_comment_source_ids),
                    target_source_ids=instagram_targeted_comment_source_ids or None,
                    comments_worker_count=effective_comments_worker_count,
                    reserved_db_session_capacity=db_session_capacity,
                    cancel_active_before_relaunch=True,
                    date_start=normalized_date_start.isoformat() if normalized_date_start else None,
                    date_end=normalized_date_end.isoformat() if normalized_date_end else None,
                )
            except SocialIngestConflictError as exc:
                if exc.code == "SOCIAL_ACCOUNT_COMMENTS_LAUNCH_IN_PROGRESS":
                    comments_result = {
                        "run_id": None,
                        "status": str(exc.detail.get("status") or "pending").strip().lower() or "pending",
                        "launch_in_progress": True,
                    }
                    comments_source = "launch_in_progress"
                    logger.info(
                        (
                            "[catalog-launch] comments launch already in progress platform=%s "
                            "account=%s catalog_run_id=%s"
                        ),
                        normalized_platform,
                        normalized_account,
                        str((catalog_result or {}).get("run_id") or "").strip() or None,
                    )
                else:
                    raise
            admitted_capacity = _metadata_dict((comments_result or {}).get("db_session_capacity"))
            if admitted_capacity:
                db_session_capacity = admitted_capacity
                budget_decision = _with_instagram_db_session_capacity(budget_decision, admitted_capacity)
            attached_followups["comments"] = _build_attached_comments_followup(
                run_id=str((comments_result or {}).get("run_id") or "").strip() or None,
                status=str((comments_result or {}).get("status") or "").strip().lower() or "pending",
                source=comments_source,
            )
            comments_started_before_detail_complete = bool(catalog_result is not None and comments_result)
            if media_attachment_id and comments_source != "reused_run":
                attached_followups["media"] = _build_attached_media_followup(
                    attachment_id=media_attachment_id,
                    source="comments_media_followups",
                    status=str((comments_result or {}).get("status") or "").strip().lower() or "pending",
                )
        comments_launch_ms = round((time_module.perf_counter() - comments_launch_started_at) * 1000, 1)
        if existing_catalog_run_id:
            cancelled = _cancel_launch_group_if_parent_cancelled(
                run_id=existing_catalog_run_id,
                platform=normalized_platform,
                account_handle=normalized_account,
            )
            if cancelled:
                return cancelled

    catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip() or None
    comments_run_id = str((comments_result or {}).get("run_id") or "").strip() or None
    comments_auth_metadata = _catalog_comments_auth_metadata(comments_result)
    if catalog_run_id and not comments_deferred_until_catalog_complete:
        if existing_catalog_run_id:
            cancelled = _cancel_launch_group_if_parent_cancelled(
                run_id=existing_catalog_run_id,
                platform=normalized_platform,
                account_handle=normalized_account,
            )
            if cancelled:
                return cancelled
        catalog_metadata_updates: dict[str, Any] = {
            "selected_tasks": normalized_selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
            **({"enable_cap4_canary": True} if enable_cap4_canary else {}),
            **({"budget_decision": budget_decision} if budget_decision else {}),
            **({"db_session_capacity": db_session_capacity} if db_session_capacity else {}),
            **({"adaptive_worker_plan": adaptive_worker_plan} if adaptive_worker_plan else {}),
            **(
                _initial_instagram_completion_metadata(
                    account_handle=normalized_account,
                    effective_selected_tasks=effective_selected_tasks,
                )
                if normalized_platform == "instagram"
                else {}
            ),
        }
        if public_posts_auth_metadata:
            catalog_metadata_updates.update(public_posts_auth_metadata)
        if comments_run_id:
            catalog_metadata_updates["comments_run_id"] = comments_run_id
        if comments_auth_metadata:
            catalog_metadata_updates.update(comments_auth_metadata)
        if attached_followups:
            catalog_metadata_updates["attached_followups"] = attached_followups
        if catalog_comments_streaming_enabled:
            catalog_metadata_updates.update(
                {
                    "comments_streaming_enabled": True,
                    "comments_streaming_state": "started",
                    "comments_streaming_source": "catalog_batch_persist",
                    "comments_streaming_account_handle": normalized_account,
                    "comments_streaming_source_scope": source_scope,
                    "comments_streaming_launch_group_id": launch_group_id,
                    "comments_streaming_worker_count": effective_comments_worker_count,
                    "comments_streaming_enable_media_followups": effective_comments_enable_media_followups,
                    "comments_streaming_targets_seen": 0,
                    "comments_streaming_targets_enqueued": 0,
                    "comments_streaming_targets_skipped_duplicate": 0,
                    "comments_streaming_append_failures": 0,
                    "deferred_comments_followup": None,
                }
            )
        catalog_metadata_updates.update(
            _catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                target_readiness=target_readiness,
                detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                comments_status=str((comments_result or {}).get("status") or "").strip().lower() or "pending",
                comments_blocker_reasons=comments_blocker_reasons,
                media_status=(
                    str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                    if "media" in effective_selected_tasks
                    else "skipped"
                ),
                enrichment_status="pending",
                finalization_status="pending",
                timing=_catalog_launch_timing_payload(
                    coverage_ms=coverage_ms,
                    catalog_launch_ms=catalog_launch_ms,
                    comments_launch_ms=comments_launch_ms,
                    launch_started_at=launch_started_at,
                    worker_plan=adaptive_worker_plan,
                ),
            )
        )
        if comments_started_before_detail_complete:
            catalog_metadata_updates["comments_started_before_detail_complete"] = True
        _merge_catalog_run_config(
            run_id=catalog_run_id,
            metadata_updates=catalog_metadata_updates,
        )

    primary_run_id = (
        str((catalog_result or {}).get("run_id") or (comments_result or {}).get("run_id") or "").strip() or None
    )
    primary_status = (
        str((catalog_result or {}).get("status") or (comments_result or {}).get("status") or "").strip() or None
    )
    logger.info(
        (
            "[catalog-launch] launch_complete platform=%s account=%s run_id=%s "
            "existing_run_id=%s coverage_ms=%.1f catalog_launch_ms=%.1f "
            "comments_launch_ms=%.1f total_ms=%.1f selected_tasks=%s "
            "effective_selected_tasks=%s"
        ),
        normalized_platform,
        normalized_account,
        primary_run_id,
        str(existing_catalog_run_id or "").strip() or None,
        coverage_ms,
        catalog_launch_ms,
        comments_launch_ms,
        round((time_module.perf_counter() - launch_started_at) * 1000, 1),
        normalized_selected_tasks,
        effective_selected_tasks,
    )
    payload = {
        "run_id": primary_run_id,
        "status": primary_status,
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "launch_group_id": launch_group_id,
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": effective_selected_tasks,
        "post_details_skipped_reason": post_details_skipped_reason,
        "catalog_run_id": catalog_run_id,
        "comments_run_id": comments_run_id,
        "catalog_status": str((catalog_result or {}).get("status") or "").strip() or None,
        "comments_status": str((comments_result or {}).get("status") or "").strip() or None,
        "comments_reused_existing_run": comments_reused_existing_run,
        "catalog_action": normalized_catalog_action,
        "catalog_action_scope": normalized_catalog_action_scope,
        "catalog_bootstrap_required": requires_catalog_bootstrap if catalog_selected else False,
        "comments_deferred_until_catalog_complete": comments_deferred_until_catalog_complete,
        "comments_streaming_enabled": catalog_comments_streaming_enabled,
        "comments_streaming_state": "started" if catalog_comments_streaming_enabled else None,
        "comments_streaming_source": "catalog_batch_persist" if catalog_comments_streaming_enabled else None,
        "attached_followups": attached_followups,
        "comments_started_before_detail_complete": comments_started_before_detail_complete,
        "enable_cap4_canary": bool(enable_cap4_canary),
        "budget_decision": budget_decision,
        "db_session_capacity": db_session_capacity,
        "adaptive_worker_plan": adaptive_worker_plan,
        **(
            _initial_instagram_completion_metadata(
                account_handle=normalized_account,
                effective_selected_tasks=effective_selected_tasks,
            )
            if normalized_platform == "instagram"
            else {}
        ),
        **public_posts_auth_metadata,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            target_readiness=target_readiness,
            detail_status=str((catalog_result or {}).get("status") or "").strip().lower()
            or ("pending" if catalog_selected else "skipped"),
            comments_status=str((comments_result or {}).get("status") or "").strip().lower()
            or ("pending" if "comments" in effective_selected_tasks else "skipped"),
            comments_blocker_reasons=comments_blocker_reasons,
            media_status=(
                str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                if "media" in effective_selected_tasks
                else "skipped"
            ),
            enrichment_status="pending" if catalog_selected else "skipped",
            finalization_status="pending" if catalog_selected else "completed",
            timing=_catalog_launch_timing_payload(
                coverage_ms=coverage_ms,
                catalog_launch_ms=catalog_launch_ms,
                comments_launch_ms=comments_launch_ms,
                launch_started_at=launch_started_at,
                worker_plan=adaptive_worker_plan,
            ),
        ),
    }
    if comments_auth_metadata:
        payload.update(comments_auth_metadata)
    return payload


_LOCAL_ROOM_NAMES = {
    "start_social_account_catalog_backfill",
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "launch_social_account_catalog_backfill",
    "get_instagram_catalog_launch_capacity",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "start_social_account_catalog_backfill",
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "get_instagram_catalog_launch_capacity",
    "launch_social_account_catalog_backfill",
]
