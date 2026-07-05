# ruff: noqa: F821
"""Catalog run progress read models for social account backfill runs."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core
from trr_backend.socials.instagram.media_completion import build_media_completion_payload

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

_SELECTED_STAGE_TASKS = {
    "detail_refresh": "post_details",
    "comments": "comments",
    "media": "media",
}

_STAGE_JOB_NAMES = {
    "detail_refresh": {"shared_account_posts"},
    "comments": {"comments"},
    "media": {"media_mirror"},
}


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def _budget_runbook_state(decision: Mapping[str, Any] | None, run_config: Mapping[str, Any]) -> dict[str, Any]:
    decision_payload = _metadata_dict(decision)
    stored = _metadata_dict(decision_payload.get("runbook_state")) or _metadata_dict(run_config.get("runbook_state"))
    if stored:
        return stored
    try:
        from trr_backend.socials.control_plane.budget import instagram_backfill_runbook_metadata
    except Exception:  # noqa: BLE001 - progress must remain read-only and best-effort
        return {}
    return instagram_backfill_runbook_metadata()


def _blocked_budget_progress_payload(run_config: Mapping[str, Any]) -> dict[str, Any]:
    decision = _metadata_dict(run_config.get("budget_decision"))
    state = str(decision.get("state") or "").strip().lower()
    if state not in {"paused", "identity_blocked"}:
        return {}
    reasons = [str(reason or "").strip() for reason in list(decision.get("reasons") or []) if str(reason or "").strip()]
    blocked_budget = {
        "state": state,
        "reason": reasons[0] if reasons else state,
        "reasons": reasons,
        "lane": str(decision.get("lane") or "instagram_backfill").strip() or "instagram_backfill",
        "account": str(decision.get("account") or "").strip() or None,
        "runbook_state": _budget_runbook_state(decision, run_config),
    }
    return {
        "budget_blocked": True,
        "blocked_budget": blocked_budget,
        "blocked_reason": blocked_budget["reason"],
        "operational_state": "blocked_budget",
    }


def _catalog_comments_streaming_progress_payload(run_config: Mapping[str, Any]) -> dict[str, Any] | None:
    """Expose catalog-to-comments streaming metadata in a compact UI shape."""

    config = _metadata_dict(run_config)
    if not (
        config.get("comments_streaming_enabled") is not None
        or config.get("comments_streaming_state") is not None
        or config.get("comments_run_id") is not None
    ):
        return None

    attempt_count = _normalize_non_negative_int(config.get("comments_streaming_enqueue_attempt_count"))
    total_lag_ms = _normalize_non_negative_int(config.get("comments_streaming_total_enqueue_lag_ms"))
    average_lag_ms = round(total_lag_ms / attempt_count, 1) if attempt_count > 0 else None
    comments_run_id = str(config.get("comments_run_id") or "").strip() or None
    state = str(config.get("comments_streaming_state") or "").strip().lower() or None
    targets_seen = _normalize_non_negative_int(config.get("comments_streaming_targets_seen"))
    targets_enqueued = _normalize_non_negative_int(config.get("comments_streaming_targets_enqueued"))
    skipped_duplicate = _normalize_non_negative_int(config.get("comments_streaming_targets_skipped_duplicate"))
    append_failures = _normalize_non_negative_int(config.get("comments_streaming_append_failures"))
    last_error = str(config.get("comments_streaming_last_error") or "").strip() or None
    if last_error or state == "failed" or append_failures > 0:
        next_action = {
            "code": "inspect_streaming_append_failure",
            "label": "Inspect failed append",
            "detail": (
                "A saved catalog batch could not be added to the comments run. "
                "Check the last streaming error before launching another backfill."
            ),
        }
    elif state == "completed":
        next_action = {
            "code": "watch_comments_run",
            "label": "Watch comments run",
            "detail": "Catalog reconciliation finished; remaining movement now belongs to the attached comments run.",
        }
    elif comments_run_id and targets_enqueued > 0:
        next_action = {
            "code": "workers_processing_streamed_posts",
            "label": "Workers processing streamed posts",
            "detail": (
                "Saved catalog posts have been queued to public-first comments workers. "
                "Keep the catalog run open while new batches arrive."
            ),
        }
    elif comments_run_id and skipped_duplicate > 0:
        next_action = {
            "code": "all_seen_posts_already_represented",
            "label": "Seen posts already represented",
            "detail": "The latest catalog posts were already queued or completed in the attached comments run.",
        }
    else:
        next_action = {
            "code": "waiting_for_saved_catalog_batch",
            "label": "Waiting for saved catalog batch",
            "detail": "The comments lane will start as soon as the next catalog batch is durably saved.",
        }
    history: list[dict[str, Any]] = []
    raw_history = config.get("comments_streaming_history")
    if isinstance(raw_history, list):
        for item in raw_history[-8:]:
            entry = _metadata_dict(item)
            if entry:
                history.append(entry)
    payload: dict[str, Any] = {
        "enabled": bool(config.get("comments_streaming_enabled")),
        "state": state,
        "source": str(config.get("comments_streaming_source") or "").strip().lower() or None,
        "comments_run_id": comments_run_id,
        "account_handle": str(config.get("comments_streaming_account_handle") or "").strip() or None,
        "source_scope": str(config.get("comments_streaming_source_scope") or "").strip() or None,
        "launch_group_id": str(config.get("comments_streaming_launch_group_id") or "").strip() or None,
        "worker_count": _normalize_non_negative_int(config.get("comments_streaming_worker_count")) or None,
        "enable_media_followups": bool(config.get("comments_streaming_enable_media_followups")),
        "targets_seen": targets_seen,
        "targets_enqueued": targets_enqueued,
        "targets_skipped_duplicate": skipped_duplicate,
        "append_failures": append_failures,
        "reconciled_source_ids": _normalize_non_negative_int(config.get("comments_streaming_reconciled_source_ids")),
        "last_updated_at": str(config.get("comments_streaming_last_updated_at") or "").strip() or None,
        "started_at": str(config.get("comments_streaming_started_at") or "").strip() or None,
        "completed_at": str(config.get("comments_streaming_completed_at") or "").strip() or None,
        "last_enqueue_targets_seen": _normalize_non_negative_int(
            config.get("comments_streaming_last_enqueue_targets_seen")
        ),
        "last_enqueue_targets_enqueued": _normalize_non_negative_int(
            config.get("comments_streaming_last_enqueue_targets_enqueued")
        ),
        "last_enqueue_requested_at": str(config.get("comments_streaming_last_enqueue_requested_at") or "").strip()
        or None,
        "last_enqueue_completed_at": str(config.get("comments_streaming_last_enqueue_completed_at") or "").strip()
        or None,
        "last_enqueue_lag_ms": _normalize_non_negative_int(config.get("comments_streaming_last_enqueue_lag_ms"))
        or None,
        "max_enqueue_lag_ms": _normalize_non_negative_int(config.get("comments_streaming_max_enqueue_lag_ms")) or None,
        "average_enqueue_lag_ms": average_lag_ms,
        "enqueue_attempt_count": attempt_count,
        "last_batch_source_ids_count": _normalize_non_negative_int(
            config.get("comments_streaming_last_batch_source_ids_count")
        ),
        "last_append_result": _metadata_dict(config.get("comments_streaming_last_append_result")) or None,
        "last_reconcile_result": _metadata_dict(config.get("comments_streaming_last_reconcile_result")) or None,
        "last_error": last_error,
        "last_conflict": str(config.get("comments_streaming_last_conflict") or "").strip() or None,
        "next_action": next_action,
        "history": history,
    }
    return payload


def _selected_catalog_tasks(run_config: Mapping[str, Any]) -> set[str]:
    effective = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        run_config.get("effective_selected_tasks")
    )
    selected = _normalize_optional_social_account_catalog_backfill_selected_tasks(run_config.get("selected_tasks"))
    return set(effective or selected or [])


def _stage_status_from_payload(
    *,
    stage_name: str,
    stages_payload: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
) -> str | None:
    stage_aliases = _STAGE_JOB_NAMES.get(stage_name, set())
    if not stage_aliases:
        return None
    aggregate = {
        "total": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0,
        "active": 0,
        "waiting": 0,
    }
    for raw_name, raw_stage in _metadata_dict(stages_payload).items():
        if str(raw_name or "").strip() not in stage_aliases:
            continue
        stage = _metadata_dict(raw_stage)
        aggregate["total"] += _normalize_non_negative_int(stage.get("jobs_total"))
        aggregate["completed"] += _normalize_non_negative_int(stage.get("jobs_completed"))
        aggregate["failed"] += _normalize_non_negative_int(stage.get("jobs_failed"))
        aggregate["cancelled"] += _normalize_non_negative_int(stage.get("jobs_cancelled"))
        aggregate["active"] += _normalize_non_negative_int(stage.get("jobs_active"))
        aggregate["waiting"] += _normalize_non_negative_int(stage.get("jobs_waiting"))

    row_statuses: list[str] = []
    for row in job_rows:
        row_stage = str(row.get("stage") or row.get("job_type") or "").strip()
        if row_stage not in stage_aliases:
            continue
        row_statuses.append(str(row.get("status") or "").strip().lower())

    if aggregate["failed"] > 0 or any(status == "failed" for status in row_statuses):
        return "failed"
    if aggregate["active"] > 0 or any(status == "running" for status in row_statuses):
        return "running"
    if aggregate["waiting"] > 0 or any(status in {"pending", "queued", "retrying"} for status in row_statuses):
        return "queued"
    if aggregate["total"] > 0 and aggregate["cancelled"] >= aggregate["total"]:
        return "cancelled"
    if row_statuses and all(status == "cancelled" for status in row_statuses):
        return "cancelled"
    terminal_total = aggregate["completed"] + aggregate["cancelled"]
    if aggregate["total"] > 0 and terminal_total >= aggregate["total"]:
        return "completed"
    if row_statuses and all(status == "completed" for status in row_statuses):
        return "completed"
    return None


def _selected_stage_graph_payload(
    *,
    run_config: Mapping[str, Any],
    stage_graph: Mapping[str, Any],
    stages_payload: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not stage_graph:
        return {}
    selected_tasks = _selected_catalog_tasks(run_config)
    sanitized = {str(stage): _metadata_dict(entry) for stage, entry in stage_graph.items()}
    for stage_name, task_name in _SELECTED_STAGE_TASKS.items():
        if task_name not in selected_tasks:
            continue
        entry = dict(_metadata_dict(sanitized.get(stage_name)))
        normalized_status = str(entry.get("status") or "").strip().lower()
        if normalized_status == "skipped" or not normalized_status:
            entry["status"] = (
                _stage_status_from_payload(
                    stage_name=stage_name,
                    stages_payload=stages_payload,
                    job_rows=job_rows,
                )
                or "pending"
            )
        entry["selected"] = True
        sanitized[stage_name] = entry
    if selected_tasks & {"post_details", "comments", "media"}:
        readiness = dict(_metadata_dict(sanitized.get("target_readiness")))
        if readiness:
            readiness["selected"] = True
            sanitized["target_readiness"] = readiness
    return sanitized


def _catalog_progress_stage_graph_payload(
    *,
    run_config: Mapping[str, Any],
    stages_payload: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    stage_graph = _metadata_dict(run_config.get("stage_graph"))
    target_readiness = _metadata_dict(run_config.get("target_readiness"))
    timing = _metadata_dict(run_config.get("timing"))
    queue_drain: dict[str, dict[str, int]] = {}
    for stage_name, raw_stage in _metadata_dict(stages_payload).items():
        stage = _metadata_dict(raw_stage)
        waiting = _normalize_non_negative_int(stage.get("jobs_waiting"))
        active = _normalize_non_negative_int(stage.get("jobs_active"))
        remaining = max(
            _normalize_non_negative_int(stage.get("jobs_total"))
            - _normalize_non_negative_int(stage.get("jobs_completed"))
            - _normalize_non_negative_int(stage.get("jobs_failed"))
            - _normalize_non_negative_int(stage.get("jobs_cancelled")),
            0,
        )
        if waiting > 0 or active > 0 or remaining > 0:
            queue_drain[str(stage_name)] = {
                "jobs_waiting": waiting,
                "jobs_active": active,
                "jobs_remaining": remaining,
            }

    first_auth_failure_at = None
    first_auth_failure_code = None
    auth_failure_codes = {
        "instagram_graphql_checkpoint_required",
        "instagram_graphql_cursor_forbidden",
        "instagram_graphql_cursor_unauthorized",
        "checkpoint_required",
        "auth_probe_failed",
    }
    for row in job_rows:
        metadata = _metadata_dict(row.get("metadata"))
        code = (
            str(row.get("last_error_code") or metadata.get("error_code") or metadata.get("auth_reason") or "")
            .strip()
            .lower()
        )
        message = str(row.get("error_message") or metadata.get("error_message") or "").strip().lower()
        is_auth_failure = code in auth_failure_codes or "checkpoint" in message or "unauthorized" in message
        if not is_auth_failure:
            continue
        timestamp = _coerce_dt(row.get("completed_at") or row.get("started_at") or row.get("created_at"))
        if timestamp is None:
            continue
        if first_auth_failure_at is None or timestamp < first_auth_failure_at:
            first_auth_failure_at = timestamp
            first_auth_failure_code = code or None

    payload: dict[str, Any] = {}
    if stage_graph:
        payload["stage_graph"] = _selected_stage_graph_payload(
            run_config=run_config,
            stage_graph=stage_graph,
            stages_payload=stages_payload,
            job_rows=job_rows,
        )
    if target_readiness:
        payload["target_readiness"] = target_readiness
    if timing:
        payload["stage_timing"] = timing
        per_stage_ms = _metadata_dict(timing.get("per_stage_ms"))
        if per_stage_ms:
            payload["per_stage_timing_ms"] = per_stage_ms
        worker_plan = _metadata_dict(timing.get("worker_plan"))
        if worker_plan:
            payload["adaptive_worker_plan"] = worker_plan
    stored_worker_plan = _metadata_dict(run_config.get("adaptive_worker_plan"))
    if stored_worker_plan and "adaptive_worker_plan" not in payload:
        payload["adaptive_worker_plan"] = stored_worker_plan
    if queue_drain:
        payload["queue_drain_estimate"] = queue_drain
    if first_auth_failure_at is not None:
        payload["first_auth_failure_at"] = _iso(first_auth_failure_at)
        payload["first_auth_failure_code"] = first_auth_failure_code

    # dg-3: surface a silently-failed deferred comments follow-up so operators can
    # see WHY a completed catalog still has no comments work. The follow-up failure
    # is recorded only in run_config metadata (run_lifecycle marks state="failed"),
    # never in run.status, so without this the admin UI shows comments "pending"/
    # "failed" with no cause. Presence-gated: the key is omitted entirely unless a
    # failure is recorded, so strict-payload consumers/snapshots are unaffected.
    followup = _metadata_dict(run_config.get("deferred_comments_followup"))
    if str(followup.get("state") or "").strip().lower() == "failed":
        failure_history = followup.get("failure_history")
        payload["deferred_comments_followup_alert"] = {
            "state": "failed",
            "platform": str(followup.get("platform") or "").strip() or None,
            "retryable": bool(followup.get("retryable")),
            "retryable_reason": str(followup.get("retryable_reason") or "").strip() or None,
            "error_message": str(followup.get("error_message") or "").strip() or None,
            "failed_at": str(followup.get("failed_at") or "").strip() or None,
            "target_filter": str(followup.get("target_filter") or "").strip() or None,
            "comments_run_id": str(followup.get("comments_run_id") or "").strip() or None,
            "failure_count": (len(failure_history) if isinstance(failure_history, list) else None),
        }
    return payload


def _catalog_completion_progress_payload(
    *,
    run_config: Mapping[str, Any],
    stages_payload: Mapping[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    budget_decision = _metadata_dict(run_config.get("budget_decision"))
    if budget_decision:
        payload["budget_decision"] = budget_decision
        runbook_state = _budget_runbook_state(budget_decision, run_config)
        if runbook_state:
            payload["runbook_state"] = runbook_state
    adaptive_worker_plan = _metadata_dict(run_config.get("adaptive_worker_plan"))
    if adaptive_worker_plan:
        payload["adaptive_worker_plan"] = adaptive_worker_plan
    requires_apply_confirmation = bool(run_config.get("requires_apply_confirmation"))
    apply_required = bool(run_config.get("apply_required"))
    apply_run_id = str(run_config.get("apply_run_id") or "").strip() or None
    required_confirmation = str(run_config.get("required_confirmation") or "").strip() or None
    if requires_apply_confirmation or apply_required or apply_run_id:
        payload.update(
            {
                "requires_apply_confirmation": requires_apply_confirmation,
                "apply_required": apply_required,
                "apply_run_id": apply_run_id,
                "required_confirmation": required_confirmation,
            }
        )
    if run_config.get("enable_cap4_canary") is not None:
        payload["enable_cap4_canary"] = bool(run_config.get("enable_cap4_canary"))
    details_worker_count = _normalize_non_negative_int(run_config.get("details_refresh_worker_count"))
    comments_worker_count = _normalize_non_negative_int(run_config.get("comments_worker_count"))
    if details_worker_count:
        payload["detail_worker_count"] = details_worker_count
    if comments_worker_count:
        payload["comments_worker_count"] = comments_worker_count
    if run_config.get("comments_enable_media_followups") is not None:
        payload["comments_enable_media_followups"] = bool(run_config.get("comments_enable_media_followups"))
    timing = _metadata_dict(run_config.get("timing"))
    per_stage_ms = _metadata_dict(timing.get("per_stage_ms"))
    if per_stage_ms:
        payload["per_stage_timing_ms"] = per_stage_ms
    snapshot_completion = _metadata_dict(run_config.get("snapshot_completion_summary"))
    if snapshot_completion:
        payload["snapshot_completion_summary"] = snapshot_completion
    comments_streaming = _catalog_comments_streaming_progress_payload(run_config)
    if comments_streaming:
        payload["comments_streaming"] = comments_streaming

    selected_tasks = _selected_catalog_tasks(run_config)
    stored_media_completion = _metadata_dict(run_config.get("media_completion"))
    media_stage = _metadata_dict(stages_payload.get("media_mirror"))
    comment_media_stage = _metadata_dict(stages_payload.get("comment_media_mirror"))
    media_waiting = _normalize_non_negative_int(media_stage.get("jobs_waiting")) + _normalize_non_negative_int(
        media_stage.get("jobs_active")
    )
    comment_media_waiting = _normalize_non_negative_int(
        comment_media_stage.get("jobs_waiting")
    ) + _normalize_non_negative_int(comment_media_stage.get("jobs_active"))
    stale_claims = {
        "total": media_waiting + comment_media_waiting,
        "by_stage": {
            "media_mirror": media_waiting,
            "comment_media_mirror": comment_media_waiting,
        },
        "by_platform": {"instagram": media_waiting + comment_media_waiting},
    }
    if "media" in selected_tasks or stored_media_completion:
        media_completion = build_media_completion_payload(
            stale_media_claims=stale_claims if stale_claims["total"] > 0 else None,
        )
        media_completion.update(stored_media_completion)
        media_completion["queue"] = {
            "media_mirror": media_stage,
            "comment_media_mirror": comment_media_stage,
        }
        if "media" not in selected_tasks and not stored_media_completion:
            media_completion["status"] = "not_selected"
            media_completion["completed"] = True
        elif stale_claims["total"] > 0:
            media_completion["status"] = "blocked"
            media_completion["completed"] = False
        payload["media_completion"] = media_completion
    payload.update(_blocked_budget_progress_payload(run_config))
    return payload


def _catalog_stage_graph_diagnostics(
    *,
    run_config: Mapping[str, Any],
    stages_payload: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """dg-1/dg-2: read-only diagnostics that compare the RAW stored stage_graph
    (verbatim from run_config) against the DERIVED/sanitized view the admin UI
    renders, and surface the full deferred-comments-followup record.

    This lets an operator distinguish *stale stored metadata* (raw says
    "skipped"/"pending") from *work that genuinely never ran in the run*
    (derived re-derived from job rows). Strictly read-only: it never writes
    run_config. Returns {} when no stage_graph is stored.
    """

    raw_graph = _metadata_dict(run_config.get("stage_graph"))
    if not raw_graph:
        return {}
    raw_stage_graph = copy.deepcopy(raw_graph)
    derived_stage_graph = _selected_stage_graph_payload(
        run_config=run_config,
        stage_graph=raw_graph,
        stages_payload=stages_payload,
        job_rows=job_rows,
    )

    # Only the 'status' field is a meaningful mismatch. The derived view also adds
    # a 'selected' flag (e.g. on target_readiness) that is not a real divergence.
    mismatches: list[dict[str, Any]] = []
    for stage_name in raw_stage_graph:
        raw_status = str(_metadata_dict(raw_stage_graph.get(stage_name)).get("status") or "").strip().lower() or None
        derived_status = (
            str(_metadata_dict(derived_stage_graph.get(stage_name)).get("status") or "").strip().lower() or None
        )
        if raw_status != derived_status:
            mismatches.append({"stage": str(stage_name), "raw_status": raw_status, "derived_status": derived_status})

    diagnostics: dict[str, Any] = {
        "raw_stage_graph": raw_stage_graph,
        "derived_stage_graph": derived_stage_graph,
        "stage_status_mismatches": mismatches,
    }

    followup = _metadata_dict(run_config.get("deferred_comments_followup"))
    if followup:
        failure_history = followup.get("failure_history")
        diagnostics["deferred_comments_followup"] = {
            "state": str(followup.get("state") or "").strip() or None,
            "platform": str(followup.get("platform") or "").strip() or None,
            "retryable": bool(followup.get("retryable")),
            "retryable_reason": str(followup.get("retryable_reason") or "").strip() or None,
            "error_message": str(followup.get("error_message") or "").strip() or None,
            "failed_at": str(followup.get("failed_at") or "").strip() or None,
            "target_filter": str(followup.get("target_filter") or "").strip() or None,
            "account_handle": str(followup.get("account_handle") or "").strip() or None,
            "source_scope": str(followup.get("source_scope") or "").strip() or None,
            "comments_run_id": str(followup.get("comments_run_id") or "").strip() or None,
            "launch_group_id": str(followup.get("launch_group_id") or "").strip() or None,
            "failure_history": failure_history if isinstance(failure_history, list) else [],
        }

    return diagnostics


def _load_linked_recovery_run_summary(run_id: str) -> dict[str, Any] | None:
    """Load a compact summary of a linked recovery comments run by PRIMARY KEY.

    Uses _load_catalog_run_row_by_id (a PK lookup) rather than a launch_group_id
    jsonb scan, which is unindexed and could trip statement_timeout while live
    runs hold connections. Returns None if the linked run cannot be loaded.
    """

    try:
        row = _load_catalog_run_row_by_id(run_id)
    except (LookupError, ValueError):
        return None
    if not row:
        return None
    summary = _metadata_dict(row.get("summary"))
    created_at = _coerce_dt(row.get("created_at"))
    return {
        "run_id": str(row.get("run_id") or row.get("id") or run_id),
        "status": str(row.get("status") or "").strip().lower() or None,
        "total_jobs": _normalize_non_negative_int(summary.get("total_jobs")),
        "completed_jobs": _normalize_non_negative_int(summary.get("completed_jobs")),
        "failed_jobs": _normalize_non_negative_int(summary.get("failed_jobs")),
        "active_jobs": _normalize_non_negative_int(summary.get("active_jobs")),
        "created_at": _iso(created_at) if created_at is not None else None,
    }


def get_social_account_catalog_run_diagnostics(
    platform: str,
    account_handle: str,
    run_id: str,
) -> dict[str, Any]:
    """dg-4: lazy, read-only diagnostics for a catalog run.

    Returns the RAW stored stage_graph vs the DERIVED view, the per-stage status
    mismatches, the full deferred-comments-followup failure record, and the linked
    recovery comments run (looked up by PRIMARY KEY). Kept separate from the
    frequently-polled progress route so this heavier payload never bloats it.
    Reads on pool 'default' (NOT the maxconn=1 social_progress pool the fast
    poller uses).
    """

    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if normalized_platform not in set(CATALOG_SUPPORTED_PLATFORMS):
        raise ValueError("Catalog backfill is not supported for this platform.")
    if not _relation_exists("social.scrape_runs") or not _relation_exists("social.scrape_jobs"):
        raise ValueError("social_ingest_queue_schema_missing")
    features = _scrape_jobs_features()
    if not bool(features.get("has_run_id")):
        raise ValueError("run_progress_requires_scrape_jobs_run_id")

    try:
        run_row = _load_social_account_catalog_run_row(
            platform=normalized_platform,
            account_handle=normalized_account,
            run_id=run_id,
            verify_account=True,
            pool_name="default",
        )
        job_rows = _load_social_account_catalog_jobs(
            run_id=run_id,
            platform=normalized_platform,
            account_handle=normalized_account,
            features=features,
            pool_name="default",
        )
    except LookupError as exc:
        raise ValueError("run_not_found") from exc

    run_config = _metadata_dict(run_row.get("config"))
    configured_platforms = {
        _normalize_platform_name(value)
        for value in _as_text_list(run_config.get("platforms") or [])
        if _normalize_platform_name(value)
    }
    configured_accounts = {
        _normalize_social_account_profile_handle(value)
        for value in _as_text_list(run_config.get("accounts_override") or [])
        if _normalize_social_account_profile_handle(value)
    }
    if configured_platforms and normalized_platform not in configured_platforms:
        raise ValueError("run_not_found")
    if configured_accounts and normalized_account not in configured_accounts:
        raise ValueError("run_not_found")

    # Derived view comes straight from ground-truth job rows (empty stages_payload),
    # so this avoids re-running the heavy progress-snapshot aggregation.
    diagnostics = _catalog_stage_graph_diagnostics(
        run_config=run_config,
        stages_payload={},
        job_rows=job_rows,
    )
    diagnostics["run_id"] = str(run_row.get("run_id") or run_row.get("id") or run_id)
    diagnostics["run_status"] = str(run_row.get("status") or "").strip().lower() or None

    followup = diagnostics.get("deferred_comments_followup") or {}
    comments_run_id = followup.get("comments_run_id") or (str(run_config.get("comments_run_id") or "").strip() or None)
    if comments_run_id and comments_run_id != diagnostics["run_id"]:
        diagnostics["linked_recovery_run"] = _load_linked_recovery_run_summary(comments_run_id)

    return diagnostics


def _catalog_posts_runtime_additive_payload(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    run_config: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
    fast: bool = False,
) -> dict[str, Any]:
    if _normalize_social_account_profile_platform(platform) != "instagram":
        return {}

    pagination_state = (
        run_config.get("pagination_state")
        if fast or isinstance(run_config.get("pagination_state"), dict)
        else latest_instagram_profile_pagination_state(
            account_handle=account_handle,
            source_scope=str(run_config.get("source_scope") or "network"),
            run_id=run_id,
            direction="forward",
        )
    )
    if not isinstance(pagination_state, Mapping):
        pagination_state = {}

    latest_fetcher_runtime: dict[str, Any] = {}
    inline_comments_upserted = 0
    for row in job_rows:
        metadata = _metadata_dict(row.get("metadata"))
        fetcher_runtime = _metadata_dict(metadata.get("fetcher_runtime") or metadata.get("runtime_metadata"))
        if fetcher_runtime:
            latest_fetcher_runtime = fetcher_runtime
        persist_counters = _metadata_dict(metadata.get("persist_counters"))
        inline_comments_upserted += _normalize_non_negative_int(
            metadata.get("inline_comments_upserted")
            or persist_counters.get("inline_comments_upserted")
            or _metadata_dict(metadata.get("posts_scrapling_persist_diagnostics")).get("inline_comments_upserted")
        )

    stop_reason = (
        str(_metadata_dict(pagination_state).get("stop_reason") or "").strip().lower()
        or str(latest_fetcher_runtime.get("fetch_reason") or "").strip().lower()
        or None
    )
    profile_posts_doc_id_metadata = _metadata_dict(latest_fetcher_runtime.get("profile_posts_doc_ids"))
    doc_id_used = (
        str(_metadata_dict(pagination_state).get("doc_id_used") or "").strip()
        or str(
            latest_fetcher_runtime.get("doc_id_used")
            or latest_fetcher_runtime.get("profile_posts_doc_id_used")
            or profile_posts_doc_id_metadata.get("used")
            or profile_posts_doc_id_metadata.get("final_selected")
            or ""
        ).strip()
        or None
    )
    doc_ids_attempted = (
        _metadata_dict(pagination_state).get("doc_ids_attempted")
        if isinstance(_metadata_dict(pagination_state).get("doc_ids_attempted"), list)
        else profile_posts_doc_id_metadata.get("attempted")
        if isinstance(profile_posts_doc_id_metadata.get("attempted"), list)
        else latest_fetcher_runtime.get("doc_ids_attempted")
    )
    runtime_proxy_pacing = _metadata_dict(latest_fetcher_runtime.get("proxy_pacing"))
    # Bandwidth metering surfaced from the fetcher runtime (top-level, falling back to
    # the nested proxy_pacing copy). bytes_total carries the per-job download size and
    # bytes_by_host the per-destination-host breakdown for cost attribution.
    runtime_bytes_total = _normalize_non_negative_int(
        latest_fetcher_runtime.get("bytes_total") or runtime_proxy_pacing.get("bytes_total")
    )
    runtime_bytes_by_host_raw = (
        latest_fetcher_runtime.get("bytes_by_host") or runtime_proxy_pacing.get("bytes_by_host") or {}
    )
    runtime_bytes_by_host = {
        str(host): _normalize_non_negative_int(value)
        for host, value in (runtime_bytes_by_host_raw.items() if isinstance(runtime_bytes_by_host_raw, Mapping) else [])
    }
    proxy_fingerprint = (
        str(_metadata_dict(pagination_state).get("proxy_fingerprint") or "").strip()
        or str(latest_fetcher_runtime.get("proxy_fingerprint") or "").strip()
        or str(latest_fetcher_runtime.get("selected_proxy_fingerprint") or "").strip()
        or None
    )
    feature_flags = instagram_posts_acceleration_flags()
    stage_graph = _metadata_dict(run_config.get("stage_graph"))
    detail_stage = _metadata_dict(stage_graph.get("detail_refresh"))
    target_readiness = _metadata_dict(run_config.get("target_readiness"))
    details_progress = _metadata_dict(run_config.get("detail_refresh") or run_config.get("details_progress"))
    if not details_progress:
        details_progress = {
            "phase": "details_refresh",
            "status": str(detail_stage.get("status") or "").strip().lower() or None,
            "selected": bool(detail_stage.get("selected")),
            "blocker_reasons": list(detail_stage.get("blocker_reasons") or []),
            "detail_gap_count": _normalize_non_negative_int(
                detail_stage.get("detail_gap_count") or target_readiness.get("detail_gap_count")
            ),
            "source": "stage_graph",
        }
    listing_progress = {
        "page_index": _normalize_non_negative_int(_metadata_dict(pagination_state).get("page_index")),
        "posts_seen": _normalize_non_negative_int(_metadata_dict(pagination_state).get("posts_seen")),
        "posts_upserted": _normalize_non_negative_int(_metadata_dict(pagination_state).get("posts_upserted")),
        "end_cursor": str(_metadata_dict(pagination_state).get("end_cursor") or "").strip() or None,
        "partial": bool(_metadata_dict(pagination_state).get("partial")) if pagination_state else None,
        "stop_reason": stop_reason,
    }
    bidirectional_probe = (
        run_config.get("bidirectional_probe")
        if isinstance(run_config.get("bidirectional_probe"), dict)
        else latest_fetcher_runtime.get("bidirectional_probe")
        if isinstance(latest_fetcher_runtime.get("bidirectional_probe"), dict)
        else None
    )
    warmup_pool = (
        run_config.get("warmup_pool")
        if isinstance(run_config.get("warmup_pool"), dict)
        else latest_fetcher_runtime.get("warmup_pool")
        if isinstance(latest_fetcher_runtime.get("warmup_pool"), dict)
        else None
    )
    posts_auth_mode = (
        str(latest_fetcher_runtime.get("auth_state") or "").strip().lower()
        or str(run_config.get("posts_auth_mode") or run_config.get("instagram_posts_auth_mode") or "").strip().lower()
        or None
    )
    if posts_auth_mode not in {"anonymous", "authenticated"}:
        posts_auth_mode = None
    return {
        "posts_acceleration_flags": feature_flags,
        "posts_auth_mode": posts_auth_mode,
        "instagram_posts_auth_mode": posts_auth_mode,
        "pagination_state": dict(pagination_state) if isinstance(pagination_state, Mapping) else {},
        "resume_cursor_saved": bool(_metadata_dict(pagination_state).get("end_cursor")),
        "listing_progress": listing_progress,
        "details_progress": details_progress,
        "inline_comments_upserted": inline_comments_upserted,
        "doc_id_used": doc_id_used,
        "profile_posts_doc_ids": doc_ids_attempted if isinstance(doc_ids_attempted, list) else [],
        "pagination_doc_id_stale": stop_reason == "pagination_doc_id_stale",
        "proxy_pacing": {
            **runtime_proxy_pacing,
            "enabled": bool(_metadata_dict(feature_flags.get("flags")).get("per_ip_pacing_enabled")),
            "proxy_fingerprint": proxy_fingerprint,
            "proxy_session_key": str(_metadata_dict(pagination_state).get("proxy_session_key") or "").strip()
            or str(latest_fetcher_runtime.get("proxy_session_key") or "").strip()
            or None,
            "bytes_total": runtime_bytes_total,
            "bytes_by_host": runtime_bytes_by_host,
        },
        "warmup_pool": warmup_pool,
        "bidirectional_probe": bidirectional_probe,
    }


def _build_catalog_terminal_progress_payload(
    *,
    run_row: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
    run_id: str,
    run_config: Mapping[str, Any],
    platform: str,
    account_handle: str,
    recent_log_limit: int,
    fast: bool = False,
) -> dict[str, Any]:
    payload = _build_terminal_catalog_run_progress_payload(
        run_row=run_row,
        job_rows=list(job_rows),
        run_id=run_id,
        run_config=run_config,
        platform=platform,
        account_handle=account_handle,
        recent_log_limit=recent_log_limit,
    )
    payload.update(
        _catalog_progress_stage_graph_payload(
            run_config=run_config,
            stages_payload=payload.get("stages") or {},
            job_rows=job_rows,
        )
    )
    payload.update(
        _catalog_completion_progress_payload(
            run_config=run_config,
            stages_payload=payload.get("stages") or {},
        )
    )
    payload.update(
        _catalog_posts_runtime_additive_payload(
            platform=platform,
            account_handle=account_handle,
            run_id=run_id,
            run_config=run_config,
            job_rows=job_rows,
            fast=fast,
        )
    )
    payload["launch_group_id"] = str(run_config.get("launch_group_id") or "").strip() or None
    payload["launch_state"] = str(run_config.get("launch_state") or "").strip().lower() or None
    payload["selected_tasks"] = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        run_config.get("selected_tasks")
    )
    payload["effective_selected_tasks"] = (
        _normalize_optional_social_account_catalog_backfill_selected_tasks(run_config.get("effective_selected_tasks"))
        or payload["selected_tasks"]
    )
    payload["comments_run_id"] = str(run_config.get("comments_run_id") or "").strip() or None
    for key in (
        "posts_auth_probe",
        "auth_repair_attempted",
        "auth_repair_status",
        "auth_repair_reason",
        "partial_scrape",
        "stop_reason",
        "launch_error_message",
        "launch_failed_at",
    ):
        if key in run_config:
            payload[key] = run_config.get(key)

    launch_state = str(payload.get("launch_state") or "").strip().lower()
    stop_reason = str(payload.get("stop_reason") or "").strip().lower()
    if launch_state == "blocked_auth" or stop_reason in {"posts_auth_blocked", "checkpoint_required"}:
        repair_environment = _catalog_run_auth_repair_environment(platform)
        repairable_reason = (
            str(run_config.get(_RUN_AUTH_REPAIR_REPAIRABLE_REASON_KEY) or "").strip().lower()
            or str(run_config.get("auth_repair_reason") or "").strip().lower()
            or str(run_config.get("blocked_reason") or "").strip().lower()
            or stop_reason
            or "posts_auth_blocked"
        )
        repair_status = (
            str(run_config.get(_RUN_AUTH_REPAIR_STATUS_KEY) or "").strip().lower()
            or str(run_config.get("auth_repair_status") or "").strip().lower()
            or "idle"
        )
        payload["run_state"] = "failed"
        payload["operational_state"] = "blocked_auth"
        payload["repair_action"] = str(repair_environment.get("repair_action") or "").strip().lower() or None
        payload["repair_status"] = repair_status
        payload["repairable_reason"] = repairable_reason
        payload["auto_resume_pending"] = bool(run_config.get(_RUN_AUTH_REPAIR_AUTO_RESUME_PENDING_KEY))
        payload["resume_stage"] = (
            str(run_config.get(_RUN_AUTH_REPAIR_RESUME_STAGE_KEY) or "").strip().lower() or "posts"
        )
        payload["repair_environment"] = repair_environment
        diagnostics = _metadata_dict(payload.get("run_diagnostics"))
        payload["run_diagnostics"] = {
            **diagnostics,
            "frontier_stop_reason": stop_reason or diagnostics.get("frontier_stop_reason"),
            "last_error_code": diagnostics.get("last_error_code") or repairable_reason,
            "last_error_message": diagnostics.get("last_error_message"),
        }
    payload.update(_blocked_budget_progress_payload(run_config))
    return payload


def get_social_account_catalog_run_progress(
    platform: str,
    account_handle: str,
    run_id: str,
    *,
    recent_log_limit: int = 20,
    fast: bool = False,
) -> dict[str, Any]:
    _sync_core_overrides()
    safe_recent_log_limit = max(1, min(int(recent_log_limit), 100))
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if normalized_platform not in set(CATALOG_SUPPORTED_PLATFORMS):
        raise ValueError("Catalog backfill is not supported for this platform.")
    if fast:
        features = {"has_run_id": True, "has_queue_fields": True}
    else:
        if not _relation_exists("social.scrape_runs") or not _relation_exists("social.scrape_jobs"):
            raise ValueError("social_ingest_queue_schema_missing")
        features = _scrape_jobs_features()
        if not bool(features.get("has_run_id")):
            raise ValueError("run_progress_requires_scrape_jobs_run_id")
    read_pool_name = SOCIAL_CATALOG_PROGRESS_POOL_NAME if fast else "default"

    def _load_run_and_jobs() -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if fast:
            with pg.db_read_connection(label="catalog_run_progress_fast", pool_name=read_pool_name) as conn:
                loaded_run_row = _load_social_account_catalog_run_row(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    run_id=run_id,
                    conn=conn,
                    verify_account=False,
                    pool_name=read_pool_name,
                )
                loaded_job_rows = _load_social_account_catalog_jobs(
                    run_id=run_id,
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    conn=conn,
                    features=features,
                    pool_name=read_pool_name,
                )
            return loaded_run_row, loaded_job_rows

        loaded_run_row = _load_social_account_catalog_run_row(
            platform=normalized_platform,
            account_handle=normalized_account,
            run_id=run_id,
            verify_account=True,
            pool_name=read_pool_name,
        )
        loaded_job_rows = _load_social_account_catalog_jobs(
            run_id=run_id,
            platform=normalized_platform,
            account_handle=normalized_account,
            features=features,
            pool_name=read_pool_name,
        )
        return loaded_run_row, loaded_job_rows

    try:
        run_row, job_rows = _load_run_and_jobs()
    except LookupError as exc:
        raise ValueError("run_not_found") from exc

    run_config = _metadata_dict(run_row.get("config"))
    configured_platforms = {
        _normalize_platform_name(value)
        for value in _as_text_list(run_config.get("platforms") or [])
        if _normalize_platform_name(value)
    }
    configured_accounts = {
        _normalize_social_account_profile_handle(value)
        for value in _as_text_list(run_config.get("accounts_override") or [])
        if _normalize_social_account_profile_handle(value)
    }
    if configured_platforms and normalized_platform not in configured_platforms:
        raise ValueError("run_not_found")
    if configured_accounts and normalized_account not in configured_accounts:
        raise ValueError("run_not_found")

    if not job_rows:
        run_config = _metadata_dict(run_row.get("config"))
        launch_state = str(run_config.get("launch_state") or "").strip().lower()
        task_pending = _catalog_launch_task_resolution_pending(run_config.get("launch_task_resolution_pending"))
        created_at = _coerce_dt(run_row.get("created_at"))
        pending_age_seconds = (_now_utc() - created_at).total_seconds() if created_at is not None else None
        fresh_pending_launch = (
            task_pending
            and launch_state in {"pending", "finalizing"}
            and (
                not _catalog_launch_finalizing_is_stale(run_config)
                or (
                    pending_age_seconds is not None
                    and pending_age_seconds < _CATALOG_LAUNCH_FINALIZING_RECOVERY_GRACE_SECONDS
                )
            )
        )
        terminal_zero_job_launch = launch_state in {"blocked_auth", "completed_no_work", "failed"}
        recovery_result: dict[str, Any] = {"recovered": False, "reason": "awaiting_finalize"}
        if not fresh_pending_launch and not terminal_zero_job_launch:
            recovery_result = recover_pending_social_account_catalog_launch(
                platform=normalized_platform,
                account_handle=normalized_account,
                run_id=run_id,
            )
        if bool(recovery_result.get("recovered")):
            try:
                run_row, job_rows = _load_run_and_jobs()
            except LookupError as exc:
                raise ValueError("run_not_found") from exc
            run_config = _metadata_dict(run_row.get("config"))
        if not job_rows:
            launch_state = str(run_config.get("launch_state") or "").strip().lower()
            no_work_reason = str(run_config.get("no_work_reason") or "").strip()
            recovery_reason = str(recovery_result.get("reason") or "").strip().lower()
            if (
                launch_state == "completed_no_work"
                or launch_state == "blocked_auth"
                or launch_state == "failed"
                or no_work_reason
                or (recovery_reason == "awaiting_finalize" and launch_state in {"pending", "finalizing"})
                or (recovery_reason == "finalize_in_progress" and launch_state == "finalizing")
            ):
                return _build_catalog_terminal_progress_payload(
                    run_row=run_row,
                    job_rows=[],
                    run_id=run_id,
                    run_config=run_config,
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    recent_log_limit=safe_recent_log_limit,
                    fast=fast,
                )
            raise ValueError("run_not_found")

    if fast:
        return _build_catalog_terminal_progress_payload(
            run_row=run_row,
            job_rows=job_rows,
            run_id=run_id,
            run_config=run_config,
            platform=normalized_platform,
            account_handle=normalized_account,
            recent_log_limit=safe_recent_log_limit,
            fast=True,
        )

    repaired_run_config = _repair_finalizing_catalog_launch_after_jobs(
        run_row=run_row,
        job_rows=job_rows,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    if repaired_run_config:
        run_config = repaired_run_config
        run_row = {**dict(run_row), "config": run_config}

    computed_summary = _summarize_run_progress_job_rows(job_rows)
    single_target_run = (not configured_platforms or configured_platforms == {normalized_platform}) and (
        not configured_accounts or configured_accounts == {normalized_account}
    )
    summary_override: Mapping[str, Any] | None = None
    if single_target_run:
        stored_summary = _metadata_dict(run_row.get("summary"))
        run_status = str(run_row.get("status") or "").strip().lower()
        if _run_progress_summary_needs_refresh(stored_summary, computed_summary) or (
            run_status in _RUN_PROGRESS_ACTIVE_JOB_STATUSES and computed_summary["active_jobs"] == 0
        ):
            if run_status in _RUN_PROGRESS_ACTIVE_JOB_STATUSES and computed_summary["active_jobs"] == 0:
                _finalize_run_status(run_id, force_recompute=True)
            else:
                _update_run_summary(run_id, force_recompute=True)
            refreshed_run_row = pg.fetch_one(
                """
                select
                  id::text as run_id,
                  season_id::text as season_id,
                  status,
                  source_scope,
                  config,
                  summary,
                  created_at,
                  started_at,
                  completed_at
                from social.scrape_runs
                where id = %s::uuid
                  and coalesce(config->>'pipeline_ingest_mode', '') = %s
                limit 1
                """,
                [run_id, SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE],
            )
            if refreshed_run_row:
                run_row = refreshed_run_row
        refreshed_summary = _metadata_dict(run_row.get("summary"))
        if _run_progress_summary_needs_refresh(refreshed_summary, computed_summary):
            summary_override = computed_summary
    else:
        summary_override = computed_summary

    if _can_fast_path_terminal_catalog_progress(
        run_row=run_row,
        configured_platforms=configured_platforms,
        configured_accounts=configured_accounts,
        normalized_platform=normalized_platform,
        normalized_account=normalized_account,
    ):
        return _build_catalog_terminal_progress_payload(
            run_row=run_row,
            job_rows=job_rows,
            run_id=run_id,
            run_config=run_config,
            platform=normalized_platform,
            account_handle=normalized_account,
            recent_log_limit=safe_recent_log_limit,
            fast=False,
        )

    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    recover_stale_unclaimed_dispatched_jobs(
        run_id=run_id,
        platform=normalized_platform,
        account_handle=normalized_account,
        limit=25,
    )
    recover_dispatch_blocked_no_progress_jobs(limit=25)

    payload = _build_run_progress_snapshot_payload(
        run_row=run_row,
        job_rows=job_rows,
        run_id=run_id,
        season_id=str(run_row.get("season_id") or "") or None,
        recent_log_limit=safe_recent_log_limit,
        summary_override=summary_override,
    )
    payload.update(_catalog_run_intent_metadata(run_config))
    payload.update(
        _catalog_progress_stage_graph_payload(
            run_config=run_config,
            stages_payload=payload.get("stages") or {},
            job_rows=job_rows,
        )
    )
    payload.update(
        _catalog_completion_progress_payload(
            run_config=run_config,
            stages_payload=payload.get("stages") or {},
        )
    )
    payload["launch_group_id"] = str(run_config.get("launch_group_id") or "").strip() or None
    payload["launch_state"] = str(run_config.get("launch_state") or "").strip().lower() or None
    payload["selected_tasks"] = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        run_config.get("selected_tasks")
    )
    payload["effective_selected_tasks"] = (
        _normalize_optional_social_account_catalog_backfill_selected_tasks(run_config.get("effective_selected_tasks"))
        or payload["selected_tasks"]
    )
    payload["comments_run_id"] = str(run_config.get("comments_run_id") or "").strip() or None
    payload.update(
        _catalog_posts_runtime_additive_payload(
            platform=normalized_platform,
            account_handle=normalized_account,
            run_id=run_id,
            run_config=run_config,
            job_rows=job_rows,
        )
    )
    payload["attached_followups"] = _resolve_run_attached_followups(
        run_config=run_config,
        run_id=run_id,
        run_status=str(run_row.get("status") or "").strip().lower() or None,
        comments_run_id=payload["comments_run_id"],
    )
    resume_state_payload = run_config.get("resume_state") if isinstance(run_config.get("resume_state"), dict) else None
    payload["resume_state"] = resume_state_payload
    partition_progress = _shared_account_partition_progress(
        run_id=run_id,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    frontier_progress = _shared_account_frontier_progress(
        run_id=run_id,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    payload["partition_strategy"] = str(
        run_config.get("partition_strategy")
        or frontier_progress.get("strategy")
        or partition_progress.get("partition_strategy")
        or ""
    )
    payload["discovery"] = partition_progress
    payload["frontier"] = frontier_progress
    post_progress = _metadata_dict(payload.get("post_progress"))
    allow_live_profile_refresh = not (
        normalized_platform == "instagram"
        and str(run_row.get("status") or "").strip().lower() in _RUN_PROGRESS_ACTIVE_JOB_STATUSES
    )
    expected_total_posts = max(
        _shared_account_expected_total_posts_from_config(
            run_config,
            platform=normalized_platform,
            account_handle=normalized_account,
        ),
        _normalize_non_negative_int(partition_progress.get("expected_total_posts")),
        _normalize_non_negative_int(frontier_progress.get("expected_total_posts")),
    )
    source_total_posts_current = _normalize_non_negative_int(
        _cached_live_profile_total_posts(normalized_platform, normalized_account)
        if allow_live_profile_refresh
        else _cached_live_profile_total_posts_cached_only(normalized_platform, normalized_account)
    )
    best_known_total_posts = _best_known_social_account_total_posts(
        normalized_platform,
        normalized_account,
        materialized_total_posts=_social_account_profile_total_posts(normalized_platform, normalized_account),
        catalog_total_posts=_shared_catalog_total_posts(normalized_platform, normalized_account),
        allow_live_refresh=allow_live_profile_refresh,
    )
    progress_total_posts = (
        max(
            expected_total_posts,
            _normalize_non_negative_int(post_progress.get("total_posts")),
            source_total_posts_current,
            best_known_total_posts,
        )
        if (
            _shared_catalog_mode(run_config)
            and _coerce_dt(run_config.get("date_start")) is None
            and _coerce_dt(run_config.get("date_end")) is None
            and str(run_config.get("catalog_action_scope") or "").strip().lower() in {"", "full_history"}
        )
        else expected_total_posts
        or _normalize_non_negative_int(post_progress.get("total_posts"))
        or best_known_total_posts
    )
    frontier_expected_total = _normalize_non_negative_int(frontier_progress.get("expected_total_posts"))
    frontier_completed_posts = _normalize_non_negative_int(frontier_progress.get("posts_checked"))
    if (
        frontier_expected_total > 0
        and bool(frontier_progress.get("exhausted"))
        and frontier_completed_posts < frontier_expected_total
    ):
        progress_total_posts = frontier_expected_total
    if progress_total_posts > 0:
        post_progress["total_posts"] = progress_total_posts
        payload["post_progress"] = post_progress
    post_progress["completed_posts"] = max(
        _normalize_non_negative_int(post_progress.get("completed_posts")),
        _normalize_non_negative_int(frontier_progress.get("posts_checked")),
    )
    post_progress["matched_posts"] = max(
        _normalize_non_negative_int(post_progress.get("matched_posts")),
        _normalize_non_negative_int(frontier_progress.get("posts_saved")),
    )
    if progress_total_posts > 0:
        post_progress["completed_posts"] = min(
            _normalize_non_negative_int(post_progress.get("completed_posts")),
            progress_total_posts,
        )
        post_progress["matched_posts"] = min(
            _normalize_non_negative_int(post_progress.get("matched_posts")),
            progress_total_posts,
        )
    payload["post_progress"] = post_progress
    payload["expected_total_posts"] = expected_total_posts or None
    payload["source_total_posts_current"] = source_total_posts_current or None
    completed_posts = _normalize_non_negative_int(post_progress.get("completed_posts"))
    total_posts_for_gap = _normalize_non_negative_int(post_progress.get("total_posts"))
    completion_gap_posts = max(total_posts_for_gap - completed_posts, 0) if total_posts_for_gap > 0 else 0
    completion_gap_reason: str | None = None
    frontier_stop_reason = (
        str(frontier_progress.get("stop_reason") or "").strip().lower()
        or str(_metadata_dict(frontier_progress.get("metadata")).get("frontier_stop_reason") or "").strip().lower()
        or None
    )
    if frontier_stop_reason == "catalog_oldest_stored_post_not_reached":
        completion_gap_reason = "history_boundary_incomplete"
    elif completion_gap_posts > 0 and bool(frontier_progress.get("exhausted")):
        if source_total_posts_current > 0 and completed_posts >= source_total_posts_current:
            completion_gap_reason = "source_total_drift"
        else:
            completion_gap_reason = "fetch_incomplete"
    payload["completion_gap_posts"] = completion_gap_posts
    payload["completion_gap_reason"] = completion_gap_reason
    stages_payload = _metadata_dict(payload.get("stages"))
    posts_stage = _metadata_dict(stages_payload.get(SHARED_ACCOUNT_POSTS_STAGE))
    classify_stage = _metadata_dict(stages_payload.get(POST_CLASSIFY_STAGE))
    posts_total = _normalize_non_negative_int(posts_stage.get("jobs_total"))
    posts_completed = _normalize_non_negative_int(posts_stage.get("jobs_completed"))
    posts_failed = _normalize_non_negative_int(posts_stage.get("jobs_failed"))
    posts_active = _normalize_non_negative_int(posts_stage.get("jobs_active"))
    posts_waiting = _normalize_non_negative_int(posts_stage.get("jobs_waiting"))
    classify_total = _normalize_non_negative_int(classify_stage.get("jobs_total"))
    classify_completed = _normalize_non_negative_int(classify_stage.get("jobs_completed"))
    classify_failed = _normalize_non_negative_int(classify_stage.get("jobs_failed"))
    classify_active = _normalize_non_negative_int(classify_stage.get("jobs_active"))
    classify_waiting = _normalize_non_negative_int(classify_stage.get("jobs_waiting"))
    dismissed_terminal_classify_cancel = (
        str(run_row.get("status") or "").strip().lower() == "completed"
        and bool(str(run_config.get(_RUN_FAILURE_DISMISSED_AT_KEY) or "").strip())
        and classify_total > 0
        and classify_active <= 0
        and classify_waiting <= 0
        and any(
            _run_progress_stage_from_row(row) == POST_CLASSIFY_STAGE
            and str(row.get("status") or "").strip().lower() == "cancelled"
            for row in job_rows
        )
    )
    payload["scrape_complete"] = (
        posts_total > 0
        and posts_completed >= posts_total
        and posts_failed <= 0
        and posts_active <= 0
        and posts_waiting <= 0
    )
    payload["classify_incomplete"] = (
        (not dismissed_terminal_classify_cancel)
        and classify_total > 0
        and (classify_completed + classify_failed < classify_total or classify_active > 0 or classify_waiting > 0)
    )
    if (
        str(payload.get("run_status") or "").strip().lower() == "completed"
        and _normalize_non_negative_int(post_progress.get("completed_posts")) <= 0
        and _normalize_non_negative_int(partition_progress.get("partition_count")) <= 0
        and _normalize_non_negative_int(
            (payload.get("stages") or {}).get(SHARED_ACCOUNT_DISCOVERY_STAGE, {}).get("jobs_completed")
        )
        > 0
        and _normalize_non_negative_int(
            (payload.get("stages") or {}).get(SHARED_ACCOUNT_POSTS_STAGE, {}).get("jobs_total")
        )
        <= 0
    ):
        payload["run_status"] = "failed"
    source_row = _load_shared_account_source_row(
        source_scope=str(run_row.get("source_scope") or payload.get("source_scope") or "network"),
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    shared_profile = _shared_profile_contract(
        source_scope=str(run_row.get("source_scope") or payload.get("source_scope") or "network"),
        platform=normalized_platform,
        account_handle=normalized_account,
        metadata=_metadata_dict((source_row or {}).get("metadata")),
    )
    queued_jobs_by_type = _queued_jobs_by_type(stages_payload)
    dispatch_health = _metadata_dict(payload.get("dispatch_health"))
    recovery_payload = _shared_account_recovery_payload(job_rows=job_rows, now=_now_utc())
    payload["shared_profile"] = shared_profile
    payload["network_name"] = shared_profile["network_name"]
    payload["profile_kind"] = shared_profile["profile_kind"]
    payload["assignment_mode"] = shared_profile["assignment_mode"]
    payload["assignment_rules"] = shared_profile["assignment_rules"]
    payload["queued_jobs_by_type"] = queued_jobs_by_type
    payload["recovery"] = recovery_payload
    payload["capacity_waiting"] = (
        _normalize_non_negative_int(dispatch_health.get("modal_pending_jobs"))
        + _normalize_non_negative_int(dispatch_health.get("modal_running_unclaimed_jobs"))
    ) > 0
    payload["active_transport"] = (
        str(frontier_progress.get("transport") or "").strip().lower()
        or str((_metadata_dict(payload.get("worker_runtime")).get("active_transport")) or "").strip().lower()
        or None
    )
    payload["required_execution_backend"] = (
        str(run_config.get("required_execution_backend") or "").strip().lower() or None
    )
    payload["allow_local_dev_inline_bypass"] = bool(run_config.get("allow_local_dev_inline_bypass"))
    frontier_metadata = _metadata_dict(frontier_progress.get("metadata"))
    declared_runner_strategy = str(run_config.get("runner_strategy") or "").strip().lower() or None
    declared_partition_strategy = str(run_config.get("partition_strategy") or "").strip().lower() or None
    effective_runner_strategy = (
        str((_metadata_dict(payload.get("worker_runtime")).get("runner_strategy")) or "").strip().lower() or None
    )
    effective_partition_strategy = (
        str((_metadata_dict(payload.get("worker_runtime")).get("partition_strategy")) or "").strip().lower() or None
    )
    worker_runtime_payload = _metadata_dict(payload.get("worker_runtime"))
    observed_runtime_versions = list(worker_runtime_payload.get("runtime_versions_observed") or [])
    effective_runtime = (
        _metadata_dict(observed_runtime_versions[0])
        if observed_runtime_versions
        else _metadata_dict(worker_runtime_payload.get("runtime_version"))
    )
    effective_execution_backend = str(effective_runtime.get("execution_backend") or "").strip().lower() or None
    replacement_run_id = str(run_config.get("replacement_run_id") or "").strip() or None
    auto_requeue_status = str(run_config.get("auto_requeue_status") or "").strip().lower() or None
    cancel_reason: str | None = str(run_config.get("cancel_reason") or "").strip().lower() or None
    last_error_code = str(frontier_metadata.get("last_error_code") or "").strip().lower() or None
    last_error_message = str(frontier_metadata.get("last_error_message") or "").strip() or None
    for row in job_rows:
        row_metadata = _metadata_dict(row.get("metadata"))
        if cancel_reason is None:
            cancel_reason = str(row_metadata.get("cancel_reason") or "").strip().lower() or None
        if last_error_code is None:
            last_error_code = str(row.get("last_error_code") or "").strip().lower() or None
        if last_error_message is None:
            last_error_message = str(row.get("error_message") or "").strip() or None
        if cancel_reason and last_error_code and last_error_message:
            break
    if dismissed_terminal_classify_cancel:
        cancel_reason = None
        last_error_code = None
        last_error_message = None
    last_transport_response = _catalog_run_last_transport_response(
        frontier_progress=frontier_progress,
        job_rows=job_rows,
    )
    payload["cancel_reason"] = cancel_reason
    payload["last_error_code"] = last_error_code
    payload["last_error_message"] = last_error_message
    payload["effective_execution_backend"] = effective_execution_backend
    payload["persist_counters"] = _run_progress_persist_counters(job_rows)
    if last_transport_response:
        payload["last_transport_response"] = last_transport_response
    payload["run_state"] = _derive_catalog_run_state(
        run_status=str(payload.get("run_status") or ""),
        scrape_complete=bool(payload.get("scrape_complete")),
        classify_incomplete=bool(payload.get("classify_incomplete")),
        stages_payload=stages_payload,
        frontier_progress=frontier_progress,
        recovery=recovery_payload,
    )
    payload["alerts"] = _build_catalog_run_progress_alerts(
        platform=normalized_platform,
        frontier_progress=frontier_progress,
        payload=payload,
        recovery=recovery_payload,
    )
    repair_environment = _catalog_run_auth_repair_environment(normalized_platform)
    repairable_reason = _catalog_run_repairable_auth_reason(
        platform=normalized_platform,
        job_rows=job_rows,
        frontier_progress=frontier_progress,
        last_error_code=last_error_code,
    )
    resume_stage = _catalog_run_auth_repair_resume_stage(
        repairable_reason=repairable_reason,
        run_config=run_config,
        frontier_progress=frontier_progress,
    )
    configured_repair_status = str(run_config.get(_RUN_AUTH_REPAIR_STATUS_KEY) or "").strip().lower() or None
    repair_status = configured_repair_status or ("idle" if repairable_reason else None)
    auto_resume_pending = bool(run_config.get(_RUN_AUTH_REPAIR_AUTO_RESUME_PENDING_KEY))
    payload["operational_state"] = (
        "runtime_superseded"
        if replacement_run_id and auto_requeue_status in {"queued", "running"}
        else (
            "blocked_auth"
            if repairable_reason
            and (
                str(payload.get("run_state") or "").strip().lower() == "failed"
                or str(payload.get("run_status") or "").strip().lower() == "failed"
                or str(run_row.get("status") or "").strip().lower() == "failed"
            )
            else payload.get("run_state")
        )
    )
    payload["repair_action"] = (
        str(repair_environment.get("repair_action") or "").strip().lower() or None if repairable_reason else None
    )
    payload["repair_status"] = repair_status
    payload["repairable_reason"] = repairable_reason
    payload["auto_resume_pending"] = auto_resume_pending
    payload["resume_stage"] = resume_stage
    payload["repair_environment"] = repair_environment
    persist_diagnostics = _metadata_dict(payload.get("persist_counters"))
    payload["run_diagnostics"] = {
        "cancel_reason": cancel_reason,
        "last_error_code": last_error_code,
        "last_error_message": last_error_message,
        "posts_upserted": _normalize_non_negative_int(persist_diagnostics.get("posts_upserted")),
        "posts_skipped": _normalize_non_negative_int(persist_diagnostics.get("posts_skipped")),
        "posts_skipped_by_reason": _metadata_dict(persist_diagnostics.get("posts_skipped_by_reason")),
        "silent_drop_detected": bool(persist_diagnostics.get("silent_drop_detected")),
        "frontier_auth_reason": str(frontier_metadata.get("auth_reason") or "").strip().lower() or None,
        "frontier_stop_reason": frontier_stop_reason,
        "declared_runner_strategy": declared_runner_strategy,
        "effective_runner_strategy": effective_runner_strategy,
        "declared_partition_strategy": declared_partition_strategy,
        "effective_partition_strategy": effective_partition_strategy,
        "effective_execution_backend": effective_execution_backend,
        "required_execution_backend": payload.get("required_execution_backend"),
        "allow_local_dev_inline_bypass": bool(payload.get("allow_local_dev_inline_bypass")),
        "catalog_oldest_post_at": _iso(_coerce_dt(frontier_progress.get("catalog_oldest_post_at"))),
        "oldest_posted_at_seen": _iso(_coerce_dt(frontier_progress.get("oldest_posted_at_seen"))),
        "newest_posted_at_seen": _iso(_coerce_dt(frontier_progress.get("newest_posted_at_seen"))),
        "last_transport_response": payload.get("last_transport_response"),
        "strategy_mismatch": bool(
            (
                declared_runner_strategy
                and effective_runner_strategy
                and declared_runner_strategy != effective_runner_strategy
            )
            or (
                declared_partition_strategy
                and effective_partition_strategy
                and declared_partition_strategy != effective_partition_strategy
            )
        ),
        "runtime_version_drift": bool(worker_runtime_payload.get("runtime_version_drift")),
        "replacement_run_id": replacement_run_id,
        "auto_requeue_status": auto_requeue_status,
    }
    payload.update(_blocked_budget_progress_payload(run_config))
    return payload


_LOCAL_ROOM_NAMES = {
    "get_social_account_catalog_run_progress",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "get_social_account_catalog_run_progress",
]
