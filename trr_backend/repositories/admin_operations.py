"""Persistence helpers for admin operation lifecycle + SSE replay events."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, Literal

from trr_backend.db import pg

OperationStatus = Literal["pending", "running", "completed", "failed", "cancelled", "cancelling"]

ACTIVE_STATUSES: set[str] = {"pending", "running", "cancelling"}
TERMINAL_STATUSES: set[str] = {"completed", "failed", "cancelled"}
DEFAULT_OPERATION_STALE_AFTER_SECONDS = 300
DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS = 60
_PERSON_SCOPED_OPERATION_TYPES: set[str] = {
    "admin_person_refresh_images",
    "admin_person_reprocess_images",
}

_OPERATION_COLUMNS = """
  id::text,
  operation_type,
  status,
  initiated_by,
  request_id,
  client_session_id,
  client_workflow_id,
  request_payload,
  progress_payload,
  result_payload,
  error_payload,
  cancel_requested_at,
  claimed_by_worker_id,
  claim_token,
  lease_expires_at,
  heartbeat_at,
  attempt_count,
  next_retry_at,
  started_at,
  completed_at,
  created_at,
  updated_at
"""


def _to_json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=True, default=str)


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _normalize_operation(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    normalized = dict(row)
    identifier = normalized.get("id")
    if identifier is not None:
        normalized["id"] = str(identifier)
    claim_token = normalized.get("claim_token")
    if claim_token is not None:
        normalized["claim_token"] = str(claim_token)
    return normalized


def _extract_person_scoped_operation_person_id(
    operation_type: str,
    request_payload: dict[str, Any] | None,
) -> str | None:
    if str(operation_type or "").strip() not in _PERSON_SCOPED_OPERATION_TYPES:
        return None
    if not isinstance(request_payload, dict):
        return None
    return _clean_text(request_payload.get("person_id"))


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _safe_positive_seconds(value: int | None, *, default: int) -> int:
    if value is None:
        return default
    return max(15, int(value))


def _health_row_query() -> str:
    return f"""
        select
          {_OPERATION_COLUMNS},
          coalesce(dispatch_event.event_payload->>'execution_owner', '') as execution_owner,
          coalesce(dispatch_event.event_payload->>'execution_mode_canonical', '') as execution_mode_canonical,
          coalesce(dispatch_event.event_payload->>'execution_backend_canonical', '') as execution_backend_canonical,
          coalesce(progress_stage.phase, progress_stage.stage, '') as latest_phase,
          greatest(
            0,
            floor(extract(epoch from (now() - coalesce(op.started_at, op.created_at))))
          )::bigint as age_seconds,
          greatest(
            0,
            floor(extract(epoch from (now() - coalesce(op.heartbeat_at, op.updated_at, op.created_at))))
          )::bigint as last_update_age_seconds,
          case
            when op.cancel_requested_at is null then null
            else greatest(0, floor(extract(epoch from (now() - op.cancel_requested_at))))::bigint
          end as cancel_requested_age_seconds
        from core.admin_operations op
        left join lateral (
          select event_payload
          from core.admin_operation_events
          where operation_id = op.id
            and event_type in ('operation', 'dispatched_to_modal')
          order by event_seq desc
          limit 1
        ) dispatch_event on true
        left join lateral (
          select
            event_payload->>'phase' as phase,
            event_payload->>'stage' as stage
          from core.admin_operation_events
          where operation_id = op.id
            and event_type = 'progress'
          order by event_seq desc
          limit 1
        ) progress_stage on true
    """


def _is_operation_row_stale(
    row: dict[str, Any],
    *,
    stale_after_seconds: int,
    cancelling_grace_seconds: int,
) -> tuple[bool, str | None]:
    status = str(row.get("status") or "").strip().lower()
    if status not in ACTIVE_STATUSES:
        return False, None

    cancel_age = _coerce_int(row.get("cancel_requested_age_seconds"))
    last_update_age = _coerce_int(row.get("last_update_age_seconds"))
    age_seconds = _coerce_int(row.get("age_seconds"))

    if status == "cancelling" and cancel_age is not None and cancel_age >= cancelling_grace_seconds:
        return True, "cancelling_grace_exceeded"
    if last_update_age is not None and last_update_age >= stale_after_seconds:
        return True, "stale_heartbeat"
    if status == "pending" and age_seconds is not None and age_seconds >= stale_after_seconds:
        return True, "pending_timeout"
    return False, None


def _normalize_operation_health_row(
    row: dict[str, Any],
    *,
    stale_after_seconds: int,
    cancelling_grace_seconds: int,
) -> dict[str, Any]:
    normalized = _normalize_operation(row) or {}
    normalized["execution_owner"] = _clean_text(str(normalized.get("execution_owner") or "")) or None
    normalized["execution_mode_canonical"] = _clean_text(str(normalized.get("execution_mode_canonical") or "")) or None
    normalized["execution_backend_canonical"] = _clean_text(
        str(normalized.get("execution_backend_canonical") or "")
    ) or None
    normalized["latest_phase"] = _clean_text(str(normalized.get("latest_phase") or "")) or None
    normalized["age_seconds"] = _coerce_int(normalized.get("age_seconds")) or 0
    normalized["last_update_age_seconds"] = _coerce_int(normalized.get("last_update_age_seconds")) or 0
    normalized["cancel_requested_age_seconds"] = _coerce_int(normalized.get("cancel_requested_age_seconds"))
    is_stale, stale_reason = _is_operation_row_stale(
        normalized,
        stale_after_seconds=stale_after_seconds,
        cancelling_grace_seconds=cancelling_grace_seconds,
    )
    normalized["is_stale"] = is_stale
    normalized["stale_reason"] = stale_reason
    return normalized


def create_or_attach_operation(
    *,
    operation_type: str,
    request_payload: dict[str, Any] | None = None,
    initiated_by: str | None = None,
    request_id: str | None = None,
    client_session_id: str | None = None,
    client_workflow_id: str | None = None,
    allow_attach: bool = True,
) -> tuple[dict[str, Any], bool]:
    """Create a new operation, or attach to an active one for the same tab/workflow."""
    op_type = str(operation_type or "").strip()
    if not op_type:
        raise ValueError("operation_type is required")

    session_id = _clean_text(client_session_id)
    workflow_id = _clean_text(client_workflow_id)

    person_scoped_person_id = _extract_person_scoped_operation_person_id(op_type, request_payload)

    if allow_attach and session_id:
        if workflow_id:
            attached = pg.fetch_one(
                f"""
                select
                  {_OPERATION_COLUMNS}
                from core.admin_operations
                where operation_type = %s
                  and client_session_id = %s
                  and client_workflow_id = %s
                  and status in ('pending', 'running', 'cancelling')
                order by created_at desc
                limit 1
                """,
                [op_type, session_id, workflow_id],
            )
        else:
            attached = pg.fetch_one(
                f"""
                select
                  {_OPERATION_COLUMNS}
                from core.admin_operations
                where operation_type = %s
                  and client_session_id = %s
                  and status in ('pending', 'running', 'cancelling')
                  and request_payload = %s::jsonb
                order by created_at desc
                limit 1
                """,
                [op_type, session_id, _to_json(request_payload)],
            )
        if attached:
            return _normalize_operation(attached) or {}, True

    if allow_attach and person_scoped_person_id:
        attached = pg.fetch_one(
            f"""
            select
              {_OPERATION_COLUMNS}
            from core.admin_operations
            where operation_type = %s
              and request_payload->>'person_id' = %s
              and status in ('pending', 'running', 'cancelling')
            order by
              case
                when status = 'running' then 0
                when status = 'cancelling' then 1
                else 2
              end,
              coalesce(heartbeat_at, updated_at, created_at) desc,
              created_at desc
            limit 1
            """,
            [op_type, person_scoped_person_id],
        )
        if attached:
            return _normalize_operation(attached) or {}, True

    created = pg.fetch_one(
        f"""
        insert into core.admin_operations (
          operation_type,
          status,
          initiated_by,
          request_id,
          client_session_id,
          client_workflow_id,
          request_payload,
          progress_payload,
          attempt_count
        )
        values (%s, 'pending', %s, %s, %s, %s, %s::jsonb, '{{}}'::jsonb, 0)
        returning
          {_OPERATION_COLUMNS}
        """,
        [
            op_type,
            _clean_text(initiated_by),
            _clean_text(request_id),
            session_id,
            workflow_id,
            _to_json(request_payload),
        ],
    )
    if not created:
        raise RuntimeError("Failed to create admin operation")
    return _normalize_operation(created) or {}, False


def get_operation(operation_id: str) -> dict[str, Any] | None:
    row = pg.fetch_one(
        f"""
        select
          {_OPERATION_COLUMNS}
        from core.admin_operations
        where id = %s::uuid
        limit 1
        """,
        [operation_id],
    )
    return _normalize_operation(row)


def append_operation_event(
    operation_id: str,
    *,
    event_type: str,
    event_payload: dict[str, Any] | None = None,
    event_seq: int | None = None,
) -> dict[str, Any]:
    event_name = str(event_type or "").strip() or "progress"
    params = [operation_id, operation_id, event_seq, event_name, _to_json(event_payload)]
    query = """
        with locked_operation as (
          select id
          from core.admin_operations
          where id = %s::uuid
          for update
        ),
        next_event as (
          select coalesce(max(e.event_seq), 0) + 1 as event_seq
          from core.admin_operation_events e
          where e.operation_id = %s::uuid
        )
        insert into core.admin_operation_events (
          operation_id,
          event_seq,
          event_type,
          event_payload
        )
        select
          locked_operation.id,
          coalesce(%s, next_event.event_seq),
          %s,
          %s::jsonb
        from locked_operation
        cross join next_event
        returning
          operation_id::text as operation_id,
          event_seq,
          event_type,
          event_payload,
          created_at
        """
    for attempt in range(3):
        try:
            row = pg.fetch_one(query, params)
        except Exception as exc:
            if event_seq is None and "admin_operation_events_op_seq_unique" in str(exc) and attempt < 2:
                continue
            raise
        if row:
            return dict(row)
    raise RuntimeError("Failed to append admin operation event")


def stream_events_after_seq(
    operation_id: str,
    *,
    after_seq: int = 0,
    limit: int = 200,
) -> list[dict[str, Any]]:
    safe_after = max(0, int(after_seq))
    safe_limit = max(1, min(int(limit), 1000))
    rows = pg.fetch_all(
        """
        select
          operation_id::text as operation_id,
          event_seq,
          event_type,
          event_payload,
          created_at
        from core.admin_operation_events
        where operation_id = %s::uuid
          and event_seq > %s
        order by event_seq asc
        limit %s
        """,
        [operation_id, safe_after, safe_limit],
    )
    return [dict(row) for row in rows]


def update_operation_progress(operation_id: str, *, progress_payload: dict[str, Any] | None = None) -> None:
    pg.fetch_one(
        """
        update core.admin_operations
        set
          progress_payload = %s::jsonb,
          status = case when status = 'pending' then 'running' else status end,
          started_at = coalesce(started_at, now()),
          heartbeat_at = now()
        where id = %s::uuid
          and status not in ('completed', 'failed', 'cancelled')
        returning id
        """,
        [_to_json(progress_payload), operation_id],
    )


def update_operation_status(
    operation_id: str,
    *,
    status: OperationStatus,
    result_payload: dict[str, Any] | None = None,
    error_payload: dict[str, Any] | None = None,
    progress_payload: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    next_status = str(status or "").strip().lower()
    if next_status not in {"pending", "running", "completed", "failed", "cancelled", "cancelling"}:
        raise ValueError(f"Unsupported operation status: {status}")

    row = pg.fetch_one(
        f"""
        update core.admin_operations
        set
          status = %s,
          progress_payload = coalesce(%s::jsonb, progress_payload),
          result_payload = coalesce(%s::jsonb, result_payload),
          error_payload = coalesce(%s::jsonb, error_payload),
          started_at = case
            when %s in ('running', 'completed', 'failed', 'cancelled', 'cancelling') then coalesce(started_at, now())
            else started_at
          end,
          completed_at = case
            when %s in ('completed', 'failed', 'cancelled') then coalesce(completed_at, now())
            else completed_at
          end,
          claimed_by_worker_id = case
            when %s in ('completed', 'failed', 'cancelled') then null
            else claimed_by_worker_id
          end,
          claim_token = case
            when %s in ('completed', 'failed', 'cancelled') then null
            else claim_token
          end,
          lease_expires_at = case
            when %s in ('completed', 'failed', 'cancelled') then null
            else lease_expires_at
          end,
          heartbeat_at = now(),
          next_retry_at = case
            when %s in ('completed', 'failed', 'cancelled', 'running', 'cancelling') then null
            else next_retry_at
          end
        where id = %s::uuid
        returning
          {_OPERATION_COLUMNS}
        """,
        [
            next_status,
            _to_json(progress_payload) if progress_payload is not None else None,
            _to_json(result_payload) if result_payload is not None else None,
            _to_json(error_payload) if error_payload is not None else None,
            next_status,
            next_status,
            next_status,
            next_status,
            next_status,
            next_status,
            operation_id,
        ],
    )
    return _normalize_operation(row)


def request_operation_cancel(operation_id: str) -> dict[str, Any] | None:
    row = pg.fetch_one(
        f"""
        update core.admin_operations
        set
          cancel_requested_at = coalesce(cancel_requested_at, now()),
          status = case
            when status in ('pending', 'running') then 'cancelling'
            else status
          end,
          heartbeat_at = now()
        where id = %s::uuid
        returning
          {_OPERATION_COLUMNS}
        """,
        [operation_id],
    )
    return _normalize_operation(row)


def request_related_operation_cancels(operation_id: str) -> list[dict[str, Any]]:
    operation = get_operation(operation_id)
    if not operation:
        return []

    operation_type = str(operation.get("operation_type") or "").strip()
    person_scoped_person_id = _extract_person_scoped_operation_person_id(
        operation_type,
        operation.get("request_payload") if isinstance(operation.get("request_payload"), dict) else None,
    )

    if not person_scoped_person_id:
        row = request_operation_cancel(operation_id)
        return [row] if row else []

    rows = pg.fetch_all(
        f"""
        update core.admin_operations
        set
          cancel_requested_at = coalesce(cancel_requested_at, now()),
          status = case
            when status in ('pending', 'running') then 'cancelling'
            else status
          end,
          heartbeat_at = now()
        where operation_type = %s
          and request_payload->>'person_id' = %s
          and status in ('pending', 'running', 'cancelling')
        returning
          {_OPERATION_COLUMNS}
        """,
        [operation_type, person_scoped_person_id],
    )
    return [_normalize_operation(dict(row)) or {} for row in rows]


def list_active_operations(
    *,
    operation_types: Iterable[str] | None = None,
    limit: int = 200,
    stale_after_seconds: int = DEFAULT_OPERATION_STALE_AFTER_SECONDS,
    cancelling_grace_seconds: int = DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    normalized_types = [str(item).strip() for item in (operation_types or []) if str(item).strip()]
    where_type = ""
    params: list[Any] = []
    if normalized_types:
        where_type = "and op.operation_type = any(%s::text[])"
        params.append(normalized_types)
    params.append(safe_limit)
    rows = pg.fetch_all(
        f"""
        {_health_row_query()}
        where op.status in ('pending', 'running', 'cancelling')
          {where_type}
        order by coalesce(op.heartbeat_at, op.updated_at, op.created_at) desc, op.created_at desc
        limit %s
        """,
        params,
    )
    return [
        _normalize_operation_health_row(
            dict(row),
            stale_after_seconds=stale_after_seconds,
            cancelling_grace_seconds=cancelling_grace_seconds,
        )
        for row in rows
    ]


def list_stale_operations(
    *,
    operation_types: Iterable[str] | None = None,
    limit: int = 200,
    stale_after_seconds: int = DEFAULT_OPERATION_STALE_AFTER_SECONDS,
    cancelling_grace_seconds: int = DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS,
) -> list[dict[str, Any]]:
    return [
        row
        for row in list_active_operations(
            operation_types=operation_types,
            limit=limit,
            stale_after_seconds=stale_after_seconds,
            cancelling_grace_seconds=cancelling_grace_seconds,
        )
        if row.get("is_stale") is True
    ]


def get_admin_operations_health(
    *,
    operation_types: Iterable[str] | None = None,
    limit: int = 200,
    stale_after_seconds: int = DEFAULT_OPERATION_STALE_AFTER_SECONDS,
    cancelling_grace_seconds: int = DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS,
) -> dict[str, Any]:
    active_operations = list_active_operations(
        operation_types=operation_types,
        limit=limit,
        stale_after_seconds=stale_after_seconds,
        cancelling_grace_seconds=cancelling_grace_seconds,
    )
    stale_operations = [row for row in active_operations if row.get("is_stale") is True]
    by_status: dict[str, int] = {}
    by_type: dict[str, int] = {}
    runtime_counts = {"modal": 0, "local": 0, "other": 0, "unknown": 0}
    for row in active_operations:
        status = str(row.get("status") or "").strip().lower() or "unknown"
        by_status[status] = by_status.get(status, 0) + 1
        operation_type = str(row.get("operation_type") or "").strip() or "unknown"
        by_type[operation_type] = by_type.get(operation_type, 0) + 1
        backend = str(row.get("execution_backend_canonical") or "").strip().lower()
        if backend == "modal":
            runtime_counts["modal"] += 1
        elif backend == "local":
            runtime_counts["local"] += 1
        elif backend:
            runtime_counts["other"] += 1
        else:
            runtime_counts["unknown"] += 1

    return {
        "summary": {
            "active_total": len(active_operations),
            "stale_total": len(stale_operations),
            "cancelling_total": by_status.get("cancelling", 0),
            "by_status": by_status,
            "by_type": by_type,
            "runtime_split": runtime_counts,
            "stale_after_seconds": stale_after_seconds,
            "cancelling_grace_seconds": cancelling_grace_seconds,
        },
        "active_operations": active_operations,
        "stale_operations": stale_operations,
        "updated_at": now_iso(),
    }


def force_cancel_stale_operations(
    *,
    operation_ids: Iterable[str] | None = None,
    cancelled_by: str | None = None,
    stale_after_seconds: int = DEFAULT_OPERATION_STALE_AFTER_SECONDS,
    cancelling_grace_seconds: int = DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS,
    force_selected: bool = False,
) -> dict[str, Any]:
    normalized_ids = [str(item).strip() for item in (operation_ids or []) if str(item).strip()]
    active_rows = list_active_operations(
        limit=max(len(normalized_ids), 200),
        stale_after_seconds=stale_after_seconds,
        cancelling_grace_seconds=cancelling_grace_seconds,
    )
    if normalized_ids:
        candidate_rows = [row for row in active_rows if str(row.get("id") or "") in set(normalized_ids)]
    else:
        candidate_rows = [row for row in active_rows if row.get("is_stale") is True]

    target_rows = (
        candidate_rows
        if (force_selected and normalized_ids)
        else [row for row in candidate_rows if row.get("is_stale") is True]
    )
    target_ids = [str(row.get("id") or "") for row in target_rows if str(row.get("id") or "").strip()]
    if not target_ids:
        health = get_admin_operations_health(
            stale_after_seconds=stale_after_seconds,
            cancelling_grace_seconds=cancelling_grace_seconds,
        )
        return {
            "cancelled_operations": 0,
            "cancelled_operation_ids": [],
            "stale_operations_remaining": len(health["stale_operations"]),
            "active_operations_remaining": len(health["active_operations"]),
            "updated_at": health["updated_at"],
        }

    stale_ids = [str(row.get("id") or "") for row in target_rows]
    reason_by_id = {
        str(row.get("id") or ""): (
            "force_selected"
            if force_selected and normalized_ids
            else str(row.get("stale_reason") or "stale_cleanup")
        )
        for row in target_rows
    }
    cancellation_code = (
        "FORCE_CANCELLED_BY_OPERATOR"
        if force_selected and normalized_ids
        else "STALE_OPERATION_CANCELLED"
    )
    cancellation_message = (
        "Admin operation force-cancelled by operator."
        if force_selected and normalized_ids
        else "Stale admin operation cancelled by health cleanup policy."
    )
    update_rows = pg.fetch_all(
        f"""
        update core.admin_operations
        set
          status = 'cancelled',
          completed_at = coalesce(completed_at, now()),
          claimed_by_worker_id = null,
          claim_token = null,
          lease_expires_at = null,
          next_retry_at = null,
          heartbeat_at = now(),
          error_payload = coalesce(error_payload, '{{}}'::jsonb) || %s::jsonb,
          progress_payload = coalesce(progress_payload, '{{}}'::jsonb) || %s::jsonb
        where id = any(%s::uuid[])
          and status in ('pending', 'running', 'cancelling')
        returning
          {_OPERATION_COLUMNS}
        """,
        [
            _to_json(
                {
                    "code": cancellation_code,
                    "message": cancellation_message,
                    "cancelled_by": _clean_text(cancelled_by),
                    "stale_cleanup": not (force_selected and normalized_ids),
                    "force_cancelled": bool(force_selected and normalized_ids),
                }
            ),
            _to_json(
                {
                    "message": cancellation_message,
                    "stale_cleanup": not (force_selected and normalized_ids),
                    "force_cancelled": bool(force_selected and normalized_ids),
                }
            ),
            stale_ids,
        ],
    )
    cancelled_ids: list[str] = []
    for row in update_rows:
        normalized = _normalize_operation(row) or {}
        operation_id = str(normalized.get("id") or "").strip()
        if not operation_id:
            continue
        cancelled_ids.append(operation_id)
        append_operation_event(
            operation_id,
            event_type="error",
            event_payload={
                "operation_id": operation_id,
                "status": "cancelled",
                "message": cancellation_message,
                "stale_cleanup": not (force_selected and normalized_ids),
                "force_cancelled": bool(force_selected and normalized_ids),
                "stale_reason": reason_by_id.get(operation_id),
                "cancelled_by": _clean_text(cancelled_by),
            },
        )

    health = get_admin_operations_health(
        stale_after_seconds=stale_after_seconds,
        cancelling_grace_seconds=cancelling_grace_seconds,
    )
    return {
        "cancelled_operations": len(cancelled_ids),
        "cancelled_operation_ids": cancelled_ids,
        "stale_operations_remaining": len(health["stale_operations"]),
        "active_operations_remaining": len(health["active_operations"]),
        "updated_at": health["updated_at"],
    }


def is_cancel_requested(operation_id: str) -> bool:
    row = pg.fetch_one(
        """
        select cancel_requested_at
        from core.admin_operations
        where id = %s::uuid
        limit 1
        """,
        [operation_id],
    )
    return bool(row and row.get("cancel_requested_at"))


def claim_next_operation(
    worker_id: str,
    *,
    lease_seconds: int = 180,
    operation_types: Iterable[str] | None = None,
    exclude_operation_types: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    """Claim the next pending/stale operation for remote execution."""
    normalized_worker = _clean_text(worker_id)
    if not normalized_worker:
        raise ValueError("worker_id is required")

    force_cancel_stale_operations(
        stale_after_seconds=DEFAULT_OPERATION_STALE_AFTER_SECONDS,
        cancelling_grace_seconds=DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS,
    )

    safe_lease_seconds = max(15, int(lease_seconds))
    normalized_types = [str(item).strip() for item in (operation_types or []) if str(item).strip()]
    excluded_types = [str(item).strip() for item in (exclude_operation_types or []) if str(item).strip()]

    where_type = ""
    params: list[Any] = []
    if normalized_types:
        where_type = "and operation_type = any(%s::text[])"
        params.append(normalized_types)
    if excluded_types:
        where_type += " and not (operation_type = any(%s::text[]))"
        params.append(excluded_types)
    params.extend([normalized_worker, safe_lease_seconds])

    row = pg.fetch_one(
        f"""
        with candidate as (
          select id
          from core.admin_operations
          where status in ('pending', 'running', 'cancelling')
            and coalesce(next_retry_at, now()) <= now()
            and (
              status = 'pending'
              or lease_expires_at is null
              or lease_expires_at < now()
              or coalesce(heartbeat_at, updated_at, created_at) < now() - interval '5 minutes'
            )
            {where_type}
          order by
            case when status = 'pending' then 0 else 1 end,
            created_at asc
          limit 1
          for update skip locked
        )
        update core.admin_operations as op
        set
          status = case when op.status = 'pending' then 'running' else op.status end,
          started_at = case when op.status = 'pending' then coalesce(op.started_at, now()) else op.started_at end,
          claimed_by_worker_id = %s,
          claim_token = gen_random_uuid()::text,
          lease_expires_at = now() + (%s::int * interval '1 second'),
          heartbeat_at = now(),
          attempt_count = coalesce(op.attempt_count, 0) + 1,
          next_retry_at = null
        from candidate
        where op.id = candidate.id
        returning
          op.id::text as id,
          op.operation_type,
          op.status,
          op.initiated_by,
          op.request_id,
          op.client_session_id,
          op.client_workflow_id,
          op.request_payload,
          op.progress_payload,
          op.result_payload,
          op.error_payload,
          op.cancel_requested_at,
          op.claimed_by_worker_id,
          op.claim_token,
          op.lease_expires_at,
          op.heartbeat_at,
          op.attempt_count,
          op.next_retry_at,
          op.started_at,
          op.completed_at,
          op.created_at,
          op.updated_at
        """,
        params,
    )
    return _normalize_operation(row)


def claim_operation(
    operation_id: str,
    worker_id: str,
    *,
    lease_seconds: int = 180,
    operation_types: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    """Claim a specific operation for remote execution if it is still available."""
    normalized_worker = _clean_text(worker_id)
    if not normalized_worker:
        raise ValueError("worker_id is required")

    force_cancel_stale_operations(
        operation_ids=[operation_id],
        stale_after_seconds=DEFAULT_OPERATION_STALE_AFTER_SECONDS,
        cancelling_grace_seconds=DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS,
    )

    safe_lease_seconds = max(15, int(lease_seconds))
    normalized_types = [str(item).strip() for item in (operation_types or []) if str(item).strip()]

    where_type = ""
    params: list[Any] = [operation_id]
    if normalized_types:
        where_type = "and operation_type = any(%s::text[])"
        params.append(normalized_types)
    params.extend([normalized_worker, safe_lease_seconds])

    row = pg.fetch_one(
        f"""
        with candidate as (
          select id
          from core.admin_operations
          where id = %s::uuid
            and status in ('pending', 'running', 'cancelling')
            and coalesce(next_retry_at, now()) <= now()
            and (
              status = 'pending'
              or lease_expires_at is null
              or lease_expires_at < now()
              or coalesce(heartbeat_at, updated_at, created_at) < now() - interval '5 minutes'
            )
            {where_type}
          for update skip locked
        )
        update core.admin_operations as op
        set
          status = case when op.status = 'pending' then 'running' else op.status end,
          started_at = case when op.status = 'pending' then coalesce(op.started_at, now()) else op.started_at end,
          claimed_by_worker_id = %s,
          claim_token = gen_random_uuid()::text,
          lease_expires_at = now() + (%s::int * interval '1 second'),
          heartbeat_at = now(),
          attempt_count = coalesce(op.attempt_count, 0) + 1,
          next_retry_at = null
        from candidate
        where op.id = candidate.id
        returning
          op.id::text as id,
          op.operation_type,
          op.status,
          op.initiated_by,
          op.request_id,
          op.client_session_id,
          op.client_workflow_id,
          op.request_payload,
          op.progress_payload,
          op.result_payload,
          op.error_payload,
          op.cancel_requested_at,
          op.claimed_by_worker_id,
          op.claim_token,
          op.lease_expires_at,
          op.heartbeat_at,
          op.attempt_count,
          op.next_retry_at,
          op.started_at,
          op.completed_at,
          op.created_at,
          op.updated_at
        """,
        params,
    )
    return _normalize_operation(row)


def heartbeat_operation_claim(
    operation_id: str,
    *,
    claim_token: str | None,
    lease_seconds: int = 180,
) -> bool:
    safe_lease_seconds = max(15, int(lease_seconds))
    token = _clean_text(claim_token)
    row = pg.fetch_one(
        """
        update core.admin_operations
        set
          heartbeat_at = now(),
          lease_expires_at = now() + (%s::int * interval '1 second')
        where id = %s::uuid
          and status in ('running', 'cancelling')
          and (%s::text is null or claim_token = %s::text)
        returning id
        """,
        [safe_lease_seconds, operation_id, token, token],
    )
    return bool(row)


def release_operation_claim(
    operation_id: str,
    *,
    claim_token: str | None,
    clear_worker_id: bool = True,
) -> bool:
    token = _clean_text(claim_token)
    row = pg.fetch_one(
        """
        update core.admin_operations
        set
          claim_token = null,
          lease_expires_at = null,
          heartbeat_at = now(),
          claimed_by_worker_id = case when %s then null else claimed_by_worker_id end
        where id = %s::uuid
          and (%s::text is null or claim_token = %s::text)
        returning id
        """,
        [bool(clear_worker_id), operation_id, token, token],
    )
    return bool(row)


def mark_operation_retry(
    operation_id: str,
    *,
    claim_token: str | None,
    error_payload: dict[str, Any] | None,
    retry_delay_seconds: int,
    max_attempts: int,
) -> dict[str, Any] | None:
    token = _clean_text(claim_token)
    safe_delay = max(1, int(retry_delay_seconds))
    safe_max_attempts = max(1, int(max_attempts))

    row = pg.fetch_one(
        f"""
        update core.admin_operations
        set
          status = case
            when coalesce(attempt_count, 0) >= %s then 'failed'
            else 'pending'
          end,
          error_payload = coalesce(%s::jsonb, error_payload),
          progress_payload = coalesce(%s::jsonb, progress_payload),
          completed_at = case
            when coalesce(attempt_count, 0) >= %s then coalesce(completed_at, now())
            else completed_at
          end,
          next_retry_at = case
            when coalesce(attempt_count, 0) >= %s then null
            else now() + (%s::int * interval '1 second')
          end,
          claimed_by_worker_id = null,
          claim_token = null,
          lease_expires_at = null,
          heartbeat_at = now()
        where id = %s::uuid
          and (%s::text is null or claim_token = %s::text)
        returning
          {_OPERATION_COLUMNS}
        """,
        [
            safe_max_attempts,
            _to_json(error_payload) if error_payload is not None else None,
            _to_json(error_payload) if error_payload is not None else None,
            safe_max_attempts,
            safe_max_attempts,
            safe_delay,
            operation_id,
            token,
            token,
        ],
    )
    return _normalize_operation(row)


def purge_old_operations(*, retention_hours: int = 336) -> int:
    safe_hours = max(1, int(retention_hours))
    row = pg.fetch_one(
        """
        select core.purge_admin_operations((%s::text || ' hours')::interval) as deleted_count
        """,
        [safe_hours],
    )
    if not row:
        return 0
    return int(row.get("deleted_count") or 0)


def operation_is_terminal(status: str | None) -> bool:
    return str(status or "").strip().lower() in TERMINAL_STATUSES


def normalize_operation_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for event in events:
        row = dict(event)
        op_id = row.get("operation_id")
        if op_id is not None:
            row["operation_id"] = str(op_id)
        row["event_seq"] = int(row.get("event_seq") or 0)
        normalized.append(row)
    return normalized


def get_operation_queue_position(operation_id: str) -> dict[str, int]:
    row = pg.fetch_one(
        """
        with current_op as (
          select created_at
          from core.admin_operations
          where id = %s::uuid
        )
        select
          (select count(*) from core.admin_operations o where o.status = 'pending') as queued_total,
          (
            select count(*)
            from core.admin_operations o
            where o.status = 'pending'
              and o.created_at <= (select created_at from current_op)
          ) as queued_ahead_or_self
        """,
        [operation_id],
    )
    return {
        "queued_total": int((row or {}).get("queued_total") or 0),
        "queued_ahead_or_self": int((row or {}).get("queued_ahead_or_self") or 0),
    }


def touch_operation_started(operation_id: str) -> None:
    pg.fetch_one(
        """
        update core.admin_operations
        set
          status = case when status = 'pending' then 'running' else status end,
          started_at = coalesce(started_at, now()),
          heartbeat_at = now()
        where id = %s::uuid
        returning id
        """,
        [operation_id],
    )


def touch_operation_completed_if_active(operation_id: str) -> None:
    pg.fetch_one(
        """
        update core.admin_operations
        set
          status = case
            when status in ('pending', 'running', 'cancelling') then 'completed'
            else status
          end,
          completed_at = case
            when status in ('pending', 'running', 'cancelling') then coalesce(completed_at, now())
            else completed_at
          end,
          claim_token = null,
          lease_expires_at = null,
          claimed_by_worker_id = null,
          heartbeat_at = now()
        where id = %s::uuid
        returning id
        """,
        [operation_id],
    )


def mark_operation_failed(operation_id: str, *, error_payload: dict[str, Any]) -> None:
    update_operation_status(
        operation_id,
        status="failed",
        error_payload=error_payload,
        progress_payload=error_payload,
    )


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
