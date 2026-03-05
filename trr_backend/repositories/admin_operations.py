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
    row = pg.fetch_one(
        """
        insert into core.admin_operation_events (
          operation_id,
          event_seq,
          event_type,
          event_payload
        )
        values (%s::uuid, %s, %s, %s::jsonb)
        returning
          operation_id::text as operation_id,
          event_seq,
          event_type,
          event_payload,
          created_at
        """,
        [operation_id, event_seq, event_name, _to_json(event_payload)],
    )
    if not row:
        raise RuntimeError("Failed to append admin operation event")
    return dict(row)


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
) -> dict[str, Any] | None:
    """Claim the next pending/stale operation for remote execution."""
    normalized_worker = _clean_text(worker_id)
    if not normalized_worker:
        raise ValueError("worker_id is required")

    safe_lease_seconds = max(15, int(lease_seconds))
    normalized_types = [str(item).strip() for item in (operation_types or []) if str(item).strip()]

    where_type = ""
    params: list[Any] = []
    if normalized_types:
        where_type = "and operation_type = any(%s::text[])"
        params.append(normalized_types)
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
