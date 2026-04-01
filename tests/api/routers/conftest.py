from __future__ import annotations

from copy import deepcopy
from threading import Lock
from typing import Any
from uuid import uuid4

import pytest

from api.routers import socials as socials_router
from trr_backend.pipeline import admin_operations as pipeline_admin_operations
from trr_backend.repositories import admin_operations as admin_ops_repo


@pytest.fixture(autouse=True)
def _clear_social_router_caches() -> None:
    socials_router._clear_account_profile_caches()
    socials_router.invalidate_week_detail_cache()


@pytest.fixture(autouse=True)
def _in_memory_admin_operation_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide an in-memory admin-operation store for router tests.

    Router tests run without applying migrations, so operation-backed SSE routes cannot rely on
    `core.admin_operations` tables existing. This fixture preserves operation stream semantics
    (operation envelope, event_seq replay, status transitions) without touching Postgres.
    """

    operations: dict[str, dict[str, Any]] = {}
    operation_events: dict[str, list[dict[str, Any]]] = {}
    lock = Lock()

    def _copy_operation(op: dict[str, Any] | None) -> dict[str, Any] | None:
        if not op:
            return None
        return deepcopy(op)

    def _ensure_operation(op_id: str) -> dict[str, Any]:
        existing = operations.get(op_id)
        if existing is None:
            existing = {
                "id": op_id,
                "operation_type": "unknown",
                "status": "pending",
                "initiated_by": None,
                "request_id": None,
                "client_session_id": None,
                "client_workflow_id": None,
                "request_payload": {},
                "progress_payload": {},
                "result_payload": None,
                "error_payload": None,
                "cancel_requested_at": None,
                "started_at": None,
                "completed_at": None,
                "created_at": "now",
                "updated_at": "now",
            }
            operations[op_id] = existing
        operation_events.setdefault(op_id, [])
        return existing

    def _create_or_attach_operation(
        *,
        operation_type: str,
        request_payload: dict[str, Any] | None = None,
        initiated_by: str | None = None,
        request_id: str | None = None,
        client_session_id: str | None = None,
        client_workflow_id: str | None = None,
        allow_attach: bool = True,
    ) -> tuple[dict[str, Any], bool]:
        with lock:
            if allow_attach and client_session_id:
                for op in operations.values():
                    if op.get("operation_type") != operation_type:
                        continue
                    if op.get("client_session_id") != client_session_id:
                        continue
                    if client_workflow_id and op.get("client_workflow_id") != client_workflow_id:
                        continue
                    if str(op.get("status") or "") not in {"pending", "running", "cancelling"}:
                        continue
                    return _copy_operation(op) or {}, True

            op_id = str(uuid4())
            operation = {
                "id": op_id,
                "operation_type": operation_type,
                "status": "pending",
                "initiated_by": initiated_by,
                "request_id": request_id,
                "client_session_id": client_session_id,
                "client_workflow_id": client_workflow_id,
                "request_payload": deepcopy(request_payload or {}),
                "progress_payload": {},
                "result_payload": None,
                "error_payload": None,
                "cancel_requested_at": None,
                "started_at": None,
                "completed_at": None,
                "created_at": "now",
                "updated_at": "now",
            }
            operations[op_id] = operation
            operation_events[op_id] = []
            return _copy_operation(operation) or {}, False

    def _get_operation(operation_id: str) -> dict[str, Any] | None:
        with lock:
            return _copy_operation(operations.get(operation_id))

    def _get_latest_operation_for_request_payload(
        *,
        operation_type: str,
        request_payload: dict[str, Any] | None = None,
        statuses: list[str] | None = None,
    ) -> dict[str, Any] | None:
        with lock:
            normalized_statuses = {
                str(status or "").strip().lower() for status in (statuses or []) if str(status or "").strip()
            }
            matches = [
                deepcopy(op)
                for op in operations.values()
                if op.get("operation_type") == operation_type
                and deepcopy(op.get("request_payload") or {}) == deepcopy(request_payload or {})
                and (not normalized_statuses or str(op.get("status") or "").strip().lower() in normalized_statuses)
            ]
            if not matches:
                return None
            matches.sort(
                key=lambda op: (
                    str(op.get("completed_at") or op.get("created_at") or ""),
                    str(op.get("created_at") or ""),
                ),
                reverse=True,
            )
            return matches[0]

    def _append_operation_event(
        operation_id: str,
        *,
        event_type: str,
        event_payload: dict[str, Any] | None = None,
        event_seq: int | None = None,
    ) -> dict[str, Any]:
        with lock:
            op = _ensure_operation(operation_id)
            events = operation_events[operation_id]
            seq = int(event_seq or (len(events) + 1))
            row = {
                "operation_id": operation_id,
                "event_seq": seq,
                "event_type": str(event_type or "progress"),
                "event_payload": deepcopy(event_payload or {}),
            }
            events.append(row)
            op["progress_payload"] = deepcopy(row["event_payload"])
            return deepcopy(row)

    def _stream_events_after_seq(
        operation_id: str,
        *,
        after_seq: int = 0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        with lock:
            rows = operation_events.get(operation_id, [])
            return [deepcopy(row) for row in rows if int(row.get("event_seq") or 0) > int(after_seq)][: int(limit)]

    def _update_operation_status(
        operation_id: str,
        *,
        status: str,
        result_payload: dict[str, Any] | None = None,
        error_payload: dict[str, Any] | None = None,
        progress_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        with lock:
            op = _ensure_operation(operation_id)
            op["status"] = status
            if progress_payload is not None:
                op["progress_payload"] = deepcopy(progress_payload)
            if result_payload is not None:
                op["result_payload"] = deepcopy(result_payload)
            if error_payload is not None:
                op["error_payload"] = deepcopy(error_payload)
            op["updated_at"] = "now"
            return _copy_operation(op)

    def _update_operation_progress(operation_id: str, *, progress_payload: dict[str, Any] | None = None) -> None:
        with lock:
            op = _ensure_operation(operation_id)
            op["progress_payload"] = deepcopy(progress_payload or {})
            op["updated_at"] = "now"

    def _touch_operation_started(operation_id: str) -> None:
        with lock:
            op = _ensure_operation(operation_id)
            if str(op.get("status") or "") == "pending":
                op["status"] = "running"
            op["updated_at"] = "now"

    def _request_operation_cancel(operation_id: str) -> dict[str, Any] | None:
        with lock:
            op = operations.get(operation_id)
            if not op:
                return None
            if str(op.get("status") or "") in {"pending", "running"}:
                op["status"] = "cancelling"
            op["cancel_requested_at"] = "now"
            op["updated_at"] = "now"
            return _copy_operation(op)

    def _is_cancel_requested(operation_id: str) -> bool:
        with lock:
            op = operations.get(operation_id)
            return bool(op and op.get("cancel_requested_at"))

    def _mark_operation_failed(operation_id: str, *, error_payload: dict[str, Any]) -> None:
        _update_operation_status(
            operation_id,
            status="failed",
            error_payload=error_payload,
            progress_payload=error_payload,
        )

    monkeypatch.setattr(admin_ops_repo, "create_or_attach_operation", _create_or_attach_operation)
    monkeypatch.setattr(admin_ops_repo, "get_operation", _get_operation)
    monkeypatch.setattr(
        admin_ops_repo,
        "get_latest_operation_for_request_payload",
        _get_latest_operation_for_request_payload,
    )
    monkeypatch.setattr(admin_ops_repo, "append_operation_event", _append_operation_event)
    monkeypatch.setattr(admin_ops_repo, "stream_events_after_seq", _stream_events_after_seq)
    monkeypatch.setattr(admin_ops_repo, "update_operation_status", _update_operation_status)
    monkeypatch.setattr(admin_ops_repo, "update_operation_progress", _update_operation_progress)
    monkeypatch.setattr(admin_ops_repo, "touch_operation_started", _touch_operation_started)
    monkeypatch.setattr(admin_ops_repo, "request_operation_cancel", _request_operation_cancel)
    monkeypatch.setattr(admin_ops_repo, "is_cancel_requested", _is_cancel_requested)
    monkeypatch.setattr(admin_ops_repo, "mark_operation_failed", _mark_operation_failed)
    monkeypatch.setattr(admin_ops_repo, "purge_old_operations", lambda *, retention_hours=336: 0)

    # Keep stream tests responsive.
    monkeypatch.setattr(pipeline_admin_operations, "_EVENT_POLL_INTERVAL_SECONDS", 0.01)
