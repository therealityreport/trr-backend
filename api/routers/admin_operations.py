from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from api.auth import AdminUser
from trr_backend.pipeline.admin_operations import operation_stream_response
from trr_backend.repositories import admin_operations

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/operations", tags=["admin-operations"])


@router.get("/{operation_id}")
def get_admin_operation(
    operation_id: UUID,
    _: AdminUser = None,
) -> dict[str, Any]:
    operation = admin_operations.get_operation(str(operation_id))
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    events = admin_operations.normalize_operation_events(
        admin_operations.stream_events_after_seq(str(operation_id), after_seq=0, limit=1_000)
    )
    latest_event_seq = int(events[-1].get("event_seq") or 0) if events else 0
    return {
        "operation": operation,
        "latest_event_seq": latest_event_seq,
    }


@router.get("/{operation_id}/stream")
def stream_admin_operation(
    operation_id: UUID,
    request: Request,
    after_seq: int = Query(default=0, ge=0),
    _: AdminUser = None,
) -> StreamingResponse:
    operation = admin_operations.get_operation(str(operation_id))
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")
    return operation_stream_response(str(operation_id), after_seq=after_seq, request=request)


@router.post("/{operation_id}/cancel")
def cancel_admin_operation(
    operation_id: UUID,
    request: Request,
    _: AdminUser = None,
) -> dict[str, Any]:
    request_id = (request.headers.get("x-trr-request-id") or "").strip() or None
    prior_operation = admin_operations.get_operation(str(operation_id))
    prior_status = str((prior_operation or {}).get("status") or "")

    operation = admin_operations.request_operation_cancel(str(operation_id))
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    payload = {
        "operation_id": str(operation_id),
        "stage": "operation",
        "message": "Cancellation requested",
        "cancel_requested": True,
    }
    try:
        event_row = admin_operations.append_operation_event(
            str(operation_id),
            event_type="progress",
            event_payload=payload,
        )
        payload["event_seq"] = int(event_row.get("event_seq") or 0)
    except Exception:  # noqa: BLE001
        logger.debug("Failed to append cancellation progress event", exc_info=True)

    logger.info(
        "Admin operation cancel request: operation_id=%s prior_status=%s resulting_status=%s request_id=%s",
        str(operation_id),
        prior_status or None,
        str(operation.get("status") or None),
        request_id,
    )

    return {
        "operation": operation,
        "cancel_requested": True,
    }
