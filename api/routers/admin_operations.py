from __future__ import annotations

import logging
from typing import Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import InternalAdminUser
from trr_backend.pipeline.admin_operations import operation_stream_response
from trr_backend.repositories import admin_operations

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/operations", tags=["admin-operations"])


class ForceCancelStaleOperationsRequest(BaseModel):
    operation_ids: list[UUID] = Field(default_factory=list)
    stale_after_seconds: int | None = Field(default=None, ge=15, le=86_400)
    cancelling_grace_seconds: int | None = Field(default=None, ge=15, le=86_400)
    force_selected: bool = False


class BulkCancelOperationsRequest(BaseModel):
    operation_ids: list[UUID] = Field(default_factory=list)
    cancel_all_active: bool = False


@router.get("/health")
def get_admin_operations_health(
    stale_after_seconds: int = Query(
        default=admin_operations.DEFAULT_OPERATION_STALE_AFTER_SECONDS,
        ge=15,
        le=86_400,
    ),
    cancelling_grace_seconds: int = Query(
        default=admin_operations.DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS,
        ge=15,
        le=86_400,
    ),
    limit: int = Query(default=200, ge=1, le=500),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    try:
        return admin_operations.get_admin_operations_health(
            stale_after_seconds=stale_after_seconds,
            cancelling_grace_seconds=cancelling_grace_seconds,
            limit=limit,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch admin operations health")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/stale/cancel")
def force_cancel_stale_admin_operations(
    payload: ForceCancelStaleOperationsRequest | None = None,
    user: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    try:
        return admin_operations.force_cancel_stale_operations(
            operation_ids=[str(operation_id) for operation_id in (payload.operation_ids if payload else [])],
            cancelled_by=(user or {}).get("email"),
            stale_after_seconds=(
                payload.stale_after_seconds
                if payload and payload.stale_after_seconds is not None
                else admin_operations.DEFAULT_OPERATION_STALE_AFTER_SECONDS
            ),
            cancelling_grace_seconds=(
                payload.cancelling_grace_seconds
                if payload and payload.cancelling_grace_seconds is not None
                else admin_operations.DEFAULT_OPERATION_CANCELLING_GRACE_SECONDS
            ),
            force_selected=bool(payload.force_selected) if payload else False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel stale admin operations")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/cancel")
def cancel_admin_operations(
    payload: BulkCancelOperationsRequest | None = None,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    try:
        return admin_operations.request_bulk_operation_cancels(
            operation_ids=[str(operation_id) for operation_id in (payload.operation_ids if payload else [])],
            cancel_all_active=bool(payload.cancel_all_active) if payload else False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel admin operations")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{operation_id}")
def get_admin_operation(
    operation_id: UUID,
    _: InternalAdminUser = cast(InternalAdminUser, None),
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
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> StreamingResponse:
    operation = admin_operations.get_operation(str(operation_id))
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")
    return operation_stream_response(str(operation_id), after_seq=after_seq, request=request)


@router.post("/{operation_id}/cancel")
def cancel_admin_operation(
    operation_id: UUID,
    request: Request,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    request_id = (request.headers.get("x-trr-request-id") or "").strip() or None
    prior_operation = admin_operations.get_operation(str(operation_id))
    prior_status = str((prior_operation or {}).get("status") or "")

    cancelled_operations = admin_operations.request_related_operation_cancels(str(operation_id))
    if not cancelled_operations:
        raise HTTPException(status_code=404, detail="Operation not found")

    operation = next(
        (row for row in cancelled_operations if str(row.get("id") or "") == str(operation_id)),
        None,
    ) or admin_operations.get_operation(str(operation_id))
    if not operation:
        raise HTTPException(status_code=404, detail="Operation not found")

    cancelled_operation_ids: list[str] = []
    for cancelled in cancelled_operations:
        cancelled_operation_id = str(cancelled.get("id") or "").strip()
        if not cancelled_operation_id:
            continue
        cancelled_operation_ids.append(cancelled_operation_id)
        payload = {
            "operation_id": cancelled_operation_id,
            "stage": "operation",
            "message": "Cancellation requested",
            "cancel_requested": True,
            "related_cancel_count": len(cancelled_operations),
        }
        try:
            event_row = admin_operations.append_operation_event(
                cancelled_operation_id,
                event_type="progress",
                event_payload=payload,
            )
            payload["event_seq"] = int(event_row.get("event_seq") or 0)
        except Exception:  # noqa: BLE001
            logger.debug("Failed to append cancellation progress event", exc_info=True)

    logger.info(
        (
            "Admin operation cancel request: operation_id=%s prior_status=%s resulting_status=%s "
            "related_cancelled=%s request_id=%s"
        ),
        str(operation_id),
        prior_status or None,
        str(operation.get("status") or None),
        len(cancelled_operation_ids),
        request_id,
    )

    return {
        "operation": operation,
        "cancel_requested": True,
        "cancelled_operations": len(cancelled_operation_ids),
        "cancelled_operation_ids": cancelled_operation_ids,
    }
