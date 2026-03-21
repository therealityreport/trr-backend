from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import AdminUser
from trr_backend.bravotv.run_service import (
    attach_operation,
    execute_bravotv_image_run_from_request_payload,
    get_bravotv_run,
    get_latest_bravotv_run,
)
from trr_backend.media.s3_mirror import get_object_storage_bucket, get_object_storage_client
from trr_backend.pipeline.admin_operations import operation_stream_response, start_operation_for_stream

router = APIRouter(prefix="/admin/bravotv/images", tags=["admin-bravotv-images"])


class BravotvImageRunRequest(BaseModel):
    mode: Literal["show", "person"]
    show_id: UUID | None = None
    person_id: UUID | None = None
    season: int | None = Field(default=None, ge=1)
    episode: int | None = Field(default=None, ge=1)
    sources: list[str] | None = None
    getty_limit: int = Field(default=200, ge=1, le=500)
    nbcumv_limit: int = Field(default=300, ge=1, le=1000)
    bravo_limit: int = Field(default=300, ge=1, le=1000)
    supplemental_limit: int = Field(default=100, ge=1, le=500)
    force_all: bool = False


def _sse_event(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=True, default=str)}\n\n"


def _build_operation_producer(*, request_payload: dict[str, Any]):
    def _producer() -> Any:
        operation_id = str(request_payload.get("operation_id") or "").strip() or None

        def _progress(event_type: str, payload: dict[str, Any]) -> None:
            payload.setdefault("operation_id", operation_id)
            chunks.append(_sse_event(event_type, payload))

        chunks: list[str] = []
        execute_bravotv_image_run_from_request_payload(request_payload, progress_cb=_progress)
        return chunks

    return _producer


def build_bravotv_image_operation_producer(*, request_payload: dict[str, Any]):
    return _build_operation_producer(request_payload=request_payload)


@router.post("/stream")
def start_bravotv_image_run(
    request: Request,
    payload: BravotvImageRunRequest = Body(...),
    admin_user: AdminUser = None,
) -> StreamingResponse:
    actor = str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin")
    request_payload = {
        "mode": payload.mode,
        "show_id": str(payload.show_id) if payload.show_id else None,
        "person_id": str(payload.person_id) if payload.person_id else None,
        "payload": payload.model_dump(mode="json"),
        "initiated_by": actor,
    }

    def _producer() -> Any:
        operation_id = str(operation.get("id") or "").strip()
        request_payload["operation_id"] = operation_id
        run = execute_bravotv_image_run_from_request_payload(
            request_payload,
            progress_cb=lambda event_type, event_payload: chunks.append(_sse_event(event_type, event_payload)),
        )
        if run and str(run.get("operation_id") or "").strip() != operation_id:
            attach_operation(str(run["id"]), operation_id=operation_id)
        return chunks

    chunks: list[str] = []
    operation = start_operation_for_stream(
        operation_type="admin_bravotv_image_run",
        producer=_producer,
        request_payload=request_payload,
        initiated_by=actor,
        request=request,
        allow_attach=False,
    )
    return operation_stream_response(str(operation.get("id")), request=request)


@router.post("/shows/{show_id}/stream")
def start_bravotv_show_run(
    show_id: UUID,
    request: Request,
    payload: BravotvImageRunRequest | None = None,
    admin_user: AdminUser = None,
) -> StreamingResponse:
    body = payload or BravotvImageRunRequest(mode="show", show_id=show_id, sources=["getty"])
    body.show_id = show_id
    body.mode = "show"
    return start_bravotv_image_run(request=request, payload=body, admin_user=admin_user)


@router.post("/people/{person_id}/stream")
def start_bravotv_person_run(
    person_id: UUID,
    request: Request,
    payload: BravotvImageRunRequest | None = None,
    admin_user: AdminUser = None,
) -> StreamingResponse:
    body = payload or BravotvImageRunRequest(mode="person", person_id=person_id, sources=["all"])
    body.person_id = person_id
    body.mode = "person"
    return start_bravotv_image_run(request=request, payload=body, admin_user=admin_user)


@router.get("/runs/{run_id}")
def get_run_detail(run_id: UUID, _: AdminUser = None) -> dict[str, Any]:
    row = get_bravotv_run(str(run_id))
    if not row:
        raise HTTPException(status_code=404, detail="BRAVOTV image run not found")
    return row


@router.get("/shows/{show_id}/latest")
def get_latest_show_run(show_id: UUID, _: AdminUser = None) -> dict[str, Any]:
    row = get_latest_bravotv_run(mode="show", show_id=str(show_id))
    if not row:
        return {"run": None}
    return {"run": row}


@router.get("/people/{person_id}/latest")
def get_latest_person_run(person_id: UUID, _: AdminUser = None) -> dict[str, Any]:
    row = get_latest_bravotv_run(mode="person", person_id=str(person_id))
    if not row:
        return {"run": None}
    return {"run": row}


@router.get("/runs/{run_id}/artifacts/{artifact_name:path}")
def get_run_artifact_preview(
    run_id: UUID,
    artifact_name: str,
    offset: int = 0,
    limit: int = 25,
    _: AdminUser = None,
) -> dict[str, Any]:
    row = get_bravotv_run(str(run_id))
    if not row:
        raise HTTPException(status_code=404, detail="BRAVOTV image run not found")
    artifact_paths = row.get("artifact_paths") if isinstance(row.get("artifact_paths"), dict) else {}
    artifact = artifact_paths.get(artifact_name)
    if not isinstance(artifact, dict):
        raise HTTPException(status_code=404, detail="Artifact not found")
    key = str(artifact.get("key") or "").strip()
    if not key:
        raise HTTPException(status_code=404, detail="Artifact object key missing")
    client = get_object_storage_client()
    bucket = get_object_storage_bucket()
    response = client.get_object(Bucket=bucket, Key=key)
    raw = response["Body"].read().decode("utf-8")
    parsed = json.loads(raw)
    if isinstance(parsed, list):
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 100))
        return {
            "artifact": artifact_name,
            "total": len(parsed),
            "offset": safe_offset,
            "limit": safe_limit,
            "items": parsed[safe_offset : safe_offset + safe_limit],
        }
    return {"artifact": artifact_name, "value": parsed}
