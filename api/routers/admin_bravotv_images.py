from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import InternalAdminUser
from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.media.bravotv import admin_review_service as review_service
from trr_backend.media.bravotv.run_service import (
    attach_operation,
    execute_bravotv_image_run_from_request_payload,
    get_latest_bravotv_run,
)
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
    getty_prefetched_assets: list[dict[str, Any]] | None = None
    getty_prefetched_events: list[dict[str, Any]] | None = None
    getty_prefetched_queries: list[dict[str, Any]] | None = None
    getty_prefetch_mode: str | None = None
    getty_prefetch_auth_mode: str | None = None
    getty_prefetch_auth_warning: str | None = None


class ApproveReplacementRequest(BaseModel):
    media_asset_id: UUID | None = None
    page_url: str
    source_domain: str
    expected_width: int | None = Field(default=None, ge=1)
    expected_height: int | None = Field(default=None, ge=1)
    note: str | None = None


class BulkApproveReplacementItem(ApproveReplacementRequest):
    group_id: str


class BulkApproveReplacementRequest(BaseModel):
    items: list[BulkApproveReplacementItem] = Field(default_factory=list, min_length=1, max_length=25)
    note: str | None = None


class ResolveDuplicateRequest(BaseModel):
    key_type: str
    key: str
    group_ids: list[str] = Field(default_factory=list)
    action: Literal["ignore", "mark_duplicate"] = "ignore"
    primary_group_id: str | None = None
    note: str | None = None


class BackfillRunRequest(BaseModel):
    force_all: bool = True
    note: str | None = None


def _bravotv_request_needs_getty_prefetch(payload: BravotvImageRunRequest) -> bool:
    if payload.getty_prefetched_assets is not None:
        return False
    requested_sources = {
        str(source or "").strip().lower() for source in (payload.sources or ["all"]) if str(source or "").strip()
    }
    return bool(requested_sources & {"all", "getty"})


def _fetch_person_full_name(person_id: UUID) -> str:
    db = create_supabase_admin_client()
    response = db.schema("core").table("people").select("id,full_name").eq("id", str(person_id)).limit(1).execute()
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Failed to load BRAVOTV person context.")
    rows = response.data if isinstance(getattr(response, "data", None), list) else []
    if not rows:
        raise HTTPException(status_code=404, detail="BRAVOTV person not found.")
    full_name = str((rows[0] or {}).get("full_name") or "").strip()
    if not full_name:
        raise HTTPException(status_code=400, detail="BRAVOTV person is missing full_name.")
    return full_name


def _fetch_show_name(show_id: UUID) -> str:
    db = create_supabase_admin_client()
    response = db.schema("core").table("shows").select("id,name").eq("id", str(show_id)).limit(1).execute()
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Failed to load BRAVOTV show context.")
    rows = response.data if isinstance(getattr(response, "data", None), list) else []
    if not rows:
        raise HTTPException(status_code=404, detail="BRAVOTV show not found.")
    show_name = str((rows[0] or {}).get("name") or "").strip()
    if not show_name:
        raise HTTPException(status_code=400, detail="BRAVOTV show is missing name.")
    return show_name


def _hydrate_bravotv_getty_prefetch(payload: BravotvImageRunRequest) -> BravotvImageRunRequest:
    if not _bravotv_request_needs_getty_prefetch(payload):
        return payload

    prefetch_mode = str(payload.getty_prefetch_mode or "full").strip().lower() or "full"
    try:
        if payload.mode == "person":
            from trr_backend.integrations.getty_local_prefetch import fetch_person_getty_prefetch_payload

            if payload.person_id is None:
                raise HTTPException(status_code=400, detail="BRAVOTV person Getty prefetch requires person_id.")
            prefetch_payload = fetch_person_getty_prefetch_payload(
                _fetch_person_full_name(payload.person_id),
                mode=prefetch_mode,
            )
        else:
            from trr_backend.integrations.getty_local_prefetch import fetch_show_getty_prefetch_payload

            if payload.show_id is None:
                raise HTTPException(status_code=400, detail="BRAVOTV show Getty prefetch requires show_id.")
            prefetch_payload = fetch_show_getty_prefetch_payload(
                _fetch_show_name(payload.show_id),
                season=payload.season,
                episode=payload.episode,
                mode=prefetch_mode,
            )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Getty prefetch failed: {exc}") from exc

    merged_value = prefetch_payload.get("merged")
    merged_events_value = prefetch_payload.get("merged_events")
    query_summaries_value = prefetch_payload.get("query_summaries")
    return payload.model_copy(
        update={
            "getty_prefetched_assets": (
                list(merged_value) if isinstance(merged_value, list) else []
            ),
            "getty_prefetched_events": (
                list(merged_events_value)
                if isinstance(merged_events_value, list)
                else []
            ),
            "getty_prefetched_queries": (
                list(query_summaries_value)
                if isinstance(query_summaries_value, list)
                else []
            ),
            "getty_prefetch_mode": str(prefetch_payload.get("prefetch_mode") or prefetch_mode).strip() or prefetch_mode,
            "getty_prefetch_auth_mode": str(prefetch_payload.get("auth_mode") or "").strip() or None,
            "getty_prefetch_auth_warning": str(prefetch_payload.get("auth_warning") or "").strip() or None,
        }
    )


def _sse_event(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=True, default=str)}\n\n"


def _safe_dict(value: Any) -> dict[str, Any]:
    return review_service.safe_dict(value)


def _safe_list(value: Any) -> list[Any]:
    return review_service.safe_list(value)


def _get_run_or_404(run_id: UUID) -> dict[str, Any]:
    return review_service.get_run_or_404(str(run_id))


def _load_run_artifact_payload(row: dict[str, Any], artifact_name: str) -> Any:
    return review_service.load_run_artifact_payload(row, artifact_name)


def _run_artifact_object(row: dict[str, Any], artifact_name: str) -> dict[str, Any]:
    return review_service.run_artifact_object(row, artifact_name)


def _write_run_artifact_payload(row: dict[str, Any], artifact_name: str, payload: Any) -> dict[str, Any]:
    return review_service.write_run_artifact_payload(row, artifact_name, payload)


def _paginate(items: list[Any], *, offset: int, limit: int) -> dict[str, Any]:
    return review_service.paginate(items, offset=offset, limit=limit)


def _candidate_values(row: dict[str, Any]) -> list[dict[str, Any]]:
    return review_service.candidate_values(row)


def _row_matches_review_filters(
    row: dict[str, Any],
    *,
    section: str,
    reason: str | None,
    display_eligible: bool | None,
    source_role: str | None,
) -> bool:
    return review_service.row_matches_review_filters(
        row,
        section=section,
        reason=reason,
        display_eligible=display_eligible,
        source_role=source_role,
    )


def _append_action_to_artifact_payload(payload: dict[str, Any], action: dict[str, Any]) -> dict[str, Any]:
    return review_service.append_action_to_artifact_payload(payload, action)


def _append_review_action(run_id: str, action: dict[str, Any]) -> dict[str, Any]:
    return review_service.append_review_action(run_id, action)


def _fetch_media_asset(db: Any, asset_id: str) -> dict[str, Any]:
    return review_service.fetch_media_asset(db, asset_id)


def _update_media_asset_metadata(db: Any, asset_id: str, patch: dict[str, Any]) -> None:
    review_service.update_media_asset_metadata(db, asset_id, patch)


def _find_replacement_candidate(
    row: dict[str, Any],
    *,
    group_id: str,
    media_asset_id: str | None,
) -> dict[str, Any]:
    return review_service.find_replacement_candidate(row, group_id=group_id, media_asset_id=media_asset_id)


def _approve_replacement_for_run(
    *,
    run_id: str,
    row: dict[str, Any],
    group_id: str,
    payload: ApproveReplacementRequest,
    note: str | None = None,
    db: Any | None = None,
) -> dict[str, Any]:
    return review_service.approve_replacement_for_run(
        run_id=run_id,
        row=row,
        group_id=group_id,
        payload=payload,
        note=note,
        db=db,
    )


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
    admin_user: InternalAdminUser = cast(InternalAdminUser, None),
) -> StreamingResponse:
    payload = _hydrate_bravotv_getty_prefetch(payload)
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
    admin_user: InternalAdminUser = cast(InternalAdminUser, None),
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
    admin_user: InternalAdminUser = cast(InternalAdminUser, None),
) -> StreamingResponse:
    body = payload or BravotvImageRunRequest(mode="person", person_id=person_id, sources=["all"])
    body.person_id = person_id
    body.mode = "person"
    return start_bravotv_image_run(request=request, payload=body, admin_user=admin_user)


@router.get("/runs/{run_id}")
def get_run_detail(run_id: UUID, _: InternalAdminUser = cast(InternalAdminUser, None)) -> dict[str, Any]:
    return _get_run_or_404(run_id)


@router.get("/shows/{show_id}/latest")
def get_latest_show_run(show_id: UUID, _: InternalAdminUser = cast(InternalAdminUser, None)) -> dict[str, Any]:
    row = get_latest_bravotv_run(mode="show", show_id=str(show_id))
    if not row:
        return {"run": None}
    return {"run": row}


@router.get("/people/{person_id}/latest")
def get_latest_person_run(person_id: UUID, _: InternalAdminUser = cast(InternalAdminUser, None)) -> dict[str, Any]:
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
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    row = _get_run_or_404(run_id)
    parsed = _load_run_artifact_payload(row, artifact_name)
    if isinstance(parsed, list):
        return {"artifact": artifact_name, **_paginate(parsed, offset=offset, limit=limit)}
    return {"artifact": artifact_name, "value": parsed}


@router.get("/runs/{run_id}/review")
def get_run_review_items(
    run_id: UUID,
    section: Literal[
        "review_candidates",
        "replacement_pending",
        "duplicate_groups",
        "unmatched_rows",
        "failed_acquisitions",
        "merged_catalog",
    ] = "review_candidates",
    reason: str | None = None,
    display_eligible: bool | None = None,
    source_role: str | None = None,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=25, ge=1, le=100),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    row = _get_run_or_404(run_id)
    artifact_name = "merged_catalog" if section == "merged_catalog" else "run_review"
    payload = _load_run_artifact_payload(row, artifact_name)
    raw_items = payload if section == "merged_catalog" else _safe_list(_safe_dict(payload).get(section))
    items = [
        _safe_dict(item)
        for item in raw_items
        if isinstance(item, dict)
        and _row_matches_review_filters(
            item,
            section=section,
            reason=reason,
            display_eligible=display_eligible,
            source_role=source_role,
        )
    ]
    return {
        "run_id": str(run_id),
        "section": section,
        "filters": {
            "reason": review_service.canonical_review_reason(reason) if reason else None,
            "display_eligible": display_eligible,
            "source_role": source_role,
        },
        **_paginate(items, offset=offset, limit=limit),
    }


@router.post("/runs/{run_id}/backfill")
def backfill_existing_run(
    run_id: UUID,
    payload: BackfillRunRequest | None = None,
    admin_user: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    row = _get_run_or_404(run_id)
    request_payload = _safe_dict(row.get("request_payload"))
    original_payload = _safe_dict(request_payload.get("payload"))
    mode = str(row.get("mode") or original_payload.get("mode") or "").strip()
    if mode not in {"show", "person"}:
        raise HTTPException(status_code=422, detail="Cannot backfill run with missing mode")
    show_id = str(row.get("target_show_id") or original_payload.get("show_id") or "").strip() or None
    person_id = str(row.get("target_person_id") or original_payload.get("person_id") or "").strip() or None
    body = BravotvImageRunRequest(
        mode=mode,  # type: ignore[arg-type]
        show_id=UUID(show_id) if show_id else None,
        person_id=UUID(person_id) if person_id else None,
        season=row.get("season") or original_payload.get("season"),
        episode=row.get("episode") or original_payload.get("episode"),
        sources=["getty"],
        getty_limit=int(original_payload.get("getty_limit") or 200),
        nbcumv_limit=int(original_payload.get("nbcumv_limit") or 300),
        bravo_limit=int(original_payload.get("bravo_limit") or 300),
        supplemental_limit=int(original_payload.get("supplemental_limit") or 100),
        force_all=bool(payload.force_all if payload else True),
        getty_prefetch_mode=str(original_payload.get("getty_prefetch_mode") or "full").strip() or "full",
    )
    body = _hydrate_bravotv_getty_prefetch(body)
    actor = str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin")
    backfill_payload = {
        "mode": body.mode,
        "show_id": str(body.show_id) if body.show_id else None,
        "person_id": str(body.person_id) if body.person_id else None,
        "payload": body.model_dump(mode="json"),
        "initiated_by": actor,
        "backfill_source_run_id": str(run_id),
        "backfill_note": payload.note if payload else None,
    }
    new_run = execute_bravotv_image_run_from_request_payload(backfill_payload)
    action = {
        "type": "nup_backfill_run_created",
        "run_id": str(run_id),
        "backfill_run_id": str(new_run.get("id") or ""),
        "note": payload.note if payload else None,
        "created_at": datetime.now(UTC).isoformat(),
    }
    try:
        _append_review_action(str(run_id), action)
    except HTTPException:
        pass
    return {"source_run_id": str(run_id), "run": new_run, "action": action}


@router.post("/runs/{run_id}/replacement-candidates/{group_id}/approve")
def approve_replacement_candidate(
    run_id: UUID,
    group_id: str,
    payload: ApproveReplacementRequest,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    row = _get_run_or_404(run_id)
    return _approve_replacement_for_run(run_id=str(run_id), row=row, group_id=group_id, payload=payload)


@router.post("/runs/{run_id}/replacement-candidates/approve-bulk")
def approve_replacement_candidates_bulk(
    run_id: UUID,
    payload: BulkApproveReplacementRequest,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    row = _get_run_or_404(run_id)
    db = create_supabase_admin_client()
    approved: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for item in payload.items:
        try:
            approved.append(
                _approve_replacement_for_run(
                    run_id=str(run_id),
                    row=row,
                    group_id=item.group_id,
                    payload=item,
                    note=item.note or payload.note,
                    db=db,
                )
            )
            row = _get_run_or_404(run_id)
        except HTTPException as exc:
            failed.append({"group_id": item.group_id, "status_code": exc.status_code, "detail": exc.detail})
        except Exception as exc:  # noqa: BLE001
            failed.append({"group_id": item.group_id, "status_code": 500, "detail": str(exc)})
    return {
        "run_id": str(run_id),
        "approved_count": len(approved),
        "failed_count": len(failed),
        "approved": approved,
        "failed": failed,
    }


@router.post("/runs/{run_id}/duplicates/resolve")
def resolve_duplicate_group(
    run_id: UUID,
    payload: ResolveDuplicateRequest,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    return review_service.resolve_duplicate_group(run_id=str(run_id), payload=payload)
