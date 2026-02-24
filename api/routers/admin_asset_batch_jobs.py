"""Admin SSE batch jobs for show/season asset operations."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from api.routers.admin_cast_photos import (
    GenerateCastPhotoVariantsRequest,
    detect_text_overlay_cast_photo,
    generate_variants_for_cast_photo,
)
from api.routers.admin_image_counts import auto_count_cast_photo, auto_count_media_asset
from api.routers.admin_media_assets import (
    GenerateMediaAssetVariantsRequest,
    detect_text_overlay_media_asset,
    generate_variants_for_media_asset,
)

router = APIRouter(prefix="/admin/shows", tags=["admin-asset-batch-jobs"])

BatchJobOperation = Literal["count", "crop", "id_text", "resize"]
BatchTargetOrigin = Literal["cast_photos", "media_assets"]


class BatchJobTarget(BaseModel):
    origin: str
    id: str
    content_type: str | None = None


class BatchJobsRequest(BaseModel):
    operations: list[BatchJobOperation] = Field(..., min_length=1)
    content_types: list[str] = Field(default_factory=list)
    targets: list[BatchJobTarget] = Field(default_factory=list)
    force: bool = True


def _yield_event(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def _normalize_content_type(value: str | None) -> str:
    if not isinstance(value, str):
        return ""
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def _parse_season_number(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.isdigit():
        return int(cleaned)
    match = re.search(r"season\s*(\d+)", cleaned, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _build_resize_center_fallback_crop_payload() -> dict[str, Any]:
    return {
        "x": 50.0,
        "y": 32.0,
        "zoom": 1.0,
        "mode": "auto",
        "strategy": "resize_center_fallback_v1",
    }


def _normalize_crop_payload(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    try:
        x = float(value.get("x"))
        y = float(value.get("y"))
        zoom = float(value.get("zoom"))
    except (TypeError, ValueError):
        return None
    mode_raw = str(value.get("mode") or "auto").strip().lower()
    mode = "manual" if mode_raw == "manual" else "auto"
    payload: dict[str, Any] = {
        "x": _clamp(x, 0.0, 100.0),
        "y": _clamp(y, 0.0, 100.0),
        "zoom": _clamp(zoom, 1.0, 4.0),
        "mode": mode,
    }
    strategy = value.get("strategy")
    if isinstance(strategy, str) and strategy.strip():
        payload["strategy"] = strategy.strip()
    return payload


def _find_manual_then_auto_crop(candidates: list[Any]) -> tuple[dict[str, Any] | None, str | None]:
    manual: dict[str, Any] | None = None
    auto: dict[str, Any] | None = None
    for candidate in candidates:
        crop = _normalize_crop_payload(candidate)
        if not crop:
            continue
        if str(crop.get("mode") or "").lower() == "manual":
            manual = crop
            break
        if auto is None:
            auto = crop
    if manual is not None:
        return manual, "manual"
    if auto is not None:
        return auto, "auto"
    return None, None


def _lookup_cast_photo_crop_payload(
    db: SupabaseAdminClient,
    target_id: str,
) -> tuple[dict[str, Any] | None, str | None]:
    response = db.schema("core").table("cast_photos").select("id,metadata").eq("id", target_id).limit(1).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(str(response.error))
    rows = response.data or []
    if not rows:
        return None, None
    metadata = rows[0].get("metadata") if isinstance(rows[0].get("metadata"), dict) else {}
    crop = metadata.get("thumbnail_crop") if isinstance(metadata, dict) else None
    return _find_manual_then_auto_crop([crop])


def _lookup_media_asset_crop_payload(
    db: SupabaseAdminClient,
    target_id: str,
) -> tuple[dict[str, Any] | None, str | None]:
    link_resp = (
        db.schema("core").table("media_links").select("id,context").eq("media_asset_id", target_id).limit(250).execute()
    )
    if hasattr(link_resp, "error") and link_resp.error:
        raise RuntimeError(str(link_resp.error))
    link_context_crops = [
        row.get("context", {}).get("thumbnail_crop")
        for row in (link_resp.data or [])
        if isinstance(row, dict) and isinstance(row.get("context"), dict)
    ]
    crop, source = _find_manual_then_auto_crop(link_context_crops)
    if crop is not None:
        return crop, source

    asset_resp = db.schema("core").table("media_assets").select("id,metadata").eq("id", target_id).limit(1).execute()
    if hasattr(asset_resp, "error") and asset_resp.error:
        raise RuntimeError(str(asset_resp.error))
    rows = asset_resp.data or []
    if not rows:
        return None, None
    metadata = rows[0].get("metadata") if isinstance(rows[0].get("metadata"), dict) else {}
    return _find_manual_then_auto_crop([metadata.get("thumbnail_crop") if isinstance(metadata, dict) else None])


def _lookup_resize_crop_payload(
    *,
    origin: BatchTargetOrigin,
    target_id: str,
    db: SupabaseAdminClient,
) -> tuple[dict[str, Any] | None, str | None]:
    if origin == "cast_photos":
        return _lookup_cast_photo_crop_payload(db, target_id)
    return _lookup_media_asset_crop_payload(db, target_id)


def _resolve_resize_crop_payload(
    *,
    origin: BatchTargetOrigin,
    target_id: str,
    force: bool,
    db: SupabaseAdminClient,
) -> tuple[dict[str, Any], str]:
    existing_crop, existing_source = _lookup_resize_crop_payload(origin=origin, target_id=target_id, db=db)
    if existing_crop is not None:
        return existing_crop, existing_source or "existing"

    target_uuid = UUID(target_id)
    try:
        if origin == "cast_photos":
            auto_count_cast_photo(target_uuid, force=force, db=db, _=None)
        else:
            auto_count_media_asset(target_uuid, force=force, db=db, _=None)
    except Exception:
        pass

    detected_crop, detected_source = _lookup_resize_crop_payload(origin=origin, target_id=target_id, db=db)
    if detected_crop is not None:
        return detected_crop, f"auto_detect:{detected_source or 'auto'}"

    return _build_resize_center_fallback_crop_payload(), "fallback"


def _fetch_target_scope_fields(
    db: SupabaseAdminClient,
    origin: BatchTargetOrigin,
    target_id: str,
) -> tuple[str | None, int | None, str | None]:
    if origin == "cast_photos":
        response = db.schema("core").table("cast_photos").select("id,metadata").eq("id", target_id).limit(1).execute()
    else:
        response = db.schema("core").table("media_assets").select("id,metadata").eq("id", target_id).limit(1).execute()

    if hasattr(response, "error") and response.error:
        raise RuntimeError(str(response.error))
    rows = response.data or []
    if not rows:
        return None, None, "not_found"

    row = rows[0] if isinstance(rows[0], dict) else {}
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    show_id = str(metadata.get("show_id") or "").strip() or None
    season_number = _parse_season_number(metadata.get("season_number"))
    if season_number is None:
        season_number = _parse_season_number(metadata.get("season"))
    if season_number is None:
        season_number = _parse_season_number(metadata.get("seasonNumber"))
    return show_id, season_number, None


def _execute_target_operation(
    *,
    origin: BatchTargetOrigin,
    target_id: str,
    operation: BatchJobOperation,
    force: bool,
    db: SupabaseAdminClient,
) -> dict[str, Any] | None:
    target_uuid = UUID(target_id)

    if origin == "cast_photos":
        if operation in {"count", "crop"}:
            auto_count_cast_photo(target_uuid, force=force, db=db, _=None)
            return
        if operation == "id_text":
            detect_text_overlay_cast_photo(target_uuid, force=force, db=db, _=None)
            return
        if operation == "resize":
            generate_variants_for_cast_photo(
                target_uuid,
                payload=GenerateCastPhotoVariantsRequest(force=force),
                db=db,
                _=None,
            )
            crop_payload, crop_source = _resolve_resize_crop_payload(
                origin=origin,
                target_id=target_id,
                force=force,
                db=db,
            )
            generate_variants_for_cast_photo(
                target_uuid,
                payload=GenerateCastPhotoVariantsRequest(force=force, crop=crop_payload),
                db=db,
                _=None,
            )
            return {"crop_source": crop_source}

    if origin == "media_assets":
        if operation in {"count", "crop"}:
            auto_count_media_asset(target_uuid, force=force, db=db, _=None)
            return
        if operation == "id_text":
            detect_text_overlay_media_asset(target_uuid, force=force, db=db, _=None)
            return
        if operation == "resize":
            generate_variants_for_media_asset(
                target_uuid,
                payload=GenerateMediaAssetVariantsRequest(force=force),
                db=db,
                _=None,
            )
            crop_payload, crop_source = _resolve_resize_crop_payload(
                origin=origin,
                target_id=target_id,
                force=force,
                db=db,
            )
            generate_variants_for_media_asset(
                target_uuid,
                payload=GenerateMediaAssetVariantsRequest(force=force, crop=crop_payload),
                db=db,
                _=None,
            )
            return {"crop_source": crop_source}

    raise RuntimeError(f"Unsupported target operation: {origin}/{operation}")


def _stream_batch_jobs(
    *,
    show_id: UUID,
    payload: BatchJobsRequest,
    db: SupabaseAdminClient,
    season_number: int | None = None,
) -> StreamingResponse:
    show_id_str = str(show_id)
    operations = list(dict.fromkeys(payload.operations))
    allowed_content_types = {_normalize_content_type(item) for item in payload.content_types if item}
    allowed_content_types.discard("")
    total = len(payload.targets) * len(operations)

    def event_generator():
        attempted = 0
        succeeded = 0
        failed = 0
        skipped = 0
        current = 0

        skip_reasons: dict[str, int] = defaultdict(int)
        failure_reasons: dict[str, int] = defaultdict(int)
        operation_counts: dict[str, dict[str, int]] = {
            op: {"attempted": 0, "succeeded": 0, "failed": 0, "skipped": 0} for op in operations
        }

        def operation_counts_snapshot() -> dict[str, dict[str, int]]:
            return {
                op: {
                    "attempted": int(values.get("attempted", 0)),
                    "succeeded": int(values.get("succeeded", 0)),
                    "failed": int(values.get("failed", 0)),
                    "skipped": int(values.get("skipped", 0)),
                }
                for op, values in operation_counts.items()
            }

        def live_counts_snapshot() -> dict[str, int]:
            snap = operation_counts_snapshot()
            return {
                "synced": 0,
                "mirrored": 0,
                "counted": int(snap.get("count", {}).get("succeeded", 0)),
                "cropped": int(snap.get("crop", {}).get("succeeded", 0)),
                "id_text": int(snap.get("id_text", {}).get("succeeded", 0)),
                "resized": int(snap.get("resize", {}).get("succeeded", 0)),
            }

        def emit_progress(payload: dict[str, Any]) -> str:
            return _yield_event(
                "progress",
                {
                    **payload,
                    "operation_counts": operation_counts_snapshot(),
                    "live_counts": live_counts_snapshot(),
                },
            )

        yield emit_progress(
            {
                "show_id": show_id_str,
                "season_number": season_number,
                "stage": "starting",
                "message": "Starting batch jobs...",
                "current": 0,
                "total": total,
            },
        )

        for target in payload.targets:
            target_id = str(target.id or "").strip()
            origin_raw = str(target.origin or "").strip().lower()
            content_type = _normalize_content_type(target.content_type)

            for operation in operations:
                current += 1
                operation_counts[operation]["attempted"] += 1

                if not target_id:
                    skipped += 1
                    operation_counts[operation]["skipped"] += 1
                    skip_reasons["skipped_missing_target_id"] += 1
                    yield emit_progress(
                        {
                            "show_id": show_id_str,
                            "season_number": season_number,
                            "stage": "batch_jobs",
                            "message": "Skipped target with missing id.",
                            "current": current,
                            "total": total,
                            "operation": operation,
                            "skip_reason": "skipped_missing_target_id",
                        },
                    )
                    continue

                if allowed_content_types and content_type not in allowed_content_types:
                    skipped += 1
                    operation_counts[operation]["skipped"] += 1
                    skip_reasons["skipped_content_type_filtered"] += 1
                    yield emit_progress(
                        {
                            "show_id": show_id_str,
                            "season_number": season_number,
                            "stage": "batch_jobs",
                            "message": f"Skipped {target_id}: filtered by content type.",
                            "current": current,
                            "total": total,
                            "operation": operation,
                            "origin": origin_raw,
                            "target_id": target_id,
                            "skip_reason": "skipped_content_type_filtered",
                        },
                    )
                    continue

                if origin_raw not in {"cast_photos", "media_assets"}:
                    skipped += 1
                    operation_counts[operation]["skipped"] += 1
                    skip_reasons["skipped_unsupported_origin"] += 1
                    yield emit_progress(
                        {
                            "show_id": show_id_str,
                            "season_number": season_number,
                            "stage": "batch_jobs",
                            "message": f"Skipped {target_id}: unsupported origin {origin_raw}.",
                            "current": current,
                            "total": total,
                            "operation": operation,
                            "origin": origin_raw,
                            "target_id": target_id,
                            "skip_reason": "skipped_unsupported_origin",
                        },
                    )
                    continue

                origin = origin_raw

                if season_number is not None:
                    try:
                        row_show_id, row_season_number, scope_error = _fetch_target_scope_fields(db, origin, target_id)
                    except Exception as exc:  # noqa: BLE001
                        skipped += 1
                        operation_counts[operation]["skipped"] += 1
                        skip_reasons["skipped_scope_lookup_failed"] += 1
                        yield emit_progress(
                            {
                                "show_id": show_id_str,
                                "season_number": season_number,
                                "stage": "batch_jobs",
                                "message": f"Skipped {target_id}: scope lookup failed ({exc}).",
                                "current": current,
                                "total": total,
                                "operation": operation,
                                "origin": origin,
                                "target_id": target_id,
                                "skip_reason": "skipped_scope_lookup_failed",
                            },
                        )
                        continue

                    if scope_error == "not_found":
                        skipped += 1
                        operation_counts[operation]["skipped"] += 1
                        skip_reasons["skipped_not_found"] += 1
                        yield emit_progress(
                            {
                                "show_id": show_id_str,
                                "season_number": season_number,
                                "stage": "batch_jobs",
                                "message": f"Skipped {target_id}: target not found.",
                                "current": current,
                                "total": total,
                                "operation": operation,
                                "origin": origin,
                                "target_id": target_id,
                                "skip_reason": "skipped_not_found",
                            },
                        )
                        continue

                    if row_show_id != show_id_str:
                        skipped += 1
                        operation_counts[operation]["skipped"] += 1
                        skip_reasons["skipped_out_of_scope_show"] += 1
                        yield emit_progress(
                            {
                                "show_id": show_id_str,
                                "season_number": season_number,
                                "stage": "batch_jobs",
                                "message": f"Skipped {target_id}: target does not belong to this show.",
                                "current": current,
                                "total": total,
                                "operation": operation,
                                "origin": origin,
                                "target_id": target_id,
                                "skip_reason": "skipped_out_of_scope_show",
                            },
                        )
                        continue

                    if row_season_number != season_number:
                        skipped += 1
                        operation_counts[operation]["skipped"] += 1
                        skip_reasons["skipped_out_of_scope_season"] += 1
                        yield emit_progress(
                            {
                                "show_id": show_id_str,
                                "season_number": season_number,
                                "stage": "batch_jobs",
                                "message": f"Skipped {target_id}: target does not belong to season {season_number}.",
                                "current": current,
                                "total": total,
                                "operation": operation,
                                "origin": origin,
                                "target_id": target_id,
                                "skip_reason": "skipped_out_of_scope_season",
                            },
                        )
                        continue

                attempted += 1
                try:
                    operation_result = _execute_target_operation(
                        origin=origin,
                        target_id=target_id,
                        operation=operation,
                        force=bool(payload.force),
                        db=db,
                    )
                    succeeded += 1
                    operation_counts[operation]["succeeded"] += 1
                    crop_source = operation_result.get("crop_source") if isinstance(operation_result, dict) else None
                    detail_suffix = (
                        f" (crop source: {crop_source})."
                        if operation == "resize" and isinstance(crop_source, str) and crop_source
                        else "."
                    )
                    yield emit_progress(
                        {
                            "show_id": show_id_str,
                            "season_number": season_number,
                            "stage": "batch_jobs",
                            "message": f"{operation} succeeded for {target_id}{detail_suffix}",
                            "current": current,
                            "total": total,
                            "operation": operation,
                            "origin": origin,
                            "target_id": target_id,
                            **({"crop_source": crop_source} if crop_source else {}),
                        },
                    )
                except ValueError:
                    skipped += 1
                    operation_counts[operation]["skipped"] += 1
                    skip_reasons["skipped_invalid_target_id"] += 1
                    yield emit_progress(
                        {
                            "show_id": show_id_str,
                            "season_number": season_number,
                            "stage": "batch_jobs",
                            "message": f"Skipped {target_id}: invalid target id.",
                            "current": current,
                            "total": total,
                            "operation": operation,
                            "origin": origin,
                            "target_id": target_id,
                            "skip_reason": "skipped_invalid_target_id",
                        },
                    )
                except HTTPException as exc:
                    reason = f"http_{int(exc.status_code)}"
                    if int(exc.status_code) in {404, 409}:
                        skipped += 1
                        operation_counts[operation]["skipped"] += 1
                        skip_reasons[f"skipped_{reason}"] += 1
                        yield emit_progress(
                            {
                                "show_id": show_id_str,
                                "season_number": season_number,
                                "stage": "batch_jobs",
                                "message": f"Skipped {target_id}: {exc.detail}",
                                "current": current,
                                "total": total,
                                "operation": operation,
                                "origin": origin,
                                "target_id": target_id,
                                "skip_reason": f"skipped_{reason}",
                            },
                        )
                    else:
                        failed += 1
                        operation_counts[operation]["failed"] += 1
                        failure_reasons[reason] += 1
                        yield emit_progress(
                            {
                                "show_id": show_id_str,
                                "season_number": season_number,
                                "stage": "batch_jobs",
                                "message": f"{operation} failed for {target_id}: {exc.detail}",
                                "current": current,
                                "total": total,
                                "operation": operation,
                                "origin": origin,
                                "target_id": target_id,
                                "error": str(exc.detail),
                            },
                        )
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    operation_counts[operation]["failed"] += 1
                    failure_reasons["unhandled_error"] += 1
                    yield emit_progress(
                        {
                            "show_id": show_id_str,
                            "season_number": season_number,
                            "stage": "batch_jobs",
                            "message": f"{operation} failed for {target_id}.",
                            "current": current,
                            "total": total,
                            "operation": operation,
                            "origin": origin,
                            "target_id": target_id,
                            "error": str(exc),
                        },
                    )

        yield _yield_event(
            "complete",
            {
                "show_id": show_id_str,
                "season_number": season_number,
                "operations": operations,
                "content_types": sorted(allowed_content_types),
                "targets": len(payload.targets),
                "attempted": attempted,
                "succeeded": succeeded,
                "failed": failed,
                "skipped": skipped,
                "skip_reasons": dict(skip_reasons),
                "failure_reasons": dict(failure_reasons),
                "operation_counts": operation_counts_snapshot(),
                "live_counts": live_counts_snapshot(),
            },
        )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{show_id}/assets/batch-jobs/stream")
def batch_jobs_for_show_stream(
    show_id: UUID,
    payload: BatchJobsRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StreamingResponse:
    return _stream_batch_jobs(show_id=show_id, payload=payload or BatchJobsRequest(operations=["count"]), db=db)


@router.post("/{show_id}/seasons/{season_number}/assets/batch-jobs/stream")
def batch_jobs_for_show_season_stream(
    show_id: UUID,
    season_number: int,
    payload: BatchJobsRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StreamingResponse:
    return _stream_batch_jobs(
        show_id=show_id,
        season_number=season_number,
        payload=payload or BatchJobsRequest(operations=["count"]),
        db=db,
    )
