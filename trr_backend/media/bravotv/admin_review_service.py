from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

from fastapi import HTTPException

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.media.bravotv.run_review import canonical_review_reason
from trr_backend.media.bravotv.run_service import get_bravotv_run
from trr_backend.media.getty_replacement import (
    apply_media_asset_replacement,
    resolve_public_replacement_from_page,
)
from trr_backend.media.s3_mirror import get_object_storage_bucket, get_object_storage_client
from trr_backend.repositories.bravotv_image_runs import update_progress as update_bravotv_run_progress


def safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def safe_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def get_run_or_404(run_id: str) -> dict[str, Any]:
    row = get_bravotv_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail="BRAVOTV image run not found")
    return row


def run_artifact_object(row: dict[str, Any], artifact_name: str) -> dict[str, Any]:
    artifact_paths = (
        cast("dict[str, Any]", row.get("artifact_paths")) if isinstance(row.get("artifact_paths"), dict) else {}
    )
    artifact = artifact_paths.get(artifact_name)
    if not isinstance(artifact, dict):
        raise HTTPException(status_code=404, detail=f"Artifact not found: {artifact_name}")
    key = str(artifact.get("key") or "").strip()
    if not key:
        raise HTTPException(status_code=404, detail=f"Artifact object key missing: {artifact_name}")
    return artifact


def load_run_artifact_payload(row: dict[str, Any], artifact_name: str) -> Any:
    artifact = run_artifact_object(row, artifact_name)
    key = str(artifact.get("key") or "").strip()
    client = get_object_storage_client()
    bucket = get_object_storage_bucket()
    response = client.get_object(Bucket=bucket, Key=key)
    raw = response["Body"].read().decode("utf-8")
    return json.loads(raw)


def write_run_artifact_payload(row: dict[str, Any], artifact_name: str, payload: Any) -> dict[str, Any]:
    artifact = run_artifact_object(row, artifact_name)
    key = str(artifact.get("key") or "").strip()
    data = json.dumps(payload, indent=2, ensure_ascii=True, default=str).encode("utf-8") + b"\n"
    client = get_object_storage_client()
    bucket = get_object_storage_bucket()
    response = client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
    updated = dict(artifact)
    updated["bytes"] = len(data)
    if isinstance(response, dict) and response.get("ETag"):
        updated["etag"] = response.get("ETag")
    return updated


def paginate(items: list[Any], *, offset: int, limit: int) -> dict[str, Any]:
    safe_offset = max(0, int(offset))
    safe_limit = max(1, min(int(limit), 100))
    return {
        "total": len(items),
        "offset": safe_offset,
        "limit": safe_limit,
        "items": items[safe_offset : safe_offset + safe_limit],
    }


def candidate_values(row: dict[str, Any]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    candidate = row.get("candidate")
    if isinstance(candidate, dict):
        values.append(candidate)
    per_source = row.get("per_source")
    if isinstance(per_source, dict):
        for source_row in per_source.values():
            if not isinstance(source_row, dict):
                continue
            values.append(source_row)
            nested = source_row.get("candidate")
            if isinstance(nested, dict):
                values.append(nested)
    return values


def row_matches_review_filters(
    row: dict[str, Any],
    *,
    section: str,
    reason: str | None,
    display_eligible: bool | None,
    source_role: str | None,
) -> bool:
    if reason:
        row_reason = canonical_review_reason(row.get("reason"))
        if section == "replacement_pending":
            row_reason = "replacement_pending"
        elif section == "duplicate_groups":
            row_reason = "duplicate_candidate"
        elif section == "failed_acquisitions":
            row_reason = "failed_download"
        if row_reason != canonical_review_reason(reason):
            return False
    candidates = candidate_values(row)
    if display_eligible is not None:
        if not candidates and display_eligible is True:
            return False
        if candidates and not any(
            bool(candidate.get("display_eligible")) is display_eligible for candidate in candidates
        ):
            return False
    if source_role:
        normalized_role = source_role.strip().lower()
        if not candidates or not any(
            str(candidate.get("source_role") or "").strip().lower() == normalized_role for candidate in candidates
        ):
            return False
    return True


def append_action_to_artifact_payload(payload: dict[str, Any], action: dict[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    actions = safe_list(updated.get("operator_actions"))
    actions.append(action)
    updated["operator_actions"] = actions
    action_type = str(action.get("type") or "").strip()
    if action_type == "replacement_approved":
        approvals = safe_list(updated.get("replacement_approvals"))
        approvals.append(action)
        updated["replacement_approvals"] = approvals
        group_id = str(action.get("group_id") or "").strip()
        if group_id:
            replacement_pending = []
            for row in safe_list(updated.get("replacement_pending")):
                candidate = safe_dict(row)
                if str(candidate.get("group_id") or "") == group_id:
                    candidate["operator_status"] = "approved"
                    candidate["operator_action"] = action
                replacement_pending.append(candidate)
            updated["replacement_pending"] = replacement_pending
    elif action_type == "duplicate_resolved":
        resolutions = safe_list(updated.get("duplicate_resolutions"))
        resolutions.append(action)
        updated["duplicate_resolutions"] = resolutions
        action_key = str(action.get("key") or "").strip()
        action_key_type = str(action.get("key_type") or "").strip()
        duplicate_groups = []
        for row in safe_list(updated.get("duplicate_groups")):
            duplicate = safe_dict(row)
            if (
                str(duplicate.get("key") or "") == action_key
                and str(duplicate.get("key_type") or "") == action_key_type
            ):
                duplicate["operator_status"] = action.get("resolution")
                duplicate["operator_action"] = action
            duplicate_groups.append(duplicate)
        updated["duplicate_groups"] = duplicate_groups
    return updated


def append_review_action(run_id: str, action: dict[str, Any]) -> dict[str, Any]:
    row = get_run_or_404(run_id)
    review_summary = safe_dict(row.get("review_summary"))
    actions = safe_list(review_summary.get("operator_actions"))
    actions.append(action)
    review_summary["operator_actions"] = actions[-100:]
    artifact_paths = (
        cast("dict[str, Any]", row.get("artifact_paths")) if isinstance(row.get("artifact_paths"), dict) else {}
    )
    if isinstance(artifact_paths.get("run_review"), dict):
        run_review = safe_dict(load_run_artifact_payload(row, "run_review"))
        updated_run_review = append_action_to_artifact_payload(run_review, action)
        updated_artifact = write_run_artifact_payload(row, "run_review", updated_run_review)
        artifact_paths = dict(artifact_paths)
        artifact_paths["run_review"] = updated_artifact
    if action.get("type") == "replacement_approved" and isinstance(artifact_paths.get("replacement_candidates"), dict):
        group_id = str(action.get("group_id") or "").strip()
        replacement_candidates = []
        for item in safe_list(load_run_artifact_payload(row, "replacement_candidates")):
            candidate = safe_dict(item)
            if group_id and str(candidate.get("group_id") or "") == group_id:
                candidate["operator_status"] = "approved"
                candidate["operator_action"] = action
            replacement_candidates.append(candidate)
        artifact_paths["replacement_candidates"] = write_run_artifact_payload(
            row,
            "replacement_candidates",
            replacement_candidates,
        )
    elif action.get("type") == "duplicate_resolved" and isinstance(artifact_paths.get("imported_records"), dict):
        duplicate_asset_ids = {str(value) for value in safe_list(action.get("updated_media_asset_ids")) if str(value)}
        imported_records = []
        for item in safe_list(load_run_artifact_payload(row, "imported_records")):
            imported = safe_dict(item)
            if str(imported.get("media_asset_id") or "") in duplicate_asset_ids:
                imported["operator_status"] = "duplicate_marked"
                imported["operator_action"] = action
            imported_records.append(imported)
        artifact_paths["imported_records"] = write_run_artifact_payload(row, "imported_records", imported_records)
    return update_bravotv_run_progress(run_id, artifact_paths=artifact_paths, review_summary=review_summary)


def fetch_media_asset(db: Any, asset_id: str) -> dict[str, Any]:
    response = (
        db.schema("core")
        .table("media_assets")
        .select("id, source, source_url, hosted_url, hosted_key, width, height, metadata")
        .eq("id", asset_id)
        .limit(1)
        .execute()
    )
    rows = response.data if isinstance(getattr(response, "data", None), list) else []
    if not rows:
        raise HTTPException(status_code=404, detail="Media asset not found")
    return rows[0] if isinstance(rows[0], dict) else {}


def update_media_asset_metadata(db: Any, asset_id: str, patch: dict[str, Any]) -> None:
    row = fetch_media_asset(db, asset_id)
    metadata = safe_dict(row.get("metadata"))
    metadata.update(patch)
    db.schema("core").table("media_assets").update({"metadata": metadata}).eq("id", asset_id).execute()


def find_replacement_candidate(
    row: dict[str, Any],
    *,
    group_id: str,
    media_asset_id: str | None,
) -> dict[str, Any]:
    replacement_candidates = safe_list(load_run_artifact_payload(row, "replacement_candidates"))
    requested_asset_id = str(media_asset_id or "").strip()
    candidate = next(
        (
            safe_dict(item)
            for item in replacement_candidates
            if isinstance(item, dict)
            and str(item.get("group_id") or "") == group_id
            and (not requested_asset_id or str(item.get("media_asset_id") or "") == requested_asset_id)
        ),
        {},
    )
    if not candidate:
        raise HTTPException(status_code=404, detail="Replacement candidate not found for this run")
    return candidate


def approve_replacement_for_run(
    *,
    run_id: str,
    row: dict[str, Any],
    group_id: str,
    payload: Any,
    note: str | None = None,
    db: Any | None = None,
) -> dict[str, Any]:
    candidate = find_replacement_candidate(row, group_id=group_id, media_asset_id=str(payload.media_asset_id or ""))
    media_asset_id = str(payload.media_asset_id or candidate.get("media_asset_id") or "").strip()
    if not media_asset_id:
        raise HTTPException(status_code=409, detail="Replacement candidate is missing media_asset_id")

    db = db or create_supabase_admin_client()
    asset = fetch_media_asset(db, media_asset_id)
    try:
        replacement = resolve_public_replacement_from_page(
            payload.page_url,
            source_domain=payload.source_domain,
            expected_width=payload.expected_width or asset.get("width"),
            expected_height=payload.expected_height or asset.get("height"),
            bravo_only=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to scrape replacement page: {exc}") from exc
    if replacement is None:
        raise HTTPException(status_code=422, detail="No approved replacement image found on the page")
    try:
        result = apply_media_asset_replacement(
            db,
            asset_id=media_asset_id,
            row=asset,
            replacement=replacement,
            resolution_label="bravotv_run_approved_replacement",
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to approve replacement: {exc}") from exc

    action = {
        "type": "replacement_approved",
        "run_id": run_id,
        "group_id": group_id,
        "media_asset_id": media_asset_id,
        "page_url": payload.page_url,
        "source_domain": payload.source_domain,
        "note": note if note is not None else payload.note,
        "result": result,
        "created_at": datetime.now(UTC).isoformat(),
    }
    append_review_action(run_id, action)
    return {"run_id": run_id, "group_id": group_id, "action": action, "result": result}


def review_items(
    *,
    run_id: str,
    section: str,
    reason: str | None,
    display_eligible: bool | None,
    source_role: str | None,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    row = get_run_or_404(run_id)
    artifact_name = "merged_catalog" if section == "merged_catalog" else "run_review"
    payload = load_run_artifact_payload(row, artifact_name)
    raw_items = payload if section == "merged_catalog" else safe_list(safe_dict(payload).get(section))
    items = [
        safe_dict(item)
        for item in raw_items
        if isinstance(item, dict)
        and row_matches_review_filters(
            item,
            section=section,
            reason=reason,
            display_eligible=display_eligible,
            source_role=source_role,
        )
    ]
    return {
        "run_id": run_id,
        "section": section,
        "filters": {
            "reason": canonical_review_reason(reason) if reason else None,
            "display_eligible": display_eligible,
            "source_role": source_role,
        },
        **paginate(items, offset=offset, limit=limit),
    }


def resolve_duplicate_group(*, run_id: str, payload: Any) -> dict[str, Any]:
    if payload.action == "mark_duplicate" and not payload.primary_group_id:
        raise HTTPException(status_code=422, detail="primary_group_id is required when marking duplicates")
    group_ids = [str(value).strip() for value in payload.group_ids if str(value).strip()]
    if payload.primary_group_id and payload.primary_group_id not in group_ids:
        raise HTTPException(status_code=422, detail="primary_group_id must be included in group_ids")

    row = get_run_or_404(run_id)
    imported_records = [safe_dict(item) for item in safe_list(load_run_artifact_payload(row, "imported_records"))]
    records_by_group = {str(item.get("group_id") or ""): item for item in imported_records if item.get("group_id")}
    updated_asset_ids: list[str] = []
    primary_asset_id = None
    if payload.primary_group_id:
        primary_asset_id = (
            str(records_by_group.get(payload.primary_group_id, {}).get("media_asset_id") or "").strip() or None
        )

    if payload.action == "mark_duplicate":
        if not primary_asset_id:
            raise HTTPException(status_code=404, detail="Primary duplicate media asset not found in imported records")
        db = create_supabase_admin_client()
        for group_id in group_ids:
            if group_id == payload.primary_group_id:
                continue
            asset_id = str(records_by_group.get(group_id, {}).get("media_asset_id") or "").strip()
            if not asset_id:
                continue
            update_media_asset_metadata(
                db,
                asset_id,
                {
                    "duplicate_resolution": {
                        "run_id": run_id,
                        "key_type": payload.key_type,
                        "key": payload.key,
                        "action": payload.action,
                        "duplicate_of_media_asset_id": primary_asset_id,
                        "resolved_at": datetime.now(UTC).isoformat(),
                    }
                },
            )
            updated_asset_ids.append(asset_id)

    action = {
        "type": "duplicate_resolved",
        "run_id": run_id,
        "key_type": payload.key_type,
        "key": payload.key,
        "group_ids": group_ids,
        "resolution": payload.action,
        "primary_group_id": payload.primary_group_id,
        "primary_media_asset_id": primary_asset_id,
        "updated_media_asset_ids": updated_asset_ids,
        "note": payload.note,
        "created_at": datetime.now(UTC).isoformat(),
    }
    append_review_action(run_id, action)
    return {"run_id": run_id, "action": action}
