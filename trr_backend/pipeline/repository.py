"""Database repository for pipeline run tracking."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from trr_backend.db.session import DbSession
from trr_backend.pipeline.models import RunConfig, RunStatus, StageStatus

# Stage name to global order mapping
STAGE_ORDER = {
    "01_collect": 1,
    "02_resolve": 2,
    "03_enrich": 3,
    "04_mirror": 4,
    "05_deploy": 5,
    "06_sync_screenalytics": 6,
}


def create_run(db: DbSession, config: RunConfig) -> UUID:
    """Create a new pipeline run record."""
    response = (
        db.schema("pipeline")
        .table("runs")
        .insert(
            {
                "name": f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                "status": RunStatus.PENDING.value,
                "config": {
                    "from_stage": config.from_stage,
                    "to_stage": config.to_stage,
                    "show_filters": config.show_filters,
                    "dry_run": config.dry_run,
                    "force": config.force,
                    "skip_s3": config.skip_s3,
                },
            }
        )
        .execute()
    )
    return UUID(response.data[0]["id"])


def create_run_stages(db: DbSession, run_id: UUID, stage_names: list[str]) -> None:
    """Create stage records with global stage_order."""
    rows = [
        {
            "run_id": str(run_id),
            "stage_name": name,
            "stage_order": STAGE_ORDER.get(name, 99),  # Global order
        }
        for name in stage_names
    ]
    db.schema("pipeline").table("run_stages").insert(rows).execute()


def ensure_run_stages(db: DbSession, run_id: UUID, stage_names: list[str]) -> None:
    """Ensure stage rows exist for all given stages (upsert missing).

    This handles the case where a run was created with --to 2 and later
    resumed with --to 5 - stages 3-5 need rows to be created.
    """
    for name in stage_names:
        order = STAGE_ORDER.get(name, 99)
        db.schema("pipeline").table("run_stages").upsert(
            {
                "run_id": str(run_id),
                "stage_name": name,
                "stage_order": order,
            },
            on_conflict="run_id,stage_name",
        ).execute()


def update_run(
    db: DbSession,
    run_id: UUID,
    *,
    status: RunStatus | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    error_stage: str | None = None,
    error_message: str | None = None,
) -> None:
    """Update run record."""
    payload: dict[str, Any] = {}
    if status is not None:
        payload["status"] = status.value
    if started_at is not None:
        payload["started_at"] = started_at.isoformat()
    if completed_at is not None:
        payload["completed_at"] = completed_at.isoformat()
    if error_stage is not None:
        payload["error_stage"] = error_stage
    if error_message is not None:
        payload["error_message"] = error_message[:1000]

    if payload:
        db.schema("pipeline").table("runs").update(payload).eq("id", str(run_id)).execute()


def update_run_stage(
    db: DbSession,
    run_id: UUID,
    stage_name: str,
    *,
    status: StageStatus | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    duration_ms: int | None = None,
    items_processed: int | None = None,
    items_skipped: int | None = None,
    items_failed: int | None = None,
    input_hash: str | None = None,
    output_hash: str | None = None,
    manifest_key: str | None = None,
    error_message: str | None = None,
    error_details: dict | None = None,
) -> None:
    """Update stage record."""
    payload: dict[str, Any] = {}
    if status is not None:
        payload["status"] = status.value
    if started_at is not None:
        payload["started_at"] = started_at.isoformat()
    if completed_at is not None:
        payload["completed_at"] = completed_at.isoformat()
    if duration_ms is not None:
        payload["duration_ms"] = duration_ms
    if items_processed is not None:
        payload["items_processed"] = items_processed
    if items_skipped is not None:
        payload["items_skipped"] = items_skipped
    if items_failed is not None:
        payload["items_failed"] = items_failed
    if input_hash is not None:
        payload["input_hash"] = input_hash
    if output_hash is not None:
        payload["output_hash"] = output_hash
    if manifest_key is not None:
        payload["manifest_key"] = manifest_key
    if error_message is not None:
        payload["error_message"] = error_message[:1000]
    if error_details is not None:
        payload["error_details"] = error_details

    if payload:
        (
            db.schema("pipeline")
            .table("run_stages")
            .update(payload)
            .eq("run_id", str(run_id))
            .eq("stage_name", stage_name)
            .execute()
        )


def fetch_run_with_stages(db: DbSession, run_id: UUID) -> dict | None:
    """Fetch run and its stages."""
    response = db.schema("pipeline").table("runs").select("*").eq("id", str(run_id)).execute()
    if not response.data:
        return None
    run = response.data[0]

    stages_response = (
        db.schema("pipeline").table("run_stages").select("*").eq("run_id", str(run_id)).order("stage_order").execute()
    )
    run["stages"] = stages_response.data or []
    return run


def get_stage_prior_state(db: DbSession, run_id: UUID, stage_name: str) -> dict | None:
    """Get prior stage state for resume logic."""
    response = (
        db.schema("pipeline")
        .table("run_stages")
        .select("status,input_hash,output_hash,manifest_key")
        .eq("run_id", str(run_id))
        .eq("stage_name", stage_name)
        .execute()
    )
    return response.data[0] if response.data else None


def list_runs(db: DbSession, *, limit: int = 10) -> list[dict]:
    """List recent pipeline runs."""
    response = (
        db.schema("pipeline")
        .table("runs")
        .select("id,name,status,created_at,started_at,completed_at")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return response.data or []
