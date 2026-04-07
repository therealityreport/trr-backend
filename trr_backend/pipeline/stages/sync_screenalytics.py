"""Stage 6: Sync Screenalytics result bundles into backend-owned state."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any

from trr_backend.pipeline.models import RunContext, StageResult, StageStatus
from trr_backend.repositories import screenalytics_runs

_RESULT_CONTRACT_VERSION = "trr-screenalytics/v1"


def _env_flag(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def validate_result_bundle(bundle: dict[str, Any]) -> str | None:
    run = bundle.get("run") if isinstance(bundle, dict) else {}
    result_contract_version = str((run or {}).get("result_contract_version") or "").strip()
    if result_contract_version != _RESULT_CONTRACT_VERSION:
        return f"incomplete result bundle: unsupported contract version '{result_contract_version or 'missing'}'"

    artifacts = bundle.get("artifacts")
    person_metrics = bundle.get("person_metrics")
    leaderboard = bundle.get("leaderboard")

    if not isinstance(artifacts, list) or len(artifacts) == 0:
        return "incomplete result bundle: missing artifacts"
    if not isinstance(person_metrics, list) or len(person_metrics) == 0:
        return "incomplete result bundle: missing person metrics"
    if not isinstance(leaderboard, list) or len(leaderboard) == 0:
        return "incomplete result bundle: missing leaderboard"

    return None


def _output_hash(run_ids: list[str]) -> str | None:
    if not run_ids:
        return None
    payload = json.dumps(sorted(run_ids), separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run(context: RunContext) -> StageResult:
    started_at = datetime.now(UTC)

    if not _env_flag("TRR_STAGE6_SYNC_ENABLED", False):
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="06_sync_screenalytics",
            status=StageStatus.SKIPPED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
        )

    if not context.show_ids:
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="06_sync_screenalytics",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message="No show_ids in context",
            items_failed=1,
        )

    bundles = screenalytics_runs.list_result_sync_candidates(context.show_ids)
    if not bundles:
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="06_sync_screenalytics",
            status=StageStatus.SKIPPED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
        )

    synced_run_ids: list[str] = []
    for bundle in bundles:
        run_payload = bundle.get("run") if isinstance(bundle, dict) else {}
        run_id = str((run_payload or {}).get("id") or "").strip()
        if not run_id:
            continue

        validation_error = validate_result_bundle(bundle)
        if validation_error:
            screenalytics_runs.mark_result_ingest_status(run_id, status="failed", error=validation_error)
            completed_at = datetime.now(UTC)
            return StageResult(
                stage_name="06_sync_screenalytics",
                status=StageStatus.FAILED,
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=int((completed_at - started_at).total_seconds() * 1000),
                items_processed=len(synced_run_ids),
                items_failed=1,
                output_hash=_output_hash(synced_run_ids),
                error_message=validation_error,
            )

        screenalytics_runs.mark_result_ingest_status(run_id, status="ingested")
        synced_run_ids.append(run_id)

    context.artifacts["screenalytics_synced_runs"] = synced_run_ids
    completed_at = datetime.now(UTC)
    return StageResult(
        stage_name="06_sync_screenalytics",
        status=StageStatus.SUCCESS,
        started_at=started_at,
        completed_at=completed_at,
        duration_ms=int((completed_at - started_at).total_seconds() * 1000),
        items_processed=len(synced_run_ids),
        output_hash=_output_hash(synced_run_ids),
    )
