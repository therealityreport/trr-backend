"""Stage 6: Sync Screenalytics results (stub - manifest-driven)."""

from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.pipeline.models import RunContext, StageResult, StageStatus


def run(context: RunContext) -> StageResult:
    """
    Ingest Screenalytics run results.

    TODO: Implement when Screenalytics manifest format is defined.

    Expected workflow:
    1. Read Screenalytics outbox (S3 bucket or API)
    2. Find completed runs with manifest.json
    3. Parse manifest for summary artifact URIs
    4. Download and parse summary artifacts
    5. Upsert results into TRR database

    Manifest assumptions:
    - runs/{run_id}/manifest.json
    - Contains: input_files, output_files, summary_uri, timestamps
    """
    started_at = datetime.now(UTC)

    if context.config.verbose:
        print("    Stage 6 (sync_screenalytics): STUB - not implemented")

    completed_at = datetime.now(UTC)
    return StageResult(
        stage_name="06_sync_screenalytics",
        status=StageStatus.SKIPPED,
        started_at=started_at,
        completed_at=completed_at,
        duration_ms=int((completed_at - started_at).total_seconds() * 1000),
    )
