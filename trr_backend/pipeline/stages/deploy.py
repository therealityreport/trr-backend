"""Stage 5: Deploy/finalize pipeline run."""

from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.pipeline.models import RunContext, StageResult, StageStatus


def run(context: RunContext) -> StageResult:
    """
    Deploy/finalize pipeline run.

    This stage performs final cleanup and validation:
    - Update sync_state for processed shows
    - Trigger PostgREST schema reload if needed
    - Generate summary statistics

    TODO: Implement sync_state updates
    """
    started_at = datetime.now(UTC)

    try:
        shows = context.artifacts.get("collected_shows", [])

        if context.config.verbose:
            print(f"    Finalizing run for {len(shows)} shows")

        # Update sync_state for each show
        # TODO: Use sync_state repository to mark shows as synced

        finalized = len(shows)

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="05_deploy",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=finalized,
        )

    except Exception as e:
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="05_deploy",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message=str(e),
        )
