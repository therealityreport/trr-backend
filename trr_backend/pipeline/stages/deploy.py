"""Stage 5: Deploy/finalize pipeline run."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from trr_backend.pipeline.models import RunContext, StageResult, StageStatus
from trr_backend.repositories.sync_state import mark_sync_state_success

logger = logging.getLogger(__name__)


def run(context: RunContext) -> StageResult:
    """
    Deploy/finalize pipeline run.

    This stage performs final cleanup and validation:
    - Update sync_state for processed shows (marks them as successfully synced)
    - Records the most recent episode seen for incremental sync detection
    """
    started_at = datetime.now(UTC)

    try:
        # Re-query shows by IDs (independent of stage 1 artifacts)
        if not context.show_ids:
            raise ValueError("No show_ids in context - stage 1 must run or be resumed with manifest")

        response = context.db.schema("core").table("shows").select("*").in_("id", context.show_ids).execute()
        shows = response.data or []

        if context.config.verbose:
            print(f"    Finalizing run for {len(shows)} shows")

        processed = 0
        failed = 0

        for show in shows:
            show_id = show["id"]

            try:
                if not context.config.dry_run:
                    # Mark sync state as success for this show
                    mark_sync_state_success(
                        context.db,
                        table_name="shows",
                        show_id=show_id,
                        last_seen_most_recent_episode=show.get("most_recent_episode"),
                    )
                processed += 1

            except Exception as e:
                logger.warning(f"Failed to mark sync state for {show_id}: {e}")
                failed += 1

        if context.config.verbose:
            print(f"    Summary: processed={processed}, failed={failed}")

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="05_deploy",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=processed,
            items_failed=failed,
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
