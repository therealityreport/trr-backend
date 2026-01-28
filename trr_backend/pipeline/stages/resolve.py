"""Stage 2: Resolve external IDs (TMDb, IMDb)."""

from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.pipeline.models import RunContext, StageResult, StageStatus


def run(context: RunContext) -> StageResult:
    """
    Resolve missing external IDs for collected shows.

    This stage uses TMDb's /find endpoint to resolve missing TMDb IDs
    from IMDb IDs, and vice versa.

    TODO: Wrap existing resolve_tmdb_ids_via_find.py logic
    """
    started_at = datetime.now(UTC)

    try:
        shows = context.artifacts.get("collected_shows", [])

        # Count shows needing resolution
        needs_tmdb = sum(1 for s in shows if not s.get("tmdb_id") and s.get("imdb_id"))
        needs_imdb = sum(1 for s in shows if not s.get("imdb_id") and s.get("tmdb_id"))

        if context.config.verbose:
            print(f"    Shows needing TMDb ID: {needs_tmdb}")
            print(f"    Shows needing IMDb ID: {needs_imdb}")

        # TODO: Implement actual resolution using resolve_tmdb_ids_via_find.py
        # For now, this is a pass-through stage

        resolved = 0
        skipped = len(shows) - needs_tmdb - needs_imdb

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="02_resolve",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=resolved,
            items_skipped=skipped,
        )

    except Exception as e:
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="02_resolve",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message=str(e),
        )
