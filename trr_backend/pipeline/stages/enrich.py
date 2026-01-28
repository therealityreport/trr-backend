"""Stage 3: Enrich show metadata from external sources."""

from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.pipeline.models import RunContext, StageResult, StageStatus


def run(context: RunContext) -> StageResult:
    """
    Enrich show metadata from external sources.

    This stage fetches additional metadata from TMDb, including:
    - Show details (genres, networks, production companies)
    - Seasons and episodes
    - Cast and crew credits
    - Watch providers

    TODO: Wrap existing sync scripts:
    - backfill_tmdb_show_details.py
    - sync_tmdb_show_entities.py
    - sync_tmdb_watch_providers.py
    - sync_seasons_episodes.py
    - sync_show_cast.py
    """
    started_at = datetime.now(UTC)

    try:
        shows = context.artifacts.get("collected_shows", [])

        if context.config.verbose:
            print(f"    Enriching {len(shows)} shows")

        # TODO: Implement actual enrichment using existing scripts
        # For now, this is a pass-through stage

        enriched = 0
        skipped = len(shows)

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="03_enrich",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=enriched,
            items_skipped=skipped,
        )

    except Exception as e:
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="03_enrich",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message=str(e),
        )
