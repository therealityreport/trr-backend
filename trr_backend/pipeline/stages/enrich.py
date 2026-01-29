"""Stage 3: Enrich show metadata from external sources."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from trr_backend.ingestion.tmdb_show_backfill import (
    build_tmdb_show_patch,
    needs_tmdb_enrichment,
)
from trr_backend.integrations.tmdb.client import (
    fetch_tv_details,
    resolve_api_key,
    resolve_bearer_token,
)
from trr_backend.pipeline.models import RunContext, StageResult, StageStatus
from trr_backend.repositories.shows import update_show

logger = logging.getLogger(__name__)


def run(context: RunContext) -> StageResult:
    """
    Enrich show metadata from external sources.

    This stage fetches TMDb details for shows that need enrichment.
    A show needs enrichment if it has a tmdb_id but is missing
    any of the tmdb_ metadata fields.
    """
    started_at = datetime.now(UTC)

    try:
        # Re-query shows by IDs (independent of stage 1 artifacts)
        if not context.show_ids:
            raise ValueError("No show_ids in context - stage 1 must run or be resumed with manifest")

        response = context.db.schema("core").table("shows").select("*").in_("id", context.show_ids).execute()
        shows = response.data or []

        api_key = resolve_api_key()
        bearer_token = resolve_bearer_token()

        # Use cache to avoid refetching same show details
        tmdb_cache: dict = {}

        processed = 0
        skipped = 0
        failed = 0

        for show in shows:
            show_id = show["id"]
            tmdb_id = show.get("tmdb_id")

            # Skip if no TMDb ID
            if not tmdb_id:
                skipped += 1
                continue

            # Check if show needs enrichment
            if not needs_tmdb_enrichment(show):
                skipped += 1
                continue

            try:
                # Fetch TMDb details
                details = fetch_tv_details(
                    tmdb_id,
                    api_key=api_key,
                    bearer_token=bearer_token,
                    append_to_response=["external_ids"],
                    cache=tmdb_cache,
                )

                # Build patch
                fetched_at = datetime.now(UTC).isoformat()
                patch = build_tmdb_show_patch(details, fetched_at=fetched_at)

                # Extract IMDb ID from external_ids if show doesn't have one
                if not show.get("imdb_id"):
                    external_ids = details.get("external_ids", {})
                    imdb_id = external_ids.get("imdb_id")
                    if imdb_id:
                        patch["imdb_id"] = imdb_id

                # Apply patch
                if not context.config.dry_run:
                    update_show(context.db, show_id, patch)

                processed += 1
                if context.config.verbose:
                    print(f"    Enriched {show.get('name')}: {len(patch)} fields updated")

            except Exception as e:
                logger.warning(f"Failed to enrich show {show_id}: {e}")
                failed += 1

        if context.config.verbose:
            print(f"    Summary: processed={processed}, skipped={skipped}, failed={failed}")

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="03_enrich",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=processed,
            items_skipped=skipped,
            items_failed=failed,
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
