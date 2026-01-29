"""Stage 2: Resolve external IDs (TMDb, IMDb)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from trr_backend.ingestion.tmdb_show_backfill import resolve_tmdb_id_from_find_payload
from trr_backend.integrations.tmdb.client import (
    find_by_imdb_id,
    resolve_api_key,
    resolve_bearer_token,
)
from trr_backend.pipeline.models import RunContext, StageResult, StageStatus
from trr_backend.repositories.shows import update_show

logger = logging.getLogger(__name__)


def run(context: RunContext) -> StageResult:
    """
    Resolve missing external IDs for collected shows.

    This stage uses TMDb's /find endpoint to resolve missing TMDb IDs
    from IMDb IDs.
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

        processed = 0
        skipped = 0
        failed = 0

        for show in shows:
            show_id = show["id"]
            tmdb_id = show.get("tmdb_id")
            imdb_id = show.get("imdb_id")

            # Skip if already has TMDb ID
            if tmdb_id:
                skipped += 1
                continue

            # Skip if no IMDb ID to resolve from
            if not imdb_id:
                skipped += 1
                continue

            # Resolve TMDb ID from IMDb ID
            try:
                payload = find_by_imdb_id(
                    imdb_id,
                    api_key=api_key,
                    bearer_token=bearer_token,
                )
                resolved_id, reason = resolve_tmdb_id_from_find_payload(
                    payload,
                    show_name=show.get("name"),
                    premiere_date=show.get("premiere_date"),
                )

                if resolved_id:
                    if not context.config.dry_run:
                        update_show(context.db, show_id, {"tmdb_id": resolved_id})
                    processed += 1
                    if context.config.verbose:
                        print(f"    Resolved {show.get('name')}: TMDb={resolved_id} ({reason})")
                else:
                    if context.config.verbose:
                        print(f"    Could not resolve {show.get('name')}: {reason}")
                    failed += 1

            except Exception as e:
                logger.warning(f"Failed to resolve TMDb ID for {show_id}: {e}")
                failed += 1

        if context.config.verbose:
            print(f"    Summary: processed={processed}, skipped={skipped}, failed={failed}")

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="02_resolve",
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
            stage_name="02_resolve",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message=str(e),
        )
