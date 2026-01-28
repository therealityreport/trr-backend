"""Stage 1: Collect shows based on filters."""

from __future__ import annotations

from datetime import UTC, datetime

# Reuse existing logic
from scripts._sync_common import SHOW_SELECT_FIELDS
from trr_backend.pipeline.models import RunContext, StageResult, StageStatus


def run(context: RunContext) -> StageResult:
    """
    Collect shows based on filters.

    This stage fetches show records from the database based on the configured
    filters (show_ids, tmdb_ids, imdb_ids, or all) and populates context.show_ids
    for downstream stages.
    """
    started_at = datetime.now(UTC)

    try:
        filters = context.config.show_filters
        db = context.db
        shows: list[dict] = []

        if filters.get("all"):
            response = db.schema("core").table("shows").select(SHOW_SELECT_FIELDS).execute()
            shows = response.data or []
        else:
            if filters.get("show_ids"):
                response = (
                    db.schema("core").table("shows").select(SHOW_SELECT_FIELDS).in_("id", filters["show_ids"]).execute()
                )
                shows.extend(response.data or [])
            if filters.get("tmdb_ids"):
                response = (
                    db.schema("core")
                    .table("shows")
                    .select(SHOW_SELECT_FIELDS)
                    .in_("tmdb_id", filters["tmdb_ids"])
                    .execute()
                )
                shows.extend(response.data or [])
            if filters.get("imdb_ids"):
                response = (
                    db.schema("core")
                    .table("shows")
                    .select(SHOW_SELECT_FIELDS)
                    .in_("imdb_id", filters["imdb_ids"])
                    .execute()
                )
                shows.extend(response.data or [])

        # Dedupe
        seen: set[str] = set()
        unique_shows: list[dict] = []
        for s in shows:
            show_id = s.get("id")
            if show_id and show_id not in seen:
                seen.add(show_id)
                unique_shows.append(s)

        context.show_ids = [s["id"] for s in unique_shows]
        context.artifacts["collected_shows"] = unique_shows

        if context.config.verbose:
            print(f"    Collected {len(unique_shows)} shows")

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="01_collect",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=len(unique_shows),
        )

    except Exception as e:
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="01_collect",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message=str(e),
        )
