"""Stage 4: Mirror media assets to S3."""

from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.pipeline.models import RunContext, StageResult, StageStatus


def run(context: RunContext) -> StageResult:
    """
    Mirror media assets to S3.

    This stage downloads images from external sources and uploads them
    to S3 with content-addressed deduplication:
    - Show images (posters, backdrops)
    - Season/episode images
    - Cast photos

    TODO: Wrap existing mirror scripts:
    - mirror_show_images_to_s3.py
    - mirror_cast_photos_to_s3.py
    """
    started_at = datetime.now(UTC)

    try:
        shows = context.artifacts.get("collected_shows", [])

        if context.config.skip_s3:
            if context.config.verbose:
                print("    Skipping S3 mirroring (--skip-s3)")

            completed_at = datetime.now(UTC)
            return StageResult(
                stage_name="04_mirror",
                status=StageStatus.SKIPPED,
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=int((completed_at - started_at).total_seconds() * 1000),
                items_skipped=len(shows),
            )

        if context.config.verbose:
            print(f"    Mirroring assets for {len(shows)} shows")

        # TODO: Implement actual mirroring using existing scripts
        # For now, this is a pass-through stage

        mirrored = 0
        skipped = len(shows)

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="04_mirror",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=mirrored,
            items_skipped=skipped,
        )

    except Exception as e:
        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="04_mirror",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message=str(e),
        )
