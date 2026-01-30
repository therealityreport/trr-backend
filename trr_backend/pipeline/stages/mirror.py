"""Stage 4: Mirror media assets to S3."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from trr_backend.media.s3_mirror import (
    get_s3_client,
    mirror_show_image_row,
)
from trr_backend.pipeline.models import RunContext, StageResult, StageStatus
from trr_backend.repositories.show_images import (
    fetch_show_images_missing_hosted,
    update_show_image_hosted_fields,
)

logger = logging.getLogger(__name__)


def run(context: RunContext) -> StageResult:
    """
    Mirror media assets to S3.

    This stage downloads images from external sources and uploads them
    to S3 with content-addressed deduplication. Focuses on show images
    (posters, backdrops) for collected shows.
    """
    started_at = datetime.now(UTC)

    try:
        # Skip if --skip-s3 is set
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
            )

        # Re-query shows by IDs (independent of stage 1 artifacts)
        if not context.show_ids:
            raise ValueError("No show_ids in context - stage 1 must run or be resumed with manifest")

        response = context.db.schema("core").table("shows").select("*").in_("id", context.show_ids).execute()
        shows = response.data or []

        if context.config.verbose:
            print(f"    Mirroring assets for {len(shows)} shows")

        # Get S3 client once for all operations
        s3_client = get_s3_client()

        processed = 0
        skipped = 0
        failed = 0

        for show in shows:
            show_id = show["id"]

            try:
                # Fetch show images missing hosted_url
                images = fetch_show_images_missing_hosted(
                    context.db,
                    show_id=show_id,
                    limit=100,
                )

                if not images:
                    skipped += 1
                    continue

                show_processed = 0
                show_failed = 0

                for image in images:
                    try:
                        # Mirror the image to S3
                        patch = mirror_show_image_row(image, s3_client=s3_client)

                        if patch and not context.config.dry_run:
                            # Update the database with hosted fields
                            update_show_image_hosted_fields(
                                context.db,
                                image["id"],
                                patch,
                            )
                            show_processed += 1
                        elif patch:
                            # Dry run - count as processed
                            show_processed += 1

                    except Exception as e:
                        logger.warning(f"Failed to mirror show image {image.get('id')}: {e}")
                        show_failed += 1

                if show_processed > 0:
                    processed += 1
                    if context.config.verbose:
                        print(f"    Mirrored {show.get('name')}: {show_processed} images")
                if show_failed > 0:
                    failed += show_failed

            except Exception as e:
                logger.warning(f"Failed to process show {show_id}: {e}")
                failed += 1

        if context.config.verbose:
            print(f"    Summary: shows_processed={processed}, shows_skipped={skipped}, images_failed={failed}")

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="04_mirror",
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
            stage_name="04_mirror",
            status=StageStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            error_message=str(e),
        )
