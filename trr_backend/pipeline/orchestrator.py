"""Pipeline orchestrator with sequential stage execution and resume support."""

from __future__ import annotations

import logging
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.pipeline.manifests import read_manifest, write_manifest
from trr_backend.pipeline.models import (
    RunConfig,
    RunContext,
    RunStatus,
    StageManifest,
    StageResult,
    StageStatus,
)
from trr_backend.pipeline.repository import (
    create_run,
    create_run_stages,
    ensure_run_stages,
    fetch_run_with_stages,
    get_stage_prior_state,
    update_run,
    update_run_stage,
)

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates sequential pipeline stage execution with resume support."""

    def __init__(
        self,
        stages: list[tuple[str, Callable[[RunContext], StageResult]]],
    ):
        """
        Initialize orchestrator with stage definitions.

        Args:
            stages: List of (stage_name, stage_function) tuples
        """
        self.stages = stages

    def run(
        self,
        config: RunConfig,
        *,
        resume_run_id: UUID | None = None,
    ) -> tuple[UUID, list[StageResult]]:
        """
        Execute pipeline stages sequentially with resume-by-hash support.

        Args:
            config: Run configuration
            resume_run_id: Optional run ID to resume from

        Returns:
            Tuple of (run_id, list of stage results)
        """
        db = create_supabase_admin_client()

        # Filter stages by from/to
        active_stages = [
            (name, fn) for i, (name, fn) in enumerate(self.stages, start=1) if config.from_stage <= i <= config.to_stage
        ]

        # Create or resume run
        if resume_run_id:
            run_id = resume_run_id
            existing = fetch_run_with_stages(db, run_id)
            if not existing:
                raise ValueError(f"Run {run_id} not found")
            # Ensure all active stages have rows (handles --to extension)
            ensure_run_stages(db, run_id, [name for name, _ in active_stages])
            if config.verbose:
                print(f"Resuming run {run_id}")
        else:
            run_id = create_run(db, config)
            create_run_stages(db, run_id, [name for name, _ in active_stages])
            if config.verbose:
                print(f"Created run {run_id}")

        context = RunContext(run_id=run_id, config=config, db=db)

        update_run(db, run_id, status=RunStatus.RUNNING, started_at=datetime.now(UTC))

        results: list[StageResult] = []
        final_status = RunStatus.SUCCESS
        error_stage = None
        error_message = None

        try:
            for stage_name, stage_fn in active_stages:
                # Compute stage-specific input hash
                current_input_hash = context.compute_stage_input_hash(stage_name)

                # Check if stage should be skipped (resume logic)
                if resume_run_id and self._should_skip_stage(db, run_id, stage_name, current_input_hash, config):
                    # CRITICAL: Hydrate context.show_ids from manifest when Stage 1 skipped
                    if stage_name == "01_collect":
                        manifest = read_manifest(str(run_id), stage_name)
                        if manifest:
                            context.show_ids = manifest.show_ids
                            if config.verbose:
                                print(f"    Hydrated {len(context.show_ids)} show_ids from manifest")

                    if config.verbose:
                        print(f"  \u25cb {stage_name}: SKIPPED (hash match)")
                    results.append(
                        StageResult(
                            stage_name=stage_name,
                            status=StageStatus.SKIPPED,
                            input_hash=current_input_hash,
                        )
                    )
                    continue

                # Mark stage as running
                started_at = datetime.now(UTC)
                update_run_stage(
                    db,
                    run_id,
                    stage_name,
                    status=StageStatus.RUNNING,
                    started_at=started_at,
                    input_hash=current_input_hash,
                )

                if config.verbose:
                    print(f"  \u25b6 {stage_name}: RUNNING")

                try:
                    result = stage_fn(context)
                    result.input_hash = current_input_hash
                    results.append(result)

                    # Write manifest
                    manifest = StageManifest(
                        run_id=str(run_id),
                        stage_name=stage_name,
                        timestamp=datetime.now(UTC).isoformat(),
                        input_hash=current_input_hash,
                        output_hash=result.output_hash,
                        show_ids=context.show_ids,
                        items_processed=result.items_processed,
                        items_skipped=result.items_skipped,
                        items_failed=result.items_failed,
                        config={
                            "from_stage": config.from_stage,
                            "to_stage": config.to_stage,
                            "show_filters": config.show_filters,
                        },
                    )
                    manifest_key = write_manifest(manifest, skip_s3=config.skip_s3)

                    update_run_stage(
                        db,
                        run_id,
                        stage_name,
                        status=result.status,
                        completed_at=result.completed_at,
                        duration_ms=result.duration_ms,
                        items_processed=result.items_processed,
                        items_skipped=result.items_skipped,
                        items_failed=result.items_failed,
                        input_hash=current_input_hash,
                        output_hash=result.output_hash,
                        manifest_key=manifest_key,
                    )

                    if config.verbose:
                        icon = "\u2713" if result.status == StageStatus.SUCCESS else "\u2717"
                        print(f"  {icon} {stage_name}: {result.status.value} (processed={result.items_processed})")

                    if result.status == StageStatus.FAILED:
                        final_status = RunStatus.FAILED
                        error_stage = stage_name
                        error_message = result.error_message
                        break

                except Exception as e:
                    error_stage = stage_name
                    error_message = str(e)
                    final_status = RunStatus.FAILED

                    result = StageResult(
                        stage_name=stage_name,
                        status=StageStatus.FAILED,
                        input_hash=current_input_hash,
                        error_message=error_message,
                        error_details={"traceback": traceback.format_exc()},
                    )
                    results.append(result)

                    update_run_stage(
                        db,
                        run_id,
                        stage_name,
                        status=StageStatus.FAILED,
                        input_hash=current_input_hash,
                        error_message=error_message,
                        error_details=result.error_details,
                    )

                    if config.verbose:
                        print(f"  \u2717 {stage_name}: FAILED - {error_message}")

                    logger.exception(f"Stage {stage_name} failed")
                    break

        finally:
            update_run(
                db,
                run_id,
                status=final_status,
                completed_at=datetime.now(UTC),
                error_stage=error_stage,
                error_message=error_message,
            )

        return run_id, results

    def _should_skip_stage(
        self,
        db,
        run_id: UUID,
        stage_name: str,
        current_input_hash: str,
        config: RunConfig,
    ) -> bool:
        """
        Check if stage can be skipped based on:
        1. Prior status = success
        2. input_hash matches current
        3. Not forced
        4. Manifest exists when required (Stage 1 always, others when skip_s3=False)

        Args:
            db: Supabase client
            run_id: The run ID
            stage_name: Name of the stage to check
            current_input_hash: Current computed input hash
            config: Run configuration

        Returns:
            True if stage should be skipped
        """
        if config.force:
            return False

        prior = get_stage_prior_state(db, run_id, stage_name)
        if not prior:
            return False

        if prior.get("status") != "success":
            return False

        if prior.get("input_hash") != current_input_hash:
            return False

        # Stage 1 ALWAYS requires manifest for hydration (otherwise context.show_ids stays empty)
        if stage_name == "01_collect" and not prior.get("manifest_key"):
            return False

        # Other stages require manifest when skip_s3=False
        if stage_name != "01_collect" and not config.skip_s3 and not prior.get("manifest_key"):
            return False

        return True
