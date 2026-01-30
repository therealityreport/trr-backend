"""Tests for pipeline models, specifically hash computation logic."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

from trr_backend.pipeline.models import RunConfig, RunContext


class TestStageInputHash:
    """Tests for compute_stage_input_hash method."""

    def _make_context(
        self,
        *,
        show_filters: dict | None = None,
        dry_run: bool = False,
        force: bool = False,
        from_stage: int = 1,
        to_stage: int = 6,
        verbose: bool = False,
        skip_s3: bool = False,
        show_ids: list[str] | None = None,
    ) -> RunContext:
        """Create a RunContext with the specified configuration."""
        config = RunConfig(
            show_filters=show_filters or {},
            dry_run=dry_run,
            force=force,
            from_stage=from_stage,
            to_stage=to_stage,
            verbose=verbose,
            skip_s3=skip_s3,
        )
        context = RunContext(
            run_id=uuid4(),
            config=config,
            db=MagicMock(),
        )
        if show_ids:
            context.show_ids = show_ids
        return context

    def test_stage1_hash_uses_filters_and_dry_run_only(self):
        """Stage 1 hash should only include show_filters and dry_run."""
        context = self._make_context(
            show_filters={"all": True},
            dry_run=False,
            show_ids=["id1", "id2"],  # Should NOT affect stage 1 hash
        )

        hash1 = context.compute_stage_input_hash("01_collect")

        # Change show_ids - hash should NOT change for stage 1
        context.show_ids = ["id3", "id4", "id5"]
        hash2 = context.compute_stage_input_hash("01_collect")

        assert hash1 == hash2, "Stage 1 hash should not depend on show_ids"

    def test_stage2_hash_includes_show_ids(self):
        """Stage 2+ hash should include show_filters, show_ids, and dry_run."""
        context = self._make_context(
            show_filters={"all": True},
            dry_run=False,
            show_ids=["id1", "id2"],
        )

        hash1 = context.compute_stage_input_hash("02_resolve")

        # Change show_ids - hash SHOULD change for stage 2+
        context.show_ids = ["id3", "id4", "id5"]
        hash2 = context.compute_stage_input_hash("02_resolve")

        assert hash1 != hash2, "Stage 2+ hash should depend on show_ids"

    def test_stage1_hash_differs_from_stage2_hash(self):
        """Stage 1 and Stage 2 should have different hash formulas."""
        context = self._make_context(
            show_filters={"all": True},
            dry_run=False,
            show_ids=["id1", "id2"],
        )

        hash_stage1 = context.compute_stage_input_hash("01_collect")
        hash_stage2 = context.compute_stage_input_hash("02_resolve")

        # With same config but show_ids populated, hashes should differ
        # because stage 2 includes show_ids and stage 1 doesn't
        assert hash_stage1 != hash_stage2

    def test_hash_changes_with_dry_run(self):
        """Hash should change when dry_run changes (prevents resuming dry-run into real run)."""
        context_real = self._make_context(
            show_filters={"all": True},
            dry_run=False,
            show_ids=["id1"],
        )
        context_dry = self._make_context(
            show_filters={"all": True},
            dry_run=True,
            show_ids=["id1"],
        )

        # Test for both stage 1 and stage 2
        assert context_real.compute_stage_input_hash("01_collect") != context_dry.compute_stage_input_hash("01_collect")
        assert context_real.compute_stage_input_hash("02_resolve") != context_dry.compute_stage_input_hash("02_resolve")

    def test_hash_excludes_force_flag(self):
        """Hash should NOT change when force flag changes."""
        context_normal = self._make_context(
            show_filters={"all": True},
            force=False,
            show_ids=["id1"],
        )
        context_forced = self._make_context(
            show_filters={"all": True},
            force=True,
            show_ids=["id1"],
        )

        assert context_normal.compute_stage_input_hash("01_collect") == context_forced.compute_stage_input_hash(
            "01_collect"
        )
        assert context_normal.compute_stage_input_hash("02_resolve") == context_forced.compute_stage_input_hash(
            "02_resolve"
        )

    def test_hash_excludes_from_to_stage(self):
        """Hash should NOT change when from_stage/to_stage change."""
        context_full = self._make_context(
            show_filters={"all": True},
            from_stage=1,
            to_stage=6,
            show_ids=["id1"],
        )
        context_partial = self._make_context(
            show_filters={"all": True},
            from_stage=2,
            to_stage=4,
            show_ids=["id1"],
        )

        assert context_full.compute_stage_input_hash("01_collect") == context_partial.compute_stage_input_hash(
            "01_collect"
        )
        assert context_full.compute_stage_input_hash("03_enrich") == context_partial.compute_stage_input_hash(
            "03_enrich"
        )

    def test_hash_excludes_verbose_flag(self):
        """Hash should NOT change when verbose flag changes."""
        context_quiet = self._make_context(
            show_filters={"all": True},
            verbose=False,
            show_ids=["id1"],
        )
        context_verbose = self._make_context(
            show_filters={"all": True},
            verbose=True,
            show_ids=["id1"],
        )

        assert context_quiet.compute_stage_input_hash("01_collect") == context_verbose.compute_stage_input_hash(
            "01_collect"
        )

    def test_hash_excludes_skip_s3_flag(self):
        """Hash should NOT change when skip_s3 flag changes."""
        context_with_s3 = self._make_context(
            show_filters={"all": True},
            skip_s3=False,
            show_ids=["id1"],
        )
        context_no_s3 = self._make_context(
            show_filters={"all": True},
            skip_s3=True,
            show_ids=["id1"],
        )

        assert context_with_s3.compute_stage_input_hash("01_collect") == context_no_s3.compute_stage_input_hash(
            "01_collect"
        )
        assert context_with_s3.compute_stage_input_hash("04_mirror") == context_no_s3.compute_stage_input_hash(
            "04_mirror"
        )

    def test_hash_changes_with_show_filters(self):
        """Hash should change when show_filters change."""
        context_all = self._make_context(
            show_filters={"all": True},
            show_ids=["id1"],
        )
        context_specific = self._make_context(
            show_filters={"tmdb_ids": [1396]},
            show_ids=["id1"],
        )

        assert context_all.compute_stage_input_hash("01_collect") != context_specific.compute_stage_input_hash(
            "01_collect"
        )
        assert context_all.compute_stage_input_hash("02_resolve") != context_specific.compute_stage_input_hash(
            "02_resolve"
        )

    def test_hash_is_deterministic(self):
        """Same inputs should always produce the same hash."""
        context1 = self._make_context(
            show_filters={"all": True},
            dry_run=False,
            show_ids=["id1", "id2"],
        )
        context2 = self._make_context(
            show_filters={"all": True},
            dry_run=False,
            show_ids=["id1", "id2"],
        )

        assert context1.compute_stage_input_hash("01_collect") == context2.compute_stage_input_hash("01_collect")
        assert context1.compute_stage_input_hash("02_resolve") == context2.compute_stage_input_hash("02_resolve")

    def test_hash_is_order_independent_for_show_ids(self):
        """show_ids order should not affect hash (sorted internally)."""
        context1 = self._make_context(
            show_filters={"all": True},
            show_ids=["id1", "id2", "id3"],
        )
        context2 = self._make_context(
            show_filters={"all": True},
            show_ids=["id3", "id1", "id2"],  # Different order
        )

        assert context1.compute_stage_input_hash("02_resolve") == context2.compute_stage_input_hash("02_resolve")

    def test_all_stage_names_work(self):
        """All valid stage names should compute hashes without error."""
        context = self._make_context(show_ids=["id1"])

        stage_names = [
            "01_collect",
            "02_resolve",
            "03_enrich",
            "04_mirror",
            "05_deploy",
            "06_sync_screenalytics",
        ]

        for stage in stage_names:
            # Should not raise
            hash_val = context.compute_stage_input_hash(stage)
            assert isinstance(hash_val, str)
            assert len(hash_val) == 64  # SHA256 hex digest
