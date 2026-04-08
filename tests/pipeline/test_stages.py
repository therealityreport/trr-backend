"""Tests for pipeline stages, specifically error handling and data dependencies."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

from trr_backend.pipeline.models import RunConfig, RunContext, StageStatus


class TestStageShowIdsDependency:
    """Tests that verify stages fail fast when context.show_ids is empty."""

    def _make_context(
        self,
        *,
        show_ids: list[str] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
        skip_s3: bool = False,
    ) -> RunContext:
        """Create a RunContext for testing."""
        config = RunConfig(
            dry_run=dry_run,
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

    def test_resolve_stage_fails_without_show_ids(self):
        """Stage 2 (resolve) should fail fast if context.show_ids is empty."""
        from trr_backend.pipeline.stages.resolve import run

        context = self._make_context(show_ids=[])

        result = run(context)

        assert result.status == StageStatus.FAILED
        assert "No show_ids in context" in (result.error_message or "")

    def test_enrich_stage_fails_without_show_ids(self):
        """Stage 3 (enrich) should fail fast if context.show_ids is empty."""
        from trr_backend.pipeline.stages.enrich import run

        context = self._make_context(show_ids=[])

        result = run(context)

        assert result.status == StageStatus.FAILED
        assert "No show_ids in context" in (result.error_message or "")

    def test_mirror_stage_fails_without_show_ids_when_not_skipped(self):
        """Stage 4 (mirror) should fail fast if context.show_ids is empty and not skip_s3."""
        from trr_backend.pipeline.stages.mirror import run

        context = self._make_context(show_ids=[], skip_s3=False)

        result = run(context)

        assert result.status == StageStatus.FAILED
        assert "No show_ids in context" in (result.error_message or "")

    def test_mirror_stage_skips_when_skip_s3_true(self):
        """Stage 4 (mirror) should skip (not fail) when skip_s3=True regardless of show_ids."""
        from trr_backend.pipeline.stages.mirror import run

        context = self._make_context(show_ids=[], skip_s3=True)

        result = run(context)

        # Should be SKIPPED, not FAILED
        assert result.status == StageStatus.SKIPPED

    def test_deploy_stage_fails_without_show_ids(self):
        """Stage 5 (deploy) should fail fast if context.show_ids is empty."""
        from trr_backend.pipeline.stages.deploy import run

        context = self._make_context(show_ids=[])

        result = run(context)

        assert result.status == StageStatus.FAILED
        assert "No show_ids in context" in (result.error_message or "")


class TestStageRequeryBehavior:
    """Tests that verify stages re-query shows from DB (not from artifacts)."""

    def test_resolve_stage_queries_db_by_show_ids(self):
        """Stage 2 should query shows by IDs from context.show_ids, not artifacts."""
        from trr_backend.pipeline.stages.resolve import run

        show_id = str(uuid4())
        context = self._make_context(show_ids=[show_id], dry_run=True)

        # Mock the DB to track the query
        mock_response = MagicMock()
        mock_response.data = [{"id": show_id, "name": "Test Show", "tmdb_id": 123, "imdb_id": "tt123"}]

        context.db.schema().table().select().in_().execute.return_value = mock_response

        run(context)

        # Verify the stage queried the DB
        context.db.schema.assert_called_with("core")
        context.db.schema().table.assert_called_with("shows")

    def test_enrich_stage_queries_db_by_show_ids(self):
        """Stage 3 should query shows by IDs from context.show_ids, not artifacts."""
        from trr_backend.pipeline.stages.enrich import run

        show_id = str(uuid4())
        context = self._make_context(show_ids=[show_id], dry_run=True)

        # Mock the DB to return a show that doesn't need enrichment
        mock_response = MagicMock()
        mock_response.data = [
            {
                "id": show_id,
                "name": "Test Show",
                "tmdb_id": 123,
                "tmdb_meta": {"some": "data"},
                "tmdb_fetched_at": "2024-01-01T00:00:00Z",
                "tmdb_vote_average": 8.5,
                "tmdb_vote_count": 100,
                "tmdb_popularity": 50.0,
                "tmdb_first_air_date": "2024-01-01",
                "tmdb_last_air_date": "2024-01-01",
                "tmdb_status": "Returning Series",
                "tmdb_type": "Scripted",
            }
        ]

        context.db.schema().table().select().in_().execute.return_value = mock_response

        result = run(context)

        # Verify success (show was skipped since it doesn't need enrichment)
        assert result.status == StageStatus.SUCCESS
        context.db.schema.assert_called_with("core")

    def test_deploy_stage_queries_db_by_show_ids(self):
        """Stage 5 should query shows by IDs from context.show_ids, not artifacts."""
        from trr_backend.pipeline.stages.deploy import run

        show_id = str(uuid4())
        context = self._make_context(show_ids=[show_id], dry_run=True)

        # Mock the DB
        mock_response = MagicMock()
        mock_response.data = [{"id": show_id, "name": "Test Show", "most_recent_episode": "S01E05"}]

        context.db.schema().table().select().in_().execute.return_value = mock_response

        result = run(context)

        # Verify success
        assert result.status == StageStatus.SUCCESS
        context.db.schema.assert_called_with("core")

    def _make_context(
        self,
        *,
        show_ids: list[str] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
        skip_s3: bool = False,
    ) -> RunContext:
        """Create a RunContext for testing."""
        config = RunConfig(
            dry_run=dry_run,
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


class TestCollectStage:
    """Tests for Stage 1 (collect) behavior."""

    def test_collect_stage_populates_show_ids(self):
        """Stage 1 should populate context.show_ids from query results."""
        from trr_backend.pipeline.stages.collect import run

        config = RunConfig(
            show_filters={"all": True},
            verbose=False,
        )
        context = RunContext(
            run_id=uuid4(),
            config=config,
            db=MagicMock(),
        )

        # Mock the DB to return some shows
        mock_response = MagicMock()
        mock_response.data = [
            {"id": "show-1", "name": "Show 1"},
            {"id": "show-2", "name": "Show 2"},
        ]

        context.db.schema().table().select().execute.return_value = mock_response

        result = run(context)

        assert result.status == StageStatus.SUCCESS
        assert context.show_ids == ["show-1", "show-2"]
        assert len(context.artifacts.get("collected_shows", [])) == 2

    def test_collect_stage_deduplicates_shows(self):
        """Stage 1 should deduplicate shows when multiple filters return same show."""
        from trr_backend.pipeline.stages.collect import run

        config = RunConfig(
            show_filters={
                "show_ids": ["show-1"],
                "tmdb_ids": [123],  # Same show
            },
            verbose=False,
        )
        context = RunContext(
            run_id=uuid4(),
            config=config,
            db=MagicMock(),
        )

        # Mock DB to return the same show from both queries
        mock_response = MagicMock()
        mock_response.data = [{"id": "show-1", "name": "Show 1", "tmdb_id": 123}]

        context.db.schema().table().select().in_().execute.return_value = mock_response

        result = run(context)

        assert result.status == StageStatus.SUCCESS
        # Should only have one show (deduplicated)
        assert len(context.show_ids) == 1
        assert context.show_ids == ["show-1"]


class TestSyncScreenalyticsStage:
    """Tests for Stage 6 (sync_screenalytics) behavior."""

    def test_sync_screenalytics_returns_skipped_when_flag_disabled(self, monkeypatch):
        """Stage 6 should remain skipped until explicitly enabled."""
        from trr_backend.pipeline.stages.sync_screenalytics import run

        monkeypatch.delenv("TRR_STAGE6_SYNC_ENABLED", raising=False)
        config = RunConfig(verbose=False)
        context = RunContext(
            run_id=uuid4(),
            config=config,
            db=MagicMock(),
        )

        result = run(context)

        # Stub stage should return SKIPPED, not SUCCESS
        assert result.status == StageStatus.SKIPPED
        assert result.stage_name == "06_sync_screenalytics"

    def test_sync_screenalytics_ingests_pending_result_bundles(self, monkeypatch):
        """Stage 6 should ingest valid result bundles when the feature flag is enabled."""
        from trr_backend.pipeline.stages.sync_screenalytics import run

        monkeypatch.setenv("TRR_STAGE6_SYNC_ENABLED", "1")

        captured: list[tuple[str, str | None]] = []

        monkeypatch.setattr(
            "trr_backend.repositories.screenalytics_runs.list_result_sync_candidates",
            lambda show_ids: [
                {
                    "run": {
                        "id": "run-1",
                        "video_asset_id": "asset-1",
                        "result_contract_version": "trr-screenalytics/v1",
                    },
                    "artifacts": [{"artifact_key": "leaderboard.json", "artifact_kind": "leaderboard"}],
                    "person_metrics": [{"person_id": str(uuid4()), "screen_time_seconds": 42.0}],
                    "leaderboard": [{"person_id": str(uuid4()), "screen_time_seconds": 42.0}],
                    "unknown_clusters": [],
                }
            ],
        )
        monkeypatch.setattr(
            "trr_backend.repositories.screenalytics_runs.mark_result_ingest_status",
            lambda run_id, *, status, error=None: captured.append((run_id, status)),
        )

        context = RunContext(
            run_id=uuid4(),
            config=RunConfig(verbose=False),
            db=MagicMock(),
            show_ids=[str(uuid4())],
        )

        result = run(context)

        assert result.status == StageStatus.SUCCESS
        assert result.items_processed == 1
        assert captured == [("run-1", "ingested")]
        assert context.artifacts["screenalytics_synced_runs"] == ["run-1"]

    def test_sync_screenalytics_fails_on_incomplete_contract(self, monkeypatch):
        """Stage 6 should fail when a candidate bundle is missing required result data."""
        from trr_backend.pipeline.stages.sync_screenalytics import run

        monkeypatch.setenv("TRR_STAGE6_SYNC_ENABLED", "1")

        captured: list[tuple[str, str, str | None]] = []

        monkeypatch.setattr(
            "trr_backend.repositories.screenalytics_runs.list_result_sync_candidates",
            lambda show_ids: [
                {
                    "run": {
                        "id": "run-1",
                        "video_asset_id": "asset-1",
                        "result_contract_version": "trr-screenalytics/v1",
                    },
                    "artifacts": [],
                    "person_metrics": [],
                    "leaderboard": [],
                    "unknown_clusters": [],
                }
            ],
        )
        monkeypatch.setattr(
            "trr_backend.repositories.screenalytics_runs.mark_result_ingest_status",
            lambda run_id, *, status, error=None: captured.append((run_id, status, error)),
        )

        context = RunContext(
            run_id=uuid4(),
            config=RunConfig(verbose=False),
            db=MagicMock(),
            show_ids=[str(uuid4())],
        )

        result = run(context)

        assert result.status == StageStatus.FAILED
        assert result.items_failed == 1
        assert "incomplete result bundle" in (result.error_message or "")
        assert captured == [("run-1", "failed", result.error_message)]
