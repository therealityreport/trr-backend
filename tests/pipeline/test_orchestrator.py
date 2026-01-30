"""Tests for pipeline orchestrator, specifically resume and skip logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from trr_backend.pipeline.models import RunConfig
from trr_backend.pipeline.orchestrator import PipelineOrchestrator


class TestShouldSkipStage:
    """Tests for _should_skip_stage logic."""

    def _make_orchestrator(self) -> PipelineOrchestrator:
        """Create an orchestrator with no stages (for testing _should_skip_stage)."""
        return PipelineOrchestrator(stages=[])

    def _make_config(
        self,
        *,
        force: bool = False,
        skip_s3: bool = False,
    ) -> RunConfig:
        """Create a RunConfig for testing."""
        return RunConfig(
            force=force,
            skip_s3=skip_s3,
        )

    def test_skip_when_all_conditions_met(self):
        """Should skip when: success, hash matches, not forced, manifest exists."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config()

        with patch(
            "trr_backend.pipeline.orchestrator.get_stage_prior_state",
            return_value={
                "status": "success",
                "input_hash": "abc123",
                "manifest_key": "pipeline_runs/xxx/02_resolve/manifest.json",
            },
        ):
            result = orchestrator._should_skip_stage(db, run_id, "02_resolve", "abc123", config)

        assert result is True

    def test_no_skip_when_forced(self):
        """Should NOT skip when --force is set."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config(force=True)

        # Don't even need to mock get_stage_prior_state - force check comes first
        result = orchestrator._should_skip_stage(db, run_id, "02_resolve", "abc123", config)

        assert result is False

    def test_no_skip_when_no_prior_state(self):
        """Should NOT skip when stage has no prior state."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config()

        with patch("trr_backend.pipeline.orchestrator.get_stage_prior_state", return_value=None):
            result = orchestrator._should_skip_stage(db, run_id, "02_resolve", "abc123", config)

        assert result is False

    def test_no_skip_when_prior_status_not_success(self):
        """Should NOT skip when prior status is not 'success'."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config()

        for status in ["pending", "running", "failed", "skipped"]:
            with patch(
                "trr_backend.pipeline.orchestrator.get_stage_prior_state",
                return_value={
                    "status": status,
                    "input_hash": "abc123",
                    "manifest_key": "some/key",
                },
            ):
                result = orchestrator._should_skip_stage(db, run_id, "02_resolve", "abc123", config)

            assert result is False, f"Should not skip when status is {status}"

    def test_no_skip_when_hash_mismatch(self):
        """Should NOT skip when input hash doesn't match."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config()

        with patch(
            "trr_backend.pipeline.orchestrator.get_stage_prior_state",
            return_value={
                "status": "success",
                "input_hash": "old_hash",
                "manifest_key": "some/key",
            },
        ):
            result = orchestrator._should_skip_stage(db, run_id, "02_resolve", "new_hash", config)

        assert result is False

    def test_stage1_requires_manifest_key_to_skip(self):
        """Stage 1 ALWAYS requires manifest_key to skip (for hydration)."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config(skip_s3=True)  # Even with skip_s3!

        with patch(
            "trr_backend.pipeline.orchestrator.get_stage_prior_state",
            return_value={
                "status": "success",
                "input_hash": "abc123",
                "manifest_key": None,  # No manifest!
            },
        ):
            result = orchestrator._should_skip_stage(db, run_id, "01_collect", "abc123", config)

        assert result is False, "Stage 1 should not skip without manifest_key"

    def test_stage1_skips_with_manifest_key(self):
        """Stage 1 CAN skip when manifest_key exists."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config()

        with patch(
            "trr_backend.pipeline.orchestrator.get_stage_prior_state",
            return_value={
                "status": "success",
                "input_hash": "abc123",
                "manifest_key": "pipeline_runs/xxx/01_collect/manifest.json",
            },
        ):
            result = orchestrator._should_skip_stage(db, run_id, "01_collect", "abc123", config)

        assert result is True

    def test_other_stages_require_manifest_when_skip_s3_false(self):
        """Stages 2+ require manifest_key when skip_s3=False."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config(skip_s3=False)

        with patch(
            "trr_backend.pipeline.orchestrator.get_stage_prior_state",
            return_value={
                "status": "success",
                "input_hash": "abc123",
                "manifest_key": None,  # No manifest
            },
        ):
            result = orchestrator._should_skip_stage(db, run_id, "02_resolve", "abc123", config)

        assert result is False, "Stage 2+ should not skip without manifest when skip_s3=False"

    def test_other_stages_skip_without_manifest_when_skip_s3_true(self):
        """Stages 2+ CAN skip without manifest_key when skip_s3=True."""
        orchestrator = self._make_orchestrator()
        db = MagicMock()
        run_id = uuid4()
        config = self._make_config(skip_s3=True)

        with patch(
            "trr_backend.pipeline.orchestrator.get_stage_prior_state",
            return_value={
                "status": "success",
                "input_hash": "abc123",
                "manifest_key": None,  # No manifest, but that's OK with skip_s3
            },
        ):
            result = orchestrator._should_skip_stage(db, run_id, "02_resolve", "abc123", config)

        assert result is True, "Stage 2+ should skip without manifest when skip_s3=True"


class TestEnsureRunStages:
    """Tests for ensure_run_stages upsert behavior."""

    def test_ensure_run_stages_upserts_missing(self):
        """ensure_run_stages should create missing stage rows on resume with extended --to."""
        from trr_backend.pipeline.repository import ensure_run_stages

        db = MagicMock()
        run_id = uuid4()
        stage_names = ["01_collect", "02_resolve", "03_enrich"]

        ensure_run_stages(db, run_id, stage_names)

        # Should have called upsert for each stage
        assert db.schema.call_count == 3
        assert db.schema().table().upsert.call_count == 3


class TestManifestHydration:
    """Tests for context hydration from manifest when Stage 1 is skipped."""

    def test_read_manifest_returns_stage_manifest(self):
        """read_manifest should return StageManifest with show_ids."""
        from trr_backend.pipeline.manifests import StageManifest

        manifest = StageManifest(
            run_id="test-run-id",
            stage_name="01_collect",
            timestamp="2024-01-28T12:00:00Z",
            input_hash="abc123",
            show_ids=["id1", "id2", "id3"],
        )

        assert manifest.show_ids == ["id1", "id2", "id3"]
        assert manifest.run_id == "test-run-id"
        assert manifest.stage_name == "01_collect"

    def test_manifest_to_dict_includes_show_ids(self):
        """StageManifest.to_dict() should include show_ids for JSON serialization."""
        from trr_backend.pipeline.manifests import StageManifest

        manifest = StageManifest(
            run_id="test-run-id",
            stage_name="01_collect",
            timestamp="2024-01-28T12:00:00Z",
            input_hash="abc123",
            show_ids=["id1", "id2"],
        )

        data = manifest.to_dict()

        assert "show_ids" in data
        assert data["show_ids"] == ["id1", "id2"]


class TestManifestErrorHandling:
    """Tests for manifest read error handling."""

    def test_read_manifest_returns_none_on_missing(self):
        """read_manifest should return None when manifest doesn't exist (not raise)."""
        from botocore.exceptions import ClientError

        from trr_backend.pipeline.manifests import read_manifest

        mock_error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
        error = ClientError(mock_error_response, "GetObject")

        with patch("trr_backend.pipeline.manifests.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_s3.get_object.side_effect = error
            mock_boto.return_value = mock_s3

            with patch("trr_backend.pipeline.manifests.get_manifest_bucket", return_value="test-bucket"):
                result = read_manifest("test-run-id", "01_collect")

        assert result is None

    def test_read_manifest_returns_none_on_404(self):
        """read_manifest should return None on 404 error code."""
        from botocore.exceptions import ClientError

        from trr_backend.pipeline.manifests import read_manifest

        mock_error_response = {"Error": {"Code": "404", "Message": "Not found"}}
        error = ClientError(mock_error_response, "GetObject")

        with patch("trr_backend.pipeline.manifests.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_s3.get_object.side_effect = error
            mock_boto.return_value = mock_s3

            with patch("trr_backend.pipeline.manifests.get_manifest_bucket", return_value="test-bucket"):
                result = read_manifest("test-run-id", "01_collect")

        assert result is None

    def test_read_manifest_raises_on_other_errors(self):
        """read_manifest should re-raise non-404/NoSuchKey errors."""
        from botocore.exceptions import ClientError

        from trr_backend.pipeline.manifests import read_manifest

        mock_error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        error = ClientError(mock_error_response, "GetObject")

        with patch("trr_backend.pipeline.manifests.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_s3.get_object.side_effect = error
            mock_boto.return_value = mock_s3

            with patch("trr_backend.pipeline.manifests.get_manifest_bucket", return_value="test-bucket"):
                with pytest.raises(ClientError):
                    read_manifest("test-run-id", "01_collect")
