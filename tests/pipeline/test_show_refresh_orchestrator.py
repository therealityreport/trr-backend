"""Tests for show_refresh_orchestrator — dependency graph, wave computation, and dispatch."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trr_backend.pipeline.show_refresh_orchestrator import (
    TARGET_DEPENDENCY_GRAPH,
    ShowRefreshOrchestrator,
    execution_waves,
)


# ---------------------------------------------------------------------------
# TARGET_DEPENDENCY_GRAPH structure tests
# ---------------------------------------------------------------------------


class TestTargetDependencyGraph:
    def test_show_core_has_no_dependencies(self):
        assert TARGET_DEPENDENCY_GRAPH["show_core"] == []

    def test_links_depends_on_show_core(self):
        assert TARGET_DEPENDENCY_GRAPH["links"] == ["show_core"]

    def test_bravo_depends_on_show_core(self):
        assert TARGET_DEPENDENCY_GRAPH["bravo"] == ["show_core"]

    def test_cast_profiles_depends_on_show_core(self):
        assert TARGET_DEPENDENCY_GRAPH["cast_profiles"] == ["show_core"]

    def test_cast_media_depends_on_cast_profiles(self):
        assert TARGET_DEPENDENCY_GRAPH["cast_media"] == ["cast_profiles"]


# ---------------------------------------------------------------------------
# execution_waves tests
# ---------------------------------------------------------------------------


class TestExecutionWaves:
    def test_full_five_targets_produce_three_waves(self):
        targets = ["show_core", "links", "bravo", "cast_profiles", "cast_media"]
        waves = execution_waves(targets)
        assert len(waves) == 3
        assert waves[0] == ["show_core"]
        assert waves[1] == ["bravo", "cast_profiles", "links"]  # sorted alphabetically
        assert waves[2] == ["cast_media"]

    def test_single_target_produces_one_wave(self):
        waves = execution_waves(["show_core"])
        assert waves == [["show_core"]]

    def test_subset_without_cast_media_produces_two_waves(self):
        targets = ["show_core", "links", "bravo", "cast_profiles"]
        waves = execution_waves(targets)
        assert len(waves) == 2
        assert waves[0] == ["show_core"]
        assert waves[1] == ["bravo", "cast_profiles", "links"]

    def test_empty_targets_returns_empty(self):
        assert execution_waves([]) == []

    def test_cast_media_alone_is_one_wave(self):
        """cast_media's dep (cast_profiles) is not in the set, so treated as satisfied."""
        waves = execution_waves(["cast_media"])
        assert waves == [["cast_media"]]

    def test_links_alone_is_one_wave(self):
        """links depends on show_core which is absent — treated as satisfied."""
        waves = execution_waves(["links"])
        assert waves == [["links"]]

    def test_cast_profiles_and_cast_media_produce_two_waves(self):
        waves = execution_waves(["cast_profiles", "cast_media"])
        assert len(waves) == 2
        assert waves[0] == ["cast_profiles"]
        assert waves[1] == ["cast_media"]

    def test_order_independence(self):
        """Input order should not affect wave grouping."""
        targets_a = ["cast_media", "bravo", "show_core", "links", "cast_profiles"]
        targets_b = ["show_core", "links", "bravo", "cast_profiles", "cast_media"]
        assert execution_waves(targets_a) == execution_waves(targets_b)


# ---------------------------------------------------------------------------
# ShowRefreshOrchestrator tests (mocked DB / Modal)
# ---------------------------------------------------------------------------


def _make_sub_op(target: str, op_id: int = 100) -> dict:
    return {
        "id": op_id,
        "refresh_target": target,
        "operation_type": "admin_show_refresh",
        "status": "pending",
    }


class TestShowRefreshOrchestratorCreateOperations:
    @patch("trr_backend.pipeline.show_refresh_orchestrator.admin_operations")
    def test_create_operations_creates_parent_and_children(self, mock_admin_ops):
        parent_op = {"id": 1, "operation_type": "admin_show_refresh", "status": "pending"}
        mock_admin_ops.create_or_attach_operation.return_value = (parent_op, False)
        mock_admin_ops.create_sub_operation.side_effect = [
            _make_sub_op("show_core", 10),
            _make_sub_op("links", 11),
        ]

        orch = ShowRefreshOrchestrator(
            show_id=42,
            targets=["show_core", "links"],
            initiated_by="test",
            request_payload={"show_id": 42},
        )
        parent_id, sub_ops = orch.create_operations()

        assert parent_id == "1"
        assert len(sub_ops) == 2
        assert mock_admin_ops.create_or_attach_operation.call_count == 1
        assert mock_admin_ops.create_sub_operation.call_count == 2

    @patch("trr_backend.pipeline.show_refresh_orchestrator.admin_operations")
    def test_create_operations_passes_correct_payload_per_target(self, mock_admin_ops):
        parent_op = {"id": 1, "operation_type": "admin_show_refresh", "status": "pending"}
        mock_admin_ops.create_or_attach_operation.return_value = (parent_op, False)
        mock_admin_ops.create_sub_operation.side_effect = [
            _make_sub_op("show_core", 10),
            _make_sub_op("bravo", 11),
        ]

        orch = ShowRefreshOrchestrator(
            show_id=42,
            targets=["show_core", "bravo"],
            request_payload={"show_id": 42},
        )
        orch.create_operations()

        calls = mock_admin_ops.create_sub_operation.call_args_list
        # First call: show_core
        assert calls[0].kwargs["refresh_target"] == "show_core"
        assert calls[0].kwargs["request_payload"]["targets"] == ["show_core"]
        # Second call: bravo
        assert calls[1].kwargs["refresh_target"] == "bravo"
        assert calls[1].kwargs["request_payload"]["targets"] == ["bravo"]


class TestShowRefreshOrchestratorDispatchWave:
    @patch("trr_backend.pipeline.show_refresh_orchestrator.is_remote_job_plane_enabled", return_value=True)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.supports_admin_operation", return_value=True)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.dispatch_admin_operation", return_value=True)
    def test_dispatch_wave_sends_to_modal_when_enabled(
        self, mock_dispatch, mock_supports, mock_remote
    ):
        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core", "links"])
        orch._parent_id = "1"
        sub_ops = [_make_sub_op("show_core", 10), _make_sub_op("links", 11)]

        count = orch.dispatch_wave(sub_ops)

        assert count == 2
        assert mock_dispatch.call_count == 2

    @patch("trr_backend.pipeline.show_refresh_orchestrator.is_remote_job_plane_enabled", return_value=False)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.supports_admin_operation", return_value=True)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.ensure_operation_execution")
    def test_dispatch_wave_falls_back_to_local_when_modal_disabled(
        self, mock_ensure, mock_supports, mock_remote
    ):
        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        orch._parent_id = "1"
        orch.request_id = "req-abc"
        sub_ops = [_make_sub_op("show_core", 10)]
        producer_factory = MagicMock(return_value="fake-producer")

        count = orch.dispatch_wave(sub_ops, producer_factory=producer_factory)

        assert count == 0
        producer_factory.assert_called_once_with(sub_ops[0])
        mock_ensure.assert_called_once_with("10", producer="fake-producer", request_id="req-abc")

    @patch("trr_backend.pipeline.show_refresh_orchestrator.is_remote_job_plane_enabled", return_value=False)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.supports_admin_operation", return_value=False)
    def test_dispatch_wave_warns_when_no_producer_and_no_modal(
        self, mock_supports, mock_remote
    ):
        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        orch._parent_id = "1"
        sub_ops = [_make_sub_op("show_core", 10)]

        count = orch.dispatch_wave(sub_ops)  # no producer_factory

        assert count == 0

    @patch("trr_backend.pipeline.show_refresh_orchestrator.is_remote_job_plane_enabled", return_value=True)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.supports_admin_operation", return_value=True)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.dispatch_admin_operation", return_value=False)
    @patch("trr_backend.pipeline.show_refresh_orchestrator.ensure_operation_execution")
    def test_dispatch_wave_falls_back_when_modal_dispatch_returns_false(
        self, mock_ensure, mock_dispatch, mock_supports, mock_remote
    ):
        """If Modal is enabled but dispatch_admin_operation returns False, fall back to local."""
        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        orch._parent_id = "1"
        sub_ops = [_make_sub_op("show_core", 10)]
        producer_factory = MagicMock(return_value="fake-producer")

        count = orch.dispatch_wave(sub_ops, producer_factory=producer_factory)

        assert count == 0
        producer_factory.assert_called_once()
        mock_ensure.assert_called_once()


class TestShowRefreshOrchestratorGetWaves:
    @patch("trr_backend.pipeline.show_refresh_orchestrator.admin_operations")
    def test_get_waves_groups_sub_ops_by_dependency(self, mock_admin_ops):
        parent_op = {"id": 1}
        mock_admin_ops.create_or_attach_operation.return_value = (parent_op, False)
        sub_ops_data = {
            "show_core": _make_sub_op("show_core", 10),
            "links": _make_sub_op("links", 11),
            "cast_profiles": _make_sub_op("cast_profiles", 12),
            "cast_media": _make_sub_op("cast_media", 13),
        }
        mock_admin_ops.create_sub_operation.side_effect = [
            sub_ops_data["show_core"],
            sub_ops_data["links"],
            sub_ops_data["cast_profiles"],
            sub_ops_data["cast_media"],
        ]

        orch = ShowRefreshOrchestrator(
            show_id=42,
            targets=["show_core", "links", "cast_profiles", "cast_media"],
        )
        orch.create_operations()
        waves = orch.get_waves()

        assert len(waves) == 3
        # Wave 0: show_core
        assert [op["refresh_target"] for op in waves[0]] == ["show_core"]
        # Wave 1: cast_profiles, links (sorted)
        assert sorted(op["refresh_target"] for op in waves[1]) == ["cast_profiles", "links"]
        # Wave 2: cast_media
        assert [op["refresh_target"] for op in waves[2]] == ["cast_media"]


class TestShowRefreshOrchestratorUpdateParentStatus:
    @patch("trr_backend.pipeline.show_refresh_orchestrator.admin_operations")
    def test_update_parent_status_persists_terminal_status(self, mock_admin_ops):
        mock_admin_ops.aggregate_parent_status.return_value = "completed"

        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        orch._parent_id = "1"

        status = orch.update_parent_status()

        assert status == "completed"
        mock_admin_ops.update_operation_status.assert_called_once_with("1", "completed")

    @patch("trr_backend.pipeline.show_refresh_orchestrator.admin_operations")
    def test_update_parent_status_does_not_persist_running_status(self, mock_admin_ops):
        mock_admin_ops.aggregate_parent_status.return_value = "running"

        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        orch._parent_id = "1"

        status = orch.update_parent_status()

        assert status == "running"
        mock_admin_ops.update_operation_status.assert_not_called()

    def test_update_parent_status_raises_without_parent(self):
        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        with pytest.raises(RuntimeError, match="No parent operation created yet"):
            orch.update_parent_status()

    @patch("trr_backend.pipeline.show_refresh_orchestrator.admin_operations")
    def test_update_parent_status_persists_failed(self, mock_admin_ops):
        mock_admin_ops.aggregate_parent_status.return_value = "failed"

        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        orch._parent_id = "1"

        status = orch.update_parent_status()

        assert status == "failed"
        mock_admin_ops.update_operation_status.assert_called_once_with("1", "failed")

    @patch("trr_backend.pipeline.show_refresh_orchestrator.admin_operations")
    def test_update_parent_status_persists_cancelled(self, mock_admin_ops):
        mock_admin_ops.aggregate_parent_status.return_value = "cancelled"

        orch = ShowRefreshOrchestrator(show_id=42, targets=["show_core"])
        orch._parent_id = "1"

        status = orch.update_parent_status()

        assert status == "cancelled"
        mock_admin_ops.update_operation_status.assert_called_once_with("1", "cancelled")
