from __future__ import annotations

import asyncio
import json
from unittest.mock import call, patch

from trr_backend.pipeline import admin_operations as admin_ops_pipeline


def _decode_sse_payload(chunk: str) -> tuple[str, dict]:
    event_type = "message"
    payload: dict = {}
    for line in chunk.strip().splitlines():
        if line.startswith("event:"):
            event_type = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            payload = json.loads(line.split(":", 1)[1].strip())
    return event_type, payload


def test_finalize_sub_operation_keeps_parent_running_while_sibling_active() -> None:
    with (
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_operation",
            return_value={"id": "child-1", "parent_operation_id": "parent-1", "refresh_target": "show_core"},
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "aggregate_parent_status",
            return_value="running",
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "update_operation_status",
        ) as update_status,
        patch.object(
            admin_ops_pipeline.admin_operations,
            "append_operation_event",
        ) as append_event,
    ):
        result = admin_ops_pipeline.finalize_sub_operation("child-1", "failed")

    assert result == "running"
    update_status.assert_called_once_with("child-1", status="failed")
    append_event.assert_not_called()


def test_finalize_sub_operation_marks_parent_failed_after_all_children_finish() -> None:
    with (
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_operation",
            return_value={"id": "child-1", "parent_operation_id": "parent-1", "refresh_target": "show_core"},
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "aggregate_parent_status",
            return_value="failed",
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_sub_operations",
            return_value=[
                {"refresh_target": "show_core", "status": "failed"},
                {"refresh_target": "links", "status": "completed"},
            ],
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "update_operation_status",
        ) as update_status,
        patch.object(
            admin_ops_pipeline.admin_operations,
            "append_operation_event",
        ) as append_event,
    ):
        result = admin_ops_pipeline.finalize_sub_operation("child-1", "failed")

    assert result == "failed"
    assert update_status.call_args_list == [
        call("child-1", status="failed"),
        call("parent-1", status="failed"),
    ]
    append_event.assert_called_once()
    event_kwargs = append_event.call_args.kwargs
    assert event_kwargs["event_type"] == "error"
    assert event_kwargs["event_payload"]["sub_operation_summary"] == {
        "show_core": "failed",
        "links": "completed",
    }


def test_finalize_sub_operation_uses_keyword_status_against_real_repo_signature() -> None:
    """autospec enforces the real keyword-only signature of
    update_operation_status: a positional status argument would raise
    TypeError here, for both the child and the parent update sites."""
    with (
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_operation",
            return_value={"id": "child-1", "parent_operation_id": "parent-1", "refresh_target": "show_core"},
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "aggregate_parent_status",
            return_value="completed",
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_sub_operations",
            return_value=[{"refresh_target": "show_core", "status": "completed"}],
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "update_operation_status",
            autospec=True,
        ) as update_status,
        patch.object(
            admin_ops_pipeline.admin_operations,
            "append_operation_event",
        ),
    ):
        result = admin_ops_pipeline.finalize_sub_operation("child-1", "completed")

    assert result == "completed"
    assert update_status.call_args_list == [
        call("child-1", status="completed"),
        call("parent-1", status="completed"),
    ]


def test_wait_for_sub_operation_dependencies_treats_missing_dependencies_as_satisfied() -> None:
    with (
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_operation",
            return_value={"id": "child-1", "parent_operation_id": "parent-1", "refresh_target": "links"},
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_sub_operations",
            return_value=[],
        ),
    ):
        result = admin_ops_pipeline.wait_for_sub_operation_dependencies(
            "child-1",
            poll_interval_seconds=0.0,
            timeout_seconds=0.01,
        )

    assert result is True


def test_wait_for_sub_operation_dependencies_returns_false_when_dependency_fails() -> None:
    with (
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_operation",
            return_value={"id": "child-1", "parent_operation_id": "parent-1", "refresh_target": "links"},
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_sub_operations",
            return_value=[{"refresh_target": "show_core", "status": "failed"}],
        ),
    ):
        result = admin_ops_pipeline.wait_for_sub_operation_dependencies(
            "child-1",
            poll_interval_seconds=0.0,
            timeout_seconds=0.01,
        )

    assert result is False


def test_wait_for_sub_operation_dependencies_returns_true_when_dependencies_complete() -> None:
    with (
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_operation",
            return_value={"id": "child-1", "parent_operation_id": "parent-1", "refresh_target": "links"},
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "get_sub_operations",
            return_value=[{"refresh_target": "show_core", "status": "completed"}],
        ),
    ):
        result = admin_ops_pipeline.wait_for_sub_operation_dependencies(
            "child-1",
            poll_interval_seconds=0.0,
            timeout_seconds=0.01,
        )

    assert result is True


def test_parent_operation_stream_generator_keeps_stream_open_until_all_children_finish(monkeypatch) -> None:
    async def _collect() -> list[str]:
        chunks: list[str] = []
        async for chunk in admin_ops_pipeline.parent_operation_stream_generator("parent-1"):
            chunks.append(chunk)
        return chunks

    async def _noop_sleep(_seconds: float) -> None:
        return None

    stream_batches = [
        [
            {
                "id": 1,
                "operation_id": "child-1",
                "event_type": "error",
                "event_payload": {"message": "child-1 failed"},
                "refresh_target": "show_core",
            }
        ],
        [
            {
                "id": 2,
                "operation_id": "child-2",
                "event_type": "progress",
                "event_payload": {"message": "child-2 still running"},
                "refresh_target": "links",
            }
        ],
        [],
        [
            {
                "id": 3,
                "operation_id": "child-2",
                "event_type": "complete",
                "event_payload": {"message": "child-2 done"},
                "refresh_target": "links",
            }
        ],
    ]
    aggregate_statuses = ["running", "running", "failed"]

    monkeypatch.setattr(admin_ops_pipeline.asyncio, "sleep", _noop_sleep)

    with (
        patch.object(
            admin_ops_pipeline.admin_operations,
            "stream_sub_operation_events_after_seq",
            side_effect=stream_batches,
        ),
        patch.object(
            admin_ops_pipeline.admin_operations,
            "aggregate_parent_status",
            side_effect=aggregate_statuses,
        ),
    ):
        chunks = asyncio.run(_collect())

    decoded = [_decode_sse_payload(chunk) for chunk in chunks]
    assert decoded[0][0] == "error"
    assert decoded[0][1]["sub_operation_id"] == "child-1"
    assert decoded[1][0] == "progress"
    assert decoded[1][1]["sub_operation_id"] == "child-2"
    assert decoded[2][0] == "complete"
    assert decoded[2][1]["sub_operation_id"] == "child-2"
    assert decoded[3] == ("error", {"operation_id": "parent-1", "status": "failed"})
