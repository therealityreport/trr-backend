from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import pytest

import trr_backend.repositories.admin_operations as admin_operations


# ---------------------------------------------------------------------------
# create_sub_operation
# ---------------------------------------------------------------------------


def test_create_sub_operation_returns_child_linked_to_parent() -> None:
    parent_id = str(uuid4())
    child_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "id": child_id,
            "operation_type": "admin_show_sync_sub",
            "status": "pending",
            "initiated_by": "admin-1",
            "request_id": "req-1",
            "client_session_id": None,
            "client_workflow_id": None,
            "request_payload": {},
            "progress_payload": {},
            "result_payload": None,
            "error_payload": None,
            "cancel_requested_at": None,
            "claimed_by_worker_id": None,
            "claim_token": None,
            "lease_expires_at": None,
            "heartbeat_at": None,
            "attempt_count": 0,
            "next_retry_at": None,
            "parent_operation_id": parent_id,
            "refresh_target": "show-42",
            "started_at": None,
            "completed_at": None,
            "created_at": "2026-04-07T00:00:00Z",
            "updated_at": "2026-04-07T00:00:00Z",
        },
    ) as fetch_one:
        result = admin_operations.create_sub_operation(
            parent_operation_id=parent_id,
            operation_type="admin_show_sync_sub",
            refresh_target="show-42",
            initiated_by="admin-1",
            request_id="req-1",
        )

    assert result["id"] == child_id
    assert result["parent_operation_id"] == parent_id
    assert result["refresh_target"] == "show-42"
    assert result["status"] == "pending"
    assert fetch_one.call_count == 1


def test_create_sub_operation_rejects_empty_parent_operation_id() -> None:
    with pytest.raises(ValueError, match="parent_operation_id is required"):
        admin_operations.create_sub_operation(
            parent_operation_id="   ",
            operation_type="admin_show_sync_sub",
            refresh_target="show-42",
        )


def test_create_sub_operation_rejects_missing_parent_operation_id() -> None:
    with pytest.raises(ValueError, match="parent_operation_id is required"):
        admin_operations.create_sub_operation(
            parent_operation_id="",
            operation_type="admin_show_sync_sub",
            refresh_target="show-42",
        )


def test_create_sub_operation_rejects_empty_refresh_target() -> None:
    parent_id = str(uuid4())
    with pytest.raises(ValueError, match="refresh_target is required"):
        admin_operations.create_sub_operation(
            parent_operation_id=parent_id,
            operation_type="admin_show_sync_sub",
            refresh_target="",
        )


def test_create_sub_operation_raises_on_db_failure() -> None:
    parent_id = str(uuid4())

    with patch.object(admin_operations.pg, "fetch_one", return_value=None):
        with pytest.raises(RuntimeError, match="Failed to create sub-operation"):
            admin_operations.create_sub_operation(
                parent_operation_id=parent_id,
                operation_type="admin_show_sync_sub",
                refresh_target="show-42",
            )


# ---------------------------------------------------------------------------
# get_sub_operations
# ---------------------------------------------------------------------------


def test_get_sub_operations_returns_children_for_parent() -> None:
    parent_id = str(uuid4())
    child_id_1 = str(uuid4())
    child_id_2 = str(uuid4())

    fake_rows = [
        {
            "id": child_id_1,
            "operation_type": "admin_show_sync_sub",
            "status": "completed",
            "initiated_by": None,
            "request_id": None,
            "client_session_id": None,
            "client_workflow_id": None,
            "request_payload": {},
            "progress_payload": {},
            "result_payload": None,
            "error_payload": None,
            "cancel_requested_at": None,
            "claimed_by_worker_id": None,
            "claim_token": None,
            "lease_expires_at": None,
            "heartbeat_at": None,
            "attempt_count": 1,
            "next_retry_at": None,
            "parent_operation_id": parent_id,
            "refresh_target": "show-1",
            "started_at": None,
            "completed_at": None,
            "created_at": "2026-04-07T00:00:00Z",
            "updated_at": "2026-04-07T00:00:01Z",
        },
        {
            "id": child_id_2,
            "operation_type": "admin_show_sync_sub",
            "status": "pending",
            "initiated_by": None,
            "request_id": None,
            "client_session_id": None,
            "client_workflow_id": None,
            "request_payload": {},
            "progress_payload": {},
            "result_payload": None,
            "error_payload": None,
            "cancel_requested_at": None,
            "claimed_by_worker_id": None,
            "claim_token": None,
            "lease_expires_at": None,
            "heartbeat_at": None,
            "attempt_count": 0,
            "next_retry_at": None,
            "parent_operation_id": parent_id,
            "refresh_target": "show-2",
            "started_at": None,
            "completed_at": None,
            "created_at": "2026-04-07T00:00:01Z",
            "updated_at": "2026-04-07T00:00:01Z",
        },
    ]

    with patch.object(admin_operations.pg, "fetch_all", return_value=fake_rows):
        results = admin_operations.get_sub_operations(parent_id)

    assert len(results) == 2
    assert results[0]["id"] == child_id_1
    assert results[0]["refresh_target"] == "show-1"
    assert results[1]["id"] == child_id_2
    assert results[1]["refresh_target"] == "show-2"


def test_get_sub_operations_returns_empty_list_when_no_children() -> None:
    parent_id = str(uuid4())

    with patch.object(admin_operations.pg, "fetch_all", return_value=[]):
        results = admin_operations.get_sub_operations(parent_id)

    assert results == []


def test_get_sub_operations_returns_empty_list_when_db_returns_none() -> None:
    parent_id = str(uuid4())

    with patch.object(admin_operations.pg, "fetch_all", return_value=None):
        results = admin_operations.get_sub_operations(parent_id)

    assert results == []


# ---------------------------------------------------------------------------
# aggregate_parent_status
# ---------------------------------------------------------------------------


def test_aggregate_parent_status_returns_completed_when_all_children_completed() -> None:
    parent_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "failed_count": 0,
            "active_count": 0,
            "pending_count": 0,
            "completed_count": 3,
            "total_count": 3,
        },
    ):
        status = admin_operations.aggregate_parent_status(parent_id)

    assert status == "completed"


def test_aggregate_parent_status_returns_failed_when_any_child_failed() -> None:
    parent_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "failed_count": 1,
            "active_count": 0,
            "pending_count": 0,
            "completed_count": 2,
            "total_count": 3,
        },
    ):
        status = admin_operations.aggregate_parent_status(parent_id)

    assert status == "failed"


def test_aggregate_parent_status_returns_running_when_any_child_running() -> None:
    parent_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "failed_count": 0,
            "active_count": 1,
            "pending_count": 1,
            "completed_count": 1,
            "total_count": 3,
        },
    ):
        status = admin_operations.aggregate_parent_status(parent_id)

    assert status == "running"


def test_aggregate_parent_status_returns_pending_when_no_children() -> None:
    parent_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "failed_count": 0,
            "active_count": 0,
            "pending_count": 0,
            "completed_count": 0,
            "total_count": 0,
        },
    ):
        status = admin_operations.aggregate_parent_status(parent_id)

    assert status == "pending"


def test_aggregate_parent_status_returns_pending_when_db_returns_none() -> None:
    parent_id = str(uuid4())

    with patch.object(admin_operations.pg, "fetch_one", return_value=None):
        status = admin_operations.aggregate_parent_status(parent_id)

    assert status == "pending"


def test_aggregate_parent_status_failed_takes_priority_over_running() -> None:
    parent_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "failed_count": 1,
            "cancelled_count": 0,
            "active_count": 1,
            "pending_count": 0,
            "completed_count": 1,
            "total_count": 3,
        },
    ):
        status = admin_operations.aggregate_parent_status(parent_id)

    assert status == "failed"


def test_aggregate_parent_status_returns_cancelled_when_any_child_cancelled() -> None:
    parent_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "failed_count": 0,
            "cancelled_count": 1,
            "active_count": 0,
            "pending_count": 0,
            "completed_count": 2,
            "total_count": 3,
        },
    ):
        status = admin_operations.aggregate_parent_status(parent_id)

    assert status == "cancelled"
