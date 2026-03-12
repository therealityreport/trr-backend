from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import trr_backend.repositories.admin_operations as admin_operations


def test_create_or_attach_operation_attaches_for_same_session_and_workflow() -> None:
    operation_id = str(uuid4())

    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "id": operation_id,
            "operation_type": "admin_show_sync",
            "status": "running",
            "client_session_id": "tab-1",
            "client_workflow_id": "flow-1",
        },
    ) as fetch_one:
        operation, attached = admin_operations.create_or_attach_operation(
            operation_type="admin_show_sync",
            request_payload={"show_id": str(uuid4())},
            initiated_by="admin-1",
            request_id="req-1",
            client_session_id="tab-1",
            client_workflow_id="flow-1",
            allow_attach=True,
        )

    assert attached is True
    assert operation["id"] == operation_id
    assert operation["status"] == "running"
    assert fetch_one.call_count == 1


def test_create_or_attach_operation_creates_when_no_active_match() -> None:
    operation_id = str(uuid4())
    fetch_calls: list[tuple[str, list[object]]] = []

    def _fake_fetch_one(query: str, params: list[object]):
        fetch_calls.append((query, list(params)))
        if "insert into core.admin_operations" in query:
            return {
                "id": operation_id,
                "operation_type": "admin_show_sync",
                "status": "pending",
                "client_session_id": "tab-2",
                "client_workflow_id": "flow-2",
            }
        return None

    with patch.object(admin_operations.pg, "fetch_one", side_effect=_fake_fetch_one):
        operation, attached = admin_operations.create_or_attach_operation(
            operation_type="admin_show_sync",
            request_payload={"show_id": str(uuid4())},
            initiated_by="admin-2",
            request_id="req-2",
            client_session_id="tab-2",
            client_workflow_id="flow-2",
            allow_attach=True,
        )

    assert attached is False
    assert operation["id"] == operation_id
    assert operation["status"] == "pending"
    assert any("insert into core.admin_operations" in call[0] for call in fetch_calls)


def test_append_operation_event_sequence_is_monotonic() -> None:
    operation_id = str(uuid4())
    captured_calls: list[tuple[str, list[object]]] = []
    rows = [
        {
            "operation_id": operation_id,
            "event_seq": 10,
            "event_type": "progress",
            "event_payload": {"stage": "one"},
            "created_at": "2026-03-03T00:00:00Z",
        },
        {
            "operation_id": operation_id,
            "event_seq": 11,
            "event_type": "complete",
            "event_payload": {"stage": "done"},
            "created_at": "2026-03-03T00:00:01Z",
        },
    ]

    def _fake_fetch_one(query: str, params: list[object]):
        captured_calls.append((query, list(params)))
        return rows[len(captured_calls) - 1]

    with patch.object(admin_operations.pg, "fetch_one", side_effect=_fake_fetch_one):
        first = admin_operations.append_operation_event(
            operation_id,
            event_type="progress",
            event_payload={"stage": "one"},
        )
        second = admin_operations.append_operation_event(
            operation_id,
            event_type="complete",
            event_payload={"stage": "done"},
        )

    assert int(first["event_seq"]) == 10
    assert int(second["event_seq"]) == 11
    assert int(second["event_seq"]) > int(first["event_seq"])
    assert captured_calls
    assert "for update" in captured_calls[0][0].lower()
    assert "cross join next_event" in captured_calls[0][0].lower()
    assert captured_calls[0][1][:2] == [operation_id, operation_id]


def test_append_operation_event_preserves_explicit_event_seq() -> None:
    operation_id = str(uuid4())

    def _fake_fetch_one(query: str, params: list[object]):
        assert "for update" in query.lower()
        assert params[0] == operation_id
        assert params[1] == operation_id
        assert params[2] == 42
        return {
            "operation_id": operation_id,
            "event_seq": 42,
            "event_type": "progress",
            "event_payload": {"stage": "custom"},
            "created_at": "2026-03-03T00:00:00Z",
        }

    with patch.object(admin_operations.pg, "fetch_one", side_effect=_fake_fetch_one):
        row = admin_operations.append_operation_event(
            operation_id,
            event_type="progress",
            event_payload={"stage": "custom"},
            event_seq=42,
        )

    assert int(row["event_seq"]) == 42


def test_stream_events_after_seq_orders_and_clamps_cursor() -> None:
    operation_id = str(uuid4())

    def _fake_fetch_all(query: str, params: list[object]):
        assert "order by event_seq asc" in query
        assert params[0] == operation_id
        assert params[1] == 0
        assert params[2] == 1000
        return [
            {
                "operation_id": operation_id,
                "event_seq": 5,
                "event_type": "progress",
                "event_payload": {"stage": "one"},
                "created_at": "2026-03-03T00:00:00Z",
            },
            {
                "operation_id": operation_id,
                "event_seq": 6,
                "event_type": "complete",
                "event_payload": {"stage": "done"},
                "created_at": "2026-03-03T00:00:01Z",
            },
        ]

    with patch.object(admin_operations.pg, "fetch_all", side_effect=_fake_fetch_all):
        rows = admin_operations.stream_events_after_seq(operation_id, after_seq=-7, limit=5000)

    assert [int(row["event_seq"]) for row in rows] == [5, 6]


def test_update_operation_status_supports_terminal_transitions() -> None:
    operation_id = str(uuid4())

    def _fake_fetch_one(_query: str, params: list[object]):
        return {
            "id": operation_id,
            "operation_type": "admin_show_sync",
            "status": params[0],
            "request_payload": {},
            "progress_payload": {},
            "result_payload": {},
            "error_payload": {},
        }

    with patch.object(admin_operations.pg, "fetch_one", side_effect=_fake_fetch_one):
        completed = admin_operations.update_operation_status(operation_id, status="completed")
        failed = admin_operations.update_operation_status(operation_id, status="failed")
        cancelled = admin_operations.update_operation_status(operation_id, status="cancelled")

    assert completed and completed["status"] == "completed"
    assert failed and failed["status"] == "failed"
    assert cancelled and cancelled["status"] == "cancelled"
    assert admin_operations.operation_is_terminal(completed["status"]) is True
    assert admin_operations.operation_is_terminal(failed["status"]) is True
    assert admin_operations.operation_is_terminal(cancelled["status"]) is True


def test_claim_next_operation_returns_claimed_row_with_worker_metadata() -> None:
    operation_id = str(uuid4())
    claim_token = str(uuid4())
    with patch.object(
        admin_operations.pg,
        "fetch_one",
        return_value={
            "id": operation_id,
            "operation_type": "admin_asset_batch_jobs",
            "status": "running",
            "claimed_by_worker_id": "worker-1",
            "claim_token": claim_token,
            "attempt_count": 1,
        },
    ):
        claimed = admin_operations.claim_next_operation("worker-1", lease_seconds=120)

    assert claimed is not None
    assert claimed["id"] == operation_id
    assert claimed["claimed_by_worker_id"] == "worker-1"
    assert claimed["claim_token"] == claim_token
    assert int(claimed["attempt_count"]) == 1


def test_claim_next_operation_supports_excluded_operation_types() -> None:
    operation_id = str(uuid4())
    captured: dict[str, object] = {}

    def _fake_fetch_one(query: str, params: list[object]):
        captured["query"] = query
        captured["params"] = list(params)
        return {
            "id": operation_id,
            "operation_type": "admin_asset_batch_jobs",
            "status": "running",
            "claimed_by_worker_id": "worker-1",
            "claim_token": str(uuid4()),
            "attempt_count": 1,
        }

    with patch.object(admin_operations.pg, "fetch_one", side_effect=_fake_fetch_one):
        claimed = admin_operations.claim_next_operation(
            "worker-1",
            lease_seconds=120,
            exclude_operation_types=["admin_show_refresh", "admin_show_refresh_photos"],
        )

    assert claimed is not None
    assert "not (operation_type = any" in str(captured["query"]).lower()
    assert captured["params"][0] == ["admin_show_refresh", "admin_show_refresh_photos"]


def test_release_operation_claim_returns_true_on_update() -> None:
    with patch.object(admin_operations.pg, "fetch_one", return_value={"id": str(uuid4())}):
        released = admin_operations.release_operation_claim(str(uuid4()), claim_token=str(uuid4()))
    assert released is True
