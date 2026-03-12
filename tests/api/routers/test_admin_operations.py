from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.pipeline import admin_operations as pipeline_admin_operations


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _parse_sse(raw_text: str) -> list[tuple[str, dict[str, object]]]:
    events: list[tuple[str, dict[str, object]]] = []
    normalized = raw_text.replace("\r\n", "\n")
    for block in normalized.split("\n\n"):
        if not block.strip():
            continue
        event_type = "message"
        data_lines: list[str] = []
        for line in block.split("\n"):
            if line.startswith("event:"):
                event_type = line.split(":", 1)[1].strip() or "message"
            elif line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].lstrip())
        payload = json.loads("\n".join(data_lines)) if data_lines else {}
        events.append((event_type, payload))
    return events


def test_get_operation_returns_operation_and_latest_event_seq(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    operation_id = str(uuid4())

    with patch(
        "api.routers.admin_operations.admin_operations.get_operation",
        return_value={"id": operation_id, "status": "running", "operation_type": "show_sync"},
    ):
        with patch(
            "api.routers.admin_operations.admin_operations.stream_events_after_seq",
            return_value=[
                {"event_seq": 6, "event_type": "progress", "event_payload": {"stage": "one"}},
                {"event_seq": 7, "event_type": "complete", "event_payload": {"stage": "done"}},
            ],
        ):
            response = client.get(
                f"/api/v1/admin/operations/{operation_id}",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["operation"]["id"] == operation_id
    assert payload["latest_event_seq"] == 7


def test_get_operation_returns_404_when_unknown(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("api.routers.admin_operations.admin_operations.get_operation", return_value=None):
        response = client.get(
            f"/api/v1/admin/operations/{uuid4()}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 404


def test_stream_operation_replays_after_seq_in_order(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    operation_id = str(uuid4())
    seen_after: list[int] = []

    operation_rows = [
        {"id": operation_id, "status": "running", "operation_type": "show_sync"},
        {"id": operation_id, "status": "completed", "operation_type": "show_sync"},
    ]

    def _fake_get_operation(_operation_id: str) -> dict[str, object]:
        if operation_rows:
            return operation_rows.pop(0)
        return {"id": operation_id, "status": "completed", "operation_type": "show_sync"}

    def _fake_stream_events(_operation_id: str, *, after_seq: int = 0, limit: int = 200):
        seen_after.append(after_seq)
        if after_seq >= 3:
            return []
        return [
            {
                "operation_id": operation_id,
                "event_seq": 2,
                "event_type": "progress",
                "event_payload": {"stage": "step_1"},
            },
            {
                "operation_id": operation_id,
                "event_seq": 3,
                "event_type": "complete",
                "event_payload": {"stage": "done"},
            },
        ]

    with patch("trr_backend.repositories.admin_operations.get_operation", side_effect=_fake_get_operation):
        with patch(
            "trr_backend.repositories.admin_operations.stream_events_after_seq",
            side_effect=_fake_stream_events,
        ):
            with client.stream(
                "GET",
                f"/api/v1/admin/operations/{operation_id}/stream?after_seq=1",
                headers={"Authorization": f"Bearer {token}"},
            ) as response:
                assert response.status_code == 200
                text = "\n".join(
                    line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                    for line in response.iter_lines()
                )

    parsed = _parse_sse(text)
    assert [event for event, _ in parsed] == ["progress", "complete"]
    seqs = [int(payload.get("event_seq") or 0) for _, payload in parsed]
    assert seqs == [2, 3]
    assert all(str(payload.get("operation_id") or "") == operation_id for _, payload in parsed)
    assert seen_after and seen_after[0] == 1


def test_cancel_operation_requests_cancellation_and_emits_event(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    operation_id = str(uuid4())

    with patch(
        "api.routers.admin_operations.admin_operations.request_operation_cancel",
        return_value={"id": operation_id, "status": "cancelling"},
    ):
        with patch(
            "api.routers.admin_operations.admin_operations.get_operation",
            return_value={"id": operation_id, "status": "running"},
        ):
            with patch(
                "api.routers.admin_operations.admin_operations.append_operation_event",
                return_value={
                    "operation_id": operation_id,
                    "event_seq": 9,
                    "event_type": "progress",
                    "event_payload": {},
                },
            ) as append_mock:
                response = client.post(
                    f"/api/v1/admin/operations/{operation_id}/cancel",
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["cancel_requested"] is True
    assert payload["operation"]["status"] == "cancelling"

    _, kwargs = append_mock.call_args
    assert kwargs["event_type"] == "progress"
    assert kwargs["event_payload"]["operation_id"] == operation_id
    assert kwargs["event_payload"]["cancel_requested"] is True


def test_start_operation_emits_operation_envelope_as_first_replayed_event() -> None:
    operation_id = str(uuid4())
    stored_events: list[dict[str, object]] = []

    def _append_event(
        _operation_id: str,
        *,
        event_type: str,
        event_payload: dict[str, object] | None = None,
        event_seq: int | None = None,
    ) -> dict[str, object]:
        seq = int(event_seq or (len(stored_events) + 1))
        row = {
            "operation_id": operation_id,
            "event_seq": seq,
            "event_type": event_type,
            "event_payload": dict(event_payload or {}),
        }
        stored_events.append(row)
        return row

    def _stream_events(_operation_id: str, *, after_seq: int = 0, limit: int = 500):
        return [row for row in stored_events if int(row["event_seq"]) > after_seq][:limit]

    async def _read_stream_once() -> list[tuple[str, dict[str, object]]]:
        raw_chunks: list[str] = []
        async for chunk in pipeline_admin_operations.operation_stream_generator(
            operation_id,
            after_seq=0,
            request=None,
        ):
            raw_chunks.append(chunk)
        return _parse_sse("".join(raw_chunks))

    with patch(
        "trr_backend.repositories.admin_operations.create_or_attach_operation",
        return_value=({"id": operation_id, "status": "pending", "operation_type": "show_sync"}, False),
    ):
        with patch(
            "trr_backend.repositories.admin_operations.get_operation",
            return_value={"id": operation_id, "status": "completed"},
        ):
            with patch("trr_backend.repositories.admin_operations.append_operation_event", side_effect=_append_event):
                with patch(
                    "trr_backend.repositories.admin_operations.stream_events_after_seq",
                    side_effect=_stream_events,
                ):
                    with patch("trr_backend.pipeline.admin_operations.ensure_operation_execution", return_value=True):
                        pipeline_admin_operations.start_operation_for_stream(
                            operation_type="show_sync",
                            producer=lambda: [],
                            request_payload={"show_id": str(uuid4())},
                            initiated_by="admin-1",
                            request=None,
                        )
                        replayed = asyncio.run(_read_stream_once())

    assert replayed
    first_type, first_payload = replayed[0]
    assert first_type == "operation"
    assert {"operation_id", "status", "attached", "request_id", "event_seq"}.issubset(first_payload.keys())
    assert first_payload["operation_id"] == operation_id
    assert int(first_payload["event_seq"]) == 1


def test_start_operation_remote_mode_does_not_execute_in_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    operation_id = str(uuid4())
    appended_payloads: list[dict[str, object]] = []

    def _append_event(
        _operation_id: str,
        *,
        event_type: str,
        event_payload: dict[str, object] | None = None,
        event_seq: int | None = None,
    ) -> dict[str, object]:
        row = {
            "operation_id": operation_id,
            "event_seq": int(event_seq or 1),
            "event_type": event_type,
            "event_payload": dict(event_payload or {}),
        }
        appended_payloads.append(row["event_payload"])
        return row

    with patch(
        "trr_backend.repositories.admin_operations.create_or_attach_operation",
        return_value=({"id": operation_id, "status": "pending", "operation_type": "admin_asset_batch_jobs"}, False),
    ):
        with patch(
            "trr_backend.repositories.admin_operations.get_operation",
            return_value={"id": operation_id, "status": "pending"},
        ):
            with patch("trr_backend.repositories.admin_operations.append_operation_event", side_effect=_append_event):
                with patch("trr_backend.pipeline.admin_operations.ensure_operation_execution") as ensure_mock:
                    response = pipeline_admin_operations.start_operation_for_stream(
                        operation_type="admin_asset_batch_jobs",
                        producer=lambda: [],
                        request_payload={"show_id": str(uuid4())},
                        initiated_by="admin-remote",
                        request=None,
                    )

    assert ensure_mock.called is False
    assert response["execution_owner"] == "remote_worker"
    assert response["execution_mode_canonical"] == "remote"
    assert appended_payloads
    assert appended_payloads[0]["execution_owner"] == "remote_worker"


def test_start_operation_remote_mode_dispatches_supported_show_refresh_to_modal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    operation_id = str(uuid4())

    with patch(
        "trr_backend.repositories.admin_operations.create_or_attach_operation",
        return_value=({"id": operation_id, "status": "pending", "operation_type": "admin_show_refresh"}, False),
    ):
        with patch(
            "trr_backend.repositories.admin_operations.get_operation",
            return_value={"id": operation_id, "status": "pending"},
        ):
            with patch(
                "trr_backend.repositories.admin_operations.append_operation_event",
                return_value={
                    "operation_id": operation_id,
                    "event_seq": 1,
                    "event_type": "operation",
                    "event_payload": {},
                },
            ):
                with patch("trr_backend.pipeline.admin_operations.ensure_operation_execution") as ensure_mock:
                    with patch(
                        "trr_backend.pipeline.admin_operations.dispatch_admin_operation",
                        return_value=True,
                    ) as dispatch_mock:
                        response = pipeline_admin_operations.start_operation_for_stream(
                            operation_type="admin_show_refresh",
                            producer=lambda: [],
                            request_payload={"show_id": str(uuid4())},
                            initiated_by="admin-modal",
                            request=None,
                        )

    ensure_mock.assert_not_called()
    dispatch_mock.assert_called_once_with(
        operation_id=operation_id,
        operation_type="admin_show_refresh",
    )
    assert response["execution_owner"] == "remote_worker"


def test_start_operation_local_mode_dispatches_supported_show_refresh_to_modal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    operation_id = str(uuid4())

    with patch(
        "trr_backend.repositories.admin_operations.create_or_attach_operation",
        return_value=({"id": operation_id, "status": "pending", "operation_type": "admin_show_refresh"}, False),
    ):
        with patch(
            "trr_backend.repositories.admin_operations.get_operation",
            return_value={"id": operation_id, "status": "pending"},
        ):
            with patch(
                "trr_backend.repositories.admin_operations.append_operation_event",
                return_value={
                    "operation_id": operation_id,
                    "event_seq": 1,
                    "event_type": "operation",
                    "event_payload": {},
                },
            ):
                with patch("trr_backend.pipeline.admin_operations.ensure_operation_execution") as ensure_mock:
                    with patch(
                        "trr_backend.pipeline.admin_operations.dispatch_admin_operation",
                        return_value=True,
                    ) as dispatch_mock:
                        response = pipeline_admin_operations.start_operation_for_stream(
                            operation_type="admin_show_refresh",
                            producer=lambda: [],
                            request_payload={"show_id": str(uuid4())},
                            initiated_by="admin-modal",
                            request=None,
                        )

    ensure_mock.assert_not_called()
    dispatch_mock.assert_called_once_with(
        operation_id=operation_id,
        operation_type="admin_show_refresh",
    )
    assert response["execution_owner"] == "remote_worker"
    assert response["execution_mode_canonical"] == "remote"


def test_claim_and_execute_operation_claims_specific_operation() -> None:
    operation_id = str(uuid4())
    claimed_row = {
        "id": operation_id,
        "operation_type": "admin_show_refresh",
        "attempt_count": 1,
    }

    with patch(
        "trr_backend.repositories.admin_operations.claim_operation",
        return_value=claimed_row,
    ) as claim_mock:
        with patch("trr_backend.pipeline.admin_operations._run_remote_claimed_operation") as run_mock:
            claimed = pipeline_admin_operations.claim_and_execute_operation(
                operation_id=operation_id,
                worker_id="modal:test",
                operation_types=["admin_show_refresh"],
            )

    assert claimed is True
    claim_mock.assert_called_once()
    run_mock.assert_called_once_with(claimed_row)
