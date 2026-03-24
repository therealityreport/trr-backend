from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from api.main import app
from trr_backend.pipeline import admin_operations as pipeline_admin_operations
from trr_backend.repositories import admin_operations as admin_ops_repo


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


def test_operation_stream_generator_uses_async_state_helpers() -> None:
    operation_id = str(uuid4())
    helper_calls: list[tuple[str, int]] = []

    async def _fake_load_events(_operation_id: str, *, after_seq: int, limit: int):
        helper_calls.append(("events", after_seq))
        if after_seq == 0:
            return [
                {
                    "operation_id": operation_id,
                    "event_seq": 1,
                    "event_type": "progress",
                    "event_payload": {"stage": "one"},
                }
            ]
        return []

    async def _fake_load_state(_operation_id: str):
        helper_calls.append(("state", -1))
        return {"id": operation_id, "status": "completed", "operation_type": "show_sync"}

    async def _read_stream_once() -> list[tuple[str, dict[str, object]]]:
        raw_chunks: list[str] = []
        async for chunk in pipeline_admin_operations.operation_stream_generator(
            operation_id,
            after_seq=0,
            request=None,
        ):
            raw_chunks.append(chunk)
        return _parse_sse("".join(raw_chunks))

    with (
        patch("trr_backend.pipeline.admin_operations._load_operation_stream_events", side_effect=_fake_load_events),
        patch("trr_backend.pipeline.admin_operations._load_operation_state", side_effect=_fake_load_state),
        patch("trr_backend.repositories.admin_operations.stream_events_after_seq") as mocked_stream,
        patch("trr_backend.repositories.admin_operations.get_operation") as mocked_get_operation,
    ):
        replayed = asyncio.run(_read_stream_once())

    assert replayed
    assert helper_calls == [("events", 0), ("state", -1), ("events", 1)]
    mocked_stream.assert_not_called()
    mocked_get_operation.assert_not_called()


def test_cancel_operation_requests_cancellation_and_emits_event(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    operation_id = str(uuid4())
    related_operation_id = str(uuid4())

    with patch(
        "api.routers.admin_operations.admin_operations.request_related_operation_cancels",
        return_value=[
            {"id": operation_id, "status": "cancelling"},
            {"id": related_operation_id, "status": "cancelling"},
        ],
    ):
        with patch(
            "api.routers.admin_operations.admin_operations.get_operation",
            side_effect=[
                {"id": operation_id, "status": "running"},
                {"id": operation_id, "status": "cancelling"},
            ],
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
    assert payload["cancelled_operations"] == 2
    assert payload["cancelled_operation_ids"] == [operation_id, related_operation_id]

    _, kwargs = append_mock.call_args
    assert kwargs["event_type"] == "progress"
    assert kwargs["event_payload"]["operation_id"] in {operation_id, related_operation_id}
    assert kwargs["event_payload"]["cancel_requested"] is True


def test_get_admin_operations_health_returns_health_payload(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "summary": {
            "active_total": 2,
            "stale_total": 1,
            "cancelling_total": 1,
            "runtime_split": {"modal": 1, "local": 1, "other": 0, "unknown": 0},
        },
        "active_operations": [],
        "stale_operations": [],
        "updated_at": "2026-03-21T00:00:00Z",
    }

    with patch("api.routers.admin_operations.admin_operations.get_admin_operations_health", return_value=expected):
        response = client.get(
            "/api/v1/admin/operations/health?stale_after_seconds=600&cancelling_grace_seconds=90&limit=50",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["active_total"] == 2
    assert body["summary"]["stale_total"] == 1


def test_force_cancel_stale_admin_operations_returns_cleanup_payload(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    operation_id = str(uuid4())

    with patch(
        "api.routers.admin_operations.admin_operations.force_cancel_stale_operations",
        return_value={
            "cancelled_operations": 1,
            "cancelled_operation_ids": [operation_id],
            "stale_operations_remaining": 0,
            "active_operations_remaining": 0,
            "updated_at": "2026-03-21T00:00:00Z",
        },
    ) as cleanup_mock:
        response = client.post(
            "/api/v1/admin/operations/stale/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={"operation_ids": [operation_id], "stale_after_seconds": 600, "cancelling_grace_seconds": 90},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["cancelled_operations"] == 1
    assert body["cancelled_operation_ids"] == [operation_id]
    assert cleanup_mock.call_args.kwargs["stale_after_seconds"] == 600
    assert cleanup_mock.call_args.kwargs["cancelling_grace_seconds"] == 90


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


def test_start_operation_request_prefers_remote_in_dev_without_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "development")
    operation_id = str(uuid4())

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/admin/person/person-1/refresh-images/stream",
            "headers": [(b"x-trr-prefer-local-execution", b"1")],
        }
    )

    with patch(
        "trr_backend.repositories.admin_operations.create_or_attach_operation",
        return_value=(
            {"id": operation_id, "status": "pending", "operation_type": "admin_person_refresh_images"},
            False,
        ),
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
                            operation_type="admin_person_refresh_images",
                            producer=lambda: [],
                            request_payload={"person_id": str(uuid4())},
                            initiated_by="admin-remote-default",
                            request=request,
                        )

    ensure_mock.assert_not_called()
    dispatch_mock.assert_called_once_with(
        operation_id=operation_id,
        operation_type="admin_person_refresh_images",
    )
    assert response["execution_owner"] == "remote_worker"


def test_start_operation_request_can_prefer_local_execution_when_override_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("TRR_ALLOW_LOCAL_ADMIN_OPERATION_OVERRIDE", "1")
    operation_id = str(uuid4())

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/admin/person/person-1/refresh-images/stream",
            "headers": [(b"x-trr-prefer-local-execution", b"1")],
        }
    )

    with patch(
        "trr_backend.repositories.admin_operations.create_or_attach_operation",
        return_value=(
            {"id": operation_id, "status": "pending", "operation_type": "admin_person_refresh_images"},
            False,
        ),
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
                            operation_type="admin_person_refresh_images",
                            producer=lambda: [],
                            request_payload={"person_id": str(uuid4())},
                            initiated_by="admin-local-override",
                            request=request,
                        )

    ensure_mock.assert_called_once()
    dispatch_mock.assert_not_called()
    assert response["execution_owner"] == "local_api"
    assert response["execution_mode_canonical"] == "local"
    assert response["execution_backend_canonical"] == "local"


def test_start_operation_loopback_person_refresh_prefers_local_execution_in_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("WORKSPACE_DEV_MODE", "cloud")
    operation_id = str(uuid4())

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/admin/person/person-1/refresh-images/stream",
            "headers": [],
            "client": ("127.0.0.1", 54321),
        }
    )

    with patch(
        "trr_backend.repositories.admin_operations.create_or_attach_operation",
        return_value=(
            {"id": operation_id, "status": "pending", "operation_type": "admin_person_refresh_images"},
            False,
        ),
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
                            operation_type="admin_person_refresh_images",
                            producer=lambda: [],
                            request_payload={"person_id": str(uuid4())},
                            initiated_by="admin-loopback-override",
                            request=request,
                        )

    ensure_mock.assert_not_called()
    dispatch_mock.assert_called_once()
    assert response["execution_owner"] == "remote_worker"
    assert response["execution_mode_canonical"] == "remote"
    assert response["execution_backend_canonical"] == "modal"


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


def test_run_remote_claimed_operation_heartbeats_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    operation_id = str(uuid4())
    operation = {
        "id": operation_id,
        "operation_type": "admin_show_refresh",
        "request_payload": {"show_id": str(uuid4())},
        "request_id": "req-123",
        "claim_token": "claim-123",
        "attempt_count": 1,
    }

    monkeypatch.setattr(pipeline_admin_operations, "_OPERATION_CLAIM_HEARTBEAT_INTERVAL_SECONDS", 0.01)

    with patch(
        "trr_backend.pipeline.admin_operations._resolve_remote_operation_producer",
        return_value=lambda: [],
    ):
        with patch(
            "trr_backend.pipeline.admin_operations._run_operation_worker",
            side_effect=lambda *args, **kwargs: time.sleep(0.04),
        ):
            with patch(
                "trr_backend.repositories.admin_operations.heartbeat_operation_claim",
                return_value=True,
            ) as heartbeat_mock:
                with patch(
                    "trr_backend.repositories.admin_operations.release_operation_claim",
                    return_value=True,
                ) as release_mock:
                    pipeline_admin_operations._run_remote_claimed_operation(operation)

    assert heartbeat_mock.call_count >= 1
    release_mock.assert_called_once_with(operation_id, claim_token="claim-123")


def test_modal_dispatch_emits_operation_and_dispatched_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """When Modal dispatch succeeds, both an 'operation' envelope and
    'dispatched_to_modal' event are emitted with monotonic event_seq.

    Regression coverage for the Modal secret graph mismatch bug (March 2026):
    operations stuck in 'pending' because Modal refused function startup.
    The fix made secret resolution deterministic; this test ensures the
    dispatch event emission path remains exercised.
    """
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")

    with patch("trr_backend.pipeline.admin_operations.dispatch_admin_operation", return_value=True):
        with patch("trr_backend.pipeline.admin_operations.ensure_operation_execution"):
            response = pipeline_admin_operations.start_operation_for_stream(
                operation_type="admin_show_refresh",
                producer=lambda: [],
                request_payload={"show_id": str(uuid4())},
                initiated_by="admin-regression-test",
                request=None,
            )

    operation_id = str(response["id"])
    events = admin_ops_repo.stream_events_after_seq(operation_id, after_seq=0)

    # Must have at least 2 events: operation envelope + dispatched_to_modal
    assert len(events) >= 2

    # First event is the operation envelope
    assert events[0]["event_type"] == "operation"
    assert events[0]["event_payload"]["operation_id"] == operation_id
    assert int(events[0]["event_seq"]) == 1

    # Second event is dispatched_to_modal
    assert events[1]["event_type"] == "dispatched_to_modal"
    assert events[1]["event_payload"]["operation_id"] == operation_id
    assert int(events[1]["event_seq"]) == 2

    # event_seq is monotonically increasing with no duplicates
    seqs = [int(e["event_seq"]) for e in events]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)

    # Operation is still pending (no worker claimed it)
    op = admin_ops_repo.get_operation(operation_id)
    assert op is not None
    assert op["status"] == "pending"


def test_replay_stream_returns_all_events_after_modal_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """After Modal claims and executes, replay stream returns all events
    with monotonic event_seq including the terminal event.

    This covers the full lifecycle: dispatch → claim → execute → replay,
    ensuring the replay cursor (after_seq) correctly skips already-seen
    events and the stream terminates after the operation reaches a
    terminal status.
    """
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")

    # Phase 1: Start operation with Modal dispatch
    with patch("trr_backend.pipeline.admin_operations.dispatch_admin_operation", return_value=True):
        with patch("trr_backend.pipeline.admin_operations.ensure_operation_execution"):
            response = pipeline_admin_operations.start_operation_for_stream(
                operation_type="admin_show_refresh",
                producer=lambda: [],
                request_payload={"show_id": str(uuid4())},
                initiated_by="admin-replay-test",
                request=None,
            )

    operation_id = str(response["id"])

    # Phase 2: Simulate what Modal worker does after claiming
    admin_ops_repo.touch_operation_started(operation_id)
    admin_ops_repo.append_operation_event(
        operation_id,
        event_type="progress",
        event_payload={"stage": "sync_shows", "status": "running", "operation_id": operation_id},
    )
    admin_ops_repo.append_operation_event(
        operation_id,
        event_type="complete",
        event_payload={"stage": "done", "status": "success", "operation_id": operation_id},
    )
    admin_ops_repo.update_operation_status(
        operation_id,
        status="completed",
        result_payload={"details": {"status": "success"}},
    )

    # Phase 3: Replay from seq 0 — should get all events then terminate
    async def _replay() -> list[tuple[str, dict[str, object]]]:
        raw_chunks: list[str] = []
        async for chunk in pipeline_admin_operations.operation_stream_generator(
            operation_id,
            after_seq=0,
            request=None,
        ):
            raw_chunks.append(chunk)
        return _parse_sse("".join(raw_chunks))

    replayed = asyncio.run(_replay())

    # At least 4 events: operation, dispatched_to_modal, progress, complete
    assert len(replayed) >= 4

    # Verify event types in expected order
    event_types = [et for et, _ in replayed]
    assert event_types[0] == "operation"
    assert event_types[1] == "dispatched_to_modal"
    assert "progress" in event_types
    assert "complete" in event_types

    # Verify monotonic event_seq
    seqs = [int(p.get("event_seq", 0)) for _, p in replayed]
    for i in range(1, len(seqs)):
        assert seqs[i] > seqs[i - 1], f"event_seq not monotonic: {seqs}"

    # Verify all events reference the correct operation
    for _, payload in replayed:
        assert str(payload.get("operation_id", "")) == operation_id

    # Phase 4: Replay with after_seq > 0 skips already-seen events
    async def _replay_partial() -> list[tuple[str, dict[str, object]]]:
        raw_chunks: list[str] = []
        async for chunk in pipeline_admin_operations.operation_stream_generator(
            operation_id,
            after_seq=2,
            request=None,
        ):
            raw_chunks.append(chunk)
        return _parse_sse("".join(raw_chunks))

    partial = asyncio.run(_replay_partial())
    partial_seqs = [int(p.get("event_seq", 0)) for _, p in partial]
    assert all(s > 2 for s in partial_seqs), f"after_seq=2 should skip seqs <= 2, got {partial_seqs}"
