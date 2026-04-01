from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import ANY, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from api.main import app


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _fake_get_object(*, bucket: str, key: str) -> dict[str, io.BytesIO]:
    assert bucket == "bucket-1"
    assert key == "bravotv-image-runs/test/replacement_candidates.json"
    return {
        "Body": io.BytesIO(
            json.dumps(
                [
                    {"group_id": "a"},
                    {"group_id": "b"},
                    {"group_id": "c"},
                ]
            ).encode("utf-8")
        )
    }


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_start_bravotv_person_run_starts_shared_admin_operation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    operation_id = str(uuid4())

    fake_stream = StreamingResponse(
        iter([b'event: progress\ndata: {"stage":"queued"}\n\n']),
        media_type="text/event-stream",
    )

    with patch(
        "api.routers.admin_bravotv_images.start_operation_for_stream",
        return_value={"id": operation_id},
    ) as start_mock:
        with patch(
            "api.routers.admin_bravotv_images.operation_stream_response",
            return_value=fake_stream,
        ) as response_mock:
            response = client.post(
                f"/api/v1/admin/bravotv/images/people/{person_id}/stream",
                headers={"Authorization": f"Bearer {token}"},
                json={"mode": "person", "sources": ["getty"], "show_id": str(uuid4())},
            )

    assert response.status_code == 200
    assert "event: progress" in response.text
    _, kwargs = start_mock.call_args
    assert kwargs["operation_type"] == "admin_bravotv_image_run"
    assert kwargs["request_payload"]["mode"] == "person"
    assert kwargs["request_payload"]["person_id"] == person_id
    assert kwargs["request_payload"]["payload"]["sources"] == ["getty"]
    response_mock.assert_called_once_with(operation_id, request=ANY)


def test_start_bravotv_person_run_preserves_getty_prefetch_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())

    fake_stream = StreamingResponse(iter([b"event: progress\ndata: {}\n\n"]), media_type="text/event-stream")

    with patch(
        "api.routers.admin_bravotv_images.start_operation_for_stream",
        return_value={"id": str(uuid4())},
    ) as start_mock:
        with patch(
            "api.routers.admin_bravotv_images.operation_stream_response",
            return_value=fake_stream,
        ):
            response = client.post(
                f"/api/v1/admin/bravotv/images/people/{person_id}/stream",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "mode": "person",
                    "sources": ["getty"],
                    "getty_prefetched_assets": [{"editorial_id": "928663262"}],
                    "getty_prefetched_events": [{"event_id": "evt-1"}],
                    "getty_prefetched_queries": [{"query": "Andy Cohen Bravo"}],
                    "getty_prefetch_mode": "full",
                    "getty_prefetch_auth_mode": "local_cookie_profile",
                    "getty_prefetch_auth_warning": "local Getty prefetch required",
                },
            )

    assert response.status_code == 200
    _, kwargs = start_mock.call_args
    payload = kwargs["request_payload"]["payload"]
    assert payload["getty_prefetched_assets"] == [{"editorial_id": "928663262"}]
    assert payload["getty_prefetched_events"] == [{"event_id": "evt-1"}]
    assert payload["getty_prefetched_queries"] == [{"query": "Andy Cohen Bravo"}]
    assert payload["getty_prefetch_mode"] == "full"
    assert payload["getty_prefetch_auth_mode"] == "local_cookie_profile"
    assert payload["getty_prefetch_auth_warning"] == "local Getty prefetch required"


def test_get_latest_show_run_returns_null_when_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("api.routers.admin_bravotv_images.get_latest_bravotv_run", return_value=None):
        response = client.get(
            f"/api/v1/admin/bravotv/images/shows/{uuid4()}/latest",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == {"run": None}


def test_get_run_artifact_preview_paginates_uploaded_list(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())

    with patch(
        "api.routers.admin_bravotv_images.get_bravotv_run",
        return_value={
            "id": run_id,
            "artifact_paths": {
                "replacement_candidates": {
                    "key": "bravotv-image-runs/test/replacement_candidates.json",
                }
            },
        },
    ):
        with patch("api.routers.admin_bravotv_images.get_object_storage_bucket", return_value="bucket-1"):
            with patch(
                "api.routers.admin_bravotv_images.get_object_storage_client",
                return_value=type(
                    "FakeClient",
                    (),
                    {
                        "get_object": staticmethod(
                            lambda **kwargs: _fake_get_object(
                                bucket=kwargs["Bucket"],
                                key=kwargs["Key"],
                            )
                        )
                    },
                )(),
            ):
                response = client.get(
                    f"/api/v1/admin/bravotv/images/runs/{run_id}/artifacts/replacement_candidates?offset=1&limit=1",
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["artifact"] == "replacement_candidates"
    assert payload["total"] == 3
    assert payload["offset"] == 1
    assert payload["limit"] == 1
    assert payload["items"] == [{"group_id": "b"}]
