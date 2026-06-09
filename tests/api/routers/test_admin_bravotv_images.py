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
from trr_backend.media.getty_replacement import ResolvedPublicReplacement


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


def _fake_json_object(payload_by_key: dict[str, object]):
    def _get_object(**kwargs):  # noqa: ANN001
        key = kwargs["Key"]
        if key not in payload_by_key:
            raise AssertionError(f"Unexpected object key: {key}")
        return {"Body": io.BytesIO(json.dumps(payload_by_key[key]).encode("utf-8"))}

    return _get_object


class _FakeObjectStorage:
    def __init__(self, payload_by_key: dict[str, object]) -> None:
        self.payload_by_key = dict(payload_by_key)
        self.puts: list[dict[str, object]] = []

    def get_object(self, **kwargs):  # noqa: ANN001
        key = kwargs["Key"]
        if key not in self.payload_by_key:
            raise AssertionError(f"Unexpected object key: {key}")
        return {"Body": io.BytesIO(json.dumps(self.payload_by_key[key]).encode("utf-8"))}

    def put_object(self, **kwargs):  # noqa: ANN001
        body = kwargs["Body"]
        payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else str(body))
        self.payload_by_key[kwargs["Key"]] = payload
        self.puts.append(dict(kwargs))
        return {"ETag": '"etag-1"'}


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
        "api.routers.admin_bravotv_images._hydrate_bravotv_getty_prefetch",
        side_effect=lambda payload: payload,
    ):
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


def test_start_bravotv_person_run_auto_hydrates_getty_prefetch_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())

    fake_stream = StreamingResponse(iter([b"event: progress\ndata: {}\n\n"]), media_type="text/event-stream")

    with patch(
        "api.routers.admin_bravotv_images._hydrate_bravotv_getty_prefetch",
        side_effect=lambda payload: payload.model_copy(
            update={
                "getty_prefetched_assets": [{"editorial_id": "928663262"}],
                "getty_prefetched_queries": [{"query": "Jane Doe Bravo"}],
                "getty_prefetch_mode": "full",
                "getty_prefetch_auth_mode": "chrome_profile_browser_session",
            }
        ),
    ) as hydrate_mock:
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
                    json={"mode": "person", "sources": ["getty"]},
                )

    assert response.status_code == 200
    hydrate_mock.assert_called_once()
    _, kwargs = start_mock.call_args
    payload = kwargs["request_payload"]["payload"]
    assert payload["getty_prefetched_assets"] == [{"editorial_id": "928663262"}]
    assert payload["getty_prefetched_queries"] == [{"query": "Jane Doe Bravo"}]
    assert payload["getty_prefetch_mode"] == "full"
    assert payload["getty_prefetch_auth_mode"] == "chrome_profile_browser_session"


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
        "trr_backend.media.bravotv.admin_review_service.get_bravotv_run",
        return_value={
            "id": run_id,
            "artifact_paths": {
                "replacement_candidates": {
                    "key": "bravotv-image-runs/test/replacement_candidates.json",
                }
            },
        },
    ):
        with patch("trr_backend.media.bravotv.admin_review_service.get_object_storage_bucket", return_value="bucket-1"):
            with patch(
                "trr_backend.media.bravotv.admin_review_service.get_object_storage_client",
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


def test_get_run_review_items_filters_and_paginates_review_reasons(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())

    run_review = {
        "review_candidates": [
            {"group_id": "a", "reason": "person_assignment_needs_review"},
            {"group_id": "b", "reason": "caption_match_ambiguous"},
            {"group_id": "c", "reason": "target_person_not_deterministic"},
        ],
        "replacement_pending": [],
        "duplicate_groups": [],
    }

    with patch(
        "trr_backend.media.bravotv.admin_review_service.get_bravotv_run",
        return_value={
            "id": run_id,
            "artifact_paths": {"run_review": {"key": "bravotv-image-runs/test/run_review.json"}},
        },
    ):
        with patch("trr_backend.media.bravotv.admin_review_service.get_object_storage_bucket", return_value="bucket-1"):
            with patch(
                "trr_backend.media.bravotv.admin_review_service.get_object_storage_client",
                return_value=type(
                    "FakeClient",
                    (),
                    {"get_object": staticmethod(_fake_json_object({"bravotv-image-runs/test/run_review.json": run_review}))},
                )(),
            ):
                response = client.get(
                    (
                        f"/api/v1/admin/bravotv/images/runs/{run_id}/review"
                        "?section=review_candidates&reason=ambiguous_people_match&offset=1&limit=1"
                    ),
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["section"] == "review_candidates"
    assert payload["filters"]["reason"] == "ambiguous_people_match"
    assert payload["total"] == 2
    assert payload["items"] == [{"group_id": "c", "reason": "target_person_not_deterministic"}]


def test_approve_replacement_candidate_uses_run_candidate_and_records_action(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    asset_id = str(uuid4())
    run_row = {
        "id": run_id,
        "artifact_paths": {
            "replacement_candidates": {"key": "bravotv-image-runs/test/replacement_candidates.json"},
            "run_review": {"key": "bravotv-image-runs/test/run_review.json"},
        },
        "review_summary": {},
    }
    storage = _FakeObjectStorage(
        {
            "bravotv-image-runs/test/replacement_candidates.json": [
                {"group_id": "group-1", "media_asset_id": asset_id}
            ],
            "bravotv-image-runs/test/run_review.json": {
                "replacement_pending": [{"group_id": "group-1", "media_asset_id": asset_id}],
                "duplicate_groups": [],
            },
        }
    )

    with patch("trr_backend.media.bravotv.admin_review_service.get_bravotv_run", return_value=run_row):
        with patch("trr_backend.media.bravotv.admin_review_service.get_object_storage_bucket", return_value="bucket-1"):
            with patch(
                "trr_backend.media.bravotv.admin_review_service.get_object_storage_client",
                return_value=storage,
            ):
                with patch("trr_backend.media.bravotv.admin_review_service.create_supabase_admin_client", return_value=object()):
                    with patch(
                        "trr_backend.media.bravotv.admin_review_service.fetch_media_asset",
                        return_value={"id": asset_id, "source": "getty", "width": 1600, "height": 900, "metadata": {}},
                    ):
                        with patch(
                            "trr_backend.media.bravotv.admin_review_service.resolve_public_replacement_from_page",
                            return_value=ResolvedPublicReplacement(
                                page_url="https://www.bravotv.com/gallery",
                                source_domain="bravotv.com",
                                image_url="https://www.bravotv.com/sites/bravo/files/replacement.jpg",
                                width=1825,
                                height=1217,
                            ),
                        ) as resolve_mock:
                            with patch(
                                "trr_backend.media.bravotv.admin_review_service.apply_media_asset_replacement",
                                return_value={
                                    "asset_id": asset_id,
                                    "status": "replaced",
                                    "new_source": "bravotv.com",
                                    "new_source_url": "https://www.bravotv.com/gallery",
                                },
                            ) as apply_mock:
                                with patch(
                                    "trr_backend.media.bravotv.admin_review_service.update_bravotv_run_progress",
                                    return_value=run_row,
                                ) as update_mock:
                                    response = client.post(
                                        (
                                            f"/api/v1/admin/bravotv/images/runs/{run_id}"
                                            "/replacement-candidates/group-1/approve"
                                        ),
                                        headers={"Authorization": f"Bearer {token}"},
                                        json={
                                            "page_url": "https://www.bravotv.com/gallery",
                                            "source_domain": "bravotv.com",
                                        },
                                    )

    assert response.status_code == 200
    resolve_mock.assert_called_once()
    apply_mock.assert_called_once()
    assert update_mock.call_args.kwargs["review_summary"]["operator_actions"][0]["type"] == "replacement_approved"
    assert storage.payload_by_key["bravotv-image-runs/test/replacement_candidates.json"][0]["operator_status"] == "approved"
    assert storage.payload_by_key["bravotv-image-runs/test/run_review.json"]["operator_actions"][0]["type"] == "replacement_approved"


def test_bulk_approve_replacement_candidates_records_partial_results(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    asset_id = str(uuid4())
    run_row = {
        "id": run_id,
        "artifact_paths": {
            "replacement_candidates": {"key": "bravotv-image-runs/test/replacement_candidates.json"},
            "run_review": {"key": "bravotv-image-runs/test/run_review.json"},
        },
        "review_summary": {},
    }
    storage = _FakeObjectStorage(
        {
            "bravotv-image-runs/test/replacement_candidates.json": [
                {"group_id": "group-1", "media_asset_id": asset_id}
            ],
            "bravotv-image-runs/test/run_review.json": {
                "replacement_pending": [{"group_id": "group-1", "media_asset_id": asset_id}],
                "duplicate_groups": [],
            },
        }
    )

    with patch("trr_backend.media.bravotv.admin_review_service.get_bravotv_run", return_value=run_row):
        with patch("trr_backend.media.bravotv.admin_review_service.get_object_storage_bucket", return_value="bucket-1"):
            with patch("trr_backend.media.bravotv.admin_review_service.get_object_storage_client", return_value=storage):
                with patch("trr_backend.media.bravotv.admin_review_service.create_supabase_admin_client", return_value=object()):
                    with patch(
                        "trr_backend.media.bravotv.admin_review_service.fetch_media_asset",
                        return_value={"id": asset_id, "source": "getty", "width": 1600, "height": 900, "metadata": {}},
                    ):
                        with patch(
                            "trr_backend.media.bravotv.admin_review_service.resolve_public_replacement_from_page",
                            return_value=ResolvedPublicReplacement(
                                page_url="https://www.bravotv.com/gallery",
                                source_domain="bravotv.com",
                                image_url="https://www.bravotv.com/sites/bravo/files/replacement.jpg",
                                width=1825,
                                height=1217,
                            ),
                        ):
                            with patch(
                                "trr_backend.media.bravotv.admin_review_service.apply_media_asset_replacement",
                                return_value={"asset_id": asset_id, "status": "replaced"},
                            ):
                                with patch(
                                    "trr_backend.media.bravotv.admin_review_service.update_bravotv_run_progress",
                                    return_value=run_row,
                                ):
                                    response = client.post(
                                        f"/api/v1/admin/bravotv/images/runs/{run_id}/replacement-candidates/approve-bulk",
                                        headers={"Authorization": f"Bearer {token}"},
                                        json={
                                            "note": "batch approved",
                                            "items": [
                                                {
                                                    "group_id": "group-1",
                                                    "page_url": "https://www.bravotv.com/gallery",
                                                    "source_domain": "bravotv.com",
                                                },
                                                {
                                                    "group_id": "missing",
                                                    "page_url": "https://www.bravotv.com/gallery",
                                                    "source_domain": "bravotv.com",
                                                },
                                            ],
                                        },
                                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["approved_count"] == 1
    assert payload["failed_count"] == 1
    assert payload["approved"][0]["action"]["note"] == "batch approved"


def test_resolve_duplicate_group_marks_non_primary_assets(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    primary_asset_id = str(uuid4())
    duplicate_asset_id = str(uuid4())
    run_row = {
        "id": run_id,
        "artifact_paths": {
            "imported_records": {"key": "bravotv-image-runs/test/imported_records.json"},
            "run_review": {"key": "bravotv-image-runs/test/run_review.json"},
        },
        "review_summary": {},
    }
    storage = _FakeObjectStorage(
        {
            "bravotv-image-runs/test/imported_records.json": [
                {"group_id": "bridge-1", "media_asset_id": primary_asset_id},
                {"group_id": "bridge-2", "media_asset_id": duplicate_asset_id},
            ],
            "bravotv-image-runs/test/run_review.json": {
                "replacement_pending": [],
                "duplicate_groups": [
                    {
                        "key_type": "source_url",
                        "key": "bravo:https://example.test/image.jpg",
                        "group_ids": ["bridge-1", "bridge-2"],
                    }
                ],
            },
        }
    )

    with patch("trr_backend.media.bravotv.admin_review_service.get_bravotv_run", return_value=run_row):
        with patch("trr_backend.media.bravotv.admin_review_service.get_object_storage_bucket", return_value="bucket-1"):
            with patch(
                "trr_backend.media.bravotv.admin_review_service.get_object_storage_client",
                return_value=storage,
            ):
                with patch("trr_backend.media.bravotv.admin_review_service.create_supabase_admin_client", return_value=object()):
                    with patch("trr_backend.media.bravotv.admin_review_service.update_media_asset_metadata") as update_asset_mock:
                        with patch(
                            "trr_backend.media.bravotv.admin_review_service.update_bravotv_run_progress",
                            return_value=run_row,
                        ) as update_run_mock:
                            response = client.post(
                                f"/api/v1/admin/bravotv/images/runs/{run_id}/duplicates/resolve",
                                headers={"Authorization": f"Bearer {token}"},
                                json={
                                    "key_type": "source_url",
                                    "key": "bravo:https://example.test/image.jpg",
                                    "group_ids": ["bridge-1", "bridge-2"],
                                    "action": "mark_duplicate",
                                    "primary_group_id": "bridge-1",
                                },
                            )

    assert response.status_code == 200
    update_asset_mock.assert_called_once()
    assert update_asset_mock.call_args.args[1] == duplicate_asset_id
    action = update_run_mock.call_args.kwargs["review_summary"]["operator_actions"][0]
    assert action["type"] == "duplicate_resolved"
    assert action["primary_media_asset_id"] == primary_asset_id
    assert storage.payload_by_key["bravotv-image-runs/test/imported_records.json"][1]["operator_status"] == "duplicate_marked"
    duplicate_group = storage.payload_by_key["bravotv-image-runs/test/run_review.json"]["duplicate_groups"][0]
    assert duplicate_group["operator_status"] == "mark_duplicate"
