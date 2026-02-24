"""Tests for admin asset batch jobs stream endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import admin_asset_batch_jobs


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
def client():
    return TestClient(app)


class TestAssetBatchJobsStream:
    def test_skips_unsupported_origin(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")
        show_id = str(uuid4())

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/shows/{show_id}/assets/batch-jobs/stream",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "operations": ["count"],
                    "targets": [{"origin": "unknown_origin", "id": str(uuid4())}],
                },
            )

        assert response.status_code == 200
        assert "skipped_unsupported_origin" in response.text
        assert '"skipped": 1' in response.text or '"skipped":1' in response.text
        assert '"operation_counts"' in response.text
        assert '"live_counts"' in response.text

    def test_enforces_season_scope_for_season_endpoint(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")
        show_id = str(uuid4())
        target_id = str(uuid4())

        mock_db = MagicMock()
        query = MagicMock()
        query.select.return_value = query
        query.eq.return_value = query
        query.limit.return_value = query
        scope_resp = MagicMock()
        scope_resp.error = None
        scope_resp.data = [
            {
                "id": target_id,
                "metadata": {
                    "show_id": show_id,
                    "season_number": 5,
                },
            }
        ]
        query.execute.return_value = scope_resp
        mock_db.schema.return_value.table.return_value = query

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/shows/{show_id}/seasons/6/assets/batch-jobs/stream",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "operations": ["id_text"],
                    "targets": [{"origin": "media_assets", "id": target_id}],
                },
            )

        assert response.status_code == 200
        assert "skipped_out_of_scope_season" in response.text
        assert '"operation_counts"' in response.text
        assert '"live_counts"' in response.text

    def test_runs_operation_and_reports_success(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")
        show_id = str(uuid4())
        target_id = str(uuid4())

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_asset_batch_jobs._execute_target_operation") as execute_op:
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/assets/batch-jobs/stream",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "operations": ["resize"],
                        "targets": [{"origin": "media_assets", "id": target_id}],
                    },
                )

        assert response.status_code == 200
        execute_op.assert_called_once()
        assert '"succeeded": 1' in response.text or '"succeeded":1' in response.text
        assert '"operation_counts"' in response.text
        assert '"live_counts"' in response.text

    def test_reports_resize_crop_source_in_progress(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")
        show_id = str(uuid4())
        target_id = str(uuid4())

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_asset_batch_jobs._execute_target_operation",
                return_value={"crop_source": "fallback"},
            ):
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/assets/batch-jobs/stream",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "operations": ["resize"],
                        "targets": [{"origin": "media_assets", "id": target_id}],
                    },
                )

        assert response.status_code == 200
        assert '"crop_source": "fallback"' in response.text or '"crop_source":"fallback"' in response.text


class TestResizeHelpers:
    def test_execute_resize_runs_base_and_crop_variants(self):
        target_id = str(uuid4())
        db = MagicMock()
        crop_payload = {"x": 50, "y": 32, "zoom": 1, "mode": "auto"}

        with patch(
            "api.routers.admin_asset_batch_jobs._resolve_resize_crop_payload",
            return_value=(crop_payload, "fallback"),
        ):
            with patch("api.routers.admin_asset_batch_jobs.generate_variants_for_media_asset") as gen_media:
                result = admin_asset_batch_jobs._execute_target_operation(
                    origin="media_assets",
                    target_id=target_id,
                    operation="resize",
                    force=True,
                    db=db,
                )

        assert result == {"crop_source": "fallback"}
        assert gen_media.call_count == 2
        first_call = gen_media.call_args_list[0]
        second_call = gen_media.call_args_list[1]
        assert first_call.kwargs["payload"].crop is None
        assert second_call.kwargs["payload"].crop == crop_payload

    def test_resolve_resize_crop_falls_back_when_detection_unavailable(self):
        target_id = str(uuid4())
        db = MagicMock()

        with patch(
            "api.routers.admin_asset_batch_jobs._lookup_resize_crop_payload",
            side_effect=[(None, None), (None, None)],
        ):
            with patch(
                "api.routers.admin_asset_batch_jobs.auto_count_cast_photo",
                side_effect=RuntimeError("detector unavailable"),
            ):
                payload, source = admin_asset_batch_jobs._resolve_resize_crop_payload(
                    origin="cast_photos",
                    target_id=target_id,
                    force=True,
                    db=db,
                )

        assert source == "fallback"
        assert payload["mode"] == "auto"
        assert payload["x"] == 50.0
        assert payload["y"] == 32.0
        assert payload["zoom"] == 1.0
