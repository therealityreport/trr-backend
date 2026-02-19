"""Tests for admin asset batch jobs stream endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app


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
