"""Tests for admin show sync endpoints."""

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


class TestSyncFromLists:
    def test_returns_401_without_auth(self, client):
        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post("/api/v1/admin/shows/sync-from-lists", json={})
        assert response.status_code == 401

    def test_returns_400_when_no_list_sources(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.delenv("IMDB_LIST_URL", raising=False)
        monkeypatch.delenv("TMDB_LIST_ID", raising=False)
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                "/api/v1/admin/shows/sync-from-lists",
                headers={"Authorization": f"Bearer {token}"},
                json={},
            )

        assert response.status_code == 400
        assert "No list sources" in response.json().get("detail", "")

    def test_returns_200_with_counts_when_patched(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("TMDB_API_KEY", "tmdb-key")
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        mock_candidates = [MagicMock(), MagicMock()]
        mock_result = MagicMock(created=1, updated=2, skipped=3)

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync.collect_candidates_from_lists",
                return_value=mock_candidates,
            ):
                with patch(
                    "api.routers.admin_show_sync.upsert_candidates_into_supabase",
                    return_value=mock_result,
                ):
                    response = client.post(
                        "/api/v1/admin/shows/sync-from-lists",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"imdb_lists": ["https://www.imdb.com/list/ls1234567890/"], "tmdb_lists": ["8301263"]},
                    )

        assert response.status_code == 200
        data = response.json()
        assert data["candidates_collected"] == 2
        assert data["created"] == 1
        assert data["updated"] == 2
        assert data["skipped"] == 3
        assert data["imdb_lists_used"]
        assert data["tmdb_lists_used"]
        assert isinstance(data["duration_ms"], int)


class TestRefreshShow:
    def test_returns_404_for_unknown_show(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

        # Mock DB to return no show row
        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = []
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        show_id = str(uuid4())
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/shows/{show_id}/refresh",
                headers={"Authorization": f"Bearer {token}"},
                json={"targets": ["details"]},
            )

        assert response.status_code == 404

    def test_calls_script_mains_for_targets(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

        # Mock DB show exists
        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": str(uuid4())}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        show_id = str(uuid4())
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync.sync_shows_all.main", return_value=0) as p_details:
                with patch("api.routers.admin_show_sync.sync_seasons_episodes.main", return_value=0) as p_seasons:
                    with patch("api.routers.admin_show_sync.sync_show_images.main", return_value=0) as p_show_images:
                        with patch(
                            "api.routers.admin_show_sync.sync_season_episode_images.main",
                            return_value=0,
                        ) as p_season_images:
                            with patch(
                                "api.routers.admin_show_sync.sync_show_cast.main",
                                return_value=0,
                            ) as p_show_cast:
                                with patch(
                                    "api.routers.admin_show_sync.sync_episode_appearances.main",
                                    return_value=0,
                                ) as p_occurrences:
                                    response = client.post(
                                        f"/api/v1/admin/shows/{show_id}/refresh",
                                        headers={"Authorization": f"Bearer {token}"},
                                        json={
                                            "targets": [
                                                "details",
                                                "seasons_episodes",
                                                "photos",
                                                "cast_credits",
                                            ],
                                            "skip_s3": True,
                                            "verbose": True,
                                        },
                                    )

        assert response.status_code == 200
        p_details.assert_called()
        p_seasons.assert_called()
        p_show_images.assert_called()
        p_season_images.assert_called()
        p_show_cast.assert_called()
        p_occurrences.assert_called()

    def test_refresh_stream_emits_complete_event(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

        # Mock DB show exists
        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": str(uuid4())}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        show_id = str(uuid4())

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result):
                with client.stream(
                    "POST",
                    f"/api/v1/admin/shows/{show_id}/refresh/stream",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"targets": ["photos"]},
                ) as response:
                    assert response.status_code == 200
                    text = "\n".join(
                        line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                        for line in response.iter_lines()
                    )

        assert "event: complete" in text


class TestRefreshShowPhotosStream:
    def test_returns_401_without_auth(self, client):
        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post("/api/v1/admin/shows/00000000-0000-0000-0000-000000000000/refresh-photos/stream")
        assert response.status_code == 401

    def test_returns_404_for_unknown_show(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = []
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        show_id = str(uuid4())
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/shows/{show_id}/refresh-photos/stream",
                headers={"Authorization": f"Bearer {token}"},
                json={},
            )

        assert response.status_code == 404
