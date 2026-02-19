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


class TestSyncNetworksStreaming:
    def test_returns_401_without_auth(self, client):
        response = client.post("/api/v1/admin/shows/sync-networks-streaming", json={})
        assert response.status_code == 401

    def test_runs_three_steps_and_aggregates_metrics(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")
        received = {"entities": None, "providers": None, "links": None}

        def fake_entities(argv):
            received["entities"] = list(argv or [])
            print("networks_upserted=7")
            print("production_companies_upserted=5")
            print("logos_mirrored=2")
            print("failures=0")
            return 0

        def fake_providers(argv):
            received["providers"] = list(argv or [])
            print("providers_upserted=12")
            print("show_watch_providers_upserted=20")
            print("logos_mirrored=3")
            print("failures=1")
            return 0

        def fake_links(argv):
            received["links"] = list(argv or [])
            print("processed=30")
            print("links_enriched=18")
            print("wikidata_linked=10")
            print("wikipedia_linked=9")
            print("logos_mirrored=4")
            print("variants_black_mirrored=3")
            print("variants_white_mirrored=2")
            print("unresolved_logos=2")
            print('unresolved_logo={"type":"network","id":"77","name":"Bravo","reason":"no_logo_claim"}')
            print('unresolved_logo={"type":"streaming","id":"531","name":"Peacock","reason":"download_failed"}')
            print("failures=2")
            return 0

        with patch("api.routers.admin_show_sync.sync_tmdb_show_entities.main", side_effect=fake_entities):
            with patch("api.routers.admin_show_sync.sync_tmdb_watch_providers.main", side_effect=fake_providers):
                with patch("api.routers.admin_show_sync.sync_networks_streaming_links.main", side_effect=fake_links):
                    response = client.post(
                        "/api/v1/admin/shows/sync-networks-streaming",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"skip_s3": True, "force": True, "limit": 50},
                    )

        assert response.status_code == 200
        payload = response.json()
        assert payload["entities_synced"] == 12
        assert payload["providers_synced"] == 12
        assert payload["links_enriched"] == 18
        assert payload["logos_mirrored"] == 9
        assert payload["variants_black_mirrored"] == 3
        assert payload["variants_white_mirrored"] == 2
        assert payload["unresolved_logos_count"] == 2
        assert payload["unresolved_logos_truncated"] is False
        assert payload["unresolved_logos"] == [
            {"type": "network", "id": "77", "name": "Bravo", "reason": "no_logo_claim"},
            {"type": "streaming", "id": "531", "name": "Peacock", "reason": "download_failed"},
        ]
        assert payload["failures"] == 3
        assert payload["steps"]["tmdb_show_entities"]["status"] == "success"
        assert payload["steps"]["tmdb_watch_providers"]["status"] == "success"
        assert payload["steps"]["network_streaming_links"]["status"] == "success"

        assert "--all" in (received["entities"] or [])
        assert "--force" in (received["entities"] or [])
        assert "--skip-s3" in (received["entities"] or [])
        assert "--limit" in (received["entities"] or [])
        assert "--skip-s3" in (received["providers"] or [])
        assert "--skip-s3" in (received["links"] or [])

    def test_truncates_unresolved_logo_list_to_cap(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

        from api.routers.admin_show_sync import SyncNetworksStreamingStepResult

        unresolved_lines = [
            f'unresolved_logo={{"type":"network","id":"{idx}","name":"Network {idx}","reason":"no_logo_claim"}}'
            for idx in range(350)
        ]
        unresolved_output = "\n".join(unresolved_lines)

        step_results = [
            (
                SyncNetworksStreamingStepResult(
                    status="success",
                    duration_ms=1,
                    exit_code=0,
                    metrics={
                        "networks_upserted": 1,
                        "production_companies_upserted": 0,
                        "logos_mirrored": 0,
                        "failures": 0,
                    },
                ),
                "",
            ),
            (
                SyncNetworksStreamingStepResult(
                    status="success",
                    duration_ms=1,
                    exit_code=0,
                    metrics={
                        "providers_upserted": 1,
                        "show_watch_providers_upserted": 0,
                        "logos_mirrored": 0,
                        "failures": 0,
                    },
                ),
                "",
            ),
            (
                SyncNetworksStreamingStepResult(
                    status="success",
                    duration_ms=1,
                    exit_code=0,
                    metrics={
                        "processed": 0,
                        "links_enriched": 0,
                        "wikidata_linked": 0,
                        "wikipedia_linked": 0,
                        "logos_mirrored": 0,
                        "variants_black_mirrored": 0,
                        "variants_white_mirrored": 0,
                        "unresolved_logos": 350,
                        "failures": 0,
                    },
                ),
                unresolved_output,
            ),
        ]

        with patch("api.routers.admin_show_sync._run_script_step_with_metrics", side_effect=step_results):
            response = client.post(
                "/api/v1/admin/shows/sync-networks-streaming",
                headers={"Authorization": f"Bearer {token}"},
                json={},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["unresolved_logos_count"] == 350
        assert payload["unresolved_logos_truncated"] is True
        assert len(payload["unresolved_logos"]) == 300

    def test_marks_step_failed_when_script_returns_non_zero(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

        def bad_entities(argv):
            print("failures=4")
            return 2

        def ok_providers(argv):
            print("providers_upserted=1")
            print("logos_mirrored=0")
            print("failures=0")
            return 0

        def ok_links(argv):
            print("links_enriched=1")
            print("logos_mirrored=0")
            print("failures=0")
            return 0

        with patch("api.routers.admin_show_sync.sync_tmdb_show_entities.main", side_effect=bad_entities):
            with patch("api.routers.admin_show_sync.sync_tmdb_watch_providers.main", side_effect=ok_providers):
                with patch("api.routers.admin_show_sync.sync_networks_streaming_links.main", side_effect=ok_links):
                    response = client.post(
                        "/api/v1/admin/shows/sync-networks-streaming",
                        headers={"Authorization": f"Bearer {token}"},
                        json={},
                    )

        assert response.status_code == 200
        payload = response.json()
        assert payload["steps"]["tmdb_show_entities"]["status"] == "failed"
        assert payload["steps"]["tmdb_show_entities"]["exit_code"] == 2
        assert payload["failures"] == 4


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

    def test_refresh_stream_runs_episodes_before_seasons(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

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
            with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result) as run_step:
                with client.stream(
                    "POST",
                    f"/api/v1/admin/shows/{show_id}/refresh/stream",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"targets": ["seasons_episodes"]},
                ) as response:
                    assert response.status_code == 200
                    # Exhaust stream so all steps execute.
                    list(response.iter_lines())

        called_step_keys = [call.args[0] for call in run_step.call_args_list]
        assert called_step_keys == ["seasons_episodes_episodes", "seasons_episodes_seasons"]

    def test_refresh_stream_emits_structured_topic_and_provider(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        token = _make_admin_token("test-secret")

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
                    json={"targets": ["details"]},
                ) as response:
                    assert response.status_code == 200
                    text = "\n".join(
                        line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                        for line in response.iter_lines()
                    )

        assert '"stage_key": "details_sync_shows"' in text
        assert '"topic": "shows"' in text
        assert '"provider": "mixed"' in text


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
