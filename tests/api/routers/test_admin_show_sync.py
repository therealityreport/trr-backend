"""Tests for admin show sync endpoints."""

from __future__ import annotations

import time
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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.delenv("IMDB_LIST_URL", raising=False)
        monkeypatch.delenv("TMDB_LIST_ID", raising=False)
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("TMDB_API_KEY", "tmdb-key")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_returns_500_json_detail_when_upsert_raises(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("TMDB_API_KEY", "tmdb-key")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync.collect_candidates_from_lists", return_value=[MagicMock()]):
                with patch(
                    "api.routers.admin_show_sync.upsert_candidates_into_supabase",
                    side_effect=RuntimeError("column facebook_id does not exist"),
                ):
                    response = client.post(
                        "/api/v1/admin/shows/sync-from-lists",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"tmdb_lists": ["8301263"]},
                    )

        assert response.status_code == 500
        payload = response.json()
        assert "Sync failed" in payload.get("detail", "")
        assert "facebook_id" in payload.get("detail", "")

    def test_explicit_empty_imdb_lists_does_not_fallback_to_env(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("TMDB_API_KEY", "tmdb-key")
        monkeypatch.setenv("IMDB_LIST_URL", "https://www.imdb.com/list/ls4106677119/")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        mock_candidates = [MagicMock()]
        mock_result = MagicMock(created=0, updated=1, skipped=0)
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync.collect_candidates_from_lists",
                return_value=mock_candidates,
            ) as mock_collect:
                with patch(
                    "api.routers.admin_show_sync.upsert_candidates_into_supabase",
                    return_value=mock_result,
                ):
                    response = client.post(
                        "/api/v1/admin/shows/sync-from-lists",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"imdb_lists": [], "tmdb_lists": ["8301274"]},
                    )

        assert response.status_code == 200
        payload = response.json()
        assert payload["imdb_lists_used"] == []
        assert payload["tmdb_lists_used"] == ["8301274"]
        assert payload["candidates_collected"] == 1
        collect_kwargs = mock_collect.call_args.kwargs
        assert collect_kwargs["imdb_list_urls"] == []
        assert collect_kwargs["tmdb_lists"] == ["8301274"]


class TestSyncNetworksStreaming:
    @pytest.fixture(autouse=True)
    def _mock_admin_client(self):
        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            yield mock_db

    def test_returns_401_without_auth(self, client):
        response = client.post("/api/v1/admin/shows/sync-networks-streaming", json={})
        assert response.status_code == 401

    def test_returns_missing_columns_and_skips_sync_when_schema_incomplete(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        with patch(
            "api.routers.admin_show_sync._schema_preflight_missing_columns",
            return_value=[{"table": "core.networks", "column": "wikidata_id"}],
        ):
            response = client.post(
                "/api/v1/admin/shows/sync-networks-streaming",
                headers={"Authorization": f"Bearer {token}"},
                json={},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["completion_gate_passed"] is False
        assert payload["missing_columns"] == [{"table": "core.networks", "column": "wikidata_id"}]
        assert payload["steps"] == {}

    def test_runs_three_steps_and_aggregates_metrics(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
        received = {"entities": None, "providers": None, "links": None, "show_logos": None}

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
            print("run_id=network-streaming-20260224T210000Z")
            print("run_status=stopped")
            print("resume_cursor_entity_type=network")
            print("resume_cursor_entity_key=bravo")
            print("processed=30")
            print("links_enriched=18")
            print("wikidata_linked=10")
            print("wikipedia_linked=9")
            print("logos_mirrored=4")
            print("variants_black_mirrored=3")
            print("variants_white_mirrored=2")
            print("logo_assets_discovered=25")
            print("logo_assets_mirrored=14")
            print("logo_assets_skipped=8")
            print("logo_assets_failed=3")
            print("completion_total=40")
            print("completion_resolved=39")
            print("completion_unresolved=1")
            print("completion_unresolved_total=1")
            print("completion_unresolved_network=1")
            print("completion_unresolved_streaming=0")
            print("completion_unresolved_production=0")
            print("production_missing_logos=6")
            print("production_missing_bw_variants=6")
            print("completion_percent=97.50")
            print("unresolved_logos=2")
            print('unresolved_logo={"type":"network","id":"77","name":"Bravo","reason":"no_logo_claim"}')
            print('unresolved_logo={"type":"streaming","id":"531","name":"Peacock","reason":"download_failed"}')
            print("failures=2")
            return 0

        def fake_show_logos(argv):
            received["show_logos"] = list(argv or [])
            print("show_logos_discovered=16")
            print("show_logos_imported=9")
            print("show_logos_skipped=4")
            print("show_logo_failures=3")
            print("failures=0")
            return 0

        with patch("api.routers.admin_show_sync._schema_preflight_missing_columns", return_value=[]):
            with patch("api.routers.admin_show_sync.sync_tmdb_show_entities.main", side_effect=fake_entities):
                with patch(
                    "api.routers.admin_show_sync.sync_tmdb_watch_providers.main",
                    side_effect=fake_providers,
                ):
                    with patch(
                        "api.routers.admin_show_sync.sync_networks_streaming_links.main",
                        side_effect=fake_links,
                    ):
                        with patch("api.routers.admin_show_sync.sync_show_logos.main", side_effect=fake_show_logos):
                            response = client.post(
                                "/api/v1/admin/shows/sync-networks-streaming",
                                headers={"Authorization": f"Bearer {token}"},
                                json={
                                    "skip_s3": True,
                                    "force": True,
                                    "limit": 50,
                                    "refresh_external_sources": True,
                                    "entity_type": "production",
                                    "entity_keys": ["shed media", "big head productions"],
                                    "batch_size": 20,
                                    "max_runtime_sec": 1200,
                                    "resume_run_id": "network-streaming-20260224T200000Z",
                                },
                            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["run_id"] == "network-streaming-20260224T210000Z"
        assert payload["status"] == "stopped"
        assert payload["resume_cursor"] == {"entity_type": "network", "entity_key": "bravo"}
        assert payload["entities_synced"] == 12
        assert payload["providers_synced"] == 12
        assert payload["links_enriched"] == 18
        assert payload["logos_mirrored"] == 9
        assert payload["variants_black_mirrored"] == 3
        assert payload["variants_white_mirrored"] == 2
        assert payload["logo_assets_discovered"] == 25
        assert payload["logo_assets_mirrored"] == 14
        assert payload["logo_assets_skipped"] == 8
        assert payload["logo_assets_failed"] == 3
        assert payload["show_logos_discovered"] == 16
        assert payload["show_logos_imported"] == 9
        assert payload["show_logos_skipped"] == 4
        assert payload["show_logo_failures"] == 3
        assert payload["completion_total"] == 40
        assert payload["completion_resolved"] == 39
        assert payload["completion_unresolved"] == 1
        assert payload["completion_unresolved_total"] == 1
        assert payload["completion_unresolved_network"] == 1
        assert payload["completion_unresolved_streaming"] == 0
        assert payload["completion_unresolved_production"] == 0
        assert payload["production_missing_logos"] == 6
        assert payload["production_missing_bw_variants"] == 6
        assert payload["completion_percent"] == 97.5
        assert payload["completion_gate_passed"] is False
        assert payload["missing_columns"] == []
        assert payload["unresolved_logos_count"] == 2
        assert payload["unresolved_logos_truncated"] is False
        assert payload["unresolved_logos"] == [
            {"type": "network", "id": "77", "name": "Bravo", "reason": "no_logo_claim"},
            {"type": "streaming", "id": "531", "name": "Peacock", "reason": "download_failed"},
        ]
        assert payload["failures"] == 4
        assert payload["steps"]["tmdb_show_entities"]["status"] == "success"
        assert payload["steps"]["tmdb_watch_providers"]["status"] == "success"
        assert payload["steps"]["network_streaming_links"]["status"] == "success"
        assert payload["steps"]["show_logos"]["status"] == "success"

        assert "--all" in (received["entities"] or [])
        assert "--force" in (received["entities"] or [])
        assert "--skip-s3" in (received["entities"] or [])
        assert "--limit" in (received["entities"] or [])
        assert "--skip-s3" in (received["providers"] or [])
        assert "--skip-s3" in (received["links"] or [])
        assert "--skip-s3" in (received["show_logos"] or [])
        assert "--refresh-external-sources" in (received["links"] or [])
        assert "--batch-size" in (received["links"] or [])
        assert "--max-runtime-sec" in (received["links"] or [])
        assert "--resume-run-id" in (received["links"] or [])
        assert "--entity-type" in (received["links"] or [])
        assert "production" in (received["links"] or [])
        assert (received["links"] or []).count("--entity-key") == 2

    def test_truncates_unresolved_logo_list_to_cap(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
                        "completion_total": 350,
                        "completion_resolved": 0,
                        "completion_unresolved": 350,
                        "completion_unresolved_total": 350,
                        "completion_unresolved_network": 350,
                        "completion_unresolved_streaming": 0,
                        "completion_unresolved_production": 0,
                        "production_missing_logos": 0,
                        "production_missing_bw_variants": 0,
                        "unresolved_logos": 350,
                        "failures": 0,
                    },
                ),
                unresolved_output,
            ),
            (
                SyncNetworksStreamingStepResult(
                    status="success",
                    duration_ms=1,
                    exit_code=0,
                    metrics={
                        "show_logos_discovered": 0,
                        "show_logos_imported": 0,
                        "show_logos_skipped": 0,
                        "show_logo_failures": 0,
                        "failures": 0,
                    },
                ),
                "",
            ),
        ]

        with patch("api.routers.admin_show_sync._schema_preflight_missing_columns", return_value=[]):
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
        assert payload["completion_gate_passed"] is False

    def test_marks_step_failed_when_script_returns_non_zero(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
            print("completion_total=2")
            print("completion_resolved=2")
            print("completion_unresolved=0")
            print("completion_percent=100.00")
            print("failures=0")
            return 0

        def ok_show_logos(argv):
            print("show_logos_discovered=0")
            print("show_logos_imported=0")
            print("show_logos_skipped=0")
            print("show_logo_failures=0")
            print("failures=0")
            return 0

        with patch("api.routers.admin_show_sync._schema_preflight_missing_columns", return_value=[]):
            with patch("api.routers.admin_show_sync.sync_tmdb_show_entities.main", side_effect=bad_entities):
                with patch("api.routers.admin_show_sync.sync_tmdb_watch_providers.main", side_effect=ok_providers):
                    with patch("api.routers.admin_show_sync.sync_networks_streaming_links.main", side_effect=ok_links):
                        with patch("api.routers.admin_show_sync.sync_show_logos.main", side_effect=ok_show_logos):
                            response = client.post(
                                "/api/v1/admin/shows/sync-networks-streaming",
                                headers={"Authorization": f"Bearer {token}"},
                                json={},
                            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "failed"
        assert payload["steps"]["tmdb_show_entities"]["status"] == "failed"
        assert payload["steps"]["tmdb_show_entities"]["exit_code"] == 2
        assert payload["failures"] == 4


class TestNetworksStreamingOverrides:
    def test_create_override_upserts_row(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        upsert_query = mock_db.schema.return_value.table.return_value.upsert.return_value
        upsert_response = MagicMock()
        upsert_response.error = None
        upsert_response.data = [
            {
                "id": str(uuid4()),
                "entity_type": "network",
                "entity_key": "bravo",
                "display_name_override": "Bravo",
                "wikidata_id_override": "Q1519874",
                "wikipedia_url_override": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
                "logo_source_urls_override": [],
                "source_priority_override": ["override", "tmdb", "wikimedia"],
                "aliases_override": ["Bravo TV"],
                "notes": "manual seed",
                "is_active": True,
                "updated_by": "admin@example.com",
                "updated_at": "2026-02-19T00:00:00+00:00",
                "created_at": "2026-02-19T00:00:00+00:00",
            }
        ]
        upsert_query.execute.return_value = upsert_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                "/api/v1/admin/shows/networks-streaming/overrides",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "entity_type": "network",
                    "entity_key": "Bravo",
                    "display_name_override": "Bravo",
                    "wikidata_id_override": "Q1519874",
                    "wikipedia_url_override": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
                    "source_priority_override": ["override", "tmdb", "wikimedia"],
                    "aliases_override": ["Bravo TV"],
                    "is_active": True,
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["entity_type"] == "network"
        assert payload["entity_key"] == "bravo"
        assert payload["is_active"] is True

    def test_delete_override_returns_404_for_missing_row(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
        override_id = str(uuid4())

        mock_db = MagicMock()
        delete_query = mock_db.schema.return_value.table.return_value.delete.return_value.eq.return_value
        delete_response = MagicMock()
        delete_response.error = None
        delete_response.data = []
        delete_query.execute.return_value = delete_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.delete(
                f"/api/v1/admin/shows/networks-streaming/overrides/{override_id}",
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 404


class TestRefreshShow:
    def test_returns_404_for_unknown_show(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_returns_409_when_cast_refresh_missing_show_imdb_id(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        show_id = str(uuid4())
        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": show_id, "imdb_id": None, "external_ids": {}}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync.sync_show_cast.main") as p_show_cast:
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/refresh",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"targets": ["cast_credits"]},
                )

        assert response.status_code == 409
        assert "missing an IMDb ID" in response.json().get("detail", "")
        p_show_cast.assert_not_called()

    def test_refresh_stream_returns_409_when_cast_refresh_missing_show_imdb_id(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        show_id = str(uuid4())
        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": show_id, "imdb_id": None, "external_ids": {}}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync.sync_show_cast.main") as p_show_cast:
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/refresh/stream",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"targets": ["cast_credits"]},
                )

        assert response.status_code == 409
        assert "missing an IMDb ID" in response.json().get("detail", "")
        p_show_cast.assert_not_called()

    def test_calls_script_mains_for_targets(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        # Mock DB show exists
        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": str(uuid4()), "imdb_id": "tt1234567", "external_ids": {}}]
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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_refresh_stream_emits_heartbeat_and_request_id(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": str(uuid4())}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        show_id = str(uuid4())

        from api.routers.admin_show_sync import RefreshStepResult

        def slow_ok_step(step_key, fn, argv):
            time.sleep(0.05)
            return RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync.STREAM_HEARTBEAT_INTERVAL_SECONDS", 0.01):
                with patch("api.routers.admin_show_sync._run_script_step", side_effect=slow_ok_step):
                    with client.stream(
                        "POST",
                        f"/api/v1/admin/shows/{show_id}/refresh/stream",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "x-trr-request-id": "req-refresh-stream-1",
                        },
                        json={"targets": ["details"]},
                    ) as response:
                        assert response.status_code == 200
                        text = "\n".join(
                            line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                            for line in response.iter_lines()
                        )

        assert '"heartbeat": true' in text or '"heartbeat":true' in text
        assert '"request_id": "req-refresh-stream-1"' in text or '"request_id":"req-refresh-stream-1"' in text


class TestRefreshShowPhotosStream:
    def test_returns_401_without_auth(self, client):
        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post("/api/v1/admin/shows/00000000-0000-0000-0000-000000000000/refresh-photos/stream")
        assert response.status_code == 401

    def test_returns_404_for_unknown_show(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_season_scoped_refresh_uses_episode_appearances_only_for_cast_discovery(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
        show_id = str(uuid4())

        mock_db = MagicMock()
        query = MagicMock()
        query.select.return_value = query
        query.eq.return_value = query
        query.in_.return_value = query
        query.order.return_value = query
        query.limit.return_value = query

        show_resp = MagicMock()
        show_resp.error = None
        show_resp.data = [
            {
                "id": show_id,
                "name": "The Real Housewives of Test",
                "imdb_id": None,
                "tmdb_id": None,
                "external_ids": {},
            }
        ]
        appearances_resp = MagicMock()
        appearances_resp.error = None
        appearances_resp.data = []
        query.execute.side_effect = [show_resp, appearances_resp]

        mock_db.schema.return_value.table.return_value = query

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/shows/{show_id}/refresh-photos/stream",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "season_number": 6,
                    "skip_s3": True,
                    "skip_auto_count": True,
                    "skip_word_detection": True,
                },
            )

        assert response.status_code == 200
        table_calls = [str(call.args[0]) for call in mock_db.schema.return_value.table.call_args_list]
        assert "episode_appearances" in table_calls
        assert "show_cast" not in table_calls
        assert '"season_number": 6' in response.text or '"season_number":6' in response.text
        assert '"live_counts"' in response.text

    def test_refresh_photos_stream_echoes_request_id(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
        show_id = str(uuid4())

        mock_db = MagicMock()
        query = MagicMock()
        query.select.return_value = query
        query.eq.return_value = query
        query.in_.return_value = query
        query.order.return_value = query
        query.limit.return_value = query

        show_resp = MagicMock()
        show_resp.error = None
        show_resp.data = [
            {
                "id": show_id,
                "name": "The Real Housewives of Test",
                "imdb_id": None,
                "tmdb_id": None,
                "external_ids": {},
            }
        ]
        appearances_resp = MagicMock()
        appearances_resp.error = None
        appearances_resp.data = []
        query.execute.side_effect = [show_resp, appearances_resp]
        mock_db.schema.return_value.table.return_value = query

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/shows/{show_id}/refresh-photos/stream",
                headers={
                    "Authorization": f"Bearer {token}",
                    "x-trr-request-id": "req-refresh-photos-1",
                },
                json={
                    "season_number": 6,
                    "skip_s3": True,
                    "skip_auto_count": True,
                    "skip_word_detection": True,
                },
            )

        assert response.status_code == 200
        assert '"request_id": "req-refresh-photos-1"' in response.text or (
            '"request_id":"req-refresh-photos-1"' in response.text
        )
