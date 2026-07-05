"""Tests for admin show sync endpoints."""

from __future__ import annotations

import asyncio
import io
import json
import re
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi import Response
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


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _default_local_job_plane(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "local")
    monkeypatch.delenv("TRR_LONG_JOB_ENFORCE_REMOTE", raising=False)
    monkeypatch.delenv("TRR_MODAL_ENABLED", raising=False)


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
        show_id = str(uuid4())
        mock_result = MagicMock(
            created=1,
            updated=2,
            skipped=3,
            upserted_show_rows=[{"id": show_id, "name": "Summer House"}],
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync.collect_candidates_from_lists",
                return_value=mock_candidates,
            ):
                with patch(
                    "api.routers.admin_show_sync.upsert_candidates_into_supabase",
                    return_value=mock_result,
                ):
                    with patch("api.routers.admin_show_sync.ShowRefreshOrchestrator") as orchestrator_cls:
                        orchestrator = orchestrator_cls.return_value
                        orchestrator.create_operations.return_value = ("operation-1", [{"id": "sub-1"}])
                        orchestrator.get_waves.return_value = [[{"id": "sub-1", "request_payload": {}}]]
                        response = client.post(
                            "/api/v1/admin/shows/sync-from-lists",
                            headers={"Authorization": f"Bearer {token}"},
                            json={
                                "imdb_lists": ["https://www.imdb.com/list/ls1234567890/"],
                                "tmdb_lists": ["8301263"],
                            },
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
        assert data["auto_refresh_operations"] == [
            {"show_id": show_id, "operation_id": "operation-1", "targets": ["show_core"]}
        ]
        assert data["auto_refresh_paused"] is False

    def test_sync_from_lists_respects_global_auto_refresh_pause(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("TMDB_API_KEY", "tmdb-key")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        mock_candidates = [MagicMock()]
        show_id = str(uuid4())
        mock_result = MagicMock(
            created=0,
            updated=1,
            skipped=0,
            upserted_show_rows=[{"id": show_id, "name": "Summer House"}],
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync.collect_candidates_from_lists", return_value=mock_candidates):
                with patch("api.routers.admin_show_sync.upsert_candidates_into_supabase", return_value=mock_result):
                    with patch(
                        "api.routers.admin_show_sync.admin_runtime_settings.show_core_auto_refresh_paused",
                        return_value=True,
                    ):
                        with patch("api.routers.admin_show_sync.ShowRefreshOrchestrator") as orchestrator_cls:
                            response = client.post(
                                "/api/v1/admin/shows/sync-from-lists",
                                headers={"Authorization": f"Bearer {token}"},
                                json={"tmdb_lists": ["8301263"]},
                            )

        assert response.status_code == 200
        data = response.json()
        assert data["auto_refresh_paused"] is True
        assert data["auto_refresh_operations"] == []
        orchestrator_cls.assert_not_called()

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
        mock_result = MagicMock(created=0, updated=1, skipped=0, upserted_show_rows=[])
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


def test_get_show_core_auto_refresh_settings(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=MagicMock()):
        with patch(
            "api.routers.admin_show_sync.admin_runtime_settings.get_show_core_auto_refresh_settings",
            return_value={"paused": True, "updated_at": "2026-06-10T20:00:00+00:00", "updated_by": "admin"},
        ):
            response = client.get(
                "/api/v1/admin/shows/settings/show-core-auto-refresh",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "paused": True,
        "updated_at": "2026-06-10T20:00:00+00:00",
        "updated_by": "admin",
    }


def test_update_show_core_auto_refresh_settings(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef", subject="admin-user")

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=MagicMock()):
        with patch(
            "api.routers.admin_show_sync.admin_runtime_settings.set_show_core_auto_refresh_paused",
            return_value={"paused": True, "updated_at": "2026-06-10T20:00:00+00:00", "updated_by": "admin-user"},
        ) as set_paused:
            response = client.put(
                "/api/v1/admin/shows/settings/show-core-auto-refresh",
                headers={"Authorization": f"Bearer {token}"},
                json={"paused": True},
            )

    assert response.status_code == 200
    assert response.json()["paused"] is True
    assert set_paused.call_args.kwargs == {"paused": True, "updated_by": "service_role:unknown"}


def test_refresh_show_bravo_target_disables_cast_matrix_sync_for_unified_refresh() -> None:
    from api.routers.admin_show_sync import _refresh_show_bravo_target

    mock_db = MagicMock()
    show_id = str(uuid4())

    with patch("api.routers.admin_show_sync._show_is_bravo", return_value=True):
        with patch("api.routers.admin_show_sync.admin_show_bravo._assert_show_sync_ready_for_bravo"):
            with patch(
                "api.routers.admin_show_sync._resolve_show_official_page_url",
                return_value="https://www.bravotv.com/summer-house",
            ):
                with patch("api.routers.admin_show_sync.admin_show_bravo.commit_bravo_import") as commit_mock:
                    _refresh_show_bravo_target(
                        show_id=show_id,
                        show_row={"id": show_id, "name": "Summer House", "networks": ["Bravo"]},
                        db=mock_db,
                        admin_user={"id": "admin"},
                    )

    payload = commit_mock.call_args.kwargs["payload"]
    assert str(payload.show_url) == "https://www.bravotv.com/summer-house"
    assert payload.sync_cast_matrix is False


def test_cast_profiles_refresh_people_profiles_without_cast_matrix_sync() -> None:
    from api.routers.admin_show_sync import _run_cast_person_refresh_stage

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    with patch(
        "api.routers.admin_show_sync._list_refresh_cast_members",
        return_value=[{"person_id": person_id, "person_name": "Carl Radke"}],
    ):
        with patch("api.routers.admin_show_sync.admin_show_roles.sync_cast_matrix_for_show") as sync_mock:
            with patch(
                "api.routers.admin_show_sync.admin_person_profile._run_person_profile_refresh",
                return_value={"status": "ok", "failures": [], "skips": []},
            ) as profile_mock:
                with patch("api.routers.admin_show_sync.admin_person_images.refresh_person_images") as images_mock:
                    result = _run_cast_person_refresh_stage(
                        show_id=show_id,
                        show_row={"id": show_id, "name": "Summer House", "networks": ["Bravo"]},
                        db=mock_db,
                        admin_user={"id": "admin@example.com"},
                        mode="profile_only",
                    )

    assert result.status == "success"
    sync_mock.assert_not_called()
    images_mock.assert_not_called()
    refresh_payload = profile_mock.call_args.kwargs["payload"]
    assert refresh_payload.refresh_links is False
    assert refresh_payload.refresh_credits is False
    assert profile_mock.call_args.kwargs["person_id"] == person_id


def test_cast_media_refresh_uses_only_allowed_show_level_sources() -> None:
    from api.routers.admin_show_sync import _run_cast_person_refresh_stage

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    with patch(
        "api.routers.admin_show_sync._list_refresh_cast_members",
        return_value=[{"person_id": person_id, "person_name": "Paige DeSorbo"}],
    ):
        with patch("api.routers.admin_show_sync.admin_person_images.refresh_person_images") as images_mock:
            result = _run_cast_person_refresh_stage(
                show_id=show_id,
                show_row={"id": show_id, "name": "Summer House", "networks": ["Bravo"]},
                db=mock_db,
                admin_user={"id": "admin"},
                mode="media_only",
            )

    assert result.status == "success"
    request = images_mock.call_args.kwargs["request"]
    assert request.sources == ["imdb", "tmdb", "nbcumv"]


def test_official_images_target_filters_to_nbc_family_shows() -> None:
    from api.routers.admin_show_sync import _targets_for_show_row

    targets = ["show_core", "official_images"]

    assert _targets_for_show_row(targets, show_row={"networks": ["Bravo"]}) == ["show_core", "official_images"]
    assert _targets_for_show_row(targets, show_row={"streaming_providers": ["Peacock"]}) == [
        "show_core",
        "official_images",
    ]
    assert _targets_for_show_row(targets, show_row={"networks": ["Netflix"]}) == ["show_core"]


def test_official_images_refresh_enqueues_show_and_cast_nbcumv_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    from api.routers import admin_show_sync

    show_id = str(uuid4())
    person_id = str(uuid4())
    enqueued: list[dict[str, object]] = []

    monkeypatch.setattr(
        admin_show_sync,
        "_resolve_nbcumv_show_catalog",
        lambda _show_name: {
            "matched": True,
            "nbcumv_show_id": "nbc-show-1",
            "asset_count": 2,
            "fingerprint": "fingerprint-1",
            "sample_asset_ids": ["asset-1", "asset-2"],
        },
    )
    monkeypatch.setattr(admin_show_sync.admin_runtime_settings, "get_runtime_setting", lambda _key: {})
    monkeypatch.setattr(admin_show_sync.admin_runtime_settings, "set_runtime_setting", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        admin_show_sync,
        "_list_refresh_cast_members",
        lambda *, show_id, db: [{"person_id": person_id, "person_name": "Cast Member"}],
    )

    def fake_enqueue(**kwargs):
        enqueued.append(dict(kwargs))
        return {"operation_id": f"op-{len(enqueued)}", "mode": kwargs["mode"], "person_id": kwargs.get("person_id")}

    monkeypatch.setattr(admin_show_sync, "_enqueue_bravotv_nbcumv_image_run", fake_enqueue)

    result = admin_show_sync._run_official_images_refresh_stage(
        show_id=show_id,
        show_row={"id": show_id, "name": "Love Island USA", "streaming_providers": ["Peacock"]},
        db=MagicMock(),
        admin_user={"id": "admin"},
    )

    assert result.status == "success"
    assert [item["mode"] for item in enqueued] == ["show", "person"]
    assert enqueued[0]["show_id"] == show_id
    assert enqueued[1]["person_id"] == person_id
    assert json.loads(result.output or "{}")["enqueued"] == 2


def test_official_images_refresh_skips_unchanged_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    from api.routers import admin_show_sync

    monkeypatch.setattr(
        admin_show_sync,
        "_resolve_nbcumv_show_catalog",
        lambda _show_name: {
            "matched": True,
            "nbcumv_show_id": "nbc-show-1",
            "asset_count": 2,
            "fingerprint": "same-fingerprint",
        },
    )
    monkeypatch.setattr(
        admin_show_sync.admin_runtime_settings,
        "get_runtime_setting",
        lambda _key: {"fingerprint": "same-fingerprint"},
    )
    enqueue_mock = MagicMock()
    monkeypatch.setattr(admin_show_sync, "_enqueue_bravotv_nbcumv_image_run", enqueue_mock)

    result = admin_show_sync._run_official_images_refresh_stage(
        show_id=str(uuid4()),
        show_row={"name": "Love Island USA", "streaming_providers": ["Peacock"]},
        db=MagicMock(),
        admin_user={"id": "admin"},
    )

    assert result.status == "skipped"
    assert result.skip_reason == "NBCUMV catalog unchanged."
    enqueue_mock.assert_not_called()


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
        from api.routers.admin_show_sync import SyncNetworksStreamingStepResult

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

        def fake_step_with_metrics(name, _fn, argv, _metric_keys):
            if name == "tmdb_show_entities":
                received["entities"] = list(argv or [])
                return (
                    SyncNetworksStreamingStepResult(
                        status="success",
                        duration_ms=1,
                        exit_code=0,
                        metrics={
                            "networks_upserted": 7,
                            "production_companies_upserted": 5,
                            "logos_mirrored": 2,
                            "failures": 0,
                        },
                    ),
                    "",
                )
            if name == "tmdb_watch_providers":
                received["providers"] = list(argv or [])
                return (
                    SyncNetworksStreamingStepResult(
                        status="success",
                        duration_ms=1,
                        exit_code=0,
                        metrics={
                            "providers_upserted": 12,
                            "show_watch_providers_upserted": 20,
                            "logos_mirrored": 3,
                            "failures": 1,
                        },
                    ),
                    "",
                )
            if name == "network_streaming_links":
                received["links"] = list(argv or [])
                return (
                    SyncNetworksStreamingStepResult(
                        status="success",
                        duration_ms=1,
                        exit_code=0,
                        metrics={
                            "processed": 30,
                            "links_enriched": 18,
                            "wikidata_linked": 10,
                            "wikipedia_linked": 9,
                            "logos_mirrored": 4,
                            "variants_black_mirrored": 3,
                            "variants_white_mirrored": 2,
                            "logo_assets_discovered": 25,
                            "logo_assets_mirrored": 14,
                            "logo_assets_skipped": 8,
                            "logo_assets_failed": 3,
                            "completion_total": 40,
                            "completion_resolved": 39,
                            "completion_unresolved": 1,
                            "completion_unresolved_total": 1,
                            "completion_unresolved_network": 1,
                            "completion_unresolved_streaming": 0,
                            "completion_unresolved_production": 0,
                            "production_missing_logos": 6,
                            "production_missing_bw_variants": 6,
                            "unresolved_logos": 2,
                            "failures": 2,
                        },
                    ),
                    "\n".join(
                        [
                            "run_id=network-streaming-20260224T210000Z",
                            "run_status=stopped",
                            "resume_cursor_entity_type=network",
                            "resume_cursor_entity_key=bravo",
                            "completion_percent=97.50",
                            'unresolved_logo={"type":"network","id":"77","name":"Bravo","reason":"no_logo_claim"}',
                            'unresolved_logo={"type":"streaming","id":"531","name":"Peacock","reason":"download_failed"}',
                        ]
                    ),
                )
            if name == "show_logos":
                received["show_logos"] = list(argv or [])
                return (
                    SyncNetworksStreamingStepResult(
                        status="success",
                        duration_ms=1,
                        exit_code=0,
                        metrics={
                            "show_logos_discovered": 16,
                            "show_logos_imported": 9,
                            "show_logos_skipped": 4,
                            "show_logo_failures": 3,
                            "failures": 0,
                        },
                    ),
                    "",
                )
            raise AssertionError(f"unexpected step {name}")

        with patch("api.routers.admin_show_sync._schema_preflight_missing_columns", return_value=[]):
            with patch("api.routers.admin_show_sync._run_script_step_with_metrics", side_effect=fake_step_with_metrics):
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
                                with patch(
                                    "api.routers.admin_show_sync._run_brand_family_wikipedia_import_step",
                                    return_value=SyncNetworksStreamingStepResult(
                                        status="success",
                                        duration_ms=1,
                                        exit_code=0,
                                        metrics={
                                            "families_total": 0,
                                            "families_processed": 0,
                                            "wikipedia_rows_imported": 0,
                                            "wikipedia_rows_matched": 0,
                                            "rules_upserted": 0,
                                            "fetch_errors": 0,
                                            "skipped_dry_run": 0,
                                        },
                                    ),
                                ):
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
                with patch(
                    "api.routers.admin_show_sync._run_brand_family_wikipedia_import_step",
                    return_value=SyncNetworksStreamingStepResult(
                        status="success",
                        duration_ms=1,
                        exit_code=0,
                        metrics={
                            "families_total": 0,
                            "families_processed": 0,
                            "wikipedia_rows_imported": 0,
                            "wikipedia_rows_matched": 0,
                            "rules_upserted": 0,
                            "fetch_errors": 0,
                            "skipped_dry_run": 0,
                        },
                    ),
                ):
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
        from api.routers.admin_show_sync import SyncNetworksStreamingStepResult

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

        def fake_step_with_metrics(name, _fn, _argv, _metric_keys):
            if name == "tmdb_show_entities":
                return (
                    SyncNetworksStreamingStepResult(
                        status="failed",
                        duration_ms=1,
                        exit_code=2,
                        metrics={"failures": 4},
                    ),
                    "",
                )
            if name == "tmdb_watch_providers":
                return (
                    SyncNetworksStreamingStepResult(
                        status="success",
                        duration_ms=1,
                        exit_code=0,
                        metrics={"providers_upserted": 1, "logos_mirrored": 0, "failures": 0},
                    ),
                    "",
                )
            if name == "network_streaming_links":
                return (
                    SyncNetworksStreamingStepResult(
                        status="success",
                        duration_ms=1,
                        exit_code=0,
                        metrics={
                            "links_enriched": 1,
                            "logos_mirrored": 0,
                            "completion_total": 2,
                            "completion_resolved": 2,
                            "completion_unresolved": 0,
                            "completion_percent": 100.0,
                            "failures": 0,
                        },
                    ),
                    "",
                )
            if name == "show_logos":
                return (
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
                )
            raise AssertionError(f"unexpected step {name}")

        with patch("api.routers.admin_show_sync._schema_preflight_missing_columns", return_value=[]):
            with patch("api.routers.admin_show_sync._run_script_step_with_metrics", side_effect=fake_step_with_metrics):
                with patch("api.routers.admin_show_sync.sync_tmdb_show_entities.main", side_effect=bad_entities):
                    with patch("api.routers.admin_show_sync.sync_tmdb_watch_providers.main", side_effect=ok_providers):
                        with patch(
                            "api.routers.admin_show_sync.sync_networks_streaming_links.main",
                            side_effect=ok_links,
                        ):
                            with patch("api.routers.admin_show_sync.sync_show_logos.main", side_effect=ok_show_logos):
                                with patch(
                                    "api.routers.admin_show_sync._run_brand_family_wikipedia_import_step",
                                    return_value=SyncNetworksStreamingStepResult(
                                        status="success",
                                        duration_ms=1,
                                        exit_code=0,
                                        metrics={
                                            "families_total": 0,
                                            "families_processed": 0,
                                            "wikipedia_rows_imported": 0,
                                            "wikipedia_rows_matched": 0,
                                            "rules_upserted": 0,
                                            "fetch_errors": 0,
                                            "skipped_dry_run": 0,
                                        },
                                    ),
                                ):
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
            with patch("api.routers.admin_show_sync._maybe_reload_postgrest_schema_cache") as p_reload_cache:
                with patch("api.routers.admin_show_sync.sync_shows.main", return_value=0) as p_sync_shows:
                    with patch(
                        "api.routers.admin_show_sync.sync_tmdb_show_entities.main",
                        return_value=0,
                    ) as p_tmdb_entities:
                        with patch(
                            "api.routers.admin_show_sync.sync_tmdb_watch_providers.main",
                            return_value=0,
                        ) as p_watch_providers:
                            with patch(
                                "api.routers.admin_show_sync.sync_seasons_episodes.main",
                                return_value=0,
                            ) as p_seasons:
                                with patch(
                                    "api.routers.admin_show_sync.sync_show_images.main",
                                    return_value=0,
                                ) as p_show_images:
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
        p_reload_cache.assert_called_once_with(False)
        p_sync_shows.assert_called()
        p_tmdb_entities.assert_called()
        p_watch_providers.assert_called()
        p_seasons.assert_called()
        p_show_images.assert_called()
        p_season_images.assert_called()
        p_show_cast.assert_called()
        p_occurrences.assert_called()

    def test_calls_inline_refresh_helpers_for_extended_targets(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [
            {
                "id": show_id,
                "name": "The Real Housewives of Beverly Hills",
                "networks": ["Bravo"],
                "imdb_id": "tt1234567",
                "external_ids": {},
            }
        ]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync._refresh_show_videos_target",
                return_value=0,
            ) as p_videos:
                with patch(
                    "api.routers.admin_show_sync._refresh_show_news_target",
                    return_value=0,
                ) as p_news:
                    with patch(
                        "api.routers.admin_show_sync._refresh_show_social_setup_target",
                        return_value=0,
                    ) as p_social:
                        response = client.post(
                            f"/api/v1/admin/shows/{show_id}/refresh",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"targets": ["videos", "news", "social_setup"]},
                        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["targets"] == ["videos", "news", "social_setup"]
        assert payload["results"]["videos"]["status"] == "success"
        assert payload["results"]["news"]["status"] == "success"
        assert payload["results"]["social_setup"]["status"] == "success"
        p_videos.assert_called_once()
        p_news.assert_called_once()
        p_social.assert_called_once()

    def test_refresh_supports_unified_targets(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [
            {
                "id": show_id,
                "name": "The Real Housewives of Beverly Hills",
                "networks": ["Bravo"],
                "imdb_id": "tt1234567",
                "external_ids": {},
            }
        ]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)
        skipped_result = RefreshStepResult(
            status="skipped",
            duration_ms=1,
            exit_code=0,
            error="Show is not Bravo-eligible.",
            skip_reason="Show is not Bravo-eligible.",
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_details_refresh_steps", return_value={"details": ok_result}):
                with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result):
                    with patch("api.routers.admin_show_sync._refresh_show_social_setup_target", return_value=0):
                        with patch(
                            "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_show_seasons_episodes",
                            return_value=1,
                        ):
                            with patch(
                                "api.routers.admin_show_sync._refresh_show_links_target",
                                return_value={"status": "ok"},
                            ):
                                with patch(
                                    "api.routers.admin_show_sync._run_cast_person_refresh_stage",
                                    side_effect=[ok_result, ok_result],
                                ) as p_cast:
                                    with patch(
                                        "api.routers.admin_show_sync._run_inline_step",
                                        side_effect=[ok_result, ok_result, ok_result, skipped_result],
                                    ):
                                        response = client.post(
                                            f"/api/v1/admin/shows/{show_id}/refresh",
                                            headers={"Authorization": f"Bearer {token}"},
                                            json={
                                                "targets": [
                                                    "show_core",
                                                    "links",
                                                    "bravo",
                                                    "cast_profiles",
                                                    "cast_media",
                                                ]
                                            },
                                        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["targets"] == ["show_core", "links", "bravo", "cast_profiles", "cast_media"]
        assert payload["results"]["show_core"]["status"] == "success"
        assert payload["results"]["links"]["status"] == "success"
        assert payload["results"]["bravo"]["status"] == "skipped"
        assert payload["results"]["cast_profiles"]["status"] == "success"
        assert payload["results"]["cast_media"]["status"] == "success"
        assert p_cast.call_count == 2

    def test_refresh_show_core_runs_reconcile_after_split_seasons_and_episodes(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [
            {
                "id": show_id,
                "name": "Summer House",
                "networks": ["Bravo"],
                "imdb_id": "tt1234567",
                "external_ids": {},
            }
        ]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_details_refresh_steps", return_value={"details": ok_result}):
                with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result):
                    with patch("api.routers.admin_show_sync._refresh_show_social_setup_target", return_value=0):
                        with patch(
                            "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_show_seasons_episodes",
                            return_value=1,
                        ) as reconcile:
                            with patch(
                                "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_missing_episode_imdb_ids",
                                return_value=1,
                            ) as reconcile_ids:
                                response = client.post(
                                    f"/api/v1/admin/shows/{show_id}/refresh",
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={"targets": ["show_core"]},
                                )

        assert response.status_code == 200
        payload = response.json()
        assert payload["results"]["show_core"]["status"] == "success"
        assert payload["results"]["show_core_reconcile"]["status"] == "success"
        reconcile_ids.assert_called_once_with(mock_db, show_ids=[show_id], verbose=False)
        reconcile.assert_called_once_with(mock_db, show_ids=[show_id], verbose=False)

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
        assert '"operation_id"' in text
        event_seq_matches = [int(match) for match in re.findall(r'"event_seq"\s*:\s*(\d+)', text)]
        assert event_seq_matches
        assert event_seq_matches == sorted(event_seq_matches)
        assert len(event_seq_matches) == len(set(event_seq_matches))

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

    def test_refresh_stream_runs_reconcile_after_seasons_and_episodes(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [{"id": show_id, "imdb_id": "tt1234567", "external_ids": {}}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result):
                with patch(
                    "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_show_seasons_episodes",
                    return_value=1,
                ) as reconcile:
                    with patch(
                        "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_missing_episode_imdb_ids",
                        return_value=1,
                    ) as reconcile_ids:
                        with client.stream(
                            "POST",
                            f"/api/v1/admin/shows/{show_id}/refresh/stream",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"targets": ["seasons_episodes"]},
                        ) as response:
                            assert response.status_code == 200
                            text = "\n".join(
                                line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                                for line in response.iter_lines()
                            )

        reconcile_ids.assert_called_once_with(mock_db, show_ids=[show_id], verbose=False)
        reconcile.assert_called_once_with(mock_db, show_ids=[show_id], verbose=False)
        assert '"stage_key": "seasons_episodes_reconcile"' in text

    def test_refresh_stream_show_core_runs_reconcile_after_split_steps(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [
            {
                "id": show_id,
                "name": "Summer House",
                "networks": ["Bravo"],
                "imdb_id": "tt1234567",
                "external_ids": {},
            }
        ]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result):
                with patch("api.routers.admin_show_sync._refresh_show_social_setup_target", return_value=0):
                    with patch(
                        "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_show_seasons_episodes",
                        return_value=1,
                    ) as reconcile:
                        with patch(
                            "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_missing_episode_imdb_ids",
                            return_value=1,
                        ) as reconcile_ids:
                            with client.stream(
                                "POST",
                                f"/api/v1/admin/shows/{show_id}/refresh/stream",
                                headers={"Authorization": f"Bearer {token}"},
                                json={"targets": ["show_core"]},
                            ) as response:
                                assert response.status_code == 200
                                text = "\n".join(
                                    line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                                    for line in response.iter_lines()
                                )

        reconcile_ids.assert_called_once_with(mock_db, show_ids=[show_id], verbose=False)
        reconcile.assert_called_once_with(mock_db, show_ids=[show_id], verbose=False)
        assert '"stage_key": "show_core_reconcile"' in text

    def test_refresh_stream_skips_reconcile_when_seasons_step_fails(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [{"id": show_id, "imdb_id": "tt1234567", "external_ids": {}}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)
        failed_result = RefreshStepResult(
            status="failed",
            duration_ms=1,
            exit_code=1,
            error="non-zero exit code: 1",
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_script_step", side_effect=[ok_result, failed_result]):
                with patch(
                    "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_show_seasons_episodes",
                    return_value=1,
                ) as reconcile:
                    with patch(
                        "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_missing_episode_imdb_ids",
                        return_value=1,
                    ) as reconcile_ids:
                        with client.stream(
                            "POST",
                            f"/api/v1/admin/shows/{show_id}/refresh/stream",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"targets": ["seasons_episodes"]},
                        ) as response:
                            assert response.status_code == 200
                            text = "\n".join(
                                line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                                for line in response.iter_lines()
                            )

        reconcile_ids.assert_not_called()
        reconcile.assert_not_called()
        assert '"stage_key": "seasons_episodes_reconcile"' in text
        assert '"step_status": "skipped"' in text

    def test_refresh_stream_uses_fullcredits_stage_keys_for_cast_credits(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": str(uuid4()), "imdb_id": "tt1234567", "external_ids": {}}]
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
                    json={"targets": ["cast_credits"]},
                ) as response:
                    assert response.status_code == 200
                    text = "\n".join(
                        line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                        for line in response.iter_lines()
                    )

        called_step_keys = [call.args[0] for call in run_step.call_args_list]
        assert called_step_keys == ["credits_fullcredits_sync", "credits_episode_appearances_sync"]
        assert '"stage_key": "credits_fullcredits_sync"' in text
        assert '"stage_key": "credits_episode_appearances_sync"' in text

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

    def test_refresh_stream_supports_extended_targets(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [{"id": show_id, "name": "Summer House", "networks": ["Bravo"]}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result):
                with patch("api.routers.admin_show_sync._refresh_show_videos_target", return_value=0):
                    with patch("api.routers.admin_show_sync._refresh_show_news_target", return_value=0):
                        with patch("api.routers.admin_show_sync._refresh_show_social_setup_target", return_value=0):
                            with client.stream(
                                "POST",
                                f"/api/v1/admin/shows/{show_id}/refresh/stream",
                                headers={"Authorization": f"Bearer {token}"},
                                json={"targets": ["videos", "news", "social_setup"]},
                            ) as response:
                                assert response.status_code == 200
                                text = "\n".join(
                                    line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                                    for line in response.iter_lines()
                                )

        assert '"stage_key": "videos_bravo_import"' in text
        assert '"stage_key": "news_google_sync"' in text
        assert '"social_setup"' in text
        assert '"operation_id":' in text
        assert "event: error" not in text

    def test_refresh_stream_emits_unified_stage_topics_and_skip_reason(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [
            {
                "id": show_id,
                "name": "The Real Housewives of Beverly Hills",
                "networks": ["Bravo"],
                "imdb_id": "tt1234567",
                "external_ids": {},
            }
        ]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)
        skipped_result = RefreshStepResult(
            status="skipped",
            duration_ms=1,
            exit_code=0,
            error="Sync seasons, episodes, and cast before Bravo import (missing: cast).",
            skip_reason="Sync seasons, episodes, and cast before Bravo import (missing: cast).",
        )
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync._run_script_step", return_value=ok_result):
                with patch("api.routers.admin_show_sync._refresh_show_social_setup_target", return_value=0):
                    with patch(
                        "api.routers.admin_show_sync.sync_seasons_episodes.reconcile_show_seasons_episodes",
                        return_value=1,
                    ):
                        with patch(
                            "api.routers.admin_show_sync._refresh_show_links_target", return_value={"status": "ok"}
                        ):
                            with patch(
                                "api.routers.admin_show_sync._run_cast_person_refresh_stage",
                                return_value=ok_result,
                            ):
                                with patch(
                                    "api.routers.admin_show_sync._run_inline_step",
                                    side_effect=[ok_result, ok_result, ok_result, skipped_result],
                                ):
                                    with client.stream(
                                        "POST",
                                        f"/api/v1/admin/shows/{show_id}/refresh/stream",
                                        headers={"Authorization": f"Bearer {token}"},
                                        json={"targets": ["show_core", "links", "bravo"]},
                                    ) as response:
                                        assert response.status_code == 200
                                        text = "\n".join(
                                            line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                                            for line in response.iter_lines()
                                        )

        assert '"topic": "show_core"' in text
        assert '"topic": "links"' in text
        assert '"topic": "bravo"' in text
        assert '"pipeline_stage": "show_core"' in text
        assert '"pipeline_stage": "links"' in text
        assert '"pipeline_stage": "bravo"' in text
        assert '"skip_reason": "Sync seasons, episodes, and cast before Bravo import (missing: cast)."' in text

    def test_refresh_supports_credits_pipeline_target(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [
            {
                "id": show_id,
                "name": "The Real Housewives of Beverly Hills",
                "networks": ["Bravo"],
                "imdb_id": "tt1234567",
                "external_ids": {},
            }
        ]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync._run_credits_sync_phase",
                return_value=(
                    ok_result,
                    {
                        "credits_fullcredits_sync": ok_result,
                        "credits_episode_appearances_sync": ok_result,
                    },
                ),
            ):
                with patch(
                    "api.routers.admin_show_sync._run_profile_links_sync_phase",
                    return_value=ok_result,
                ):
                    with patch(
                        "api.routers.admin_show_sync._run_bio_sync_phase",
                        return_value=(ok_result, []),
                    ):
                        with patch(
                            "api.routers.admin_show_sync._run_network_augmentation_phase",
                            return_value=ok_result,
                        ):
                            with patch(
                                "api.routers.admin_show_sync._run_media_ingest_phase",
                                return_value=ok_result,
                            ):
                                response = client.post(
                                    f"/api/v1/admin/shows/{show_id}/refresh",
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={"targets": ["credits_pipeline"]},
                                )

        assert response.status_code == 200
        payload = response.json()
        assert payload["targets"] == ["credits_pipeline"]
        assert payload["results"]["credits_pipeline"]["status"] == "success"
        assert payload["results"]["credits_sync"]["status"] == "success"
        assert payload["results"]["profile_links_sync"]["status"] == "success"
        assert payload["results"]["bio_sync"]["status"] == "success"
        assert payload["results"]["network_augmentation"]["status"] == "success"
        assert payload["results"]["media_ingest"]["status"] == "success"

    def test_refresh_stream_emits_credits_pipeline_topics(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [
            {
                "id": show_id,
                "name": "The Real Housewives of Beverly Hills",
                "networks": ["Bravo"],
                "imdb_id": "tt1234567",
                "external_ids": {},
            }
        ]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        from api.routers.admin_show_sync import RefreshStepResult

        ok_result = RefreshStepResult(status="success", duration_ms=1, exit_code=0, error=None)

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync._run_credits_sync_phase",
                return_value=(
                    ok_result,
                    {
                        "credits_fullcredits_sync": ok_result,
                        "credits_episode_appearances_sync": ok_result,
                    },
                ),
            ):
                with patch(
                    "api.routers.admin_show_sync._run_profile_links_sync_phase",
                    return_value=ok_result,
                ):
                    with patch(
                        "api.routers.admin_show_sync._run_bio_sync_phase",
                        return_value=(ok_result, []),
                    ):
                        with patch(
                            "api.routers.admin_show_sync._run_network_augmentation_phase",
                            return_value=ok_result,
                        ):
                            with patch(
                                "api.routers.admin_show_sync._run_media_ingest_phase",
                                return_value=ok_result,
                            ):
                                with client.stream(
                                    "POST",
                                    f"/api/v1/admin/shows/{show_id}/refresh/stream",
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={"targets": ["credits_pipeline"]},
                                ) as response:
                                    assert response.status_code == 200
                                    text = "\n".join(
                                        line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                                        for line in response.iter_lines()
                                    )

        assert '"stage_key": "credits_sync"' in text
        assert '"stage_key": "profile_links_sync"' in text
        assert '"stage_key": "bio_sync"' in text
        assert '"stage_key": "network_augmentation"' in text
        assert '"stage_key": "media_ingest"' in text
        assert '"pipeline_stage": "credits_sync"' in text
        assert '"pipeline_stage": "profile_links_sync"' in text
        assert '"pipeline_stage": "bio_sync"' in text
        assert '"pipeline_stage": "network_augmentation"' in text
        assert '"pipeline_stage": "media_ingest"' in text

    def test_refresh_stream_disables_attach_for_explicit_rerun(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [{"id": show_id, "imdb_id": "tt1234567", "external_ids": {}}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync.start_operation_for_stream",
                return_value={"id": str(uuid4())},
            ) as start_operation:
                with patch("api.routers.admin_show_sync.operation_stream_response", return_value=Response("ok")):
                    response = client.post(
                        f"/api/v1/admin/shows/{show_id}/refresh/stream",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"targets": ["show_core"], "force_new_operation": True},
                    )

        assert response.status_code == 200
        assert start_operation.call_args.kwargs["allow_attach"] is False

    def test_refresh_stream_blocks_automatic_show_core_when_global_pause_enabled(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_id = str(uuid4())
        show_resp = MagicMock()
        show_resp.data = [{"id": show_id, "imdb_id": "tt1234567", "external_ids": {}}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_show_sync.admin_runtime_settings.show_core_auto_refresh_paused",
                return_value=True,
            ):
                with patch("api.routers.admin_show_sync.start_operation_for_stream") as start_operation:
                    response = client.post(
                        f"/api/v1/admin/shows/{show_id}/refresh/stream",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"targets": ["show_core"], "force_new_operation": True, "auto_refresh": True},
                    )

        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "SHOW_CORE_AUTO_REFRESH_PAUSED"
        start_operation.assert_not_called()

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

    def test_refresh_stream_surfaces_live_script_output_for_cast_credits(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        show_resp = MagicMock()
        show_resp.data = [{"id": str(uuid4()), "imdb_id": "tt1234567", "external_ids": {}}]
        show_resp.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = show_resp

        show_id = str(uuid4())

        from api.routers.admin_show_sync import RefreshStepResult

        def fake_script_step(name, _fn, _argv, *, on_output_line=None):
            output_by_name = {
                "credits_fullcredits_sync": "Fetching IMDb Full Credits for live updates...",
                "credits_episode_appearances_sync": "Syncing episode appearances for live updates...",
            }
            if on_output_line is not None:
                on_output_line(output_by_name[name])
            return RefreshStepResult(status="success", duration_ms=1, exit_code=0, output=output_by_name[name])

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_show_sync.STREAM_HEARTBEAT_INTERVAL_SECONDS", 0.01):
                with patch("api.routers.admin_show_sync._run_script_step", side_effect=fake_script_step):
                    with client.stream(
                        "POST",
                        f"/api/v1/admin/shows/{show_id}/refresh/stream",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"targets": ["cast_credits"]},
                    ) as response:
                        assert response.status_code == 200
                        text = "\n".join(
                            line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else str(line)
                            for line in response.iter_lines()
                        )

        assert "Fetching IMDb Full Credits for live updates..." in text
        assert "Syncing episode appearances for live updates..." in text
        assert '"stage_key": "credits_fullcredits_sync"' in text
        assert '"stage_key": "credits_episode_appearances_sync"' in text

    def test_run_script_step_captures_real_sync_script_output_in_subprocess(self, monkeypatch):
        from api.routers import admin_show_sync

        popen_calls: list[list[str]] = []

        class FakeProc:
            stdout = io.StringIO("line from stdout\n")
            stderr = io.StringIO("line from stderr\n")

            def wait(self) -> int:
                return 0

        def fake_popen(command, **_kwargs):
            popen_calls.append(command)
            return FakeProc()

        lines: list[str] = []
        monkeypatch.setattr(admin_show_sync.subprocess, "Popen", fake_popen)

        result = admin_show_sync._run_script_step(
            "credits_fullcredits_sync",
            admin_show_sync.sync_show_cast.main,
            ["--show-id", "show-1", "--force"],
            on_output_line=lines.append,
        )

        assert result.status == "success"
        assert result.output == "line from stdout\nline from stderr"
        assert lines == ["line from stdout", "line from stderr"]
        assert popen_calls == [
            [
                admin_show_sync.sys.executable,
                "-m",
                "scripts.sync.sync_show_cast",
                "--show-id",
                "show-1",
                "--force",
            ]
        ]


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
        assert '"operation_id"' in response.text
        event_seq_matches = [int(match) for match in re.findall(r'"event_seq"\s*:\s*(\d+)', response.text)]
        assert event_seq_matches
        assert event_seq_matches == sorted(event_seq_matches)
        assert len(event_seq_matches) == len(set(event_seq_matches))

    def test_skip_cast_photos_runs_gallery_only_stream(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "test-bucket")
        monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")
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
        empty_resp = MagicMock()
        empty_resp.error = None
        empty_resp.data = []
        query.execute.side_effect = [show_resp] + [empty_resp] * 20
        mock_db.schema.return_value.table.return_value = query

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/shows/{show_id}/refresh-photos/stream",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "skip_cast_photos": True,
                },
            )

        assert response.status_code == 200
        assert "Skipping cast photos (skip_cast_photos=true)." in response.text
        assert "Skipping cast photo mirroring (skip_cast_photos=true)." in response.text
        assert "Skipping cast photo prune (skip_cast_photos=true)." in response.text
        assert "Skipping auto-count (skip_cast_photos=true)." in response.text
        assert "Skipping word detection (skip_cast_photos=true)." in response.text
        assert (
            '"skip_reason": "skip_cast_photos"' in response.text or '"skip_reason":"skip_cast_photos"' in response.text
        )
        assert '"live_counts"' in response.text

        table_calls = [str(call.args[0]) for call in mock_db.schema.return_value.table.call_args_list]
        assert "episode_appearances" not in table_calls
        assert "show_cast" not in table_calls
        assert "people" not in table_calls
        assert "cast_photos" not in table_calls


def test_retry_refresh_target_reuses_parent_refresh_flags_in_child_payload() -> None:
    from api.routers.admin_show_sync import retry_refresh_target

    class _Request:
        headers = {"x-trr-request-id": "req-retry-1"}

    async def _run():
        with (
            patch(
                "api.routers.admin_show_sync.admin_operations_repo.get_operation",
                return_value={
                    "id": "parent-1",
                    "request_payload": {
                        "show_id": "show-123",
                        "request_id": "req-parent-1",
                        "initiated_by": "admin@example.com",
                        "payload": {
                            "targets": ["show_core", "links", "cast_media"],
                            "skip_s3": True,
                            "verbose": True,
                            "reload_schema_cache": True,
                            "force_new_operation": False,
                        },
                    },
                },
            ),
            patch(
                "api.routers.admin_show_sync.admin_operations_repo.create_sub_operation",
                return_value={
                    "id": "child-1",
                    "request_payload": {},
                },
            ) as create_sub_operation,
            patch(
                "api.routers.admin_show_sync.supports_admin_operation",
                return_value=False,
            ),
            patch(
                "api.routers.admin_show_sync.build_show_refresh_operation_producer",
                return_value="producer",
            ),
            patch(
                "api.routers.admin_show_sync.ensure_operation_execution",
            ),
            patch(
                "api.routers.admin_show_sync.admin_operations_repo.update_operation_status",
            ),
        ):
            await retry_refresh_target(
                show_id="show-123",
                target="cast_media",
                request=_Request(),
                payload={"parent_operation_id": "parent-1"},
                db=MagicMock(),
            )
        return create_sub_operation.call_args.kwargs["request_payload"]

    child_payload = asyncio.run(_run())
    assert child_payload == {
        "show_id": "show-123",
        "request_id": "req-retry-1",
        "initiated_by": "admin@example.com",
        "payload": {
            "targets": ["cast_media"],
            "skip_s3": True,
            "verbose": True,
            "reload_schema_cache": True,
            "force_new_operation": False,
        },
    }
