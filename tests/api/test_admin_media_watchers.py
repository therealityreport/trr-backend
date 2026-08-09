from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app

WATCH_ID = "11111111-1111-1111-1111-111111111111"
SHOW_ID = "22222222-2222-2222-2222-222222222222"
SEASON_ID = "33333333-3333-3333-3333-333333333333"
BRAVO_ID = "44444444-4444-4444-4444-444444444444"


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {"id": "admin-1", "role": "internal_admin"}
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def _watch(**overrides: object) -> dict[str, object]:
    return {
        "id": WATCH_ID,
        "status": "active",
        "lease_fence": 4,
        "baseline_completed_at": None,
        "poll_interval_seconds": 60,
        **overrides,
    }


def test_router_is_registered_and_requires_internal_admin() -> None:
    paths = {path for route in app.routes if (path := getattr(route, "path", None))}
    for included in app.routes:
        original_router = getattr(included, "original_router", None)
        context = getattr(included, "include_context", None)
        if original_router is not None and context is not None:
            paths.update(f"{context.prefix}{route.path}" for route in original_router.routes)
    assert "/api/v1/admin/media-watchers/{watch_id}/manifest" in paths

    app.dependency_overrides.pop(require_internal_admin, None)
    response = TestClient(app).get(f"/api/v1/admin/media-watchers/{WATCH_ID}/status")
    assert response.status_code == 401


def test_create_pause_resume_and_status_use_authorized_watcher_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.admin_media_watchers as router_module
    import trr_backend.repositories.media_watchers as repository

    create_calls: list[dict[str, object]] = []
    monkeypatch.setattr(repository, "create_watch", lambda **kwargs: create_calls.append(kwargs) or _watch())
    monkeypatch.setattr(repository, "pause_watch", lambda **_kwargs: _watch(status="paused", lease_fence=5))
    monkeypatch.setattr(repository, "resume_watch", lambda **_kwargs: _watch(status="active", lease_fence=6))
    monkeypatch.setattr(repository, "get_watch", lambda _watch_id: _watch())
    monkeypatch.setattr(router_module, "_recent_runs", lambda _watch_id, *, limit: [{"id": "run-1"}])
    client = TestClient(app)

    create = client.post(
        "/api/v1/admin/media-watchers",
        json={
            "show_id": SHOW_ID,
            "season_id": SEASON_ID,
            "target_season_number": 7,
            "nbcumv_show_id": "nbcumv-show",
            "bravo_show_uuid": BRAVO_ID,
            "source_season_rules": {"season": 7},
            "qualification_rules_version": "v1",
            "r2_prefix": "shows/rhoslc/season-7",
            "desktop_folder_name": "RHOSLC-S7",
        },
    )
    pause = client.post(f"/api/v1/admin/media-watchers/{WATCH_ID}/pause")
    resume = client.post(f"/api/v1/admin/media-watchers/{WATCH_ID}/resume")
    status = client.get(f"/api/v1/admin/media-watchers/{WATCH_ID}/status")
    recent = client.get(f"/api/v1/admin/media-watchers/{WATCH_ID}/runs/recent")

    assert create.status_code == 200
    assert create_calls[0]["created_by"] == "admin-1"
    assert pause.json()["watch"]["status"] == "paused"
    assert resume.json()["watch"]["lease_fence"] == 6
    assert status.json()["recent_run"] == {"id": "run-1"}
    assert recent.json()["run"] == {"id": "run-1"}


def test_run_now_and_backfill_claim_one_fenced_watch_before_modal_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.admin_media_watchers as router_module
    import trr_backend.repositories.media_watchers as repository

    claims: list[dict[str, object]] = []
    dispatches: list[dict[str, object]] = []
    monkeypatch.setattr(repository, "get_watch", lambda _watch_id: _watch())
    monkeypatch.setattr(
        router_module,
        "_claim_watch_for_admin_run",
        lambda **kwargs: claims.append(kwargs) or {"id": WATCH_ID, "lease_fence": 12},
    )
    monkeypatch.setattr(
        router_module.modal_dispatch,
        "dispatch_show_season_media_watch_worker",
        lambda **kwargs: dispatches.append(kwargs) or {"dispatched": True, "call_id": "fc-1"},
    )
    client = TestClient(app)

    run_now = client.post(f"/api/v1/admin/media-watchers/{WATCH_ID}/run-now")
    backfill = client.post(f"/api/v1/admin/media-watchers/{WATCH_ID}/backfill")

    assert run_now.status_code == 200
    assert backfill.status_code == 200
    assert len(claims) == 2
    assert all(call["watch_id"] == WATCH_ID for call in claims)
    assert [call["lease_fence"] for call in dispatches] == [12, 12]
    assert [call["backfill"] for call in dispatches] == [False, True]
    assert run_now.json()["call_id"] == "fc-1"
    assert backfill.json()["call_id"] == "fc-1"


def test_manifest_is_watch_scoped_expiring_committed_only_and_credential_free(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.admin_media_watchers as router_module

    queries: list[tuple[str, list[object]]] = []
    monkeypatch.setattr(router_module.media_watchers, "get_watch", lambda _watch_id: _watch())

    def fake_fetch_all(sql: str, params: list[object]):
        queries.append((sql, params))
        return [
            {
                "revision_id": "rev-1",
                "watch_id": WATCH_ID,
                "media_asset_id": "asset-1",
                "sha256": "a" * 64,
                "size_bytes": 123,
                "content_type": "image/jpeg",
                "hosted_bucket": "media",
                "hosted_key": "shows/rhoslc/a.jpg",
                "hosted_url": "https://media.example.test/shows/rhoslc/a.jpg?X-Amz-Signature=secret",
                "fetched_at": "2026-08-06T12:00:00Z",
            }
        ]

    monkeypatch.setattr(router_module.pg, "fetch_all", fake_fetch_all)
    response = TestClient(app).get(f"/api/v1/admin/media-watchers/{WATCH_ID}/manifest?expires_in_seconds=120")

    assert response.status_code == 200
    payload = response.json()
    assert payload["watch_id"] == WATCH_ID
    assert payload["generated_at"] < payload["expires_at"]
    assert payload["revisions"] == [
        {
            "revision_id": "rev-1",
            "media_asset_id": "asset-1",
            "sha256": "a" * 64,
            "size_bytes": 123,
            "content_type": "image/jpeg",
            "hosted_bucket": "media",
            "hosted_key": "shows/rhoslc/a.jpg",
            "hosted_url": "https://media.example.test/shows/rhoslc/a.jpg",
            "fetched_at": "2026-08-06T12:00:00Z",
        }
    ]
    assert queries[0][1] == [WATCH_ID]
    assert "watch_id = %s::uuid" in queries[0][0]
    assert "acquisition_state = 'db_committed'" in queries[0][0]


def test_run_now_rejects_an_overlapping_or_paused_watch(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.admin_media_watchers as router_module
    import trr_backend.repositories.media_watchers as repository

    monkeypatch.setattr(repository, "get_watch", lambda _watch_id: _watch(status="paused"))
    monkeypatch.setattr(router_module, "_claim_watch_for_admin_run", lambda **_kwargs: None)

    response = TestClient(app).post(f"/api/v1/admin/media-watchers/{WATCH_ID}/run-now")

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "WATCH_NOT_CLAIMABLE"
