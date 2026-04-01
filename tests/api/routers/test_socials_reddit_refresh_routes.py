"""Tests for Reddit refresh admin routes."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app


def _make_admin_token(secret: str, subject: str = "admin-reddit") -> str:
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
def client() -> TestClient:
    return TestClient(app)


def test_start_reddit_refresh_run_local_mode_starts_in_api_background(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "local")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "0")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "0")
    monkeypatch.delenv("TRR_REMOTE_EXECUTOR", raising=False)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())

    payload = {
        "community_id": community_id,
        "season_id": season_id,
        "period_key": "pre-season",
        "period_stable_key": "period-preseason",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
        "is_show_focused": False,
        "analysis_flairs": ["Salt Lake City"],
        "analysis_all_flairs": ["Salt Lake City"],
        "force_include_flairs": ["Salt Lake City"],
        "sort_modes": ["new"],
        "period_start": "2025-08-14T00:00:00Z",
        "period_end": "2025-09-16T23:00:00Z",
        "exhaustive_window": True,
        "search_backfill": True,
        "coverage_mode": "adaptive_deep",
        "max_backfill_queries": 30,
        "max_backfill_pages_per_query": 50,
        "period_label": "Pre-Season",
        "run_config_hash": "1234567890abcdef1234567890abcdef12345678",
        "seed_post_urls": [
            "https://www.reddit.com/r/BravoRealHousewives/comments/1neufq0/example/",
            "https://www.reddit.com/r/BravoRealHousewives/comments/1mr9pb8/example/",
        ],
        "max_pages": 500,
    }

    run_id = str(uuid4())

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ):
        with patch("trr_backend.repositories.reddit_refresh.execute_refresh_run", return_value={}) as exec_mock:
            with patch(
                "trr_backend.repositories.reddit_refresh.get_refresh_run",
                return_value={
                    "run_id": run_id,
                    "status": "queued",
                    "execution_owner": "local_api",
                    "execution_mode_canonical": "local",
                },
            ):
                response = client.post(
                    "/api/v1/admin/socials/reddit/runs",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["reused"] is False
    assert body["execution_owner"] == "local_api"
    assert body["execution_mode_canonical"] == "local"
    assert body["run"]["execution_owner"] == "local_api"
    assert exec_mock.called is True


def test_start_reddit_refresh_run_accepts_sync_full_mode(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "local")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "0")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "0")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "episode-1",
        "period_stable_key": "episode-1",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
        "mode": "sync_full",
        "fetch_comments": False,
        "comment_delta_only": True,
        "max_pages": 10_000,
    }

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ) as create_mock:
        with patch("trr_backend.repositories.reddit_refresh.execute_refresh_run", return_value={}):
            with patch(
                "trr_backend.repositories.reddit_refresh.get_refresh_run",
                return_value={
                    "run_id": run_id,
                    "status": "queued",
                    "execution_owner": "local_api",
                    "execution_mode_canonical": "local",
                },
            ):
                response = client.post(
                    "/api/v1/admin/socials/reddit/runs",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert create_mock.call_args.kwargs["payload"]["mode"] == "sync_full"


def test_start_reddit_refresh_run_remote_mode_does_not_start_in_api(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "0")
    monkeypatch.setenv("TRR_REMOTE_EXECUTOR", "legacy_worker")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
    }

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ):
        with patch("trr_backend.repositories.reddit_refresh.execute_refresh_run", return_value={}) as exec_mock:
            with patch(
                "trr_backend.repositories.reddit_refresh.get_refresh_run",
                return_value={
                    "run_id": run_id,
                    "status": "queued",
                    "execution_owner": "remote_worker",
                    "execution_mode_canonical": "remote",
                },
            ):
                response = client.post(
                    "/api/v1/admin/socials/reddit/runs",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["reused"] is False
    assert body["execution_owner"] == "remote_worker"
    assert body["execution_mode_canonical"] == "remote"
    assert body["run"]["execution_owner"] == "remote_worker"
    assert exec_mock.called is False


def test_start_reddit_refresh_run_modal_dispatches_without_api_background(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("TRR_REMOTE_EXECUTOR", "modal")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
    }

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ):
        with patch(
            "api.routers.socials.get_modal_reddit_runtime_health",
            return_value={
                "healthy": True,
                "reason": "ok",
                "missing_env": [],
                "warnings": [],
                "supports_oauth": True,
                "user_agent_configured": True,
                "uses_default_user_agent": False,
                "effective_user_agent": "TRRTest/1.0",
            },
        ):
            with patch("api.routers.socials.dispatch_reddit_refresh", return_value=True) as dispatch_mock:
                with patch("trr_backend.repositories.reddit_refresh.execute_refresh_run", return_value={}) as exec_mock:
                    with patch(
                        "trr_backend.repositories.reddit_refresh.get_refresh_run",
                        return_value={
                            "run_id": run_id,
                            "status": "queued",
                            "execution_owner": "remote_worker",
                            "execution_mode_canonical": "remote",
                        },
                    ):
                        response = client.post(
                            "/api/v1/admin/socials/reddit/runs",
                            headers={"Authorization": f"Bearer {token}"},
                            json=payload,
                        )

    assert response.status_code == 200
    body = response.json()
    assert body["execution_owner"] == "remote_worker"
    assert body["execution_mode_canonical"] == "remote"
    assert body["run"]["execution_owner"] == "remote_worker"
    dispatch_mock.assert_called_once_with(run_id=run_id)
    exec_mock.assert_not_called()


def test_start_reddit_refresh_run_returns_503_when_modal_dispatch_not_ready(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("TRR_REMOTE_EXECUTOR", "modal")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
    }

    run_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ):
        with patch(
            "trr_backend.repositories.reddit_refresh.get_refresh_run",
            return_value={
                "run_id": run_id,
                "status": "queued",
                "execution_owner": "remote_worker",
                "execution_mode_canonical": "remote",
            },
        ):
            with patch("api.routers.socials.modal_dispatch_ready", return_value=(False, "modal_disabled")):
                response = client.post(
                    "/api/v1/admin/socials/reddit/runs",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["code"] == "REDDIT_REMOTE_DISPATCH_UNAVAILABLE"
    assert detail["execution_mode"] == "remote"
    assert detail["execution_owner"] == "remote_worker"
    assert detail["worker_health"]["healthy"] is False
    assert detail["worker_health"]["reason"] == "modal_disabled"


def test_start_reddit_refresh_run_returns_503_when_modal_runtime_lacks_reddit_oauth(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("TRR_REMOTE_EXECUTOR", "modal")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
    }

    run_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ):
        with patch(
            "trr_backend.repositories.reddit_refresh.get_refresh_run",
            return_value={
                "run_id": run_id,
                "status": "queued",
                "execution_owner": "remote_worker",
                "execution_mode_canonical": "remote",
            },
        ):
            with patch("api.routers.socials.modal_dispatch_ready", return_value=(True, None)):
                with patch(
                    "api.routers.socials.get_modal_reddit_runtime_health",
                    return_value={
                        "healthy": False,
                        "reason": "reddit_oauth_missing",
                        "missing_env": ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"],
                        "warnings": ["REDDIT_USER_AGENT"],
                        "supports_oauth": False,
                        "user_agent_configured": False,
                        "uses_default_user_agent": True,
                        "effective_user_agent": "TRRBackendRedditRefresh/1.0 (+https://thereality.report)",
                    },
                ):
                    response = client.post(
                        "/api/v1/admin/socials/reddit/runs",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["code"] == "REDDIT_REMOTE_RUNTIME_UNHEALTHY"
    assert "Missing: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET." in detail["message"]
    assert detail["worker_health"]["healthy"] is False
    assert detail["worker_health"]["reason"] == "reddit_oauth_missing"
    assert detail["worker_health"]["missing_env"] == ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"]
    assert detail["worker_health"]["warnings"] == ["REDDIT_USER_AGENT"]


def test_start_reddit_refresh_run_reuses_existing_modal_run_without_runtime_probe(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("TRR_REMOTE_EXECUTOR", "modal")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
    }

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": True},
    ):
        with patch(
            "trr_backend.repositories.reddit_refresh.get_refresh_run",
            return_value={
                "run_id": run_id,
                "status": "running",
                "claimed_by_worker_id": "modal-worker",
                "heartbeat_at": "2026-03-22T13:00:00Z",
                "execution_owner": "remote_worker",
                "execution_mode_canonical": "remote",
            },
        ):
            with patch("api.routers.socials.get_modal_reddit_runtime_health") as health_mock:
                with patch("api.routers.socials.dispatch_reddit_refresh", return_value=True) as dispatch_mock:
                    response = client.post(
                        "/api/v1/admin/socials/reddit/runs",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    assert response.json()["reused"] is True
    health_mock.assert_not_called()
    dispatch_mock.assert_not_called()


def test_start_reddit_refresh_run_redispatches_reused_orphaned_modal_run_with_attempts(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("TRR_REMOTE_EXECUTOR", "modal")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
    }

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": True},
    ):
        with patch(
            "trr_backend.repositories.reddit_refresh.get_refresh_run",
            return_value={
                "run_id": run_id,
                "status": "queued",
                "attempt_count": 2,
                "claimed_by_worker_id": None,
                "heartbeat_at": None,
                "execution_owner": "remote_worker",
                "execution_mode_canonical": "remote",
            },
        ):
            with patch(
                "api.routers.socials.get_modal_reddit_runtime_health",
                return_value={
                    "healthy": True,
                    "reason": "ok",
                    "missing_env": [],
                    "warnings": [],
                    "supports_oauth": True,
                    "user_agent_configured": True,
                    "uses_default_user_agent": False,
                    "effective_user_agent": "TRRTest/1.0",
                },
            ):
                with patch("api.routers.socials.dispatch_reddit_refresh", return_value=True) as dispatch_mock:
                    response = client.post(
                        "/api/v1/admin/socials/reddit/runs",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    assert response.json()["reused"] is True
    dispatch_mock.assert_called_once_with(run_id=run_id)


def test_start_reddit_refresh_run_returns_503_when_modal_dispatch_fails(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("TRR_REMOTE_EXECUTOR", "modal")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    payload = {
        "community_id": str(uuid4()),
        "season_id": str(uuid4()),
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
    }

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ):
        with patch(
            "api.routers.socials.get_modal_reddit_runtime_health",
            return_value={
                "healthy": True,
                "reason": "ok",
                "missing_env": [],
                "warnings": [],
                "supports_oauth": True,
                "user_agent_configured": True,
                "uses_default_user_agent": False,
                "effective_user_agent": "TRRTest/1.0",
            },
        ):
            with patch("api.routers.socials.dispatch_reddit_refresh", return_value=False):
                with patch(
                    "trr_backend.repositories.reddit_refresh.get_refresh_run",
                    return_value={
                        "run_id": run_id,
                        "status": "queued",
                        "execution_owner": "remote_worker",
                        "execution_mode_canonical": "remote",
                    },
                ):
                    response = client.post(
                        "/api/v1/admin/socials/reddit/runs",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["code"] == "REDDIT_REMOTE_DISPATCH_UNAVAILABLE"
    assert detail["execution_mode"] == "remote"
    assert detail["execution_owner"] == "remote_worker"
    assert detail["worker_health"]["healthy"] is False
    assert detail["worker_health"]["reason"] == "modal_dispatch_failed"


def test_get_reddit_refresh_run_returns_404_for_missing_run(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.get_refresh_run",
        side_effect=ValueError("Refresh run not found"),
    ):
        response = client.get(
            f"/api/v1/admin/socials/reddit/runs/{run_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Refresh run not found"


def test_list_reddit_refresh_runs_returns_filtered_rows(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())
    mocked_runs = [
        {
            "run_id": str(uuid4()),
            "status": "running",
            "community_id": community_id,
            "season_id": season_id,
        }
    ]

    with patch("trr_backend.repositories.reddit_refresh.list_refresh_runs", return_value=mocked_runs) as list_mock:
        response = client.get(
            "/api/v1/admin/socials/reddit/runs",
            params={
                "community_id": community_id,
                "season_id": season_id,
                "period_key": "period-preseason",
                "status": "queued,running",
                "limit": "10",
            },
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["runs"][0]["status"] == "running"
    assert list_mock.call_args.kwargs["community_id"] == community_id
    assert list_mock.call_args.kwargs["season_id"] == season_id
    assert list_mock.call_args.kwargs["period_key"] == "period-preseason"
    assert list_mock.call_args.kwargs["statuses"] == ["queued", "running"]
    assert list_mock.call_args.kwargs["limit"] == 10


def test_list_reddit_refresh_runs_rejects_invalid_status(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.reddit_refresh.list_refresh_runs",
        side_effect=ValueError("status must be one of: queued, running, completed, partial, failed, cancelled"),
    ):
        response = client.get(
            "/api/v1/admin/socials/reddit/runs",
            params={"status": "bogus"},
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 400
    assert "status must be one of" in response.json()["detail"]


def test_get_reddit_refresh_run_includes_queue_metrics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    mocked_run = {
        "run_id": run_id,
        "status": "queued",
        "queue": {
            "running_total": 2,
            "queued_total": 4,
            "other_running": 2,
            "other_queued": 3,
            "queued_ahead": 1,
        },
    }

    with patch("trr_backend.repositories.reddit_refresh.get_refresh_run", return_value=mocked_run):
        response = client.get(
            f"/api/v1/admin/socials/reddit/runs/{run_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["queue"]["running_total"] == 2
    assert response.json()["queue"]["queued_ahead"] == 1


def test_get_reddit_refresh_run_includes_seed_diagnostics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    mocked_run = {
        "run_id": run_id,
        "status": "completed",
        "diagnostics": {
            "seed_urls": {
                "seed_urls_requested": 3,
                "seed_urls_ingested": 2,
                "seed_urls_failed": 1,
            }
        },
    }

    with patch("trr_backend.repositories.reddit_refresh.get_refresh_run", return_value=mocked_run):
        response = client.get(
            f"/api/v1/admin/socials/reddit/runs/{run_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    diagnostics = response.json()["diagnostics"]
    assert diagnostics["seed_urls"]["seed_urls_requested"] == 3
    assert diagnostics["seed_urls"]["seed_urls_ingested"] == 2


def test_get_reddit_refresh_run_includes_live_progress_diagnostics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    run_id = str(uuid4())
    mocked_run = {
        "run_id": run_id,
        "status": "running",
        "diagnostics": {
            "progress": {
                "stage": "fetching_comments",
                "listing_pages_fetched": 10,
                "search_pages_fetched": 4,
                "rows_discovered_raw": 150,
                "rows_matched": 27,
                "comments_targets_total": 20,
                "comments_targets_done": 6,
                "comments_rows_upserted": 920,
            }
        },
    }

    with patch("trr_backend.repositories.reddit_refresh.get_refresh_run", return_value=mocked_run):
        response = client.get(
            f"/api/v1/admin/socials/reddit/runs/{run_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    progress = response.json()["diagnostics"]["progress"]
    assert progress["stage"] == "fetching_comments"
    assert progress["comments_targets_done"] == 6


def test_get_reddit_cached_period_payload_returns_discovery(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())

    with patch(
        "trr_backend.repositories.reddit_refresh.get_cached_period_payload_snapshot",
        return_value={
            "discovery": {"subreddit": "bravorealhousewives", "threads": []},
            "resolved_period_key": "pre-season",
            "cache_status": "fresh",
            "cache_age_seconds": None,
            "run_status": "completed",
            "phase": None,
            "partial_failures": [],
        },
    ):
        response = client.get(
            "/api/v1/admin/socials/reddit/cache",
            params={
                "community_id": community_id,
                "season_id": season_id,
                "period_key": "pre-season",
            },
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["discovery"]["subreddit"] == "bravorealhousewives"


def test_get_reddit_cached_period_payload_404_when_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())

    with (
        patch("trr_backend.repositories.reddit_refresh.get_cached_period_payload_snapshot", return_value=None),
        patch("trr_backend.repositories.reddit_refresh.get_cached_period_payload", return_value=None),
    ):
        response = client.get(
            "/api/v1/admin/socials/reddit/cache",
            params={
                "community_id": community_id,
                "season_id": season_id,
                "period_key": "pre-season",
            },
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 404


def test_get_reddit_cached_period_payload_bulk_returns_first_match_and_misses(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())
    resolve_calls: list[str] = []
    snapshot_calls: list[str] = []

    def _fake_resolve(*, community_id: str, season_id: str, period_key: str):  # type: ignore[override]
        del community_id, season_id
        resolve_calls.append(period_key)
        if period_key == "period-preseason":
            return period_key
        return None

    def _fake_snapshot(*, community_id: str, season_id: str, period_key: str):  # type: ignore[override]
        del community_id, season_id
        snapshot_calls.append(period_key)
        if period_key == "period-preseason":
            return {
                "discovery": {"subreddit": "bravorealhousewives", "threads": [{"reddit_post_id": "post-1"}]},
                "resolved_period_key": "period-preseason",
                "cache_status": "fresh",
                "cache_age_seconds": None,
                "run_status": "completed",
                "phase": None,
                "partial_failures": [],
            }
        return None

    with (
        patch("trr_backend.repositories.reddit_refresh.resolve_cached_period_key", side_effect=_fake_resolve),
        patch("trr_backend.repositories.reddit_refresh.get_cached_period_payload_snapshot", side_effect=_fake_snapshot),
    ):
        response = client.post(
            "/api/v1/admin/socials/reddit/cache/bulk",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "community_id": community_id,
                "season_id": season_id,
                "period_keys": ["period-preseason", "period-preseason", "legacy-preseason", "period-episode-1"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["matched_period_key"] == "period-preseason"
    assert payload["misses"] == []
    assert payload["discovery"]["threads"][0]["reddit_post_id"] == "post-1"
    assert payload["source"] == "cache"
    assert resolve_calls == ["period-preseason"]
    assert snapshot_calls == ["period-preseason"]


def test_get_reddit_cached_period_payload_bulk_returns_all_misses_when_no_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())
    resolve_calls: list[str] = []

    def _fake_resolve(*, community_id: str, season_id: str, period_key: str):  # type: ignore[override]
        del community_id, season_id
        resolve_calls.append(period_key)
        return None

    with (
        patch("trr_backend.repositories.reddit_refresh.resolve_cached_period_key", side_effect=_fake_resolve),
        patch("trr_backend.repositories.reddit_refresh.get_cached_period_payload") as payload_mock,
    ):
        response = client.post(
            "/api/v1/admin/socials/reddit/cache/bulk",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "community_id": community_id,
                "season_id": season_id,
                "period_keys": ["period-preseason", "legacy-preseason", "period-episode-1"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["matched_period_key"] is None
    assert payload["discovery"] is None
    assert payload["misses"] == ["period-preseason", "legacy-preseason", "period-episode-1"]
    assert payload["source"] == "none"
    assert resolve_calls == ["period-preseason", "legacy-preseason", "period-episode-1"]
    payload_mock.assert_not_called()


def test_get_reddit_cached_period_payload_bulk_returns_400_when_period_keys_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())

    response = client.post(
        "/api/v1/admin/socials/reddit/cache/bulk",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "community_id": community_id,
            "season_id": season_id,
            "period_keys": ["", "   "],
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "At least one non-empty period_key or container_key is required"


def test_get_reddit_cached_period_payload_bulk_uses_container_keys_first(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())
    derived_key = f"community:{community_id}:season:{season_id}:container:period-preseason"
    resolve_calls: list[str] = []
    snapshot_calls: list[str] = []

    def _fake_resolve(*, community_id: str, season_id: str, period_key: str):  # type: ignore[override]
        del community_id, season_id
        resolve_calls.append(period_key)
        if period_key == derived_key:
            return period_key
        return None

    def _fake_snapshot(*, community_id: str, season_id: str, period_key: str):  # type: ignore[override]
        del community_id, season_id
        snapshot_calls.append(period_key)
        if period_key == derived_key:
            return {
                "discovery": {"subreddit": "bravorealhousewives", "threads": [{"reddit_post_id": "post-xyz"}]},
                "resolved_period_key": derived_key,
                "cache_status": "fresh",
                "cache_age_seconds": None,
                "run_status": "completed",
                "phase": None,
                "partial_failures": [],
            }
        return None

    with (
        patch("trr_backend.repositories.reddit_refresh.resolve_cached_period_key", side_effect=_fake_resolve),
        patch("trr_backend.repositories.reddit_refresh.get_cached_period_payload_snapshot", side_effect=_fake_snapshot),
    ):
        response = client.post(
            "/api/v1/admin/socials/reddit/cache/bulk",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "community_id": community_id,
                "season_id": season_id,
                "container_keys": ["period-preseason"],
                "period_keys": ["legacy-preseason"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["matched_period_key"] == derived_key
    assert payload["source"] == "cache"
    assert resolve_calls == [derived_key]
    assert snapshot_calls == [derived_key]


def test_get_reddit_analytics_summary_requires_season_id_for_season_scope(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    community_id = str(uuid4())

    response = client.get(
        f"/api/v1/admin/socials/reddit/analytics/community/{community_id}/summary",
        params={"scope": "season"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    assert "season_id is required" in response.json()["detail"]


def test_backfill_reddit_refresh_runs_starts_async_operation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    community_id = str(uuid4())
    season_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.build_reddit_refresh_backfill_operation_producer",
        return_value=lambda: iter(()),
    ) as producer_mock:
        with patch(
            "trr_backend.pipeline.admin_operations.start_operation_for_stream",
            return_value={
                "id": "op-123",
                "status": "pending",
                "attached": False,
                "execution_owner": "remote_worker",
                "execution_mode_canonical": "remote",
            },
        ) as start_mock:
            response = client.post(
                "/api/v1/admin/socials/reddit/runs/backfill",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "community_id": community_id,
                    "season_id": season_id,
                    "mode": "sync_full",
                    "detail_refresh": False,
                },
            )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "started"
    assert body["operation_id"] == "op-123"
    assert body["attached"] is False
    assert body["requested"]["community_id"] == community_id
    assert body["requested"]["mode"] == "sync_full"
    assert body["operation"]["id"] == "op-123"
    assert body["operation"]["status"] == "pending"
    producer_mock.assert_called_once()
    assert start_mock.call_args.kwargs["operation_type"] == "admin_reddit_refresh_backfill"


def test_backfill_reddit_refresh_runs_attaches_existing_operation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.reddit_refresh.build_reddit_refresh_backfill_operation_producer",
        return_value=lambda: iter(()),
    ):
        with patch(
            "trr_backend.pipeline.admin_operations.start_operation_for_stream",
            return_value={
                "id": "op-existing",
                "status": "running",
                "attached": True,
            },
        ):
            response = client.post(
                "/api/v1/admin/socials/reddit/runs/backfill",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "community_id": str(uuid4()),
                    "season_id": str(uuid4()),
                    "mode": "sync_full",
                    "detail_refresh": False,
                },
            )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "attached"
    assert body["operation_id"] == "op-existing"
    assert body["attached"] is True


def test_get_reddit_analytics_summary_all_scope(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    community_id = str(uuid4())
    payload = {
        "scope": "all",
        "season_id": None,
        "totals": {
            "post_count": 10,
            "tracked_flair_post_count": 7,
            "show_match_post_count": 8,
            "comment_count": 120,
            "score_sum": 999,
            "season_count": 2,
        },
        "diagnostics": {
            "updated_at": "2026-03-01T00:00:00Z",
            "source_table": "social.reddit_period_post_matches",
            "row_count": 10,
        },
    }
    with patch(
        "trr_backend.repositories.reddit_refresh.get_reddit_community_analytics_summary",
        return_value=payload,
    ) as summary_mock:
        response = client.get(
            f"/api/v1/admin/socials/reddit/analytics/community/{community_id}/summary",
            params={"scope": "all"},
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["totals"]["post_count"] == 10
    summary_mock.assert_called_once_with(
        community_id=community_id,
        scope="all",
        season_id=None,
    )


def test_get_reddit_analytics_shows_forwards_scope_and_season_id(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    community_id = str(uuid4())
    season_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.get_reddit_community_show_breakdown",
        return_value={"scope": "season", "season_id": season_id, "shows": []},
    ) as shows_mock:
        response = client.get(
            f"/api/v1/admin/socials/reddit/analytics/community/{community_id}/shows",
            params={"scope": "season", "season_id": season_id},
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    shows_mock.assert_called_once_with(
        community_id=community_id,
        scope="season",
        season_id=season_id,
    )


def test_get_reddit_analytics_flairs_forwards_scope_and_season_id(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    community_id = str(uuid4())
    season_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.get_reddit_community_flair_breakdown",
        return_value={"scope": "season", "season_id": season_id, "flairs": []},
    ) as flairs_mock:
        response = client.get(
            f"/api/v1/admin/socials/reddit/analytics/community/{community_id}/flairs",
            params={"scope": "season", "season_id": season_id},
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    flairs_mock.assert_called_once_with(
        community_id=community_id,
        scope="season",
        season_id=season_id,
    )


def test_get_reddit_analytics_flair_detail_forwards_filters(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    community_id = str(uuid4())
    season_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.get_reddit_community_flair_detail",
        return_value={
            "scope": "season",
            "season_id": season_id,
            "flair": {"flair_key": "salt lake city"},
            "posts": [],
            "pagination": {"page": 1, "per_page": 25, "total_count": 0},
        },
    ) as detail_mock:
        response = client.get(
            f"/api/v1/admin/socials/reddit/analytics/community/{community_id}/flairs/salt%20lake%20city",
            params={
                "scope": "season",
                "season_id": season_id,
                "container_key": "period-preseason",
                "page": 1,
                "per_page": 25,
            },
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    detail_mock.assert_called_once_with(
        community_id=community_id,
        flair_key="salt lake city",
        scope="season",
        season_id=season_id,
        container_key="period-preseason",
        page=1,
        per_page=25,
    )


def test_get_reddit_analytics_posts_forwards_filters(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    community_id = str(uuid4())
    season_id = str(uuid4())
    with patch(
        "trr_backend.repositories.reddit_refresh.list_reddit_community_posts",
        return_value={
            "scope": "season",
            "season_id": season_id,
            "container_key": "period-preseason",
            "flair_key": "salt lake city",
            "pagination": {"page": 1, "per_page": 10, "total_count": 1},
            "posts": [{"reddit_post_id": "abc123"}],
        },
    ) as posts_mock:
        response = client.get(
            f"/api/v1/admin/socials/reddit/analytics/community/{community_id}/posts",
            params={
                "scope": "season",
                "season_id": season_id,
                "container_key": "period-preseason",
                "flair_key": "salt lake city",
                "page": 1,
                "per_page": 10,
            },
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["pagination"]["total_count"] == 1
    posts_mock.assert_called_once_with(
        community_id=community_id,
        scope="season",
        season_id=season_id,
        container_key="period-preseason",
        flair_key="salt lake city",
        page=1,
        per_page=10,
    )
