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
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_start_reddit_refresh_run_enqueues_background_job(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    community_id = str(uuid4())
    season_id = str(uuid4())
    run_id = str(uuid4())

    payload = {
        "community_id": community_id,
        "season_id": season_id,
        "period_key": "pre-season",
        "subreddit": "BravoRealHousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
        "is_show_focused": False,
        "analysis_flares": ["Salt Lake City"],
        "analysis_all_flares": ["Salt Lake City"],
        "force_include_flares": ["Salt Lake City"],
        "sort_modes": ["new"],
        "period_start": "2025-08-14T00:00:00Z",
        "period_end": "2025-09-16T23:00:00Z",
        "exhaustive_window": True,
        "search_backfill": True,
        "seed_post_urls": [
            "https://www.reddit.com/r/BravoRealHousewives/comments/1neufq0/example/",
            "https://www.reddit.com/r/BravoRealHousewives/comments/1mr9pb8/example/",
        ],
        "max_pages": 500,
    }

    with patch(
        "trr_backend.repositories.reddit_refresh.create_or_reuse_refresh_run",
        return_value={"id": run_id, "reused": False},
    ) as create_mock:
        with patch("trr_backend.repositories.reddit_refresh.execute_refresh_run", return_value={}) as exec_mock:
            with patch(
                "trr_backend.repositories.reddit_refresh.get_refresh_run",
                return_value={"run_id": run_id, "status": "queued"},
            ) as get_mock:
                response = client.post(
                    "/api/v1/admin/socials/reddit/runs",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["reused"] is False
    assert body["run"]["run_id"] == run_id
    assert create_mock.called
    assert exec_mock.called
    assert get_mock.called
    sent_payload = create_mock.call_args.kwargs.get("payload")
    assert isinstance(sent_payload, dict)
    assert sent_payload.get("seed_post_urls") == payload["seed_post_urls"]


def test_get_reddit_refresh_run_returns_404_for_missing_run(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

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


def test_get_reddit_refresh_run_includes_queue_metrics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

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


def test_get_reddit_cached_period_payload_returns_discovery(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    community_id = str(uuid4())
    season_id = str(uuid4())

    with patch(
        "trr_backend.repositories.reddit_refresh.get_cached_period_payload",
        return_value={"subreddit": "bravorealhousewives", "threads": []},
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    community_id = str(uuid4())
    season_id = str(uuid4())

    with patch("trr_backend.repositories.reddit_refresh.get_cached_period_payload", return_value=None):
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
