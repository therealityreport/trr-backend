"""Tests for season social analytics admin endpoints."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from threading import Event
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from urllib.parse import urlencode
from uuid import uuid4

import jwt
import pytest
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.db.pg import DatabaseServiceUnavailableError


def _iter_app_routes(routes: Any) -> list[Any]:
    flattened: list[Any] = []
    for route in routes:
        flattened.append(route)
        child_routes = getattr(route, "routes", None)
        if child_routes:
            flattened.extend(_iter_app_routes(child_routes))
    return flattened


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "admin",
        "email": "admin@example.com",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _make_internal_admin_token(
    secret: str,
    subject: str = "trr-app-internal-admin",
) -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "iss": "trr-app-internal",
        "aud": "trr-backend-internal-admin",
        "scope": "internal_admin",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def _default_local_job_plane(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "local")
    monkeypatch.delenv("TRR_LONG_JOB_ENFORCE_REMOTE", raising=False)
    monkeypatch.delenv("SOCIAL_QUEUE_ENABLED", raising=False)


@pytest.fixture(autouse=True)
def _default_modal_resolution_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "api.routers.socials.resolve_modal_function",
        lambda _function_name: {
            "resolved": True,
            "reason": None,
            "error": None,
            "app_name": "trr-backend-jobs",
            "function_name": "run_social_job",
            "modal_environment": "main",
        },
    )


@pytest.fixture(autouse=True)
def _block_live_social_worker_launches(monkeypatch: pytest.MonkeyPatch) -> None:
    def _blocked_live_worker_launch(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("social route test attempted to launch a live inline worker")

    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics.execute_run",
        _blocked_live_worker_launch,
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics.execute_run_with_inline_worker_registration",
        _blocked_live_worker_launch,
    )


def test_get_season_targets(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "show_id": str(uuid4()),
        "season_number": 10,
        "show_name": "Test Show",
        "source_scope": "bravo",
        "targets": [],
        "using_defaults": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.get_targets", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/targets",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["season_id"] == season_id
    assert response.json()["using_defaults"] is True


def test_get_queue_status_endpoint_returns_503_on_pool_init_failure(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status",
        side_effect=DatabaseServiceUnavailableError(
            "Database pool initialization failed: no database URL candidates available",
            reason="database_configuration",
        ),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["reason"] == "database_configuration"


@pytest.mark.parametrize(
    ("path_template", "repo_target"),
    [
        (
            "/api/v1/admin/socials/seasons/{season_id}/targets",
            "trr_backend.repositories.social_season_analytics.get_targets",
        ),
        (
            "/api/v1/admin/socials/shared/sources?source_scope=bravo&include_inactive=true",
            "trr_backend.repositories.social_season_analytics.get_shared_account_sources",
        ),
        (
            "/api/v1/admin/socials/shared/review-queue?source_scope=bravo&review_status=open&limit=100",
            "trr_backend.repositories.social_season_analytics.list_shared_review_queue",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/shared-status?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_season_shared_status",
        ),
        (
            "/api/v1/admin/socials/ingest/worker-health",
            "trr_backend.repositories.social_season_analytics.get_worker_health",
        ),
        (
            "/api/v1/admin/socials/ingest/queue-status",
            "trr_backend.repositories.social_season_analytics.get_queue_status",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/ingest/jobs",
            "trr_backend.repositories.social_season_analytics.list_jobs",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/ingest/runs?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.list_runs",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/ingest/runs/summary?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.list_run_summaries",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_analytics",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_week_detail",
        ),
    ],
)
def test_social_read_endpoints_return_503_on_pool_exhaustion(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    path_template: str,
    repo_target: str,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    path = path_template.format(season_id=season_id)

    with patch(
        repo_target,
        side_effect=DatabaseServiceUnavailableError("connection pool exhausted", reason="pool_capacity"),
    ):
        response = client.get(
            path,
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["reason"] == "pool_capacity"


def test_put_season_targets(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "show_id": str(uuid4()),
        "season_number": 10,
        "show_name": "Test Show",
        "source_scope": "bravo",
        "targets": [
            {
                "platform": "instagram",
                "accounts": ["bravotv"],
                "hashtags": ["rhoslc"],
                "keywords": ["Real Housewives"],
                "timezone": "America/New_York",
                "is_active": True,
                "config": {},
            }
        ],
    }

    payload = {
        "source_scope": "bravo",
        "targets": [
            {
                "platform": "instagram",
                "accounts": ["bravotv"],
                "hashtags": ["rhoslc"],
                "keywords": ["Real Housewives"],
            }
        ],
    }

    with patch("trr_backend.repositories.social_season_analytics.put_targets", return_value=expected):
        response = client.put(
            f"/api/v1/admin/socials/seasons/{season_id}/targets",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["targets"][0]["platform"] == "instagram"


def test_get_shared_account_sources(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "source_scope": "network",
        "using_defaults": False,
        "sources": [
            {
                "id": "shared-source-1",
                "platform": "instagram",
                "source_scope": "network",
                "account_handle": "bravotv",
                "is_active": True,
                "scrape_priority": 100,
            }
        ],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_shared_account_sources", return_value=expected
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/shared/sources?source_scope=bravo&include_inactive=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["source_scope"] == "network"
    assert body["sources"][0]["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["source_scope"] == "network"


def test_get_social_account_profile_summary(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "total_posts": 42,
        "per_show_counts": [{"show_id": str(uuid4()), "show_name": "RHOSLC", "post_count": 12}],
        "per_season_counts": [{"season_id": str(uuid4()), "season_number": 6, "post_count": 9}],
        "top_hashtags": [{"hashtag": "rhoslc", "usage_count": 5}],
        "top_collaborators": [],
        "top_tags": [],
        "source_status": [],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        return_value=expected,
    ) as summary_mock:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/summary?detail=lite",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["platform"] == "instagram"
    assert body["account_handle"] == "bravotv"
    assert body["total_posts"] == 42
    summary_mock.assert_called_once_with(platform="instagram", account_handle="bravotv", detail="lite")


def test_get_social_account_profile_summary_defaults_to_lite(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        return_value={
            "platform": "instagram",
            "account_handle": "bravotv",
            "total_posts": 42,
            "per_show_counts": [],
            "per_season_counts": [],
            "top_hashtags": [],
            "top_collaborators": [],
            "top_tags": [],
            "source_status": [],
        },
    ) as summary_mock:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    summary_mock.assert_called_once_with(platform="instagram", account_handle="bravotv", detail="lite")


def test_get_social_account_profile_dashboard(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "data": {
            "summary": {
                "platform": "instagram",
                "account_handle": "thetraitorsus",
                "total_posts": 431,
            },
            "catalog_run_progress": {"run_id": "run-active"},
        },
        "freshness": {
            "status": "fresh",
            "source": "live",
            "generated_at": "2026-04-27T12:00:00+00:00",
            "age_seconds": 0,
        },
        "operational_alerts": [],
    }

    with patch(
        "trr_backend.socials.profile_dashboard.build_social_account_profile_dashboard",
        return_value=expected,
    ) as dashboard_mock:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/thetraitorsus/dashboard"
            "?detail=full&run_id=run-active&recent_log_limit=12",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["data"]["summary"]["total_posts"] == 431
    dashboard_mock.assert_called_once_with(
        platform="instagram",
        account_handle="thetraitorsus",
        detail="full",
        run_id="run-active",
        recent_log_limit=12,
    )


def test_get_social_account_profile_dashboard_reuses_cached_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "data": {
            "summary": {
                "platform": "instagram",
                "account_handle": "cacheprobe",
                "total_posts": 12,
            },
            "catalog_run_progress": None,
        },
        "freshness": {
            "status": "fresh",
            "source": "live",
            "generated_at": "2026-04-27T12:00:00+00:00",
            "age_seconds": 0,
        },
        "operational_alerts": [],
    }

    with patch(
        "trr_backend.socials.profile_dashboard.build_social_account_profile_dashboard",
        return_value=expected,
    ) as dashboard_mock:
        for _ in range(2):
            response = client.get(
                "/api/v1/admin/socials/profiles/instagram/cacheprobe/dashboard?detail=lite",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert response.status_code == 200
            assert response.json()["data"]["summary"]["total_posts"] == 12

    dashboard_mock.assert_called_once()


def test_get_social_account_profile_dashboard_uses_progress_cache_ttl_for_run_id(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setattr(socials_router, "_ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS", 0)
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    call_numbers = iter([1, 2])

    def _dashboard_payload(*_args: object, **_kwargs: object) -> dict[str, object]:
        call_number = next(call_numbers)
        return {
            "data": {
                "summary": {
                    "platform": "instagram",
                    "account_handle": "cacheprobe",
                    "total_posts": 12,
                },
                "catalog_run_progress": {"run_id": "run-active", "completed_posts": call_number},
            },
            "freshness": {
                "status": "fresh",
                "source": "live",
                "generated_at": "2026-04-27T12:00:00+00:00",
                "age_seconds": 0,
            },
            "operational_alerts": [],
        }

    with patch(
        "trr_backend.socials.profile_dashboard.build_social_account_profile_dashboard",
        side_effect=_dashboard_payload,
    ) as dashboard_mock:
        responses = [
            client.get(
                "/api/v1/admin/socials/profiles/instagram/cacheprobe/dashboard?detail=lite&run_id=run-active",
                headers={"Authorization": f"Bearer {token}"},
            )
            for _ in range(2)
        ]

    assert [response.status_code for response in responses] == [200, 200]
    assert responses[0].json()["data"]["catalog_run_progress"]["completed_posts"] == 1
    assert responses[1].json()["data"]["catalog_run_progress"]["completed_posts"] == 2
    assert dashboard_mock.call_count == 2


def test_get_social_account_profile_summary_returns_503_on_session_pool_saturation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        side_effect=DatabaseServiceUnavailableError(
            'Database pool initialization failed: connection to server at "aws-1-us-east-1.pooler.supabase.com" '
            "(18.214.78.123), port 5432 failed: FATAL: MaxClientsInSessionMode: max clients reached",
            reason="session_pool_capacity",
        ),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["reason"] == "session_pool_capacity"


def test_get_social_account_profile_summary_saturation_does_not_fan_out_secondary_reads(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "api.routers.socials.social_profile_reads.get_profile_summary",
        side_effect=DatabaseServiceUnavailableError("session pool saturated", reason="session_pool_capacity"),
    ) as summary_mock:
        with patch("api.routers.socials.social_profile_reads.get_catalog_freshness") as freshness_mock:
            with patch("api.routers.socials.social_profile_reads.get_catalog_gap_analysis_status") as gap_analysis_mock:
                response = client.get(
                    "/api/v1/admin/socials/profiles/instagram/bravotv/summary?detail=lite",
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "session_pool_capacity"
    summary_mock.assert_called_once_with(platform="instagram", account_handle="bravotv", detail="lite")
    freshness_mock.assert_not_called()
    gap_analysis_mock.assert_not_called()


def test_get_social_account_live_profile_total(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "platform": "instagram",
        "account_handle": "thetraitorsus",
        "profile_url": "https://www.instagram.com/thetraitorsus/",
        "live_total_posts_current": 321,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_live_profile_total",
        return_value=expected,
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/thetraitorsus/live-profile-total",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["platform"] == "instagram"
    assert body["account_handle"] == "thetraitorsus"
    assert body["live_total_posts_current"] == 321


def test_get_social_account_profile_hashtags_returns_503_on_session_pool_saturation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_hashtags",
        side_effect=DatabaseServiceUnavailableError(
            'Database pool initialization failed: connection to server at "aws-1-us-east-1.pooler.supabase.com" '
            "(18.214.78.123), port 5432 failed: FATAL: MaxClientsInSessionMode: max clients reached",
            reason="session_pool_capacity",
        ),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/hashtags",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["reason"] == "session_pool_capacity"


def test_get_social_account_profile_posts(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [
            {
                "id": "post-1",
                "source_id": "source-1",
                "platform": "instagram",
                "account_handle": "bravotv",
                "show_name": "RHOSLC",
                "season_number": 6,
            }
        ],
        "pagination": {"page": 2, "page_size": 10, "total": 11, "total_pages": 2},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts?page=2&page_size=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["pagination"]["page"] == 2
    assert mocked.call_args.kwargs["page"] == 2
    assert mocked.call_args.kwargs["page_size"] == 10


def test_get_social_account_profile_posts_accepts_limit_alias(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [],
        "pagination": {"page": 1, "page_size": 20, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts?limit=20",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["pagination"]["page_size"] == 20
    assert mocked.call_args.kwargs["page_size"] == 20


def test_get_social_account_profile_posts_page_size_overrides_limit_alias(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [],
        "pagination": {"page": 1, "page_size": 30, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts?limit=20&page_size=30",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["pagination"]["page_size"] == 30
    assert mocked.call_args.kwargs["page_size"] == 30


def test_get_social_account_profile_posts_forwards_search(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [],
        "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts?page=1&page_size=25&search=%23BravoCon",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["search"] == "#BravoCon"


def test_get_social_account_profile_posts_forwards_comments_only(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [],
        "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts?page=1&page_size=25&comments_only=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["comments_only"] is True


def test_get_social_account_profile_posts_forwards_comment_filter(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [],
        "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts"
            "?page=1&page_size=25&comments_only=true&comment_filter=incomplete",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["comments_only"] is True
    assert mocked.call_args.kwargs["comment_filter"] == "incomplete"


def test_get_social_account_profile_posts_forwards_sort_params(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [],
        "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts"
            "?page=1&page_size=25&comments_only=true&sort_by=missing_comments&sort_dir=asc",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["sort_by"] == "missing_comments"
    assert mocked.call_args.kwargs["sort_dir"] == "asc"


def test_get_social_account_profile_posts_returns_503_on_statement_timeout(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        side_effect=Exception("canceling statement due to statement timeout"),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts?page=1&page_size=25&comments_only=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert body["detail"]["reason"] == "statement_timeout"
    assert body["detail"]["retryable"] is True


def test_get_social_account_catalog_posts(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [
            {
                "id": "catalog-1",
                "source_id": "source-1",
                "platform": "instagram",
                "account_handle": "bravotv",
                "assignment_status": "assigned",
            }
        ],
        "pagination": {"page": 3, "page_size": 5, "total": 11, "total_pages": 3},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/posts?page=3&page_size=5&assignment_status=assigned",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["items"][0]["assignment_status"] == "assigned"
    assert mocked.call_args.kwargs["page"] == 3
    assert mocked.call_args.kwargs["page_size"] == 5
    assert mocked.call_args.kwargs["assignment_status"] == "assigned"


def test_get_social_account_catalog_review_queue(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [
            {
                "id": "review-1",
                "platform": "instagram",
                "account_handle": "bravotv",
                "hashtag": "rhop",
                "review_status": "pending",
            }
        ]
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_review_queue",
        return_value=expected,
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/review-queue",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["items"][0]["hashtag"] == "rhop"


def test_get_social_account_catalog_verification(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "run_id": run_id,
        "expected_total_posts": 16_454,
        "catalog_posts": 16_454,
        "caption_rows": 16_454,
        "stored_hashtag_instances": 18_876,
        "aggregated_hashtag_instances": 18_876,
        "catalog_complete": True,
        "caption_complete": True,
        "hashtag_counts_match": True,
        "verified": True,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_verification",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/verification?run_id={run_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["verified"] is True
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["run_id"] == run_id


def test_get_social_account_catalog_gap_analysis(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "status": "completed",
        "operation_id": "gap-op-1",
        "platform": "instagram",
        "account_handle": "bravotv",
        "result": {
            "platform": "instagram",
            "account_handle": "bravotv",
            "gap_type": "tail_gap",
            "catalog_posts": 15880,
            "materialized_posts": 16575,
            "expected_total_posts": 16575,
            "live_total_posts_current": 16575,
            "missing_from_catalog_count": 695,
            "sample_missing_source_ids": ["ABC123"],
            "has_resumable_frontier": True,
            "needs_recent_sync": False,
            "recommended_action": "backfill_posts",
            "repair_window_start": None,
            "repair_window_end": None,
            "catalog_oldest_post_at": "2015-01-02T12:00:00Z",
            "catalog_newest_post_at": "2026-03-20T14:06:43Z",
            "latest_catalog_run_status": "completed",
            "active_run_status": None,
        },
        "stale": False,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_gap_analysis_status",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/gap-analysis",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["result"]["gap_type"] == "tail_gap"
    assert response.json()["result"]["recommended_action"] == "backfill_posts"
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"


def test_get_social_account_catalog_gap_analysis_returns_503_on_session_pool_saturation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_gap_analysis_status",
        side_effect=DatabaseServiceUnavailableError(
            'Database pool initialization failed: connection to server at "aws-1-us-east-1.pooler.supabase.com" '
            "(18.214.78.123), port 5432 failed: FATAL: MaxClientsInSessionMode: max clients reached",
            reason="session_pool_capacity",
        ),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/gap-analysis",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["reason"] == "session_pool_capacity"


def test_post_social_account_catalog_gap_analysis_run_returns_status_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    started_operation = {
        "id": "gap-op-queued-1",
        "status": "pending",
        "attached": False,
    }
    status_payload = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "status": "queued",
        "operation_id": "gap-op-queued-1",
        "result": None,
        "stale": False,
        "last_requested_at": "2026-03-31T12:00:00.000000+00:00",
        "last_completed_at": None,
        "last_error": None,
    }

    with patch(
        "api.routers.socials._start_social_catalog_gap_analysis_operation",
        return_value=started_operation,
    ) as start_mock:
        with patch(
            "trr_backend.repositories.social_season_analytics.get_social_account_catalog_gap_analysis_status",
            return_value=status_payload,
        ) as status_mock:
            response = client.post(
                "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/gap-analysis/run",
                headers={
                    "Authorization": f"Bearer {token}",
                    "x-trr-tab-session-id": "tab-1",
                    "x-trr-flow-key": "flow-gap-1",
                },
            )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["operation_id"] == "gap-op-queued-1"
    assert response.json()["attached"] is False
    start_mock.assert_called_once()
    status_mock.assert_called_once_with(platform="instagram", account_handle="bravotv")


def test_post_social_account_catalog_backfill(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "catalog-run-1",
        "status": "queued",
        "ingest_mode": "shared_account_catalog_backfill",
        "catalog_action": "backfill",
        "catalog_action_scope": "full_history",
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ) as worker_guard:
            with patch(
                "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
                return_value=expected,
            ) as mocked_begin:
                with patch(
                    "api.routers.socials._queue_catalog_backfill_finalize_task",
                    return_value=None,
                ) as mocked_finalize:
                    response = client.post(
                        "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"backfill_scope": "full_history"},
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "catalog-run-1"
    assert body["status"] == "queued"
    assert body["catalog_action"] == "backfill"
    assert body["catalog_action_scope"] == "full_history"
    assert body["queue_enabled"] is True
    assert body["used_inline_fallback"] is False
    assert body["requires_modal_executor"] is True
    worker_guard.assert_called_once_with(
        required_execution_backend="modal",
        platform="instagram",
    )
    assert mocked_begin.call_args.kwargs["platform"] == "instagram"
    assert mocked_begin.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked_begin.call_args.kwargs["selected_tasks"] == ["post_details"]
    assert mocked_finalize.call_args.kwargs["selected_tasks"] == ["post_details"]


def test_post_social_account_catalog_apify_backfill_route_is_retired(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    response = client.post(
        "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/apify-backfill",
        headers={"Authorization": f"Bearer {token}"},
        json={"results_limit": 10},
    )

    assert response.status_code == 404


def test_post_social_account_catalog_backfill_forwards_selected_tasks(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
                return_value={
                    "run_id": "launch-run-1",
                    "status": "queued",
                    "launch_group_id": "launch-group-1",
                    "selected_tasks": ["post_details", "comments", "media"],
                    "catalog_run_id": "catalog-run-1",
                    "launch_state": "pending",
                    "launch_task_resolution_pending": True,
                },
            ) as mocked_begin:
                with patch(
                    "api.routers.socials._queue_catalog_backfill_finalize_task",
                    return_value=None,
                ) as mocked_finalize:
                    response = client.post(
                        "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
                        headers={"Authorization": f"Bearer {token}"},
                        json={
                            "backfill_scope": "full_history",
                            "selected_tasks": ["comments", "media", "post_details"],
                            "comments_worker_count": 8,
                            "comments_enable_media_followups": True,
                        },
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["launch_group_id"] == "launch-group-1"
    assert body["catalog_run_id"] == "catalog-run-1"
    assert body["launch_state"] == "pending"
    assert body["launch_task_resolution_pending"] is True
    assert mocked_begin.call_args.kwargs["selected_tasks"] == ["post_details", "comments", "media"]
    assert mocked_begin.call_args.kwargs["comments_worker_count"] == 8
    assert mocked_begin.call_args.kwargs["comments_enable_media_followups"] is True
    assert mocked_finalize.call_args.kwargs["selected_tasks"] == ["post_details", "comments", "media"]
    assert mocked_finalize.call_args.kwargs["comments_worker_count"] == 8
    assert mocked_finalize.call_args.kwargs["comments_enable_media_followups"] is True


def test_post_social_account_catalog_backfill_without_comments_uses_async_kickoff(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
                return_value={
                    "run_id": "catalog-run-post-details",
                    "status": "queued",
                    "launch_group_id": "launch-group-post-details",
                    "selected_tasks": ["post_details"],
                    "effective_selected_tasks": ["post_details"],
                    "ingest_mode": "shared_account_catalog_backfill",
                },
            ) as mocked_begin:
                with patch(
                    "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill",
                    side_effect=AssertionError("non-comments launch should use async kickoff"),
                ):
                    with patch(
                        "api.routers.socials._queue_catalog_backfill_finalize_task",
                        return_value=None,
                    ) as mocked_finalize:
                        response = client.post(
                            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
                            headers={"Authorization": f"Bearer {token}"},
                            json={
                                "backfill_scope": "full_history",
                                "selected_tasks": ["post_details"],
                                "detail_worker_count": 8,
                            },
                        )

    assert response.status_code == 200
    assert response.json()["run_id"] == "catalog-run-post-details"
    assert mocked_begin.call_args.kwargs["selected_tasks"] == ["post_details"]
    assert mocked_begin.call_args.kwargs["details_refresh_worker_count"] == 8
    assert mocked_finalize.call_args.kwargs["run_id"] == "catalog-run-post-details"
    assert mocked_finalize.call_args.kwargs["details_refresh_worker_count"] == 8


def test_queue_catalog_backfill_finalize_task_runs_finalize_and_clears_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    finalized: list[dict[str, Any]] = []
    cleared: list[str] = []
    done = Event()
    background_tasks = BackgroundTasks()

    def _finalize(**kwargs: Any) -> dict[str, Any]:
        finalized.append(kwargs)
        done.set()
        return {"run_id": kwargs["run_id"], "status": "queued"}

    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics.finalize_social_account_catalog_backfill_launch",
        _finalize,
    )
    monkeypatch.setattr("trr_backend.repositories.social_season_analytics.is_queue_enabled", lambda: True)
    monkeypatch.setattr(socials_router, "_clear_account_profile_caches", lambda: cleared.append("cleared"))
    monkeypatch.setattr(
        socials_router,
        "Thread",
        lambda *args, **kwargs: SimpleNamespace(
            start=lambda: kwargs["target"](**kwargs["kwargs"]),
            join=lambda: None,
        ),
    )

    socials_router._queue_catalog_backfill_finalize_task(
        background_tasks=background_tasks,
        platform="instagram",
        account_handle="bravotv",
        run_id="catalog-run-1",
        source_scope="bravo",
        date_start=None,
        date_end=None,
        initiated_by="admin@example.com",
        allow_local_dev_inline_bypass=False,
        execution_preference="auto",
        selected_tasks=["post_details", "comments", "media"],
        details_refresh_worker_count=6,
        comments_worker_count=4,
        comments_enable_media_followups=True,
        launch_group_id="launch-group-1",
    )

    assert len(background_tasks.tasks) == 0
    assert done.wait(1)

    assert finalized == [
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "run_id": "catalog-run-1",
            "source_scope": "bravo",
            "date_start": None,
            "date_end": None,
            "initiated_by": "admin@example.com",
            "allow_local_dev_inline_bypass": False,
            "execution_preference": "auto",
            "selected_tasks": ["post_details", "comments", "media"],
            "details_refresh_worker_count": 6,
            "comments_worker_count": 4,
            "comments_enable_media_followups": True,
            "launch_group_id": "launch-group-1",
            "force_catalog_rediscovery": False,
        }
    ]
    assert cleared == ["cleared"]


def test_shared_account_deferred_comments_launch_requires_endpoint_auth_probe() -> None:
    from api.routers import socials as socials_router

    launch_kwargs: dict[str, Any] = {}
    metadata_updates: list[dict[str, Any]] = []

    def _start_comments_scrape(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        launch_kwargs.update(kwargs)
        return {"run_id": "comments-run-1", "status": "queued"}

    comments_run_id = socials_router._start_deferred_comments_inline_followup(
        catalog_run_id="catalog-run-1",
        normalized_platform="instagram",
        normalized_account="bravotv",
        source_scope="network",
        initiated_by="admin@example.com",
        allow_local_dev_inline_bypass=False,
        launch_group_id="launch-group-1",
        result={"effective_selected_tasks": ["comments"]},
        start_social_account_comments_scrape=_start_comments_scrape,
        social_ingest_conflict_error=Exception,
        merge_catalog_run_config=lambda **kwargs: metadata_updates.append(kwargs),
        metadata_dict=lambda value: dict(value or {}) if isinstance(value, dict) else {},
        build_attached_comments_followup=lambda **kwargs: kwargs,
        comments_worker_count=4,
        comments_enable_media_followups=False,
    )

    assert comments_run_id == "comments-run-1"
    assert launch_kwargs["skip_launch_auth_probe"] is False
    assert launch_kwargs["comments_worker_count"] == 4
    assert metadata_updates[0]["metadata_updates"]["deferred_comments_followup"]["state"] == "started"


def test_comments_endpoint_auth_launch_blocker_detail_is_operator_safe() -> None:
    from trr_backend.socials.pipelines.comments import instagram as comments_pipeline

    detail = comments_pipeline._comments_launch_auth_blocker_detail(
        account_handle="@BravoTV",
        probe={
            "shortcode": "SHORT1",
            "status": "auth_blocked",
            "reason": "html_challenge_or_auth_required",
            "auth_source": "browser_session",
            "cookie_fingerprint": "abc123",
            "cookie_fingerprint_algorithm": "sha256:16",
        },
        reason="html_challenge_or_auth_required",
    )

    assert detail["account_handle"] == "bravotv"
    assert detail["probe_shortcode"] == "SHORT1"
    assert detail["reason"] == "html_challenge_or_auth_required"
    assert detail["session_source"] == "browser_session"
    assert detail["cookie_fingerprint"] == "abc123"
    assert "cookie" not in detail["operator_action"].lower()
    assert "sessionid" not in str(detail)


def test_queue_catalog_backfill_finalize_task_starts_inline_fallback_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    executed: list[dict[str, Any]] = []
    done = Event()
    background_tasks = BackgroundTasks()

    monkeypatch.setenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_LOCAL_WORKERS", "3")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "6")
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics.finalize_social_account_catalog_backfill_launch",
        lambda **_kwargs: {
            "run_id": "catalog-run-1",
            "catalog_run_id": "catalog-run-1",
            "comments_run_id": "comments-run-1",
            "selected_tasks": ["post_details", "comments", "media"],
            "effective_selected_tasks": ["post_details", "comments", "media"],
            "status": "running",
        },
    )
    monkeypatch.setattr("trr_backend.repositories.social_season_analytics.is_queue_enabled", lambda: False)
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics.execute_run_with_inline_worker_registration",
        lambda run_id, **kwargs: (
            executed.append({"run_id": run_id, **kwargs})
            or (done.set() if len(executed) >= 4 else None)
            or {"run_id": run_id}
        ),
    )
    monkeypatch.setattr(socials_router, "_clear_account_profile_caches", lambda: None)
    monkeypatch.setattr(
        socials_router,
        "Thread",
        lambda *args, **kwargs: SimpleNamespace(
            start=lambda: kwargs["target"](**kwargs["kwargs"]),
            join=lambda: None,
        ),
    )

    socials_router._queue_catalog_backfill_finalize_task(
        background_tasks=background_tasks,
        platform="instagram",
        account_handle="bravotv",
        run_id="catalog-run-1",
        source_scope="bravo",
        date_start=None,
        date_end=None,
        initiated_by="admin@example.com",
        allow_local_dev_inline_bypass=True,
        execution_preference="auto",
        selected_tasks=["post_details", "comments", "media"],
        details_refresh_worker_count=None,
        comments_worker_count=4,
        comments_enable_media_followups=True,
        launch_group_id="launch-group-1",
    )

    assert len(background_tasks.tasks) == 0
    assert done.wait(1)

    catalog_calls = [call for call in executed if call["run_id"] == "catalog-run-1"]
    comments_calls = [call for call in executed if call["run_id"] == "comments-run-1"]
    assert sorted(call["worker_id"] for call in catalog_calls) == [
        "api-background:catalog:instagram:1",
        "api-background:catalog:instagram:2",
        "api-background:catalog:instagram:3",
    ]
    assert all(call["supported_platforms"] == ["instagram"] for call in catalog_calls)
    assert sorted(call["metadata_updates"]["worker_lane"] for call in catalog_calls) == ["a", "b", "c"]
    assert len(comments_calls) == 1
    assert comments_calls[0]["worker_id"] == "api-background:comments:instagram"
    assert comments_calls[0]["platform"] == "instagram"


def test_post_social_account_catalog_backfill_rejects_empty_selected_tasks(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    response = client.post(
        "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
        headers={"Authorization": f"Bearer {token}"},
        json={"backfill_scope": "full_history", "selected_tasks": []},
    )

    assert response.status_code == 422
    assert "selected_tasks must include at least one" in str(response.json()["detail"]).lower()


def test_post_social_account_catalog_backfill_ignores_date_bounds_for_full_history(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
                return_value={
                    "run_id": "catalog-run-full-history",
                    "status": "queued",
                    "ingest_mode": "shared_account_catalog_backfill",
                },
            ) as mocked_begin:
                with patch(
                    "api.routers.socials._queue_catalog_backfill_finalize_task",
                    return_value=None,
                ) as mocked_finalize:
                    response = client.post(
                        "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
                        headers={"Authorization": f"Bearer {token}"},
                        json={
                            "backfill_scope": "full_history",
                            "date_start": "2026-01-01T00:00:00Z",
                            "date_end": "2026-01-08T00:00:00Z",
                        },
                    )

    assert response.status_code == 200
    assert mocked_begin.call_args.kwargs["date_start"] is None
    assert mocked_begin.call_args.kwargs["date_end"] is None
    assert mocked_finalize.call_args.kwargs["date_start"] is None
    assert mocked_finalize.call_args.kwargs["date_end"] is None


def test_post_social_account_catalog_backfill_twitter_full_history_uses_past_year_window(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    fixed_end = datetime(2026, 5, 12, 12, 0, tzinfo=UTC)
    fixed_start = fixed_end - timedelta(days=365)

    with (
        patch("api.routers.socials._twitter_catalog_backfill_default_window", return_value=(fixed_start, fixed_end)),
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
            return_value={
                "run_id": "catalog-run-twitter-window",
                "status": "queued",
                "ingest_mode": "shared_account_catalog_backfill",
            },
        ) as mocked_begin,
        patch("api.routers.socials._queue_catalog_backfill_finalize_task", return_value=None) as mocked_finalize,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/twitter/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history"},
        )

    assert response.status_code == 200
    assert mocked_begin.call_args.kwargs["date_start"] == fixed_start
    assert mocked_begin.call_args.kwargs["date_end"] == fixed_end
    assert mocked_finalize.call_args.kwargs["date_start"] == fixed_start
    assert mocked_finalize.call_args.kwargs["date_end"] == fixed_end


def test_post_social_account_catalog_backfill_forwards_bounded_window_dates(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
                return_value={
                    "run_id": "catalog-run-windowed",
                    "status": "queued",
                    "ingest_mode": "shared_account_catalog_backfill",
                },
            ) as mocked_begin:
                with patch(
                    "api.routers.socials._queue_catalog_backfill_finalize_task",
                    return_value=None,
                ) as mocked_finalize:
                    response = client.post(
                        "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
                        headers={"Authorization": f"Bearer {token}"},
                        json={
                            "backfill_scope": "bounded_window",
                            "date_start": "2026-01-01T00:00:00Z",
                            "date_end": "2026-01-08T00:00:00Z",
                        },
                    )

    assert response.status_code == 200
    assert mocked_begin.call_args.kwargs["date_start"] == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert mocked_begin.call_args.kwargs["date_end"] == datetime(2026, 1, 8, 23, 59, 59, tzinfo=UTC)
    assert mocked_finalize.call_args.kwargs["date_start"] == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert mocked_finalize.call_args.kwargs["date_end"] == datetime(2026, 1, 8, 23, 59, 59, tzinfo=UTC)


def test_get_social_account_catalog_post_detail_route(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "source_id": "ABC123",
        "title": "Premiere night",
        "content": "Saved caption",
        "permalink": "https://www.instagram.com/p/ABC123/",
        "media_mirror_last_job_id": "mirror-job-1",
        "hosted_media_urls": ["https://cdn.example.com/post.mp4"],
        "source_media_urls": ["https://instagram.example.com/post.mp4"],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_post_detail",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/posts/ABC123/detail",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["source_id"] == "ABC123"
    assert response.json()["media_mirror_last_job_id"] == "mirror-job-1"
    assert mocked.call_args.kwargs == {
        "platform": "instagram",
        "account_handle": "bravotv",
        "source_id": "ABC123",
    }


def test_post_social_account_catalog_backfill_tiktok(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "catalog-run-tt-1",
        "status": "queued",
        "ingest_mode": "shared_account_catalog_backfill",
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ) as worker_guard:
            with patch(
                "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
                return_value=expected,
            ) as mocked_begin:
                with patch(
                    "api.routers.socials._queue_catalog_backfill_finalize_task",
                    return_value=None,
                ) as mocked_finalize:
                    response = client.post(
                        "/api/v1/admin/socials/profiles/tiktok/bravotv/catalog/backfill",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"backfill_scope": "full_history"},
                    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "catalog-run-tt-1"
    worker_guard.assert_called_once_with(required_execution_backend="modal", platform="tiktok")
    assert mocked_begin.call_args.kwargs["platform"] == "tiktok"
    assert mocked_begin.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked_begin.call_args.kwargs["selected_tasks"] == ["post_details", "comments", "media"]
    assert mocked_finalize.call_args.kwargs["selected_tasks"] == ["post_details", "comments", "media"]


def test_post_social_account_catalog_remediate_drift_cancels_and_requeues(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "candidate_job_count": 1,
        "candidate_runs": [{"run_id": "run-stuck-1", "job_ids": ["job-1"]}],
        "cancelled_runs": [{"run_id": "run-stuck-1", "status": "cancelled"}],
        "requeued_canary": {"run_id": "run-canary-1", "status": "queued"},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.remediate_social_account_catalog_runtime_supersession",
        return_value=expected,
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/remediate-drift",
            headers={"Authorization": f"Bearer {token}"},
            json={"requeue_canary": True, "source_scope": "bravo"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["cancelled_runs"][0]["run_id"] == "run-stuck-1"
    assert body["requeued_canary"]["run_id"] == "run-canary-1"
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["requeue_canary"] is True
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["initiated_by"] == "admin@example.com"


def test_post_social_account_catalog_remediate_drift_defaults_to_dry_cancel_without_requeue(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "candidate_job_count": 0,
        "candidate_runs": [],
        "cancelled_runs": [],
        "requeued_canary": None,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.remediate_social_account_catalog_runtime_supersession",
        return_value=expected,
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/remediate-drift",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["requeue_canary"] is False
    assert mocked.call_args.kwargs["source_scope"] == "bravo"


@pytest.mark.parametrize(
    ("platform", "handle", "run_id"),
    [
        ("twitter", "bravotv", "catalog-run-tw-1"),
        ("threads", "bravotv", "catalog-run-th-1"),
        ("youtube", "bravo", "catalog-run-yt-1"),
        ("facebook", "bravotv", "catalog-run-fb-1"),
    ],
)
def test_post_social_account_catalog_backfill_additional_supported_platforms(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
    handle: str,
    run_id: str,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ) as worker_guard:
            with patch(
                "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
                return_value={
                    "run_id": run_id,
                    "status": "queued",
                    "ingest_mode": "shared_account_catalog_backfill",
                },
            ) as mocked_begin:
                with patch(
                    "api.routers.socials._queue_catalog_backfill_finalize_task",
                    return_value=None,
                ) as mocked_finalize:
                    response = client.post(
                        f"/api/v1/admin/socials/profiles/{platform}/{handle}/catalog/backfill",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"backfill_scope": "full_history"},
                    )

    assert response.status_code == 200
    assert response.json()["run_id"] == run_id
    worker_guard.assert_called_once_with(required_execution_backend="modal", platform=platform)
    assert mocked_begin.call_args.kwargs["platform"] == platform
    assert mocked_begin.call_args.kwargs["account_handle"] == handle
    assert mocked_finalize.call_args.kwargs["platform"] == platform
    assert mocked_finalize.call_args.kwargs["account_handle"] == handle


def test_post_social_account_catalog_backfill_requires_modal_executor(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "api.routers.socials.is_remote_job_plane_enabled",
            return_value=False,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "modal executor unavailable",
                worker_health={"healthy": False, "reason": "modal_dispatch_unavailable"},
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_EXECUTOR_REQUIRED"
    assert body["detail"]["required_execution_backend"] == "modal"


def test_post_social_account_catalog_backfill_surfaces_auth_preflight_failure_when_remote_enforced(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch("api.routers.socials.is_remote_job_plane_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "Modal social dispatch auth preflight is not ready for instagram.",
                worker_health={
                    "healthy": True,
                    "reason": "modal_executor_ready",
                    "shared_account_backfill_readiness": {
                        "ready": False,
                        "reason": "validation_exception:Error",
                        "platform": "instagram",
                        "platform_requires_remote_auth": True,
                        "platform_remote_auth_ready": False,
                        "platform_remote_auth_reason": "validation_exception:Error",
                    },
                },
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_EXECUTOR_REQUIRED"
    assert body["detail"]["required_execution_backend"] == "modal"
    assert "modal remote executor" in body["detail"]["message"].lower()
    assert "reporting heartbeats" not in body["detail"]["message"].lower()
    readiness = body["detail"]["worker_health"]["shared_account_backfill_readiness"]
    assert readiness["reason"] == "validation_exception:Error"
    assert readiness["platform_remote_auth_ready"] is False


def test_post_social_account_catalog_backfill_prefers_modal_when_available_even_if_inline_fallback_allowed(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch("api.routers.socials._start_runs_in_background") as mocked_background,
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ) as worker_guard,
        patch(
            "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
            return_value={
                "run_id": "catalog-run-modal-1",
                "status": "queued",
                "ingest_mode": "shared_account_catalog_backfill",
            },
        ) as mocked_start,
        patch(
            "api.routers.socials._queue_catalog_backfill_finalize_task",
            return_value=None,
        ) as mocked_finalize,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history", "allow_inline_dev_fallback": True},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "catalog-run-modal-1"
    assert body["status"] == "queued"
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["queue_enabled"] is True
    assert body["used_inline_fallback"] is False
    assert body["requires_modal_executor"] is True
    worker_guard.assert_called_once()
    mocked_background.assert_not_called()
    assert mocked_start.call_args.kwargs["allow_local_dev_inline_bypass"] is False
    mocked_finalize.assert_called_once()


def test_post_social_account_catalog_backfill_prefer_local_inline_forces_inline_when_available(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch("api.routers.socials._is_local_or_dev_runtime", return_value=True),
        patch("api.routers.socials._start_runs_in_background") as mocked_background,
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ) as worker_guard,
        patch(
            "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill",
            return_value={
                "run_id": "catalog-run-inline-preference-1",
                "status": "pending",
                "ingest_mode": "shared_account_catalog_backfill",
            },
        ) as mocked_start,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/tiktok/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history", "execution_preference": "prefer_local_inline"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "catalog-run-inline-preference-1"
    assert body["status"] == "started"
    assert body["execution_mode"] == "inline"
    assert body["execution_mode_canonical"] == "inline_fallback"
    assert body["queue_enabled"] is False
    assert body["used_inline_fallback"] is True
    worker_guard.assert_not_called()
    mocked_background.assert_called_once()
    assert mocked_start.call_args.kwargs["inline_worker_id"] == "api-background:catalog:tiktok"
    assert mocked_start.call_args.kwargs["allow_local_dev_inline_bypass"] is True
    assert mocked_start.call_args.kwargs["execution_preference"] == "prefer_local_inline"


def test_post_social_account_catalog_backfill_prefer_local_inline_returns_validation_error_when_unavailable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch("api.routers.socials._is_local_or_dev_runtime", return_value=False),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ) as worker_guard,
        patch(
            "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill"
        ) as mocked_start,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/tiktok/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history", "execution_preference": "prefer_local_inline"},
        )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "SOCIAL_LOCAL_INLINE_PREFERENCE_UNAVAILABLE"
    assert detail["execution_preference"] == "prefer_local_inline"
    worker_guard.assert_not_called()
    mocked_start.assert_not_called()


def test_post_social_account_catalog_backfill_blocks_inline_fallback_when_modal_required(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch("api.routers.socials._start_runs_in_background") as mocked_background,
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "modal executor unavailable",
                worker_health={"healthy": False, "reason": "modal_dispatch_unavailable"},
            ),
        ) as worker_guard,
        patch(
            "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
            return_value={
                "run_id": "catalog-run-inline-1",
                "catalog_run_id": "catalog-run-inline-1",
                "status": "running",
                "launch_state": "pending",
                "launch_task_resolution_pending": True,
                "ingest_mode": "shared_account_catalog_backfill",
            },
        ) as mocked_begin,
        patch(
            "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill",
            side_effect=AssertionError("inline Instagram launch should use async kickoff"),
        ) as mocked_start,
        patch(
            "api.routers.socials._queue_catalog_backfill_finalize_task",
            return_value=None,
        ) as mocked_finalize,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history", "allow_inline_dev_fallback": True},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_EXECUTOR_REQUIRED"
    assert body["detail"]["required_execution_backend"] == "modal"
    worker_guard.assert_called_once()
    mocked_background.assert_not_called()
    mocked_begin.assert_not_called()
    mocked_start.assert_not_called()
    mocked_finalize.assert_not_called()


def test_post_social_account_catalog_backfill_blocks_local_admin_override_when_modal_required(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("TRR_ALLOW_LOCAL_ADMIN_OPERATION_OVERRIDE", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch("api.routers.socials.is_remote_job_plane_enabled", return_value=True),
        patch("api.routers.socials._start_runs_in_background") as mocked_background,
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError("worker unavailable", worker_health={}),
        ) as worker_guard,
        patch(
            "api.routers.socials.resolve_modal_function",
            return_value={"resolved": False, "reason": "not available", "error": None},
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
            return_value={
                "run_id": "catalog-run-inline-override-1",
                "catalog_run_id": "catalog-run-inline-override-1",
                "status": "running",
                "launch_state": "pending",
                "launch_task_resolution_pending": True,
                "ingest_mode": "shared_account_catalog_backfill",
            },
        ) as mocked_begin,
        patch(
            "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill",
            side_effect=AssertionError("inline Instagram launch should use async kickoff"),
        ) as mocked_start,
        patch(
            "api.routers.socials._queue_catalog_backfill_finalize_task",
            return_value=None,
        ) as mocked_finalize,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_EXECUTOR_REQUIRED"
    assert body["detail"]["required_execution_backend"] == "modal"
    worker_guard.assert_called_once()
    mocked_background.assert_not_called()
    mocked_begin.assert_not_called()
    mocked_start.assert_not_called()
    mocked_finalize.assert_not_called()


def test_post_social_account_catalog_backfill_returns_modal_dispatch_unavailable_when_target_unresolvable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ),
        patch(
            "api.routers.socials.resolve_modal_function",
            return_value={
                "resolved": False,
                "reason": "modal_app_not_found",
                "error": "App 'trr-backend-jobs' not found in environment 'main'",
                "app_name": "trr-backend-jobs",
                "function_name": "run_social_job",
                "modal_environment": "main",
            },
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill"
        ) as mocked_start,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_DISPATCH_UNAVAILABLE"
    assert body["detail"]["reason"] == "modal_app_not_found"
    assert body["detail"]["configured_app_name"] == "trr-backend-jobs"
    assert body["detail"]["configured_function_name"] == "run_social_job"
    assert body["detail"]["modal_environment"] == "main"
    mocked_start.assert_not_called()


def test_post_social_account_catalog_backfill_returns_conflict_for_active_profile_run(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialIngestConflictError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
            side_effect=SocialIngestConflictError(
                "SOCIAL_ACCOUNT_CATALOG_RUN_ALREADY_ACTIVE",
                "Catalog run run-active-1 is already running for @bravotv.",
                detail={"run_id": "run-active-1", "status": "running"},
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history"},
        )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "SOCIAL_ACCOUNT_CATALOG_RUN_ALREADY_ACTIVE"
    assert response.json()["detail"]["run_id"] == "run-active-1"


def test_post_social_account_catalog_backfill_conflict_serializes_datetime_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime

    from trr_backend.repositories.social_season_analytics import SocialIngestConflictError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.begin_social_account_catalog_backfill_launch",
            side_effect=SocialIngestConflictError(
                "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE",
                "Comments scrape run comments-run-1 is already active for @bravotv.",
                detail={
                    "run_id": "comments-run-1",
                    "job_id": "job-1",
                    "status": "queued",
                    "created_at": datetime(2026, 4, 21, 19, 1, 49, tzinfo=UTC),
                    "started_at": None,
                    "completed_at": None,
                },
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/backfill",
            headers={"Authorization": f"Bearer {token}"},
            json={"backfill_scope": "full_history"},
        )

    assert response.status_code == 409
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE"
    assert body["detail"]["run_id"] == "comments-run-1"
    assert body["detail"]["created_at"] == "2026-04-21T19:01:49+00:00"


def test_post_social_account_catalog_sync_recent(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "catalog-run-2",
        "status": "queued",
        "ingest_mode": "shared_account_catalog_backfill",
        "catalog_action": "sync_recent",
        "catalog_action_scope": "recent_window",
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.sync_recent_social_account_catalog",
                return_value=expected,
            ) as mocked:
                response = client.post(
                    "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-recent",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"lookback_days": 3},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "catalog-run-2"
    assert body["status"] == "queued"
    assert body["catalog_action"] == "sync_recent"
    assert body["catalog_action_scope"] == "recent_window"
    assert body["queue_enabled"] is True
    assert body["used_inline_fallback"] is False
    assert body["requires_modal_executor"] is True
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["lookback_days"] == 3


def test_post_social_account_catalog_sync_newer(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "catalog-run-3",
        "status": "queued",
        "ingest_mode": "shared_account_catalog_backfill",
        "catalog_action": "sync_newer",
        "catalog_action_scope": "head_gap",
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.sync_newer_social_account_catalog",
                return_value=expected,
            ) as mocked:
                response = client.post(
                    "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-newer",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source_scope": "bravo"},
                )

    assert response.status_code == 200
    assert response.json()["run_id"] == "catalog-run-3"
    assert response.json()["catalog_action"] == "sync_newer"
    assert response.json()["catalog_action_scope"] == "head_gap"
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"


def test_post_social_account_catalog_sync_newer_returns_validation_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialIngestValidationError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.sync_newer_social_account_catalog",
            side_effect=SocialIngestValidationError(
                "NO_STORED_POSTS",
                "No stored posts found for this account. Run a full backfill first.",
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-newer",
            headers={"Authorization": f"Bearer {token}"},
            json={"source_scope": "bravo"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "NO_STORED_POSTS"


def test_post_social_account_catalog_resume_tail(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "catalog-run-4",
        "status": "queued",
        "ingest_mode": "shared_account_catalog_backfill",
        "catalog_action": "backfill",
        "catalog_action_scope": "full_history",
        "backfill_mode": "resume_frontier",
        "resumed_from_cursor": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill",
                return_value=expected,
            ) as mocked:
                response = client.post(
                    "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/resume-tail",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source_scope": "bravo"},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "catalog-run-4"
    assert body["catalog_action"] == "backfill"
    assert body["catalog_action_scope"] == "full_history"
    assert body["backfill_mode"] == "resume_frontier"
    assert body["deprecated_route"] is True
    assert body["queue_enabled"] is True
    assert body["used_inline_fallback"] is False
    assert body["requires_modal_executor"] is True
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["selected_tasks"] == ["post_details", "comments", "media"]


def test_post_social_account_catalog_resume_tail_returns_validation_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialIngestValidationError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.launch_social_account_catalog_backfill",
            side_effect=SocialIngestValidationError(
                "SOCIAL_REMOTE_WORKER_REQUIRED",
                "Remote worker readiness is required.",
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/resume-tail",
            headers={"Authorization": f"Bearer {token}"},
            json={"source_scope": "bravo"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "SOCIAL_REMOTE_WORKER_REQUIRED"


@pytest.mark.parametrize("route_suffix", ["repair-auth", "manual-auth"])
def test_post_social_account_catalog_repair_auth_route(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    route_suffix: str,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = "12345678-1234-1234-1234-123456789012"
    expected = {
        "run_id": run_id,
        "status": "accepted",
        "repair_status": "running",
        "operational_state": "blocked_auth",
        "repair_action": "repair_instagram_auth",
        "resume_stage": "posts",
    }

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.request_social_account_catalog_run_auth_repair",
            return_value=expected,
        ) as mocked_request,
        patch(
            "trr_backend.repositories.social_season_analytics.execute_social_account_catalog_run_auth_repair",
            return_value={"ok": True},
        ) as mocked_execute,
    ):
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/runs/{run_id}/{route_suffix}",
            headers={"Authorization": f"Bearer {token}"},
            json={"operator_confirmation": "I UNDERSTAND INSTAGRAM AUTH RISK"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert body["repair_status"] == "running"
    assert body["operational_state"] == "blocked_auth"
    assert body["resume_stage"] == "posts"
    assert mocked_request.call_args.kwargs["platform"] == "instagram"
    assert mocked_request.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked_request.call_args.kwargs["run_id"] == run_id
    assert mocked_execute.called


def test_post_social_account_catalog_repair_auth_route_for_tiktok_cookie_refresh(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = "12345678-1234-1234-1234-123456789099"
    expected = {
        "run_id": run_id,
        "status": "accepted",
        "repair_status": "running",
        "operational_state": "blocked_auth",
        "repair_action": "cookie_refresh",
        "resume_stage": "discovery",
    }

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.request_social_account_catalog_run_auth_repair",
            return_value=expected,
        ) as mocked_request,
        patch(
            "trr_backend.repositories.social_season_analytics.execute_social_account_catalog_run_auth_repair",
            return_value={"ok": True},
        ) as mocked_execute,
    ):
        response = client.post(
            f"/api/v1/admin/socials/profiles/tiktok/bravowwhl/catalog/runs/{run_id}/repair-auth",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert body["repair_status"] == "running"
    assert body["repair_action"] == "cookie_refresh"
    assert body["resume_stage"] == "discovery"
    assert mocked_request.call_args.kwargs["platform"] == "tiktok"
    assert mocked_request.call_args.kwargs["account_handle"] == "bravowwhl"
    assert mocked_request.call_args.kwargs["run_id"] == run_id
    assert mocked_execute.called


def test_post_social_account_catalog_repair_auth_route_requires_instagram_confirmation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = "12345678-1234-1234-1234-123456789012"

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.request_social_account_catalog_run_auth_repair",
        ) as mocked_request,
        patch(
            "trr_backend.repositories.social_season_analytics.execute_social_account_catalog_run_auth_repair",
        ) as mocked_execute,
    ):
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/runs/{run_id}/repair-auth",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INSTAGRAM_AUTH_REFRESH_CONFIRMATION_REQUIRED"
    mocked_request.assert_not_called()
    mocked_execute.assert_not_called()


def test_get_social_account_cookie_health_marks_comments_auth_blocked_unhealthy(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.check_platform_cookie_health",
            return_value={
                "platform": "instagram",
                "required": True,
                "healthy": True,
                "reason": None,
                "refresh_supported": True,
                "refresh_available": True,
                "refresh_action": "instagram_auth_repair",
                "refresh_label": "Manual Instagram Auth",
                "source_kind": "default_file",
                "source_path": "/tmp/instagram_cookies.json",
            },
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.probe_modal_instagram_posts_auth_health",
            return_value={
                "platform": "instagram",
                "account_handle": "thetraitorsus",
                "ready": True,
                "status": "valid",
                "result": "valid",
                "execution_backend": "modal",
            },
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.probe_modal_instagram_comments_auth_health",
            return_value={
                "platform": "instagram",
                "account_handle": "thetraitorsus",
                "shortcode": "DSfwXnYAaEs",
                "ready": False,
                "status": "auth_blocked",
                "result": "auth_blocked",
                "reason": "html_challenge_or_auth_required",
                "execution_backend": "modal",
            },
        ),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/thetraitorsus/cookies/health?posts_auth=true&comments_auth=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["healthy"] is False
    assert body["reason"] == "html_challenge_or_auth_required"
    assert body["auth_surface_blocked"] is True
    assert body["auth_surface_probe_only"] is True
    assert body["posts_auth_health"]["ready"] is True
    assert body["posts_auth_health"]["probe_only"] is True
    assert body["posts_auth_health"]["probe_source"] == "cookie_health"
    assert body["posts_auth_health"]["repair_action"] is None
    assert body["posts_auth_health"]["repair_available"] is False
    assert body["posts_auth_probe"]["probe_only"] is True
    assert body["posts_auth_probe"]["probe_source"] == "cookie_health"
    assert body["posts_auth_probe"]["repair_action"] is None
    assert body["posts_auth_probe"]["repair_available"] is False
    assert body["comments_auth_health"] == {
        "platform": "instagram",
        "account_handle": "thetraitorsus",
        "shortcode": "DSfwXnYAaEs",
        "ready": False,
        "status": "auth_blocked",
        "category": "auth",
        "reason": "html_challenge_or_auth_required",
        "execution_backend": "modal",
        "probe_only": True,
        "probe_source": "cookie_health",
        "repair_action": None,
        "repair_available": False,
    }
    assert body["comments_auth_probe"]["probe_only"] is True
    assert body["comments_auth_probe"]["probe_source"] == "cookie_health"
    assert body["comments_auth_probe"]["repair_action"] is None
    assert body["comments_auth_probe"]["repair_available"] is False


def test_post_social_account_cookie_refresh_route_defaults_allow_cookie_refresh_and_returns_instagram_auth_repair_summary(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.check_platform_cookie_health",
            return_value={
                "platform": "instagram",
                "required": True,
                "healthy": False,
                "reason": "expired",
                "refresh_supported": True,
                "refresh_available": True,
                "refresh_action": "instagram_auth_repair",
                "refresh_label": "Manual Instagram Auth",
                "source_kind": "default_file",
            },
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.refresh_platform_cookies_interactive",
            return_value={
                "platform": "instagram",
                "required": True,
                "healthy": True,
                "reason": None,
                "refresh_supported": True,
                "refresh_available": True,
                "refresh_action": "instagram_auth_repair",
                "refresh_label": "Manual Instagram Auth",
                "source_kind": "default_file",
                "success": True,
                "steps": [{"name": "refresh", "status": "ok"}],
                "remote_auth_probe": {"platform": "instagram", "ready": True},
            },
        ) as mocked_refresh,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/cookies/refresh",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "headless": False,
                "timeout_seconds": 180,
                "operator_confirmation": "I UNDERSTAND INSTAGRAM AUTH RISK",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["refresh_action"] == "instagram_auth_repair"
    assert body["steps"] == [{"name": "refresh", "status": "ok"}]
    assert body["remote_auth_probe"] == {"platform": "instagram", "ready": True}
    mocked_refresh.assert_called_once()
    assert mocked_refresh.call_args.kwargs["allow_cookie_refresh"] is True


def test_get_social_account_comments_run_progress_route_is_read_only(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = uuid4()
    expected = {
        "run_id": str(run_id),
        "run_status": "running",
        "post_progress": {"completed_posts": 1, "matched_posts": 1, "total_posts": 2},
        "comment_shards": [],
        "shards": [],
        "shard_progress": [],
    }

    with patch(
        "trr_backend.socials.pipelines.comments.instagram.get_social_account_comments_scrape_run_progress",
        return_value=expected,
    ) as progress_mock:
        response = client.get(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/progress",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    progress_mock.assert_called_once_with(
        platform="instagram",
        account_handle="bravotv",
        run_id=str(run_id),
        auto_rebalance_slow_shards=False,
    )


def test_post_social_account_comments_run_rebalance_route_is_explicit_mutation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = uuid4()
    expected = {
        "created_job_ids": ["job-1", "job-2"],
        "rebalanced_source_job_ids": ["slow-job-1"],
        "reason": None,
    }

    with patch(
        "trr_backend.socials.pipelines.comments.instagram.rebalance_slow_instagram_comments_shards",
        return_value=expected,
    ) as rebalance_mock:
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/rebalance",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    rebalance_mock.assert_called_once_with(run_id=str(run_id))


def test_post_social_account_comments_run_rebalance_rejects_non_instagram(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = uuid4()

    with patch("trr_backend.socials.pipelines.comments.instagram.rebalance_slow_instagram_comments_shards") as rebalance_mock:
        response = client.post(
            f"/api/v1/admin/socials/profiles/tiktok/bravotv/comments/runs/{run_id}/rebalance",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "UNSUPPORTED_PLATFORM"
    rebalance_mock.assert_not_called()


def test_post_social_account_cookie_refresh_route_blocks_instagram_auth_repair_on_remote_runtime(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.check_platform_cookie_health",
            return_value={
                "platform": "instagram",
                "required": True,
                "healthy": False,
                "reason": "expired",
                "refresh_supported": True,
                "refresh_available": False,
                "refresh_action": "instagram_auth_repair",
                "refresh_label": "Manual Instagram Auth",
                "source_kind": "default_file",
            },
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.refresh_platform_cookies_interactive",
        ) as mocked_refresh,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/cookies/refresh",
            headers={"Authorization": f"Bearer {token}"},
            json={"headless": False, "timeout_seconds": 180},
        )

    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "COOKIE_REFRESH_REQUIRES_LOCAL"
    mocked_refresh.assert_not_called()


def test_post_social_account_cookie_refresh_route_requires_instagram_confirmation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.check_platform_cookie_health",
            return_value={
                "platform": "instagram",
                "required": True,
                "healthy": False,
                "reason": "expired",
                "refresh_supported": True,
                "refresh_available": True,
                "refresh_action": "instagram_auth_repair",
                "refresh_label": "Manual Instagram Auth",
                "source_kind": "default_file",
            },
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.refresh_platform_cookies_interactive",
        ) as mocked_refresh,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/cookies/refresh",
            headers={"Authorization": f"Bearer {token}"},
            json={"headless": False, "timeout_seconds": 180},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INSTAGRAM_AUTH_REFRESH_CONFIRMATION_REQUIRED"
    mocked_refresh.assert_not_called()


def test_post_social_account_cookie_refresh_route_returns_instagram_auth_repair_failure_summary(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.check_platform_cookie_health",
            return_value={
                "platform": "instagram",
                "required": True,
                "healthy": False,
                "reason": "expired",
                "refresh_supported": True,
                "refresh_available": True,
                "refresh_action": "instagram_auth_repair",
                "refresh_label": "Manual Instagram Auth",
                "source_kind": "default_file",
            },
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.refresh_platform_cookies_interactive",
            return_value={
                "platform": "instagram",
                "required": True,
                "healthy": False,
                "reason": "remote_probe_failed",
                "refresh_supported": True,
                "refresh_available": True,
                "refresh_action": "instagram_auth_repair",
                "refresh_label": "Manual Instagram Auth",
                "source_kind": "default_file",
                "success": False,
                "steps": [{"name": "verify_remote_auth", "status": "failed"}],
                "remote_auth_probe": {
                    "platform": "instagram",
                    "ready": False,
                    "reason": "checkpoint_required",
                },
            },
        ) as mocked_refresh,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/cookies/refresh",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "headless": False,
                "timeout_seconds": 180,
                "operator_confirmation": "I UNDERSTAND INSTAGRAM AUTH RISK",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    assert body["healthy"] is False
    assert body["refresh_action"] == "instagram_auth_repair"
    assert body["steps"] == [{"name": "verify_remote_auth", "status": "failed"}]
    assert body["remote_auth_probe"] == {
        "platform": "instagram",
        "ready": False,
        "reason": "checkpoint_required",
    }
    mocked_refresh.assert_called_once()


def test_post_social_account_catalog_sync_recent_returns_modal_dispatch_unavailable_when_target_unresolvable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ),
        patch(
            "api.routers.socials.resolve_modal_function",
            return_value={
                "resolved": False,
                "reason": "modal_function_not_found",
                "error": "Function 'run_social_job' not found in app 'trr-backend-jobs'",
                "app_name": "trr-backend-jobs",
                "function_name": "run_social_job",
                "modal_environment": "main",
            },
        ),
        patch("trr_backend.repositories.social_season_analytics.sync_recent_social_account_catalog") as mocked_start,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-recent",
            headers={"Authorization": f"Bearer {token}"},
            json={"lookback_days": 1},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_DISPATCH_UNAVAILABLE"
    assert body["detail"]["reason"] == "modal_function_not_found"
    assert body["detail"]["configured_app_name"] == "trr-backend-jobs"
    assert body["detail"]["configured_function_name"] == "run_social_job"
    assert body["detail"]["modal_environment"] == "main"
    mocked_start.assert_not_called()


def test_post_social_account_catalog_sync_recent_serializes_worker_health_on_modal_requirement(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "Modal social dispatch is required for this social ingest job.",
                worker_health={
                    "healthy": True,
                    "healthy_workers": 1,
                    "last_seen_at": datetime(2026, 3, 27, 18, 20, 3, tzinfo=UTC),
                },
            ),
        ),
        patch("trr_backend.repositories.social_season_analytics.sync_recent_social_account_catalog") as mocked_start,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-recent",
            headers={"Authorization": f"Bearer {token}"},
            json={"lookback_days": 1},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_EXECUTOR_REQUIRED"
    assert body["detail"]["worker_health"]["healthy"] is True
    assert body["detail"]["worker_health"]["last_seen_at"] == "2026-03-27T18:20:03+00:00"
    mocked_start.assert_not_called()


def test_post_social_account_catalog_sync_recent_prefers_modal_when_available_even_if_inline_fallback_allowed(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch("api.routers.socials._start_runs_in_background") as mocked_background,
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value=None,
        ) as worker_guard,
        patch(
            "trr_backend.repositories.social_season_analytics.sync_recent_social_account_catalog",
            return_value={
                "run_id": "catalog-run-modal-2",
                "status": "queued",
                "ingest_mode": "shared_account_catalog_backfill",
            },
        ) as mocked_start,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-recent",
            headers={"Authorization": f"Bearer {token}"},
            json={"lookback_days": 1, "allow_inline_dev_fallback": True},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "catalog-run-modal-2"
    assert body["status"] == "queued"
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["queue_enabled"] is True
    assert body["used_inline_fallback"] is False
    assert body["requires_modal_executor"] is True
    worker_guard.assert_called_once()
    mocked_background.assert_not_called()
    assert mocked_start.call_args.kwargs["inline_worker_id"] is None
    assert mocked_start.call_args.kwargs["allow_local_dev_inline_bypass"] is False


def test_post_social_account_catalog_run_cancel(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    expected = {
        "run_id": run_id,
        "status": "cancelled",
        "accepted": True,
    }

    with (
        patch(
            "trr_backend.repositories.social_season_analytics.request_cancel_social_account_catalog_run",
            return_value=expected,
        ) as mocked,
        patch("api.routers.socials._cancel_catalog_run_in_background", return_value=None) as background_mock,
    ):
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/runs/{run_id}/cancel",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["run_id"] == run_id
    background_mock.assert_called_once()


def test_post_social_account_comments_run_cancel(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    expected = {
        "run_id": run_id,
        "status": "cancelled",
        "accepted": True,
        "cancelled_jobs": 1,
    }

    with patch(
        "trr_backend.socials.pipelines.comments.instagram.cancel_social_account_comments_run",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/cancel",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["cancelled_by"] == "admin@example.com"


def test_post_social_account_comments_job_cancel(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    job_id = str(uuid4())
    expected = {
        "run_id": run_id,
        "job_id": job_id,
        "status": "cancelled",
        "accepted": True,
    }

    with patch(
        "trr_backend.socials.pipelines.comments.instagram.cancel_social_account_comments_job",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/jobs/{job_id}/cancel",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["job_id"] == job_id
    assert mocked.call_args.kwargs["cancelled_by"] == "admin@example.com"


def test_post_social_account_catalog_run_dismiss(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    expected = {
        "run_id": run_id,
        "status": "failed",
        "dismissed": True,
        "dismissed_at": "2026-03-19T20:00:00.000Z",
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.dismiss_social_account_catalog_run",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/runs/{run_id}/dismiss",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["dismissed"] is True
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["run_id"] == run_id


def test_post_social_account_catalog_review_queue_resolve(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    item_id = str(uuid4())
    show_id = str(uuid4())
    expected = {
        "item_id": item_id,
        "review_status": "resolved_show_hashtag",
        "resolution_action": "assign_show",
        "resolved_show_id": show_id,
        "resolved_season_id": None,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.resolve_social_account_catalog_review_queue_item",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/review-queue/{item_id}/resolve",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "resolution_action": "assign_show",
                "show_id": show_id,
            },
        )

    assert response.status_code == 200
    assert response.json()["item_id"] == item_id
    assert response.json()["resolved_show_id"] == show_id
    assert mocked.call_args.kwargs["item_id"] == item_id
    assert mocked.call_args.kwargs["resolution_action"] == "assign_show"
    assert mocked.call_args.kwargs["show_id"] == show_id


def test_put_social_account_profile_hashtags(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    payload = {
        "hashtags": [
            {
                "hashtag": "rhoslc",
                "assignments": [
                    {"show_id": show_id},
                ],
            }
        ]
    }
    expected = {
        "items": [
            {
                "hashtag": "rhoslc",
                "assignments": [
                    {"show_id": show_id, "season_id": None},
                ],
            }
        ]
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.put_social_account_profile_hashtags",
        return_value=expected,
    ) as mocked:
        response = client.put(
            "/api/v1/admin/socials/profiles/instagram/bravotv/hashtags",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 200
    assert response.json()["items"][0]["hashtag"] == "rhoslc"
    assert mocked.call_args.kwargs["updated_by"] == "admin@example.com"


def test_get_social_account_profile_hashtags_forwards_window(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_hashtags",
        return_value={"items": []},
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/hashtags?window=30d",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["window"] == "30d"


def test_post_social_account_catalog_freshness(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "eligible": True,
        "checked_at": "2026-03-24T17:00:00Z",
        "stored_total_posts": 100,
        "live_total_posts_current": 103,
        "delta_posts": 3,
        "needs_recent_sync": True,
    }

    with patch(
        "api.routers.socials.social_profile_reads.get_catalog_freshness",
        return_value=expected,
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/freshness",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["delta_posts"] == 3
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
    assert mocked.call_args.kwargs["use_cached_live_total_only"] is True
    assert mocked.call_args.kwargs["statement_timeout_ms"] == 3000


def test_post_social_account_catalog_freshness_force_uses_deep_probe(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "api.routers.socials.social_profile_reads.get_catalog_freshness",
        return_value={"platform": "instagram", "account_handle": "bravotv", "delta_posts": 4},
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/freshness?force=true&statement_timeout_ms=4000",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["delta_posts"] == 4
    assert "use_cached_live_total_only" not in mocked.call_args.kwargs
    assert mocked.call_args.kwargs["statement_timeout_ms"] == 4000


def test_post_social_account_catalog_freshness_returns_stale_on_default_refresh_failure(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    cache_key = socials_router._account_profile_cache_key(
        surface="catalog-freshness",
        platform="instagram",
        account_handle="bravotv",
        extra=(3000,),
    )
    stale_payload = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "eligible": True,
        "checked_at": "2026-03-24T17:00:00Z",
        "stored_total_posts": 100,
        "live_total_posts_current": 103,
        "delta_posts": 3,
        "needs_recent_sync": True,
    }
    socials_router._ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE[cache_key] = (
        socials_router.monotonic() - 1,
        stale_payload,
    )

    with patch(
        "api.routers.socials.social_profile_reads.get_catalog_freshness",
        side_effect=DatabaseServiceUnavailableError("pool exhausted", reason="pool_capacity"),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/freshness",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["delta_posts"] == 3
    assert payload["stale"] is True
    assert payload["degraded"] is True
    assert payload["freshness_error"]["code"] == "CATALOG_FRESHNESS_REFRESH_FAILED"


def test_post_social_account_catalog_freshness_returns_degraded_recent_runs_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.repositories.social_season_analytics as social_repo
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda **_kwargs: nullcontext(object()))
    monkeypatch.setattr(
        social_repo.pg,
        "db_cursor",
        lambda conn=None, **_kwargs: nullcontext(SimpleNamespace(execute=lambda *_args, **_kwargs: None)),
    )
    monkeypatch.setattr(social_repo, "_assert_social_account_profile_exists", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        social_repo,
        "_catalog_recent_runs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DatabaseServiceUnavailableError("pool exhausted", reason="pool_capacity")
        ),
    )
    monkeypatch.setattr(social_repo, "_shared_catalog_total_posts", lambda *_args, **_kwargs: 100)
    monkeypatch.setattr(social_repo, "_catalog_newest_stored_post_at", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(social_repo, "_catalog_oldest_stored_post_at", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(social_repo, "_latest_account_frontier", lambda *_args, **_kwargs: {})

    response = client.post(
        "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/freshness",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is True
    assert payload["recent_runs_available"] is False
    assert payload["stored_total_posts"] == 100
    assert payload["freshness_error"]["code"] == "CATALOG_RECENT_RUNS_UNAVAILABLE"
    assert payload["freshness_error"]["reason"] == "pool_capacity"


def test_post_social_account_catalog_freshness_returns_degraded_on_session_pool_saturation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "api.routers.socials.social_profile_reads.get_catalog_freshness",
        side_effect=DatabaseServiceUnavailableError(
            'Database pool initialization failed: connection to server at "aws-1-us-east-1.pooler.supabase.com" '
            "(18.214.78.123), port 5432 failed: FATAL: MaxClientsInSessionMode: max clients reached",
            reason="session_pool_capacity",
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/catalog/freshness",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is True
    assert payload["stale"] is False
    assert payload["eligible"] is False
    assert payload["reason"] == "catalog_freshness_unavailable"
    assert payload["freshness_error"]["code"] == "CATALOG_FRESHNESS_UNAVAILABLE"
    assert payload["freshness_error"]["reason"] == "session_pool_capacity"


def test_get_social_account_profile_collaborators_tags(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "collaborators": [{"handle": "andycohen", "usage_count": 3}],
        "tags": [{"handle": "bravoandy", "usage_count": 5}],
        "mentions": [{"handle": "peacock", "usage_count": 8}],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_collaborators_tags",
        return_value=expected,
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/collaborators-tags",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["collaborators"][0]["handle"] == "andycohen"
    assert body["tags"][0]["handle"] == "bravoandy"
    assert body["mentions"][0]["handle"] == "peacock"


def test_social_account_profile_summary_invalid_platform_returns_400(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        side_effect=ValueError("INVALID_PLATFORM_FILTER: Unsupported platform 'myspace'"),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/myspace/bravotv/summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_PLATFORM_FILTER"


def test_social_account_profile_summary_missing_account_returns_404(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        side_effect=LookupError("Social account profile not found."),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/missing/summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Social account profile not found."


def test_post_shared_ingest_returns_run_metadata(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "shared-run-123",
        "status": "queued",
        "source_scope": "network",
        "ingest_mode": "shared_account_async",
        "shared_scrape_status": {"status": "queued", "job_count": 4},
        "classification_status": None,
        "materialization_status": None,
        "review_queue_count": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.ingest_shared_accounts", return_value=expected
        ) as ingest_mock:
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    "/api/v1/admin/socials/shared/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source_scope": "bravo", "platforms": ["instagram", "twitter"]},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "shared-run-123"
    assert body["ingest_mode"] == "shared_account_async"
    assert ingest_mock.call_args.kwargs["source_scope"] == "network"
    assert ingest_mock.call_args.kwargs["platforms"] == ["instagram", "twitter"]


def test_get_shared_review_queue(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "source_scope": "network",
        "review_status": "open",
        "count": 1,
        "items": [
            {
                "id": "review-1",
                "platform": "instagram",
                "source_id": "abc123",
                "review_reason": "ambiguous_match",
                "review_status": "open",
            }
        ],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.list_shared_review_queue", return_value=expected
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/shared/review-queue?source_scope=bravo&review_status=open&limit=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["items"][0]["review_reason"] == "ambiguous_match"
    assert mocked.call_args.kwargs["source_scope"] == "network"


def test_shared_social_reads_accept_internal_admin_token_without_supabase_jwt(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    token = _make_internal_admin_token("internal-secret-32-bytes-minimum")
    expected = {
        "source_scope": "bravo",
        "include_inactive": True,
        "sources": [{"id": "source-1", "platform": "instagram", "account_handle": "bravotv"}],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_shared_account_sources", return_value=expected):
        response = client.get(
            "/api/v1/admin/socials/shared/sources?source_scope=bravo&include_inactive=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["sources"][0]["account_handle"] == "bravotv"


def test_shared_social_reads_reject_invalid_internal_admin_token_when_supabase_jwt_unavailable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")

    response = client.get(
        "/api/v1/admin/socials/shared/sources?source_scope=bravo&include_inactive=true",
        headers={"Authorization": "Bearer not-a-valid-internal-token"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Allowlist admin access required"


def test_get_season_shared_status_route(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "ingest_mode": "shared_account_async",
        "matched_posts": 8,
        "review_queue_count": 2,
        "shared_scrape_status": {"status": "running", "job_count": 2, "active_jobs": 1},
        "classification_status": {"status": "complete", "job_count": 2, "active_jobs": 0},
        "materialization_status": {"status": "queued", "job_count": 1, "active_jobs": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_season_shared_status", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/shared-status?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["shared_scrape_status"]["status"] == "running"


def test_ingest_allows_zero_comments_limit(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "none")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "show_id": str(uuid4()),
        "season_number": 6,
        "source_scope": "bravo",
        "results": [],
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "max_posts_per_target": 5000,
        "max_comments_per_post": 0,
        "fetch_replies": False,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as mocked:
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 200
    assert response.json()["season_id"] == season_id
    assert mocked.call_args.kwargs["max_comments_per_post"] == 0
    assert mocked.call_args.kwargs["sync_strategy"] == "incremental"


def test_ingest_returns_run_id_and_stage_metadata(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-123",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 4,
        "summary": {"total_jobs": 4},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "sync_strategy": "full_refresh",
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-123"
    assert body["stages"] == ["posts", "comments"]
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "posts_and_comments"
    assert ingest_mock.call_args.kwargs["sync_strategy"] == "full_refresh"


def test_ingest_accepts_comments_only_mode(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-456",
        "status": "queued",
        "stages": ["comments"],
        "queued_or_started_jobs": 1,
        "summary": {"total_jobs": 1},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "sync_strategy": "incremental",
        "ingest_mode": "comments_only",
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-456"
    assert body["stages"] == ["comments"]
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "comments_only"


def test_ingest_passes_comment_targeting_options(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-789",
        "status": "queued",
        "stages": ["comments"],
        "queued_or_started_jobs": 1,
        "summary": {"total_jobs": 1},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "sync_strategy": "incremental",
        "ingest_mode": "comments_only",
        "comment_refresh_policy": "missing_only",
        "comment_anchor_source_ids": {
            "instagram": ["abc123", "def456"],
        },
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert ingest_mock.call_args.kwargs["comment_refresh_policy"] == "missing_only"
    assert ingest_mock.call_args.kwargs["comment_anchor_source_ids"] == {"instagram": ["abc123", "def456"]}


def test_ingest_passes_override_fields_and_details_refresh_mode(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-details-refresh",
        "status": "queued",
        "stages": ["posts"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "ingest_mode": "details_refresh",
        "accounts_override": ["@BravoTV", "https://instagram.com/BRAVOTV/"],
        "hashtags_override": ["RHOSLC"],
        "keywords_override": ["Salt Lake City"],
        "sound_ids": ["7540327234013301517"],
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-details-refresh"
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert body["job_count"] == 2
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "details_refresh"
    assert ingest_mock.call_args.kwargs["accounts_override"] == payload["accounts_override"]
    assert ingest_mock.call_args.kwargs["hashtags_override"] == payload["hashtags_override"]
    assert ingest_mock.call_args.kwargs["keywords_override"] == payload["keywords_override"]
    assert ingest_mock.call_args.kwargs["sound_ids"] == payload["sound_ids"]


def test_ingest_accepts_scheduler_fields(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-scheduler",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 8,
        "summary": {"total_jobs": 8},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "runner_strategy": "adaptive_dual_runner",
        "runner_count": 2,
        "window_shard_hours": 2,
        "runner_b_start_offset_hours": 48,
        "day_weight_profile": "rhoslc_default",
        "priority_mode": "episode_peak_weighted",
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-08T00:00:00Z",
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert ingest_mock.call_args.kwargs["runner_strategy"] == "adaptive_dual_runner"
    assert ingest_mock.call_args.kwargs["runner_count"] == 2
    assert ingest_mock.call_args.kwargs["window_shard_hours"] == 2
    assert ingest_mock.call_args.kwargs["runner_b_start_offset_hours"] == 48
    assert ingest_mock.call_args.kwargs["day_weight_profile"] == "rhoslc_default"
    assert ingest_mock.call_args.kwargs["priority_mode"] == "episode_peak_weighted"


def test_ingest_passes_youtube_hybrid_controls(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "run_id": "run-youtube-controls",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["youtube"],
        "youtube_source_mode": "api_only",
        "youtube_force_reindex": True,
        "youtube_force_media_refresh": True,
        "youtube_force_comment_refresh": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert ingest_mock.call_args.kwargs["youtube_source_mode"] == "api_only"
    assert ingest_mock.call_args.kwargs["youtube_force_reindex"] is True
    assert ingest_mock.call_args.kwargs["youtube_force_media_refresh"] is True
    assert ingest_mock.call_args.kwargs["youtube_force_comment_refresh"] is True


def test_ingest_resolves_week_index_to_canonical_window(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-week-2",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 4,
        "summary": {"total_jobs": 4},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "week_index": 2,
        "timezone": "America/New_York",
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
    }
    resolved_week = {
        "week_index": 2,
        "label": "Week 2",
        "start": "2026-01-07T00:00:00Z",
        "end": "2026-01-13T23:59:59Z",
        "week_type": "episode",
        "episode_number": 2,
        "timezone": "America/New_York",
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.resolve_week_window",
        return_value=resolved_week,
    ) as resolve_mock:
        with patch(
            "trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected
        ) as ingest_mock:
            with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
                with patch(
                    "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                    return_value={"healthy": True, "healthy_workers": 1},
                ):
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["scope"]["type"] == "week"
    assert body["scope"]["week"]["week_index"] == 2
    assert any("canonical season week window" in warning for warning in (body.get("warnings") or []))
    assert resolve_mock.called
    assert ingest_mock.call_args.kwargs["week_index"] == 2
    assert ingest_mock.call_args.kwargs["window_timezone"] == "America/New_York"
    assert ingest_mock.call_args.kwargs["run_scope_label"] == "Week 2"
    assert ingest_mock.call_args.kwargs["date_start"] == datetime(2026, 1, 7, 0, 0, tzinfo=UTC)
    assert ingest_mock.call_args.kwargs["date_end"] == datetime(2026, 1, 13, 23, 59, 59, tzinfo=UTC)


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("NO_INGEST_TARGETS", "No executable ingest targets"),
        ("INVALID_ACCOUNT_HANDLE", "Invalid account handle"),
    ],
)
def test_ingest_maps_structured_validation_errors_to_400(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    code: str,
    message: str,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialIngestValidationError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "none")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}

    with patch(
        "trr_backend.repositories.social_season_analytics.ingest_season",
        side_effect=SocialIngestValidationError(code, message),
    ):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == code
    assert message.lower() in str(body["detail"]["message"]).lower()


@pytest.mark.parametrize(
    ("path", "patch_target"),
    [
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_analytics",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_week_detail",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics/comments-coverage?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_comments_coverage",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics/mirror-coverage?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_mirror_coverage",
        ),
    ],
)
def test_analytics_platform_filter_value_errors_map_to_structured_400(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    patch_target: str,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    endpoint = path.format(season_id=season_id)

    with patch(patch_target, side_effect=ValueError("INVALID_PLATFORM_FILTER: unsupported")):
        response = client.get(
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == "INVALID_PLATFORM_FILTER"
    assert "unsupported" in str(body["detail"]["message"]).lower()


def test_analytics_platform_filter_query_validation_rejects_unknown_platform(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    response = client.get(
        f"/api/v1/admin/socials/seasons/{season_id}/analytics?source_scope=bravo&platforms=instagram,myspace",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == "INVALID_PLATFORM_FILTER"
    assert "myspace" in str(body["detail"]["message"]).lower()


def test_instagram_scrape_async_returns_400_when_show_or_season_missing(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    payload = {
        "username": "bravotv",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
    }

    response = client.post(
        "/api/v1/admin/socials/instagram/scrape/async",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
    )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == "BAD_REQUEST"


def test_instagram_scrape_async_creates_trackable_ingest_run(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    season_id = str(uuid4())
    payload = {
        "username": "BravoTV",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
        "show_id": show_id,
        "season_number": 6,
    }

    with patch("trr_backend.db.pg.fetch_one", return_value={"season_id": season_id}):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                with patch(
                    "trr_backend.repositories.social_season_analytics.ingest_season",
                    return_value={"run_id": "run-ig-async"},
                ) as ingest_mock:
                    response = client.post(
                        "/api/v1/admin/socials/instagram/scrape/async",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-ig-async"
    assert body["job_id"] == "run-ig-async"
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert "/ingest/jobs?run_id=run-ig-async" in body["jobs_url"]
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "posts_only"
    assert ingest_mock.call_args.kwargs["accounts_override"] == ["BravoTV"]
    assert ingest_mock.call_args.kwargs["hashtags_override"] == ["rhoslc"]


def test_instagram_scrape_async_starts_inline_when_queue_disabled(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    season_id = str(uuid4())
    payload = {
        "username": "BravoTV",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
        "show_id": show_id,
        "season_number": 6,
    }

    with patch("trr_backend.db.pg.fetch_one", return_value={"season_id": season_id}):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
            with patch(
                "trr_backend.repositories.social_season_analytics.ingest_season",
                return_value={"run_id": str(uuid4())},
            ):
                with patch("trr_backend.repositories.social_season_analytics.execute_run") as execute_mock:
                    response = client.post(
                        "/api/v1/admin/socials/instagram/scrape/async",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["run_id"], str)
    assert body["execution_mode_canonical"] == "inline"
    assert body["execution_owner"] == "local_api"
    assert execute_mock.called is True


def test_instagram_scrape_async_allows_explicit_local_dev_fallback(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    season_id = str(uuid4())
    payload = {
        "username": "BravoTV",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
        "show_id": show_id,
        "season_number": 6,
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.db.pg.fetch_one", return_value={"season_id": season_id}):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
            with patch(
                "trr_backend.repositories.social_season_analytics.ingest_season",
                return_value={"run_id": str(uuid4())},
            ):
                with patch("trr_backend.repositories.social_season_analytics.execute_run"):
                    response = client.post(
                        "/api/v1/admin/socials/instagram/scrape/async",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["run_id"], str)
    assert body["execution_mode_canonical"] == "inline_fallback"
    assert body["execution_owner"] == "local_api"


def test_scrape_instagram_route_inherits_current_auth_session_metadata(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    from trr_backend.socials.instagram import InstagramAuthSession, set_current_instagram_auth_session
    from trr_backend.socials.instagram.scraper import InstagramScraper

    auth_session = InstagramAuthSession(
        cookies={"sessionid": "resolver-session", "csrftoken": "resolver-csrf", "ds_user_id": "123"},
        source="browser_session",
        validated=True,
        validation_reason=None,
        validation_category="validated",
        stale_ok=False,
        browser_account_id="bravotv",
        session_account_id="bravotv",
        caller_context="api_route_test",
        cookie_file_path=None,
        storage_state_path=None,
        refreshed=False,
        refresh_method=None,
        repaired_from_browser_session=False,
    )
    set_current_instagram_auth_session(auth_session)

    captured: dict[str, object] = {}

    def _fake_load_social_auth_or_503(*, platform: str, surface: str, loader: object) -> dict[str, str]:
        captured["platform"] = platform
        captured["surface"] = surface
        return dict(auth_session.cookies)

    def _fake_scrape(self: InstagramScraper, config: object) -> list[object]:
        captured["browser_account_id"] = self.browser_account_id
        captured["auth_session_account_id"] = self.last_retrieval_meta.get("auth_session_account_id")
        captured["auth_cookie_source"] = self.last_retrieval_meta.get("auth_cookie_source")
        captured["config"] = config
        return [
            type(
                "_Post",
                (),
                {
                    "shortcode": "abc123",
                    "post_type": "image",
                    "date_time": "2026-01-01T12:00:00Z",
                    "caption": "caption",
                    "profile_tags": [],
                    "sponsored": False,
                    "likes": 1,
                    "comments": 2,
                    "video_views": 0,
                    "url": "https://instagram.com/p/abc123/",
                    "username": "bravotv",
                },
            )()
        ]

    monkeypatch.setattr("api.routers.socials._load_social_auth_or_503", _fake_load_social_auth_or_503)
    monkeypatch.setattr("trr_backend.socials.instagram.scraper.InstagramScraper.scrape", _fake_scrape)

    try:
        response = client.post(
            "/api/v1/admin/socials/instagram/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "username": "bravotv",
                "hashtags": [],
                "date_start": "2026-01-01T00:00:00Z",
                "date_end": "2026-01-02T00:00:00Z",
                "entity_type": "show",
                "show_id": None,
                "season_number": None,
                "person_id": None,
            },
        )
    finally:
        set_current_instagram_auth_session(None)

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert captured["platform"] == "instagram"
    assert captured["surface"] == "scrape"
    assert captured["browser_account_id"] == "bravotv"
    assert captured["auth_session_account_id"] == "bravotv"
    assert captured["auth_cookie_source"] == "browser_session"


def test_post_shared_ingest_starts_inline_when_queue_disabled(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "none")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "shared-run-123",
        "status": "pending",
        "source_scope": "bravo",
        "ingest_mode": "shared_account_async",
        "shared_scrape_status": {"status": "pending", "job_count": 4},
        "classification_status": None,
        "materialization_status": None,
        "review_queue_count": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_shared_accounts", return_value=expected):
            with patch("api.routers.socials._start_runs_in_background") as background_mock:
                response = client.post(
                    "/api/v1/admin/socials/shared/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source_scope": "bravo", "platforms": ["instagram", "twitter"]},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "started"
    assert body["execution_mode_canonical"] == "inline"
    assert body["execution_owner"] == "local_api"
    background_mock.assert_called_once()


def test_post_shared_ingest_requires_modal_when_queue_disabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_shared_accounts") as ingest_mock:
            response = client.post(
                "/api/v1/admin/socials/shared/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json={"source_scope": "bravo", "platforms": ["instagram", "twitter"]},
            )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_WORKER_REQUIRED"
    assert body["detail"]["required_platforms"] == ["instagram", "twitter"]
    assert body["detail"]["required_execution_backend"] == "modal"
    ingest_mock.assert_not_called()


def test_post_shared_ingest_accepts_internal_admin_token_without_supabase_jwt(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    token = _make_internal_admin_token("internal-secret-32-bytes-minimum")
    expected = {
        "run_id": "shared-run-internal",
        "status": "queued",
        "source_scope": "bravo",
        "ingest_mode": "shared_account_async",
        "shared_scrape_status": {"status": "queued", "job_count": 1},
        "classification_status": None,
        "materialization_status": None,
        "review_queue_count": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.ingest_shared_accounts",
            return_value=expected,
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    "/api/v1/admin/socials/shared/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source_scope": "bravo", "platforms": ["instagram"]},
                )

    assert response.status_code == 200
    assert response.json()["run_id"] == "shared-run-internal"


def test_get_ingest_jobs_supports_run_filters(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.list_jobs", return_value=[]) as mocked:
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/ingest/jobs"
                f"?run_id={run_id}&status=running&platform=instagram&limit=75&offset=25"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert body["pagination"] == {
        "limit": 75,
        "offset": 25,
        "returned": 0,
        "has_more": False,
    }
    assert mocked.call_args.kwargs["limit"] == 75
    assert mocked.call_args.kwargs["offset"] == 25
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["status"] == "running"
    assert mocked.call_args.kwargs["platform"] == "instagram"


def test_get_ingest_runs_supports_filters(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())
    workflow_id = "social-orch-demo"
    date_start = "2025-08-14T04:00:00+00:00"
    date_end = "2025-09-16T23:59:59.999999+00:00"

    runs_payload = [
        {
            "id": str(uuid4()),
            "season_id": season_id,
            "status": "completed",
            "source_scope": "bravo",
        }
    ]
    query = urlencode(
        {
            "status": "completed",
            "source_scope": "bravo",
            "run_id": run_id,
            "limit": 25,
            "client_workflow_id": workflow_id,
            "platforms": "twitter",
            "week_index": 2,
            "date_start": date_start,
            "date_end": date_end,
        }
    )

    with patch("trr_backend.repositories.social_season_analytics.list_runs", return_value=runs_payload) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs?{query}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["filters"]["status"] == "completed"
    assert body["filters"]["source_scope"] == "bravo"
    assert body["filters"]["run_id"] == run_id
    assert body["filters"]["client_workflow_id"] == workflow_id
    assert body["filters"]["platforms"] == ["twitter"]
    assert body["filters"]["week_index"] == 2
    assert body["filters"]["date_start"] == date_start
    assert body["filters"]["date_end"] == date_end
    assert body["runs"] == runs_payload
    assert mocked.call_args.kwargs["status"] == "completed"
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["client_workflow_id"] == workflow_id
    assert mocked.call_args.kwargs["platforms"] == ["twitter"]
    assert mocked.call_args.kwargs["week_index"] == 2
    assert mocked.call_args.kwargs["date_start"].isoformat() == date_start
    assert mocked.call_args.kwargs["date_end"].isoformat() == date_end
    assert mocked.call_args.kwargs["limit"] == 25


def test_orchestrate_ingest_endpoint_queues_grouped_runs(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value={"healthy": True, "healthy_workers": 2},
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.orchestrate_season_ingest",
                return_value={
                    "season_id": season_id,
                    "orchestration_id": "social-orch-demo",
                    "scope": "single_week_single_platform",
                    "requested_runs": [{"slot_key": "week:2|platform:instagram"}],
                    "created_runs": [{"run_id": "run-1", "slot_key": "week:2|platform:instagram"}],
                    "reused_runs": [],
                    "total_requested_runs": 1,
                    "created_count": 1,
                    "reused_count": 0,
                    "execution_owner": "remote_worker",
                    "execution_mode_canonical": "remote",
                    "remote_job_plane_enforced": True,
                    "weeks": [{"week_index": 2, "label": "Week 2"}],
                    "platforms": ["instagram"],
                },
            ) as mocked:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest/orchestrations",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"week_index": 2, "platforms": ["instagram"], "resume_existing": True},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "queued"
    assert body["orchestration_id"] == "social-orch-demo"
    assert body["scope"] == "single_week_single_platform"
    assert body["execution_owner"] == "remote_worker"
    assert mocked.call_args.kwargs["week_index"] == 2
    assert mocked.call_args.kwargs["platforms"] == ["instagram"]
    assert mocked.call_args.kwargs["resume_existing"] is True


def test_orchestrate_ingest_endpoint_starts_inline_when_queue_disabled(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch(
            "trr_backend.repositories.social_season_analytics.orchestrate_season_ingest",
            return_value={
                "season_id": season_id,
                "orchestration_id": "social-orch-demo",
                "scope": "single_week_single_platform",
                "requested_runs": [{"slot_key": "week:2|platform:instagram"}],
                "created_runs": [{"run_id": "run-1", "slot_key": "week:2|platform:instagram"}],
                "reused_runs": [],
                "total_requested_runs": 1,
                "created_count": 1,
                "reused_count": 0,
                "execution_owner": "local_api",
                "execution_mode_canonical": "local",
                "remote_job_plane_enforced": False,
                "weeks": [{"week_index": 2, "label": "Week 2"}],
                "platforms": ["instagram"],
            },
        ) as mocked:
            with patch("api.routers.socials._start_runs_in_background") as background_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest/orchestrations",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"week_index": 2, "platforms": ["instagram"], "resume_existing": True},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "started"
    assert body["message"] == "Season social orchestration started inline."
    assert mocked.call_args.kwargs["week_index"] == 2
    background_mock.assert_called_once()


def test_get_ingest_runs_rejects_invalid_season_id(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    response = client.get(
        "/api/v1/admin/socials/seasons/not-a-uuid/ingest/runs",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 422


def test_get_ingest_runs_requires_admin_auth(client: TestClient) -> None:
    season_id = str(uuid4())
    response = client.get(f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs")
    assert response.status_code in {401, 403}


def test_get_week_detail_endpoint_returns_youtube_comment_totals(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "youtube": {
                "posts": [
                    {
                        "source_id": "vid123",
                        "comments_count": 420,
                        "total_comments_available": 420,
                    }
                ],
                "totals": {"posts": 1, "total_comments": 420, "total_engagement": 1000},
            }
        },
        "totals": {"posts": 1, "total_comments": 420, "total_engagement": 1000},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["totals"]["total_comments"] == 420
    assert body["platforms"]["youtube"]["posts"][0]["comments_count"] == 420


def test_get_week_detail_endpoint_includes_additive_week_metadata(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "BYE WEEK (Jan 15-Jan 22)",
            "start": "2026-01-16T01:00:00Z",
            "end": "2026-01-23T00:59:59Z",
            "week_type": "bye",
            "episode_number": None,
        },
        "platforms": {
            "youtube": {
                "posts": [],
                "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["week"]["label"] == "BYE WEEK (Jan 15-Jan 22)"
    assert body["week"]["week_type"] == "bye"
    assert body["week"]["episode_number"] is None


def test_get_week_detail_endpoint_includes_additive_diagnostics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "youtube": {
                "posts": [],
                "totals": {
                    "posts": 1,
                    "total_comments": 420,
                    "total_engagement": 1000,
                    "expected_comments_total": 500,
                    "saved_comments_total": 420,
                    "comments_saved_pct": 84.0,
                },
            }
        },
        "totals": {
            "posts": 1,
            "total_comments": 420,
            "total_engagement": 1000,
            "expected_comments_total": 500,
            "saved_comments_total": 420,
            "comments_saved_pct": 84.0,
        },
        "diagnostics": {
            "run_id": "run-abc",
            "generated_at": "2026-02-24T00:00:00Z",
            "source_scope": "bravo",
        },
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["totals"]["expected_comments_total"] == 500
    assert body["totals"]["saved_comments_total"] == 420
    assert body["totals"]["comments_saved_pct"] == 84.0
    assert body["diagnostics"]["run_id"] == "run-abc"


def test_get_week_detail_endpoint_passes_threads_topic_field(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "threads": {
                "posts": [
                    {
                        "source_id": "th-1",
                        "author": "bravotv",
                        "text": "Post body",
                        "topic": "bravotv > rhoslc",
                        "posted_at": "2026-01-01T00:00:00Z",
                        "replies_count": 3,
                        "likes": 10,
                        "reposts": 2,
                        "quotes": 1,
                        "views": 20,
                        "engagement": 36,
                        "total_comments_available": 3,
                        "comments": [],
                    }
                ],
                "totals": {"posts": 1, "total_comments": 3, "total_engagement": 36},
                "total_posts": 1,
            }
        },
        "totals": {"posts": 1, "total_comments": 3, "total_engagement": 36},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["platforms"]["threads"]["posts"][0]["topic"] == "bravotv > rhoslc"


def test_get_week_detail_endpoint_defaults_to_25_comments_and_paginated_page(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    posts = [
        {
            "source_id": f"p{i}",
            "author": "bravotv",
            "text": f"Post {i}",
            "url": "https://instagram.com/p/abc",
            "posted_at": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=i)).isoformat(),
            "engagement": 10 + i,
            "total_comments_available": 0,
            "comments": [],
            "likes": 0,
            "comments_count": 0,
            "views": 100,
            "thumbnail_url": None,
        }
        for i in range(25)
    ]
    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "Week 3",
            "start": "2025-09-30T00:00:00Z",
            "end": "2025-10-07T00:00:00Z",
        },
        "platforms": {
            "instagram": {
                "posts": posts[:20],
                "total_posts": 25,
                "totals": {"posts": 20, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 25, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["pagination"] == {
        "limit": 20,
        "offset": 0,
        "returned": 20,
        "total": 25,
        "has_more": True,
    }
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["max_comments_per_post"] == 0
    assert mocked.call_args.kwargs["post_limit"] == 20
    assert mocked.call_args.kwargs["post_offset"] == 0
    assert mocked.call_args.kwargs["include_status"] is True


def test_get_week_detail_endpoint_forwards_include_status_false(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "Week 3",
            "start": "2025-09-30T00:00:00Z",
            "end": "2025-10-07T00:00:00Z",
        },
        "platforms": {
            "instagram": {
                "posts": [],
                "total_posts": 0,
                "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo&include_status=false",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["include_status"] is False


def test_get_week_summary_endpoint_forwards_defaults_and_shape(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload: dict[str, Any] = {
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "season": {
            "season_id": season_id,
            "show_id": str(uuid4()),
            "show_name": "Test",
            "show_slug": "test",
            "season_number": 6,
        },
        "source_scope": "bravo",
        "platforms": {"instagram": {"total_posts": 12, "totals": {"posts": 12}}},
        "totals": {"posts": 12},
        "meta": {"performance": {"total_duration_ms": 10, "by_platform": {"instagram": 2}}},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_summary_cache()
    with patch(
        "trr_backend.repositories.social_season_analytics.get_week_detail_summary_fast", return_value=payload
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3/summary?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["platforms"]["instagram"]["totals"]["posts"] == 12
    assert mocked.call_args.kwargs["source_scope"] == "bravo"


def test_get_week_summary_endpoint_include_full_uses_legacy_path(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload: dict[str, Any] = {
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "season": {
            "season_id": season_id,
            "show_id": str(uuid4()),
            "show_name": "Test",
            "show_slug": "test",
            "season_number": 6,
        },
        "source_scope": "bravo",
        "platforms": {"instagram": {"total_posts": 12, "totals": {"posts": 12}}},
        "totals": {"posts": 12},
        "meta": {"performance": {"total_duration_ms": 10, "by_platform": {"instagram": 2}}},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_summary_cache()
    with patch(
        "trr_backend.repositories.social_season_analytics.get_week_detail_summary", return_value=payload
    ) as full_mock:
        with patch("trr_backend.repositories.social_season_analytics.get_week_detail_summary_fast") as fast_mock:
            response = client.get(
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3/summary?source_scope=bravo&include=full",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert full_mock.called
    assert not fast_mock.called


def test_get_schedule_preview_endpoint_returns_payload(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    preview_payload = {
        "season_id": season_id,
        "scheduler": {
            "strategy": "adaptive_dual_runner",
            "runner_count": 2,
            "window_shard_hours": 2,
            "runner_b_start_offset_hours": 48,
            "day_weight_profile": "rhoslc_default",
            "priority_mode": "episode_peak_weighted",
        },
        "lanes": {"A": [{"shard_index": 0}], "B": [{"shard_index": 1}]},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.preview_ingest_schedule", return_value=preview_payload
    ) as mocked:
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/ingest/schedule-preview"
                "?source_scope=bravo&platforms=instagram&runner_strategy=adaptive_dual_runner&runner_count=2"
                "&window_shard_hours=2&day_weight_profile=rhoslc_default&priority_mode=episode_peak_weighted"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["scheduler"]["strategy"] == "adaptive_dual_runner"
    assert body["scheduler"]["runner_b_start_offset_hours"] == 48
    assert mocked.call_args.kwargs["runner_count"] == 2


def test_get_schedule_preview_endpoint_resolves_week_scope(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    resolved_week = {
        "week_index": 3,
        "label": "Week 3",
        "start": "2026-01-14T00:00:00Z",
        "end": "2026-01-20T23:59:59Z",
        "week_type": "episode",
        "episode_number": 3,
        "timezone": "America/New_York",
    }
    preview_payload = {
        "season_id": season_id,
        "scope": {"type": "week", "label": "Week 3", "week_index": 3, "timezone": "America/New_York"},
        "scheduler": {"strategy": "single_runner"},
        "lanes": {"A": [{"shard_index": 0}], "B": []},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.resolve_week_window",
        return_value=resolved_week,
    ) as resolve_mock:
        with patch(
            "trr_backend.repositories.social_season_analytics.preview_ingest_schedule", return_value=preview_payload
        ) as mocked:
            response = client.get(
                (
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest/schedule-preview"
                    "?source_scope=bravo&platforms=instagram&week_index=3&timezone=America/New_York"
                ),
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.json()["scope"]["type"] == "week"
    assert resolve_mock.called
    assert mocked.call_args.kwargs["week_index"] == 3
    assert mocked.call_args.kwargs["window_timezone"] == "America/New_York"
    assert mocked.call_args.kwargs["run_scope_label"] == "Week 3"
    assert mocked.call_args.kwargs["date_start"] == datetime(2026, 1, 14, 0, 0, tzinfo=UTC)
    assert mocked.call_args.kwargs["date_end"] == datetime(2026, 1, 20, 23, 59, 59, tzinfo=UTC)


def test_get_week_detail_endpoint_supports_page_offset_and_de_duplicated_newest_first_order(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    posts = [
        {
            "source_id": f"p{i}",
            "author": "bravotv",
            "text": f"Post {i}",
            "url": "https://instagram.com/p/abc",
            "posted_at": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=i)).isoformat(),
            "engagement": 10 + i,
            "total_comments_available": 0,
            "comments": [],
            "likes": 0,
            "comments_count": 0,
            "views": 100,
            "thumbnail_url": None,
        }
        for i in range(40)
    ]
    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "Week 3",
            "start": "2025-09-30T00:00:00Z",
            "end": "2025-10-07T00:00:00Z",
        },
        "platforms": {
            "instagram": {
                "posts": posts,
                "total_posts": 40,
                "totals": {"posts": 40, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 40, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response_page1 = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )
        response_page2 = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo&post_limit=20&post_offset=20",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response_page1.status_code == 200
    assert response_page2.status_code == 200

    page1 = response_page1.json()
    page2 = response_page2.json()
    assert page1["pagination"]["returned"] == 20
    assert page1["pagination"]["offset"] == 0
    assert page1["pagination"]["has_more"] is True
    assert [post["source_id"] for post in page1["platforms"]["instagram"]["posts"]] == [
        f"p{i}" for i in range(39, 19, -1)
    ]
    assert page1["pagination"]["total"] == 40

    assert page2["pagination"]["returned"] == 20
    assert page2["pagination"]["offset"] == 20
    assert page2["pagination"]["has_more"] is False
    assert page2["pagination"]["total"] == 40
    assert [post["source_id"] for post in page2["platforms"]["instagram"]["posts"]] == [
        f"p{i}" for i in range(19, -1, -1)
    ]
    assert len({post["source_id"] for post in page1["platforms"]["instagram"]["posts"]}) == 20
    assert len({post["source_id"] for post in page2["platforms"]["instagram"]["posts"]}) == 20
    assert mocked.call_count == 1


def test_get_week_detail_endpoint_forwards_sort_params_to_repository(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "instagram": {
                "posts": [],
                "total_posts": 0,
                "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=asc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["sort_field"] == "likes"
    assert mocked.call_args.kwargs["sort_dir"] == "asc"


def test_get_week_detail_endpoint_sorts_page_by_requested_metric(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "instagram": {
                "posts": [
                    {"source_id": "ig-1", "posted_at": "2026-01-01T00:00:00Z", "likes": 10},
                    {"source_id": "ig-2", "posted_at": "2026-01-02T00:00:00Z", "likes": 90},
                ],
                "total_posts": 2,
                "totals": {"posts": 2, "total_comments": 0, "total_engagement": 0},
            },
            "tiktok": {
                "posts": [
                    {"source_id": "tt-1", "posted_at": "2026-01-03T00:00:00Z", "likes": 50},
                    {"source_id": "tt-2", "posted_at": "2026-01-04T00:00:00Z", "likes": 70},
                ],
                "total_posts": 2,
                "totals": {"posts": 2, "total_comments": 0, "total_engagement": 0},
            },
        },
        "totals": {"posts": 4, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=desc&post_limit=2"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    flat_posts = [post for platform_payload in body["platforms"].values() for post in platform_payload["posts"]]
    ordered = sorted(flat_posts, key=lambda post: int(post.get("sort_rank", 0)))
    assert [post["source_id"] for post in ordered] == ["ig-2", "tt-2"]
    assert body["pagination"]["returned"] == 2
    assert body["pagination"]["total"] == 4


def test_get_week_detail_endpoint_uses_cached_payload_when_repeating_same_page_request(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "Week 3",
            "start": "2025-09-30T00:00:00Z",
            "end": "2025-10-07T00:00:00Z",
        },
        "platforms": {
            "instagram": {
                "posts": [
                    {"source_id": "cached-1", "posted_at": "2026-10-01T00:00:00Z"},
                    {"source_id": "cached-2", "posted_at": "2026-09-30T00:00:00Z"},
                    {"source_id": "cached-3", "posted_at": "2026-09-29T00:00:00Z"},
                    {"source_id": "cached-4", "posted_at": "2026-09-28T00:00:00Z"},
                    {"source_id": "cached-5", "posted_at": "2026-09-27T00:00:00Z"},
                ],
                "total_posts": 5,
                "totals": {"posts": 5, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 5, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response_first = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )
        response_second = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response_first.status_code == 200
    assert response_second.status_code == 200
    assert mocked.call_count == 1


def test_get_week_detail_endpoint_cache_key_includes_sort_signature(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "instagram": {
                "posts": [{"source_id": "cached-1", "posted_at": "2026-10-01T00:00:00Z", "likes": 1, "views": 100}],
                "total_posts": 1,
                "totals": {"posts": 1, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 1, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response_a = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=desc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        response_b = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=desc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        response_c = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=views&sort_dir=desc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response_a.status_code == 200
    assert response_b.status_code == 200
    assert response_c.status_code == 200
    assert mocked.call_count == 2


def test_get_analytics_endpoint_includes_additive_week_metadata(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "window": {"week": None},
        "summary": {},
        "weekly": [
            {
                "week_index": 2,
                "label": "BYE WEEK (Jan 15-Jan 22)",
                "week_type": "bye",
                "episode_number": None,
                "start": "2026-01-16T01:00:00Z",
                "end": "2026-01-23T00:59:59Z",
                "post_volume": 0,
                "comment_volume": 0,
                "engagement": 0,
                "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            }
        ],
        "weekly_platform_posts": [
            {
                "week_index": 2,
                "label": "BYE WEEK (Jan 15-Jan 22)",
                "week_type": "bye",
                "episode_number": None,
                "start": "2026-01-16T01:00:00Z",
                "end": "2026-01-23T00:59:59Z",
                "posts": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                "comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                "reported_comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                "total_posts": 0,
                "total_comments": 0,
                "total_reported_comments": 0,
                "comments_saved_pct": None,
            }
        ],
        "weekly_platform_engagement": [],
        "weekly_daily_activity": [
            {
                "week_index": 2,
                "label": "BYE WEEK (Jan 15-Jan 22)",
                "week_type": "bye",
                "episode_number": None,
                "start": "2026-01-16T01:00:00Z",
                "end": "2026-01-23T00:59:59Z",
                "days": [
                    {
                        "day_index": 0,
                        "date_local": "2026-01-15",
                        "posts": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                        "comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                        "reported_comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                        "total_posts": 0,
                        "total_comments": 0,
                        "total_reported_comments": 0,
                    }
                ],
            }
        ],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["weekly"][0]["label"] == "BYE WEEK (Jan 15-Jan 22)"
    assert body["weekly"][0]["week_type"] == "bye"
    assert body["weekly"][0]["episode_number"] is None
    assert body["weekly_daily_activity"][0]["days"][0]["reported_comments"]["instagram"] == 0
    assert body["weekly_daily_activity"][0]["days"][0]["total_reported_comments"] == 0


def test_get_post_comments_endpoint_returns_youtube_effective_stats(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "platform": "youtube",
        "source_id": "vid123",
        "stats": {"views": 1000, "likes": 100, "comments_count": 420, "engagement": 1520},
        "total_comments_in_db": 420,
        "comments": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_post_comments", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/youtube/vid123",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["stats"]["comments_count"] == 420
    assert body["total_comments_in_db"] == 420


def test_get_post_comments_endpoint_returns_twitter_quotes_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "platform": "twitter",
        "source_id": "tweet-1",
        "stats": {"likes": 10, "retweets": 2, "replies_count": 1, "quotes": 5, "views": 100, "engagement": 118},
        "total_comments_in_db": 1,
        "total_quotes_in_db": 2,
        "comments": [],
        "quotes": [
            {
                "comment_id": "quote-1",
                "author": "viewer",
                "text": "quote body",
                "likes": 4,
                "is_reply": False,
            }
        ],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_post_comments", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/twitter/tweet-1",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["total_quotes_in_db"] == 2
    assert body["quotes"][0]["comment_id"] == "quote-1"


def test_get_post_comments_endpoint_passes_threads_topic_field(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "platform": "threads",
        "source_id": "th-1",
        "author": "bravotv",
        "text": "Post body",
        "topic": "bravotv > rhoslc",
        "stats": {"likes": 10, "replies_count": 3, "reposts": 2, "quotes": 1, "views": 20, "engagement": 36},
        "total_comments_in_db": 3,
        "comments": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_post_comments", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/threads/th-1",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["topic"] == "bravotv > rhoslc"


def test_refresh_post_comments_endpoint_returns_latest_post_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    refreshed_post = {
        "platform": "tiktok",
        "source_id": "72899887766",
        "author": "bravotv",
        "text": "Example post",
        "url": "https://www.tiktok.com/@bravotv/video/72899887766",
        "posted_at": "2026-02-17T10:00:00+00:00",
        "stats": {"comments_count": 604, "likes": 59500, "engagement": 564500},
        "total_comments_in_db": 604,
        "comments": [],
    }
    refresh_summary = {
        "platform": "tiktok",
        "source_id": "72899887766",
        "comments_fetched": 604,
        "comments_upserted": 604,
        "total_comments_in_db": 604,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.refresh_post",
        return_value=refresh_summary,
    ) as refresh_mock:
        with patch(
            "trr_backend.repositories.social_season_analytics.get_post_comments",
            return_value=refreshed_post,
        ) as get_mock:
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/tiktok/72899887766/refresh",
                headers={"Authorization": f"Bearer {token}"},
                json={"max_comments_per_post": 1000, "fetch_replies": True},
            )

    assert response.status_code == 200
    body = response.json()
    assert body["source_id"] == "72899887766"
    assert body["refresh"]["comments_upserted"] == 604
    assert refresh_mock.call_args.kwargs["platform"] == "tiktok"
    assert refresh_mock.call_args.kwargs["source_id"] == "72899887766"
    assert refresh_mock.call_args.kwargs["max_comments_per_post"] == 1000
    assert get_mock.call_args.kwargs["platform"] == "tiktok"


def test_requeue_instagram_mirror_jobs_endpoint(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "source_scope": "bravo",
        "failed_only": True,
        "scanned": 20,
        "queued_jobs": 8,
        "skipped": 10,
        "failed": 2,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.requeue_instagram_media_mirror_jobs",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/instagram/mirror/requeue"
            "?source_scope=bravo&limit=500&failed_only=true"
            "&date_start=2026-02-01T00:00:00Z&date_end=2026-02-08T00:00:00Z",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["queued_jobs"] == 8
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["limit"] == 500
    assert mocked.call_args.kwargs["failed_only"] is True
    assert mocked.call_args.kwargs["date_start"] is not None
    assert mocked.call_args.kwargs["date_end"] is not None


def test_requeue_platform_mirror_jobs_endpoint(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "platform": "tiktok",
        "source_scope": "bravo",
        "failed_only": False,
        "scanned": 15,
        "queued_jobs": 4,
        "skipped": 11,
        "failed": 0,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.requeue_media_mirror_jobs",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/mirror/requeue"
            "?source_scope=bravo&limit=250"
            "&date_start=2026-02-01T00:00:00Z&date_end=2026-02-08T00:00:00Z",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["platform"] == "tiktok"
    assert mocked.call_args.kwargs["platform"] == "tiktok"
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["limit"] == 250
    assert mocked.call_args.kwargs["failed_only"] is False
    assert mocked.call_args.kwargs["date_start"] is not None
    assert mocked.call_args.kwargs["date_end"] is not None


def test_cancel_ingest_run_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())
    expected = {"run_id": run_id, "status": "cancelled", "cancelled_jobs": 2}

    with patch("trr_backend.repositories.social_season_analytics.cancel_run", return_value=expected):
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs/{run_id}/cancel",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert response.json()["run_id"] == run_id


def test_ingest_returns_400_when_queue_schema_missing(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "none")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch(
            "trr_backend.repositories.social_season_analytics.ingest_season",
            side_effect=ValueError("Social ingest queue schema is not migrated"),
        ):
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 400
    assert "not migrated" in response.json()["detail"]


def test_ingest_returns_503_when_queue_enabled_and_worker_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "none")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_WORKER_UNAVAILABLE"
    assert body["detail"]["worker_health"]["healthy"] is False
    ingest_mock.assert_not_called()


def test_get_social_account_profile_comments_returns_503_when_database_unavailable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_comments",
        side_effect=DatabaseServiceUnavailableError(
            "Database pool initialization failed: no database URL candidates available",
            reason="database_configuration",
        ),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments?page=1&page_size=25",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert body["detail"]["reason"] == "database_configuration"


def test_get_social_account_profile_comments_forwards_post_source_id(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_comments",
        return_value={"items": [], "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1}},
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments?page=1&page_size=25&post_source_id=DVfQnTcjsCA",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["post_source_id"] == "DVfQnTcjsCA"


def test_get_social_account_profile_comments_forwards_search_and_sort(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_comments",
        return_value={"items": [], "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1}},
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments"
            "?page=1&page_size=25&post_source_id=DWAGS9iFCFy&search=carol&sort_by=likes&sort_dir=asc",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["post_source_id"] == "DWAGS9iFCFy"
    assert mocked.call_args.kwargs["search"] == "carol"
    assert mocked.call_args.kwargs["sort_by"] == "likes"
    assert mocked.call_args.kwargs["sort_dir"] == "asc"


def test_post_social_account_comments_scrape_returns_modal_error_when_modal_dispatch_unavailable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": True,
                "used_inline_fallback": False,
                "requires_modal_executor": True,
            },
        ),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            side_effect=HTTPException(
                status_code=503,
                detail={
                    "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED",
                    "message": "Modal social dispatch is required for Instagram comments scraping.",
                    "required_execution_backend": "modal",
                    "worker_health": {"healthy": False, "reason": "modal_dispatch_unavailable"},
                },
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={"mode": "profile", "source_scope": "bravo", "refresh_policy": "stale_or_missing"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_MODAL_EXECUTOR_REQUIRED"
    assert body["detail"]["required_execution_backend"] == "modal"


def test_post_social_account_comments_scrape_accepts_all_saved_posts_profile_sync(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": False,
                "used_inline_fallback": False,
                "requires_modal_executor": False,
            },
        ),
        patch("api.routers.socials._start_runs_in_background", return_value=None),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            return_value={"run_id": "comments-run-1", "status": "pending"},
        ) as scrape_mock,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={"mode": "profile", "source_scope": "bravo", "refresh_policy": "all_saved_posts"},
        )

    assert response.status_code == 200
    assert response.json()["run_id"] == "comments-run-1"
    scrape_mock.assert_called_once()
    assert scrape_mock.call_args.kwargs["max_posts"] is None
    assert scrape_mock.call_args.kwargs["max_comments_per_post"] is None
    assert scrape_mock.call_args.kwargs["refresh_policy"] == "all_saved_posts"
    assert scrape_mock.call_args.kwargs["comments_load_strategy"] == "public_relay"
    assert scrape_mock.call_args.kwargs["dispatch_immediately"] is True


def test_post_social_account_comments_scrape_returns_auth_repair_metadata(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": False,
                "used_inline_fallback": False,
                "requires_modal_executor": False,
            },
        ),
        patch("api.routers.socials._start_runs_in_background", return_value=None),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            return_value={
                "run_id": "comments-run-1",
                "status": "pending",
                "auth_repair_attempted": True,
                "auth_repair_status": "succeeded",
                "auth_repair_reason": None,
                "comments_auth_probe": {"status": "valid", "shortcode": "SHORT1"},
            },
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={"mode": "profile", "source_scope": "bravo", "refresh_policy": "stale_or_missing"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["auth_repair_attempted"] is True
    assert body["auth_repair_status"] == "succeeded"
    assert body["comments_auth_probe"]["status"] == "valid"


def test_post_social_account_comments_scrape_returns_503_when_auth_repair_fails(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialIngestValidationError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": False,
                "used_inline_fallback": False,
                "requires_modal_executor": False,
            },
        ),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            side_effect=SocialIngestValidationError(
                "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED",
                "Instagram comments manual auth sync failed before launch: operator cancelled.",
            ),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={"mode": "profile", "source_scope": "bravo", "refresh_policy": "stale_or_missing"},
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED"
    assert "operator cancelled" in body["detail"]["message"]


def test_post_social_account_comments_scrape_forwards_incomplete_target_filter(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": False,
                "used_inline_fallback": False,
                "requires_modal_executor": False,
            },
        ),
        patch("api.routers.socials._start_runs_in_background", return_value=None),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            return_value={
                "run_id": "comments-run-1",
                "status": "pending",
                "target_filter": "incomplete",
                "incomplete_fill": True,
            },
        ) as scrape_mock,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "mode": "profile",
                "source_scope": "bravo",
                "refresh_policy": "stale_or_missing",
                "target_filter": "incomplete",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "comments-run-1"
    assert body["target_filter"] == "incomplete"
    assert body["incomplete_fill"] is True
    scrape_mock.assert_called_once()
    assert scrape_mock.call_args.kwargs["target_filter"] == "incomplete"


def test_post_social_account_comments_scrape_forwards_comments_load_strategy(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": False,
                "used_inline_fallback": False,
                "requires_modal_executor": False,
            },
        ),
        patch("api.routers.socials._start_runs_in_background", return_value=None),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            return_value={
                "run_id": "comments-run-1",
                "status": "pending",
                "comments_load_strategy": "single_session_load_all",
                "comments_session_scope": "profile_single_worker",
            },
        ) as scrape_mock,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "mode": "profile",
                "source_scope": "bravo",
                "refresh_policy": "stale_or_missing",
                "comments_load_strategy": "single_session_load_all",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "comments-run-1"
    assert body["comments_load_strategy"] == "single_session_load_all"
    assert body["comments_session_scope"] == "profile_single_worker"
    scrape_mock.assert_called_once()
    assert scrape_mock.call_args.kwargs["comments_load_strategy"] == "single_session_load_all"


def test_post_social_account_comments_scrape_dry_run_returns_preview_without_launch(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.socials.pipelines.comments.instagram.preview_social_account_comments_scrape",
            return_value={
                "dry_run": True,
                "platform": "instagram",
                "account_handle": "bravotv",
                "mode": "profile",
                "target_source_ids_count": 431,
                "comments_shard_count": 8,
                "recommended_comments_shard_count": 8,
                "sample_target_source_ids": ["C123"],
                "refresh_policy": "all_saved_posts",
                "target_priority": "gap_first",
                "timing": {
                    "target_preview_ms": 12.5,
                    "target_count_ms": 12.5,
                    "sample_target_source_ids_ms": 12.5,
                    "cache_lookup_ms": 0.1,
                    "total_ms": 12.6,
                },
                "preview_cache": {
                    "enabled": True,
                    "hit": False,
                    "age_seconds": None,
                    "ttl_seconds": 60,
                },
                "cache": {
                    "enabled": True,
                    "hit": False,
                    "age_seconds": None,
                    "ttl_seconds": 60,
                },
                "debug": {
                    "target_plan_strategy": "bounded_profile_preview",
                    "full_target_list_built": False,
                },
            },
        ) as preview_mock,
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            side_effect=AssertionError("dry run should not launch a scrape"),
        ),
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            side_effect=AssertionError("dry run should not resolve queue execution"),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape?dry_run=true",
            headers={"Authorization": f"Bearer {token}"},
            json={"mode": "profile", "source_scope": "bravo", "refresh_policy": "all_saved_posts"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["dry_run"] is True
    assert body["target_source_ids_count"] == 431
    assert body["comments_shard_count"] == 8
    assert body["sample_target_source_ids"] == ["C123"]
    assert body["timing"]["target_preview_ms"] == 12.5
    assert body["preview_cache"]["hit"] is False
    assert body["cache"] == body["preview_cache"]
    assert body["debug"]["target_plan_strategy"] == "bounded_profile_preview"
    preview_mock.assert_called_once()
    assert preview_mock.call_args.kwargs["refresh_policy"] == "all_saved_posts"
    assert preview_mock.call_args.kwargs["comments_load_strategy"] == "public_relay"


def test_post_social_account_comments_scrape_dry_run_forwards_incomplete_target_filter(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.socials.pipelines.comments.instagram.preview_social_account_comments_scrape",
            return_value={
                "dry_run": True,
                "platform": "instagram",
                "account_handle": "bravotv",
                "mode": "profile",
                "target_filter": "incomplete",
                "incomplete_fill": True,
                "target_source_ids_count": 3,
                "comments_shard_count": 1,
                "recommended_comments_shard_count": 1,
                "sample_target_source_ids": ["GAP1"],
                "refresh_policy": "stale_or_missing",
                "target_priority": "missing_first_recent",
                "timing": {
                    "target_preview_ms": 2.5,
                    "target_count_ms": 2.5,
                    "sample_target_source_ids_ms": 2.5,
                    "cache_lookup_ms": 0.0,
                    "total_ms": 2.5,
                },
                "preview_cache": {"enabled": False, "hit": False, "age_seconds": None, "ttl_seconds": 0},
                "cache": {"enabled": False, "hit": False, "age_seconds": None, "ttl_seconds": 0},
                "debug": {"target_plan_strategy": "bounded_profile_preview"},
            },
        ) as preview_mock,
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            side_effect=AssertionError("dry run should not launch a scrape"),
        ),
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            side_effect=AssertionError("dry run should not resolve queue execution"),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape?dry_run=true",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "mode": "profile",
                "source_scope": "bravo",
                "refresh_policy": "stale_or_missing",
                "target_filter": "incomplete",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["target_filter"] == "incomplete"
    assert body["incomplete_fill"] is True
    assert body["target_source_ids_count"] == 3
    preview_mock.assert_called_once()
    assert preview_mock.call_args.kwargs["target_filter"] == "incomplete"


def test_post_social_account_comments_scrape_dry_run_forwards_comments_load_strategy(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "trr_backend.socials.pipelines.comments.instagram.preview_social_account_comments_scrape",
            return_value={
                "dry_run": True,
                "platform": "instagram",
                "account_handle": "bravotv",
                "mode": "profile",
                "target_source_ids_count": 431,
                "comments_load_strategy": "single_session_load_all",
                "comments_session_scope": "profile_single_worker",
                "comments_internal_pagination": "cursor_preserved",
                "comments_sharding_forced_single_session": True,
                "comments_shard_count": 1,
                "effective_comments_shard_count": 1,
                "recommended_comments_shard_count": 8,
                "single_session_enabled": True,
                "strategy_warnings": [
                    {
                        "code": "INSTAGRAM_COMMENTS_SINGLE_SESSION_FORCES_ONE_SHARD",
                        "message": "single_session_load_all runs profile comment scrapes in one comments shard.",
                    }
                ],
                "sample_target_source_ids": ["C123"],
                "refresh_policy": "all_saved_posts",
                "target_priority": "gap_first",
                "timing": {"target_preview_ms": 12.5, "total_ms": 12.6},
                "preview_cache": {"enabled": True, "hit": False, "age_seconds": None, "ttl_seconds": 60},
                "cache": {"enabled": True, "hit": False, "age_seconds": None, "ttl_seconds": 60},
                "debug": {"target_plan_strategy": "bounded_profile_preview"},
            },
        ) as preview_mock,
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            side_effect=AssertionError("dry run should not launch a scrape"),
        ),
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            side_effect=AssertionError("dry run should not resolve queue execution"),
        ),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape?dry_run=true",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "mode": "profile",
                "source_scope": "bravo",
                "refresh_policy": "all_saved_posts",
                "comments_load_strategy": "single_session_load_all",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["comments_load_strategy"] == "single_session_load_all"
    assert body["comments_session_scope"] == "profile_single_worker"
    assert body["comments_shard_count"] == 1
    assert body["effective_comments_shard_count"] == 1
    assert body["recommended_comments_shard_count"] == 8
    assert body["comments_sharding_forced_single_session"] is True
    assert body["strategy_warnings"][0]["code"] == "INSTAGRAM_COMMENTS_SINGLE_SESSION_FORCES_ONE_SHARD"
    preview_mock.assert_called_once()
    assert preview_mock.call_args.kwargs["comments_load_strategy"] == "single_session_load_all"


def test_post_social_account_comments_scrape_queue_dispatches_in_background(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": True,
                "used_inline_fallback": False,
                "requires_modal_executor": True,
            },
        ),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            return_value={"run_id": "comments-run-queued-1", "status": "queued"},
        ) as scrape_mock,
        patch(
            "trr_backend.repositories.social_season_analytics._dispatch_due_social_jobs_in_background"
        ) as dispatch_mock,
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={"mode": "profile", "source_scope": "bravo", "refresh_policy": "all_saved_posts"},
        )

    assert response.status_code == 200
    assert response.json()["run_id"] == "comments-run-queued-1"
    scrape_mock.assert_called_once()
    assert scrape_mock.call_args.kwargs["dispatch_immediately"] is False
    dispatch_mock.assert_called_once_with(run_id="comments-run-queued-1")


def test_post_social_account_comments_scrape_background_dispatch_exception_keeps_response_200(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    class SynchronousThread:
        def __init__(self, *, target: Any, name: str | None = None, daemon: bool | None = None) -> None:
            self._target = target
            self.name = name
            self.daemon = daemon

        def start(self) -> None:
            self._target()

    with (
        patch(
            "api.routers.socials._resolve_social_account_comments_route_execution",
            return_value={
                "queue_enabled": True,
                "used_inline_fallback": False,
                "requires_modal_executor": True,
            },
        ),
        patch(
            "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
            return_value={"run_id": "comments-run-queued-2", "status": "queued"},
        ) as scrape_mock,
        patch(
            "trr_backend.repositories.social_season_analytics.dispatch_due_social_jobs",
            side_effect=RuntimeError("dispatch failed"),
        ) as dispatch_mock,
        patch("trr_backend.repositories.social_season_analytics.Thread", SynchronousThread),
    ):
        response = client.post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            headers={"Authorization": f"Bearer {token}"},
            json={"mode": "profile", "source_scope": "bravo", "refresh_policy": "all_saved_posts"},
        )

    assert response.status_code == 200
    assert response.json()["run_id"] == "comments-run-queued-2"
    assert scrape_mock.call_args.kwargs["dispatch_immediately"] is False
    dispatch_mock.assert_called_once_with(run_id="comments-run-queued-2")


def test_ingest_returns_503_when_remote_job_plane_enforced_and_queue_disabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["twitter"], "allow_inline_dev_fallback": True}

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_JOB_PLANE_ENFORCED"
    ingest_mock.assert_not_called()


def test_ingest_returns_503_when_modal_required_platforms_and_queue_disabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["twitter"]}

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_WORKER_REQUIRED"
    assert body["detail"]["required_platforms"] == ["twitter"]
    assert body["detail"]["required_execution_backend"] == "modal"
    ingest_mock.assert_not_called()


def test_ingest_returns_503_when_remote_job_plane_enforced_and_worker_missing_even_with_inline_fallback(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["youtube"],
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_JOB_PLANE_ENFORCED"
    assert body["detail"]["worker_health"]["healthy"] is False
    ingest_mock.assert_not_called()


def test_ingest_falls_back_inline_in_dev_when_worker_missing_and_flag_enabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "instagram,tiktok")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["youtube"],
        "allow_inline_dev_fallback": True,
    }
    expected = {
        "season_id": season_id,
        "run_id": "run-inline-fallback",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.ingest_season",
                return_value=expected,
            ) as ingest_mock:
                with patch("api.routers.socials._run_inline_season_ingest") as inline_runner:
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-inline-fallback"
    assert body["execution_mode"] == "inline"
    assert body["execution_mode_canonical"] == "inline_fallback"
    assert body["execution_mode_legacy"] == "inline_fallback"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert body["status"] == "started"
    assert body["worker_health"]["healthy"] is False
    assert isinstance(body.get("warnings"), list)
    assert any("inline dev fallback" in str(item).lower() for item in (body.get("warnings") or []))
    ingest_mock.assert_called_once()
    inline_runner.assert_called_once_with(
        run_id="run-inline-fallback",
        platforms=["youtube"],
        ingest_mode="posts_and_comments",
        worker_prefix="api-background",
    )


def test_ingest_requires_remote_worker_for_instagram_even_with_inline_fallback_enabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "instagram,tiktok")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_WORKER_REQUIRED"
    assert body["detail"]["required_platforms"] == ["instagram"]
    ingest_mock.assert_not_called()


def test_ingest_comments_only_inline_fallback_spawns_per_platform_workers() -> None:
    from trr_backend.socials.inline_ingest import run_inline_season_ingest_execution

    worker_counts: list[int] = []
    execute_calls: list[dict[str, object]] = []

    class _FakeFuture:
        def result(self) -> None:
            return None

    class _FakeThreadPoolExecutor:
        def __init__(self, *, max_workers: int):
            worker_counts.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):  # noqa: ANN001
            fn(*args, **kwargs)
            return _FakeFuture()

    def _record_execute(
        run_id: str, *, worker_id: str | None = None, stage: str | None = None, platform: str | None = None
    ) -> None:
        execute_calls.append(
            {
                "run_id": run_id,
                "worker_id": worker_id,
                "stage": stage,
                "platform": platform,
            }
        )

    run_inline_season_ingest_execution(
        "run-inline-comments-only",
        platforms=["instagram", "youtube", "tiktok", "twitter"],
        supported_platforms=["instagram", "youtube", "tiktok", "twitter"],
        ingest_mode="comments_only",
        worker_prefix="api-background",
        comments_workers_cap=3,
        execute_run=_record_execute,
        thread_pool_executor_factory=_FakeThreadPoolExecutor,
    )

    assert worker_counts == [3]
    assert execute_calls == [
        {
            "run_id": "run-inline-comments-only",
            "worker_id": "api-background:comments:instagram",
            "stage": "comments",
            "platform": "instagram",
        },
        {
            "run_id": "run-inline-comments-only",
            "worker_id": "api-background:comments:youtube",
            "stage": "comments",
            "platform": "youtube",
        },
        {
            "run_id": "run-inline-comments-only",
            "worker_id": "api-background:comments:tiktok",
            "stage": "comments",
            "platform": "tiktok",
        },
        {
            "run_id": "run-inline-comments-only",
            "worker_id": "api-background:comments:twitter",
            "stage": "comments",
            "platform": "twitter",
        },
    ]


def test_ingest_inline_background_failure_runs_recovery_path(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "none")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["youtube"],
    }
    expected = {
        "season_id": season_id,
        "run_id": "run-inline-recovery",
        "status": "pending",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }
    execute_worker_ids: list[str | None] = []

    def _inline_with_initial_failure(
        run_id: str, *, worker_prefix: str, platforms: list[str] | None, ingest_mode: str
    ) -> None:
        execute_worker_ids.append(worker_prefix)
        if worker_prefix == "api-background":
            raise RuntimeError("inline execution failed")
        return None

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected):
            with patch(
                "trr_backend.repositories.social_season_analytics.recover_stale_running_jobs",
                return_value=[{"id": "job-stale"}],
            ) as recover_mock:
                with patch(
                    "api.routers.socials._run_inline_season_ingest",
                    side_effect=_inline_with_initial_failure,
                ):
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    assert execute_worker_ids == ["api-background", "api-background:recovery"]
    recover_mock.assert_called_once_with(run_id="run-inline-recovery", limit=250)


def test_ingest_comments_only_queue_mode_does_not_spawn_inline_runners(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_COMMENTS_RUN_WORKERS", "3")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram", "youtube", "tiktok", "twitter"],
        "ingest_mode": "comments_only",
    }
    expected = {
        "season_id": season_id,
        "run_id": "run-queue-comments-only",
        "status": "queued",
        "stages": ["comments"],
        "queued_or_started_jobs": 4,
        "summary": {"total_jobs": 4},
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value={"healthy": True, "healthy_workers": 1},
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected):
                with patch("api.routers.socials._run_inline_season_ingest") as inline_runner:
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert body["status"] == "queued"
    inline_runner.assert_not_called()


def test_get_mirror_coverage_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "up_to_date": False,
        "needs_mirror_count": 4,
        "mirrored_count": 9,
        "failed_count": 1,
        "partial_count": 2,
        "pending_count": 1,
        "posts_scanned": 13,
        "by_platform": {"instagram": {"needs_mirror_count": 4}},
        "evaluated_at": "2026-02-24T12:00:00+00:00",
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_mirror_coverage",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/mirror-coverage"
            "?source_scope=bravo&platforms=instagram,twitter&timezone=America/New_York",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["needs_mirror_count"] == 4
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["platforms"] == ["instagram", "twitter"]


def test_ingest_keeps_503_when_worker_missing_outside_dev_even_with_fallback_flag(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("TRR_LOCAL_DEV", raising=False)
    monkeypatch.delenv("SOCIAL_ALLOW_INLINE_DEV_FALLBACK", raising=False)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_WORKER_REQUIRED"
    assert body["detail"]["worker_health"]["healthy"] is False
    ingest_mock.assert_not_called()


def test_ingest_with_queue_enabled_and_worker_present_succeeds(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}
    expected = {
        "season_id": season_id,
        "run_id": "run-healthy",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value={"healthy": True, "healthy_workers": 1},
        ) as worker_guard:
            with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert response.json()["run_id"] == "run-healthy"
    assert response.json()["execution_mode"] == "queued"
    assert response.json()["execution_mode_canonical"] == "queued"
    assert response.json()["execution_mode_legacy"] == "queue"
    assert response.json()["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    worker_guard.assert_called_once_with(required_execution_backend="modal")


def test_get_worker_health_endpoint_returns_health_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "healthy": True,
        "healthy_workers": 2,
        "active_workers": 2,
        "total_workers": 2,
        "stale_after_seconds": 180,
        "workers": [{"worker_id": "social-worker:host:1"}],
        "reason": None,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch("trr_backend.repositories.social_season_analytics.get_worker_health", return_value=expected):
            response = client.get(
                "/api/v1/admin/socials/ingest/worker-health",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    body = response.json()
    assert body["queue_enabled"] is True
    assert body["healthy"] is True
    assert body["healthy_workers"] == 2


def test_purge_inactive_workers_endpoint_returns_counts(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "stale_after_seconds": 180,
        "active_workers": 2,
        "total_workers_before": 30,
        "total_workers_after": 2,
        "deleted_workers": 28,
        "reason": None,
    }

    with patch("trr_backend.repositories.social_season_analytics.purge_inactive_workers", return_value=expected):
        response = client.post(
            "/api/v1/admin/socials/ingest/workers/purge-inactive",
            headers={"Authorization": f"Bearer {token}"},
            json={"stale_after_seconds": 180},
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_get_social_account_catalog_run_progress_returns_503_on_session_pool_saturation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_run_progress",
        side_effect=DatabaseServiceUnavailableError(
            'Database pool initialization failed: connection to server at "aws-1-us-east-1.pooler.supabase.com" '
            "(18.214.78.123), port 5432 failed: FATAL: MaxClientsInSessionMode: max clients reached",
            reason="session_pool_capacity",
        ),
    ):
        response = client.get(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/runs/{run_id}/progress",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["reason"] == "session_pool_capacity"


def test_get_social_account_catalog_run_progress_forwards_fast_flag(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    run_id = str(uuid4())
    expected = {"run_id": run_id, "run_status": "running"}

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_catalog_run_progress",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/catalog/runs/{run_id}/progress"
            "?recent_log_limit=7&fast=1",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    mocked.assert_called_once_with(
        platform="instagram",
        account_handle="bravotv",
        run_id=run_id,
        recent_log_limit=7,
        fast=True,
    )


def test_account_profile_singleflight_collapses_same_key_concurrent_loads() -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    cache_key = socials_router._account_profile_cache_key(
        surface="summary",
        platform="instagram",
        account_handle="bravotv",
    )
    loader_started = Event()
    release_loader = Event()
    call_count = 0

    def _loader() -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        loader_started.set()
        assert release_loader.wait(timeout=1), "loader was never released"
        return {"platform": "instagram", "account_handle": "bravotv"}

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            socials_router._resolve_account_profile_singleflight,
            cache_key,
            _loader,
        )
        assert loader_started.wait(timeout=1), "loader never started"
        second = executor.submit(
            socials_router._resolve_account_profile_singleflight,
            cache_key,
            _loader,
        )
        release_loader.set()
        assert first.result(timeout=1)["account_handle"] == "bravotv"
        assert second.result(timeout=1)["account_handle"] == "bravotv"

    assert call_count == 1


def test_account_profile_singleflight_does_not_cache_failures() -> None:
    from api.routers import socials as socials_router

    socials_router._clear_account_profile_caches()
    cache_key = socials_router._account_profile_cache_key(
        surface="summary",
        platform="instagram",
        account_handle="bravotv",
    )
    attempts = 0

    def _failing_loader() -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        raise DatabaseServiceUnavailableError("pool exhausted", reason="session_pool_capacity")

    with pytest.raises(DatabaseServiceUnavailableError):
        socials_router._resolve_account_profile_singleflight(cache_key, _failing_loader)

    payload = socials_router._resolve_account_profile_singleflight(
        cache_key,
        lambda: {"platform": "instagram", "account_handle": "bravotv"},
    )

    assert payload["account_handle"] == "bravotv"
    assert attempts == 1


@pytest.mark.parametrize(
    ("path", "method"),
    [
        ("/api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/gap-analysis", "GET"),
        ("/api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/gap-analysis/run", "POST"),
        ("/api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/freshness", "POST"),
        ("/api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/review-queue/{item_id}/resolve", "POST"),
    ],
)
def test_social_account_catalog_routes_are_registered_once(path: str, method: str) -> None:
    from api.routers import socials as socials_router

    route_app = FastAPI()
    route_app.include_router(socials_router.router, prefix="/api/v1")
    matches = [
        route
        for route in _iter_app_routes(route_app.routes)
        if getattr(route, "path", None) == path and method in getattr(route, "methods", set())
    ]

    assert len(matches) == 1


def test_purge_inactive_workers_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.purge_inactive_workers",
        side_effect=RuntimeError("purge workers failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/workers/purge-inactive",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "purge workers failed"


def test_get_queue_status_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "queue_enabled": True,
        "workers": {
            "healthy": True,
            "healthy_workers": 1,
            "active_workers": 1,
            "total_workers": 1,
            "stale_after_seconds": 180,
            "workers": [{"worker_id": "social-worker:host:1"}],
            "reason": None,
        },
        "queue": {
            "by_status": {
                "queued": 1,
                "pending": 0,
                "running": 0,
                "retrying": 0,
                "failed": 0,
                "cancelled": 0,
                "completed": 0,
            },
            "by_platform": {"instagram": {"queued": 1}},
            "by_job_type": {"ingest_posts": {"queued": 1}},
            "recent_failures": [],
            "stuck_jobs": [],
            "stuck_jobs_total": 0,
            "runs_by_status": {
                "queued": 1,
                "pending": 0,
                "running": 0,
                "retrying": 0,
                "failed": 0,
                "cancelled": 0,
                "completed": 0,
            },
            "runs_total": 1,
        },
    }

    with patch("trr_backend.repositories.social_season_analytics.get_queue_status", return_value=expected) as mocked:
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["summary_only"] is True
    assert mocked.call_args.kwargs["include_recent_failures"] is False
    assert mocked.call_args.kwargs["include_stuck_jobs"] is False
    assert mocked.call_args.kwargs["include_runs_summary"] is False


def test_get_queue_status_endpoint_forwards_fresh_flag(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status", return_value={"ok": True}
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status?fresh=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert mocked.call_args.kwargs.get("fresh") is True
    assert mocked.call_args.kwargs["summary_only"] is False
    assert mocked.call_args.kwargs["include_recent_failures"] is True
    assert mocked.call_args.kwargs["include_stuck_jobs"] is True
    assert mocked.call_args.kwargs["include_runs_summary"] is True


def test_get_queue_status_endpoint_allows_explicit_summary_refresh(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status", return_value={"ok": True}
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status?fresh=true&detail=summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["fresh"] is True
    assert mocked.call_args.kwargs["summary_only"] is True
    assert mocked.call_args.kwargs["include_recent_failures"] is False


def test_get_queue_status_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status",
        side_effect=RuntimeError("queue status failed"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "queue status failed"


def test_get_run_progress_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "run_id": run_id,
        "stages": {"posts": {"jobs_total": 2}},
        "per_handle": [],
        "recent_log": [],
        "worker_runtime": {"active_workers_now": 1},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_run_progress_snapshot", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs/{run_id}/progress?recent_log_limit=15",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_get_run_progress_endpoint_uses_threadpool(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())
    expected = {"season_id": season_id, "run_id": run_id, "recent_log": []}
    captured: dict[str, Any] = {}

    async def _fake_run_admin_repo_call(func: Any, *args: Any, **kwargs: Any) -> Any:
        captured["func"] = func
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    with (
        patch("api.routers.socials._run_admin_repo_call", side_effect=_fake_run_admin_repo_call) as mocked_threadpool,
        patch("trr_backend.repositories.social_season_analytics.get_run_progress_snapshot") as mocked_repo,
    ):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs/{run_id}/progress?recent_log_limit=15",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked_threadpool.call_count == 1
    mocked_repo.assert_not_called()
    assert captured["func"] is mocked_repo
    assert captured["args"] == (season_id, run_id)
    assert captured["kwargs"]["recent_log_limit"] == 15


def test_create_sync_session_endpoint_returns_payload(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    date_start = "2026-01-01T00:00:00Z"
    date_end = "2026-01-08T00:00:00Z"
    expected = {
        "sync_session_id": str(uuid4()),
        "status": "created",
        "season_id": season_id,
        "current_pass_kind": "posts_and_comments",
        "current_pass_attempt": 1,
        "current_run_id": str(uuid4()),
        "pass_sequence": 1,
        "follow_up_reason": "initial_start",
        "completeness_snapshot": {"up_to_date": False},
    }

    with (
        patch(
            "trr_backend.repositories.social_sync_orchestrator.create_sync_session",
            return_value=expected,
        ) as mocked,
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value={"healthy": True, "healthy_workers": 1},
        ),
    ):
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "source_scope": "bravo",
                "platforms": ["instagram"],
                "date_start": date_start,
                "date_end": date_end,
            },
        )

    assert response.status_code == 200
    assert response.json()["sync_session_id"] == expected["sync_session_id"]
    assert mocked.call_args.args[0] == season_id
    assert mocked.call_args.kwargs["platforms"] == ["instagram"]
    assert mocked.call_args.kwargs["source_scope"] == "bravo"


def test_create_sync_session_endpoint_blocks_when_workers_unhealthy(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "instagram,tiktok")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "api.routers.socials.is_remote_job_plane_enabled",
            return_value=False,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "worker unavailable",
                worker_health={"healthy": False, "healthy_workers": 0, "reason": "no_workers"},
            ),
        ),
    ):
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "source_scope": "bravo",
                "platforms": ["twitter"],
                "date_start": "2026-01-01T00:00:00Z",
                "date_end": "2026-01-08T00:00:00Z",
            },
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_WORKER_UNAVAILABLE"
    assert body["detail"]["worker_health"]["reason"] == "no_workers"


def test_create_sync_session_endpoint_requires_remote_worker_for_configured_platforms(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "twitter,facebook")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    with (
        patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True),
        patch(
            "api.routers.socials.is_remote_job_plane_enabled",
            return_value=False,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "worker unavailable",
                worker_health={"healthy": False, "healthy_workers": 0, "reason": "no_workers"},
            ),
        ),
    ):
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "source_scope": "bravo",
                "platforms": ["facebook", "twitter"],
                "date_start": "2026-01-01T00:00:00Z",
                "date_end": "2026-01-08T00:00:00Z",
            },
        )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_WORKER_REQUIRED"
    assert body["detail"]["required_platforms"] == ["facebook", "twitter"]
    assert "facebook, twitter" in body["detail"]["message"]


def test_get_sync_session_endpoint_returns_payload(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    sync_session_id = str(uuid4())
    expected = {
        "sync_session_id": sync_session_id,
        "season_id": season_id,
        "status": "pass_running",
        "current_pass_kind": "comments_only",
        "current_pass_attempt": 1,
        "current_run_id": str(uuid4()),
        "pass_sequence": 2,
        "follow_up_reason": "comments_incomplete",
        "completeness_snapshot": {"up_to_date": False},
    }

    with patch(
        "trr_backend.repositories.social_sync_orchestrator.evaluate_sync_session",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions/{sync_session_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "pass_running"
    mocked.assert_called_once_with(sync_session_id)


def test_get_sync_session_endpoint_uses_threadpool(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    sync_session_id = str(uuid4())
    expected = {"sync_session_id": sync_session_id, "season_id": season_id, "status": "pass_running"}
    captured: dict[str, Any] = {}

    async def _fake_run_admin_repo_call(func: Any, *args: Any, **kwargs: Any) -> Any:
        captured["func"] = func
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    with (
        patch("api.routers.socials._run_admin_repo_call", side_effect=_fake_run_admin_repo_call) as mocked_threadpool,
        patch("trr_backend.repositories.social_sync_orchestrator.evaluate_sync_session") as mocked_repo,
    ):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions/{sync_session_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked_threadpool.call_count == 1
    mocked_repo.assert_not_called()
    assert captured["func"] is mocked_repo
    assert captured["args"] == (sync_session_id,)
    assert captured["kwargs"] == {}


def test_stream_sync_session_endpoint_emits_event_stream(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    sync_session_id = str(uuid4())
    payload = {
        "sync_session": {
            "sync_session_id": sync_session_id,
            "season_id": season_id,
            "status": "completed",
            "current_pass_kind": "details_refresh",
            "current_pass_attempt": 1,
            "current_run_id": None,
            "pass_sequence": 3,
            "follow_up_reason": None,
            "source_scope": "bravo",
            "platforms": ["instagram"],
            "date_start": None,
            "date_end": None,
            "pass_history": [],
            "completeness_snapshot": {"up_to_date": True},
        },
        "run_progress": None,
        "emitted_at": "2026-03-16T12:00:00+00:00",
    }

    with patch("api.routers.socials._build_sync_session_stream_payload", return_value=payload):
        with client.stream(
            "GET",
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions/{sync_session_id}/stream",
            headers={"Authorization": f"Bearer {token}"},
        ) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            body = "".join(response.iter_text())

    assert "event: sync_session" in body
    assert sync_session_id in body


def test_build_sync_session_stream_payload_includes_run_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    sync_session_id = str(uuid4())
    season_id = str(uuid4())
    run_id = str(uuid4())
    expected_sync_session = {
        "sync_session_id": sync_session_id,
        "season_id": season_id,
        "current_run_id": run_id,
        "status": "pass_running",
    }
    expected_run_progress = {
        "season_id": season_id,
        "run_id": run_id,
        "status": "running",
    }

    with patch(
        "trr_backend.repositories.social_sync_orchestrator.evaluate_sync_session",
        return_value=expected_sync_session,
    ) as mocked_session:
        with patch(
            "trr_backend.repositories.social_season_analytics.get_run_progress_snapshot",
            return_value=expected_run_progress,
        ) as mocked_progress:
            from api.routers.socials import _build_sync_session_stream_payload

            payload = asyncio.run(_build_sync_session_stream_payload(sync_session_id))

    assert payload["sync_session"] == expected_sync_session
    assert payload["run_progress"] == expected_run_progress
    mocked_session.assert_called_once_with(sync_session_id)
    mocked_progress.assert_called_once_with(season_id, run_id, recent_log_limit=20)


def test_cancel_sync_session_endpoint_returns_payload(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    sync_session_id = str(uuid4())
    expected = {
        "sync_session_id": sync_session_id,
        "season_id": season_id,
        "status": "cancelling",
    }

    with patch(
        "trr_backend.repositories.social_sync_orchestrator.cancel_sync_session",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions/{sync_session_id}/cancel",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "cancelling"
    assert mocked.call_args.kwargs["cancelled_by"] == "admin@example.com"


def test_retry_sync_session_endpoint_returns_payload(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    sync_session_id = str(uuid4())
    expected = {
        "sync_session_id": sync_session_id,
        "season_id": season_id,
        "status": "pass_running",
        "current_pass_kind": "details_refresh",
        "current_pass_attempt": 2,
    }

    with patch(
        "trr_backend.repositories.social_sync_orchestrator.retry_sync_session",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/sync-sessions/{sync_session_id}/retry",
            headers={"Authorization": f"Bearer {token}"},
            json={"retry_kind": "retry_failed_media"},
        )

    assert response.status_code == 200
    assert response.json()["current_pass_attempt"] == 2
    assert mocked.call_args.kwargs["retry_kind"] == "retry_failed_media"


def test_get_week_live_health_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "day_account_rows": [
            {"day": "2025-11-25", "platform": "instagram", "account": "bravotv", "posts": 1, "comments": 2, "likes": 3}
        ],
        "asset_health": [
            {"asset": "images", "scraped": 3, "saved": 2},
        ],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_week_live_health_snapshot", return_value=expected
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/11/live-health?platforms=instagram,tiktok&timezone=America/New_York&source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["week_index"] == 11
    assert mocked.call_args.kwargs["platforms"] == ["instagram", "tiktok"]
    assert mocked.call_args.kwargs["timezone"] == "America/New_York"


def test_get_health_dot_endpoint_returns_lightweight_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "queue_enabled": True,
        "workers": {
            "healthy": True,
            "healthy_workers": 2,
            "shared_account_backfill_readiness": None,
        },
        "queue": {
            "by_status": {
                "running": 3,
                "pending": 4,
                "queued": 5,
                "failed": 1,
            },
        },
        "alerts": [
            {
                "code": "tiktok_single_path_risk",
                "severity": "warning",
                "message": "TikTok posts currently rely on yt-dlp as the only proven live path.",
            },
            {
                "code": "tiktok_single_path_degraded",
                "severity": "critical",
                "message": "Recent TikTok queue failures include yt-dlp-related rows.",
            },
        ],
        "updated_at": "2026-02-28T12:00:00Z",
    }

    with patch("trr_backend.repositories.social_season_analytics.get_queue_status") as mocked:
        mocked.return_value = {
            "queue_enabled": True,
            "workers": {
                "healthy": True,
                "healthy_workers": 2,
            },
            "queue": {
                "by_status": {
                    "running": 3,
                    "pending": 4,
                    "queued": 5,
                    "failed": 1,
                },
            },
            "alerts": expected["alerts"],
        }
        response = client.get(
            "/api/v1/admin/socials/ingest/health-dot",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["queue_enabled"] is expected["queue_enabled"]
    assert body["workers"] == expected["workers"]
    assert body["queue"] == expected["queue"]
    assert body["alerts"] == expected["alerts"]
    assert isinstance(body.get("updated_at"), str)
    mocked.assert_called_once_with(
        include_recent_failures=False,
        include_stuck_jobs=False,
        include_runs_summary=False,
        summary_only=True,
        statement_timeout_ms=2000,
    )


def test_get_health_dot_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status",
        side_effect=RuntimeError("health dot failed"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/health-dot",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "health dot failed"


def test_get_live_status_aggregates_health_queue_and_operations(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    queue_status = {
        "queue_enabled": True,
        "workers": {
            "healthy": True,
            "healthy_workers": 2,
            "shared_account_backfill_readiness": {"ready": True},
        },
        "queue": {
            "by_status": {
                "running": 3,
                "pending": 4,
                "queued": 5,
                "failed": 1,
            },
        },
    }
    operations_health = {
        "summary": {
            "active_total": 2,
            "stale_total": 0,
            "cancelling_total": 0,
            "by_status": {"running": 2},
            "by_type": {"sync": 2},
            "runtime_split": {"modal": 1, "local": 1, "other": 0, "unknown": 0},
            "stale_after_seconds": 120,
            "cancelling_grace_seconds": 300,
        },
        "active_operations": [],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status",
        return_value=queue_status,
    ) as mocked_queue:
        with patch(
            "trr_backend.repositories.admin_operations.get_admin_operations_health",
            return_value=operations_health,
        ) as mocked_ops:
            response = client.get(
                "/api/v1/admin/socials/live-status",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    body = response.json()
    assert body["health_dot"]["queue_enabled"] is True
    assert body["health_dot"]["workers"]["healthy_workers"] == 2
    assert body["queue_status"] == queue_status
    assert body["admin_operations"] == operations_health
    assert isinstance(body["generated_at"], str)
    assert isinstance(body["sequence"], int)
    mocked_queue.assert_called_once_with(
        include_recent_failures=False,
        include_stuck_jobs=False,
        include_runs_summary=False,
        summary_only=True,
        statement_timeout_ms=1000,
    )
    mocked_ops.assert_called_once()


def test_live_status_stream_uses_threadpool_for_payload_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import api.routers.socials as router_module
    from trr_backend.socials.api.handlers import live_status as live_status_handler

    payload = {
        "health_dot": {"queue_enabled": True},
        "queue_status": {"queue_enabled": True},
        "admin_operations": {"summary": {"active_total": 0}},
        "generated_at": "2026-02-28T12:00:00Z",
        "sequence": 7,
    }
    captured: dict[str, Any] = {}
    disconnect_checks = {"count": 0}

    async def _fake_run_in_threadpool(func: Any, *args: Any, **kwargs: Any) -> Any:
        captured["func"] = func
        captured["args"] = args
        captured["kwargs"] = kwargs
        return payload

    class _DummyRequest:
        async def is_disconnected(self) -> bool:
            disconnect_checks["count"] += 1
            return disconnect_checks["count"] > 1

    with (
        patch.object(router_module, "run_in_threadpool", side_effect=_fake_run_in_threadpool) as mocked_threadpool,
        patch.object(live_status_handler, "build_live_status_payload") as mocked_builder,
    ):
        response = asyncio.run(router_module.stream_social_live_status(_DummyRequest(), None))

        async def _collect_chunks() -> list[str]:
            chunks: list[str] = []
            async for chunk in response.body_iterator:
                chunks.append(chunk)
            return chunks

        chunks = asyncio.run(_collect_chunks())

    assert response.status_code == 200
    assert chunks[0] == "event: live_status\n"
    assert '"sequence": 7' in chunks[1]
    assert mocked_threadpool.call_count == 1
    mocked_builder.assert_not_called()
    assert captured["func"] is mocked_builder
    assert captured["args"] == ()
    assert captured["kwargs"] == {}


def test_cancel_stuck_jobs_endpoint_accepts_targeted_job_ids(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "requested_job_ids_count": 1,
        "cancelled_jobs": 1,
        "cancelled_job_ids": [job_id],
        "affected_run_ids": [],
        "stuck_jobs_remaining": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.cancel_stuck_jobs", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/stuck-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={"job_ids": [job_id]},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["job_ids"] == [job_id]


def test_cancel_stuck_jobs_endpoint_defaults_to_clear_all(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "requested_job_ids_count": 0,
        "cancelled_jobs": 3,
        "cancelled_job_ids": [str(uuid4()), str(uuid4()), str(uuid4())],
        "affected_run_ids": [str(uuid4())],
        "stuck_jobs_remaining": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.cancel_stuck_jobs", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/stuck-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert response.json()["cancelled_jobs"] == 3
    assert mocked.call_args.kwargs["job_ids"] is None


def test_cancel_stuck_jobs_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.cancel_stuck_jobs",
        side_effect=RuntimeError("cancel stuck failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/stuck-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "cancel stuck failed"


def test_cancel_dispatch_blocked_jobs_endpoint_accepts_targeted_job_ids(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "requested_job_ids_count": 1,
        "cancelled_jobs": 1,
        "cancelled_job_ids": [job_id],
        "affected_run_ids": [],
        "dispatch_blocked_jobs_remaining": 0,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.cancel_dispatch_blocked_jobs",
        return_value=expected,
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/dispatch-blocked-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={"job_ids": [job_id]},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["job_ids"] == [job_id]


def test_cancel_active_jobs_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "cancelled_jobs": 5,
        "cancelled_job_ids": [str(uuid4())],
        "affected_run_ids": [str(uuid4())],
        "active_jobs_remaining": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.cancel_active_jobs", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/active-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_count == 1


def test_cancel_active_jobs_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.cancel_active_jobs",
        side_effect=RuntimeError("cancel active failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/active-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "cancel active failed"


def test_dismiss_recent_failures_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "requested_job_ids_count": 1,
        "dismissed_jobs": 1,
        "dismissed_job_ids": [job_id],
        "recent_failures_remaining": 3,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.dismiss_recent_failures",
        return_value=expected,
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/recent-failures/dismiss",
            headers={"Authorization": f"Bearer {token}"},
            json={"job_ids": [job_id]},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["job_ids"] == [job_id]
    assert mocked.call_args.kwargs["dismiss_all_visible"] is False


def test_dismiss_recent_failures_endpoint_supports_dismiss_all_visible(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "requested_job_ids_count": 0,
        "dismissed_jobs": 4,
        "dismissed_job_ids": [],
        "recent_failures_remaining": 0,
        "dismiss_all_visible": True,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.dismiss_recent_failures",
        return_value=expected,
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/recent-failures/dismiss",
            headers={"Authorization": f"Bearer {token}"},
            json={"dismiss_all_visible": True},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["dismiss_all_visible"] is True


def test_dismiss_recent_failures_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.dismiss_recent_failures",
        side_effect=RuntimeError("dismiss failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/recent-failures/dismiss",
            headers={"Authorization": f"Bearer {token}"},
            json={"job_ids": [job_id]},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "dismiss failed"


def test_reset_social_ingest_health_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "cancelled_jobs": 0,
        "dismissed_failures": 12,
        "dismissed_failed_runs": 4,
        "active_jobs_remaining": 0,
        "recent_failures_remaining": 0,
        "failed_runs_remaining": 0,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.reset_social_ingest_health",
        return_value=expected,
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/reset-health",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_count == 1


def test_reset_social_ingest_health_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.reset_social_ingest_health",
        side_effect=RuntimeError("reset failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/reset-health",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "reset failed"


def test_get_worker_detail_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "worker": {"worker_id": "social-worker:test"},
        "current_job": {"id": str(uuid4()), "run_id": str(uuid4()), "platform": "twitter"},
        "run": {"run_id": str(uuid4()), "status": "running"},
        "currently_scraping": "comments_scan",
        "progress_made": {"items_found": 12},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_worker_detail", return_value=expected):
        response = client.get(
            "/api/v1/admin/socials/ingest/workers/social-worker:test/detail",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_get_worker_detail_endpoint_returns_404_when_worker_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_worker_detail",
        side_effect=ValueError("worker_not_found"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/workers/social-worker:missing/detail",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Worker not found"


def test_get_worker_detail_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_worker_detail",
        side_effect=RuntimeError("worker detail failed"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/workers/social-worker:test/detail",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "worker detail failed"


def test_debug_job_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "job_id": job_id,
        "run_id": str(uuid4()),
        "model_used": "gpt-5.3-codex",
        "fallback_used": False,
        "analysis": {"root_cause": "x", "confidence": 0.8, "files_touched": [], "tests_to_run": []},
        "patch_unified_diff": "--- a/api/routers/socials.py\n+++ b/api/routers/socials.py\n",
        "apply": {
            "enabled": False,
            "requested": False,
            "applied": False,
            "check_ok": False,
            "error": None,
            "files_changed": [],
        },
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={"apply_patch": True, "confirm_apply": True, "include_context": False},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "apply_patch": True,
        "confirm_apply": True,
        "include_context": False,
    }


def test_debug_job_endpoint_returns_apply_blocked_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "job_id": job_id,
        "run_id": str(uuid4()),
        "model_used": "gpt-5.3-codex",
        "fallback_used": False,
        "analysis": {"root_cause": "x", "confidence": 0.8, "files_touched": [], "tests_to_run": []},
        "patch_unified_diff": "",
        "apply": {
            "enabled": False,
            "requested": True,
            "applied": False,
            "check_ok": False,
            "error": "Patch apply is disabled by server configuration.",
            "files_changed": [],
        },
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        return_value=expected,
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={"apply_patch": True, "confirm_apply": True},
        )

    assert response.status_code == 200
    assert response.json()["apply"]["error"] == "Patch apply is disabled by server configuration."


def test_debug_job_endpoint_returns_apply_success_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "job_id": job_id,
        "run_id": str(uuid4()),
        "model_used": "gpt-5.3-codex",
        "fallback_used": False,
        "analysis": {
            "root_cause": "x",
            "confidence": 0.8,
            "files_touched": ["api/routers/socials.py"],
            "tests_to_run": [],
        },
        "patch_unified_diff": "--- a/api/routers/socials.py\n+++ b/api/routers/socials.py\n",
        "apply": {
            "enabled": True,
            "requested": True,
            "applied": True,
            "check_ok": True,
            "error": None,
            "files_changed": ["api/routers/socials.py"],
        },
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        return_value=expected,
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={"apply_patch": True, "confirm_apply": True},
        )

    assert response.status_code == 200
    assert response.json()["apply"]["applied"] is True


def test_debug_job_endpoint_returns_404_for_missing_job(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        side_effect=ValueError("job_not_found"),
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"


def test_debug_job_endpoint_returns_503_when_openai_key_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        side_effect=ValueError("openai_api_key_missing"),
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "OPENAI_API_KEY is not configured"


def test_debug_job_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        side_effect=RuntimeError("debug failed"),
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "debug failed"


def test_get_comments_coverage_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "total_saved_comments": 10,
        "total_reported_comments": 10,
        "coverage_pct": 100.0,
        "up_to_date": True,
        "stale_posts_count": 0,
        "posts_scanned": 5,
        "by_platform": {"instagram": {"saved_comments": 10, "reported_comments": 10}},
        "evaluated_at": "2026-02-24T12:00:00+00:00",
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_comments_coverage",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/comments-coverage"
            "?source_scope=bravo&platforms=instagram,twitter&timezone=America/New_York",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["total_saved_comments"] == 10
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["platforms"] == ["instagram", "twitter"]


def test_get_analytics_endpoint_accepts_facebook_threads_platform_filters(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "window": {"week": None},
        "totals": {"posts": 0, "comments": 0},
        "weekly": [],
        "platforms": {},
        "rows": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics"
            "?source_scope=bravo&platforms=facebook,threads&timezone=America/New_York",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["platforms"] == ["facebook", "threads"]


def test_get_analytics_endpoint_returns_additive_reddit_block(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "window": {"week": None},
        "summary": {
            "total_posts": 0,
            "total_comments": 0,
            "total_engagement": 0,
            "sentiment_mix": {
                "positive": 0,
                "neutral": 0,
                "negative": 0,
                "counts": {"positive": 0, "neutral": 0, "negative": 0},
            },
        },
        "weekly": [],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
        "reddit": {
            "tracked_post_count": 660,
            "show_match_post_count": 401,
            "comment_count": 18841,
            "deep_link": {"path": "/admin/social/reddit/BravoRealHousewives/rhoslc/s6"},
        },
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["reddit"]["tracked_post_count"] == 660
    assert response.json()["reddit"]["deep_link"]["path"].endswith("/rhoslc/s6")


def test_get_week_detail_endpoint_returns_facebook_threads_platform_maps(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 2, "label": "Week 2", "start": "2026-01-10T00:00:00Z", "end": "2026-01-17T00:00:00Z"},
        "platforms": {
            "facebook": {"posts": [], "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0}},
            "threads": {"posts": [], "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0}},
        },
        "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/2?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert "facebook" in body["platforms"]
    assert "threads" in body["platforms"]


def test_export_csv(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    snapshot = {
        "rows": [
            {
                "week_index": 1,
                "platform": "instagram",
                "kind": "comment",
                "source_id": "abc",
                "timestamp": "2026-02-10T10:00:00+00:00",
                "author": "viewer",
                "url": "https://example.com",
                "engagement": 10,
                "sentiment": "positive",
                "text": "Great episode",
            }
        ]
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=snapshot):
        with patch("trr_backend.repositories.social_season_analytics.build_csv", return_value="a,b\n1,2\n"):
            response = client.get(
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/export.csv",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=" in response.headers["content-disposition"]
    assert "1,2" in response.text


def test_get_analytics_allows_week_zero(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "window": {"week": 0},
        "summary": {},
        "weekly": [],
        "weekly_platform_engagement": [],
        "weekly_daily_activity": [],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics?week=0",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["window"]["week"] == 0
    assert "weekly_platform_engagement" in response.json()
    assert "weekly_daily_activity" in response.json()
    assert mocked.call_args.kwargs["week"] == 0


def test_get_analytics_include_slices_forwarded(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "window": {"week": None},
        "summary": {},
        "weekly": [],
        "weekly_platform_posts": [],
        "weekly_platform_engagement": [],
        "weekly_daily_activity": [],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
        "benchmark": {"week_index": 1},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics?include=rows,benchmark",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["benchmark"]["week_index"] == 1
    assert mocked.call_args.kwargs["include_rows"] is True
    assert mocked.call_args.kwargs["include_flags"] is False
    assert mocked.call_args.kwargs["include_schedule"] is False
    assert mocked.call_args.kwargs["include_benchmark"] is True


def test_get_analytics_endpoint_uses_threadpool(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "window": {"week": None},
        "summary": {},
        "weekly": [],
        "weekly_platform_engagement": [],
        "weekly_daily_activity": [],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
    }
    captured: dict[str, Any] = {}

    async def _fake_run_in_threadpool(func: Any, *args: Any, **kwargs: Any) -> Any:
        captured["func"] = func
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    with (
        patch("api.routers.socials.run_in_threadpool", side_effect=_fake_run_in_threadpool) as mocked_threadpool,
        patch("trr_backend.repositories.social_season_analytics.get_analytics") as mocked_get_analytics,
    ):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics?include=rows,benchmark",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked_threadpool.call_count == 1
    mocked_get_analytics.assert_not_called()
    assert captured["func"] is mocked_get_analytics
    assert captured["args"] == (season_id,)
    assert captured["kwargs"]["include_rows"] is True
    assert captured["kwargs"]["include_flags"] is False
    assert captured["kwargs"]["include_schedule"] is False
    assert captured["kwargs"]["include_benchmark"] is True


def test_get_ingest_run_summary_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    summaries = [
        {
            "run_id": str(uuid4()),
            "status": "completed",
            "source_scope": "bravo",
            "success_rate_pct": 100.0,
        }
    ]

    with patch("trr_backend.repositories.social_season_analytics.list_run_summaries", return_value=summaries) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs/summary?source_scope=bravo&limit=20",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["filters"]["source_scope"] == "bravo"
    assert body["summaries"] == summaries
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["limit"] == 20


def test_export_pdf(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    snapshot = {
        "summary": {
            "show_id": str(uuid4()),
            "season_number": 10,
        }
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=snapshot):
        with patch(
            "trr_backend.repositories.social_season_analytics.build_pdf",
            return_value=b"%PDF-1.4\n...",
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.pdf_filename",
                return_value="social_report_test.pdf",
            ):
                response = client.get(
                    f"/api/v1/admin/socials/seasons/{season_id}/analytics/export.pdf",
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert response.headers["content-disposition"] == 'attachment; filename="social_report_test.pdf"'
    assert response.content.startswith(b"%PDF")


def test_get_tiktok_overview_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "kpis": {"post_count": 10, "views": 1000, "likes": 100, "comments": 40, "shares": 20, "saves": 5},
    }
    with patch("trr_backend.repositories.social_season_analytics.get_tiktok_overview", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/overview?sound_id=7540327234013301517",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["kpis"]["post_count"] == 10
    assert mocked.call_args.kwargs["sound_id"] == "7540327234013301517"


def test_get_tiktok_sound_posts_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "sound_id": "7540327234013301517",
        "posts": [{"platform_post_id": "123"}],
    }
    with patch(
        "trr_backend.repositories.social_season_analytics.get_tiktok_sound_posts", return_value=expected
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/sounds/7540327234013301517/posts?limit=25",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["sound_id"] == "7540327234013301517"
    assert mocked.call_args.kwargs["sound_id"] == "7540327234013301517"
    assert mocked.call_args.kwargs["limit"] == 25


def test_get_tiktok_sounds_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "sounds": [{"sound_id": "7540327234013301517", "creator_post_count": 3}],
    }
    with patch("trr_backend.repositories.social_season_analytics.get_tiktok_sounds", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/sounds?search=lisa&limit=20",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["sounds"][0]["sound_id"] == "7540327234013301517"
    assert mocked.call_args.kwargs["search"] == "lisa"
    assert mocked.call_args.kwargs["limit"] == 20


def test_get_tiktok_content_health_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "thresholds": {"median_saves": 12},
        "posts": [{"post_id": "abc", "reason_flags": ["low_saves"]}],
    }
    with patch(
        "trr_backend.repositories.social_season_analytics.get_tiktok_content_health",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/content-health?hashtag=rhoslc",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["posts"][0]["post_id"] == "abc"
    assert mocked.call_args.kwargs["hashtag"] == "rhoslc"
