"""Tests for comments recovery admin endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import jwt
import pytest
from fastapi import BackgroundTasks
from fastapi.testclient import TestClient

from api.main import app


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


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def auth_headers(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    secret = "test-secret-32-bytes-minimum-abcdef"
    monkeypatch.setenv("SUPABASE_JWT_SECRET", secret)
    return {"Authorization": f"Bearer {_make_admin_token(secret)}"}


def test_post_social_account_comments_run_resume_route(
    client: TestClient,
    auth_headers: dict[str, str],
) -> None:
    run_id = uuid4()
    expected = {"run_id": str(run_id), "status": "running"}

    with patch(
        "trr_backend.socials.pipelines.comments.instagram.resume_social_account_comments_run",
        return_value=expected,
    ) as resume_mock:
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/resume",
            headers=auth_headers,
        )

    assert response.status_code == 200
    assert response.json() == expected
    resume_mock.assert_called_once_with(
        platform="instagram",
        account_handle="bravotv",
        run_id=str(run_id),
        initiated_by="admin@example.com",
    )


@pytest.mark.parametrize(
    ("exception", "expected_status", "expected_code"),
    [
        pytest.param(
            "conflict",
            409,
            "SOCIAL_COMMENTS_RUN_ACTIVE",
            id="conflict",
        ),
        pytest.param(
            "auth_repair_failed",
            503,
            "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED",
            id="auth-repair-failed",
        ),
        pytest.param(
            "validation",
            400,
            "SOCIAL_COMMENTS_RUN_INVALID_STATE",
            id="validation",
        ),
    ],
)
def test_post_social_account_comments_run_resume_route_errors(
    client: TestClient,
    auth_headers: dict[str, str],
    exception: str,
    expected_status: int,
    expected_code: str,
) -> None:
    # The compatibility alias module swaps itself for the implementation module
    # at import time, so resolve the exception classes via an Any-typed handle.
    season_analytics: Any = import_module("trr_backend.repositories.social_season_analytics")

    run_id = uuid4()
    error: Exception
    if exception == "conflict":
        error = season_analytics.SocialIngestConflictError(expected_code, "Comments run is already active.")
    else:
        error = season_analytics.SocialIngestValidationError(expected_code, "Comments run cannot be resumed.")

    with patch(
        "trr_backend.socials.pipelines.comments.instagram.resume_social_account_comments_run",
        side_effect=error,
    ):
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/resume",
            headers=auth_headers,
        )

    assert response.status_code == expected_status
    assert response.json()["detail"]["code"] == expected_code


def test_post_social_account_comments_run_repair_auth_requires_confirmation(
    client: TestClient,
    auth_headers: dict[str, str],
) -> None:
    run_id = uuid4()

    with (
        patch(
            "trr_backend.socials.pipelines.comments.instagram.request_social_account_comments_run_auth_repair",
        ) as request_mock,
        patch.object(BackgroundTasks, "add_task") as add_task_mock,
    ):
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/repair-auth",
            headers=auth_headers,
            json={},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INSTAGRAM_AUTH_REFRESH_CONFIRMATION_REQUIRED"
    request_mock.assert_not_called()
    add_task_mock.assert_not_called()


def test_post_social_account_comments_run_repair_auth_schedules_background_task(
    client: TestClient,
    auth_headers: dict[str, str],
) -> None:
    run_id = uuid4()
    expected: dict[str, Any] = {
        "run_id": str(run_id),
        "status": "accepted",
        "repair_status": "running",
    }

    with (
        patch(
            "trr_backend.socials.pipelines.comments.instagram.request_social_account_comments_run_auth_repair",
            return_value=expected,
        ) as request_mock,
        patch(
            "trr_backend.socials.pipelines.comments.instagram.execute_social_account_comments_run_auth_repair",
        ) as execute_mock,
        patch.object(BackgroundTasks, "add_task") as add_task_mock,
    ):
        response = client.post(
            f"/api/v1/admin/socials/profiles/instagram/bravotv/comments/runs/{run_id}/repair-auth",
            headers=auth_headers,
            json={
                "operator_confirmation": "I UNDERSTAND INSTAGRAM AUTH RISK",
                "allow_cookie_refresh": True,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    request_mock.assert_called_once_with(
        platform="instagram",
        account_handle="bravotv",
        run_id=str(run_id),
        initiated_by="admin@example.com",
    )
    add_task_mock.assert_called_once_with(
        execute_mock,
        platform="instagram",
        account_handle="bravotv",
        run_id=str(run_id),
        initiated_by="admin@example.com",
        allow_cookie_refresh=True,
    )
