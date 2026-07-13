from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_social_completion as router_module
from trr_backend.db import pg


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "internal-admin:test",
        "role": "internal_admin",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def _completion_payload(*, handle: str = "bravotv", year: int = 2026) -> dict[str, object]:
    return {
        "platform": "instagram",
        "handle": handle,
        "year": year,
        "total_posts": 3,
        "total_reported_comments": 1200,
        "saved_comments": 780,
        "missing_comments": 420,
        "accounted_comments": 1200,
        "lanes": {
            "comments": {"finished": 1, "in_progress": 2, "not_started": 0},
            "details": {"finished": 2, "in_progress": 0, "not_started": 1},
            "media": {"finished": 1, "in_progress": 1, "not_started": 1},
        },
    }


def test_completion_summary_normalizes_profile_and_preserves_json(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_social_completion_summary(**kwargs):
        captured.update(kwargs)
        return _completion_payload(handle=str(kwargs["account_handle"]), year=int(kwargs["year"]))

    monkeypatch.setattr(
        router_module.completion_repo,
        "get_social_completion_summary",
        fake_get_social_completion_summary,
    )

    response = TestClient(app).get(
        "/api/v1/admin/socials/profiles/Instagram/%40BravoTV/completion-summary?year=2026"
    )

    assert response.status_code == 200
    assert response.json() == _completion_payload()
    assert captured == {"platform": "instagram", "account_handle": "bravotv", "year": 2026}


def test_completion_summary_defaults_invalid_or_omitted_year_to_current_utc_year(monkeypatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2027, 7, 1, 12, 0, tzinfo=tz or UTC)

    years: list[int] = []
    monkeypatch.setattr(router_module, "datetime", FixedDateTime)

    def fake_get_social_completion_summary(**kwargs):
        years.append(int(kwargs["year"]))
        return _completion_payload(year=int(kwargs["year"]))

    monkeypatch.setattr(
        router_module.completion_repo,
        "get_social_completion_summary",
        fake_get_social_completion_summary,
    )
    client = TestClient(app)

    invalid = client.get(
        "/api/v1/admin/socials/profiles/instagram/bravotv/completion-summary?year=nope"
    )
    omitted = client.get("/api/v1/admin/socials/profiles/instagram/bravotv/completion-summary")

    assert invalid.status_code == 200
    assert omitted.status_code == 200
    assert invalid.json()["year"] == 2027
    assert omitted.json()["year"] == 2027
    assert years == [2027, 2027]


def test_completion_summary_rejects_unsupported_platform_before_repository(monkeypatch) -> None:
    def fail_if_called(**_kwargs):
        raise AssertionError("repository should not be called")

    monkeypatch.setattr(
        router_module.completion_repo,
        "get_social_completion_summary",
        fail_if_called,
    )

    response = TestClient(app).get(
        "/api/v1/admin/socials/profiles/tiktok/bravotv/completion-summary?year=2026"
    )

    assert response.status_code == 400
    assert response.json() == {"error": "unsupported_profile"}


def test_completion_summary_maps_database_saturation_to_retryable_503(monkeypatch) -> None:
    def fail(**_kwargs):
        raise pg.DatabaseServiceUnavailableError("pool saturated", reason="pool_capacity")

    monkeypatch.setattr(router_module.completion_repo, "get_social_completion_summary", fail)

    response = TestClient(app).get(
        "/api/v1/admin/socials/profiles/instagram/bravotv/completion-summary?year=2026"
    )

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert detail["reason"] == "pool_capacity"
    assert detail["retryable"] is True
    assert detail["retry_after_ms"] == 1000


def test_completion_summary_maps_unexpected_errors_without_leaking_details(monkeypatch) -> None:
    sentinel = "secret database diagnostic"

    def fail(**_kwargs):
        raise RuntimeError(sentinel)

    monkeypatch.setattr(router_module.completion_repo, "get_social_completion_summary", fail)

    response = TestClient(app).get(
        "/api/v1/admin/socials/profiles/instagram/bravotv/completion-summary?year=2026"
    )

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "SOCIAL_COMPLETION_SUMMARY_FAILED"
    assert sentinel not in response.text


def test_landing_scrape_job_health_preserves_response_contract(monkeypatch) -> None:
    payload = {
        "window_hours": 8,
        "window_started_at": "2026-07-13T04:00:00.000Z",
        "generated_at": "2026-07-13T12:00:00.000Z",
        "total_jobs": 15,
        "active_jobs": 4,
        "failed_jobs": 2,
        "failure_signal_jobs": 3,
        "in_failed_sql_transaction_hits": 1,
        "latest_failure_at": "2026-07-13T11:30:00.000Z",
    }
    monkeypatch.setattr(
        router_module.completion_repo,
        "get_social_landing_scrape_job_health",
        lambda: payload,
    )

    response = TestClient(app).get("/api/v1/admin/socials/landing-scrape-job-health")

    assert response.status_code == 200
    assert response.json() == payload


def test_landing_scrape_job_health_maps_database_errors(monkeypatch) -> None:
    def fail():
        raise pg.DatabaseServiceUnavailableError("statement timeout", reason="statement_timeout")

    monkeypatch.setattr(router_module.completion_repo, "get_social_landing_scrape_job_health", fail)

    response = TestClient(app).get("/api/v1/admin/socials/landing-scrape-job-health")

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "statement_timeout"

