from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.repositories import show_slug_reads

SHOW_ID = "11111111-1111-1111-1111-111111111111"
SHOW = {
    "id": SHOW_ID,
    "name": "The Real Housewives of Beverly Hills",
    "slug": "rhobh",
}
PATH = "/api/v2/admin/shows/exact-slug/{slug}"


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "trr-app-internal-admin",
        "admin_uid": "signed-admin-uid",
        "role": "internal_admin",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_v2_exact_show_slug_normalizes_and_returns_strict_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[str] = []

    def fake_get(slug: str):
        captured.append(slug)
        return SHOW, 1

    monkeypatch.setattr(show_slug_reads, "get_show_by_exact_slug", fake_get)

    response = TestClient(app).get(
        "/api/v2/admin/shows/exact-slug/RHOBH",
        headers={"X-TRR-Admin-User-Uid": "spoofed-raw-header"},
    )

    assert response.status_code == 200
    assert response.json() == {"show": SHOW}
    assert captured == ["rhobh"]


def test_v2_exact_show_slug_rejects_raw_admin_header_without_signed_bearer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_get(_slug: str):
        nonlocal calls
        calls += 1
        return SHOW, 1

    monkeypatch.setattr(show_slug_reads, "get_show_by_exact_slug", fake_get)
    app.dependency_overrides.pop(require_internal_admin, None)

    response = TestClient(app).get(
        "/api/v2/admin/shows/exact-slug/rhobh",
        headers={"X-TRR-Admin-User-Uid": "spoofed-raw-header"},
    )

    assert response.status_code == 401
    assert calls == 0


def test_v2_exact_show_slug_is_uncached_and_calls_repository_once_per_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_get(slug: str):
        calls.append(slug)
        return SHOW, 1

    monkeypatch.setattr(show_slug_reads, "get_show_by_exact_slug", fake_get)
    client = TestClient(app)

    first = client.get("/api/v2/admin/shows/exact-slug/rhobh")
    second = client.get("/api/v2/admin/shows/exact-slug/rhobh")

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls == ["rhobh", "rhobh"]


@pytest.mark.parametrize(
    "slug",
    [
        "bad_slug",
        "bad!slug",
        "a" * 161,
    ],
)
def test_v2_exact_show_slug_invalid_values_use_safe_problem_400_without_query(
    monkeypatch: pytest.MonkeyPatch,
    slug: str,
) -> None:
    calls = 0

    def fake_get(_slug: str):
        nonlocal calls
        calls += 1
        return SHOW, 1

    monkeypatch.setattr(show_slug_reads, "get_show_by_exact_slug", fake_get)

    response = TestClient(app).get(
        f"/api/v2/admin/shows/exact-slug/{slug}",
        headers={"x-request-id": "invalid-show-slug"},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SHOW_SLUG"
    assert response.json()["detail"]["status"] == 400
    assert response.json()["detail"]["request_id"] == "invalid-show-slug"
    assert "422" not in response.text
    assert calls == 0


def test_v2_exact_show_slug_missing_uses_stable_problem_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        show_slug_reads,
        "get_show_by_exact_slug",
        lambda _slug: (None, 1),
    )

    response = TestClient(app).get("/api/v2/admin/shows/exact-slug/missing-show")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "SHOW_NOT_FOUND"
    assert response.json()["detail"]["message"] == "Show not found"


def test_v2_exact_show_slug_database_and_unexpected_failures_are_safe_problems(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(_slug: str):
        raise DatabaseServiceUnavailableError(
            "secret database topology",
            reason="pool_capacity",
        )

    monkeypatch.setattr(show_slug_reads, "get_show_by_exact_slug", unavailable)
    unavailable_response = TestClient(app).get(
        "/api/v2/admin/shows/exact-slug/rhobh"
    )

    assert unavailable_response.status_code == 503
    assert unavailable_response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in unavailable_response.text

    def failed(_slug: str):
        raise RuntimeError("secret exact-slug query")

    monkeypatch.setattr(show_slug_reads, "get_show_by_exact_slug", failed)
    failed_response = TestClient(app).get(
        "/api/v2/admin/shows/exact-slug/rhobh"
    )

    assert failed_response.status_code == 500
    assert failed_response.json()["detail"]["code"] == "SHOW_SLUG_REQUEST_FAILED"
    assert "secret exact-slug query" not in failed_response.text


def test_v2_exact_show_slug_response_drift_uses_safe_problem_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        show_slug_reads,
        "get_show_by_exact_slug",
        lambda _slug: ({**SHOW, "unexpected": "drift"}, 1),
    )

    response = TestClient(app).get("/api/v2/admin/shows/exact-slug/rhobh")

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "SHOW_SLUG_REQUEST_FAILED"
    assert "validation error" not in response.text.lower()


def test_v2_exact_show_slug_rejects_repository_slug_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        show_slug_reads,
        "get_show_by_exact_slug",
        lambda _slug: ({**SHOW, "slug": "different-slug"}, 1),
    )

    response = TestClient(app).get("/api/v2/admin/shows/exact-slug/rhobh")

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "SHOW_SLUG_REQUEST_FAILED"
    assert "different-slug" not in response.text


def test_v2_exact_show_slug_openapi_is_strict_and_preserves_admin_auth() -> None:
    schema = app.openapi()
    operation = schema["paths"][PATH]["get"]

    assert operation["operationId"] == "getAdminShowByExactSlugV2"
    assert operation["security"] == [{"InternalAdminBearer": []}]
    assert set(operation["responses"]) == {"200", "400", "404", "500", "503"}
    assert "422" not in operation["responses"]
    assert [(parameter["name"], parameter["in"]) for parameter in operation["parameters"]] == [
        ("slug", "path")
    ]
    assert "post" not in schema["paths"][PATH]

    response_schema = schema["components"]["schemas"]["ExactShowSlugResponseV2"]
    show_schema = schema["components"]["schemas"]["ExactShowSlugV2"]
    assert response_schema["additionalProperties"] is False
    assert response_schema["required"] == ["show"]
    assert set(response_schema["properties"]) == {"show"}
    assert show_schema["additionalProperties"] is False
    assert set(show_schema["required"]) == {"id", "name", "slug"}
    assert set(show_schema["properties"]) == {"id", "name", "slug"}
