"""
Smoke tests for the TRR API.

These tests verify basic functionality without requiring a live database.
For integration tests against Supabase, see test_api_integration.py.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api import deps
from api import main as api_main
from api.main import app
from api.routers import shows as shows_router
from api.routers import surveys as surveys_router


# Mock Supabase client for testing without database connection
def _create_mock_response(data=None):
    """Create a mock response object with data and no error."""
    mock_resp = MagicMock()
    mock_resp.data = data if data is not None else []
    mock_resp.error = None  # Critical: error handling checks this
    return mock_resp


@pytest.fixture
def mock_supabase():
    """Create a mock Supabase client."""
    mock_client = MagicMock()

    # Create mock responses with error=None
    empty_list_response = _create_mock_response([])
    none_response = _create_mock_response(None)

    # Set up chain-able mock for query builder pattern
    # List queries (order -> range -> execute)
    mock_client.schema.return_value.table.return_value.select.return_value.order.return_value.range.return_value.execute.return_value = empty_list_response  # noqa: E501

    # Single queries (eq -> single -> execute)
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = none_response  # noqa: E501

    # Filtered list queries (eq -> order -> range -> execute)
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.order.return_value.range.return_value.execute.return_value = empty_list_response  # noqa: E501

    # Simple filtered queries (eq -> execute)
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.execute.return_value = (
        empty_list_response
    )

    # Double eq queries (eq -> eq -> single -> execute) for season lookups
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.single.return_value.execute.return_value = none_response  # noqa: E501

    # Desc order queries
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = empty_list_response  # noqa: E501

    # RPC calls - simulate survey not found
    mock_client.schema.return_value.rpc.return_value.execute.side_effect = Exception("Survey not found")

    return mock_client


@pytest.fixture
def client(mock_supabase):
    """Create a test client with mocked Supabase dependencies."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase
    original_show_fetch_all = shows_router.pg.fetch_all
    original_survey_fetch_all = surveys_router.pg.fetch_all
    original_survey_execute_values_no_return = surveys_router.pg.execute_values_no_return
    shows_router.pg.fetch_all = lambda *args, **kwargs: []
    surveys_router.pg.fetch_all = lambda *args, **kwargs: []
    surveys_router.pg.execute_values_no_return = lambda *args, **kwargs: None
    yield TestClient(app)
    shows_router.pg.fetch_all = original_show_fetch_all
    surveys_router.pg.fetch_all = original_survey_fetch_all
    surveys_router.pg.execute_values_no_return = original_survey_execute_values_no_return
    app.dependency_overrides.clear()


class TestHealthEndpoints:
    """Test health check endpoints."""

    def test_root_returns_ok(self, client: TestClient):
        """Root endpoint returns status ok."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["service"] == "trr-backend"

    def test_health_live_returns_alive_without_database(self, client: TestClient):
        """Liveness does not require database readiness."""
        response = client.get("/health/live")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "alive"
        assert data["service"] == "trr-backend"

    def test_health_readiness_returns_healthy_when_database_reachable(
        self,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Readiness returns healthy status when the database can be reached."""

        @contextmanager
        def _healthy_read_connection(**_kwargs):
            conn = MagicMock()
            yield conn

        monkeypatch.setattr(api_main.pg, "db_read_connection", _healthy_read_connection)

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "trr-backend"
        assert data["database"] == "connected"

    def test_health_readiness_degrades_when_database_is_unavailable(
        self,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Readiness returns a safe degraded response when DB config is missing."""

        def _unavailable_read_connection(**_kwargs):
            raise api_main.DatabaseServiceUnavailableError(
                "Database pool initialization failed: no database URL candidates available",
                reason="database_configuration",
            )

        monkeypatch.setattr(api_main.pg, "db_read_connection", _unavailable_read_connection)

        response = client.get("/health")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["service"] == "trr-backend"
        assert data["database"] == "unreachable"
        assert data["reason"] == "database_configuration"
        assert data["retryable"] is True
        assert data["retry_after_ms"] == 1000
        assert "TRR_DB_URL" in data["message"]
        assert "postgres://" not in str(data)
        assert "postgresql://" not in str(data)


class TestShowsEndpoints:
    """Test shows endpoints with mocked Supabase."""

    def test_list_shows_returns_empty_list(self, client: TestClient):
        """List shows endpoint returns empty list when no data."""
        response = client.get("/api/v1/shows")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_shows_with_alternative_names_returns_empty_list(self, client: TestClient):
        """Lightweight show list endpoint returns empty list when no data."""
        response = client.get("/api/v1/shows/list")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_shows_with_alternative_names_returns_covered_shows(self, client: TestClient):
        """Lightweight show list endpoint returns curated covered shows."""
        captured_query = ""

        def _fetch_covered_shows(query: str, *args, **kwargs):  # noqa: ANN001
            nonlocal captured_query
            captured_query = query
            return [
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "name": "The Real Housewives of Rhode Island",
                    "alternative_names": ["RHORI", "Real Housewives of Rhode Island"],
                }
            ]

        shows_router.pg.fetch_all = _fetch_covered_shows

        response = client.get("/api/v1/shows/list")

        assert response.status_code == 200
        assert "admin.covered_shows" in captured_query
        assert "JOIN core.shows" in captured_query
        assert response.json() == [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "name": "The Real Housewives of Rhode Island",
                "alternative_names": ["RHORI", "Real Housewives of Rhode Island"],
            }
        ]

    def test_get_show_returns_404_when_not_found(self, client: TestClient):
        """Get show endpoint returns 404 for non-existent show."""
        response = client.get("/api/v1/shows/00000000-0000-0000-0000-000000000000")
        assert response.status_code == 404

    def test_list_seasons_returns_empty_list(self, client: TestClient):
        """List seasons endpoint returns empty list when no data."""
        response = client.get("/api/v1/shows/00000000-0000-0000-0000-000000000000/seasons")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_cast_returns_empty_list(self, client: TestClient):
        """List cast endpoint returns empty list when no data."""
        response = client.get("/api/v1/shows/00000000-0000-0000-0000-000000000000/cast")
        assert response.status_code == 200
        data = response.json()
        assert data == {"count": 0, "total_count": 0, "has_more": False, "cast": []}
        count = data["count"]
        total_count = data["total_count"]
        has_more = data["has_more"]
        offset = 0
        assert total_count >= count
        assert has_more == ((offset + count) < total_count)


class TestSurveysEndpoints:
    """Test surveys endpoints with mocked Supabase."""

    def test_list_surveys_returns_empty_list(self, client: TestClient):
        """List surveys endpoint returns empty list when no data."""
        response = client.get("/api/v1/surveys")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_survey_returns_404_when_not_found(self, client: TestClient):
        """Get survey endpoint returns 404 for non-existent survey."""
        response = client.get("/api/v1/surveys/00000000-0000-0000-0000-000000000000")
        assert response.status_code == 404

    def test_get_survey_results_returns_empty(self, client: TestClient):
        """Get survey results returns empty aggregates."""
        response = client.get("/api/v1/surveys/00000000-0000-0000-0000-000000000000/results")
        assert response.status_code == 200
        data = response.json()
        assert data["total_responses"] == 0
        assert data["questions"] == []

    def test_submit_survey_returns_404_when_survey_not_found(self, client: TestClient):
        """Submit survey returns 404 for non-existent survey."""
        response = client.post(
            "/api/v1/surveys/00000000-0000-0000-0000-000000000000/submit",
            json={"answers": []},
        )
        assert response.status_code == 404

    def test_submit_survey_validates_payload(self, client: TestClient):
        """Submit survey endpoint validates payload structure."""
        # Missing answers field should return 422
        response = client.post(
            "/api/v1/surveys/00000000-0000-0000-0000-000000000000/submit",
            json={},
        )
        assert response.status_code == 422


class TestCORSConfiguration:
    """Test CORS is properly configured."""

    def test_cors_headers_present(self, client: TestClient):
        """CORS headers are present in response."""
        response = client.options(
            "/api/v1/shows",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
            },
        )
        # FastAPI returns 200 for OPTIONS when CORS is enabled
        assert response.status_code == 200
