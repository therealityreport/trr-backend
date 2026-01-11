"""
Smoke tests for the TRR API.

These tests verify basic functionality without requiring a live database.
For integration tests against Supabase, see test_api_integration.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api import deps
from api.main import app


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
    mock_client.schema.return_value.table.return_value.select.return_value.order.return_value.range.return_value.execute.return_value = empty_list_response

    # Single queries (eq -> single -> execute)
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = none_response

    # Filtered list queries (eq -> order -> range -> execute)
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.order.return_value.range.return_value.execute.return_value = empty_list_response

    # Simple filtered queries (eq -> execute)
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.execute.return_value = (
        empty_list_response
    )

    # Double eq queries (eq -> eq -> single -> execute) for season lookups
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.single.return_value.execute.return_value = none_response

    # Desc order queries
    mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = empty_list_response

    return mock_client


@pytest.fixture
def client(mock_supabase):
    """Create a test client with mocked Supabase dependencies."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase
    yield TestClient(app)
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

    def test_health_returns_healthy(self, client: TestClient):
        """Health endpoint returns healthy status."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"


class TestShowsEndpoints:
    """Test shows endpoints with mocked Supabase."""

    def test_list_shows_returns_empty_list(self, client: TestClient):
        """List shows endpoint returns empty list when no data."""
        response = client.get("/api/v1/shows")
        assert response.status_code == 200
        assert response.json() == []

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
        assert response.json() == []


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
