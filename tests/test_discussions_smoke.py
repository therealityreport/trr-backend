"""
Smoke tests for the discussions API (threads, posts, reactions).

These tests verify basic functionality with mocked Supabase.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api import deps, auth


def create_chainable_mock(return_data=None, single_data=None):
    """
    Create a deeply chainable mock that returns a response with data and no error.
    All method calls return the same mock, allowing any chain depth.

    Args:
        return_data: Data to return for list queries (default: [])
        single_data: Data to return for .single() queries (default: first item of return_data or None)
    """
    mock = MagicMock()

    # Normalize return_data to always be a list
    if return_data is None:
        return_data = []

    # For .single() queries, return single item (dict) or None
    if single_data is None:
        single_data = return_data[0] if return_data else None

    # Create two different responses
    list_response = MagicMock()
    list_response.data = return_data
    list_response.error = None

    single_response = MagicMock()
    single_response.data = single_data
    single_response.error = None

    # Create a separate mock for single() chains
    single_mock = MagicMock()
    single_mock.execute.return_value = single_response
    # Single mock also chains to itself for any additional methods
    single_mock.eq.return_value = single_mock
    single_mock.neq.return_value = single_mock

    # Default execute returns list response
    mock.execute.return_value = list_response

    # Make all other methods return self for chaining
    mock.schema.return_value = mock
    mock.table.return_value = mock
    mock.select.return_value = mock
    mock.insert.return_value = mock
    mock.update.return_value = mock
    mock.delete.return_value = mock
    mock.upsert.return_value = mock
    mock.eq.return_value = mock
    mock.neq.return_value = mock
    mock.gt.return_value = mock
    mock.gte.return_value = mock
    mock.lt.return_value = mock
    mock.lte.return_value = mock
    mock.is_.return_value = mock
    mock.in_.return_value = mock
    mock.order.return_value = mock
    mock.range.return_value = mock
    mock.limit.return_value = mock

    # .single() returns a mock that gives single_response on execute
    mock.single.return_value = single_mock

    return mock


# --- Test data ---

MOCK_USER = {
    "id": "11111111-1111-1111-1111-111111111111",
    "email": "test@example.com",
    "role": "authenticated",
    "token": "mock-jwt-token",
}

MOCK_THREAD = {
    "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "episode_id": "3d037712-54b6-4037-8109-1c69ab00448a",
    "title": "Episode 1 Live Discussion",
    "type": "episode_live",
    "created_by": "11111111-1111-1111-1111-111111111111",
    "is_locked": False,
    "created_at": "2025-01-01T00:00:00Z",
}

MOCK_POST = {
    "id": "b2c3d4e5-f6a7-8901-bcde-f23456789012",
    "thread_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "parent_post_id": None,
    "user_id": "11111111-1111-1111-1111-111111111111",
    "body": "Test post body",
    "created_at": "2025-01-01T00:00:00Z",
    "edited_at": None,
}

MOCK_EPISODE = {
    "id": "3d037712-54b6-4037-8109-1c69ab00448a",
}


# --- Fixtures ---

@pytest.fixture
def mock_supabase_with_threads():
    """Create a mock Supabase client that returns thread data."""
    return create_chainable_mock([MOCK_THREAD])


@pytest.fixture
def mock_supabase_with_posts():
    """Create a mock Supabase client that returns post data.

    For create_post endpoint, we need:
    - .single() to return thread data (thread exists check, is_locked check)
    - list queries to return post data
    """
    # Thread data for single() queries (thread exists, is_locked checks)
    thread_data = {
        "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "is_locked": False,
        "thread_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",  # For parent post check
    }
    return create_chainable_mock([MOCK_POST], single_data=thread_data)


@pytest.fixture
def mock_supabase_empty():
    """Create a mock Supabase client that returns empty results."""
    return create_chainable_mock([])


@pytest.fixture
def mock_supabase_single():
    """Create a mock Supabase client that returns single item data.

    - For .single() queries, returns the dict directly
    - For list/insert queries, returns [dict] so response.data[0] works
    """
    return create_chainable_mock([MOCK_THREAD])


@pytest.fixture
def mock_supabase_none():
    """Create a mock Supabase client that returns None (not found)."""
    return create_chainable_mock(None)


@pytest.fixture
def client_with_threads(mock_supabase_with_threads):
    """Test client with thread data."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase_with_threads
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase_with_threads
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def client_with_posts(mock_supabase_with_posts):
    """Test client with post data."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase_with_posts
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase_with_posts
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def client_empty(mock_supabase_empty):
    """Test client with empty results."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase_empty
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase_empty
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def client_single(mock_supabase_single):
    """Test client that returns single items."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase_single
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase_single
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def client_none(mock_supabase_none):
    """Test client that returns None (not found)."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase_none
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase_none
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def authenticated_client():
    """Test client with mocked authentication and Supabase."""
    # Mock Supabase to return appropriate data for all operations
    mock_db = create_chainable_mock([MOCK_THREAD], single_data=MOCK_EPISODE)

    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_db
    app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

    # Patch get_user_supabase_client to return our mock
    with patch("api.routers.discussions.get_user_supabase_client", return_value=mock_db):
        yield TestClient(app)

    app.dependency_overrides.clear()


# --- Thread tests ---

class TestListThreads:
    """Test listing episode threads."""

    def test_list_threads_returns_list(self, client_with_threads: TestClient):
        """List threads endpoint returns a list."""
        response = client_with_threads.get("/api/v1/episodes/3d037712-54b6-4037-8109-1c69ab00448a/threads")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_list_threads_empty(self, client_empty: TestClient):
        """List threads returns empty list when no threads exist."""
        response = client_empty.get("/api/v1/episodes/00000000-0000-0000-0000-000000000000/threads")
        assert response.status_code == 200
        assert response.json() == []


class TestCreateThread:
    """Test creating threads."""

    def test_create_thread_requires_auth(self, client_single: TestClient):
        """Create thread returns 401 without auth."""
        response = client_single.post(
            "/api/v1/episodes/3d037712-54b6-4037-8109-1c69ab00448a/threads",
            json={"title": "New Discussion", "type": "episode_live"},
        )
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_create_thread_success_with_auth(self, authenticated_client: TestClient):
        """Create thread succeeds with valid auth and payload."""
        response = authenticated_client.post(
            "/api/v1/episodes/3d037712-54b6-4037-8109-1c69ab00448a/threads",
            json={"title": "New Discussion", "type": "episode_live"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert data["title"] == "Episode 1 Live Discussion"

    def test_create_thread_invalid_type(self, authenticated_client: TestClient):
        """Create thread with invalid type returns 400."""
        response = authenticated_client.post(
            "/api/v1/episodes/3d037712-54b6-4037-8109-1c69ab00448a/threads",
            json={"title": "New Discussion", "type": "invalid_type"},
        )
        assert response.status_code == 400
        assert "Invalid thread type" in response.json()["detail"]


class TestGetThread:
    """Test getting a single thread."""

    def test_get_thread_success(self, client_single: TestClient):
        """Get thread returns thread details."""
        response = client_single.get("/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        assert response.status_code == 200
        data = response.json()
        assert "id" in data

    def test_get_thread_not_found(self, client_none: TestClient):
        """Get thread returns 404 for non-existent thread."""
        response = client_none.get("/api/v1/threads/00000000-0000-0000-0000-000000000000")
        assert response.status_code == 404


# --- Post tests ---

class TestListPosts:
    """Test listing posts in a thread."""

    def test_list_posts_returns_list(self, client_empty: TestClient):
        """List posts returns list (empty when no posts)."""
        response = client_empty.get("/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890/posts")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_list_posts_accepts_parent_filter(self, client_empty: TestClient):
        """List posts accepts parent_post_id filter."""
        response = client_empty.get(
            "/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890/posts",
            params={"parent_post_id": "b2c3d4e5-f6a7-8901-bcde-f23456789012"},
        )
        assert response.status_code == 200

    def test_list_posts_accepts_cursor(self, client_empty: TestClient):
        """List posts accepts cursor for pagination."""
        response = client_empty.get(
            "/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890/posts",
            params={"cursor": "2025-01-01T00:00:00Z"},
        )
        assert response.status_code == 200

    def test_list_posts_empty(self, client_empty: TestClient):
        """List posts returns empty list."""
        response = client_empty.get("/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890/posts")
        assert response.status_code == 200
        assert response.json() == []


class TestCreatePost:
    """Test creating posts."""

    def test_create_post_requires_auth(self, client_with_posts: TestClient):
        """Create post returns 401 without auth."""
        response = client_with_posts.post(
            "/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890/posts",
            json={"body": "Test post content"},
        )
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_create_post_success_with_auth(self):
        """Create post succeeds with valid auth and payload."""
        # Mock Supabase for this specific test
        thread_data = {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "is_locked": False,
        }
        mock_db = create_chainable_mock([MOCK_POST], single_data=thread_data)

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.discussions.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.post(
                "/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890/posts",
                json={"body": "Test post content"},
            )
            assert response.status_code == 200
            data = response.json()
            assert "id" in data
            assert data["body"] == "Test post body"

        app.dependency_overrides.clear()


# --- Reaction tests ---

class TestReactions:
    """Test reaction endpoints."""

    def test_toggle_reaction_requires_auth(self, client_single: TestClient):
        """Toggle reaction returns 401 without auth."""
        response = client_single.post(
            "/api/v1/posts/b2c3d4e5-f6a7-8901-bcde-f23456789012/reactions",
            json={"reaction": "upvote"},
        )
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_get_reactions_post_not_found(self, client_none: TestClient):
        """Get reactions returns 404 for non-existent post."""
        response = client_none.get("/api/v1/posts/00000000-0000-0000-0000-000000000000/reactions")
        assert response.status_code == 404

    def test_toggle_reaction_success_with_auth(self):
        """Toggle reaction succeeds with valid auth."""
        # For toggle_reaction, we need:
        # 1. Post lookup (single) -> returns post with thread_id
        # 2. Thread lookup (single) -> returns is_locked: False
        # 3. Check existing reactions (list) -> returns empty []
        # 4. Insert new reaction -> returns the reaction

        # Combined data for both post and thread lookups (mock returns same for all single() calls)
        single_data = {
            "id": "b2c3d4e5-f6a7-8901-bcde-f23456789012",
            "thread_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "is_locked": False,
        }

        # Use the helper mock - returns empty list for regular queries
        mock_db = create_chainable_mock(return_data=[], single_data=single_data)

        # User-scoped mock also needs proper setup
        user_mock = create_chainable_mock(return_data=[])

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.discussions.get_user_supabase_client", return_value=user_mock):
            client = TestClient(app)
            response = client.post(
                "/api/v1/posts/b2c3d4e5-f6a7-8901-bcde-f23456789012/reactions",
                json={"reaction": "upvote"},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["action"] == "added"
            assert data["reaction"] == "upvote"

        app.dependency_overrides.clear()

    def test_toggle_reaction_invalid_type(self):
        """Toggle reaction with invalid type returns 400."""
        mock_db = create_chainable_mock()

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        client = TestClient(app)
        response = client.post(
            "/api/v1/posts/b2c3d4e5-f6a7-8901-bcde-f23456789012/reactions",
            json={"reaction": "invalid_reaction"},
        )
        assert response.status_code == 400
        assert "Invalid reaction type" in response.json()["detail"]

        app.dependency_overrides.clear()


# --- Route registration tests ---

class TestRouteRegistration:
    """Test that all discussion routes are registered."""

    def test_routes_exist(self, client_empty: TestClient):
        """Verify all discussion endpoints are registered (not 405)."""
        # Test with OPTIONS requests to check route registration without triggering handlers
        endpoints = [
            "/api/v1/episodes/3d037712-54b6-4037-8109-1c69ab00448a/threads",
            "/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "/api/v1/threads/a1b2c3d4-e5f6-7890-abcd-ef1234567890/posts",
            "/api/v1/posts/b2c3d4e5-f6a7-8901-bcde-f23456789012/reactions",
        ]

        for path in endpoints:
            # OPTIONS requests verify route exists (used for CORS preflight)
            response = client_empty.options(
                path,
                headers={
                    "Origin": "http://localhost:3000",
                    "Access-Control-Request-Method": "GET",
                },
            )
            # FastAPI returns 200 for OPTIONS when route exists and CORS is enabled
            assert response.status_code == 200, f"OPTIONS {path} returned {response.status_code}"
