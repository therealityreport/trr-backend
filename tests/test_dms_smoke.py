"""
Smoke tests for the Direct Messages API.

These tests verify basic functionality with mocked Supabase.
All DM endpoints require authentication.
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
    """
    mock = MagicMock()

    if return_data is None:
        return_data = []

    if single_data is None:
        single_data = return_data[0] if return_data else None

    list_response = MagicMock()
    list_response.data = return_data
    list_response.error = None

    single_response = MagicMock()
    single_response.data = single_data
    single_response.error = None

    single_mock = MagicMock()
    single_mock.execute.return_value = single_response
    single_mock.eq.return_value = single_mock
    single_mock.neq.return_value = single_mock

    mock.execute.return_value = list_response

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
    mock.rpc.return_value = mock
    mock.single.return_value = single_mock

    return mock


# --- Test data ---

MOCK_USER = {
    "id": "11111111-1111-1111-1111-111111111111",
    "email": "test@example.com",
    "role": "authenticated",
    "token": "mock-jwt-token",
}

MOCK_OTHER_USER_ID = "22222222-2222-2222-2222-222222222222"

MOCK_CONVERSATION = {
    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "is_group": False,
    "created_at": "2025-01-01T00:00:00Z",
    "last_message_at": None,
}

MOCK_MESSAGE = {
    "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "conversation_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "sender_id": "11111111-1111-1111-1111-111111111111",
    "body": "Hello!",
    "created_at": "2025-01-01T00:00:00Z",
}

MOCK_READ_RECEIPT = {
    "conversation_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "user_id": "11111111-1111-1111-1111-111111111111",
    "last_read_message_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "last_read_at": "2025-01-01T00:00:00Z",
}


# --- Fixtures ---

@pytest.fixture
def mock_supabase():
    """Create a mock Supabase client."""
    return create_chainable_mock([])


@pytest.fixture
def client(mock_supabase):
    """Test client with mocked Supabase (no auth)."""
    app.dependency_overrides[deps.get_supabase_client] = lambda: mock_supabase
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: mock_supabase
    yield TestClient(app)
    app.dependency_overrides.clear()


# --- Auth requirement tests ---

class TestAuthRequired:
    """Test that all DM endpoints require authentication."""

    def test_create_conversation_requires_auth(self, client: TestClient):
        """Create conversation returns 401 without auth."""
        response = client.post(
            "/api/v1/dms",
            json={"other_user_id": MOCK_OTHER_USER_ID},
        )
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_list_conversations_requires_auth(self, client: TestClient):
        """List conversations returns 401 without auth."""
        response = client.get("/api/v1/dms")
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_list_messages_requires_auth(self, client: TestClient):
        """List messages returns 401 without auth."""
        response = client.get(f"/api/v1/dms/{MOCK_CONVERSATION['id']}/messages")
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_send_message_requires_auth(self, client: TestClient):
        """Send message returns 401 without auth."""
        response = client.post(
            f"/api/v1/dms/{MOCK_CONVERSATION['id']}/messages",
            json={"body": "Hello!"},
        )
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_update_read_receipt_requires_auth(self, client: TestClient):
        """Update read receipt returns 401 without auth."""
        response = client.post(
            f"/api/v1/dms/{MOCK_CONVERSATION['id']}/read",
            json={"last_read_message_id": MOCK_MESSAGE["id"]},
        )
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]


# --- Validation tests ---

class TestValidation:
    """Test request validation (with auth, since auth check happens first)."""

    def test_create_conversation_requires_other_user_id(self):
        """Create conversation requires other_user_id in payload."""
        mock_db = create_chainable_mock([])
        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.post("/api/v1/dms", json={})
            assert response.status_code == 422

        app.dependency_overrides.clear()

    def test_send_message_requires_body(self):
        """Send message requires body in payload."""
        mock_db = create_chainable_mock([])
        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.post(
                f"/api/v1/dms/{MOCK_CONVERSATION['id']}/messages",
                json={},
            )
            assert response.status_code == 422

        app.dependency_overrides.clear()

    def test_update_read_receipt_requires_message_id(self):
        """Update read receipt requires last_read_message_id in payload."""
        mock_db = create_chainable_mock([])
        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.post(
                f"/api/v1/dms/{MOCK_CONVERSATION['id']}/read",
                json={},
            )
            assert response.status_code == 422

        app.dependency_overrides.clear()


# --- Success path tests ---

class TestCreateConversation:
    """Test conversation creation."""

    def test_create_conversation_success(self):
        """Create conversation succeeds with valid auth."""
        # Build mock with properly typed responses
        mock_db = MagicMock()

        # RPC response returns conversation ID
        rpc_response = MagicMock()
        rpc_response.data = MOCK_CONVERSATION["id"]
        rpc_response.error = None
        mock_db.rpc.return_value.execute.return_value = rpc_response

        # Conversation single() query response
        conv_response = MagicMock()
        conv_response.data = MOCK_CONVERSATION.copy()
        conv_response.error = None

        # Members list query response
        mock_member = {
            "user_id": MOCK_USER["id"],
            "role": "member",
            "joined_at": "2025-01-01T00:00:00Z",
        }
        members_response = MagicMock()
        members_response.data = [mock_member]
        members_response.error = None

        # Set up the chained mock
        single_mock = MagicMock()
        single_mock.execute.return_value = conv_response

        chain_mock = MagicMock()
        chain_mock.single.return_value = single_mock
        chain_mock.execute.return_value = members_response

        # All chainable methods return chain_mock
        mock_db.schema.return_value = chain_mock
        chain_mock.table.return_value = chain_mock
        chain_mock.select.return_value = chain_mock
        chain_mock.eq.return_value = chain_mock

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.post(
                "/api/v1/dms",
                json={"other_user_id": MOCK_OTHER_USER_ID},
            )
            assert response.status_code == 200
            data = response.json()
            assert "id" in data
            assert data["is_group"] is False

        app.dependency_overrides.clear()


class TestListConversations:
    """Test listing conversations."""

    def test_list_conversations_success(self):
        """List conversations succeeds with valid auth."""
        mock_db = create_chainable_mock([MOCK_CONVERSATION])

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.get("/api/v1/dms")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)

        app.dependency_overrides.clear()


class TestMessages:
    """Test message endpoints."""

    def test_list_messages_success(self):
        """List messages succeeds with valid auth."""
        mock_db = create_chainable_mock([MOCK_MESSAGE], single_data=MOCK_CONVERSATION)

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.get(f"/api/v1/dms/{MOCK_CONVERSATION['id']}/messages")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)

        app.dependency_overrides.clear()

    def test_send_message_success(self):
        """Send message succeeds with valid auth."""
        mock_db = create_chainable_mock([MOCK_MESSAGE], single_data=MOCK_MESSAGE)

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.post(
                f"/api/v1/dms/{MOCK_CONVERSATION['id']}/messages",
                json={"body": "Hello!"},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["body"] == "Hello!"
            assert data["sender_id"] == MOCK_USER["id"]

        app.dependency_overrides.clear()

    def test_list_messages_accepts_cursor(self):
        """List messages accepts cursor parameter."""
        mock_db = create_chainable_mock([])

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.get(
                f"/api/v1/dms/{MOCK_CONVERSATION['id']}/messages",
                params={"cursor": "2025-01-01T00:00:00Z"},
            )
            # Should not fail due to cursor parameter
            assert response.status_code == 200

        app.dependency_overrides.clear()


class TestReadReceipts:
    """Test read receipt endpoints."""

    def test_update_read_receipt_success(self):
        """Update read receipt succeeds with valid auth."""
        # Mock that returns message for verification, then upsert result
        mock_db = create_chainable_mock([MOCK_READ_RECEIPT], single_data=MOCK_MESSAGE)

        app.dependency_overrides[deps.get_supabase_client] = lambda: mock_db
        app.dependency_overrides[auth.require_user] = lambda: MOCK_USER

        with patch("api.routers.dms.get_user_supabase_client", return_value=mock_db):
            client = TestClient(app)
            response = client.post(
                f"/api/v1/dms/{MOCK_CONVERSATION['id']}/read",
                json={"last_read_message_id": MOCK_MESSAGE["id"]},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["conversation_id"] == MOCK_CONVERSATION["id"]
            assert data["user_id"] == MOCK_USER["id"]

        app.dependency_overrides.clear()


# --- Route registration tests ---

class TestRouteRegistration:
    """Test that all DM routes are registered."""

    def test_routes_exist(self, client: TestClient):
        """Verify all DM endpoints are registered (not 405)."""
        endpoints = [
            "/api/v1/dms",
            f"/api/v1/dms/{MOCK_CONVERSATION['id']}/messages",
            f"/api/v1/dms/{MOCK_CONVERSATION['id']}/read",
        ]

        for path in endpoints:
            response = client.options(
                path,
                headers={
                    "Origin": "http://localhost:3000",
                    "Access-Control-Request-Method": "GET",
                },
            )
            assert response.status_code == 200, f"OPTIONS {path} returned {response.status_code}"
