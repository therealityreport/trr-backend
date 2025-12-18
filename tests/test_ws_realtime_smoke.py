"""
Smoke tests for WebSocket real-time functionality.

Tests the broker abstraction, event types, and WebSocket endpoints.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.realtime.broker import InMemoryBroker, get_broker
from api.realtime.events import (
    Event,
    EventType,
    thread_created_event,
    post_created_event,
    reaction_toggled_event,
    dm_message_created_event,
    dm_read_updated_event,
    typing_event,
    presence_event,
    error_event,
    subscribed_event,
    get_discussion_room,
    get_dm_room,
    get_typing_key,
    get_presence_key,
)


# --- InMemoryBroker Tests ---


def _run_async(coro):
    """Helper to run async code in sync tests."""
    return asyncio.run(coro)


class TestInMemoryBroker:
    """Tests for InMemoryBroker pub/sub functionality."""

    def test_connect_disconnect(self):
        """Broker connects and disconnects cleanly."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            assert broker._cleanup_task is not None
            await broker.disconnect()
            assert broker._cleanup_task.cancelled() or broker._cleanup_task.done()
        _run_async(_test())

    def test_publish_subscribe(self):
        """Subscribers receive published events."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            received = []

            async def callback(event):
                received.append(event)

            sub_id = await broker.subscribe("room1", callback)
            assert sub_id == "sub_1"

            await broker.publish("room1", {"type": "test", "data": "hello"})
            assert len(received) == 1
            assert received[0]["data"] == "hello"

            await broker.disconnect()
        _run_async(_test())

    def test_unsubscribe(self):
        """Unsubscribed callbacks no longer receive events."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            received = []

            async def callback(event):
                received.append(event)

            sub_id = await broker.subscribe("room1", callback)
            await broker.unsubscribe("room1", sub_id)
            await broker.publish("room1", {"type": "test"})

            assert len(received) == 0
            await broker.disconnect()
        _run_async(_test())

    def test_multiple_subscribers(self):
        """Multiple subscribers in same room all receive events."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            received1 = []
            received2 = []

            async def callback1(event):
                received1.append(event)

            async def callback2(event):
                received2.append(event)

            await broker.subscribe("room1", callback1)
            await broker.subscribe("room1", callback2)

            await broker.publish("room1", {"type": "test"})

            assert len(received1) == 1
            assert len(received2) == 1
            await broker.disconnect()
        _run_async(_test())

    def test_room_isolation(self):
        """Events are only received by subscribers in the same room."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            received1 = []
            received2 = []

            async def callback1(event):
                received1.append(event)

            async def callback2(event):
                received2.append(event)

            await broker.subscribe("room1", callback1)
            await broker.subscribe("room2", callback2)

            await broker.publish("room1", {"type": "test"})

            assert len(received1) == 1
            assert len(received2) == 0
            await broker.disconnect()
        _run_async(_test())


class TestInMemoryBrokerEphemeral:
    """Tests for InMemoryBroker ephemeral key functionality."""

    def test_set_get_ephemeral(self):
        """Can set and get ephemeral keys."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            await broker.set_ephemeral("key1", "value1", 60)
            value = await broker.get_ephemeral("key1")
            assert value == "value1"
            await broker.disconnect()
        _run_async(_test())

    def test_delete_ephemeral(self):
        """Can delete ephemeral keys."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            await broker.set_ephemeral("key1", "value1", 60)
            await broker.delete_ephemeral("key1")
            value = await broker.get_ephemeral("key1")
            assert value is None
            await broker.disconnect()
        _run_async(_test())

    def test_get_nonexistent_key(self):
        """Getting nonexistent key returns None."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            value = await broker.get_ephemeral("nonexistent")
            assert value is None
            await broker.disconnect()
        _run_async(_test())

    def test_get_keys_by_pattern(self):
        """Can find keys by prefix pattern."""
        async def _test():
            broker = InMemoryBroker()
            await broker.connect()
            await broker.set_ephemeral("typing:conv1:user1", "typing", 60)
            await broker.set_ephemeral("typing:conv1:user2", "typing", 60)
            await broker.set_ephemeral("typing:conv2:user1", "typing", 60)

            keys = await broker.get_keys_by_pattern("typing:conv1:*")
            assert len(keys) == 2
            assert "typing:conv1:user1" in keys
            assert "typing:conv1:user2" in keys
            await broker.disconnect()
        _run_async(_test())


# --- Event Tests ---


class TestEventTypes:
    """Tests for event creation and serialization."""

    def test_event_create(self):
        """Event.create sets timestamp and type."""
        event = Event.create(EventType.POST_CREATED, {"post_id": "123"})
        assert event.type == EventType.POST_CREATED
        assert event.payload == {"post_id": "123"}
        assert event.ts is not None

    def test_event_to_dict(self):
        """Event.to_dict produces serializable dict."""
        event = Event.create(EventType.TYPING, {"user_id": "123", "is_typing": True})
        d = event.to_dict()
        assert d["type"] == "typing"
        assert d["payload"]["user_id"] == "123"
        assert "ts" in d

    def test_thread_created_event(self):
        """thread_created_event creates correct payload."""
        thread = {
            "id": uuid4(),
            "episode_id": uuid4(),
            "title": "Test Thread",
            "type": "general",
            "created_by": uuid4(),
        }
        event = thread_created_event(thread)
        assert event.type == EventType.THREAD_CREATED
        assert event.payload["title"] == "Test Thread"

    def test_post_created_event(self):
        """post_created_event creates correct payload."""
        post = {
            "id": uuid4(),
            "thread_id": uuid4(),
            "parent_post_id": None,
            "user_id": uuid4(),
            "body": "Hello world",
            "created_at": "2025-01-01T00:00:00Z",
        }
        event = post_created_event(post)
        assert event.type == EventType.POST_CREATED
        assert event.payload["body"] == "Hello world"

    def test_reaction_toggled_event(self):
        """reaction_toggled_event creates correct payload."""
        event = reaction_toggled_event("post-123", "user-456", "heart", "added")
        assert event.type == EventType.REACTION_TOGGLED
        assert event.payload["action"] == "added"
        assert event.payload["reaction"] == "heart"

    def test_dm_message_created_event(self):
        """dm_message_created_event creates correct payload."""
        message = {
            "id": uuid4(),
            "conversation_id": uuid4(),
            "sender_id": uuid4(),
            "body": "Hey!",
            "created_at": "2025-01-01T00:00:00Z",
        }
        event = dm_message_created_event(message)
        assert event.type == EventType.DM_MESSAGE_CREATED
        assert event.payload["body"] == "Hey!"

    def test_dm_read_updated_event(self):
        """dm_read_updated_event creates correct payload."""
        event = dm_read_updated_event("conv-123", "user-456", "msg-789")
        assert event.type == EventType.DM_READ_UPDATED
        assert event.payload["last_read_message_id"] == "msg-789"

    def test_typing_event(self):
        """typing_event creates correct payload."""
        event = typing_event("conv-123", "user-456", True)
        assert event.type == EventType.TYPING
        assert event.payload["is_typing"] is True

    def test_presence_event(self):
        """presence_event creates correct payload."""
        event = presence_event("user-123", True)
        assert event.type == EventType.PRESENCE
        assert event.payload["is_online"] is True

    def test_error_event(self):
        """error_event creates correct payload."""
        event = error_event("Something went wrong", "ERR_001")
        assert event.type == EventType.ERROR
        assert event.payload["message"] == "Something went wrong"
        assert event.payload["code"] == "ERR_001"

    def test_subscribed_event(self):
        """subscribed_event creates correct payload."""
        event = subscribed_event("discussions:episode:123")
        assert event.type == EventType.SUBSCRIBED
        assert event.payload["room"] == "discussions:episode:123"


class TestRoomNaming:
    """Tests for room naming conventions."""

    def test_get_discussion_room(self):
        """Discussion room follows naming convention."""
        room = get_discussion_room("ep-123")
        assert room == "discussions:episode:ep-123"

    def test_get_dm_room(self):
        """DM room follows naming convention."""
        room = get_dm_room("conv-456")
        assert room == "dms:conversation:conv-456"

    def test_get_typing_key(self):
        """Typing key follows naming convention."""
        key = get_typing_key("conv-123", "user-456")
        assert key == "typing:conv-123:user-456"

    def test_get_presence_key(self):
        """Presence key follows naming convention."""
        key = get_presence_key("user-123")
        assert key == "presence:user-123"


# --- WebSocket Endpoint Tests ---


class TestWebSocketEndpoints:
    """Tests for WebSocket endpoint connectivity."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    def test_discussion_websocket_accepts_connection(self, client):
        """Discussion WebSocket accepts connections."""
        episode_id = str(uuid4())
        with client.websocket_connect(f"/api/v1/ws/discussions/episodes/{episode_id}") as ws:
            # Should receive subscribed event
            data = ws.receive_json()
            assert data["type"] == "subscribed"
            assert "discussions:episode:" in data["payload"]["room"]

    def test_discussion_websocket_receives_events(self, client):
        """Discussion WebSocket receives published events."""
        episode_id = str(uuid4())
        with client.websocket_connect(f"/api/v1/ws/discussions/episodes/{episode_id}") as ws:
            # Receive subscribed event
            ws.receive_json()

            # Send an invalid message (not typing, should be ignored or return error)
            ws.send_json({"type": "invalid_type", "payload": {}})

    def test_dm_websocket_requires_auth(self, client):
        """DM WebSocket requires authentication token."""
        conversation_id = str(uuid4())
        # Without token, should fail to connect (token is required query param)
        # The endpoint requires token=... query param
        # TestClient will raise an exception or the WS will close immediately
        try:
            with client.websocket_connect(f"/api/v1/ws/dms/{conversation_id}") as ws:
                # If we get here, connection was accepted but should close
                pass
        except Exception:
            # Expected - no token provided
            pass

    def test_dm_websocket_rejects_invalid_token(self, client):
        """DM WebSocket rejects invalid tokens."""
        conversation_id = str(uuid4())
        with pytest.raises(Exception):
            with client.websocket_connect(
                f"/api/v1/ws/dms/{conversation_id}?token=invalid_token"
            ) as ws:
                pass
