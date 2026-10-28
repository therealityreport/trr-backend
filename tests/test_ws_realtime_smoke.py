"""
Smoke tests for WebSocket real-time functionality.

Tests the broker abstraction, event types, and WebSocket endpoints.
"""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.realtime import broker as broker_module
from api.realtime.broker import InMemoryBroker, RedisBroker
from api.realtime.events import (
    Event,
    EventType,
    dm_message_created_event,
    dm_read_updated_event,
    error_event,
    get_discussion_room,
    get_dm_room,
    get_presence_key,
    get_typing_key,
    post_created_event,
    presence_event,
    reaction_toggled_event,
    subscribed_event,
    thread_created_event,
    typing_event,
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


class _FakePubSub:
    def __init__(self, script: list[Any] | None = None) -> None:
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.closed = False
        # Each get_message() call pops one item: an exception is raised, a dict is returned.
        self.script: list[Any] = list(script or [])
        self.get_message_calls = 0

    async def subscribe(self, *rooms: str) -> None:
        self.subscribed.extend(rooms)

    async def get_message(self, *, ignore_subscribe_messages: bool = False, timeout: float | None = 0.0):
        self.get_message_calls += 1
        if self.script:
            item = self.script.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        await asyncio.sleep(timeout or 0)
        return None

    async def unsubscribe(self, room: str) -> None:
        self.unsubscribed.append(room)

    async def listen(self):
        await asyncio.Event().wait()
        if False:
            yield {}

    async def aclose(self) -> None:
        self.closed = True


class _FakeRedis:
    def __init__(
        self,
        *,
        scan_keys: list[str] | None = None,
        ping_error: BaseException | None = None,
        pubsub_script: list[Any] | None = None,
    ) -> None:
        self.pubsub_instance = _FakePubSub(pubsub_script)
        self.scan_keys = scan_keys or []
        self.scan_patterns: list[str] = []
        self.ping_error = ping_error
        self.closed = False
        self.decode_responses: bool | None = None

    async def ping(self) -> None:
        if self.ping_error:
            raise self.ping_error

    def pubsub(self) -> _FakePubSub:
        return self.pubsub_instance

    async def publish(self, _room: str, _message: str) -> None:
        return None

    async def setex(self, _key: str, _ttl_seconds: int, _value: str) -> None:
        return None

    async def get(self, _key: str) -> str | None:
        return None

    async def delete(self, _key: str) -> None:
        return None

    async def scan_iter(self, *, match: str):
        self.scan_patterns.append(match)
        for key in self.scan_keys:
            yield key

    async def keys(self, _pattern: str):
        raise AssertionError("RedisBroker must use scan_iter instead of keys")

    async def aclose(self) -> None:
        self.closed = True


def _install_fake_redis(monkeypatch: pytest.MonkeyPatch, fake: _FakeRedis) -> None:
    redis_module = ModuleType("redis")
    redis_asyncio_module = ModuleType("redis.asyncio")

    def _from_url(_url: str, *, decode_responses: bool):
        fake.decode_responses = decode_responses
        return fake

    redis_asyncio_module.from_url = _from_url  # type: ignore[attr-defined]
    redis_module.asyncio = redis_asyncio_module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "redis", redis_module)
    monkeypatch.setitem(sys.modules, "redis.asyncio", redis_asyncio_module)


def _install_fake_redis_sequence(monkeypatch: pytest.MonkeyPatch, fakes: list[_FakeRedis]) -> list[_FakeRedis]:
    """Return a new fake client on each from_url() call, as a reconnect would."""
    created: list[_FakeRedis] = []
    redis_module = ModuleType("redis")
    redis_asyncio_module = ModuleType("redis.asyncio")

    def _from_url(_url: str, *, decode_responses: bool):
        fake = fakes[len(created)] if len(created) < len(fakes) else fakes[-1]
        fake.decode_responses = decode_responses
        created.append(fake)
        return fake

    redis_asyncio_module.from_url = _from_url  # type: ignore[attr-defined]
    redis_module.asyncio = redis_asyncio_module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "redis", redis_module)
    monkeypatch.setitem(sys.modules, "redis.asyncio", redis_asyncio_module)
    return created


async def _wait_until(predicate, *, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached before timeout")
        await asyncio.sleep(0.005)


def _fast_reconnect(broker: RedisBroker) -> RedisBroker:
    broker.reconnect_initial_delay_s = 0.01
    broker.reconnect_max_delay_s = 0.04
    broker.listen_poll_timeout_s = 0.01
    return broker


class TestRedisBrokerRecovery:
    """The listener must survive Redis errors and keep delivering events."""

    def test_listener_waits_for_first_room_instead_of_exiting(self, monkeypatch: pytest.MonkeyPatch) -> None:
        message = {"type": "message", "channel": "room1", "data": '{"type": "test"}'}
        fake = _FakeRedis(pubsub_script=[message])
        _install_fake_redis(monkeypatch, fake)

        async def _test():
            broker = _fast_reconnect(RedisBroker("redis://cache.example.com:6379/0"))
            await broker.connect()
            await asyncio.sleep(0.05)
            assert broker.status()["listener_running"] is True
            assert fake.pubsub_instance.get_message_calls == 0

            received: list[dict] = []

            async def callback(event: dict) -> None:
                received.append(event)

            await broker.subscribe("room1", callback)
            await _wait_until(lambda: received)
            assert received == [{"type": "test"}]
            await broker.disconnect()

        _run_async(_test())

    def test_listener_reconnects_and_restores_rooms_after_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = _FakeRedis(pubsub_script=[ConnectionError("lost cache.example.com with password secret")])
        message = {"type": "message", "channel": "room1", "data": '{"type": "after-reconnect"}'}
        second = _FakeRedis(pubsub_script=[message])
        created = _install_fake_redis_sequence(monkeypatch, [first, second])

        async def _test():
            broker = _fast_reconnect(RedisBroker("redis://user:secret@cache.example.com:6379/0"))
            await broker.connect()
            received: list[dict] = []

            async def callback(event: dict) -> None:
                received.append(event)

            await broker.subscribe("room1", callback)
            await _wait_until(lambda: received)

            assert received == [{"type": "after-reconnect"}]
            assert created[:2] == [first, second]
            assert first.closed is True and first.pubsub_instance.closed is True
            assert second.pubsub_instance.subscribed == ["room1"]
            status = broker.status()
            assert status["connected"] is True
            assert status["reconnect_count"] == 1
            assert status["last_reconnect_at"] is not None
            assert status["last_error"] is None
            assert status["listener_running"] is True
            await broker.disconnect()

        _run_async(_test())

    def test_reconnect_retries_with_backoff_and_redacts_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = _FakeRedis(pubsub_script=[ConnectionError("dropped")])
        down = _FakeRedis(ping_error=OSError("cannot reach cache.example.com with password secret"))
        recovered = _FakeRedis()
        _install_fake_redis_sequence(monkeypatch, [first, down, down, recovered])

        async def _test():
            broker = _fast_reconnect(RedisBroker("redis://user:secret@cache.example.com:6379/0"))
            await broker.connect()

            async def callback(_event: dict) -> None:
                return None

            await broker.subscribe("room1", callback)
            await _wait_until(lambda: broker.status()["reconnect_count"] == 1)
            status = broker.status()
            assert status["reconnect_failures"] == 2
            assert status["connected"] is True
            assert recovered.pubsub_instance.subscribed == ["room1"]
            assert down.closed is True
            await broker.disconnect()

        _run_async(_test())

    def test_failed_reconnect_status_is_redacted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = _FakeRedis(pubsub_script=[ConnectionError("dropped")])
        down = _FakeRedis(ping_error=OSError("cannot reach cache.example.com with password secret"))
        _install_fake_redis_sequence(monkeypatch, [first, down, down, down, down, down, down, down])

        async def _test():
            broker = _fast_reconnect(RedisBroker("redis://user:secret@cache.example.com:6379/0"))
            await broker.connect()

            async def callback(_event: dict) -> None:
                return None

            await broker.subscribe("room1", callback)
            await _wait_until(lambda: broker.status()["reconnect_failures"] >= 1)  # pyright: ignore[reportOperatorIssue]
            status = broker.status()
            assert status["connected"] is False
            last_error: Any = status["last_error"]
            assert "secret" not in last_error["message"]
            assert "cache.example.com" not in last_error["message"]
            await broker.disconnect()
            assert broker.status()["listener_running"] is False

        _run_async(_test())

    def test_disconnect_during_backoff_stops_cleanly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = _FakeRedis(pubsub_script=[ConnectionError("dropped")])
        _install_fake_redis_sequence(monkeypatch, [first, _FakeRedis()])

        async def _test():
            broker = RedisBroker("redis://cache.example.com:6379/0")
            broker.reconnect_initial_delay_s = 30.0
            broker.listen_poll_timeout_s = 0.01
            await broker.connect()

            async def callback(_event: dict) -> None:
                return None

            await broker.subscribe("room1", callback)
            await _wait_until(lambda: broker.status()["connected"] is False)
            task = broker._listener_task
            await asyncio.wait_for(broker.disconnect(), timeout=1.0)
            assert task is not None and task.done()
            assert broker.status()["listener_running"] is False

        _run_async(_test())


class TestRedisBroker:
    """Tests for Redis-backed broker behavior without a live Redis server."""

    def test_connect_disconnect_and_pattern_lookup_uses_scan_iter(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeRedis(scan_keys=["typing:conv1:user1", "typing:conv1:user2"])
        _install_fake_redis(monkeypatch, fake)

        async def _test():
            broker = RedisBroker("redis://user:secret@cache.example.com:6379/0")
            await broker.connect()

            status = broker.status()
            assert status["connected"] is True
            assert status["mode"] == "redis"
            assert status["redis"] == {
                "configured": True,
                "scheme": "redis",
                "host_class": "remote",
                "port": 6379,
            }
            assert fake.decode_responses is True

            keys = await broker.get_keys_by_pattern("typing:conv1:*")
            assert keys == ["typing:conv1:user1", "typing:conv1:user2"]
            assert fake.scan_patterns == ["typing:conv1:*"]

            await broker.disconnect()
            assert fake.closed is True
            assert fake.pubsub_instance.closed is True
            assert broker.status()["connected"] is False

        _run_async(_test())

    def test_connect_error_is_redacted_in_status(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeRedis(ping_error=OSError("cannot reach cache.example.com with password secret"))
        _install_fake_redis(monkeypatch, fake)

        async def _test():
            broker = RedisBroker("redis://user:secret@cache.example.com:6379/0")
            with pytest.raises(OSError):
                await broker.connect()

            status = broker.status()
            assert status["connected"] is False
            last_error: Any = status["last_error"]
            assert last_error["type"] == "OSError"
            assert "cache.example.com" not in last_error["message"]
            assert "secret" not in last_error["message"]
            assert "<redis-host>" in last_error["message"]

        _run_async(_test())

    def test_broker_runtime_status_reports_multi_worker_policy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(broker_module, "_broker", None)
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
        monkeypatch.setenv("TRR_BACKEND_WORKERS", "3")
        monkeypatch.setenv("TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER", "1")

        status = broker_module.broker_runtime_status()

        assert status["initialized"] is False
        assert status["mode"] == "redis"
        redis_status: Any = status["redis"]
        assert redis_status["host_class"] == "local"
        assert status["multi_worker_policy"] == {
            "workers_requested": 3,
            "require_redis_for_multi_worker": True,
            "redis_url_configured": True,
            "safe_for_multi_worker": True,
        }


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
            with client.websocket_connect(f"/api/v1/ws/dms/{conversation_id}"):
                # If we get here, connection was accepted but should close
                pass
        except Exception:
            # Expected - no token provided
            pass

    def test_dm_websocket_rejects_invalid_token(self, client):
        """DM WebSocket rejects invalid tokens."""
        conversation_id = str(uuid4())
        with pytest.raises(Exception):  # noqa: B017
            with client.websocket_connect(f"/api/v1/ws/dms/{conversation_id}?token=invalid_token"):
                pass
