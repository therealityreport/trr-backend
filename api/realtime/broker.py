"""
Pub/sub broker abstraction for real-time events.

Uses Redis pub/sub when REDIS_URL is set, otherwise falls back
to in-process pub/sub (fine for local dev, not multi-instance).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_LOCAL_REDIS_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _positive_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        if raw:
            return max(int(raw), 1)
    except ValueError:
        return default
    return default


def _redis_url_snapshot(redis_url: str | None) -> dict[str, object]:
    if not redis_url:
        return {"configured": False}

    parsed = urlparse(redis_url)
    host = parsed.hostname or ""
    if not host:
        host_class = "unknown"
    elif host in _LOCAL_REDIS_HOSTS:
        host_class = "local"
    else:
        host_class = "remote"

    return {
        "configured": True,
        "scheme": parsed.scheme or "unknown",
        "host_class": host_class,
        "port": parsed.port,
    }


def _redact_redis_error(exc: BaseException, *, redis_url: str | None = None) -> dict[str, str]:
    message = str(exc).strip() or type(exc).__name__
    if redis_url:
        parsed = urlparse(redis_url)
        replacements = {redis_url: "<redis-url>"}
        if parsed.hostname:
            replacements[parsed.hostname] = "<redis-host>"
        if parsed.password:
            replacements[parsed.password] = "<redacted>"
        for needle, replacement in replacements.items():
            message = message.replace(needle, replacement)
    return {"type": type(exc).__name__, "message": message[:240]}


def _multi_worker_policy_snapshot() -> dict[str, object]:
    workers = _positive_int_env("TRR_BACKEND_WORKERS", 1)
    require_redis = _env_flag("TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER", True)
    redis_url_configured = bool((os.getenv("REDIS_URL") or "").strip())
    return {
        "workers_requested": workers,
        "require_redis_for_multi_worker": require_redis,
        "redis_url_configured": redis_url_configured,
        "safe_for_multi_worker": workers <= 1 or (not require_redis) or redis_url_configured,
    }


async def _close_async(resource: Any) -> None:
    if resource is None:
        return
    close = getattr(resource, "aclose", None) or getattr(resource, "close", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        await result


class Broker(ABC):
    """Abstract broker interface for pub/sub operations."""

    @abstractmethod
    async def connect(self) -> None:
        """Initialize connection to the broker."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the broker."""
        pass

    @abstractmethod
    async def publish(self, room: str, event: dict) -> None:
        """Publish an event to a room."""
        pass

    @abstractmethod
    async def subscribe(
        self,
        room: str,
        callback: Callable[[dict], Coroutine[Any, Any, None]],
    ) -> str:
        """
        Subscribe to a room. Returns a subscription ID.

        The callback will be called with each event dict.
        """
        pass

    @abstractmethod
    async def unsubscribe(self, room: str, subscription_id: str) -> None:
        """Unsubscribe from a room using the subscription ID."""
        pass

    # --- Ephemeral state (typing, presence) ---

    @abstractmethod
    async def set_ephemeral(self, key: str, value: str, ttl_seconds: int) -> None:
        """Set an ephemeral key with TTL."""
        pass

    @abstractmethod
    async def get_ephemeral(self, key: str) -> str | None:
        """Get an ephemeral key value."""
        pass

    @abstractmethod
    async def delete_ephemeral(self, key: str) -> None:
        """Delete an ephemeral key."""
        pass

    @abstractmethod
    async def get_keys_by_pattern(self, pattern: str) -> list[str]:
        """Get all keys matching a pattern (e.g., 'typing:conv123:*')."""
        pass

    def status(self) -> dict[str, object]:
        """Return public-safe runtime status for health diagnostics."""
        return {"mode": "unknown", "connected": False}


class InMemoryBroker(Broker):
    """
    In-memory pub/sub broker for local development.

    Not suitable for multi-instance deployments.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, dict[str, Callable]] = defaultdict(dict)
        self._ephemeral: dict[str, tuple[str, float]] = {}  # key -> (value, expires_at)
        self._sub_counter = 0
        self._cleanup_task: asyncio.Task | None = None
        self._connected = False

    async def connect(self) -> None:
        """Start the cleanup task for ephemeral keys."""
        logger.info("InMemoryBroker connected (local dev mode)")
        self._connected = True
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_expired())

    async def disconnect(self) -> None:
        """Stop the cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self._connected = False
        logger.info("InMemoryBroker disconnected")

    async def publish(self, room: str, event: dict) -> None:
        """Publish to all subscribers in the room."""
        subscribers = list(self._subscribers.get(room, {}).values())
        for callback in subscribers:
            try:
                await callback(event)
            except Exception as e:
                logger.error(f"Error in subscriber callback: {e}")

    async def subscribe(
        self,
        room: str,
        callback: Callable[[dict], Coroutine[Any, Any, None]],
    ) -> str:
        """Subscribe to a room."""
        self._sub_counter += 1
        sub_id = f"sub_{self._sub_counter}"
        self._subscribers[room][sub_id] = callback
        logger.debug(f"Subscribed {sub_id} to room {room}")
        return sub_id

    async def unsubscribe(self, room: str, subscription_id: str) -> None:
        """Unsubscribe from a room."""
        if room in self._subscribers:
            self._subscribers[room].pop(subscription_id, None)
            if not self._subscribers[room]:
                del self._subscribers[room]
        logger.debug(f"Unsubscribed {subscription_id} from room {room}")

    async def set_ephemeral(self, key: str, value: str, ttl_seconds: int) -> None:
        """Set ephemeral key with TTL."""
        expires_at = datetime.now(UTC).timestamp() + ttl_seconds
        self._ephemeral[key] = (value, expires_at)

    async def get_ephemeral(self, key: str) -> str | None:
        """Get ephemeral key if not expired."""
        if key not in self._ephemeral:
            return None
        value, expires_at = self._ephemeral[key]
        if datetime.now(UTC).timestamp() > expires_at:
            del self._ephemeral[key]
            return None
        return value

    async def delete_ephemeral(self, key: str) -> None:
        """Delete ephemeral key."""
        self._ephemeral.pop(key, None)

    async def get_keys_by_pattern(self, pattern: str) -> list[str]:
        """Get keys matching pattern (simple glob: * at end)."""
        # Simple implementation: only supports prefix* pattern
        prefix = pattern.rstrip("*")
        now = datetime.now(UTC).timestamp()
        matching = []
        for key, (_, expires_at) in list(self._ephemeral.items()):
            if key.startswith(prefix) and expires_at > now:
                matching.append(key)
        return matching

    def status(self) -> dict[str, object]:
        return {
            "mode": "memory",
            "connected": self._connected,
            "pubsub_backend": "process",
            "ephemeral_backend": "process",
            "subscriber_rooms": len(self._subscribers),
            "subscription_count": sum(len(subscribers) for subscribers in self._subscribers.values()),
            "ephemeral_key_count": len(self._ephemeral),
        }

    async def _cleanup_expired(self) -> None:
        """Periodically clean up expired ephemeral keys."""
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                now = datetime.now(UTC).timestamp()
                expired = [k for k, (_, exp) in self._ephemeral.items() if exp <= now]
                for key in expired:
                    del self._ephemeral[key]
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")


class RedisBroker(Broker):
    """
    Redis-backed pub/sub broker for production.

    Supports multi-instance deployments via Redis pub/sub.
    """

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._redis: Any = None
        self._pubsub: Any = None
        self._subscribers: dict[str, dict[str, Callable]] = defaultdict(dict)
        self._sub_counter = 0
        self._listener_task: asyncio.Task | None = None
        self._subscribed_rooms: set[str] = set()
        self._connected = False
        self._last_error: dict[str, str] | None = None

    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
            await self._redis.ping()
            self._pubsub = self._redis.pubsub()
            self._listener_task = asyncio.create_task(self._listen())
            self._connected = True
            self._last_error = None
            logger.info("RedisBroker connected")
        except ImportError as exc:
            self._connected = False
            self._last_error = {"type": type(exc).__name__, "message": "redis package not installed"}
            raise RuntimeError("redis package not installed. Run: pip install redis") from None
        except Exception as e:
            self._connected = False
            self._last_error = _redact_redis_error(e, redis_url=self._redis_url)
            await _close_async(self._pubsub)
            await _close_async(self._redis)
            self._pubsub = None
            self._redis = None
            logger.error("Failed to connect to Redis: %s", self._last_error["type"])
            raise

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None
        if self._pubsub:
            await _close_async(self._pubsub)
            self._pubsub = None
        if self._redis:
            await _close_async(self._redis)
            self._redis = None
        self._connected = False
        logger.info("RedisBroker disconnected")

    async def publish(self, room: str, event: dict) -> None:
        """Publish event to Redis channel."""
        if not self._redis:
            raise RuntimeError("Not connected to Redis")
        message = json.dumps(event)
        await self._redis.publish(room, message)

    async def subscribe(
        self,
        room: str,
        callback: Callable[[dict], Coroutine[Any, Any, None]],
    ) -> str:
        """Subscribe to a Redis channel."""
        self._sub_counter += 1
        sub_id = f"sub_{self._sub_counter}"
        self._subscribers[room][sub_id] = callback

        # Subscribe to Redis channel if not already
        if room not in self._subscribed_rooms:
            if not self._pubsub:
                raise RuntimeError("Not connected to Redis pubsub")
            await self._pubsub.subscribe(room)
            self._subscribed_rooms.add(room)
            logger.debug(f"Subscribed to Redis channel: {room}")

        return sub_id

    async def unsubscribe(self, room: str, subscription_id: str) -> None:
        """Unsubscribe from a room."""
        if room in self._subscribers:
            self._subscribers[room].pop(subscription_id, None)
            # Unsubscribe from Redis if no more local subscribers
            if not self._subscribers[room]:
                del self._subscribers[room]
                if room in self._subscribed_rooms:
                    if not self._pubsub:
                        raise RuntimeError("Not connected to Redis pubsub")
                    await self._pubsub.unsubscribe(room)
                    self._subscribed_rooms.discard(room)
                    logger.debug(f"Unsubscribed from Redis channel: {room}")

    async def set_ephemeral(self, key: str, value: str, ttl_seconds: int) -> None:
        """Set ephemeral key in Redis with TTL."""
        if not self._redis:
            raise RuntimeError("Not connected to Redis")
        await self._redis.setex(key, ttl_seconds, value)

    async def get_ephemeral(self, key: str) -> str | None:
        """Get ephemeral key from Redis."""
        if not self._redis:
            raise RuntimeError("Not connected to Redis")
        return await self._redis.get(key)

    async def delete_ephemeral(self, key: str) -> None:
        """Delete ephemeral key from Redis."""
        if not self._redis:
            raise RuntimeError("Not connected to Redis")
        await self._redis.delete(key)

    async def get_keys_by_pattern(self, pattern: str) -> list[str]:
        """Get keys matching pattern from Redis."""
        if not self._redis:
            raise RuntimeError("Not connected to Redis")
        keys: list[str] = []
        iterator = self._redis.scan_iter(match=pattern)
        if hasattr(iterator, "__aiter__"):
            async for key in iterator:
                keys.append(key)
        else:
            keys.extend(iterator)
        return keys

    async def _listen(self) -> None:
        """Listen for messages from Redis pub/sub."""
        try:
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    room = message["channel"]
                    try:
                        event = json.loads(message["data"])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in message: {message['data']}")
                        continue

                    # Dispatch to all local subscribers
                    subscribers = list(self._subscribers.get(room, {}).values())
                    for callback in subscribers:
                        try:
                            await callback(event)
                        except Exception as e:
                            logger.error(f"Error in subscriber callback: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._connected = False
            self._last_error = _redact_redis_error(e, redis_url=self._redis_url)
            logger.error("Error in Redis listener: %s", self._last_error["type"])

    def status(self) -> dict[str, object]:
        return {
            "mode": "redis",
            "connected": self._connected,
            "pubsub_backend": "redis",
            "ephemeral_backend": "redis",
            "redis": _redis_url_snapshot(self._redis_url),
            "last_error": self._last_error,
            "subscriber_rooms": len(self._subscribers),
            "subscription_count": sum(len(subscribers) for subscribers in self._subscribers.values()),
            "subscribed_room_count": len(self._subscribed_rooms),
            "listener_running": self._listener_task is not None and not self._listener_task.done(),
        }


# --- Singleton broker instance ---

_broker: Broker | None = None


def get_broker() -> Broker:
    """
    Get the broker singleton.

    Uses Redis if REDIS_URL is set, otherwise in-memory.
    """
    global _broker
    if _broker is None:
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            _broker = RedisBroker(redis_url)
        else:
            _broker = InMemoryBroker()
    return _broker


def broker_runtime_status() -> dict[str, object]:
    """Return a public-safe broker status snapshot for runtime health."""
    redis_url = (os.getenv("REDIS_URL") or "").strip()
    if _broker is None:
        broker_status: dict[str, object] = {
            "mode": "redis" if redis_url else "memory",
            "connected": False,
            "initialized": False,
            "redis": _redis_url_snapshot(redis_url),
        }
    else:
        broker_status = dict(_broker.status())
        broker_status["initialized"] = True
        if broker_status.get("mode") == "memory":
            broker_status["redis"] = _redis_url_snapshot(redis_url)

    broker_status["multi_worker_policy"] = _multi_worker_policy_snapshot()
    return broker_status


async def init_broker() -> Broker:
    """Initialize and connect the broker."""
    broker = get_broker()
    await broker.connect()
    return broker


async def shutdown_broker() -> None:
    """Shutdown the broker connection."""
    global _broker
    if _broker:
        await _broker.disconnect()
        _broker = None
