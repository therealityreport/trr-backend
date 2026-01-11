"""
Pub/sub broker abstraction for real-time events.

Uses Redis pub/sub when REDIS_URL is set, otherwise falls back
to in-process pub/sub (fine for local dev, not multi-instance).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


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

    async def connect(self) -> None:
        """Start the cleanup task for ephemeral keys."""
        logger.info("InMemoryBroker connected (local dev mode)")
        self._cleanup_task = asyncio.create_task(self._cleanup_expired())

    async def disconnect(self) -> None:
        """Stop the cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
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

    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
            self._pubsub = self._redis.pubsub()
            self._listener_task = asyncio.create_task(self._listen())
            logger.info(f"RedisBroker connected to {self._redis_url}")
        except ImportError:
            raise RuntimeError("redis package not installed. Run: pip install redis") from None
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._pubsub:
            await self._pubsub.close()
        if self._redis:
            await self._redis.close()
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
        return await self._redis.keys(pattern)

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
            logger.error(f"Error in Redis listener: {e}")


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
