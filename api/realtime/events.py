"""
Event types and envelope for real-time WebSocket communication.

All events follow a consistent envelope format:
{
    "type": "event_type",
    "ts": "2025-12-17T12:34:56Z",
    "payload": { ... }
}
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel


class EventType(str, Enum):
    """Event types for real-time communication."""

    # Discussion events
    THREAD_CREATED = "thread_created"
    POST_CREATED = "post_created"
    REACTION_TOGGLED = "reaction_toggled"

    # DM events
    DM_MESSAGE_CREATED = "dm_message_created"
    DM_READ_UPDATED = "dm_read_updated"

    # Ephemeral events
    TYPING = "typing"
    PRESENCE = "presence"

    # System events
    ERROR = "error"
    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"


class Event(BaseModel):
    """
    Event envelope for WebSocket messages.

    All events follow this structure for consistency.
    """

    type: EventType
    ts: str
    payload: dict[str, Any]

    @classmethod
    def create(cls, event_type: EventType, payload: dict[str, Any]) -> "Event":
        """Create an event with current timestamp."""
        return cls(
            type=event_type,
            ts=datetime.now(timezone.utc).isoformat(),
            payload=payload,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "type": self.type.value,
            "ts": self.ts,
            "payload": self.payload,
        }


# --- Helper functions to create specific events ---


def thread_created_event(thread: dict) -> Event:
    """Create a thread_created event."""
    return Event.create(
        EventType.THREAD_CREATED,
        {
            "thread_id": str(thread["id"]),
            "episode_id": str(thread["episode_id"]),
            "title": thread["title"],
            "type": thread["type"],
            "created_by": str(thread.get("created_by")) if thread.get("created_by") else None,
        },
    )


def post_created_event(post: dict) -> Event:
    """Create a post_created event."""
    return Event.create(
        EventType.POST_CREATED,
        {
            "post_id": str(post["id"]),
            "thread_id": str(post["thread_id"]),
            "parent_post_id": str(post["parent_post_id"]) if post.get("parent_post_id") else None,
            "user_id": str(post.get("user_id")) if post.get("user_id") else None,
            "body": post["body"],
            "created_at": post["created_at"],
        },
    )


def reaction_toggled_event(post_id: str, user_id: str, reaction: str, action: str) -> Event:
    """Create a reaction_toggled event."""
    return Event.create(
        EventType.REACTION_TOGGLED,
        {
            "post_id": post_id,
            "user_id": user_id,
            "reaction": reaction,
            "action": action,  # "added" or "removed"
        },
    )


def dm_message_created_event(message: dict) -> Event:
    """Create a dm_message_created event."""
    return Event.create(
        EventType.DM_MESSAGE_CREATED,
        {
            "message_id": str(message["id"]),
            "conversation_id": str(message["conversation_id"]),
            "sender_id": str(message.get("sender_id")) if message.get("sender_id") else None,
            "body": message["body"],
            "created_at": message["created_at"],
        },
    )


def dm_read_updated_event(conversation_id: str, user_id: str, last_read_message_id: str) -> Event:
    """Create a dm_read_updated event."""
    return Event.create(
        EventType.DM_READ_UPDATED,
        {
            "conversation_id": conversation_id,
            "user_id": user_id,
            "last_read_message_id": last_read_message_id,
        },
    )


def typing_event(conversation_id: str, user_id: str, is_typing: bool) -> Event:
    """Create a typing event."""
    return Event.create(
        EventType.TYPING,
        {
            "conversation_id": conversation_id,
            "user_id": user_id,
            "is_typing": is_typing,
        },
    )


def presence_event(user_id: str, is_online: bool) -> Event:
    """Create a presence event."""
    return Event.create(
        EventType.PRESENCE,
        {
            "user_id": user_id,
            "is_online": is_online,
        },
    )


def error_event(message: str, code: str | None = None) -> Event:
    """Create an error event."""
    payload: dict[str, Any] = {"message": message}
    if code:
        payload["code"] = code
    return Event.create(EventType.ERROR, payload)


def subscribed_event(room: str) -> Event:
    """Create a subscribed confirmation event."""
    return Event.create(EventType.SUBSCRIBED, {"room": room})


def unsubscribed_event(room: str) -> Event:
    """Create an unsubscribed confirmation event."""
    return Event.create(EventType.UNSUBSCRIBED, {"room": room})


# --- Room naming conventions ---


def get_discussion_room(episode_id: str) -> str:
    """Get the room name for episode discussions."""
    return f"discussions:episode:{episode_id}"


def get_dm_room(conversation_id: str) -> str:
    """Get the room name for a DM conversation."""
    return f"dms:conversation:{conversation_id}"


def get_typing_key(conversation_id: str, user_id: str) -> str:
    """Get the Redis key for typing state."""
    return f"typing:{conversation_id}:{user_id}"


def get_presence_key(user_id: str) -> str:
    """Get the Redis key for presence state."""
    return f"presence:{user_id}"
