"""
Direct Messages (DM) endpoints.

All DM endpoints require authentication.
RLS policies enforce member-only access to conversations.

Events are published to WebSocket subscribers after successful writes.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel

from api.auth import CurrentUser, get_user_db_session  # noqa: F401
from api.realtime.broker import get_broker
from api.realtime.events import (
    dm_message_created_event,
    dm_read_updated_event,
    get_dm_room,
)
from trr_backend.db import pg

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dms", tags=["dms"])


# --- Helper for publishing events ---


def publish_event_sync(room: str, event: dict) -> bool:
    """
    Publish an event to a room (sync wrapper for background task).

    This runs the async publish in a new event loop since
    FastAPI sync endpoints don't have an event loop.
    """
    try:
        broker = get_broker()
        asyncio.run(broker.publish(room, event))
    except Exception:
        logger.exception("Failed to publish realtime event to %s", room)
        return False
    return True


# --- Pydantic models ---


class ConversationCreate(BaseModel):
    """Create/get a 1:1 DM conversation."""

    other_user_id: UUID


class ConversationMember(BaseModel):
    user_id: UUID
    role: str
    joined_at: str


class Conversation(BaseModel):
    id: UUID
    is_group: bool
    created_at: str
    last_message_at: str | None
    members: list[ConversationMember] = []


class ConversationSummary(BaseModel):
    """Conversation with preview info for listing."""

    id: UUID
    is_group: bool
    created_at: str
    last_message_at: str | None


class MessageCreate(BaseModel):
    """Message creation payload. sender_id is server-derived."""

    body: str


class Message(BaseModel):
    id: UUID
    conversation_id: UUID
    sender_id: UUID | None
    body: str
    created_at: str


class ReadReceiptUpdate(BaseModel):
    """Update read receipt to mark messages as read."""

    last_read_message_id: UUID


class ReadReceipt(BaseModel):
    conversation_id: UUID
    user_id: UUID
    last_read_message_id: UUID | None
    last_read_at: str


# --- Endpoints ---


def _require_membership(conversation_id: str, user_id: str) -> None:
    row = pg.fetch_one(
        "SELECT user_id FROM social.dm_members WHERE conversation_id = %s AND user_id = %s",
        [conversation_id, user_id],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Conversation not found or you don't have access")


@router.post("", response_model=Conversation)
def create_or_get_conversation(
    payload: ConversationCreate,
    user: CurrentUser,
) -> dict:
    """
    Create or get a 1:1 DM conversation with another user.

    If a conversation already exists between the two users, returns it.
    Otherwise, creates a new conversation with both users as members.

    Requires authentication.
    """
    user_id = str(user["id"])
    other_id = str(payload.other_user_id)

    existing = pg.fetch_one(
        """
        SELECT m1.conversation_id
        FROM social.dm_members m1
        JOIN social.dm_members m2 ON m1.conversation_id = m2.conversation_id
        WHERE m1.user_id = %s AND m2.user_id = %s
        LIMIT 1
        """,
        [user_id, other_id],
    )
    conversation_id = existing["conversation_id"] if existing else None

    if not conversation_id:
        conversation = pg.fetch_one("INSERT INTO social.dm_conversations (is_group) VALUES (false) RETURNING *")
        if not conversation:
            raise HTTPException(status_code=500, detail="Failed to create conversation")
        conversation_id = conversation["id"]
        pg.execute_returning(
            """
            INSERT INTO social.dm_members (conversation_id, user_id, role)
            VALUES (%s, %s, 'member'), (%s, %s, 'member')
            RETURNING user_id, role, joined_at
            """,
            [conversation_id, user_id, conversation_id, other_id],
        )

    conversation = pg.fetch_one(
        "SELECT * FROM social.dm_conversations WHERE id = %s",
        [conversation_id],
    )
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    members = pg.fetch_all(
        "SELECT user_id, role, joined_at FROM social.dm_members WHERE conversation_id = %s",
        [conversation_id],
    )
    conversation["members"] = members
    return conversation


@router.get("", response_model=list[ConversationSummary])
def list_conversations(
    user: CurrentUser,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """
    List the authenticated user's DM conversations.

    Ordered by last_message_at (most recent first), then created_at.
    Requires authentication.
    """
    rows = pg.fetch_all(
        """
        SELECT c.id, c.is_group, c.created_at, c.last_message_at
        FROM social.dm_conversations c
        JOIN social.dm_members m ON m.conversation_id = c.id
        WHERE m.user_id = %s
        ORDER BY c.last_message_at DESC NULLS LAST, c.created_at DESC
        LIMIT %s OFFSET %s
        """,
        [str(user["id"]), limit, offset],
    )
    return rows


@router.get("/{conversation_id}/messages", response_model=list[Message])
def list_messages(
    conversation_id: UUID,
    user: CurrentUser,
    limit: int = Query(default=50, le=100),
    cursor: str | None = Query(default=None, description="created_at cursor for pagination"),
) -> list[dict]:
    """
    List messages in a conversation.

    Cursor-based pagination using created_at timestamp.
    Messages are returned oldest to newest (for chat display).

    Requires authentication. RLS enforces member-only access.
    """
    conv_id = str(conversation_id)
    _require_membership(conv_id, str(user["id"]))

    params: list[object] = [conv_id]
    cursor_clause = ""
    if cursor:
        cursor_clause = " AND created_at > %s"
        params.append(cursor)
    params.append(limit)

    messages = pg.fetch_all(
        f"""
        SELECT * FROM social.dm_messages
        WHERE conversation_id = %s{cursor_clause}
        ORDER BY created_at ASC
        LIMIT %s
        """,
        params,
    )

    return messages


@router.post("/{conversation_id}/messages", response_model=Message)
def send_message(
    conversation_id: UUID,
    payload: MessageCreate,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
) -> dict:
    """
    Send a message to a conversation.

    sender_id is server-derived from the auth token.
    Updates the conversation's last_message_at timestamp.

    Requires authentication. RLS enforces member-only access.
    """
    conv_id = str(conversation_id)
    _require_membership(conv_id, str(user["id"]))

    message = pg.fetch_one(
        """
        INSERT INTO social.dm_messages (conversation_id, sender_id, body)
        VALUES (%s, %s, %s)
        RETURNING *
        """,
        [conv_id, str(user["id"]), payload.body],
    )
    if not message:
        raise HTTPException(status_code=500, detail="Failed to send message")

    pg.execute_returning(
        "UPDATE social.dm_conversations SET last_message_at = %s WHERE id = %s RETURNING id",
        [message["created_at"], conv_id],
    )
    # Don't fail if this update fails - the message was still sent

    # Publish event to WebSocket subscribers
    room = get_dm_room(str(conversation_id))
    event = dm_message_created_event(message)
    background_tasks.add_task(publish_event_sync, room, event.to_dict())

    return message


@router.post("/{conversation_id}/read", response_model=ReadReceipt)
def update_read_receipt(
    conversation_id: UUID,
    payload: ReadReceiptUpdate,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
) -> dict:
    """
    Update read receipt to mark messages as read up to a specific message.

    Only updates the authenticated user's read receipt.
    Requires authentication.
    """
    conv_id = str(conversation_id)
    user_id = str(user["id"])
    _require_membership(conv_id, user_id)

    message = pg.fetch_one(
        """
        SELECT id FROM social.dm_messages
        WHERE id = %s AND conversation_id = %s
        """,
        [str(payload.last_read_message_id), conv_id],
    )
    if not message:
        raise HTTPException(status_code=404, detail="Message not found in this conversation")

    receipt = pg.fetch_one(
        """
        INSERT INTO social.dm_read_receipts (conversation_id, user_id, last_read_message_id, last_read_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (conversation_id, user_id)
        DO UPDATE SET last_read_message_id = EXCLUDED.last_read_message_id,
                      last_read_at = EXCLUDED.last_read_at
        RETURNING *
        """,
        [conv_id, user_id, str(payload.last_read_message_id), datetime.now(UTC).isoformat()],
    )
    if not receipt:
        raise HTTPException(status_code=500, detail="Failed to update read receipt")

    # Publish event to WebSocket subscribers
    room = get_dm_room(str(conversation_id))
    event = dm_read_updated_event(
        str(conversation_id),
        user["id"],
        str(payload.last_read_message_id),
    )
    background_tasks.add_task(publish_event_sync, room, event.to_dict())

    return receipt
