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

from api.auth import CurrentUser, get_user_supabase_client
from api.deps import (
    SupabaseClient,
    get_list_result,
    raise_for_supabase_error,
    require_single_result,
)
from api.realtime.broker import get_broker
from api.realtime.events import (
    dm_message_created_event,
    dm_read_updated_event,
    get_dm_room,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dms", tags=["dms"])


# --- Helper for publishing events ---


def publish_event_sync(room: str, event: dict) -> None:
    """
    Publish an event to a room (sync wrapper for background task).

    This runs the async publish in a new event loop since
    FastAPI sync endpoints don't have an event loop.
    """
    try:
        broker = get_broker()
        asyncio.run(broker.publish(room, event))
    except Exception as e:
        logger.error(f"Failed to publish event to {room}: {e}")


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
    user_db = get_user_supabase_client(user)

    # Call the RPC function to get or create conversation
    response = user_db.rpc("get_or_create_direct_conversation", {"other_user_id": str(payload.other_user_id)}).execute()

    if response.data is None:
        raise HTTPException(status_code=500, detail="Failed to create conversation")

    conversation_id = response.data

    # Fetch the conversation details with members
    conv_response = (
        user_db.schema("social").table("dm_conversations").select("*").eq("id", conversation_id).single().execute()
    )
    conversation = require_single_result(conv_response, "Conversation")

    # Fetch members
    members_response = (
        user_db.schema("social")
        .table("dm_members")
        .select("user_id, role, joined_at")
        .eq("conversation_id", conversation_id)
        .execute()
    )
    members = get_list_result(members_response, "fetching members")

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
    user_db = get_user_supabase_client(user)

    # Get conversations where user is a member
    # RLS will filter to only conversations the user is in
    response = (
        user_db.schema("social")
        .table("dm_conversations")
        .select("id, is_group, created_at, last_message_at")
        .order("last_message_at", desc=True, nullsfirst=False)
        .order("created_at", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return get_list_result(response, "listing conversations")


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
    user_db = get_user_supabase_client(user)

    # Verify user has access (RLS will block if not a member)
    query = user_db.schema("social").table("dm_messages").select("*").eq("conversation_id", str(conversation_id))

    if cursor:
        query = query.gt("created_at", cursor)

    response = query.order("created_at", desc=False).limit(limit).execute()

    messages = get_list_result(response, "listing messages")

    # If no messages and no cursor, verify conversation exists and user has access
    if not messages and not cursor:
        # This will raise 404 if conversation doesn't exist or user has no access
        conv_response = (
            user_db.schema("social")
            .table("dm_conversations")
            .select("id")
            .eq("id", str(conversation_id))
            .single()
            .execute()
        )
        if conv_response.data is None:
            raise HTTPException(status_code=404, detail="Conversation not found or you don't have access")

    return messages


@router.post("/{conversation_id}/messages", response_model=Message)
def send_message(
    conversation_id: UUID,
    payload: MessageCreate,
    user: CurrentUser,
    db: SupabaseClient,
    background_tasks: BackgroundTasks,
) -> dict:
    """
    Send a message to a conversation.

    sender_id is server-derived from the auth token.
    Updates the conversation's last_message_at timestamp.

    Requires authentication. RLS enforces member-only access.
    """
    user_db = get_user_supabase_client(user)

    # Insert message (RLS will verify membership and sender_id)
    message_response = (
        user_db.schema("social")
        .table("dm_messages")
        .insert(
            {
                "conversation_id": str(conversation_id),
                "sender_id": user["id"],
                "body": payload.body,
            }
        )
        .execute()
    )
    raise_for_supabase_error(message_response, "sending message")

    if not message_response.data:
        raise HTTPException(status_code=500, detail="Failed to send message")

    message = message_response.data[0]

    # Update conversation's last_message_at
    # Note: This uses user client, will only work if user is member (RLS)
    update_response = (
        user_db.schema("social")
        .table("dm_conversations")
        .update({"last_message_at": message["created_at"]})
        .eq("id", str(conversation_id))
        .execute()
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
    user_db = get_user_supabase_client(user)

    # Verify the message exists and belongs to this conversation
    message_response = (
        user_db.schema("social")
        .table("dm_messages")
        .select("id, conversation_id")
        .eq("id", str(payload.last_read_message_id))
        .eq("conversation_id", str(conversation_id))
        .single()
        .execute()
    )

    if message_response.data is None:
        raise HTTPException(status_code=404, detail="Message not found in this conversation")

    # Update the read receipt (upsert in case it doesn't exist)
    # RLS enforces that user can only update their own read receipt
    update_response = (
        user_db.schema("social")
        .table("dm_read_receipts")
        .upsert(
            {
                "conversation_id": str(conversation_id),
                "user_id": user["id"],
                "last_read_message_id": str(payload.last_read_message_id),
                "last_read_at": datetime.now(UTC).isoformat(),
            },
            on_conflict="conversation_id,user_id",
        )
        .execute()
    )
    raise_for_supabase_error(update_response, "updating read receipt")

    if not update_response.data:
        raise HTTPException(status_code=500, detail="Failed to update read receipt")

    receipt = update_response.data[0]

    # Publish event to WebSocket subscribers
    room = get_dm_room(str(conversation_id))
    event = dm_read_updated_event(
        str(conversation_id),
        user["id"],
        str(payload.last_read_message_id),
    )
    background_tasks.add_task(publish_event_sync, room, event.to_dict())

    return receipt
