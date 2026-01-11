"""
WebSocket endpoints for real-time updates.

Provides:
- Episode discussion subscriptions (public read, auth for typing)
- DM conversation subscriptions (auth required)
- Typing indicators
- Presence heartbeats
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ValidationError

from api.deps import get_supabase_anon_key, get_supabase_url
from api.realtime.broker import get_broker
from api.realtime.events import (
    Event,
    error_event,
    get_discussion_room,
    get_dm_room,
    get_presence_key,
    get_typing_key,
    presence_event,
    subscribed_event,
    typing_event,
)
from supabase import create_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["websocket"])

# --- Constants ---

TYPING_TTL_SECONDS = 10
PRESENCE_TTL_SECONDS = 45
HEARTBEAT_INTERVAL_SECONDS = 20


# --- Pydantic models for client messages ---


class ClientMessage(BaseModel):
    """Base model for client-to-server WebSocket messages."""

    type: str
    payload: dict[str, Any] = {}


# --- Helper functions ---


async def validate_token(token: str | None) -> dict | None:
    """
    Validate a Supabase JWT token.

    Returns user dict if valid, None otherwise.
    """
    if not token:
        return None

    try:
        client = create_client(get_supabase_url(), get_supabase_anon_key())
        user_response = client.auth.get_user(token)

        if user_response and user_response.user:
            return {
                "id": str(user_response.user.id),
                "email": user_response.user.email,
                "role": user_response.user.role,
                "token": token,
            }
        return None
    except Exception as e:
        logger.warning(f"Token validation failed: {e}")
        return None


async def check_dm_membership(user_id: str, conversation_id: str, token: str) -> bool:
    """
    Check if a user is a member of a DM conversation.

    Uses RLS to verify membership.
    """
    try:
        client = create_client(get_supabase_url(), get_supabase_anon_key())
        client.postgrest.auth(token)

        response = (
            client.schema("social")
            .table("dm_members")
            .select("user_id")
            .eq("conversation_id", conversation_id)
            .eq("user_id", user_id)
            .execute()
        )

        return bool(response.data)
    except Exception as e:
        logger.error(f"Failed to check DM membership: {e}")
        return False


async def send_event(websocket: WebSocket, event: Event) -> None:
    """Send an event to a WebSocket client."""
    try:
        await websocket.send_json(event.to_dict())
    except Exception as e:
        logger.error(f"Failed to send event: {e}")


# --- Discussion WebSocket endpoint ---


@router.websocket("/discussions/episodes/{episode_id}")
async def discussion_websocket(
    websocket: WebSocket,
    episode_id: UUID,
    token: str | None = Query(default=None, description="JWT token for authenticated features"),
):
    """
    WebSocket for episode discussion updates.

    - Read-only subscription allowed for anonymous users
    - Authenticated users can send typing events

    Events received:
    - thread_created
    - post_created
    - reaction_toggled
    - typing (if authenticated users are typing)

    Client messages (auth required):
    - {"type": "typing_start", "payload": {}}
    - {"type": "typing_stop", "payload": {}}
    """
    await websocket.accept()

    # Validate token if provided
    user = await validate_token(token) if token else None
    room = get_discussion_room(str(episode_id))
    broker = get_broker()

    # Callback for broker events
    async def on_event(event: dict) -> None:
        try:
            await websocket.send_json(event)
        except Exception:
            pass  # Connection may be closed

    # Subscribe to discussion room
    sub_id = await broker.subscribe(room, on_event)
    await send_event(websocket, subscribed_event(room))

    try:
        while True:
            # Receive client messages
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                msg = ClientMessage(**message)

                # Handle typing events (auth required)
                if msg.type in ("typing_start", "typing_stop"):
                    if not user:
                        await send_event(
                            websocket,
                            error_event("Authentication required for typing", "AUTH_REQUIRED"),
                        )
                        continue

                    # For discussions, typing doesn't make as much sense
                    # but we can still support it if needed
                    # Just acknowledge for now
                    pass

            except json.JSONDecodeError:
                await send_event(websocket, error_event("Invalid JSON"))
            except ValidationError as e:
                await send_event(websocket, error_event(f"Invalid message: {e}"))
            except WebSocketDisconnect:
                break

    finally:
        # Cleanup
        await broker.unsubscribe(room, sub_id)


# --- DM WebSocket endpoint ---


@router.websocket("/dms/{conversation_id}")
async def dm_websocket(
    websocket: WebSocket,
    conversation_id: UUID,
    token: str = Query(..., description="JWT token (required)"),
):
    """
    WebSocket for DM conversation updates.

    Authentication is required. Only conversation members can connect.

    Events received:
    - dm_message_created
    - dm_read_updated
    - typing
    - presence

    Client messages:
    - {"type": "typing_start", "payload": {}}
    - {"type": "typing_stop", "payload": {}}
    - {"type": "heartbeat", "payload": {}}
    """
    # Validate token
    user = await validate_token(token)
    if not user:
        await websocket.close(code=4001, reason="Authentication required")
        return

    # Verify membership
    is_member = await check_dm_membership(user["id"], str(conversation_id), user["token"])
    if not is_member:
        await websocket.close(code=4003, reason="Not a member of this conversation")
        return

    await websocket.accept()

    room = get_dm_room(str(conversation_id))
    broker = get_broker()
    user_id = user["id"]

    # Callback for broker events
    async def on_event(event: dict) -> None:
        try:
            await websocket.send_json(event)
        except Exception:
            pass

    # Subscribe to DM room
    sub_id = await broker.subscribe(room, on_event)
    await send_event(websocket, subscribed_event(room))

    # Set initial presence
    presence_key = get_presence_key(user_id)
    await broker.set_ephemeral(presence_key, "online", PRESENCE_TTL_SECONDS)

    # Broadcast presence to room
    await broker.publish(room, presence_event(user_id, True).to_dict())

    # Start heartbeat timeout checker
    last_heartbeat = asyncio.get_event_loop().time()

    try:
        while True:
            try:
                # Wait for message with timeout for heartbeat check
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=HEARTBEAT_INTERVAL_SECONDS + 5,
                )
                message = json.loads(data)
                msg = ClientMessage(**message)

                if msg.type == "typing_start":
                    # Set typing state
                    typing_key = get_typing_key(str(conversation_id), user_id)
                    await broker.set_ephemeral(typing_key, "typing", TYPING_TTL_SECONDS)
                    # Broadcast typing event
                    await broker.publish(
                        room,
                        typing_event(str(conversation_id), user_id, True).to_dict(),
                    )

                elif msg.type == "typing_stop":
                    # Clear typing state
                    typing_key = get_typing_key(str(conversation_id), user_id)
                    await broker.delete_ephemeral(typing_key)
                    # Broadcast typing stop event
                    await broker.publish(
                        room,
                        typing_event(str(conversation_id), user_id, False).to_dict(),
                    )

                elif msg.type == "heartbeat":
                    # Update presence TTL
                    await broker.set_ephemeral(presence_key, "online", PRESENCE_TTL_SECONDS)
                    last_heartbeat = asyncio.get_event_loop().time()

            except TimeoutError:
                # Check if we should close due to missing heartbeats
                elapsed = asyncio.get_event_loop().time() - last_heartbeat
                if elapsed > PRESENCE_TTL_SECONDS:
                    logger.info(f"Closing WS for user {user_id} due to heartbeat timeout")
                    break
            except json.JSONDecodeError:
                await send_event(websocket, error_event("Invalid JSON"))
            except ValidationError as e:
                await send_event(websocket, error_event(f"Invalid message: {e}"))
            except WebSocketDisconnect:
                break

    finally:
        # Cleanup
        await broker.unsubscribe(room, sub_id)

        # Clear typing state
        typing_key = get_typing_key(str(conversation_id), user_id)
        await broker.delete_ephemeral(typing_key)

        # Clear presence
        await broker.delete_ephemeral(presence_key)

        # Broadcast offline presence
        try:
            await broker.publish(room, presence_event(user_id, False).to_dict())
        except Exception:
            pass  # Best effort


# --- Connection manager for tracking active connections ---


class ConnectionManager:
    """
    Manages WebSocket connections for presence tracking.

    This is used to track which users are connected and broadcast
    presence changes across the system.
    """

    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = {}  # user_id -> connections

    def connect(self, user_id: str, websocket: WebSocket) -> None:
        """Register a new connection."""
        if user_id not in self._connections:
            self._connections[user_id] = set()
        self._connections[user_id].add(websocket)

    def disconnect(self, user_id: str, websocket: WebSocket) -> bool:
        """
        Remove a connection.

        Returns True if this was the user's last connection (went offline).
        """
        if user_id in self._connections:
            self._connections[user_id].discard(websocket)
            if not self._connections[user_id]:
                del self._connections[user_id]
                return True
        return False

    def is_online(self, user_id: str) -> bool:
        """Check if a user has any active connections."""
        return user_id in self._connections and len(self._connections[user_id]) > 0


# Global connection manager
connection_manager = ConnectionManager()
