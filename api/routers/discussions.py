"""
Episode discussion endpoints (Reddit-style threads, posts, and reactions).

All reads are public. Writes require authentication.
user_id is always server-derived from the auth token, never from client.

Events are published to WebSocket subscribers after successful writes.
"""

from __future__ import annotations

import asyncio
import logging
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
    get_discussion_room,
    post_created_event,
    reaction_toggled_event,
    thread_created_event,
)

logger = logging.getLogger(__name__)


router = APIRouter(tags=["discussions"])


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


# Valid thread types
VALID_THREAD_TYPES = ("episode_live", "post_episode", "spoilers", "general")

# Valid reaction types
VALID_REACTION_TYPES = ("upvote", "downvote", "lol", "shade", "fire", "heart")


# --- Pydantic models ---


class Thread(BaseModel):
    id: UUID
    episode_id: UUID
    title: str
    type: str
    created_by: UUID | None
    is_locked: bool
    created_at: str


class ThreadCreate(BaseModel):
    """
    Thread creation payload.
    Note: created_by is server-derived from auth token, not from client.
    """

    title: str
    type: str  # episode_live, post_episode, spoilers, general


class Post(BaseModel):
    id: UUID
    thread_id: UUID
    parent_post_id: UUID | None
    user_id: UUID | None
    body: str
    created_at: str
    edited_at: str | None


class PostCreate(BaseModel):
    """
    Post creation payload.
    Note: user_id is server-derived from auth token, not from client.
    """

    body: str
    parent_post_id: UUID | None = None


class Reaction(BaseModel):
    post_id: UUID
    user_id: UUID
    reaction: str
    created_at: str


class ReactionToggle(BaseModel):
    """
    Reaction toggle payload.
    Note: user_id is server-derived from auth token, not from client.
    """

    reaction: str  # upvote, downvote, lol, shade, fire, heart


class PostWithReactions(Post):
    reactions: dict[str, int] = {}  # reaction type -> count


# --- Thread endpoints ---


@router.get("/episodes/{episode_id}/threads", response_model=list[Thread])
def list_episode_threads(
    db: SupabaseClient,
    episode_id: UUID,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """
    List all discussion threads for an episode.
    Public endpoint - no auth required.
    """
    response = (
        db.schema("social")
        .table("threads")
        .select("*")
        .eq("episode_id", str(episode_id))
        .order("created_at", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return get_list_result(response, "listing episode threads")


@router.post("/episodes/{episode_id}/threads", response_model=Thread)
def create_thread(
    db: SupabaseClient,
    episode_id: UUID,
    thread: ThreadCreate,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
) -> dict:
    """
    Create a new discussion thread for an episode.
    Requires authentication.
    """
    # Validate thread type
    if thread.type not in VALID_THREAD_TYPES:
        raise HTTPException(
            status_code=400, detail=f"Invalid thread type. Must be one of: {', '.join(VALID_THREAD_TYPES)}"
        )

    # Verify episode exists (public read)
    episode_response = db.schema("core").table("episodes").select("id").eq("id", str(episode_id)).single().execute()
    require_single_result(episode_response, "Episode")

    # Create thread using user-scoped client (RLS enforces created_by = auth.uid())
    user_db = get_user_supabase_client(user)
    response = (
        user_db.schema("social")
        .table("threads")
        .insert(
            {
                "episode_id": str(episode_id),
                "title": thread.title,
                "type": thread.type,
                "created_by": user["id"],
            }
        )
        .execute()
    )
    raise_for_supabase_error(response, "creating thread")

    if not response.data:
        raise HTTPException(status_code=500, detail="Failed to create thread")

    created_thread = response.data[0]

    # Publish event to WebSocket subscribers
    room = get_discussion_room(str(episode_id))
    event = thread_created_event(created_thread)
    background_tasks.add_task(publish_event_sync, room, event.to_dict())

    return created_thread


@router.get("/threads/{thread_id}", response_model=Thread)
def get_thread(db: SupabaseClient, thread_id: UUID) -> dict:
    """
    Get a specific thread by ID.
    Public endpoint - no auth required.
    """
    response = db.schema("social").table("threads").select("*").eq("id", str(thread_id)).single().execute()
    return require_single_result(response, "Thread")


# --- Post endpoints ---


@router.get("/threads/{thread_id}/posts", response_model=list[PostWithReactions])
def list_thread_posts(
    db: SupabaseClient,
    thread_id: UUID,
    parent_post_id: UUID | None = Query(default=None),  # noqa: B008
    limit: int = Query(default=50, le=100),
    cursor: str | None = Query(default=None, description="created_at cursor for pagination"),
) -> list[dict]:
    """
    List posts in a thread with reaction counts.
    Public endpoint - no auth required.

    Pagination: Use cursor (created_at value) for stable pagination.
    If parent_post_id is omitted, returns top-level posts.
    If parent_post_id is provided, returns replies to that post.
    """
    # Build query
    query = db.schema("social").table("posts").select("*").eq("thread_id", str(thread_id))

    # Filter by parent
    if parent_post_id is None:
        query = query.is_("parent_post_id", "null")
    else:
        query = query.eq("parent_post_id", str(parent_post_id))

    # Cursor-based pagination
    if cursor:
        query = query.gt("created_at", cursor)

    response = query.order("created_at", desc=False).limit(limit).execute()
    posts = get_list_result(response, "listing posts")

    # Fetch reaction counts for all posts
    if posts:
        post_ids = [p["id"] for p in posts]
        reactions_response = (
            db.schema("social").table("reactions").select("post_id, reaction").in_("post_id", post_ids).execute()
        )
        reactions = get_list_result(reactions_response, "fetching reactions")

        # Aggregate reaction counts per post
        reaction_counts: dict[str, dict[str, int]] = {}
        for r in reactions:
            pid = r["post_id"]
            rtype = r["reaction"]
            if pid not in reaction_counts:
                reaction_counts[pid] = {}
            reaction_counts[pid][rtype] = reaction_counts[pid].get(rtype, 0) + 1

        # Add reaction counts to posts
        for post in posts:
            post["reactions"] = reaction_counts.get(post["id"], {})

    return posts


@router.post("/threads/{thread_id}/posts", response_model=Post)
def create_post(
    db: SupabaseClient,
    thread_id: UUID,
    post: PostCreate,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
) -> dict:
    """
    Create a new post in a thread.
    Requires authentication.
    """
    # Verify thread exists and is not locked (public read)
    # Also get episode_id for the room
    thread_response = (
        db.schema("social")
        .table("threads")
        .select("id, is_locked, episode_id")
        .eq("id", str(thread_id))
        .single()
        .execute()
    )
    thread = require_single_result(thread_response, "Thread")

    if thread["is_locked"]:
        raise HTTPException(status_code=403, detail="Thread is locked")

    # If replying to a post, verify parent exists
    if post.parent_post_id:
        parent_response = (
            db.schema("social")
            .table("posts")
            .select("id, thread_id")
            .eq("id", str(post.parent_post_id))
            .single()
            .execute()
        )
        parent = require_single_result(parent_response, "Parent post")

        # Ensure parent post is in the same thread
        if parent["thread_id"] != str(thread_id):
            raise HTTPException(status_code=400, detail="Parent post is not in this thread")

    # Create post using user-scoped client (RLS enforces user_id = auth.uid())
    user_db = get_user_supabase_client(user)
    insert_data = {
        "thread_id": str(thread_id),
        "body": post.body,
        "user_id": user["id"],
    }
    if post.parent_post_id:
        insert_data["parent_post_id"] = str(post.parent_post_id)

    response = user_db.schema("social").table("posts").insert(insert_data).execute()
    raise_for_supabase_error(response, "creating post")

    if not response.data:
        raise HTTPException(status_code=500, detail="Failed to create post")

    created_post = response.data[0]

    # Publish event to WebSocket subscribers
    room = get_discussion_room(str(thread["episode_id"]))
    event = post_created_event(created_post)
    background_tasks.add_task(publish_event_sync, room, event.to_dict())

    return created_post


# --- Reaction endpoints ---


@router.post("/posts/{post_id}/reactions", response_model=dict)
def toggle_reaction(
    db: SupabaseClient,
    post_id: UUID,
    reaction_toggle: ReactionToggle,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
) -> dict:
    """
    Toggle a reaction on a post (add if missing, remove if present).
    Requires authentication.

    Returns the current reaction state and action taken.
    """
    # Validate reaction type
    if reaction_toggle.reaction not in VALID_REACTION_TYPES:
        raise HTTPException(
            status_code=400, detail=f"Invalid reaction type. Must be one of: {', '.join(VALID_REACTION_TYPES)}"
        )

    # Verify post exists and get thread info to check lock status
    post_response = db.schema("social").table("posts").select("id, thread_id").eq("id", str(post_id)).single().execute()
    post_data = require_single_result(post_response, "Post")

    # Check if thread is locked and get episode_id for room
    thread_response = (
        db.schema("social")
        .table("threads")
        .select("is_locked, episode_id")
        .eq("id", post_data["thread_id"])
        .single()
        .execute()
    )
    thread = require_single_result(thread_response, "Thread")

    if thread["is_locked"]:
        raise HTTPException(status_code=403, detail="Thread is locked")

    # Use user-scoped client for writes
    user_db = get_user_supabase_client(user)

    # Check if reaction already exists
    existing_response = (
        user_db.schema("social")
        .table("reactions")
        .select("*")
        .eq("post_id", str(post_id))
        .eq("user_id", user["id"])
        .eq("reaction", reaction_toggle.reaction)
        .execute()
    )
    existing = get_list_result(existing_response, "checking reaction")

    # Determine room for event
    room = get_discussion_room(str(thread["episode_id"]))

    if existing:
        # Remove existing reaction
        delete_response = (
            user_db.schema("social")
            .table("reactions")
            .delete()
            .eq("post_id", str(post_id))
            .eq("user_id", user["id"])
            .eq("reaction", reaction_toggle.reaction)
            .execute()
        )
        raise_for_supabase_error(delete_response, "removing reaction")

        # Publish event
        event = reaction_toggled_event(str(post_id), user["id"], reaction_toggle.reaction, "removed")
        background_tasks.add_task(publish_event_sync, room, event.to_dict())

        return {"action": "removed", "reaction": reaction_toggle.reaction}
    else:
        # Add new reaction
        insert_response = (
            user_db.schema("social")
            .table("reactions")
            .insert(
                {
                    "post_id": str(post_id),
                    "user_id": user["id"],
                    "reaction": reaction_toggle.reaction,
                }
            )
            .execute()
        )
        raise_for_supabase_error(insert_response, "adding reaction")

        # Publish event
        event = reaction_toggled_event(str(post_id), user["id"], reaction_toggle.reaction, "added")
        background_tasks.add_task(publish_event_sync, room, event.to_dict())

        return {"action": "added", "reaction": reaction_toggle.reaction}


@router.get("/posts/{post_id}/reactions", response_model=dict)
def get_post_reactions(db: SupabaseClient, post_id: UUID) -> dict:
    """
    Get reaction counts for a specific post.
    Public endpoint - no auth required.
    """
    # Verify post exists
    post_response = db.schema("social").table("posts").select("id").eq("id", str(post_id)).single().execute()
    require_single_result(post_response, "Post")

    # Get reactions
    reactions_response = db.schema("social").table("reactions").select("reaction").eq("post_id", str(post_id)).execute()
    reactions = get_list_result(reactions_response, "fetching reactions")

    # Count by type
    counts: dict[str, int] = {}
    for r in reactions:
        rtype = r["reaction"]
        counts[rtype] = counts.get(rtype, 0) + 1

    return {"post_id": str(post_id), "reactions": counts}
