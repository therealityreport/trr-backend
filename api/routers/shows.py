"""
Core browse endpoints for shows, seasons, episodes, and cast.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Query
from pydantic import BaseModel

from api.deps import (
    SupabaseClient,
    get_list_result,
    require_single_result,
)


router = APIRouter(prefix="/shows", tags=["shows"])


# --- Pydantic models ---

class Show(BaseModel):
    id: UUID
    title: str
    description: str | None
    premiere_date: str | None
    external_ids: dict[str, Any]


class Season(BaseModel):
    id: UUID
    show_id: UUID
    season_number: int
    title: str | None
    premiere_date: str | None
    external_ids: dict[str, Any]


class Episode(BaseModel):
    id: UUID
    season_id: UUID
    episode_number: int
    title: str | None
    air_date: str | None
    synopsis: str | None
    external_ids: dict[str, Any]


class Person(BaseModel):
    id: UUID
    full_name: str
    known_for: str | None
    external_ids: dict[str, Any]


class CastMember(BaseModel):
    id: UUID
    show_id: UUID
    season_id: UUID | None
    person_id: UUID
    role: str
    billing_order: int | None
    notes: str | None
    person: Person | None = None


# --- Endpoints ---

@router.get("", response_model=list[Show])
def list_shows(
    db: SupabaseClient,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """List all shows with pagination."""
    response = (
        db.schema("core")
        .table("shows")
        .select("*")
        .order("title")
        .range(offset, offset + limit - 1)
        .execute()
    )
    return get_list_result(response, "listing shows")


@router.get("/{show_id}", response_model=Show)
def get_show(db: SupabaseClient, show_id: UUID) -> dict:
    """Get a specific show by ID."""
    response = (
        db.schema("core")
        .table("shows")
        .select("*")
        .eq("id", str(show_id))
        .single()
        .execute()
    )
    return require_single_result(response, "Show")


@router.get("/{show_id}/seasons", response_model=list[Season])
def list_seasons(
    db: SupabaseClient,
    show_id: UUID,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """List all seasons for a show."""
    response = (
        db.schema("core")
        .table("seasons")
        .select("*")
        .eq("show_id", str(show_id))
        .order("season_number")
        .range(offset, offset + limit - 1)
        .execute()
    )
    return get_list_result(response, "listing seasons")


@router.get("/{show_id}/seasons/{season_number}", response_model=Season)
def get_season(
    db: SupabaseClient,
    show_id: UUID,
    season_number: int,
) -> dict:
    """Get a specific season by show ID and season number."""
    response = (
        db.schema("core")
        .table("seasons")
        .select("*")
        .eq("show_id", str(show_id))
        .eq("season_number", season_number)
        .single()
        .execute()
    )
    return require_single_result(response, "Season")


@router.get("/{show_id}/seasons/{season_number}/episodes", response_model=list[Episode])
def list_episodes(
    db: SupabaseClient,
    show_id: UUID,
    season_number: int,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """List all episodes for a season."""
    # First get the season ID
    season_response = (
        db.schema("core")
        .table("seasons")
        .select("id")
        .eq("show_id", str(show_id))
        .eq("season_number", season_number)
        .single()
        .execute()
    )
    season = require_single_result(season_response, "Season")
    season_id = season["id"]

    # Then get episodes
    response = (
        db.schema("core")
        .table("episodes")
        .select("*")
        .eq("season_id", season_id)
        .order("episode_number")
        .range(offset, offset + limit - 1)
        .execute()
    )
    return get_list_result(response, "listing episodes")


@router.get("/{show_id}/cast", response_model=list[CastMember])
def list_show_cast(
    db: SupabaseClient,
    show_id: UUID,
    season_number: int | None = Query(default=None),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """
    List cast members for a show.
    Optionally filter by season number.
    """
    query = (
        db.schema("core")
        .table("cast_memberships")
        .select("*, person:people(*)")
        .eq("show_id", str(show_id))
    )

    if season_number is not None:
        # Get season ID first
        season_response = (
            db.schema("core")
            .table("seasons")
            .select("id")
            .eq("show_id", str(show_id))
            .eq("season_number", season_number)
            .single()
            .execute()
        )
        season = require_single_result(season_response, "Season")
        query = query.eq("season_id", season["id"])

    response = (
        query
        .order("billing_order", nullsfirst=False)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return get_list_result(response, "listing cast")


@router.get("/people/{person_id}", response_model=Person)
def get_person(db: SupabaseClient, person_id: UUID) -> dict:
    """Get a specific person by ID."""
    response = (
        db.schema("core")
        .table("people")
        .select("*")
        .eq("id", str(person_id))
        .single()
        .execute()
    )
    return require_single_result(response, "Person")
