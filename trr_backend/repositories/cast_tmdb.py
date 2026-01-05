"""Repository for cast_tmdb table operations."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from supabase import Client


class CastTMDbRepositoryError(RuntimeError):
    """Error during cast_tmdb repository operations."""
    pass


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Serialize a row for Supabase, handling UUIDs."""
    cleaned: dict[str, Any] = {}
    for key, value in row.items():
        if value is None:
            continue
        if isinstance(value, UUID):
            cleaned[key] = str(value)
        else:
            cleaned[key] = value
    return cleaned


def upsert_cast_tmdb(
    db: Client,
    row: dict[str, Any],
) -> dict[str, Any]:
    """
    Upsert a cast_tmdb record.

    Args:
        db: Supabase client
        row: Row data with person_id, tmdb_id, and other fields

    Returns:
        The upserted row
    """
    payload = _serialize_row(row)
    if not payload.get("person_id"):
        raise CastTMDbRepositoryError("person_id is required")
    if not payload.get("tmdb_id"):
        raise CastTMDbRepositoryError("tmdb_id is required")

    try:
        response = (
            db.schema("core")
            .table("cast_tmdb")
            .upsert(payload, on_conflict="person_id")
            .execute()
        )
    except Exception as exc:
        raise CastTMDbRepositoryError(f"Supabase error upserting cast_tmdb: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastTMDbRepositoryError(f"Supabase error upserting cast_tmdb: {response.error}")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return payload


def get_cast_tmdb_by_person_id(
    db: Client,
    person_id: str,
) -> dict[str, Any] | None:
    """
    Get cast_tmdb record by person_id.

    Args:
        db: Supabase client
        person_id: Person UUID

    Returns:
        The record or None if not found
    """
    try:
        response = (
            db.schema("core")
            .table("cast_tmdb")
            .select("*")
            .eq("person_id", str(person_id))
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise CastTMDbRepositoryError(f"Supabase error fetching cast_tmdb: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastTMDbRepositoryError(f"Supabase error fetching cast_tmdb: {response.error}")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None


def get_cast_tmdb_by_tmdb_id(
    db: Client,
    tmdb_id: int,
) -> dict[str, Any] | None:
    """
    Get cast_tmdb record by TMDb ID.

    Args:
        db: Supabase client
        tmdb_id: TMDb person ID

    Returns:
        The record or None if not found
    """
    try:
        response = (
            db.schema("core")
            .table("cast_tmdb")
            .select("*")
            .eq("tmdb_id", int(tmdb_id))
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise CastTMDbRepositoryError(f"Supabase error fetching cast_tmdb: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastTMDbRepositoryError(f"Supabase error fetching cast_tmdb: {response.error}")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None


def fetch_people_missing_tmdb(
    db: Client,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Fetch people who have a TMDb ID in external_ids but no cast_tmdb record.

    Args:
        db: Supabase client
        limit: Maximum number of records to return

    Returns:
        List of people records
    """
    # Get people with TMDb IDs who don't have cast_tmdb records yet
    # This requires a LEFT JOIN check
    try:
        # First, get people with TMDb IDs in external_ids
        response = (
            db.schema("core")
            .table("people")
            .select("id,full_name,external_ids")
            .limit(limit * 2)  # Fetch more since we'll filter
            .execute()
        )
    except Exception as exc:
        raise CastTMDbRepositoryError(f"Supabase error fetching people: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastTMDbRepositoryError(f"Supabase error fetching people: {response.error}")

    people = response.data or []
    if not isinstance(people, list):
        return []

    # Filter to those with TMDb IDs
    people_with_tmdb = []
    for person in people:
        external_ids = person.get("external_ids") or {}
        tmdb_id = external_ids.get("tmdb_id") or external_ids.get("tmdb")
        if tmdb_id:
            person["_tmdb_id"] = int(tmdb_id)
            people_with_tmdb.append(person)

    if not people_with_tmdb:
        return []

    # Check which ones already have cast_tmdb records
    person_ids = [p["id"] for p in people_with_tmdb]
    try:
        existing_response = (
            db.schema("core")
            .table("cast_tmdb")
            .select("person_id")
            .in_("person_id", person_ids)
            .execute()
        )
    except Exception as exc:
        raise CastTMDbRepositoryError(f"Supabase error checking cast_tmdb: {exc}") from exc

    existing_ids = set()
    if existing_response.data:
        existing_ids = {r["person_id"] for r in existing_response.data}

    # Return people who don't have cast_tmdb records
    missing = [p for p in people_with_tmdb if p["id"] not in existing_ids]
    return missing[:limit]


def fetch_cast_tmdb_needing_refresh(
    db: Client,
    *,
    days_old: int = 30,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Fetch cast_tmdb records that haven't been refreshed recently.

    Args:
        db: Supabase client
        days_old: Consider records older than this many days as needing refresh
        limit: Maximum number of records to return

    Returns:
        List of cast_tmdb records
    """
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_old)
    cutoff_str = cutoff.isoformat()

    try:
        response = (
            db.schema("core")
            .table("cast_tmdb")
            .select("*")
            .lt("fetched_at", cutoff_str)
            .order("fetched_at")
            .limit(limit)
            .execute()
        )
    except Exception as exc:
        raise CastTMDbRepositoryError(f"Supabase error fetching cast_tmdb: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastTMDbRepositoryError(f"Supabase error fetching cast_tmdb: {response.error}")

    data = response.data or []
    return data if isinstance(data, list) else []
