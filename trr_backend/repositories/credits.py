"""Repository functions for core.credits and core.credit_occurrences tables."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from typing import Any

from supabase import Client


class CreditsRepositoryError(RuntimeError):
    pass


def is_credits_v2_read_enabled() -> bool:
    """Check if credits v2 read is enabled via environment variable.

    When enabled, consumers should read from v2 views (v_show_cast_from_credits,
    v_episode_appearances_from_credits) instead of legacy tables.
    """
    return os.environ.get("ENABLE_CREDITS_V2_READ", "").lower() in ("1", "true", "yes")


def assert_core_credits_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.credits` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "credits" in msg)
            or ("could not find" in msg and "relation" in msg)
        )

    def is_schema_not_exposed(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "pgrst106" in msg
            or ("invalid schema" in msg and "core" in msg)
            or ("schemas are exposed" in msg and "public" in msg)
        )

    def help_message() -> str:
        return (
            "Database table `core.credits` is missing. "
            "Run `supabase db push` to apply migrations "
            "(see `supabase/migrations/0065_create_credits_tables.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.credits`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Dashboard -> Settings -> API -> Exposed schemas), then re-run."
        )

    try:
        response = db.schema("core").table("credits").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise CreditsRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise CreditsRepositoryError(help_message()) from exc
        raise CreditsRepositoryError(f"Supabase error during core.credits preflight: {exc}") from exc

    error = getattr(response, "error", None)
    if not error:
        return

    parts = [
        str(getattr(error, "code", "") or ""),
        str(getattr(error, "message", "") or ""),
        str(getattr(error, "details", "") or ""),
        str(getattr(error, "hint", "") or ""),
        str(error),
    ]
    combined = " ".join([p for p in parts if p]).strip()
    if is_schema_not_exposed(combined):
        raise CreditsRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise CreditsRepositoryError(help_message())
    raise CreditsRepositoryError(f"Supabase error during core.credits preflight: {combined}")


def assert_core_credit_occurrences_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.credit_occurrences` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "credit_occurrences" in msg)
            or ("could not find" in msg and "relation" in msg)
        )

    def is_schema_not_exposed(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "pgrst106" in msg
            or ("invalid schema" in msg and "core" in msg)
            or ("schemas are exposed" in msg and "public" in msg)
        )

    def help_message() -> str:
        return (
            "Database table `core.credit_occurrences` is missing. "
            "Run `supabase db push` to apply migrations "
            "(see `supabase/migrations/0065_create_credits_tables.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, cannot access `core.credit_occurrences`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Dashboard -> Settings -> API -> Exposed schemas), then re-run."
        )

    try:
        response = db.schema("core").table("credit_occurrences").select("credit_id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise CreditsRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise CreditsRepositoryError(help_message()) from exc
        raise CreditsRepositoryError(f"Supabase error during core.credit_occurrences preflight: {exc}") from exc

    error = getattr(response, "error", None)
    if not error:
        return

    parts = [
        str(getattr(error, "code", "") or ""),
        str(getattr(error, "message", "") or ""),
        str(getattr(error, "details", "") or ""),
        str(getattr(error, "hint", "") or ""),
        str(error),
    ]
    combined = " ".join([p for p in parts if p]).strip()
    if is_schema_not_exposed(combined):
        raise CreditsRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise CreditsRepositoryError(help_message())
    raise CreditsRepositoryError(f"Supabase error during core.credit_occurrences preflight: {combined}")


def upsert_credits(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    chunk_size: int = 500,
) -> list[dict[str, Any]]:
    """
    Upsert rows into core.credits.

    Uses ignore_duplicates=True for ON CONFLICT DO NOTHING behavior since
    PostgREST doesn't support expression-based on_conflict columns like COALESCE(role, '').
    Postgres will automatically use the unique index (credits_unique_idx) for conflict detection.

    Args:
        db: Supabase client
        rows: Iterable of credit row dicts
        chunk_size: Batch size for upserts

    Returns:
        List of upserted rows (excludes duplicates)
    """
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []

    results: list[dict[str, Any]] = []
    for i in range(0, len(payload), max(1, int(chunk_size))):
        chunk = payload[i : i + max(1, int(chunk_size))]
        response = db.schema("core").table("credits").upsert(chunk, ignore_duplicates=True).execute()
        if hasattr(response, "error") and response.error:
            raise CreditsRepositoryError(f"Supabase error upserting credits rows: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            results.extend(data)
    return results


def insert_credits_ignore_conflicts(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    chunk_size: int = 500,
) -> list[dict[str, Any]]:
    """
    Insert rows into core.credits, ignoring conflicts (ON CONFLICT DO NOTHING).

    This is more efficient for bulk backfill where we don't need to update existing rows.
    Note: Supabase upsert with ignoreDuplicates=True is the equivalent.

    Args:
        db: Supabase client
        rows: Iterable of credit row dicts
        chunk_size: Batch size for inserts

    Returns:
        List of inserted rows (excludes duplicates that were ignored)
    """
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []

    results: list[dict[str, Any]] = []
    for i in range(0, len(payload), max(1, int(chunk_size))):
        chunk = payload[i : i + max(1, int(chunk_size))]
        try:
            # Use ignore_duplicates=True for ON CONFLICT DO NOTHING behavior.
            # Note: PostgREST's ignore_duplicates may not work perfectly with expression-based
            # unique indexes (COALESCE(role, '')), so we also catch duplicate key errors.
            response = db.schema("core").table("credits").upsert(chunk, ignore_duplicates=True).execute()
            if hasattr(response, "error") and response.error:
                raise CreditsRepositoryError(f"Supabase error inserting credits rows: {response.error}")
            data = response.data or []
            if isinstance(data, list):
                results.extend(data)
        except Exception as e:
            # Handle duplicate key violation (23505) gracefully - this can happen with
            # expression-based unique indexes where PostgREST's ignore_duplicates fails
            error_str = str(e)
            if "23505" in error_str or "duplicate key" in error_str.lower():
                # Fall back to inserting one row at a time for this chunk
                for row in chunk:
                    try:
                        response = db.schema("core").table("credits").upsert([row], ignore_duplicates=True).execute()
                        if response.data:
                            results.extend(response.data)
                    except Exception as row_e:
                        row_error_str = str(row_e)
                        if "23505" not in row_error_str and "duplicate key" not in row_error_str.lower():
                            raise
                        # Silently skip duplicates
            else:
                raise
    return results


def insert_credit_occurrences_ignore_conflicts(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    chunk_size: int = 500,
) -> list[dict[str, Any]]:
    """
    Insert rows into core.credit_occurrences, ignoring conflicts (ON CONFLICT DO NOTHING).

    Args:
        db: Supabase client
        rows: Iterable of occurrence row dicts with credit_id and episode_id
        chunk_size: Batch size for inserts

    Returns:
        List of inserted rows (excludes duplicates that were ignored)
    """
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []

    results: list[dict[str, Any]] = []
    for i in range(0, len(payload), max(1, int(chunk_size))):
        chunk = payload[i : i + max(1, int(chunk_size))]
        # Use upsert with the primary key columns for ON CONFLICT DO NOTHING behavior
        response = (
            db.schema("core").table("credit_occurrences").upsert(chunk, on_conflict="credit_id,episode_id").execute()
        )
        if hasattr(response, "error") and response.error:
            raise CreditsRepositoryError(f"Supabase error inserting credit_occurrences rows: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            results.extend(data)
    return results


def fetch_credits_by_show(db: Client, show_id: str) -> list[dict[str, Any]]:
    """Fetch all credits for a given show."""
    response = (
        db.schema("core")
        .table("credits")
        .select("id,show_id,person_id,credit_category,role,billing_order,source_type,metadata")
        .eq("show_id", show_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise CreditsRepositoryError(f"Supabase error fetching credits: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def fetch_all_credits(db: Client, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch all credits, optionally limited."""
    query = (
        db.schema("core").table("credits").select("id,show_id,person_id,credit_category,role,billing_order,source_type")
    )
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise CreditsRepositoryError(f"Supabase error fetching credits: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def delete_credits_for_show(db: Client, show_id: str) -> int:
    """Delete all credits for a given show (cascades to occurrences)."""
    response = db.schema("core").table("credits").delete().eq("show_id", show_id).execute()
    if hasattr(response, "error") and response.error:
        raise CreditsRepositoryError(f"Supabase error deleting credits rows: {response.error}")
    data = response.data or []
    return len(data) if isinstance(data, list) else 0


def fetch_show_cast_from_credits(
    db: Client,
    show_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch show cast from v_show_cast_from_credits view.

    This reads from the v2 credits tables via the compatibility view.
    Use this when ENABLE_CREDITS_V2_READ is enabled.

    Args:
        db: Supabase client
        show_id: UUID of the show
        limit: Max results to return
        offset: Offset for pagination

    Returns:
        List of cast member dicts with person data joined
    """
    response = (
        db.schema("core")
        .table("v_show_cast_from_credits")
        .select("*, person:people(*)")
        .eq("show_id", show_id)
        .order("billing_order", nullsfirst=False)
        .range(offset, offset + limit - 1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise CreditsRepositoryError(f"Supabase error fetching show cast from credits: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def fetch_episode_credits(
    db: Client,
    episode_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch credits for an episode from v_episode_credits view.

    This reads from the v2 credits tables via the view.

    Args:
        db: Supabase client
        episode_id: UUID of the episode
        limit: Max results to return
        offset: Offset for pagination

    Returns:
        List of credit dicts with person data
    """
    response = (
        db.schema("core")
        .table("v_episode_credits")
        .select("*")
        .eq("episode_id", episode_id)
        .order("billing_order", nullsfirst=False)
        .range(offset, offset + limit - 1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise CreditsRepositoryError(f"Supabase error fetching episode credits: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def fetch_person_show_seasons(
    db: Client,
    show_id: str,
    person_id: str,
) -> dict[str, Any] | None:
    """Fetch season appearances for a person in a show from v_person_show_seasons view.

    Args:
        db: Supabase client
        show_id: UUID of the show
        person_id: UUID of the person

    Returns:
        Dict with seasons_appeared array and total_episodes, or None if not found
    """
    response = (
        db.schema("core")
        .table("v_person_show_seasons")
        .select("*")
        .eq("show_id", show_id)
        .eq("person_id", person_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise CreditsRepositoryError(f"Supabase error fetching person show seasons: {response.error}")
    data = response.data or []
    if isinstance(data, list) and len(data) > 0:
        return data[0]
    return None
