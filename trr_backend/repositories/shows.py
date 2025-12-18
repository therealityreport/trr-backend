from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

from supabase import Client

from trr_backend.models.shows import ShowUpsert


class ShowRepositoryError(RuntimeError):
    pass


def assert_core_shows_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.shows` is missing in Supabase.

    This avoids confusing downstream failures when running ingestion jobs.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg  # undefined_table
            or "pgrst205" in msg  # postgrest: relation not found in schema cache
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "shows" in msg)
            or ("could not find" in msg and "relation" in msg)
        )

    def help_message() -> str:
        return (
            "Database table `core.shows` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0004_core_shows.sql`), "
            "then re-run the import job."
        )

    try:
        response = db.schema("core").table("shows").select("id").limit(1).execute()
    except Exception as exc:
        if is_missing_relation(str(exc)):
            raise ShowRepositoryError(help_message()) from exc
        raise ShowRepositoryError(f"Supabase error during core.shows preflight: {exc}") from exc

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
    if is_missing_relation(combined):
        raise ShowRepositoryError(help_message())
    raise ShowRepositoryError(f"Supabase error during core.shows preflight: {combined}")


def _raise_for_supabase_error(response: Any, context: str) -> None:
    if hasattr(response, "error") and response.error:
        raise ShowRepositoryError(f"Supabase error during {context}: {response.error}")


def find_show_by_imdb_id(db: Client, imdb_id: str) -> dict[str, Any] | None:
    response = (
        db.schema("core")
        .table("shows")
        .select("*")
        .eq("external_ids->>imdb", imdb_id)
        .limit(1)
        .execute()
    )
    _raise_for_supabase_error(response, "finding show by imdb id")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None


def find_show_by_tmdb_id(db: Client, tmdb_id: int) -> dict[str, Any] | None:
    response = (
        db.schema("core")
        .table("shows")
        .select("*")
        .eq("external_ids->>tmdb", str(int(tmdb_id)))
        .limit(1)
        .execute()
    )
    _raise_for_supabase_error(response, "finding show by tmdb id")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None


def insert_show(db: Client, show: ShowUpsert) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "title": show.title,
        "description": show.description,
        "premiere_date": show.premiere_date,
        "external_ids": show.external_ids,
    }
    response = db.schema("core").table("shows").insert(payload).execute()
    _raise_for_supabase_error(response, "inserting show")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise ShowRepositoryError("Supabase insert returned no data for show.")


def update_show(db: Client, show_id: UUID | str, patch: Mapping[str, Any]) -> dict[str, Any]:
    response = db.schema("core").table("shows").update(dict(patch)).eq("id", str(show_id)).execute()
    _raise_for_supabase_error(response, "updating show")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise ShowRepositoryError("Supabase update returned no data for show.")
