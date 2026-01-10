from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from uuid import UUID

from supabase import Client


class EpisodeRepositoryError(RuntimeError):
    pass


def assert_core_episodes_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.episodes` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg  # undefined_table
            or "pgrst205" in msg  # postgrest: relation not found in schema cache
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "episodes" in msg)
            or ("could not find" in msg and "relation" in msg)
        )

    def is_schema_not_exposed(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "pgrst106" in msg  # postgrest: invalid schema
            or ("invalid schema" in msg and "core" in msg)
            or ("schemas are exposed" in msg and "public" in msg)
        )

    def help_message() -> str:
        return (
            "Database table `core.episodes` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0012_seasons_and_episodes.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.episodes`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard → Settings → API → Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("episodes").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise EpisodeRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise EpisodeRepositoryError(help_message()) from exc
        raise EpisodeRepositoryError(f"Supabase error during core.episodes preflight: {exc}") from exc

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
        raise EpisodeRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise EpisodeRepositoryError(help_message())
    raise EpisodeRepositoryError(f"Supabase error during core.episodes preflight: {combined}")


def upsert_episodes(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: str = "show_id,season_number,episode_number",
) -> list[dict[str, Any]]:
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []
    response = db.schema("core").table("episodes").upsert(payload, on_conflict=on_conflict).execute()
    if hasattr(response, "error") and response.error:
        raise EpisodeRepositoryError(f"Supabase error upserting episodes: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def fetch_episodes_for_show_season(
    db: Client,
    *,
    show_id: UUID | str,
    season_number: int,
) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("episodes")
        .select("episode_number,title,overview,synopsis,air_date,imdb_episode_id")
        .eq("show_id", str(show_id))
        .eq("season_number", int(season_number))
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise EpisodeRepositoryError(f"Supabase error listing episodes: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def delete_episodes_for_show(db: Client, *, show_id: UUID | str) -> None:
    response = db.schema("core").table("episodes").delete().eq("show_id", str(show_id)).execute()
    if hasattr(response, "error") and response.error:
        raise EpisodeRepositoryError(f"Supabase error deleting episodes for show_id={show_id}: {response.error}")


def delete_episodes_for_tmdb_series(db: Client, *, tmdb_series_id: int) -> None:
    response = db.schema("core").table("episodes").delete().eq("tmdb_series_id", str(int(tmdb_series_id))).execute()
    if hasattr(response, "error") and response.error:
        raise EpisodeRepositoryError(
            f"Supabase error deleting episodes for tmdb_series_id={tmdb_series_id}: {response.error}"
        )
