from __future__ import annotations

import re
import time
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from supabase import Client
from trr_backend.db.postgrest_cache import is_pgrst204_error, reload_postgrest_schema
from trr_backend.models.shows import ShowUpsert


class ShowRepositoryError(RuntimeError):
    pass


_MISSING_COLUMN_RE = re.compile(r"could not find the '([^']+)' column", re.IGNORECASE)

# PGRST204 retry configuration
_PGRST204_MAX_RETRIES = 1
_PGRST204_RETRY_DELAY = 0.5


def _missing_column_from_error(message: str) -> str | None:
    match = _MISSING_COLUMN_RE.search(message or "")
    return match.group(1) if match else None


def _handle_pgrst204_with_retry(exc: Exception, attempt: int, context: str) -> bool:
    """
    Handle PGRST204 schema cache errors with retry.

    Returns True if retry should be attempted, False otherwise.
    Raises the exception with a helpful hint if retries are exhausted.
    """
    if not is_pgrst204_error(exc):
        return False

    if attempt >= _PGRST204_MAX_RETRIES:
        hint = (
            f"\n\nPostgREST schema cache may still be stale after retry during {context}. "
            "Wait 30-60s and try again, or run:\n"
            '  psql "$SUPABASE_DB_URL" -f scripts/db/reload_postgrest_schema.sql'
        )
        raise ShowRepositoryError(f"{exc}{hint}") from exc

    # Trigger schema reload and retry
    try:
        reload_postgrest_schema()
    except Exception:
        pass  # Best effort - continue with retry anyway

    time.sleep(_PGRST204_RETRY_DELAY)
    return True


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

    def is_schema_not_exposed(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "pgrst106" in msg  # postgrest: invalid schema
            or ("invalid schema" in msg and "core" in msg)
            or ("schemas are exposed" in msg and "public" in msg)
        )

    def help_message() -> str:
        return (
            "Database table `core.shows` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0004_core_shows.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.shows`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard → Settings → API → Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("shows").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise ShowRepositoryError(schema_help_message()) from exc
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
    if is_schema_not_exposed(combined):
        raise ShowRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise ShowRepositoryError(help_message())
    raise ShowRepositoryError(f"Supabase error during core.shows preflight: {combined}")


def _raise_for_supabase_error(response: Any, context: str) -> None:
    if hasattr(response, "error") and response.error:
        raise ShowRepositoryError(f"Supabase error during {context}: {response.error}")


def find_show_by_imdb_id(db: Client, imdb_id: str) -> dict[str, Any] | None:
    response = db.schema("core").table("shows").select("*").eq("imdb_id", imdb_id).limit(1).execute()
    _raise_for_supabase_error(response, "finding show by imdb id")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None


def find_show_by_tmdb_id(db: Client, tmdb_id: int) -> dict[str, Any] | None:
    response = db.schema("core").table("shows").select("*").eq("tmdb_id", int(tmdb_id)).limit(1).execute()
    _raise_for_supabase_error(response, "finding show by tmdb id")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None


def insert_show(db: Client, show: ShowUpsert) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": show.name,
        "description": show.description,
        "premiere_date": show.premiere_date,
    }
    if show.show_total_seasons is not None:
        payload["show_total_seasons"] = int(show.show_total_seasons)
    if show.show_total_episodes is not None:
        payload["show_total_episodes"] = int(show.show_total_episodes)
    if show.imdb_id is not None:
        payload["imdb_id"] = show.imdb_id
    if show.tmdb_id is not None:
        payload["tmdb_id"] = int(show.tmdb_id)
    if show.most_recent_episode is not None:
        payload["most_recent_episode"] = show.most_recent_episode
    if show.most_recent_episode_season is not None:
        payload["most_recent_episode_season"] = int(show.most_recent_episode_season)
    if show.most_recent_episode_number is not None:
        payload["most_recent_episode_number"] = int(show.most_recent_episode_number)
    if show.most_recent_episode_title is not None:
        payload["most_recent_episode_title"] = show.most_recent_episode_title
    if show.most_recent_episode_air_date is not None:
        payload["most_recent_episode_air_date"] = show.most_recent_episode_air_date
    if show.most_recent_episode_imdb_id is not None:
        payload["most_recent_episode_imdb_id"] = show.most_recent_episode_imdb_id
    if show.needs_imdb_resolution is not None:
        payload["needs_imdb_resolution"] = bool(show.needs_imdb_resolution)
    if show.needs_tmdb_resolution is not None:
        payload["needs_tmdb_resolution"] = bool(show.needs_tmdb_resolution)

    # Array columns (only include if not None/empty to avoid overwriting)
    if show.genres:
        payload["genres"] = list(show.genres)
    if show.keywords:
        payload["keywords"] = list(show.keywords)
    if show.tags:
        payload["tags"] = list(show.tags)
    if show.networks:
        payload["networks"] = list(show.networks)
    if show.streaming_providers:
        payload["streaming_providers"] = list(show.streaming_providers)
    if show.listed_on:
        payload["listed_on"] = list(show.listed_on)

    # External IDs
    if show.tvdb_id is not None:
        payload["tvdb_id"] = int(show.tvdb_id)
    if show.tvrage_id is not None:
        payload["tvrage_id"] = int(show.tvrage_id)
    if show.wikidata_id is not None:
        payload["wikidata_id"] = show.wikidata_id
    if show.facebook_id is not None:
        payload["facebook_id"] = show.facebook_id
    if show.instagram_id is not None:
        payload["instagram_id"] = show.instagram_id
    if show.twitter_id is not None:
        payload["twitter_id"] = show.twitter_id

    for attempt in range(_PGRST204_MAX_RETRIES + 1):
        try:
            response = db.schema("core").table("shows").insert(payload).execute()
            break
        except Exception as exc:
            # Check for PGRST204 first (schema cache error)
            if _handle_pgrst204_with_retry(exc, attempt, "inserting show"):
                continue

            # Handle missing column errors
            missing = _missing_column_from_error(str(exc))
            if missing and missing in payload:
                payload.pop(missing, None)
                response = db.schema("core").table("shows").insert(payload).execute()
                break
            else:
                raise ShowRepositoryError(f"Supabase error during inserting show: {exc}") from exc

    _raise_for_supabase_error(response, "inserting show")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise ShowRepositoryError("Supabase insert returned no data for show.")


def update_show(db: Client, show_id: UUID | str, patch: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(patch)
    for attempt in range(_PGRST204_MAX_RETRIES + 1):
        try:
            response = db.schema("core").table("shows").update(payload).eq("id", str(show_id)).execute()
            break
        except Exception as exc:
            # Check for PGRST204 first (schema cache error)
            if _handle_pgrst204_with_retry(exc, attempt, "updating show"):
                continue

            # Handle missing column errors
            missing = _missing_column_from_error(str(exc))
            if missing and missing in payload:
                payload.pop(missing, None)
                if not payload:
                    response = db.schema("core").table("shows").select("*").eq("id", str(show_id)).limit(1).execute()
                    _raise_for_supabase_error(response, "finding show after missing column skip")
                    data = response.data or []
                    if isinstance(data, list) and data:
                        return data[0]
                    raise ShowRepositoryError("Supabase fetch returned no data for show after missing column skip.")
                response = db.schema("core").table("shows").update(payload).eq("id", str(show_id)).execute()
                break
            else:
                raise ShowRepositoryError(f"Supabase error during updating show: {exc}") from exc

    _raise_for_supabase_error(response, "updating show")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise ShowRepositoryError("Supabase update returned no data for show.")


def merge_shows(db: Client, *, source_show_id: UUID | str, target_show_id: UUID | str) -> None:
    try:
        response = (
            db.schema("core")
            .rpc(
                "merge_shows",
                {"source_show_id": str(source_show_id), "target_show_id": str(target_show_id)},
            )
            .execute()
        )
    except Exception as exc:
        raise ShowRepositoryError(f"Supabase error during merging shows: {exc}") from exc
    _raise_for_supabase_error(response, "merging shows")
