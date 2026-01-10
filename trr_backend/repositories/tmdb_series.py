from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from supabase import Client


class TmdbSeriesRepositoryError(RuntimeError):
    pass


def _raise_for_supabase_error(response: Any, context: str) -> None:
    if hasattr(response, "error") and response.error:
        raise TmdbSeriesRepositoryError(f"Supabase error during {context}: {response.error}")


def upsert_tmdb_series(db: Client, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    payload = [dict(r) for r in rows]
    if not payload:
        return []
    response = db.schema("core").table("tmdb_series").upsert(payload, on_conflict="tmdb_id").execute()
    _raise_for_supabase_error(response, "upserting tmdb_series")
    data = response.data or []
    return data if isinstance(data, list) else []


def fetch_tmdb_series(db: Client, *, tmdb_id: int) -> dict[str, Any] | None:
    response = db.schema("core").table("tmdb_series").select("*").eq("tmdb_id", int(tmdb_id)).limit(1).execute()
    _raise_for_supabase_error(response, "fetching tmdb_series")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return None
