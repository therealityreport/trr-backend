from __future__ import annotations

from typing import Any, Iterable, Mapping

from supabase import Client


class ImdbSeriesRepositoryError(RuntimeError):
    pass


def _raise_for_supabase_error(response: Any, context: str) -> None:
    if hasattr(response, "error") and response.error:
        raise ImdbSeriesRepositoryError(f"Supabase error during {context}: {response.error}")


def upsert_imdb_series(db: Client, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    payload = [dict(r) for r in rows]
    if not payload:
        return []
    response = db.schema("core").table("imdb_series").upsert(payload, on_conflict="imdb_id").execute()
    _raise_for_supabase_error(response, "upserting imdb_series")
    data = response.data or []
    return data if isinstance(data, list) else []
