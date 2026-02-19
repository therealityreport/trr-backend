from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from trr_backend.db.session import DbSession


class SeasonFandomRepositoryError(RuntimeError):
    pass


def upsert_season_fandom(db: DbSession, row: Mapping[str, Any]) -> dict[str, Any]:
    payload = {k: v for k, v in dict(row).items() if v is not None}
    if not payload:
        raise SeasonFandomRepositoryError("season_fandom upsert payload is empty.")

    response = db.schema("core").table("season_fandom").upsert(payload, on_conflict="season_id,source").execute()
    if hasattr(response, "error") and response.error:
        raise SeasonFandomRepositoryError(f"Supabase error upserting season_fandom: {response.error}")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise SeasonFandomRepositoryError("Supabase season_fandom upsert returned no data.")


def list_season_fandom(
    db: DbSession,
    *,
    season_id: str,
) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("season_fandom")
        .select("*")
        .eq("season_id", season_id)
        .order("scraped_at", desc=True)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise SeasonFandomRepositoryError(f"Supabase error reading season_fandom: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []
