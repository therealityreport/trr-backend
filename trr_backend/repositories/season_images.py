from __future__ import annotations

from typing import Any, Iterable, Mapping

from supabase import Client


class SeasonImageRepositoryError(RuntimeError):
    pass


def assert_core_season_images_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.season_images` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg  # undefined_table
            or "pgrst205" in msg  # postgrest: relation not found in schema cache
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "season_images" in msg)
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
            "Database table `core.season_images` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0013_season_images.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.season_images`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard → Settings → API → Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("season_images").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise SeasonImageRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise SeasonImageRepositoryError(help_message()) from exc
        raise SeasonImageRepositoryError(f"Supabase error during core.season_images preflight: {exc}") from exc

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
        raise SeasonImageRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise SeasonImageRepositoryError(help_message())
    raise SeasonImageRepositoryError(f"Supabase error during core.season_images preflight: {combined}")


def upsert_season_images(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: str = "tmdb_series_id,season_number,source,file_path",
) -> list[dict[str, Any]]:
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []
    response = db.schema("core").table("season_images").upsert(payload, on_conflict=on_conflict).execute()
    if hasattr(response, "error") and response.error:
        raise SeasonImageRepositoryError(f"Supabase error upserting season images: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def delete_tmdb_season_images(db: Client, *, tmdb_series_id: int) -> None:
    response = (
        db.schema("core")
        .table("season_images")
        .delete()
        .eq("source", "tmdb")
        .eq("tmdb_series_id", str(int(tmdb_series_id)))
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise SeasonImageRepositoryError(
            f"Supabase error deleting season images for tmdb_series_id={tmdb_series_id}: {response.error}"
        )

