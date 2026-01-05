from __future__ import annotations

from typing import Any, Iterable, Mapping

from supabase import Client


class ShowCastRepositoryError(RuntimeError):
    pass


def assert_core_show_cast_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.show_cast` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "show_cast" in msg)
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
            "Database table `core.show_cast` is missing. "
            "Run `supabase db push` to apply migrations "
            "(see `supabase/migrations/0018_imdb_cast_episode_appearances.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.show_cast`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard -> Settings -> API -> Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("show_cast").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise ShowCastRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise ShowCastRepositoryError(help_message()) from exc
        raise ShowCastRepositoryError(f"Supabase error during core.show_cast preflight: {exc}") from exc

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
        raise ShowCastRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise ShowCastRepositoryError(help_message())
    raise ShowCastRepositoryError(f"Supabase error during core.show_cast preflight: {combined}")


def delete_show_cast_for_show(db: Client, show_id: str) -> int:
    """Delete all show_cast rows for a given show."""
    response = db.schema("core").table("show_cast").delete().eq("show_id", show_id).execute()
    if hasattr(response, "error") and response.error:
        raise ShowCastRepositoryError(f"Supabase error deleting show_cast rows: {response.error}")
    data = response.data or []
    return len(data) if isinstance(data, list) else 0


def upsert_show_cast(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: str = "show_id,person_id,credit_category",
) -> list[dict[str, Any]]:
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []

    response = db.schema("core").table("show_cast").upsert(payload, on_conflict=on_conflict).execute()
    if hasattr(response, "error") and response.error:
        raise ShowCastRepositoryError(f"Supabase error upserting show_cast rows: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []
