from __future__ import annotations

from typing import Any, Iterable, Mapping

from supabase import Client


class CastMembershipRepositoryError(RuntimeError):
    pass


def assert_core_cast_memberships_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.cast_memberships` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "cast_memberships" in msg)
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
            "Database table `core.cast_memberships` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0001_init.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.cast_memberships`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard -> Settings -> API -> Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("cast_memberships").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise CastMembershipRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise CastMembershipRepositoryError(help_message()) from exc
        raise CastMembershipRepositoryError(f"Supabase error during core.cast_memberships preflight: {exc}") from exc

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
        raise CastMembershipRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise CastMembershipRepositoryError(help_message())
    raise CastMembershipRepositoryError(f"Supabase error during core.cast_memberships preflight: {combined}")


def upsert_cast_memberships(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: str = "show_id,person_id,role",
) -> list[dict[str, Any]]:
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []

    response = db.schema("core").table("cast_memberships").upsert(payload, on_conflict=on_conflict).execute()
    if hasattr(response, "error") and response.error:
        raise CastMembershipRepositoryError(f"Supabase error upserting cast memberships: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def delete_cast_memberships_for_show(db: Client, *, show_id: str) -> None:
    response = db.schema("core").table("cast_memberships").delete().eq("show_id", str(show_id)).execute()
    if hasattr(response, "error") and response.error:
        raise CastMembershipRepositoryError(
            f"Supabase error deleting cast memberships for show_id={show_id}: {response.error}"
        )
