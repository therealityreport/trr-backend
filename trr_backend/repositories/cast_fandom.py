from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from supabase import Client
from trr_backend.db.postgrest_cache import is_pgrst204_error, reload_postgrest_schema


class CastFandomRepositoryError(RuntimeError):
    pass


_PGRST204_MAX_RETRIES = 1
_PGRST204_RETRY_DELAY = 0.5


def _handle_pgrst204_with_retry(exc: Exception, attempt: int, context: str) -> bool:
    if not is_pgrst204_error(exc):
        return False

    if attempt >= _PGRST204_MAX_RETRIES:
        hint = (
            f"\n\nPostgREST schema cache may still be stale after retry during {context}. "
            "Wait 30-60s and try again, or run:\n"
            '  psql "$SUPABASE_DB_URL" -f scripts/db/reload_postgrest_schema.sql'
        )
        raise CastFandomRepositoryError(f"{exc}{hint}") from exc

    try:
        reload_postgrest_schema()
    except Exception:
        pass

    time.sleep(_PGRST204_RETRY_DELAY)
    return True


def assert_core_cast_fandom_table_exists(db: Client) -> None:
    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "cast_fandom" in msg)
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
            "Database table `core.cast_fandom` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0041_create_cast_fandom_and_extend_cast_photos.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.cast_fandom`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard -> Settings -> API -> Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("cast_fandom").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise CastFandomRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise CastFandomRepositoryError(help_message()) from exc
        raise CastFandomRepositoryError(f"Supabase error during core.cast_fandom preflight: {exc}") from exc

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
        raise CastFandomRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise CastFandomRepositoryError(help_message())
    raise CastFandomRepositoryError(f"Supabase error during core.cast_fandom preflight: {combined}")


def upsert_cast_fandom(db: Client, row: Mapping[str, Any]) -> dict[str, Any]:
    payload = {k: v for k, v in dict(row).items() if v is not None}
    if not payload:
        raise CastFandomRepositoryError("cast_fandom upsert payload is empty.")

    for attempt in range(_PGRST204_MAX_RETRIES + 1):
        try:
            response = db.schema("core").table("cast_fandom").upsert(payload, on_conflict="person_id,source").execute()
            break
        except Exception as exc:
            if _handle_pgrst204_with_retry(exc, attempt, "upserting cast_fandom"):
                continue
            raise CastFandomRepositoryError(f"Supabase error upserting cast_fandom: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastFandomRepositoryError(f"Supabase error upserting cast_fandom: {response.error}")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise CastFandomRepositoryError("Supabase cast_fandom upsert returned no data.")
