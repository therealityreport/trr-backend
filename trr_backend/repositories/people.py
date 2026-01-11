from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from supabase import Client


class PeopleRepositoryError(RuntimeError):
    pass


def assert_core_people_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.people` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "people" in msg)
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
            "Database table `core.people` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0001_init.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.people`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard -> Settings -> API -> Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("people").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise PeopleRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise PeopleRepositoryError(help_message()) from exc
        raise PeopleRepositoryError(f"Supabase error during core.people preflight: {exc}") from exc

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
        raise PeopleRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise PeopleRepositoryError(help_message())
    raise PeopleRepositoryError(f"Supabase error during core.people preflight: {combined}")


def fetch_people_by_imdb_ids(db: Client, imdb_ids: Iterable[str]) -> list[dict[str, Any]]:
    ids = [str(i).strip() for i in imdb_ids if str(i).strip()]
    if not ids:
        return []

    rows: list[dict[str, Any]] = []
    chunk_size = 200
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        response = (
            db.schema("core")
            .table("people")
            .select("id,full_name,known_for,external_ids")
            .in_("external_ids->>imdb", chunk)
            .execute()
        )
        if hasattr(response, "error") and response.error:
            raise PeopleRepositoryError(f"Supabase error listing people: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)
    return rows


def insert_people(db: Client, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []

    response = db.schema("core").table("people").insert(payload).execute()
    if hasattr(response, "error") and response.error:
        raise PeopleRepositoryError(f"Supabase error inserting people: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []
