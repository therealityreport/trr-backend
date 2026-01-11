from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from supabase import Client


class SyncStateRepositoryError(RuntimeError):
    pass


def assert_core_sync_state_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.sync_state` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "sync_state" in msg)
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
            "Database table `core.sync_state` is missing. "
            "Run `supabase db push` to apply migrations "
            "(see `supabase/migrations/0025_sync_state.sql`), "
            "then re-run the sync job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the sync job cannot access `core.sync_state`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard -> Settings -> API -> Exposed schemas), then re-run the sync job."
        )

    try:
        response = db.schema("core").table("sync_state").select("show_id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise SyncStateRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise SyncStateRepositoryError(help_message()) from exc
        raise SyncStateRepositoryError(f"Supabase error during core.sync_state preflight: {exc}") from exc

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
        raise SyncStateRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise SyncStateRepositoryError(help_message())
    raise SyncStateRepositoryError(f"Supabase error during core.sync_state preflight: {combined}")


def _normalize_table_name(value: str) -> str:
    return str(value or "").strip()


def _coerce_show_id(value: object) -> str:
    return str(value or "").strip()


def _truncate_error(value: object, *, max_length: int = 1000) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text[: max(1, int(max_length))]


def fetch_sync_state_map(
    db: Client,
    *,
    table_name: str,
    show_ids: Iterable[str],
    chunk_size: int = 200,
) -> dict[str, dict[str, Any]]:
    table_name = _normalize_table_name(table_name)
    ids = [_coerce_show_id(show_id) for show_id in show_ids if _coerce_show_id(show_id)]
    if not table_name or not ids:
        return {}

    results: dict[str, dict[str, Any]] = {}
    for i in range(0, len(ids), max(1, int(chunk_size))):
        chunk = ids[i : i + max(1, int(chunk_size))]
        response = (
            db.schema("core")
            .table("sync_state")
            .select("show_id,table_name,status,last_success_at,last_seen_most_recent_episode,last_error")
            .eq("table_name", table_name)
            .in_("show_id", chunk)
            .execute()
        )
        if hasattr(response, "error") and response.error:
            raise SyncStateRepositoryError(f"Supabase error listing sync_state rows for {table_name}: {response.error}")
        data = response.data or []
        if not isinstance(data, list):
            continue
        for row in data:
            show_id = _coerce_show_id(row.get("show_id"))
            if show_id:
                results[show_id] = row
    return results


def _upsert_sync_state(
    db: Client,
    *,
    table_name: str,
    show_id: str,
    status: str,
    last_success_at: str | None = None,
    last_seen_most_recent_episode: str | None = None,
    last_error: str | None = None,
) -> None:
    table_name = _normalize_table_name(table_name)
    show_id = _coerce_show_id(show_id)
    if not table_name or not show_id:
        raise SyncStateRepositoryError("sync_state update requires table_name and show_id.")

    payload: dict[str, Any] = {
        "table_name": table_name,
        "show_id": show_id,
        "status": str(status or "").strip() or "in_progress",
        "last_success_at": last_success_at,
        "last_seen_most_recent_episode": last_seen_most_recent_episode,
        "last_error": _truncate_error(last_error),
    }

    response = db.schema("core").table("sync_state").upsert(payload, on_conflict="table_name,show_id").execute()
    if hasattr(response, "error") and response.error:
        raise SyncStateRepositoryError(
            f"Supabase error upserting sync_state for {table_name} show_id={show_id}: {response.error}"
        )


def mark_sync_state_in_progress(db: Client, *, table_name: str, show_id: str) -> None:
    _upsert_sync_state(db, table_name=table_name, show_id=show_id, status="in_progress", last_error=None)


def mark_sync_state_success(
    db: Client,
    *,
    table_name: str,
    show_id: str,
    last_seen_most_recent_episode: str | None,
) -> None:
    now = datetime.now(UTC).isoformat()
    _upsert_sync_state(
        db,
        table_name=table_name,
        show_id=show_id,
        status="success",
        last_success_at=now,
        last_seen_most_recent_episode=last_seen_most_recent_episode,
        last_error=None,
    )


def mark_sync_state_failed(
    db: Client,
    *,
    table_name: str,
    show_id: str,
    error: object,
) -> None:
    _upsert_sync_state(
        db,
        table_name=table_name,
        show_id=show_id,
        status="failed",
        last_error=error,
    )
