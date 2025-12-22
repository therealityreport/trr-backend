from __future__ import annotations

from typing import Any, Iterable, Mapping

from supabase import Client


class EpisodeAppearancesRepositoryError(RuntimeError):
    pass


def assert_core_episode_appearances_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.episode_appearances` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "episode_appearances" in msg)
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
            "Database table `core.episode_appearances` is missing. "
            "Run `supabase db push` to apply migrations "
            "(see `supabase/migrations/0018_imdb_cast_episode_appearances.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.episode_appearances`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard -> Settings -> API -> Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("episode_appearances").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise EpisodeAppearancesRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise EpisodeAppearancesRepositoryError(help_message()) from exc
        raise EpisodeAppearancesRepositoryError(
            f"Supabase error during core.episode_appearances preflight: {exc}"
        ) from exc

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
        raise EpisodeAppearancesRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise EpisodeAppearancesRepositoryError(help_message())
    raise EpisodeAppearancesRepositoryError(
        f"Supabase error during core.episode_appearances preflight: {combined}"
    )


def fetch_existing_episode_ids(
    db: Client,
    *,
    show_id: str,
    person_id: str,
    credit_category: str = "Self",
) -> set[str]:
    response = (
        db.schema("core")
        .table("episode_appearances")
        .select("episode_imdb_id")
        .eq("show_id", str(show_id))
        .eq("person_id", str(person_id))
        .eq("credit_category", str(credit_category))
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise EpisodeAppearancesRepositoryError(
            f"Supabase error listing episode appearances for person_id={person_id}: {response.error}"
        )
    data = response.data or []
    if not isinstance(data, list):
        return set()
    return {str(row.get("episode_imdb_id")) for row in data if row.get("episode_imdb_id")}


def upsert_episode_appearances(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: str = "show_id,person_id,episode_imdb_id,credit_category",
    chunk_size: int = 500,
) -> list[dict[str, Any]]:
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []

    results: list[dict[str, Any]] = []
    for i in range(0, len(payload), max(1, int(chunk_size))):
        chunk = payload[i : i + max(1, int(chunk_size))]
        response = db.schema("core").table("episode_appearances").upsert(chunk, on_conflict=on_conflict).execute()
        if hasattr(response, "error") and response.error:
            raise EpisodeAppearancesRepositoryError(
                f"Supabase error upserting episode_appearances rows: {response.error}"
            )
        data = response.data or []
        if isinstance(data, list):
            results.extend(data)
    return results
