from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from supabase import Client


class EpisodeImageRepositoryError(RuntimeError):
    pass


def assert_core_episode_images_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.episode_images` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg  # undefined_table
            or "pgrst205" in msg  # postgrest: relation not found in schema cache
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "episode_images" in msg)
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
            "Database table `core.episode_images` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0067_create_episode_images.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.episode_images`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard → Settings → API → Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("episode_images").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise EpisodeImageRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise EpisodeImageRepositoryError(help_message()) from exc
        raise EpisodeImageRepositoryError(f"Supabase error during core.episode_images preflight: {exc}") from exc

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
        raise EpisodeImageRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise EpisodeImageRepositoryError(help_message())
    raise EpisodeImageRepositoryError(f"Supabase error during core.episode_images preflight: {combined}")


def upsert_episode_images(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: str = "tmdb_series_id,season_number,episode_number,source,file_path",
) -> list[dict[str, Any]]:
    payload = [{k: v for k, v in dict(r).items() if v is not None} for r in rows]
    payload = [r for r in payload if r]
    if not payload:
        return []
    response = db.schema("core").table("episode_images").upsert(payload, on_conflict=on_conflict).execute()
    if hasattr(response, "error") and response.error:
        raise EpisodeImageRepositoryError(f"Supabase error upserting episode images: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def delete_tmdb_episode_images(db: Client, *, tmdb_series_id: int) -> None:
    response = (
        db.schema("core")
        .table("episode_images")
        .delete()
        .eq("source", "tmdb")
        .eq("tmdb_series_id", str(int(tmdb_series_id)))
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise EpisodeImageRepositoryError(
            f"Supabase error deleting episode images for tmdb_series_id={tmdb_series_id}: {response.error}"
        )


def fetch_episode_images_missing_hosted(
    db: Client,
    *,
    show_id: str | None = None,
    episode_id: str | None = None,
    imdb_id: str | None = None,
    tmdb_id: int | None = None,
    season_number: int | None = None,
    episode_number: int | None = None,
    limit: int = 200,
    include_hosted: bool = False,
    cdn_base_url: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch episode images missing hosted URLs (for S3 mirroring).
    """

    def _base_query():
        return (
            db.schema("core")
            .table("episode_images")
            .select(
                "id,show_id,season_id,episode_id,season_number,episode_number,source,file_path,url,url_original,"
                "hosted_url,hosted_sha256,hosted_key,hosted_bucket,hosted_content_type,"
                "hosted_bytes,hosted_etag,hosted_at,"
                "shows:show_id(imdb_id),episodes:episode_id(imdb_id:external_ids->>imdb_id)"
            )
        )

    def _apply_filters(query):
        if show_id:
            query = query.eq("show_id", show_id)
        if episode_id:
            query = query.eq("episode_id", episode_id)
        if imdb_id:
            query = query.eq("shows.imdb_id", imdb_id)
        if tmdb_id is not None:
            query = query.eq("tmdb_series_id", int(tmdb_id))
        if season_number is not None:
            query = query.eq("season_number", int(season_number))
        if episode_number is not None:
            query = query.eq("episode_number", int(episode_number))
        if limit is not None:
            query = query.limit(max(0, int(limit)))
        return query

    queries = []
    base = (cdn_base_url or "").strip().rstrip("/")
    if include_hosted:
        if base:
            missing_query = _apply_filters(_base_query()).is_("hosted_url", "null")
            mismatch_query = (
                _apply_filters(_base_query())
                .not_.is_("hosted_url", "null")
                .not_.like(
                    "hosted_url",
                    f"{base}/%",
                )
            )
            queries.extend([missing_query, mismatch_query])
        else:
            queries.append(_apply_filters(_base_query()))
    else:
        queries.append(_apply_filters(_base_query()).is_("hosted_url", "null"))

    rows: list[dict[str, Any]] = []
    for query in queries:
        response = query.execute()
        if hasattr(response, "error") and response.error:
            raise EpisodeImageRepositoryError(f"Supabase error fetching episode images: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)
    return rows


def update_episode_image_hosted_fields(
    db: Client,
    image_id: str,
    patch: Mapping[str, Any],
) -> dict[str, Any]:
    payload = {k: v for k, v in dict(patch).items() if v is not None}
    if not payload:
        raise EpisodeImageRepositoryError("Hosted fields update payload is empty.")

    response = db.schema("core").table("episode_images").update(payload).eq("id", str(image_id)).execute()
    if hasattr(response, "error") and response.error:
        raise EpisodeImageRepositoryError(f"Supabase error updating episode image hosted fields: {response.error}")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise EpisodeImageRepositoryError("Supabase episode image update returned no data.")


def fetch_hosted_keys_for_show(
    db: Client,
    *,
    show_id: str,
) -> set[str]:
    response = (
        db.schema("core")
        .table("episode_images")
        .select("hosted_key")
        .eq("show_id", show_id)
        .not_.is_("hosted_key", "null")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise EpisodeImageRepositoryError(f"Supabase error fetching hosted keys: {response.error}")
    data = response.data or []
    return {row.get("hosted_key") for row in data if row.get("hosted_key")}


def fetch_hosted_keys_for_episode(
    db: Client,
    *,
    episode_id: str,
) -> set[str]:
    response = (
        db.schema("core")
        .table("episode_images")
        .select("hosted_key")
        .eq("episode_id", episode_id)
        .not_.is_("hosted_key", "null")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise EpisodeImageRepositoryError(f"Supabase error fetching hosted keys: {response.error}")
    data = response.data or []
    return {row.get("hosted_key") for row in data if row.get("hosted_key")}
