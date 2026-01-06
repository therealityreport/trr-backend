from __future__ import annotations

import time
from typing import Any, Iterable, Mapping

from supabase import Client

from trr_backend.db.postgrest_cache import is_pgrst204_error, reload_postgrest_schema


class ShowImageRepositoryError(RuntimeError):
    pass


# PGRST204 retry configuration
_PGRST204_MAX_RETRIES = 1
_PGRST204_RETRY_DELAY = 0.5


def _handle_pgrst204_with_retry(exc: Exception, attempt: int, context: str) -> bool:
    """
    Handle PGRST204 schema cache errors with retry.

    Returns True if retry should be attempted, False otherwise.
    Raises the exception with a helpful hint if retries are exhausted.
    """
    if not is_pgrst204_error(exc):
        return False

    if attempt >= _PGRST204_MAX_RETRIES:
        hint = (
            f"\n\nPostgREST schema cache may still be stale after retry during {context}. "
            "Wait 30-60s and try again, or run:\n"
            "  psql \"$SUPABASE_DB_URL\" -f scripts/db/reload_postgrest_schema.sql"
        )
        raise ShowImageRepositoryError(f"{exc}{hint}") from exc

    # Trigger schema reload and retry
    try:
        reload_postgrest_schema()
    except Exception:
        pass  # Best effort - continue with retry anyway

    time.sleep(_PGRST204_RETRY_DELAY)
    return True


def assert_core_show_images_table_exists(db: Client) -> None:
    """
    Fail fast with a clear error if `core.show_images` is missing in Supabase.
    """

    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg  # undefined_table
            or "pgrst205" in msg  # postgrest: relation not found in schema cache
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "show_images" in msg)
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
            "Database table `core.show_images` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0005_show_images.sql` "
            "`supabase/migrations/0008_show_images_tmdb_id.sql`, `supabase/migrations/0010_show_images_no_votes.sql`, "
            "and `supabase/migrations/0027_show_images_media_sources.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.show_images`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard → Settings → API → Exposed schemas), then re-run the import job."
        )

    try:
        response = db.schema("core").table("show_images").select("id").limit(1).execute()
    except Exception as exc:
        msg = str(exc).casefold()
        if "permission denied" in msg or "42501" in msg:
            raise ShowImageRepositoryError(
                "Supabase service role lacks access to `core.show_images`. "
                "Apply grants via `supabase db push` (see `supabase/migrations/0006_show_images_grants.sql` "
                "and `supabase/migrations/0011_show_images_view_no_votes.sql`)."
            ) from exc
        if is_schema_not_exposed(str(exc)):
            raise ShowImageRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise ShowImageRepositoryError(help_message()) from exc
        raise ShowImageRepositoryError(f"Supabase error during core.show_images preflight: {exc}") from exc

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
        raise ShowImageRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise ShowImageRepositoryError(help_message())
    raise ShowImageRepositoryError(f"Supabase error during core.show_images preflight: {combined}")


def upsert_show_images(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    payload = [dict(r) for r in rows]
    if not payload:
        return []
    tmdb_rows = [row for row in payload if row.get("source") == "tmdb" and row.get("tmdb_id") is not None]
    other_rows = [row for row in payload if row not in tmdb_rows]

    results: list[dict[str, Any]] = []

    if other_rows:
        for attempt in range(_PGRST204_MAX_RETRIES + 1):
            try:
                response = db.schema("core").rpc("upsert_show_images_by_identity", {"rows": other_rows}).execute()
                break
            except Exception as exc:
                if _handle_pgrst204_with_retry(exc, attempt, "upserting show images (identity)"):
                    continue
                raise ShowImageRepositoryError(
                    f"Supabase error upserting show images (identity constraint): {exc}"
                ) from exc
        if hasattr(response, "error") and response.error:
            raise ShowImageRepositoryError(
                f"Supabase error upserting show images (identity constraint): {response.error}"
            )
        data = response.data or []
        if isinstance(data, list):
            results.extend(data)

    if tmdb_rows:
        for attempt in range(_PGRST204_MAX_RETRIES + 1):
            try:
                response = db.schema("core").rpc("upsert_tmdb_show_images_by_identity", {"rows": tmdb_rows}).execute()
                break
            except Exception as exc:
                if _handle_pgrst204_with_retry(exc, attempt, "upserting show images (tmdb)"):
                    continue
                raise ShowImageRepositoryError(
                    f"Supabase error upserting show images (tmdb constraint): {exc}"
                ) from exc
        if hasattr(response, "error") and response.error:
            raise ShowImageRepositoryError(
                f"Supabase error upserting show images (tmdb constraint): {response.error}"
            )
        data = response.data or []
        if isinstance(data, list):
            results.extend(data)

    return results


def delete_tmdb_show_images(db: Client, *, tmdb_id: int) -> None:
    for attempt in range(_PGRST204_MAX_RETRIES + 1):
        try:
            response = (
                db.schema("core")
                .table("show_images")
                .delete()
                .eq("source", "tmdb")
                .eq("tmdb_id", str(int(tmdb_id)))
                .execute()
            )
            break
        except Exception as exc:
            if _handle_pgrst204_with_retry(exc, attempt, "deleting tmdb show images"):
                continue
            raise ShowImageRepositoryError(f"Supabase error deleting TMDb show images: {exc}") from exc
    if hasattr(response, "error") and response.error:
        raise ShowImageRepositoryError(f"Supabase error deleting TMDb show images: {response.error}")


def fetch_show_images_missing_hosted(
    db: Client,
    *,
    source: str | None = None,
    show_id: str | None = None,
    imdb_id: str | None = None,
    tmdb_id: int | None = None,
    kind: str | None = None,
    limit: int = 200,
    include_hosted: bool = False,
    cdn_base_url: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch show images that are missing hosted URLs (for S3 mirroring).

    Joins with shows table to get show metadata for S3 path building.
    """
    def _base_query():
        return db.schema("core").table("show_images").select(
            "id,show_id,source,source_image_id,kind,file_path,url,url_path,"
            "width,height,caption,position,image_type,tmdb_id,"
            "hosted_url,hosted_sha256,hosted_key,hosted_bucket,hosted_content_type,"
            "shows:show_id(name,imdb_id,tmdb_id)"
        )

    def _apply_filters(query):
        if source and source != "all":
            query = query.eq("source", source)
        if show_id:
            query = query.eq("show_id", show_id)
        if imdb_id:
            # Filter via the joined shows table
            query = query.eq("shows.imdb_id", imdb_id)
        if tmdb_id is not None:
            query = query.eq("tmdb_id", int(tmdb_id))
        if kind:
            query = query.eq("kind", kind)
        if limit is not None:
            query = query.limit(max(0, int(limit)))
        return query

    queries = []
    base = (cdn_base_url or "").strip().rstrip("/")
    if include_hosted:
        if base:
            missing_query = _apply_filters(_base_query()).is_("hosted_url", "null")
            mismatch_query = _apply_filters(_base_query()).not_.is_("hosted_url", "null").not_.like(
                "hosted_url",
                f"{base}/%",
            )
            queries.extend([missing_query, mismatch_query])
        else:
            queries.append(_apply_filters(_base_query()))
    else:
        queries.append(_apply_filters(_base_query()).is_("hosted_url", "null"))

    rows: list[dict[str, Any]] = []
    for query in queries:
        for attempt in range(_PGRST204_MAX_RETRIES + 1):
            try:
                response = query.execute()
                break
            except Exception as exc:
                if _handle_pgrst204_with_retry(exc, attempt, "fetching show images missing hosted"):
                    continue
                raise ShowImageRepositoryError(f"Supabase error fetching show images: {exc}") from exc

        if hasattr(response, "error") and response.error:
            raise ShowImageRepositoryError(f"Supabase error fetching show images: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)

    return rows


def update_show_image_hosted_fields(
    db: Client,
    image_id: str,
    patch: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Update hosted fields for a single show image after S3 upload.
    """
    payload = {k: v for k, v in dict(patch).items() if v is not None}
    if not payload:
        raise ShowImageRepositoryError("Hosted fields update payload is empty.")

    for attempt in range(_PGRST204_MAX_RETRIES + 1):
        try:
            response = (
                db.schema("core")
                .table("show_images")
                .update(payload)
                .eq("id", str(image_id))
                .execute()
            )
            break
        except Exception as exc:
            if _handle_pgrst204_with_retry(exc, attempt, "updating show image hosted fields"):
                continue
            raise ShowImageRepositoryError(f"Supabase error updating show image hosted fields: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise ShowImageRepositoryError(f"Supabase error updating show image hosted fields: {response.error}")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise ShowImageRepositoryError("Supabase show image update returned no data.")


def fetch_hosted_keys_for_show(
    db: Client,
    *,
    show_id: str,
) -> set[str]:
    """
    Fetch all hosted_key values for a show's images.
    """
    response = (
        db.schema("core")
        .table("show_images")
        .select("hosted_key")
        .eq("show_id", show_id)
        .not_.is_("hosted_key", "null")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise ShowImageRepositoryError(f"Supabase error fetching hosted keys: {response.error}")
    data = response.data or []
    return {row.get("hosted_key") for row in data if row.get("hosted_key")}
