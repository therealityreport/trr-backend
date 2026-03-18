from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict
from typing import Any
from uuid import UUID

from trr_backend.db.postgrest_cache import is_pgrst204_error, reload_postgrest_schema
from trr_backend.db.session import DbSession
from trr_backend.models.cast_photos import CastPhotoUpsert


class CastPhotoRepositoryError(RuntimeError):
    pass


def _is_missing_archived_column_error(error: object) -> bool:
    raw = str(error or "").lower()
    return "archived_at" in raw and "does not exist" in raw


# PGRST204 retry configuration
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
        raise CastPhotoRepositoryError(f"{exc}{hint}") from exc

    try:
        reload_postgrest_schema()
    except Exception:
        pass

    time.sleep(_PGRST204_RETRY_DELAY)
    return True


def assert_core_cast_photos_table_exists(db: DbSession) -> None:
    def is_missing_relation(message: str) -> bool:
        msg = (message or "").casefold()
        return (
            "42p01" in msg
            or "pgrst205" in msg
            or ("relation" in msg and "does not exist" in msg)
            or ("schema cache" in msg and "cast_photos" in msg)
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
            "Database table `core.cast_photos` is missing. "
            "Run `supabase db push` to apply migrations (see `supabase/migrations/0040_create_cast_photos.sql`), "
            "then re-run the import job."
        )

    def schema_help_message() -> str:
        return (
            "Supabase API does not expose schema `core`, so the importer cannot access `core.cast_photos`. "
            "Add `core` to `supabase/config.toml` under `[api].schemas` and run `supabase config push` "
            "(or enable `core` in Supabase Dashboard -> Settings -> API -> Exposed schemas), then re-run the import job."  # noqa: E501
        )

    try:
        response = db.schema("core").table("cast_photos").select("id").limit(1).execute()
    except Exception as exc:
        if is_schema_not_exposed(str(exc)):
            raise CastPhotoRepositoryError(schema_help_message()) from exc
        if is_missing_relation(str(exc)):
            raise CastPhotoRepositoryError(help_message()) from exc
        raise CastPhotoRepositoryError(f"Supabase error during core.cast_photos preflight: {exc}") from exc

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
        raise CastPhotoRepositoryError(schema_help_message())
    if is_missing_relation(combined):
        raise CastPhotoRepositoryError(help_message())
    raise CastPhotoRepositoryError(f"Supabase error during core.cast_photos preflight: {combined}")


def _serialize_row(row: Mapping[str, Any] | CastPhotoUpsert) -> dict[str, Any]:
    from datetime import date, datetime
    from decimal import Decimal
    from enum import Enum

    if isinstance(row, CastPhotoUpsert):
        data = asdict(row)
    else:
        data = dict(row)

    def _serialize_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, Enum):
            return value.value if isinstance(value.value, (str, int, float, bool)) else str(value.value)
        if isinstance(value, Mapping):
            serialized: dict[str, Any] = {}
            for key, nested in value.items():
                serialized_key = _serialize_value(key)
                if not isinstance(serialized_key, str):
                    serialized_key = str(serialized_key)
                serialized[serialized_key] = _serialize_value(nested)
            return serialized
        if isinstance(value, (list, tuple)):
            return [_serialize_value(item) for item in value]
        return value

    cleaned: dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            continue
        cleaned[key] = _serialize_value(value)
    return cleaned


def _normalize_cast_photo_canonical_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.split("?", 1)[0].strip().lower()
    return normalized or None


def _build_upsert_metadata_key(row: Mapping[str, Any], *, dedupe_on: str) -> tuple[str, str, str] | None:
    person_id = str(row.get("person_id") or "").strip()
    source = str(row.get("source") or "").strip().lower() or "imdb"
    if dedupe_on == "source_image_id":
        key_value = str(row.get("source_image_id") or "").strip()
    else:
        key_value = (
            _normalize_cast_photo_canonical_url(
                row.get("image_url_canonical") or row.get("image_url") or row.get("url")
            )
            or ""
        )
    if not person_id or not key_value:
        return None
    return (person_id, source, key_value)


def _merge_upserted_metadata(
    db: DbSession,
    *,
    payload_rows: list[dict[str, Any]],
    upserted_rows: list[dict[str, Any]],
    dedupe_on: str,
) -> None:
    metadata_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for payload_row in payload_rows:
        metadata = payload_row.get("metadata")
        if not isinstance(metadata, dict) or not metadata:
            continue
        key = _build_upsert_metadata_key(payload_row, dedupe_on=dedupe_on)
        if key is None:
            continue
        metadata_by_key[key] = metadata

    if not metadata_by_key:
        return

    for upserted_row in upserted_rows:
        if not isinstance(upserted_row, dict):
            continue
        row_id = str(upserted_row.get("id") or "").strip()
        if not row_id:
            continue
        key = _build_upsert_metadata_key(upserted_row, dedupe_on=dedupe_on)
        if key is None:
            continue
        incoming_metadata = metadata_by_key.get(key)
        if not incoming_metadata:
            continue
        existing_metadata = upserted_row.get("metadata")
        existing_metadata_dict = dict(existing_metadata) if isinstance(existing_metadata, dict) else {}
        merged_metadata = {**existing_metadata_dict, **incoming_metadata}
        if merged_metadata == existing_metadata_dict:
            continue
        db.schema("core").table("cast_photos").update({"metadata": merged_metadata}).eq("id", row_id).execute()
        upserted_row["metadata"] = merged_metadata


def upsert_cast_photos(
    db: DbSession,
    rows: Iterable[Mapping[str, Any] | CastPhotoUpsert],
    *,
    dedupe_on: str = "source_image_id",
) -> list[dict[str, Any]]:
    payload = [_serialize_row(row) for row in rows]
    payload = [row for row in payload if row]
    if not payload:
        return []

    rpc_name = "upsert_cast_photos_by_identity"
    if dedupe_on == "image_url_canonical":
        rpc_name = "upsert_cast_photos_by_canonical"
    elif dedupe_on != "source_image_id":
        raise CastPhotoRepositoryError(f"Unsupported cast photo dedupe key: {dedupe_on}")

    for row in payload:
        if dedupe_on == "image_url_canonical":
            normalized_canonical = _normalize_cast_photo_canonical_url(
                row.get("image_url_canonical") or row.get("image_url") or row.get("url")
            )
            if normalized_canonical:
                row["image_url_canonical"] = normalized_canonical
        if dedupe_on == "source_image_id" and row.get("source") == "imdb" and not row.get("source_image_id"):
            raise CastPhotoRepositoryError("source_image_id is required for IMDb cast photo upserts.")
        if dedupe_on == "image_url_canonical" and not row.get("image_url_canonical"):
            raise CastPhotoRepositoryError("image_url_canonical is required for canonical cast photo upserts.")

    for attempt in range(_PGRST204_MAX_RETRIES + 1):
        try:
            response = db.schema("core").rpc(rpc_name, {"rows": payload}).execute()
            break
        except Exception as exc:
            if _handle_pgrst204_with_retry(exc, attempt, "upserting cast photos"):
                continue
            raise CastPhotoRepositoryError(f"Supabase error upserting cast photos: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastPhotoRepositoryError(f"Supabase error upserting cast photos: {response.error}")
    data = response.data or []
    if not isinstance(data, list):
        return []
    _merge_upserted_metadata(db, payload_rows=payload, upserted_rows=data, dedupe_on=dedupe_on)
    return data


def update_cast_photo_hosted_fields(db: DbSession, photo_id: str, patch: Mapping[str, Any]) -> dict[str, Any]:
    payload = {k: v for k, v in dict(patch).items() if v is not None}
    if not payload:
        raise CastPhotoRepositoryError("Hosted fields update payload is empty.")

    for attempt in range(_PGRST204_MAX_RETRIES + 1):
        try:
            response = db.schema("core").table("cast_photos").update(payload).eq("id", str(photo_id)).execute()
            break
        except Exception as exc:
            if _handle_pgrst204_with_retry(exc, attempt, "updating cast photo hosted fields"):
                continue
            raise CastPhotoRepositoryError(f"Supabase error updating cast photo hosted fields: {exc}") from exc

    if hasattr(response, "error") and response.error:
        raise CastPhotoRepositoryError(f"Supabase error updating cast photo hosted fields: {response.error}")
    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise CastPhotoRepositoryError("Supabase cast photo update returned no data.")


def fetch_cast_photos_missing_hosted(
    db: DbSession,
    *,
    source: str | None = None,
    person_ids: list[str] | None = None,
    limit: int = 200,
    include_hosted: bool = False,
    cdn_base_url: str | None = None,
) -> list[dict[str, Any]]:
    def _base_query(*, include_archived_filter: bool):
        query = (
            db.schema("core")
            .table("cast_photos")
            .select(
                "id,person_id,imdb_person_id,source,source_page_url,image_url,url,thumb_url,"
                "hosted_url,hosted_sha256,hosted_key,hosted_bucket,hosted_content_type"
            )
        )
        if include_archived_filter:
            query = query.is_("archived_at", "null")
        return query

    def _apply_filters(query):
        if source and source != "all":
            query = query.eq("source", source)
        if person_ids:
            query = query.in_("person_id", person_ids)
        if limit is not None:
            query = query.limit(max(0, int(limit)))
        return query

    def _execute_queries(*, include_archived_filter: bool) -> list[dict[str, Any]]:
        queries = []
        base = (cdn_base_url or "").strip().rstrip("/")
        if include_hosted:
            if base:
                missing_query = _apply_filters(_base_query(include_archived_filter=include_archived_filter)).is_(
                    "hosted_url", "null"
                )
                mismatch_query = (
                    _apply_filters(_base_query(include_archived_filter=include_archived_filter))
                    .not_.is_("hosted_url", "null")
                    .not_.ilike(
                        "hosted_url",
                        f"{base}/%",
                    )
                )
                queries.extend([missing_query, mismatch_query])
            else:
                queries.append(_apply_filters(_base_query(include_archived_filter=include_archived_filter)))
        else:
            queries.append(
                _apply_filters(_base_query(include_archived_filter=include_archived_filter)).is_("hosted_url", "null")
            )

        rows: list[dict[str, Any]] = []
        for query in queries:
            response = query.execute()
            if hasattr(response, "error") and response.error:
                raise CastPhotoRepositoryError(f"Supabase error listing cast photos: {response.error}")
            data = response.data or []
            if isinstance(data, list):
                rows.extend(data)
        return rows

    try:
        return _execute_queries(include_archived_filter=True)
    except CastPhotoRepositoryError as exc:
        if not _is_missing_archived_column_error(exc):
            raise
        return _execute_queries(include_archived_filter=False)


def fetch_hosted_keys_for_person(
    db: DbSession,
    person_identifier: str,
) -> set[str]:
    """
    Fetch all hosted_key values for a person's cast photos.

    Used by S3 prune logic to determine which objects are still referenced.

    Args:
        db: Supabase client
        person_identifier: Either IMDb person ID (nm...) or person UUID

    Returns:
        Set of hosted_key values (S3 object keys)
    """
    # Determine if this is an IMDb ID or UUID
    is_imdb_id = person_identifier.startswith("nm")

    if is_imdb_id:
        # Query by imdb_person_id
        query = (
            db.schema("core")
            .table("cast_photos")
            .select("hosted_key")
            .eq("imdb_person_id", person_identifier)
            .not_.is_("hosted_key", "null")
        )
    else:
        # Query by person_id UUID
        query = (
            db.schema("core")
            .table("cast_photos")
            .select("hosted_key")
            .eq("person_id", person_identifier)
            .not_.is_("hosted_key", "null")
        )

    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise CastPhotoRepositoryError(f"Supabase error fetching hosted keys: {response.error}")

    data = response.data or []
    return {row["hosted_key"] for row in data if row.get("hosted_key")}


def fetch_cast_photos_for_person(
    db: DbSession,
    person_id: str,
    *,
    sources: list[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch gallery images for a person from `core.v_person_images_served_media_v2`.

    Args:
        db: Supabase client
        person_id: Person UUID
        sources: Optional list of sources to filter by
        limit: Optional limit on results

    Returns:
        List of cast photo records
    """
    query = (
        db.schema("core")
        .table("v_person_images_served_media_v2")
        .select("*")
        .eq("person_id", person_id)
        .eq("kind", "gallery")
    )

    if sources:
        query = query.in_("source", sources)
    if limit is not None:
        query = query.limit(max(0, int(limit)))

    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise CastPhotoRepositoryError(f"Supabase error fetching cast photos: {response.error}")

    data = response.data or []
    return data if isinstance(data, list) else []
