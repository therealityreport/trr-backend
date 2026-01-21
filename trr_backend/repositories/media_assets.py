from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid5

if TYPE_CHECKING:
    from supabase import Client
else:
    Client = Any

_ASSET_ID_NAMESPACE = UUID("52f296b6-0f8d-4bfb-8f39-6e7e5ea8a3a6")
_LINK_ID_NAMESPACE = UUID("3e73e1b4-6b0f-4cbf-a0f4-9029a4f9f2b7")


def _as_str(value: Any) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in ("true", "1", "yes", "y"):
            return True
        if raw in ("false", "0", "no", "n"):
            return False
    return None


def _compact_dict(payload: Mapping[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        cleaned[key] = value
    return cleaned


def _asset_id_for(source: str, source_asset_id: str | None, source_url: str | None) -> UUID | None:
    if source_asset_id:
        name = f"{source}:asset:{source_asset_id}"
    elif source_url:
        name = f"{source}:url:{source_url}"
    else:
        return None
    return uuid5(_ASSET_ID_NAMESPACE, name)


def _link_id_for(
    *,
    entity_type: str,
    entity_id: str,
    asset_id: str,
    kind: str,
    position: int | None,
    is_primary: bool,
    context: dict[str, Any],
) -> UUID:
    context_json = json.dumps(context, sort_keys=True, separators=(",", ":"))
    name = f"{entity_type}:{entity_id}:{asset_id}:{kind}:{position}:{int(is_primary)}:{context_json}"
    return uuid5(_LINK_ID_NAMESPACE, name)


def transform_show_images_to_media(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    assets_by_id: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []

    for row in rows:
        show_id = _as_str(row.get("show_id"))
        source = _as_str(row.get("source"))
        kind = _as_str(row.get("kind"))
        if not show_id or not source or not kind:
            continue

        source_asset_id = _as_str(row.get("source_image_id")) or _as_str(row.get("file_path"))
        source_url = _as_str(row.get("url")) or _as_str(row.get("source_url"))
        asset_id = _asset_id_for(source, source_asset_id, source_url)
        if asset_id is None:
            continue

        asset_id_str = str(asset_id)
        if asset_id_str not in assets_by_id:
            metadata: dict[str, Any] = {}
            legacy_metadata = row.get("metadata")
            if isinstance(legacy_metadata, dict):
                metadata.update(legacy_metadata)

            vote_average = _as_float(row.get("vote_average"))
            if vote_average is not None and "vote_average" not in metadata:
                metadata["vote_average"] = vote_average
            vote_count = _as_int(row.get("vote_count"))
            if vote_count is not None and "vote_count" not in metadata:
                metadata["vote_count"] = vote_count

            asset = {
                "id": asset_id_str,
                "media_type": "image",
                "source": source,
                "source_asset_id": source_asset_id,
                "source_url": source_url,
                "width": _as_int(row.get("width")),
                "height": _as_int(row.get("height")),
                "caption": _as_str(row.get("caption")),
                "hosted_bucket": _as_str(row.get("hosted_bucket")),
                "hosted_key": _as_str(row.get("hosted_key")),
                "hosted_url": _as_str(row.get("hosted_url")),
                "hosted_sha256": _as_str(row.get("hosted_sha256")),
                "hosted_content_type": _as_str(row.get("hosted_content_type")),
                "hosted_bytes": _as_int(row.get("hosted_bytes")),
                "hosted_etag": _as_str(row.get("hosted_etag")),
                "hosted_at": row.get("hosted_at"),
                "fetched_at": row.get("fetched_at"),
            }
            if metadata:
                asset["metadata"] = metadata
            assets_by_id[asset_id_str] = _compact_dict(asset)

        context = _compact_dict(
            {
                "iso_639_1": _as_str(row.get("iso_639_1")),
                "file_path": _as_str(row.get("file_path")),
                "image_type": _as_str(row.get("image_type")),
                "url_path": _as_str(row.get("url_path")),
                "tmdb_id": _as_str(row.get("tmdb_id")),
            }
        )
        position = _as_int(row.get("position"))
        is_primary = bool(_as_bool(row.get("is_primary")) or False)
        link_id = _link_id_for(
            entity_type="show",
            entity_id=show_id,
            asset_id=asset_id_str,
            kind=kind,
            position=position,
            is_primary=is_primary,
            context=context,
        )
        link = {
            "id": str(link_id),
            "entity_type": "show",
            "entity_id": show_id,
            "media_asset_id": asset_id_str,
            "kind": kind,
            "position": position,
            "is_primary": is_primary,
            "context": context or {},
        }
        links.append(link)

    return list(assets_by_id.values()), links


def transform_person_images_to_media(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    assets_by_id: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []

    for row in rows:
        person_id = _as_str(row.get("person_id"))
        source = _as_str(row.get("source"))
        source_url = _as_str(row.get("url")) or _as_str(row.get("source_url"))
        if not person_id or not source or not source_url:
            continue

        asset_id = _asset_id_for(source, None, source_url)
        if asset_id is None:
            continue

        asset_id_str = str(asset_id)
        if asset_id_str not in assets_by_id:
            asset = {
                "id": asset_id_str,
                "media_type": "image",
                "source": source,
                "source_url": source_url,
                "width": _as_int(row.get("width")),
                "height": _as_int(row.get("height")),
                "caption": _as_str(row.get("caption")),
                "hosted_bucket": _as_str(row.get("hosted_bucket")),
                "hosted_key": _as_str(row.get("hosted_key")),
                "hosted_url": _as_str(row.get("hosted_url")),
                "hosted_sha256": _as_str(row.get("hosted_sha256")),
                "hosted_content_type": _as_str(row.get("hosted_content_type")),
                "hosted_bytes": _as_int(row.get("hosted_bytes")),
                "hosted_etag": _as_str(row.get("hosted_etag")),
                "hosted_at": row.get("hosted_at"),
            }
            assets_by_id[asset_id_str] = _compact_dict(asset)

        position = _as_int(row.get("position"))
        is_primary = _as_bool(row.get("is_primary"))
        if is_primary is None:
            is_primary = True
        link_id = _link_id_for(
            entity_type="person",
            entity_id=person_id,
            asset_id=asset_id_str,
            kind="profile",
            position=position,
            is_primary=is_primary,
            context={},
        )
        link = {
            "id": str(link_id),
            "entity_type": "person",
            "entity_id": person_id,
            "media_asset_id": asset_id_str,
            "kind": "profile",
            "position": position,
            "is_primary": is_primary,
            "context": {},
        }
        links.append(link)

    return list(assets_by_id.values()), links


def upsert_media_assets(db: Client, assets: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    payload = [dict(asset) for asset in assets]
    if not payload:
        return []

    response = (
        db.schema("core").table("media_assets").upsert(payload, on_conflict="id", default_to_null=False).execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting media_assets: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def upsert_media_links(db: Client, links: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    payload = [dict(link) for link in links]
    if not payload:
        return []

    response = db.schema("core").table("media_links").upsert(payload, on_conflict="id", default_to_null=False).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting media_links: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def upsert_media_with_links(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    entity_type: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if entity_type == "show":
        assets, links = transform_show_images_to_media(rows)
    elif entity_type == "person":
        assets, links = transform_person_images_to_media(rows)
    else:
        raise ValueError(f"Unsupported entity_type for media upsert: {entity_type}")

    upserted_assets = upsert_media_assets(db, assets)
    upserted_links = upsert_media_links(db, links)
    return upserted_assets, upserted_links


# ---------------------------------------------------------------------------
# Ingest status helpers for async S3 mirroring (Phase 3)
# ---------------------------------------------------------------------------


def update_ingest_status(
    db: Client,
    asset_id: str,
    status: str,
    *,
    error: str | None = None,
    retry_count: int | None = None,
    failed_at: str | None = None,
    completed_at: str | None = None,
    next_retry_at: str | None = None,
) -> dict[str, Any]:
    """
    Update ingest status fields for a media asset.

    Args:
        db: Supabase client
        asset_id: UUID of the media asset
        status: New status (pending|in_progress|hosted|failed|skipped)
        error: Error message (for failed status)
        retry_count: Number of retry attempts
        failed_at: Timestamp of failure
        completed_at: Timestamp of completion
        next_retry_at: Timestamp for next retry attempt

    Returns:
        Updated asset row
    """
    payload: dict[str, Any] = {"ingest_status": status}

    if error is not None:
        payload["ingest_last_error"] = error
    if retry_count is not None:
        payload["ingest_retry_count"] = retry_count
    if failed_at is not None:
        payload["ingest_failed_at"] = failed_at
    if completed_at is not None:
        payload["ingest_completed_at"] = completed_at
    if next_retry_at is not None:
        payload["ingest_next_retry_at"] = next_retry_at

    # Clear error fields when marking as hosted
    if status == "hosted":
        payload["ingest_last_error"] = None
        payload["ingest_failed_at"] = None
        payload["ingest_next_retry_at"] = None

    response = db.schema("core").table("media_assets").update(payload).eq("id", asset_id).execute()

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error updating ingest status: {response.error}")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return {}


def update_asset_with_mirror_result(
    db: Client,
    asset_id: str,
    *,
    sha256: str,
    hosted_bucket: str,
    hosted_key: str,
    hosted_url: str,
    hosted_bytes: int,
    hosted_content_type: str | None = None,
    hosted_etag: str | None = None,
    completed_at: str | None = None,
) -> dict[str, Any]:
    """
    Update a media asset after successful S3 mirroring.

    Sets all hosted_* fields and marks status as 'hosted'.

    Args:
        db: Supabase client
        asset_id: UUID of the media asset
        sha256: Content hash of the mirrored file
        hosted_bucket: S3 bucket name
        hosted_key: S3 object key
        hosted_url: Public CDN URL
        hosted_bytes: File size in bytes
        hosted_content_type: MIME type
        hosted_etag: S3 ETag
        completed_at: Completion timestamp (ISO format)

    Returns:
        Updated asset row
    """
    payload: dict[str, Any] = {
        "sha256": sha256,
        "hosted_bucket": hosted_bucket,
        "hosted_key": hosted_key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_bytes": hosted_bytes,
        "hosted_at": completed_at,
        "ingest_status": "hosted",
        "ingest_completed_at": completed_at,
        "ingest_last_error": None,
        "ingest_failed_at": None,
        "ingest_next_retry_at": None,
    }

    if hosted_content_type:
        payload["hosted_content_type"] = hosted_content_type
    if hosted_etag:
        payload["hosted_etag"] = hosted_etag

    response = db.schema("core").table("media_assets").update(payload).eq("id", asset_id).execute()

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error updating mirror result: {response.error}")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return {}


def fetch_assets_for_mirroring(
    db: Client,
    *,
    source: str | None = None,
    status: str = "pending",
    limit: int = 100,
    respect_backoff: bool = True,
) -> list[dict[str, Any]]:
    """
    Fetch media assets that need mirroring.

    Args:
        db: Supabase client
        source: Filter by source (tmdb, imdb_graphql, etc.) or None for all
        status: Filter by status ('pending', 'failed', or 'all')
        limit: Maximum number of assets to return
        respect_backoff: If True, only return failed assets where
                         ingest_next_retry_at is null or in the past

    Returns:
        List of asset rows ready for mirroring
    """
    query = db.schema("core").table("media_assets").select("*")

    # Filter by source if specified
    if source and source != "all":
        query = query.eq("source", source)

    # Filter by status
    if status == "pending":
        query = query.eq("ingest_status", "pending")
    elif status == "failed":
        query = query.eq("ingest_status", "failed")
        if respect_backoff:
            # Only return assets where retry time has passed
            # PostgREST: or filter for null or past timestamp
            query = query.or_("ingest_next_retry_at.is.null,ingest_next_retry_at.lt.now()")
    elif status == "all":
        query = query.in_("ingest_status", ["pending", "failed"])
        if respect_backoff:
            # For 'all', apply backoff only to failed ones
            # This is tricky with PostgREST, so we'll filter in Python
            pass
    else:
        raise ValueError(f"Invalid status filter: {status}")

    # Must have source_url to mirror
    query = query.not_.is_("source_url", "null")

    # Order by created_at for consistent processing
    query = query.order("created_at").limit(limit)

    response = query.execute()

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error fetching assets for mirroring: {response.error}")

    data = response.data or []
    results = data if isinstance(data, list) else []

    # Post-filter for 'all' status with backoff
    if status == "all" and respect_backoff:
        from datetime import datetime

        now = datetime.now(UTC)
        filtered = []
        for row in results:
            if row.get("ingest_status") == "pending":
                filtered.append(row)
            elif row.get("ingest_status") == "failed":
                next_retry = row.get("ingest_next_retry_at")
                if next_retry is None:
                    filtered.append(row)
                else:
                    # Parse ISO timestamp
                    try:
                        retry_time = datetime.fromisoformat(next_retry.replace("Z", "+00:00"))
                        if retry_time <= now:
                            filtered.append(row)
                    except (ValueError, TypeError):
                        filtered.append(row)  # Include if can't parse
        results = filtered

    return results
