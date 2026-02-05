"""
Repository functions for web-scraped images.

Handles database operations for images scraped from URLs,
storing them in the unified media_assets and media_links tables.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid5

if TYPE_CHECKING:
    from trr_backend.db.session import DbSession
else:
    DbSession = Any

logger = logging.getLogger(__name__)

# Namespace UUIDs for deterministic ID generation
_ASSET_ID_NAMESPACE = UUID("52f296b6-0f8d-4bfb-8f39-6e7e5ea8a3a6")
_LINK_ID_NAMESPACE = UUID("3e73e1b4-6b0f-4cbf-a0f4-9029a4f9f2b7")


def _asset_id_for_sha256(source: str, sha256: str) -> UUID:
    """Generate deterministic asset ID from source and SHA256."""
    name = f"{source}:sha256:{sha256}"
    return uuid5(_ASSET_ID_NAMESPACE, name)


def _link_id_for(
    *,
    entity_type: str,
    entity_id: str,
    asset_id: str,
    kind: str,
    position: int | None,
) -> UUID:
    """Generate deterministic link ID."""
    name = f"{entity_type}:{entity_id}:{asset_id}:{kind}:{position}"
    return uuid5(_LINK_ID_NAMESPACE, name)


def find_asset_by_sha256(db: DbSession, sha256: str) -> dict[str, Any] | None:
    """
    Find an existing media asset by its SHA256 hash.

    Returns:
        Asset dict if found, None otherwise.
    """
    result = db.schema("core").table("media_assets").select("*").eq("sha256", sha256).limit(1).execute()

    if result.data:
        return result.data[0]
    return None


def get_season_and_show_identifiers(
    db: DbSession,
    show_id: str | None,
    season_number: int | None,
    *,
    season_id: str | None = None,
) -> dict[str, Any] | None:
    """
    Get season ID and show identifier (IMDb ID preferred) for S3 path construction.

    Returns:
        Dict with 'season_id', 'show_identifier', 'show_id', and 'season_number', or None if not found.
    """
    query = db.schema("core").table("seasons").select("id, season_number, show_id")

    def run_query(next_query) -> dict[str, Any] | None:
        result = next_query.limit(1).execute()
        if getattr(result, "error", None):
            logger.error("Failed to lookup season identifiers: %s", result.error)
            return None
        if not result.data:
            return None
        return result.data[0]

    season: dict[str, Any] | None = None

    if season_id:
        season = run_query(query.eq("id", season_id))

    if season is None and show_id is not None and season_number is not None:
        season = run_query(query.eq("show_id", show_id).eq("season_number", season_number))

    if season is None:
        logger.error(
            "Season lookup returned no results: show_id=%s season_number=%s season_id=%s",
            show_id,
            season_number,
            season_id,
        )
        return None
    season_id = season["id"]
    resolved_show_id = season.get("show_id") or show_id
    resolved_season_number = season.get("season_number") or season_number

    # Extract show identifier - prefer IMDb ID
    imdb_id = None
    if resolved_show_id:
        show_result = (
            db.schema("core")
            .table("shows")
            .select("external_ids")
            .eq("id", resolved_show_id)
            .limit(1)
            .execute()
        )
        if getattr(show_result, "error", None):
            logger.error("Failed to lookup show external_ids: %s", show_result.error)
        elif show_result.data:
            external_ids = show_result.data[0].get("external_ids", {}) or {}
            imdb_id = external_ids.get("imdb_id") or external_ids.get("imdb")

    # Use IMDb ID if available, otherwise fall back to show UUID
    show_identifier = imdb_id if imdb_id else resolved_show_id

    return {
        "season_id": season_id,
        "show_identifier": show_identifier,
        "show_id": resolved_show_id,
        "season_number": resolved_season_number,
    }


def get_person_identifier(db: DbSession, person_id: str) -> dict[str, Any] | None:
    """Get person IMDb ID (preferred) or UUID for S3 path."""
    result = (
        db.schema("core").table("people").select("id, full_name, external_ids").eq("id", person_id).limit(1).execute()
    )
    if not result.data:
        return None
    person = result.data[0]
    imdb_id = (person.get("external_ids") or {}).get("imdb")
    return {
        "person_id": person["id"],
        "identifier": imdb_id or person_id,  # IMDb ID preferred for S3 path
        "full_name": person.get("full_name"),
    }


def create_media_asset_from_scrape(
    db: DbSession,
    *,
    source: str,
    source_url: str,
    sha256: str,
    hosted_bucket: str,
    hosted_key: str,
    hosted_url: str,
    hosted_bytes: int,
    hosted_etag: str | None,
    content_type: str,
    width: int | None,
    height: int | None,
    caption: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a media asset record for a web-scraped image.

    Uses upsert on sha256 to handle duplicates gracefully.

    Returns:
        The created/updated asset dict.
    """
    now = datetime.now(UTC).isoformat()

    # Generate deterministic ID based on source and sha256
    asset_id = str(_asset_id_for_sha256(source, sha256))

    row = {
        "id": asset_id,
        "media_type": "image",
        "source": source,
        "source_url": source_url,
        "sha256": sha256,
        "hosted_bucket": hosted_bucket,
        "hosted_key": hosted_key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_bytes": hosted_bytes,
        "hosted_etag": hosted_etag,
        "hosted_content_type": content_type,
        "hosted_at": now,
        "ingest_status": "hosted",
        "width": width,
        "height": height,
        "caption": caption,
        "metadata": metadata or {},
        "created_at": now,
        "updated_at": now,
    }

    # Use id for conflict resolution - the id is deterministically generated from source+sha256
    # Note: on_conflict="sha256" doesn't work because it's a partial unique index
    result = db.schema("core").table("media_assets").upsert(row, on_conflict="id").execute()

    if result.data:
        return result.data[0]

    # If upsert didn't return data, fetch by sha256
    existing = find_asset_by_sha256(db, sha256)
    if existing:
        return existing

    # Fallback - return the row we tried to insert
    return row


def create_media_link_for_season(
    db: DbSession,
    *,
    season_id: str,
    media_asset_id: str,
    kind: str = "gallery",
    position: int | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a media link between a season and a media asset.

    Uses upsert to avoid duplicates.

    Returns:
        The created/updated link dict.
    """
    now = datetime.now(UTC).isoformat()

    link_id = str(
        _link_id_for(
            entity_type="season",
            entity_id=season_id,
            asset_id=media_asset_id,
            kind=kind,
            position=position,
        )
    )

    row = {
        "id": link_id,
        "entity_type": "season",
        "entity_id": season_id,
        "media_asset_id": media_asset_id,
        "kind": kind,
        "position": position,
        "is_primary": False,
        "context": context or {},
        "created_at": now,
        "updated_at": now,
    }

    result = (
        db.schema("core")
        .table("media_links")
        .upsert(row, on_conflict="entity_type,entity_id,media_asset_id,kind")
        .execute()
    )

    if result.data:
        return result.data[0]

    return row


def create_media_link_for_entity(
    db: DbSession,
    *,
    entity_type: str,
    entity_id: str,
    media_asset_id: str,
    kind: str = "gallery",
    position: int | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create media link for any entity type with provenance tracking."""
    now = datetime.now(UTC).isoformat()
    ctx = {**(context or {}), "scrape_ts": now}
    ctx = {k: v for k, v in ctx.items() if v is not None}

    link_id = str(
        _link_id_for(
            entity_type=entity_type,
            entity_id=entity_id,
            asset_id=media_asset_id,
            kind=kind,
            position=position,
        )
    )

    row = {
        "id": link_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "media_asset_id": media_asset_id,
        "kind": kind,
        "position": position,
        "is_primary": False,
        "context": ctx,
        "created_at": now,
        "updated_at": now,
    }

    db.schema("core").table("media_links").upsert(
        row, on_conflict="entity_type,entity_id,kind,media_asset_id"
    ).execute()

    return row


def list_season_gallery_images(
    db: DbSession,
    season_id: str,
) -> list[dict[str, Any]]:
    """
    List all gallery images for a season.

    Returns:
        List of media assets with link metadata.
    """
    result = (
        db.schema("core")
        .table("media_links")
        .select("*, media_assets!inner(*)")
        .eq("entity_type", "season")
        .eq("entity_id", season_id)
        .eq("kind", "gallery")
        .order("position", desc=False)
        .execute()
    )

    if not result.data:
        return []

    # Flatten the response
    images = []
    for link in result.data:
        asset = link.get("media_assets", {})
        images.append(
            {
                **asset,
                "link_id": link["id"],
                "position": link.get("position"),
                "is_primary": link.get("is_primary"),
                "link_context": link.get("context"),
            }
        )

    return images
