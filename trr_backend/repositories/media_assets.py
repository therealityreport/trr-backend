from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse
from uuid import UUID, uuid5

from trr_backend.media.s3_mirror import build_hosted_url

if TYPE_CHECKING:
    from trr_backend.db.session import DbSession
else:
    DbSession = Any

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


def _first_str(*values: Any) -> str | None:
    for value in values:
        cleaned = _as_str(value)
        if cleaned:
            return cleaned
    return None


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    step = max(1, int(size))
    for index in range(0, len(values), step):
        yield values[index : index + step]


def _looks_like_getty_media_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    cleaned = value.strip()
    if not cleaned:
        return False
    try:
        hostname = (urlparse(cleaned).hostname or "").strip().lower()
    except Exception:
        return False
    return hostname == "media.gettyimages.com" or hostname.endswith(".gettyimages.com")


def _hosted_url_from_row(row: Mapping[str, Any]) -> str | None:
    hosted_key = _as_str(row.get("hosted_key"))
    if hosted_key:
        return build_hosted_url(hosted_key)
    hosted_url = _as_str(row.get("hosted_url"))
    if _as_str(row.get("source")) == "getty" and _looks_like_getty_media_url(hosted_url):
        return None
    return hosted_url


def _asset_id_for(source: str, source_asset_id: str | None, source_url: str | None) -> UUID | None:
    if source_asset_id:
        name = f"{source}:asset:{source_asset_id}"
    elif source_url:
        name = f"{source}:url:{source_url}"
    else:
        return None
    return uuid5(_ASSET_ID_NAMESPACE, name)


def asset_id_for(source: str, source_asset_id: str | None = None, source_url: str | None = None) -> UUID | None:
    """Public wrapper for deterministic media_asset identity derivation."""
    return _asset_id_for(source, source_asset_id, source_url)


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
        source_url = _first_str(row.get("url_original"), row.get("url"), row.get("source_url"))
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
                "hosted_url": _hosted_url_from_row(row),
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


def transform_season_images_to_media(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    assets_by_id: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []

    for row in rows:
        season_id = _as_str(row.get("season_id"))
        source = _as_str(row.get("source"))
        kind = _as_str(row.get("kind"))
        if not season_id or not source or not kind:
            continue

        source_asset_id = _as_str(row.get("source_image_id")) or _as_str(row.get("file_path"))
        source_url = _first_str(row.get("url_original"), row.get("url"), row.get("source_url"))
        asset_id = _asset_id_for(source, source_asset_id, source_url)
        if asset_id is None:
            continue

        asset_id_str = str(asset_id)
        if asset_id_str not in assets_by_id:
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
                "hosted_url": _hosted_url_from_row(row),
                "hosted_sha256": _as_str(row.get("hosted_sha256")),
                "hosted_content_type": _as_str(row.get("hosted_content_type")),
                "hosted_bytes": _as_int(row.get("hosted_bytes")),
                "hosted_etag": _as_str(row.get("hosted_etag")),
                "hosted_at": row.get("hosted_at"),
                "fetched_at": row.get("fetched_at"),
                "metadata": dict(row.get("metadata")) if isinstance(row.get("metadata"), dict) else {},
            }
            assets_by_id[asset_id_str] = _compact_dict(asset)

        context = _compact_dict(
            {
                "legacy_table": "season_images",
                "legacy_id": _as_str(row.get("id")),
                "file_path": _as_str(row.get("file_path")),
                "url_path": _as_str(row.get("url_path")),
                "iso_639_1": _as_str(row.get("iso_639_1")),
                "tmdb_series_id": _as_str(row.get("tmdb_series_id")),
                "season_number": _as_int(row.get("season_number")),
                "source_image_id": source_asset_id,
                "image_type": _as_str(row.get("image_type")),
            }
        )
        position = _as_int(row.get("position"))
        link_id = _link_id_for(
            entity_type="season",
            entity_id=season_id,
            asset_id=asset_id_str,
            kind=kind,
            position=position,
            is_primary=False,
            context=context,
        )
        links.append(
            {
                "id": str(link_id),
                "entity_type": "season",
                "entity_id": season_id,
                "media_asset_id": asset_id_str,
                "kind": kind,
                "position": position,
                "is_primary": False,
                "context": context,
            }
        )

    return list(assets_by_id.values()), links


def transform_episode_images_to_media(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    assets_by_id: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []

    for row in rows:
        episode_id = _as_str(row.get("episode_id"))
        source = _as_str(row.get("source"))
        kind = _as_str(row.get("kind"))
        if not episode_id or not source or not kind:
            continue

        source_asset_id = _as_str(row.get("source_image_id")) or _as_str(row.get("file_path"))
        source_url = _first_str(row.get("url_original"), row.get("url"), row.get("source_url"))
        asset_id = _asset_id_for(source, source_asset_id, source_url)
        if asset_id is None:
            continue

        asset_id_str = str(asset_id)
        if asset_id_str not in assets_by_id:
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
                "hosted_url": _hosted_url_from_row(row),
                "hosted_sha256": _as_str(row.get("hosted_sha256")),
                "hosted_content_type": _as_str(row.get("hosted_content_type")),
                "hosted_bytes": _as_int(row.get("hosted_bytes")),
                "hosted_etag": _as_str(row.get("hosted_etag")),
                "hosted_at": row.get("hosted_at"),
                "fetched_at": row.get("fetched_at"),
                "metadata": dict(row.get("metadata")) if isinstance(row.get("metadata"), dict) else {},
            }
            assets_by_id[asset_id_str] = _compact_dict(asset)

        context = _compact_dict(
            {
                "legacy_table": "episode_images",
                "legacy_id": _as_str(row.get("id")),
                "file_path": _as_str(row.get("file_path")),
                "url_path": _as_str(row.get("url_path")),
                "iso_639_1": _as_str(row.get("iso_639_1")),
                "tmdb_series_id": _as_str(row.get("tmdb_series_id")),
                "season_number": _as_int(row.get("season_number")),
                "episode_number": _as_int(row.get("episode_number")),
                "source_image_id": source_asset_id,
                "image_type": _as_str(row.get("image_type")),
            }
        )
        position = _as_int(row.get("position"))
        link_id = _link_id_for(
            entity_type="episode",
            entity_id=episode_id,
            asset_id=asset_id_str,
            kind=kind,
            position=position,
            is_primary=False,
            context=context,
        )
        links.append(
            {
                "id": str(link_id),
                "entity_type": "episode",
                "entity_id": episode_id,
                "media_asset_id": asset_id_str,
                "kind": kind,
                "position": position,
                "is_primary": False,
                "context": context,
            }
        )

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
                "hosted_url": _hosted_url_from_row(row),
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


def transform_cast_photos_to_media(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    assets_by_id: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []

    for row in rows:
        person_id = _as_str(row.get("person_id"))
        source = _as_str(row.get("source"))
        metadata = dict(row.get("metadata")) if isinstance(row.get("metadata"), dict) else {}
        if source == "getty":
            source_url = _first_str(
                row.get("image_url_canonical"),
                row.get("image_url"),
                metadata.get("getty_original_image_url"),
                metadata.get("original_source_url"),
                metadata.get("original_source_file_url"),
                metadata.get("source_url"),
                row.get("url"),
                row.get("thumb_url"),
                row.get("source_url"),
            )
        else:
            source_url = _first_str(
                row.get("image_url_canonical"),
                row.get("image_url"),
                row.get("url"),
                row.get("thumb_url"),
                row.get("source_url"),
            )
        if not person_id or not source or not source_url:
            continue

        source_asset_id = _as_str(row.get("source_image_id"))
        asset_id = _asset_id_for(source, source_asset_id, source_url)
        if asset_id is None:
            continue

        asset_id_str = str(asset_id)
        if asset_id_str not in assets_by_id:
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
                "hosted_url": _hosted_url_from_row(row),
                "hosted_sha256": _as_str(row.get("hosted_sha256")),
                "hosted_content_type": _as_str(row.get("hosted_content_type")),
                "hosted_bytes": _as_int(row.get("hosted_bytes")),
                "hosted_etag": _as_str(row.get("hosted_etag")),
                "hosted_at": row.get("hosted_at"),
                "fetched_at": row.get("fetched_at"),
                "metadata": metadata,
            }
            assets_by_id[asset_id_str] = _compact_dict(asset)

        position = _as_int(row.get("gallery_index"))
        context = _compact_dict(
            {
                "legacy_table": "cast_photos",
                "legacy_id": _as_str(row.get("id")),
                "source_image_id": source_asset_id,
                "viewer_id": _as_str(row.get("viewer_id")),
                "mediaindex_url_path": _as_str(row.get("mediaindex_url_path")),
                "mediaviewer_url_path": _as_str(row.get("mediaviewer_url_path")),
                "image_url_canonical": _as_str(row.get("image_url_canonical")),
                "gallery_index": position,
                "gallery_total": _as_int(row.get("gallery_total")),
            }
        )
        link_id = _link_id_for(
            entity_type="person",
            entity_id=person_id,
            asset_id=asset_id_str,
            kind="gallery",
            position=position,
            is_primary=False,
            context=context,
        )
        links.append(
            {
                "id": str(link_id),
                "entity_type": "person",
                "entity_id": person_id,
                "media_asset_id": asset_id_str,
                "kind": "gallery",
                "position": position,
                "is_primary": False,
                "context": context,
            }
        )

    return list(assets_by_id.values()), links


def reconcile_media_asset_id_conflicts(
    db: DbSession,
    assets: Iterable[Mapping[str, Any]],
    links: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    asset_rows = [dict(asset) for asset in assets]
    link_rows = [dict(link) for link in links] if links is not None else []
    if not asset_rows:
        return asset_rows, link_rows

    source_asset_ids_by_source: dict[str, set[str]] = {}
    source_urls_by_source: dict[str, set[str]] = {}
    for asset in asset_rows:
        source = _as_str(asset.get("source"))
        source_asset_id = _as_str(asset.get("source_asset_id"))
        source_url = _as_str(asset.get("source_url"))
        if not source:
            continue
        if source_asset_id:
            source_asset_ids_by_source.setdefault(source, set()).add(source_asset_id)
        elif source_url:
            source_urls_by_source.setdefault(source, set()).add(source_url)

    existing_by_source_asset_id: dict[tuple[str, str], str] = {}
    for source, source_asset_ids in source_asset_ids_by_source.items():
        for chunk in _chunked(sorted(source_asset_ids), 200):
            response = (
                db.schema("core")
                .table("media_assets")
                .select("id, source, source_asset_id")
                .eq("source", source)
                .in_("source_asset_id", chunk)
                .execute()
            )
            for row in response.data or []:
                row_source = _as_str(row.get("source"))
                row_source_asset_id = _as_str(row.get("source_asset_id"))
                row_id = _as_str(row.get("id"))
                if row_source and row_source_asset_id and row_id:
                    existing_by_source_asset_id[(row_source, row_source_asset_id)] = row_id

    existing_by_source_url: dict[tuple[str, str], str] = {}
    for source, source_urls in source_urls_by_source.items():
        for chunk in _chunked(sorted(source_urls), 100):
            response = (
                db.schema("core")
                .table("media_assets")
                .select("id, source, source_url")
                .eq("source", source)
                .in_("source_url", chunk)
                .execute()
            )
            for row in response.data or []:
                row_source = _as_str(row.get("source"))
                row_source_url = _as_str(row.get("source_url"))
                row_id = _as_str(row.get("id"))
                if row_source and row_source_url and row_id:
                    existing_by_source_url[(row_source, row_source_url)] = row_id

    replacements: dict[str, str] = {}
    for asset in asset_rows:
        asset_id = _as_str(asset.get("id"))
        source = _as_str(asset.get("source"))
        source_asset_id = _as_str(asset.get("source_asset_id"))
        source_url = _as_str(asset.get("source_url"))
        if not asset_id or not source:
            continue
        existing_id = (
            existing_by_source_asset_id.get((source, source_asset_id))
            if source_asset_id
            else existing_by_source_url.get((source, source_url or ""))
        )
        if existing_id and existing_id != asset_id:
            replacements[asset_id] = existing_id
            asset["id"] = existing_id

    if replacements:
        for link in link_rows:
            media_asset_id = _as_str(link.get("media_asset_id"))
            replacement_id = replacements.get(media_asset_id or "")
            if not replacement_id:
                continue
            link["media_asset_id"] = replacement_id
            entity_type = _as_str(link.get("entity_type"))
            entity_id = _as_str(link.get("entity_id"))
            kind = _as_str(link.get("kind"))
            if not entity_type or not entity_id or not kind:
                continue
            position = _as_int(link.get("position"))
            is_primary = bool(_as_bool(link.get("is_primary")) or False)
            context = dict(link.get("context")) if isinstance(link.get("context"), dict) else {}
            link["id"] = str(
                _link_id_for(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    asset_id=replacement_id,
                    kind=kind,
                    position=position,
                    is_primary=is_primary,
                    context=context,
                )
            )

    deduped_assets_by_id: dict[str, dict[str, Any]] = {}
    for asset in asset_rows:
        asset_id = _as_str(asset.get("id"))
        if asset_id:
            deduped_assets_by_id[asset_id] = asset

    deduped_links_by_id: dict[str, dict[str, Any]] = {}
    for link in link_rows:
        link_id = _as_str(link.get("id"))
        if link_id:
            deduped_links_by_id[link_id] = link

    return list(deduped_assets_by_id.values()), list(deduped_links_by_id.values())


def upsert_media_assets(db: DbSession, assets: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
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


def upsert_media_links(db: DbSession, links: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    payload = [dict(link) for link in links]
    if not payload:
        return []

    response = (
        db.schema("core")
        .table("media_links")
        .upsert(payload, on_conflict="entity_type,entity_id,kind,media_asset_id", default_to_null=False)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting media_links: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def upsert_media_with_links(
    db: DbSession,
    rows: Iterable[Mapping[str, Any]],
    *,
    entity_type: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if entity_type == "show":
        assets, links = transform_show_images_to_media(rows)
    elif entity_type == "season":
        assets, links = transform_season_images_to_media(rows)
    elif entity_type == "episode":
        assets, links = transform_episode_images_to_media(rows)
    elif entity_type == "person":
        assets, links = transform_person_images_to_media(rows)
    elif entity_type == "cast":
        assets, links = transform_cast_photos_to_media(rows)
    else:
        raise ValueError(f"Unsupported entity_type for media upsert: {entity_type}")

    assets, links = reconcile_media_asset_id_conflicts(db, assets, links)
    upserted_assets = upsert_media_assets(db, assets)
    upserted_links = upsert_media_links(db, links)
    return upserted_assets, upserted_links


# ---------------------------------------------------------------------------
# Ingest status helpers for async S3 mirroring (Phase 3)
# ---------------------------------------------------------------------------


def update_ingest_status(
    db: DbSession,
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
    db: DbSession,
    asset_id: str,
    *,
    sha256: str,
    hosted_bucket: str,
    hosted_key: str,
    hosted_url: str,
    hosted_bytes: int,
    hosted_content_type: str | None = None,
    hosted_etag: str | None = None,
    width: int | None = None,
    height: int | None = None,
    completed_at: str | None = None,
    metadata: Mapping[str, Any] | None = None,
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
        width: Optional image width
        height: Optional image height
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
    if isinstance(width, int) and width > 0:
        payload["width"] = width
    if isinstance(height, int) and height > 0:
        payload["height"] = height
    if metadata is not None:
        payload["metadata"] = dict(metadata)

    response = db.schema("core").table("media_assets").update(payload).eq("id", asset_id).execute()

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error updating mirror result: {response.error}")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return {}


def update_asset_with_hosted_fields(
    db: DbSession,
    asset_id: str,
    *,
    hosted_bucket: str,
    hosted_key: str,
    hosted_url: str,
    hosted_bytes: int,
    hosted_content_type: str | None = None,
    hosted_etag: str | None = None,
    width: int | None = None,
    height: int | None = None,
    completed_at: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Update hosted fields without setting sha columns.

    This is used as a recovery path when the mirrored bytes are already
    represented by another media_asset row and sha uniqueness would otherwise
    turn a usable hosted mirror into a hard failure.
    """

    if not hosted_bucket or not hosted_key or not hosted_url:
        raise ValueError("update_asset_with_hosted_fields requires hosted_bucket, hosted_key, and hosted_url")

    payload: dict[str, Any] = {
        "hosted_bucket": hosted_bucket,
        "hosted_key": hosted_key,
        "hosted_url": hosted_url,
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
    if isinstance(width, int) and width > 0:
        payload["width"] = width
    if isinstance(height, int) and height > 0:
        payload["height"] = height
    if metadata is not None:
        payload["metadata"] = dict(metadata)

    response = db.schema("core").table("media_assets").update(payload).eq("id", asset_id).execute()

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error updating hosted fields: {response.error}")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    return {}


def fetch_assets_for_mirroring(
    db: DbSession,
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
