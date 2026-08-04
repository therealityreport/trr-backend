"""Version-neutral admin media service."""

from __future__ import annotations

import base64
import math
import re
from typing import Any, cast

from trr_backend.repositories import admin_media as admin_media_repo
from trr_backend.repositories import admin_show_reads as show_reads_repo
from trr_backend.repositories.admin_media import (
    FeaturedImageKind,
    ImageType,
    MediaEntityType,
    ReassignMode,
    SourceImageNotFoundError,
)

_ASSET_CURSOR_PREFIX = "offset:"
_PARSE_INT_PREFIX_RE = re.compile(r"^[+-]?\d+")
_PARSE_FLOAT_PREFIX_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")


class MediaAssetNotFoundError(RuntimeError):
    """Raised when a requested media asset does not exist."""


def _encode_asset_cursor(offset: int) -> str | None:
    normalized_offset = max(0, int(offset))
    if normalized_offset <= 0:
        return None
    return base64.urlsafe_b64encode(f"{_ASSET_CURSOR_PREFIX}{normalized_offset}".encode()).decode("ascii")


def _asset_pagination(
    *,
    limit: int,
    offset: int,
    count: int,
    has_more: bool,
    full: bool,
    truncated: bool,
) -> dict[str, Any]:
    return {
        "limit": limit,
        "offset": offset,
        "count": count,
        "has_more": has_more,
        "next_cursor": _encode_asset_cursor(offset + count) if has_more else None,
        "cursor": _encode_asset_cursor(offset),
        "full": full,
        "truncated": truncated,
    }


def get_show_season_assets(
    *,
    show_id: str,
    season_number: int,
    limit: int,
    offset: int,
    sources: list[str] | None,
    full: bool,
) -> tuple[dict[str, Any], int]:
    requested_offset = 0 if full else offset
    request_limit = 5001 if full else limit + 1
    assets, query_count = show_reads_repo.get_show_season_assets(
        show_id,
        season_number,
        limit=request_limit,
        offset=requested_offset,
        sources=sources,
        full=full,
    )
    visible_assets = assets[:5000] if full else assets[:limit]
    has_more = False if full else len(assets) > limit
    truncated = full and len(assets) > 5000
    return (
        {
            "assets": visible_assets,
            "pagination": _asset_pagination(
                limit=5000 if full else limit,
                offset=requested_offset,
                count=len(visible_assets),
                has_more=has_more,
                full=full,
                truncated=truncated,
            ),
        },
        query_count,
    )


def validate_show_featured_image(
    *,
    show_id: str,
    image_id: str,
    expected_kind: FeaturedImageKind,
) -> tuple[bool, int]:
    return admin_media_repo.validate_show_featured_image(
        show_id=show_id,
        image_id=image_id,
        expected_kind=expected_kind,
    )


def get_image(image_type: ImageType, image_id: str) -> tuple[dict[str, Any] | None, int]:
    return admin_media_repo.get_image(image_type, image_id)


def delete_image(
    *,
    image_type: ImageType,
    image_id: str,
    actor_uid: str,
) -> int:
    return admin_media_repo.delete_image(
        image_type=image_type,
        image_id=image_id,
        actor_uid=actor_uid,
    )


def set_image_archive_state(
    *,
    image_type: ImageType,
    image_id: str,
    archive: bool,
    actor_uid: str,
    reason: str | None = None,
) -> int:
    if archive:
        return admin_media_repo.archive_image(
            image_type=image_type,
            image_id=image_id,
            actor_uid=actor_uid,
            reason=reason,
        )
    return admin_media_repo.unarchive_image(
        image_type=image_type,
        image_id=image_id,
        actor_uid=actor_uid,
    )


def reassign_image(
    *,
    image_type: ImageType,
    image_id: str,
    to_type: ImageType | None,
    to_entity_id: str,
    mode: ReassignMode,
    actor_uid: str,
) -> int:
    return admin_media_repo.reassign_image(
        image_type=image_type,
        image_id=image_id,
        to_type=to_type,
        to_entity_id=to_entity_id,
        mode=mode,
        actor_uid=actor_uid,
    )


def get_media_links(media_asset_id: str) -> tuple[list[dict[str, Any]], int]:
    return admin_media_repo.get_media_links(media_asset_id)


def create_media_link(
    *,
    media_asset_id: str,
    entity_type: MediaEntityType,
    entity_id: str,
    kind: str,
    context: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    exists, query_count = admin_media_repo.media_asset_exists(media_asset_id)
    if not exists:
        raise MediaAssetNotFoundError("Media asset not found")
    result, create_query_count = admin_media_repo.create_media_link(
        media_asset_id=media_asset_id,
        entity_type=entity_type,
        entity_id=entity_id,
        kind=kind,
        context=context,
    )
    result["message"] = "Link already exists" if result["already_exists"] else "Link created successfully"
    return result, query_count + create_query_count


def parse_people_count(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        return max(0, math.floor(numeric))
    if isinstance(value, str) and value.strip():
        match = _PARSE_INT_PREFIX_RE.match(value.strip())
        if match is None:
            return None
        return max(0, int(match.group(0)))
    return None


def parse_people_count_source(value: object) -> str | None:
    return str(value) if value in {"auto", "manual"} else None


def _parse_finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    if isinstance(value, str):
        match = _PARSE_FLOAT_PREFIX_RE.match(value.strip())
        if match is not None:
            numeric = float(match.group(0))
            return numeric if math.isfinite(numeric) else None
    return None


def parse_thumbnail_crop(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    mode = value.get("mode")
    x = _parse_finite_number(value.get("x"))
    y = _parse_finite_number(value.get("y"))
    zoom = _parse_finite_number(value.get("zoom"))
    if mode not in {"manual", "auto"} or x is None or y is None or zoom is None:
        return None
    return {
        "x": min(100.0, max(0.0, x)),
        "y": min(100.0, max(0.0, y)),
        "zoom": min(4.0, max(1.0, zoom)),
        "mode": mode,
    }


def update_media_link_context(
    link_id: str,
    patch: dict[str, Any],
) -> tuple[dict[str, Any] | None, int]:
    link, query_count = admin_media_repo.update_media_link_context(link_id, patch)
    if link is None:
        return None, query_count
    context_value = link.get("context")
    context = cast(dict[str, Any], context_value) if isinstance(context_value, dict) else {}
    return (
        {
            "link_id": str(link.get("id") or ""),
            "people_count": parse_people_count(context.get("people_count")),
            "people_count_source": parse_people_count_source(context.get("people_count_source")),
            "thumbnail_crop": parse_thumbnail_crop(context.get("thumbnail_crop")),
        },
        query_count,
    )


__all__ = [
    "MediaAssetNotFoundError",
    "SourceImageNotFoundError",
    "create_media_link",
    "delete_image",
    "get_image",
    "get_media_links",
    "get_show_season_assets",
    "parse_people_count",
    "parse_people_count_source",
    "parse_thumbnail_crop",
    "reassign_image",
    "set_image_archive_state",
    "update_media_link_context",
    "validate_show_featured_image",
]
