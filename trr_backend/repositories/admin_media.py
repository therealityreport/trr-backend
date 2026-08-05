"""Backend-owned persistence for admin image and media-link operations."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

from trr_backend.db import pg

ImageType = Literal["cast", "episode", "season"]
ReassignMode = Literal["preserve", "copy"]
FeaturedImageKind = Literal["poster", "backdrop"]
MediaEntityType = Literal["person", "season", "show", "episode"]

_TABLE_BY_IMAGE_TYPE: dict[ImageType, str] = {
    "cast": "cast_photos",
    "episode": "episode_images",
    "season": "season_images",
}
_ENTITY_ID_COLUMN_BY_IMAGE_TYPE: dict[ImageType, str] = {
    "cast": "person_id",
    "episode": "episode_id",
    "season": "season_id",
}
_MEDIA_LINK_FIELDS = """
  id::text AS id,
  entity_type,
  entity_id::text AS entity_id,
  media_asset_id::text AS media_asset_id,
  kind,
  position,
  context,
  created_at
"""

logger = logging.getLogger(__name__)


class SourceImageNotFoundError(RuntimeError):
    """Raised when a reassignment source image no longer exists."""


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)


def _json_dumps(value: object) -> str:
    return json.dumps(value, default=_json_default, separators=(",", ":"))


def _normalize_featured_kind(image_type: object, kind: object) -> FeaturedImageKind | Literal["other"]:
    raw_value = image_type if image_type is not None else kind
    token = str(raw_value or "").strip().lower()
    if token == "poster":
        return "poster"
    if token in {"backdrop", "background"}:
        return "backdrop"
    return "other"


def validate_show_featured_image(
    *,
    show_id: str,
    image_id: str,
    expected_kind: FeaturedImageKind,
) -> tuple[bool, int]:
    row = pg.fetch_one(
        """
        SELECT kind, image_type
        FROM core.show_images
        WHERE id = %s::uuid
          AND show_id = %s::uuid
        LIMIT 1
        """,
        [image_id, show_id],
    )
    if row is None:
        return False, 1
    return _normalize_featured_kind(row.get("image_type"), row.get("kind")) == expected_kind, 1


def get_image(image_type: ImageType, image_id: str) -> tuple[dict[str, Any] | None, int]:
    table = _TABLE_BY_IMAGE_TYPE[image_type]
    row = pg.fetch_one(
        f"""
        SELECT *
        FROM core.{table}
        WHERE id = %s::uuid
        LIMIT 1
        """,
        [image_id],
    )
    return row, 1


def _write_audit(
    *,
    image_type: ImageType,
    image_id: str,
    action: str,
    actor_uid: str,
    details: dict[str, Any] | None = None,
) -> int:
    try:
        pg.execute(
            """
            INSERT INTO admin.image_audit_log (
              image_type,
              image_id,
              action,
              performed_by_firebase_uid,
              details
            )
            VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            [
                image_type,
                image_id,
                action,
                actor_uid,
                _json_dumps(details) if details is not None else None,
            ],
        )
    except Exception as error:  # noqa: BLE001 - audit logging is intentionally best-effort.
        codes = {str(getattr(error, attribute, "") or "") for attribute in ("code", "sqlstate", "pgcode")}
        if not codes.intersection({"42P01", "3F000"}):
            logger.warning("[admin-media] failed to write image audit log", exc_info=True)
    return 1


def archive_image(
    *,
    image_type: ImageType,
    image_id: str,
    actor_uid: str,
    reason: str | None = None,
) -> int:
    table = _TABLE_BY_IMAGE_TYPE[image_type]
    patch = {
        "archived": True,
        "archived_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "archived_by_firebase_uid": actor_uid,
        "archived_reason": reason,
    }
    pg.execute(
        f"""
        UPDATE core.{table}
        SET metadata = COALESCE(metadata, '{{}}'::jsonb) || %s::jsonb,
            updated_at = NOW()
        WHERE id = %s::uuid
        """,
        [_json_dumps(patch), image_id],
    )
    details = {"reason": reason} if reason is not None else {}
    return 1 + _write_audit(
        image_type=image_type,
        image_id=image_id,
        action="archive",
        actor_uid=actor_uid,
        details=details,
    )


def unarchive_image(
    *,
    image_type: ImageType,
    image_id: str,
    actor_uid: str,
) -> int:
    table = _TABLE_BY_IMAGE_TYPE[image_type]
    patch = {
        "archived": False,
        "archived_at": None,
        "archived_by_firebase_uid": None,
        "archived_reason": None,
    }
    pg.execute(
        f"""
        UPDATE core.{table}
        SET metadata = COALESCE(metadata, '{{}}'::jsonb) || %s::jsonb,
            updated_at = NOW()
        WHERE id = %s::uuid
        """,
        [_json_dumps(patch), image_id],
    )
    return 1 + _write_audit(
        image_type=image_type,
        image_id=image_id,
        action="unarchive",
        actor_uid=actor_uid,
    )


def delete_image(
    *,
    image_type: ImageType,
    image_id: str,
    actor_uid: str,
) -> int:
    image, query_count = get_image(image_type, image_id)
    table = _TABLE_BY_IMAGE_TYPE[image_type]
    pg.execute(
        f"""
        DELETE FROM core.{table}
        WHERE id = %s::uuid
        """,
        [image_id],
    )
    query_count += 1
    query_count += _write_audit(
        image_type=image_type,
        image_id=image_id,
        action="delete",
        actor_uid=actor_uid,
        details={"deletedImage": image},
    )
    return query_count


def reassign_image(
    *,
    image_type: ImageType,
    image_id: str,
    to_type: ImageType | None,
    to_entity_id: str,
    mode: ReassignMode,
    actor_uid: str,
) -> int:
    del mode  # The legacy contract accepts this hint; reassignment behavior is type-driven.
    source_image, query_count = get_image(image_type, image_id)
    if source_image is None:
        raise SourceImageNotFoundError("Source image not found")

    source_table = _TABLE_BY_IMAGE_TYPE[image_type]
    source_entity_column = _ENTITY_ID_COLUMN_BY_IMAGE_TYPE[image_type]
    from_entity_id = source_image.get(source_entity_column)
    destination_type = to_type or image_type

    if destination_type != image_type:
        destination_table = _TABLE_BY_IMAGE_TYPE[destination_type]
        destination_entity_column = _ENTITY_ID_COLUMN_BY_IMAGE_TYPE[destination_type]
        pg.execute(
            f"""
            INSERT INTO core.{destination_table} (
              source,
              url,
              hosted_url,
              caption,
              width,
              height,
              metadata,
              {destination_entity_column}
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::uuid)
            """,
            [
                source_image.get("source"),
                source_image.get("url"),
                source_image.get("hosted_url"),
                source_image.get("caption"),
                source_image.get("width"),
                source_image.get("height"),
                _json_dumps(source_image.get("metadata") or {}),
                to_entity_id,
            ],
        )
        query_count += 1
        query_count += archive_image(
            image_type=image_type,
            image_id=image_id,
            actor_uid=actor_uid,
            reason=f"Reassigned to {destination_type} {to_entity_id}",
        )
        query_count += _write_audit(
            image_type=image_type,
            image_id=image_id,
            action="copy_reassign",
            actor_uid=actor_uid,
            details={
                "fromType": image_type,
                "fromEntityId": from_entity_id,
                "toType": destination_type,
                "toEntityId": to_entity_id,
            },
        )
        return query_count

    pg.execute(
        f"""
        UPDATE core.{source_table}
        SET {source_entity_column} = %s::uuid,
            updated_at = NOW()
        WHERE id = %s::uuid
        """,
        [to_entity_id, image_id],
    )
    query_count += 1
    query_count += _write_audit(
        image_type=image_type,
        image_id=image_id,
        action="reassign",
        actor_uid=actor_uid,
        details={
            "fromEntityId": from_entity_id,
            "toEntityId": to_entity_id,
        },
    )
    return query_count


def media_asset_exists(media_asset_id: str) -> tuple[bool, int]:
    row = pg.fetch_one(
        """
        SELECT id::text AS id
        FROM core.media_assets
        WHERE id = %s::uuid
        LIMIT 1
        """,
        [media_asset_id],
    )
    return row is not None, 1


def get_media_link(link_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        f"""
        SELECT {_MEDIA_LINK_FIELDS}
        FROM core.media_links
        WHERE id = %s::uuid
        LIMIT 1
        """,
        [link_id],
    )
    return row, 1


def get_media_links(media_asset_id: str) -> tuple[list[dict[str, Any]], int]:
    rows = pg.fetch_all(
        f"""
        SELECT {_MEDIA_LINK_FIELDS}
        FROM core.media_links
        WHERE media_asset_id = %s::uuid
        """,
        [media_asset_id],
    )
    return rows, 1


def create_media_link(
    *,
    media_asset_id: str,
    entity_type: MediaEntityType,
    entity_id: str,
    kind: str = "gallery",
    context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], int]:
    normalized_context = context or {}
    existing = pg.fetch_one(
        f"""
        SELECT {_MEDIA_LINK_FIELDS}
        FROM core.media_links
        WHERE media_asset_id = %s::uuid
          AND entity_type = %s::text
          AND entity_id = %s::uuid
          AND kind = %s::text
        LIMIT 1
        """,
        [media_asset_id, entity_type, entity_id, kind],
    )
    query_count = 1
    if existing is not None:
        existing_context = existing.get("context") if isinstance(existing.get("context"), dict) else {}
        next_context = {**existing_context, **normalized_context}
        if next_context != existing_context:
            rows = pg.execute_returning(
                f"""
                UPDATE core.media_links
                SET context = %s::jsonb
                WHERE id = %s::uuid
                RETURNING {_MEDIA_LINK_FIELDS}
                """,
                [_json_dumps(next_context), str(existing.get("id") or "")],
            )
            query_count += 1
            if rows:
                return {"link": rows[0], "already_exists": True}, query_count
        return {"link": existing, "already_exists": True}, query_count

    rows = pg.execute_returning(
        f"""
        INSERT INTO core.media_links (
          media_asset_id,
          entity_type,
          entity_id,
          kind,
          position,
          context
        )
        VALUES (%s::uuid, %s::text, %s::uuid, %s::text, NULL, %s::jsonb)
        RETURNING {_MEDIA_LINK_FIELDS}
        """,
        [media_asset_id, entity_type, entity_id, kind, _json_dumps(normalized_context)],
    )
    query_count += 1
    if not rows:
        raise RuntimeError("Failed to create media link")
    return {"link": rows[0], "already_exists": False}, query_count


def update_media_link_context(
    link_id: str,
    patch: dict[str, Any],
) -> tuple[dict[str, Any] | None, int]:
    existing, query_count = get_media_link(link_id)
    if existing is None:
        return None, query_count

    existing_context = existing.get("context") if isinstance(existing.get("context"), dict) else {}
    next_context = dict(existing_context)
    for key in ("people_count", "people_count_source", "thumbnail_crop"):
        if key in patch:
            next_context[key] = patch[key]

    rows = pg.execute_returning(
        f"""
        UPDATE core.media_links
        SET context = %s::jsonb,
            updated_at = NOW()
        WHERE id = %s::uuid
        RETURNING {_MEDIA_LINK_FIELDS}
        """,
        [_json_dumps(next_context), link_id],
    )
    query_count += 1
    return (rows[0] if rows else None), query_count
