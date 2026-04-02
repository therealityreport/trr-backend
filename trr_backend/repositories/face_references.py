"""Backend-owned face reference persistence for retained facebank flows."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import Json

from trr_backend.db import pg


def _json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Json(value)
    return value


def _media_link_image_row(link_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT
          ml.id::text AS media_link_id,
          ml.entity_id::text AS person_id,
          ml.media_asset_id::text AS media_asset_id,
          ma.source_url,
          ma.hosted_url,
          ma.hosted_sha256
        FROM core.media_links ml
        JOIN core.media_assets ma ON ma.id = ml.media_asset_id
        WHERE ml.id = %s::uuid
          AND ml.entity_type = 'person'
          AND ml.kind = 'gallery'
        LIMIT 1
        """,
        [link_id],
    )


def sync_face_reference_image(*, link_id: str, enabled: bool) -> dict[str, Any] | None:
    image_row = _media_link_image_row(link_id)
    if not image_row:
        return None

    now = datetime.now(UTC).isoformat()
    if enabled:
        rows = pg.execute_returning(
            """
            INSERT INTO ml.face_reference_images (
              person_id,
              media_link_id,
              media_asset_id,
              is_active,
              approved,
              embedding_status,
              source_url,
              hosted_url,
              hosted_sha256,
              last_enqueued_at,
              metadata
            )
            VALUES (%s::uuid, %s::uuid, %s::uuid, true, true, 'pending', %s, %s, %s, %s, %s)
            ON CONFLICT (media_link_id) DO UPDATE SET
              person_id = EXCLUDED.person_id,
              media_asset_id = EXCLUDED.media_asset_id,
              is_active = true,
              approved = true,
              embedding_status = 'pending',
              source_url = EXCLUDED.source_url,
              hosted_url = EXCLUDED.hosted_url,
              hosted_sha256 = EXCLUDED.hosted_sha256,
              last_enqueued_at = EXCLUDED.last_enqueued_at,
              deactivated_at = NULL,
              updated_at = now()
            RETURNING *
            """,
            [
                image_row["person_id"],
                image_row["media_link_id"],
                image_row["media_asset_id"],
                image_row.get("source_url"),
                image_row.get("hosted_url"),
                image_row.get("hosted_sha256"),
                now,
                _json({"source": "core.media_links.facebank_seed"}),
            ],
        )
        return rows[0] if rows else None

    rows = pg.execute_returning(
        """
        UPDATE ml.face_reference_images
        SET is_active = false,
            embedding_status = 'disabled',
            deactivated_at = %s,
            updated_at = now()
        WHERE media_link_id = %s::uuid
        RETURNING *
        """,
        [now, link_id],
    )
    return rows[0] if rows else None


def list_active_face_reference_person_ids(person_ids: list[str]) -> set[str]:
    normalized = [person_id for person_id in person_ids if person_id]
    if not normalized:
        return set()
    rows = pg.fetch_all(
        """
        SELECT DISTINCT person_id::text AS person_id
        FROM ml.face_reference_images
        WHERE is_active = true
          AND approved = true
          AND person_id::text = ANY(%s)
        """,
        [normalized],
    )
    return {str(row.get("person_id") or "").strip() for row in rows if str(row.get("person_id") or "").strip()}
