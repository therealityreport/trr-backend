"""Backend-owned face reference persistence for retained facebank flows."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import Json

from trr_backend.db import pg

FACE_REFERENCE_REVIEW_PENDING = "pending_review"
FACE_REFERENCE_REVIEW_APPROVED = "approved"
FACE_REFERENCE_REVIEW_REJECTED = "rejected"
FACE_REFERENCE_REVIEW_DUPLICATE = "duplicate"
FACE_REFERENCE_ACTIVE_REVIEW_STATUSES = frozenset({FACE_REFERENCE_REVIEW_APPROVED})
FACE_REFERENCE_EMBEDDING_PENDING = "pending"
FACE_REFERENCE_EMBEDDING_READY = "ready"
FACE_REFERENCE_EMBEDDING_DISABLED = "disabled"
FACE_REFERENCE_EMBEDDING_FAILED = "failed"

_FACE_REFERENCE_SELECT = """
SELECT
  fri.id::text AS id,
  fri.person_id::text AS person_id,
  fri.media_link_id::text AS media_link_id,
  fri.media_asset_id::text AS media_asset_id,
  fri.legacy_screenalytics_face_bank_image_id::text AS legacy_screenalytics_face_bank_image_id,
  fri.is_active,
  fri.approved,
  fri.review_status,
  fri.review_notes,
  fri.reviewed_at,
  fri.reviewed_by,
  fri.duplicate_of_reference_image_id::text AS duplicate_of_reference_image_id,
  fri.embedding_status,
  fri.source_url,
  fri.hosted_url,
  fri.hosted_sha256,
  fri.metadata,
  fri.last_enqueued_at,
  fri.deactivated_at,
  fri.created_at,
  fri.updated_at
FROM ml.face_reference_images AS fri
"""


def _json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Json(value)
    return value


def _vector_literal(values: Sequence[float]) -> str:
    return "[" + ",".join(f"{float(value):.10f}" for value in values) + "]"


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


def list_face_reference_images(*, person_id: str, include_inactive: bool = False) -> list[dict[str, Any]]:
    sql = (
        _FACE_REFERENCE_SELECT
        + """
    WHERE fri.person_id = %s::uuid
      AND (%s OR fri.is_active = true)
    ORDER BY
      case when fri.review_status = 'approved' then 0 else 1 end,
      fri.is_active DESC,
      fri.created_at DESC
    """
    )
    return pg.fetch_all(sql, [person_id, include_inactive])


def resolve_face_reference_image(
    *,
    reference_image_id: str | None = None,
    media_link_id: str | None = None,
    legacy_screenalytics_face_bank_image_id: str | None = None,
) -> dict[str, Any] | None:
    filters: list[str] = []
    params: list[Any] = []
    if reference_image_id:
        filters.append("fri.id = %s::uuid")
        params.append(reference_image_id)
    if media_link_id:
        filters.append("fri.media_link_id = %s::uuid")
        params.append(media_link_id)
    if legacy_screenalytics_face_bank_image_id:
        filters.append("fri.legacy_screenalytics_face_bank_image_id = %s::uuid")
        params.append(legacy_screenalytics_face_bank_image_id)
    if not filters:
        raise ValueError("reference_image_id, media_link_id, or legacy_screenalytics_face_bank_image_id is required")

    sql = _FACE_REFERENCE_SELECT + f"\nWHERE {' OR '.join(filters)}\nORDER BY fri.created_at DESC\nLIMIT 1"
    return pg.fetch_one(sql, params)


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
              review_status,
              review_notes,
              reviewed_at,
              reviewed_by,
              duplicate_of_reference_image_id,
              embedding_status,
              source_url,
              hosted_url,
              hosted_sha256,
              last_enqueued_at,
              metadata
            )
            VALUES (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              true,
              false,
              'pending_review',
              %s,
              NULL,
              NULL,
              NULL,
              'pending',
              %s,
              %s,
              %s,
              %s,
              %s
            )
            ON CONFLICT (media_link_id) DO UPDATE SET
              person_id = EXCLUDED.person_id,
              media_asset_id = EXCLUDED.media_asset_id,
              is_active = true,
              approved = CASE
                WHEN ml.face_reference_images.review_status = 'approved'
                  AND ml.face_reference_images.media_asset_id = EXCLUDED.media_asset_id
                  AND ml.face_reference_images.hosted_sha256 IS NOT DISTINCT FROM EXCLUDED.hosted_sha256
                THEN true
                ELSE false
              END,
              review_status = CASE
                WHEN ml.face_reference_images.review_status IN ('approved', 'rejected', 'duplicate')
                  AND ml.face_reference_images.media_asset_id = EXCLUDED.media_asset_id
                  AND ml.face_reference_images.hosted_sha256 IS NOT DISTINCT FROM EXCLUDED.hosted_sha256
                THEN ml.face_reference_images.review_status
                ELSE 'pending_review'
              END,
              review_notes = CASE
                WHEN ml.face_reference_images.review_status IN ('approved', 'rejected', 'duplicate')
                  AND ml.face_reference_images.media_asset_id = EXCLUDED.media_asset_id
                  AND ml.face_reference_images.hosted_sha256 IS NOT DISTINCT FROM EXCLUDED.hosted_sha256
                THEN ml.face_reference_images.review_notes
                ELSE EXCLUDED.review_notes
              END,
              reviewed_at = CASE
                WHEN ml.face_reference_images.review_status IN ('approved', 'rejected', 'duplicate')
                  AND ml.face_reference_images.media_asset_id = EXCLUDED.media_asset_id
                  AND ml.face_reference_images.hosted_sha256 IS NOT DISTINCT FROM EXCLUDED.hosted_sha256
                THEN ml.face_reference_images.reviewed_at
                ELSE NULL
              END,
              reviewed_by = CASE
                WHEN ml.face_reference_images.review_status IN ('approved', 'rejected', 'duplicate')
                  AND ml.face_reference_images.media_asset_id = EXCLUDED.media_asset_id
                  AND ml.face_reference_images.hosted_sha256 IS NOT DISTINCT FROM EXCLUDED.hosted_sha256
                THEN ml.face_reference_images.reviewed_by
                ELSE NULL
              END,
              duplicate_of_reference_image_id = CASE
                WHEN ml.face_reference_images.review_status = 'duplicate'
                  AND ml.face_reference_images.media_asset_id = EXCLUDED.media_asset_id
                  AND ml.face_reference_images.hosted_sha256 IS NOT DISTINCT FROM EXCLUDED.hosted_sha256
                THEN ml.face_reference_images.duplicate_of_reference_image_id
                ELSE NULL
              END,
              embedding_status = CASE
                WHEN ml.face_reference_images.review_status = 'approved'
                  AND ml.face_reference_images.media_asset_id = EXCLUDED.media_asset_id
                  AND ml.face_reference_images.hosted_sha256 IS NOT DISTINCT FROM EXCLUDED.hosted_sha256
                THEN ml.face_reference_images.embedding_status
                ELSE 'pending'
              END,
              source_url = EXCLUDED.source_url,
              hosted_url = EXCLUDED.hosted_url,
              hosted_sha256 = EXCLUDED.hosted_sha256,
              last_enqueued_at = EXCLUDED.last_enqueued_at,
              deactivated_at = NULL,
              metadata = ml.face_reference_images.metadata || EXCLUDED.metadata,
              updated_at = now()
            RETURNING *
            """,
            [
                image_row["person_id"],
                image_row["media_link_id"],
                image_row["media_asset_id"],
                _json({"source": "core.media_links.facebank_seed", "enrollment": True}),
                image_row.get("source_url"),
                image_row.get("hosted_url"),
                image_row.get("hosted_sha256"),
                now,
                _json({"source": "core.media_links.facebank_seed", "enrollment": True}),
            ],
        )
        return rows[0] if rows else None

    rows = pg.execute_returning(
        """
        UPDATE ml.face_reference_images
        SET is_active = false,
            approved = false,
            embedding_status = 'disabled',
            deactivated_at = %s,
            updated_at = now()
        WHERE media_link_id = %s::uuid
        RETURNING *
        """,
        [now, link_id],
    )
    return rows[0] if rows else None


def set_face_reference_review_status(
    *,
    reference_image_id: str,
    review_status: str,
    reviewed_by: str | None = None,
    review_notes: dict[str, Any] | None = None,
    duplicate_of_reference_image_id: str | None = None,
) -> dict[str, Any] | None:
    normalized_status = str(review_status or "").strip().lower()
    if normalized_status not in {
        FACE_REFERENCE_REVIEW_PENDING,
        FACE_REFERENCE_REVIEW_APPROVED,
        FACE_REFERENCE_REVIEW_REJECTED,
        FACE_REFERENCE_REVIEW_DUPLICATE,
    }:
        raise ValueError(f"Unsupported review_status: {review_status}")

    approved = normalized_status == FACE_REFERENCE_REVIEW_APPROVED
    duplicate_target = duplicate_of_reference_image_id if normalized_status == FACE_REFERENCE_REVIEW_DUPLICATE else None
    embedding_status = FACE_REFERENCE_EMBEDDING_PENDING if approved else FACE_REFERENCE_EMBEDDING_DISABLED
    rows = pg.execute_returning(
        """
        UPDATE ml.face_reference_images
        SET approved = %s,
            review_status = %s,
            review_notes = %s,
            reviewed_at = now(),
            reviewed_by = %s,
            duplicate_of_reference_image_id = %s::uuid,
            embedding_status = %s,
            is_active = CASE
              WHEN %s = 'approved' THEN true
              WHEN %s = 'pending_review' THEN is_active
              ELSE false
            END,
            deactivated_at = CASE
              WHEN %s = 'approved' THEN NULL
              WHEN %s = 'pending_review' THEN deactivated_at
              ELSE now()
            END,
            updated_at = now()
        WHERE id = %s::uuid
        RETURNING *
        """,
        [
            approved,
            normalized_status,
            _json(review_notes or {}),
            reviewed_by,
            duplicate_target,
            embedding_status,
            normalized_status,
            normalized_status,
            normalized_status,
            normalized_status,
            reference_image_id,
        ],
    )
    return rows[0] if rows else None


def upsert_face_reference_embedding(
    *,
    reference_image_id: str,
    provider: str,
    model_name: str,
    model_version: str | None,
    embedding_status: str,
    embedding: Sequence[float] | None,
    metadata: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> dict[str, Any] | None:
    rows = pg.execute_returning(
        """
        INSERT INTO ml.face_reference_embeddings (
          reference_image_id,
          provider,
          model_name,
          model_version,
          embedding_status,
          embedding,
          metadata,
          error_message,
          generated_at
        )
        VALUES (
          %s::uuid,
          %s,
          %s,
          %s,
          %s,
          %s::vector,
          %s,
          %s,
          CASE WHEN %s = 'ready' THEN now() ELSE NULL END
        )
        ON CONFLICT (reference_image_id, provider, model_name, coalesce(model_version, '')) DO UPDATE SET
          embedding_status = EXCLUDED.embedding_status,
          embedding = EXCLUDED.embedding,
          metadata = ml.face_reference_embeddings.metadata || EXCLUDED.metadata,
          error_message = EXCLUDED.error_message,
          generated_at = CASE
            WHEN EXCLUDED.embedding_status = 'ready' THEN now()
            ELSE ml.face_reference_embeddings.generated_at
          END,
          updated_at = now()
        RETURNING *
        """,
        [
            reference_image_id,
            provider,
            model_name,
            model_version,
            embedding_status,
            _vector_literal(embedding) if embedding else None,
            _json(metadata or {}),
            error_message,
            embedding_status,
        ],
    )
    return rows[0] if rows else None


def get_ready_face_reference_embedding(
    *,
    reference_image_id: str,
    contract_key: str | None = None,
) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT
          fre.id::text AS id,
          fre.reference_image_id::text AS reference_image_id,
          fre.provider,
          fre.model_name,
          fre.model_version,
          fre.embedding_status,
          fre.embedding,
          fre.metadata,
          fre.error_message,
          fre.generated_at,
          fre.created_at,
          fre.updated_at
        FROM ml.face_reference_embeddings AS fre
        WHERE fre.reference_image_id = %s::uuid
          AND fre.embedding_status = 'ready'
          AND (%s = '' OR coalesce(fre.metadata->>'contract_key', '') = %s)
        ORDER BY fre.generated_at DESC NULLS LAST, fre.created_at DESC
        LIMIT 1
        """,
        [reference_image_id, str(contract_key or ""), str(contract_key or "")],
    )


def search_face_reference_matches(
    *,
    embedding: Sequence[float],
    limit: int = 5,
    person_id: str | None = None,
    contract_key: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 25))
    vector = _vector_literal(embedding)
    return pg.fetch_all(
        """
        SELECT
          fri.id::text AS reference_image_id,
          fri.person_id::text AS person_id,
          p.name AS person_name,
          fri.media_link_id::text AS media_link_id,
          fri.review_status,
          fri.metadata AS reference_metadata,
          fre.id::text AS embedding_id,
          fre.provider,
          fre.model_name,
          fre.model_version,
          fre.metadata AS embedding_metadata,
          (fre.embedding <=> %s::vector) AS cosine_distance
        FROM ml.face_reference_embeddings AS fre
        JOIN ml.face_reference_images AS fri ON fri.id = fre.reference_image_id
        LEFT JOIN core.people AS p ON p.id = fri.person_id
        WHERE fri.is_active = true
          AND fri.approved = true
          AND fri.review_status = 'approved'
          AND fre.embedding_status = 'ready'
          AND fre.embedding IS NOT NULL
          AND (%s = '' OR fri.person_id = %s::uuid)
          AND (%s = '' OR coalesce(fre.metadata->>'contract_key', '') = %s)
        ORDER BY fre.embedding <=> %s::vector
        LIMIT %s
        """,
        [
            vector,
            str(person_id or ""),
            str(person_id or ""),
            str(contract_key or ""),
            str(contract_key or ""),
            vector,
            safe_limit,
        ],
    )


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
          AND review_status = 'approved'
          AND person_id::text = ANY(%s)
        """,
        [normalized],
    )
    return {str(row.get("person_id") or "").strip() for row in rows if str(row.get("person_id") or "").strip()}
