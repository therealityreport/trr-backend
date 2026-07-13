"""Private Instagram payload-sidecar persistence helpers.

The legacy payload columns intentionally remain populated during the sidecar
rollout.  These helpers only add transactional, bulk dual-writes and the
sidecar-first preservation lookup needed by metadata-poor comment passes.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from psycopg2.extras import Json as PgJson

from trr_backend.db import pg

PayloadReadMode = Literal["legacy", "compare", "sidecar"]
PAYLOAD_READ_MODES = frozenset({"legacy", "compare", "sidecar"})
PAYLOAD_READ_MODE_ENV = "SOCIAL_INSTAGRAM_PAYLOAD_READ_MODE"


def payload_read_mode() -> PayloadReadMode:
    """Return the configured read mode, retaining legacy as the safe default."""
    value = str(os.getenv(PAYLOAD_READ_MODE_ENV) or "legacy").strip().lower()
    return value if value in PAYLOAD_READ_MODES else "legacy"  # type: ignore[return-value]


def payload_for_read_mode(
    *,
    legacy: Any,
    sidecar: Any,
    mode: PayloadReadMode | None = None,
) -> Any:
    """Resolve a payload during the legacy/compare/sidecar rollout.

    Compare mode intentionally serves legacy while callers measure semantic
    parity. Sidecar mode falls back to legacy until backfill is complete.
    """
    selected_mode = mode or payload_read_mode()
    if selected_mode == "sidecar" and sidecar is not None:
        return sidecar
    return legacy


@contextmanager
def payload_write_transaction(conn: Any | None, *, label: str) -> Iterator[Any]:
    """Reuse a caller transaction or own one for an atomic legacy/sidecar write."""
    if conn is not None:
        yield conn
        return
    with pg.db_connection(label=label) as managed_conn:
        yield managed_conn


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        default=lambda item: item.isoformat() if hasattr(item, "isoformat") else str(item),
        separators=(",", ":"),
        sort_keys=True,
    )


def _json(value: Any) -> PgJson:
    return PgJson(value, dumps=_json_dumps)


def _uuid_text(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return str(UUID(raw))
    except ValueError:
        return None


def _seed_timestamp(value: Any) -> Any:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return value


def fetch_post_preservation_rows(
    shortcodes: Sequence[str],
    *,
    conn: Any,
) -> dict[str, dict[str, Any]]:
    """Fetch rich payload preservation state once for an ingest batch.

    This lookup deliberately prefers the sidecar and falls back to the legacy
    row regardless of the public read-mode switch.  A thin comments-header
    update must never replace a richer payload already captured in a sidecar.
    """
    keys = sorted({str(value or "").strip() for value in shortcodes if str(value or "").strip()})
    if not keys:
        return {}
    with pg.db_cursor(conn=conn, label="instagram_post_payload_preservation") as cur:
        cur.execute(
            """
            select
              p.shortcode,
              coalesce(p.views, 0)::bigint as views,
              coalesce(s.raw_data, p.raw_data, '{}'::jsonb) as raw_data,
              coalesce(s.child_posts_data, p.child_posts_data, '[]'::jsonb) as child_posts_data,
              coalesce(s.asset_manifest, p.asset_manifest, '{}'::jsonb) as asset_manifest,
              coalesce(p.media_urls, '[]'::jsonb) as media_urls,
              nullif(p.thumbnail_url, '') as thumbnail_url
            from social.instagram_posts p
            left join social.instagram_post_payloads s on s.post_id = p.id
            where p.shortcode = any(%s)
            """,
            [keys],
        )
        return {str(row["shortcode"]): dict(row) for row in (cur.fetchall() or [])}


def fetch_catalog_preservation_rows(
    source_ids: Sequence[str],
    *,
    conn: Any,
) -> dict[str, dict[str, Any]]:
    """Fetch catalog payloads sidecar-first in one bounded query."""
    keys = sorted({str(value or "").strip() for value in source_ids if str(value or "").strip()})
    if not keys:
        return {}
    with pg.db_cursor(conn=conn, label="instagram_catalog_payload_preservation") as cur:
        cur.execute(
            """
            select
              p.source_id,
              coalesce(s.raw_data, p.raw_data, '{}'::jsonb) as raw_data,
              coalesce(s.child_posts_data, p.child_posts_data, '[]'::jsonb) as child_posts_data
            from social.instagram_account_catalog_posts p
            left join social.instagram_account_catalog_post_payloads s on s.catalog_post_id = p.id
            where p.source_id = any(%s)
            """,
            [keys],
        )
        return {str(row["source_id"]): dict(row) for row in (cur.fetchall() or [])}


def post_sidecar_payload(*, legacy_row: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any] | None:
    post_id = _uuid_text(legacy_row.get("id"))
    if not post_id:
        return None
    seed_timestamp = (
        payload.get("metadata_scraped_at")
        or payload.get("scraped_at")
        or legacy_row.get("metadata_scraped_at")
        or legacy_row.get("scraped_at")
    )
    return {
        "post_id": post_id,
        "raw_data": payload.get("raw_data"),
        "asset_manifest": payload.get("asset_manifest") or {},
        "child_posts_data": payload.get("child_posts_data") or [],
        "payload_updated_at": seed_timestamp,
    }


def catalog_sidecar_payload(*, legacy_row: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any] | None:
    catalog_post_id = _uuid_text(legacy_row.get("id"))
    if not catalog_post_id:
        return None
    seed_timestamp = (
        payload.get("updated_at")
        or payload.get("last_seen_at")
        or legacy_row.get("updated_at")
        or legacy_row.get("last_seen_at")
    )
    return {
        "catalog_post_id": catalog_post_id,
        "raw_data": payload.get("raw_data") or {},
        "child_posts_data": payload.get("child_posts_data") or [],
        "payload_updated_at": seed_timestamp,
    }


def upsert_post_payloads(payloads: Sequence[Mapping[str, Any]], *, conn: Any) -> list[dict[str, Any]]:
    rows = [payload for payload in payloads if payload.get("post_id")]
    if not rows:
        return []
    values = [
        (
            row["post_id"],
            _json(row.get("raw_data")) if row.get("raw_data") is not None else None,
            _json(row.get("asset_manifest") or {}),
            _json(row.get("child_posts_data") or []),
            _seed_timestamp(row.get("payload_updated_at")),
        )
        for row in rows
    ]
    return pg.execute_values_returning(
        """
        insert into social.instagram_post_payloads (
          post_id, raw_data, asset_manifest, child_posts_data, payload_updated_at
        ) values %s
        on conflict (post_id) do update set
          raw_data = excluded.raw_data,
          asset_manifest = excluded.asset_manifest,
          child_posts_data = excluded.child_posts_data,
          payload_updated_at = case
            when (
              social.instagram_post_payloads.raw_data,
              social.instagram_post_payloads.asset_manifest,
              social.instagram_post_payloads.child_posts_data
            ) is distinct from (
              excluded.raw_data,
              excluded.asset_manifest,
              excluded.child_posts_data
            ) then now()
            else social.instagram_post_payloads.payload_updated_at
          end
        returning post_id::text, payload_updated_at
        """,
        values,
        conn=conn,
    )


def upsert_catalog_payloads(payloads: Sequence[Mapping[str, Any]], *, conn: Any) -> list[dict[str, Any]]:
    rows = [payload for payload in payloads if payload.get("catalog_post_id")]
    if not rows:
        return []
    values = [
        (
            row["catalog_post_id"],
            _json(row.get("raw_data") or {}),
            _json(row.get("child_posts_data") or []),
            _seed_timestamp(row.get("payload_updated_at")),
        )
        for row in rows
    ]
    return pg.execute_values_returning(
        """
        insert into social.instagram_account_catalog_post_payloads (
          catalog_post_id, raw_data, child_posts_data, payload_updated_at
        ) values %s
        on conflict (catalog_post_id) do update set
          raw_data = excluded.raw_data,
          child_posts_data = excluded.child_posts_data,
          payload_updated_at = case
            when (
              social.instagram_account_catalog_post_payloads.raw_data,
              social.instagram_account_catalog_post_payloads.child_posts_data
            ) is distinct from (
              excluded.raw_data,
              excluded.child_posts_data
            ) then now()
            else social.instagram_account_catalog_post_payloads.payload_updated_at
          end
        returning catalog_post_id::text, payload_updated_at
        """,
        values,
        conn=conn,
    )

