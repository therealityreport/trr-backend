"""Fenced persistence helpers for show + season media watchers.

Worker writes always include the current ``lease_owner`` and ``lease_fence``.
This makes a worker which loses its lease unable to advance state after it
finishes network I/O.  Operator pause/resume is deliberately separate: it
increments the fence before changing status so it also invalidates a worker.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from trr_backend.db import pg

_WATCH_COLUMNS = """
  id::text AS id,
  show_id::text AS show_id,
  season_id::text AS season_id,
  target_season_number,
  nbcumv_show_id,
  bravo_show_uuid::text AS bravo_show_uuid,
  source_season_rules,
  qualification_rules_version,
  status,
  sources,
  resource_types,
  poll_interval_seconds,
  backfill_mode,
  overlap_seconds,
  r2_prefix,
  desktop_folder_name,
  next_check_at,
  lease_owner,
  lease_expires_at,
  lease_fence,
  lease_heartbeat_at,
  source_state,
  baseline_completed_at,
  last_checked_at,
  last_success_at,
  consecutive_failures,
  last_error,
  created_by,
  created_at,
  updated_at
"""

_RUN_COLUMNS = """
  id::text AS id,
  watch_id::text AS watch_id,
  lease_fence,
  baseline_generation_id::text AS baseline_generation_id,
  bravotv_image_run_id::text AS bravotv_image_run_id,
  status,
  source_state_before,
  source_state_after,
  cursor_journal,
  candidate_journal,
  summary,
  continuation,
  error_detail,
  started_at,
  completed_at,
  created_at,
  updated_at
"""


def _qualified_columns(columns: str, alias: str) -> str:
    """Qualify a simple comma-separated projection for UPDATE ... FROM queries."""
    return "\n".join(
        f"  {alias}.{line.strip()}" if line.strip() else ""
        for line in columns.splitlines()
    )


_WATCH_COLUMNS_FROM_WATCH = _qualified_columns(_WATCH_COLUMNS, "watch")
_RUN_COLUMNS_FROM_RUN = _qualified_columns(_RUN_COLUMNS, "run")


def _json(value: Mapping[str, Any] | Iterable[Any] | None, *, default: str) -> str:
    if value is None:
        return default
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def _required_text(value: str, name: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError(f"{name} is required")
    return cleaned


def _lease_seconds(value: int) -> int:
    return max(15, min(int(value), 3600))


def _normalize(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def create_watch(
    *,
    show_id: str,
    season_id: str,
    target_season_number: int,
    nbcumv_show_id: str,
    bravo_show_uuid: str,
    source_season_rules: Mapping[str, Any],
    qualification_rules_version: str,
    r2_prefix: str,
    desktop_folder_name: str,
    sources: Iterable[str] = ("nbcumv", "bravo"),
    resource_types: Iterable[str] = ("image",),
    poll_interval_seconds: int = 60,
    overlap_seconds: int = 300,
    backfill_mode: bool = False,
    created_by: str | None = None,
) -> dict[str, Any]:
    """Create a configuration; the composite FK verifies show/season/number."""
    rows = pg.execute_returning(
        f"""
        INSERT INTO core.show_season_media_watches (
          show_id, season_id, target_season_number, nbcumv_show_id, bravo_show_uuid,
          source_season_rules, qualification_rules_version, r2_prefix,
          desktop_folder_name, sources, resource_types, poll_interval_seconds,
          overlap_seconds, backfill_mode, created_by
        ) VALUES (
          %s::uuid, %s::uuid, %s::int, %s, %s::uuid, %s::jsonb, %s, %s, %s,
          %s::jsonb, %s::jsonb, %s::int, %s::int, %s, %s
        )
        RETURNING {_WATCH_COLUMNS}
        """,
        [
            _required_text(show_id, "show_id"),
            _required_text(season_id, "season_id"),
            int(target_season_number),
            _required_text(nbcumv_show_id, "nbcumv_show_id"),
            _required_text(bravo_show_uuid, "bravo_show_uuid"),
            _json(source_season_rules, default="{}"),
            _required_text(qualification_rules_version, "qualification_rules_version"),
            _required_text(r2_prefix, "r2_prefix"),
            _required_text(desktop_folder_name, "desktop_folder_name"),
            _json(list(sources), default="[]"),
            _json(list(resource_types), default="[]"),
            int(poll_interval_seconds),
            int(overlap_seconds),
            bool(backfill_mode),
            created_by,
        ],
    )
    if not rows:
        raise RuntimeError("Failed to create show-season media watch")
    return dict(rows[0])


def get_watch(watch_id: str) -> dict[str, Any] | None:
    return _normalize(
        pg.fetch_one(
            f"""
            SELECT {_WATCH_COLUMNS}
            FROM core.show_season_media_watches
            WHERE id = %s::uuid
            """,
            [_required_text(watch_id, "watch_id")],
        )
    )


def claim_due_watch(*, lease_owner: str, lease_seconds: int = 180) -> dict[str, Any] | None:
    """Claim exactly one due watch using SKIP LOCKED and a new fencing token."""
    owner = _required_text(lease_owner, "lease_owner")
    row = pg.fetch_one(
        f"""
        WITH candidate AS (
          SELECT id
          FROM core.show_season_media_watches
          WHERE status = 'active'
            AND next_check_at <= now()
            AND (lease_expires_at IS NULL OR lease_expires_at <= now())
          ORDER BY next_check_at ASC, id ASC
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE core.show_season_media_watches AS watch
        SET lease_owner = %s,
            lease_expires_at = now() + (%s::int * interval '1 second'),
            lease_heartbeat_at = now(),
            lease_fence = watch.lease_fence + 1,
            last_checked_at = now()
        FROM candidate
        WHERE watch.id = candidate.id
        RETURNING {_WATCH_COLUMNS_FROM_WATCH}
        """,
        [owner, _lease_seconds(lease_seconds)],
    )
    return _normalize(row)


def heartbeat_lease(
    *, watch_id: str, lease_owner: str, lease_fence: int, lease_seconds: int = 180
) -> bool:
    row = pg.fetch_one(
        """
        UPDATE core.show_season_media_watches
        SET lease_heartbeat_at = now(),
            lease_expires_at = now() + (%s::int * interval '1 second')
        WHERE id = %s::uuid
          AND lease_owner = %s
          AND lease_fence = %s::bigint
          AND lease_expires_at > now()
        RETURNING id
        """,
        [
            _lease_seconds(lease_seconds),
            _required_text(watch_id, "watch_id"),
            _required_text(lease_owner, "lease_owner"),
            int(lease_fence),
        ],
    )
    return bool(row)


def start_run(
    *,
    watch_id: str,
    lease_owner: str,
    lease_fence: int,
    source_state_before: Mapping[str, Any],
    baseline_generation_id: str | None = None,
    bravotv_image_run_id: str | None = None,
) -> dict[str, Any] | None:
    """Open the durable whole-scan journal only for the current lease fence."""
    row = pg.fetch_one(
        f"""
        INSERT INTO core.show_season_media_watch_runs (
          watch_id, lease_fence, baseline_generation_id, bravotv_image_run_id,
          source_state_before
        )
        SELECT watch.id, watch.lease_fence, %s::uuid, %s::uuid, %s::jsonb
        FROM core.show_season_media_watches AS watch
        WHERE watch.id = %s::uuid
          AND watch.lease_owner = %s
          AND watch.lease_fence = %s::bigint
          AND watch.lease_expires_at > now()
        RETURNING {_RUN_COLUMNS}
        """,
        [
            baseline_generation_id,
            bravotv_image_run_id,
            _json(source_state_before, default="{}"),
            _required_text(watch_id, "watch_id"),
            _required_text(lease_owner, "lease_owner"),
            int(lease_fence),
        ],
    )
    return _normalize(row)


def update_run_journal(
    *,
    run_id: str,
    watch_id: str,
    lease_owner: str,
    lease_fence: int,
    cursor_journal: Mapping[str, Any] | None = None,
    candidate_journal: Mapping[str, Any] | None = None,
    summary: Mapping[str, Any] | None = None,
    continuation: Mapping[str, Any] | None = None,
    source_state_after: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Checkpoint an in-progress scan without committing source watermarks."""
    row = pg.fetch_one(
        f"""
        UPDATE core.show_season_media_watch_runs AS run
        SET cursor_journal = COALESCE(%s::jsonb, run.cursor_journal),
            candidate_journal = COALESCE(%s::jsonb, run.candidate_journal),
            summary = COALESCE(%s::jsonb, run.summary),
            continuation = COALESCE(%s::jsonb, run.continuation),
            source_state_after = COALESCE(%s::jsonb, run.source_state_after)
        FROM core.show_season_media_watches AS watch
        WHERE run.id = %s::uuid
          AND run.watch_id = %s::uuid
          AND watch.id = run.watch_id
          AND watch.lease_owner = %s
          AND watch.lease_fence = %s::bigint
          AND watch.lease_expires_at > now()
        RETURNING {_RUN_COLUMNS_FROM_RUN}
        """,
        [
            _json(cursor_journal, default="{}") if cursor_journal is not None else None,
            _json(candidate_journal, default="{}") if candidate_journal is not None else None,
            _json(summary, default="{}") if summary is not None else None,
            _json(continuation, default="{}") if continuation is not None else None,
            _json(source_state_after, default="{}") if source_state_after is not None else None,
            _required_text(run_id, "run_id"),
            _required_text(watch_id, "watch_id"),
            _required_text(lease_owner, "lease_owner"),
            int(lease_fence),
        ],
    )
    return _normalize(row)


def finish_run(
    *,
    run_id: str,
    watch_id: str,
    lease_owner: str,
    lease_fence: int,
    status: str,
    source_state_after: Mapping[str, Any],
    next_check_seconds: int,
    summary: Mapping[str, Any] | None = None,
    continuation: Mapping[str, Any] | None = None,
    error_detail: str | None = None,
) -> dict[str, Any] | None:
    """Atomically finish a journal then advance watch state under the same fence.

    A completed scan alone can move the durable source watermark.  Incomplete
    and failed scans retain the prior state while preserving their journal.
    """
    if status not in {"completed", "incomplete", "failed", "fenced"}:
        raise ValueError("invalid watcher run status")
    with pg.db_connection(label="finish-show-season-media-watch-run") as conn:
        run_rows = pg.execute_returning(
            f"""
            UPDATE core.show_season_media_watch_runs AS run
            SET status = %s,
                source_state_after = %s::jsonb,
                summary = COALESCE(%s::jsonb, run.summary),
                continuation = COALESCE(%s::jsonb, run.continuation),
                error_detail = %s,
                completed_at = now()
            FROM core.show_season_media_watches AS watch
            WHERE run.id = %s::uuid
              AND run.watch_id = %s::uuid
              AND watch.id = run.watch_id
              AND watch.lease_owner = %s
              AND watch.lease_fence = %s::bigint
              AND watch.lease_expires_at > now()
            RETURNING {_RUN_COLUMNS_FROM_RUN}
            """,
            [
                status,
                _json(source_state_after, default="{}"),
                _json(summary, default="{}") if summary is not None else None,
                _json(continuation, default="{}") if continuation is not None else None,
                error_detail,
                _required_text(run_id, "run_id"),
                _required_text(watch_id, "watch_id"),
                _required_text(lease_owner, "lease_owner"),
                int(lease_fence),
            ],
            conn=conn,
        )
        if not run_rows:
            return None
        watch_rows = pg.execute_returning(
            f"""
            UPDATE core.show_season_media_watches
            SET source_state = CASE WHEN %s = 'completed' THEN %s::jsonb ELSE source_state END,
                next_check_at = now() + (%s::int * interval '1 second'),
                last_success_at = CASE WHEN %s = 'completed' THEN now() ELSE last_success_at END,
                consecutive_failures = CASE WHEN %s = 'failed' THEN consecutive_failures + 1 ELSE 0 END,
                last_error = CASE WHEN %s = 'failed' THEN %s ELSE NULL END,
                lease_owner = NULL,
                lease_expires_at = NULL,
                lease_heartbeat_at = now()
            WHERE id = %s::uuid
              AND lease_owner = %s
              AND lease_fence = %s::bigint
              AND lease_expires_at > now()
            RETURNING {_WATCH_COLUMNS}
            """,
            [
                status,
                _json(source_state_after, default="{}"),
                max(1, int(next_check_seconds)),
                status,
                status,
                status,
                error_detail,
                _required_text(watch_id, "watch_id"),
                _required_text(lease_owner, "lease_owner"),
                int(lease_fence),
            ],
            conn=conn,
        )
        if not watch_rows:
            raise RuntimeError("watch lease fence was lost while completing the run")
    return dict(run_rows[0])


def start_baseline_generation(
    *, watch_id: str, lease_owner: str, lease_fence: int, created_by: str | None = None
) -> dict[str, Any] | None:
    """Snapshot immutable mapping/qualification rules for a new baseline."""
    return _normalize(
        pg.fetch_one(
            """
            INSERT INTO core.show_season_media_watch_baseline_generations (
              watch_id, generation, qualification_rules_version,
              source_season_rules_snapshot, created_by
            )
            SELECT watch.id,
                   COALESCE((SELECT max(generation) + 1
                             FROM core.show_season_media_watch_baseline_generations
                             WHERE watch_id = watch.id), 1),
                   watch.qualification_rules_version,
                   watch.source_season_rules,
                   %s
            FROM core.show_season_media_watches AS watch
            WHERE watch.id = %s::uuid
              AND watch.lease_owner = %s
              AND watch.lease_fence = %s::bigint
              AND watch.lease_expires_at > now()
            RETURNING id::text AS id, watch_id::text AS watch_id, generation,
                      qualification_rules_version, source_season_rules_snapshot,
                      status, started_at, completed_at, created_by, created_at
            """,
            [
                created_by,
                _required_text(watch_id, "watch_id"),
                _required_text(lease_owner, "lease_owner"),
                int(lease_fence),
            ],
        )
    )


def upsert_observation(
    *,
    watch_id: str,
    lease_owner: str,
    lease_fence: int,
    source: str,
    source_asset_id: str,
    source_fingerprint: Mapping[str, Any],
    source_updated_at: Any = None,
    source_url: str | None = None,
    raw_season_fields: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    baseline_generation_id: str | None = None,
    acquisition_state: str = "observed_without_bytes",
    revalidate_after: Any = None,
) -> dict[str, Any] | None:
    """Persist current source state only while the caller still owns its fence."""
    row = pg.fetch_one(
        """
        INSERT INTO core.show_season_media_watch_observations (
          watch_id, baseline_generation_id, source, source_asset_id, source_updated_at,
          source_fingerprint, source_url, raw_season_fields, metadata,
          acquisition_state, revalidate_after
        )
        SELECT watch.id, %s::uuid, %s, %s, %s, %s::jsonb, %s, %s::jsonb,
               %s::jsonb, %s, %s
        FROM core.show_season_media_watches AS watch
        WHERE watch.id = %s::uuid
          AND watch.lease_owner = %s
          AND watch.lease_fence = %s::bigint
          AND watch.lease_expires_at > now()
        ON CONFLICT (watch_id, source, source_asset_id) DO UPDATE
        SET baseline_generation_id = COALESCE(EXCLUDED.baseline_generation_id,
                                             core.show_season_media_watch_observations.baseline_generation_id),
            source_updated_at = EXCLUDED.source_updated_at,
            source_fingerprint = EXCLUDED.source_fingerprint,
            source_url = EXCLUDED.source_url,
            raw_season_fields = EXCLUDED.raw_season_fields,
            metadata = EXCLUDED.metadata,
            acquisition_state = EXCLUDED.acquisition_state,
            revalidate_after = EXCLUDED.revalidate_after
        RETURNING id::text AS id, watch_id::text AS watch_id,
                  baseline_generation_id::text AS baseline_generation_id, source,
                  source_asset_id, acquisition_state, source_fingerprint,
                  source_updated_at, source_url, raw_season_fields, metadata,
                  revalidate_after, last_acquired_at, created_at, updated_at
        """,
        [
            baseline_generation_id,
            _required_text(source, "source"),
            _required_text(source_asset_id, "source_asset_id"),
            source_updated_at,
            _json(source_fingerprint, default="{}"),
            source_url,
            _json(raw_season_fields, default="{}"),
            _json(metadata, default="{}"),
            acquisition_state,
            revalidate_after,
            _required_text(watch_id, "watch_id"),
            _required_text(lease_owner, "lease_owner"),
            int(lease_fence),
        ],
    )
    return _normalize(row)


def insert_source_revision(
    *,
    watch_id: str,
    lease_owner: str,
    lease_fence: int,
    media_asset_id: str,
    source: str,
    source_asset_id: str,
    sha256: str,
    acquisition_state: str = "db_committed",
    **values: Any,
) -> dict[str, Any] | None:
    """Insert an immutable revision idempotently under the current lease fence."""
    row = pg.fetch_one(
        """
        INSERT INTO core.media_source_revisions (
          watch_id, media_asset_id, source, source_asset_id, sha256,
          source_updated_at, content_type, bytes, width, height, etag, source_url,
          hosted_bucket, hosted_key, hosted_url, fetched_at, metadata, acquisition_state
        )
        SELECT watch.id, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s, COALESCE(%s, now()), %s::jsonb, %s
        FROM core.show_season_media_watches AS watch
        WHERE watch.id = %s::uuid
          AND watch.lease_owner = %s
          AND watch.lease_fence = %s::bigint
          AND watch.lease_expires_at > now()
        ON CONFLICT (media_asset_id, sha256) DO NOTHING
        RETURNING id::text AS id, watch_id::text AS watch_id,
                  media_asset_id::text AS media_asset_id, source, source_asset_id,
                  sha256, acquisition_state, hosted_bucket, hosted_key, hosted_url,
                  created_at
        """,
        [
            _required_text(media_asset_id, "media_asset_id"),
            _required_text(source, "source"),
            _required_text(source_asset_id, "source_asset_id"),
            _required_text(sha256, "sha256").lower(),
            values.get("source_updated_at"),
            values.get("content_type"),
            values.get("bytes"),
            values.get("width"),
            values.get("height"),
            values.get("etag"),
            values.get("source_url"),
            values.get("hosted_bucket"),
            values.get("hosted_key"),
            values.get("hosted_url"),
            values.get("fetched_at"),
            _json(values.get("metadata"), default="{}"),
            acquisition_state,
            _required_text(watch_id, "watch_id"),
            _required_text(lease_owner, "lease_owner"),
            int(lease_fence),
        ],
    )
    return _normalize(row)


def pause_watch(*, watch_id: str) -> dict[str, Any] | None:
    """Fence any active worker before pausing; no history is deleted."""
    return _normalize(
        pg.fetch_one(
            f"""
            UPDATE core.show_season_media_watches
            SET status = 'paused', lease_fence = lease_fence + 1,
                lease_owner = NULL, lease_expires_at = NULL, lease_heartbeat_at = now()
            WHERE id = %s::uuid
            RETURNING {_WATCH_COLUMNS}
            """,
            [_required_text(watch_id, "watch_id")],
        )
    )


def resume_watch(*, watch_id: str) -> dict[str, Any] | None:
    """Re-enable a watch and make it due without reusing a prior lease fence."""
    return _normalize(
        pg.fetch_one(
            f"""
            UPDATE core.show_season_media_watches
            SET status = 'active', next_check_at = now(), lease_fence = lease_fence + 1,
                lease_owner = NULL, lease_expires_at = NULL, lease_heartbeat_at = now()
            WHERE id = %s::uuid
              AND status IN ('paused', 'disabled')
            RETURNING {_WATCH_COLUMNS}
            """,
            [_required_text(watch_id, "watch_id")],
        )
    )
