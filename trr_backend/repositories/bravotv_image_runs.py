"""Persistence helpers for durable BRAVOTV image pipeline runs."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from psycopg2 import errors as psycopg_errors

from trr_backend.db import pg

_RUN_COLUMNS = """
  id::text,
  operation_id::text,
  mode,
  status,
  target_show_id::text,
  target_person_id::text,
  show_name,
  person_name,
  season,
  episode,
  selected_sources,
  refreshed_artifacts,
  artifact_paths,
  request_payload,
  manifest,
  summary,
  import_summary,
  review_summary,
  created_by,
  error_detail,
  started_at,
  completed_at,
  created_at,
  updated_at
"""


def _to_json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=True, default=str)


def _to_array_json(values: Iterable[Any] | None) -> str:
    return json.dumps(list(values or []), ensure_ascii=True, default=str)


def _normalize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    normalized = dict(row)
    for key in ("id", "operation_id", "target_show_id", "target_person_id"):
        value = normalized.get(key)
        normalized[key] = str(value) if value is not None else None
    return normalized


def _relation_exists() -> bool:
    try:
        row = pg.fetch_one("select to_regclass(%s) is not null as exists", ["core.bravotv_image_runs"]) or {}
    except Exception:
        return False
    return bool(row.get("exists"))


def _missing_table_error() -> RuntimeError:
    return RuntimeError("BRAVOTV image runs table is not installed. Apply migration 0202_bravotv_image_runs.sql first.")


def create_run(
    *,
    mode: str,
    status: str,
    target_show_id: str | None,
    target_person_id: str | None,
    show_name: str | None,
    person_name: str | None,
    season: int | None,
    episode: int | None,
    selected_sources: Iterable[str] | None,
    refreshed_artifacts: Iterable[str] | None = None,
    request_payload: dict[str, Any] | None = None,
    created_by: str | None = None,
    operation_id: str | None = None,
) -> dict[str, Any]:
    if not _relation_exists():
        raise _missing_table_error()
    row = pg.fetch_one(
        f"""
        insert into core.bravotv_image_runs (
          operation_id,
          mode,
          status,
          target_show_id,
          target_person_id,
          show_name,
          person_name,
          season,
          episode,
          selected_sources,
          refreshed_artifacts,
          request_payload,
          created_by,
          started_at
        )
        values (
          %s::uuid,
          %s,
          %s,
          %s::uuid,
          %s::uuid,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          %s::jsonb,
          %s::jsonb,
          %s,
          case when %s = 'running' then now() else null end
        )
        returning
          {_RUN_COLUMNS}
        """,
        [
            operation_id,
            mode,
            status,
            target_show_id,
            target_person_id,
            show_name,
            person_name,
            season,
            episode,
            _to_array_json(selected_sources),
            _to_array_json(refreshed_artifacts),
            _to_json(request_payload),
            created_by,
            status,
        ],
    )
    if not row:
        raise RuntimeError("Failed to create BRAVOTV image run")
    return _normalize_row(row) or {}


def attach_operation(run_id: str, *, operation_id: str) -> dict[str, Any]:
    if not _relation_exists():
        raise _missing_table_error()
    row = pg.fetch_one(
        f"""
        update core.bravotv_image_runs
        set operation_id = %s::uuid
        where id = %s::uuid
        returning
          {_RUN_COLUMNS}
        """,
        [operation_id, run_id],
    )
    if not row:
        raise RuntimeError("Failed to attach operation to BRAVOTV image run")
    return _normalize_row(row) or {}


def update_progress(
    run_id: str,
    *,
    status: str | None = None,
    refreshed_artifacts: Iterable[str] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
    import_summary: dict[str, Any] | None = None,
    review_summary: dict[str, Any] | None = None,
    error_detail: str | None = None,
    completed: bool = False,
) -> dict[str, Any]:
    if not _relation_exists():
        raise _missing_table_error()
    row = pg.fetch_one(
        f"""
        update core.bravotv_image_runs
        set
          status = coalesce(%s, status),
          refreshed_artifacts = case when %s::jsonb is null then refreshed_artifacts else %s::jsonb end,
          artifact_paths = case when %s::jsonb is null then artifact_paths else %s::jsonb end,
          manifest = case when %s::jsonb is null then manifest else %s::jsonb end,
          summary = case when %s::jsonb is null then summary else %s::jsonb end,
          import_summary = case when %s::jsonb is null then import_summary else %s::jsonb end,
          review_summary = case when %s::jsonb is null then review_summary else %s::jsonb end,
          error_detail = coalesce(%s, error_detail),
          started_at = coalesce(started_at, now()),
          completed_at = case when %s then now() else completed_at end
        where id = %s::uuid
        returning
          {_RUN_COLUMNS}
        """,
        [
            status,
            _to_array_json(refreshed_artifacts) if refreshed_artifacts is not None else None,
            _to_array_json(refreshed_artifacts) if refreshed_artifacts is not None else None,
            _to_json(artifact_paths) if artifact_paths is not None else None,
            _to_json(artifact_paths) if artifact_paths is not None else None,
            _to_json(manifest) if manifest is not None else None,
            _to_json(manifest) if manifest is not None else None,
            _to_json(summary) if summary is not None else None,
            _to_json(summary) if summary is not None else None,
            _to_json(import_summary) if import_summary is not None else None,
            _to_json(import_summary) if import_summary is not None else None,
            _to_json(review_summary) if review_summary is not None else None,
            _to_json(review_summary) if review_summary is not None else None,
            error_detail,
            completed,
            run_id,
        ],
    )
    if not row:
        raise RuntimeError("Failed to update BRAVOTV image run")
    return _normalize_row(row) or {}


def get_run(run_id: str) -> dict[str, Any] | None:
    if not _relation_exists():
        return None
    try:
        row = pg.fetch_one(
            f"""
            select
              {_RUN_COLUMNS}
            from core.bravotv_image_runs
            where id = %s::uuid
            limit 1
            """,
            [run_id],
        )
    except psycopg_errors.UndefinedTable:
        return None
    return _normalize_row(row)


def get_latest_run(
    *,
    mode: str,
    target_show_id: str | None = None,
    target_person_id: str | None = None,
) -> dict[str, Any] | None:
    if not _relation_exists():
        return None
    filters: list[str] = ["mode = %s"]
    params: list[Any] = [mode]
    if target_show_id:
        filters.append("target_show_id = %s::uuid")
        params.append(target_show_id)
    if target_person_id:
        filters.append("target_person_id = %s::uuid")
        params.append(target_person_id)
    try:
        row = pg.fetch_one(
            f"""
            select
              {_RUN_COLUMNS}
            from core.bravotv_image_runs
            where {" and ".join(filters)}
            order by created_at desc
            limit 1
            """,
            params,
        )
    except psycopg_errors.UndefinedTable:
        return None
    return _normalize_row(row)
