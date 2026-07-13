#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


DUPLICATE_ERROR_CODE = "duplicate_media_mirror_job_retired"
DUPLICATE_ERROR_CLASS = "DuplicateMediaMirrorJobRetired"
DUPLICATE_ERROR_MESSAGE = "duplicate_active_media_mirror_job_retired"
ACTIVE_DUPLICATE_STATUSES = ("queued", "pending", "retrying", "running")


@dataclass(slots=True)
class CleanupStats:
    matched_rows: int = 0
    retired_rows: int = 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="retire_duplicate_instagram_media_mirror_jobs",
        description="Retire duplicate active Instagram media_mirror jobs before uniqueness enforcement.",
    )
    parser.add_argument("--season-id", action="append", default=[], help="Optional season UUID filter.")
    parser.add_argument("--show-id", action="append", default=[], help="Optional show UUID filter.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Preview matching duplicate rows (default).")
    mode.add_argument("--apply", action="store_true", help="Retire duplicate rows by marking them cancelled.")
    parser.set_defaults(dry_run=True)
    return parser.parse_args(argv)


def _normalize_text_filters(values: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _duplicate_media_where_clause(*, season_ids: list[str], show_ids: list[str]) -> tuple[str, list[object]]:
    filters = [
        "platform = 'instagram'",
        "status in ('queued', 'pending', 'retrying', 'running')",
        "coalesce(config->>'stage', metadata->>'stage', job_type) = 'media_mirror'",
        "coalesce(config->>'post_id', '') <> ''",
    ]
    params: list[object] = []
    if season_ids:
        filters.append("season_id::text = any(%s)")
        params.append(season_ids)
    if show_ids:
        filters.append("show_id::text = any(%s)")
        params.append(show_ids)
    return " and ".join(filters), params


def _fetch_matches(*, season_ids: list[str], show_ids: list[str]) -> list[dict[str, object]]:
    where_clause, params = _duplicate_media_where_clause(season_ids=season_ids, show_ids=show_ids)
    return pg.fetch_all(
        f"""
        with ranked as (
          select
            id::text as id,
            season_id::text as season_id,
            show_id::text as show_id,
            config->>'post_id' as post_id,
            created_at,
            first_value(id::text) over (
              partition by platform, config->>'post_id'
              order by created_at desc, id desc
            ) as keep_job_id,
            row_number() over (
              partition by platform, config->>'post_id'
              order by created_at desc, id desc
            ) as row_num,
            count(*) over (
              partition by platform, config->>'post_id'
            ) as duplicate_count
          from social.scrape_jobs
          where {where_clause}
        )
        select
          id,
          season_id,
          show_id,
          post_id,
          keep_job_id,
          duplicate_count,
          created_at
        from ranked
        where row_num > 1
        order by post_id asc, created_at desc, id desc
        """,
        params,
    )


def _retire_matches(*, season_ids: list[str], show_ids: list[str]) -> list[dict[str, object]]:
    matched_rows = _fetch_matches(season_ids=season_ids, show_ids=show_ids)
    duplicate_ids = [str(row.get("id") or "").strip() for row in matched_rows if str(row.get("id") or "").strip()]
    if not duplicate_ids:
        return []
    payload = json.dumps({"duplicate_active_media_mirror_job": True})
    return pg.execute_returning(
        """
        update social.scrape_jobs
        set
          status = 'cancelled',
          error_message = %s,
          last_error_code = %s,
          last_error_class = %s,
          metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb
        where id::text = any(%s)
          and status = any(%s)
        returning id::text as id
        """,
        [
            DUPLICATE_ERROR_MESSAGE,
            DUPLICATE_ERROR_CODE,
            DUPLICATE_ERROR_CLASS,
            payload,
            duplicate_ids,
            list(ACTIVE_DUPLICATE_STATUSES),
        ],
    )


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    season_ids = _normalize_text_filters(args.season_id)
    show_ids = _normalize_text_filters(args.show_id)
    dry_run = bool(args.dry_run and not args.apply)

    matched_rows = _fetch_matches(season_ids=season_ids, show_ids=show_ids)
    stats = CleanupStats(matched_rows=len(matched_rows), retired_rows=0)
    if not dry_run and matched_rows:
        stats.retired_rows = len(_retire_matches(season_ids=season_ids, show_ids=show_ids))

    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "season_ids": season_ids,
                "show_ids": show_ids,
                "totals": {"matched_rows": stats.matched_rows, "retired_rows": stats.retired_rows},
                "preview_job_ids": [str(row.get("id") or "") for row in matched_rows[:10]],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
