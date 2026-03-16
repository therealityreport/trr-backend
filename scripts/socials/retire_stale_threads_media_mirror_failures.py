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


STALE_THREADS_MEDIA_MIRROR_ERROR = "mirror_platform_not_supported:threads"
OBSOLETE_ERROR_CODE = "obsolete_historical_failure"
OBSOLETE_ERROR_CLASS = "ObsoleteHistoricalFailure"
OBSOLETE_ERROR_MESSAGE = f"obsolete_historical_failure:{STALE_THREADS_MEDIA_MIRROR_ERROR}"


@dataclass(slots=True)
class CleanupStats:
    matched_rows: int = 0
    retired_rows: int = 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="retire_stale_threads_media_mirror_failures",
        description="Retire historical Threads media_mirror jobs that failed before Threads mirror support existed.",
    )
    parser.add_argument(
        "--season-id",
        action="append",
        default=[],
        help="Optional season UUID filter. Repeat to target multiple seasons.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Optional show UUID filter. Repeat to target multiple shows.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matching stale failure rows without mutating them (default).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Retire the matching stale failures by marking them cancelled with obsolete-failure metadata.",
    )
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


def _stale_threads_where_clause(*, season_ids: list[str], show_ids: list[str]) -> tuple[str, list[object]]:
    filters = [
        "platform = 'threads'",
        "status = 'failed'",
        "coalesce(config->>'stage', metadata->>'stage', job_type) = 'media_mirror'",
        "coalesce(error_message, '') = %s",
    ]
    params: list[object] = [STALE_THREADS_MEDIA_MIRROR_ERROR]
    if season_ids:
        filters.append("season_id::text = any(%s)")
        params.append(season_ids)
    if show_ids:
        filters.append("show_id::text = any(%s)")
        params.append(show_ids)
    return " and ".join(filters), params


def _fetch_matches(*, season_ids: list[str], show_ids: list[str]) -> list[dict[str, object]]:
    where_clause, params = _stale_threads_where_clause(season_ids=season_ids, show_ids=show_ids)
    return pg.fetch_all(
        f"""
        select
          id::text as id,
          season_id::text as season_id,
          show_id::text as show_id,
          created_at
        from social.scrape_jobs
        where {where_clause}
        order by created_at asc, id asc
        """,
        params,
    )


def _retire_matches(*, season_ids: list[str], show_ids: list[str]) -> list[dict[str, object]]:
    where_clause, params = _stale_threads_where_clause(season_ids=season_ids, show_ids=show_ids)
    obsolete_payload = json.dumps(
        {
            "obsolete_historical_failure": True,
            "obsolete_failure_reason": STALE_THREADS_MEDIA_MIRROR_ERROR,
            "obsolete_failure_resolution": "threads_media_mirror_supported_now",
        }
    )
    return pg.execute_returning(
        f"""
        update social.scrape_jobs
        set
          status = 'cancelled',
          error_message = %s,
          last_error_code = %s,
          last_error_class = %s,
          metadata = coalesce(metadata, '{{}}'::jsonb) || %s::jsonb
        where {where_clause}
        returning
          id::text as id,
          season_id::text as season_id,
          show_id::text as show_id
        """,
        [OBSOLETE_ERROR_MESSAGE, OBSOLETE_ERROR_CODE, OBSOLETE_ERROR_CLASS, obsolete_payload, *params],
    )


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    season_ids = _normalize_text_filters(args.season_id)
    show_ids = _normalize_text_filters(args.show_id)
    dry_run = bool(args.dry_run and not args.apply)

    matched_rows = _fetch_matches(season_ids=season_ids, show_ids=show_ids)
    stats = CleanupStats(matched_rows=len(matched_rows), retired_rows=0)
    retired_rows: list[dict[str, object]] = []
    if not dry_run and matched_rows:
        retired_rows = _retire_matches(season_ids=season_ids, show_ids=show_ids)
        stats.retired_rows = len(retired_rows)

    preview = [str(row.get("id") or "") for row in matched_rows[:10] if str(row.get("id") or "").strip()]
    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "season_ids": season_ids,
                "show_ids": show_ids,
                "match_error_message": STALE_THREADS_MEDIA_MIRROR_ERROR,
                "replacement_error_message": OBSOLETE_ERROR_MESSAGE,
                "totals": {
                    "matched_rows": stats.matched_rows,
                    "retired_rows": stats.retired_rows,
                },
                "preview_job_ids": preview,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
