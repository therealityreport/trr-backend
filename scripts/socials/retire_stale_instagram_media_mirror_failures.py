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


NON_RETRYABLE_ERRORS = {
    "http_403_auth_or_expired",
    "http_404_not_found",
    "invalid_source_url",
    "asset_too_large",
    "asset_wrong_content_type",
}
KNOWN_REASON_PREFIXES = (
    "download_failed:",
    "upload_failed:",
    "ytdlp_fallback_failed:",
)
OBSOLETE_ERROR_CODE = "obsolete_non_retryable_instagram_media_mirror_failure"
OBSOLETE_ERROR_CLASS = "ObsoleteInstagramMediaMirrorFailure"
OBSOLETE_ERROR_MESSAGE = "obsolete_non_retryable_instagram_media_mirror_failure"


@dataclass(slots=True)
class CleanupStats:
    matched_rows: int = 0
    retired_rows: int = 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="retire_stale_instagram_media_mirror_failures",
        description="Retire stale, non-retryable failed Instagram media_mirror jobs without deleting queue history.",
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


def _normalize_failure_reason(reason: str | None) -> str:
    normalized = str(reason or "").strip().lower()
    while any(normalized.startswith(prefix) for prefix in KNOWN_REASON_PREFIXES):
        normalized = normalized.split(":", 1)[1]
    return normalized


def _is_non_retryable_failure_reason(reason: str | None) -> bool:
    return _normalize_failure_reason(reason) in NON_RETRYABLE_ERRORS


def _base_where_clause(*, season_ids: list[str], show_ids: list[str]) -> tuple[str, list[object]]:
    filters = [
        "platform = 'instagram'",
        "status = 'failed'",
        "coalesce(config->>'stage', metadata->>'stage', job_type) = 'media_mirror'",
    ]
    params: list[object] = []
    if season_ids:
        filters.append("season_id::text = any(%s)")
        params.append(season_ids)
    if show_ids:
        filters.append("show_id::text = any(%s)")
        params.append(show_ids)
    return " and ".join(filters), params


def _fetch_candidates(*, season_ids: list[str], show_ids: list[str]) -> list[dict[str, object]]:
    where_clause, params = _base_where_clause(season_ids=season_ids, show_ids=show_ids)
    return pg.fetch_all(
        f"""
        select
          id::text as id,
          season_id::text as season_id,
          show_id::text as show_id,
          created_at,
          coalesce(last_error_code, '') as last_error_code,
          coalesce(error_message, '') as error_message
        from social.scrape_jobs
        where {where_clause}
        order by created_at asc, id asc
        """,
        params,
    )


def _fetch_matches(*, season_ids: list[str], show_ids: list[str]) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    for row in _fetch_candidates(season_ids=season_ids, show_ids=show_ids):
        last_error_code = str(row.get("last_error_code") or "").strip()
        error_message = str(row.get("error_message") or "").strip()
        if _is_non_retryable_failure_reason(last_error_code) or _is_non_retryable_failure_reason(error_message):
            matches.append(dict(row))
    return matches


def _retire_matches(*, season_ids: list[str], show_ids: list[str]) -> list[dict[str, object]]:
    matches = _fetch_matches(season_ids=season_ids, show_ids=show_ids)
    job_ids = [str(row.get("id") or "").strip() for row in matches if str(row.get("id") or "").strip()]
    if not job_ids:
        return []

    payload = json.dumps(
        {
            "obsolete_non_retryable_media_mirror_failure": True,
            "obsolete_failure_reason": "instagram_media_mirror_non_retryable",
            "obsolete_failure_resolution": "retired_from_retry_backlog",
        }
    )
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
          and status = %s
        returning id::text as id,
                  season_id::text as season_id,
                  show_id::text as show_id
        """,
        [OBSOLETE_ERROR_MESSAGE, OBSOLETE_ERROR_CODE, OBSOLETE_ERROR_CLASS, payload, job_ids, "failed"],
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
        retired_rows = _retire_matches(season_ids=season_ids, show_ids=show_ids)
        stats.retired_rows = len(retired_rows)

    preview = [str(row.get("id") or "") for row in matched_rows[:10] if str(row.get("id") or "").strip()]
    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "season_ids": season_ids,
                "show_ids": show_ids,
                "non_retryable_errors": sorted(NON_RETRYABLE_ERRORS),
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
