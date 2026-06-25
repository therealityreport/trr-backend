#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


CSV_COLUMNS = [
    "shortcode",
    "post_id",
    "posted_at",
    "reported_comment_count",
    "saved_comment_count",
    "detail_refresh_incomplete",
    "comments_incomplete",
    "media_mirror_incomplete",
    "missing_materialized",
    "hard_media_error",
    "media_mirror_status",
    "media_mirror_error",
    "hosted_thumbnail_present",
    "hosted_media_url_count",
]


def _csv_value(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def fetch_rows(*, account_handle: str, run_id: str | None = None) -> list[dict[str, Any]]:
    normalized_account = str(account_handle or "").strip().lower().lstrip("@")
    normalized_run_id = str(run_id or "").strip() or None
    if not normalized_account:
        raise ValueError("account_handle is required")
    run_filter = "and c.last_backfill_run_id = %s::uuid" if normalized_run_id else ""
    params: list[Any] = [normalized_account]
    if normalized_run_id:
        params.append(normalized_run_id)
    return pg.fetch_all(
        f"""
        with catalog as (
          select distinct on (c.source_id)
            c.source_id as shortcode,
            c.posted_at
          from social.instagram_account_catalog_posts c
          where lower(c.source_account) = %s
            and nullif(c.source_id, '') is not null
            {run_filter}
          order by c.source_id, c.posted_at desc nulls last, c.id desc
        ),
        joined as (
          select
            c.shortcode,
            ip.id::text as post_id,
            coalesce(ip.posted_at, c.posted_at) as posted_at,
            coalesce(ip.comments_count, 0)::int as reported_comment_count,
            coalesce(r.active_comment_count, 0)::int as saved_comment_count,
            (
              ip.id is null
              or ip.scraped_at is null
              or ip.raw_data is null
              or ip.raw_data = '{{}}'::jsonb
              or nullif(ip.permalink, '') is null
            ) as detail_refresh_incomplete,
            (
              coalesce(ip.comments_count, 0) > coalesce(r.active_comment_count, 0)
            ) as comments_incomplete,
            (
              ip.id is not null
              and coalesce(nullif(ip.media_mirror_status, ''), 'pending')
                not in ('mirrored', 'complete', 'completed', 'up_to_date', 'unrecoverable')
            ) as media_mirror_incomplete,
            (ip.id is null) as missing_materialized,
            (
              coalesce(ip.media_mirror_error, '') ilike '%%asset_too_large%%'
              or coalesce(ip.media_mirror_error, '') ilike '%%invalid_source_url%%'
              or coalesce(ip.media_mirror_error, '') ilike '%%empty_response_body%%'
            ) as hard_media_error,
            nullif(ip.media_mirror_status, '') as media_mirror_status,
            nullif(ip.media_mirror_error, '') as media_mirror_error,
            (nullif(ip.hosted_thumbnail_url, '') is not null) as hosted_thumbnail_present,
            coalesce(jsonb_array_length(
              case
                when jsonb_typeof(to_jsonb(ip.hosted_media_urls)) = 'array'
                then to_jsonb(ip.hosted_media_urls)
                else '[]'::jsonb
              end
            ), 0)::int as hosted_media_url_count
          from catalog c
          left join social.instagram_posts ip on ip.shortcode = c.shortcode
          left join social.instagram_post_comment_rollups r on r.post_id = ip.id
        )
        select *
        from joined
        where detail_refresh_incomplete
           or comments_incomplete
           or media_mirror_incomplete
           or missing_materialized
           or hard_media_error
        order by posted_at desc nulls last, shortcode
        """,
        params,
    )


def write_csv(rows: list[dict[str, Any]], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in CSV_COLUMNS})
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export unresolved Instagram backfill gaps to CSV.")
    parser.add_argument("--account", default="bravotv", help="Instagram account handle.")
    parser.add_argument("--run-id", default=None, help="Optional catalog scrape run id.")
    parser.add_argument("--output", type=Path, required=True, help="CSV output path.")
    parser.add_argument("--no-env", action="store_true", help="Skip loading local .env files.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.no_env:
        load_env()
    rows = fetch_rows(account_handle=args.account, run_id=args.run_id)
    output = write_csv(rows, args.output)
    print(f"wrote {len(rows)} unresolved row(s) to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
