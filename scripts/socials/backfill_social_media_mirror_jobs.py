#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env


@dataclass(slots=True)
class PlatformCounters:
    scanned: int = 0
    queued: int = 0
    skipped: int = 0
    failed: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill cross-platform media mirror jobs.")
    parser.add_argument("--weeks", type=int, default=8, help="Lookback window in weeks (default: 8)")
    parser.add_argument(
        "--platforms",
        default="instagram,tiktok,youtube,twitter",
        help="Comma-separated platforms (default: instagram,tiktok,youtube,twitter)",
    )
    parser.add_argument(
        "--source-scope",
        default="bravo",
        choices=["bravo", "creator", "community"],
        help="Week window scope for deterministic mirror key routing.",
    )
    parser.add_argument("--limit-per-platform", type=int, default=5000, help="Max scanned rows per platform")
    parser.add_argument("--failed-only", action="store_true", help="Only requeue failed/partial mirror rows")
    return parser.parse_args()


def _load_rows(*, platform: str, cutoff: datetime, limit: int) -> list[dict[str, Any]]:
    normalized_platform = (platform or "").strip().lower()
    table = social_repo.PLATFORM_POST_TABLES.get(normalized_platform)
    source_id_column = social_repo.PLATFORM_SOURCE_ID_COLUMN.get(normalized_platform)
    posted_at_column = social_repo.PLATFORM_POSTED_AT_COLUMN.get(normalized_platform)
    if not table or not source_id_column or not posted_at_column:
        return []

    account_expr = (
        "coalesce(nullif(p.source_account, ''), nullif(p.channel_title, ''), '')"
        if normalized_platform == "youtube"
        else "coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')"
    )
    thumbnail_expr = (
        "coalesce(nullif(p.media_urls ->> 0, ''), '')"
        if normalized_platform == "twitter"
        else "coalesce(nullif(p.thumbnail_url, ''), '')"
    )
    media_urls_expr = (
        "p.media_urls"
        if social_repo._platform_posts_has_column(normalized_platform, "media_urls")  # noqa: SLF001
        else "'[]'::jsonb"
    )

    return pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.season_id::text as season_id,
          p.{source_id_column} as source_id,
          {account_expr} as account,
          p.{posted_at_column} as posted_at,
          {thumbnail_expr} as thumbnail_url,
          {media_urls_expr} as media_urls,
          coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
          coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(p) ->> 'media_mirror_status', '') as media_mirror_status
        from social.{table} p
        where coalesce(p.{posted_at_column}, p.scraped_at) >= %s
        order by coalesce(p.{posted_at_column}, p.scraped_at) desc
        limit %s
        """,
        [cutoff, max(1, int(limit))],
    )


def main() -> int:
    load_env()
    args = _parse_args()
    try:
        social_repo.ensure_media_mirror_s3_ready()
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Social media mirror S3 preflight failed: {exc}") from exc
    now_utc = datetime.now(tz=UTC)
    cutoff = now_utc - timedelta(weeks=max(1, int(args.weeks)))
    platforms = [item.strip().lower() for item in str(args.platforms or "").split(",") if item.strip()]
    platforms = [p for p in platforms if p in social_repo.PLATFORM_POST_TABLES]
    if not platforms:
        raise SystemExit("No valid platforms requested")

    context_cache: dict[str, social_repo.SeasonContext] = {}
    windows_cache: dict[str, list[social_repo.WeekWindow]] = {}
    counters: dict[str, PlatformCounters] = defaultdict(PlatformCounters)

    with pg.db_connection() as conn:
        for platform in platforms:
            rows = _load_rows(platform=platform, cutoff=cutoff, limit=args.limit_per_platform)
            for row in rows:
                counters[platform].scanned += 1
                mirror_status = str(row.get("media_mirror_status") or "").strip().lower()
                if args.failed_only and mirror_status not in {"failed", "partial"}:
                    counters[platform].skipped += 1
                    continue
                if not social_repo._platform_post_needs_media_mirror(platform, row):  # noqa: SLF001
                    counters[platform].skipped += 1
                    continue

                season_id = str(row.get("season_id") or "").strip()
                if not season_id:
                    counters[platform].failed += 1
                    continue

                context = context_cache.get(season_id)
                if context is None:
                    context = social_repo.get_season_context(season_id)
                    context_cache[season_id] = context
                    try:
                        season_windows, _ = social_repo._resolve_week_windows(  # noqa: SLF001
                            context,
                            timezone="America/New_York",
                            source_scope=args.source_scope,
                            now_utc=now_utc,
                        )
                    except Exception:
                        season_windows = []
                    windows_cache[season_id] = season_windows

                week_index: int | None = None
                posted_at = social_repo._coerce_dt(row.get("posted_at"))  # noqa: SLF001
                season_windows = windows_cache.get(season_id) or []
                if posted_at and season_windows:
                    week_window = social_repo._week_for_timestamp(  # noqa: SLF001
                        posted_at,
                        windows=season_windows,
                        timezone="America/New_York",
                    )
                    week_index = week_window.week_index if week_window else None

                try:
                    job_id = social_repo._enqueue_platform_media_mirror_job(  # noqa: SLF001
                        context,
                        platform=platform,
                        run_id=None,
                        source_scope=args.source_scope,
                        account=str(row.get("account") or ""),
                        post_row=row,
                        week_index=week_index,
                        parent_job_id="backfill-social-media-mirror",
                        conn=conn,
                    )
                    if job_id:
                        counters[platform].queued += 1
                    else:
                        counters[platform].skipped += 1
                except Exception:
                    counters[platform].failed += 1

    totals = PlatformCounters()
    by_platform = {}
    for platform in platforms:
        data = counters[platform]
        totals.scanned += data.scanned
        totals.queued += data.queued
        totals.skipped += data.skipped
        totals.failed += data.failed
        by_platform[platform] = {
            "scanned": data.scanned,
            "queued": data.queued,
            "skipped": data.skipped,
            "failed": data.failed,
        }

    print(
        json.dumps(
            {
                "source_scope": args.source_scope,
                "weeks": max(1, int(args.weeks)),
                "cutoff": social_repo._iso(cutoff),  # noqa: SLF001
                "platforms": platforms,
                "failed_only": bool(args.failed_only),
                "totals": {
                    "scanned": totals.scanned,
                    "queued": totals.queued,
                    "skipped": totals.skipped,
                    "failed": totals.failed,
                },
                "by_platform": by_platform,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
