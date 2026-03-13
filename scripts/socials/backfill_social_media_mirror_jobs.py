#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env


@dataclass(slots=True)
class PlatformCounters:
    scanned: int = 0
    eligible: int = 0
    queued: int = 0
    skipped: int = 0
    failed: int = 0
    reason_counts: Counter[str] = field(default_factory=Counter)


REPAIR_REASON_CHOICES = (
    "legacy_host",
    "hosted_content",
    "missing_hosted_thumbnail",
    "missing_hosted_media",
    "mirror_retry",
    "non_video_hosted_media",
    "source_quality",
    "twitter_video_thumbnail",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill cross-platform media mirror jobs.")
    parser.add_argument("--weeks", type=int, default=8, help="Lookback window in weeks (default: 8)")
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Scan all available history instead of limiting by the recent weeks window.",
    )
    parser.add_argument(
        "--platforms",
        default="instagram,tiktok,youtube,twitter,facebook,threads",
        help="Comma-separated platforms (default: instagram,tiktok,youtube,twitter,facebook,threads)",
    )
    parser.add_argument(
        "--source-scope",
        default="bravo",
        choices=["bravo", "creator", "community"],
        help="Week window scope for deterministic mirror key routing.",
    )
    parser.add_argument("--limit-per-platform", type=int, default=5000, help="Max scanned rows per platform")
    parser.add_argument(
        "--season-id",
        action="append",
        default=[],
        help="Optional season UUID filter. Repeat to target multiple seasons.",
    )
    parser.add_argument(
        "--post-id",
        action="append",
        default=[],
        help="Optional platform row UUID filter. Repeat to target multiple rows.",
    )
    parser.add_argument(
        "--source-id",
        action="append",
        default=[],
        help="Optional platform source_id filter. Repeat to target multiple source items.",
    )
    parser.add_argument("--failed-only", action="store_true", help="Only requeue failed/partial mirror rows")
    parser.add_argument(
        "--hosted-html-only",
        action="store_true",
        help="Only requeue rows with hosted_media_urls entries ending in .html/.htm",
    )
    parser.add_argument(
        "--repair-reasons",
        default="",
        help=(
            "Optional comma-separated remediation reasons to include: "
            + ",".join(REPAIR_REASON_CHOICES)
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report eligible historical cleanup rows without enqueueing mirror jobs.",
    )
    return parser.parse_args()


def _row_has_html_hosted_media(row: dict[str, Any]) -> bool:
    hosted_urls = social_repo._as_text_list(row.get("hosted_media_urls"))  # noqa: SLF001
    for url in hosted_urls:
        path = (urlparse(url).path or "").strip().lower()
        if path.endswith(".html") or path.endswith(".htm"):
            return True
    return False


def _normalize_text_filters(values: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        value = str(raw or "").strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _parse_repair_reasons(value: str) -> set[str]:
    reasons = {item.strip().lower() for item in str(value or "").split(",") if item.strip()}
    invalid = sorted(reason for reason in reasons if reason not in REPAIR_REASON_CHOICES)
    if invalid:
        valid = ", ".join(REPAIR_REASON_CHOICES)
        raise SystemExit(f"Unsupported repair reasons: {', '.join(invalid)}. Valid values: {valid}")
    return reasons


def _row_repair_reasons(platform: str, row: dict[str, Any]) -> list[str]:
    normalized_platform = (platform or "").strip().lower()
    hosted_thumbnail_url = str(row.get("hosted_thumbnail_url") or "").strip()
    hosted_media_urls = social_repo._as_text_list(row.get("hosted_media_urls"))  # noqa: SLF001
    source_thumbnail_url, source_media_urls = social_repo._platform_post_source_urls(  # noqa: SLF001
        normalized_platform,
        row,
    )
    mirror_status = str(row.get("media_mirror_status") or "").strip().lower()
    source_id = social_repo._platform_source_id(normalized_platform, row)  # noqa: SLF001

    reasons: list[str] = []

    if social_repo._hosted_urls_need_cdn_host_repair(  # noqa: SLF001
        hosted_thumbnail_url=hosted_thumbnail_url,
        hosted_media_urls=hosted_media_urls,
    ):
        reasons.append("legacy_host")
    if social_repo._hosted_media_urls_need_content_repair(hosted_media_urls=hosted_media_urls):  # noqa: SLF001
        reasons.append("hosted_content")
    if social_repo._source_media_urls_need_quality_repair(  # noqa: SLF001
        platform=normalized_platform,
        source_media_urls=source_media_urls,
    ):
        reasons.append("source_quality")
    if source_thumbnail_url and not hosted_thumbnail_url:
        reasons.append("missing_hosted_thumbnail")
    if source_media_urls and len(hosted_media_urls) < len(source_media_urls):
        reasons.append("missing_hosted_media")
    if normalized_platform in {"tiktok", "youtube"} and not hosted_media_urls:
        reasons.append("missing_hosted_media")
    if (
        normalized_platform in {"tiktok", "youtube"}
        and hosted_media_urls
        and not any(social_repo._is_video_like_media_url(url) for url in hosted_media_urls)  # noqa: SLF001
    ):
        reasons.append("non_video_hosted_media")
    if normalized_platform == "instagram" and source_id:
        if mirror_status in {"failed", "partial", "pending"}:
            reasons.append("mirror_retry")
        if not source_thumbnail_url and not source_media_urls:
            reasons.append("mirror_retry")
    if (
        normalized_platform == "twitter"
        and hosted_thumbnail_url
        and social_repo._is_video_like_media_url(hosted_thumbnail_url)  # noqa: SLF001
        and (
            social_repo._select_non_video_media_url(hosted_media_urls)  # noqa: SLF001
            or social_repo._select_non_video_media_url(source_media_urls)  # noqa: SLF001
        )
    ):
        reasons.append("twitter_video_thumbnail")

    deduped: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        if reason in seen:
            continue
        seen.add(reason)
        deduped.append(reason)
    if not deduped and mirror_status in {"failed", "partial", "pending"}:
        deduped.append("mirror_retry")
    return deduped


def _load_rows(
    *,
    platform: str,
    cutoff: datetime | None,
    limit: int,
    season_ids: list[str],
    post_ids: list[str],
    source_ids: list[str],
) -> list[dict[str, Any]]:
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
    filters: list[str] = []
    params: list[Any] = []
    if cutoff is not None:
        filters.append(f"coalesce(p.{posted_at_column}, p.scraped_at) >= %s")
        params.append(cutoff)
    if season_ids:
        filters.append("p.season_id::text = any(%s)")
        params.append(season_ids)
    if post_ids:
        filters.append("p.id::text = any(%s)")
        params.append(post_ids)
    if source_ids:
        filters.append(f"p.{source_id_column}::text = any(%s)")
        params.append(source_ids)
    where_clause = f"where {' and '.join(filters)}" if filters else ""
    params.append(max(1, int(limit)))

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
        {where_clause}
        order by coalesce(p.{posted_at_column}, p.scraped_at) desc
        limit %s
        """,
        params,
    )


def main() -> int:
    load_env()
    args = _parse_args()
    if not args.dry_run:
        try:
            social_repo.ensure_media_mirror_s3_ready()
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Social media mirror object-storage preflight failed: {exc}") from exc
    now_utc = datetime.now(tz=UTC)
    cutoff = None if args.all_history else now_utc - timedelta(weeks=max(1, int(args.weeks)))
    platforms = [item.strip().lower() for item in str(args.platforms or "").split(",") if item.strip()]
    platforms = [p for p in platforms if p in social_repo.PLATFORM_POST_TABLES]
    if not platforms:
        raise SystemExit("No valid platforms requested")
    season_ids = _normalize_text_filters(args.season_id)
    post_ids = _normalize_text_filters(args.post_id)
    source_ids = _normalize_text_filters(args.source_id)
    repair_reasons = _parse_repair_reasons(args.repair_reasons)

    context_cache: dict[str, social_repo.SeasonContext] = {}
    windows_cache: dict[str, list[social_repo.WeekWindow]] = {}
    counters: dict[str, PlatformCounters] = defaultdict(PlatformCounters)

    with pg.db_connection() as conn:
        for platform in platforms:
            rows = _load_rows(
                platform=platform,
                cutoff=cutoff,
                limit=args.limit_per_platform,
                season_ids=season_ids,
                post_ids=post_ids,
                source_ids=source_ids,
            )
            for row in rows:
                counters[platform].scanned += 1
                mirror_status = str(row.get("media_mirror_status") or "").strip().lower()
                if args.failed_only and mirror_status not in {"failed", "partial"}:
                    counters[platform].skipped += 1
                    continue
                if args.hosted_html_only and not _row_has_html_hosted_media(row):
                    counters[platform].skipped += 1
                    continue
                if not social_repo._platform_post_needs_media_mirror(platform, row):  # noqa: SLF001
                    counters[platform].skipped += 1
                    continue
                row_reasons = _row_repair_reasons(platform, row)
                if repair_reasons and not repair_reasons.intersection(row_reasons):
                    counters[platform].skipped += 1
                    continue
                counters[platform].eligible += 1
                counters[platform].reason_counts.update(row_reasons)
                if args.dry_run:
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
        totals.eligible += data.eligible
        totals.queued += data.queued
        totals.skipped += data.skipped
        totals.failed += data.failed
        totals.reason_counts.update(data.reason_counts)
        by_platform[platform] = {
            "scanned": data.scanned,
            "eligible": data.eligible,
            "queued": data.queued,
            "skipped": data.skipped,
            "failed": data.failed,
            "repair_reasons": dict(data.reason_counts),
        }

    print(
        json.dumps(
            {
                "source_scope": args.source_scope,
                "weeks": max(1, int(args.weeks)),
                "all_history": bool(args.all_history),
                "cutoff": social_repo._iso(cutoff) if cutoff else None,  # noqa: SLF001
                "platforms": platforms,
                "season_ids": season_ids,
                "post_ids": post_ids,
                "source_ids": source_ids,
                "failed_only": bool(args.failed_only),
                "hosted_html_only": bool(args.hosted_html_only),
                "repair_reasons": sorted(repair_reasons),
                "dry_run": bool(args.dry_run),
                "totals": {
                    "scanned": totals.scanned,
                    "eligible": totals.eligible,
                    "queued": totals.queued,
                    "skipped": totals.skipped,
                    "failed": totals.failed,
                },
                "repair_reasons_matched": dict(totals.reason_counts),
                "by_platform": by_platform,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
