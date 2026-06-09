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
    "legacy_hosted_url",
    "hosted_content",
    "missing_hosted_thumbnail",
    "missing_hosted_media",
    "missing_source_avatar",
    "missing_hosted_avatar",
    "mirror_retry",
    "non_video_hosted_media",
    "source_quality",
    "stale_media_metadata",
    "twitter_video_thumbnail",
    "missing_display_variants",
)

REPAIR_REASON_ALIASES = {
    "legacy_host": "legacy_hosted_url",
}


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
        "--show-id",
        action="append",
        default=[],
        help="Optional show UUID filter. Repeat to target multiple shows.",
    )
    parser.add_argument(
        "--season-number",
        action="append",
        default=[],
        help="Optional season number filter. Repeat to target multiple season numbers.",
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
        help=("Optional comma-separated remediation reasons to include: " + ",".join(REPAIR_REASON_CHOICES)),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report eligible historical cleanup rows without enqueueing mirror jobs.",
    )
    parser.add_argument(
        "--commit-batch-size",
        type=int,
        default=100,
        help="Commit queued jobs every N successful enqueue operations (default: 100).",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--normalize-only",
        action="store_true",
        help="Only include rows whose remaining work is hosted-URL normalization.",
    )
    mode_group.add_argument(
        "--mirror-only",
        action="store_true",
        help="Exclude normalization-only rows and target rows that still need mirror/avatar/media work.",
    )
    mode_group.add_argument(
        "--repair-all",
        action="store_true",
        help="Explicitly include all repair classes (default behavior).",
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
    reasons = {
        REPAIR_REASON_ALIASES.get(item.strip().lower(), item.strip().lower())
        for item in str(value or "").split(",")
        if item.strip()
    }
    invalid = sorted(reason for reason in reasons if reason not in REPAIR_REASON_CHOICES)
    if invalid:
        valid = ", ".join(REPAIR_REASON_CHOICES)
        raise SystemExit(f"Unsupported repair reasons: {', '.join(invalid)}. Valid values: {valid}")
    return reasons


def _row_repair_reasons(platform: str, row: dict[str, Any]) -> list[str]:
    return social_repo._platform_post_repair_reasons((platform or "").strip().lower(), row)  # noqa: SLF001


def _normalize_int_filters(values: list[str] | tuple[str, ...] | None) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for raw in values or []:
        try:
            value = int(str(raw or "").strip())
        except (TypeError, ValueError):
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _row_matches_mode(row_reasons: list[str], *, normalize_only: bool, mirror_only: bool) -> bool:
    reason_set = set(row_reasons)
    if normalize_only:
        return bool(reason_set) and reason_set.issubset({"legacy_hosted_url"})
    if mirror_only:
        return bool(reason_set - {"legacy_hosted_url"})
    return True


def _load_rows(
    *,
    platform: str,
    cutoff: datetime | None,
    limit: int,
    season_ids: list[str],
    show_ids: list[str],
    season_numbers: list[int],
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
    if show_ids:
        filters.append("p.show_id::text = any(%s)")
        params.append(show_ids)
    if season_numbers:
        filters.append("s.season_number = any(%s)")
        params.append(season_numbers)
    if post_ids:
        filters.append("p.id::text = any(%s)")
        params.append(post_ids)
    if source_ids:
        filters.append(f"p.{source_id_column}::text = any(%s)")
        params.append(source_ids)
    filters.append(
        """
        not exists (
          select 1
          from social.scrape_jobs j
          where j.platform = %s
            and j.status in ('queued', 'pending', 'retrying', 'running')
            and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
            and j.config->>'post_id' = p.id::text
        )
        """.strip()
    )
    params.extend([normalized_platform, social_repo.INSTAGRAM_MEDIA_MIRROR_STAGE])
    where_clause = f"where {' and '.join(filters)}" if filters else ""
    params.append(max(1, int(limit)))

    return pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.season_id::text as season_id,
          p.show_id::text as show_id,
          s.season_number as season_number,
          p.{source_id_column} as source_id,
          {account_expr} as account,
          p.{posted_at_column} as posted_at,
          {thumbnail_expr} as thumbnail_url,
          {media_urls_expr} as media_urls,
          coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
          coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(p) ->> 'media_mirror_status', '') as media_mirror_status,
          coalesce(to_jsonb(p) -> 'asset_manifest', '{{}}'::jsonb) as asset_manifest,
          coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
          coalesce(to_jsonb(p) ->> 'user_avatar_url', '') as user_avatar_url,
          coalesce(to_jsonb(p) ->> 'hosted_user_avatar_url', '') as hosted_user_avatar_url,
          coalesce(to_jsonb(p) ->> 'owner_profile_pic_url', '') as owner_profile_pic_url,
          coalesce(to_jsonb(p) ->> 'hosted_owner_profile_pic_url', '') as hosted_owner_profile_pic_url,
          coalesce(to_jsonb(p) -> 'hosted_tagged_profile_pics', '{{}}'::jsonb) as hosted_tagged_profile_pics
        from social.{table} p
        left join core.seasons s on s.id = p.season_id
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
    show_ids = _normalize_text_filters(args.show_id)
    season_numbers = _normalize_int_filters(args.season_number)
    post_ids = _normalize_text_filters(args.post_id)
    source_ids = _normalize_text_filters(args.source_id)
    repair_reasons = _parse_repair_reasons(args.repair_reasons)
    source_scope = social_repo.normalize_source_scope(args.source_scope)
    commit_batch_size = max(1, int(getattr(args, "commit_batch_size", 100)))

    context_cache: dict[str, social_repo.SeasonContext] = {}
    windows_cache: dict[str, list[social_repo.WeekWindow]] = {}
    counters: dict[str, PlatformCounters] = defaultdict(PlatformCounters)

    with pg.db_connection() as conn:
        pending_commits = 0
        for platform in platforms:
            rows = _load_rows(
                platform=platform,
                cutoff=cutoff,
                limit=args.limit_per_platform,
                season_ids=season_ids,
                show_ids=show_ids,
                season_numbers=season_numbers,
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
                row_reasons = _row_repair_reasons(platform, row)
                if not row_reasons:
                    counters[platform].skipped += 1
                    continue
                if not _row_matches_mode(
                    row_reasons,
                    normalize_only=bool(args.normalize_only),
                    mirror_only=bool(args.mirror_only),
                ):
                    counters[platform].skipped += 1
                    continue
                if repair_reasons and not repair_reasons.intersection(row_reasons):
                    counters[platform].skipped += 1
                    continue
                if not social_repo._platform_post_needs_media_mirror(platform, row):  # noqa: SLF001
                    counters[platform].skipped += 1
                    continue
                counters[platform].eligible += 1
                counters[platform].reason_counts.update(row_reasons)
                if args.dry_run:
                    continue

                week_index: int | None = None
                posted_at = social_repo._coerce_dt(row.get("posted_at"))  # noqa: SLF001
                season_id = str(row.get("season_id") or "").strip()
                if season_id:
                    context = context_cache.get(season_id)
                    if context is None:
                        context = social_repo.get_season_context(season_id)
                        context_cache[season_id] = context
                        try:
                            season_windows, _ = social_repo._resolve_week_windows(  # noqa: SLF001
                            context,
                            timezone="America/New_York",
                            source_scope=source_scope,
                            now_utc=now_utc,
                        )
                        except Exception:
                            season_windows = []
                        windows_cache[season_id] = season_windows

                    season_windows = windows_cache.get(season_id) or []
                    if posted_at and season_windows:
                        week_window = social_repo._week_for_timestamp(  # noqa: SLF001
                            posted_at,
                            windows=season_windows,
                            timezone="America/New_York",
                        )
                        week_index = week_window.week_index if week_window else None
                else:
                    context = None

                try:
                    job_id = social_repo._enqueue_platform_media_mirror_job(  # noqa: SLF001
                        context,
                        platform=platform,
                        run_id=None,
                        source_scope=source_scope,
                        account=str(row.get("account") or ""),
                        post_row=row,
                        week_index=week_index,
                        parent_job_id="backfill-social-media-mirror",
                        conn=conn,
                    )
                    if job_id:
                        counters[platform].queued += 1
                        pending_commits += 1
                        if pending_commits >= commit_batch_size and hasattr(conn, "commit"):
                            conn.commit()
                            pending_commits = 0
                    else:
                        counters[platform].skipped += 1
                except Exception:
                    counters[platform].failed += 1
            if pending_commits > 0 and hasattr(conn, "commit"):
                conn.commit()
                pending_commits = 0

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
                "effective_source_scope": source_scope,
                "weeks": max(1, int(args.weeks)),
                "all_history": bool(args.all_history),
                "cutoff": social_repo._iso(cutoff) if cutoff else None,  # noqa: SLF001
                "platforms": platforms,
                "season_ids": season_ids,
                "show_ids": show_ids,
                "season_numbers": season_numbers,
                "post_ids": post_ids,
                "source_ids": source_ids,
                "failed_only": bool(args.failed_only),
                "hosted_html_only": bool(args.hosted_html_only),
                "normalize_only": bool(args.normalize_only),
                "mirror_only": bool(args.mirror_only),
                "repair_all": bool(args.repair_all or (not args.normalize_only and not args.mirror_only)),
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
