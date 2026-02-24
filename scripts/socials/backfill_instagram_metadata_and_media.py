#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.socials.instagram import InstagramScraper


@dataclass(slots=True)
class BackfillCounters:
    scanned: int = 0
    enriched: int = 0
    mirrored: int = 0
    partial: int = 0
    failed: int = 0
    mirror_jobs_enqueued: int = 0
    mirror_job_enqueue_failed: int = 0


class _BackfillPost:
    def __init__(self, row: dict[str, Any]) -> None:
        self.shortcode = str(row.get("shortcode") or "")
        self.pk = row.get("media_id")
        self.username = row.get("username") or row.get("source_account") or ""
        self.caption = row.get("caption") or ""
        self.post_type = row.get("media_type") or "post"
        self.media_urls = social_repo._as_text_list(row.get("media_urls"))  # noqa: SLF001
        self.thumbnail_url = row.get("thumbnail_url")
        self.likes = int(row.get("likes") or 0)
        self.comments = int(row.get("comments_count") or 0)
        self.video_views = int(row.get("views") or 0)
        self.taken_at = row.get("posted_at")
        self.profile_tags = social_repo._as_text_list(row.get("profile_tags"))  # noqa: SLF001
        self.collaborators = social_repo._as_text_list(row.get("collaborators"))  # noqa: SLF001
        self.hashtags = social_repo._as_text_list(row.get("hashtags"), strip_prefix="#")  # noqa: SLF001
        self.mentions = social_repo._as_text_list(  # noqa: SLF001
            row.get("mentions"),
            prefix="@",
            strip_prefix="@",
        )
        self.duration_seconds = row.get("duration_seconds")
        self.post_format = row.get("post_format")
        self.metadata_source = row.get("metadata_source")
        self.metadata_scraped_at = row.get("metadata_scraped_at")
        self.metadata_error = row.get("metadata_error")
        self.hosted_thumbnail_url = row.get("hosted_thumbnail_url")
        self.hosted_media_urls = social_repo._as_text_list(row.get("hosted_media_urls"))  # noqa: SLF001
        self.media_mirror_status = row.get("media_mirror_status")
        self.media_mirror_error = row.get("media_mirror_error")
        self._raw_data = row.get("raw_data") if isinstance(row.get("raw_data"), dict) else {}

    def to_dict(self) -> dict[str, Any]:
        return dict(self._raw_data)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Instagram permalink metadata and S3 mirrors.")
    parser.add_argument("--weeks", type=int, default=8, help="Lookback window in weeks (default: 8).")
    parser.add_argument("--limit", type=int, default=None, help="Optional max post count.")
    parser.add_argument(
        "--source-scope",
        default="bravo",
        choices=["bravo", "creator", "community"],
        help="Week window scope for deterministic mirror key routing.",
    )
    parser.add_argument(
        "--metadata-stale-hours",
        type=int,
        default=24 * 7,
        help="Treat metadata older than this as stale (default: 168 hours).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not persist updates.")
    return parser.parse_args()


def _load_candidate_rows(*, weeks: int, limit: int | None) -> list[dict[str, Any]]:
    params: list[Any] = [max(1, int(weeks))]
    limit_clause = ""
    if limit is not None and limit > 0:
        limit_clause = "limit %s"
        params.append(int(limit))
    return pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.shortcode,
          p.media_id,
          p.username,
          p.caption,
          p.media_type,
          p.media_urls,
          p.thumbnail_url,
          p.likes,
          p.comments_count,
          p.views,
          p.posted_at,
          p.raw_data,
          p.source_account,
          p.show_id::text as show_id,
          p.season_id::text as season_id,
          p.post_format,
          p.profile_tags,
          p.collaborators,
          p.hashtags,
          p.mentions,
          p.duration_seconds,
          p.metadata_source,
          p.metadata_scraped_at,
          p.metadata_error,
          p.hosted_thumbnail_url,
          p.hosted_media_urls,
          p.media_mirror_status,
          p.media_mirror_error
        from social.instagram_posts p
        where coalesce(p.posted_at, p.scraped_at) >= now() - (%s * interval '1 week')
        order by coalesce(p.posted_at, p.scraped_at) desc
        {limit_clause}
        """,
        params,
    )


def _metadata_is_missing_or_stale(row: dict[str, Any], *, stale_before: datetime) -> bool:
    metadata_scraped_at = social_repo._coerce_dt(row.get("metadata_scraped_at"))  # noqa: SLF001
    metadata_source = str(row.get("metadata_source") or "").strip()
    post_format = str(row.get("post_format") or "").strip()
    if not metadata_scraped_at or not metadata_source or not post_format:
        return True
    return metadata_scraped_at < stale_before


def _mirror_is_missing(row: dict[str, Any]) -> bool:
    hosted_thumbnail = str(row.get("hosted_thumbnail_url") or "").strip()
    hosted_media = social_repo._as_text_list(row.get("hosted_media_urls"))  # noqa: SLF001
    source_thumbnail = str(row.get("thumbnail_url") or "").strip()
    source_media = social_repo._as_text_list(row.get("media_urls"))  # noqa: SLF001
    if not source_thumbnail and not source_media:
        return False
    if source_thumbnail and not hosted_thumbnail:
        return True
    if source_media and len(hosted_media) < len(source_media):
        return True
    mirror_status = str(row.get("media_mirror_status") or "").strip().lower()
    if mirror_status in {"pending", "partial", "failed"}:
        return True
    source_count = len(source_media) + (1 if source_thumbnail else 0)
    hosted_count = len(hosted_media) + (1 if hosted_thumbnail else 0)
    if source_count > 0 and hosted_count >= source_count:
        return False
    return mirror_status != "mirrored"


def main() -> int:
    args = _parse_args()
    now_utc = datetime.now(tz=UTC)
    stale_before = now_utc - timedelta(hours=max(1, int(args.metadata_stale_hours)))
    rows = _load_candidate_rows(weeks=args.weeks, limit=args.limit)

    counters = BackfillCounters()
    context_cache: dict[str, social_repo.SeasonContext] = {}
    windows_cache: dict[str, list[social_repo.WeekWindow]] = {}

    try:
        cookies = social_repo._load_instagram_cookies()  # noqa: SLF001
    except Exception:
        cookies = {}
    scraper = InstagramScraper(cookies=cookies)

    for row in rows:
        counters.scanned += 1
        season_id = str(row.get("season_id") or "").strip()
        if not season_id:
            continue
        context = context_cache.get(season_id)
        if context is None:
            try:
                context = social_repo.get_season_context(season_id)
            except Exception:
                continue
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

        needs_metadata = _metadata_is_missing_or_stale(row, stale_before=stale_before)
        needs_mirror = _mirror_is_missing(row)
        if not needs_metadata and not needs_mirror:
            continue

        post = _BackfillPost(row)

        if needs_metadata:
            social_repo._enrich_instagram_post_from_permalink(post=post, scraper=scraper, now_utc=now_utc)  # noqa: SLF001
            if not post.metadata_error and post.metadata_source:
                counters.enriched += 1

        if needs_mirror:
            week_index: int | None = None
            post_ts = social_repo._coerce_dt(post.taken_at)  # noqa: SLF001
            season_windows = windows_cache.get(season_id) or []
            if post_ts and season_windows:
                week_window = social_repo._week_for_timestamp(  # noqa: SLF001
                    post_ts,
                    windows=season_windows,
                    timezone="America/New_York",
                )
                week_index = week_window.week_index if week_window else None
            post.media_mirror_status = "pending"
            post.media_mirror_error = None
            if args.dry_run:
                counters.mirror_jobs_enqueued += 1
                counters.mirrored += 1
            else:
                try:
                    mirror_job_id = social_repo._enqueue_platform_media_mirror_job(  # noqa: SLF001
                        context,
                        platform="instagram",
                        run_id=None,
                        source_scope=args.source_scope,
                        account=str(row.get("source_account") or row.get("username") or ""),
                        post_row=row,
                        week_index=week_index,
                        parent_job_id="backfill-instagram-metadata-media",
                        conn=None,
                    )
                    if mirror_job_id:
                        counters.mirror_jobs_enqueued += 1
                        counters.mirrored += 1
                    else:
                        counters.partial += 1
                except Exception:
                    counters.failed += 1
                    counters.mirror_job_enqueue_failed += 1

        if args.dry_run:
            continue

        social_repo._upsert_instagram_post(  # noqa: SLF001
            context,
            job_id="backfill-instagram-metadata-media",
            account=str(row.get("source_account") or row.get("username") or ""),
            post=post,
        )

    print(
        json.dumps(
            {
                "scanned": counters.scanned,
                "enriched": counters.enriched,
                "mirrored": counters.mirrored,
                "partial": counters.partial,
                "failed": counters.failed,
                "mirror_jobs_enqueued": counters.mirror_jobs_enqueued,
                "mirror_job_enqueue_failed": counters.mirror_job_enqueue_failed,
                "dry_run": bool(args.dry_run),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
