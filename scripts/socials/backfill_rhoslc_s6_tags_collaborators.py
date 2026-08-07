#!/usr/bin/env python3
"""One-off backfill: re-enrich RHOSLC S6 Instagram posts to populate
profile_tags, collaborators, tagged_users_detail, and collaborators_detail
using the fixed permalink metadata extraction pipeline.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from typing import Any

from trr_backend.db import pg
from trr_backend.socials import social_season_analytics_impl as social_repo
from trr_backend.socials.instagram import InstagramScraper
from trr_backend.utils.env import load_env


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill RHOSLC S6 Instagram post tags & collaborators.")
    parser.add_argument("--limit", type=int, default=None, help="Max posts to process.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without persisting.")
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds between enrichment calls (rate limiting). Default: 1.0",
    )
    return parser.parse_args()


def _find_rhoslc_s6_season_id() -> str:
    rows = pg.fetch_all(
        """
        select s.id::text as season_id, s.season_number, sh.name as show_name
        from core.seasons s
        join core.shows sh on sh.id = s.show_id
        where lower(sh.name) like '%%salt lake%%'
          and s.season_number = 6
        limit 1
        """,
        [],
    )
    if not rows:
        raise SystemExit("Could not find RHOSLC Season 6 in database.")
    row = rows[0]
    print(f"Found: {row['show_name']} S{row['season_number']} → {row['season_id']}")
    return str(row["season_id"])


def _load_posts(season_id: str, limit: int | None) -> list[dict[str, Any]]:
    params: list[Any] = [season_id]
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
          p.media_mirror_error,
          p.tagged_users_detail,
          p.collaborators_detail,
          p.job_id::text as job_id
        from social.instagram_posts p
        where p.season_id = %s::uuid
        order by coalesce(p.posted_at, p.scraped_at) desc
        {limit_clause}
        """,
        params,
    )


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
        self.tagged_users_detail = row.get("tagged_users_detail") or []
        self.collaborators_detail = row.get("collaborators_detail") or []
        raw_data = row.get("raw_data")
        self._raw_data: dict[str, Any] = raw_data if isinstance(raw_data, dict) else {}

    def to_dict(self) -> dict[str, Any]:
        return dict(self._raw_data)


def main() -> int:
    load_env()
    args = _parse_args()

    season_id = _find_rhoslc_s6_season_id()
    context = social_repo.get_season_context(season_id)

    rows = _load_posts(season_id, args.limit)
    total = len(rows)
    print(f"Loaded {total} Instagram posts for RHOSLC S6.")

    try:
        cookies = social_repo._load_instagram_cookies()  # noqa: SLF001
    except Exception:
        cookies = {}
    scraper = InstagramScraper(cookies=cookies)

    enriched = 0
    failed = 0
    skipped = 0
    now_utc = datetime.now(tz=UTC)

    for i, row in enumerate(rows, 1):
        shortcode = row.get("shortcode") or "?"
        post = _BackfillPost(row)

        # Re-enrich every post to pick up the new tag/collaborator extraction
        social_repo._enrich_instagram_post_from_permalink(  # noqa: SLF001
            post=post, scraper=scraper, now_utc=now_utc
        )

        if post.metadata_error:
            failed += 1
            status = f"FAIL ({post.metadata_error})"
        elif post.metadata_source:
            enriched += 1
            tags = len(post.profile_tags or [])
            collabs = len(post.collaborators or [])
            tag_detail = len(getattr(post, "tagged_users_detail", []) or [])
            collab_detail = len(getattr(post, "collaborators_detail", []) or [])
            status = (
                f"OK src={post.metadata_source} "
                f"tags={tags} collabs={collabs} "
                f"tag_detail={tag_detail} collab_detail={collab_detail}"
            )
        else:
            skipped += 1
            status = "SKIP (no source)"

        print(f"[{i}/{total}] {shortcode}: {status}")

        if not args.dry_run and not post.metadata_error:
            original_job_id = str(row.get("job_id") or "")
            social_repo._upsert_instagram_post(  # noqa: SLF001
                context,
                job_id=original_job_id,
                account=str(row.get("source_account") or row.get("username") or ""),
                post=post,
            )

        if i < total and args.delay > 0:
            time.sleep(args.delay)

    print(
        json.dumps(
            {
                "total": total,
                "enriched": enriched,
                "failed": failed,
                "skipped": skipped,
                "dry_run": bool(args.dry_run),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
