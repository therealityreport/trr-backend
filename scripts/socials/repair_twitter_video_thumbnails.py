#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit("Missing psycopg2; install deps (e.g., `pip install -r requirements.txt`).") from exc

try:
    from scripts._db_url import resolve_db_url
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts._db_url import resolve_db_url
    from trr_backend.utils.env import load_env

VIDEO_EXTENSION_RE = re.compile(r"\.(mp4|mov|m4v|webm)(\?|$)", re.IGNORECASE)


@dataclass(slots=True)
class RepairStats:
    scanned: int = 0
    eligible: int = 0
    updated: int = 0
    skipped: int = 0
    unresolved: int = 0


def _resolve_db_url() -> str:
    return resolve_db_url(allow_database_url=True).value


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_twitter_video_thumbnails",
        description=(
            "Repair social.twitter_tweets hosted_thumbnail_url values that point to video assets by "
            "selecting the first non-video URL from hosted_media_urls or media_urls."
        ),
    )
    parser.add_argument(
        "--season-id",
        default="",
        help="Optional season UUID filter.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum rows scanned (default: 5000).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview updates without writing (default).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates to the database.",
    )
    parser.set_defaults(dry_run=True)
    return parser.parse_args(argv)


def _is_video_like_url(url: str) -> bool:
    value = str(url or "").strip().lower()
    if not value:
        return False
    parsed = urlparse(value)
    if "video.twimg.com" in (parsed.netloc or "").lower():
        return True
    return bool(VIDEO_EXTENSION_RE.search(value))


def _as_text_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _first_non_video_url(urls: list[str]) -> str:
    for url in urls:
        normalized = str(url or "").strip()
        if normalized and not _is_video_like_url(normalized):
            return normalized
    return ""


def _fetch_rows(
    cur: RealDictCursor,
    *,
    season_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    if season_id:
        cur.execute(
            """
            select
              id::text as id,
              tweet_id,
              coalesce(hosted_thumbnail_url, '') as hosted_thumbnail_url,
              coalesce(hosted_media_urls, '[]'::jsonb) as hosted_media_urls,
              coalesce(media_urls, '[]'::jsonb) as media_urls
            from social.twitter_tweets
            where is_reply = false
              and season_id = %s::uuid
              and coalesce(hosted_thumbnail_url, '') <> ''
              and (
                lower(coalesce(hosted_thumbnail_url, '')) like '%%video.twimg.com%%'
                or coalesce(hosted_thumbnail_url, '') ~* '\\.(mp4|mov|m4v|webm)(\\?|$)'
              )
            order by created_at desc
            limit %s
            """,
            (season_id, max(1, int(limit))),
        )
    else:
        cur.execute(
            """
            select
              id::text as id,
              tweet_id,
              coalesce(hosted_thumbnail_url, '') as hosted_thumbnail_url,
              coalesce(hosted_media_urls, '[]'::jsonb) as hosted_media_urls,
              coalesce(media_urls, '[]'::jsonb) as media_urls
            from social.twitter_tweets
            where is_reply = false
              and coalesce(hosted_thumbnail_url, '') <> ''
              and (
                lower(coalesce(hosted_thumbnail_url, '')) like '%%video.twimg.com%%'
                or coalesce(hosted_thumbnail_url, '') ~* '\\.(mp4|mov|m4v|webm)(\\?|$)'
              )
            order by created_at desc
            limit %s
            """,
            (max(1, int(limit)),),
        )
    rows = cur.fetchall()
    return rows if isinstance(rows, list) else []


def _repair_rows(
    cur: RealDictCursor,
    *,
    season_id: str,
    limit: int,
    dry_run: bool,
) -> RepairStats:
    stats = RepairStats()
    rows = _fetch_rows(cur, season_id=season_id, limit=limit)
    for row in rows:
        stats.scanned += 1
        row_id = str(row.get("id") or "").strip()
        current_thumbnail = str(row.get("hosted_thumbnail_url") or "").strip()
        if not row_id or not current_thumbnail:
            stats.skipped += 1
            continue

        hosted_media_urls = _as_text_list(row.get("hosted_media_urls"))
        source_media_urls = _as_text_list(row.get("media_urls"))
        replacement = _first_non_video_url(hosted_media_urls) or _first_non_video_url(source_media_urls)
        if not replacement:
            stats.unresolved += 1
            continue

        stats.eligible += 1
        if replacement == current_thumbnail:
            stats.skipped += 1
            continue
        if dry_run:
            stats.skipped += 1
            continue

        cur.execute(
            """
            update social.twitter_tweets
            set hosted_thumbnail_url = %s
            where id = %s::uuid
            """,
            (replacement, row_id),
        )
        stats.updated += 1

    return stats


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    dry_run = bool(args.dry_run and not args.apply)
    season_id = str(args.season_id or "").strip()
    limit = max(1, int(args.limit))

    conn = psycopg2.connect(_resolve_db_url(), cursor_factory=RealDictCursor)
    try:
        cur = conn.cursor()
        stats = _repair_rows(
            cur,
            season_id=season_id,
            limit=limit,
            dry_run=dry_run,
        )
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "season_id": season_id or None,
                "limit": limit,
                "stats": {
                    "scanned": stats.scanned,
                    "eligible": stats.eligible,
                    "updated": stats.updated,
                    "skipped": stats.skipped,
                    "unresolved": stats.unresolved,
                },
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
