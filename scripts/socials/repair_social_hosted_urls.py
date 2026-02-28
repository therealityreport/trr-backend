#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse, urlunparse

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit("Missing psycopg2; install deps (e.g., `pip install -r requirements.txt`).") from exc

try:
    from trr_backend.media.s3_mirror import get_cdn_base_url
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.media.s3_mirror import get_cdn_base_url
    from trr_backend.utils.env import load_env

PLATFORM_TABLES = {
    "facebook": "facebook_posts",
    "youtube": "youtube_videos",
}


@dataclass(slots=True)
class RepairStats:
    scanned_rows: int = 0
    rows_needing_repair: int = 0
    rows_updated: int = 0
    thumbnail_urls_rewritten: int = 0
    media_urls_rewritten: int = 0


def _resolve_db_url() -> str:
    url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if not url:
        raise RuntimeError("SUPABASE_DB_URL is required for repair_social_hosted_urls.")
    return url


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_social_hosted_urls",
        description="Rewrite legacy social hosted URLs from S3 hosts to AWS_CDN_BASE_URL.",
    )
    parser.add_argument(
        "--platforms",
        default="facebook,youtube",
        help="Comma-separated platforms to repair (default: facebook,youtube).",
    )
    parser.add_argument(
        "--limit-per-platform",
        type=int,
        default=5000,
        help="Maximum rows scanned per platform (default: 5000).",
    )
    parser.add_argument(
        "--season-id",
        default="",
        help="Optional season UUID filter.",
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


def _parse_platforms(value: str) -> list[str]:
    platforms = [item.strip().lower() for item in str(value or "").split(",") if item.strip()]
    if not platforms:
        raise RuntimeError("At least one platform is required.")
    invalid = [platform for platform in platforms if platform not in PLATFORM_TABLES]
    if invalid:
        valid = ", ".join(sorted(PLATFORM_TABLES))
        raise RuntimeError(f"Unsupported platforms: {', '.join(sorted(invalid))}. Valid values: {valid}")
    return platforms


def _is_s3_style_host(host: str) -> bool:
    lowered = str(host or "").strip().lower()
    if not lowered:
        return False
    if lowered == "s3.amazonaws.com" or lowered.endswith(".s3.amazonaws.com"):
        return True
    if re.match(r"^s3[.-][a-z0-9-]+\.amazonaws\.com$", lowered):
        return True
    if re.match(r"^[a-z0-9.-]+\.s3[.-][a-z0-9-]+\.amazonaws\.com$", lowered):
        return True
    return False


def _rewrite_to_cdn(url: str, *, cdn_base_url: str) -> tuple[str, bool]:
    raw = str(url or "").strip()
    if not raw:
        return raw, False
    parsed = urlparse(raw)
    if not parsed.netloc or not _is_s3_style_host(parsed.netloc):
        return raw, False

    cdn = urlparse(cdn_base_url)
    rewritten = parsed._replace(scheme=cdn.scheme, netloc=cdn.netloc)
    return urlunparse(rewritten), True


def _as_text_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _fetch_rows(
    cur: RealDictCursor,
    *,
    table: str,
    season_id: str,
    limit: int,
) -> list[dict[str, object]]:
    if season_id:
        cur.execute(
            f"""
            select
              id::text as id,
              coalesce(hosted_thumbnail_url, '') as hosted_thumbnail_url,
              coalesce(hosted_media_urls, '[]'::jsonb) as hosted_media_urls
            from social.{table}
            where season_id = %s::uuid
              and (
                coalesce(hosted_thumbnail_url, '') <> ''
                or jsonb_array_length(coalesce(hosted_media_urls, '[]'::jsonb)) > 0
              )
            order by id
            limit %s
            """,
            (season_id, int(limit)),
        )
    else:
        cur.execute(
            f"""
            select
              id::text as id,
              coalesce(hosted_thumbnail_url, '') as hosted_thumbnail_url,
              coalesce(hosted_media_urls, '[]'::jsonb) as hosted_media_urls
            from social.{table}
            where
              coalesce(hosted_thumbnail_url, '') <> ''
              or jsonb_array_length(coalesce(hosted_media_urls, '[]'::jsonb)) > 0
            order by id
            limit %s
            """,
            (int(limit),),
        )
    rows = cur.fetchall()
    return rows if isinstance(rows, list) else []


def _repair_platform(
    cur: RealDictCursor,
    *,
    table: str,
    cdn_base_url: str,
    season_id: str,
    limit_per_platform: int,
    dry_run: bool,
) -> RepairStats:
    stats = RepairStats()
    rows = _fetch_rows(
        cur,
        table=table,
        season_id=season_id,
        limit=limit_per_platform,
    )
    for row in rows:
        stats.scanned_rows += 1
        row_id = str(row.get("id") or "").strip()
        if not row_id:
            continue

        old_thumbnail_url = str(row.get("hosted_thumbnail_url") or "").strip()
        old_media_urls = _as_text_list(row.get("hosted_media_urls"))

        new_thumbnail_url, thumb_changed = _rewrite_to_cdn(old_thumbnail_url, cdn_base_url=cdn_base_url)
        new_media_urls: list[str] = []
        media_changed = False
        media_rewrites = 0
        for url in old_media_urls:
            rewritten, changed = _rewrite_to_cdn(url, cdn_base_url=cdn_base_url)
            if changed:
                media_changed = True
                media_rewrites += 1
            new_media_urls.append(rewritten)

        if not thumb_changed and not media_changed:
            continue

        stats.rows_needing_repair += 1
        if thumb_changed:
            stats.thumbnail_urls_rewritten += 1
        stats.media_urls_rewritten += media_rewrites

        if dry_run:
            continue

        cur.execute(
            f"""
            update social.{table}
            set
              hosted_thumbnail_url = %s,
              hosted_media_urls = %s::jsonb
            where id = %s::uuid
            """,
            (
                new_thumbnail_url,
                json.dumps(new_media_urls),
                row_id,
            ),
        )
        stats.rows_updated += 1
    return stats


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    dry_run = bool(args.dry_run and not args.apply)

    platforms = _parse_platforms(args.platforms)
    cdn_base_url = get_cdn_base_url()
    season_id = str(args.season_id or "").strip()
    limit_per_platform = max(1, int(args.limit_per_platform))

    conn = psycopg2.connect(_resolve_db_url(), cursor_factory=RealDictCursor)
    by_platform: dict[str, dict[str, int]] = {}
    totals = RepairStats()
    try:
        cur = conn.cursor()
        for platform in platforms:
            table = PLATFORM_TABLES[platform]
            stats = _repair_platform(
                cur,
                table=table,
                cdn_base_url=cdn_base_url,
                season_id=season_id,
                limit_per_platform=limit_per_platform,
                dry_run=dry_run,
            )
            by_platform[platform] = {
                "scanned_rows": stats.scanned_rows,
                "rows_needing_repair": stats.rows_needing_repair,
                "rows_updated": stats.rows_updated,
                "thumbnail_urls_rewritten": stats.thumbnail_urls_rewritten,
                "media_urls_rewritten": stats.media_urls_rewritten,
            }
            totals.scanned_rows += stats.scanned_rows
            totals.rows_needing_repair += stats.rows_needing_repair
            totals.rows_updated += stats.rows_updated
            totals.thumbnail_urls_rewritten += stats.thumbnail_urls_rewritten
            totals.media_urls_rewritten += stats.media_urls_rewritten

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
                "cdn_base_url": cdn_base_url,
                "season_id": season_id or None,
                "platforms": platforms,
                "limit_per_platform": limit_per_platform,
                "totals": {
                    "scanned_rows": totals.scanned_rows,
                    "rows_needing_repair": totals.rows_needing_repair,
                    "rows_updated": totals.rows_updated,
                    "thumbnail_urls_rewritten": totals.thumbnail_urls_rewritten,
                    "media_urls_rewritten": totals.media_urls_rewritten,
                },
                "by_platform": by_platform,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
