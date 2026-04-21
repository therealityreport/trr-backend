#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit("Missing psycopg2; install deps (e.g., `pip install -r requirements.txt`).") from exc

try:
    from scripts._db_url import resolve_db_url
    from trr_backend.media.s3_mirror import get_cdn_base_url
    from trr_backend.repositories import social_season_analytics as social_repo
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts._db_url import resolve_db_url
    from trr_backend.media.s3_mirror import get_cdn_base_url
    from trr_backend.repositories import social_season_analytics as social_repo
    from trr_backend.utils.env import load_env


@dataclass(slots=True)
class RepairStats:
    scanned_rows: int = 0
    rows_needing_repair: int = 0
    rows_updated: int = 0
    thumbnail_urls_rewritten: int = 0
    media_urls_rewritten: int = 0
    avatar_urls_rewritten: int = 0
    media_asset_meta_urls_rewritten: int = 0


def _resolve_db_url() -> str:
    return resolve_db_url(allow_database_url=True).value


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_social_hosted_urls",
        description="Normalize hosted social URLs to OBJECT_STORAGE_PUBLIC_BASE_URL.",
    )
    parser.add_argument(
        "--platforms",
        default="instagram,tiktok,youtube,twitter,facebook,threads",
        help="Comma-separated platforms to repair.",
    )
    parser.add_argument(
        "--limit-per-platform",
        type=int,
        default=5000,
        help="Maximum rows scanned per platform (default: 5000).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Compatibility alias for chunk sizing in operator docs; currently informational only.",
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
        "--season-number",
        action="append",
        default=[],
        help="Optional season number filter. Repeat to target multiple season numbers.",
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
    invalid = [platform for platform in platforms if platform not in social_repo.PLATFORM_POST_TABLES]
    if invalid:
        valid = ", ".join(sorted(social_repo.PLATFORM_POST_TABLES))
        raise RuntimeError(f"Unsupported platforms: {', '.join(sorted(invalid))}. Valid values: {valid}")
    return platforms


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


def _rewrite_to_cdn(url: str, *, cdn_base_url: str) -> tuple[str, bool]:
    raw = str(url or "").strip()
    if not raw:
        return raw, False
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw, False
    cdn = urlparse(cdn_base_url)
    if parsed.netloc == cdn.netloc and parsed.scheme == cdn.scheme:
        return raw, False
    rewritten = parsed._replace(scheme=cdn.scheme, netloc=cdn.netloc)
    return urlunparse(rewritten), True


def _as_text_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _as_json_object(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _rewrite_media_asset_meta(raw_data: object, *, cdn_base_url: str) -> tuple[dict[str, Any], int]:
    payload = _as_json_object(raw_data)
    if not payload:
        return payload, 0
    updated = dict(payload)
    media_asset_meta = social_repo._extract_media_asset_meta_from_raw_data(updated)  # noqa: SLF001
    if not media_asset_meta:
        return updated, 0

    rewrite_count = 0
    new_meta = dict(media_asset_meta)
    hosted_assets = []
    for item in media_asset_meta.get("hosted_assets") or []:
        if not isinstance(item, dict):
            continue
        entry = dict(item)
        rewritten_url, changed = _rewrite_to_cdn(str(entry.get("url") or "").strip(), cdn_base_url=cdn_base_url)
        if changed:
            rewrite_count += 1
            entry["url"] = rewritten_url
        hosted_assets.append(entry)
    if hosted_assets:
        new_meta["hosted_assets"] = hosted_assets

    thumbnail_hosted = media_asset_meta.get("thumbnail_hosted")
    if isinstance(thumbnail_hosted, dict):
        new_thumbnail_hosted = dict(thumbnail_hosted)
        rewritten_url, changed = _rewrite_to_cdn(
            str(new_thumbnail_hosted.get("url") or "").strip(),
            cdn_base_url=cdn_base_url,
        )
        if changed:
            rewrite_count += 1
            new_thumbnail_hosted["url"] = rewritten_url
        new_meta["thumbnail_hosted"] = new_thumbnail_hosted

    if rewrite_count == 0:
        return updated, 0

    updated["media_asset_meta"] = new_meta
    return updated, rewrite_count


def _fetch_rows(
    cur: RealDictCursor,
    *,
    platform: str,
    table: str,
    season_ids: list[str],
    show_ids: list[str],
    season_numbers: list[int],
    limit: int,
) -> list[dict[str, object]]:
    filters: list[str] = [
        "("
        "coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') <> ''"
        " or jsonb_array_length(coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb)) > 0"
        " or coalesce(to_jsonb(p) ->> 'hosted_user_avatar_url', '') <> ''"
        " or coalesce(to_jsonb(p) ->> 'hosted_owner_profile_pic_url', '') <> ''"
        " or coalesce(to_jsonb(p) -> 'hosted_tagged_profile_pics', '{}'::jsonb) <> '{}'::jsonb"
        ")"
    ]
    params: list[object] = []
    if season_ids:
        filters.append("p.season_id::text = any(%s)")
        params.append(season_ids)
    if show_ids:
        filters.append("p.show_id::text = any(%s)")
        params.append(show_ids)
    if season_numbers:
        filters.append("s.season_number = any(%s)")
        params.append(season_numbers)
    params.append(max(1, int(limit)))

    cur.execute(
        f"""
        select
          p.id::text as id,
          p.season_id::text as season_id,
          p.show_id::text as show_id,
          s.season_number as season_number,
          coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
          coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(p) ->> 'hosted_user_avatar_url', '') as hosted_user_avatar_url,
          coalesce(to_jsonb(p) ->> 'hosted_owner_profile_pic_url', '') as hosted_owner_profile_pic_url,
          coalesce(to_jsonb(p) -> 'hosted_tagged_profile_pics', '{{}}'::jsonb) as hosted_tagged_profile_pics,
          coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data
        from social.{table} p
        left join core.seasons s on s.id = p.season_id
        where {" and ".join(filters)}
        order by p.id
        limit %s
        """,
        tuple(params),
    )
    rows = cur.fetchall()
    return rows if isinstance(rows, list) else []


def _repair_platform(
    cur: RealDictCursor,
    *,
    platform: str,
    table: str,
    cdn_base_url: str,
    season_ids: list[str],
    show_ids: list[str],
    season_numbers: list[int],
    limit_per_platform: int,
    dry_run: bool,
) -> RepairStats:
    stats = RepairStats()
    rows = _fetch_rows(
        cur,
        platform=platform,
        table=table,
        season_ids=season_ids,
        show_ids=show_ids,
        season_numbers=season_numbers,
        limit=limit_per_platform,
    )
    for row in rows:
        stats.scanned_rows += 1
        row_id = str(row.get("id") or "").strip()
        if not row_id:
            continue

        old_thumbnail_url = str(row.get("hosted_thumbnail_url") or "").strip()
        old_media_urls = _as_text_list(row.get("hosted_media_urls"))
        old_avatar_url = str(row.get("hosted_user_avatar_url") or "").strip()
        old_owner_avatar_url = str(row.get("hosted_owner_profile_pic_url") or "").strip()
        old_tagged_profile_pics = social_repo._normalize_hosted_tagged_profile_pics(  # noqa: SLF001
            row.get("hosted_tagged_profile_pics")
        )
        old_raw_data = _as_json_object(row.get("raw_data"))

        new_thumbnail_url, thumb_changed = _rewrite_to_cdn(old_thumbnail_url, cdn_base_url=cdn_base_url)

        new_media_urls: list[str] = []
        media_rewrites = 0
        for url in old_media_urls:
            rewritten, changed = _rewrite_to_cdn(url, cdn_base_url=cdn_base_url)
            if changed:
                media_rewrites += 1
            new_media_urls.append(rewritten)

        new_avatar_url, avatar_changed = _rewrite_to_cdn(old_avatar_url, cdn_base_url=cdn_base_url)
        new_owner_avatar_url, owner_avatar_changed = _rewrite_to_cdn(
            old_owner_avatar_url,
            cdn_base_url=cdn_base_url,
        )

        tagged_profile_rewrites = 0
        new_tagged_profile_pics: dict[str, dict[str, object]] = {}
        for key, value in old_tagged_profile_pics.items():
            rewritten, changed = _rewrite_to_cdn(str(value.get("hosted_url") or "").strip(), cdn_base_url=cdn_base_url)
            if changed:
                tagged_profile_rewrites += 1
            new_tagged_profile_pics[str(key)] = {
                "hosted_url": rewritten,
                "sha256": value.get("sha256"),
                "mirrored_at": value.get("mirrored_at"),
            }

        new_raw_data, media_asset_meta_rewrites = _rewrite_media_asset_meta(
            old_raw_data,
            cdn_base_url=cdn_base_url,
        )

        if not any(
            (
                thumb_changed,
                media_rewrites > 0,
                avatar_changed,
                owner_avatar_changed,
                tagged_profile_rewrites > 0,
                media_asset_meta_rewrites > 0,
            )
        ):
            continue

        stats.rows_needing_repair += 1
        if thumb_changed:
            stats.thumbnail_urls_rewritten += 1
        stats.media_urls_rewritten += media_rewrites
        stats.avatar_urls_rewritten += int(avatar_changed) + int(owner_avatar_changed) + tagged_profile_rewrites
        stats.media_asset_meta_urls_rewritten += media_asset_meta_rewrites

        if dry_run:
            continue

        assignments = [
            "hosted_thumbnail_url = %s",
            "hosted_media_urls = %s::jsonb",
            "raw_data = %s::jsonb",
        ]
        params: list[object] = [
            new_thumbnail_url,
            json.dumps(new_media_urls),
            json.dumps(new_raw_data),
        ]
        if social_repo._platform_posts_has_column(platform, "hosted_user_avatar_url"):  # noqa: SLF001
            assignments.append("hosted_user_avatar_url = %s")
            params.append(new_avatar_url)
        if social_repo._platform_posts_has_column(platform, "hosted_owner_profile_pic_url"):  # noqa: SLF001
            assignments.append("hosted_owner_profile_pic_url = %s")
            params.append(new_owner_avatar_url)
        if social_repo._platform_posts_has_column(platform, "hosted_tagged_profile_pics"):  # noqa: SLF001
            assignments.append("hosted_tagged_profile_pics = %s::jsonb")
            params.append(json.dumps(new_tagged_profile_pics))
        params.append(row_id)

        cur.execute(
            f"""
            update social.{table}
            set {", ".join(assignments)}
            where id = %s::uuid
            """,
            tuple(params),
        )
        stats.rows_updated += 1
    return stats


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    dry_run = bool(args.dry_run and not args.apply)

    platforms = _parse_platforms(args.platforms)
    cdn_base_url = get_cdn_base_url()
    season_ids = _normalize_text_filters(args.season_id)
    show_ids = _normalize_text_filters(args.show_id)
    season_numbers = _normalize_int_filters(args.season_number)
    limit_per_platform = max(1, int(args.limit_per_platform))
    batch_size = max(1, int(args.batch_size))

    conn = psycopg2.connect(_resolve_db_url(), cursor_factory=RealDictCursor)
    by_platform: dict[str, dict[str, int]] = {}
    totals = RepairStats()
    try:
        cur = conn.cursor()
        for platform in platforms:
            table = social_repo.PLATFORM_POST_TABLES[platform]
            stats = _repair_platform(
                cur,
                platform=platform,
                table=table,
                cdn_base_url=cdn_base_url,
                season_ids=season_ids,
                show_ids=show_ids,
                season_numbers=season_numbers,
                limit_per_platform=limit_per_platform,
                dry_run=dry_run,
            )
            by_platform[platform] = {
                "scanned_rows": stats.scanned_rows,
                "rows_needing_repair": stats.rows_needing_repair,
                "rows_updated": stats.rows_updated,
                "thumbnail_urls_rewritten": stats.thumbnail_urls_rewritten,
                "media_urls_rewritten": stats.media_urls_rewritten,
                "avatar_urls_rewritten": stats.avatar_urls_rewritten,
                "media_asset_meta_urls_rewritten": stats.media_asset_meta_urls_rewritten,
            }
            totals.scanned_rows += stats.scanned_rows
            totals.rows_needing_repair += stats.rows_needing_repair
            totals.rows_updated += stats.rows_updated
            totals.thumbnail_urls_rewritten += stats.thumbnail_urls_rewritten
            totals.media_urls_rewritten += stats.media_urls_rewritten
            totals.avatar_urls_rewritten += stats.avatar_urls_rewritten
            totals.media_asset_meta_urls_rewritten += stats.media_asset_meta_urls_rewritten

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
                "season_ids": season_ids,
                "show_ids": show_ids,
                "season_numbers": season_numbers,
                "platforms": platforms,
                "limit_per_platform": limit_per_platform,
                "batch_size": batch_size,
                "totals": {
                    "scanned_rows": totals.scanned_rows,
                    "rows_needing_repair": totals.rows_needing_repair,
                    "rows_updated": totals.rows_updated,
                    "thumbnail_urls_rewritten": totals.thumbnail_urls_rewritten,
                    "media_urls_rewritten": totals.media_urls_rewritten,
                    "avatar_urls_rewritten": totals.avatar_urls_rewritten,
                    "media_asset_meta_urls_rewritten": totals.media_asset_meta_urls_rewritten,
                },
                "by_platform": by_platform,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
