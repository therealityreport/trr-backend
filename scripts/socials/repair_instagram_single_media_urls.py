#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


VIDEO_URL_RE = re.compile(r"\.(?:mp4|m4v|mov|webm|avi)(?:$|[?#])", re.IGNORECASE)


@dataclass(slots=True)
class RepairStats:
    matched_rows: int = 0
    rows_needing_repair: int = 0
    rows_updated: int = 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_instagram_single_media_urls",
        description=(
            "Normalize historical Instagram non-carousel rows so single-media posts store one canonical source URL."
        ),
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
        "--limit",
        type=int,
        default=1000,
        help="Maximum candidate rows to scan (default: 1000).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matching rows without mutating them (default).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the repair to matching rows.",
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


def _as_text_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for raw in value:
        text = str(raw or "").strip()
        if text:
            items.append(text)
    return items


def _dedupe_urls(urls: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in urls:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(value)
    return normalized


def _is_video_like_url(url: str) -> bool:
    value = str(url or "").strip()
    if not value:
        return False
    return bool(VIDEO_URL_RE.search(value))


def _select_primary_media_url(*, media_type: str | None, post_format: str | None, media_urls: list[str]) -> str | None:
    canonical_urls = _dedupe_urls(media_urls)
    if not canonical_urls:
        return None
    normalized_media_type = str(media_type or "").strip().lower()
    normalized_post_format = str(post_format or "").strip().lower()
    expects_video = normalized_media_type in {"video", "reel"} or normalized_post_format in {"video", "reel"}
    if expects_video:
        for url in canonical_urls:
            if _is_video_like_url(url):
                return url
    else:
        for url in canonical_urls:
            if not _is_video_like_url(url):
                return url
    return canonical_urls[0]


def _repair_candidate_row(row: dict[str, Any]) -> dict[str, Any] | None:
    media_urls = _dedupe_urls(_as_text_list(row.get("media_urls")))
    if len(media_urls) <= 1:
        return None

    primary_url = _select_primary_media_url(
        media_type=str(row.get("media_type") or "").strip() or None,
        post_format=str(row.get("post_format") or "").strip() or None,
        media_urls=media_urls,
    )
    if not primary_url:
        return None
    if media_urls == [primary_url]:
        return None

    return {
        "id": str(row.get("id") or "").strip(),
        "shortcode": str(row.get("shortcode") or "").strip(),
        "old_media_urls": media_urls,
        "new_media_urls": [primary_url],
    }


def _fetch_candidate_rows(*, season_ids: list[str], show_ids: list[str], limit: int) -> list[dict[str, Any]]:
    filters = [
        "jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb)) > 1",
        "coalesce(nullif(to_jsonb(p) ->> 'post_format', ''), 'post') <> 'carousel'",
        "coalesce(nullif(p.media_type, ''), 'image') <> 'carousel'",
    ]
    params: list[object] = []
    if season_ids:
        filters.append("p.season_id::text = any(%s)")
        params.append(season_ids)
    if show_ids:
        filters.append("p.show_id::text = any(%s)")
        params.append(show_ids)
    params.append(max(1, int(limit)))
    return pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.shortcode,
          p.media_type,
          coalesce(nullif(to_jsonb(p) ->> 'post_format', ''), 'post') as post_format,
          coalesce(p.media_urls, '[]'::jsonb) as media_urls
        from social.instagram_posts p
        where {" and ".join(filters)}
        order by p.posted_at desc nulls last, p.id desc
        limit %s
        """,
        params,
    )


def _apply_repairs(repairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    updated: list[dict[str, Any]] = []
    for repair in repairs:
        row_id = str(repair.get("id") or "").strip()
        if not row_id:
            continue
        media_urls_json = json.dumps(repair["new_media_urls"])
        result = pg.execute_returning(
            """
            update social.instagram_posts
            set media_urls = %s::jsonb
            where id::text = %s
            returning id::text as id, shortcode
            """,
            [media_urls_json, row_id],
        )
        updated.extend(result)
    return updated


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    season_ids = _normalize_text_filters(args.season_id)
    show_ids = _normalize_text_filters(args.show_id)
    dry_run = bool(args.dry_run and not args.apply)

    candidate_rows = _fetch_candidate_rows(season_ids=season_ids, show_ids=show_ids, limit=args.limit)
    repairs = [repair for row in candidate_rows if (repair := _repair_candidate_row(row))]
    updated_rows: list[dict[str, Any]] = []
    if not dry_run and repairs:
        updated_rows = _apply_repairs(repairs)

    stats = RepairStats(
        matched_rows=len(candidate_rows),
        rows_needing_repair=len(repairs),
        rows_updated=len(updated_rows),
    )
    preview = [
        {
            "shortcode": str(repair.get("shortcode") or ""),
            "old_count": len(repair.get("old_media_urls") or []),
            "new_media_urls": repair.get("new_media_urls") or [],
        }
        for repair in repairs[:10]
    ]
    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "season_ids": season_ids,
                "show_ids": show_ids,
                "limit": max(1, int(args.limit)),
                "totals": {
                    "matched_rows": stats.matched_rows,
                    "rows_needing_repair": stats.rows_needing_repair,
                    "rows_updated": stats.rows_updated,
                },
                "preview": preview,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
