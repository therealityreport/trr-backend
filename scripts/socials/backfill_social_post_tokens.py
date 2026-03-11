#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env


@dataclass(slots=True)
class PlatformCounters:
    scanned: int = 0
    updated: int = 0
    skipped_existing_tokens: int = 0
    skipped_empty_text: int = 0
    extracted_hashtags: int = 0
    extracted_mentions: int = 0


PLATFORM_CONFIG: dict[str, dict[str, str]] = {
    "tiktok": {
        "table": "tiktok_posts",
        "id_col": "id",
        "text_expr": "coalesce(p.description, '')",
        "order_col": "coalesce(p.posted_at, p.scraped_at)",
        "hashtags_col": "hashtags",
        "mentions_col": "mentions",
    },
    "youtube": {
        "table": "youtube_videos",
        "id_col": "id",
        "text_expr": "concat_ws(E'\\n', nullif(p.title, ''), nullif(p.description, ''))",
        "order_col": "coalesce(p.published_at, p.scraped_at)",
        "hashtags_col": "hashtags",
        "mentions_col": "mentions",
    },
    "facebook": {
        "table": "facebook_posts",
        "id_col": "id",
        "text_expr": "coalesce(p.caption, '')",
        "order_col": "coalesce(p.posted_at, p.scraped_at)",
        "hashtags_col": "hashtags",
        "mentions_col": "mentions",
    },
    "threads": {
        "table": "meta_threads_posts",
        "id_col": "id",
        "text_expr": "coalesce(p.text, '')",
        "order_col": "coalesce(p.posted_at, p.scraped_at)",
        "hashtags_col": "hashtags",
        "mentions_col": "mentions",
    },
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill social post hashtags/mentions for existing rows.")
    parser.add_argument("--season-id", default="", help="Optional season UUID filter.")
    parser.add_argument(
        "--platforms",
        default="tiktok,youtube,facebook,threads",
        help="Comma-separated platforms (default: tiktok,youtube,facebook,threads).",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per batch (default: 500).")
    parser.add_argument("--dry-run", action="store_true", help="Compute updates without writing rows.")
    return parser.parse_args()


def _normalize_platforms(raw: str) -> list[str]:
    platforms = [item.strip().lower() for item in str(raw or "").split(",") if item.strip()]
    ordered: list[str] = []
    seen: set[str] = set()
    for platform in platforms:
        if platform not in PLATFORM_CONFIG or platform in seen:
            continue
        seen.add(platform)
        ordered.append(platform)
    return ordered


def _tokenize_text(text: str) -> tuple[list[str], list[str]]:
    hashtags = social_repo._as_text_list(social_repo._parse_hashtags(text), strip_prefix="#")  # noqa: SLF001
    mentions = social_repo._as_text_list(social_repo._parse_mentions(text), prefix="@", strip_prefix="@")  # noqa: SLF001
    return hashtags, mentions


def _fetch_batch(
    *,
    platform: str,
    season_id: str,
    batch_size: int,
    offset: int,
    hashtags_col_enabled: bool,
    mentions_col_enabled: bool,
) -> list[dict[str, Any]]:
    cfg = PLATFORM_CONFIG[platform]
    hashtags_expr = f"coalesce(p.{cfg['hashtags_col']}, '[]'::jsonb)" if hashtags_col_enabled else "'[]'::jsonb"
    mentions_expr = f"coalesce(p.{cfg['mentions_col']}, '[]'::jsonb)" if mentions_col_enabled else "'[]'::jsonb"
    where_clause = "where (%s = '' or p.season_id::text = %s)"
    params: list[Any] = [season_id, season_id, max(1, int(batch_size)), max(0, int(offset))]
    return pg.fetch_all(
        f"""
        select
          p.{cfg["id_col"]}::text as row_id,
          {cfg["text_expr"]} as text,
          {hashtags_expr} as hashtags,
          {mentions_expr} as mentions
        from social.{cfg["table"]} p
        {where_clause}
        order by {cfg["order_col"]} desc nulls last, p.{cfg["id_col"]} desc
        limit %s
        offset %s
        """,
        params,
    )


def _update_tokens(
    *,
    platform: str,
    row_id: str,
    hashtags: list[str] | None,
    mentions: list[str] | None,
) -> None:
    cfg = PLATFORM_CONFIG[platform]
    assignments: list[str] = []
    params: list[Any] = []
    if hashtags is not None:
        assignments.append(f"{cfg['hashtags_col']} = %s::jsonb")
        params.append(json.dumps(hashtags))
    if mentions is not None:
        assignments.append(f"{cfg['mentions_col']} = %s::jsonb")
        params.append(json.dumps(mentions))
    if not assignments:
        return
    params.append(row_id)
    with pg.db_cursor() as cur:
        cur.execute(
            f"""
            update social.{cfg["table"]}
            set {", ".join(assignments)}
            where {cfg["id_col"]} = %s::uuid
            """,
            params,
        )


def main() -> int:
    load_env()
    args = _parse_args()

    platforms = _normalize_platforms(args.platforms)
    if not platforms:
        raise SystemExit("No valid platforms requested.")

    season_id = str(args.season_id or "").strip()
    batch_size = max(1, int(args.batch_size))
    counters: dict[str, PlatformCounters] = {platform: PlatformCounters() for platform in platforms}

    for platform in platforms:
        cfg = PLATFORM_CONFIG[platform]
        hashtags_col_enabled = social_repo._platform_posts_has_column(platform, cfg["hashtags_col"])  # noqa: SLF001
        mentions_col_enabled = social_repo._platform_posts_has_column(platform, cfg["mentions_col"])  # noqa: SLF001
        if not hashtags_col_enabled and not mentions_col_enabled:
            continue

        offset = 0
        while True:
            rows = _fetch_batch(
                platform=platform,
                season_id=season_id,
                batch_size=batch_size,
                offset=offset,
                hashtags_col_enabled=hashtags_col_enabled,
                mentions_col_enabled=mentions_col_enabled,
            )
            if not rows:
                break

            for row in rows:
                counters[platform].scanned += 1
                text = str(row.get("text") or "").strip()
                existing_hashtags = social_repo._as_text_list(row.get("hashtags"), strip_prefix="#")  # noqa: SLF001
                existing_mentions = social_repo._as_text_list(  # noqa: SLF001
                    row.get("mentions"),
                    prefix="@",
                    strip_prefix="@",
                )

                if not text:
                    counters[platform].skipped_empty_text += 1
                    continue

                parsed_hashtags, parsed_mentions = _tokenize_text(text)

                next_hashtags: list[str] | None = None
                next_mentions: list[str] | None = None
                changed = False

                if hashtags_col_enabled and not existing_hashtags and parsed_hashtags:
                    next_hashtags = parsed_hashtags
                    counters[platform].extracted_hashtags += len(parsed_hashtags)
                    changed = True
                if mentions_col_enabled and not existing_mentions and parsed_mentions:
                    next_mentions = parsed_mentions
                    counters[platform].extracted_mentions += len(parsed_mentions)
                    changed = True

                if not changed:
                    counters[platform].skipped_existing_tokens += 1
                    continue

                if not args.dry_run:
                    _update_tokens(
                        platform=platform,
                        row_id=str(row.get("row_id") or ""),
                        hashtags=next_hashtags,
                        mentions=next_mentions,
                    )
                counters[platform].updated += 1

            offset += len(rows)

    totals = PlatformCounters()
    for data in counters.values():
        totals.scanned += data.scanned
        totals.updated += data.updated
        totals.skipped_existing_tokens += data.skipped_existing_tokens
        totals.skipped_empty_text += data.skipped_empty_text
        totals.extracted_hashtags += data.extracted_hashtags
        totals.extracted_mentions += data.extracted_mentions

    print(
        json.dumps(
            {
                "season_id": season_id or None,
                "platforms": platforms,
                "batch_size": batch_size,
                "dry_run": bool(args.dry_run),
                "totals": {
                    "rows_scanned": totals.scanned,
                    "rows_updated": totals.updated,
                    "skipped_existing_tokens": totals.skipped_existing_tokens,
                    "skipped_empty_text": totals.skipped_empty_text,
                    "extracted_hashtags": totals.extracted_hashtags,
                    "extracted_mentions": totals.extracted_mentions,
                },
                "by_platform": {
                    platform: {
                        "rows_scanned": data.scanned,
                        "rows_updated": data.updated,
                        "skipped_existing_tokens": data.skipped_existing_tokens,
                        "skipped_empty_text": data.skipped_empty_text,
                        "extracted_hashtags": data.extracted_hashtags,
                        "extracted_mentions": data.extracted_mentions,
                    }
                    for platform, data in counters.items()
                },
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
