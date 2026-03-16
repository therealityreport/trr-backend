#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

try:
    from trr_backend.db import pg
    from trr_backend.socials.youtube.scraper import YouTubeScraper
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.socials.youtube.scraper import YouTubeScraper
    from trr_backend.utils.env import load_env


EPOCHISH_CUTOFF_SQL = "1970-01-02T00:00:00+00:00"


@dataclass(slots=True)
class RepairStats:
    examined_rows: int = 0
    epoch_rows_found: int = 0
    rows_repaired: int = 0
    rows_unresolved: int = 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_youtube_short_timestamps",
        description="Repair epoch or missing published_at timestamps for YouTube Shorts rows.",
    )
    parser.add_argument("--season-id", action="append", default=[], help="Optional season UUID filter.")
    parser.add_argument("--show-id", action="append", default=[], help="Optional show UUID filter.")
    parser.add_argument(
        "--season-number",
        action="append",
        default=[],
        help="Optional season number filter. Repeat to target multiple seasons.",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Maximum candidate rows to examine.")
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.25,
        help="Delay used by the YouTube page fetches when resolving exact publish timestamps.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview repairs without mutating rows (default).")
    parser.add_argument("--apply", action="store_true", help="Apply repaired timestamps.")
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


def _fetch_epoch_short_rows(
    *,
    season_ids: list[str],
    show_ids: list[str],
    season_numbers: list[int],
    limit: int,
) -> list[dict[str, object]]:
    filters = [
        "(coalesce(v.is_short, false) = true or coalesce(v.source_surface, '') = 'shorts')",
        "(v.published_at is null or v.published_at <= %s::timestamptz)",
    ]
    params: list[object] = [EPOCHISH_CUTOFF_SQL]
    if season_ids:
        filters.append("v.season_id::text = any(%s)")
        params.append(season_ids)
    if show_ids:
        filters.append("v.show_id::text = any(%s)")
        params.append(show_ids)
    if season_numbers:
        filters.append("s.season_number = any(%s)")
        params.append(season_numbers)
    params.append(max(1, int(limit or 1)))
    return pg.fetch_all(
        f"""
        select
          v.id::text as id,
          v.video_id,
          v.title,
          v.published_at,
          v.season_id::text as season_id,
          v.show_id::text as show_id,
          s.season_number,
          coalesce(v.is_short, false) as is_short,
          coalesce(v.source_surface, '') as source_surface
        from social.youtube_videos v
        left join core.seasons s on s.id = v.season_id
        where {' and '.join(filters)}
        order by v.id
        limit %s
        """,
        params,
    )


def _repair_row(*, row_id: str, published_at_iso: str) -> list[dict[str, object]]:
    return pg.execute_returning(
        """
        update social.youtube_videos
        set published_at = %s::timestamptz
        where id = %s::uuid
          and (published_at is null or published_at <= %s::timestamptz)
        returning id::text as id, published_at
        """,
        [published_at_iso, row_id, EPOCHISH_CUTOFF_SQL],
    )


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    season_ids = _normalize_text_filters(args.season_id)
    show_ids = _normalize_text_filters(args.show_id)
    season_numbers = _normalize_int_filters(args.season_number)
    dry_run = bool(args.dry_run and not args.apply)

    rows = _fetch_epoch_short_rows(
        season_ids=season_ids,
        show_ids=show_ids,
        season_numbers=season_numbers,
        limit=args.limit,
    )
    scraper = YouTubeScraper()
    stats = RepairStats(examined_rows=len(rows), epoch_rows_found=len(rows))
    repaired_rows: list[dict[str, object]] = []
    unresolved_video_ids: list[str] = []

    for row in rows:
        video_id = str(row.get("video_id") or "").strip()
        if not video_id:
            stats.rows_unresolved += 1
            continue
        precise_ts = scraper._fetch_precise_publish_timestamp(  # noqa: SLF001
            video_id,
            delay=max(0.0, float(args.delay_seconds or 0.0)),
        )
        if precise_ts <= 0:
            stats.rows_unresolved += 1
            unresolved_video_ids.append(video_id)
            continue
        published_at_iso = datetime.fromtimestamp(precise_ts, tz=UTC).isoformat()
        if dry_run:
            stats.rows_repaired += 1
            repaired_rows.append(
                {
                    "id": str(row.get("id") or ""),
                    "video_id": video_id,
                    "published_at": published_at_iso,
                }
            )
            continue
        updated = _repair_row(row_id=str(row.get("id") or ""), published_at_iso=published_at_iso)
        if updated:
            stats.rows_repaired += len(updated)
            repaired_rows.extend(
                {
                    "id": str(item.get("id") or ""),
                    "video_id": video_id,
                    "published_at": str(item.get("published_at") or published_at_iso),
                }
                for item in updated
            )
        else:
            stats.rows_unresolved += 1

    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "season_ids": season_ids,
                "show_ids": show_ids,
                "season_numbers": season_numbers,
                "totals": {
                    "examined_rows": stats.examined_rows,
                    "epoch_rows_found": stats.epoch_rows_found,
                    "rows_repaired": stats.rows_repaired,
                    "rows_unresolved": stats.rows_unresolved,
                },
                "repaired_preview": repaired_rows[:10],
                "unresolved_video_ids_preview": unresolved_video_ids[:10],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
