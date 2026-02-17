#!/usr/bin/env python3
"""Remove known YouTube false positives from season social analytics.

Default target:
- Show: The Real Housewives of Salt Lake City
- Title pattern: contains "wife swap" and "real housewives edition"
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path (scripts/socials -> project root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from trr_backend.db import pg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


DEFAULT_SHOW_NAME = "The Real Housewives of Salt Lake City"


def _resolve_show_id(*, show_id: str | None, show_name: str) -> str:
    if show_id:
        return show_id

    row = pg.fetch_one(
        """
        select id::text as id
        from core.shows
        where lower(name) = lower(%s)
        limit 1
        """,
        [show_name],
    )
    if not row:
        raise ValueError(f"Could not resolve show by name: {show_name}")
    return str(row["id"])


def _find_candidate_rows(*, resolved_show_id: str) -> list[dict[str, object]]:
    return pg.fetch_all(
        """
        select
          id::text as id,
          season_id::text as season_id,
          video_id,
          title,
          published_at
        from social.youtube_videos
        where show_id = %s::uuid
          and lower(coalesce(title, '')) like '%%wife swap%%'
          and lower(coalesce(title, '')) like '%%real housewives edition%%'
        order by published_at desc nulls last
        """,
        [resolved_show_id],
    )


def _delete_rows(*, row_ids: list[str]) -> int:
    deleted = pg.execute_returning(
        """
        delete from social.youtube_videos
        where id = any(%s::uuid[])
        returning id
        """,
        [row_ids],
    )
    return len(deleted)


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean known YouTube false positives for a show.")
    parser.add_argument("--show-id", help="Show UUID to scope cleanup.")
    parser.add_argument("--show-name", default=DEFAULT_SHOW_NAME, help=f"Show name (default: {DEFAULT_SHOW_NAME})")
    parser.add_argument("--dry-run", action="store_true", help="Preview candidate rows without deleting.")
    args = parser.parse_args()

    resolved_show_id = _resolve_show_id(show_id=args.show_id, show_name=args.show_name)
    candidates = _find_candidate_rows(resolved_show_id=resolved_show_id)

    logger.info("Show: %s (%s)", args.show_name, resolved_show_id)
    logger.info("Matched YouTube false-positive rows: %d", len(candidates))
    for row in candidates:
        logger.info(
            "- season=%s video=%s title=%s",
            row.get("season_id"),
            row.get("video_id"),
            row.get("title"),
        )

    if args.dry_run:
        logger.info("Dry run enabled: no rows deleted.")
        return

    if not candidates:
        logger.info("No rows to delete.")
        return

    deleted_count = _delete_rows(row_ids=[str(row["id"]) for row in candidates])
    logger.info("Deleted rows: %d", deleted_count)


if __name__ == "__main__":
    main()
