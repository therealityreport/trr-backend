#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from typing import Any

from trr_backend.db import pg
from trr_backend.integrations.tmdb.client import fetch_tv_details, resolve_api_key
from trr_backend.utils.env import load_env

DEFAULT_SHOW_IDS = [
    "7782652f-783a-488b-8860-41b97de32e75",
]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill core.shows.description from TMDb series-level overview text.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Show UUID to refresh. Repeatable. Defaults to RHOSLC when omitted.",
    )
    parser.add_argument("--apply", action="store_true", help="Persist the refreshed descriptions.")
    return parser.parse_args(argv)


def _dedupe(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _fetch_show_rows(show_ids: list[str]) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT id::text AS id, name, tmdb_id, description
        FROM core.shows
        WHERE id = ANY(%s::uuid[])
        ORDER BY name ASC
        """,
        [show_ids],
    )


def _fetch_tmdb_series_overview(tmdb_id: int, api_key: str) -> str | None:
    details = fetch_tv_details(tmdb_id, api_key=api_key)
    overview = details.get("overview") if isinstance(details, dict) else None
    if not isinstance(overview, str):
        return None
    cleaned = overview.strip()
    return cleaned or None


def _update_show_description(show_id: str, description: str) -> None:
    with pg.db_connection() as conn:
        with pg.db_cursor(conn=conn) as cur:
            cur.execute(
                """
                UPDATE core.shows
                SET description = %s,
                    updated_at = NOW()
                WHERE id = %s::uuid
                """,
                [description, show_id],
            )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    api_key = resolve_api_key()
    if not api_key:
        print("TMDb API key is required.", file=sys.stderr)
        return 1

    show_ids = _dedupe(args.show_id or DEFAULT_SHOW_IDS)
    rows = _fetch_show_rows(show_ids)
    if not rows:
        print("No matching shows found.")
        return 1

    mode = "apply" if args.apply else "dry-run"
    print(f"mode: {mode}")
    for row in rows:
        show_id = str(row.get("id") or "").strip()
        name = str(row.get("name") or "").strip() or show_id
        tmdb_id = row.get("tmdb_id")
        before = str(row.get("description") or "").strip() or None
        if not show_id or not isinstance(tmdb_id, int):
            print(f"skip {name}: missing tmdb_id")
            continue
        after = _fetch_tmdb_series_overview(tmdb_id, api_key)
        if not after:
            print(f"skip {name}: TMDb overview missing")
            continue
        print(f"{name} ({show_id})")
        print(f"  before: {before or '<blank>'}")
        print(f"  after:  {after}")
        if args.apply:
            _update_show_description(show_id, after)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
