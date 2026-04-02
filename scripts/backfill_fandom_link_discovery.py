#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from typing import Any

from api.routers import admin_show_links
from trr_backend.db import pg
from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Re-run show link discovery for shows that already have allowlisted Fandom seeds.",
    )
    parser.add_argument("--show-id", action="append", default=[], help="Optional show UUID filter. Repeatable.")
    parser.add_argument("--limit", type=int, default=100, help="Max shows to scan.")
    parser.add_argument("--apply", action="store_true", help="Run discovery instead of dry-run output.")
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


def filter_allowlisted_fandom_seed_show_ids(rows: list[dict[str, Any]]) -> list[str]:
    show_ids: list[str] = []
    seen: set[str] = set()
    for row in rows:
        show_id = str(row.get("show_id") or "").strip()
        url = str(row.get("url") or "").strip()
        if not show_id or not url:
            continue
        if not admin_show_links._is_allowlisted_fandom_seed_input(url):
            continue
        if show_id in seen:
            continue
        seen.add(show_id)
        show_ids.append(show_id)
    return show_ids


def _fetch_candidate_rows(show_ids: list[str], limit: int) -> list[dict[str, Any]]:
    params: list[Any] = [max(1, int(limit))]
    where_clauses = [
        "lower(coalesce(link_kind, '')) in ('fandom', 'wikia')",
        "lower(coalesce(status, 'approved')) <> 'rejected'",
    ]
    if show_ids:
        where_clauses.append("show_id = ANY(%s::uuid[])")
        params.append(show_ids)
    return pg.fetch_all(
        f"""
        SELECT show_id::text AS show_id, url, link_kind, status
        FROM core.entity_links
        WHERE {" AND ".join(where_clauses)}
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        LIMIT %s
        """,
        params if not show_ids else [show_ids, max(1, int(limit))],
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    show_filter = _dedupe(args.show_id)
    rows = _fetch_candidate_rows(show_filter, args.limit)
    target_show_ids = filter_allowlisted_fandom_seed_show_ids(rows)

    mode = "apply" if args.apply else "dry-run"
    print(f"mode: {mode}")
    print(f"target_shows: {len(target_show_ids)}")
    for show_id in target_show_ids:
        print(f"- {show_id}")

    if not args.apply or not target_show_ids:
        return 0

    db = create_supabase_admin_client()
    for show_id in target_show_ids:
        admin_show_links._run_show_link_discovery(
            show_id_str=show_id,
            payload=admin_show_links.LinkDiscoverRequest(include_seasons=True, include_people=True),
            db=db,
            actor="backfill_fandom_link_discovery",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
