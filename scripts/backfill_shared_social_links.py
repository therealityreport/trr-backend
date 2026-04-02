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
        description="Persist shared social account source links onto matching shows.",
    )
    parser.add_argument("--show-id", action="append", default=[], help="Optional show UUID filter. Repeatable.")
    parser.add_argument(
        "--source-scope",
        default="bravo",
        help="Shared social source scope to backfill. Defaults to bravo.",
    )
    parser.add_argument("--limit", type=int, default=200, help="Max shows to scan.")
    parser.add_argument("--apply", action="store_true", help="Persist the links instead of dry-run output.")
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


def _fetch_show_rows(*, show_ids: list[str], source_scope: str, limit: int) -> list[dict[str, Any]]:
    normalized_scope = str(source_scope or "").strip().casefold()
    if normalized_scope == "bravo":
        network_clause = """
        exists (
          select 1
          from unnest(coalesce(s.networks, array[]::text[])) as network_name
          where lower(btrim(network_name)) in ('bravo', 'bravo tv')
        )
        """
    else:
        network_clause = "false"

    if show_ids:
        return pg.fetch_all(
            f"""
            SELECT id::text AS id, name, networks
            FROM core.shows AS s
            WHERE {network_clause}
              AND id = ANY(%s::uuid[])
            ORDER BY name ASC
            LIMIT %s
            """,
            [show_ids, max(1, int(limit))],
        )

    return pg.fetch_all(
        f"""
        SELECT id::text AS id, name, networks
        FROM core.shows AS s
        WHERE {network_clause}
        ORDER BY name ASC
        LIMIT %s
        """,
        [max(1, int(limit))],
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    show_ids = _dedupe(args.show_id)
    source_scope = str(args.source_scope or "").strip().casefold() or "bravo"
    rows = _fetch_show_rows(show_ids=show_ids, source_scope=source_scope, limit=args.limit)

    mode = "apply" if args.apply else "dry-run"
    print(f"mode: {mode}")
    print(f"source_scope: {source_scope}")
    print(f"target_shows: {len(rows)}")
    for row in rows:
        print(f"- {row.get('name') or row.get('id')} ({row.get('id')})")

    if not args.apply or not rows:
        return 0

    db = create_supabase_admin_client()
    for row in rows:
        show_id = str(row.get("id") or "").strip()
        if not show_id:
            continue
        persisted = admin_show_links._persist_shared_social_source_links(
            show_id=show_id,
            db=db,
            actor="backfill_shared_social_links",
            discovered_by="shared_account_source",
            source_scope=source_scope,
        )
        print(f"persisted {persisted} links for {row.get('name') or show_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
