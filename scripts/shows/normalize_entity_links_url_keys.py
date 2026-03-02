#!/usr/bin/env python3
"""
Normalize legacy entity link kinds and URL keys.

Default mode is dry-run. Use --apply to persist changes.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from api.routers import admin_show_links
from trr_backend.db import pg
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="normalize_entity_links_url_keys",
        description="Normalize core.entity_links url/url_key canonicalization and legacy knowledge kinds.",
    )
    parser.add_argument("--show-id", default="", help="Optional show UUID filter.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only (default unless --apply is set).")
    parser.add_argument("--apply", action="store_true", help="Write updates.")
    parser.add_argument("--limit", type=int, default=5000, help="Max rows to scan.")
    return parser.parse_args(argv)


def _fetch_rows(show_id: str, limit: int) -> list[dict[str, Any]]:
    params: list[Any] = []
    clauses = ["true"]
    if show_id:
        clauses.append("show_id = %s::uuid")
        params.append(show_id)
    params.append(max(1, int(limit)))
    return pg.fetch_all(
        f"""
        SELECT id::text AS id, url, url_key, link_kind
        FROM core.entity_links
        WHERE {" AND ".join(clauses)}
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        LIMIT %s
        """,
        params,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    dry_run = not args.apply or args.dry_run
    show_id = str(args.show_id or "").strip()
    load_env()

    rows = _fetch_rows(show_id, args.limit)
    updates: list[tuple[str, str, str, str]] = []
    for row in rows:
        link_id = str(row.get("id") or "").strip()
        if not link_id:
            continue
        old_url = str(row.get("url") or "").strip()
        old_kind = str(row.get("link_kind") or "").strip()
        canonical_url = admin_show_links._canonicalize_url(old_url)
        canonical_key = admin_show_links._url_key(canonical_url)
        canonical_kind = admin_show_links._normalize_link_kind(old_kind)
        if (
            old_url == canonical_url
            and str(row.get("url_key") or "").strip() == canonical_key
            and old_kind == canonical_kind
        ):
            continue
        updates.append((link_id, canonical_url, canonical_key, canonical_kind))

    print(f"rows_scanned: {len(rows)}")
    print(f"rows_to_update: {len(updates)}")
    if dry_run:
        print("mode: dry-run")
        return 0

    with pg.db_connection() as conn:
        for link_id, canonical_url, canonical_key, canonical_kind in updates:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute(
                    """
                    UPDATE core.entity_links
                    SET
                      url = %s,
                      url_key = %s,
                      link_kind = %s,
                      updated_at = NOW()
                    WHERE id = %s::uuid
                    """,
                    [canonical_url, canonical_key, canonical_kind, link_id],
                )
    print("mode: apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
