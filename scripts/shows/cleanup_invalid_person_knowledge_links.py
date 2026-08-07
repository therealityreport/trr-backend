#!/usr/bin/env python3
"""
Clean invalid person source links for a show.

Default mode is dry-run. Use --apply to delete invalid rows.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from typing import Any

from api.routers import admin_show_links
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cleanup_invalid_person_knowledge_links",
        description="Validate and optionally delete invalid person source links for a show.",
    )
    parser.add_argument("--show-id", required=True, help="Show UUID.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only (default unless --apply is set).")
    parser.add_argument("--apply", action="store_true", help="Delete invalid rows.")
    return parser.parse_args(argv)


def _group_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        link_kind = str(row.get("link_kind") or "unknown").strip().lower() or "unknown"
        status = str(row.get("status") or "unknown").strip().lower() or "unknown"
        counter[f"{link_kind}:{status}"] += 1
    return dict(sorted(counter.items()))


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    dry_run = not args.apply or args.dry_run

    load_env()

    show_id = str(args.show_id).strip()
    if not admin_show_links._show_exists(show_id):
        print(f"Show not found: {show_id}", file=sys.stderr)
        return 2

    scan = admin_show_links._scan_invalid_person_knowledge_links(show_id)
    raw_scanned_rows = scan.get("scanned_rows")
    raw_invalid_rows = scan.get("invalid_rows")
    scanned_rows: list[dict[str, Any]] = raw_scanned_rows if isinstance(raw_scanned_rows, list) else []
    invalid_rows: list[dict[str, Any]] = raw_invalid_rows if isinstance(raw_invalid_rows, list) else []
    validation_failures = int(scan.get("validation_failures") or 0)

    invalid_ids = [str(row.get("id") or "").strip() for row in invalid_rows if row.get("id")]
    scanned_groups = _group_counts(scanned_rows)
    invalid_groups = _group_counts(invalid_rows)

    deleted = 0
    if not dry_run and invalid_ids:
        deleted = admin_show_links._delete_entity_links_by_id(invalid_ids)

    print(f"show_id: {show_id}")
    print(f"mode: {'dry-run' if dry_run else 'apply'}")
    print(f"scanned: {len(scanned_rows)}")
    print(f"invalid: {len(invalid_rows)}")
    print(f"validation_failures: {validation_failures}")
    print(f"deleted: {deleted}")
    print("scanned_by_kind_status:", scanned_groups)
    print("invalid_by_kind_status:", invalid_groups)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
