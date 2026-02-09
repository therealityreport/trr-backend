#!/usr/bin/env python3
"""
Repair cast_photos rows with missing or invalid hosted image data.

Defaults to dry-run. Use --apply to perform updates.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.media.s3_mirror import mirror_cast_photo_row
from trr_backend.repositories.cast_photos import update_cast_photo_hosted_fields
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair cast_photos hosted image data.")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default: dry-run).")
    parser.add_argument("--limit", type=int, default=200, help="Limit number of rows to inspect.")
    parser.add_argument("--source", choices=["imdb", "tmdb", "fandom", "fandom-gallery"], help="Filter by source.")
    parser.add_argument("--person-id", help="Filter by person UUID.")
    return parser.parse_args(argv)


def _is_image_content_type(value: str | None) -> bool:
    if not value:
        return False
    return value.split(";", 1)[0].strip().lower().startswith("image/")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    db = create_supabase_admin_client()

    query = (
        db.schema("core")
        .table("cast_photos")
        .select(
            "id,person_id,imdb_person_id,source,hosted_url,hosted_key,hosted_content_type,hosted_sha256,"
            "image_url,url,thumb_url,source_page_url"
        )
    )
    if args.source:
        query = query.eq("source", args.source)
    if args.person_id:
        query = query.eq("person_id", args.person_id)

    query = query.or_("hosted_url.is.null,hosted_content_type.is.null,hosted_content_type.not.ilike.image/%")
    if args.limit:
        query = query.limit(args.limit)

    result = query.execute()
    if getattr(result, "error", None):
        print(f"Query error: {result.error}")
        return 1

    rows: list[dict[str, Any]] = result.data or []
    if not rows:
        print("No cast_photos rows require repair.")
        return 0

    print(f"Found {len(rows)} cast_photos rows requiring repair.")
    if not args.apply:
        print("Dry-run mode. Re-run with --apply to update.")
        return 0

    updated = 0
    skipped = 0
    failed = 0

    for row in rows:
        ct = row.get("hosted_content_type")
        needs_force = bool(row.get("hosted_url") and not _is_image_content_type(ct))
        if row.get("hosted_url") and ct is None:
            needs_force = True
        try:
            patch = mirror_cast_photo_row(row, force=needs_force)
            if not patch:
                skipped += 1
                continue
            update_cast_photo_hosted_fields(db, str(row["id"]), patch)
            updated += 1
        except Exception as exc:
            failed += 1
            print(f"Failed to repair {row.get('id')}: {exc}")

    print(f"Updated: {updated}, Skipped: {skipped}, Failed: {failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
