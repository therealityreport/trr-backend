#!/usr/bin/env python3
"""Mirror core.show_images to S3 and store hosted_url."""
from __future__ import annotations

import argparse
import sys
from typing import Any

from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.repositories.show_images import (
    assert_core_show_images_table_exists,
    fetch_show_images_missing_hosted,
    update_show_image_hosted_fields,
)
from trr_backend.media.s3_mirror import get_cdn_base_url, mirror_show_image_row
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mirror_show_images_to_s3",
        description="Mirror core.show_images to S3 and store hosted_url.",
    )
    parser.add_argument(
        "--source",
        default="all",
        choices=["imdb", "tmdb", "all"],
        help="Image source to mirror (default: all).",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Filter by show UUID. Repeatable.",
    )
    parser.add_argument(
        "--imdb-id",
        action="append",
        default=[],
        help="Filter by IMDb title ID (tt...). Repeatable.",
    )
    parser.add_argument(
        "--tmdb-id",
        action="append",
        default=[],
        type=int,
        help="Filter by TMDb series ID. Repeatable.",
    )
    parser.add_argument(
        "--kind",
        default=None,
        choices=["poster", "backdrop", "logo", "media"],
        help="Filter by image kind.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max rows to mirror (default: 200).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned work without writing to S3 or Supabase.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing hosted fields.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args(argv)


def _coerce_str_list(values: list[str]) -> list[str]:
    out: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if text:
            out.append(text)
    return out


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    db = create_supabase_admin_client()
    assert_core_show_images_table_exists(db)

    show_ids = _coerce_str_list(args.show_id)
    imdb_ids = _coerce_str_list(args.imdb_id)
    tmdb_ids = args.tmdb_id or []

    # Fetch images for each filter combination
    all_rows: list[dict[str, Any]] = []

    cdn_base_url = None if args.force else get_cdn_base_url()

    if show_ids:
        for sid in show_ids:
            rows = fetch_show_images_missing_hosted(
                db,
                source=args.source,
                show_id=sid,
                kind=args.kind,
                limit=args.limit,
                include_hosted=True,
                cdn_base_url=cdn_base_url,
            )
            all_rows.extend(rows)
    elif imdb_ids:
        for iid in imdb_ids:
            rows = fetch_show_images_missing_hosted(
                db,
                source=args.source,
                imdb_id=iid,
                kind=args.kind,
                limit=args.limit,
                include_hosted=True,
                cdn_base_url=cdn_base_url,
            )
            all_rows.extend(rows)
    elif tmdb_ids:
        for tid in tmdb_ids:
            rows = fetch_show_images_missing_hosted(
                db,
                source=args.source,
                tmdb_id=tid,
                kind=args.kind,
                limit=args.limit,
                include_hosted=True,
                cdn_base_url=cdn_base_url,
            )
            all_rows.extend(rows)
    else:
        all_rows = fetch_show_images_missing_hosted(
            db,
            source=args.source,
            kind=args.kind,
            limit=args.limit,
            include_hosted=True,
            cdn_base_url=cdn_base_url,
        )

    # Dedupe by id
    seen_ids: set[str] = set()
    rows: list[dict[str, Any]] = []
    for row in all_rows:
        rid = str(row.get("id") or "")
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            rows.append(row)

    if not rows:
        print("No show images matched the filters.")
        return 0

    scanned = 0
    mirrored = 0
    skipped = 0
    failed = 0

    for row in rows:
        scanned += 1
        row_id = row.get("id")
        show_data = row.get("shows") or {}
        show_title = show_data.get("name") or row.get("show_id") or "unknown"

        try:
            patch = mirror_show_image_row(row, force=bool(args.force))
            if not patch:
                skipped += 1
                if args.verbose:
                    print(f"SKIP {row_id}: already hosted or unchanged")
                continue

            if args.dry_run:
                mirrored += 1
                if args.verbose:
                    print(f"DRY RUN: would upload {row_id} ({show_title}) -> {patch.get('hosted_url')}")
                continue

            updated = update_show_image_hosted_fields(db, str(row_id), patch)
            mirrored += 1
            if args.verbose:
                print(f"OK {updated.get('id')} ({show_title}) -> {updated.get('hosted_url')}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"WARN {row_id} ({show_title}): {exc}")

    print("\nSummary")
    print(f"scanned={scanned}")
    print(f"mirrored={mirrored}")
    print(f"skipped={skipped}")
    print(f"failed={failed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
