#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Any
from uuid import UUID

from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert
from trr_backend.media.s3_mirror import (
    get_cdn_base_url,
    get_s3_client,
    mirror_show_image_row,
    prune_orphaned_show_image_objects,
)
from trr_backend.models.shows import ShowRecord
from trr_backend.repositories.show_images import (
    assert_core_show_images_table_exists,
    fetch_show_images_missing_hosted,
    update_show_image_hosted_fields,
    upsert_show_images,
)

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_show_images",
        description="Sync show images from TMDb/IMDb and mirror to S3.",
    )
    add_show_filter_args(parser)
    parser.add_argument(
        "--source",
        default="all",
        choices=["imdb", "tmdb", "all"],
        help="Image source to mirror (default: all).",
    )
    parser.add_argument(
        "--kind",
        default=None,
        choices=["poster", "backdrop", "logo", "media"],
        help="Filter by image kind when mirroring.",
    )
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 mirroring.")
    parser.add_argument("--no-prune", action="store_true", help="Skip S3 prune step.")
    parser.add_argument("--force", action="store_true", help="Re-download and re-upload hosted images.")
    parser.add_argument(
        "--mirror-limit",
        type=int,
        default=200,
        help="Max rows to mirror per show (default: 200).",
    )
    return parser.parse_args(argv)


def _build_show_records(show_rows: list[dict[str, object]]) -> list[ShowRecord]:
    records: list[ShowRecord] = []
    for row in show_rows:
        show_id = row.get("id")
        try:
            show_uuid = UUID(str(show_id))
        except Exception:
            continue
        records.append(
            ShowRecord(
                id=show_uuid,
                name=str(row.get("name") or ""),
                description=row.get("description") if isinstance(row.get("description"), str) else None,
                premiere_date=row.get("premiere_date") if isinstance(row.get("premiere_date"), str) else None,
                imdb_id=row.get("imdb_id") if isinstance(row.get("imdb_id"), str) else None,
                tmdb_id=row.get("tmdb_id") if isinstance(row.get("tmdb_id"), int) else None,
            )
        )
    return records


def _mirror_show_images(
    db,
    *,
    show_rows: list[dict[str, Any]],
    source: str,
    kind: str | None,
    mirror_limit: int,
    force: bool,
    prune: bool,
    dry_run: bool,
    verbose: bool,
) -> None:
    if dry_run:
        return

    s3_client = get_s3_client()
    cdn_base_url = None if force else get_cdn_base_url()

    for show in show_rows:
        show_id = str(show.get("id") or "").strip()
        imdb_id = str(show.get("imdb_id") or "").strip()
        if not show_id:
            continue

        rows = fetch_show_images_missing_hosted(
            db,
            source=source,
            show_id=show_id,
            kind=kind,
            limit=mirror_limit,
            include_hosted=True,
            cdn_base_url=cdn_base_url,
        )
        if not rows:
            continue

        mirrored = 0
        skipped = 0
        failed = 0

        for row in rows:
            try:
                patch = mirror_show_image_row(row, force=force, s3_client=s3_client)
                if not patch:
                    skipped += 1
                    continue
                updated = update_show_image_hosted_fields(db, str(row.get("id")), patch)
                mirrored += 1
                if verbose:
                    print(f"OK {updated.get('id')} -> {updated.get('hosted_url')}")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if verbose:
                    print(f"WARN {row.get('id')}: {exc}")

        if verbose and (mirrored or skipped or failed):
            print(f"SHOW {show_id} mirrored={mirrored} skipped={skipped} failed={failed}")

        if not prune or dry_run or force or source != "all" or kind is not None:
            continue
        show_identifier = imdb_id or show_id
        if not show_identifier:
            continue
        prune_orphaned_show_image_objects(
            db,
            show_identifier,
            show_id=show_id,
            dry_run=dry_run,
            verbose=verbose,
            s3_client=s3_client,
        )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()
    assert_core_show_images_table_exists(db)

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    records = _build_show_records(show_rows)
    if not records:
        print("No valid show rows to enrich.")
        return 0

    summary = enrich_shows_after_upsert(records, force_refresh=bool(args.force), dry_run=bool(args.dry_run))
    if args.verbose:
        print(
            "ENRICH summary "
            f"attempted={summary.attempted} "
            f"updated={summary.updated} "
            f"skipped={summary.skipped} "
            f"failed={summary.failed}"
        )

    if args.dry_run:
        return 0

    upserted = 0
    for patch in summary.patches:
        if not patch.show_images_rows:
            continue
        upserted += len(patch.show_images_rows)
        upsert_show_images(db, patch.show_images_rows)

    if args.verbose:
        print(f"show_images_upserted={upserted}")

    if not args.no_s3:
        _mirror_show_images(
            db,
            show_rows=show_rows,
            source=args.source,
            kind=args.kind,
            mirror_limit=int(args.mirror_limit),
            force=bool(args.force),
            prune=not bool(args.no_prune),
            dry_run=bool(args.dry_run),
            verbose=bool(args.verbose),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
