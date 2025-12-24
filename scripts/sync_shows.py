#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from uuid import UUID

from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert
from trr_backend.models.shows import ShowRecord
from trr_backend.repositories.shows import update_show

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db


def _merge_external_ids(existing: dict[str, object], updates: dict[str, object]) -> dict[str, object]:
    merged = dict(existing)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_shows",
        description="Refresh core.shows metadata from existing show rows.",
    )
    add_show_filter_args(parser)
    return parser.parse_args(argv)


def _build_show_records(show_rows: list[dict[str, object]]) -> list[ShowRecord]:
    records: list[ShowRecord] = []
    for row in show_rows:
        show_id = row.get("id")
        try:
            show_uuid = UUID(str(show_id))
        except Exception:
            continue
        external_ids = row.get("external_ids")
        external_ids_map = external_ids if isinstance(external_ids, dict) else {}
        records.append(
            ShowRecord(
                id=show_uuid,
                name=str(row.get("name") or ""),
                description=row.get("description") if isinstance(row.get("description"), str) else None,
                premiere_date=row.get("premiere_date") if isinstance(row.get("premiere_date"), str) else None,
                external_ids=external_ids_map,
                imdb_series_id=row.get("imdb_series_id") if isinstance(row.get("imdb_series_id"), str) else None,
                tmdb_series_id=row.get("tmdb_series_id") if isinstance(row.get("tmdb_series_id"), int) else None,
            )
        )
    return records


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    records = _build_show_records(show_rows)
    if not records:
        print("No valid show rows to enrich.")
        return 0

    summary = enrich_shows_after_upsert(records, dry_run=args.dry_run)
    print(
        "ENRICH summary "
        f"attempted={summary.attempted} "
        f"updated={summary.updated} "
        f"skipped_complete={summary.skipped_complete} "
        f"skipped={summary.skipped} "
        f"failed={summary.failed}"
    )

    if args.dry_run:
        return 0

    show_by_id = {str(row.get("id")): row for row in show_rows if row.get("id")}
    for patch in summary.patches:
        row = show_by_id.get(str(patch.show_id))
        if row is None:
            continue
        existing_external_ids = row.get("external_ids")
        existing_external_ids_map = existing_external_ids if isinstance(existing_external_ids, dict) else {}
        merged_external_ids = _merge_external_ids(existing_external_ids_map, patch.external_ids_update)

        update_patch: dict[str, object] = {}
        if merged_external_ids != existing_external_ids_map:
            update_patch["external_ids"] = merged_external_ids
        for key, value in (patch.show_update or {}).items():
            if row.get(key) != value:
                update_patch[key] = value

        if not update_patch:
            continue
        updated_row = update_show(db, patch.show_id, update_patch)
        if args.verbose:
            print(f"UPDATED show id={updated_row.get('id')} name={updated_row.get('name')!r}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
