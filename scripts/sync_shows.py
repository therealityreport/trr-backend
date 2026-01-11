#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from uuid import UUID

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db
from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert
from trr_backend.models.shows import ShowRecord
from trr_backend.repositories.show_images import upsert_show_images
from trr_backend.repositories.shows import update_show


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_shows",
        description="Refresh core.shows metadata from existing show rows.",
    )
    add_show_filter_args(parser)
    return parser.parse_args(argv)


def _extract_imdb_id(row: dict[str, object]) -> str | None:
    """Extract IMDb ID, supporting both old and new column names."""
    for key in ("imdb_id", "imdb_series_id"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_tmdb_id(row: dict[str, object]) -> int | None:
    """Extract TMDb ID, supporting both old and new column names."""
    for key in ("tmdb_id", "tmdb_series_id"):
        value = row.get(key)
        if isinstance(value, int):
            return value
    return None


def _merge_str_arrays(existing: object, incoming: list[str] | None) -> list[str] | None:
    if not incoming:
        return None
    existing_values = [
        str(v).strip()
        for v in (existing if isinstance(existing, list) else [])
        if isinstance(v, str) and str(v).strip()
    ]
    incoming_values = [str(v).strip() for v in incoming if isinstance(v, str) and str(v).strip()]
    merged = sorted(set(existing_values) | set(incoming_values))
    if not merged or merged == sorted(existing_values):
        return None
    return merged


def _merge_int_arrays(existing: object, incoming: list[int] | None) -> list[int] | None:
    if not incoming:
        return None
    existing_values = [v for v in (existing if isinstance(existing, list) else []) if isinstance(v, int)]
    incoming_values = [v for v in incoming if isinstance(v, int)]
    merged = sorted(set(existing_values) | set(incoming_values))
    if not merged or merged == sorted(existing_values):
        return None
    return merged


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
                imdb_id=_extract_imdb_id(row),
                tmdb_id=_extract_tmdb_id(row),
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

        update_patch: dict[str, object] = {}
        for key, value in (patch.show_update or {}).items():
            if row.get(key) != value:
                update_patch[key] = value

        merged_genres = _merge_str_arrays(row.get("genres"), patch.genres)
        if merged_genres is not None:
            update_patch["genres"] = merged_genres

        merged_keywords = _merge_str_arrays(row.get("keywords"), patch.keywords)
        if merged_keywords is not None:
            update_patch["keywords"] = merged_keywords

        merged_tags = _merge_str_arrays(row.get("tags"), patch.tags)
        if merged_tags is not None:
            update_patch["tags"] = merged_tags

        merged_networks = _merge_str_arrays(row.get("networks"), patch.networks)
        if merged_networks is not None:
            update_patch["networks"] = merged_networks

        merged_streaming = _merge_str_arrays(row.get("streaming_providers"), patch.streaming_providers)
        if merged_streaming is not None:
            update_patch["streaming_providers"] = merged_streaming

        merged_tmdb_network_ids = _merge_int_arrays(row.get("tmdb_network_ids"), patch.tmdb_network_ids)
        if merged_tmdb_network_ids is not None:
            update_patch["tmdb_network_ids"] = merged_tmdb_network_ids

        merged_tmdb_company_ids = _merge_int_arrays(
            row.get("tmdb_production_company_ids"), patch.tmdb_production_company_ids
        )
        if merged_tmdb_company_ids is not None:
            update_patch["tmdb_production_company_ids"] = merged_tmdb_company_ids

        if patch.show_images_rows:
            try:
                upsert_show_images(db, patch.show_images_rows)
            except Exception as exc:  # noqa: BLE001
                print(f"ENRICH images failed show_id={patch.show_id} error={exc}")

        if not update_patch:
            continue
        updated_row = update_show(db, patch.show_id, update_patch)
        if args.verbose:
            print(f"UPDATED show id={updated_row.get('id')} name={updated_row.get('name')!r}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
