#!/usr/bin/env python3
"""Repair stale IMDb request-context show metadata on cast photos."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

try:
    from api.routers.admin_person_images import (
        _build_show_lookup_maps,
        _evaluate_imdb_request_context_staleness,
        _load_existing_imdb_cast_photos_for_person,
        _repair_existing_imdb_cast_photos,
    )
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from api.routers.admin_person_images import (
        _build_show_lookup_maps,
        _evaluate_imdb_request_context_staleness,
        _load_existing_imdb_cast_photos_for_person,
        _repair_existing_imdb_cast_photos,
    )
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env


REQUEST_CONTEXT_SOURCES = {"request_context", "request_context_inferred", "show_context_request"}


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_imdb_show_context",
        description="Repair stale IMDb request-context show metadata across cast photos.",
    )
    parser.add_argument("--person-id", action="append", default=[], help="Optional person UUID(s) to process.")
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on unique people processed.")
    parser.add_argument(
        "--scan-batch-size",
        type=int,
        default=1000,
        help="Batch size for cast photo scanning when --person-id is omitted.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Defaults to dry-run when omitted.",
    )
    return parser.parse_args(argv)


def _is_request_context_row(row: dict[str, Any]) -> bool:
    raw_metadata = row.get("metadata")
    metadata: dict[str, Any] = raw_metadata if isinstance(raw_metadata, dict) else {}
    source = str(metadata.get("show_context_source") or "").strip().lower()
    return source in REQUEST_CONTEXT_SOURCES


def _list_candidate_person_ids(db: Any, *, limit: int, scan_batch_size: int) -> list[str]:
    person_ids: list[str] = []
    seen: set[str] = set()
    start = 0
    while True:
        response = (
            db.schema("core")
            .table("cast_photos")
            .select("person_id,metadata")
            .eq("source", "imdb")
            .range(start, start + scan_batch_size - 1)
            .execute()
        )
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error scanning cast_photos: {response.error}")
        rows = response.data if isinstance(response.data, list) else []
        if not rows:
            break
        for row in rows:
            if not isinstance(row, dict) or not _is_request_context_row(row):
                continue
            person_id = str(row.get("person_id") or "").strip()
            if not person_id or person_id in seen:
                continue
            seen.add(person_id)
            person_ids.append(person_id)
            if limit > 0 and len(person_ids) >= limit:
                return person_ids
        if len(rows) < scan_batch_size:
            break
        start += scan_batch_size
    return person_ids


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    db = create_supabase_admin_client()

    explicit_person_ids = [str(person_id).strip() for person_id in args.person_id if str(person_id).strip()]
    if explicit_person_ids:
        person_ids = list(dict.fromkeys(explicit_person_ids))
    else:
        person_ids = _list_candidate_person_ids(
            db,
            limit=max(int(args.limit or 0), 0),
            scan_batch_size=max(int(args.scan_batch_size or 1000), 1),
        )

    if args.limit and args.limit > 0:
        person_ids = person_ids[: args.limit]

    if not person_ids:
        print("No IMDb request-context people found.")
        return 0

    mode = "apply" if args.apply else "dry-run"
    print(f"mode={mode} person_count={len(person_ids)}")

    summary = {
        "scanned": 0,
        "repaired": 0,
        "rejected": 0,
        "unchanged": 0,
        "failed": 0,
    }

    show_lookup_by_imdb_id, show_lookup_by_alias, show_lookup_by_id = _build_show_lookup_maps(db)

    for person_id in person_ids:
        rows_before = _load_existing_imdb_cast_photos_for_person(db, person_id)
        request_rows_before = [row for row in rows_before if _is_request_context_row(row)]
        summary["scanned"] += len(request_rows_before)

        stale_before = 0
        for row in request_rows_before:
            is_stale, _ = _evaluate_imdb_request_context_staleness(
                row,
                show_lookup_by_imdb_id=show_lookup_by_imdb_id,
                show_lookup_by_alias=show_lookup_by_alias,
                show_lookup_by_id=show_lookup_by_id,
            )
            if is_stale:
                stale_before += 1

        rejected_delta = 0
        repaired = 0
        failed = 0

        if args.apply:
            repaired, failed = _repair_existing_imdb_cast_photos(
                db,
                person_id,
                show_id=None,
                show_name=None,
            )
            rows_after = _load_existing_imdb_cast_photos_for_person(db, person_id)
            rejected_before = sum(
                1
                for row in rows_before
                if isinstance(row, dict)
                and isinstance(row.get("metadata"), dict)
                and str(row["metadata"].get("show_context_source") or "").strip().lower() == "request_context_rejected"
            )
            rejected_after = sum(
                1
                for row in rows_after
                if isinstance(row, dict)
                and isinstance(row.get("metadata"), dict)
                and str(row["metadata"].get("show_context_source") or "").strip().lower() == "request_context_rejected"
            )
            rejected_delta = max(0, rejected_after - rejected_before)
            summary["repaired"] += int(repaired)
            summary["failed"] += int(failed)
        else:
            rejected_delta = stale_before

        summary["rejected"] += rejected_delta
        unchanged_for_person = max(0, len(request_rows_before) - rejected_delta)
        summary["unchanged"] += unchanged_for_person

        print(
            f"person_id={person_id} scanned={len(request_rows_before)} stale={stale_before} "
            f"repaired={repaired} rejected={rejected_delta} failed={failed} unchanged={unchanged_for_person}"
        )

    print(
        "summary: "
        f"scanned={summary['scanned']} repaired={summary['repaired']} rejected={summary['rejected']} "
        f"unchanged={summary['unchanged']} failed={summary['failed']}"
    )
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
