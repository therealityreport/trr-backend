#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from api.routers import admin_person_images  # noqa: E402
from trr_backend.db.admin import create_supabase_admin_client  # noqa: E402
from trr_backend.utils.env import load_env  # noqa: E402

DEFAULT_BATCH_SIZE = 50
DEFAULT_PAGE_SIZE = 1000
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_PROGRESS_FILE = "scripts/backfill_imdb_metadata.progress.jsonl"


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_imdb_metadata",
        description=(
            "Backfill IMDb metadata fixes across core.cast_photos using the same repair logic as "
            "the admin Refresh/Reprocess pipeline."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Inspect and report rows needing repair without writing."
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of person_ids to process.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of person_ids per logical batch (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Person-id fetch page size from Supabase (default: {DEFAULT_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--progress-file",
        type=str,
        default=DEFAULT_PROGRESS_FILE,
        help=f"JSONL progress log path (default: {DEFAULT_PROGRESS_FILE}).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from --progress-file by skipping person_ids already marked completed.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Max retries per person on repair failure (default: {DEFAULT_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--backoff-base-seconds",
        type=float,
        default=DEFAULT_BACKOFF_BASE_SECONDS,
        help=("Base seconds for exponential backoff between retries. Delay uses base * 2^(attempt-1) plus jitter."),
    )
    parser.add_argument(
        "--person-id",
        action="append",
        default=[],
        help="Target one or more specific person IDs (repeat flag to add multiple).",
    )
    parser.add_argument(
        "--person-name",
        action="append",
        default=[],
        help="Target one or more exact core.people.full_name values (repeat flag to add multiple).",
    )
    parser.add_argument("--verbose", action="store_true", help="Emit per-person logging.")
    return parser.parse_args(argv)


def _append_progress(progress_file: Path, payload: dict[str, Any]) -> None:
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    with progress_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True))
        handle.write("\n")


def _load_completed_person_ids(progress_file: Path) -> set[str]:
    if not progress_file.exists():
        return set()
    completed: set[str] = set()
    with progress_file.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            person_id = str(payload.get("person_id") or "").strip()
            status = str(payload.get("status") or "").strip().lower()
            if person_id and status in {"completed", "completed_dry_run"}:
                completed.add(person_id)
    return completed


def _fetch_imdb_person_ids(
    db: Any,
    *,
    page_size: int,
    limit: int | None,
) -> list[str]:
    seen: set[str] = set()
    person_ids: list[str] = []
    offset = 0
    while True:
        query = (
            db.schema("core")
            .table("cast_photos")
            .select("person_id")
            .eq("source", "imdb")
            .not_.is_("person_id", "null")
            .range(offset, offset + page_size - 1)
        )
        response = query.execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing IMDb cast_photos person_id rows: {response.error}")
        rows = response.data if isinstance(getattr(response, "data", None), list) else []
        if not rows:
            break

        for row in rows:
            person_id = str(row.get("person_id") or "").strip() if isinstance(row, dict) else ""
            if not person_id or person_id in seen:
                continue
            seen.add(person_id)
            person_ids.append(person_id)
            if limit is not None and len(person_ids) >= limit:
                return person_ids
        offset += page_size
    return person_ids


def _resolve_person_ids_by_name(
    db: Any,
    *,
    full_names: list[str],
) -> tuple[list[str], list[str]]:
    resolved: list[str] = []
    missing: list[str] = []
    for raw_name in full_names:
        full_name = str(raw_name or "").strip()
        if not full_name:
            continue
        response = (
            db.schema("core").table("people").select("id,full_name").eq("full_name", full_name).limit(1).execute()
        )
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error resolving person-name '{full_name}': {response.error}")
        rows = response.data if isinstance(getattr(response, "data", None), list) else []
        if not rows:
            missing.append(full_name)
            continue
        person_id = str(rows[0].get("id") or "").strip()
        if person_id:
            resolved.append(person_id)
        else:
            missing.append(full_name)
    return resolved, missing


def _iter_batches(values: list[str], *, batch_size: int) -> list[list[str]]:
    return [values[idx : idx + batch_size] for idx in range(0, len(values), batch_size)]


def _repair_person_with_retry(
    db: Any,
    *,
    person_id: str,
    max_retries: int,
    backoff_base_seconds: float,
) -> tuple[int, int, int]:
    attempts = 0
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        attempts = attempt
        try:
            repaired, failed = admin_person_images._repair_existing_imdb_cast_photos(
                db,
                person_id,
                show_id=None,
                show_name=None,
                strict_context=None,
                wwhl_credit_episode_imdb_ids=None,
                progress_cb=None,
            )
            return repaired, failed, attempts
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max_retries:
                break
            backoff_seconds = max(0.0, float(backoff_base_seconds)) * (2 ** (attempt - 1))
            jitter = random.uniform(0.0, 0.3)
            time.sleep(backoff_seconds + jitter)
    assert last_error is not None
    raise last_error


def _dry_run_person(
    db: Any,
    *,
    person_id: str,
    show_lookup_by_imdb_id: dict[str, dict[str, Any]],
    show_lookup_by_alias: dict[str, dict[str, Any]],
    show_lookup_by_id: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    rows = admin_person_images._load_existing_imdb_cast_photos_for_person(db, person_id)
    if not rows:
        return 0, 0
    needs = [
        row
        for row in rows
        if admin_person_images._needs_imdb_metadata_refresh_with_show_lookup(
            row,
            show_lookup_by_imdb_id=show_lookup_by_imdb_id,
            show_lookup_by_alias=show_lookup_by_alias,
            show_lookup_by_id=show_lookup_by_id,
        )
    ]
    return len(rows), len(needs)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    db = create_supabase_admin_client()

    progress_file = Path(args.progress_file)
    completed_person_ids = _load_completed_person_ids(progress_file) if args.resume else set()

    target_person_ids: list[str] = []
    for raw_person_id in args.person_id:
        person_id = str(raw_person_id or "").strip()
        if person_id:
            target_person_ids.append(person_id)

    target_person_name_values = [str(value or "").strip() for value in args.person_name if str(value or "").strip()]
    if target_person_name_values:
        resolved_person_ids, missing_person_names = _resolve_person_ids_by_name(
            db,
            full_names=target_person_name_values,
        )
        target_person_ids.extend(resolved_person_ids)
        if missing_person_names:
            print(f"Unable to resolve person-name values: {', '.join(missing_person_names)}")
            return 1

    if target_person_ids:
        person_ids = list(dict.fromkeys(target_person_ids))
    else:
        person_ids = _fetch_imdb_person_ids(
            db,
            page_size=max(1, int(args.page_size)),
            limit=max(1, int(args.limit)) if args.limit is not None else None,
        )
    if completed_person_ids:
        person_ids = [person_id for person_id in person_ids if person_id not in completed_person_ids]

    if args.limit is not None:
        person_ids = person_ids[: max(1, int(args.limit))]
    if not person_ids:
        print("No IMDb person_ids to process.")
        return 0

    batches = _iter_batches(person_ids, batch_size=max(1, int(args.batch_size)))
    started_at = time.time()
    totals: dict[str, int] = {
        "processed": 0,
        "completed": 0,
        "failed": 0,
        "rows_seen": 0,
        "rows_needing_repair": 0,
        "rows_repaired": 0,
        "repair_failures_reported": 0,
    }

    dry_run_lookups: tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]] | None = (
        None
    )
    if args.dry_run:
        dry_run_lookups = admin_person_images._build_show_lookup_maps(db)

    print(
        f"Starting IMDb metadata backfill: people={len(person_ids)} batches={len(batches)} "
        f"batch_size={max(1, int(args.batch_size))} dry_run={bool(args.dry_run)}"
    )

    for batch_index, batch in enumerate(batches, start=1):
        batch_started_at = time.time()
        batch_processed = 0
        batch_failed = 0
        batch_repaired = 0
        batch_needing_repair = 0

        for person_id in batch:
            totals["processed"] += 1
            batch_processed += 1
            now_iso = datetime.now(UTC).isoformat()
            try:
                if args.dry_run:
                    assert dry_run_lookups is not None
                    rows_seen, rows_needing_repair = _dry_run_person(
                        db,
                        person_id=person_id,
                        show_lookup_by_imdb_id=dry_run_lookups[0],
                        show_lookup_by_alias=dry_run_lookups[1],
                        show_lookup_by_id=dry_run_lookups[2],
                    )
                    totals["rows_seen"] += rows_seen
                    totals["rows_needing_repair"] += rows_needing_repair
                    batch_needing_repair += rows_needing_repair
                    totals["completed"] += 1
                    _append_progress(
                        progress_file,
                        {
                            "ts": now_iso,
                            "person_id": person_id,
                            "status": "completed_dry_run",
                            "rows_seen": rows_seen,
                            "rows_needing_repair": rows_needing_repair,
                        },
                    )
                    if args.verbose:
                        print(
                            f"[{batch_index}/{len(batches)}] dry-run person_id={person_id} "
                            f"rows={rows_seen} needing_repair={rows_needing_repair}"
                        )
                    continue

                repaired, failed, attempts = _repair_person_with_retry(
                    db,
                    person_id=person_id,
                    max_retries=max(1, int(args.max_retries)),
                    backoff_base_seconds=max(0.0, float(args.backoff_base_seconds)),
                )
                totals["rows_repaired"] += int(repaired)
                totals["repair_failures_reported"] += int(failed)
                totals["completed"] += 1
                batch_repaired += int(repaired)
                _append_progress(
                    progress_file,
                    {
                        "ts": now_iso,
                        "person_id": person_id,
                        "status": "completed",
                        "rows_repaired": int(repaired),
                        "repair_failures_reported": int(failed),
                        "attempts": attempts,
                    },
                )
                if args.verbose:
                    print(
                        f"[{batch_index}/{len(batches)}] repaired person_id={person_id} "
                        f"rows_repaired={repaired} failures={failed} attempts={attempts}"
                    )
            except Exception as exc:  # noqa: BLE001
                totals["failed"] += 1
                batch_failed += 1
                _append_progress(
                    progress_file,
                    {
                        "ts": now_iso,
                        "person_id": person_id,
                        "status": "failed",
                        "error": str(exc),
                    },
                )
                print(f"[{batch_index}/{len(batches)}] failed person_id={person_id}: {exc}")

        batch_elapsed = round(time.time() - batch_started_at, 2)
        if args.dry_run:
            print(
                f"Batch {batch_index}/{len(batches)} complete: processed={batch_processed} "
                f"need_repair={batch_needing_repair} failed={batch_failed} elapsed_s={batch_elapsed}"
            )
        else:
            print(
                f"Batch {batch_index}/{len(batches)} complete: processed={batch_processed} "
                f"repaired_rows={batch_repaired} failed={batch_failed} elapsed_s={batch_elapsed}"
            )

    elapsed = round(time.time() - started_at, 2)
    print("\nSummary")
    print(f"processed={totals['processed']}")
    print(f"completed={totals['completed']}")
    print(f"failed={totals['failed']}")
    if args.dry_run:
        print(f"rows_seen={totals['rows_seen']}")
        print(f"rows_needing_repair={totals['rows_needing_repair']}")
    else:
        print(f"rows_repaired={totals['rows_repaired']}")
        print(f"repair_failures_reported={totals['repair_failures_reported']}")
    print(f"elapsed_s={elapsed}")
    print(f"progress_file={progress_file}")
    return 0 if totals["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
