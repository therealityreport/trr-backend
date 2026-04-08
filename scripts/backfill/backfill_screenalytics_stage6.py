#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from scripts._sync_common import load_env_and_db
from trr_backend.pipeline.stages.sync_screenalytics import validate_result_bundle
from trr_backend.repositories import screenalytics_runs


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_screenalytics_stage6",
        description="Validate and optionally ingest historical Screenalytics result bundles into Stage 6 ingest state.",
    )
    parser.add_argument("--show-id", action="append", default=[], help="Limit to one or more TRR show UUIDs.")
    parser.add_argument("--run-id", action="append", default=[], help="Limit to one or more screentime run UUIDs.")
    parser.add_argument(
        "--all-pending",
        action="store_true",
        help="Scan all successful, non-ingested Screenalytics result bundles.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on bundles processed.")
    parser.add_argument("--apply", action="store_true", help="Persist ingest status updates instead of dry-run output.")
    parser.add_argument("--verbose", action="store_true", help="Print per-run status lines.")
    args = parser.parse_args(argv)

    if not args.run_id and not args.show_id and not args.all_pending:
        parser.error("pass --run-id, --show-id, or --all-pending")
    return args


def _trimmed(values: list[str]) -> list[str]:
    return [str(value).strip() for value in values if str(value).strip()]


def _load_bundles(args: argparse.Namespace) -> list[dict[str, Any]]:
    run_ids = _trimmed(args.run_id)
    if run_ids:
        bundles = screenalytics_runs.list_result_bundles(run_ids)
        if args.limit is not None:
            return bundles[: max(0, int(args.limit))]
        return bundles

    if args.all_pending:
        return screenalytics_runs.list_all_result_sync_candidates(limit=args.limit)

    bundles = screenalytics_runs.list_result_sync_candidates(_trimmed(args.show_id))
    if args.limit is not None:
        return bundles[: max(0, int(args.limit))]
    return bundles


def _build_summary(*, dry_run: bool, synced: list[str], failed: list[dict[str, str]], skipped: int) -> dict[str, Any]:
    return {
        "dry_run": dry_run,
        "ingested_run_ids": synced,
        "failed_runs": failed,
        "skipped_missing_run_id": skipped,
        "processed_count": len(synced),
        "failed_count": len(failed),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env_and_db()

    bundles = _load_bundles(args)
    if not bundles:
        print(
            json.dumps(
                {"dry_run": not args.apply, "processed_count": 0, "failed_count": 0},
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    synced_run_ids: list[str] = []
    failed_runs: list[dict[str, str]] = []
    skipped_missing_run_id = 0

    for bundle in bundles:
        run = bundle.get("run") if isinstance(bundle, dict) else {}
        run_id = str((run or {}).get("id") or "").strip()
        if not run_id:
            skipped_missing_run_id += 1
            continue

        validation_error = validate_result_bundle(bundle)
        if validation_error:
            failed_runs.append({"run_id": run_id, "error": validation_error})
            if args.apply:
                screenalytics_runs.mark_result_ingest_status(run_id, status="failed", error=validation_error)
            if args.verbose:
                print(f"FAILED run_id={run_id} error={validation_error}")
            continue

        synced_run_ids.append(run_id)
        if args.apply:
            screenalytics_runs.mark_result_ingest_status(run_id, status="ingested")
        if args.verbose:
            verb = "INGESTED" if args.apply else "READY"
            print(f"{verb} run_id={run_id}")

    print(
        json.dumps(
            _build_summary(
                dry_run=not args.apply,
                synced=synced_run_ids,
                failed=failed_runs,
                skipped=skipped_missing_run_id,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if failed_runs else 0


if __name__ == "__main__":
    raise SystemExit(main())
