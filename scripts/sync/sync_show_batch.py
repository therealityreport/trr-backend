#!/usr/bin/env python3
"""Batch sync script with rate limiting for shows."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_show_batch",
        description="Batch sync shows with rate limiting to avoid HTTP 202 errors.",
    )
    add_show_filter_args(parser)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of shows to process before resting (default: 10).",
    )
    parser.add_argument(
        "--rest-seconds",
        type=int,
        default=30,
        help="Seconds to rest between batches (default: 30).",
    )
    parser.add_argument(
        "--per-show-delay",
        type=int,
        default=2,
        help="Seconds to wait between each show within a batch (default: 2).",
    )
    parser.add_argument(
        "--imdb-delay",
        type=int,
        default=3,
        help="Extra seconds to wait before IMDb API calls (default: 3).",
    )
    parser.add_argument(
        "--skip-imdb",
        action="store_true",
        help="Skip IMDb syncs (episodes, cast, appearances) - only do TMDb syncs.",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Skip image syncs (show images, season images, cast photos).",
    )
    return parser.parse_args(argv)


def sync_single_show(
    show: dict[str, Any],
    *,
    skip_imdb: bool,
    skip_images: bool,
    imdb_delay: int,
    verbose: bool,
) -> dict[str, Any]:
    """Sync a single show with all its data."""
    import subprocess

    show_id = str(show.get("id") or "")
    imdb_id = str(show.get("imdb_id") or "")
    show_name = str(show.get("name") or "")

    if not imdb_id:
        return {
            "show_id": show_id,
            "show_name": show_name,
            "success": False,
            "error": "No IMDb ID",
        }

    results = {
        "show_id": show_id,
        "show_name": show_name,
        "imdb_id": imdb_id,
        "success": True,
        "steps": {},
    }

    # Mark which steps use IMDb API (need extra delay)
    steps = [
        ("show_images", ["scripts/sync/sync_show_images.py", "--imdb-id", imdb_id], False),
        ("seasons", ["scripts/sync/sync_seasons.py", "--imdb-id", imdb_id], False),
        ("season_images", ["scripts/sync/sync_season_episode_images.py", "--imdb-id", imdb_id], False),
    ]

    if not skip_imdb:
        steps.extend(
            [
                ("episodes", ["scripts/sync/sync_episodes.py", "--imdb-id", imdb_id], True),  # IMDb
                ("show_cast", ["scripts/sync/sync_show_cast.py", "--imdb-id", imdb_id], True),  # IMDb
                ("episode_appearances", ["scripts/sync/sync_episode_appearances.py", "--imdb-id", imdb_id], True),  # IMDb
            ]
        )

    if not skip_images:
        steps.append(("cast_photos", ["scripts/sync/sync_cast_photos.py", "--imdb-show-id", imdb_id], False))

    for step_name, cmd, uses_imdb in steps:
        if skip_images and "image" in step_name:
            results["steps"][step_name] = "skipped"
            continue

        # Add delay before IMDb API calls to avoid rate limits
        if uses_imdb and imdb_delay > 0:
            if verbose:
                print(f"  ⏸  Waiting {imdb_delay}s before {step_name} (IMDb rate limit)...")
            time.sleep(imdb_delay)

        try:
            env = {"PYTHONPATH": "."}
            result = subprocess.run(
                ["python"] + cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout per step
                env={**subprocess.os.environ, **env},
                check=False,
            )

            if result.returncode == 0:
                results["steps"][step_name] = "success"
                if verbose:
                    print(f"  ✓ {step_name}")
            else:
                results["steps"][step_name] = f"failed (exit {result.returncode})"
                if verbose:
                    print(f"  ✗ {step_name}: {result.stderr[:200]}")
                # Don't mark whole show as failed for individual step failures
                # results["success"] = False

        except subprocess.TimeoutExpired:
            results["steps"][step_name] = "timeout"
            if verbose:
                print(f"  ⏱ {step_name}: timeout")
        except Exception as exc:  # noqa: BLE001
            results["steps"][step_name] = f"error: {exc}"
            if verbose:
                print(f"  ✗ {step_name}: {exc}")

    return results


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if args.skip_db:
        print("ERROR: --skip-db is not supported for this script (database access required).", file=sys.stderr)
        return 2
    db = load_env_and_db(skip_db=args.skip_db)

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    total = len(show_rows)
    print(f"Processing {total} shows in batches of {args.batch_size}")
    print(f"Batch rest interval: {args.rest_seconds}s")
    print(f"Per-show delay: {args.per_show_delay}s")
    print(f"IMDb API delay: {args.imdb_delay}s")
    print(f"Skip IMDb: {args.skip_imdb}")
    print(f"Skip Images: {args.skip_images}")
    print("=" * 80)

    all_results: list[dict[str, Any]] = []
    batch_num = 0

    show_counter = 0
    for i in range(0, total, args.batch_size):
        batch = show_rows[i : i + args.batch_size]
        batch_num += 1

        print(f"\nBatch {batch_num}/{(total + args.batch_size - 1) // args.batch_size}")
        print(f"Shows {i + 1}-{min(i + args.batch_size, total)} of {total}")
        print("-" * 80)

        for idx, show in enumerate(batch):
            show_counter += 1
            show_name = str(show.get("name") or "")
            imdb_id = str(show.get("imdb_id") or "")

            print(f"\n[{show_counter}/{total}] {imdb_id} {show_name}")

            result = sync_single_show(
                show,
                skip_imdb=args.skip_imdb,
                skip_images=args.skip_images,
                imdb_delay=args.imdb_delay,
                verbose=args.verbose,
            )
            all_results.append(result)

            if args.verbose:
                percent = int((show_counter / total) * 100)
                print(f"  → Progress: {show_counter}/{total} ({percent}%)")

            # Add delay between shows within the batch (but not after the last show in batch)
            if args.per_show_delay > 0 and idx < len(batch) - 1:
                if args.verbose:
                    print(f"  ⏸  Waiting {args.per_show_delay}s before next show...")
                time.sleep(args.per_show_delay)

        # Rest between batches (but not after the last batch)
        if i + args.batch_size < total:
            print(f"\n⏸  Resting for {args.rest_seconds}s before next batch...")
            time.sleep(args.rest_seconds)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    successful = sum(1 for r in all_results if r.get("success"))
    failed = len(all_results) - successful

    print(f"Total shows: {len(all_results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")

    if args.verbose and failed > 0:
        print("\nFailed shows:")
        for r in all_results:
            if not r.get("success"):
                print(f"  - {r.get('imdb_id')} {r.get('show_name')}: {r.get('error')}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
