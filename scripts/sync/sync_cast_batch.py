#!/usr/bin/env python3
"""Batch sync script with rate limiting for cast operations."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from typing import Any, cast

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_cast_batch",
        description="Batch sync cast operations with rate limiting to avoid HTTP 202 errors.",
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
        default=5,
        help="Extra seconds to wait before IMDb API calls (default: 5).",
    )
    parser.add_argument(
        "--skip-cast-photos",
        action="store_true",
        help="Skip cast photo syncs (Fandom/TMDb images).",
    )
    # --verbose is already defined in add_show_filter_args
    return parser.parse_args(argv)


def run_command(cmd: list[str], step_name: str, verbose: bool) -> tuple[bool, str]:
    """Run a subprocess command and return success status."""
    try:
        if verbose:
            print(f"  → {step_name}...")

        result = subprocess.run(
            ["python"] + cmd,
            capture_output=True,
            text=True,
            timeout=300,
            env={**cast(Any, subprocess).os.environ, "PYTHONPATH": "."},
            check=False,
        )

        if result.returncode == 0:
            if verbose:
                print(f"  ✓ {step_name}")
            return True, ""
        else:
            error = result.stderr[:500] if result.stderr else "Unknown error"
            if verbose:
                print(f"  ✗ {step_name}: {error}")
            return False, error

    except subprocess.TimeoutExpired:
        if verbose:
            print(f"  ⏱ {step_name}: timeout")
        return False, "timeout"
    except Exception as exc:
        if verbose:
            print(f"  ✗ {step_name}: {exc}")
        return False, str(exc)


def get_show_cast_person_ids(db, show_id: str) -> list[dict[str, Any]]:
    """Get all person IDs for cast members of a show."""
    response = db.schema("core").table("show_cast").select("person_id").eq("show_id", show_id).execute()

    if hasattr(response, "error") and response.error:
        return []

    return response.data or []


def sync_cast_for_show(
    show: dict[str, Any],
    db,
    *,
    skip_cast_photos: bool,
    imdb_delay: int,
    verbose: bool,
) -> dict[str, Any]:
    """Sync all cast-related data for a single show."""
    show_id = str(show.get("id") or "")
    imdb_id = str(show.get("imdb_id") or "")
    show_name = str(show.get("name") or "")

    if not imdb_id:
        return {
            "show_id": show_id,
            "show_name": show_name,
            "success": False,
            "error": "No IMDb ID",
            "steps": {},
        }

    results = {
        "show_id": show_id,
        "show_name": show_name,
        "imdb_id": imdb_id,
        "success": True,
        "steps": {},
    }

    # ========================================================================
    # STEP 1: Show Cast (IMDb)
    # ========================================================================
    if imdb_delay > 0:
        if verbose:
            print(f"  ⏸  Waiting {imdb_delay}s before show_cast (IMDb rate limit)...")
        time.sleep(imdb_delay)

    success, error = run_command(
        ["scripts/sync/sync_show_cast.py", "--imdb-id", imdb_id] + (["--verbose"] if verbose else []),
        "Show cast (main cast)",
        verbose,
    )
    results["steps"]["show_cast"] = "success" if success else f"failed: {error}"

    # ========================================================================
    # STEP 2: Episode Appearances (IMDb)
    # ========================================================================
    if imdb_delay > 0:
        if verbose:
            print(f"  ⏸  Waiting {imdb_delay}s before episode_appearances (IMDb rate limit)...")
        time.sleep(imdb_delay)

    success, error = run_command(
        ["scripts/sync/sync_episode_appearances.py", "--imdb-id", imdb_id] + (["--verbose"] if verbose else []),
        "Episode appearances (guest cast)",
        verbose,
    )
    results["steps"]["episode_appearances"] = "success" if success else f"failed: {error}"

    # ========================================================================
    # STEP 3: Cast Photos (if not skipped)
    # ========================================================================
    if not skip_cast_photos:
        success, error = run_command(
            ["scripts/sync/sync_cast_photos.py", "--imdb-show-id", imdb_id] + (["--verbose"] if verbose else []),
            "Cast photos (Fandom/TMDb)",
            verbose,
        )
        results["steps"]["cast_photos"] = "success" if success else f"failed: {error}"

        # Get all person IDs for this show's cast
        if verbose:
            print("  → Fetching cast member person IDs...")
        cast_members = get_show_cast_person_ids(db, show_id)

        if cast_members:
            if verbose:
                print(f"  Found {len(cast_members)} cast members")

            # Sync TMDb person images for each cast member
            if verbose:
                print("  → Syncing TMDb person images for all cast...")
            person_ids = [str(c.get("person_id")) for c in cast_members if c.get("person_id")]

            if person_ids:
                # Build command with all person IDs
                cmd = ["scripts/sync/sync_tmdb_person_images.py"]
                for pid in person_ids:
                    cmd.extend(["--person-id", pid])
                if verbose:
                    cmd.append("--verbose")

                success, error = run_command(
                    cmd,
                    f"TMDb person images ({len(person_ids)} people)",
                    verbose,
                )
                results["steps"]["tmdb_person_images"] = "success" if success else f"failed: {error}"
            else:
                if verbose:
                    print("  ⊘ No person IDs found for TMDb sync")
                results["steps"]["tmdb_person_images"] = "skipped"
        else:
            if verbose:
                print("  ⊘ No cast members found yet")
            results["steps"]["tmdb_person_images"] = "skipped"
    else:
        if verbose:
            print("  ⊘ Skipping all cast photos")
        results["steps"]["cast_photos"] = "skipped"
        results["steps"]["tmdb_person_images"] = "skipped"

    return results


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if args.skip_db:
        print("ERROR: --skip-db is not supported for this script (database access required).", file=sys.stderr)
        return 2
    db = load_env_and_db()

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    total = len(show_rows)
    print(f"Processing cast for {total} shows in batches of {args.batch_size}")
    print(f"Batch rest interval: {args.rest_seconds}s")
    print(f"Per-show delay: {args.per_show_delay}s")
    print(f"IMDb API delay: {args.imdb_delay}s")
    print(f"Skip Cast Photos: {args.skip_cast_photos}")
    print("=" * 80)

    all_results: list[dict[str, Any]] = []
    batch_num = 0

    for i in range(0, total, args.batch_size):
        batch = show_rows[i : i + args.batch_size]
        batch_num += 1

        print(f"\nBatch {batch_num}/{(total + args.batch_size - 1) // args.batch_size}")
        print(f"Shows {i + 1}-{min(i + args.batch_size, total)} of {total}")
        print("-" * 80)

        for idx, show in enumerate(batch):
            show_name = str(show.get("name") or "")
            imdb_id = str(show.get("imdb_id") or "")

            print(f"\n[{imdb_id}] {show_name}")

            result = sync_cast_for_show(
                show,
                db,
                skip_cast_photos=args.skip_cast_photos,
                imdb_delay=args.imdb_delay,
                verbose=args.verbose,
            )
            all_results.append(result)

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

    # Show step-level summary
    step_stats: dict[str, dict[str, int]] = {}
    for r in all_results:
        for step, status in r.get("steps", {}).items():
            if step not in step_stats:
                step_stats[step] = {"success": 0, "failed": 0, "skipped": 0}

            if status == "success":
                step_stats[step]["success"] += 1
            elif status == "skipped":
                step_stats[step]["skipped"] += 1
            else:
                step_stats[step]["failed"] += 1

    if step_stats:
        print("\nStep-level summary:")
        for step, stats in step_stats.items():
            print(f"  {step}:")
            print(f"    ✓ {stats['success']} | ✗ {stats['failed']} | ⊘ {stats['skipped']}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
