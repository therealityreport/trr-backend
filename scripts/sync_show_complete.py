#!/usr/bin/env python3
"""Complete sync for a single show - all metadata, cast, and photos."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from typing import Any

from scripts._sync_common import load_env_and_db


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_show_complete",
        description="Complete sync for one show: metadata, seasons, episodes, cast, and all photos.",
    )
    parser.add_argument(
        "--imdb-id",
        required=True,
        help="IMDb series ID (tt...) to sync completely.",
    )
    parser.add_argument(
        "--imdb-delay",
        type=int,
        default=3,
        help="Seconds to wait before IMDb API calls (default: 3).",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Skip all image syncs (faster, metadata only).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
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
            env={**subprocess.os.environ, "PYTHONPATH": "."},
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


def _build_cmd(base: list[str], verbose: bool) -> list[str]:
    cmd = list(base)
    if verbose:
        cmd.append("--verbose")
    return cmd


def get_show_cast_person_ids(db, show_id: str) -> list[dict[str, Any]]:
    """Get all person IDs for cast members of a show."""
    # Get from show_cast
    response = db.schema("core").table("show_cast").select("person_id").eq("show_id", show_id).execute()

    if hasattr(response, "error") and response.error:
        return []

    return response.data or []


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    imdb_id = args.imdb_id

    print("=" * 80)
    print(f"COMPLETE SYNC FOR SHOW: {imdb_id}")
    print("=" * 80)
    print(f"IMDb delay: {args.imdb_delay}s")
    print(f"Skip images: {args.skip_images}")
    print()

    # Get show UUID
    show_response = db.schema("core").table("shows").select("id,name").eq("imdb_id", imdb_id).execute()
    if not show_response.data:
        print(f"ERROR: Show {imdb_id} not found in database.")
        print("Import the show first with: PYTHONPATH=. python scripts/sync_shows.py --imdb-id {imdb_id}")
        return 1

    show = show_response.data[0]
    show_id = str(show.get("id"))
    show_name = str(show.get("name"))

    print(f"Show: {show_name}")
    print(f"Show ID: {show_id}")
    print()

    results = {}

    # ========================================================================
    # PHASE 1: SHOW METADATA & IMAGES
    # ========================================================================
    print("PHASE 1: Show Metadata & Images")
    print("-" * 80)

    if not args.skip_images:
        success, error = run_command(
            _build_cmd(["scripts/sync_show_images.py", "--imdb-id", imdb_id], args.verbose),
            "Show images (posters, backdrops, logos)",
            args.verbose,
        )
        results["show_images"] = success
    else:
        print("  ⊘ Skipping show images")
        results["show_images"] = "skipped"

    # ========================================================================
    # PHASE 2: SEASONS & SEASON IMAGES
    # ========================================================================
    print("\nPHASE 2: Seasons & Season Images")
    print("-" * 80)

    success, error = run_command(
        _build_cmd(["scripts/sync_seasons.py", "--imdb-id", imdb_id], args.verbose),
        "Seasons metadata",
        args.verbose,
    )
    results["seasons"] = success

    if not args.skip_images:
        success, error = run_command(
            _build_cmd(["scripts/sync_season_episode_images.py", "--imdb-id", imdb_id], args.verbose),
            "Season images (posters)",
            args.verbose,
        )
        results["season_images"] = success
    else:
        print("  ⊘ Skipping season images")
        results["season_images"] = "skipped"

    # ========================================================================
    # PHASE 3: EPISODES (IMDb)
    # ========================================================================
    print("\nPHASE 3: Episodes")
    print("-" * 80)

    if args.imdb_delay > 0:
        print(f"  ⏸  Waiting {args.imdb_delay}s before IMDb API call...")
        time.sleep(args.imdb_delay)

    success, error = run_command(
        _build_cmd(["scripts/sync_episodes.py", "--imdb-id", imdb_id], args.verbose),
        "Episodes metadata",
        args.verbose,
    )
    results["episodes"] = success

    # ========================================================================
    # PHASE 4: SHOW CAST (IMDb)
    # ========================================================================
    print("\nPHASE 4: Show Cast")
    print("-" * 80)

    if args.imdb_delay > 0:
        print(f"  ⏸  Waiting {args.imdb_delay}s before IMDb API call...")
        time.sleep(args.imdb_delay)

    success, error = run_command(
        _build_cmd(["scripts/sync_show_cast.py", "--imdb-id", imdb_id], args.verbose),
        "Show cast (main cast)",
        args.verbose,
    )
    results["show_cast"] = success

    # ========================================================================
    # PHASE 5: EPISODE APPEARANCES (IMDb)
    # ========================================================================
    print("\nPHASE 5: Episode Appearances")
    print("-" * 80)

    if args.imdb_delay > 0:
        print(f"  ⏸  Waiting {args.imdb_delay}s before IMDb API call...")
        time.sleep(args.imdb_delay)

    success, error = run_command(
        _build_cmd(["scripts/sync_episode_appearances.py", "--imdb-id", imdb_id], args.verbose),
        "Episode appearances (guest cast)",
        args.verbose,
    )
    results["episode_appearances"] = success

    # ========================================================================
    # PHASE 6: CAST PHOTOS
    # ========================================================================
    if not args.skip_images:
        print("\nPHASE 6: Cast Photos")
        print("-" * 80)

        # Sync cast photos for the show
        success, error = run_command(
            _build_cmd(["scripts/sync_cast_photos.py", "--imdb-show-id", imdb_id], args.verbose),
            "Cast photos (Fandom/TMDb)",
            args.verbose,
        )
        results["cast_photos"] = success

        # Get all person IDs for this show's cast
        print("\n  Fetching cast member person IDs...")
        cast_members = get_show_cast_person_ids(db, show_id)

        if cast_members:
            print(f"  Found {len(cast_members)} cast members")

            # Sync TMDb person images for each cast member
            print("\n  Syncing TMDb person images for all cast...")
            person_ids = [str(c.get("person_id")) for c in cast_members if c.get("person_id")]

            if person_ids:
                # Build command with all person IDs
                cmd = ["scripts/sync_tmdb_person_images.py"]
                for pid in person_ids:
                    cmd.extend(["--person-id", pid])
                if args.verbose:
                    cmd.append("--verbose")

                success, error = run_command(
                    cmd,
                    f"TMDb person images ({len(person_ids)} people)",
                    args.verbose,
                )
                results["tmdb_person_images"] = success
            else:
                print("  ⊘ No person IDs found for TMDb sync")
                results["tmdb_person_images"] = "skipped"
        else:
            print("  ⊘ No cast members found yet - run show_cast sync first")
            results["tmdb_person_images"] = "skipped"
    else:
        print("\nPHASE 6: Cast Photos")
        print("-" * 80)
        print("  ⊘ Skipping all cast photos")
        results["cast_photos"] = "skipped"
        results["tmdb_person_images"] = "skipped"

    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for step, status in results.items():
        if status == "skipped":
            symbol = "⊘"
        elif status is True:
            symbol = "✓"
        else:
            symbol = "✗"
        print(f"  {symbol} {step}: {status}")

    successful = sum(1 for s in results.values() if s is True)
    failed = sum(1 for s in results.values() if s is False)
    skipped = sum(1 for s in results.values() if s == "skipped")

    print()
    print(f"Total steps: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Skipped: {skipped}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
