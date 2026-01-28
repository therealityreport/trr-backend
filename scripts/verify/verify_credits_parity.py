#!/usr/bin/env python3
"""Verify parity between legacy credits tables and v2 credits views.

This script compares:
1. core.show_cast vs core.v_show_cast_from_credits
2. core.episode_appearances vs core.v_episode_appearances_from_credits

Use this before enabling ENABLE_CREDITS_V2_READ to verify data parity.

Usage:
    # Full verification (all shows)
    PYTHONPATH=. python scripts/verify/verify_credits_parity.py [--verbose]

    # Verify specific show
    PYTHONPATH=. python scripts/verify/verify_credits_parity.py --show-id <uuid>

    # Verify limited number of shows (random sampling)
    PYTHONPATH=. python scripts/verify/verify_credits_parity.py --limit 10

    # Spot-check random people for episode appearances
    PYTHONPATH=. python scripts/verify/verify_credits_parity.py --spot-check 10

Prerequisites:
    - Local Supabase running (`supabase start`) or hosted Supabase
    - Migrations 0065 + 0066 applied
    - Backfill completed (`scripts/backfill_credits.py`)
"""

from __future__ import annotations

import argparse
import random
import sys

from dotenv import load_dotenv


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="verify_credits_parity",
        description="Verify parity between legacy credits tables and v2 views.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Print detailed output")
    parser.add_argument(
        "--show-id",
        type=str,
        help="Verify parity for a specific show UUID only",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit verification to N shows (random sample)",
    )
    parser.add_argument(
        "--spot-check",
        type=int,
        default=10,
        help="Number of random people to spot-check for episode appearances (default: 10)",
    )
    return parser.parse_args(argv if argv is not None else sys.argv[1:])


def compare_show_cast_for_show(db, show_id: str, verbose: bool = False) -> bool:
    """Compare show_cast data for a specific show."""
    legacy_rows = (
        db.schema("core")
        .table("show_cast")
        .select("person_id,billing_order,role,credit_category")
        .eq("show_id", show_id)
        .order("billing_order")
        .execute()
    )

    v2_rows = (
        db.schema("core")
        .table("v_show_cast_from_credits")
        .select("person_id,billing_order,role,credit_category")
        .eq("show_id", show_id)
        .order("billing_order")
        .execute()
    )

    legacy_set = {
        (r["person_id"], r.get("billing_order"), r.get("role"), r.get("credit_category"))
        for r in (legacy_rows.data or [])
    }
    v2_set = {
        (r["person_id"], r.get("billing_order"), r.get("role"), r.get("credit_category")) for r in (v2_rows.data or [])
    }

    match = legacy_set == v2_set

    if not match and verbose:
        print(f"      Show {show_id}: MISMATCH")
        only_legacy = legacy_set - v2_set
        only_v2 = v2_set - legacy_set
        if only_legacy:
            print(f"         Only in legacy ({len(only_legacy)}):")
            for item in list(only_legacy)[:3]:
                print(f"            {item}")
        if only_v2:
            print(f"         Only in v2 ({len(only_v2)}):")
            for item in list(only_v2)[:3]:
                print(f"            {item}")

    return match


def compare_episode_appearances_for_person(db, show_id: str, person_id: str, verbose: bool = False) -> bool:
    """Compare episode_appearances data for a specific person in a show."""
    legacy_row = (
        db.schema("core")
        .table("episode_appearances")
        .select("total_episodes,seasons,imdb_episode_title_ids")
        .eq("show_id", show_id)
        .eq("person_id", person_id)
        .maybe_single()
        .execute()
    )

    v2_row = (
        db.schema("core")
        .table("v_episode_appearances_from_credits")
        .select("total_episodes,seasons,imdb_episode_title_ids")
        .eq("show_id", show_id)
        .eq("person_id", person_id)
        .maybe_single()
        .execute()
    )

    legacy_data = legacy_row.data if legacy_row.data else {}
    v2_data = v2_row.data if v2_row.data else {}

    # Both missing is OK (person not in either)
    if not legacy_data and not v2_data:
        return True

    # Compare total_episodes
    legacy_total = legacy_data.get("total_episodes")
    v2_total = v2_data.get("total_episodes")
    total_match = legacy_total == v2_total

    # Compare seasons (as sets since order may vary)
    legacy_seasons = set(legacy_data.get("seasons") or [])
    v2_seasons = set(v2_data.get("seasons") or [])
    seasons_match = legacy_seasons == v2_seasons

    # Compare episode IDs (as sets since order may vary)
    legacy_eps = set(legacy_data.get("imdb_episode_title_ids") or [])
    v2_eps = set(v2_data.get("imdb_episode_title_ids") or [])
    eps_match = legacy_eps == v2_eps

    match = total_match and seasons_match and eps_match

    if not match and verbose:
        print(f"      Person {person_id} in show {show_id}: MISMATCH")
        if not total_match:
            print(f"         total_episodes: legacy={legacy_total}, v2={v2_total}")
        if not seasons_match:
            print(f"         seasons: legacy={sorted(legacy_seasons)}, v2={sorted(v2_seasons)}")
        if not eps_match:
            print(f"         episode_ids: legacy={len(legacy_eps)}, v2={len(v2_eps)}")

    return match


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    load_dotenv()

    from trr_backend.db import create_supabase_admin_client

    db = create_supabase_admin_client()

    print("=" * 60)
    print("Credits Parity Verification")
    if args.show_id:
        print(f"Mode: Single show ({args.show_id})")
    elif args.limit:
        print(f"Mode: Random sample ({args.limit} shows)")
    else:
        print("Mode: Full verification")
    print("=" * 60)

    # Get shows to verify
    if args.show_id:
        show_ids = [args.show_id]
    else:
        # Get all shows with cast data
        shows_response = db.schema("core").table("show_cast").select("show_id").execute()
        all_show_ids = list({r["show_id"] for r in (shows_response.data or [])})

        if args.limit and len(all_show_ids) > args.limit:
            show_ids = random.sample(all_show_ids, args.limit)
            print(f"   Sampled {args.limit} shows from {len(all_show_ids)} total")
        else:
            show_ids = all_show_ids

    # 1. Compare show_cast counts (global or per-show)
    print("\n1. Show Cast Row Counts")
    print("-" * 40)

    if args.show_id:
        legacy_count_resp = (
            db.schema("core").table("show_cast").select("*", count="exact").eq("show_id", args.show_id).execute()
        )
        v2_count_resp = (
            db.schema("core")
            .table("v_show_cast_from_credits")
            .select("*", count="exact")
            .eq("show_id", args.show_id)
            .execute()
        )
    else:
        legacy_count_resp = db.schema("core").table("show_cast").select("*", count="exact").execute()
        v2_count_resp = db.schema("core").table("v_show_cast_from_credits").select("*", count="exact").execute()

    legacy_count = legacy_count_resp.count or 0
    v2_count = v2_count_resp.count or 0
    print(f"   core.show_cast: {legacy_count} rows")
    print(f"   v_show_cast_from_credits: {v2_count} rows")

    show_cast_match = legacy_count == v2_count
    print(f"   Match: {'YES' if show_cast_match else 'NO'}")

    # 2. Compare episode_appearances counts (global or per-show)
    print("\n2. Episode Appearances Row Counts")
    print("-" * 40)

    if args.show_id:
        legacy_ep_resp = (
            db.schema("core")
            .table("episode_appearances")
            .select("*", count="exact")
            .eq("show_id", args.show_id)
            .execute()
        )
        v2_ep_resp = (
            db.schema("core")
            .table("v_episode_appearances_from_credits")
            .select("*", count="exact")
            .eq("show_id", args.show_id)
            .execute()
        )
    else:
        legacy_ep_resp = db.schema("core").table("episode_appearances").select("*", count="exact").execute()
        v2_ep_resp = db.schema("core").table("v_episode_appearances_from_credits").select("*", count="exact").execute()

    legacy_ep_count = legacy_ep_resp.count or 0
    v2_ep_count = v2_ep_resp.count or 0
    print(f"   core.episode_appearances: {legacy_ep_count} rows")
    print(f"   v_episode_appearances_from_credits: {v2_ep_count} rows")

    ep_match = legacy_ep_count == v2_ep_count
    print(f"   Match: {'YES' if ep_match else 'NO'}")

    # 3. Show-level cast comparison
    print("\n3. Show Cast Data Comparison")
    print("-" * 40)

    show_cast_data_match = True
    shows_checked = 0
    shows_mismatched = 0

    for show_id in show_ids:
        match = compare_show_cast_for_show(db, show_id, verbose=args.verbose)
        shows_checked += 1
        if not match:
            shows_mismatched += 1
            show_cast_data_match = False
            if not args.verbose:
                print(f"   Show {show_id}: MISMATCH")

    print(f"   Shows checked: {shows_checked}")
    print(f"   Shows mismatched: {shows_mismatched}")
    print(f"   Match: {'YES' if show_cast_data_match else 'NO'}")

    # 4. Spot-check random people for episode appearances
    print("\n4. Episode Appearances Spot-Check")
    print("-" * 40)

    ep_spot_match = True

    if legacy_ep_count > 0:
        # Get random people to spot-check
        if args.show_id:
            people_resp = (
                db.schema("core")
                .table("episode_appearances")
                .select("show_id,person_id")
                .eq("show_id", args.show_id)
                .execute()
            )
        else:
            people_resp = db.schema("core").table("episode_appearances").select("show_id,person_id").execute()

        all_people = [(r["show_id"], r["person_id"]) for r in (people_resp.data or [])]

        spot_check_count = min(args.spot_check, len(all_people))
        if spot_check_count < len(all_people):
            people_to_check = random.sample(all_people, spot_check_count)
        else:
            people_to_check = all_people

        people_checked = 0
        people_mismatched = 0

        for show_id, person_id in people_to_check:
            match = compare_episode_appearances_for_person(db, show_id, person_id, verbose=args.verbose)
            people_checked += 1
            if not match:
                people_mismatched += 1
                ep_spot_match = False
                if not args.verbose:
                    print(f"   Person {person_id} in show {show_id}: MISMATCH")

        print(f"   People spot-checked: {people_checked}")
        print(f"   People mismatched: {people_mismatched}")
        print(f"   Match: {'YES' if ep_spot_match else 'NO'}")
    else:
        print("   No episode_appearances rows to spot-check")

    # 5. Credits table counts
    print("\n5. V2 Credits Tables")
    print("-" * 40)

    if args.show_id:
        credits_resp = (
            db.schema("core").table("credits").select("*", count="exact").eq("show_id", args.show_id).execute()
        )
        credits_count = credits_resp.count or 0

        # Get credit IDs for this show to count occurrences
        credit_ids_resp = db.schema("core").table("credits").select("id").eq("show_id", args.show_id).execute()
        credit_ids = [r["id"] for r in (credit_ids_resp.data or [])]
        if credit_ids:
            occurrences_resp = (
                db.schema("core")
                .table("credit_occurrences")
                .select("*", count="exact")
                .in_("credit_id", credit_ids)
                .execute()
            )
            occurrences_count = occurrences_resp.count or 0
        else:
            occurrences_count = 0
    else:
        credits_resp = db.schema("core").table("credits").select("*", count="exact").execute()
        credits_count = credits_resp.count or 0

        occurrences_resp = db.schema("core").table("credit_occurrences").select("*", count="exact").execute()
        occurrences_count = occurrences_resp.count or 0

    print(f"   core.credits: {credits_count} rows")
    print(f"   core.credit_occurrences: {occurrences_count} rows")

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    all_pass = show_cast_match and ep_match and show_cast_data_match and ep_spot_match

    if all_pass:
        print("\nParity verification PASSED")
        print("Safe to enable ENABLE_CREDITS_V2_READ=1")
        return 0
    else:
        print("\nParity verification FAILED")
        print("Do NOT enable ENABLE_CREDITS_V2_READ until issues are resolved")
        if not show_cast_match:
            print("  - show_cast count mismatch")
        if not ep_match:
            print("  - episode_appearances count mismatch")
        if not show_cast_data_match:
            print("  - show_cast data mismatch in one or more shows")
        if not ep_spot_match:
            print("  - episode_appearances spot-check failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
