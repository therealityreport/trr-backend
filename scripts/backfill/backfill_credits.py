#!/usr/bin/env python3
"""
Backfill script for Phase 5 credits consolidation.

Populates core.credits and core.credit_occurrences from legacy tables:
- Pass A: show_cast → credits
- Pass B: episode_appearances (arrays) → credit_occurrences (resolved via episodes)

This script is idempotent - safe to run multiple times.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from scripts._sync_common import load_env_and_db
from trr_backend.db.session import DbSession
from trr_backend.repositories.credits import (
    assert_core_credit_occurrences_table_exists,
    assert_core_credits_table_exists,
    insert_credit_occurrences_ignore_conflicts,
    insert_credits_ignore_conflicts,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_credits",
        description="Backfill core.credits and core.credit_occurrences from legacy show_cast/episode_appearances.",
    )
    parser.add_argument(
        "--pass",
        dest="passes",
        choices=["A", "B", "all"],
        default="all",
        help="Which pass to run: A (show_cast→credits), B (episode_appearances→occurrences), or all.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of rows to process per pass.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to database.")
    parser.add_argument(
        "--show-id",
        default=None,
        help="Only backfill for a specific show_id (UUID).",
    )
    return parser.parse_args(argv)


def _fetch_show_cast(db: DbSession, *, limit: int | None, show_id: str | None) -> list[dict[str, Any]]:
    """Fetch show_cast rows to backfill into credits."""
    fields = "id,show_id,person_id,billing_order,role,credit_category,source_type,created_at,updated_at"
    query = db.schema("core").table("show_cast").select(fields)
    if show_id:
        query = query.eq("show_id", show_id)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.show_cast: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _fetch_episode_appearances(db: DbSession, *, limit: int | None, show_id: str | None) -> list[dict[str, Any]]:
    """Fetch episode_appearances rows to backfill into credit_occurrences."""
    fields = "id,show_id,person_id,imdb_episode_title_ids,tmdb_episode_ids"
    query = db.schema("core").table("episode_appearances").select(fields)
    if show_id:
        query = query.eq("show_id", show_id)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.episode_appearances: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _fetch_episodes_by_imdb_ids(db: DbSession, imdb_ids: list[str]) -> dict[str, str]:
    """Fetch episode UUIDs by IMDB episode IDs. Returns {imdb_id: episode_uuid}."""
    if not imdb_ids:
        return {}

    # Supabase .in_() has limits, so batch if needed
    result: dict[str, str] = {}
    batch_size = 500
    for i in range(0, len(imdb_ids), batch_size):
        batch = imdb_ids[i : i + batch_size]
        response = (
            db.schema("core").table("episodes").select("id,imdb_episode_id").in_("imdb_episode_id", batch).execute()
        )
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error fetching episodes: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            for row in data:
                imdb_id = row.get("imdb_episode_id")
                ep_id = row.get("id")
                if imdb_id and ep_id:
                    result[imdb_id] = ep_id
    return result


def _fetch_credits_lookup(db: DbSession, show_id: str) -> dict[tuple[str, str, str, str, str], str]:
    """
    Fetch existing credits for a show and build lookup by unique key.
    Returns {(show_id, person_id, credit_category, role_or_empty, source_type): credit_id}
    """
    response = (
        db.schema("core")
        .table("credits")
        .select("id,show_id,person_id,credit_category,role,source_type")
        .eq("show_id", show_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error fetching credits: {response.error}")
    data = response.data or []
    result: dict[tuple[str, str, str, str, str], str] = {}
    if isinstance(data, list):
        for row in data:
            key = (
                str(row.get("show_id") or ""),
                str(row.get("person_id") or ""),
                str(row.get("credit_category") or ""),
                str(row.get("role") or ""),
                str(row.get("source_type") or ""),
            )
            result[key] = str(row.get("id") or "")
    return result


def _pass_a_show_cast_to_credits(
    db: DbSession,
    *,
    limit: int | None,
    show_id: str | None,
    verbose: bool,
    dry_run: bool,
) -> dict[str, int]:
    """Pass A: Backfill show_cast → credits."""
    stats = {"show_cast_rows": 0, "credits_inserted": 0, "credits_skipped": 0}

    show_cast_rows = _fetch_show_cast(db, limit=limit, show_id=show_id)
    stats["show_cast_rows"] = len(show_cast_rows)

    if verbose:
        print(f"Pass A: Found {len(show_cast_rows)} show_cast rows to backfill")

    if not show_cast_rows:
        return stats

    # Transform show_cast rows to credits format
    credit_rows: list[dict[str, Any]] = []
    for row in show_cast_rows:
        credit_rows.append(
            {
                "show_id": row.get("show_id"),
                "person_id": row.get("person_id"),
                "credit_category": row.get("credit_category") or "Self",
                "role": row.get("role"),
                "billing_order": row.get("billing_order"),
                "source_type": row.get("source_type") or "fullcredits_html",
                "metadata": {},
            }
        )

    if dry_run:
        stats["credits_inserted"] = len(credit_rows)
        if verbose:
            print(f"Pass A (dry-run): Would insert {len(credit_rows)} credits")
        return stats

    # Insert with conflict handling (idempotent)
    inserted = insert_credits_ignore_conflicts(db, credit_rows)
    stats["credits_inserted"] = len(inserted)
    stats["credits_skipped"] = len(credit_rows) - len(inserted)

    if verbose:
        print(f"Pass A: Inserted {len(inserted)} credits, skipped {stats['credits_skipped']} (already exist)")

    return stats


def _pass_b_episode_appearances_to_occurrences(
    db: DbSession,
    *,
    limit: int | None,
    show_id: str | None,
    verbose: bool,
    dry_run: bool,
) -> dict[str, int]:
    """Pass B: Backfill episode_appearances → credit_occurrences.

    Deterministic mapping strategy:
    - Uses credit with: credit_category='Self', role=NULL, source_type='fullcredits_html'
    - If no exact match, falls back to any credit for this person/show with role=NULL
    - If still no match, falls back to any credit for this person/show
    - Logs when ambiguous matches are used
    """
    stats = {
        "ea_rows": 0,
        "imdb_ids_total": 0,
        "imdb_ids_resolved": 0,
        "imdb_ids_unresolved": 0,
        "occurrences_inserted": 0,
        "occurrences_skipped": 0,
        "credits_not_found": 0,
        "credits_ambiguous_match": 0,
    }

    ea_rows = _fetch_episode_appearances(db, limit=limit, show_id=show_id)
    stats["ea_rows"] = len(ea_rows)

    if verbose:
        print(f"Pass B: Found {len(ea_rows)} episode_appearances rows to backfill")

    if not ea_rows:
        return stats

    # Collect all unique IMDB episode IDs
    all_imdb_ids: set[str] = set()
    for row in ea_rows:
        imdb_ids = row.get("imdb_episode_title_ids") or []
        if isinstance(imdb_ids, list):
            all_imdb_ids.update(imdb_ids)

    stats["imdb_ids_total"] = len(all_imdb_ids)

    if verbose:
        print(f"Pass B: Found {len(all_imdb_ids)} unique IMDB episode IDs to resolve")

    # Build episode lookup: imdb_id → episode_uuid
    episode_lookup = _fetch_episodes_by_imdb_ids(db, list(all_imdb_ids))
    stats["imdb_ids_resolved"] = len(episode_lookup)
    stats["imdb_ids_unresolved"] = len(all_imdb_ids) - len(episode_lookup)

    # Collect unresolved IMDB IDs for logging
    unresolved_imdb_ids = all_imdb_ids - set(episode_lookup.keys())

    if verbose:
        print(f"Pass B: Resolved {len(episode_lookup)} episodes, {stats['imdb_ids_unresolved']} unresolved")
        if unresolved_imdb_ids:
            sample = sorted(unresolved_imdb_ids)[:10]
            print(f"  Sample unresolved IMDB IDs: {sample}")
            if len(unresolved_imdb_ids) > 10:
                print(f"  ... and {len(unresolved_imdb_ids) - 10} more")

    # Build occurrence rows
    occurrence_rows: list[dict[str, Any]] = []
    credits_cache: dict[str, dict[tuple[str, str, str, str, str], str]] = {}

    for row in ea_rows:
        row_show_id = str(row.get("show_id") or "")
        person_id = str(row.get("person_id") or "")
        imdb_ids = row.get("imdb_episode_title_ids") or []

        if not row_show_id or not person_id:
            continue

        # Get or fetch credits lookup for this show
        if row_show_id not in credits_cache:
            credits_cache[row_show_id] = _fetch_credits_lookup(db, row_show_id)

        credits_lookup = credits_cache[row_show_id]

        # Deterministic credit matching strategy:
        # 1. Exact match: Self, no role, fullcredits_html source
        # 2. Fallback: any credit with no role for this person/show
        # 3. Last resort: any credit for this person/show
        credit_id = None
        match_type = None

        # Priority 1: Exact match (Self, no role, fullcredits_html)
        exact_key = (row_show_id, person_id, "Self", "", "fullcredits_html")
        if exact_key in credits_lookup:
            credit_id = credits_lookup[exact_key]
            match_type = "exact"

        # Priority 2: Any credit with empty role
        if not credit_id:
            for (sid, pid, _cat, role, _src), cid in credits_lookup.items():
                if sid == row_show_id and pid == person_id and not role:
                    credit_id = cid
                    match_type = "empty_role"
                    break

        # Priority 3: Any credit for this person/show (ambiguous)
        if not credit_id:
            for (sid, pid, _cat, _role, _src), cid in credits_lookup.items():
                if sid == row_show_id and pid == person_id:
                    credit_id = cid
                    match_type = "ambiguous"
                    stats["credits_ambiguous_match"] += 1
                    break

        if not credit_id:
            stats["credits_not_found"] += 1
            if verbose and stats["credits_not_found"] <= 10:
                print(f"  WARN: No credit found for show_id={row_show_id}, person_id={person_id}")
            continue

        if verbose and match_type == "ambiguous" and stats["credits_ambiguous_match"] <= 5:
            print(f"  INFO: Ambiguous credit match for show_id={row_show_id}, person_id={person_id}")

        # Create occurrence rows for each resolved episode
        for imdb_id in imdb_ids:
            episode_id = episode_lookup.get(imdb_id)
            if episode_id:
                occurrence_rows.append(
                    {
                        "credit_id": credit_id,
                        "episode_id": episode_id,
                        "appearance_type": "appears",
                    }
                )

    if verbose:
        print(f"Pass B: Built {len(occurrence_rows)} occurrence rows")
        print(f"  credits_not_found: {stats['credits_not_found']}")
        print(f"  credits_ambiguous_match: {stats['credits_ambiguous_match']}")

    if dry_run:
        stats["occurrences_inserted"] = len(occurrence_rows)
        if verbose:
            print(f"Pass B (dry-run): Would insert {len(occurrence_rows)} occurrences")
        return stats

    # Insert with conflict handling (idempotent)
    if occurrence_rows:
        inserted = insert_credit_occurrences_ignore_conflicts(db, occurrence_rows)
        stats["occurrences_inserted"] = len(inserted)
        stats["occurrences_skipped"] = len(occurrence_rows) - len(inserted)

        if verbose:
            print(
                f"Pass B: Inserted {len(inserted)} occurrences, skipped {stats['occurrences_skipped']} (already exist)"
            )

    return stats


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    if db is None:
        print("ERROR: Database connection required for backfill.", file=sys.stderr)
        return 1

    # Preflight checks
    assert_core_credits_table_exists(db)
    assert_core_credit_occurrences_table_exists(db)

    run_pass_a = args.passes in ("A", "all")
    run_pass_b = args.passes in ("B", "all")

    total_stats: dict[str, int] = {}

    if run_pass_a:
        if args.verbose:
            print("=" * 60)
            print("PASS A: show_cast → credits")
            print("=" * 60)
        stats_a = _pass_a_show_cast_to_credits(
            db,
            limit=args.limit,
            show_id=args.show_id,
            verbose=args.verbose,
            dry_run=args.dry_run,
        )
        total_stats.update({f"pass_a_{k}": v for k, v in stats_a.items()})

    if run_pass_b:
        if args.verbose:
            print("=" * 60)
            print("PASS B: episode_appearances → credit_occurrences")
            print("=" * 60)
        stats_b = _pass_b_episode_appearances_to_occurrences(
            db,
            limit=args.limit,
            show_id=args.show_id,
            verbose=args.verbose,
            dry_run=args.dry_run,
        )
        total_stats.update({f"pass_b_{k}": v for k, v in stats_b.items()})

    # Summary
    print()
    print("=" * 60)
    print("BACKFILL SUMMARY")
    print("=" * 60)
    if args.dry_run:
        print("MODE: dry-run (no writes)")
    for key, value in sorted(total_stats.items()):
        print(f"  {key}: {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
