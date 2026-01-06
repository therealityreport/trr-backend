#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Any

import requests
from supabase import Client

from trr_backend.ingestion.tmdb_show_backfill import (
    build_tmdb_show_patch,
    extract_tmdb_network_ids,
    extract_tmdb_production_company_ids,
    needs_tmdb_enrichment,
)
from trr_backend.integrations.tmdb.client import TmdbClientError, fetch_tv_details, resolve_api_key, resolve_bearer_token
from trr_backend.repositories.shows import update_show
from scripts._sync_common import load_env_and_db


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_tmdb_show_details",
        description="Backfill TMDb show details into core.shows.",
    )
    parser.add_argument("--all", action="store_true", help="Fetch details for all shows with tmdb_id.")
    parser.add_argument("--show-id", action="append", default=[], help="core.shows id (UUID). Repeatable.")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of shows to process.")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to Supabase.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def _require_tmdb_auth() -> tuple[str | None, str | None]:
    api_key = resolve_api_key()
    bearer = resolve_bearer_token()
    if not api_key and not bearer:
        raise RuntimeError("TMDB_BEARER_TOKEN or TMDB_API_KEY must be set to fetch TMDb details.")
    return api_key, bearer


def _fetch_show_rows(db: Client, args: argparse.Namespace) -> list[dict[str, Any]]:
    fields = (
        "id,name,tmdb_id,tmdb_meta,tmdb_fetched_at,tmdb_vote_average,tmdb_vote_count,tmdb_popularity,"
        "tmdb_first_air_date,tmdb_last_air_date,tmdb_status,tmdb_type,tmdb_network_ids,tmdb_production_company_ids,"
        "needs_tmdb_resolution"
    )
    rows: list[dict[str, Any]] = []

    show_ids = [str(show_id).strip() for show_id in (args.show_id or []) if str(show_id).strip()]
    if show_ids:
        response = db.schema("core").table("shows").select(fields).in_("id", show_ids).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing shows by id: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)
    else:
        query = db.schema("core").table("shows").select(fields).not_.is_("tmdb_id", "null")
        if args.limit is not None:
            query = query.limit(max(0, int(args.limit)))
        response = query.execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing shows: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows = data

    if args.limit is not None and show_ids:
        rows = rows[: max(0, int(args.limit))]
    return rows


def _merge_int_arrays(existing: object, incoming: list[int]) -> list[int] | None:
    if not incoming:
        return None
    existing_values = [v for v in (existing if isinstance(existing, list) else []) if isinstance(v, int)]
    merged = sorted(set(existing_values) | set(incoming))
    if not merged or merged == sorted(existing_values):
        return None
    return merged


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()
    api_key, bearer = _require_tmdb_auth()

    rows = _fetch_show_rows(db, args)
    if args.verbose:
        print(f"backfill_tmdb_show_details: candidates={len(rows)}")

    scanned = 0
    enriched = 0
    skipped = 0
    api_errors = 0

    session = requests.Session()

    for row in rows:
        scanned += 1
        show_id = row.get("id")
        tmdb_id = row.get("tmdb_id")
        if not isinstance(tmdb_id, int):
            skipped += 1
            continue

        if not args.all and not needs_tmdb_enrichment(row):
            skipped += 1
            continue

        try:
            details = fetch_tv_details(
                int(tmdb_id),
                api_key=api_key,
                bearer_token=bearer,
                session=session,
                language="en-US",
            )
        except (TmdbClientError, requests.RequestException) as exc:
            api_errors += 1
            print(f"ERROR: tmdb details failed tmdb_id={tmdb_id} error={exc}")
            continue

        fetched_at = _now_utc_iso()
        patch = build_tmdb_show_patch(details, fetched_at=fetched_at)

        network_ids = extract_tmdb_network_ids(details)
        merged_network_ids = _merge_int_arrays(row.get("tmdb_network_ids"), network_ids)
        if merged_network_ids is not None:
            patch["tmdb_network_ids"] = merged_network_ids

        company_ids = extract_tmdb_production_company_ids(details)
        merged_company_ids = _merge_int_arrays(row.get("tmdb_production_company_ids"), company_ids)
        if merged_company_ids is not None:
            patch["tmdb_production_company_ids"] = merged_company_ids

        if row.get("needs_tmdb_resolution") is not False:
            patch["needs_tmdb_resolution"] = False

        enriched += 1
        if args.verbose:
            show_name = row.get("name") if isinstance(row.get("name"), str) else ""
            print(f"ENRICHED tmdb_id={tmdb_id} show={show_name}")

        if args.dry_run or not show_id:
            continue

        update_show(db, show_id, patch)

    print(
        "backfill_tmdb_show_details: "
        f"scanned={scanned} enriched={enriched} skipped={skipped} api_errors={api_errors}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
