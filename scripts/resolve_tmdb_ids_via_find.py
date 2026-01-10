#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Any

import requests

from scripts._sync_common import load_env_and_db
from supabase import Client
from trr_backend.ingestion.tmdb_show_backfill import resolve_tmdb_id_from_find_payload
from trr_backend.integrations.tmdb.client import TmdbClientError, find_by_imdb_id, resolve_api_key, resolve_bearer_token
from trr_backend.repositories.shows import update_show


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="resolve_tmdb_ids_via_find",
        description="Resolve TMDb IDs for shows using TMDb /find by IMDb id.",
    )
    parser.add_argument("--all", action="store_true", help="Process all eligible shows.")
    parser.add_argument("--show-id", action="append", default=[], help="core.shows id (UUID). Repeatable.")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of shows to process.")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to Supabase.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def _require_tmdb_auth() -> tuple[str | None, str | None]:
    api_key = resolve_api_key()
    bearer = resolve_bearer_token()
    if not api_key and not bearer:
        raise RuntimeError("TMDB_BEARER_TOKEN or TMDB_API_KEY must be set to resolve TMDb IDs.")
    return api_key, bearer


def _fetch_show_rows(db: Client, args: argparse.Namespace) -> list[dict[str, Any]]:
    fields = "id,name,imdb_id,tmdb_id,premiere_date,needs_tmdb_resolution"
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
        query = db.schema("core").table("shows").select(fields).not_.is_("imdb_id", "null").is_("tmdb_id", "null")
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


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()
    api_key, bearer = _require_tmdb_auth()

    rows = _fetch_show_rows(db, args)
    if args.verbose:
        print(f"resolve_tmdb_ids_via_find: candidates={len(rows)}")

    scanned = 0
    resolved = 0
    unresolved = 0
    skipped = 0
    api_errors = 0

    session = requests.Session()

    for row in rows:
        scanned += 1
        show_id = row.get("id")
        imdb_id = row.get("imdb_id")
        tmdb_id = row.get("tmdb_id")

        if not isinstance(imdb_id, str) or not imdb_id.strip():
            skipped += 1
            continue
        if isinstance(tmdb_id, int):
            skipped += 1
            continue

        try:
            payload = find_by_imdb_id(
                imdb_id.strip(),
                api_key=api_key,
                bearer_token=bearer,
                session=session,
                language="en-US",
            )
        except (TmdbClientError, requests.RequestException) as exc:
            api_errors += 1
            print(f"ERROR: tmdb find failed imdb_id={imdb_id} error={exc}")
            continue

        resolved_tmdb_id, reason = resolve_tmdb_id_from_find_payload(
            payload,
            show_name=row.get("name") if isinstance(row.get("name"), str) else None,
            premiere_date=row.get("premiere_date") if isinstance(row.get("premiere_date"), str) else None,
        )

        if resolved_tmdb_id is None:
            unresolved += 1
            print(f"UNRESOLVED imdb_id={imdb_id} reason={reason}")
            if not args.dry_run and show_id:
                if row.get("needs_tmdb_resolution") is not True:
                    update_show(db, show_id, {"needs_tmdb_resolution": True})
            continue

        resolved += 1
        if args.verbose:
            print(f"RESOLVED imdb_id={imdb_id} -> tmdb_id={resolved_tmdb_id} ({reason})")
        if not args.dry_run and show_id:
            update_show(
                db,
                show_id,
                {"tmdb_id": int(resolved_tmdb_id), "needs_tmdb_resolution": False},
            )

    print(
        "resolve_tmdb_ids_via_find: "
        f"scanned={scanned} resolved={resolved} unresolved={unresolved} skipped={skipped} api_errors={api_errors}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
