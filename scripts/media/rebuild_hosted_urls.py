#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit("Missing psycopg2; install deps (e.g., `pip install -r requirements.txt`).") from exc

from trr_backend.media.s3_mirror import build_hosted_url, get_cdn_base_url
from trr_backend.utils.env import load_env

TABLES = ("cast_photos", "show_images", "season_images")


def _resolve_db_url() -> str:
    url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if not url:
        raise RuntimeError("SUPABASE_DB_URL is required for rebuild_hosted_urls.")
    return url


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rebuild_hosted_urls",
        description="Rebuild hosted_url values from hosted_key without re-uploading.",
    )
    parser.add_argument(
        "--table",
        default="all",
        choices=["cast_photos", "show_images", "season_images", "all"],
        help="Target table to update (default: all).",
    )
    parser.add_argument("--person-id", action="append", default=[], help="core.people UUID. Repeatable.")
    parser.add_argument("--imdb-person-id", action="append", default=[], help="IMDb person ID (nm...). Repeatable.")
    parser.add_argument("--show-id", action="append", default=[], help="core.shows UUID. Repeatable.")
    parser.add_argument("--limit", type=int, default=200, help="Max rows per table (default: 200).")
    parser.add_argument("--dry-run", action="store_true", help="Preview updates without writing.")
    parser.add_argument("--verbose", action="store_true", help="Print each updated row.")
    return parser.parse_args(argv)


def _coerce_list(values: Iterable[str]) -> list[str]:
    return [str(v).strip() for v in values if str(v).strip()]


def _fetch_rows(
    cur: RealDictCursor,
    table: str,
    *,
    imdb_person_ids: list[str],
    person_ids: list[str],
    show_ids: list[str],
    limit: int | None,
    base_pattern: str,
) -> list[dict[str, object]]:
    conditions = [
        "hosted_key is not null",
        "(hosted_url is null or hosted_url not like %s)",
    ]
    params: list[object] = [base_pattern]

    if table == "cast_photos":
        if person_ids and imdb_person_ids:
            conditions.append("(person_id = ANY(%s) OR imdb_person_id = ANY(%s))")
            params.extend([person_ids, imdb_person_ids])
        elif person_ids:
            conditions.append("person_id = ANY(%s)")
            params.append(person_ids)
        elif imdb_person_ids:
            conditions.append("imdb_person_id = ANY(%s)")
            params.append(imdb_person_ids)
    else:
        if show_ids:
            conditions.append("show_id = ANY(%s)")
            params.append(show_ids)

    sql = f"select id, hosted_key, hosted_url from core.{table} where {' and '.join(conditions)}"
    if limit is not None:
        sql += " limit %s"
        params.append(int(limit))

    cur.execute(sql, params)
    rows = cur.fetchall()
    return rows if isinstance(rows, list) else []


def _update_rows(
    cur: RealDictCursor,
    table: str,
    rows: list[dict[str, object]],
    *,
    dry_run: bool,
    verbose: bool,
) -> tuple[int, int]:
    scanned = len(rows)
    updated = 0
    for row in rows:
        hosted_key = row.get("hosted_key")
        if not hosted_key:
            continue
        desired_url = build_hosted_url(str(hosted_key))
        current_url = row.get("hosted_url")
        if current_url == desired_url:
            continue
        updated += 1
        if verbose:
            print(f"  {table}:{row.get('id')} -> {desired_url}")
        if dry_run:
            continue
        cur.execute(
            f"update core.{table} set hosted_url = %s where id = %s",
            (desired_url, row.get("id")),
        )
    return scanned, updated


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    base_url = get_cdn_base_url()
    base_pattern = f"{base_url}/%"

    imdb_person_ids = _coerce_list(args.imdb_person_id)
    person_ids = _coerce_list(args.person_id)
    show_ids = _coerce_list(args.show_id)

    tables = TABLES if args.table == "all" else (args.table,)

    conn = psycopg2.connect(_resolve_db_url(), cursor_factory=RealDictCursor)
    try:
        cur = conn.cursor()
        for table in tables:
            rows = _fetch_rows(
                cur,
                table,
                imdb_person_ids=imdb_person_ids,
                person_ids=person_ids,
                show_ids=show_ids,
                limit=args.limit,
                base_pattern=base_pattern,
            )
            scanned, updated = _update_rows(
                cur,
                table,
                rows,
                dry_run=bool(args.dry_run),
                verbose=bool(args.verbose),
            )
            if not args.dry_run:
                conn.commit()
            print(f"{table}: scanned={scanned} updated={updated}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
