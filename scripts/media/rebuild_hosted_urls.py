#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse

try:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit("Missing psycopg2; install deps (e.g., `pip install -r requirements.txt`).") from exc

from scripts._db_url import resolve_db_url
from trr_backend.media.s3_mirror import build_hosted_url, get_cdn_base_url
from trr_backend.utils.env import load_env

TABLES = (
    "cast_photos",
    "show_images",
    "season_images",
    "episode_images",
    "person_images",
    "media_assets",
    "media_asset_variants",
)

TABLE_SELECT_COLUMNS: dict[str, tuple[str, ...]] = {
    "cast_photos": ("hosted_key", "hosted_url", "metadata"),
    "show_images": ("hosted_key", "hosted_url", "metadata"),
    "season_images": ("hosted_key", "hosted_url", "metadata"),
    "episode_images": ("hosted_key", "hosted_url", "metadata"),
    "person_images": (),
    "media_assets": ("hosted_key", "hosted_url", "metadata"),
    "media_asset_variants": ("hosted_key", "hosted_url"),
}

TABLE_METADATA_COLUMNS: dict[str, str | None] = {
    "cast_photos": "metadata",
    "show_images": "metadata",
    "season_images": "metadata",
    "episode_images": "metadata",
    "person_images": None,
    "media_assets": "metadata",
    "media_asset_variants": None,
}

PERSON_FILTER_COLUMNS: dict[str, str | None] = {
    "cast_photos": "person_id",
    "person_images": "person_id",
}

SHOW_FILTER_COLUMNS: dict[str, str | None] = {
    "show_images": "show_id",
    "season_images": "show_id",
    "episode_images": "show_id",
}

LEGACY_HOST_MARKERS = ("cloudfront.net", "amazonaws.com", "s3.")
REWRITEABLE_HOSTED_PATH_PREFIXES = (
    "media/",
    "media-variants/",
    "cast-photo-variants/",
    "face-crops/",
    "images/",
    "icons/",
    "social/",
    "fonts/",
)


def _resolve_db_url() -> str:
    return resolve_db_url(allow_database_url=True).value


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rebuild_hosted_urls",
        description=(
            "Rebuild hosted_url values and embedded hosted-media metadata URLs onto the canonical public base "
            "without re-uploading."
        ),
    )
    parser.add_argument(
        "--table",
        default="all",
        choices=[*TABLES, "all"],
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


def _is_legacy_host(host: str | None) -> bool:
    normalized = str(host or "").strip().lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in LEGACY_HOST_MARKERS)


def _hosted_path_from_url(url: str | None) -> str | None:
    if not isinstance(url, str):
        return None
    trimmed = url.strip()
    if not trimmed:
        return None
    try:
        parsed = urlparse(trimmed)
    except ValueError:
        return None
    if not _is_legacy_host(parsed.netloc):
        return None
    normalized_path = (parsed.path or "").lstrip("/")
    if not normalized_path:
        return None
    if not normalized_path.startswith(REWRITEABLE_HOSTED_PATH_PREFIXES):
        return None
    return normalized_path


def _rewrite_legacy_hosted_url(url: str | None) -> str | None:
    if not isinstance(url, str):
        return None
    trimmed = url.strip()
    if not trimmed:
        return None
    hosted_path = _hosted_path_from_url(trimmed)
    if hosted_path:
        return build_hosted_url(hosted_path)
    return trimmed


def _rewrite_metadata_value(value: Any) -> tuple[Any, bool]:
    if isinstance(value, dict):
        changed = False
        rewritten: dict[str, Any] = {}
        for key, item in value.items():
            next_value, item_changed = _rewrite_metadata_value(item)
            rewritten[key] = next_value
            changed = changed or item_changed
        return rewritten, changed
    if isinstance(value, list):
        changed = False
        rewritten_list: list[Any] = []
        for item in value:
            next_value, item_changed = _rewrite_metadata_value(item)
            rewritten_list.append(next_value)
            changed = changed or item_changed
        return rewritten_list, changed
    if isinstance(value, str):
        rewritten_url = _rewrite_legacy_hosted_url(value)
        if rewritten_url != value:
            return rewritten_url, True
    return value, False


def rewrite_metadata_urls(metadata: Any) -> tuple[Any, bool]:
    if not isinstance(metadata, (dict, list)):
        return metadata, False
    return _rewrite_metadata_value(metadata)


def resolve_desired_hosted_url(*, hosted_key: str | None, current_url: str | None) -> str | None:
    key = str(hosted_key or "").strip()
    if key:
        return build_hosted_url(key)
    return _rewrite_legacy_hosted_url(current_url)


def build_row_patch(*, table: str, row: dict[str, Any]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    desired_url = resolve_desired_hosted_url(
        hosted_key=str(row.get("hosted_key") or "").strip() or None,
        current_url=str(row.get("hosted_url") or "").strip() or None,
    )
    current_url = row.get("hosted_url")
    if isinstance(desired_url, str) and desired_url and desired_url != current_url:
        patch["hosted_url"] = desired_url

    metadata_column = TABLE_METADATA_COLUMNS.get(table)
    if metadata_column:
        metadata = row.get(metadata_column)
        rewritten_metadata, metadata_changed = rewrite_metadata_urls(metadata)
        if metadata_changed:
            patch[metadata_column] = rewritten_metadata

    return patch


def _fetch_rows(
    cur: RealDictCursor,
    table: str,
    *,
    imdb_person_ids: list[str],
    person_ids: list[str],
    show_ids: list[str],
    limit: int | None,
    base_pattern: str,
    legacy_patterns: list[str],
) -> list[dict[str, Any]]:
    selectable_columns = TABLE_SELECT_COLUMNS.get(table, ())
    if not selectable_columns:
        return []

    metadata_column = TABLE_METADATA_COLUMNS.get(table)
    has_hosted_key = "hosted_key" in selectable_columns
    has_hosted_url = "hosted_url" in selectable_columns

    columns = ["id", *selectable_columns]

    candidate_presence_parts: list[str] = []
    if has_hosted_key:
        candidate_presence_parts.append("hosted_key is not null")
    if has_hosted_url:
        candidate_presence_parts.append("hosted_url is not null")
    if metadata_column:
        candidate_presence_parts.append(f"{metadata_column} is not null")
    if not candidate_presence_parts:
        return []

    conditions: list[str] = [f"({' or '.join(candidate_presence_parts)})"]
    stale_conditions: list[str] = []
    params: list[object] = []

    if has_hosted_url:
        stale_conditions.extend(["hosted_url is null", "hosted_url not like %s"])
        params.append(base_pattern)
    if metadata_column:
        for pattern in legacy_patterns:
            stale_conditions.append(f"{metadata_column}::text like %s")
            params.append(pattern)
    if not stale_conditions:
        return []
    conditions.append(f"({' or '.join(stale_conditions)})")

    person_filter_column = PERSON_FILTER_COLUMNS.get(table)
    show_filter_column = SHOW_FILTER_COLUMNS.get(table)
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
    elif person_filter_column and person_ids:
        conditions.append(f"{person_filter_column} = ANY(%s)")
        params.append(person_ids)
    elif show_filter_column and show_ids:
        conditions.append(f"{show_filter_column} = ANY(%s)")
        params.append(show_ids)

    sql = f"select {', '.join(columns)} from core.{table} where {' and '.join(conditions)}"
    if limit is not None:
        sql += " limit %s"
        params.append(int(limit))

    cur.execute(sql, params)
    rows = cur.fetchall()
    return rows if isinstance(rows, list) else []


def _update_rows(
    cur: RealDictCursor,
    table: str,
    rows: list[dict[str, Any]],
    *,
    dry_run: bool,
    verbose: bool,
) -> tuple[int, int]:
    scanned = len(rows)
    updated = 0
    for row in rows:
        patch = build_row_patch(table=table, row=row)
        if not patch:
            continue

        updated += 1
        if verbose:
            print(f"  {table}:{row.get('id')} patch_keys={sorted(patch.keys())}")
        if dry_run:
            continue

        assignments: list[str] = []
        values: list[object] = []
        for key, value in patch.items():
            assignments.append(f"{key} = %s")
            values.append(Json(value) if key == "metadata" else value)
        values.append(row.get("id"))
        cur.execute(
            f"update core.{table} set {', '.join(assignments)} where id = %s",
            values,
        )
    return scanned, updated


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    base_url = get_cdn_base_url()
    base_pattern = f"{base_url}/%"
    legacy_patterns = ["%cloudfront.net%", "%amazonaws.com%"]

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
                legacy_patterns=legacy_patterns,
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
