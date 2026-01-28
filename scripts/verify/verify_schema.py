#!/usr/bin/env python3
from __future__ import annotations

import os
from collections.abc import Iterable

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit("Missing psycopg2; install deps (e.g., `pip install -r requirements.txt`).") from exc

from trr_backend.utils.env import load_env


def _resolve_db_url() -> str:
    url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if not url:
        raise RuntimeError("SUPABASE_DB_URL is required for verify_schema.")
    return url


def _fetch_relkind(cur: RealDictCursor, schema: str, name: str) -> str | None:
    cur.execute(
        """
        select c.relkind
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = %s and c.relname = %s
        """,
        (schema, name),
    )
    row = cur.fetchone()
    return row["relkind"] if row else None


def _fetch_columns(cur: RealDictCursor, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
        """,
        (schema, table),
    )
    return {row["column_name"] for row in cur.fetchall()}


def _fetch_primary_key(cur: RealDictCursor, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        select kcu.column_name
        from information_schema.table_constraints tc
        join information_schema.key_column_usage kcu
          on tc.constraint_name = kcu.constraint_name
         and tc.table_schema = kcu.table_schema
        where tc.table_schema = %s
          and tc.table_name = %s
          and tc.constraint_type = 'PRIMARY KEY'
        order by kcu.ordinal_position
        """,
        (schema, table),
    )
    return [row["column_name"] for row in cur.fetchall()]


def _fetch_view_definition(cur: RealDictCursor, schema: str, view: str) -> str | None:
    cur.execute(
        """
        select pg_get_viewdef(c.oid, true) as definition
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = %s and c.relname = %s
        """,
        (schema, view),
    )
    row = cur.fetchone()
    return row["definition"] if row else None


def _report(label: str, ok: bool, details: str | None = None, *, optional: bool = False) -> bool:
    status = "PASS" if ok else ("WARN" if optional else "FAIL")
    suffix = f" ({details})" if details else ""
    print(f"{status}: {label}{suffix}")
    return ok or optional


def _check_view(cur: RealDictCursor, schema: str, view: str) -> bool:
    relkind = _fetch_relkind(cur, schema, view)
    if relkind is None:
        return _report(f"{schema}.{view} view exists", False, "missing")
    return _report(f"{schema}.{view} is a view", relkind == "v", f"relkind={relkind}")


def _check_table(cur: RealDictCursor, schema: str, table: str) -> bool:
    relkind = _fetch_relkind(cur, schema, table)
    if relkind is None:
        return _report(f"{schema}.{table} table exists", False, "missing")
    return _report(f"{schema}.{table} is a table", relkind in {"r", "p"}, f"relkind={relkind}")


def _check_columns(
    cur: RealDictCursor,
    schema: str,
    table: str,
    required: Iterable[str],
) -> bool:
    existing = _fetch_columns(cur, schema, table)
    missing = [col for col in required if col not in existing]
    return _report(
        f"{schema}.{table} columns",
        not missing,
        "missing=" + ",".join(missing) if missing else None,
    )


def main(argv: list[str] | None = None) -> int:
    _ = argv
    load_env()

    conn = psycopg2.connect(_resolve_db_url(), cursor_factory=RealDictCursor)
    try:
        cur = conn.cursor()
        ok = True

        ok &= _check_view(cur, "core", "tmdb_series")
        ok &= _check_view(cur, "core", "imdb_series")

        for table in ("networks", "production_companies", "watch_providers", "show_watch_providers"):
            ok &= _check_table(cur, "core", table)

        ok &= _check_columns(
            cur,
            "core",
            "networks",
            ["id", "name", "origin_country", "logo_path"],
        )
        ok &= _check_columns(
            cur,
            "core",
            "production_companies",
            ["id", "name", "origin_country", "logo_path"],
        )
        ok &= _check_columns(
            cur,
            "core",
            "watch_providers",
            ["provider_id", "provider_name", "logo_path"],
        )

        pk = _fetch_primary_key(cur, "core", "show_watch_providers")
        ok &= _report(
            "core.show_watch_providers primary key",
            pk == ["show_id", "region", "offer_type", "provider_id"],
            f"pk={pk}",
        )

        hosted_cols = [
            "hosted_bucket",
            "hosted_key",
            "hosted_url",
            "hosted_sha256",
            "hosted_content_type",
            "hosted_bytes",
            "hosted_etag",
            "hosted_at",
        ]
        ok &= _check_columns(cur, "core", "cast_photos", hosted_cols)
        ok &= _check_columns(cur, "core", "show_images", hosted_cols)
        ok &= _check_columns(cur, "core", "season_images", hosted_cols)

        view_ok = _check_view(cur, "core", "v_show_images_served")
        ok &= view_ok
        if view_ok:
            definition = _fetch_view_definition(cur, "core", "v_show_images_served") or ""
            def_lower = definition.lower()
            ok &= _report(
                "core.v_show_images_served served_url coalesce(hosted_url, url)",
                "coalesce" in def_lower
                and "hosted_url" in def_lower
                and "url" in def_lower
                and "served_url" in def_lower,
            )

        v_cast = _fetch_relkind(cur, "core", "v_cast_photos")
        _report(
            "core.v_cast_photos view exists",
            v_cast == "v",
            f"relkind={v_cast}" if v_cast else "missing",
            optional=True,
        )

        return 0 if ok else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
