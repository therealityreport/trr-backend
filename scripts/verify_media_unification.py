#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit("Missing psycopg2; install deps (e.g., `pip install -r requirements.txt`).") from exc

from trr_backend.utils.env import load_env


BRIDGE_TRIGGERS = {
    "bridge_show_images_to_media": "core.show_images",
    "bridge_season_images_to_media": "core.season_images",
    "bridge_episode_images_to_media": "core.episode_images",
    "bridge_person_images_to_media": "core.person_images",
    "bridge_cast_photos_to_media": "core.cast_photos",
    "bridge_show_source_snapshots": "core.shows",
}


def _resolve_db_url() -> str:
    url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if not url:
        raise RuntimeError("SUPABASE_DB_URL is required for verify_media_unification.")
    return url


def _report(label: str, ok: bool, details: str | None = None, *, optional: bool = False) -> bool:
    status = "PASS" if ok else ("WARN" if optional else "FAIL")
    suffix = f" ({details})" if details else ""
    print(f"{status}: {label}{suffix}")
    return ok or optional


def _check_triggers(cur: RealDictCursor) -> bool:
    trigger_names = list(BRIDGE_TRIGGERS.keys())
    cur.execute(
        """
        select t.tgname, t.tgenabled, n.nspname as schema, c.relname as table_name
        from pg_trigger t
        join pg_class c on c.oid = t.tgrelid
        join pg_namespace n on n.oid = c.relnamespace
        where t.tgname = any(%s)
        """,
        (trigger_names,),
    )
    rows = cur.fetchall()
    found = {row["tgname"]: row for row in rows}

    ok = True
    for name, expected_table in BRIDGE_TRIGGERS.items():
        row = found.get(name)
        if not row:
            ok &= _report(f"trigger {name} exists", False, "missing")
            continue
        table = f"{row['schema']}.{row['table_name']}"
        ok &= _report(
            f"trigger {name} target",
            table == expected_table,
            f"table={table}",
        )
        enabled = row["tgenabled"] in {"O", "A"}
        ok &= _report(
            f"trigger {name} enabled",
            enabled,
            f"tgenabled={row['tgenabled']}",
        )
    return ok


def _check_missing_links(
    cur: RealDictCursor,
    *,
    legacy_table: str,
    entity_type: str,
    entity_id_column: str | None,
) -> bool:
    if entity_id_column:
        cur.execute(
            f"""
            select count(*) as missing
            from core.{legacy_table} s
            where not exists (
              select 1
              from core.media_links ml
              where ml.entity_type = %s
                and ml.entity_id = s.{entity_id_column}
                and (ml.context->>'legacy_table') = %s
                and (ml.context->>'legacy_id')::uuid = s.id
            )
            """,
            (entity_type, legacy_table),
        )
    else:
        cur.execute(
            f"""
            select count(*) as missing
            from core.{legacy_table} s
            where not exists (
              select 1
              from core.media_links ml
              where ml.entity_type = %s
                and (ml.context->>'legacy_table') = %s
                and (ml.context->>'legacy_id')::uuid = s.id
            )
            """,
            (entity_type, legacy_table),
        )
    missing = int(cur.fetchone()["missing"])
    return _report(
        f"legacy {legacy_table} mirrored to media_links",
        missing == 0,
        f"missing={missing}",
    )


def _check_primary_uniqueness(cur: RealDictCursor) -> bool:
    cur.execute(
        """
        select count(*) as violations
        from (
          select entity_type, entity_id, kind
          from core.media_links
          where is_primary = true
          group by entity_type, entity_id, kind
          having count(*) > 1
        ) dupes
        """
    )
    violations = int(cur.fetchone()["violations"])
    return _report(
        "media_links primary uniqueness",
        violations == 0,
        f"violations={violations}",
    )


def _check_snapshot_bridge(cur: RealDictCursor) -> bool:
    cur.execute("select id from core.shows order by created_at desc limit 1")
    row = cur.fetchone()
    if not row:
        return _report("snapshot bridge check", False, "no shows found", optional=True)

    show_id = row["id"]
    cur.execute(
        """
        update core.shows
        set tmdb_meta = jsonb_build_object('verify_media_bridge', true, 'ts', now()::text),
            tmdb_fetched_at = now()
        where id = %s
        """,
        (show_id,),
    )

    cur.execute(
        """
        select count(*) as count
        from core.show_source_latest
        where show_id = %s
          and source_id = 'tmdb'
          and variant = 'details'
          and payload->>'verify_media_bridge' = 'true'
        """,
        (show_id,),
    )
    latest_count = int(cur.fetchone()["count"])

    cur.execute(
        """
        select count(*) as count
        from core.show_source_history
        where show_id = %s
          and source_id = 'tmdb'
          and variant = 'details'
          and payload->>'verify_media_bridge' = 'true'
        """,
        (show_id,),
    )
    history_count = int(cur.fetchone()["count"])

    ok = True
    ok &= _report(
        "snapshot bridge latest",
        latest_count > 0,
        f"rows={latest_count}",
    )
    ok &= _report(
        "snapshot bridge history",
        history_count > 0,
        f"rows={history_count}",
    )
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify media unification bridges and coverage.")
    parser.add_argument(
        "--check-snapshot-bridge",
        action="store_true",
        help="Update a show tmdb_meta and verify snapshot bridge writes latest/history.",
    )
    args = parser.parse_args()

    load_env()
    conn = psycopg2.connect(
        _resolve_db_url(),
        cursor_factory=RealDictCursor,
        connect_timeout=5,
    )
    try:
        cur = conn.cursor()
        ok = True

        ok &= _check_triggers(cur)
        ok &= _check_missing_links(
            cur,
            legacy_table="show_images",
            entity_type="show",
            entity_id_column="show_id",
        )
        ok &= _check_missing_links(
            cur,
            legacy_table="season_images",
            entity_type="season",
            entity_id_column="season_id",
        )
        ok &= _check_missing_links(
            cur,
            legacy_table="episode_images",
            entity_type="episode",
            entity_id_column="episode_id",
        )
        ok &= _check_missing_links(
            cur,
            legacy_table="person_images",
            entity_type="person",
            entity_id_column=None,
        )
        ok &= _check_missing_links(
            cur,
            legacy_table="cast_photos",
            entity_type="person",
            entity_id_column=None,
        )
        ok &= _check_primary_uniqueness(cur)

        if args.check_snapshot_bridge:
            ok &= _check_snapshot_bridge(cur)

        if args.check_snapshot_bridge:
            conn.commit()
        return 0 if ok else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
