#!/usr/bin/env python3
"""Apply migration 0054 to remote database."""

from __future__ import annotations

import os
from pathlib import Path


def main() -> int:
    migration_path = (
        Path(__file__).parent.parent / "supabase" / "migrations" / "0054_show_images_upsert_rpc_remove_votes.sql"
    )
    sql = migration_path.read_text()

    print("Applying migration 0054 to remote database...")
    print("=" * 80)

    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 not installed. Install with: pip install psycopg2-binary")
        return 1

    conn_str = os.getenv("SUPABASE_DB_URL")
    if not conn_str:
        print("ERROR: SUPABASE_DB_URL not set")
        return 1

    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        # Set search path to include core schema
        cursor.execute("SET search_path TO core, public;")

        # Execute the full migration SQL
        print("Executing SQL...")
        cursor.execute(sql)
        conn.commit()

        cursor.close()
        conn.close()

        print("=" * 80)
        print("✓ Migration 0054 applied successfully!")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}")
        print("\nIf the core schema doesn't exist, you may need to run all migrations first.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
