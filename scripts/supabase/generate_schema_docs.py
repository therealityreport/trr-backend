#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit(
        "Missing psycopg2; install dev deps (e.g., `pip install -r requirements.txt`)."
    ) from exc

_OUTPUT_DIR = Path("supabase/schema_docs")
_SCHEMAS = ("core",)
_SENSITIVE_KEYS = ("password", "token", "secret", "api_key", "access_key", "refresh_key", "session")


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: str
    default: str | None
    is_identity: str
    is_generated: str
    udt_name: str


def _resolve_db_url() -> str:
    for key in ("SUPABASE_DB_URL", "TRR_DB_URL"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value

    env = {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "sslmode": os.getenv("PGSSLMODE"),
    }
    if env["host"] and env["dbname"] and env["user"]:
        auth = f"{env['user']}:{env['password']}@" if env["password"] else f"{env['user']}@"
        port = f":{env['port']}" if env["port"] else ""
        sslmode = f"?sslmode={env['sslmode']}" if env["sslmode"] else ""
        return f"postgresql://{auth}{env['host']}{port}/{env['dbname']}{sslmode}"

    supabase_db_url = _resolve_supabase_db_url()
    if supabase_db_url:
        return supabase_db_url

    raise RuntimeError(
        "Database connection env vars not set. Start Supabase or export SUPABASE_DB_URL "
        "(preferred: run `make schema-docs`)."
    )


def _resolve_supabase_db_url() -> str | None:
    try:
        result = subprocess.run(
            ["supabase", "status", "--output", "env"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None

    for line in (result.stdout or "").splitlines():
        if not line.startswith("DB_URL="):
            continue
        value = line.split("=", 1)[1].strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value:
            return value
    return None


def _connect() -> psycopg2.extensions.connection:
    try:
        return psycopg2.connect(_resolve_db_url(), cursor_factory=RealDictCursor)
    except psycopg2.OperationalError as exc:
        raise RuntimeError(
            "Failed to connect to database. Start Supabase or export SUPABASE_DB_URL "
            "then run `make schema-docs`."
        ) from exc


def _list_schemas(cur: RealDictCursor) -> list[str]:
    cur.execute(
        """
        select schema_name
        from information_schema.schemata
        where schema_name not in ('pg_catalog', 'information_schema')
        order by schema_name
        """
    )
    schemas = [row["schema_name"] for row in cur.fetchall()]
    if _SCHEMAS:
        return [schema for schema in schemas if schema in _SCHEMAS]
    return schemas


def _list_tables(cur: RealDictCursor, schema: str) -> list[str]:
    cur.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = %s
          and table_type = 'BASE TABLE'
        order by table_name
        """,
        (schema,),
    )
    return [row["table_name"] for row in cur.fetchall()]


def _load_columns(cur: RealDictCursor, schema: str, table: str) -> list[ColumnInfo]:
    cur.execute(
        """
        select column_name, data_type, is_nullable, column_default, is_identity, is_generated, udt_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
        order by ordinal_position
        """,
        (schema, table),
    )
    columns = []
    for row in cur.fetchall():
        columns.append(
            ColumnInfo(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=row["is_nullable"],
                default=row["column_default"],
                is_identity=row["is_identity"],
                is_generated=row["is_generated"],
                udt_name=row["udt_name"],
            )
        )
    return columns


def _load_primary_keys(cur: RealDictCursor, schema: str, table: str) -> list[str]:
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


def _load_unique_constraints(cur: RealDictCursor, schema: str, table: str) -> list[list[str]]:
    cur.execute(
        """
        select tc.constraint_name, kcu.column_name, kcu.ordinal_position
        from information_schema.table_constraints tc
        join information_schema.key_column_usage kcu
          on tc.constraint_name = kcu.constraint_name
         and tc.table_schema = kcu.table_schema
        where tc.table_schema = %s
          and tc.table_name = %s
          and tc.constraint_type = 'UNIQUE'
        order by tc.constraint_name, kcu.ordinal_position
        """,
        (schema, table),
    )
    grouped: dict[str, list[str]] = {}
    for row in cur.fetchall():
        grouped.setdefault(row["constraint_name"], []).append(row["column_name"])
    return list(grouped.values())


def _load_foreign_keys(cur: RealDictCursor, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        select
          tc.constraint_name,
          kcu.column_name as local_column,
          ccu.table_schema as foreign_schema,
          ccu.table_name as foreign_table,
          ccu.column_name as foreign_column
        from information_schema.table_constraints tc
        join information_schema.key_column_usage kcu
          on tc.constraint_name = kcu.constraint_name
         and tc.table_schema = kcu.table_schema
        join information_schema.constraint_column_usage ccu
          on ccu.constraint_name = tc.constraint_name
         and ccu.table_schema = tc.table_schema
        where tc.table_schema = %s
          and tc.table_name = %s
          and tc.constraint_type = 'FOREIGN KEY'
        order by tc.constraint_name
        """,
        (schema, table),
    )
    out = []
    for row in cur.fetchall():
        out.append(
            {
                "constraint": row["constraint_name"],
                "column": row["local_column"],
                "references": f"{row['foreign_schema']}.{row['foreign_table']}.{row['foreign_column']}",
            }
        )
    return out


def _load_indexes(cur: RealDictCursor, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        select indexname, indexdef
        from pg_indexes
        where schemaname = %s and tablename = %s
        order by indexname
        """,
        (schema, table),
    )
    indexes = []
    for row in cur.fetchall():
        indexdef = row["indexdef"]
        columns: list[str] = []
        match = re.search(r"\((.*)\)", indexdef)
        if match:
            raw = match.group(1)
            columns = [part.strip().strip('"') for part in raw.split(",")]
        indexes.append(
            {
                "name": row["indexname"],
                "columns": [c for c in columns if c],
                "unique": indexdef.upper().startswith("CREATE UNIQUE INDEX"),
                "definition": indexdef,
            }
        )
    return indexes


def _load_rls(cur: RealDictCursor, schema: str, table: str) -> bool | None:
    cur.execute(
        """
        select relrowsecurity
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = %s and c.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    if not row:
        return None
    return bool(row["relrowsecurity"])


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return str(value)


def _redact_row(row: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in row.items():
        lower = key.casefold()
        if any(token in lower for token in _SENSITIVE_KEYS):
            redacted[key] = "REDACTED"
        else:
            redacted[key] = _json_safe(value)
    return redacted


def _placeholder_for_column(column: ColumnInfo) -> Any:
    data_type = column.data_type
    udt = column.udt_name
    if data_type.endswith("[]") or udt.startswith("_"):
        return []
    if data_type in {"integer", "smallint", "bigint"}:
        return 0
    if data_type in {"numeric", "real", "double precision"}:
        return 0
    if data_type == "boolean":
        return False
    if data_type in {"json", "jsonb"}:
        return {}
    if data_type in {"uuid"}:
        return "00000000-0000-0000-0000-000000000000"
    if data_type in {"date"}:
        return "1970-01-01"
    if data_type in {"timestamp without time zone", "timestamp with time zone"}:
        return "1970-01-01T00:00:00Z"
    return "example"


def _synthetic_row(columns: Iterable[ColumnInfo]) -> dict[str, Any]:
    return {col.name: _placeholder_for_column(col) for col in columns}


def _fetch_example_row(
    cur: RealDictCursor,
    schema: str,
    table: str,
    columns: list[ColumnInfo],
    *,
    use_live_examples: bool,
) -> dict[str, Any]:
    if not use_live_examples:
        return _synthetic_row(columns)
    try:
        cur.execute(f"select * from {schema}.{table} limit 1")
        row = cur.fetchone()
    except Exception:
        row = None
    if isinstance(row, dict) and row:
        return _redact_row(row)
    return _synthetic_row(columns)


def _write_table_docs(schema: str, table: str, payload: dict[str, Any]) -> None:
    json_path = _OUTPUT_DIR / f"{schema}.{table}.json"
    md_path = _OUTPUT_DIR / f"{schema}.{table}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    lines = [f"# {schema}.{table}", "", "## Columns", ""]
    lines.append("| name | type | nullable | default | identity | generated |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for col in payload["columns"]:
        lines.append(
            f"| {col['name']} | {col['data_type']} | {col['is_nullable']} | {col['default'] or ''} "
            f"| {col['is_identity']} | {col['is_generated']} |"
        )

    lines.extend(["", "## Primary Key", ""])
    lines.append(", ".join(payload["primary_key"]) or "(none)")

    lines.extend(["", "## Unique Constraints", ""])
    if payload["unique_constraints"]:
        for constraint in payload["unique_constraints"]:
            lines.append(f"- {', '.join(constraint)}")
    else:
        lines.append("(none)")

    lines.extend(["", "## Foreign Keys", ""])
    if payload["foreign_keys"]:
        for fk in payload["foreign_keys"]:
            lines.append(f"- {fk['column']} -> {fk['references']}")
    else:
        lines.append("(none)")

    lines.extend(["", "## Indexes", ""])
    if payload["indexes"]:
        for idx in payload["indexes"]:
            cols = ", ".join(idx.get("columns") or [])
            unique = "unique" if idx.get("unique") else "non-unique"
            lines.append(f"- {idx['name']} ({unique}): {cols}")
    else:
        lines.append("(none)")

    if payload.get("rls_enabled") is not None:
        lines.extend(["", "## RLS Enabled", "", str(payload["rls_enabled"]).lower()])

    lines.extend(["", "## Example Row", "", "```json", json.dumps(payload["example_row"], indent=2), "```"])

    md_path.write_text("\n".join(lines), encoding="utf-8")


def _write_index(entries: list[dict[str, str]]) -> None:
    lines = ["# Schema Docs Index", ""]
    for entry in entries:
        lines.append(f"- [{entry['schema']}.{entry['table']}]({entry['filename']})")
    (_OUTPUT_DIR / "INDEX.md").write_text("\n".join(lines), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="generate_schema_docs",
        description="Generate deterministic schema docs from the local database.",
    )
    parser.add_argument(
        "--with-live-examples",
        action="store_true",
        help="Use a single live row per table as the example (redacted). Default is synthetic examples.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if _OUTPUT_DIR.exists():
        shutil.rmtree(_OUTPUT_DIR)
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    entries: list[dict[str, str]] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            schemas = _list_schemas(cur)
            for schema in schemas:
                for table in _list_tables(cur, schema):
                    columns = _load_columns(cur, schema, table)
                    payload = {
                        "schema": schema,
                        "table": table,
                        "columns": [
                            {
                                "name": col.name,
                                "data_type": col.data_type,
                                "is_nullable": col.is_nullable,
                                "default": col.default,
                                "is_identity": col.is_identity,
                                "is_generated": col.is_generated,
                            }
                            for col in columns
                        ],
                        "primary_key": _load_primary_keys(cur, schema, table),
                        "foreign_keys": _load_foreign_keys(cur, schema, table),
                        "unique_constraints": _load_unique_constraints(cur, schema, table),
                        "indexes": _load_indexes(cur, schema, table),
                        "rls_enabled": _load_rls(cur, schema, table),
                        "example_row": _fetch_example_row(
                            cur,
                            schema,
                            table,
                            columns,
                            use_live_examples=bool(args.with_live_examples),
                        ),
                    }
                    _write_table_docs(schema, table, payload)
                    filename = f"{schema}.{table}.md"
                    entries.append({"schema": schema, "table": table, "filename": filename})

    _write_index(entries)
    print(f"Wrote schema docs for {len(entries)} tables to {_OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
