from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg2
import psycopg2.extras
import yaml

from scripts._db_url import ResolvedDbUrl, resolve_db_url, resolve_direct_db_url

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DB = REPO_ROOT / "scripts" / "db"
DOCS_DB = REPO_ROOT / "docs" / "db" / "fk-index-hardening"
WAVE_SCHEMAS = {
    "wave-1": ["core", "admin", "social"],
    "wave-2": ["surveys", "firebase_surveys", "public", "screenalytics", "ml", "pipeline"],
}


def load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} must contain a top-level mapping.")
    return data


def _filter_owned_schemas(*, owned_schemas: list[str], wave_name: str | None) -> list[str]:
    if wave_name is None:
        return owned_schemas
    expected = WAVE_SCHEMAS[wave_name]
    filtered = [schema for schema in owned_schemas if schema in expected]
    missing = [schema for schema in expected if schema not in filtered]
    if missing:
        missing_csv = ", ".join(missing)
        raise RuntimeError(f"Owned schema config is missing wave schemas: {missing_csv}.")
    return filtered


def _read_sql(path: Path) -> str:
    rendered: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith(r"\i "):
            include_target = stripped[3:].strip()
            include_path = Path(include_target)
            if not include_path.is_absolute():
                include_path = REPO_ROOT / include_path
            rendered.append(_read_sql(include_path.resolve()))
            continue
        rendered.append(line)
    return "".join(rendered)


def _connect(resolved: ResolvedDbUrl):
    return psycopg2.connect(resolved.value, cursor_factory=psycopg2.extras.RealDictCursor)


def _resolve_connection_url(connection_mode: str) -> ResolvedDbUrl:
    if connection_mode == "direct":
        return resolve_direct_db_url()
    if connection_mode == "runtime":
        return resolve_db_url()
    raise RuntimeError(f"Unsupported connection mode {connection_mode!r}.")


def _fetch_rows(cur: psycopg2.extensions.cursor) -> list[dict[str, Any]]:
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def analyze_owned_tables(cur: psycopg2.extensions.cursor, owned_schemas: list[str]) -> list[str]:
    cur.execute(
        """
        select distinct
          quote_ident(n.nspname) || '.' || quote_ident(c.relname) as relation_name
        from pg_constraint con
        join pg_class c on c.oid = con.conrelid
        join pg_namespace n on n.oid = c.relnamespace
        where con.contype = 'f'
          and n.nspname = any(%s::text[])
        order by relation_name
        """,
        [owned_schemas],
    )
    analyzed: list[str] = []
    for row in _fetch_rows(cur):
        relation_name = str(row["relation_name"])
        cur.execute(f"analyze {relation_name}")
        analyzed.append(relation_name)
    return analyzed


def _rows_to_inventory(rows: list[dict[str, Any]]) -> dict[str, Any]:
    inventory_rows: list[dict[str, Any]] = []
    for row in rows:
        entry = {
            "schema": row["schema_name"],
            "table": row["table_name"],
            "constraint_name": row["constraint_name"],
            "fk_columns_in_order": row["fk_columns_in_order"] or [],
            "referenced_schema": row["referenced_schema"],
            "referenced_table": row["referenced_table"],
            "nullable_columns": row["nullable_columns"] or [],
            "estimated_row_count": int(row["estimated_row_count"] or 0),
            "single_column_null_frac": (
                float(row["single_column_null_frac"])
                if row.get("single_column_null_frac") is not None
                else None
            ),
            "hot_table": bool(row["hot_table"]),
            "covered_by_existing_index": bool(row["covered_by_existing_index"]),
            "proposed_index_name": row["proposed_index_name"],
            "proposed_index_columns": row["proposed_index_columns"] or [],
            "proposed_partial_predicate": row["proposed_partial_predicate"],
            "statement_timeout_tier": row["statement_timeout_tier"],
            "decision": row["decision"],
        }
        inventory_rows.append(entry)
    return {"entries": inventory_rows}


def generate_inventory(
    *,
    owned_schemas_path: Path,
    hot_tables_path: Path,
    output_path: Path,
    wave_name: str | None = None,
    connection_mode: str = "direct",
    refresh_stats: bool = True,
) -> dict[str, Any]:
    owned = load_yaml(owned_schemas_path)
    hot_tables = load_yaml(hot_tables_path)
    owned_schemas = list(owned.get("owned_schemas") or [])
    hot_table_entries = list(hot_tables.get("hot_tables") or [])
    if not owned_schemas:
        raise RuntimeError(f"{owned_schemas_path} must define `owned_schemas`.")
    owned_schemas = _filter_owned_schemas(owned_schemas=owned_schemas, wave_name=wave_name)

    resolved = _resolve_connection_url(connection_mode)
    with _connect(resolved) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            analyzed_tables = analyze_owned_tables(cur, owned_schemas) if refresh_stats else []
        with conn.cursor() as cur:
            cur.execute(
                _read_sql(SCRIPTS_DB / "fk_index_inventory.sql"),
                {
                    "owned_schemas_json": json.dumps(owned_schemas),
                    "hot_tables_json": json.dumps(hot_table_entries),
                },
            )
            rows = _fetch_rows(cur)

    payload = {
        "metadata": {
            "db_url_source": resolved.source,
            "connection_mode": connection_mode,
            "resolved_db_host": urlsplit(resolved.value).hostname,
            "wave": wave_name,
            "owned_schemas": owned_schemas,
            "stats_refreshed": refresh_stats,
            "analyzed_tables": analyzed_tables,
        },
        **_rows_to_inventory(rows),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate FK index inventory from the live direct DB.")
    parser.add_argument(
        "--owned-schemas",
        default=str(SCRIPTS_DB / "fk_index_owned_schemas.yml"),
        help="Path to the owned schema YAML.",
    )
    parser.add_argument(
        "--hot-tables",
        default=str(SCRIPTS_DB / "fk_index_hot_tables.yml"),
        help="Path to the hot tables YAML.",
    )
    parser.add_argument(
        "--output",
        default=str(DOCS_DB / "wave-1-inventory.yml"),
        help="Output YAML path.",
    )
    parser.add_argument(
        "--wave",
        choices=sorted(WAVE_SCHEMAS),
        help="Restrict output to a named rollout wave.",
    )
    parser.add_argument(
        "--connection-mode",
        choices=["direct", "runtime"],
        default="direct",
        help="Use the direct Supabase host by default; runtime uses the configured session-pooler/runtime DSN.",
    )
    parser.add_argument(
        "--skip-refresh-stats",
        action="store_true",
        help="Skip ANALYZE before reading pg_stats.null_frac.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    generate_inventory(
        owned_schemas_path=Path(args.owned_schemas),
        hot_tables_path=Path(args.hot_tables),
        output_path=Path(args.output),
        wave_name=args.wave,
        connection_mode=args.connection_mode,
        refresh_stats=not args.skip_refresh_stats,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
