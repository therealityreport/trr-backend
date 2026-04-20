"""FK index presence/invalid/duplicate check runner.

Thin wrapper around the parameterized check SQL files that `run_sql.sh`
cannot execute because they expect a Python-bound jsonb payload. The
`compare` subcommand is a thin passthrough to the observer's existing
`compare_baseline` function so callers have a single entry point for the
wave verification loop.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

from scripts._db_url import resolve_direct_db_url
from scripts.db.run_fk_index_observer import (
    _planned_indexes_from_inventory,
    _read_sql,
    compare_baseline,
    load_inventory,
    write_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DB = REPO_ROOT / "scripts" / "db"
DOCS_ROOT = REPO_ROOT / "docs" / "db" / "fk-index-hardening"

CHECK_SPECS: dict[str, dict[str, str]] = {
    "presence": {"sql": "fk_index_presence_check.sql", "param": "expected_indexes_json"},
    "invalid": {"sql": "fk_index_invalid_check.sql", "param": "planned_indexes_json"},
    "duplicate": {"sql": "fk_index_duplicate_check.sql", "param": "expected_indexes_json"},
}


def _connect():
    resolved = resolve_direct_db_url()
    return psycopg2.connect(resolved.value, cursor_factory=psycopg2.extras.RealDictCursor)


def _inventory_path_for_wave(wave_name: str) -> Path:
    return DOCS_ROOT / f"{wave_name}-inventory.yml"


def run_check(*, kind: str, wave_name: str, output_path: Path) -> Path:
    """Run a parameterized FK-index check and write results to CSV.

    `kind` must be a key in `CHECK_SPECS`. The wave inventory's `add`
    entries are serialized into the jsonb parameter expected by the SQL.
    """

    if kind not in CHECK_SPECS:
        raise ValueError(f"Unsupported check kind {kind!r}; expected one of {sorted(CHECK_SPECS)}.")
    spec = CHECK_SPECS[kind]
    inventory = load_inventory(_inventory_path_for_wave(wave_name))
    planned = _planned_indexes_from_inventory(inventory)
    payload: dict[str, Any] = {spec["param"]: json.dumps(planned)}
    sql_text = _read_sql(SCRIPTS_DB / spec["sql"])
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql_text, payload)
        rows = [dict(row) for row in cur.fetchall()]
    write_csv(output_path, rows)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FK index presence/invalid/duplicate check runner.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for kind in CHECK_SPECS:
        sub = subparsers.add_parser(kind, help=f"Run the {kind} check against a wave inventory.")
        sub.add_argument("--wave", required=True, choices=["wave-1", "wave-2"])
        sub.add_argument("--output", required=True)

    compare = subparsers.add_parser("compare", help="Compare baseline and post pg_stat_statements snapshots.")
    compare.add_argument("--baseline-csv", required=True)
    compare.add_argument("--post-csv", required=True)
    compare.add_argument("--output", required=True)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command in CHECK_SPECS:
        run_check(
            kind=args.command,
            wave_name=args.wave,
            output_path=Path(args.output),
        )
        return 0
    if args.command == "compare":
        compare_baseline(
            baseline_csv=Path(args.baseline_csv),
            post_csv=Path(args.post_csv),
            output_path=Path(args.output),
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
