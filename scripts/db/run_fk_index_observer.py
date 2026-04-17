from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import yaml

from scripts._db_url import resolve_direct_db_url

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DB = REPO_ROOT / "scripts" / "db"


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


def _connect():
    resolved = resolve_direct_db_url()
    return psycopg2.connect(resolved.value, cursor_factory=psycopg2.extras.RealDictCursor)


def load_inventory(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} must contain a top-level mapping.")
    return data


def _query_patterns_for_wave(wave_name: str) -> list[dict[str, str]]:
    if wave_name == "wave-1":
        return [
            {"label": "core_admin", "pattern": "%from core.shows as s%"},
            {"label": "social", "pattern": "%from social.scrape_runs%"},
        ]
    if wave_name == "wave-2":
        return [
            {"label": "surveys", "pattern": "%from surveys.answers as a%"},
            {"label": "ml", "pattern": "%from ml.screentime_runs r%"},
            {"label": "pipeline", "pattern": "%from pipeline.socialblade_growth_data%"},
            {"label": "firebase_surveys", "pattern": "%from firebase_surveys.answers%"},
            {"label": "screenalytics", "pattern": "%from screenalytics.%"},
        ]
    raise RuntimeError(f"Unsupported wave name {wave_name!r}.")


def _planned_indexes_from_inventory(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    entries = inventory.get("entries") or []
    return [
        {
            "schema": entry["schema"],
            "table": entry["table"],
            "index_name": entry["proposed_index_name"],
            "columns": entry["proposed_index_columns"],
            "predicate": entry["proposed_partial_predicate"],
        }
        for entry in entries
        if entry.get("decision") == "add"
    ]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def snapshot(
    *,
    wave_name: str,
    inventory_path: Path,
    output_dir: Path,
    known_pids: list[int],
) -> Path:
    inventory = load_inventory(inventory_path)
    payload = {
        "query_patterns_json": json.dumps(_query_patterns_for_wave(wave_name)),
        "known_pids_json": json.dumps(known_pids),
        "planned_indexes_json": json.dumps(_planned_indexes_from_inventory(inventory)),
    }
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(_read_sql(SCRIPTS_DB / "fk_index_observer_snapshot.sql"), payload)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Observer snapshot returned no rows.")
    snapshot_json = row["snapshot"]
    captured_at = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = output_dir / captured_at
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for key in (
        "build_progress",
        "locks",
        "activity",
        "statement_stats",
        "table_stats",
        "index_stats",
        "invalid_indexes",
    ):
        write_csv(snapshot_dir / f"{key}.csv", list(snapshot_json.get(key) or []))
    return snapshot_dir


def write_baseline(
    *,
    wave_name: str,
    output_path: Path,
) -> Path:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            _read_sql(SCRIPTS_DB / "fk_index_baseline_snapshot.sql"),
            {"query_patterns_json": json.dumps(_query_patterns_for_wave(wave_name))},
        )
        rows = [dict(row) for row in cur.fetchall()]
    write_csv(output_path, rows)
    return output_path


def compare_baseline(
    *,
    baseline_csv: Path,
    post_csv: Path,
    output_path: Path,
) -> Path:
    def _read_csv(path: Path) -> list[dict[str, Any]]:
        if not path.exists() or not path.read_text(encoding="utf-8").strip():
            return []
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    baseline_rows = _read_csv(baseline_csv)
    post_rows = _read_csv(post_csv)

    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""
            create temporary table baseline_snapshot (
              label text,
              queryid text,
              calls bigint,
              total_exec_time double precision
            )
        """)
        cur.execute("""
            create temporary table post_snapshot (
              label text,
              queryid text,
              calls bigint,
              total_exec_time double precision
            )
        """)
        psycopg2.extras.execute_values(
            cur,
            "insert into baseline_snapshot (label, queryid, calls, total_exec_time) values %s",
            [
                (
                    row.get("label"),
                    row.get("queryid"),
                    int(row.get("calls") or 0),
                    float(row.get("total_exec_time") or 0.0),
                )
                for row in baseline_rows
            ]
            or [("__empty__", "0", 0, 0.0)],
        )
        if baseline_rows:
            cur.execute("delete from baseline_snapshot where label = '__empty__'")
        psycopg2.extras.execute_values(
            cur,
            "insert into post_snapshot (label, queryid, calls, total_exec_time) values %s",
            [
                (
                    row.get("label"),
                    row.get("queryid"),
                    int(row.get("calls") or 0),
                    float(row.get("total_exec_time") or 0.0),
                )
                for row in post_rows
            ]
            or [("__empty__", "0", 0, 0.0)],
        )
        if post_rows:
            cur.execute("delete from post_snapshot where label = '__empty__'")
        cur.execute(_read_sql(SCRIPTS_DB / "fk_index_compare_baseline.sql"))
        rows = [dict(row) for row in cur.fetchall()]
    write_csv(output_path, rows)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Observer tooling for FK index rollout.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    baseline = subparsers.add_parser("baseline", help="Capture pg_stat_statements baseline snapshot.")
    baseline.add_argument("--wave", required=True, choices=["wave-1", "wave-2"])
    baseline.add_argument("--output", required=True)

    compare = subparsers.add_parser("compare", help="Compare baseline and post snapshots.")
    compare.add_argument("--baseline-csv", required=True)
    compare.add_argument("--post-csv", required=True)
    compare.add_argument("--output", required=True)

    snapshot_parser = subparsers.add_parser("snapshot", help="Capture a single observer snapshot.")
    snapshot_parser.add_argument("--wave", required=True, choices=["wave-1", "wave-2"])
    snapshot_parser.add_argument("--inventory", required=True)
    snapshot_parser.add_argument("--output-dir", required=True)
    snapshot_parser.add_argument("--known-pid", action="append", type=int, default=[])

    loop = subparsers.add_parser("loop", help="Capture observer snapshots on an interval.")
    loop.add_argument("--wave", required=True, choices=["wave-1", "wave-2"])
    loop.add_argument("--inventory", required=True)
    loop.add_argument("--output-dir", required=True)
    loop.add_argument("--known-pid", action="append", type=int, default=[])
    loop.add_argument("--interval-sec", type=int, default=15)
    loop.add_argument("--iterations", type=int, default=0, help="0 means run forever.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "baseline":
        write_baseline(wave_name=args.wave, output_path=Path(args.output))
        return 0
    if args.command == "compare":
        compare_baseline(
            baseline_csv=Path(args.baseline_csv),
            post_csv=Path(args.post_csv),
            output_path=Path(args.output),
        )
        return 0
    if args.command == "snapshot":
        snapshot(
            wave_name=args.wave,
            inventory_path=Path(args.inventory),
            output_dir=Path(args.output_dir),
            known_pids=list(args.known_pid),
        )
        return 0
    if args.command == "loop":
        iteration = 0
        while True:
            snapshot(
                wave_name=args.wave,
                inventory_path=Path(args.inventory),
                output_dir=Path(args.output_dir),
                known_pids=list(args.known_pid),
            )
            iteration += 1
            if args.iterations and iteration >= args.iterations:
                return 0
            time.sleep(max(1, int(args.interval_sec)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
