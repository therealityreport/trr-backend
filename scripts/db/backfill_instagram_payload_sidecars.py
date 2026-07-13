#!/usr/bin/env python3
"""Bounded, resumable backfill for private Instagram payload sidecars."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - direct script convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env

Target = Literal["posts", "catalog"]
TARGETS: tuple[Target, ...] = ("posts", "catalog")


@dataclass(frozen=True, slots=True)
class TargetSpec:
    base_table: str
    sidecar_table: str
    pk_column: str
    payload_columns: tuple[str, ...]
    select_payload_sql: tuple[str, ...]
    seed_timestamp_sql: str


SPECS: dict[Target, TargetSpec] = {
    "posts": TargetSpec(
        base_table="social.instagram_posts",
        sidecar_table="social.instagram_post_payloads",
        pk_column="post_id",
        payload_columns=("raw_data", "asset_manifest", "child_posts_data"),
        select_payload_sql=("p.raw_data", "p.asset_manifest", "p.child_posts_data"),
        seed_timestamp_sql="coalesce(p.metadata_scraped_at, p.scraped_at, now())",
    ),
    "catalog": TargetSpec(
        base_table="social.instagram_account_catalog_posts",
        sidecar_table="social.instagram_account_catalog_post_payloads",
        pk_column="catalog_post_id",
        payload_columns=("raw_data", "child_posts_data"),
        select_payload_sql=("p.raw_data", "p.child_posts_data"),
        seed_timestamp_sql="coalesce(p.updated_at, p.last_seen_at, p.created_at, now())",
    ),
}


def _selected_targets(value: str) -> tuple[Target, ...]:
    return TARGETS if value == "all" else (value,)  # type: ignore[return-value]


def build_batch_sql(target: Target, *, dry_run: bool) -> str:
    spec = SPECS[target]
    selected_columns = ",\n        ".join(spec.select_payload_sql)
    parity = " and ".join(f"s.{column} is not distinct from p.{column}" for column in spec.payload_columns)
    eligible_where = f"s.{spec.pk_column} is null or not ({parity})"
    if dry_run:
        return f"""
        select p.id::text as id
        from {spec.base_table} p
        left join {spec.sidecar_table} s on s.{spec.pk_column} = p.id
        where ({eligible_where})
          and (%(after_id)s::uuid is null or p.id > %(after_id)s::uuid)
        order by p.id
        limit %(batch_size)s
        """

    insert_columns = ", ".join((spec.pk_column, *spec.payload_columns, "payload_updated_at"))
    select_columns = ", ".join(("batch.id", *[f"batch.{column}" for column in spec.payload_columns], "batch.seed_at"))
    update_assignments = ",\n          ".join(f"{column} = excluded.{column}" for column in spec.payload_columns)
    current_tuple = ", ".join(f"{spec.sidecar_table}.{column}" for column in spec.payload_columns)
    excluded_tuple = ", ".join(f"excluded.{column}" for column in spec.payload_columns)
    return f"""
    with batch as materialized (
      select
        p.id,
        {selected_columns},
        {spec.seed_timestamp_sql} as seed_at
      from {spec.base_table} p
      left join {spec.sidecar_table} s on s.{spec.pk_column} = p.id
      where ({eligible_where})
        and (%(after_id)s::uuid is null or p.id > %(after_id)s::uuid)
      order by p.id
      limit %(batch_size)s
      for update of p
    ), upserted as (
      insert into {spec.sidecar_table} ({insert_columns})
      select {select_columns}
      from batch
      on conflict ({spec.pk_column}) do update set
          {update_assignments},
          payload_updated_at = case
            when ({current_tuple}) is distinct from ({excluded_tuple}) then now()
            else {spec.sidecar_table}.payload_updated_at
          end
      returning {spec.pk_column}
    )
    select batch.id::text as id
    from batch
    order by batch.id
    """


def build_status_sql(target: Target) -> str:
    spec = SPECS[target]
    parity = " and ".join(f"s.{column} is not distinct from p.{column}" for column in spec.payload_columns)
    return f"""
    select
      (select count(*) from {spec.base_table})::bigint as base_count,
      (select count(*) from {spec.sidecar_table})::bigint as sidecar_count,
      (select count(*) from {spec.base_table} p left join {spec.sidecar_table} s
         on s.{spec.pk_column} = p.id where s.{spec.pk_column} is null)::bigint as missing_count,
      (select count(*) from {spec.sidecar_table} s left join {spec.base_table} p
         on p.id = s.{spec.pk_column} where p.id is null)::bigint as orphan_count,
      (select count(*) from {spec.base_table} p join {spec.sidecar_table} s
         on s.{spec.pk_column} = p.id where not ({parity}))::bigint as mismatch_count,
      (select count(*) from {spec.base_table} p left join {spec.sidecar_table} s
         on s.{spec.pk_column} = p.id
         where s.{spec.pk_column} is null or not ({parity}))::bigint as pending_count,
      (select p.id::text from {spec.base_table} p left join {spec.sidecar_table} s
         on s.{spec.pk_column} = p.id
         where s.{spec.pk_column} is null or not ({parity})
         order by p.id
         limit 1) as next_pending_id
    """


def _set_timeouts(conn: Any, *, lock_timeout_ms: int, statement_timeout_ms: int) -> None:
    with pg.db_cursor(conn=conn, label="instagram_payload_sidecar_backfill_timeouts") as cur:
        cur.execute("set local lock_timeout = %s", [f"{lock_timeout_ms}ms"])
        cur.execute("set local statement_timeout = %s", [f"{statement_timeout_ms}ms"])


def status(targets: tuple[Target, ...], *, lock_timeout_ms: int, statement_timeout_ms: int) -> dict[str, Any]:
    report: dict[str, Any] = {"mode": "status", "targets": {}}
    for target in targets:
        with pg.db_connection(label=f"instagram-payload-sidecar-status:{target}") as conn:
            _set_timeouts(conn, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)
            with pg.db_cursor(conn=conn) as cur:
                cur.execute(build_status_sql(target))
                target_report = dict(cur.fetchone() or {})
                target_report["converged"] = (
                    int(target_report.get("pending_count") or 0) == 0
                    and int(target_report.get("orphan_count") or 0) == 0
                    and int(target_report.get("base_count") or 0) == int(target_report.get("sidecar_count") or 0)
                )
                report["targets"][target] = target_report
    return report


def run_target(
    target: Target,
    *,
    after_id: str | None,
    batch_size: int,
    max_rows: int,
    dry_run: bool,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
) -> dict[str, Any]:
    processed = 0
    cursor = after_id
    last_processed_id = after_id
    batches = 0
    wrapped_to_start = False
    exhausted = False
    while processed < max_rows:
        current_batch_size = min(batch_size, max_rows - processed)
        with pg.db_connection(label=f"instagram-payload-sidecar-backfill:{target}") as conn:
            _set_timeouts(conn, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)
            with pg.db_cursor(conn=conn) as cur:
                cur.execute(
                    build_batch_sql(target, dry_run=dry_run),
                    {"after_id": cursor, "batch_size": current_batch_size},
                )
                rows = [dict(row) for row in (cur.fetchall() or [])]
        if not rows:
            # UUIDs are not chronological and sidecars can be sparse. A cursor
            # is only a preferred starting point: wrap to the lowest eligible
            # UUID after every exhausted forward scan. Repeating the wrap after
            # progress also catches a lower row that became eligible while the
            # bounded run was active.
            if cursor is not None:
                cursor = None
                wrapped_to_start = True
                continue
            exhausted = True
            break
        cursor = str(rows[-1]["id"])
        last_processed_id = cursor
        processed += len(rows)
        batches += 1
        if dry_run:
            break
    return {
        "target": target,
        "dry_run": dry_run,
        "rows_processed": processed,
        "batches_committed": 0 if dry_run else batches,
        "last_processed_id": last_processed_id,
        "wrapped_to_start": wrapped_to_start,
        "complete_for_bound": exhausted,
        "bound_exhausted": not exhausted and processed >= max_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("command", choices=("status", "run"))
    parser.add_argument("--target", choices=("all", *TARGETS), default="all")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument(
        "--after-id",
        help="Optional cursor for this invocation; omit it to start at the lowest missing or mismatched base row.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview one bounded keyset batch without writing.")
    parser.add_argument("--lock-timeout-ms", type=int, default=1000)
    parser.add_argument("--statement-timeout-ms", type=int, default=15000)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = build_parser().parse_args(argv)
    targets = _selected_targets(args.target)
    lock_timeout_ms = max(100, int(args.lock_timeout_ms))
    statement_timeout_ms = max(1000, int(args.statement_timeout_ms))
    if args.command == "status":
        result = status(
            targets,
            lock_timeout_ms=lock_timeout_ms,
            statement_timeout_ms=statement_timeout_ms,
        )
    else:
        result = {
            "mode": "run",
            "results": [
                run_target(
                    target,
                    after_id=args.after_id,
                    batch_size=max(1, min(1000, int(args.batch_size))),
                    max_rows=max(1, int(args.max_rows)),
                    dry_run=bool(args.dry_run),
                    lock_timeout_ms=lock_timeout_ms,
                    statement_timeout_ms=statement_timeout_ms,
                )
                for target in targets
            ],
        }
    print(json.dumps(result, indent=2, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

