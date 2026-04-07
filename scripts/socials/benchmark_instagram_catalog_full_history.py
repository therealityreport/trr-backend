#!/usr/bin/env python3
"""Benchmark a true Instagram catalog full-history backfill run.

This starts a real catalog backfill run, polls progress until the run reaches a
terminal state, and then computes throughput metrics from the persisted job
rows. Use it to compare full-history backfill throughput across runtime images
or strategy changes.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


def _load_social_repo():
    from trr_backend.repositories import social_season_analytics as social_repo

    return social_repo


def _safe_rate(total: int, elapsed_seconds: float) -> float | None:
    if elapsed_seconds <= 0:
        return None
    return round((max(0, int(total)) / elapsed_seconds) * 60.0, 2)


def _collect_catalog_run_metrics(job_rows: list[dict[str, Any]], *, elapsed_seconds: float) -> dict[str, Any]:
    total_posts_checked = 0
    total_posts_saved = 0
    total_pages_scanned = 0
    transports: list[str] = []
    seen_transports: set[str] = set()
    discovery_pages = 0
    discovery_partitions = 0
    posts_pages = 0
    posts_upserted = 0
    partition_jobs = 0

    for row in job_rows:
        metadata = dict(row.get("metadata") or {})
        retrieval_meta = dict(metadata.get("retrieval_meta") or {})
        activity = dict(metadata.get("activity") or {})
        persist_counters = dict(metadata.get("persist_counters") or {})
        retrieval_persist_counters = dict(retrieval_meta.get("persist_counters") or {})
        row_posts_checked = max(int(activity.get("posts_checked") or 0), int(retrieval_meta.get("posts_checked") or 0))
        total_posts_checked += row_posts_checked
        posts_saved = int(persist_counters.get("posts_upserted") or 0)
        if posts_saved <= 0:
            posts_saved = int(retrieval_persist_counters.get("posts_upserted") or 0)
        if posts_saved <= 0:
            posts_saved = int(row.get("items_found") or 0)
        total_posts_saved += max(0, posts_saved)
        row_pages = max(int(activity.get("pages_scanned") or 0), int(retrieval_meta.get("pages_scanned") or 0))
        total_pages_scanned += row_pages
        for candidate in (
            retrieval_meta.get("retrieval_transport"),
            retrieval_meta.get("transport"),
            metadata.get("transport"),
        ):
            label = str(candidate or "").strip().lower()
            if label and label not in seen_transports:
                seen_transports.add(label)
                transports.append(label)
        # Phase-level accumulation
        stage = str(metadata.get("stage") or "").strip().lower()
        if stage == "shared_account_discovery":
            discovery_pages += row_pages
            discovery_partitions += max(0, int(row.get("items_found") or 0))
        elif stage == "shared_account_posts":
            posts_pages += row_pages
            posts_upserted += row_posts_checked
            partition_jobs += 1

    return {
        "elapsed_seconds": round(elapsed_seconds, 2),
        "total_posts_checked": total_posts_checked,
        "total_posts_saved": total_posts_saved,
        "pages_scanned": total_pages_scanned,
        "posts_per_minute": _safe_rate(total_posts_checked, elapsed_seconds),
        "pages_per_minute": _safe_rate(total_pages_scanned, elapsed_seconds),
        "transport_used": transports,
        "discovery_phase": {
            "pages_scanned": discovery_pages,
            "partitions_discovered": discovery_partitions,
        },
        "posts_phase": {
            "pages_scanned": posts_pages,
            "posts_upserted": posts_upserted,
        },
        "partition_jobs_total": partition_jobs,
    }


def _load_catalog_job_rows(run_id: str) -> list[dict[str, Any]]:
    social_repo = _load_social_repo()
    return social_repo.pg.fetch_all(
        """
        select
          id::text as id,
          job_type,
          status,
          items_found,
          metadata
        from social.scrape_jobs
        where run_id = %s::uuid
          and lower(coalesce(config->>'stage', metadata->>'stage', job_type, '')) in (
            'shared_account_discovery',
            'shared_account_posts'
          )
        order by created_at asc
        """,
        [run_id],
    )


def benchmark_instagram_catalog_full_history(
    *,
    account_handle: str = "bravotv",
    source_scope: str = "bravo",
    poll_seconds: float = 15.0,
    timeout_minutes: float = 30.0,
) -> dict[str, Any]:
    social_repo = _load_social_repo()

    started_at = datetime.now(UTC)
    t0 = time.monotonic()
    run_payload = social_repo.start_social_account_catalog_backfill(
        "instagram",
        account_handle,
        source_scope=source_scope,
    )
    run_id = str(run_payload.get("run_id") or "").strip()
    if not run_id:
        raise RuntimeError("Catalog benchmark did not return a run_id")

    deadline = t0 + max(1.0, float(timeout_minutes)) * 60.0
    latest_progress: dict[str, Any] | None = None
    while True:
        latest_progress = social_repo.get_social_account_catalog_run_progress("instagram", account_handle, run_id)
        run_status = str(latest_progress.get("run_status") or "").strip().lower()
        if run_status in {"completed", "failed", "cancelled"}:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.0, float(poll_seconds)))

    elapsed_seconds = time.monotonic() - t0
    job_rows = _load_catalog_job_rows(run_id)
    metrics = _collect_catalog_run_metrics(job_rows, elapsed_seconds=elapsed_seconds)
    progress = latest_progress or {}
    run_status = str(progress.get("run_status") or "unknown").strip().lower()

    return {
        "status": "ok" if run_status == "completed" else "timeout" if time.monotonic() >= deadline else run_status,
        "platform": "instagram",
        "account_handle": account_handle,
        "source_scope": source_scope,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "run_status": run_status,
        "run_state": progress.get("run_state"),
        "worker_runtime": progress.get("worker_runtime"),
        "post_progress": progress.get("post_progress"),
        "metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Instagram full-history catalog backfill throughput")
    parser.add_argument("--account", default="bravotv", help="Instagram handle to benchmark")
    parser.add_argument("--source-scope", default="bravo", help="Shared source scope")
    parser.add_argument("--poll-seconds", type=float, default=15.0, help="Progress poll interval in seconds")
    parser.add_argument("--timeout-minutes", type=float, default=30.0, help="Hard timeout for the benchmark run")
    parser.add_argument("--output", type=str, default=None, help="JSON output file path")
    args = parser.parse_args()

    payload = benchmark_instagram_catalog_full_history(
        account_handle=args.account,
        source_scope=args.source_scope,
        poll_seconds=args.poll_seconds,
        timeout_minutes=args.timeout_minutes,
    )

    output_path = args.output
    if output_path is None:
        output_dir = Path(__file__).resolve().parents[2] / "docs" / "ai" / "benchmarks"
        output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        output_path = str(output_dir / f"instagram_catalog_full_history_benchmark_{ts}.json")

    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)

    print(json.dumps(payload, indent=2, default=str))
    print(f"\nResults written to: {output_path}")
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
