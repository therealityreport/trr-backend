#!/usr/bin/env python3
"""Social ingest queue worker (Postgres-backed via social.scrape_jobs)."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import time
from datetime import UTC, datetime

from trr_backend.repositories.social_season_analytics import (
    execute_run,
    process_next_queued_job,
)

logger = logging.getLogger("socials.worker")


def _build_worker_id(explicit: str | None) -> str:
    if explicit:
        return explicit
    host = socket.gethostname()
    pid = os.getpid()
    return f"social-worker:{host}:{pid}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process queued social ingest jobs.")
    parser.add_argument("--worker-id", default=None, help="Explicit worker id")
    parser.add_argument("--interval", type=float, default=2.0, help="Idle sleep interval in seconds")
    parser.add_argument("--once", action="store_true", help="Process at most one job then exit")
    parser.add_argument("--run-id", default=None, help="Execute one specific run id then exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    worker_id = _build_worker_id(args.worker_id)
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info("Starting socials worker: worker_id=%s", worker_id)

    if args.run_id:
        logger.info("Executing specific run_id=%s", args.run_id)
        execute_run(args.run_id, worker_id=worker_id)
        return 0

    processed = 0
    while True:
        started = datetime.now(tz=UTC)
        job = process_next_queued_job(worker_id=worker_id)
        if job:
            processed += 1
            logger.info(
                "Processed job=%s run_id=%s platform=%s status=%s items=%s elapsed=%.2fs",
                job.get("id"),
                job.get("run_id"),
                job.get("platform"),
                job.get("status"),
                job.get("items_found"),
                (datetime.now(tz=UTC) - started).total_seconds(),
            )
            if args.once:
                break
            continue

        if args.once:
            logger.info("No queued jobs found")
            break
        time.sleep(max(0.25, args.interval))

    logger.info("Worker exiting: processed=%d", processed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
