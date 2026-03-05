#!/usr/bin/env python3
"""Remote claim-loop worker for reddit refresh runs."""

from __future__ import annotations

import argparse
import logging
import os

from trr_backend.repositories.reddit_refresh import run_reddit_refresh_worker_loop
from trr_backend.utils.env import load_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run reddit refresh worker loop")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> int:
    load_env()
    args = parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return run_reddit_refresh_worker_loop(
        worker_id=args.worker_id,
        poll_seconds=args.poll_seconds,
        once=bool(args.once),
    )


if __name__ == "__main__":
    raise SystemExit(main())
