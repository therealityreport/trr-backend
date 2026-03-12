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


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def main() -> int:
    load_env()
    args = parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger(__name__)
    enabled = _env_flag("TRR_REDDIT_REFRESH_WORKER_ENABLED", True)
    worker_id = args.worker_id or f"reddit-refresh:{os.uname().nodename}:{os.getpid()}"
    logger.info(
        "[reddit_refresh_worker_boot] enabled=%s worker_id=%s once=%s poll_seconds=%.2f pid=%s",
        enabled,
        worker_id,
        bool(args.once),
        args.poll_seconds,
        os.getpid(),
    )
    if not enabled:
        logger.info("[reddit_refresh_worker_disabled] worker_id=%s", worker_id)
        return 0

    try:
        exit_code = run_reddit_refresh_worker_loop(
            worker_id=worker_id,
            poll_seconds=args.poll_seconds,
            once=bool(args.once),
        )
    except KeyboardInterrupt:
        logger.info("[reddit_refresh_worker_interrupted] worker_id=%s", worker_id)
        return 130
    except Exception:  # noqa: BLE001
        logger.exception("[reddit_refresh_worker_crashed] worker_id=%s", worker_id)
        return 1

    logger.info("[reddit_refresh_worker_exit] worker_id=%s exit_code=%s", worker_id, exit_code)
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
