#!/usr/bin/env python3
"""Remote claim-loop worker for operation-backed admin streams."""

from __future__ import annotations

import argparse
import logging
import os

from trr_backend.pipeline.admin_operations import run_remote_operation_worker_loop
from trr_backend.utils.env import load_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run remote admin operations worker loop")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--operation-type",
        action="append",
        default=[],
        help="Optional operation_type filter (repeatable)",
    )
    parser.add_argument(
        "--exclude-operation-type",
        action="append",
        default=[],
        help="Optional operation_type exclusion filter (repeatable)",
    )
    return parser.parse_args()


def _env_operation_types(name: str) -> list[str]:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def main() -> int:
    load_env()
    args = parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return run_remote_operation_worker_loop(
        worker_id=args.worker_id,
        operation_types=(args.operation_type or _env_operation_types("TRR_ADMIN_OPERATION_WORKER_TYPES")) or None,
        exclude_operation_types=(
            args.exclude_operation_type or _env_operation_types("TRR_ADMIN_OPERATION_WORKER_EXCLUDE_TYPES")
        )
        or None,
        poll_seconds=args.poll_seconds,
        once=bool(args.once),
    )


if __name__ == "__main__":
    raise SystemExit(main())
