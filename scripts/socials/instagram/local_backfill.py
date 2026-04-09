#!/usr/bin/env python3
"""Run a local Instagram catalog backfill with interactive Chrome login support.

Bypasses the API server entirely — calls the backfill pipeline directly in-process.
If auth fails mid-scrape, the interactive Chrome login (entertainmentdatagroup@gmail.com)
will open a headed browser for manual login.

Usage:
    python scripts/socials/instagram/local_backfill.py --account bravotv
    python scripts/socials/instagram/local_backfill.py --account bravotv --source-scope bravo
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

# Load .env before any trr_backend imports
from dotenv import load_dotenv

load_dotenv(REPO_ROOT / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("local_backfill")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--account", required=True, help="Instagram handle (e.g. bravotv)")
    parser.add_argument("--source-scope", default="bravo", choices=["bravo", "creator", "community"])
    parser.add_argument("--backfill-scope", default="full_history", choices=["full_history", "bounded_window"])
    args = parser.parse_args()

    from trr_backend.socials.control_plane import (
        execute_run_with_inline_worker_registration,
        start_social_account_catalog_backfill,
    )

    logger.info("Starting LOCAL backfill for @%s (scope=%s)", args.account, args.backfill_scope)
    logger.info("Interactive Chrome login is ENABLED for auth recovery")

    try:
        result = start_social_account_catalog_backfill(
            platform="instagram",
            account_handle=args.account,
            source_scope=args.source_scope,
            initiated_by="local-script:local_backfill.py",
            inline_worker_id="local-script:catalog:instagram",
            allow_local_dev_inline_bypass=True,
            catalog_action="backfill",
            catalog_action_scope=args.backfill_scope,
        )
        run_id = str(result.get("run_id") or "").strip()
        logger.info("Run created: %s — now executing inline", run_id)

        if run_id:
            execute_run_with_inline_worker_registration(
                run_id,
                worker_id="local-script:catalog:instagram:1",
            )
            logger.info("Inline execution completed for run %s", run_id)
        else:
            logger.error("No run_id returned — cannot execute inline")
            return 1

        return 0
    except Exception as exc:
        logger.error("Backfill failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
