#!/usr/bin/env python3
"""Verify shared-account catalog completeness for a specific social account."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="verify_shared_account_catalog",
        description="Verify catalog row, caption, and hashtag completeness for a shared social account.",
    )
    parser.add_argument("--platform", required=True, help="Platform name, for example instagram")
    parser.add_argument("--account", required=True, help="Account handle, for example bravotv")
    parser.add_argument("--run-id", help="Optional catalog backfill run UUID to verify against")
    parser.add_argument(
        "--expected-total-posts",
        type=int,
        help="Override the expected total posts denominator instead of deriving it from the run or profile summary",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the JSON payload")
    return parser.parse_args(argv if argv is not None else sys.argv[1:])


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()
    apply_workspace_runtime_env(repo_root=REPO_ROOT)

    from trr_backend.repositories.social_season_analytics import get_social_account_catalog_verification

    payload = get_social_account_catalog_verification(
        args.platform,
        args.account,
        run_id=args.run_id,
    )
    if args.expected_total_posts is not None:
        payload["expected_total_posts"] = max(0, int(args.expected_total_posts))
        payload["catalog_complete"] = payload["catalog_posts"] >= payload["expected_total_posts"]
        payload["caption_complete"] = payload["caption_rows"] >= payload["expected_total_posts"]
        payload["verified"] = (
            payload["catalog_complete"] and payload["caption_complete"] and payload["hashtag_counts_match"]
        )

    if args.pretty:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload, sort_keys=True))
    return 0 if bool(payload.get("verified")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
