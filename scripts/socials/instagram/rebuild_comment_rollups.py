#!/usr/bin/env python3
"""Rebuild persisted Instagram post comment-count rollups."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional in slim runtimes
    load_dotenv = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

if load_dotenv is not None:
    load_dotenv(REPO_ROOT / ".env", override=False)

from trr_backend.socials.read_models.account_profile.common import (  # noqa: E402
    rebuild_instagram_post_comment_rollups,
)


def _post_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh social.instagram_post_comment_rollups for Instagram posts.",
    )
    parser.add_argument(
        "--account-handle",
        help="Restrict the rebuild to posts owned by this Instagram account handle.",
    )
    parser.add_argument(
        "--post-id",
        action="append",
        default=[],
        help="Restrict the rebuild to a post UUID. Repeat for multiple posts.",
    )
    parser.add_argument(
        "--post-ids",
        help="Comma-separated post UUIDs. Combined with repeated --post-id values.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of target posts to rebuild.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count target posts without refreshing rollups.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    post_ids = [*args.post_id, *_post_ids(args.post_ids)]
    result = rebuild_instagram_post_comment_rollups(
        account_handle=args.account_handle,
        post_ids=post_ids,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        scope = "all comment-bearing Instagram posts"
        if result.get("account_handle"):
            scope = f"@{result['account_handle']}"
        if result.get("post_ids"):
            scope = f"{len(result['post_ids'])} explicit post(s)"
        dry_run = "yes" if result.get("dry_run") else "no"
        print(
            "Instagram comment rollup rebuild: "
            f"scope={scope} target_count={result['target_count']} "
            f"refreshed_count={result['refreshed_count']} dry_run={dry_run}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
