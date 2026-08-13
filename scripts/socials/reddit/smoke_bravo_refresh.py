#!/usr/bin/env python3
"""Bounded Reddit Bravo refresh smoke with saved-row proof."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Execute the live bounded refresh.")
    parser.add_argument("--community-id", help="Reddit community id for the Bravo target.")
    parser.add_argument("--season-id", help="Season id to attach matches to.")
    parser.add_argument("--period-key", help="Period key. Defaults to a unique smoke key.")
    parser.add_argument("--subreddit", default="BravoRealHousewives", help="Subreddit to fetch.")
    parser.add_argument("--show-name", default="Bravo", help="Show name used by the matcher.")
    parser.add_argument("--show-alias", action="append", default=["Bravo", "Bravo TV"], help="Show alias.")
    parser.add_argument("--sort-mode", action="append", default=["new"], help="Reddit sort mode.")
    parser.add_argument("--max-pages", type=int, default=1, help="Maximum pages per sort mode.")
    parser.add_argument("--limit-per-mode", type=int, default=10, help="Maximum rows per sort mode.")
    parser.add_argument("--fetch-comments", action="store_true", help="Also fetch comments. Off by default.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def build_payload(args: argparse.Namespace) -> dict[str, object]:
    smoke_id = uuid4().hex[:12]
    period_key = (
        str(args.period_key or "").strip()
        or f"bravo-reddit-smoke-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{smoke_id}"
    )
    return {
        "community_id": str(args.community_id or "").strip(),
        "season_id": str(args.season_id or "").strip(),
        "period_key": period_key,
        "subreddit": str(args.subreddit or "").strip(),
        "mode": "sync_posts",
        "sort_modes": [str(mode).strip() for mode in args.sort_mode if str(mode).strip()],
        "max_pages": max(1, int(args.max_pages or 1)),
        "limit_per_mode": max(1, int(args.limit_per_mode or 10)),
        "fetch_comments": bool(args.fetch_comments),
        "search_backfill": False,
        "exhaustive_window": False,
        "show_name": str(args.show_name or "Bravo").strip() or "Bravo",
        "show_aliases": [str(alias).strip() for alias in args.show_alias if str(alias).strip()],
        "smoke": True,
        "smoke_id": smoke_id,
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = build_payload(args)
    if not args.run:
        print(json.dumps({"dry_run": True, "payload": payload}, indent=2 if args.pretty else None, sort_keys=True))
        return 0
    missing = [key for key in ("community_id", "season_id") if not str(payload.get(key) or "").strip()]
    if missing:
        parser.error("--run requires --community-id and --season-id")

    load_dotenv(REPO_ROOT / ".env", override=False)
    apply_workspace_runtime_env(repo_root=REPO_ROOT)

    from trr_backend.repositories import reddit_refresh

    run_row = reddit_refresh.create_or_reuse_refresh_run(payload=payload)
    run_id = str(run_row.get("id") or run_row.get("run_id") or "").strip()
    if not run_id:
        raise RuntimeError("reddit_refresh_run_id_missing")
    result = reddit_refresh.execute_refresh_run(run_id, worker_id="local-script:reddit-bravo-smoke")
    proof = reddit_refresh.build_reddit_refresh_save_proof(run_id)
    output = {"run_id": run_id, "result": result, "save_proof": proof}
    print(json.dumps(output, indent=2 if args.pretty else None, sort_keys=True, default=str))
    return 0 if proof.get("verified") else 2


if __name__ == "__main__":
    raise SystemExit(main())
