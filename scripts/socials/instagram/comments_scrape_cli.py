#!/usr/bin/env python3
"""Local debug entrypoint for the Instagram Scrapling comments lane.

Single asyncio.run() — fetcher is created, warmed up, used, and closed
within one event loop (same lifetime model as job_runner).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from trr_backend.utils.env import load_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug the Instagram Scrapling comments lane.")
    parser.add_argument("--account", required=True, help="Instagram account handle")
    parser.add_argument("--shortcode", required=True, help="Instagram post shortcode")
    parser.add_argument("--max-comments", type=int, default=200, help="Maximum top-level comments to fetch")
    parser.add_argument("--no-replies", action="store_true", help="Skip reply fetching")
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> int:
    from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher
    from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
    from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session

    session = resolve_comments_scrapling_session(
        browser_account_id=args.account,
        caller_context=f"comments_scrape_cli:{args.account}",
    )
    proxy_config = select_comments_proxy()
    fetcher = InstagramCommentsScraplingFetcher(
        cookies=session.cookies,
        raw_cookies=session.auth_session.cookies,
        browser_account_id=session.browser_account_id,
        proxy_config=proxy_config,
    )
    try:
        await fetcher.warmup()
        result = await fetcher.fetch_comments_for_shortcode(
            args.shortcode,
            max_comments=max(1, int(args.max_comments)),
            fetch_replies=not args.no_replies,
        )
        print(
            {
                "shortcode": args.shortcode,
                "comments_fetched": len(result.comments),
                "fetch_failed": result.fetch_failed,
                "auth_failed": result.auth_failed,
                "fetch_reason": result.fetch_reason,
                "request_count": result.request_count,
                "retryable": result.retryable,
                "runtime_metadata": fetcher.runtime_metadata,
            }
        )
        return 0 if not result.auth_failed else 1
    finally:
        await fetcher.aclose()


def main() -> int:
    load_env()
    args = parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
