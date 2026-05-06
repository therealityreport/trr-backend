#!/usr/bin/env python3
"""Local debug entrypoint for the Instagram Scrapling comments lane.

Single asyncio.run() — fetcher is created, warmed up, used, and closed
within one event loop (same lifetime model as job_runner).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from trr_backend.utils.env import load_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug the Instagram Scrapling comments lane.")
    parser.add_argument("--account", help="Instagram account handle")
    parser.add_argument("--shortcode", help="Instagram post shortcode")
    parser.add_argument("--fixture", help="Parse a sanitized comments fixture instead of calling Instagram")
    parser.add_argument("--max-comments", type=int, default=200, help="Maximum top-level comments to fetch")
    parser.add_argument("--expected-comment-count", type=int, help="Reported Instagram comment count for gap math")
    parser.add_argument("--no-replies", action="store_true", help="Skip reply fetching")
    args = parser.parse_args()
    if not args.fixture and (not args.account or not args.shortcode):
        parser.error("--account and --shortcode are required unless --fixture is provided")
    return args


def _phase_counts(comments: list[Any]) -> dict[str, int]:
    counts: Counter[str] = Counter()

    def visit(comment: Any) -> None:
        counts[str(getattr(comment, "phase", "") or "unknown")] += 1
        for reply in list(getattr(comment, "replies", []) or []):
            visit(reply)

    for comment in comments:
        visit(comment)
    return dict(sorted(counts.items()))


def _flattened_count(comments: list[Any]) -> int:
    total = 0

    def visit(comment: Any) -> None:
        nonlocal total
        total += 1
        for reply in list(getattr(comment, "replies", []) or []):
            visit(reply)

    for comment in comments:
        visit(comment)
    return total


def _capture_gap(*, expected_comment_count: int | None, observed_count: int) -> int | None:
    if expected_comment_count is None:
        return None
    return max(0, int(expected_comment_count) - max(0, int(observed_count)))


def _print_report(report: dict[str, Any]) -> None:
    print(json.dumps(report, indent=2, sort_keys=True))


def _run_fixture(args: argparse.Namespace) -> int:
    from trr_backend.socials.instagram.comments_scrapling.fetcher import (
        _extract_fb_crosspost_comment_rows,
        _extract_top_level_page_envelope,
        _fb_crosspost_comment_to_instagram_comment,
    )
    from trr_backend.socials.instagram.scraper import InstagramScraper

    fixture_path = Path(args.fixture).expanduser()
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    response: dict[str, Any] = {}
    envelope = _extract_top_level_page_envelope(payload, response)
    shortcode = args.shortcode or "FIXTURE"
    post_url = f"https://www.instagram.com/p/{shortcode}/"
    parser = InstagramScraper(cookies={"sessionid": "fixture"}, browser_account_id="fixture")
    page_phase = envelope.phase_signal or "ranked"
    cursor_payload = dict(envelope.cursor_payload)
    if envelope.primary_cursor and envelope.primary_cursor_param:
        cursor_payload["chosen_cursor_param"] = envelope.primary_cursor_param
    comments = [
        parser.parse_comment(
            row,
            shortcode,
            post_url,
            phase=page_phase,
            cursor_param=envelope.primary_cursor_param,
            cursor_min_id=envelope.primary_cursor,
            cursor_payload=cursor_payload,
            comment_filter_param=envelope.comment_filter_param,
        )
        for row in envelope.rows
    ]
    for row in _extract_fb_crosspost_comment_rows(payload):
        fb_comment = _fb_crosspost_comment_to_instagram_comment(
            row,
            shortcode=shortcode,
            post_url=post_url,
            cursor_payload=cursor_payload,
            comment_filter_param=envelope.comment_filter_param,
        )
        if fb_comment is not None:
            comments.append(fb_comment)
    observed_count = _flattened_count(comments)
    _print_report(
        {
            "mode": "fixture",
            "shortcode": shortcode,
            "fixture": str(fixture_path),
            "comments_fetched": len(comments),
            "flattened_comments": observed_count,
            "phase_counts": _phase_counts(comments),
            "cursor_shapes": sorted(envelope.cursor_shape_names),
            "stop_reason": None,
            "capture_gap": _capture_gap(
                expected_comment_count=args.expected_comment_count,
                observed_count=observed_count,
            ),
            "comment_filter_param": envelope.comment_filter_param,
        }
    )
    return 0


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
            expected_comment_count=args.expected_comment_count,
        )
        observed_count = _flattened_count(result.comments)
        _print_report(
            {
                "mode": "live",
                "shortcode": args.shortcode,
                "comments_fetched": len(result.comments),
                "flattened_comments": observed_count,
                "fetch_failed": result.fetch_failed,
                "auth_failed": result.auth_failed,
                "stop_reason": result.fetch_reason,
                "request_count": result.request_count,
                "retryable": result.retryable,
                "phase_counts": result.diagnostic_metadata.get("phase_counts") or _phase_counts(result.comments),
                "cursor_shapes": sorted(
                    (result.diagnostic_metadata.get("cursor_shape_counts") or {}).keys()
                    or (fetcher.runtime_metadata.get("cursor_shape_counts", {}).get("top_level") or {}).keys()
                ),
                "capture_gap": _capture_gap(
                    expected_comment_count=args.expected_comment_count or result.reported_comment_count,
                    observed_count=observed_count,
                ),
                "comment_filter_param": result.diagnostic_metadata.get("comment_filter_param"),
            }
        )
        return 0 if not result.auth_failed else 1
    finally:
        await fetcher.aclose()


def main() -> int:
    load_env()
    args = parse_args()
    if args.fixture:
        return _run_fixture(args)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
