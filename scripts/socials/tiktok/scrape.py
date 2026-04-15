#!/usr/bin/env python3
"""
CLI script for scraping TikTok posts and comments.

Usage:
    # Scrape posts
    python -m scripts.socials.tiktok.scrape --username bravotv --hashtags RHOSLC --start 2025-08-14 --end 2026-02-04

    # Scrape comments from a specific video
    python -m scripts.socials.tiktok.scrape --comments --video 7123456789012345678 --video-user bravotv
    python -m scripts.socials.tiktok.scrape --comments --video 7123456789012345678 --with-replies --max-comments 100
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

# Add project root to path (scripts/socials/tiktok -> project root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from trr_backend.socials.tiktok import (
    TikTokComment,
    TikTokPost,
    TikTokScrapeConfig,
    TikTokScraper,
)
from trr_backend.utils.env import load_env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d")


def _proxy_label(proxy_url: str | None) -> str | None:
    if not proxy_url:
        return None
    parsed = urlparse(str(proxy_url))
    return parsed.hostname or str(proxy_url)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape TikTok posts and comments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape @bravotv posts with #RHOSLC
  python -m scripts.socials.tiktok.scrape --username bravotv --hashtags RHOSLC --start 2025-08-14 --end 2026-02-04

  # Scrape comments from a specific video
  python -m scripts.socials.tiktok.scrape --comments --video 7123456789012345678 --video-user bravotv

  # Scrape comments with replies and limit
  python -m scripts.socials.tiktok.scrape --comments --video 7123456789012345678 --with-replies --max-comments 100
        """,
    )

    parser.add_argument("--username", help="TikTok username to scrape (without @)")
    parser.add_argument("--hashtags", nargs="+", help="Hashtags to filter by (without #)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--scrape-mode",
        choices=("ytdlp", "api", "browser_intercept", "auto"),
        default="ytdlp",
        help=(
            "Post scrape mode. Use 'ytdlp' for the production path. "
            "'api' and 'browser_intercept' are experimental direct modes; "
            "'auto' is a deprecated compatibility alias for 'ytdlp'."
        ),
    )
    parser.add_argument("--comments", action="store_true", help="Scrape comments instead of posts")
    parser.add_argument("--video", help="Video ID to scrape comments from (aweme_id)")
    parser.add_argument("--video-user", help="Username of the video author (optional, for building URL)")
    parser.add_argument("--with-replies", action="store_true", help="Also fetch replies to comments")
    parser.add_argument("--max-comments", type=int, help="Maximum number of top-level comments to fetch")
    parser.add_argument("--cookies", default="tiktok_cookies.json", help="Path to TikTok cookies JSON file")
    parser.add_argument("--no-auth", action="store_true", help="Run without authentication (may limit availability)")
    parser.add_argument("--output", help="Output file prefix (default: {username}_tiktok_posts or {video_id}_comments)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between requests in seconds (default: 2.0)")
    parser.add_argument("--max-pages", type=int, help="Maximum number of pages to fetch (posts only)")
    parser.add_argument("--show-id", type=int, help="Associated show ID for metadata")
    parser.add_argument("--season", type=int, help="Associated season number for metadata")
    parser.add_argument("--person-id", type=int, help="Associated person ID for metadata")
    parser.add_argument(
        "--http-client",
        choices=("requests", "curl_cffi"),
        help="HTTP transport for experimental direct TikTok requests only",
    )
    parser.add_argument(
        "--proxy-url",
        help="Single proxy URL for the experimental direct TikTok scraper transport only",
    )
    parser.add_argument("--diagnostics-json", help="Optional path to write scraper diagnostics JSON")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return _build_parser().parse_args(argv)


def _emit_diagnostics_summary(
    *,
    target_label: str,
    scrape_mode: str,
    diagnostics: dict[str, object],
) -> None:
    endpoint_responses = diagnostics.get("endpoint_responses") if isinstance(diagnostics, dict) else {}
    endpoint_responses = endpoint_responses if isinstance(endpoint_responses, dict) else {}
    failure_summary = {
        endpoint: payload.get("failure_reason")
        for endpoint, payload in endpoint_responses.items()
        if isinstance(payload, dict) and payload.get("failure_reason")
    }

    print("\nDiagnostics:")
    print(f"  Target: {target_label}")
    print(f"  Scrape mode: {scrape_mode}")
    print(f"  HTTP client: {diagnostics.get('http_client') or 'requests'}")
    if diagnostics.get("curl_cffi_impersonate"):
        print(f"  Impersonate: {diagnostics.get('curl_cffi_impersonate')}")
    print(f"  Proxy: {diagnostics.get('proxy_label') or 'none'}")
    print(f"  Auth mode: {diagnostics.get('auth_mode') or 'without_cookies'}")
    if diagnostics.get("risk_state"):
        print(f"  Risk state: {diagnostics.get('risk_state')}")
    if diagnostics.get("operator_summary"):
        print(f"  Operator summary: {diagnostics.get('operator_summary')}")
    if diagnostics.get("operator_action"):
        print(f"  Operator action: {diagnostics.get('operator_action')}")
    if diagnostics.get("triage_bucket"):
        print(f"  Triage bucket: {diagnostics.get('triage_bucket')}")
    if diagnostics.get("stop_reason"):
        print(f"  Stop reason: {diagnostics.get('stop_reason')}")
    if failure_summary:
        print(f"  Endpoint failures: {json.dumps(failure_summary, sort_keys=True)}")


def _write_diagnostics_json(path: str | None, diagnostics: dict[str, object]) -> None:
    if not path:
        return
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(diagnostics, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def save_results(posts: list[TikTokPost], output_prefix: str):
    """Save posts to JSON and CSV files."""
    if not posts:
        logger.warning("No posts to save")
        return

    # Sort by date (newest first)
    posts_sorted = sorted(posts, key=lambda p: p.create_time, reverse=True)
    posts_dicts = [p.to_dict() for p in posts_sorted]

    # Save JSON
    json_file = f"{output_prefix}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(posts_dicts, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Saved {len(posts)} posts to {json_file}")

    # Save CSV
    csv_file = f"{output_prefix}.csv"
    fieldnames = [
        "date_time",
        "description",
        "hashtags",
        "mentions",
        "likes",
        "comments",
        "shares",
        "saves",
        "views",
        "duration",
        "video_id",
        "url",
        "username",
        "author_nickname",
        "music_title",
        "music_author",
    ]

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for post in posts_dicts:
            row = post.copy()
            row["hashtags"] = ", ".join(f"#{t}" for t in post.get("hashtags", []))
            row["mentions"] = ", ".join(f"@{t}" for t in post.get("mentions", []))
            writer.writerow(row)
    logger.info(f"Saved {len(posts)} posts to {csv_file}")


def save_comments(comments: list[TikTokComment], output_prefix: str, video_id: str):
    """Save comments to JSON and CSV files."""
    if not comments:
        logger.warning("No comments to save")
        return

    # Flatten comments and replies for counting
    def count_all(comment_list: list) -> int:
        total = len(comment_list)
        for c in comment_list:
            total += count_all(c.replies)
        return total

    total_count = count_all(comments)
    comments_dicts = [c.to_dict() for c in comments]

    # Save JSON (nested structure)
    json_file = f"{output_prefix}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(comments_dicts, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Saved {total_count} comments to {json_file}")

    # Save CSV (flattened)
    csv_file = f"{output_prefix}.csv"
    fieldnames = [
        "comment_id",
        "date_time",
        "username",
        "nickname",
        "text",
        "likes",
        "is_reply",
        "parent_comment_id",
        "reply_count",
        "video_id",
        "post_url",
    ]

    def flatten_comments(comment_list: list, rows: list):
        for c in comment_list:
            row = {
                "comment_id": c.comment_id,
                "date_time": c.date_time,
                "username": c.username,
                "nickname": c.nickname,
                "text": c.text,
                "likes": c.likes,
                "is_reply": c.is_reply,
                "parent_comment_id": c.parent_comment_id or "",
                "reply_count": c.reply_count,
                "video_id": c.video_id,
                "post_url": c.post_url,
            }
            rows.append(row)
            flatten_comments(c.replies, rows)

    rows: list[dict] = []
    flatten_comments(comments, rows)

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved {len(rows)} comments to {csv_file}")


def main(argv: list[str] | None = None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    env_path = load_env()
    if env_path:
        logger.debug("Loaded environment from %s", env_path)

    # Create scraper
    tiktok_cookies: dict[str, str] = {}
    if not args.no_auth:
        try:
            from trr_backend.socials.control_plane import _load_tiktok_cookies

            if args.cookies and args.cookies != "tiktok_cookies.json":
                os.environ["SOCIAL_TIKTOK_COOKIES_FILE"] = str(Path(args.cookies).expanduser())
            tiktok_cookies = _load_tiktok_cookies()
            if tiktok_cookies:
                logger.info("Loaded TikTok cookies via canonical repo auth loader")
            else:
                logger.warning("No TikTok cookies resolved; continuing unauthenticated")
        except Exception as exc:
            logger.error("Failed to load TikTok cookies: %s", exc)
            logger.warning("Running TikTok scrape without authenticated cookies")
    scraper = TikTokScraper(
        cookies=tiktok_cookies,
        http_client_name=args.http_client,
        proxy_urls=[args.proxy_url] if args.proxy_url else None,
    )

    # Comment scraping mode
    if args.comments:
        if not args.video:
            parser.error("--video is required when using --comments")

        video_id = args.video
        logger.info(f"Fetching comments for video: {video_id}")

        comments = scraper.fetch_comments(
            video_id=video_id,
            username=args.video_user,
            max_comments=args.max_comments,
            fetch_replies=args.with_replies,
            delay=args.delay,
        )

        # Count all comments including replies
        def count_all(comment_list: list) -> int:
            total = len(comment_list)
            for c in comment_list:
                total += count_all(c.replies)
            return total

        total_count = count_all(comments)
        reply_count = total_count - len(comments)

        print("\n" + "=" * 60)
        print(f"SUMMARY: Found {len(comments)} top-level comments")
        if args.with_replies:
            print(f"         + {reply_count} replies = {total_count} total")
        print("=" * 60)

        if comments:
            print("\nPreview (first 5 comments):")
            for i, comment in enumerate(comments[:5], 1):
                print(f"\n{i}. @{comment.username} ({comment.date_time})")
                print(f"   Likes: {comment.likes:,} | Replies: {comment.reply_count}")
                text_preview = comment.text[:80] + "..." if len(comment.text) > 80 else comment.text
                print(f"   Text: {text_preview}")

            # Save results
            output_prefix = args.output or f"{video_id}_comments"
            output_path = Path(__file__).parent / "output" / output_prefix
            output_path.parent.mkdir(parents=True, exist_ok=True)
            save_comments(comments, str(output_path), video_id)

        return 0

    # Post scraping mode - validate required args
    if not args.username:
        parser.error("--username is required for post scraping")
    if not args.hashtags:
        parser.error("--hashtags is required for post scraping")
    if not args.start:
        parser.error("--start is required for post scraping")
    if not args.end:
        parser.error("--end is required for post scraping")

    config = TikTokScrapeConfig(
        username=args.username,
        hashtags=args.hashtags,
        date_start=parse_date(args.start),
        date_end=parse_date(args.end),
        delay_seconds=args.delay,
        max_pages=args.max_pages,
        scrape_mode=args.scrape_mode,
        show_id=args.show_id,
        season_number=args.season,
        person_id=args.person_id,
    )

    posts = scraper.scrape(config)
    diagnostics = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    if args.proxy_url and "proxy_label" not in diagnostics:
        diagnostics["proxy_label"] = _proxy_label(args.proxy_url)
    if diagnostics:
        _emit_diagnostics_summary(target_label=args.username, scrape_mode=args.scrape_mode, diagnostics=diagnostics)
        _write_diagnostics_json(args.diagnostics_json, diagnostics)

    # Print summary
    print("\n" + "=" * 60)
    print(f"SUMMARY: Found {len(posts)} posts")
    print("=" * 60)

    if posts:
        print("\nPreview (first 5 posts):")
        for i, post in enumerate(sorted(posts, key=lambda p: p.create_time, reverse=True)[:5], 1):
            print(f"\n{i}. {post.date_time}")
            print(f"   Views: {post.views:,} | Likes: {post.likes:,} | Comments: {post.comments:,}")
            print(f"   URL: {post.url}")
            desc_preview = post.description[:80] + "..." if len(post.description) > 80 else post.description
            print(f"   Description: {desc_preview}")

        # Save results
        output_prefix = args.output or f"{config.username}_tiktok_posts"
        output_path = Path(__file__).parent / "output" / output_prefix
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_results(posts, str(output_path))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
