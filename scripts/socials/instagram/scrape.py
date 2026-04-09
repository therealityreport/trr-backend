#!/usr/bin/env python3
"""
CLI script for scraping Instagram posts and comments.

Usage:
    # Scrape posts
    python -m scripts.socials.instagram.scrape --username bravotv --hashtags RHOSLC
    python -m scripts.socials.instagram.scrape --username bravotv --hashtags RHOSLC --start 2025-08-14 --end 2026-02-04
    python -m scripts.socials.instagram.scrape --config config.json

    # Scrape comments from a specific post
    python -m scripts.socials.instagram.scrape --comments --post DUBSkVeEp4c
    python -m scripts.socials.instagram.scrape --comments --post DUBSkVeEp4c --with-replies --max-comments 100
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path (scripts/socials/instagram -> project root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from trr_backend.socials.instagram import (
    InstagramComment,
    InstagramScraper,
    ScrapeConfig,
    load_cookies_from_file,
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


def save_results(posts: list, output_prefix: str):
    """Save posts to JSON and CSV files."""
    if not posts:
        logger.warning("No posts to save")
        return

    # Sort by date (newest first)
    posts_sorted = sorted(posts, key=lambda p: p.taken_at, reverse=True)
    posts_dicts = [p.to_dict() for p in posts_sorted]

    # Save JSON
    json_file = f"{output_prefix}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(posts_dicts, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Saved {len(posts)} posts to {json_file}")

    # Save CSV
    csv_file = f"{output_prefix}.csv"
    fieldnames = [
        "post_type",
        "date_time",
        "caption",
        "profile_tags",
        "sponsored",
        "likes",
        "comments",
        "video_views",
        "shortcode",
        "url",
        "username",
    ]

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for post in posts_dicts:
            # Convert profile_tags list to string for CSV
            row = post.copy()
            row["profile_tags"] = ", ".join(f"@{t}" for t in post.get("profile_tags", []))
            writer.writerow(row)
    logger.info(f"Saved {len(posts)} posts to {csv_file}")


def save_comments(comments: list[InstagramComment], output_prefix: str, shortcode: str):
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
        "text",
        "likes",
        "is_reply",
        "parent_comment_id",
        "reply_count",
        "post_shortcode",
        "post_url",
    ]

    def flatten_comments(comment_list: list, rows: list):
        for c in comment_list:
            row = {
                "comment_id": c.comment_id,
                "date_time": c.date_time,
                "username": c.username,
                "text": c.text,
                "likes": c.likes,
                "is_reply": c.is_reply,
                "parent_comment_id": c.parent_comment_id or "",
                "reply_count": c.reply_count,
                "post_shortcode": c.post_shortcode,
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


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Instagram posts and comments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape @bravotv posts with #RHOSLC
  python -m scripts.socials.instagram.scrape --username bravotv --hashtags RHOSLC

  # Scrape with date range
  python -m scripts.socials.instagram.scrape --username bravotv --hashtags RHOSLC --start 2025-08-14 --end 2026-02-04

  # Use config file
  python -m scripts.socials.instagram.scrape --config scrape_config.json

  # Scrape comments from a specific post
  python -m scripts.socials.instagram.scrape --comments --post DUBSkVeEp4c

  # Scrape comments with replies and limit
  python -m scripts.socials.instagram.scrape --comments --post DUBSkVeEp4c --with-replies --max-comments 100
        """,
    )

    # Post scraping arguments
    parser.add_argument("--username", help="Instagram username to scrape")
    parser.add_argument(
        "--hashtags",
        nargs="+",
        help="Hashtags to filter by (without #)",
    )
    parser.add_argument(
        "--start",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--config",
        help="Path to JSON config file",
    )

    # Comment scraping arguments
    parser.add_argument(
        "--comments",
        action="store_true",
        help="Scrape comments instead of posts",
    )
    parser.add_argument(
        "--post",
        help="Post shortcode to scrape comments from (e.g., DUBSkVeEp4c)",
    )
    parser.add_argument(
        "--with-replies",
        action="store_true",
        help="Also fetch replies to comments",
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        help="Maximum number of top-level comments to fetch",
    )

    # Common arguments
    parser.add_argument(
        "--cookies",
        default="instagram_cookies.json",
        help="Path to cookies JSON file",
    )
    parser.add_argument(
        "--output",
        help="Output file prefix (default: {username}_posts or {shortcode}_comments)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay between requests in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Maximum number of pages to fetch (posts only)",
    )
    parser.add_argument(
        "--no-auth",
        action="store_true",
        help="Run without authentication (limited results)",
    )
    parser.add_argument(
        "--show-id",
        type=int,
        help="Associated show ID for metadata",
    )
    parser.add_argument(
        "--season",
        type=int,
        help="Associated season number for metadata",
    )
    parser.add_argument(
        "--person-id",
        type=int,
        help="Associated person ID for metadata",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    env_path = load_env()
    if env_path:
        logger.debug("Loaded environment from %s", env_path)

    # Load cookies
    cookies = {}
    if not args.no_auth:
        try:
            from trr_backend.socials.control_plane import _load_instagram_cookies

            if args.cookies and args.cookies != "instagram_cookies.json":
                os.environ["SOCIAL_INSTAGRAM_COOKIES_FILE"] = str(Path(args.cookies).expanduser())
            cookies = _load_instagram_cookies()
            if cookies:
                logger.info("Loaded Instagram cookies via canonical repo auth loader")
            else:
                cookies_path = Path(args.cookies)
                if not cookies_path.exists():
                    cookies_path = Path(__file__).parent / args.cookies
                if cookies_path.exists():
                    cookies = load_cookies_from_file(str(cookies_path))
                    logger.info(f"Loaded cookies from {cookies_path}")
                else:
                    logger.warning(f"Cookies file not found: {args.cookies}")
                    logger.warning("Running in unauthenticated mode")
        except Exception as e:
            logger.error(f"Failed to load cookies: {e}")
            logger.warning("Running in unauthenticated mode")

    # Create scraper
    scraper = InstagramScraper(cookies=cookies)

    # Comment scraping mode
    if args.comments:
        if not args.post:
            parser.error("--post is required when using --comments")

        shortcode = args.post.strip("/").split("/")[-1]  # Handle full URL or shortcode
        logger.info(f"Fetching comments for post: {shortcode}")

        comments = scraper.fetch_comments(
            shortcode=shortcode,
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
            output_prefix = args.output or f"{shortcode}_comments"
            output_path = Path(__file__).parent / "output" / output_prefix
            output_path.parent.mkdir(parents=True, exist_ok=True)
            save_comments(comments, str(output_path), shortcode)

        return

    # Post scraping mode
    # Build config from args or config file
    if args.config:
        with open(args.config) as f:
            config_data = json.load(f)
        config = ScrapeConfig(
            username=config_data["username"],
            hashtags=config_data.get("hashtags", []),
            date_start=parse_date(config_data["date_start"]) if config_data.get("date_start") else None,
            date_end=parse_date(config_data["date_end"]) if config_data.get("date_end") else None,
            delay_seconds=config_data.get("delay_seconds", 2.0),
            max_pages=config_data.get("max_pages"),
            show_id=config_data.get("show_id"),
            season_number=config_data.get("season_number"),
            person_id=config_data.get("person_id"),
        )
    elif args.username:
        config = ScrapeConfig(
            username=args.username,
            hashtags=args.hashtags or [],
            date_start=parse_date(args.start) if args.start else None,
            date_end=parse_date(args.end) if args.end else None,
            delay_seconds=args.delay,
            max_pages=args.max_pages,
            show_id=args.show_id,
            season_number=args.season,
            person_id=args.person_id,
        )
    else:
        parser.error("Either --username, --config, or --comments with --post is required")

    posts = scraper.scrape(config)

    # Print summary
    print("\n" + "=" * 60)
    print(f"SUMMARY: Found {len(posts)} posts")
    print("=" * 60)

    if posts:
        print("\nPreview (first 5 posts):")
        for i, post in enumerate(sorted(posts, key=lambda p: p.taken_at, reverse=True)[:5], 1):
            print(f"\n{i}. [{post.post_type.upper()}] {post.date_time}")
            print(f"   Likes: {post.likes:,} | Comments: {post.comments:,}")
            print(f"   URL: {post.url}")
            caption_preview = post.caption[:80] + "..." if len(post.caption) > 80 else post.caption
            print(f"   Caption: {caption_preview}")

        # Save results
        output_prefix = args.output or f"{config.username}_posts"
        output_path = Path(__file__).parent / "output" / output_prefix
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_results(posts, str(output_path))


if __name__ == "__main__":
    main()
