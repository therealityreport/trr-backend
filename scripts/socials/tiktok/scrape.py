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


def main():
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

    # Post scraping arguments
    parser.add_argument("--username", help="TikTok username to scrape (without @)")
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

    # Comment scraping arguments
    parser.add_argument(
        "--comments",
        action="store_true",
        help="Scrape comments instead of posts",
    )
    parser.add_argument(
        "--video",
        help="Video ID to scrape comments from (aweme_id)",
    )
    parser.add_argument(
        "--video-user",
        help="Username of the video author (optional, for building URL)",
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
        default="tiktok_cookies.json",
        help="Path to TikTok cookies JSON file",
    )
    parser.add_argument(
        "--no-auth",
        action="store_true",
        help="Run without authentication (may limit availability)",
    )
    parser.add_argument(
        "--output",
        help="Output file prefix (default: {username}_tiktok_posts or {video_id}_comments)",
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

    # Create scraper
    tiktok_cookies: dict[str, str] = {}
    if not args.no_auth:
        try:
            from trr_backend.repositories.social_season_analytics import _load_tiktok_cookies

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
    scraper = TikTokScraper(cookies=tiktok_cookies)

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

        return

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
        show_id=args.show_id,
        season_number=args.season,
        person_id=args.person_id,
    )

    posts = scraper.scrape(config)

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


if __name__ == "__main__":
    main()
