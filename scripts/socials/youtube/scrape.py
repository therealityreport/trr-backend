#!/usr/bin/env python3
"""
CLI script for scraping YouTube channel videos, shorts, and comments.

Usage:
    # Scrape videos and shorts
    python -m scripts.socials.youtube.scrape --channel bravo --keywords RHOSLC \\
        "Salt Lake City" --start 2025-08-14 --end 2026-02-04

    # Scrape and download at best quality
    python -m scripts.socials.youtube.scrape --channel bravo --keywords RHOSLC \\
        --start 2025-08-14 --end 2026-02-04 --download

    # Scrape comments from a specific video
    python -m scripts.socials.youtube.scrape --comments --video dQw4w9WgXcQ
    python -m scripts.socials.youtube.scrape --comments --video dQw4w9WgXcQ --with-replies --max-comments 100
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path (scripts/socials/youtube -> project root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from trr_backend.socials.youtube import (
    YouTubeComment,
    YouTubeScrapeConfig,
    YouTubeScraper,
    YouTubeVideo,
)
from trr_backend.socials.youtube.ops import (
    default_download_root as default_download_root,
)
from trr_backend.socials.youtube.ops import (
    download_videos,
    resolve_download_dir,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d")


def save_results(videos: list[YouTubeVideo], output_prefix: str):
    """Save videos to JSON and CSV files."""
    if not videos:
        logger.warning("No videos to save")
        return

    # Sort by date (newest first)
    videos_sorted = sorted(videos, key=lambda v: v.published_at, reverse=True)
    videos_dicts = [v.to_dict() for v in videos_sorted]

    # Save JSON
    json_file = f"{output_prefix}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(videos_dicts, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Saved {len(videos)} videos to {json_file}")

    # Save CSV
    csv_file = f"{output_prefix}.csv"
    fieldnames = [
        "date_time",
        "title",
        "description",
        "channel_title",
        "duration",
        "duration_seconds",
        "views",
        "likes",
        "comments",
        "keywords_matched",
        "video_id",
        "url",
        "thumbnail_url",
    ]

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for video in videos_dicts:
            row = video.copy()
            row["keywords_matched"] = ", ".join(video.get("keywords_matched", []))
            # Truncate description for CSV
            desc = row["description"]
            row["description"] = desc[:200] + "..." if len(desc) > 200 else desc
            writer.writerow(row)
    logger.info(f"Saved {len(videos)} videos to {csv_file}")


def save_comments(comments: list[YouTubeComment], output_prefix: str, video_id: str):
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
        "author",
        "author_channel_id",
        "text",
        "likes",
        "is_reply",
        "parent_comment_id",
        "reply_count",
        "video_id",
        "video_url",
    ]

    def flatten_comments(comment_list: list, rows: list):
        for c in comment_list:
            row = {
                "comment_id": c.comment_id,
                "date_time": c.date_time,
                "author": c.author,
                "author_channel_id": c.author_channel_id,
                "text": c.text,
                "likes": c.likes,
                "is_reply": c.is_reply,
                "parent_comment_id": c.parent_comment_id or "",
                "reply_count": c.reply_count,
                "video_id": c.video_id,
                "video_url": c.video_url,
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
        description="Scrape YouTube channel videos and comments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape @bravo videos and shorts about RHOSLC
  python -m scripts.socials.youtube.scrape --channel bravo --keywords RHOSLC \\
    --start 2025-08-14 --end 2026-02-04

  # Scrape and download at best quality
  python -m scripts.socials.youtube.scrape --channel bravo --keywords RHOSLC \\
    --start 2025-08-14 --end 2026-02-04 --download

  # Scrape comments from a specific video
  python -m scripts.socials.youtube.scrape --comments --video dQw4w9WgXcQ

  # Scrape comments with replies and limit
  python -m scripts.socials.youtube.scrape --comments --video dQw4w9WgXcQ --with-replies --max-comments 100
        """,
    )

    # Video scraping arguments
    parser.add_argument("--channel", help="YouTube channel handle (without @)")
    parser.add_argument(
        "--keywords",
        nargs="+",
        help="Keywords to filter by (e.g., RHOSLC 'Salt Lake City')",
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")

    # Comment scraping arguments
    parser.add_argument(
        "--comments",
        action="store_true",
        help="Scrape comments instead of videos",
    )
    parser.add_argument(
        "--video",
        help="Video ID to scrape comments from",
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
        "--output",
        help="Output file prefix (default: youtube_{channel}_videos or {video_id}_comments)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay between requests in seconds (default: 2.0)",
    )
    parser.add_argument("--max-results", type=int, help="Maximum number of videos to fetch (videos only)")
    parser.add_argument("--max-pages", type=int, help="Maximum continuation pages per surface (videos only)")
    parser.add_argument(
        "--no-ytdlp-supplement",
        action="store_true",
        help="Skip yt-dlp search supplement when channel browsing finds no matching videos.",
    )
    parser.add_argument("--api-key", help="YouTube Data API key (optional)")
    parser.add_argument("--show-id", type=int, help="Associated show ID for metadata")
    parser.add_argument("--season", type=int, help="Associated season number for metadata")
    parser.add_argument("--person-id", type=int, help="Associated person ID for metadata")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download videos/shorts at best quality using yt-dlp",
    )
    parser.add_argument(
        "--download-dir",
        help=(
            "Optional download directory. Defaults to an OS cache path outside the repo "
            "(for example ~/Library/Caches/TRR/youtube-downloads/<channel> on macOS)."
        ),
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create scraper
    scraper = YouTubeScraper(api_key=args.api_key)

    # Comment scraping mode
    if args.comments:
        if not args.video:
            parser.error("--video is required when using --comments")

        video_id = args.video
        # Handle full URLs
        if "youtube.com" in video_id or "youtu.be" in video_id:
            import re

            match = re.search(r"(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})", video_id)
            if match:
                video_id = match.group(1)

        logger.info(f"Fetching comments for video: {video_id}")

        comments = scraper.fetch_comments(
            video_id=video_id,
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
                print(f"\n{i}. {comment.author} ({comment.date_time})")
                print(f"   Likes: {comment.likes:,} | Replies: {comment.reply_count}")
                text_preview = comment.text[:80] + "..." if len(comment.text) > 80 else comment.text
                text_preview = text_preview.replace("\n", " ")
                print(f"   Text: {text_preview}")

            # Save results
            output_prefix = args.output or f"{video_id}_comments"
            output_path = Path(__file__).parent / "output" / output_prefix
            output_path.parent.mkdir(parents=True, exist_ok=True)
            save_comments(comments, str(output_path), video_id)

        return

    # Video scraping mode - validate required args
    if not args.channel:
        parser.error("--channel is required for video scraping")
    if not args.keywords:
        parser.error("--keywords is required for video scraping")
    if not args.start:
        parser.error("--start is required for video scraping")
    if not args.end:
        parser.error("--end is required for video scraping")

    config = YouTubeScrapeConfig(
        channel_handle=args.channel,
        keywords=args.keywords,
        date_start=parse_date(args.start),
        date_end=parse_date(args.end),
        delay_seconds=args.delay,
        max_results=args.max_results,
        max_pages=args.max_pages,
        allow_ytdlp_search_supplement=not args.no_ytdlp_supplement,
        show_id=args.show_id,
        season_number=args.season,
        person_id=args.person_id,
    )

    videos = scraper.scrape(config)

    # Report coverage metadata
    meta = scraper.last_retrieval_meta
    if meta:
        print(
            f"\nCoverage: {meta.get('continuation_pages', 0)} pages scanned, "
            f"{meta.get('in_range_hits', 0)} in-range hits"
        )
        if meta.get("scan_capped_reason"):
            print(f"WARNING: Scan was capped — reason: {meta['scan_capped_reason']}")

    # Separate videos and shorts for summary
    regular_videos = [v for v in videos if not v.is_short]
    shorts = [v for v in videos if v.is_short]

    # Print summary
    print("\n" + "=" * 60)
    print(f"SUMMARY: Found {len(videos)} total ({len(regular_videos)} videos, {len(shorts)} shorts)")
    print(f"Channel: @{args.channel}")
    print(f"Keywords: {args.keywords}")
    print("=" * 60)

    if videos:
        print("\nPreview (first 5 results):")
        for i, video in enumerate(sorted(videos, key=lambda v: v.published_at, reverse=True)[:5], 1):
            label = "[Short]" if video.is_short else "[Video]"
            print(f"\n{i}. {label} {video.date_time}")
            title_preview = video.title[:60] + "..." if len(video.title) > 60 else video.title
            print(f"   Title: {title_preview}")
            print(f"   Views: {video.views:,} | Duration: {video.duration}")
            print(f"   Keywords matched: {', '.join(video.keywords_matched) or 'N/A'}")
            print(f"   URL: {video.url}")

        # Save results
        output_prefix = args.output or f"youtube_{config.channel_handle}_videos"
        output_path = Path(__file__).parent / "output" / output_prefix
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_results(videos, str(output_path))

        # Download if requested
        if args.download:
            download_dir = resolve_download_dir(args.download_dir, config.channel_handle)
            download_videos(videos, download_dir)


if __name__ == "__main__":
    main()
