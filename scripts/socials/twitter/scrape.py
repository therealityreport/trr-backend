#!/usr/bin/env python3
"""
CLI script for searching Twitter/X tweets and fetching replies.

Usage:
    # Search for tweets
    python -m scripts.socials.twitter.scrape --query RHOSLC --start 2026-01-01 --end 2026-01-11
    python -m scripts.socials.twitter.scrape --query "#RHOSLC" --start 2026-01-01 --end 2026-01-11 --include-replies

    # Fetch replies to a specific tweet
    python -m scripts.socials.twitter.scrape --replies --tweet 1234567890123456789
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path (scripts/socials/twitter -> project root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from trr_backend.socials.twitter import Tweet, TwitterScrapeConfig, TwitterScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d")


def load_cookies_from_file(filepath: str) -> dict:
    """Load Twitter cookies from a JSON file."""
    with open(filepath) as f:
        cookies = json.load(f)
    return {k: v for k, v in cookies.items() if not k.startswith("_")}


def save_results(tweets: list[Tweet], output_prefix: str):
    """Save tweets to JSON and CSV files."""
    if not tweets:
        logger.warning("No tweets to save")
        return

    # Sort by date (newest first)
    tweets_sorted = sorted(tweets, key=lambda t: t.created_at, reverse=True)
    tweets_dicts = [t.to_dict() for t in tweets_sorted]

    # Save JSON
    json_file = f"{output_prefix}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(tweets_dicts, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Saved {len(tweets)} tweets to {json_file}")

    # Save CSV
    csv_file = f"{output_prefix}.csv"
    fieldnames = [
        "date_time",
        "username",
        "display_name",
        "text",
        "hashtags",
        "mentions",
        "likes",
        "retweets",
        "replies",
        "quotes",
        "views",
        "is_reply",
        "is_retweet",
        "tweet_id",
        "url",
    ]

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for tweet in tweets_dicts:
            row = tweet.copy()
            row["hashtags"] = ", ".join(f"#{t}" for t in tweet.get("hashtags", []))
            row["mentions"] = ", ".join(f"@{t}" for t in tweet.get("mentions", []))
            writer.writerow(row)
    logger.info(f"Saved {len(tweets)} tweets to {csv_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Search Twitter/X for tweets and fetch replies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for RHOSLC tweets
  python -m scripts.socials.twitter.scrape --query RHOSLC --start 2026-01-01 --end 2026-01-11

  # Search with hashtag explicitly
  python -m scripts.socials.twitter.scrape --query "#RHOSLC" --start 2026-01-01 --end 2026-01-11

  # Include replies in results
  python -m scripts.socials.twitter.scrape --query RHOSLC --start 2026-01-01 --end 2026-01-11 --include-replies

  # Fetch replies for specific tweets found in search
  python -m scripts.socials.twitter.scrape --query RHOSLC --start 2026-01-01 --end 2026-01-11 --fetch-replies

  # Fetch replies to a specific tweet (dedicated mode)
  python -m scripts.socials.twitter.scrape --replies --tweet 1234567890123456789
        """,
    )

    # Search arguments
    parser.add_argument("--query", help="Search query (hashtag or phrase, e.g., RHOSLC or #RHOSLC)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--include-replies", action="store_true", help="Include reply tweets in search results")
    parser.add_argument("--exclude-links", action="store_true", help="Exclude tweets with links")
    parser.add_argument("--fetch-replies", action="store_true", help="Fetch replies for each found tweet (search mode)")

    # Dedicated reply mode arguments
    parser.add_argument(
        "--replies",
        action="store_true",
        help="Fetch replies to a specific tweet (requires --tweet)",
    )
    parser.add_argument(
        "--tweet",
        help="Tweet ID to fetch replies from (use with --replies)",
    )

    # Common arguments
    parser.add_argument("--cookies", default="twitter_cookies.json", help="Path to cookies JSON file")
    parser.add_argument("--bearer-token", help="Twitter API bearer token")
    parser.add_argument("--output", help="Output file prefix")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between requests in seconds (default: 2.0)")
    parser.add_argument("--max-pages", type=int, help="Maximum number of pages to fetch (search mode)")
    parser.add_argument("--show-id", type=int, help="Associated show ID for metadata")
    parser.add_argument("--season", type=int, help="Associated season number for metadata")
    parser.add_argument("--person-id", type=int, help="Associated person ID for metadata")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load cookies
    cookies = {}
    bearer_token = args.bearer_token
    try:
        cookies_path = Path(args.cookies)
        if not cookies_path.exists():
            cookies_path = Path(__file__).parent / args.cookies
        if cookies_path.exists():
            cookies = load_cookies_from_file(str(cookies_path))
            logger.info(f"Loaded cookies from {cookies_path}")
        else:
            logger.warning(f"Cookies file not found: {args.cookies}")
            logger.warning("Running without authentication (may have limited results)")
    except Exception as e:
        logger.error(f"Failed to load cookies: {e}")

    # Create scraper
    scraper = TwitterScraper(cookies=cookies, bearer_token=bearer_token)

    # Dedicated reply fetching mode
    if args.replies:
        if not args.tweet:
            parser.error("--tweet is required when using --replies")

        tweet_id = args.tweet
        # Handle full URLs
        if "twitter.com" in tweet_id or "x.com" in tweet_id:
            import re

            match = re.search(r"/status/(\d+)", tweet_id)
            if match:
                tweet_id = match.group(1)

        logger.info(f"Fetching replies for tweet: {tweet_id}")

        replies = scraper.fetch_tweet_replies(tweet_id, args.delay)

        print("\n" + "=" * 60)
        print(f"SUMMARY: Found {len(replies)} replies")
        print(f"Tweet: https://x.com/i/status/{tweet_id}")
        print("=" * 60)

        if replies:
            print("\nPreview (first 5 replies):")
            for i, reply in enumerate(sorted(replies, key=lambda t: t.created_at, reverse=True)[:5], 1):
                print(f"\n{i}. @{reply.username} ({reply.date_time})")
                print(f"   Likes: {reply.likes:,} | RTs: {reply.retweets:,}")
                print(f"   URL: {reply.url}")
                text_preview = reply.text[:100] + "..." if len(reply.text) > 100 else reply.text
                text_preview = text_preview.replace("\n", " ")
                print(f"   Text: {text_preview}")

            # Save results
            output_prefix = args.output or f"{tweet_id}_replies"
            output_path = Path(__file__).parent / "output" / output_prefix
            output_path.parent.mkdir(parents=True, exist_ok=True)
            save_results(replies, str(output_path))

        return

    # Search mode - validate required args
    if not args.query:
        parser.error("--query is required for tweet search")
    if not args.start:
        parser.error("--start is required for tweet search")
    if not args.end:
        parser.error("--end is required for tweet search")

    config = TwitterScrapeConfig(
        query=args.query,
        date_start=parse_date(args.start),
        date_end=parse_date(args.end),
        include_replies=args.include_replies,
        include_links=not args.exclude_links,
        delay_seconds=args.delay,
        max_pages=args.max_pages,
        show_id=args.show_id,
        season_number=args.season,
        person_id=args.person_id,
    )

    tweets = scraper.scrape(config)

    # Optionally fetch replies for each tweet
    if args.fetch_replies and tweets:
        logger.info(f"Fetching replies for {len(tweets)} tweets...")
        all_replies = []
        for tweet in tweets[:10]:  # Limit to first 10 to avoid rate limits
            logger.info(f"Fetching replies for {tweet.tweet_id}...")
            replies = scraper.fetch_tweet_replies(tweet.tweet_id, config.delay_seconds)
            all_replies.extend(replies)
            logger.info(f"Found {len(replies)} replies")
        tweets.extend(all_replies)
        logger.info(f"Total: {len(tweets)} tweets including replies")

    # Print summary
    print("\n" + "=" * 60)
    print(f"SUMMARY: Found {len(tweets)} tweets")
    print(f"Search query: {config.build_search_query()}")
    print("=" * 60)

    if tweets:
        print("\nPreview (first 5 tweets):")
        for i, tweet in enumerate(sorted(tweets, key=lambda t: t.created_at, reverse=True)[:5], 1):
            print(f"\n{i}. @{tweet.username} ({tweet.date_time})")
            print(f"   Likes: {tweet.likes:,} | RTs: {tweet.retweets:,} | Replies: {tweet.replies:,}")
            print(f"   URL: {tweet.url}")
            text_preview = tweet.text[:100] + "..." if len(tweet.text) > 100 else tweet.text
            text_preview = text_preview.replace("\n", " ")
            print(f"   Text: {text_preview}")

        # Save results
        safe_query = args.query.replace("#", "").replace(" ", "_")
        output_prefix = args.output or f"twitter_search_{safe_query}"
        output_path = Path(__file__).parent / "output" / output_prefix
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_results(tweets, str(output_path))


if __name__ == "__main__":
    main()
