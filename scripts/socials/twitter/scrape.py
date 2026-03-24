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

from trr_backend.repositories.twitter_standalone import persist_standalone_twitter_search
from trr_backend.socials.twitter import Tweet, TwitterScrapeConfig, TwitterScraper, mirror_tweet_media
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


def _normalize_tweet_id(raw_tweet_id: str) -> str:
    tweet_id = str(raw_tweet_id or "").strip()
    if "twitter.com" in tweet_id or "x.com" in tweet_id:
        import re

        match = re.search(r"/status/(\d+)", tweet_id)
        if match:
            return match.group(1)
    return tweet_id


def _print_mirror_summary(tweets: list[Tweet]) -> None:
    mirrored_count = sum(len(tweet.hosted_media_urls or []) for tweet in tweets)
    print(f"\nMirroring summary: {mirrored_count} hosted media URL(s) across {len(tweets)} tweet(s)")
    preview_rows: list[tuple[str, str]] = []
    for tweet in tweets:
        for hosted_url in tweet.hosted_media_urls or []:
            preview_rows.append((tweet.tweet_id, hosted_url))
    if preview_rows:
        print("Hosted URL preview:")
        for tweet_id, hosted_url in preview_rows[:10]:
            print(f"  - {tweet_id}: {hosted_url}")


def _print_root_tweet_summary(summary: dict, fallback_tweet_id: str) -> None:
    tweet_id = str(summary.get("tweet_id") or fallback_tweet_id or "").strip() or fallback_tweet_id
    username = str(summary.get("username") or "").strip()
    display_name = str(summary.get("display_name") or "").strip()
    text_preview = str(summary.get("text") or "").replace("\n", " ").strip()
    if len(text_preview) > 140:
        text_preview = text_preview[:140] + "..."
    root_url = str(summary.get("url") or "").strip() or f"https://x.com/i/status/{tweet_id}"

    print("\nRoot Tweet Context:")
    print(f"  - Resolved Tweet ID: {tweet_id}")
    if username:
        if display_name:
            print(f"  - Author: @{username} ({display_name})")
        else:
            print(f"  - Author: @{username}")
    print(f"  - URL: {root_url}")
    if text_preview:
        print(f"  - Text: {text_preview}")


def _print_fetch_diagnostics(scraper: TwitterScraper, fetch_mode: str) -> None:
    print("\nFetch diagnostics:")
    if fetch_mode == "quotes":
        meta = getattr(scraper, "last_quote_fetch_meta", {}) or {}
        attempts = meta.get("attempts", []) or []
        for attempt in attempts:
            source = str(attempt.get("source") or "unknown")
            count = int(attempt.get("count") or 0)
            skipped = " (skipped)" if attempt.get("skipped") else ""
            reason = str(attempt.get("failure_reason") or "none")
            print(f"  - {source}{skipped}: count={count}, failure_reason={reason}")
        failure_reason = getattr(scraper, "last_quote_fetch_reason", None) or meta.get("failure_reason")
        print(f"  - final_failure_reason: {failure_reason or 'none'}")
        return
    failure_reason = getattr(scraper, "last_reply_fetch_reason", None)
    print(f"  - final_failure_reason: {failure_reason or 'none'}")


def _print_search_diagnostics(scraper: TwitterScraper) -> None:
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    if not retrieval_meta:
        return
    print("\nSearch diagnostics:")
    print(f"  - complete: {bool(retrieval_meta.get('complete'))}")
    print(f"  - retrieval_mode: {retrieval_meta.get('retrieval_mode') or 'unknown'}")
    print(f"  - posts_checked: {int(retrieval_meta.get('posts_checked') or 0)}")
    print(f"  - pages_scanned: {int(retrieval_meta.get('pages_scanned') or 0)}")
    print(f"  - stop_reason: {retrieval_meta.get('stop_reason') or 'unknown'}")
    print(f"  - retryable: {bool(retrieval_meta.get('retryable'))}")
    if retrieval_meta.get("error_code"):
        print(f"  - error_code: {retrieval_meta['error_code']}")


def _print_persist_summary(summary: dict[str, object] | None) -> None:
    if not isinstance(summary, dict):
        return
    print("\nPersistence summary:")
    print(f"  - succeeded: {bool(summary.get('succeeded'))}")
    print(f"  - scrape_run_id: {summary.get('scrape_run_id') or 'none'}")
    print(f"  - tweets_upserted: {int(summary.get('tweets_upserted') or 0)}")
    print(f"  - tweet_memberships_created: {int(summary.get('tweet_memberships_created') or 0)}")
    if summary.get("error"):
        print(f"  - error: {summary['error']}")


def main():
    parser = argparse.ArgumentParser(
        description="Search Twitter/X for tweets and fetch replies/quotes",
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

  # Fetch quote tweets for a specific tweet (dedicated mode)
  python -m scripts.socials.twitter.scrape --quotes --tweet 1234567890123456789
        """,
    )

    # Search arguments
    parser.add_argument("--query", help="Search query (hashtag or phrase, e.g., RHOSLC or #RHOSLC)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--include-replies", action="store_true", help="Include reply tweets in search results")
    parser.add_argument("--exclude-links", action="store_true", help="Exclude tweets with links")
    parser.add_argument("--fetch-replies", action="store_true", help="Fetch replies for each found tweet (search mode)")

    # Dedicated mode arguments
    dedicated_mode = parser.add_mutually_exclusive_group()
    dedicated_mode.add_argument(
        "--replies",
        action="store_true",
        help="Fetch replies to a specific tweet (requires --tweet)",
    )
    dedicated_mode.add_argument(
        "--quotes",
        action="store_true",
        help="Fetch quote tweets for a specific tweet (requires --tweet)",
    )
    parser.add_argument(
        "--tweet",
        help="Tweet ID to fetch replies/quotes from (use with --replies or --quotes)",
    )

    # Common arguments
    parser.add_argument("--cookies", default="twitter_cookies.json", help="Path to cookies JSON file")
    parser.add_argument("--bearer-token", help="Twitter API bearer token")
    parser.add_argument("--output", help="Output file prefix")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between requests in seconds (default: 2.0)")
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Maximum number of pages to fetch (search mode, and quote fallbacks in --quotes mode)",
    )
    parser.add_argument("--show-id", type=int, help="Associated show ID for metadata")
    parser.add_argument("--season", type=int, help="Associated season number for metadata")
    parser.add_argument("--person-id", type=int, help="Associated person ID for metadata")
    parser.add_argument("--mirror", action="store_true", help="Mirror tweet media URLs to S3 and print hosted URLs")
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Upsert results to social.twitter_tweets (standalone, no season required)",
    )
    parser.add_argument(
        "--scrape-query",
        help="Label stored on each persisted row (defaults to --query value)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.persist and (args.replies or args.quotes):
        parser.error("--persist is only supported in search mode (not --replies / --quotes)")

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    env_path = load_env()
    if env_path:
        logger.debug("Loaded environment from %s", env_path)

    # Load auth: prefer env vars (SOCIAL_TWITTER_COOKIES_JSON), fall back to cookie file.
    cookies = {}
    bearer_token = args.bearer_token
    twikit_creds = None
    twikit_loader = None

    try:
        from trr_backend.repositories.social_season_analytics import _load_twikit_credentials, _load_twitter_auth

        twikit_loader = _load_twikit_credentials
        cookies, env_bearer = _load_twitter_auth()
        if not bearer_token and env_bearer:
            bearer_token = env_bearer
        twikit_creds = _load_twikit_credentials(cookies)
        if cookies:
            logger.info("Loaded Twitter auth from environment variables")
    except Exception:
        logger.debug("Could not load auth from environment", exc_info=True)

    # Fall back to cookies file if env didn't provide auth.
    if not cookies:
        try:
            cookies_path = Path(args.cookies)
            if not cookies_path.exists():
                cookies_path = Path(__file__).parent / args.cookies
            if cookies_path.exists():
                cookies = load_cookies_from_file(str(cookies_path))
                logger.info(f"Loaded cookies from {cookies_path}")
            else:
                logger.warning(f"No auth found: env vars empty and cookies file not found ({args.cookies})")
        except Exception as e:
            logger.error(f"Failed to load cookies: {e}")

    if twikit_loader and not twikit_creds:
        try:
            twikit_creds = twikit_loader(cookies)
        except Exception:
            logger.debug("Could not derive twikit credentials from resolved Twitter cookies", exc_info=True)

    # Create scraper
    scraper = TwitterScraper(
        cookies=cookies,
        bearer_token=bearer_token,
        twikit_credentials=twikit_creds,
    )

    # Dedicated replies/quotes mode
    if args.replies or args.quotes:
        if not args.tweet:
            parser.error("--tweet is required when using --replies or --quotes")

        tweet_id = _normalize_tweet_id(args.tweet)
        fetch_mode = "replies" if args.replies else "quotes"
        logger.info(f"Fetching {fetch_mode} for tweet: {tweet_id}")

        root_summary = scraper.fetch_public_tweet_summary(tweet_id, delay=0.0)
        if not root_summary:
            parser.error(
                f"Could not resolve root tweet metadata for tweet '{tweet_id}'. "
                "Check the tweet ID/URL and authentication."
            )
        _print_root_tweet_summary(root_summary, tweet_id)

        tweets = (
            scraper.fetch_tweet_replies(tweet_id, args.delay)
            if args.replies
            else scraper.fetch_tweet_quotes(tweet_id, args.delay, max_pages=args.max_pages or 5)
        )
        if args.mirror and tweets:
            mirror_tweet_media(tweets)

        print("\n" + "=" * 60)
        print(f"SUMMARY: Found {len(tweets)} {fetch_mode}")
        print(f"Tweet: https://x.com/i/status/{tweet_id}")
        print("=" * 60)

        if not tweets:
            _print_fetch_diagnostics(scraper, fetch_mode)
            return

        print(f"\nPreview (first 5 {fetch_mode}):")
        for i, tweet in enumerate(sorted(tweets, key=lambda t: t.created_at, reverse=True)[:5], 1):
            print(f"\n{i}. @{tweet.username} ({tweet.date_time})")
            print(f"   Likes: {tweet.likes:,} | RTs: {tweet.retweets:,}")
            print(f"   URL: {tweet.url}")
            text_preview = tweet.text[:100] + "..." if len(tweet.text) > 100 else tweet.text
            text_preview = text_preview.replace("\n", " ")
            print(f"   Text: {text_preview}")
            if tweet.hosted_media_urls:
                print(f"   Hosted Media: {', '.join(tweet.hosted_media_urls)}")

        # Save results
        output_prefix = args.output or f"{tweet_id}_{fetch_mode}"
        output_path = Path(__file__).parent / "output" / output_prefix
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_results(tweets, str(output_path))
        if args.mirror:
            _print_mirror_summary(tweets)

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
    if args.mirror and tweets:
        mirror_tweet_media(tweets)

    # Print summary
    print("\n" + "=" * 60)
    print(f"SUMMARY: Found {len(tweets)} tweets")
    print(f"Search query: {config.build_search_query()}")
    print("=" * 60)
    _print_search_diagnostics(scraper)

    if tweets:
        print("\nPreview (first 5 tweets):")
        for i, tweet in enumerate(sorted(tweets, key=lambda t: t.created_at, reverse=True)[:5], 1):
            print(f"\n{i}. @{tweet.username} ({tweet.date_time})")
            print(f"   Likes: {tweet.likes:,} | RTs: {tweet.retweets:,} | Replies: {tweet.replies:,}")
            print(f"   URL: {tweet.url}")
            text_preview = tweet.text[:100] + "..." if len(tweet.text) > 100 else tweet.text
            text_preview = text_preview.replace("\n", " ")
            print(f"   Text: {text_preview}")
            if tweet.hosted_media_urls:
                print(f"   Hosted Media: {', '.join(tweet.hosted_media_urls)}")

        # Save results
        safe_query = args.query.replace("#", "").replace(" ", "_")
        output_prefix = args.output or f"twitter_search_{safe_query}"
        output_path = Path(__file__).parent / "output" / output_prefix
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_results(tweets, str(output_path))
        if args.mirror:
            _print_mirror_summary(tweets)

    if args.persist:
        label = str(args.scrape_query or args.query).strip() or args.query
        persist_summary = persist_standalone_twitter_search(
            tweets,
            raw_query=args.query,
            normalized_search_query=config.build_search_query(),
            scrape_query_label=label,
            window_start_day=config.window_start_day(),
            window_end_day_exclusive=config.window_end_day_exclusive(),
            requested_via="cli",
            retrieval_meta=dict(getattr(scraper, "last_retrieval_meta", {}) or {}),
            complete=bool(getattr(scraper, "last_retrieval_meta", {}).get("complete")),
        )
        _print_persist_summary(persist_summary)
        logger.info(
            "Persisted Twitter search run with scrape_query=%r and run_id=%r",
            label,
            persist_summary.get("scrape_run_id"),
        )


if __name__ == "__main__":
    main()
