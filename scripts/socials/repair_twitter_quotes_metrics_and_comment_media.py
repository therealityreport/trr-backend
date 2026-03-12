#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.socials.twitter import TwitterScraper
from trr_backend.utils.env import load_env

DEFAULT_SOURCE_SCOPE = "bravo"


@dataclass(slots=True)
class RepairCounters:
    roots_scanned: int = 0
    roots_refreshed: int = 0
    replies_fetched: int = 0
    replies_upserted: int = 0
    quotes_fetched: int = 0
    quotes_upserted: int = 0
    media_jobs_enqueued: int = 0
    unresolved_posts: list[dict[str, Any]] = field(default_factory=list)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_twitter_quotes_metrics_and_comment_media",
        description=(
            "Repair twitter root metrics, refresh quote/reply ingestion, and enqueue missing comment media mirror jobs."
        ),
    )
    parser.add_argument("--season-id", default="", help="Season UUID scope. Required when --apply is set.")
    parser.add_argument("--source-account", default="", help="Optional source account filter.")
    parser.add_argument("--tweet-id", default="", help="Optional single tweet_id filter.")
    parser.add_argument("--limit", type=int, default=200, help="Max root tweets to scan (default: 200).")
    parser.add_argument(
        "--max-pages-cap",
        type=int,
        default=60,
        help="Override twitter reply/quote fallback page cap (default: 60).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview mode (default).")
    parser.add_argument("--apply", action="store_true", help="Apply updates.")
    parser.set_defaults(dry_run=True)
    return parser.parse_args(argv)


def _load_root_rows(
    *,
    season_id: str,
    source_account: str,
    tweet_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    where_clauses = ["t.is_reply = false"]
    params: list[Any] = []
    if season_id:
        where_clauses.append("t.season_id = %s::uuid")
        params.append(season_id)
    if source_account:
        where_clauses.append("lower(coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '')) = lower(%s)")
        params.append(source_account)
    if tweet_id:
        where_clauses.append("t.tweet_id = %s")
        params.append(tweet_id)
    params.append(max(1, int(limit)))
    return pg.fetch_all(
        f"""
        select
          t.id::text as id,
          t.tweet_id,
          t.season_id::text as season_id,
          coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '') as source_account
        from social.twitter_tweets t
        where {" and ".join(where_clauses)}
        order by t.created_at desc
        limit %s
        """,
        params,
    )


def _select_media_comment_rows_for_root(root_tweet_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          c.id::text as id,
          c.tweet_id,
          c.reply_to_tweet_id,
          c.quoted_tweet_id,
          coalesce(c.media_urls, '[]'::jsonb) as media_urls,
          coalesce(c.hosted_media_urls, '[]'::jsonb) as hosted_media_urls,
          coalesce(c.media_mirror_status, '') as media_mirror_status
        from social.twitter_tweets c
        where (c.is_reply = true or c.is_quote = true)
          and (
            c.reply_to_tweet_id = %s
            or c.quoted_tweet_id = %s
          )
          and jsonb_typeof(coalesce(c.media_urls, '[]'::jsonb)) = 'array'
          and jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb)) > 0
        order by c.created_at asc
        """,
        [root_tweet_id, root_tweet_id],
    )


def _enqueue_missing_comment_media_jobs_for_root(
    *,
    context: social_repo.SeasonContext,
    run_id: str | None,
    source_scope: str,
    account: str,
    parent_job_id: str,
    root_tweet_id: str,
    dry_run: bool,
) -> int:
    comment_rows = _select_media_comment_rows_for_root(root_tweet_id)
    enqueued = 0
    for row in comment_rows:
        if not social_repo._twitter_comment_needs_media_mirror(row):  # noqa: SLF001
            continue
        if dry_run:
            enqueued += 1
            continue
        job_id = social_repo._enqueue_twitter_comment_media_mirror_job(  # noqa: SLF001
            context,
            run_id=run_id,
            source_scope=source_scope,
            account=account,
            comment_row=row,
            parent_job_id=parent_job_id,
            conn=None,
        )
        if job_id:
            enqueued += 1
    return enqueued


def _build_scraper() -> TwitterScraper:
    cookies, bearer = social_repo._load_twitter_auth()  # noqa: SLF001
    twikit_credentials = social_repo._load_twikit_credentials()  # noqa: SLF001
    return TwitterScraper(cookies=cookies, bearer_token=bearer, twikit_credentials=twikit_credentials)


def _refresh_root(
    *,
    scraper: TwitterScraper | None,
    row: dict[str, Any],
    source_scope: str,
    counters: RepairCounters,
    dry_run: bool,
) -> None:
    tweet_id = str(row.get("tweet_id") or "").strip()
    season_id = str(row.get("season_id") or "").strip()
    account = str(row.get("source_account") or "").strip()
    if not tweet_id or not season_id:
        counters.unresolved_posts.append(
            {
                "tweet_id": tweet_id or None,
                "season_id": season_id or None,
                "reason": "missing_identifiers",
            }
        )
        return

    context = social_repo.get_season_context(season_id)
    if dry_run:
        enqueued = _enqueue_missing_comment_media_jobs_for_root(
            context=context,
            run_id=None,
            source_scope=source_scope,
            account=account,
            parent_job_id="repair-twitter-quotes-metrics-comment-media",
            root_tweet_id=tweet_id,
            dry_run=True,
        )
        counters.media_jobs_enqueued += enqueued
        return

    if scraper is None:
        raise RuntimeError("twitter_scraper_unavailable")

    social_repo._fetch_and_apply_twitter_metric_summary(  # noqa: SLF001
        scraper=scraper,
        tweet_id=tweet_id,
        conn=None,
    )
    refresh_payload = social_repo.refresh_post_comments(
        season_id,
        platform="twitter",
        source_id=tweet_id,
        max_comments_per_post=100000,
        fetch_replies=True,
    )
    counters.roots_refreshed += 1
    counters.replies_fetched += int(refresh_payload.get("comments_fetched") or 0)
    counters.replies_upserted += int(refresh_payload.get("comments_upserted") or 0)
    counters.quotes_fetched += int(refresh_payload.get("quotes_fetched") or 0)
    counters.quotes_upserted += int(refresh_payload.get("quotes_upserted") or 0)
    counters.media_jobs_enqueued += int(refresh_payload.get("comment_media_mirror_jobs_enqueued") or 0)

    extra_enqueued = _enqueue_missing_comment_media_jobs_for_root(
        context=context,
        run_id=None,
        source_scope=source_scope,
        account=account,
        parent_job_id="repair-twitter-quotes-metrics-comment-media",
        root_tweet_id=tweet_id,
        dry_run=False,
    )
    counters.media_jobs_enqueued += int(extra_enqueued)


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv)
    dry_run = bool(args.dry_run and not args.apply)
    season_id = str(args.season_id or "").strip()
    source_account = str(args.source_account or "").strip()
    tweet_id = str(args.tweet_id or "").strip()
    limit = max(1, int(args.limit))
    max_pages_cap = max(1, int(args.max_pages_cap))

    if args.apply and not season_id:
        raise SystemExit("--season-id is required when --apply is set.")

    rows = _load_root_rows(
        season_id=season_id,
        source_account=source_account,
        tweet_id=tweet_id,
        limit=limit,
    )

    previous_page_cap = social_repo.TWITTER_COMMENT_MAX_PAGE_BUDGET
    social_repo.TWITTER_COMMENT_MAX_PAGE_BUDGET = max_pages_cap
    counters = RepairCounters()
    scraper = _build_scraper() if not dry_run else None
    try:
        for row in rows:
            counters.roots_scanned += 1
            try:
                _refresh_root(
                    scraper=scraper,
                    row=row,
                    source_scope=DEFAULT_SOURCE_SCOPE,
                    counters=counters,
                    dry_run=dry_run,
                )
            except Exception as exc:  # noqa: BLE001
                counters.unresolved_posts.append(
                    {
                        "tweet_id": str(row.get("tweet_id") or "").strip() or None,
                        "season_id": str(row.get("season_id") or "").strip() or None,
                        "reason": str(exc)[:200],
                    }
                )
    finally:
        social_repo.TWITTER_COMMENT_MAX_PAGE_BUDGET = previous_page_cap

    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "season_id": season_id or None,
                "source_account": source_account or None,
                "tweet_id": tweet_id or None,
                "limit": limit,
                "max_pages_cap": max_pages_cap,
                "roots_scanned": counters.roots_scanned,
                "roots_refreshed": counters.roots_refreshed,
                "replies_fetched": counters.replies_fetched,
                "replies_upserted": counters.replies_upserted,
                "quotes_fetched": counters.quotes_fetched,
                "quotes_upserted": counters.quotes_upserted,
                "media_jobs_enqueued": counters.media_jobs_enqueued,
                "unresolved_posts": counters.unresolved_posts,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
