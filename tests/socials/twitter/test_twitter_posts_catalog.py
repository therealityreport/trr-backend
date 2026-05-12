from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from trr_backend.socials.twitter.posts_catalog import (
    TwitterPostsCatalogDependencies,
    scrape_shared_twitter_posts,
)
from trr_backend.socials.twitter.scraper import Tweet, TwitterScrapeConfig

SHARED_MODE = "shared_account_catalog_backfill"
FIXED_NOW = datetime(2026, 5, 5, 12, 0, tzinfo=UTC)


def _tweet(
    tweet_id: str,
    *,
    username: str = "bravotv",
    display_name: str = "Bravo TV",
    is_reply: bool = False,
    avatar_url: str = "https://pbs.twimg.com/profile_images/123/bravo_normal.jpg",
    profile_url: str = "https://x.com/bravotv",
) -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2026-04-30 18:00:00",
        created_at=int(datetime(2026, 4, 30, 18, 0, tzinfo=UTC).timestamp()),
        text=f"tweet {tweet_id}",
        hashtags=["RHOSLC"],
        mentions=[],
        likes=10,
        retweets=2,
        replies=3,
        quotes=1,
        views=100,
        url=f"https://x.com/{username}/status/{tweet_id}",
        username=username,
        display_name=display_name,
        user_verified=True,
        is_reply=is_reply,
        is_retweet=False,
        is_quote=False,
        reply_to_tweet_id="root" if is_reply else None,
        user_profile_url=profile_url,
        user_avatar_url=avatar_url,
    )


class FakeTwitterScraper:
    def __init__(
        self,
        posts: list[Tweet],
        retrieval_meta: Mapping[str, Any] | None = None,
        *,
        replies_by_tweet_id: Mapping[str, list[Tweet]] | None = None,
        quotes_by_tweet_id: Mapping[str, list[Tweet]] | None = None,
    ) -> None:
        self.posts = posts
        self.last_retrieval_meta = dict(retrieval_meta or {})
        self.scrape_calls: list[dict[str, Any]] = []
        self.replies_by_tweet_id = {key: list(value) for key, value in (replies_by_tweet_id or {}).items()}
        self.quotes_by_tweet_id = {key: list(value) for key, value in (quotes_by_tweet_id or {}).items()}
        self.reply_calls: list[dict[str, Any]] = []
        self.quote_calls: list[dict[str, Any]] = []
        self.last_reply_fetch_reason: str | None = None
        self.last_quote_fetch_reason: str | None = None

    def scrape(self, config: TwitterScrapeConfig, progress_cb=None) -> list[Tweet]:
        self.scrape_calls.append({"config": config, "progress_cb": progress_cb})
        if progress_cb:
            progress_cb(
                {
                    "phase": "scrape_complete",
                    "pages_scanned": self.last_retrieval_meta.get("pages_scanned", 0),
                    "posts_checked": self.last_retrieval_meta.get("posts_checked", len(self.posts)),
                    "matched_posts": len(self.posts),
                }
            )
        return list(self.posts)

    def fetch_tweet_replies(
        self,
        tweet_id: str,
        delay: float = 0.5,
        *,
        search_max_pages: int = 20,
        twikit_max_pages: int = 5,
        progress_callback=None,
    ) -> list[Tweet]:
        self.reply_calls.append(
            {
                "tweet_id": tweet_id,
                "delay": delay,
                "search_max_pages": search_max_pages,
                "twikit_max_pages": twikit_max_pages,
            }
        )
        replies = list(self.replies_by_tweet_id.get(tweet_id, []))
        if progress_callback:
            progress_callback({"phase": "fake_replies_page", "comments_fetched": len(replies)})
        return replies

    def fetch_tweet_quotes(
        self,
        tweet_id: str,
        delay: float = 0.5,
        max_pages: int = 5,
        progress_callback=None,
    ) -> list[Tweet]:
        self.quote_calls.append({"tweet_id": tweet_id, "delay": delay, "max_pages": max_pages})
        quotes = list(self.quotes_by_tweet_id.get(tweet_id, []))
        if progress_callback:
            progress_callback({"phase": "fake_quotes_page", "quotes_fetched": len(quotes)})
        return quotes


class FakeCatalogPersistenceAdapter:
    def __init__(self) -> None:
        self.shared_persist_calls: list[dict[str, Any]] = []
        self.shared_upsert_calls: list[dict[str, Any]] = []
        self.legacy_upsert_calls: list[dict[str, Any]] = []

    def persist_shared_catalog_posts_with_progress(self, **kwargs) -> list[dict[str, Any]]:
        self.shared_persist_calls.append(kwargs)
        rows: list[dict[str, Any]] = []
        for item in kwargs["items"]:
            row = kwargs["upsert_item"](item)
            if row:
                rows.append(row)
        return rows

    def upsert_shared_catalog_post(self, **kwargs) -> dict[str, Any]:
        self.shared_upsert_calls.append(kwargs)
        tweet = kwargs["post"]
        return {
            "platform": kwargs["platform"],
            "run_id": kwargs["run_id"],
            "source_account": kwargs["account_handle"],
            "tweet_id": tweet.tweet_id,
        }

    def upsert_tweet(self, context, **kwargs) -> dict[str, Any]:
        self.legacy_upsert_calls.append({"context": context, **kwargs})
        tweet = kwargs["tweet"]
        return {
            "job_id": kwargs["job_id"],
            "run_id": kwargs["run_id"],
            "source_account": kwargs["account"],
            "tweet_id": tweet.tweet_id,
        }


def _dependencies(
    *,
    scraper: FakeTwitterScraper,
    persistence: FakeCatalogPersistenceAdapter,
    captured_scraper_kwargs: dict[str, Any] | None = None,
    load_existing_catalog_posts=None,
) -> TwitterPostsCatalogDependencies:
    def _scraper_factory(**kwargs):
        if captured_scraper_kwargs is not None:
            captured_scraper_kwargs.update(kwargs)
        return scraper

    return TwitterPostsCatalogDependencies(
        scraper_factory=_scraper_factory,
        load_twitter_auth=lambda: ({"auth_token": "cookie-auth", "ct0": "cookie-csrf"}, "bearer-token"),
        load_twikit_credentials=lambda cookies: {
            "auth_token": str((cookies or {}).get("auth_token") or ""),
            "ct0": str((cookies or {}).get("ct0") or ""),
        },
        now_utc=lambda: FIXED_NOW,
        load_existing_catalog_posts=load_existing_catalog_posts,
        persist_shared_catalog_posts_with_progress=persistence.persist_shared_catalog_posts_with_progress,
        upsert_shared_catalog_post=persistence.upsert_shared_catalog_post,
        upsert_tweet=persistence.upsert_tweet,
    )


def test_shared_catalog_filters_replies_and_preserves_metadata_shape() -> None:
    scraper = FakeTwitterScraper(
        [
            _tweet("root"),
            _tweet("reply", is_reply=True),
        ],
        retrieval_meta={
            "retrieval_mode": "fake",
            "pages_scanned": 2,
            "posts_checked": 5,
            "tweet_count": 2,
            "window_contract": "whole_day",
        },
    )
    persistence = FakeCatalogPersistenceAdapter()
    captured_scraper_kwargs: dict[str, Any] = {}
    progress_events: list[dict[str, Any]] = []

    rows, retrieval_meta = scrape_shared_twitter_posts(
        run_id="run-1",
        account_handle="BravoTV",
        config={
            "pipeline_ingest_mode": SHARED_MODE,
            "catalog_action_scope": "full_history",
            "profile_snapshot": {"username": "existing", "display_name": "Existing Name"},
        },
        job_id="job-1",
        progress_cb=progress_events.append,
        dependencies=_dependencies(
            scraper=scraper,
            persistence=persistence,
            captured_scraper_kwargs=captured_scraper_kwargs,
        ),
    )

    assert captured_scraper_kwargs == {
        "cookies": {"auth_token": "cookie-auth", "ct0": "cookie-csrf"},
        "bearer_token": "bearer-token",
        "twikit_credentials": {"auth_token": "cookie-auth", "ct0": "cookie-csrf"},
    }
    scrape_config = scraper.scrape_calls[0]["config"]
    assert scrape_config.query == "from:BravoTV"
    assert scrape_config.include_replies is False
    assert scrape_config.include_links is True
    assert scrape_config.delay_seconds == 0.35
    assert scrape_config.max_pages is None
    assert scrape_config.window_start_day() == "2006-01-01"
    assert scrape_config.window_end_day_inclusive() == "2026-05-04"

    assert [row["tweet_id"] for row in rows] == ["root"]
    assert [call["post"].tweet_id for call in persistence.shared_upsert_calls] == ["root"]
    assert [call["tweet"].tweet_id for call in persistence.legacy_upsert_calls] == ["root"]
    assert persistence.legacy_upsert_calls[0]["run_id"] == "run-1"
    assert persistence.shared_persist_calls[0]["platform"] == "twitter"
    assert persistence.shared_persist_calls[0]["run_id"] == "run-1"
    assert persistence.shared_persist_calls[0]["account_handle"] == "BravoTV"
    assert [tweet.tweet_id for tweet in persistence.shared_persist_calls[0]["items"]] == ["root"]
    assert persistence.shared_persist_calls[0]["progress_cb"].__self__ is progress_events

    assert retrieval_meta == {
        "retrieval_mode": "fake",
        "pages_scanned": 2,
        "posts_checked": 5,
        "tweet_count": 2,
        "window_contract": "whole_day",
        "persist_counters": {
            "posts_upserted": 1,
            "catalog_posts_upserted": 1,
            "materialized_posts_upserted": 1,
            "comments_upserted": 0,
        },
        "profile_snapshot": {
            "username": "existing",
            "display_name": "Existing Name",
            "avatar_url": "https://pbs.twimg.com/profile_images/123/bravo_400x400.jpg",
            "is_verified": True,
            "profile_url": "https://x.com/bravotv",
        },
    }
    assert progress_events == [
        {
            "phase": "scrape_complete",
            "pages_scanned": 2,
            "posts_checked": 5,
            "matched_posts": 2,
        },
        {
            "phase": "materialize_catalog_posts",
            "pages_scanned": 2,
            "posts_checked": 1,
            "matched_posts": 1,
            "materialized_posts": 1,
        },
    ]


def test_legacy_catalog_uses_tweet_upsert_and_caps_non_full_history_pages() -> None:
    scraper = FakeTwitterScraper([_tweet("root")], retrieval_meta={"pages_scanned": 99, "posts_checked": 99})
    persistence = FakeCatalogPersistenceAdapter()

    rows, _retrieval_meta = scrape_shared_twitter_posts(
        run_id="run-ignored",
        account_handle="BravoTV",
        config={
            "date_start": "2026-04-01T00:00:00Z",
            "date_end": "2026-04-30T00:00:00Z",
            "max_posts_per_target": 50,
        },
        job_id="legacy-job",
        dependencies=_dependencies(scraper=scraper, persistence=persistence),
    )

    scrape_config = scraper.scrape_calls[0]["config"]
    assert scrape_config.max_pages == 20
    assert scrape_config.window_start_day() == "2026-04-01"
    assert scrape_config.window_end_day_inclusive() == "2026-04-30"
    assert rows == [
        {
            "job_id": "legacy-job",
            "run_id": None,
            "source_account": "BravoTV",
            "tweet_id": "root",
        }
    ]
    assert persistence.shared_persist_calls == []
    assert persistence.shared_upsert_calls == []
    assert len(persistence.legacy_upsert_calls) == 1
    assert persistence.legacy_upsert_calls[0]["context"] is None
    assert persistence.legacy_upsert_calls[0]["run_id"] is None


def test_shared_catalog_hydrates_twitter_comments_when_selected() -> None:
    root = _tweet("root", username="TheTraitorsUS")
    root.replies = 787
    root.quotes = 787
    reply = _tweet("reply-1", username="viewer", is_reply=True, profile_url="https://x.com/viewer")
    reply.reply_to_tweet_id = None
    quote = _tweet("quote-1", username="critic", profile_url="https://x.com/critic")
    quote.is_quote = True
    quote.quoted_tweet_id = None
    scraper = FakeTwitterScraper(
        [root],
        replies_by_tweet_id={"root": [reply]},
        quotes_by_tweet_id={"root": [quote]},
    )
    persistence = FakeCatalogPersistenceAdapter()
    progress_events: list[dict[str, Any]] = []

    rows, retrieval_meta = scrape_shared_twitter_posts(
        run_id="run-1",
        account_handle="TheTraitorsUS",
        config={
            "pipeline_ingest_mode": SHARED_MODE,
            "catalog_action_scope": "full_history",
            "twitter_comments_in_posts_stage": True,
            "max_comments_per_post": 10,
        },
        job_id="job-1",
        progress_cb=progress_events.append,
        dependencies=_dependencies(scraper=scraper, persistence=persistence),
    )

    assert [row["tweet_id"] for row in rows] == ["root"]
    assert scraper.reply_calls == [{"tweet_id": "root", "delay": 0.5, "search_max_pages": 5, "twikit_max_pages": 5}]
    assert scraper.quote_calls == [{"tweet_id": "root", "delay": 0.5, "max_pages": 5}]
    assert [call["tweet"].tweet_id for call in persistence.legacy_upsert_calls] == ["root", "reply-1", "quote-1"]
    reply_call = persistence.legacy_upsert_calls[1]
    quote_call = persistence.legacy_upsert_calls[2]
    assert reply_call["tweet"].reply_to_tweet_id == "root"
    assert reply_call["tweet"].is_reply is True
    assert reply_call["tweet"].thread_root_tweet_id == "root"
    assert reply_call["tweet"].twitter_context_role == "audience_reply"
    assert quote_call["tweet"].quoted_tweet_id == "root"
    assert quote_call["tweet"].is_quote is True
    assert quote_call["tweet"].twitter_context_role == "quote"
    assert retrieval_meta["persist_counters"] == {
        "posts_upserted": 1,
        "catalog_posts_upserted": 1,
        "materialized_posts_upserted": 1,
        "comments_upserted": 1,
    }
    assert retrieval_meta["comment_stats"] == {
        "comments_fetched": 1,
        "comments_upserted": 1,
        "comment_errors": 0,
    }
    assert retrieval_meta["quote_stats"] == {
        "quotes_fetched": 1,
        "quotes_upserted": 1,
        "quote_errors": 0,
    }
    quote_progress = [event for event in progress_events if event.get("phase") == "fake_quotes_page"]
    assert quote_progress
    assert quote_progress[-1]["current_source_id"] == "root"


def test_shared_catalog_hydrates_twitter_comments_uncapped_by_default() -> None:
    root = _tweet("root", username="TheTraitorsUS")
    root.replies = 787
    root.quotes = 787
    replies = [_tweet(f"reply-{index}", username="viewer", is_reply=True) for index in range(55)]
    quotes: list[Tweet] = []
    for index in range(55):
        quote = _tweet(f"quote-{index}", username="critic")
        quote.is_quote = True
        quotes.append(quote)
    scraper = FakeTwitterScraper(
        [root],
        replies_by_tweet_id={"root": replies},
        quotes_by_tweet_id={"root": quotes},
    )
    persistence = FakeCatalogPersistenceAdapter()

    rows, retrieval_meta = scrape_shared_twitter_posts(
        run_id="run-1",
        account_handle="TheTraitorsUS",
        config={
            "pipeline_ingest_mode": SHARED_MODE,
            "catalog_action_scope": "full_history",
            "twitter_comments_in_posts_stage": True,
        },
        job_id="job-1",
        dependencies=_dependencies(scraper=scraper, persistence=persistence),
    )

    assert [row["tweet_id"] for row in rows] == ["root"]
    assert scraper.reply_calls == [{"tweet_id": "root", "delay": 0.5, "search_max_pages": 81, "twikit_max_pages": 81}]
    assert scraper.quote_calls == [{"tweet_id": "root", "delay": 0.5, "max_pages": 81}]
    persisted_comment_ids = [call["tweet"].tweet_id for call in persistence.legacy_upsert_calls[1:]]
    assert persisted_comment_ids == [tweet.tweet_id for tweet in replies] + [tweet.tweet_id for tweet in quotes]
    assert retrieval_meta["persist_counters"] == {
        "posts_upserted": 1,
        "catalog_posts_upserted": 1,
        "materialized_posts_upserted": 1,
        "comments_upserted": 55,
    }
    assert retrieval_meta["quote_stats"] == {
        "quotes_fetched": 55,
        "quotes_upserted": 55,
        "quote_errors": 0,
    }


@pytest.mark.parametrize(
    ("error_code", "stop_reason"),
    [
        ("twitter_search_fallback_exhausted", "playwright_no_search_payload"),
        (None, "playwright_no_tweet_entries"),
    ],
)
def test_shared_catalog_seeds_existing_catalog_posts_when_search_window_has_no_usable_entries(
    error_code: str | None,
    stop_reason: str,
) -> None:
    root = _tweet("seed-root", username="TheTraitorsUS")
    root.replies = 5
    reply = _tweet("reply-1", username="viewer", is_reply=True)
    scraper = FakeTwitterScraper(
        [],
        retrieval_meta={
            "error_code": error_code,
            "error_class": "TwitterSearchFallbackError" if error_code else None,
            "retryable": bool(error_code),
            "stop_reason": stop_reason,
            "pages_scanned": 0,
            "posts_checked": 0,
        },
        replies_by_tweet_id={"seed-root": [reply]},
    )
    persistence = FakeCatalogPersistenceAdapter()
    loader_calls: list[dict[str, Any]] = []

    def _load_existing_catalog_posts(**kwargs) -> list[Tweet]:
        loader_calls.append(kwargs)
        return [root]

    rows, retrieval_meta = scrape_shared_twitter_posts(
        run_id="run-1",
        account_handle="TheTraitorsUS",
        config={
            "pipeline_ingest_mode": SHARED_MODE,
            "catalog_action_scope": "bounded_window",
            "date_start": "2026-02-16T01:37:33Z",
            "date_end": "2026-02-26T01:37:33Z",
            "twitter_comments_in_posts_stage": True,
            "max_comments_per_post": 10,
        },
        job_id="job-1",
        dependencies=_dependencies(
            scraper=scraper,
            persistence=persistence,
            load_existing_catalog_posts=_load_existing_catalog_posts,
        ),
    )

    assert [row["tweet_id"] for row in rows] == ["seed-root"]
    assert len(loader_calls) == 1
    assert loader_calls[0]["account_handle"] == "TheTraitorsUS"
    assert loader_calls[0]["date_start"] == datetime(2026, 2, 16, tzinfo=UTC)
    assert loader_calls[0]["date_end"] == datetime(2026, 2, 27, tzinfo=UTC)
    assert [call["post"].tweet_id for call in persistence.shared_upsert_calls] == ["seed-root"]
    assert [call["tweet"].tweet_id for call in persistence.legacy_upsert_calls] == ["seed-root", "reply-1"]
    assert scraper.reply_calls == [
        {"tweet_id": "seed-root", "delay": 0.5, "search_max_pages": 5, "twikit_max_pages": 5}
    ]
    assert retrieval_meta["catalog_seeded_fallback"] is True
    assert retrieval_meta["catalog_seeded_post_count"] == 1
    assert retrieval_meta["catalog_seeded_original_error_code"] == error_code
    assert retrieval_meta["catalog_seeded_original_stop_reason"] == stop_reason
    assert retrieval_meta["retrieval_mode"] == "catalog_seeded_window"
    assert retrieval_meta["stop_reason"] == "catalog_seeded_window"
    assert retrieval_meta["error_code"] is None
    assert retrieval_meta["error_class"] is None
    assert retrieval_meta["retryable"] is False
    assert retrieval_meta["complete"] is True
    assert retrieval_meta["persist_counters"] == {
        "posts_upserted": 1,
        "catalog_posts_upserted": 1,
        "materialized_posts_upserted": 1,
        "comments_upserted": 1,
    }


def test_shared_catalog_completes_empty_seeded_window_without_retryable_error() -> None:
    scraper = FakeTwitterScraper(
        [],
        retrieval_meta={
            "error_code": "twitter_search_fallback_exhausted",
            "error_class": "TwitterSearchFallbackError",
            "retryable": True,
            "stop_reason": "playwright_no_tweet_entries",
        },
    )
    persistence = FakeCatalogPersistenceAdapter()

    rows, retrieval_meta = scrape_shared_twitter_posts(
        run_id="run-1",
        account_handle="TheTraitorsUS",
        config={
            "pipeline_ingest_mode": SHARED_MODE,
            "catalog_action_scope": "bounded_window",
            "date_start": "2025-10-29T17:15:28Z",
            "date_end": "2025-11-08T17:15:28Z",
            "twitter_comments_in_posts_stage": True,
        },
        job_id="job-1",
        dependencies=_dependencies(
            scraper=scraper,
            persistence=persistence,
            load_existing_catalog_posts=lambda **_kwargs: [],
        ),
    )

    assert rows == []
    assert retrieval_meta["catalog_seeded_empty_window"] is True
    assert retrieval_meta["catalog_seeded_post_count"] == 0
    assert retrieval_meta["retrieval_mode"] == "catalog_seeded_empty_window"
    assert retrieval_meta["error_code"] is None
    assert retrieval_meta["error_class"] is None
    assert retrieval_meta["retryable"] is False
    assert retrieval_meta["complete"] is True


@pytest.mark.parametrize(("raw_limit", "expected_pages"), [(7, 7), (0, None)])
def test_full_history_with_explicit_limit_uses_shared_stage_page_policy(raw_limit, expected_pages) -> None:
    scraper = FakeTwitterScraper([_tweet("root")])
    persistence = FakeCatalogPersistenceAdapter()

    scrape_shared_twitter_posts(
        run_id="run-1",
        account_handle="BravoTV",
        config={
            "pipeline_ingest_mode": SHARED_MODE,
            "catalog_action_scope": "full_history",
            "max_posts_per_target": raw_limit,
        },
        job_id="job-1",
        dependencies=_dependencies(scraper=scraper, persistence=persistence),
    )

    assert scraper.scrape_calls[0]["config"].max_pages == expected_pages


def test_dependencies_keep_worker_lane_boundary_out_of_catalog_module() -> None:
    field_names = set(TwitterPostsCatalogDependencies.__dataclass_fields__)

    assert not any(
        token in field_name
        for field_name in field_names
        for token in ("dispatch", "enqueue", "lane", "worker", "queue")
    )
