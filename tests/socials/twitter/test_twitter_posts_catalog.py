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
    def __init__(self, posts: list[Tweet], retrieval_meta: Mapping[str, Any] | None = None) -> None:
        self.posts = posts
        self.last_retrieval_meta = dict(retrieval_meta or {})
        self.scrape_calls: list[dict[str, Any]] = []

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
    assert persistence.legacy_upsert_calls == []
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
        "persist_counters": {"posts_upserted": 1, "comments_upserted": 0},
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
        }
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
