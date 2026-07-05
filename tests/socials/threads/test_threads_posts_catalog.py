from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.threads.posts_catalog import (
    ThreadsPostsCatalogDependencies,
    scrape_shared_threads_posts,
)


class _FakeSharedCatalogPersistence:
    def __init__(self) -> None:
        self.persist_calls: list[dict[str, Any]] = []
        self.upsert_calls: list[dict[str, Any]] = []

    def persist_shared_catalog_posts_with_progress(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.persist_calls.append(kwargs)
        retrieval_meta = kwargs["retrieval_meta"]
        progress_cb = kwargs.get("progress_cb")
        items = list(kwargs["items"])
        rows: list[dict[str, Any]] = []
        if progress_cb:
            progress_cb(
                {
                    "phase": "persist_catalog_posts",
                    "pages_scanned": retrieval_meta.get("pages_scanned", 0),
                    "posts_checked": retrieval_meta.get("posts_checked", len(items)),
                    "matched_posts": len(items),
                    "saved_posts": 0,
                }
            )
        for item in items:
            row = kwargs["upsert_item"](item)
            if row:
                rows.append(row)
        if progress_cb:
            progress_cb(
                {
                    "phase": "persist_catalog_posts",
                    "pages_scanned": retrieval_meta.get("pages_scanned", 0),
                    "posts_checked": retrieval_meta.get("posts_checked", len(items)),
                    "matched_posts": len(items),
                    "saved_posts": len(rows),
                }
            )
        retrieval_meta["persist_counters"] = {
            "posts_upserted": len(rows),
            "comments_upserted": 0,
        }
        return rows

    def upsert_shared_catalog_post(self, **kwargs: Any) -> dict[str, Any]:
        self.upsert_calls.append(kwargs)
        post = kwargs["post"]
        return {
            "id": f"catalog-{post.post_id}",
            "source_id": post.post_id,
            "source_account": kwargs["account_handle"],
        }


class _FakeThreadsScraper:
    def __init__(self, posts: list[Any], retrieval_meta: dict[str, Any], *, cookies: dict[str, str]) -> None:
        self.posts = posts
        self.cookies = cookies
        self.config: Any | None = None
        self.last_retrieval_meta = dict(retrieval_meta)

    def scrape(self, config: Any, *, progress_cb=None) -> list[Any]:
        self.config = config
        if progress_cb:
            progress_cb(
                {
                    "phase": "scrape_posts",
                    "pages_scanned": 1,
                    "posts_checked": len(self.posts),
                    "matched_posts": len(self.posts),
                }
            )
        return list(self.posts)


def _posts_catalog_dependencies(
    *,
    scraper: _FakeThreadsScraper,
    persistence: _FakeSharedCatalogPersistence | None = None,
    upsert_threads_post=None,
) -> ThreadsPostsCatalogDependencies:
    def _scraper_factory(*, cookies: dict[str, str]) -> _FakeThreadsScraper:
        scraper.cookies = dict(cookies)
        return scraper

    return ThreadsPostsCatalogDependencies(
        scraper_factory=_scraper_factory,
        load_cookies=lambda: {"sessionid": "session-cookie", "csrftoken": "csrf-cookie"},
        persist_shared_catalog_posts_with_progress=(
            None if persistence is None else persistence.persist_shared_catalog_posts_with_progress
        ),
        upsert_shared_catalog_post=None if persistence is None else persistence.upsert_shared_catalog_post,
        upsert_threads_post=upsert_threads_post,
    )


def test_threads_posts_catalog_shared_mode_preserves_config_metadata_and_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_THREADS_DELAY_SEC", "0.25")
    post = SimpleNamespace(
        post_id="post-1",
        username="BravoTV",
        user_avatar_url="https://images.test/profile_pic_640x640.jpg",
        raw_data={"full_name": "Bravo TV", "is_verified": True},
    )
    scraper = _FakeThreadsScraper(
        [post],
        {
            "source": "threads_graphql_api",
            "pages_scanned": 1,
            "posts_checked": 1,
            "last_cursor": "cursor-1",
        },
        cookies={},
    )
    persistence = _FakeSharedCatalogPersistence()
    dependencies = _posts_catalog_dependencies(scraper=scraper, persistence=persistence)
    progress_events: list[dict[str, Any]] = []

    rows, meta = scrape_shared_threads_posts(
        run_id="run-1",
        account_handle="@BravoTV",
        config={
            "pipeline_ingest_mode": "shared_account_catalog_backfill",
            "max_posts_per_target": 3,
            "date_start": datetime(2026, 1, 1, tzinfo=UTC),
            "date_end": datetime(2026, 2, 1, tzinfo=UTC),
        },
        job_id="job-1",
        progress_cb=lambda payload: progress_events.append(dict(payload)),
        dependencies=dependencies,
    )

    created_scraper = persistence.upsert_calls[0]["post"]
    persist_call = persistence.persist_calls[0]
    assert rows == [{"id": "catalog-post-1", "source_id": "post-1", "source_account": "@BravoTV"}]
    assert created_scraper is post
    assert scraper.config.username == "@BravoTV"
    assert scraper.config.date_start == datetime(2026, 1, 1, tzinfo=UTC)
    assert scraper.config.date_end == datetime(2026, 2, 1, tzinfo=UTC)
    assert scraper.config.delay_seconds == 0.5
    assert scraper.config.max_pages == 3
    assert scraper.cookies == {"sessionid": "session-cookie", "csrftoken": "csrf-cookie"}
    assert persist_call["platform"] == "threads"
    assert persist_call["run_id"] == "run-1"
    assert persist_call["account_handle"] == "@BravoTV"
    assert persist_call["retrieval_meta"]["last_cursor"] == "cursor-1"
    assert persist_call["retrieval_meta"]["persist_counters"] == {
        "posts_upserted": 1,
        "comments_upserted": 0,
    }
    assert meta["source"] == "threads_graphql_api"
    assert meta["profile_snapshot"] == {
        "username": "bravotv",
        "display_name": "Bravo TV",
        "avatar_url": "https://images.test/profile_pic_640x640.jpg",
        "is_verified": True,
        "profile_url": "https://www.threads.com/@bravotv",
    }
    assert [event["phase"] for event in progress_events] == [
        "scrape_posts",
        "persist_catalog_posts",
        "persist_catalog_posts",
    ]


def test_threads_posts_catalog_builds_scrape_config_from_env_and_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_THREADS_DELAY_SEC", "2.25")
    captured: dict[str, Any] = {}

    class _CapturingScraper(_FakeThreadsScraper):
        def scrape(self, config: Any, *, progress_cb=None) -> list[Any]:
            captured["config"] = config
            captured["cookies"] = dict(self.cookies)
            return []

    scraper = _CapturingScraper([], {"pages_scanned": 0, "posts_checked": 0}, cookies={})
    persistence = _FakeSharedCatalogPersistence()

    rows, meta = scrape_shared_threads_posts(
        run_id="run-1",
        account_handle="bravotv",
        config={
            "pipeline_ingest_mode": "shared_account_catalog_backfill",
            "max_posts_per_target": 0,
            "date_start": "2026-01-01T00:00:00Z",
            "date_end": "2026-01-31T00:00:00Z",
        },
        job_id="job-1",
        dependencies=_posts_catalog_dependencies(scraper=scraper, persistence=persistence),
    )

    assert rows == []
    assert meta["persist_counters"] == {"posts_upserted": 0, "comments_upserted": 0}
    assert captured["cookies"] == {"sessionid": "session-cookie", "csrftoken": "csrf-cookie"}
    assert captured["config"].username == "bravotv"
    assert captured["config"].date_start == "2026-01-01T00:00:00Z"
    assert captured["config"].date_end == "2026-01-31T00:00:00Z"
    assert captured["config"].delay_seconds == 2.25
    assert captured["config"].max_pages is None


def test_threads_posts_catalog_shared_mode_is_uncapped_by_default() -> None:
    captured: dict[str, Any] = {}

    class _CapturingScraper(_FakeThreadsScraper):
        def scrape(self, config: Any, *, progress_cb=None) -> list[Any]:
            del progress_cb
            captured["config"] = config
            return []

    scraper = _CapturingScraper([], {"pages_scanned": 0, "posts_checked": 0}, cookies={})
    persistence = _FakeSharedCatalogPersistence()

    scrape_shared_threads_posts(
        run_id="run-1",
        account_handle="bravotv",
        config={"pipeline_ingest_mode": "shared_account_catalog_backfill"},
        job_id="job-1",
        dependencies=_posts_catalog_dependencies(scraper=scraper, persistence=persistence),
    )

    assert captured["config"].max_pages is None


def test_threads_posts_catalog_non_shared_mode_uses_legacy_post_upsert() -> None:
    post = SimpleNamespace(
        post_id="post-2",
        username="bravotv",
        user_avatar_url=None,
        raw_data={},
    )
    scraper = _FakeThreadsScraper([post], {"source": "public_meta_fallback"}, cookies={})
    upsert_calls: list[dict[str, Any]] = []

    def _upsert_threads_post(_context: Any, **kwargs: Any) -> dict[str, Any]:
        upsert_calls.append(kwargs)
        return {"id": "threads-row-1", "source_id": kwargs["post"].post_id}

    rows, meta = scrape_shared_threads_posts(
        run_id="run-1",
        account_handle="bravotv",
        config={"pipeline_ingest_mode": "legacy_season_targeted"},
        job_id="job-1",
        dependencies=_posts_catalog_dependencies(
            scraper=scraper,
            persistence=None,
            upsert_threads_post=_upsert_threads_post,
        ),
    )

    assert rows == [{"id": "threads-row-1", "source_id": "post-2"}]
    assert upsert_calls == [{"job_id": "job-1", "account": "bravotv", "post": post}]
    assert meta["source"] == "public_meta_fallback"
    assert meta["profile_snapshot"]["username"] == "bravotv"
    assert meta["profile_snapshot"]["profile_url"] == "https://www.threads.com/@bravotv"


def test_threads_posts_catalog_merges_config_profile_snapshot_first() -> None:
    post = SimpleNamespace(
        post_id="post-3",
        username="bravotv",
        user_avatar_url="https://images.test/post-avatar.jpg",
        raw_data={"full_name": "Post Display", "is_verified": True},
    )
    scraper = _FakeThreadsScraper([post], {}, cookies={})
    persistence = _FakeSharedCatalogPersistence()

    _rows, meta = scrape_shared_threads_posts(
        run_id="run-1",
        account_handle="bravotv",
        config={
            "pipeline_ingest_mode": "shared_account_catalog_backfill",
            "profile_snapshot": {
                "username": "bravotv",
                "display_name": "Existing Display",
                "avatar_url": "https://images.test/existing-avatar.jpg",
                "follower_count": 1234,
            },
        },
        job_id="job-1",
        dependencies=_posts_catalog_dependencies(scraper=scraper, persistence=persistence),
    )

    assert meta["profile_snapshot"]["display_name"] == "Existing Display"
    assert meta["profile_snapshot"]["avatar_url"] == "https://images.test/existing-avatar.jpg"
    assert meta["profile_snapshot"]["follower_count"] == 1234
    assert meta["profile_snapshot"]["is_verified"] is True


def test_threads_posts_catalog_marks_graphql_no_edges_empty_result_retryable() -> None:
    scraper = _FakeThreadsScraper(
        [],
        {
            "source": "threads_graphql_api",
            "pages_scanned": 1,
            "posts_checked": 0,
            "matched_posts": 0,
            "stop_reason": "no_edges",
        },
        cookies={},
    )
    persistence = _FakeSharedCatalogPersistence()

    rows, meta = scrape_shared_threads_posts(
        run_id="run-soft-block",
        account_handle="bravotv",
        config={"pipeline_ingest_mode": "shared_account_catalog_backfill"},
        job_id="job-soft-block",
        dependencies=_posts_catalog_dependencies(scraper=scraper, persistence=persistence),
    )

    assert rows == []
    assert meta["error_code"] == "threads_empty_soft_block"
    assert meta["error_class"] == "ThreadsEmptySoftBlock"
    assert meta["empty_result_reason"] == "no_edges"
    assert meta["retryable"] is True
    assert meta["complete"] is False
    assert meta["persist_counters"] == {"posts_upserted": 0, "comments_upserted": 0}
