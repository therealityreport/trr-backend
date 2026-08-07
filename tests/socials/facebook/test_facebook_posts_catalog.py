from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.facebook.posts_catalog import (
    FacebookPostsCatalogDependencies,
    scrape_shared_facebook_posts,
)


class _FakeFacebookScraper:
    def __init__(self, *, posts: list[Any], retrieval_meta: dict[str, Any]) -> None:
        self.posts = posts
        self.last_retrieval_meta = dict(retrieval_meta)
        self.config: Any = None

    def scrape(self, config: Any, *, progress_cb=None) -> list[Any]:
        self.config = config
        if progress_cb:
            progress_cb(
                {
                    "phase": "scrape_posts",
                    "pages_scanned": self.last_retrieval_meta.get("pages_scanned", 0),
                    "posts_checked": self.last_retrieval_meta.get("posts_checked", len(self.posts)),
                    "matched_posts": len(self.posts),
                }
            )
        return list(self.posts)


class _LocalFakeCatalogPersistence:
    def __init__(self) -> None:
        self.upsert_calls: list[dict[str, Any]] = []
        self.persist_calls: list[dict[str, Any]] = []

    def persist_shared_catalog_posts_with_progress(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.persist_calls.append(dict(kwargs))
        progress_cb = kwargs.get("progress_cb")
        items = list(kwargs.get("items") or [])
        retrieval_meta = kwargs["retrieval_meta"]
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
        retrieval_meta["persist_counters"] = {
            "posts_upserted": len(rows),
            "comments_upserted": 0,
        }
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
        return rows

    def upsert_shared_catalog_post(self, **kwargs: Any) -> dict[str, Any]:
        self.upsert_calls.append(dict(kwargs))
        post = kwargs["post"]
        return {
            "id": f"catalog-{post.post_id}",
            "source_id": post.post_id,
            "source_account": kwargs["account_handle"],
        }


def _scrape_config_factory(**kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(**kwargs)


def test_facebook_posts_catalog_uses_injected_scraper_document_fetch_and_shared_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_FACEBOOK_DELAY_SEC", "0.2")
    monkeypatch.setenv("SOCIAL_FACEBOOK_MAX_SCROLL_ITERATIONS", "7")
    scraper = _FakeFacebookScraper(
        posts=[
            SimpleNamespace(
                post_id="fb-1",
                username="Bravo TV",
                user_avatar_url="https://scontent.xx.fbcdn.net/profile_pic_320x320.jpg?stp=dst-jpg",
            )
        ],
        retrieval_meta={
            "source": "scroll_and_static",
            "pages_scanned": 2,
            "posts_checked": 3,
            "matched_posts": 1,
        },
    )
    persistence = _LocalFakeCatalogPersistence()
    created_scraper_kwargs: dict[str, Any] = {}

    def _document_fetcher_factory(**_kwargs: Any) -> object:
        return object()

    def _scraper_factory(**kwargs: Any) -> _FakeFacebookScraper:
        created_scraper_kwargs.update(kwargs)
        return scraper

    dependencies = FacebookPostsCatalogDependencies(
        scraper_factory=_scraper_factory,
        document_fetcher_factory=_document_fetcher_factory,
        scrape_config_factory=_scrape_config_factory,
        load_cookies=lambda: {"c_user": "1", "xs": "token"},
        persist_shared_catalog_posts_with_progress=persistence.persist_shared_catalog_posts_with_progress,
        upsert_shared_catalog_post=persistence.upsert_shared_catalog_post,
    )
    progress_events: list[dict[str, Any]] = []

    rows, meta = scrape_shared_facebook_posts(
        run_id="run-1",
        account_handle="bravotv",
        config={
            "pipeline_ingest_mode": "shared_account_catalog_backfill",
            "max_posts_per_target": "3",
            "date_start": "2026-01-01",
        },
        job_id="job-1",
        progress_cb=lambda payload: progress_events.append(dict(payload)),
        dependencies=dependencies,
    )

    assert created_scraper_kwargs["cookies"] == {"c_user": "1", "xs": "token"}
    assert created_scraper_kwargs["document_fetcher_factory"] is _document_fetcher_factory
    assert scraper.config.page_handle == "bravotv"
    assert scraper.config.date_start == datetime(2026, 1, 1, tzinfo=UTC)
    assert scraper.config.delay_seconds == 0.5
    assert scraper.config.max_pages == 3
    assert scraper.config.include_feed is True
    assert scraper.config.include_reels is True
    assert scraper.config.include_photos is True
    assert scraper.config.max_scroll_iterations == 7
    assert rows == [{"id": "catalog-fb-1", "source_id": "fb-1", "source_account": "bravotv"}]
    assert persistence.upsert_calls[0]["platform"] == "facebook"
    assert persistence.upsert_calls[0]["run_id"] == "run-1"
    assert persistence.upsert_calls[0]["account_handle"] == "bravotv"
    assert persistence.upsert_calls[0]["post"].post_id == "fb-1"
    assert meta["source"] == "scroll_and_static"
    assert meta["profile_snapshot"]["username"] == "bravotv"
    assert meta["profile_snapshot"]["display_name"] == "Bravo TV"
    assert meta["profile_snapshot"]["avatar_url"] == "https://scontent.xx.fbcdn.net/profile_pic_320x320.jpg?stp=dst-jpg"
    assert meta["profile_snapshot"]["profile_url"] == "https://www.facebook.com/bravotv"
    assert meta["persist_counters"] == {"posts_upserted": 1, "comments_upserted": 0}
    assert [event["phase"] for event in progress_events] == [
        "scrape_posts",
        "persist_catalog_posts",
        "persist_catalog_posts",
    ]


def test_facebook_posts_catalog_uses_default_env_values_and_legacy_post_upsert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SOCIAL_FACEBOOK_DELAY_SEC", raising=False)
    monkeypatch.delenv("SOCIAL_FACEBOOK_MAX_SCROLL_ITERATIONS", raising=False)
    scraper = _FakeFacebookScraper(
        posts=[SimpleNamespace(post_id="fb-2", username="Bravo TV", user_avatar_url=None)],
        retrieval_meta={"source": "public_meta_fallback"},
    )
    upsert_calls: list[dict[str, Any]] = []

    def _scraper_factory(**_kwargs: Any) -> _FakeFacebookScraper:
        return scraper

    def _upsert_facebook_post(_context: Any, **kwargs: Any) -> dict[str, Any]:
        upsert_calls.append(dict(kwargs))
        return {"id": "post-row-1", "post_id": kwargs["post"].post_id}

    dependencies = FacebookPostsCatalogDependencies(
        scraper_factory=_scraper_factory,
        document_fetcher_factory=None,
        scrape_config_factory=_scrape_config_factory,
        load_cookies=lambda: {},
        upsert_facebook_post=_upsert_facebook_post,
    )

    rows, meta = scrape_shared_facebook_posts(
        run_id=None,
        account_handle="bravotv",
        config={},
        job_id="job-2",
        dependencies=dependencies,
    )

    assert scraper.config.delay_seconds == 1.0
    assert scraper.config.max_pages == 5
    assert scraper.config.max_scroll_iterations == 50
    assert rows == [{"id": "post-row-1", "post_id": "fb-2"}]
    assert upsert_calls == [
        {
            "job_id": "job-2",
            "account": "bravotv",
            "post": scraper.posts[0],
        }
    ]
    assert meta["profile_snapshot"]["username"] == "bravotv"
    assert meta["profile_snapshot"]["profile_url"] == "https://www.facebook.com/bravotv"
    assert "persist_counters" not in meta


def test_facebook_posts_catalog_preserves_injected_profile_snapshot_precedence() -> None:
    scraper = _FakeFacebookScraper(
        posts=[
            SimpleNamespace(
                post_id="fb-3",
                username="Scraped Bravo",
                user_avatar_url="https://images.test/scraped-avatar.jpg",
            )
        ],
        retrieval_meta={},
    )
    persistence = _LocalFakeCatalogPersistence()
    dependencies = FacebookPostsCatalogDependencies(
        scraper_factory=lambda **_kwargs: scraper,
        document_fetcher_factory=None,
        scrape_config_factory=_scrape_config_factory,
        persist_shared_catalog_posts_with_progress=persistence.persist_shared_catalog_posts_with_progress,
        upsert_shared_catalog_post=persistence.upsert_shared_catalog_post,
    )

    _rows, meta = scrape_shared_facebook_posts(
        run_id="run-3",
        account_handle="bravotv",
        config={
            "pipeline_ingest_mode": "shared_account_catalog_backfill",
            "profile_snapshot": {
                "display_name": "Configured Bravo",
                "avatar_url": "https://images.test/configured-avatar.jpg",
            },
        },
        job_id="job-3",
        dependencies=dependencies,
    )

    assert meta["profile_snapshot"]["username"] == "bravotv"
    assert meta["profile_snapshot"]["display_name"] == "Configured Bravo"
    assert meta["profile_snapshot"]["avatar_url"] == "https://images.test/configured-avatar.jpg"


def test_facebook_posts_catalog_marks_soft_block_empty_result_retryable() -> None:
    scraper = _FakeFacebookScraper(
        posts=[],
        retrieval_meta={
            "source": "public_meta_fallback",
            "pages_scanned": 1,
            "posts_checked": 0,
            "matched_posts": 0,
            "stop_reason": "empty_response_soft_block",
        },
    )
    persistence = _LocalFakeCatalogPersistence()
    dependencies = FacebookPostsCatalogDependencies(
        scraper_factory=lambda **_kwargs: scraper,
        document_fetcher_factory=None,
        scrape_config_factory=_scrape_config_factory,
        persist_shared_catalog_posts_with_progress=persistence.persist_shared_catalog_posts_with_progress,
        upsert_shared_catalog_post=persistence.upsert_shared_catalog_post,
    )

    rows, meta = scrape_shared_facebook_posts(
        run_id="run-soft-block",
        account_handle="bravotv",
        config={"pipeline_ingest_mode": "shared_account_catalog_backfill"},
        job_id="job-soft-block",
        dependencies=dependencies,
    )

    assert rows == []
    assert meta["error_code"] == "facebook_empty_soft_block"
    assert meta["error_class"] == "FacebookEmptySoftBlock"
    assert meta["empty_result_reason"] == "empty_response_soft_block"
    assert meta["retryable"] is True
    assert meta["complete"] is False
    assert meta["persist_counters"] == {"posts_upserted": 0, "comments_upserted": 0}
