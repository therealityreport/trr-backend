from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime
from typing import Any

import pytest

import trr_backend.socials.social_season_analytics_impl as social_repo
from trr_backend.socials.social_season_analytics_impl import SeasonContext


def _context() -> SeasonContext:
    return SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )


def _opts() -> Any:
    return social_repo.IngestOptions(
        platforms={"twitter"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=100,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )


def _install_scraper(monkeypatch: pytest.MonkeyPatch, retrieval_meta: dict[str, Any]) -> None:
    class _FakeTwitterScraper:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.last_retrieval_meta: dict[str, Any] = {}

        def scrape(self, config: Any, progress_cb: Any = None) -> list[Any]:
            del config, progress_cb
            self.last_retrieval_meta = dict(retrieval_meta)
            return []

    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: {})
    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *_args, **_kwargs: None)


def _run_posts_stage() -> tuple[int, int, dict[str, Any]]:
    return social_repo._ingest_twitter(
        _context(),
        run_id="run-1",
        account="bravotv",
        hashtags=["rhoslc"],
        keywords=["rhoslc"],
        opts=_opts(),
        job_id="job-1",
        include_reply_records=False,
        hydrate_audience_replies=False,
        stage="posts",
    )


def test_ingest_twitter_posts_stage_raises_retryable_error_when_search_transports_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_scraper(
        monkeypatch,
        {
            "error_code": "twitter_search_fallback_exhausted",
            "retryable": True,
            "complete": False,
            "stop_reason": "graphql_failed",
        },
    )

    with pytest.raises(social_repo.SharedStageRuntimeError) as exc_info:
        _run_posts_stage()

    assert exc_info.value.error_code == "twitter_search_fallback_exhausted"
    assert exc_info.value.retryable is True
    assert exc_info.value.runtime_metadata["retrieval_meta"]["stop_reason"] == "graphql_failed"


def test_ingest_twitter_posts_stage_keeps_clean_empty_search_as_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_scraper(monkeypatch, {"error_code": None, "retryable": False, "complete": True})

    posts, comments, meta = _run_posts_stage()

    assert (posts, comments) == (0, 0)
    assert meta.get("error_code") is None
