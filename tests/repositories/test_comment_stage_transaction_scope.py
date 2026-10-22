"""Comment stages must not hold a database transaction open across network fetches.

pg.db_connection sets idle_in_transaction_session_timeout, so a stage-wide transaction
that waits on slow reply/quote/comment fetches is killed by Postgres and rolls back every
earlier anchor. Each anchor or video commits its writes in its own short transaction.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

import pytest

import trr_backend.socials.social_season_analytics_impl as social_repo
from trr_backend.socials.social_season_analytics_impl import SeasonContext


class _ConnectionTracker:
    def __init__(self) -> None:
        self.open_count = 0
        self.opened: list[object] = []

    @contextmanager
    def connect(self, **_kwargs: Any):
        conn = object()
        self.open_count += 1
        self.opened.append(conn)
        try:
            yield conn
        finally:
            self.open_count -= 1

    def assert_no_open_transaction(self, what: str) -> None:
        assert self.open_count == 0, f"{what} ran while a database transaction was open"


def _context() -> SeasonContext:
    return SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )


def _opts(platform: str) -> Any:
    return social_repo.IngestOptions(
        platforms={platform},
        source_scope="bravo",
        sync_strategy="full_refresh",
        max_posts_per_target=0,
        max_comments_per_post=50,
        max_replies_per_post=50,
        fetch_replies=True,
        ingest_mode="comments_only",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )


def _install_common(monkeypatch: pytest.MonkeyPatch, tracker: _ConnectionTracker) -> None:
    monkeypatch.setattr(social_repo.pg, "db_connection", tracker.connect)
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(social_repo, "_load_comment_lifecycle_snapshots", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        social_repo,
        "_decide_comment_refresh",
        lambda **_kwargs: social_repo.CommentRefreshDecision(should_refresh=True, reason="test"),
    )
    monkeypatch.setattr(social_repo, "_is_comment_fetch_complete", lambda **_kwargs: True)


def test_twitter_comments_stage_commits_each_anchor_outside_network_fetches(monkeypatch: pytest.MonkeyPatch) -> None:
    tracker = _ConnectionTracker()
    _install_common(monkeypatch, tracker)
    write_conns: dict[str, list[object]] = {}

    class _FakeTwitterScraper:
        last_reply_fetch_reason = ""
        last_quote_fetch_reason = ""
        comments_auth_failed = False

        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

    def _fetch_replies(**kwargs: Any) -> list[Any]:
        tracker.assert_no_open_transaction("reply fetch")
        return [SimpleNamespace(tweet_id=f"reply-{kwargs['tweet_id']}", reply_to_tweet_id=None, is_reply=True)]

    def _fetch_quotes(**_kwargs: Any) -> tuple[list[Any], None]:
        tracker.assert_no_open_transaction("quote fetch")
        return [], None

    def _upsert_batch(_context: Any, *, tweets: list[Any], conn: Any, **_kwargs: Any) -> dict[str, Any]:
        assert tracker.open_count == 1
        for tweet in tweets:
            write_conns.setdefault(str(tweet.tweet_id), []).append(conn)
        return {}

    def _mark_missing(*, anchor_id: str, conn: Any, **_kwargs: Any) -> int:
        write_conns.setdefault(f"mark-{anchor_id}", []).append(conn)
        return 0

    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: {})
    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda *_args, **_kwargs: [
            {"tweet_id": "t-1", "is_reply": False, "replies_count": 1, "quotes": 0},
            {"tweet_id": "t-2", "is_reply": False, "replies_count": 1, "quotes": 0},
        ],
    )
    monkeypatch.setattr(social_repo, "_fetch_and_apply_twitter_metric_summary", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_fetch_twitter_replies_compat", _fetch_replies)
    monkeypatch.setattr(social_repo, "_fetch_twitter_quotes_compat", _fetch_quotes)
    monkeypatch.setattr(social_repo, "_upsert_tweet_batch", _upsert_batch)
    monkeypatch.setattr(social_repo, "_mark_missing_comments_for_anchor", _mark_missing)
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **_kwargs: None)

    social_repo._ingest_twitter(
        _context(),
        run_id="run-1",
        account="bravotv",
        hashtags=[],
        keywords=[],
        opts=_opts("twitter"),
        job_id="job-1",
        include_reply_records=False,
        hydrate_audience_replies=False,
        stage="comments",
    )

    # Each anchor's writes share one connection, and anchors do not share a connection.
    assert write_conns["reply-t-1"][0] is write_conns["mark-t-1"][0]
    assert write_conns["reply-t-2"][0] is write_conns["mark-t-2"][0]
    assert write_conns["reply-t-1"][0] is not write_conns["reply-t-2"][0]
    assert tracker.open_count == 0


def test_youtube_comments_stage_commits_each_video_outside_network_fetches(monkeypatch: pytest.MonkeyPatch) -> None:
    tracker = _ConnectionTracker()
    _install_common(monkeypatch, tracker)
    comment_conns: dict[str, object] = {}
    sync_state_conns: dict[str, object] = {}

    class _FakeYouTubeScraper:
        last_comment_fetch_reason = ""
        comments_auth_failed = False

        def fetch_comments(self, video_id: str, **_kwargs: Any) -> list[Any]:
            tracker.assert_no_open_transaction("comment fetch")
            return [SimpleNamespace(comment_id=f"c-{video_id}", replies=[])]

    def _upsert_comment_tree(_context: Any, *, video_db_id: str, conn: Any, **_kwargs: Any) -> int:
        assert tracker.open_count == 1
        comment_conns[video_db_id] = conn
        return 1

    def _save_sync_state(_context: Any, *, post_id: str, conn: Any, **_kwargs: Any) -> None:
        assert tracker.open_count == 1
        sync_state_conns[post_id] = conn

    monkeypatch.setattr("trr_backend.socials.youtube.YouTubeScraper", _FakeYouTubeScraper)
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda *_args, **_kwargs: [
            {"id": "db-1", "video_id": "v-1", "comments_count": 1, "published_at": datetime(2025, 1, 2, tzinfo=UTC)},
            {"id": "db-2", "video_id": "v-2", "comments_count": 1, "published_at": datetime(2025, 1, 3, tzinfo=UTC)},
        ],
    )
    monkeypatch.setattr(social_repo, "_upsert_youtube_comment_tree", _upsert_comment_tree)
    monkeypatch.setattr(social_repo, "_save_youtube_video_sync_state", _save_sync_state)
    monkeypatch.setattr(social_repo, "_youtube_comment_checkpoint_from_comments", lambda _comments: (None, None, None))
    monkeypatch.setattr(social_repo, "_mark_missing_comments_for_anchor", lambda **_kwargs: 0)
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_sync_youtube_video_comment_counts", lambda _ids, conn=None: 0)
    monkeypatch.setattr(
        social_repo,
        "_cleanup_mismatched_youtube_rows",
        lambda **_kwargs: {"scanned": 0, "videos_deleted": 0, "comments_deleted": 0},
    )

    social_repo._ingest_youtube(
        _context(),
        run_id="run-1",
        account="bravo",
        hashtags=[],
        keywords=[],
        opts=_opts("youtube"),
        job_id="job-1",
        stage="comments",
    )

    assert set(comment_conns) == {"db-1", "db-2"}
    assert comment_conns["db-1"] is not comment_conns["db-2"]
    assert set(sync_state_conns) == {"db-1", "db-2"}
    assert tracker.open_count == 0
