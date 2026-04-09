#!/usr/bin/env python3
"""Controlled social sync benchmark harness (synthetic, deterministic).

Runs three scenarios under two profiles:
- baseline: smaller batch sizing
- optimized: default tuned batch sizing

Scenarios:
1) Single-platform comments-heavy
2) Sync-all (posts+comments across Twitter/TikTok/YouTube)
3) Concurrent multi-run backlog
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import trr_backend.repositories.social_season_analytics as social_repo
from trr_backend.socials.control_plane import IngestOptions, SeasonContext


@dataclass
class ScenarioResult:
    name: str
    duration_sec: float
    counters: dict[str, int]
    details: dict[str, Any]


@dataclass
class ProfileRun:
    profile: str
    scenarios: list[ScenarioResult]
    db_metrics: dict[str, int]


class _PatchScope:
    def __init__(self) -> None:
        self._attrs: list[tuple[Any, str, Any]] = []
        self._env: list[tuple[str, str | None]] = []

    def set_attr(self, obj: Any, name: str, value: Any) -> None:
        self._attrs.append((obj, name, getattr(obj, name)))
        setattr(obj, name, value)

    def set_env(self, name: str, value: str) -> None:
        self._env.append((name, os.environ.get(name)))
        os.environ[name] = value

    def restore(self) -> None:
        for name, prior in reversed(self._env):
            if prior is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = prior
        for obj, name, prior in reversed(self._attrs):
            setattr(obj, name, prior)


class _FakeTikTokPost(SimpleNamespace):
    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class _FakeTikTokComment(SimpleNamespace):
    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class _FakeYouTubeVideo(SimpleNamespace):
    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class _FakeYouTubeComment(SimpleNamespace):
    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _twitter_tweet(tweet_id: str, text: str, created_ts: int) -> SimpleNamespace:
    return SimpleNamespace(
        tweet_id=tweet_id,
        username="bravotv",
        display_name="BravoTV",
        user_verified=False,
        text=text,
        hashtags=["RHOSLC"],
        mentions=[],
        media_urls=["https://images.test/media.jpg"],
        likes=5,
        retweets=2,
        replies=20,
        quotes=8,
        views=100,
        is_reply=False,
        is_retweet=False,
        is_quote=False,
        reply_to_tweet_id=None,
        quoted_tweet_id=None,
        created_at=created_ts,
        user_id="user-1",
        user_profile_url="https://x.com/bravotv",
        user_avatar_url="https://images.test/avatar.jpg",
        link_preview_media_count=0,
        to_dict=lambda: {"tweet_id": tweet_id, "text": text},
    )


def _fake_tiktok_posts(count: int) -> list[_FakeTikTokPost]:
    rows: list[_FakeTikTokPost] = []
    base = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp())
    for idx in range(count):
        rows.append(
            _FakeTikTokPost(
                video_id=f"tt-{idx}",
                create_time=base + idx * 3600,
                username="bravotv",
                author_nickname="BravoTV",
                description="RHOSLC benchmark post",
                hashtags=["RHOSLC"],
                mentions=["@bravotv"],
                likes=100 + idx,
                comments=18,
                shares=10,
                views=2000 + idx,
                duration=30,
                thumbnail_url=f"https://images.test/tt-{idx}.jpg",
                media_urls=[f"https://video.test/tt-{idx}.mp4"],
                saves=3,
                to_dict=lambda idx=idx: {"video_id": f"tt-{idx}"},
            )
        )
    return rows


def _fake_tiktok_comments(video_id: str, count: int) -> list[_FakeTikTokComment]:
    return [
        _FakeTikTokComment(
            comment_id=f"{video_id}-c-{idx}",
            created_at=int(datetime(2025, 1, 2, tzinfo=UTC).timestamp()),
            username=f"user_{idx}",
            user_id=f"user-id-{idx}",
            nickname=f"User {idx}",
            text=f"comment {idx} RHOSLC",
            likes=idx,
            is_reply=False,
            reply_count=0,
            replies=[],
            media_urls=[],
            to_dict=lambda video_id=video_id, idx=idx: {"comment_id": f"{video_id}-c-{idx}"},
        )
        for idx in range(count)
    ]


def _fake_youtube_videos(count: int) -> list[_FakeYouTubeVideo]:
    rows: list[_FakeYouTubeVideo] = []
    for idx in range(count):
        rows.append(
            _FakeYouTubeVideo(
                video_id=f"yt-{idx}",
                channel_id="chan-1",
                channel_title="Bravo",
                title="RHOSLC benchmark clip",
                description="Salt Lake City benchmark",
                duration="PT1M",
                duration_seconds=60,
                views=1000 + idx,
                likes=30 + idx,
                comments=14,
                thumbnail_url=f"https://images.test/yt-{idx}.jpg",
                published_at=datetime(2025, 1, 2, tzinfo=UTC),
                is_short=(idx % 2 == 0),
                source_surface="videos",
                tags=["RHOSLC"],
                to_dict=lambda idx=idx: {"video_id": f"yt-{idx}"},
            )
        )
    return rows


def _fake_youtube_comments(video_id: str, count: int) -> list[_FakeYouTubeComment]:
    return [
        _FakeYouTubeComment(
            comment_id=f"{video_id}-yc-{idx}",
            created_at=datetime(2025, 1, 3, tzinfo=UTC),
            author=f"author_{idx}",
            author_channel_id=f"chan_{idx}",
            text=f"yt comment {idx}",
            likes=idx,
            is_reply=False,
            reply_count=0,
            replies=[],
            to_dict=lambda video_id=video_id, idx=idx: {"comment_id": f"{video_id}-yc-{idx}"},
        )
        for idx in range(count)
    ]


def _build_context() -> SeasonContext:
    return SeasonContext(
        season_id="benchmark-season-1",
        show_id="benchmark-show-1",
        show_name="Benchmark Housewives",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )


def _result_signature(result: ScenarioResult) -> dict[str, int]:
    return dict(result.counters)


def _build_harness(profile: str) -> tuple[_PatchScope, dict[str, int], dict[str, Any]]:
    scope = _PatchScope()
    metrics = {"db_connections": 0, "upsert_rows": 0, "upsert_batches": 0}
    state: dict[str, Any] = {"twitter_replies": {}, "twitter_quotes": {}, "twitter_posts": []}

    def no_op(*args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs

    if profile == "baseline":
        scope.set_env("SOCIAL_DB_UPSERT_BATCH_SIZE_COMMENTS", "1")
        scope.set_env("SOCIAL_DB_UPSERT_BATCH_SIZE_POSTS", "1")
    else:
        scope.set_env("SOCIAL_DB_UPSERT_BATCH_SIZE_COMMENTS", "200")
        scope.set_env("SOCIAL_DB_UPSERT_BATCH_SIZE_POSTS", "50")

    @contextmanager
    def _fake_db_connection():
        metrics["db_connections"] += 1
        time.sleep(0.0008)
        yield object()

    class _Cursor:
        def execute(self, *_args, **_kwargs) -> None:  # noqa: ANN002
            return None

        def fetchall(self) -> list[dict[str, Any]]:
            return []

        def fetchone(self) -> dict[str, Any] | None:
            return None

    @contextmanager
    def _fake_db_cursor(*, conn: Any | None = None):  # noqa: ANN401
        del conn
        yield _Cursor()

    def _fake_pg_upsert(table: str, payload: dict[str, Any], *, conflict_col: str, conn: Any | None = None):
        del conn
        metrics["upsert_rows"] += 1
        time.sleep(0.00005)
        source_id = str(payload.get(conflict_col) or payload.get("id") or metrics["upsert_rows"])
        return {"id": f"{table}-{source_id}", **payload}

    def _fake_pg_upsert_many(
        table: str,
        payloads: list[dict[str, Any]],
        *,
        conflict_col: str,
        conn: Any | None = None,
    ) -> list[dict[str, Any]]:
        del conn
        metrics["upsert_batches"] += 1
        metrics["upsert_rows"] += len(payloads)
        time.sleep(0.00005 * len(payloads) + 0.0008)
        rows: list[dict[str, Any]] = []
        for payload in payloads:
            source_id = str(payload.get(conflict_col) or payload.get("id") or len(rows))
            rows.append({"id": f"{table}-{source_id}", **payload})
        return rows

    scope.set_attr(social_repo.pg, "db_connection", _fake_db_connection)
    scope.set_attr(social_repo.pg, "db_cursor", _fake_db_cursor)
    scope.set_attr(social_repo, "_pg_upsert", _fake_pg_upsert)
    scope.set_attr(social_repo, "_pg_upsert_many", _fake_pg_upsert_many)
    scope.set_attr(social_repo, "_update_job_progress", no_op)
    scope.set_attr(social_repo, "_touch_job_heartbeat", no_op)
    scope.set_attr(social_repo, "_mark_missing_comments_for_anchor", lambda *args, **kwargs: 0)
    scope.set_attr(social_repo, "_reconcile_post_comment_count", no_op)
    scope.set_attr(social_repo, "_enqueue_platform_media_mirror_job", no_op)
    scope.set_attr(social_repo, "_enqueue_twitter_comment_media_mirror_job", no_op)
    scope.set_attr(social_repo, "_enqueue_tiktok_comment_media_mirror_job", no_op)
    scope.set_attr(
        social_repo,
        "_cleanup_mismatched_youtube_rows",
        lambda *args, **kwargs: {"videos_deleted": 0, "comments_deleted": 0},
    )
    scope.set_attr(social_repo, "_persist_tiktok_post_enrichment", no_op)
    scope.set_attr(social_repo, "_persist_tiktok_sound_snapshot", no_op)
    scope.set_attr(social_repo, "_persist_tiktok_comment_enrichment", no_op)
    scope.set_attr(social_repo, "_sync_youtube_video_comment_counts", lambda ids, conn=None: len(ids))
    scope.set_attr(social_repo, "_load_twitter_auth", lambda: ({}, None))
    scope.set_attr(social_repo, "_load_twikit_credentials", lambda: {})
    scope.set_attr(social_repo, "_load_tiktok_cookies", lambda: {"sessionid": "benchmark"})
    scope.set_attr(social_repo, "_text_contains_any_term", lambda **kwargs: True)
    scope.set_attr(social_repo, "_twitter_post_matches_show_terms", lambda **kwargs: True)
    scope.set_attr(social_repo, "_youtube_video_matches_show_terms", lambda **kwargs: True)
    scope.set_attr(social_repo, "_youtube_video_matches_owner_identity", lambda *args, **kwargs: True)
    scope.set_attr(social_repo, "_youtube_transcript_ingest_enabled", lambda: False)
    scope.set_attr(
        social_repo,
        "_decide_comment_refresh",
        lambda **kwargs: social_repo.CommentRefreshDecision(should_refresh=True, reason="benchmark_refresh"),
    )

    def _fake_replies(*, scraper: Any, tweet_id: str, delay: float, search_max_pages: int, twikit_max_pages: int):
        del scraper, delay, search_max_pages, twikit_max_pages
        total = int(state["twitter_replies"].get(tweet_id, 0))
        return [
            SimpleNamespace(
                tweet_id=f"{tweet_id}-r-{idx}",
                reply_to_tweet_id=tweet_id,
                is_reply=True,
                media_urls=["https://images.test/reply.jpg"],
                to_dict=lambda tweet_id=tweet_id, idx=idx: {"tweet_id": f"{tweet_id}-r-{idx}"},
            )
            for idx in range(total)
        ]

    def _fake_quotes(*, scraper: Any, tweet_id: str, delay: float, max_pages: int):
        del scraper, delay, max_pages
        total = int(state["twitter_quotes"].get(tweet_id, 0))
        return (
            [
                SimpleNamespace(
                    tweet_id=f"{tweet_id}-q-{idx}",
                    quoted_tweet_id=tweet_id,
                    is_quote=True,
                    media_urls=["https://images.test/quote.jpg"],
                    to_dict=lambda tweet_id=tweet_id, idx=idx: {"tweet_id": f"{tweet_id}-q-{idx}"},
                )
                for idx in range(total)
            ],
            None,
        )

    scope.set_attr(social_repo, "_fetch_twitter_replies_compat", _fake_replies)
    scope.set_attr(social_repo, "_fetch_twitter_quotes_compat", _fake_quotes)
    scope.set_attr(social_repo, "_fetch_and_apply_twitter_metric_summary", lambda **kwargs: None)

    class _TwitterScraper:
        comments_auth_failed = False
        last_reply_fetch_reason = ""
        last_quote_fetch_reason = ""
        last_retrieval_meta = {"retrieval_mode": "benchmark"}

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def scrape(self, _config, progress_cb=None):
            if progress_cb:
                progress_cb({"phase": "scrape", "pages_scanned": 2, "posts_checked": len(state["twitter_posts"])})
            return list(state["twitter_posts"])

        def fetch_tweet_detail_summary(self, _tweet_id: str, delay: float = 0.0):
            del delay
            return None

        def fetch_public_tweet_summary(self, _tweet_id: str, delay: float = 0.0):
            del delay
            return None

    class _TikTokScraper:
        comments_auth_failed = False
        last_comment_fetch_reason = ""
        _posts: list[_FakeTikTokPost] = []
        _comments_per_post: int = 0
        last_retrieval_meta = {"retrieval_mode": "benchmark"}

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def scrape(self, _config, progress_cb=None):
            if progress_cb:
                progress_cb({"phase": "scrape", "pages_scanned": 1, "posts_checked": len(self._posts)})
            return list(self._posts)

        def fetch_comments(self, video_id: str, **kwargs):
            del kwargs
            return _fake_tiktok_comments(video_id, self._comments_per_post)

    class _YouTubeScraper:
        comments_auth_failed = False
        last_comment_fetch_reason = ""
        _videos: list[_FakeYouTubeVideo] = []
        _comments_per_video: int = 0
        last_retrieval_meta = {"retrieval_mode": "benchmark"}

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def resolve_channel_identity(self, account: str, delay: float = 0.25):
            del delay
            return {"canonical_handle": account, "channel_id": "chan-1"}

        def scrape(self, _config, progress_cb=None):
            if progress_cb:
                progress_cb({"phase": "scrape", "pages_scanned": 1, "posts_checked": len(self._videos)})
            return list(self._videos)

        def fetch_comments(self, video_id: str, **kwargs):
            del kwargs
            return _fake_youtube_comments(video_id, self._comments_per_video)

    import trr_backend.socials.tiktok as tiktok_mod
    import trr_backend.socials.twitter as twitter_mod
    import trr_backend.socials.youtube as youtube_mod

    scope.set_attr(twitter_mod, "TwitterScraper", _TwitterScraper)
    scope.set_attr(tiktok_mod, "TikTokScraper", _TikTokScraper)
    scope.set_attr(youtube_mod, "YouTubeScraper", _YouTubeScraper)

    state["twitter_scraper_cls"] = _TwitterScraper
    state["tiktok_scraper_cls"] = _TikTokScraper
    state["youtube_scraper_cls"] = _YouTubeScraper
    return scope, metrics, state


def _run_twitter_comments_heavy(
    context: SeasonContext,
    state: dict[str, Any],
    *,
    anchors: int,
    replies: int,
    quotes: int,
) -> ScenarioResult:
    account = "bench_comments"
    rows = []
    state["twitter_replies"] = {}
    state["twitter_quotes"] = {}
    for idx in range(anchors):
        tweet_id = f"{account}-anchor-{idx}"
        rows.append(
            {
                "tweet_id": tweet_id,
                "is_reply": False,
                "replies_count": replies,
                "quotes": quotes,
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            }
        )
        state["twitter_replies"][tweet_id] = replies
        state["twitter_quotes"][tweet_id] = quotes

    social_repo._load_existing_posts = lambda *args, **kwargs: rows  # type: ignore[method-assign]
    social_repo._load_comment_lifecycle_snapshots = lambda *args, **kwargs: {}  # type: ignore[method-assign]
    opts = IngestOptions(
        platforms={"twitter"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=0,
        max_comments_per_post=500,
        max_replies_per_post=500,
        fetch_replies=True,
        ingest_mode="comments_only",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )
    started = time.perf_counter()
    posts, comments, meta = social_repo._ingest_twitter(
        context,
        run_id="bench-run-comments",
        account=account,
        hashtags=["RHOSLC"],
        keywords=["RHOSLC"],
        opts=opts,
        job_id="bench-job-comments",
        stage="comments",
    )
    duration = time.perf_counter() - started
    counters = {
        "posts": int(posts),
        "comments": int(comments),
        "comments_upserted": int((meta.get("comment_stats") or {}).get("comments_upserted") or 0),
        "quotes_upserted": int((meta.get("quote_stats") or {}).get("quotes_upserted") or 0),
    }
    return ScenarioResult(
        "single_platform_comments_heavy",
        duration,
        counters,
        {"account": account, "anchors": anchors},
    )


def _run_sync_all(context: SeasonContext, state: dict[str, Any]) -> ScenarioResult:
    state["twitter_posts"] = [
        _twitter_tweet(
            f"sync-anchor-{idx}",
            "RHOSLC benchmark sync all",
            int(datetime(2025, 1, 5, tzinfo=UTC).timestamp()),
        )
        for idx in range(24)
    ]
    state["twitter_replies"] = {f"sync-anchor-{idx}": 18 for idx in range(24)}
    state["twitter_quotes"] = {f"sync-anchor-{idx}": 8 for idx in range(24)}
    state["tiktok_scraper_cls"]._posts = _fake_tiktok_posts(18)
    state["tiktok_scraper_cls"]._comments_per_post = 14
    state["youtube_scraper_cls"]._videos = _fake_youtube_videos(14)
    state["youtube_scraper_cls"]._comments_per_video = 12
    social_repo._load_existing_posts = lambda *args, **kwargs: []  # type: ignore[method-assign]
    social_repo._load_comment_lifecycle_snapshots = lambda *args, **kwargs: {}  # type: ignore[method-assign]

    started = time.perf_counter()
    t_opts = IngestOptions(
        platforms={"twitter"},
        source_scope="bravo",
        sync_strategy="full_refresh",
        max_posts_per_target=0,
        max_comments_per_post=400,
        max_replies_per_post=400,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )
    tw_posts, tw_comments, tw_meta = social_repo._ingest_twitter(
        context,
        run_id="bench-run-syncall",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC"],
        opts=t_opts,
        job_id="bench-job-syncall-twitter",
        include_reply_records=False,
        hydrate_audience_replies=True,
        stage="posts",
    )

    tk_opts = IngestOptions(
        platforms={"tiktok"},
        source_scope="bravo",
        sync_strategy="full_refresh",
        max_posts_per_target=0,
        max_comments_per_post=300,
        max_replies_per_post=100,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )
    tt_posts, tt_comments, tt_meta = social_repo._ingest_tiktok(
        context,
        run_id="bench-run-syncall",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC"],
        opts=tk_opts,
        job_id="bench-job-syncall-tiktok",
        stage="posts",
    )

    yt_opts = IngestOptions(
        platforms={"youtube"},
        source_scope="bravo",
        sync_strategy="full_refresh",
        max_posts_per_target=0,
        max_comments_per_post=250,
        max_replies_per_post=100,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )
    yt_posts, yt_comments, yt_meta = social_repo._ingest_youtube(
        context,
        run_id="bench-run-syncall",
        account="bravo",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC"],
        opts=yt_opts,
        job_id="bench-job-syncall-youtube",
        stage="posts",
    )
    duration = time.perf_counter() - started

    counters = {
        "posts": int(tw_posts + tt_posts + yt_posts),
        "comments": int(tw_comments + tt_comments + yt_comments),
        "comments_upserted": int((tw_meta.get("comment_stats") or {}).get("comments_upserted") or 0)
        + int((tt_meta.get("comment_stats") or {}).get("comments_upserted") or 0)
        + int((yt_meta.get("comment_stats") or {}).get("comments_upserted") or 0),
        "quotes_upserted": int((tw_meta.get("quote_stats") or {}).get("quotes_upserted") or 0),
    }
    return ScenarioResult(
        "sync_all",
        duration,
        counters,
        {"platforms": ["twitter", "tiktok", "youtube"]},
    )


def _run_concurrent_backlog(context: SeasonContext, state: dict[str, Any]) -> ScenarioResult:
    per_run: dict[str, dict[str, Any]] = {}
    rows_by_account: dict[str, list[dict[str, Any]]] = {}
    for run_index in range(3):
        account = f"bench-concurrent-{run_index}"
        rows: list[dict[str, Any]] = []
        for idx in range(75):
            tweet_id = f"{account}-anchor-{idx}"
            rows.append(
                {
                    "tweet_id": tweet_id,
                    "is_reply": False,
                    "replies_count": 16,
                    "quotes": 7,
                    "created_at": datetime(2025, 1, 1, tzinfo=UTC),
                }
            )
            state["twitter_replies"][tweet_id] = 16
            state["twitter_quotes"][tweet_id] = 7
        rows_by_account[account] = rows

    social_repo._load_existing_posts = lambda _platform, _context, account, *args, **kwargs: rows_by_account.get(
        str(account), []
    )  # type: ignore[method-assign]
    social_repo._load_comment_lifecycle_snapshots = lambda *args, **kwargs: {}  # type: ignore[method-assign]
    opts = IngestOptions(
        platforms={"twitter"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=0,
        max_comments_per_post=400,
        max_replies_per_post=400,
        fetch_replies=True,
        ingest_mode="comments_only",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )

    def _task(account: str) -> tuple[int, int]:
        started = time.perf_counter()
        posts, comments, _meta = social_repo._ingest_twitter(
            context,
            run_id=f"bench-run-concurrent-{account}",
            account=account,
            hashtags=["RHOSLC"],
            keywords=["RHOSLC"],
            opts=opts,
            job_id=f"bench-job-concurrent-{account}",
            stage="comments",
        )
        per_run[account] = {"duration_sec": time.perf_counter() - started, "posts": posts, "comments": comments}
        return int(posts), int(comments)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        totals = list(executor.map(_task, sorted(rows_by_account)))
    duration = time.perf_counter() - started
    counters = {
        "posts": sum(item[0] for item in totals),
        "comments": sum(item[1] for item in totals),
        "comments_upserted": sum(item[1] for item in totals),
        "quotes_upserted": 0,
    }
    return ScenarioResult("concurrent_backlog", duration, counters, {"runs": per_run})


def _run_profile(profile: str) -> ProfileRun:
    scope, metrics, state = _build_harness(profile)
    try:
        context = _build_context()
        scenarios = [
            _run_twitter_comments_heavy(context, state, anchors=120, replies=22, quotes=10),
            _run_sync_all(context, state),
            _run_concurrent_backlog(context, state),
        ]
        return ProfileRun(profile=profile, scenarios=scenarios, db_metrics=metrics)
    finally:
        scope.restore()


def _compare_profiles(baseline: ProfileRun, optimized: ProfileRun) -> list[dict[str, Any]]:
    paired: list[dict[str, Any]] = []
    by_name_opt = {item.name: item for item in optimized.scenarios}
    for base_item in baseline.scenarios:
        opt_item = by_name_opt.get(base_item.name)
        if opt_item is None:
            raise RuntimeError(f"Missing optimized scenario result for {base_item.name}")
        if _result_signature(base_item) != _result_signature(opt_item):
            raise RuntimeError(f"Count parity mismatch in scenario {base_item.name}")
        base_duration = max(1e-9, base_item.duration_sec)
        improvement_pct = ((base_duration - opt_item.duration_sec) / base_duration) * 100.0
        paired.append(
            {
                "scenario": base_item.name,
                "baseline_sec": round(base_item.duration_sec, 6),
                "optimized_sec": round(opt_item.duration_sec, 6),
                "improvement_pct": round(improvement_pct, 2),
                "counters": base_item.counters,
            }
        )
    return paired


def main() -> int:
    parser = argparse.ArgumentParser(description="Run controlled social sync benchmark scenarios.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path. Defaults to docs/ai/benchmarks/social_sync_benchmark_<timestamp>.json",
    )
    args = parser.parse_args()

    baseline = _run_profile("baseline")
    optimized = _run_profile("optimized")
    comparison = _compare_profiles(baseline, optimized)

    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = args.output or Path("docs/ai/benchmarks") / f"social_sync_benchmark_{timestamp}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "profiles": {
            "baseline": {
                "db_metrics": baseline.db_metrics,
                "scenarios": [
                    {
                        "name": item.name,
                        "duration_sec": item.duration_sec,
                        "counters": item.counters,
                        "details": item.details,
                    }
                    for item in baseline.scenarios
                ],
            },
            "optimized": {
                "db_metrics": optimized.db_metrics,
                "scenarios": [
                    {
                        "name": item.name,
                        "duration_sec": item.duration_sec,
                        "counters": item.counters,
                        "details": item.details,
                    }
                    for item in optimized.scenarios
                ],
            },
        },
        "comparison": comparison,
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Social sync benchmark complete")
    for row in comparison:
        print(
            f"- {row['scenario']}: baseline={row['baseline_sec']:.4f}s "
            f"optimized={row['optimized_sec']:.4f}s improvement={row['improvement_pct']:.2f}%"
        )
    print(f"Output: {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
