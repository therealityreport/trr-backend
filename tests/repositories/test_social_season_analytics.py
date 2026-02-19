"""Unit tests for season social analytics helpers."""

import inspect
from datetime import UTC, date, datetime, timedelta

import trr_backend.repositories.social_season_analytics as social_repo
from trr_backend.repositories.social_season_analytics import (
    SeasonContext,
    SentimentAnalyzerContext,
    WeekWindow,
    _build_drivers,
    _default_targets,
    _load_instagram_cookies,
    _resolve_depth_defaults,
    _rows_for_platform,
    _rule_based_sentiment_for_text,
    _text_contains_any_term,
    _text_is_trailer_marker,
    _video_matches_season,
    _week_detail_instagram,
    _week_detail_tiktok,
    _youtube_title_is_cross_show_excluded,
    _youtube_video_matches_show_terms,
    get_analytics,
    get_post_comments,
    sentiment_for_text,
)


def test_sentiment_for_text_deterministic() -> None:
    assert sentiment_for_text("I love this amazing episode") == ("positive", 2)
    assert sentiment_for_text("This was boring and awful") == ("negative", -2)
    assert sentiment_for_text("Just a comment without sentiment words") == ("neutral", 0)
    assert sentiment_for_text("") == ("neutral", 0)


def test_sentiment_for_text_handles_negation_and_contrast() -> None:
    assert sentiment_for_text("not good") == ("negative", -1)
    assert sentiment_for_text("not bad") == ("positive", 1)
    label, score = sentiment_for_text("bad but loved the ending")
    assert label == "positive"
    assert score > 0


def test_rule_sentiment_treats_name_only_comment_as_neutral() -> None:
    context = SentimentAnalyzerContext(
        cast_terms={"mary", "lisa", "angie"},
        cast_phrases={"mary cosby", "lisa barlow", "angie katsanevas"},
        episode_terms=set(),
        episode_summary="",
    )
    result = _rule_based_sentiment_for_text("Mary Lisa Angie", analyzer_context=context)
    assert result.label == "neutral"
    assert result.score == 0


def test_text_is_trailer_marker_detects_first_look_and_trailer() -> None:
    assert _text_is_trailer_marker("Your First Look at RHOSLC Season 6")
    assert _text_is_trailer_marker("Official Trailer: RHOSLC")
    assert not _text_is_trailer_marker("Watch this week's full episode")


def test_video_matches_season_from_numeric_or_text() -> None:
    assert _video_matches_season({"season_number": 6, "title": "anything"}, 6)
    assert _video_matches_season({"season_number": "6", "title": "anything"}, 6)
    assert _video_matches_season({"season_number": None, "title": "First Look at Season 6"}, 6)
    assert _video_matches_season({"season_number": None, "title": "RHOSLC S6 Reunion"}, 6)
    assert not _video_matches_season({"season_number": None, "title": "Season 5 trailer"}, 6)


def test_default_targets_include_rhoslc_aliases() -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="The Real Housewives of Salt Lake City",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )

    defaults = _default_targets(context)
    assert defaults
    hashtag_hit = False
    for target in defaults:
        hashtags = [str(item).lower().lstrip("#") for item in target.get("hashtags", [])]
        keywords = [str(item).lower() for item in target.get("keywords", [])]
        if "rhoslc" in hashtags:
            hashtag_hit = True
        assert "rhoslc" in keywords
        assert "salt lake city" in keywords
    assert hashtag_hit


def test_text_contains_any_term_accepts_phrase_or_hashtag_or_token() -> None:
    hashtags = ["RHOSLC"]
    keywords = ["Salt Lake City", "RHOSLC"]

    assert _text_contains_any_term(
        text="Tonight on #RHOSLC at 8/7c",
        hashtags=hashtags,
        keywords=keywords,
    )
    assert _text_contains_any_term(
        text="The women return to Salt Lake City this week",
        hashtags=hashtags,
        keywords=keywords,
    )
    assert _text_contains_any_term(
        text="RHOSLC reunion sneak peek",
        hashtags=hashtags,
        keywords=keywords,
    )
    assert not _text_contains_any_term(
        text="Top Chef finale recap",
        hashtags=hashtags,
        keywords=keywords,
    )


def test_youtube_video_matches_show_terms_strict_title_or_hashtag_description() -> None:
    hashtags = ["RHOSLC"]
    keywords = ["Salt Lake City", "RHOSLC", "season 6"]

    assert _youtube_video_matches_show_terms(
        title="RHOSLC Season 6 First Look",
        description="Watch now",
        hashtags=hashtags,
        keywords=keywords,
    )
    assert _youtube_video_matches_show_terms(
        title="Bravo sneak peek",
        description="Premieres soon #RHOSLC",
        hashtags=hashtags,
        keywords=keywords,
    )
    assert not _youtube_video_matches_show_terms(
        title="Bravo sneak peek",
        description="Cast includes Angie Katsanevas (RHOSLC)",
        hashtags=hashtags,
        keywords=keywords,
    )


def test_youtube_video_matches_show_terms_excludes_wife_swap_housewives_edition() -> None:
    assert _youtube_title_is_cross_show_excluded(
        "SNEAK PEEK: Wife Swap: The Real Housewives Edition"
    )
    assert not _youtube_video_matches_show_terms(
        title="SNEAK PEEK: Wife Swap: The Real Housewives Edition",
        description="Cast includes #RHOSLC",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC", "Salt Lake City"],
    )


def test_load_instagram_cookies_prefers_env_json(monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COOKIES_JSON",
        '{"sessionid":"abc","csrftoken":"def","_comment":"ignore-me"}',
    )
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COOKIES_FILE", raising=False)
    monkeypatch.delenv("INSTAGRAM_COOKIES_FILE", raising=False)
    cookies = _load_instagram_cookies()
    assert cookies["sessionid"] == "abc"
    assert cookies["csrftoken"] == "def"
    assert "_comment" not in cookies


def test_load_instagram_cookies_uses_file_when_env_json_invalid(monkeypatch, tmp_path) -> None:
    cookie_file = tmp_path / "ig-cookies.json"
    cookie_file.write_text('{"sessionid":"file-session","csrftoken":"file-csrf","_meta":"ignore"}')
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_JSON", "{invalid-json")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_FILE", str(cookie_file))
    cookies = _load_instagram_cookies()
    assert cookies["sessionid"] == "file-session"
    assert cookies["csrftoken"] == "file-csrf"
    assert "_meta" not in cookies


def test_resolve_depth_defaults_enforces_minimums() -> None:
    posts, comments, replies, fetch_replies = _resolve_depth_defaults(
        max_posts_per_target=500,
        max_comments_per_post=50,
        max_replies_per_post=10,
        fetch_replies=False,
    )
    assert posts == 500
    assert comments == 200  # enforces minimum of 200
    assert replies == 100  # enforces minimum of 100
    assert fetch_replies is False

    posts2, comments2, replies2, fetch_replies2 = _resolve_depth_defaults(
        max_posts_per_target=10000,
        max_comments_per_post=300,
        max_replies_per_post=200,
        fetch_replies=True,
    )
    assert posts2 == 10000
    assert comments2 == 300  # already above minimum
    assert replies2 == 200  # already above minimum
    assert fetch_replies2 is True


def test_rows_for_platform_twitter_bravo_uses_parent_post_scoping(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    start_dt = datetime(2025, 1, 1, tzinfo=UTC)
    end_dt = datetime(2025, 1, 31, tzinfo=UTC)
    rows = _rows_for_platform(
        "season-1",
        platform="twitter",
        start_dt=start_dt,
        end_dt=end_dt,
        source_scope="bravo",
        target_accounts_by_platform={"twitter": {"bravotv", "bravowwhl"}},
    )

    assert rows == []
    sql = str(captured["sql"]).lower()
    params = captured["params"]

    assert "with recursive in_scope_posts as" in sql
    assert "legacy_thread_replies" in sql
    assert "child.reply_to_tweet_id = parent.tweet_id" in sql
    assert "and t.is_reply = false" in sql
    assert "and t.is_reply = true" in sql
    assert "lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)" in sql
    assert "lower(coalesce(nullif(t.username, ''), t.source_account, '')) = any(%s)" in sql
    assert "lower(coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '')) = any(%s)" in sql
    assert "t.tweet_id in (select tweet_id from legacy_thread_replies)" in sql
    assert "and t.created_at >= %s" in sql
    assert "and t.created_at <= %s" in sql

    expected_accounts = ["bravotv", "bravowwhl"]
    assert params == [
        "season-1",
        start_dt,
        end_dt,
        expected_accounts,
        "season-1",
        "season-1",
        "season-1",
        start_dt,
        end_dt,
        expected_accounts,
        "season-1",
        start_dt,
        end_dt,
        expected_accounts,
    ]


def test_rows_for_platform_twitter_non_bravo_keeps_open_comment_scope(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    start_dt = datetime(2025, 2, 1, tzinfo=UTC)
    end_dt = datetime(2025, 2, 28, tzinfo=UTC)
    rows = _rows_for_platform(
        "season-2",
        platform="twitter",
        start_dt=start_dt,
        end_dt=end_dt,
        source_scope="community",
        target_accounts_by_platform={},
    )

    assert rows == []
    sql = str(captured["sql"]).lower()
    params = captured["params"]

    assert "with posts as (" in sql
    assert "legacy_thread_replies" not in sql
    assert "in_scope_posts" not in sql
    assert "('bravotv', 'bravo')" not in sql
    assert "and t.is_reply = false" in sql
    assert "and t.is_reply = true" in sql
    assert "and t.created_at >= %s" in sql
    assert "and t.created_at <= %s" in sql

    assert params == ["season-2", start_dt, end_dt, "season-2", start_dt, end_dt]


def test_build_drivers_excludes_cast_names_and_handles() -> None:
    rows = [
        {
            "kind": "comment",
            "text": "Mary was funny and iconic tonight",
            "sentiment": "positive",
            "author": "bravofan",
        },
        {
            "kind": "comment",
            "text": "@bravofan Lisa was funny but not boring",
            "sentiment": "positive",
            "author": "bravofan",
        },
        {
            "kind": "comment",
            "text": "Angie was awful and boring",
            "sentiment": "negative",
            "author": "viewer_2",
        },
        {
            "kind": "comment",
            "text": "Bronwyn is boring lately",
            "sentiment": "negative",
            "author": "viewer_3",
        },
    ]
    context = SentimentAnalyzerContext(
        cast_terms={"mary", "lisa", "angie", "bronwyn"},
        cast_phrases={"mary cosby", "lisa barlow", "angie katsanevas", "bronwyn newport"},
        episode_terms=set(),
        episode_summary="",
    )

    drivers = _build_drivers(rows, analyzer_context=context)
    positive_terms = {item["term"] for item in drivers["positive"]}
    negative_terms = {item["term"] for item in drivers["negative"]}

    assert "mary" not in positive_terms
    assert "lisa" not in positive_terms
    assert "angie" not in negative_terms
    assert "bronwyn" not in negative_terms
    assert "bravofan" not in positive_terms
    assert "funny" in positive_terms
    assert "boring" in negative_terms


def test_get_analytics_includes_weekly_platform_engagement_and_has_data(monkeypatch) -> None:
    season_id = "season-analytics-1"
    show_id = "show-analytics-1"
    context = SeasonContext(
        season_id=season_id,
        show_id=show_id,
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )
    week_zero_start = datetime(2025, 9, 9, tzinfo=UTC)
    week_windows = [
        WeekWindow(week_index=0, start_local=week_zero_start, end_local=week_zero_start + timedelta(days=7)),
        WeekWindow(
            week_index=1,
            start_local=week_zero_start + timedelta(days=7),
            end_local=week_zero_start + timedelta(days=14),
        ),
        WeekWindow(
            week_index=2,
            start_local=week_zero_start + timedelta(days=14),
            end_local=week_zero_start + timedelta(days=21),
        ),
    ]

    def _fake_rows_for_platform(
        sid: str,
        *,
        platform: str,
        start_dt: datetime,
        end_dt: datetime,
        source_scope: str,
        target_accounts_by_platform: dict[str, set[str]] | None = None,
    ) -> list[dict[str, object]]:
        assert sid == season_id
        assert source_scope == "bravo"
        assert target_accounts_by_platform is not None
        assert start_dt <= end_dt
        if platform == "instagram":
            return [
                {
                    "platform": "instagram",
                    "kind": "post",
                    "source_id": "ig-post-1",
                    "text": "Official post",
                    "engagement": 120,
                    "ts": week_windows[1].start_local + timedelta(hours=1),
                    "url": "https://example.com/ig-post-1",
                    "author": "bravotv",
                },
                {
                    "platform": "instagram",
                    "kind": "comment",
                    "source_id": "ig-comment-1",
                    "text": "Loved this episode",
                    "engagement": 20,
                    "ts": week_windows[1].start_local + timedelta(hours=2),
                    "url": "https://example.com/ig-comment-1",
                    "author": "viewer1",
                },
            ]
        if platform == "youtube":
            return [
                {
                    "platform": "youtube",
                    "kind": "post",
                    "source_id": "yt-video-1",
                    "text": "Episode clip",
                    "engagement": 300,
                    "ts": week_windows[1].start_local + timedelta(hours=3),
                    "url": "https://example.com/yt-video-1",
                    "author": "bravo",
                }
            ]
        return []

    monkeypatch.setattr(social_repo, "get_season_context", lambda _: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_zero_start),
    )
    def _empty_sentiment_context(_ctx: SeasonContext) -> SentimentAnalyzerContext:
        return SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        )

    monkeypatch.setattr(social_repo, "_build_sentiment_context", _empty_sentiment_context)
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {
            "instagram": {"bravotv", "bravodailydish", "bravowwhl"},
            "youtube": {"bravo", "wwhl"},
            "tiktok": {"bravotv", "bravowwhl"},
            "twitter": {"bravotv", "bravowwhl"},
        },
    )
    monkeypatch.setattr(social_repo, "_rows_for_platform", _fake_rows_for_platform)
    monkeypatch.setattr(social_repo, "list_jobs", lambda *_args, **_kwargs: [])

    payload = get_analytics(
        season_id,
        platforms=["instagram", "youtube"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
    )

    weekly_engagement = payload["weekly_platform_engagement"]
    assert weekly_engagement
    week_one = next(item for item in weekly_engagement if item["week_index"] == 1)
    week_two = next(item for item in weekly_engagement if item["week_index"] == 2)

    assert week_one["has_data"] is True
    assert week_one["engagement"]["instagram"] == 140
    assert week_one["engagement"]["youtube"] == 300
    assert week_one["total_engagement"] == 440
    assert week_two["has_data"] is False
    assert week_two["total_engagement"] == 0


def test_week_detail_instagram_includes_thumbnail_url(monkeypatch) -> None:
    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        if "from social.instagram_posts p" in sql:
            return [
                {
                    "id": "post-1",
                    "source_id": "abc123",
                    "author": "bravotv",
                    "text": "caption",
                    "likes": 10,
                    "comments_count": 2,
                    "views": 30,
                    "media_type": "image",
                    "media_urls": ["https://example.com/ig.jpg"],
                    "thumbnail_url": "https://example.com/ig-thumb.jpg",
                    "ts": datetime(2025, 1, 1, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    payload = _week_detail_instagram(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 8, tzinfo=UTC),
        account_handles={"bravotv", "bravodailydish", "bravowwhl"},
        max_comments=0,
    )
    assert payload["posts"][0]["thumbnail_url"] == "https://example.com/ig-thumb.jpg"


def test_week_detail_tiktok_includes_thumbnail_url(monkeypatch) -> None:
    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        if "from social.tiktok_posts p" in sql:
            return [
                {
                    "id": "post-1",
                    "source_id": "vid123",
                    "author": "bravotv",
                    "nickname": "BravoTV",
                    "text": "caption",
                    "likes": 10,
                    "comments_count": 2,
                    "shares": 3,
                    "views": 30,
                    "hashtags": ["rhoslc"],
                    "thumbnail_url": "https://example.com/tt-thumb.jpg",
                    "duration_seconds": 14,
                    "ts": datetime(2025, 1, 1, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    payload = _week_detail_tiktok(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 8, tzinfo=UTC),
        account_handles={"bravotv", "bravowwhl"},
        max_comments=0,
    )
    assert payload["posts"][0]["thumbnail_url"] == "https://example.com/tt-thumb.jpg"


def test_get_post_comments_youtube_includes_thumbnail_url(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda sql, params: {
            "id": "db-1",
            "source_id": "vid123",
            "author": "Bravo",
            "title": "RHOSLC Trailer",
            "text": "desc",
            "views": 100,
            "likes": 5,
            "comments_count": 2,
            "thumbnail_url": "https://example.com/yt-thumb.jpg",
            "ts": datetime(2025, 1, 1, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda sql, params: [])
    payload = get_post_comments("season-1", platform="youtube", source_id="vid123")
    assert payload["thumbnail_url"] == "https://example.com/yt-thumb.jpg"


def test_list_runs_applies_filters_and_order(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "run-1", "status": "completed"}]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    rows = social_repo.list_runs(
        "season-1",
        limit=25,
        status="completed",
        source_scope="bravo",
    )

    assert rows == [{"id": "run-1", "status": "completed"}]
    sql = str(captured["sql"]).lower()
    params = captured["params"]
    assert "from social.scrape_runs" in sql
    assert "where season_id = %s" in sql
    assert "summary" in sql
    assert "created_at" in sql
    assert "started_at" in sql
    assert "completed_at" in sql
    assert "and status = %s" in sql
    assert "and source_scope = %s" in sql
    assert "order by created_at desc limit %s" in sql
    assert params == ["season-1", "completed", "bravo", 25]


def test_decide_comment_refresh_matrix() -> None:
    now = datetime(2026, 2, 17, 15, 0, tzinfo=UTC)
    fresh_snapshot = social_repo.CommentLifecycleSnapshot(
        active_count=10,
        total_count=10,
        latest_comment_created_at=now - timedelta(days=1),
        last_seen_at=now - timedelta(hours=2),
        last_checked_at=now - timedelta(hours=2),
    )

    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=11,
        snapshot=fresh_snapshot,
        post_published_at=now - timedelta(days=5),
        now=now,
    )
    assert decision.should_refresh is True
    assert decision.reason == "count_gap"

    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=9,
        snapshot=fresh_snapshot,
        post_published_at=now - timedelta(days=5),
        now=now,
    )
    assert decision.should_refresh is True
    assert decision.reason == "count_drop"

    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=10,
        snapshot=None,
        post_published_at=now - timedelta(days=1),
        now=now,
    )
    assert decision.should_refresh is True
    assert decision.reason == "never_checked"

    stale_snapshot = social_repo.CommentLifecycleSnapshot(
        active_count=10,
        total_count=10,
        latest_comment_created_at=now - timedelta(days=2),
        last_seen_at=now - timedelta(days=2),
        last_checked_at=now - timedelta(hours=25),
    )
    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=10,
        snapshot=stale_snapshot,
        post_published_at=now - timedelta(days=3),
        now=now,
    )
    assert decision.should_refresh is True
    assert decision.reason == "stale_recheck"

    quiet_snapshot = social_repo.CommentLifecycleSnapshot(
        active_count=10,
        total_count=10,
        latest_comment_created_at=now - timedelta(days=15),
        last_seen_at=now - timedelta(hours=1),
        last_checked_at=now - timedelta(hours=1),
    )
    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=10,
        snapshot=quiet_snapshot,
        post_published_at=now - timedelta(days=20),
        now=now,
    )
    assert decision.should_refresh is True
    assert decision.reason == "quiet_post_force_recheck"

    decision = social_repo._decide_comment_refresh(
        sync_strategy="full_refresh",
        expected_count=10,
        snapshot=fresh_snapshot,
        post_published_at=now - timedelta(days=1),
        now=now,
    )
    assert decision.should_refresh is True
    assert decision.reason == "full_refresh"

    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=10,
        snapshot=fresh_snapshot,
        post_published_at=now - timedelta(days=1),
        now=now,
    )
    assert decision.should_refresh is False
    assert decision.reason == "up_to_date"


def test_is_comment_fetch_complete_is_conservative() -> None:
    assert social_repo._is_comment_fetch_complete(
        fetch_failed=False,
        fail_reason=None,
        auth_failed=False,
        fetched_count=10,
        max_comments_per_post=1000,
    )
    assert not social_repo._is_comment_fetch_complete(
        fetch_failed=True,
        fail_reason=None,
        auth_failed=False,
        fetched_count=0,
        max_comments_per_post=1000,
    )
    assert not social_repo._is_comment_fetch_complete(
        fetch_failed=False,
        fail_reason="comment_status_10201",
        auth_failed=False,
        fetched_count=0,
        max_comments_per_post=1000,
    )
    assert not social_repo._is_comment_fetch_complete(
        fetch_failed=False,
        fail_reason=None,
        auth_failed=True,
        fetched_count=0,
        max_comments_per_post=1000,
    )
    assert not social_repo._is_comment_fetch_complete(
        fetch_failed=False,
        fail_reason=None,
        auth_failed=False,
        fetched_count=1000,
        max_comments_per_post=1000,
    )


def test_upsert_instagram_comment_tree_reappearance_clears_missing(monkeypatch) -> None:
    captured_payload: dict[str, object] = {}

    class _Comment:
        comment_id = "comment-1"
        username = "viewer"
        user_id = "user-1"
        text = "hello"
        likes = 3
        is_reply = False
        reply_count = 0
        created_at = datetime(2026, 2, 10, tzinfo=UTC)
        replies: list[object] = []

        def to_dict(self) -> dict[str, object]:
            return {"comment_id": self.comment_id}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        captured_payload.update(payload)
        assert table == "instagram_comments"
        assert conflict_col == "comment_id"
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "instagram_comments")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )

    written = social_repo._upsert_instagram_comment_tree(
        context,
        job_id="job-1",
        run_id="run-1",
        account="bravotv",
        post_id="post-1",
        comment=_Comment(),
    )

    assert written == 1
    assert captured_payload["is_missing"] is False
    assert captured_payload["missing_at"] is None
    assert captured_payload["last_seen_run_id"] == "run-1"


def test_ingest_season_stores_sync_strategy_and_platform_scope(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    captured_run_configs: list[dict[str, object]] = []
    created_job_configs: list[dict[str, object]] = []

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "get_season_context", lambda season_id: context)
    monkeypatch.setattr(
        social_repo,
        "get_targets",
        lambda *_args, **_kwargs: {
            "targets": [
                {
                    "platform": "instagram",
                    "accounts": ["bravotv"],
                    "hashtags": ["rhoslc"],
                    "keywords": ["Salt Lake City"],
                    "is_active": True,
                }
            ]
        },
    )

    def _fake_create_run(*_args, **kwargs):
        captured_run_configs.append(kwargs["config"])
        return "run-1"

    def _fake_create_job(*_args, **kwargs):
        created_job_configs.append(kwargs["config"])
        return f"job-{len(created_job_configs)}"

    monkeypatch.setattr(social_repo, "_create_run", _fake_create_run)
    monkeypatch.setattr(social_repo, "_create_job", _fake_create_job)
    monkeypatch.setattr(social_repo, "_update_run_summary", lambda _run_id: {"total_jobs": len(created_job_configs)})
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)

    payload = social_repo.ingest_season(
        "season-1",
        platforms=["instagram"],
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=500,
        max_comments_per_post=300,
        max_replies_per_post=200,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 10, tzinfo=UTC),
        initiated_by="admin@test",
    )

    assert payload["run_id"] == "run-1"
    assert captured_run_configs
    assert captured_run_configs[0]["sync_strategy"] == "incremental"
    assert captured_run_configs[0]["platforms"] == ["instagram"]
    assert all(config["sync_strategy"] == "incremental" for config in created_job_configs)

    captured_run_configs.clear()
    created_job_configs.clear()

    social_repo.ingest_season(
        "season-1",
        platforms=None,
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=500,
        max_comments_per_post=300,
        max_replies_per_post=200,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 10, tzinfo=UTC),
        initiated_by="admin@test",
    )
    assert captured_run_configs[0]["platforms"] == "all"

    captured_run_configs.clear()
    created_job_configs.clear()

    comments_only_payload = social_repo.ingest_season(
        "season-1",
        platforms=["instagram"],
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=500,
        max_comments_per_post=300,
        max_replies_per_post=200,
        fetch_replies=True,
        ingest_mode="comments_only",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 10, tzinfo=UTC),
        initiated_by="admin@test",
    )
    assert comments_only_payload["stages"] == ["comments"]
    assert len(created_job_configs) == 1
    assert all(config["stage"] == "comments" for config in created_job_configs)

def test_repository_has_single_pg_upsert_definition() -> None:
    source = inspect.getsource(social_repo)
    assert source.count("def _pg_upsert(") == 1


def test_resolve_sentiment_gemini_model_selection_prefers_pro_alias(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_SENTIMENT_GEMINI_MODEL", "")
    monkeypatch.setenv("GEMINI_MODEL_PRO", "gemini-2.5-pro")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    monkeypatch.setenv("GEMINI_MODEL_FAST", "gemini-2.5-flash-lite")
    monkeypatch.delenv("GOOGLE_GEMINI_MODEL", raising=False)

    model, source, fallback = social_repo._resolve_sentiment_gemini_model_selection()

    assert model == "gemini-2.5-pro"
    assert source == "GEMINI_MODEL_PRO"
    assert fallback == "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO"


def test_classify_ambiguous_sentiments_logs_model_source_and_fallback(monkeypatch, caplog) -> None:
    monkeypatch.setenv("SOCIAL_SENTIMENT_GEMINI_ENABLED", "true")
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setenv("SOCIAL_SENTIMENT_GEMINI_MODEL", "")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-pro-canonical")
    monkeypatch.delenv("GEMINI_MODEL_PRO", raising=False)
    monkeypatch.delenv("GOOGLE_GEMINI_MODEL", raising=False)
    monkeypatch.delenv("GEMINI_MODEL_FAST", raising=False)

    class _Response:
        text = '[{"index":0,"sentiment":"positive","confidence":0.9}]'

    monkeypatch.setattr(
        social_repo,
        "_build_gemini_text_generator",
        lambda **_kwargs: (lambda _prompt: _Response(), "google-genai"),
    )

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    analyzer_context = SentimentAnalyzerContext(
        cast_terms=set(),
        cast_phrases=set(),
        episode_terms=set(),
        episode_summary="",
    )

    with caplog.at_level("INFO", logger=social_repo.__name__):
        overrides = social_repo._classify_ambiguous_sentiments_with_gemini(
            [("comment-1", "Loved this episode")],
            context=context,
            analyzer_context=analyzer_context,
        )

    assert overrides["comment-1"][0] == "positive"
    assert any(
        "Gemini sentiment route=pro model=gemini-2.5-pro-canonical sdk=google-genai "
        "source=GEMINI_MODEL "
        "fallback_path=SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL->GEMINI_MODEL"
        in record.message
        for record in caplog.records
    )
