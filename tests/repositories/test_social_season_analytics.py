"""Unit tests for season social analytics helpers."""

import inspect
import json
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

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
    get_comments_coverage,
    get_mirror_coverage,
    get_post_comments,
    get_targets,
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


def test_resolve_week_windows_inserts_bye_windows_and_caps_final_week(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-week-windows",
        show_id="show-week-windows",
        show_name="The Real Housewives of Salt Lake City",
        season_number=6,
        anchor_date=date(2026, 1, 8),
    )

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        assert params == [context.season_id]
        assert "from core.episodes" in normalized
        return [
            {"episode_number": 1, "air_date": date(2026, 1, 8)},
            {"episode_number": 2, "air_date": date(2026, 1, 15)},
            {"episode_number": 3, "air_date": date(2026, 1, 28)},
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_find_week_zero_start_override", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_find_week_zero_start_from_snapshot", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_find_week_zero_start_from_social_rows", lambda **_kwargs: None)

    windows, week_zero_start = social_repo._resolve_week_windows(
        context,
        timezone="America/New_York",
        source_scope="bravo",
        now_utc=datetime(2026, 2, 1, 12, 0, tzinfo=UTC),
    )

    zone = ZoneInfo("America/New_York")
    assert [window.week_index for window in windows] == [0, 1, 2, 3, 4, 5, 6, 7]
    assert [window.week_type for window in windows] == [
        "preseason",
        "episode",
        "episode",
        "bye",
        "episode",
        "postseason",
        "postseason",
        "postseason",
    ]
    assert [window.episode_number for window in windows] == [None, 1, 2, None, 3, None, None, None]
    assert week_zero_start == datetime(2026, 1, 1, 20, 0, tzinfo=zone)
    assert windows[3].start_local == datetime(2026, 1, 22, 20, 0, tzinfo=zone)
    assert windows[3].end_local == datetime(2026, 1, 28, 20, 0, tzinfo=zone)
    assert windows[4].end_local == datetime(2026, 2, 4, 20, 0, tzinfo=zone)
    assert windows[5].start_local == datetime(2026, 2, 4, 20, 0, tzinfo=zone)
    assert windows[7].end_local == datetime(2026, 2, 25, 20, 0, tzinfo=zone)


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


def test_get_analytics_caps_full_window_end_and_emits_bye_week_metadata(monkeypatch) -> None:
    season_id = "season-analytics-bye-week"
    context = SeasonContext(
        season_id=season_id,
        show_id="show-analytics-bye-week",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 8),
    )
    zone = ZoneInfo("America/New_York")
    week_windows = [
        WeekWindow(
            week_index=0,
            start_local=datetime(2026, 1, 1, 20, 0, tzinfo=zone),
            end_local=datetime(2026, 1, 8, 20, 0, tzinfo=zone),
            week_type="preseason",
            episode_number=None,
        ),
        WeekWindow(
            week_index=1,
            start_local=datetime(2026, 1, 8, 20, 0, tzinfo=zone),
            end_local=datetime(2026, 1, 15, 20, 0, tzinfo=zone),
            week_type="episode",
            episode_number=1,
        ),
        WeekWindow(
            week_index=2,
            start_local=datetime(2026, 1, 15, 20, 0, tzinfo=zone),
            end_local=datetime(2026, 1, 22, 20, 0, tzinfo=zone),
            week_type="bye",
            episode_number=None,
        ),
        WeekWindow(
            week_index=3,
            start_local=datetime(2026, 1, 22, 20, 0, tzinfo=zone),
            end_local=datetime(2026, 1, 29, 20, 0, tzinfo=zone),
            week_type="episode",
            episode_number=2,
        ),
    ]
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "get_season_context", lambda _sid: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_windows[0].start_local),
    )
    monkeypatch.setattr(
        social_repo,
        "_build_sentiment_context",
        lambda _ctx: SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {
            "instagram": {"bravotv"},
            "youtube": {"bravo"},
            "tiktok": {"bravotv"},
            "twitter": {"bravotv"},
        },
    )

    def _fake_build_rows(
        _season_id: str,
        *,
        platforms: list[str],
        start_dt: datetime,
        end_dt: datetime,
        source_scope: str,
        season_context: SeasonContext,
        analyzer_context: SentimentAnalyzerContext,
        target_accounts_by_platform: dict[str, set[str]] | None = None,
        include_post_text: bool = True,
    ) -> list[dict[str, object]]:
        del platforms, source_scope, season_context, analyzer_context, target_accounts_by_platform, include_post_text
        captured["start_dt"] = start_dt
        captured["end_dt"] = end_dt
        return []

    monkeypatch.setattr(social_repo, "_build_rows", _fake_build_rows)
    monkeypatch.setattr(social_repo, "list_jobs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(social_repo, "_now_utc", lambda: datetime(2026, 2, 20, 12, 0, tzinfo=UTC))

    payload = get_analytics(
        season_id,
        platforms=["instagram"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
        include_jobs=False,
    )

    assert captured["start_dt"] == datetime(2026, 1, 2, 1, 0, tzinfo=UTC)
    assert captured["end_dt"] == datetime(2026, 1, 30, 1, 0, tzinfo=UTC)
    assert payload["window"]["end"] == social_repo._iso(datetime(2026, 1, 30, 1, 0, tzinfo=UTC))

    bye_week = next(item for item in payload["weekly"] if item["week_type"] == "bye")
    assert bye_week["label"] == "BYE WEEK (Jan 15-Jan 22)"
    assert bye_week["episode_number"] is None

    bye_week_platform = next(item for item in payload["weekly_platform_posts"] if item["week_type"] == "bye")
    assert bye_week_platform["label"] == "BYE WEEK (Jan 15-Jan 22)"
    assert bye_week_platform["episode_number"] is None

    bye_week_daily = next(item for item in payload["weekly_daily_activity"] if item["week_type"] == "bye")
    assert bye_week_daily["label"] == "BYE WEEK (Jan 15-Jan 22)"
    assert bye_week_daily["episode_number"] is None


def test_get_analytics_emits_postseason_week_metadata(monkeypatch) -> None:
    season_id = "season-analytics-postseason-week"
    context = SeasonContext(
        season_id=season_id,
        show_id="show-analytics-postseason-week",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 8),
    )
    zone = ZoneInfo("America/New_York")
    week_windows = [
        WeekWindow(
            week_index=0,
            start_local=datetime(2026, 1, 1, 20, 0, tzinfo=zone),
            end_local=datetime(2026, 1, 8, 20, 0, tzinfo=zone),
            week_type="preseason",
            episode_number=None,
        ),
        WeekWindow(
            week_index=1,
            start_local=datetime(2026, 1, 8, 20, 0, tzinfo=zone),
            end_local=datetime(2026, 1, 15, 20, 0, tzinfo=zone),
            week_type="episode",
            episode_number=1,
        ),
        WeekWindow(
            week_index=2,
            start_local=datetime(2026, 1, 15, 20, 0, tzinfo=zone),
            end_local=datetime(2026, 1, 22, 20, 0, tzinfo=zone),
            week_type="postseason",
            episode_number=None,
        ),
    ]

    monkeypatch.setattr(social_repo, "get_season_context", lambda _sid: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_windows[0].start_local),
    )
    monkeypatch.setattr(
        social_repo,
        "_build_sentiment_context",
        lambda _ctx: SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {
            "instagram": {"bravotv"},
            "youtube": {"bravo"},
            "tiktok": {"bravotv"},
            "twitter": {"bravotv"},
        },
    )
    monkeypatch.setattr(social_repo, "_build_rows", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(social_repo, "list_jobs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(social_repo, "_now_utc", lambda: datetime(2026, 1, 23, 12, 0, tzinfo=UTC))

    payload = get_analytics(
        season_id,
        platforms=["instagram"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
        include_jobs=False,
    )

    postseason_week = next(item for item in payload["weekly"] if item["week_type"] == "postseason")
    assert postseason_week["week_index"] == 2
    assert postseason_week["episode_number"] is None
    assert postseason_week["label"] == "Week 2"

    postseason_platform = next(item for item in payload["weekly_platform_posts"] if item["week_type"] == "postseason")
    assert postseason_platform["week_index"] == 2
    assert postseason_platform["episode_number"] is None
    assert postseason_platform["label"] == "Week 2"

    postseason_daily = next(item for item in payload["weekly_daily_activity"] if item["week_type"] == "postseason")
    assert postseason_daily["week_index"] == 2
    assert postseason_daily["episode_number"] is None
    assert postseason_daily["label"] == "Week 2"


def test_youtube_video_matches_show_terms_accepts_title_hashtag_or_description() -> None:
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
    assert _youtube_video_matches_show_terms(
        title="Bravo sneak peek",
        description="Cast includes Angie Katsanevas (RHOSLC)",
        hashtags=hashtags,
        keywords=keywords,
    )


def test_youtube_video_matches_show_terms_excludes_wife_swap_housewives_edition() -> None:
    assert _youtube_title_is_cross_show_excluded("SNEAK PEEK: Wife Swap: The Real Housewives Edition")
    assert not _youtube_video_matches_show_terms(
        title="SNEAK PEEK: Wife Swap: The Real Housewives Edition",
        description="Cast includes #RHOSLC",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC", "Salt Lake City"],
    )


def test_upsert_youtube_video_persists_short_flags(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        del conn
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-yt-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    original_has_column = social_repo._platform_posts_has_column

    def _fake_has_column(platform: str, column: str) -> bool:
        if platform == "youtube" and column in {"is_short", "source_surface"}:
            return True
        return original_has_column(platform, column)

    monkeypatch.setattr(social_repo, "_platform_posts_has_column", _fake_has_column)
    context = SeasonContext(
        season_id="season-yt-upsert",
        show_id="show-yt-upsert",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 8, 14),
    )
    video = SimpleNamespace(
        video_id="short-abc",
        channel_id="channel-1",
        channel_title="Bravo",
        title="RHOSLC short",
        description="desc",
        duration="PT45S",
        duration_seconds=45,
        views=100,
        likes=10,
        comments=5,
        thumbnail_url="https://img.test/short.jpg",
        published_at=datetime(2025, 8, 14, tzinfo=UTC),
        is_short=True,
        source_surface="shorts",
        to_dict=lambda: {"video_id": "short-abc"},
    )

    payload = social_repo._upsert_youtube_video(
        context,
        job_id="job-yt-upsert",
        account="bravo",
        video=video,
        conn=None,
    )

    assert payload == {"id": "db-yt-1"}
    assert captured["table"] == "youtube_videos"
    assert captured["conflict_col"] == "video_id"
    saved = captured["payload"]
    assert isinstance(saved, dict)
    assert saved["is_short"] is True
    assert saved["source_surface"] == "shorts"


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


def test_upsert_instagram_post_persists_metadata_and_hosted_urls(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-post-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_instagram_posts_has_column", lambda _column: True)

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    scraped_at = datetime(2026, 2, 19, 15, 0, tzinfo=UTC)
    post = SimpleNamespace(
        shortcode="abc123",
        pk="media-1",
        username="bravotv",
        caption="Watch #RHOSLC with @bravotv",
        post_type="reel",
        media_urls=["https://source.example/media.mp4"],
        thumbnail_url="https://source.example/thumb.jpg",
        likes=111,
        comments=22,
        video_views=333,
        taken_at=datetime(2026, 2, 18, 20, 0, tzinfo=UTC),
        profile_tags=["tagged_user"],
        collaborators=["collab_user"],
        hashtags=["RHOSLC"],
        mentions=["@bravotv"],
        duration_seconds=19,
        post_format="reel",
        metadata_source="permalink_html",
        metadata_scraped_at=scraped_at,
        metadata_error=None,
        hosted_thumbnail_url="https://cdn.example/social/thumb.jpg",
        hosted_media_urls=["https://cdn.example/social/media.mp4"],
        media_mirror_status="mirrored",
        media_mirror_error=None,
        to_dict=lambda: {"shortcode": "abc123"},
    )

    row = social_repo._upsert_instagram_post(
        context,
        job_id="job-1",
        account="bravotv",
        post=post,
        conn=None,
    )

    assert row == {"id": "db-post-1"}
    assert captured["table"] == "instagram_posts"
    assert captured["conflict_col"] == "shortcode"
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["post_format"] == "reel"
    assert payload["profile_tags"] == ["tagged_user"]
    assert payload["collaborators"] == ["collab_user"]
    assert payload["hashtags"] == ["RHOSLC"]
    assert payload["mentions"] == ["@bravotv"]
    assert payload["duration_seconds"] == 19
    assert payload["metadata_source"] == "permalink_html"
    assert payload["metadata_scraped_at"] == scraped_at
    assert payload["hosted_thumbnail_url"] == "https://cdn.example/social/thumb.jpg"
    assert payload["hosted_media_urls"] == ["https://cdn.example/social/media.mp4"]
    assert payload["media_mirror_status"] == "mirrored"


def test_upsert_instagram_post_skips_missing_optional_columns(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-post-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_instagram_posts_has_column", lambda _column: False)

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post = SimpleNamespace(
        shortcode="abc123",
        pk="media-1",
        username="bravotv",
        caption="Watch #RHOSLC with @bravotv",
        post_type="reel",
        media_urls=["https://source.example/media.mp4"],
        thumbnail_url="https://source.example/thumb.jpg",
        likes=111,
        comments=22,
        video_views=333,
        taken_at=datetime(2026, 2, 18, 20, 0, tzinfo=UTC),
        to_dict=lambda: {"shortcode": "abc123"},
    )

    row = social_repo._upsert_instagram_post(
        context,
        job_id="job-1",
        account="bravotv",
        post=post,
        conn=None,
    )

    assert row == {"id": "db-post-1"}
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert "post_format" not in payload
    assert "hosted_thumbnail_url" not in payload
    assert "hosted_media_urls" not in payload


def test_resolve_depth_defaults_respects_explicit_values_and_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_DEFAULT_MAX_COMMENTS_PER_POST", "180")
    monkeypatch.setenv("SOCIAL_DEFAULT_MAX_REPLIES_PER_POST", "90")

    posts, comments, replies, fetch_replies = _resolve_depth_defaults(
        max_posts_per_target=500,
        max_comments_per_post=50,
        max_replies_per_post=10,
        fetch_replies=False,
    )
    assert posts == 500
    assert comments == 50
    assert replies == 10
    assert fetch_replies is False

    posts2, comments2, replies2, fetch_replies2 = _resolve_depth_defaults(
        max_posts_per_target=10000,
        max_comments_per_post=None,
        max_replies_per_post=None,
        fetch_replies=True,
    )
    assert posts2 == 10000
    assert comments2 == 180
    assert replies2 == 90
    assert fetch_replies2 is True


def test_load_existing_posts_normalizes_account_filter(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    rows = social_repo._load_existing_posts(
        "instagram",
        context,
        "@BravoTV",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )
    assert rows == []
    params = captured["params"]
    assert isinstance(params, list)
    assert params[1] == "bravotv"
    sql = str(captured["sql"]).lower()
    assert "ltrim(lower(coalesce(nullif(source_account, ''), nullif(username, ''), '')), '@') = %s" in sql


def test_load_existing_posts_applies_source_id_filter(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    rows = social_repo._load_existing_posts(
        "instagram",
        context,
        "@BravoTV",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
        source_ids={"abc123", "def456"},
    )
    assert rows == []
    params = captured["params"]
    assert isinstance(params, list)
    assert sorted(params[-1]) == ["abc123", "def456"]
    sql = str(captured["sql"]).lower()
    assert "shortcode = any(%s)" in sql


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
    assert "ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)" in sql
    assert "ltrim(lower(coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '')), '@') = any(%s)" in sql
    assert "ltrim(lower(coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '')), '@') = any(%s)" in sql
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
                    "reported_comments": 12,
                    "ts": week_windows[1].start_local + timedelta(hours=1),
                    "url": "https://example.com/ig-post-1",
                    "author": "bravotv",
                    "thumbnail_url": "https://images.test/ig-post-1.jpg",
                },
                {
                    "platform": "instagram",
                    "kind": "comment",
                    "source_id": "ig-comment-1",
                    "text": "Loved this episode",
                    "engagement": 20,
                    "reported_comments": 0,
                    "ts": week_windows[1].start_local + timedelta(hours=2),
                    "url": "https://example.com/ig-comment-1",
                    "author": "viewer1",
                    "thumbnail_url": "https://images.test/ig-post-1.jpg",
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
                    "reported_comments": 30,
                    "ts": week_windows[1].start_local + timedelta(hours=3),
                    "url": "https://example.com/yt-video-1",
                    "author": "bravo",
                    "thumbnail_url": None,
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

    weekly_daily = payload["weekly_daily_activity"]
    weekly_platform_rows = payload["weekly_platform_posts"]
    weekly_rows = payload["weekly"]
    assert weekly_daily
    daily_week_one = next(item for item in weekly_daily if item["week_index"] == 1)
    daily_week_two = next(item for item in weekly_daily if item["week_index"] == 2)
    weekly_row_one = next(item for item in weekly_rows if item["week_index"] == 1)
    weekly_platform_row_one = next(item for item in weekly_platform_rows if item["week_index"] == 1)

    assert len(daily_week_one["days"]) == 7
    assert len(daily_week_two["days"]) == 7

    populated_days = [day for day in daily_week_one["days"] if day["total_posts"] > 0 or day["total_comments"] > 0]
    assert len(populated_days) == 1
    populated_day = populated_days[0]
    assert populated_day["posts"]["instagram"] == 1
    assert populated_day["posts"]["youtube"] == 1
    assert populated_day["comments"]["instagram"] == 1
    assert populated_day["reported_comments"]["instagram"] == 12
    assert populated_day["reported_comments"]["youtube"] == 30
    assert populated_day["total_posts"] == 2
    assert populated_day["total_comments"] == 1
    assert populated_day["total_reported_comments"] == 42

    assert sum(day["total_posts"] for day in daily_week_one["days"]) == weekly_row_one["post_volume"]
    assert sum(day["total_comments"] for day in daily_week_one["days"]) == weekly_row_one["comment_volume"]
    assert sum(day["total_reported_comments"] for day in daily_week_one["days"]) == 42
    assert weekly_platform_row_one["comments"]["instagram"] == 1
    assert weekly_platform_row_one["comments"]["youtube"] == 0
    assert weekly_platform_row_one["reported_comments"]["instagram"] == 12
    assert weekly_platform_row_one["reported_comments"]["youtube"] == 30
    assert weekly_platform_row_one["total_comments"] == 1
    assert weekly_platform_row_one["total_reported_comments"] == 42
    assert weekly_platform_row_one["comments_saved_pct"] == 2.4
    assert all(day["total_posts"] == 0 for day in daily_week_two["days"])
    assert all(day["total_comments"] == 0 for day in daily_week_two["days"])
    assert all(day["total_reported_comments"] == 0 for day in daily_week_two["days"])
    bravo_entries = payload["leaderboards"]["bravo_content"]
    viewer_entries = payload["leaderboards"]["viewer_discussion"]
    ig_bravo_entry = next(item for item in bravo_entries if item["source_id"] == "ig-post-1")
    ig_viewer_entry = next(item for item in viewer_entries if item["source_id"] == "ig-comment-1")
    assert ig_bravo_entry["thumbnail_url"] == "https://images.test/ig-post-1.jpg"
    assert ig_viewer_entry["thumbnail_url"] == "https://images.test/ig-post-1.jpg"


def test_get_analytics_include_jobs_false_keeps_jobs_key_and_skips_listing(monkeypatch) -> None:
    season_id = "season-analytics-no-jobs"
    context = SeasonContext(
        season_id=season_id,
        show_id="show-analytics-no-jobs",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )
    week_zero_start = datetime(2025, 9, 9, tzinfo=UTC)
    week_windows = [
        WeekWindow(week_index=0, start_local=week_zero_start, end_local=week_zero_start + timedelta(days=7)),
    ]

    monkeypatch.setattr(social_repo, "get_season_context", lambda _: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_zero_start),
    )
    monkeypatch.setattr(
        social_repo,
        "_build_sentiment_context",
        lambda _ctx: SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {
            "instagram": {"bravotv"},
            "youtube": {"bravo"},
            "tiktok": {"bravotv"},
            "twitter": {"bravotv"},
        },
    )
    monkeypatch.setattr(social_repo, "_rows_for_platform", lambda *_args, **_kwargs: [])

    def _raise_if_called(*_args, **_kwargs):
        raise AssertionError("list_jobs should not be called when include_jobs=False")

    monkeypatch.setattr(social_repo, "list_jobs", _raise_if_called)

    payload = get_analytics(
        season_id,
        platforms=["instagram"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
        include_jobs=False,
    )

    assert payload["jobs"] == []


def test_get_analytics_includes_youtube_content_breakdown(monkeypatch) -> None:
    season_id = "season-analytics-youtube-breakdown"
    context = SeasonContext(
        season_id=season_id,
        show_id="show-analytics-youtube-breakdown",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 8, 14),
    )
    week_start = datetime(2025, 8, 14, tzinfo=UTC)
    week_windows = [WeekWindow(week_index=0, start_local=week_start, end_local=week_start + timedelta(days=7))]

    monkeypatch.setattr(social_repo, "get_season_context", lambda _sid: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_start),
    )
    monkeypatch.setattr(
        social_repo,
        "_build_sentiment_context",
        lambda _ctx: SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {"youtube": {"bravo"}},
    )

    def _fake_rows_for_platform(
        _season_id: str,
        *,
        platform: str,
        **_kwargs,
    ) -> list[dict[str, object]]:
        if platform != "youtube":
            return []
        return [
            {
                "platform": "youtube",
                "kind": "post",
                "source_id": "watch-1",
                "text": "Episode recap",
                "engagement": 100,
                "reported_comments": 10,
                "ts": week_start + timedelta(hours=1),
                "url": "https://www.youtube.com/watch?v=watch-1",
                "author": "bravo",
                "thumbnail_url": None,
                "is_short": False,
            },
            {
                "platform": "youtube",
                "kind": "post",
                "source_id": "short-1",
                "text": "Quick short",
                "engagement": 50,
                "reported_comments": 4,
                "ts": week_start + timedelta(hours=2),
                "url": "https://www.youtube.com/shorts/short-1",
                "author": "bravo",
                "thumbnail_url": None,
                "is_short": True,
            },
            {
                "platform": "youtube",
                "kind": "comment",
                "source_id": "comment-1",
                "text": "Nice",
                "engagement": 3,
                "reported_comments": 0,
                "ts": week_start + timedelta(hours=3),
                "url": "https://www.youtube.com/watch?v=watch-1",
                "author": "viewer",
                "thumbnail_url": None,
            },
        ]

    monkeypatch.setattr(social_repo, "_rows_for_platform", _fake_rows_for_platform)
    monkeypatch.setattr(social_repo, "list_jobs", lambda *_args, **_kwargs: [])

    payload = get_analytics(
        season_id,
        platforms=["youtube"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
        include_jobs=False,
    )

    breakdown = payload["summary"]["data_quality"]["youtube_content_breakdown"]
    assert breakdown["videos_count"] == 1
    assert breakdown["reels_count"] == 1
    assert breakdown["total_count"] == 2


def test_get_targets_uses_existing_rows_without_season_context_lookup(monkeypatch) -> None:
    season_id = "season-targets-existing"
    rows = [
        {
            "season_id": season_id,
            "show_id": "show-1",
            "season_number": 6,
            "show_name": "Test Show",
            "platform": "youtube",
            "source_scope": "bravo",
            "timezone": "America/New_York",
            "accounts": ["bravo"],
            "hashtags": [],
            "keywords": ["rhoslc"],
            "is_active": True,
            "config": {"include_comments": True},
            "updated_by": "admin@example.com",
            "updated_at": datetime(2026, 2, 20, tzinfo=UTC),
            "created_at": datetime(2026, 2, 20, tzinfo=UTC),
        }
    ]

    def _fake_fetch_all(_sql: str, params: list[object]) -> list[dict[str, object]]:
        assert params == [season_id, "bravo"]
        assert "join core.seasons" in _sql.lower()
        assert "join core.shows" in _sql.lower()
        return rows

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    def _raise_if_context_called(_season_id: str) -> SeasonContext:
        raise AssertionError("get_season_context should not be called when targets rows exist")

    monkeypatch.setattr(social_repo, "get_season_context", _raise_if_context_called)

    payload = get_targets(season_id, source_scope="bravo")

    assert payload["season_id"] == season_id
    assert payload["show_id"] == "show-1"
    assert payload["season_number"] == 6
    assert payload["show_name"] == "Test Show"
    assert payload["source_scope"] == "bravo"
    assert payload["targets"] == [
        {
            "season_id": season_id,
            "show_id": "show-1",
            "platform": "youtube",
            "source_scope": "bravo",
            "timezone": "America/New_York",
            "accounts": ["bravo"],
            "hashtags": [],
            "keywords": ["rhoslc"],
            "is_active": True,
            "config": {"include_comments": True},
            "updated_by": "admin@example.com",
            "updated_at": rows[0]["updated_at"],
            "created_at": rows[0]["created_at"],
        }
    ]
    assert payload["using_defaults"] is False


def test_target_accounts_by_platform_uses_direct_targets_query(monkeypatch) -> None:
    season_id = "season-target-accounts"
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {"platform": "youtube", "accounts": ["bravo", "@wwhl"], "is_active": True},
            {"platform": "instagram", "accounts": ["bravotv"], "is_active": False},
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: (_ for _ in ()).throw(AssertionError("get_season_context should not be called")),
    )

    payload = social_repo._target_accounts_by_platform(season_id, source_scope="bravo")

    assert captured["params"] == [season_id, "bravo"]
    assert "from social.season_targets" in str(captured["sql"]).lower()
    assert payload["youtube"] == {"bravo", "wwhl"}
    assert payload["instagram"] == set()


def test_get_analytics_weekly_daily_activity_uses_dynamic_day_count(monkeypatch) -> None:
    season_id = "season-analytics-dynamic-days"
    context = SeasonContext(
        season_id=season_id,
        show_id="show-analytics-dynamic-days",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )
    week_zero_start = datetime(2025, 9, 1, tzinfo=UTC)
    week_windows = [
        WeekWindow(week_index=0, start_local=week_zero_start, end_local=week_zero_start + timedelta(days=10)),
        WeekWindow(
            week_index=1,
            start_local=week_zero_start + timedelta(days=10),
            end_local=week_zero_start + timedelta(days=17),
        ),
    ]

    monkeypatch.setattr(social_repo, "get_season_context", lambda _: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_zero_start),
    )
    monkeypatch.setattr(
        social_repo,
        "_build_sentiment_context",
        lambda _ctx: SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {
            "instagram": {"bravotv"},
            "youtube": {"bravo"},
            "tiktok": {"bravotv"},
            "twitter": {"bravotv"},
        },
    )
    monkeypatch.setattr(social_repo, "_rows_for_platform", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(social_repo, "list_jobs", lambda *_args, **_kwargs: [])

    payload = get_analytics(
        season_id,
        platforms=["instagram"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
    )

    weekly_daily = payload["weekly_daily_activity"]
    week_zero = next(item for item in weekly_daily if item["week_index"] == 0)
    week_one = next(item for item in weekly_daily if item["week_index"] == 1)

    assert len(week_zero["days"]) == 10
    assert len(week_one["days"]) == 7
    assert all(day["total_posts"] == 0 for day in week_zero["days"])
    assert all(day["total_comments"] == 0 for day in week_zero["days"])
    assert all(day["total_reported_comments"] == 0 for day in week_zero["days"])


def test_get_analytics_additive_quality_flags_schedule_and_benchmark(monkeypatch) -> None:
    season_id = "season-analytics-quality"
    context = SeasonContext(
        season_id=season_id,
        show_id="show-analytics-quality",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )
    week_start = datetime(2025, 9, 16, tzinfo=UTC)
    week_windows = [
        WeekWindow(week_index=1, start_local=week_start, end_local=week_start + timedelta(days=7)),
        WeekWindow(week_index=2, start_local=week_start + timedelta(days=7), end_local=week_start + timedelta(days=14)),
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
        if platform != "instagram":
            return []
        return [
            {
                "platform": "instagram",
                "kind": "post",
                "source_id": "ig-post-1",
                "text": "RHOSLC",
                "engagement": 10,
                "reported_comments": 12,
                "ts": datetime(2025, 9, 17, 12, 0, tzinfo=UTC),
                "url": "https://example.com/ig-post-1",
                "author": "bravotv",
            },
            {
                "platform": "instagram",
                "kind": "comment",
                "source_id": "ig-comment-1",
                "text": "great episode",
                "engagement": 1,
                "reported_comments": 0,
                "ts": datetime(2025, 9, 17, 13, 0, tzinfo=UTC),
                "url": "https://example.com/ig-comment-1",
                "author": "viewer",
            },
        ]

    monkeypatch.setattr(social_repo, "get_season_context", lambda _: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_start),
    )
    monkeypatch.setattr(
        social_repo,
        "_build_sentiment_context",
        lambda _ctx: SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {
            "instagram": {"bravotv"},
            "youtube": {"bravo"},
            "tiktok": {"bravotv"},
            "twitter": {"bravotv"},
        },
    )
    monkeypatch.setattr(social_repo, "_rows_for_platform", _fake_rows_for_platform)
    monkeypatch.setattr(social_repo, "list_jobs", lambda *_args, **_kwargs: [])

    payload = get_analytics(
        season_id,
        platforms=["instagram"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
    )

    assert "data_quality" in payload["summary"]
    assert payload["summary"]["data_quality"]["comments_saved_pct_overall"] is not None
    assert "weekly_flags" in payload
    zero_flags = [flag for flag in payload["weekly_flags"] if flag["code"] == "zero_activity"]
    assert zero_flags
    assert payload["schedule_profile"]["timezone"] == "America/New_York"
    assert payload["schedule_profile"]["platforms"][0]["platform"] == "instagram"
    assert "benchmark" in payload


def test_get_comments_coverage_aggregates_scoped_platform_totals(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    monkeypatch.setattr(social_repo, "get_season_context", lambda _sid: context)
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {"instagram": {"bravotv"}, "twitter": {"bravotv"}},
    )
    monkeypatch.setattr(
        social_repo,
        "_comments_coverage_for_platform",
        lambda _season_id, *, platform, **_kwargs: (
            {
                "posts_scanned": 4,
                "stale_posts_count": 1,
                "saved_comments": 8,
                "reported_comments": 10,
            }
            if platform == "instagram"
            else {
                "posts_scanned": 3,
                "stale_posts_count": 0,
                "saved_comments": 5,
                "reported_comments": 3,
            }
        ),
    )
    monkeypatch.setattr(social_repo, "_now_utc", lambda: datetime(2026, 2, 24, 12, 0, tzinfo=UTC))

    payload = get_comments_coverage(
        "season-1",
        platforms=["instagram", "twitter"],
        timezone="America/New_York",
        source_scope="bravo",
        date_start=datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
        date_end=datetime(2026, 2, 20, 0, 0, tzinfo=UTC),
    )

    assert payload["total_saved_comments"] == 13
    assert payload["total_reported_comments"] == 13
    assert payload["up_to_date"] is True
    assert payload["stale_posts_count"] == 1
    assert payload["posts_scanned"] == 7
    assert payload["by_platform"]["instagram"]["up_to_date"] is False
    assert payload["by_platform"]["twitter"]["up_to_date"] is True


def test_get_comments_coverage_uses_week_zero_window_when_dates_omitted(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    week_zero_start = datetime(2026, 1, 10, 20, 0, tzinfo=ZoneInfo("America/New_York"))
    now_utc = datetime(2026, 2, 24, 12, 0, tzinfo=UTC)

    monkeypatch.setattr(social_repo, "get_season_context", lambda _sid: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (
            [WeekWindow(0, week_zero_start, week_zero_start + timedelta(days=7))],
            week_zero_start,
        ),
    )
    monkeypatch.setattr(social_repo, "_now_utc", lambda: now_utc)
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {"instagram": {"bravotv"}},
    )
    monkeypatch.setattr(
        social_repo,
        "_comments_coverage_for_platform",
        lambda *_args, **_kwargs: {
            "posts_scanned": 0,
            "stale_posts_count": 0,
            "saved_comments": 0,
            "reported_comments": 0,
        },
    )

    payload = get_comments_coverage(
        "season-1",
        platforms=["instagram"],
        timezone="America/New_York",
        source_scope="bravo",
    )

    assert payload["window"]["start"] == social_repo._iso(week_zero_start.astimezone(UTC))
    assert payload["window"]["end"] == social_repo._iso(now_utc)
    assert payload["evaluated_at"] == social_repo._iso(now_utc)


def test_get_mirror_coverage_aggregates_platform_totals(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    monkeypatch.setattr(social_repo, "get_season_context", lambda _sid: context)
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {"instagram": {"bravotv"}, "twitter": {"bravotv"}},
    )
    monkeypatch.setattr(
        social_repo,
        "_mirror_coverage_for_platform",
        lambda _season_id, *, platform, **_kwargs: (
            {
                "posts_scanned": 5,
                "needs_mirror_count": 2,
                "mirrored_count": 3,
                "failed_count": 1,
                "partial_count": 1,
                "pending_count": 0,
            }
            if platform == "instagram"
            else {
                "posts_scanned": 4,
                "needs_mirror_count": 0,
                "mirrored_count": 4,
                "failed_count": 0,
                "partial_count": 0,
                "pending_count": 0,
            }
        ),
    )
    monkeypatch.setattr(social_repo, "_now_utc", lambda: datetime(2026, 2, 24, 12, 0, tzinfo=UTC))

    payload = get_mirror_coverage(
        "season-1",
        platforms=["instagram", "twitter"],
        timezone="America/New_York",
        source_scope="bravo",
        date_start=datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
        date_end=datetime(2026, 2, 20, 0, 0, tzinfo=UTC),
    )

    assert payload["posts_scanned"] == 9
    assert payload["needs_mirror_count"] == 2
    assert payload["mirrored_count"] == 7
    assert payload["failed_count"] == 1
    assert payload["partial_count"] == 1
    assert payload["pending_count"] == 0
    assert payload["up_to_date"] is False
    assert payload["by_platform"]["instagram"]["up_to_date"] is False
    assert payload["by_platform"]["twitter"]["up_to_date"] is True


def test_comments_coverage_twitter_recursive_filter_uses_reply_aliases(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_one(query: str, params=None):
        captured["query"] = query
        captured["params"] = params
        return {
            "posts_scanned": 1,
            "stale_posts_count": 0,
            "saved_comments": 0,
            "reported_comments": 0,
        }

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(
        social_repo,
        "_comment_lifecycle_supported",
        lambda table: table == "twitter_tweets",
    )

    social_repo._comments_coverage_for_platform(
        "season-1",
        platform="twitter",
        start_dt=datetime(2026, 2, 1, tzinfo=UTC),
        end_dt=datetime(2026, 2, 2, tzinfo=UTC),
        source_scope="bravo",
        target_accounts_by_platform={"twitter": {"bravotv"}},
    )

    normalized = " ".join(str(captured.get("query") or "").split())
    assert "coalesce(t.replies_count, 0) + coalesce(t.quotes, 0)" in normalized
    assert (
        "where r.season_id = %s and r.is_reply = true and r.is_missing = false and r.reply_to_tweet_id in"
    ) in normalized
    assert "where child.season_id = %s and child.is_reply = true and child.is_missing = false" in normalized
    assert (
        "from social.twitter_tweets q where q.season_id = %s and q.is_quote = true and q.is_missing = false"
        in normalized
    )
    assert "and t.is_missing = false and r.reply_to_tweet_id in" not in normalized


def test_weekly_daily_activity_indexes_by_calendar_day_not_elapsed_hours(monkeypatch) -> None:
    season_id = "season-analytics-calendar-day-index"
    context = SeasonContext(
        season_id=season_id,
        show_id="show-analytics-calendar-day-index",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )
    week_start = datetime(2025, 9, 30, 20, 0, tzinfo=ZoneInfo("America/New_York"))
    week_windows = [
        WeekWindow(week_index=3, start_local=week_start, end_local=week_start + timedelta(days=7)),
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
        if platform != "youtube":
            return []
        # Oct 1 local calendar day (should map to day_index=1, not 0).
        return [
            {
                "platform": "youtube",
                "kind": "post",
                "source_id": "yt-calendar-day",
                "text": "Episode clip",
                "engagement": 100,
                "ts": datetime(2025, 10, 1, 12, 0, tzinfo=ZoneInfo("America/New_York")),
                "url": "https://example.com/yt-calendar-day",
                "author": "bravo",
            }
        ]

    monkeypatch.setattr(social_repo, "get_season_context", lambda _: context)
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (week_windows, week_start),
    )
    monkeypatch.setattr(
        social_repo,
        "_build_sentiment_context",
        lambda _ctx: SentimentAnalyzerContext(
            cast_terms=set(),
            cast_phrases=set(),
            episode_terms=set(),
            episode_summary="",
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_target_accounts_by_platform",
        lambda *_args, **_kwargs: {
            "instagram": {"bravotv"},
            "youtube": {"bravo"},
            "tiktok": {"bravotv"},
            "twitter": {"bravotv"},
        },
    )
    monkeypatch.setattr(social_repo, "_rows_for_platform", _fake_rows_for_platform)
    monkeypatch.setattr(social_repo, "list_jobs", lambda *_args, **_kwargs: [])

    payload = get_analytics(
        season_id,
        platforms=["youtube"],
        timezone="America/New_York",
        week=None,
        source_scope="bravo",
        include_rows=False,
    )

    week_three = next(item for item in payload["weekly_daily_activity"] if item["week_index"] == 3)
    assert len(week_three["days"]) == 7
    assert week_three["days"][0]["date_local"] == "2025-09-30"
    assert week_three["days"][1]["date_local"] == "2025-10-01"
    assert week_three["days"][0]["total_posts"] == 0
    assert week_three["days"][1]["total_posts"] == 1
    assert week_three["days"][1]["posts"]["youtube"] == 1


def test_week_detail_instagram_includes_thumbnail_url(monkeypatch) -> None:
    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        if "from social.instagram_posts p" in sql:
            return [
                {
                    "id": "post-1",
                    "source_id": "abc123",
                    "author": "bravotv",
                    "text": "caption #RHOSLC @bravotv",
                    "likes": 10,
                    "comments_count": 2,
                    "views": 30,
                    "media_type": "image",
                    "media_urls": ["https://example.com/ig.jpg"],
                    "hosted_media_urls": ["https://cdn.example/ig-hosted.jpg"],
                    "thumbnail_url": "https://example.com/ig-thumb.jpg",
                    "post_format": "post",
                    "profile_tags": ["tagged_user"],
                    "collaborators": ["collab_user"],
                    "hashtags": ["RHOSLC"],
                    "mentions": ["@bravotv"],
                    "duration_seconds": 12,
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
    post = payload["posts"][0]
    assert post["thumbnail_url"] == "https://example.com/ig-thumb.jpg"
    assert post["media_urls"] == ["https://cdn.example/ig-hosted.jpg"]
    assert post["post_format"] == "post"
    assert post["profile_tags"] == ["@tagged_user"]
    assert post["collaborators"] == ["@collab_user"]
    assert post["hashtags"] == ["RHOSLC"]
    assert post["mentions"] == ["@bravotv"]
    assert post["duration_seconds"] == 12
    assert post["source_media_urls"] == []
    assert post["hosted_media_urls"] == ["https://cdn.example/ig-hosted.jpg"]
    assert post["source_thumbnail_url"] is None
    assert post["hosted_thumbnail_url"] is None
    assert post["cover_source"] is None
    assert post["cover_source_confidence"] is None


def test_instagram_cover_source_detects_custom_cover_hint() -> None:
    cover_source, confidence = social_repo._instagram_cover_source_from_post_row(
        {
            "media_type": "video",
            "post_format": "reel",
            "thumbnail_url": "https://cdninstagram.com/thumb.jpg",
            "source_media_urls": ["https://cdninstagram.com/video.mp4"],
            "raw_data": {"is_custom_cover": True},
        }
    )
    assert cover_source == "custom_cover"
    assert confidence == "high"


def test_instagram_cover_source_defaults_to_still_frame_for_video_posts() -> None:
    cover_source, confidence = social_repo._instagram_cover_source_from_post_row(
        {
            "media_type": "video",
            "post_format": "reel",
            "thumbnail_url": "https://cdninstagram.com/thumb.jpg",
            "source_media_urls": ["https://cdninstagram.com/video.mp4"],
            "raw_data": {},
        }
    )
    assert cover_source == "still_frame_or_default"
    assert confidence == "low"


def test_rows_for_platform_instagram_uses_schema_safe_thumbnail_expr(monkeypatch) -> None:
    captured_sql: dict[str, str] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured_sql["sql"] = sql
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    rows = social_repo._rows_for_platform(
        "season-1",
        platform="instagram",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 2, tzinfo=UTC),
        source_scope="community",
        target_accounts_by_platform={},
    )

    assert rows == []
    sql = captured_sql.get("sql", "")
    assert "p.hosted_thumbnail_url" not in sql
    assert "to_jsonb(p) ->> 'hosted_thumbnail_url'" in sql
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in sql


def test_platform_thumbnail_expr_prefers_hosted_media_fallback() -> None:
    instagram_expr = social_repo._instagram_posts_thumbnail_expr("p")
    tiktok_expr = social_repo._platform_thumbnail_expr("p", "tiktok")
    youtube_expr = social_repo._platform_thumbnail_expr("p", "youtube")
    twitter_expr = social_repo._platform_thumbnail_expr("p", "twitter")

    assert "to_jsonb(p) ->> 'hosted_thumbnail_url'" in instagram_expr
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in instagram_expr

    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in tiktok_expr
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in youtube_expr
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in twitter_expr
    assert "p.media_urls ->> 0" in twitter_expr


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


def test_get_post_comments_instagram_includes_metadata_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda sql, params: {
            "id": "ig-db-1",
            "source_id": "abc123",
            "author": "bravotv",
            "text": "caption #RHOSLC",
            "likes": 50,
            "comments_count": 10,
            "views": 700,
            "media_type": "video",
            "source_media_urls": ["https://instagram.example/reel.mp4"],
            "hosted_media_urls": ["https://cdn.example/reel.mp4"],
            "source_thumbnail_url": "https://instagram.example/thumb.jpg",
            "hosted_thumbnail_url": "https://cdn.example/thumb.jpg",
            "thumbnail_url": "https://cdn.example/ig-thumb.jpg",
            "post_format": "reel",
            "profile_tags": ["tagged_user"],
            "collaborators": ["collab_a"],
            "hashtags": ["RHOSLC"],
            "mentions": ["@bravotv"],
            "duration_seconds": 21,
            "raw_data": {"custom_cover_url": "https://instagram.example/custom-cover.jpg"},
            "ts": datetime(2025, 1, 1, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda sql, params: [])

    payload = get_post_comments("season-1", platform="instagram", source_id="abc123")
    assert payload["thumbnail_url"] == "https://cdn.example/ig-thumb.jpg"
    assert payload["post_format"] == "reel"
    assert payload["profile_tags"] == ["@tagged_user"]
    assert payload["collaborators"] == ["@collab_a"]
    assert payload["hashtags"] == ["RHOSLC"]
    assert payload["mentions"] == ["@bravotv"]
    assert payload["duration_seconds"] == 21
    assert payload["source_media_urls"] == ["https://instagram.example/reel.mp4"]
    assert payload["hosted_media_urls"] == ["https://cdn.example/reel.mp4"]
    assert payload["source_thumbnail_url"] == "https://instagram.example/thumb.jpg"
    assert payload["hosted_thumbnail_url"] == "https://cdn.example/thumb.jpg"
    assert payload["cover_source"] == "custom_cover"
    assert payload["cover_source_confidence"] == "high"


def test_get_post_comments_tiktok_includes_comment_media_and_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "tt-db-1",
            "source_id": "6862153058223197445",
            "author": "bravotv",
            "text": "TikTok post",
            "likes": 75,
            "comments_count": 10,
            "shares": 5,
            "saves": 471,
            "views": 999,
            "thumbnail_url": "https://cdn.example/tiktok-thumb.jpg",
            "media_urls": ["https://source.example/tiktok.mp4"],
            "hosted_media_urls": ["https://cdn.example/tiktok.mp4"],
            "ts": datetime(2025, 1, 1, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda _sql, _params: [
            {
                "id": "tt-comment-db-1",
                "comment_id": "7399984975553086214",
                "parent_comment_id": None,
                "author": "rizqirxq",
                "user_id": "6904063862041396225",
                "nickname": "Riz",
                "text": "comment with media",
                "likes": 12,
                "is_reply": False,
                "reply_count": 0,
                "created_at": datetime(2025, 1, 2, tzinfo=UTC),
                "comment_language": "es",
                "is_author_liked": False,
                "aweme_id": "6862153058223197445",
                "parent_source_comment_id": "7399984975553086000",
                "user_url": "https://www.tiktok.com/@rizqirxq",
                "user_bio": "bio",
                "user_avatar_url": "https://cdn.example/avatar.jpg",
                "user_region": "CO",
                "user_language": "es",
                "media_urls": ["https://source.example/comment-media.jpeg"],
                "hosted_media_urls": ["https://cdn.example/comment-media.jpeg"],
                "media_mirror_status": "mirrored",
            }
        ],
    )

    payload = get_post_comments("season-1", platform="tiktok", source_id="6862153058223197445")
    assert payload["media_urls"] == ["https://cdn.example/tiktok.mp4"]
    assert payload["stats"]["engagement"] == 1089
    assert payload["stats"]["saves"] == 471
    assert payload["total_comments_in_db"] == 1
    assert payload["comments"][0]["comment_language"] == "es"
    assert payload["comments"][0]["aweme_id"] == "6862153058223197445"
    assert payload["comments"][0]["media_urls"] == ["https://source.example/comment-media.jpeg"]
    assert payload["comments"][0]["hosted_media_urls"] == ["https://cdn.example/comment-media.jpeg"]
    assert payload["comments"][0]["media_mirror_status"] == "mirrored"
    assert payload["comments"][0]["user"]["username"] == "rizqirxq"
    assert payload["comments"][0]["user"]["url"] == "https://www.tiktok.com/@rizqirxq"


def test_week_detail_youtube_uses_effective_saved_comment_count(monkeypatch) -> None:
    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        if "from social.youtube_videos v" in sql:
            return [
                {
                    "id": "yt-db-1",
                    "source_id": "vid123",
                    "author": "Bravo",
                    "title": "RHOSLC trailer",
                    "text": "desc",
                    "views": 100,
                    "likes": 5,
                    "comments_count": 0,
                    "thumbnail_url": "https://example.com/thumb.jpg",
                    "duration_seconds": 90,
                    "ts": datetime(2025, 1, 1, tzinfo=UTC),
                }
            ]
        if "from social.youtube_comments c" in sql and "group by c.video_id" in sql:
            return [{"video_id": "yt-db-1", "cnt": 5}]
        if "from social.youtube_comments c" in sql and "cross join lateral" in sql:
            return []
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    payload = social_repo._week_detail_youtube(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 8, tzinfo=UTC),
        account_handles={"bravo"},
        max_comments=0,
    )

    post = payload["posts"][0]
    assert post["comments_count"] == 5
    assert post["engagement"] == 110
    assert payload["totals"]["total_comments"] == 5


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


def test_get_post_comments_youtube_uses_effective_saved_comment_count(monkeypatch) -> None:
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
            "comments_count": 0,
            "thumbnail_url": "https://example.com/yt-thumb.jpg",
            "ts": datetime(2025, 1, 1, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda sql, params: [
            {
                "id": "comment-1",
                "comment_id": "c1",
                "parent_comment_id": None,
                "author": "viewer",
                "text": "nice",
                "likes": 1,
                "is_reply": False,
                "reply_count": 0,
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            },
            {
                "id": "comment-2",
                "comment_id": "c2",
                "parent_comment_id": None,
                "author": "viewer",
                "text": "wow",
                "likes": 1,
                "is_reply": False,
                "reply_count": 0,
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            },
            {
                "id": "comment-3",
                "comment_id": "c3",
                "parent_comment_id": None,
                "author": "viewer",
                "text": "ok",
                "likes": 1,
                "is_reply": False,
                "reply_count": 0,
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            },
        ],
    )
    payload = get_post_comments("season-1", platform="youtube", source_id="vid123")
    assert payload["stats"]["comments_count"] == 3
    assert payload["stats"]["engagement"] == 108
    assert payload["total_comments_in_db"] == 3


def test_get_post_comments_twitter_returns_separate_quotes_payload(monkeypatch) -> None:
    def _fake_fetch_one(sql: str, _params: list[object]) -> dict[str, object] | None:
        if "from social.twitter_tweets t" in sql and "t.is_reply = false" in sql:
            return {
                "source_id": "123",
                "author": "bravotv",
                "user_id": "42",
                "user_profile_url": "https://x.com/bravotv",
                "user_avatar_url": "https://pbs.twimg.com/profile_images/bravo.jpg",
                "display_name": "Bravo",
                "text": "Original post",
                "likes": 10,
                "retweets": 2,
                "replies_count": 1,
                "quotes": 5,
                "views": 100,
                "thumbnail_url": "https://img.test/root.jpg",
                "ts": datetime(2025, 1, 1, tzinfo=UTC),
            }
        return None

    def _fake_fetch_all(sql: str, _params: list[object]) -> list[dict[str, object]]:
        assert "{hosted_media_expr}" not in sql
        if "with recursive thread_replies as" in sql:
            return [
                {
                    "id": "reply-1",
                    "comment_id": "reply-1",
                    "parent_comment_id": "123",
                    "author": "viewer",
                    "display_name": "Viewer",
                    "user_id": "99",
                    "user_url": "https://x.com/viewer",
                    "user_avatar_url": "https://pbs.twimg.com/profile_images/viewer.jpg",
                    "text": "reply",
                    "likes": 3,
                    "is_reply": True,
                    "reply_count": 0,
                    "media_urls": ["https://pbs.twimg.com/media/reply.jpg"],
                    "hosted_media_urls": ["https://cdn.example/reply.jpg"],
                    "created_at": datetime(2025, 1, 1, 1, 0, tzinfo=UTC),
                }
            ]
        if "and t.is_quote = true" in sql:
            return [
                {
                    "comment_id": "quote-1",
                    "author": "viewer2",
                    "user_id": "100",
                    "user_url": "https://x.com/viewer2",
                    "user_avatar_url": "https://pbs.twimg.com/profile_images/viewer2.jpg",
                    "display_name": "Viewer Two",
                    "text": "quoted text",
                    "likes": 7,
                    "retweets": 1,
                    "reply_count": 0,
                    "quotes": 0,
                    "views": 20,
                    "media_urls": ["https://img.test/quote.jpg"],
                    "hosted_media_urls": ["https://cdn.example/quote.jpg"],
                    "thumbnail_url": "https://img.test/quote-thumb.jpg",
                    "created_at": datetime(2025, 1, 1, 2, 0, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = get_post_comments("season-1", platform="twitter", source_id="123")

    assert payload["total_comments_in_db"] == 1
    assert payload["total_quotes_in_db"] == 1
    assert len(payload["comments"]) == 1
    assert len(payload["quotes"]) == 1
    assert payload["user"]["avatar_url"] == "https://pbs.twimg.com/profile_images/bravo.jpg"
    assert payload["comments"][0]["user"]["avatar_url"] == "https://pbs.twimg.com/profile_images/viewer.jpg"
    assert payload["comments"][0]["hosted_media_urls"] == ["https://cdn.example/reply.jpg"]
    assert payload["quotes"][0]["comment_id"] == "quote-1"
    assert payload["quotes"][0]["is_reply"] is False
    assert payload["quotes"][0]["hosted_media_urls"] == ["https://cdn.example/quote.jpg"]
    assert payload["quotes"][0]["user"]["avatar_url"] == "https://pbs.twimg.com/profile_images/viewer2.jpg"


def test_week_detail_twitter_includes_total_quotes_in_db(monkeypatch) -> None:
    def _fake_fetch_all(sql: str, _params: list[object]) -> list[dict[str, object]]:
        if "from social.twitter_tweets t" in sql and "t.is_reply = false" in sql:
            return [
                {
                    "source_id": "tweet-1",
                    "author": "bravotv",
                    "display_name": "Bravo",
                    "text": "Original tweet",
                    "likes": 12,
                    "retweets": 3,
                    "replies_count": 1,
                    "quotes": 4,
                    "views": 200,
                    "hashtags": ["RHOSLC"],
                    "mentions": ["bravotv"],
                    "media_urls": ["https://img.test/root.jpg"],
                    "thumbnail_url": "https://img.test/root-thumb.jpg",
                    "ts": datetime(2025, 1, 1, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_count_stored_quotes", lambda tweet_ids: {"tweet-1": 6})

    payload = social_repo._week_detail_twitter(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 2, tzinfo=UTC),
        account_handles={"bravotv"},
        max_comments=25,
    )

    assert payload["posts"][0]["total_quotes_in_db"] == 6
    assert payload["totals"]["total_comments"] == 0


def test_sync_youtube_video_comment_counts_dedupes_ids_and_uses_greatest(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext("cursor"))

    def _fake_fetch_all_with_cursor(cur, sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "video-1"}]

    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)

    updated = social_repo._sync_youtube_video_comment_counts(["video-2", "", "video-1", "video-1"])
    assert updated == 1
    assert captured["params"] == ["video-1", "video-2", "video-1", "video-2"]
    sql = str(captured["sql"]).lower()
    assert "update social.youtube_videos" in sql
    assert "greatest(" in sql
    assert "from social.youtube_comments c" in sql


def test_expected_comment_count_for_platform_youtube_uses_snapshot_fallback() -> None:
    snapshot = social_repo.CommentLifecycleSnapshot(
        active_count=12,
        total_count=12,
        latest_comment_created_at=datetime(2025, 1, 2, tzinfo=UTC),
        last_seen_at=datetime(2025, 1, 2, tzinfo=UTC),
        last_checked_at=datetime(2025, 1, 2, tzinfo=UTC),
    )
    assert (
        social_repo._expected_comment_count_for_platform(
            "youtube",
            {"comments_count": 0},
            snapshot=snapshot,
        )
        == 12
    )
    assert (
        social_repo._expected_comment_count_for_platform(
            "youtube",
            {"comments_count": 3},
            snapshot=snapshot,
        )
        == 3
    )


def test_expected_comment_count_for_platform_twitter_includes_quotes() -> None:
    assert (
        social_repo._expected_comment_count_for_platform(
            "twitter",
            {"replies_count": 89, "quotes": 14},
        )
        == 103
    )
    assert (
        social_repo._expected_comment_count_for_platform(
            "twitter",
            {"replies_count": 89},
        )
        == 89
    )


def test_apply_twitter_public_summary_uses_non_empty_fields(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_platform_posts_has_column", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext("cursor"))

    def _fake_fetch_one_with_cursor(cur, sql: str, params: list[object]):
        del cur
        captured["sql"] = sql
        captured["params"] = params
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)

    applied = social_repo._apply_twitter_public_summary(
        tweet_id="1962923513301639212",
        summary={
            "username": "BravoTV",
            "display_name": "Bravo",
            "user_id": "15169907",
            "user_profile_url": "",
            "user_avatar_url": "https://pbs.twimg.com/profile_images/bravo.jpg",
            "likes": 135,
            "replies": 89,
            "media_urls": ["https://video.twimg.com/amplify_video/test.mp4"],
        },
    )

    assert applied is True
    sql = str(captured["sql"]).lower()
    assert "update social.twitter_tweets set" in sql
    assert "replies_count = greatest" in sql
    assert "likes = greatest" in sql
    assert "user_profile_url = coalesce" in sql
    assert "where tweet_id = %s and is_reply = false" in sql
    params = captured["params"]
    assert params[-1] == "1962923513301639212"
    assert any("https://x.com/BravoTV" == p for p in params)


def test_merge_twitter_media_urls_prefers_video_from_public_summary() -> None:
    merged = social_repo._merge_twitter_media_urls(  # noqa: SLF001
        ["https://pbs.twimg.com/tweet_video_thumb/G-m2mzhbQAQ-Kho.jpg"],
        [
            "https://video.twimg.com/tweet_video/G-m2mzhbQAQ-Kho.mp4",
            "https://pbs.twimg.com/tweet_video_thumb/G-m2mzhbQAQ-Kho.jpg",
        ],
    )
    assert merged[0] == "https://video.twimg.com/tweet_video/G-m2mzhbQAQ-Kho.mp4"
    assert "https://pbs.twimg.com/tweet_video_thumb/G-m2mzhbQAQ-Kho.jpg" in merged


def test_ingest_youtube_comments_stage_syncs_comment_counts_and_uses_snapshot_expected(monkeypatch) -> None:
    expected_counts: list[int] = []
    synced_video_ids: list[str] = []

    class _FakeYouTubeScraper:
        def fetch_comments(self, *args, **kwargs):
            return []

    context = SeasonContext(
        season_id="season-youtube-comments",
        show_id="show-youtube-comments",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"youtube"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=0,
        max_comments_per_post=100,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="comments_only",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 8, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.youtube.YouTubeScraper", _FakeYouTubeScraper)
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda *args, **kwargs: [
            {
                "id": "youtube-db-1",
                "video_id": "vid-1",
                "comments_count": 0,
                "published_at": datetime(2025, 1, 2, tzinfo=UTC),
            }
        ],
    )
    monkeypatch.setattr(
        social_repo,
        "_load_comment_lifecycle_snapshots",
        lambda *args, **kwargs: {
            "youtube-db-1": social_repo.CommentLifecycleSnapshot(
                active_count=9,
                total_count=9,
                latest_comment_created_at=datetime(2025, 1, 2, tzinfo=UTC),
                last_seen_at=datetime(2025, 1, 2, tzinfo=UTC),
                last_checked_at=datetime(2025, 1, 2, tzinfo=UTC),
            )
        },
    )

    def _fake_decide_comment_refresh(**kwargs):
        expected_counts.append(int(kwargs["expected_count"]))
        return social_repo.CommentRefreshDecision(should_refresh=True, reason="count_gap")

    monkeypatch.setattr(social_repo, "_decide_comment_refresh", _fake_decide_comment_refresh)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_cleanup_mismatched_youtube_rows",
        lambda **kwargs: {"scanned": 0, "videos_deleted": 0, "comments_deleted": 0},
    )
    monkeypatch.setattr(social_repo, "_mark_missing_comments_for_anchor", lambda **kwargs: 0)
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_sync_youtube_video_comment_counts",
        lambda ids, conn=None: synced_video_ids.extend(ids) or 1,
    )

    _, _, meta = social_repo._ingest_youtube(
        context,
        run_id="run-youtube-comments",
        account="bravo",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC"],
        opts=opts,
        job_id="job-youtube-comments",
        stage="comments",
    )

    assert expected_counts == [9]
    assert synced_video_ids == ["youtube-db-1"]
    assert meta["youtube_comment_count_synced"] == 1
    assert meta["youtube_comment_count_sync_targets"] == 1


def test_refresh_post_comments_youtube_reports_comment_count_sync(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2025, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda sql, params: {"id": "db-yt-1", "account": "bravo"},
    )

    class _FakeYouTubeScraper:
        def fetch_comments(self, *args, **kwargs):
            return []

    monkeypatch.setattr("trr_backend.socials.youtube.YouTubeScraper", _FakeYouTubeScraper)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_sync_youtube_video_comment_counts",
        lambda ids, conn=None: 1,
    )
    monkeypatch.setattr(
        social_repo,
        "_count_stored_comments",
        lambda post_ids, platform: {"db-yt-1": 0},
    )

    payload = social_repo.refresh_post_comments(
        "season-1",
        platform="youtube",
        source_id="vid123",
        max_comments_per_post=0,
    )
    assert payload["youtube_comment_count_synced"] == 1
    assert payload["is_complete"] is False
    assert payload["incomplete_reason"] == "fetch_disabled"
    assert payload["comment_fail_reasons"] == ["fetch_disabled"]


def test_refresh_post_comments_tiktok_returns_additive_completeness_fields(monkeypatch) -> None:
    marked_missing: list[str] = []
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2025, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {"id": "tt-db-1", "account": "bravotv"},
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_count_stored_comments", lambda *_args, **_kwargs: {"tt-db-1": 0})
    monkeypatch.setattr(
        social_repo,
        "_mark_missing_comments_for_anchor",
        lambda **kwargs: marked_missing.append(str(kwargs.get("anchor_id"))) or 0,
    )
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **kwargs: None)

    class _FakeTikTokScraper:
        comments_auth_failed = False
        last_comment_fetch_reason = ""
        _last_api_fail_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def fetch_comments(self, *args, **kwargs):
            del args, kwargs
            self.last_comment_fetch_reason = "api_status_fail"
            self._last_api_fail_reason = "api_status_fail"
            return []

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeTikTokScraper)

    payload = social_repo.refresh_post_comments(
        "season-1",
        platform="tiktok",
        source_id="777",
        max_comments_per_post=25,
        fetch_replies=True,
    )

    assert payload["fetch_failed"] is False
    assert payload["is_complete"] is False
    assert payload["incomplete_reason"] == "api_status_fail"
    assert payload["comment_fail_reasons"] == ["api_status_fail"]
    assert payload["comments_marked_missing"] == 0
    assert marked_missing == []


def test_refresh_post_comments_twitter_zero_limit_sets_fetch_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2025, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {"tweet_id": "123", "account": "bravotv"},
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_count_stored_replies", lambda *_args, **_kwargs: {"123": 2})
    monkeypatch.setattr(social_repo, "_count_stored_quotes", lambda *_args, **_kwargs: {"123": 0})
    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, "bearer"))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: {})

    class _FakeTwitterScraper:
        comments_auth_failed = False
        last_reply_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def fetch_tweet_replies(self, *args, **kwargs):
            raise AssertionError("fetch_tweet_replies should not be called when max_comments_per_post=0")

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)

    payload = social_repo.refresh_post_comments(
        "season-1",
        platform="twitter",
        source_id="123",
        max_comments_per_post=0,
        fetch_replies=True,
    )

    assert payload["comments_fetched"] == 0
    assert payload["fetch_failed"] is False
    assert payload["is_complete"] is False
    assert payload["incomplete_reason"] == "fetch_disabled"
    assert payload["comment_fail_reasons"] == ["fetch_disabled"]


def test_refresh_post_comments_twitter_infers_auth_failed_when_no_session_artifacts(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2025, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {"tweet_id": "123", "account": "bravotv", "replies_count": 4, "quotes": 7},
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_count_stored_replies", lambda *_args, **_kwargs: {"123": 0})
    monkeypatch.setattr(social_repo, "_count_stored_quotes", lambda *_args, **_kwargs: {"123": 0})
    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: None)

    class _FakeTwitterScraper:
        comments_auth_failed = False
        last_reply_fetch_reason = ""
        last_quote_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def fetch_tweet_replies(self, *args, **kwargs):
            del args, kwargs
            self.last_reply_fetch_reason = "http_404"
            return []

        def fetch_tweet_quotes(self, *args, **kwargs):
            del args, kwargs
            self.last_quote_fetch_reason = "http_404"
            return []

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)

    payload = social_repo.refresh_post_comments(
        "season-1",
        platform="twitter",
        source_id="123",
        max_comments_per_post=100,
        fetch_replies=True,
    )

    assert payload["auth_session_artifacts_present"] is False
    assert payload["comments_auth_failed"] is True
    assert payload["quote_fetch_reason"] == "http_404"
    assert payload["is_complete"] is False
    assert payload["incomplete_reason"] == "http_404"


def test_backfill_youtube_comment_counts_for_season_scopes_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda sql, params: [{"id": "yt-1"}, {"id": "yt-2"}],
    )
    monkeypatch.setattr(
        social_repo,
        "_sync_youtube_video_comment_counts",
        lambda ids, conn=None: 2 if ids == ["yt-1", "yt-2"] else 0,
    )

    payload = social_repo.backfill_youtube_comment_counts_for_season(
        "season-1",
        source_account="bravo",
        date_start=datetime(2025, 10, 1, tzinfo=UTC),
        date_end=datetime(2025, 10, 7, tzinfo=UTC),
    )
    assert payload["season_id"] == "season-1"
    assert payload["source_account"] == "bravo"
    assert payload["videos_scanned"] == 2
    assert payload["youtube_comment_count_backfilled"] == 2


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
        run_id="123e4567-e89b-12d3-a456-426614174000",
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
    assert "and id = %s::uuid" in sql
    assert "order by created_at desc limit %s" in sql
    assert params == ["season-1", "completed", "bravo", "123e4567-e89b-12d3-a456-426614174000", 25]


def test_list_jobs_uses_candidate_cte_for_unscoped_queries(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "id": "job-1",
                "run_id": "run-1",
                "platform": "youtube",
                "job_type": "comments",
                "status": "running",
                "items_found": 0,
                "error_message": "network timeout",
                "metadata": {},
                "last_error_code": "network",
                "last_error_class": "TimeoutError",
            }
        ]

    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    rows = social_repo.list_jobs("season-1", limit=100, status="running", platform="youtube")

    assert rows and rows[0]["job_error_code"] == "NETWORK"
    sql = " ".join(str(captured["sql"]).lower().split())
    params = captured["params"]
    assert "with candidate_jobs as (" in sql
    assert "from social.scrape_jobs where season_id = %s and status = %s and platform = %s" in sql
    assert "join candidate_jobs c on c.id = j.id" in sql
    assert "order by j.created_at desc" in sql
    assert params == ["season-1", "running", "youtube", 100]


def test_list_jobs_uses_direct_query_for_run_scoped_queries(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "id": "job-1",
                "metadata": {},
                "last_error_code": None,
                "error_message": None,
                "last_error_class": None,
            }
        ]

    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": False})
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    social_repo.list_jobs(
        "season-1",
        limit=50,
        run_id="123e4567-e89b-12d3-a456-426614174000",
        status="queued",
        platform="instagram",
    )

    sql = " ".join(str(captured["sql"]).lower().split())
    params = captured["params"]
    assert "with candidate_jobs as (" not in sql
    assert "from social.scrape_jobs j where season_id = %s and run_id = %s and status = %s and platform = %s" in sql
    assert "order by j.created_at desc limit %s" in sql
    assert params == ["season-1", "123e4567-e89b-12d3-a456-426614174000", "queued", "instagram", 50]


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


def test_decide_comment_refresh_missing_only_matrix() -> None:
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
        post_published_at=now - timedelta(days=3),
        refresh_policy="missing_only",
        now=now,
    )
    assert decision.should_refresh is True
    assert decision.reason == "count_gap"

    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=10,
        snapshot=fresh_snapshot,
        post_published_at=now - timedelta(days=30),
        refresh_policy="missing_only",
        now=now,
    )
    assert decision.should_refresh is False
    assert decision.reason == "up_to_date"

    decision = social_repo._decide_comment_refresh(
        sync_strategy="incremental",
        expected_count=9,
        snapshot=fresh_snapshot,
        post_published_at=now - timedelta(days=3),
        refresh_policy="missing_only",
        now=now,
    )
    assert decision.should_refresh is False
    assert decision.reason == "count_drop"


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
    assert not social_repo._is_comment_fetch_complete(
        fetch_failed=False,
        fail_reason=None,
        auth_failed=False,
        fetched_count=0,
        max_comments_per_post=1000,
        expected_count=42,
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
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: False)

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


def test_upsert_instagram_comment_tree_without_run_id_preserves_last_seen_run(monkeypatch) -> None:
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
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "instagram_comments")
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: False)

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
        run_id=None,
        account="bravotv",
        post_id="post-1",
        comment=_Comment(),
    )

    assert written == 1
    assert "last_seen_run_id" not in captured_payload
    assert captured_payload["is_missing"] is False


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
    assert all(config["max_posts_per_target"] == 0 for config in created_job_configs)

    captured_run_configs.clear()
    created_job_configs.clear()

    comments_only_missing_payload = social_repo.ingest_season(
        "season-1",
        platforms=["instagram"],
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=500,
        max_comments_per_post=300,
        max_replies_per_post=200,
        fetch_replies=True,
        ingest_mode="comments_only",
        comment_refresh_policy="missing_only",
        comment_anchor_source_ids={"instagram": []},
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 10, tzinfo=UTC),
        initiated_by="admin@test",
    )
    assert comments_only_missing_payload["stages"] == ["comments"]
    assert comments_only_missing_payload["queued_or_started_jobs"] == 0
    assert created_job_configs == []


def test_assert_worker_available_when_queue_enabled_raises_without_healthy_worker(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(
        social_repo,
        "get_worker_health",
        lambda: {
            "healthy": False,
            "healthy_workers": 0,
            "active_workers": 0,
            "total_workers": 0,
            "workers": [],
            "reason": "no_healthy_workers",
        },
    )

    with pytest.raises(social_repo.SocialWorkerUnavailableError) as exc_info:
        social_repo.assert_worker_available_when_queue_enabled()

    assert exc_info.value.worker_health["healthy"] is False
    assert exc_info.value.worker_health["reason"] == "no_healthy_workers"


def test_assert_worker_available_when_queue_enabled_returns_health_when_healthy(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    healthy_payload = {
        "healthy": True,
        "healthy_workers": 1,
        "active_workers": 1,
        "total_workers": 1,
        "workers": [{"worker_id": "worker-1"}],
        "reason": None,
    }
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: healthy_payload)

    assert social_repo.assert_worker_available_when_queue_enabled() == healthy_payload


def test_emit_job_progress_flushes_on_delta_and_time(monkeypatch) -> None:
    updates: list[dict[str, object]] = []
    base = datetime(2026, 2, 19, 12, 0, tzinfo=UTC)
    timestamps = iter(
        [
            base,
            base + timedelta(seconds=1),
            base + timedelta(seconds=2),
            base + timedelta(seconds=6),
        ]
    )

    monkeypatch.setattr(social_repo, "_now_utc", lambda: next(timestamps))
    monkeypatch.setattr(
        social_repo,
        "_update_job_progress",
        lambda job_id, *, items_found, metadata=None: updates.append(
            {"job_id": job_id, "items_found": items_found, "metadata": metadata or {}}
        ),
    )

    state = social_repo._new_job_progress_state()
    social_repo._emit_job_progress(
        job_id="job-1",
        stage="posts",
        platform="twitter",
        account="bravotv",
        scraped_posts=0,
        scraped_comments=0,
        posts_upserted=0,
        comments_upserted=0,
        activity={"phase": "stage_start"},
        progress_state=state,
        force=True,
    )
    social_repo._emit_job_progress(
        job_id="job-1",
        stage="posts",
        platform="twitter",
        account="bravotv",
        scraped_posts=2,
        scraped_comments=0,
        posts_upserted=0,
        comments_upserted=0,
        activity={"phase": "scrape", "pages_scanned": 1, "posts_checked": 2},
        progress_state=state,
    )
    social_repo._emit_job_progress(
        job_id="job-1",
        stage="posts",
        platform="twitter",
        account="bravotv",
        scraped_posts=7,
        scraped_comments=0,
        posts_upserted=1,
        comments_upserted=0,
        activity={"phase": "scrape", "pages_scanned": 2, "posts_checked": 7, "matched_posts": 1},
        progress_state=state,
    )
    social_repo._emit_job_progress(
        job_id="job-1",
        stage="posts",
        platform="twitter",
        account="bravotv",
        scraped_posts=8,
        scraped_comments=0,
        posts_upserted=1,
        comments_upserted=0,
        activity={"phase": "scrape", "pages_scanned": 3, "posts_checked": 8, "matched_posts": 1},
        progress_state=state,
    )

    assert len(updates) == 3
    assert updates[0]["items_found"] == 0
    assert updates[1]["items_found"] == 7
    assert updates[2]["items_found"] == 8
    assert updates[2]["metadata"]["activity"]["pages_scanned"] == 3


def test_recover_stale_running_jobs_updates_worker_state_and_run_summaries(monkeypatch) -> None:
    cleared: list[str] = []
    finalized: list[str] = []
    captured_sql: dict[str, object] = {}

    def _count_unescaped_placeholders(sql: str) -> int:
        count = 0
        i = 0
        while i < len(sql) - 1:
            if sql[i] == "%":
                if sql[i + 1] == "%":
                    i += 2
                    continue
                if sql[i + 1] == "s":
                    count += 1
                    i += 2
                    continue
            i += 1
        return count

    def _fake_fetch_all(sql: str, params: list[object]):
        captured_sql["sql"] = sql
        captured_sql["params"] = list(params)
        return [
            {
                "id": "job-1",
                "run_id": "run-1",
                "platform": "instagram",
                "status": "retrying",
                "attempt_count": 1,
                "max_attempts": 3,
            },
            {
                "id": "job-2",
                "run_id": "run-2",
                "platform": "twitter",
                "status": "failed",
                "attempt_count": 3,
                "max_attempts": 3,
            },
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(
        social_repo,
        "_clear_worker_heartbeat_for_job",
        lambda *, job_id, status="idle", metadata=None: cleared.append(job_id),
    )
    monkeypatch.setattr(social_repo, "_finalize_run_status", lambda run_id: finalized.append(run_id) or {})

    recovered = social_repo.recover_stale_running_jobs(run_id=None, stage=None, stale_after_seconds=300)

    assert [row["id"] for row in recovered] == ["job-1", "job-2"]
    assert cleared == ["job-1", "job-2"]
    assert finalized == ["run-1", "run-2"]
    sql_text = str(captured_sql.get("sql") or "")
    params = captured_sql.get("params")
    assert isinstance(params, list)
    assert _count_unescaped_placeholders(sql_text) == len(params)


def test_ingest_twitter_scrape_callback_updates_progress_before_upserts(monkeypatch) -> None:
    updates: list[dict[str, object]] = []

    class _FakeTwitterScraper:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def scrape(self, config, progress_cb=None):
            if progress_cb:
                progress_cb(
                    {
                        "phase": "scrape_graphql_page",
                        "pages_scanned": 2,
                        "posts_checked": 12,
                        "matched_posts": 4,
                    }
                )
            return []

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
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

    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: {})
    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_update_job_progress",
        lambda job_id, *, items_found, metadata=None: updates.append(
            {"job_id": job_id, "items_found": items_found, "metadata": metadata or {}}
        ),
    )

    posts, comments, _meta = social_repo._ingest_twitter(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["rhoslc"],
        keywords=["rhoslc"],
        opts=opts,
        job_id="job-1",
        include_reply_records=False,
        hydrate_audience_replies=False,
        stage="posts",
    )

    assert posts >= 12
    assert comments == 0
    assert any(
        isinstance(entry.get("metadata"), dict)
        and isinstance((entry.get("metadata") or {}).get("activity"), dict)
        and ((entry.get("metadata") or {}).get("activity") or {}).get("pages_scanned") == 2
        for entry in updates
    )


def test_ingest_instagram_posts_stage_skips_up_to_date_posts_in_incremental_mode(monkeypatch) -> None:
    upsert_calls: list[str] = []

    class _FakePost:
        shortcode = "abc123"
        caption = "rhoslc"
        comments = 10
        likes = 1
        video_views = 1
        taken_at = datetime(2026, 2, 15, tzinfo=UTC)
        post_type = "image"
        media_urls: list[str] = []
        thumbnail_url = ""
        pk = "pk-1"
        username = "bravotv"

        def to_dict(self) -> dict[str, object]:
            return {"shortcode": self.shortcode}

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}

        def __init__(self, *args, **kwargs) -> None:
            pass

        def scrape(self, config, progress_cb=None):
            if progress_cb:
                progress_cb(
                    {
                        "phase": "scrape_graphql_page",
                        "pages_scanned": 1,
                        "posts_checked": 1,
                        "matched_posts": 1,
                    }
                )
            return [_FakePost()]

        def fetch_comments(self, *args, **kwargs):
            raise AssertionError("fetch_comments should not run for up-to-date incremental posts")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"instagram"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=100,
        max_comments_per_post=100,
        max_replies_per_post=100,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))

    class _Cursor:
        def execute(self, *_args, **_kwargs) -> None:
            return None

    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda platform, *_args, **_kwargs: (
            [
                {
                    "id": "post-db-1",
                    "shortcode": "abc123",
                    "posted_at": datetime(2026, 2, 15, tzinfo=UTC),
                    "comments_count": 10,
                }
            ]
            if platform == "instagram"
            else []
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_load_comment_lifecycle_snapshots",
        lambda *_args, **_kwargs: {
            "post-db-1": social_repo.CommentLifecycleSnapshot(
                active_count=10,
                total_count=10,
                latest_comment_created_at=datetime(2026, 2, 15, tzinfo=UTC),
                last_seen_at=datetime(2026, 2, 15, tzinfo=UTC),
                last_checked_at=datetime(2026, 2, 19, 12, 0, tzinfo=UTC),
            )
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_decide_comment_refresh",
        lambda **_kwargs: social_repo.CommentRefreshDecision(should_refresh=False, reason="up_to_date"),
    )
    monkeypatch.setattr(
        social_repo,
        "_upsert_instagram_post",
        lambda *_args, **_kwargs: upsert_calls.append("called") or {"id": "new-post"},
    )

    posts, comments, meta = social_repo._ingest_instagram(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["rhoslc"],
        keywords=["rhoslc"],
        opts=opts,
        job_id="job-1",
        stage="posts",
    )

    assert posts == 1
    assert comments == 0
    assert upsert_calls == []
    assert (meta.get("comment_refresh_decisions") or {}).get("up_to_date") == 1
    assert (meta.get("persist_counters") or {}).get("posts_upserted") == 0


def test_ingest_instagram_comments_stage_skips_mark_missing_on_logical_api_failure(monkeypatch) -> None:
    mark_missing_calls: list[str] = []

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}
        last_comment_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            pass

        def fetch_comments(self, *args, **kwargs):
            self.last_comment_fetch_reason = "api_status_fail"
            return []

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"instagram"},
        source_scope="bravo",
        sync_strategy="full_refresh",
        max_posts_per_target=0,
        max_comments_per_post=25,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="comments_only",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda platform, *_args, **_kwargs: (
            [
                {
                    "id": "post-db-1",
                    "shortcode": "abc123",
                    "posted_at": datetime(2026, 2, 15, tzinfo=UTC),
                    "comments_count": 10,
                }
            ]
            if platform == "instagram"
            else []
        ),
    )
    monkeypatch.setattr(social_repo, "_load_comment_lifecycle_snapshots", lambda *args, **kwargs: {})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo, "_create_savepoint", lambda conn, name: None)
    monkeypatch.setattr(social_repo, "_release_savepoint", lambda conn, name: None)
    monkeypatch.setattr(social_repo, "_rollback_to_savepoint", lambda conn, name: None)
    monkeypatch.setattr(
        social_repo,
        "_mark_missing_comments_for_anchor",
        lambda **kwargs: mark_missing_calls.append(str(kwargs.get("anchor_id"))) or 1,
    )
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **kwargs: None)

    posts, comments, meta = social_repo._ingest_instagram(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["rhoslc"],
        keywords=["rhoslc"],
        opts=opts,
        job_id="job-1",
        stage="comments",
    )

    assert posts == 1
    assert comments == 0
    assert mark_missing_calls == []
    assert meta["comments_mark_missing_skipped"] == 1
    assert (meta["comment_fetch_failures_by_reason"] or {}).get("api_status_fail") == 1


def test_ingest_instagram_comments_stage_targets_only_requested_source_ids(monkeypatch) -> None:
    requested_source_ids: list[set[str] | None] = []
    fetched_shortcodes: list[str] = []

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}
        last_comment_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            pass

        def fetch_comments(self, shortcode: str, **_kwargs):
            fetched_shortcodes.append(shortcode)
            return []

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"instagram"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=0,
        max_comments_per_post=25,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="comments_only",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
        comment_refresh_policy="missing_only",
        comment_anchor_source_ids={"instagram": {"only-me"}},
    )

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo, "_load_comment_lifecycle_snapshots", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        social_repo,
        "_decide_comment_refresh",
        lambda **_kwargs: social_repo.CommentRefreshDecision(should_refresh=True, reason="count_gap"),
    )
    monkeypatch.setattr(social_repo, "_mark_missing_comments_for_anchor", lambda **kwargs: 0)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))

    def _fake_load_existing_posts(
        platform: str,
        _context: SeasonContext,
        _account: str,
        _date_start: datetime | None,
        _date_end: datetime | None,
        source_ids: set[str] | None = None,
    ) -> list[dict[str, object]]:
        requested_source_ids.append(source_ids)
        rows = [
            {
                "id": "post-db-1",
                "shortcode": "only-me",
                "posted_at": datetime(2026, 2, 15, tzinfo=UTC),
                "comments_count": 10,
            },
            {
                "id": "post-db-2",
                "shortcode": "not-targeted",
                "posted_at": datetime(2026, 2, 15, tzinfo=UTC),
                "comments_count": 5,
            },
        ]
        if platform != "instagram":
            return []
        if source_ids is None:
            return rows
        return [row for row in rows if str(row.get("shortcode") or "") in source_ids]

    monkeypatch.setattr(social_repo, "_load_existing_posts", _fake_load_existing_posts)

    posts, comments, _meta = social_repo._ingest_instagram(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["rhoslc"],
        keywords=["rhoslc"],
        opts=opts,
        job_id="job-1",
        stage="comments",
    )

    assert posts == 1
    assert comments == 0
    assert requested_source_ids == [{"only-me"}]
    assert fetched_shortcodes == ["only-me"]


def test_ingest_instagram_posts_stage_skips_db_write_on_comment_fetch_exception(monkeypatch) -> None:
    class _FakePost:
        shortcode = "abc123"
        caption = "rhoslc"
        comments = 10
        likes = 1
        video_views = 1
        taken_at = datetime(2026, 2, 15, tzinfo=UTC)
        post_type = "image"
        media_urls: list[str] = []
        thumbnail_url = ""
        pk = "pk-1"
        username = "bravotv"

        def to_dict(self) -> dict[str, object]:
            return {"shortcode": self.shortcode}

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}

        def __init__(self, *args, **kwargs) -> None:
            pass

        def scrape(self, config, progress_cb=None):
            if progress_cb:
                progress_cb(
                    {
                        "phase": "scrape_graphql_page",
                        "pages_scanned": 1,
                        "posts_checked": 1,
                        "matched_posts": 1,
                    }
                )
            return [_FakePost()]

        def fetch_comments(self, *args, **kwargs):
            raise RuntimeError("boom")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"instagram"},
        source_scope="bravo",
        sync_strategy="full_refresh",
        max_posts_per_target=100,
        max_comments_per_post=100,
        max_replies_per_post=100,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext("conn"))
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo, "_resolve_week_windows", lambda *_args, **_kwargs: ([], None))
    monkeypatch.setattr(social_repo, "_enrich_instagram_post_from_permalink", lambda **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_mirror_instagram_media_to_s3",
        lambda *_args, **_kwargs: (None, [], "pending", None),
    )
    monkeypatch.setattr(
        social_repo,
        "_upsert_instagram_post",
        lambda *_args, **_kwargs: {"id": "new-post"},
    )
    posts, comments, meta = social_repo._ingest_instagram(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["rhoslc"],
        keywords=["rhoslc"],
        opts=opts,
        job_id="job-1",
        stage="posts",
    )

    assert posts == 1
    assert comments == 0
    assert meta.get("rolled_back_posts", 0) == 0
    assert meta.get("rolled_back_comments", 0) == 0
    assert meta["incomplete_comment_fetches"] == 1
    assert (meta["persist_counters"] or {}).get("posts_upserted") == 0


def test_refresh_post_comments_instagram_returns_additive_completeness_fields(monkeypatch) -> None:
    marked_missing: list[str] = []

    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2025, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda sql, params: {"id": "ig-db-1", "account": "bravotv"},
    )

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_comment_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            pass

        def fetch_comments(self, *args, **kwargs):
            self.last_comment_fetch_reason = "api_status_fail"
            return []

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_count_stored_comments",
        lambda post_ids, platform: {"ig-db-1": 0},
    )
    monkeypatch.setattr(
        social_repo,
        "_mark_missing_comments_for_anchor",
        lambda **kwargs: marked_missing.append(str(kwargs.get("anchor_id"))) or 0,
    )
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **kwargs: None)

    payload = social_repo.refresh_post_comments(
        "season-1",
        platform="instagram",
        source_id="abc123",
        max_comments_per_post=10,
        fetch_replies=False,
    )

    assert payload["fetch_failed"] is False
    assert payload["is_complete"] is False
    assert payload["incomplete_reason"] == "api_status_fail"
    assert payload["comment_fail_reasons"] == ["api_status_fail"]
    assert payload["comments_marked_missing"] == 0
    assert marked_missing == []


def test_refresh_post_comments_instagram_zero_limit_sets_fetch_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2025, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {"id": "ig-db-1", "account": "bravotv"},
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_count_stored_comments",
        lambda _post_ids, _platform: {"ig-db-1": 2},
    )

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_comment_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def fetch_comments(self, *args, **kwargs):
            raise AssertionError("fetch_comments should not be called when max_comments_per_post=0")

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)

    payload = social_repo.refresh_post_comments(
        "season-1",
        platform="instagram",
        source_id="abc123",
        max_comments_per_post=0,
        fetch_replies=False,
    )

    assert payload["comments_fetched"] == 0
    assert payload["is_complete"] is False
    assert payload["incomplete_reason"] == "fetch_disabled"
    assert payload["comment_fetch_reason"] == "fetch_disabled"
    assert payload["comment_fail_reasons"] == ["fetch_disabled"]


def test_mirror_instagram_media_to_s3_dedupes_thumbnail_and_media(monkeypatch) -> None:
    requests_seen: list[str] = []
    uploads_seen: list[str] = []

    class _FakeStreamResponse:
        def __init__(self, body: bytes):
            self._body = body
            self.headers = {"Content-Type": "image/jpeg"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int = 0):
            yield self._body

    def _fake_requests_get(url: str, **kwargs):
        requests_seen.append(url)
        return _FakeStreamResponse(b"abc")

    class _FakeS3Client:
        def upload_fileobj(self, fileobj, bucket: str, key: str, ExtraArgs=None) -> None:  # noqa: N803
            del fileobj, bucket, ExtraArgs
            uploads_seen.append(key)

    monkeypatch.setattr(social_repo.requests, "get", _fake_requests_get)
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_client", lambda: _FakeS3Client())
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_bucket", lambda: "bucket")
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_prefix", lambda: "")
    monkeypatch.setattr("trr_backend.media.s3_mirror.build_hosted_url", lambda key: f"https://cdn.test/{key}")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post = SimpleNamespace(
        shortcode="abc123",
        thumbnail_url="https://img.test/asset.jpg",
        media_urls=["https://img.test/asset.jpg"],
    )
    hosted_thumbnail_url, hosted_media_urls, status, error = social_repo._mirror_instagram_media_to_s3(
        context,
        post=post,
        week_index=1,
    )

    assert status == "mirrored"
    assert error is None
    assert len(requests_seen) == 1
    assert len(uploads_seen) == 1
    assert hosted_thumbnail_url == hosted_media_urls[0]


def test_mirror_instagram_media_to_s3_enforces_asset_size_cap(monkeypatch) -> None:
    uploads_seen: list[str] = []

    class _FakeStreamResponse:
        headers = {"Content-Type": "image/jpeg"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int = 0):
            yield b"x" * 2048

    class _FakeS3Client:
        def upload_fileobj(self, fileobj, bucket: str, key: str, ExtraArgs=None) -> None:  # noqa: N803
            del fileobj, bucket, ExtraArgs
            uploads_seen.append(key)

    monkeypatch.setenv("SOCIAL_MEDIA_MIRROR_MAX_BYTES", "1024")
    monkeypatch.setattr(social_repo.requests, "get", lambda *args, **kwargs: _FakeStreamResponse())
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_client", lambda: _FakeS3Client())
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_bucket", lambda: "bucket")
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_prefix", lambda: "")
    monkeypatch.setattr("trr_backend.media.s3_mirror.build_hosted_url", lambda key: f"https://cdn.test/{key}")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post = SimpleNamespace(
        shortcode="abc123",
        thumbnail_url="https://img.test/asset.jpg",
        media_urls=[],
    )
    _thumb, _media, status, error = social_repo._mirror_instagram_media_to_s3(
        context,
        post=post,
        week_index=1,
    )

    assert status == "failed"
    assert error and "asset_too_large" in error
    assert uploads_seen == []


def test_platform_post_needs_media_mirror_youtube_requires_hosted_media_urls() -> None:
    post_row = {
        "id": "yt-db-1",
        "video_id": "video-123",
        "thumbnail_url": "https://img.test/thumb.jpg",
        "media_urls": [],
        "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
        "hosted_media_urls": [],
        "media_mirror_status": "mirrored",
    }

    assert social_repo._platform_post_needs_media_mirror("youtube", post_row) is True


def test_platform_post_needs_media_mirror_tiktok_requires_hosted_media_urls() -> None:
    post_row = {
        "id": "tt-db-1",
        "video_id": "video-123",
        "thumbnail_url": "https://img.test/thumb.jpg",
        "media_urls": ["https://video.test/main.mp4"],
        "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
        "hosted_media_urls": [],
        "media_mirror_status": "mirrored",
    }

    assert social_repo._platform_post_needs_media_mirror("tiktok", post_row) is True


def test_update_platform_post_media_mirror_fields_writes_hosted_media_urls_as_jsonb(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_platform_posts_has_column", lambda _platform, _column: True)
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext("cur"))

    def _fake_fetch_one_with_cursor(cur, sql: str, params: list[object]):  # noqa: ANN001
        del cur
        captured["sql"] = sql
        captured["params"] = params
        return {"id": "ok"}

    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)

    social_repo._update_platform_post_media_mirror_fields(
        platform="instagram",
        post_id="00000000-0000-0000-0000-000000000001",
        hosted_media_urls=["https://cdn.test/media.jpg"],
    )

    sql = str(captured.get("sql") or "")
    params = list(captured.get("params") or [])
    assert "hosted_media_urls = %s::jsonb" in sql
    assert params[0] == '["https://cdn.test/media.jpg"]'


def test_run_platform_media_mirror_stage_instagram_re_resolves_source_media(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000001"
    updates: list[dict[str, object]] = []
    source_update_calls: list[dict[str, object]] = []
    mirrored_inputs: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "abc123",
            "thumbnail_url": "",
            "media_urls": [],
            "raw_data": {},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_update_instagram_post_source_media_fields",
        lambda **kwargs: source_update_calls.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_resolve_instagram_media_for_shortcode",
        lambda **_kwargs: {
            "source": "graphql_shortcode",
            "media_urls": ["https://src.test/video.mp4"],
            "thumbnail_url": "https://src.test/thumb.jpg",
            "attempts": [
                {
                    "source": "api_media_info",
                    "success": False,
                    "reason_code": "instagram_api_failed",
                    "http_status": 400,
                    "selected_url_count": 0,
                },
                {
                    "source": "graphql_shortcode",
                    "success": True,
                    "reason_code": None,
                    "http_status": 200,
                    "selected_url_count": 1,
                },
            ],
        },
    )

    def _fake_mirror_result(_context, *, platform: str, post, week_index: int | None, display_name: str | None = None):  # noqa: ANN001
        mirrored_inputs.append(
            {
                "platform": platform,
                "week_index": week_index,
                "thumbnail_url": post.thumbnail_url,
                "media_urls": list(post.media_urls or []),
            }
        )
        return {
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/video.mp4"],
            "status": "mirrored",
            "error": None,
            "retryable_error": False,
            "mirrored_count": 2,
            "source_count": 2,
        }

    monkeypatch.setattr(social_repo, "_mirror_platform_media_to_s3_result", _fake_mirror_result)

    posts, mirrored, metadata = social_repo._run_platform_media_mirror_stage(
        context=context,
        platform="instagram",
        job_id="job-1",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert mirrored_inputs and mirrored_inputs[0]["thumbnail_url"] == "https://src.test/thumb.jpg"
    assert mirrored_inputs[0]["media_urls"] == ["https://src.test/video.mp4"]
    assert source_update_calls and source_update_calls[0]["post_id"] == post_id
    assert metadata["mirror"]["selected_source"] == "graphql_shortcode"
    attempts = metadata["mirror"]["attempts"]
    assert attempts[0]["source"] == "api_media_info"
    assert attempts[1]["source"] == "graphql_shortcode"
    assert updates and updates[0]["media_mirror_status"] == "pending"


def test_run_platform_media_mirror_stage_tiktok_resolves_source_media(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000001"
    updates: list[dict[str, object]] = []
    mirrored_inputs: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "tt-video-123",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": [],
            "raw_data": {"url": "https://www.tiktok.com/@bravotv/video/tt-video-123"},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_resolve_tiktok_media_for_video_id",
        lambda **_kwargs: {
            "source": "yt_dlp_manifest",
            "media_urls": ["https://video.test/main.mp4"],
            "thumbnail_url": "https://img.test/thumb.jpg",
            "attempts": [
                {
                    "source": "yt_dlp_manifest",
                    "success": True,
                    "reason_code": None,
                    "http_status": None,
                    "selected_url_count": 1,
                }
            ],
        },
    )

    def _fake_mirror_result(_context, *, platform: str, post, week_index: int | None, display_name: str | None = None):  # noqa: ANN001
        mirrored_inputs.append(
            {
                "platform": platform,
                "week_index": week_index,
                "thumbnail_url": post.thumbnail_url,
                "media_urls": list(post.media_urls or []),
            }
        )
        return {
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/main.mp4"],
            "status": "mirrored",
            "error": None,
            "retryable_error": False,
            "mirrored_count": 2,
            "source_count": 2,
        }

    monkeypatch.setattr(social_repo, "_mirror_platform_media_to_s3_result", _fake_mirror_result)

    posts, mirrored, metadata = social_repo._run_platform_media_mirror_stage(
        context=context,
        platform="tiktok",
        job_id="job-tt-1",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert mirrored_inputs and mirrored_inputs[0]["media_urls"] == ["https://video.test/main.mp4"]
    assert metadata["mirror"]["selected_source"] == "yt_dlp_manifest"
    assert metadata["mirror"]["attempts"][0]["source"] == "yt_dlp_manifest"
    assert updates and updates[0]["media_mirror_status"] == "pending"


def test_run_platform_media_mirror_stage_youtube_resolves_source_media(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000001"
    updates: list[dict[str, object]] = []
    mirrored_inputs: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "yt-video-123",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": [],
            "raw_data": {},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_resolve_youtube_media_for_video_id",
        lambda **_kwargs: {
            "source": "yt_dlp_manifest",
            "media_urls": ["https://video.test/main.mp4"],
            "thumbnail_url": "https://img.test/thumb.jpg",
            "attempts": [
                {
                    "source": "yt_dlp_manifest",
                    "success": True,
                    "reason_code": None,
                    "http_status": None,
                    "selected_url_count": 1,
                }
            ],
        },
    )

    def _fake_mirror_result(_context, *, platform: str, post, week_index: int | None, display_name: str | None = None):  # noqa: ANN001
        mirrored_inputs.append(
            {
                "platform": platform,
                "week_index": week_index,
                "thumbnail_url": post.thumbnail_url,
                "media_urls": list(post.media_urls or []),
            }
        )
        return {
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/main.mp4"],
            "status": "mirrored",
            "error": None,
            "retryable_error": False,
            "mirrored_count": 2,
            "source_count": 2,
        }

    monkeypatch.setattr(social_repo, "_mirror_platform_media_to_s3_result", _fake_mirror_result)

    posts, mirrored, metadata = social_repo._run_platform_media_mirror_stage(
        context=context,
        platform="youtube",
        job_id="job-yt-1",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert mirrored_inputs and mirrored_inputs[0]["media_urls"] == ["https://video.test/main.mp4"]
    assert metadata["mirror"]["selected_source"] == "yt_dlp_manifest"
    assert metadata["mirror"]["attempts"][0]["source"] == "yt_dlp_manifest"
    assert updates and updates[0]["media_mirror_status"] == "pending"


def test_run_platform_media_mirror_stage_twitter_resolves_video_from_public_summary(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000001"
    updates: list[dict[str, object]] = []
    mirrored_inputs: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "2011339494734078234",
            "thumbnail_url": "https://pbs.twimg.com/tweet_video_thumb/G-m2mzhbQAQ-Kho.jpg",
            "media_urls": ["https://pbs.twimg.com/tweet_video_thumb/G-m2mzhbQAQ-Kho.jpg"],
            "raw_data": {},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: None)

    class _FakeTwitterScraper:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def fetch_public_tweet_summary(self, tweet_id: str, delay: float = 0.0) -> dict[str, object]:
            del delay
            assert tweet_id == "2011339494734078234"
            return {
                "media_urls": [
                    "https://video.twimg.com/tweet_video/G-m2mzhbQAQ-Kho.mp4",
                    "https://pbs.twimg.com/tweet_video_thumb/G-m2mzhbQAQ-Kho.jpg",
                ]
            }

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)

    def _fake_mirror_result(_context, *, platform: str, post, week_index: int | None, display_name: str | None = None):  # noqa: ANN001
        mirrored_inputs.append(
            {
                "platform": platform,
                "week_index": week_index,
                "thumbnail_url": post.thumbnail_url,
                "media_urls": list(post.media_urls or []),
            }
        )
        return {
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/video.mp4", "https://cdn.test/thumb.jpg"],
            "status": "mirrored",
            "error": None,
            "retryable_error": False,
            "mirrored_count": 2,
            "source_count": 2,
        }

    monkeypatch.setattr(social_repo, "_mirror_platform_media_to_s3_result", _fake_mirror_result)

    posts, mirrored, metadata = social_repo._run_platform_media_mirror_stage(
        context=context,
        platform="twitter",
        job_id="job-twitter-1",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert mirrored_inputs and mirrored_inputs[0]["media_urls"][0].startswith("https://video.twimg.com/")
    assert mirrored_inputs[0]["thumbnail_url"].startswith("https://pbs.twimg.com/tweet_video_thumb/")
    assert metadata["mirror"]["selected_source"] == "public_tweet_summary"
    assert metadata["mirror"]["attempts"][0]["source"] == "public_tweet_summary"
    assert updates and updates[0]["media_mirror_status"] == "pending"


def test_requeue_media_mirror_jobs_supports_non_instagram_platforms(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    enqueue_calls: list[dict[str, object]] = []

    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda _sql, _params: [
            {
                "id": "tt-db-1",
                "source_id": "777",
                "account": "bravotv",
                "posted_at": datetime(2026, 2, 22, tzinfo=UTC),
                "thumbnail_url": "https://cdn.test/source-thumb.jpg",
                "media_urls": ["https://cdn.test/source-vid.mp4"],
                "hosted_thumbnail_url": "",
                "hosted_media_urls": [],
                "media_mirror_status": "",
            }
        ],
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: (
            [
                WeekWindow(
                    week_index=1,
                    start_local=datetime(2026, 2, 20, tzinfo=UTC),
                    end_local=datetime(2026, 2, 27, tzinfo=UTC),
                )
            ],
            datetime(2026, 2, 20, tzinfo=UTC),
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_week_for_timestamp",
        lambda *_args, **_kwargs: WeekWindow(
            week_index=1,
            start_local=datetime(2026, 2, 20, tzinfo=UTC),
            end_local=datetime(2026, 2, 27, tzinfo=UTC),
        ),
    )

    def _fake_enqueue(*_args, **kwargs):
        enqueue_calls.append(dict(kwargs))
        return "job-1"

    monkeypatch.setattr(social_repo, "_enqueue_platform_media_mirror_job", _fake_enqueue)

    payload = social_repo.requeue_media_mirror_jobs(
        "season-1",
        platform="tiktok",
        source_scope="bravo",
        limit=100,
        date_start=datetime(2026, 2, 20, tzinfo=UTC),
        date_end=datetime(2026, 2, 27, tzinfo=UTC),
    )

    assert payload["platform"] == "tiktok"
    assert payload["queued_jobs"] == 1
    assert payload["failed"] == 0
    assert payload["window_applied"] is True
    assert payload["eligible_in_window"] == 1
    assert len(enqueue_calls) == 1
    assert enqueue_calls[0]["platform"] == "tiktok"


def test_week_detail_instagram_includes_media_mirror_diagnostics(monkeypatch) -> None:
    post_id = "00000000-0000-0000-0000-000000000001"
    mirror_ts = datetime(2026, 2, 24, 16, 30, tzinfo=UTC)

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        del params
        normalized = " ".join(sql.split()).lower()
        if "from social.instagram_posts p" in normalized:
            return [
                {
                    "id": post_id,
                    "source_id": "abc123",
                    "author": "bravotv",
                    "text": "hello #rhoslc @andy",
                    "likes": 5,
                    "comments_count": 2,
                    "views": 10,
                    "media_type": "image",
                    "media_urls": ["https://src/media.jpg"],
                    "hosted_media_urls": ["https://cdn/media.jpg"],
                    "thumbnail_url": "https://cdn/thumb.jpg",
                    "post_format": "post",
                    "profile_tags": ["tagged_user"],
                    "collaborators": ["collab_user"],
                    "hashtags": ["RHOSLC"],
                    "mentions": ["@andy"],
                    "duration_seconds": None,
                    "media_mirror_attempt_count": 3,
                    "media_mirror_last_attempt_at": mirror_ts,
                    "media_mirror_last_job_id": "job-123",
                    "ts": datetime(2026, 2, 23, 12, 0, tzinfo=UTC),
                }
            ]
        if "from social.instagram_comments c where c.post_id = any" in normalized:
            return [{"post_id": post_id, "cnt": 2}]
        if "cross join lateral" in normalized:
            return []
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    payload = social_repo._week_detail_instagram(
        "season-1",
        start_dt=datetime(2026, 2, 20, tzinfo=UTC),
        end_dt=datetime(2026, 2, 27, tzinfo=UTC),
        account_handles=set(),
        max_comments=10,
    )

    assert payload["posts"]
    post = payload["posts"][0]
    assert post["media_mirror_attempt_count"] == 3
    assert post["media_mirror_last_attempt_at"] == mirror_ts.isoformat()
    assert post["media_mirror_last_job_id"] == "job-123"


def test_get_post_comments_instagram_includes_media_mirror_diagnostics(monkeypatch) -> None:
    mirror_ts = datetime(2026, 2, 24, 16, 30, tzinfo=UTC)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "00000000-0000-0000-0000-000000000001",
            "source_id": "abc123",
            "author": "bravotv",
            "text": "hello #rhoslc @andy",
            "likes": 5,
            "comments_count": 2,
            "views": 10,
            "thumbnail_url": "https://cdn/thumb.jpg",
            "post_format": "post",
            "profile_tags": ["tagged_user"],
            "collaborators": ["collab_user"],
            "hashtags": ["RHOSLC"],
            "mentions": ["@andy"],
            "duration_seconds": None,
            "media_mirror_attempt_count": 2,
            "media_mirror_last_attempt_at": mirror_ts,
            "media_mirror_last_job_id": "job-789",
            "ts": datetime(2026, 2, 23, 12, 0, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda _sql, _params: [])

    payload = social_repo.get_post_comments("season-1", platform="instagram", source_id="abc123")
    assert payload["media_mirror_attempt_count"] == 2
    assert payload["media_mirror_last_attempt_at"] == mirror_ts.isoformat()
    assert payload["media_mirror_last_job_id"] == "job-789"


def test_ingest_youtube_posts_stage_reports_filter_diagnostics(monkeypatch) -> None:
    upserted_video_ids: list[str] = []

    class _FakeYouTubeScraper:
        last_retrieval_meta: dict[str, object] = {}

        def scrape(self, config, progress_cb=None):
            if progress_cb:
                progress_cb(
                    {
                        "phase": "scrape_posts",
                        "pages_scanned": 1,
                        "posts_checked": 3,
                        "matched_posts": 2,
                    }
                )
            return [
                SimpleNamespace(
                    video_id="vid-filtered",
                    title="Bravo sneak peek",
                    description="No show keywords here",
                    comments=2,
                ),
                SimpleNamespace(
                    video_id="vid-up-to-date",
                    title="RHOSLC teaser",
                    description="Tonight on Bravo",
                    comments=10,
                ),
                SimpleNamespace(
                    video_id="vid-keep",
                    title="Bravo teaser",
                    description="All about RHOSLC this week",
                    comments=3,
                ),
            ]

        def fetch_comments(self, *args, **kwargs):
            return []

    context = SeasonContext(
        season_id="season-youtube-meta",
        show_id="show-youtube-meta",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"youtube"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=100,
        max_comments_per_post=25,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_and_comments",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 31, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.youtube.YouTubeScraper", _FakeYouTubeScraper)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_cleanup_mismatched_youtube_rows",
        lambda **kwargs: {"scanned": 0, "videos_deleted": 0, "comments_deleted": 0},
    )
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda *args, **kwargs: [
            {
                "id": "youtube-post-db-1",
                "video_id": "vid-up-to-date",
                "published_at": datetime(2026, 1, 10, tzinfo=UTC),
                "comments_count": 10,
            }
        ],
    )
    monkeypatch.setattr(
        social_repo,
        "_load_comment_lifecycle_snapshots",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        social_repo,
        "_decide_comment_refresh",
        lambda **kwargs: social_repo.CommentRefreshDecision(should_refresh=False, reason="up_to_date"),
    )
    monkeypatch.setattr(
        social_repo,
        "_upsert_youtube_video",
        lambda *_args, **kwargs: (
            upserted_video_ids.append(str(getattr(kwargs.get("video"), "video_id", ""))),
            {"id": f"db-{getattr(kwargs.get('video'), 'video_id', 'unknown')}"},
        )[1],
    )
    monkeypatch.setattr(social_repo, "_sync_youtube_video_comment_counts", lambda *_args, **_kwargs: 0)

    posts, comments, meta = social_repo._ingest_youtube(
        context,
        run_id="run-youtube-meta",
        account="bravo",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC", "Salt Lake City"],
        opts=opts,
        job_id="job-youtube-meta",
        stage="posts",
    )

    assert posts == 3
    assert comments == 0
    assert upserted_video_ids == ["vid-keep"]
    assert meta["videos_scanned"] == 3
    assert meta["videos_matched_show_terms"] == 2
    assert meta["videos_filtered_show_terms"] == 1
    assert meta["videos_skipped_up_to_date"] == 1
    assert (meta.get("persist_counters") or {}).get("posts_upserted") == 1

    filter_samples = meta.get("filter_samples") or []
    reasons = {str(sample.get("reason") or "") for sample in filter_samples if isinstance(sample, dict)}
    assert "show_terms_filtered" in reasons
    assert "up_to_date" in reasons


def test_ingest_twitter_comments_stage_treats_non_positive_post_limit_as_no_cap(monkeypatch) -> None:
    fetched_reply_ids: list[str] = []
    fetched_reply_requests: list[str] = []

    class _FakeTwitterScraper:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def fetch_tweet_replies(self, tweet_id: str, delay: float) -> list[SimpleNamespace]:
            fetched_reply_requests.append(tweet_id)
            return [SimpleNamespace(tweet_id=f"{tweet_id}-r1", reply_to_tweet_id=None, is_reply=False)]

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"twitter"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=0,
        max_comments_per_post=50,
        max_replies_per_post=50,
        fetch_replies=True,
        ingest_mode="comments_only",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2025, 1, 31, tzinfo=UTC),
    )

    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: {})
    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda *args, **kwargs: [
            {
                "tweet_id": "anchor-1",
                "is_reply": False,
                "replies_count": 2,
                "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            },
            {
                "tweet_id": "anchor-2",
                "is_reply": False,
                "replies_count": 1,
                "created_at": datetime(2025, 1, 2, tzinfo=UTC),
            },
            {
                "tweet_id": "reply-no-anchor",
                "is_reply": True,
                "replies_count": 0,
                "created_at": datetime(2025, 1, 2, tzinfo=UTC),
            },
        ],
    )
    monkeypatch.setattr(social_repo, "_load_comment_lifecycle_snapshots", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        social_repo,
        "_decide_comment_refresh",
        lambda **kwargs: social_repo.CommentRefreshDecision(should_refresh=True, reason="never_checked"),
    )
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda _table: False)
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo, "_mark_missing_comments_for_anchor", lambda **kwargs: 0)
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **kwargs: None)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_pg_upsert",
        lambda table, payload, *, conflict_col, conn=None: (
            fetched_reply_ids.append(str(payload.get("tweet_id"))),
            {"id": f"row-{payload.get('tweet_id')}"},
        )[1],
    )

    posts, replies, meta = social_repo._ingest_twitter(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["rhoslc"],
        keywords=["rhoslc"],
        opts=opts,
        job_id="job-1",
        stage="comments",
    )

    assert posts == 2
    assert replies == 2
    assert fetched_reply_requests == ["anchor-1", "anchor-2"]
    assert fetched_reply_ids == ["anchor-1-r1", "anchor-2-r1"]
    assert meta["comment_stats"]["comments_fetched"] == 2
    assert meta["comment_stats"]["comments_upserted"] == 2
    assert meta["comment_stats"]["comments_skipped_missing_id"] == 0


def test_upsert_instagram_comment_tree_skips_blank_comment_id_and_counts(monkeypatch) -> None:
    class _Comment:
        comment_id = "   "
        username = "viewer"
        user_id = "user-1"
        text = "hello"
        likes = 0
        is_reply = False
        reply_count = 0
        created_at = datetime(2026, 2, 10, tzinfo=UTC)
        replies: list[object] = []

        def to_dict(self) -> dict[str, object]:
            return {}

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    stats = social_repo._new_comment_persist_stats()
    upsert_called = False

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        nonlocal upsert_called
        upsert_called = True
        return {"id": "should-not-happen"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)

    written = social_repo._upsert_instagram_comment_tree(
        context,
        job_id="job-1",
        run_id="run-1",
        account="bravotv",
        post_id="post-1",
        comment=_Comment(),
        persist_stats=stats,
    )

    assert written == 0
    assert upsert_called is False
    assert stats["comments_fetched"] == 1
    assert stats["comments_upserted"] == 0
    assert stats["comments_skipped_missing_id"] == 1


def test_upsert_tweet_skips_blank_tweet_id_and_counts(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    stats = social_repo._new_comment_persist_stats()
    upsert_called = False

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        nonlocal upsert_called
        upsert_called = True
        return {"id": "should-not-happen"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)

    row = social_repo._upsert_tweet(
        context,
        job_id="job-1",
        run_id="run-1",
        account="bravotv",
        tweet=SimpleNamespace(tweet_id="  "),
        persist_stats=stats,
    )

    assert row is None
    assert upsert_called is False
    assert stats["comments_fetched"] == 1
    assert stats["comments_upserted"] == 0
    assert stats["comments_skipped_missing_id"] == 1


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


def _job_execute_test_harness(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    state: dict[str, object] = {}

    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2025, 1, 1),
        ),
    )
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(social_repo, "_finalize_run_status", lambda *_args, **_kwargs: {})

    def _fake_finish_job(
        _job_id: str,
        *,
        status: str,
        items_found: int,
        error_message: str | None = None,
        metadata: dict[str, object] | None = None,
        last_error_code: str | None = None,
        last_error_class: str | None = None,
        next_available_at: datetime | None = None,
    ) -> None:
        state["last_finish"] = {
            "status": status,
            "items_found": items_found,
            "error_message": error_message,
            "metadata": dict(metadata or {}),
            "last_error_code": last_error_code,
            "last_error_class": last_error_class,
            "next_available_at": next_available_at,
        }

    monkeypatch.setattr(social_repo, "_finish_job", _fake_finish_job)

    def _fake_fetch_one(sql: str, params: list[object]):
        normalized = " ".join(sql.lower().split())
        if "select status from social.scrape_runs where id = %s" in normalized:
            return {"status": "running"}
        if "select items_found, metadata from social.scrape_jobs where id = %s" in normalized:
            last_finish = dict(state.get("last_finish") or {})
            return {
                "items_found": int(last_finish.get("items_found") or 0),
                "metadata": dict(last_finish.get("metadata") or {}),
            }
        if "from social.scrape_jobs where id = %s" in normalized:
            last_finish = dict(state.get("last_finish") or {})
            return {
                "id": str(params[0]),
                "run_id": "run-1",
                "platform": "instagram",
                "job_type": "instagram",
                "status": str(last_finish.get("status") or "unknown"),
                "items_found": int(last_finish.get("items_found") or 0),
                "error_message": last_finish.get("error_message"),
                "metadata": dict(last_finish.get("metadata") or {}),
            }
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    return state


def _instagram_job_fixture(*, attempt_count: int = 1, max_attempts: int = 3) -> dict[str, object]:
    return {
        "id": "job-1",
        "run_id": "run-1",
        "platform": "instagram",
        "job_type": "instagram",
        "status": "running",
        "config": {
            "season_id": "season-1",
            "source_scope": "bravo",
            "account": "bravotv",
            "stage": "posts",
            "ingest_mode": "posts_and_comments",
            "max_posts_per_target": 5,
            "max_comments_per_post": 5,
            "max_replies_per_post": 0,
            "fetch_replies": False,
            "hashtags": ["rhoslc"],
            "keywords": ["rhoslc"],
        },
        "metadata": {"stage": "posts"},
        "attempt_count": attempt_count,
        "max_attempts": max_attempts,
        "source_scope": "bravo",
        "season_id": "season-1",
    }


def test_execute_claimed_job_uses_crawlee_path_for_instagram_with_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _job_execute_test_harness(monkeypatch)
    calls: dict[str, int] = {"crawlee": 0, "legacy": 0}

    monkeypatch.setattr(social_repo, "should_use_crawlee", lambda _platform: True)
    monkeypatch.setattr(
        social_repo,
        "build_runtime_config",
        lambda _platform: SimpleNamespace(max_concurrency=2, max_retries=3, auth_strict=False),
    )
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie-1"})
    monkeypatch.setattr(
        social_repo,
        "_run_platform_stage_via_crawlee",
        lambda **_kwargs: (
            calls.__setitem__("crawlee", calls["crawlee"] + 1) or 2,
            4,
            {
                "persist_counters": {"posts_upserted": 2, "comments_upserted": 4},
                "activity": {"phase": "posts_end"},
                "crawler_runtime": {
                    "engine": "crawlee_python_incremental",
                    "crawlee_request_count": 1,
                    "crawlee_retry_count": 0,
                    "crawlee_session_pool_used": True,
                },
                "auth_context": {
                    "auth_preflight_ok": True,
                    "fallback_to_legacy": False,
                },
            },
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_run_platform_stage",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("legacy path should not run")),
    )

    result = social_repo._execute_claimed_job(_instagram_job_fixture())

    assert calls["crawlee"] == 1
    assert calls["legacy"] == 0
    assert result["status"] == "completed"
    metadata = dict((state.get("last_finish") or {}).get("metadata") or {})
    assert metadata["crawler_runtime"]["crawlee_request_count"] == 1
    assert metadata["crawler_runtime"]["crawlee_retry_count"] == 0
    assert metadata["crawler_runtime"]["crawlee_session_pool_used"] is True
    assert metadata["auth_context"]["auth_preflight_ok"] is True
    assert metadata["auth_context"]["fallback_to_legacy"] is False


def test_execute_claimed_job_falls_back_to_legacy_for_instagram_missing_auth_when_not_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _job_execute_test_harness(monkeypatch)
    calls: dict[str, int] = {"crawlee": 0, "legacy": 0}

    monkeypatch.setattr(social_repo, "should_use_crawlee", lambda _platform: True)
    monkeypatch.setattr(
        social_repo,
        "build_runtime_config",
        lambda _platform: SimpleNamespace(max_concurrency=2, max_retries=3, auth_strict=False),
    )
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {})
    monkeypatch.setattr(
        social_repo,
        "_run_platform_stage_via_crawlee",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("crawlee path should not run on fail-open fallback")),
    )

    def _legacy_runner(**_kwargs):
        calls["legacy"] += 1
        return 1, 3, {"persist_counters": {"posts_upserted": 1, "comments_upserted": 3}}

    monkeypatch.setattr(social_repo, "_run_platform_stage", _legacy_runner)

    result = social_repo._execute_claimed_job(_instagram_job_fixture())

    assert result["status"] == "completed"
    assert calls["crawlee"] == 0
    assert calls["legacy"] == 1
    metadata = dict((state.get("last_finish") or {}).get("metadata") or {})
    assert metadata["auth_context"]["auth_preflight_ok"] is False
    assert metadata["auth_context"]["fallback_to_legacy"] is True
    assert metadata["auth_context"]["reason"] == "instagram_cookies_missing"
    assert metadata["crawler_runtime"]["crawlee_request_count"] == 0
    assert metadata["crawler_runtime"]["crawlee_retry_count"] == 0
    assert metadata["crawler_runtime"]["crawlee_session_pool_used"] is False


def test_execute_claimed_job_fails_fast_for_instagram_missing_auth_when_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _job_execute_test_harness(monkeypatch)

    monkeypatch.setattr(social_repo, "should_use_crawlee", lambda _platform: True)
    monkeypatch.setattr(
        social_repo,
        "build_runtime_config",
        lambda _platform: SimpleNamespace(max_concurrency=2, max_retries=3, auth_strict=True),
    )
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {})
    monkeypatch.setattr(
        social_repo,
        "_run_platform_stage_via_crawlee",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("crawlee stage should not execute")),
    )
    monkeypatch.setattr(
        social_repo,
        "_run_platform_stage",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("legacy stage should not execute in strict mode")),
    )

    result = social_repo._execute_claimed_job(_instagram_job_fixture())

    assert result["status"] == "failed"
    last_finish = dict(state.get("last_finish") or {})
    metadata = dict(last_finish.get("metadata") or {})
    assert metadata["error_code"] == "auth"
    assert metadata["auth_context"]["auth_preflight_ok"] is False
    assert metadata["auth_context"]["fallback_to_legacy"] is False
    assert last_finish["status"] == "failed"


def test_execute_claimed_job_marks_crawlee_blocked_errors_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _job_execute_test_harness(monkeypatch)

    monkeypatch.setattr(social_repo, "should_use_crawlee", lambda _platform: True)
    monkeypatch.setattr(
        social_repo,
        "build_runtime_config",
        lambda _platform: SimpleNamespace(max_concurrency=2, max_retries=3, auth_strict=False),
    )
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie-1"})

    class _BlockedRuntimeError(RuntimeError):
        error_code = "blocked"
        error_class = "BlockedRuntimeError"
        retryable = True
        runtime_metadata = {
            "crawler_runtime": {
                "crawlee_request_count": 1,
                "crawlee_retry_count": 1,
                "crawlee_session_pool_used": True,
            },
            "auth_context": {
                "auth_mode": "cookies",
                "auth_source": "SOCIAL_INSTAGRAM_COOKIES_*",
                "auth_preflight_ok": True,
                "fallback_to_legacy": False,
            },
        }

    def _raise_blocked(**_kwargs):
        raise _BlockedRuntimeError("blocked by anti-bot challenge")

    monkeypatch.setattr(social_repo, "_run_platform_stage_via_crawlee", _raise_blocked)
    monkeypatch.setattr(
        social_repo,
        "_retry_backoff_seconds",
        lambda _attempt_count: 1,
    )

    result = social_repo._execute_claimed_job(_instagram_job_fixture(attempt_count=1, max_attempts=3))

    assert result["status"] == "retrying"
    last_finish = dict(state.get("last_finish") or {})
    metadata = dict(last_finish.get("metadata") or {})
    assert last_finish["status"] == "retrying"
    assert metadata["error_code"] == "blocked"
    assert metadata["retryable"] is True
    assert metadata["crawler_runtime"]["crawlee_request_count"] == 1
    assert metadata["crawler_runtime"]["crawlee_retry_count"] == 1
    assert last_finish["next_available_at"] is not None


def test_upsert_facebook_post_falls_back_to_scraped_at_when_posted_at_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = datetime(2026, 2, 27, 12, 0, tzinfo=UTC)
    captured: dict[str, object] = {}

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    post = SimpleNamespace(
        post_id="fb-1",
        username="bravo",
        caption="hello",
        post_type="reel",
        media_urls=[],
        thumbnail_url=None,
        likes=0,
        comments=0,
        shares=0,
        views=0,
        posted_at=None,
        to_dict=lambda: {"posted_at": None},
    )

    monkeypatch.setattr(social_repo, "_now_utc", lambda: fixed_now)

    def _fake_pg_upsert(table: str, payload: dict[str, object], conflict_col: str, conn=None):  # noqa: ANN001
        del conn
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_pg_upsert)

    result = social_repo._upsert_facebook_post(
        context,
        job_id="job-1",
        account="bravotv",
        post=post,
    )

    assert result == {"id": "db-1"}
    payload = dict(captured["payload"])  # type: ignore[index]
    assert payload["posted_at"] == fixed_now
    assert payload["scraped_at"] == fixed_now
    raw_data = dict(payload["raw_data"])  # type: ignore[index]
    assert raw_data["_ingest"]["posted_at_fallback"] == "scraped_at"  # type: ignore[index]


def test_upsert_threads_post_falls_back_to_scraped_at_when_posted_at_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = datetime(2026, 2, 27, 12, 5, tzinfo=UTC)
    captured: dict[str, object] = {}

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    post = SimpleNamespace(
        post_id="th-1",
        username="bravotv",
        text="hello",
        media_urls=[],
        thumbnail_url=None,
        likes=0,
        replies=0,
        reposts=0,
        quotes=0,
        views=0,
        posted_at=None,
        to_dict=lambda: {"posted_at": None},
    )

    monkeypatch.setattr(social_repo, "_now_utc", lambda: fixed_now)

    def _fake_pg_upsert(table: str, payload: dict[str, object], conflict_col: str, conn=None):  # noqa: ANN001
        del conn
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-2"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_pg_upsert)

    result = social_repo._upsert_meta_threads_post(
        context,
        job_id="job-2",
        account="bravotv",
        post=post,
    )

    assert result == {"id": "db-2"}
    payload = dict(captured["payload"])  # type: ignore[index]
    assert payload["posted_at"] == fixed_now
    assert payload["scraped_at"] == fixed_now
    raw_data = dict(payload["raw_data"])  # type: ignore[index]
    assert raw_data["_ingest"]["posted_at_fallback"] == "scraped_at"  # type: ignore[index]


def test_load_twitter_auth_accepts_storage_state_json_env(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "cookies": [
            {"name": "auth_token", "value": "token-1"},
            {"name": "ct0", "value": "ct0-1"},
            {"name": "lang", "value": "en"},
        ]
    }
    monkeypatch.setenv("SOCIAL_TWITTER_COOKIES_JSON", json.dumps(payload))
    monkeypatch.delenv("TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_FILE", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("TWITTER_BEARER_TOKEN", raising=False)

    cookies, bearer = social_repo._load_twitter_auth()

    assert bearer is None
    assert cookies["auth_token"] == "token-1"
    assert cookies["ct0"] == "ct0-1"
    assert cookies["lang"] == "en"


def test_load_twitter_auth_accepts_cookie_list_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    cookie_file = tmp_path / "twitter-cookies.json"
    cookie_file.write_text(
        json.dumps(
            [
                {"name": "auth_token", "value": "token-file"},
                {"name": "ct0", "value": "ct0-file"},
                {"name": "guest_id", "value": "v1%3Aabc"},
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.setenv("SOCIAL_TWITTER_COOKIES_FILE", str(cookie_file))
    monkeypatch.delenv("TWITTER_COOKIES_FILE", raising=False)

    cookies, _bearer = social_repo._load_twitter_auth()

    assert cookies["auth_token"] == "token-file"
    assert cookies["ct0"] == "ct0-file"
    assert cookies["guest_id"] == "v1%3Aabc"


def test_load_twikit_credentials_accepts_storage_state_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    cookie_file = tmp_path / "twikit-cookies.json"
    cookie_file.write_text(
        json.dumps(
            {
                "cookies": [
                    {"name": "auth_token", "value": "token-twikit"},
                    {"name": "ct0", "value": "ct0-twikit"},
                    {"name": "other", "value": "ignored"},
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("TWIKIT_COOKIES_FILE", str(cookie_file))
    monkeypatch.delenv("TWIKIT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWIKIT_CT0", raising=False)
    monkeypatch.delenv("TWIKIT_USERNAME", raising=False)
    monkeypatch.delenv("TWIKIT_PASSWORD", raising=False)
    monkeypatch.delenv("TWIKIT_EMAIL", raising=False)

    creds = social_repo._load_twikit_credentials()

    assert creds == {"auth_token": "token-twikit", "ct0": "ct0-twikit"}


def test_load_twikit_credentials_accepts_storage_state_json_env(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "cookies": [
            {"name": "auth_token", "value": "token-inline"},
            {"name": "ct0", "value": "ct0-inline"},
            {"name": "lang", "value": "en"},
        ]
    }
    monkeypatch.setenv("TWIKIT_COOKIES_JSON", json.dumps(payload))
    monkeypatch.delenv("TWIKIT_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWIKIT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWIKIT_CT0", raising=False)
    monkeypatch.delenv("TWIKIT_USERNAME", raising=False)
    monkeypatch.delenv("TWIKIT_PASSWORD", raising=False)
    monkeypatch.delenv("TWIKIT_EMAIL", raising=False)

    creds = social_repo._load_twikit_credentials()

    assert creds == {"auth_token": "token-inline", "ct0": "ct0-inline"}


def test_load_twitter_auth_falls_back_to_browser_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_FILE", raising=False)
    monkeypatch.setattr(
        social_repo,
        "_load_twitter_browser_cookies",
        lambda: {"auth_token": "browser-token", "ct0": "browser-ct0"},
    )

    cookies, bearer = social_repo._load_twitter_auth()

    assert bearer is None
    assert cookies["auth_token"] == "browser-token"
    assert cookies["ct0"] == "browser-ct0"


def test_load_twikit_credentials_falls_back_to_browser_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TWIKIT_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWIKIT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWIKIT_CT0", raising=False)
    monkeypatch.delenv("TWIKIT_USERNAME", raising=False)
    monkeypatch.delenv("TWIKIT_PASSWORD", raising=False)
    monkeypatch.delenv("TWIKIT_EMAIL", raising=False)
    monkeypatch.setattr(
        social_repo,
        "_load_twitter_browser_cookies",
        lambda: {"auth_token": "browser-token", "ct0": "browser-ct0"},
    )

    creds = social_repo._load_twikit_credentials()

    assert creds == {"auth_token": "browser-token", "ct0": "browser-ct0"}


def test_load_twitter_auth_accepts_cookie_header_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_TWITTER_COOKIES_HEADER", "auth_token=token-h; ct0=ct0-h; lang=en")
    monkeypatch.delenv("TWITTER_COOKIES_HEADER", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_FILE", raising=False)

    cookies, bearer = social_repo._load_twitter_auth()

    assert bearer is None
    assert cookies["auth_token"] == "token-h"
    assert cookies["ct0"] == "ct0-h"
    assert cookies["lang"] == "en"


def test_load_twikit_credentials_accepts_cookie_header_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TWIKIT_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWIKIT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWIKIT_CT0", raising=False)
    monkeypatch.delenv("TWIKIT_USERNAME", raising=False)
    monkeypatch.delenv("TWIKIT_PASSWORD", raising=False)
    monkeypatch.delenv("TWIKIT_EMAIL", raising=False)
    monkeypatch.setenv("SOCIAL_TWITTER_COOKIES_HEADER", "Cookie: auth_token=token-h; ct0=ct0-h; other=1")
    monkeypatch.delenv("TWITTER_COOKIES_HEADER", raising=False)
    monkeypatch.setattr(social_repo, "_load_twitter_browser_cookies", lambda: {})

    creds = social_repo._load_twikit_credentials()

    assert creds == {"auth_token": "token-h", "ct0": "ct0-h"}


# ---------------------------------------------------------------------------
# Structured display name helpers
# ---------------------------------------------------------------------------


class TestResolveShowSlug:
    def test_explicit_slug(self):
        ctx = SeasonContext(
            season_id="s1", show_id="sh1", show_name="The Real Housewives of Salt Lake City",
            season_number=6, anchor_date=date(2025, 9, 16), show_slug="RHOSLC",
        )
        assert social_repo._resolve_show_slug(ctx) == "RHOSLC"

    def test_slug_with_special_chars(self):
        ctx = SeasonContext(
            season_id="s1", show_id="sh1", show_name="Test",
            season_number=1, anchor_date=date(2025, 1, 1), show_slug="RHO-SLC!",
        )
        assert social_repo._resolve_show_slug(ctx) == "RHOSLC"

    def test_fallback_to_show_name(self):
        ctx = SeasonContext(
            season_id="s1", show_id="sh1", show_name="The Real Housewives of Salt Lake City",
            season_number=6, anchor_date=date(2025, 9, 16), show_slug=None,
        )
        assert social_repo._resolve_show_slug(ctx) == "TheRealHousewivesofSaltLakeCity"

    def test_fallback_no_name(self):
        ctx = SeasonContext(
            season_id="s1", show_id="sh1", show_name=None,
            season_number=1, anchor_date=date(2025, 1, 1), show_slug=None,
        )
        assert social_repo._resolve_show_slug(ctx) == "UnknownShow"

    def test_empty_slug_falls_back(self):
        ctx = SeasonContext(
            season_id="s1", show_id="sh1", show_name="Test Show",
            season_number=1, anchor_date=date(2025, 1, 1), show_slug="  ",
        )
        assert social_repo._resolve_show_slug(ctx) == "TestShow"


class TestBuildPostDisplayName:
    def test_basic(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOSLC", username="BravoBetsy", platform="tiktok", post_number=1,
        )
        assert result == "RHOSLCBravoBetsyTikTokPost1"

    def test_instagram(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOSLC", username="heatherrgay", platform="instagram", post_number=3,
        )
        assert result == "RHOSLCheatherrgayInstagramPost3"

    def test_youtube(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOBH", username="BravoTV", platform="youtube", post_number=2,
        )
        assert result == "RHOBHBravoTVYouTubePost2"

    def test_twitter(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOSLC", username="user123", platform="twitter", post_number=1,
        )
        assert result == "RHOSLCuser123TwitterPost1"

    def test_unknown_platform(self):
        result = social_repo._build_post_display_name(
            show_slug="X", username="u", platform="newplatform", post_number=1,
        )
        assert result == "XuNewplatformPost1"


class TestBuildMediaObjectName:
    def test_thumbnail(self):
        assert social_repo._build_media_object_name(
            "RHOSLCBravoBetsyTikTokPost1", is_thumbnail=True, slide_number=None,
        ) == "RHOSLCBravoBetsyTikTokPost1_Thumbnail"

    def test_single_media(self):
        assert social_repo._build_media_object_name(
            "RHOSLCBravoBetsyTikTokPost1", is_thumbnail=False, slide_number=None,
        ) == "RHOSLCBravoBetsyTikTokPost1"

    def test_multi_slide(self):
        assert social_repo._build_media_object_name(
            "RHOSLCheatherrgayInstagramPost2", is_thumbnail=False, slide_number=3,
        ) == "RHOSLCheatherrgayInstagramPost2_S3"


class TestResolvePostUsername:
    def test_standard_platform(self):
        row = {"username": "bravobetsy", "source_account": "betsy123"}
        assert social_repo._resolve_post_username("tiktok", row) == "bravobetsy"

    def test_youtube_channel_title(self):
        row = {"channel_title": "Bravo TV", "source_account": "bravotv"}
        assert social_repo._resolve_post_username("youtube", row) == "BravoTV"

    def test_strip_at_sign(self):
        row = {"username": "@bravobetsy"}
        assert social_repo._resolve_post_username("instagram", row) == "bravobetsy"

    def test_fallback_to_post_username(self):
        row = {"post_username": "fallback_user"}
        assert social_repo._resolve_post_username("tiktok", row) == "fallback_user"

    def test_unknown(self):
        row = {}
        assert social_repo._resolve_post_username("tiktok", row) == "unknown"

    def test_special_chars_removed(self):
        row = {"username": "user.name!@#"}
        assert social_repo._resolve_post_username("instagram", row) == "username"
