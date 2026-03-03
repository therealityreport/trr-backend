"""Unit tests for season social analytics helpers."""

import inspect
import json
import subprocess
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import trr_backend.repositories.social_season_analytics as social_repo
from trr_backend.repositories.social_season_analytics import (
    SeasonContext,
    SentimentAnalyzerContext,
    WeekWindow,
    _build_ingest_shard_schedule,
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
    assert {"instagram", "tiktok", "twitter", "youtube", "facebook", "threads"} <= {
        str(item.get("platform") or "") for item in defaults
    }
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


def test_instagram_post_matches_show_terms_requires_explicit_rhoslc_hashtag() -> None:
    context = SeasonContext(
        season_id="season-rhoslc-instagram-match",
        show_id="show-rhoslc-instagram-match",
        show_name="The Real Housewives of Salt Lake City",
        show_slug="rhoslc",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )

    assert social_repo._instagram_post_matches_show_terms(
        context=context,
        text="Tonight on #RHOSLC",
        post_hashtags=["RHOSLC"],
        hashtags=["RHOSLC"],
        keywords=["Salt Lake City"],
    )
    assert not social_repo._instagram_post_matches_show_terms(
        context=context,
        text="The Salt Lake City cast arrives",
        post_hashtags=[],
        hashtags=["RHOSLC"],
        keywords=["Salt Lake City"],
    )


def test_threads_extract_topic_from_nested_raw_data() -> None:
    raw_data = {
        "metadata": {"ignored": True},
        "activity": {
            "breadcrumbs": ["bravotv", "rhoslc"],
        },
    }
    assert social_repo._threads_extract_topic(raw_data) == "bravotv > rhoslc"


def test_threads_extract_topic_from_tag_header_payload() -> None:
    raw_data = {
        "raw_data": {
            "text_post_app_info": {
                "tag_header": {
                    "display_name": "RHOSLC",
                    "tag_cluster_name": "rhoslc",
                }
            }
        }
    }
    assert social_repo._threads_extract_topic(raw_data) == "RHOSLC"


def test_threads_extract_topic_falls_back_to_text_rhoslc_hashtag() -> None:
    assert social_repo._threads_extract_topic({}, text="Your prayers answered. #RHOSLC is back!") == "RHOSLC"


def test_threads_build_relevance_terms_rhoslc_is_strict_token_only() -> None:
    context = SeasonContext(
        season_id="season-rhoslc",
        show_id="show-rhoslc",
        show_name="The Real Housewives of Salt Lake City",
        show_slug="rhoslc",
        season_number=6,
        anchor_date=date(2025, 9, 16),
    )
    hashtags, keywords = social_repo._threads_build_relevance_terms(
        context.season_id,
        source_scope="bravo",
        context=context,
    )
    assert hashtags == ["RHOSLC"]
    assert keywords == ["RHOSLC"]


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


def test_youtube_video_matches_show_terms_accepts_tag_only_match() -> None:
    assert _youtube_video_matches_show_terms(
        title="Bravo sneak peek",
        description="Premieres soon",
        tags=["RealHousewivesOfSaltLakeCity", "BronwynNewport"],
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


def test_upsert_tiktok_post_persists_mentions(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        del conn
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-tt-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "tiktok"
        and column
        in {
            "mentions",
            "media_urls",
            "sound_id",
            "sound_title",
            "sound_author",
            "sound_usage_count",
            "user_avatar_url",
        },
    )

    context = SeasonContext(
        season_id="season-tt-upsert",
        show_id="show-tt-upsert",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    post = SimpleNamespace(
        video_id="tt-1",
        username="bravotv",
        author_nickname="BravoTV",
        description="Tune in with @BravoTV and @bravotv #RHOSLC",
        hashtags=[],
        mentions=[],
        media_urls=[],
        likes=1,
        comments=2,
        shares=3,
        views=4,
        duration=10,
        user_avatar_url="https://images.test/tiktok-avatar.jpg",
        create_time=datetime(2026, 2, 1, tzinfo=UTC),
        to_dict=lambda: {
            "video_id": "tt-1",
            "music": {"id": "7540327234013301517", "title": "Lisa Flies Coach", "authorName": "Bravo"},
        },
    )

    payload = social_repo._upsert_tiktok_post(context, job_id="job-tt", account="bravotv", post=post)
    assert payload == {"id": "db-tt-1"}
    assert captured["table"] == "tiktok_posts"
    saved = captured["payload"]
    assert isinstance(saved, dict)
    assert saved["mentions"] == ["@BravoTV"]
    assert saved["hashtags"] == ["RHOSLC"]
    assert saved["sound_id"] == "7540327234013301517"
    assert saved["sound_title"] == "Lisa Flies Coach"
    assert saved["sound_author"] == "Bravo"
    assert saved["user_avatar_url"] == "https://images.test/tiktok-avatar.jpg"


def test_upsert_youtube_video_persists_hashtags_and_mentions(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        del conn
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-yt-2"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "youtube" and column in {"hashtags", "mentions", "user_avatar_url"},
    )

    context = SeasonContext(
        season_id="season-yt-upsert",
        show_id="show-yt-upsert",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 8, 14),
    )
    video = SimpleNamespace(
        video_id="vid-hash-1",
        channel_id="channel-1",
        channel_title="Bravo",
        title="RHOSLC #RHOSLC",
        description="Feat @BravoTV",
        tags=["RealHousewivesOfSaltLakeCity", "BronwynNewport"],
        duration="PT45S",
        duration_seconds=45,
        views=100,
        likes=10,
        comments=5,
        thumbnail_url="https://img.test/short.jpg",
        user_avatar_url="https://images.test/youtube-avatar.jpg",
        published_at=datetime(2025, 8, 14, tzinfo=UTC),
        to_dict=lambda: {"video_id": "vid-hash-1"},
    )

    payload = social_repo._upsert_youtube_video(
        context,
        job_id="job-yt-upsert",
        account="bravo",
        video=video,
        conn=None,
    )

    assert payload == {"id": "db-yt-2"}
    saved = captured["payload"]
    assert isinstance(saved, dict)
    assert saved["hashtags"] == ["RHOSLC", "RealHousewivesOfSaltLakeCity", "BronwynNewport"]
    assert saved["mentions"] == ["@BravoTV"]
    assert saved["user_avatar_url"] == "https://images.test/youtube-avatar.jpg"


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
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(SimpleNamespace()))
    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", lambda *_args, **_kwargs: {"views": 300})

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
        tagged_users_detail=[
            {
                "username": "tagged_user",
                "full_name": "Tagged User",
                "tag_x": 0.1234,
                "tag_y": 0.9876,
                "tag_position_source": "rest_usertags.position_array",
            }
        ],
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
    assert payload["tagged_users_detail"][0]["tag_x"] == 0.1234
    assert payload["tagged_users_detail"][0]["tag_y"] == 0.9876
    assert payload["tagged_users_detail"][0]["tag_position_source"] == "rest_usertags.position_array"
    assert payload["hosted_thumbnail_url"] == "https://cdn.example/social/thumb.jpg"
    assert payload["hosted_media_urls"] == ["https://cdn.example/social/media.mp4"]
    assert payload["media_mirror_status"] == "mirrored"
    assert payload["views"] == 333
    assert (payload.get("raw_data") or {}).get("view_metrics", {}).get("source") is None


def test_upsert_instagram_post_skips_missing_optional_columns(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        captured["table"] = table
        captured["payload"] = payload
        captured["conflict_col"] = conflict_col
        return {"id": "db-post-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_instagram_posts_has_column", lambda _column: False)
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(SimpleNamespace()))
    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", lambda *_args, **_kwargs: {"views": 500})

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
    assert payload["views"] == 500


def test_upsert_instagram_post_uses_monotonic_views_and_preserves_when_observed_missing(monkeypatch) -> None:
    captured_payloads: list[dict[str, object]] = []

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        captured_payloads.append(payload)
        return {"id": "db-post-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_instagram_posts_has_column", lambda _column: True)
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(SimpleNamespace()))

    existing_views_values = [{"views": 1200}, {"views": 1200}]
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one_with_cursor",
        lambda *_args, **_kwargs: existing_views_values.pop(0),
    )

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_with_observed = SimpleNamespace(
        shortcode="abc123",
        pk="media-1",
        username="bravotv",
        caption="Watch #RHOSLC with @bravotv",
        post_type="reel",
        media_urls=[],
        thumbnail_url=None,
        likes=111,
        comments=22,
        video_views=333,
        video_views_observed=13700,
        video_views_source="node.play_count",
        video_views_raw_candidates=[{"source": "node.play_count", "raw": "13.7K", "parsed": 13700}],
        taken_at=datetime(2026, 2, 18, 20, 0, tzinfo=UTC),
        to_dict=lambda: {"shortcode": "abc123"},
    )
    post_without_observed = SimpleNamespace(
        shortcode="abc123",
        pk="media-1",
        username="bravotv",
        caption="Watch #RHOSLC with @bravotv",
        post_type="reel",
        media_urls=[],
        thumbnail_url=None,
        likes=111,
        comments=22,
        video_views=0,
        video_views_observed=None,
        taken_at=datetime(2026, 2, 18, 20, 0, tzinfo=UTC),
        to_dict=lambda: {"shortcode": "abc123"},
    )

    social_repo._upsert_instagram_post(
        context,
        job_id="job-1",
        account="bravotv",
        post=post_with_observed,
        conn=None,
    )
    social_repo._upsert_instagram_post(
        context,
        job_id="job-2",
        account="bravotv",
        post=post_without_observed,
        conn=None,
    )

    first_payload = captured_payloads[0]
    second_payload = captured_payloads[1]
    assert first_payload["views"] == 13700
    assert second_payload["views"] == 1200
    first_view_metrics = (first_payload.get("raw_data") or {}).get("view_metrics") or {}
    assert first_view_metrics.get("source") == "node.play_count"
    assert first_view_metrics.get("observed_count") == 13700


def test_enrich_instagram_post_preserves_existing_collaborators_when_metadata_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_utc = datetime(2026, 2, 28, 12, 0, tzinfo=UTC)

    metadata = SimpleNamespace(
        taken_at=None,
        profile_tags=None,
        collaborators=None,
        hashtags=None,
        mentions=None,
        duration_seconds=None,
        post_format=None,
        thumbnail_url=None,
        media_urls=[],
    )
    resolution = SimpleNamespace(source="permalink_html", metadata=metadata, attempts=[])
    monkeypatch.setattr("trr_backend.socials.instagram.resolve_instagram_media", lambda *_args, **_kwargs: resolution)

    post = SimpleNamespace(
        shortcode="abc123",
        caption="Watch #RHOSLC with @BravoTV",
        collaborators=["existing_collab"],
        profile_tags=[],
        post_type="reel",
        media_urls=[],
        thumbnail_url=None,
    )
    scraper = SimpleNamespace(session=None, cookies={})

    social_repo._enrich_instagram_post_from_permalink(post=post, scraper=scraper, now_utc=now_utc)

    assert post.collaborators == ["existing_collab"]


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

    posts3, comments3, replies3, fetch_replies3 = _resolve_depth_defaults(
        max_posts_per_target=0,
        max_comments_per_post=None,
        max_replies_per_post=None,
        fetch_replies=True,
    )
    assert posts3 == 0
    assert comments3 == 180
    assert replies3 == 90
    assert fetch_replies3 is True

    posts4, comments4, replies4, fetch_replies4 = _resolve_depth_defaults(
        max_posts_per_target=None,
        max_comments_per_post=None,
        max_replies_per_post=None,
        fetch_replies=True,
    )
    assert posts4 == 0
    assert comments4 == 180
    assert replies4 == 90
    assert fetch_replies4 is True


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


def test_resolve_requested_platforms_rejects_invalid_or_empty_inputs() -> None:
    with pytest.raises(ValueError, match="INVALID_PLATFORM_FILTER"):
        social_repo._resolve_requested_platforms(["instagram", "myspace"])
    with pytest.raises(ValueError, match="INVALID_PLATFORM_FILTER"):
        social_repo._resolve_requested_platforms([])


def test_normalize_account_handle_accepts_profile_urls_and_rejects_invalid_values() -> None:
    assert social_repo._normalize_account_handle("@BravoTV") == "bravotv"
    assert social_repo._normalize_account_handle("BravoTV") == "bravotv"
    assert social_repo._normalize_account_handle("https://instagram.com/BravoTV/?hl=en") == "bravotv"
    assert social_repo._normalize_account_handle("https://www.instagram.com/BravoTV/reels/") == "bravotv"
    assert social_repo._normalize_account_handle("not valid !") == ""


def test_ingest_season_raises_no_targets_before_run_creation(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-ingest-empty",
        show_id="show-ingest-empty",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    create_run_calls: list[dict[str, object]] = []

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(
        social_repo,
        "get_targets",
        lambda *_args, **_kwargs: {
            "targets": [],
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_create_run",
        lambda *_args, **_kwargs: create_run_calls.append({"called": True}) or "run-should-not-exist",
    )

    with pytest.raises(social_repo.SocialIngestValidationError) as exc:
        social_repo.ingest_season(
            context.season_id,
            platforms=["instagram"],
            source_scope="creator",
            max_posts_per_target=0,
            max_comments_per_post=0,
            max_replies_per_post=0,
            fetch_replies=False,
            ingest_mode="posts_only",
            date_start=None,
            date_end=None,
            initiated_by=None,
        )

    assert exc.value.code == "NO_INGEST_TARGETS"
    assert create_run_calls == []


def test_ingest_season_dedupes_normalized_account_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-ingest-overrides",
        show_id="show-ingest-overrides",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    created_job_accounts: list[str] = []
    run_config_holder: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(
        social_repo,
        "get_targets",
        lambda *_args, **_kwargs: {
            "targets": [
                {
                    "platform": "instagram",
                    "accounts": [],
                    "hashtags": [],
                    "keywords": [],
                    "is_active": True,
                }
            ]
        },
    )
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(
        social_repo,
        "_create_run",
        lambda _context, **kwargs: run_config_holder.update({"config": kwargs.get("config")}) or "run-1",
    )
    monkeypatch.setattr(
        social_repo,
        "_create_job",
        lambda _context, **kwargs: created_job_accounts.append(str(kwargs.get("config", {}).get("account") or ""))
        or f"job-{len(created_job_accounts)}",
    )
    monkeypatch.setattr(social_repo, "_update_run_summary", lambda _run_id: {"total_jobs": 1})

    payload = social_repo.ingest_season(
        context.season_id,
        platforms=["instagram"],
        accounts_override=["@BravoTV", "https://instagram.com/BRAVOTV/", "bravotv"],
        hashtags_override=["RHOSLC"],
        keywords_override=["Salt Lake City"],
        source_scope="bravo",
        max_posts_per_target=0,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
        date_start=None,
        date_end=None,
        initiated_by=None,
    )

    assert payload["queued_or_started_jobs"] == 2
    assert sorted(created_job_accounts) == ["bravotv", "bravowwhl"]
    run_config = run_config_holder.get("config")
    assert isinstance(run_config, dict)
    assert run_config.get("accounts_override") == ["bravotv"]


@pytest.mark.parametrize("platform", ["instagram", "tiktok", "twitter"])
def test_ingest_season_enforces_bravo_core_accounts_when_no_override(
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
) -> None:
    context = SeasonContext(
        season_id=f"season-ingest-core-{platform}",
        show_id="show-ingest-core",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    created_job_accounts: list[str] = []

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(
        social_repo,
        "get_targets",
        lambda *_args, **_kwargs: {
            "targets": [
                {
                    "platform": platform,
                    "accounts": ["bravotv"],
                    "hashtags": [],
                    "keywords": [],
                    "is_active": True,
                }
            ]
        },
    )
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(social_repo, "_create_run", lambda *_args, **_kwargs: "run-core-accounts")
    monkeypatch.setattr(
        social_repo,
        "_create_job",
        lambda _context, **kwargs: created_job_accounts.append(str(kwargs.get("config", {}).get("account") or ""))
        or f"job-{len(created_job_accounts)}",
    )
    monkeypatch.setattr(social_repo, "_update_run_summary", lambda _run_id: {"total_jobs": len(created_job_accounts)})

    payload = social_repo.ingest_season(
        context.season_id,
        platforms=[platform],
        source_scope="bravo",
        max_posts_per_target=25,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
        date_start=None,
        date_end=None,
        initiated_by=None,
    )

    assert payload["queued_or_started_jobs"] == 2
    assert sorted(created_job_accounts) == ["bravotv", "bravowwhl"]


@pytest.mark.parametrize("platform", ["instagram", "tiktok", "twitter"])
def test_ingest_season_enforces_bravo_core_accounts_even_with_override(
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
) -> None:
    context = SeasonContext(
        season_id=f"season-ingest-core-override-{platform}",
        show_id="show-ingest-core-override",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    created_job_accounts: list[str] = []

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(
        social_repo,
        "get_targets",
        lambda *_args, **_kwargs: {
            "targets": [
                {
                    "platform": platform,
                    "accounts": ["networkpromo"],
                    "hashtags": [],
                    "keywords": [],
                    "is_active": True,
                }
            ]
        },
    )
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(social_repo, "_create_run", lambda *_args, **_kwargs: "run-core-accounts-override")
    monkeypatch.setattr(
        social_repo,
        "_create_job",
        lambda _context, **kwargs: created_job_accounts.append(str(kwargs.get("config", {}).get("account") or ""))
        or f"job-{len(created_job_accounts)}",
    )
    monkeypatch.setattr(social_repo, "_update_run_summary", lambda _run_id: {"total_jobs": len(created_job_accounts)})

    payload = social_repo.ingest_season(
        context.season_id,
        platforms=[platform],
        source_scope="bravo",
        accounts_override=["networkpromo"],
        max_posts_per_target=25,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
        date_start=None,
        date_end=None,
        initiated_by=None,
    )

    assert payload["queued_or_started_jobs"] == 3
    assert sorted(created_job_accounts) == ["bravotv", "bravowwhl", "networkpromo"]


def test_build_ingest_shard_schedule_assigns_dual_runner_lanes_and_offset() -> None:
    schedule = _build_ingest_shard_schedule(
        date_start=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        date_end=datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
        runner_strategy="adaptive_dual_runner",
        runner_count=2,
        window_shard_hours=2,
        runner_b_start_offset_hours=None,
        day_weight_profile="rhoslc_default",
        priority_mode="episode_peak_weighted",
    )

    assert schedule.strategy == "adaptive_dual_runner"
    assert schedule.runner_count == 2
    assert schedule.runner_b_offset_hours == 48
    assert schedule.shards
    assert all(shard.window_end >= shard.window_start for shard in schedule.shards)
    assert any(shard.runner_lane == "B" for shard in schedule.shards)
    assert schedule.day_weights[1] == 1.0


def test_build_ingest_shard_schedule_starts_both_runners_when_window_is_short() -> None:
    schedule = _build_ingest_shard_schedule(
        date_start=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        date_end=datetime(2026, 1, 3, 23, 59, tzinfo=UTC),
        runner_strategy="adaptive_dual_runner",
        runner_count=2,
        window_shard_hours=2,
        runner_b_start_offset_hours=None,
        day_weight_profile="rhoslc_default",
        priority_mode="episode_peak_weighted",
    )

    assert schedule.runner_b_offset_hours == 0
    assert any(shard.runner_lane == "B" for shard in schedule.shards)


def test_ingest_season_details_refresh_stage_plan_creates_posts_jobs_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = SeasonContext(
        season_id="season-ingest-refresh",
        show_id="show-ingest-refresh",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    created_job_stages: list[str] = []

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(
        social_repo,
        "get_targets",
        lambda *_args, **_kwargs: {
            "targets": [
                {
                    "platform": "instagram",
                    "accounts": ["@bravotv"],
                    "hashtags": [],
                    "keywords": [],
                    "is_active": True,
                }
            ]
        },
    )
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(social_repo, "_create_run", lambda *_args, **_kwargs: "run-2")
    monkeypatch.setattr(
        social_repo,
        "_create_job",
        lambda _context, **kwargs: created_job_stages.append(str(kwargs.get("stage") or ""))
        or f"job-{len(created_job_stages)}",
    )
    monkeypatch.setattr(social_repo, "_update_run_summary", lambda _run_id: {"total_jobs": 1})

    payload = social_repo.ingest_season(
        context.season_id,
        platforms=["instagram"],
        source_scope="bravo",
        max_posts_per_target=0,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=True,
        ingest_mode="details_refresh",
        date_start=None,
        date_end=None,
        initiated_by=None,
    )

    assert payload["stages"] == ["posts"]
    assert len(created_job_stages) == 2
    assert all(stage == "posts" for stage in created_job_stages)


def test_set_run_status_invalidates_week_detail_cache_on_terminal_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalidation_calls: list[str] = []
    monkeypatch.setattr(social_repo.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "run-1"})
    social_repo.register_week_detail_cache_invalidator(lambda: invalidation_calls.append("called"))
    try:
        social_repo._set_run_status("run-1", "running")
        assert invalidation_calls == []
        social_repo._set_run_status("run-1", "completed")
        assert invalidation_calls == ["called"]
    finally:
        social_repo.register_week_detail_cache_invalidator(None)


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


def test_get_targets_enforces_rhoslc_instagram_account_floor_on_existing_rows(monkeypatch) -> None:
    season_id = "season-targets-rhoslc-existing"
    rows = [
        {
            "season_id": season_id,
            "show_id": "show-rhoslc",
            "season_number": 6,
            "show_name": "The Real Housewives of Salt Lake City",
            "platform": "instagram",
            "source_scope": "bravo",
            "timezone": "America/New_York",
            "accounts": ["bravotv"],
            "hashtags": [],
            "keywords": ["rhoslc"],
            "is_active": True,
            "config": {},
            "updated_by": "admin@example.com",
            "updated_at": datetime(2026, 2, 20, tzinfo=UTC),
            "created_at": datetime(2026, 2, 20, tzinfo=UTC),
        }
    ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda _sql, _params: rows)

    payload = get_targets(season_id, source_scope="bravo")

    assert payload["targets"][0]["accounts"] == ["bravotv", "bravowwhl"]
    assert payload["targets"][0]["hashtags"] == ["RHOSLC"]


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
        lambda _season_id: SeasonContext(
            season_id=season_id,
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2026, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_default_targets",
        lambda _context, source_scope="bravo": [
            {
                "platform": "facebook",
                "accounts": ["default_fb"],
                "is_active": True,
            },
            {
                "platform": "threads",
                "accounts": ["default_threads"],
                "is_active": True,
            },
        ]
        if source_scope == "bravo"
        else [],
    )

    payload = social_repo._target_accounts_by_platform(season_id, source_scope="bravo")

    assert captured["params"] == [season_id, "bravo"]
    assert "from social.season_targets" in str(captured["sql"]).lower()
    assert payload["youtube"] == {"bravo", "wwhl"}
    assert payload["instagram"] == {"bravotv", "bravowwhl"}
    assert payload["tiktok"] == {"bravotv", "bravowwhl"}
    assert payload["twitter"] == {"bravotv", "bravowwhl"}
    assert payload["facebook"] == {"default_fb"}
    assert payload["threads"] == {"default_threads"}


def test_target_accounts_by_platform_does_not_override_explicit_platform_rows(monkeypatch) -> None:
    season_id = "season-target-accounts-explicit"

    def _fake_fetch_all(_sql: str, _params: list[object]) -> list[dict[str, object]]:
        return [
            {"platform": "youtube", "accounts": ["bravo"], "is_active": True},
            {"platform": "facebook", "accounts": [], "is_active": False},
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id=season_id,
            show_id="show-1",
            show_name="Test Show",
            season_number=6,
            anchor_date=date(2026, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_default_targets",
        lambda _context, source_scope="bravo": [
            {"platform": "facebook", "accounts": ["default_fb"], "is_active": True},
            {"platform": "threads", "accounts": ["default_threads"], "is_active": True},
        ]
        if source_scope == "bravo"
        else [],
    )

    payload = social_repo._target_accounts_by_platform(season_id, source_scope="bravo")

    assert payload["youtube"] == {"bravo"}
    assert payload["instagram"] == {"bravotv", "bravowwhl"}
    assert payload["tiktok"] == {"bravotv", "bravowwhl"}
    assert payload["twitter"] == {"bravotv", "bravowwhl"}
    assert payload["facebook"] == set()
    assert payload["threads"] == {"default_threads"}


def test_target_accounts_by_platform_enforces_rhoslc_instagram_account_floor(monkeypatch) -> None:
    season_id = "season-target-accounts-rhoslc"

    def _fake_fetch_all(_sql: str, _params: list[object]) -> list[dict[str, object]]:
        return [
            {"platform": "instagram", "accounts": ["bravotv"], "is_active": True},
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id=season_id,
            show_id="show-rhoslc",
            show_name="The Real Housewives of Salt Lake City",
            show_slug="rhoslc",
            season_number=6,
            anchor_date=date(2026, 1, 1),
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_default_targets",
        lambda _context, source_scope="bravo": [] if source_scope == "bravo" else [],
    )

    payload = social_repo._target_accounts_by_platform(season_id, source_scope="bravo")

    assert payload["instagram"] == {"bravotv", "bravowwhl"}
    assert payload["tiktok"] == {"bravotv", "bravowwhl"}
    assert payload["twitter"] == {"bravotv", "bravowwhl"}


def test_compute_post_metadata_counts_tags_and_mentions_for_cross_platform_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_has_column(platform: str, column: str) -> bool:
        supported = {
            ("tiktok", "hashtags"),
            ("tiktok", "mentions"),
            ("youtube", "hashtags"),
            ("youtube", "mentions"),
            ("facebook", "hashtags"),
            ("facebook", "mentions"),
            ("facebook", "post_type"),
            ("threads", "hashtags"),
            ("threads", "mentions"),
            ("threads", "media_type"),
        }
        return (platform, column) in supported

    def _fake_fetch_all(sql: str, _params: list[object]) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if "from social.tiktok_posts p" in normalized:
            return [{"total": 3, "has_caption": 3, "has_tags": 2, "has_mentions": 3}]
        if "from social.youtube_videos p" in normalized:
            return [{"total": 2, "has_caption": 2, "has_tags": 2, "has_mentions": 1}]
        if "from social.facebook_posts p" in normalized:
            return [{"total": 1, "has_caption": 1, "has_tags": 1, "has_mentions": 1, "mtype": "reel"}]
        if "from social.meta_threads_posts p" in normalized:
            return [{"total": 1, "has_caption": 1, "has_tags": 1, "has_mentions": 1, "mtype": "text"}]
        return []

    monkeypatch.setattr(social_repo, "_platform_posts_has_column", _fake_has_column)
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = social_repo._compute_post_metadata(
        "season-1",
        platforms=["tiktok", "youtube", "facebook", "threads"],
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 8, tzinfo=UTC),
        target_accounts_by_platform={
            "tiktok": {"bravotv"},
            "youtube": {"bravo"},
            "facebook": {"bravo"},
            "threads": {"bravotv"},
        },
    )

    assert payload is not None
    assert payload["total_posts"] == 7
    assert payload["tags"]["posts_with"] == 6
    assert payload["mentions"]["posts_with"] == 6
    assert payload["collaborators"]["posts_with"] == 0


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


def test_comments_coverage_threads_counts_only_rhoslc_relevant_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="The Real Housewives of Salt Lake City",
        season_number=6,
        anchor_date=date(2026, 1, 1),
        show_slug="rhoslc",
    )

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if "from social.meta_threads_posts p" in normalized:
            return [
                {
                    "id": "post-1",
                    "reported_comments": 4,
                    "text": "generic bravo copy",
                    "raw_data": {"activity": {"topic_path": "bravotv > rhoslc"}},
                },
                {
                    "id": "post-2",
                    "reported_comments": 9,
                    "text": "generic bravo copy",
                    "raw_data": {"activity": {"topic_path": "bravotv > rhobh"}},
                },
            ]
        if "from social.meta_threads_comments c" in normalized:
            assert params == [["post-1"]]
            return [{"post_id": "post-1", "saved_comments": 3}]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(
        social_repo, "_threads_build_relevance_terms", lambda *_args, **_kwargs: (["rhoslc"], ["rhoslc"])
    )

    payload = social_repo._comments_coverage_for_platform(
        "season-1",
        platform="threads",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 8, tzinfo=UTC),
        source_scope="bravo",
        target_accounts_by_platform={"threads": {"bravotv"}},
        season_context=context,
    )
    assert payload == {
        "posts_scanned": 1,
        "stale_posts_count": 1,
        "saved_comments": 3,
        "reported_comments": 4,
    }


def test_mirror_coverage_threads_counts_only_rhoslc_relevant_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="The Real Housewives of Salt Lake City",
        season_number=6,
        anchor_date=date(2026, 1, 1),
        show_slug="rhoslc",
    )

    def _fake_fetch_all(_sql: str, _params: list[object]) -> list[dict[str, object]]:
        return [
            {
                "id": "post-1",
                "source_id": "th-1",
                "posted_at": datetime(2026, 1, 2, tzinfo=UTC),
                "thumbnail_url": "https://example.com/thumb-1.jpg",
                "media_urls": ["https://example.com/source-1.jpg"],
                "hosted_thumbnail_url": "",
                "hosted_media_urls": [],
                "media_mirror_status": "pending",
                "text": "generic bravo copy",
                "raw_data": {"activity": {"topic_path": "bravotv > rhoslc"}},
            },
            {
                "id": "post-2",
                "source_id": "th-2",
                "posted_at": datetime(2026, 1, 2, tzinfo=UTC),
                "thumbnail_url": "https://example.com/thumb-2.jpg",
                "media_urls": [],
                "hosted_thumbnail_url": "",
                "hosted_media_urls": [],
                "media_mirror_status": "",
                "text": "generic bravo copy",
                "raw_data": {"activity": {"topic_path": "bravotv > rhobh"}},
            },
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(
        social_repo, "_threads_build_relevance_terms", lambda *_args, **_kwargs: (["rhoslc"], ["rhoslc"])
    )
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "threads" and column == "media_urls",
    )

    payload = social_repo._mirror_coverage_for_platform(
        "season-1",
        platform="threads",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 8, tzinfo=UTC),
        source_scope="bravo",
        target_accounts_by_platform={"threads": {"bravotv"}},
        season_context=context,
    )
    assert payload == {
        "posts_scanned": 1,
        "needs_mirror_count": 1,
        "mirrored_count": 0,
        "failed_count": 0,
        "partial_count": 0,
        "pending_count": 1,
    }


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
                    "owner_profile_pic_url": "https://images.test/ig-owner-avatar.jpg",
                    "hosted_owner_profile_pic_url": "https://images.test/ig-owner-avatar-hosted.jpg",
                    "hosted_tagged_profile_pics": {"tagged_user": "https://images.test/tagged-user-avatar.jpg"},
                    "tagged_users_detail": [
                        {
                            "username": "tagged_user",
                            "full_name": "Tagged User",
                            "tag_x": 0.42,
                            "tag_y": 0.58,
                            "tag_position_source": "graphql_node.xy",
                        }
                    ],
                    "collaborators_detail": [
                        {"username": "collab_user", "profile_pic_url": "https://images.test/collab-user-avatar.jpg"}
                    ],
                    "child_posts_data": [
                        {
                            "slide_index": 0,
                            "display_url": "https://images.test/ig-slide-1.jpg",
                            "tagged_users_detail": [],
                        },
                        {
                            "slide_index": 1,
                            "display_url": "https://images.test/ig-slide-2.jpg",
                            "tagged_users_detail": [
                                {
                                    "username": "slide_tagged",
                                    "tag_x": 0.2,
                                    "tag_y": 0.8,
                                    "tag_position_source": "rest_usertags.position_array",
                                }
                            ],
                        },
                    ],
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
    assert post["user"]["username"] == "bravotv"
    assert post["user"]["url"] == "https://www.instagram.com/bravotv/"
    assert post["user"]["avatar_url"] == "https://images.test/ig-owner-avatar-hosted.jpg"
    assert post["owner_profile_pic_url"] == "https://images.test/ig-owner-avatar.jpg"
    assert post["hosted_owner_profile_pic_url"] == "https://images.test/ig-owner-avatar-hosted.jpg"
    assert post["hosted_tagged_profile_pics"]["tagged_user"] == "https://images.test/tagged-user-avatar.jpg"
    assert post["tagged_users_detail"][0]["username"] == "tagged_user"
    assert post["tagged_users_detail"][0]["url"] == "https://www.instagram.com/tagged_user/"
    assert post["tagged_users_detail"][0]["tag_x"] == 0.42
    assert post["tagged_users_detail"][0]["tag_y"] == 0.58
    assert post["tagged_users_detail"][0]["tag_position_source"] == "graphql_node.xy"
    assert post["collaborators_detail"][0]["username"] == "collab_user"
    assert post["collaborators_detail"][0]["url"] == "https://www.instagram.com/collab_user/"
    assert any(
        entry.get("username") == "bravotv" and entry.get("url") == "https://www.instagram.com/bravotv/"
        for entry in post["mentions_detail"]
    )
    assert len(post["child_posts_data"]) == 2
    assert post["child_posts_data"][1]["slide_index"] == 1
    assert post["child_posts_data"][1]["tagged_users_detail"][0]["username"] == "slide_tagged"
    assert post["child_posts_data"][1]["tagged_users_detail"][0]["tag_x"] == 0.2


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


def test_platform_thumbnail_expr_tiktok_prefers_thumbnail_before_hosted_media_fallback() -> None:
    instagram_expr = social_repo._instagram_posts_thumbnail_expr("p")
    tiktok_expr = social_repo._platform_thumbnail_expr("p", "tiktok")
    youtube_expr = social_repo._platform_thumbnail_expr("p", "youtube")
    twitter_expr = social_repo._platform_thumbnail_expr("p", "twitter")

    assert "to_jsonb(p) ->> 'hosted_thumbnail_url'" in instagram_expr
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in instagram_expr

    assert "nullif(p.thumbnail_url, '')" in tiktok_expr
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in tiktok_expr
    assert tiktok_expr.index("nullif(p.thumbnail_url, '')") < tiktok_expr.index(
        "to_jsonb(p) -> 'hosted_media_urls' ->> 0"
    )
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in youtube_expr
    assert "to_jsonb(p) -> 'hosted_media_urls' ->> 0" in twitter_expr
    assert "p.media_urls ->> 0" in twitter_expr


def test_platform_post_source_urls_twitter_prefers_non_video_media_candidate() -> None:
    source_thumbnail_url, source_media_urls = social_repo._platform_post_source_urls(
        "twitter",
        {
            "thumbnail_url": "https://video.twimg.com/ext_tw_video/123456789/pu/vid/avc1/1280x720/main.mp4?tag=12",
            "media_urls": [
                "https://video.twimg.com/ext_tw_video/123456789/pu/vid/avc1/1280x720/main.mp4?tag=12",
                "https://pbs.twimg.com/ext_tw_video_thumb/123456789/pu/img/cover.jpg",
            ],
        },
    )

    assert source_thumbnail_url == "https://pbs.twimg.com/ext_tw_video_thumb/123456789/pu/img/cover.jpg"
    assert source_media_urls[0].startswith("https://video.twimg.com/")


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
                    "mentions": ["@bravowwhl"],
                    "thumbnail_url": "https://example.com/tt-thumb.jpg",
                    "duration_seconds": 14,
                    "user_avatar_url": "https://images.test/tiktok-avatar.jpg",
                    "raw_data": {},
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
    assert payload["posts"][0]["user"]["url"] == "https://www.tiktok.com/@bravotv"
    assert payload["posts"][0]["user"]["avatar_url"] == "https://images.test/tiktok-avatar.jpg"
    assert payload["posts"][0]["mentions_detail"] == [
        {
            "username": "bravowwhl",
            "url": "https://www.tiktok.com/@bravowwhl",
        }
    ]


def test_week_detail_tiktok_query_prefers_thumbnail_before_hosted_media_fallback(monkeypatch) -> None:
    captured_sql: dict[str, str] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        normalized = " ".join(sql.lower().split())
        if "count(*)::int as post_count" in normalized and "from social.tiktok_posts p" in normalized:
            return [{"post_count": 0}]
        if "from social.tiktok_posts p" in normalized and "select p.id" in normalized:
            captured_sql["sql"] = sql
            return []
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    payload = _week_detail_tiktok(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 8, tzinfo=UTC),
        account_handles={"bravotv", "bravowwhl"},
        max_comments=0,
    )

    assert payload["posts"] == []
    sql = captured_sql["sql"]
    hosted_thumbnail_token = "nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', '')"
    thumbnail_token = "nullif(p.thumbnail_url, '')"
    hosted_media_token = "to_jsonb(p) -> 'hosted_media_urls' ->> 0"
    assert hosted_thumbnail_token in sql
    assert thumbnail_token in sql
    assert hosted_media_token in sql
    assert sql.index(thumbnail_token) < sql.index(hosted_media_token)


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
            "tagged_users_detail": [
                {
                    "username": "tagged_user",
                    "full_name": "Tagged User",
                    "tag_x": 0.42,
                    "tag_y": 0.58,
                    "tag_position_source": "graphql_node.xy",
                }
            ],
            "collaborators_detail": [{"username": "collab_a", "full_name": "Collab A"}],
            "child_posts_data": [
                {"slide_index": 0, "display_url": "https://instagram.example/slide-1.jpg", "tagged_users_detail": []},
                {
                    "slide_index": 1,
                    "display_url": "https://instagram.example/slide-2.jpg",
                    "tagged_users_detail": [{"username": "slide_tagged", "tag_x": 0.1, "tag_y": 0.9}],
                },
            ],
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
    assert payload["tagged_users_detail"][0]["username"] == "tagged_user"
    assert payload["tagged_users_detail"][0]["tag_x"] == 0.42
    assert payload["tagged_users_detail"][0]["tag_y"] == 0.58
    assert payload["profile_tags_detail"][0]["tag_position_source"] == "graphql_node.xy"
    assert payload["collaborators_detail"][0]["username"] == "collab_a"
    assert len(payload["child_posts_data"]) == 2
    assert payload["child_posts_data"][1]["slide_index"] == 1
    assert payload["child_posts_data"][1]["tagged_users_detail"][0]["username"] == "slide_tagged"
    assert payload["child_posts_data"][1]["tagged_users_detail"][0]["tag_y"] == 0.9
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
    assert payload["hashtags"] == []
    assert payload["mentions"] == []
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
                    "text": "desc #RHOSLC @BravoTV",
                    "views": 100,
                    "likes": 5,
                    "comments_count": 0,
                    "is_short": True,
                    "hashtags": [],
                    "mentions": [],
                    "thumbnail_url": "https://example.com/thumb.jpg",
                    "duration_seconds": 90,
                    "user_avatar_url": "https://images.test/youtube-avatar.jpg",
                    "raw_data": {},
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
    assert post["is_short"] is True
    assert post["url"] == "https://www.youtube.com/shorts/vid123"
    assert post["hashtags"] == ["RHOSLC"]
    assert post["mentions"] == ["@BravoTV"]
    assert post["user"]["url"] == "https://www.youtube.com/@bravo"
    assert post["user"]["avatar_url"] == "https://images.test/youtube-avatar.jpg"
    assert post["mentions_detail"] == [
        {
            "username": "bravotv",
            "url": "https://www.youtube.com/@bravotv",
        }
    ]
    assert payload["totals"]["total_comments"] == 5


def test_week_detail_youtube_excludes_missing_comments_when_lifecycle_supported(monkeypatch) -> None:
    seen_sql: list[str] = []

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        seen_sql.append(" ".join(str(sql).split()).lower())
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
                    "is_short": False,
                    "hashtags": [],
                    "mentions": [],
                    "thumbnail_url": "https://example.com/thumb.jpg",
                    "duration_seconds": 90,
                    "ts": datetime(2025, 1, 1, tzinfo=UTC),
                }
            ]
        if "from social.youtube_comments c" in sql and "group by c.video_id" in sql:
            return [{"video_id": "yt-db-1", "cnt": 1}]
        if "from social.youtube_comments c" in sql and "cross join lateral" in sql:
            return []
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "youtube_comments")
    social_repo._week_detail_youtube(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 8, tzinfo=UTC),
        account_handles={"bravo"},
        max_comments=0,
    )

    assert any(
        "from social.youtube_comments c" in sql and "coalesce(c.is_missing, false) = false" in sql for sql in seen_sql
    )


def test_week_detail_youtube_interpolates_comment_lifecycle_filter_in_count_query(monkeypatch) -> None:
    seen_sql: list[str] = []

    def _fake_fetch_all(sql: str, _params: list[object]) -> list[dict[str, object]]:
        seen_sql.append(str(sql))
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
                    "is_short": False,
                    "hashtags": [],
                    "mentions": [],
                    "thumbnail_url": "https://example.com/thumb.jpg",
                    "duration_seconds": 90,
                    "ts": datetime(2025, 1, 1, tzinfo=UTC),
                }
            ]
        if "from social.youtube_comments c" in sql and "group by c.video_id" in sql:
            return [{"video_id": "yt-db-1", "cnt": 1}]
        if "from social.youtube_comments c" in sql and "cross join lateral" in sql:
            return []
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "youtube_comments")

    social_repo._week_detail_youtube(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 8, tzinfo=UTC),
        account_handles={"bravo"},
        max_comments=0,
    )

    count_queries = [
        " ".join(query.split()).lower()
        for query in seen_sql
        if "from social.youtube_comments c" in query and "group by c.video_id" in query
    ]
    assert count_queries
    assert "coalesce(c.is_missing, false) = false" in count_queries[0]
    assert "{youtube_comment_active_filter}" not in count_queries[0]


def test_week_detail_facebook_includes_token_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_fetch_all(sql: str, _params: list[object]) -> list[dict[str, object]]:
        if "from social.facebook_posts p" in sql:
            return [
                {
                    "id": "fb-db-1",
                    "source_id": "fb-1",
                    "author": "bravo",
                    "text": "Watch #RHOSLC with @BravoTV",
                    "post_type": "feed",
                    "likes": 10,
                    "comments_count": 3,
                    "shares": 2,
                    "views": 20,
                    "hashtags": [],
                    "mentions": [],
                    "thumbnail_url": "https://img.test/fb.jpg",
                    "user_avatar_url": "https://images.test/facebook-avatar.jpg",
                    "raw_data": {},
                    "ts": datetime(2026, 1, 1, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    payload = social_repo._week_detail_facebook(
        "season-1",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 8, tzinfo=UTC),
        account_handles={"bravo"},
        max_comments=0,
    )
    post = payload["posts"][0]
    assert post["hashtags"] == ["RHOSLC"]
    assert post["mentions"] == ["@BravoTV"]
    assert post["user"]["url"] == "https://www.facebook.com/bravo"
    assert post["user"]["avatar_url"] == "https://images.test/facebook-avatar.jpg"
    assert post["mentions_detail"] == [
        {
            "username": "bravotv",
            "url": "https://www.facebook.com/bravotv",
        }
    ]


def test_week_detail_threads_includes_token_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_fetch_all(sql: str, _params: list[object]) -> list[dict[str, object]]:
        if "from social.meta_threads_posts p" in sql:
            return [
                {
                    "id": "th-db-1",
                    "source_id": "th-1",
                    "author": "bravotv",
                    "text": "Watch #RHOSLC with @BravoTV",
                    "likes": 10,
                    "replies_count": 3,
                    "reposts": 2,
                    "quotes": 1,
                    "views": 20,
                    "hashtags": [],
                    "mentions": [],
                    "thumbnail_url": "https://img.test/th.jpg",
                    "raw_data": {"user": {"profile_pic_url": "https://images.test/threads-avatar.jpg"}},
                    "ts": datetime(2026, 1, 1, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    payload = social_repo._week_detail_threads(
        "season-1",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 8, tzinfo=UTC),
        account_handles={"bravotv"},
        max_comments=0,
    )
    post = payload["posts"][0]
    assert post["hashtags"] == ["RHOSLC"]
    assert post["mentions"] == ["@BravoTV"]
    assert post["user"]["url"] == "https://www.threads.com/@bravotv"
    assert post["user"]["avatar_url"] == "https://images.test/threads-avatar.jpg"
    assert post["mentions_detail"] == [
        {
            "username": "bravotv",
            "url": "https://www.threads.com/@bravotv",
        }
    ]


def test_week_detail_threads_filters_to_rhoslc_relevant_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="The Real Housewives of Salt Lake City",
        season_number=6,
        anchor_date=date(2026, 1, 1),
        show_slug="rhoslc",
    )

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if "from social.meta_threads_posts p" in normalized:
            return [
                {
                    "id": "th-db-1",
                    "source_id": "th-1",
                    "author": "bravotv",
                    "text": "General bravo promo",
                    "likes": 10,
                    "replies_count": 3,
                    "reposts": 2,
                    "quotes": 1,
                    "views": 20,
                    "hashtags": [],
                    "mentions": [],
                    "thumbnail_url": "https://img.test/th.jpg",
                    "raw_data": {"activity": {"topic_path": "bravotv > rhoslc"}},
                    "ts": datetime(2026, 1, 1, tzinfo=UTC),
                },
                {
                    "id": "th-db-2",
                    "source_id": "th-2",
                    "author": "bravotv",
                    "text": "General bravo promo",
                    "likes": 8,
                    "replies_count": 1,
                    "reposts": 0,
                    "quotes": 0,
                    "views": 10,
                    "hashtags": [],
                    "mentions": [],
                    "thumbnail_url": "https://img.test/th2.jpg",
                    "raw_data": {"activity": {"topic_path": "bravotv > rhobh"}},
                    "ts": datetime(2026, 1, 1, tzinfo=UTC),
                },
            ]
        if "from social.meta_threads_comments c" in normalized and "group by c.post_id" in normalized:
            assert params == [["th-db-1"]]
            return [{"post_id": "th-db-1", "cnt": 1}]
        if "from social.meta_threads_comments c" in normalized and "cross join lateral" in normalized:
            return [
                {
                    "comment_id": "c1",
                    "post_id": "th-db-1",
                    "author": "viewer",
                    "text": "nice",
                    "likes": 1,
                    "is_reply": True,
                    "reply_count": 0,
                    "created_at": datetime(2026, 1, 2, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(
        social_repo, "_threads_build_relevance_terms", lambda *_args, **_kwargs: (["rhoslc"], ["rhoslc"])
    )

    payload = social_repo._week_detail_threads(
        "season-1",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 8, tzinfo=UTC),
        account_handles={"bravotv"},
        max_comments=5,
        source_scope="bravo",
        season_context=context,
    )
    assert payload["total_posts"] == 1
    assert len(payload["posts"]) == 1
    assert payload["posts"][0]["source_id"] == "th-1"
    assert payload["posts"][0]["topic"] == "bravotv > rhoslc"


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
    assert payload["hashtags"] == []
    assert payload["mentions"] == []


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


def test_get_post_comments_youtube_excludes_missing_when_lifecycle_supported(monkeypatch) -> None:
    seen_sql: list[str] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
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

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        del params
        seen_sql.append(" ".join(str(sql).split()).lower())
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "youtube_comments")

    get_post_comments("season-1", platform="youtube", source_id="vid123")
    assert any("coalesce(c.is_missing, false) = false" in sql for sql in seen_sql)


def test_get_post_comments_facebook_includes_hashtags_and_mentions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "fb-db-1",
            "source_id": "fb-1",
            "author": "bravo",
            "text": "Watch #RHOSLC with @BravoTV",
            "post_type": "feed",
            "likes": 10,
            "comments_count": 2,
            "shares": 1,
            "views": 20,
            "thumbnail_url": "https://img.test/fb.jpg",
            "ts": datetime(2026, 1, 1, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda _sql, _params: [])

    payload = get_post_comments("season-1", platform="facebook", source_id="fb-1")
    assert payload["hashtags"] == ["RHOSLC"]
    assert payload["mentions"] == ["@BravoTV"]


def test_get_post_comments_threads_includes_hashtags_and_mentions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "th-db-1",
            "source_id": "th-1",
            "author": "bravotv",
            "text": "Watch #RHOSLC with @BravoTV",
            "likes": 10,
            "replies_count": 2,
            "reposts": 1,
            "quotes": 1,
            "views": 20,
            "thumbnail_url": "https://img.test/th.jpg",
            "raw_data": {"activity": {"topic_path": "bravotv > rhoslc"}},
            "ts": datetime(2026, 1, 1, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda _sql, _params: [])
    monkeypatch.setattr(
        social_repo,
        "get_season_context",
        lambda _season_id: SeasonContext(
            season_id="season-1",
            show_id="show-1",
            show_name="The Real Housewives of Salt Lake City",
            season_number=6,
            anchor_date=date(2026, 1, 1),
            show_slug="rhoslc",
        ),
    )
    monkeypatch.setattr(
        social_repo, "_threads_build_relevance_terms", lambda *_args, **_kwargs: (["rhoslc"], ["rhoslc"])
    )

    payload = get_post_comments("season-1", platform="threads", source_id="th-1")
    assert payload["hashtags"] == ["RHOSLC"]
    assert payload["mentions"] == ["@BravoTV"]
    assert payload["topic"] == "bravotv > rhoslc"


def test_rows_for_platform_threads_filters_non_relevant_rows_for_rhoslc(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="The Real Housewives of Salt Lake City",
        season_number=6,
        anchor_date=date(2026, 1, 1),
        show_slug="rhoslc",
    )

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda _sql, _params: [
            {
                "platform": "threads",
                "kind": "post",
                "post_db_id": "post-1",
                "source_id": "th-1",
                "text": "generic bravo copy",
                "engagement": 10,
                "ts": datetime(2026, 1, 1, tzinfo=UTC),
                "url": "https://threads.com/@bravotv/post/th-1",
                "author": "bravotv",
                "thumbnail_url": None,
                "reported_comments": 2,
                "raw_data": {"activity": {"topic_path": "bravotv > rhoslc"}},
            },
            {
                "platform": "threads",
                "kind": "post",
                "post_db_id": "post-2",
                "source_id": "th-2",
                "text": "generic bravo copy",
                "engagement": 5,
                "ts": datetime(2026, 1, 1, tzinfo=UTC),
                "url": "https://threads.com/@bravotv/post/th-2",
                "author": "bravotv",
                "thumbnail_url": None,
                "reported_comments": 1,
                "raw_data": {"activity": {"topic_path": "bravotv > rhobh"}},
            },
            {
                "platform": "threads",
                "kind": "comment",
                "post_db_id": "post-1",
                "source_id": "comment-1",
                "text": "viewer comment",
                "engagement": 1,
                "ts": datetime(2026, 1, 1, tzinfo=UTC),
                "url": "https://threads.com/@bravotv/post/th-1",
                "author": "viewer",
                "thumbnail_url": None,
                "reported_comments": 0,
                "raw_data": {"activity": {"topic_path": "bravotv > rhoslc"}},
            },
            {
                "platform": "threads",
                "kind": "comment",
                "post_db_id": "post-2",
                "source_id": "comment-2",
                "text": "viewer comment",
                "engagement": 1,
                "ts": datetime(2026, 1, 1, tzinfo=UTC),
                "url": "https://threads.com/@bravotv/post/th-2",
                "author": "viewer",
                "thumbnail_url": None,
                "reported_comments": 0,
                "raw_data": {"activity": {"topic_path": "bravotv > rhobh"}},
            },
        ],
    )
    monkeypatch.setattr(
        social_repo, "_threads_build_relevance_terms", lambda *_args, **_kwargs: (["rhoslc"], ["rhoslc"])
    )

    rows = _rows_for_platform(
        "season-1",
        platform="threads",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 8, tzinfo=UTC),
        source_scope="bravo",
        target_accounts_by_platform={"threads": {"bravotv"}},
        season_context=context,
    )
    assert [row["source_id"] for row in rows] == ["th-1", "comment-1"]


def test_get_post_comments_twitter_returns_separate_quotes_payload(monkeypatch) -> None:
    captured_sql: dict[str, str] = {}

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
        normalized = " ".join(sql.lower().split())
        assert "{hosted_media_expr}" not in sql
        if "with recursive thread_replies as" in sql:
            captured_sql["replies"] = normalized
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
            captured_sql["quotes"] = normalized
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
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "twitter_tweets")

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
    assert "coalesce(is_missing, false) = false" in captured_sql["replies"]
    assert "coalesce(child.is_missing, false) = false" in captured_sql["replies"]
    assert "coalesce(t.is_missing, false) = false" in captured_sql["quotes"]


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


def test_week_detail_twitter_user_avatar_uses_raw_data_fallback(monkeypatch) -> None:
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
                    "hashtags": [],
                    "mentions": [],
                    "media_urls": [],
                    "thumbnail_url": None,
                    "user_avatar_url": None,
                    "raw_data": {"user": {"profile_image_url_https": "https://images.test/twitter-avatar.jpg"}},
                    "ts": datetime(2025, 1, 1, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_count_stored_quotes", lambda tweet_ids: {"tweet-1": 0})

    payload = social_repo._week_detail_twitter(
        "season-1",
        start_dt=datetime(2025, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 2, tzinfo=UTC),
        account_handles={"bravotv"},
        max_comments=0,
    )

    assert payload["posts"][0]["user"]["avatar_url"] == "https://images.test/twitter-avatar.jpg"


def test_resolve_post_avatar_url_prefers_high_resolution_variant_from_raw_data() -> None:
    resolved = social_repo._resolve_post_avatar_url(
        hosted_avatar_url=None,
        direct_avatar_url="https://p19.tiktokcdn.com/avatar_thumb.jpeg",
        raw_data={"user": {"avatarLarger": "https://p19.tiktokcdn.com/avatar~tplv-tiktokx-cropcenter:1080:1080.jpeg"}},
    )

    assert resolved == "https://p19.tiktokcdn.com/avatar~tplv-tiktokx-cropcenter:1080:1080.jpeg"


def test_get_week_detail_summary_returns_totals_without_posts(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_get_week_detail(season_id: str, **kwargs: object) -> dict[str, object]:
        captured["season_id"] = season_id
        captured["kwargs"] = kwargs
        return {
            "week": {"week_index": 1, "label": "Week 1", "start": "a", "end": "b"},
            "season": {"season_id": season_id},
            "source_scope": "bravo",
            "platforms": {
                "instagram": {
                    "posts": [{"source_id": "ig-1"}],
                    "total_posts": 9,
                    "totals": {"posts": 9, "total_comments": 1, "total_engagement": 2},
                }
            },
            "totals": {"posts": 9},
            "meta": {"performance": {"total_duration_ms": 12}},
        }

    monkeypatch.setattr(social_repo, "get_week_detail", _fake_get_week_detail)

    payload = social_repo.get_week_detail_summary(
        "season-1",
        week_index=1,
        platforms=["instagram"],
        timezone="America/New_York",
        source_scope="bravo",
        max_comments_per_post=0,
        sort_field="posted_at",
        sort_dir="desc",
    )

    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["post_limit"] == 0
    assert kwargs["post_offset"] == 0
    assert payload["platforms"]["instagram"]["total_posts"] == 9
    assert payload["platforms"]["instagram"]["totals"]["posts"] == 9
    assert "posts" not in payload["platforms"]["instagram"]


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


def test_sync_youtube_video_comment_counts_excludes_missing_rows_when_supported(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext("cursor"))
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "youtube_comments")

    def _fake_fetch_all_with_cursor(cur, sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "video-1"}]

    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)

    updated = social_repo._sync_youtube_video_comment_counts(["video-1"])
    assert updated == 1
    sql = str(captured["sql"]).lower()
    assert "coalesce(c.is_missing, false) = false" in sql


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


def test_merge_twitter_metric_summaries_uses_max_per_metric() -> None:
    merged = social_repo._merge_twitter_metric_summaries(  # noqa: SLF001
        {
            "_source": "tweet_detail",
            "likes": 7000,
            "replies": 330,
            "retweets": 1700,
            "quotes": 40,
            "views": 600000,
            "media_urls": ["https://pbs.twimg.com/media/cover.jpg"],
        },
        {
            "_source": "public_summary",
            "likes": 7092,
            "replies": 338,
            "retweets": 1800,
            "quotes": 44,
            "views": 617900,
            "media_urls": ["https://video.twimg.com/ext_tw_video/clip.mp4"],
        },
    )

    assert merged is not None
    assert merged["likes"] == 7092
    assert merged["replies"] == 338
    assert merged["retweets"] == 1800
    assert merged["quotes"] == 44
    assert merged["views"] == 617900
    assert merged["metric_sources"] == {
        "likes": "public_summary",
        "replies": "public_summary",
        "retweets": "public_summary",
        "quotes": "public_summary",
        "views": "public_summary",
    }
    assert "https://video.twimg.com/ext_tw_video/clip.mp4" in merged["media_urls"]
    assert "https://pbs.twimg.com/media/cover.jpg" in merged["media_urls"]


def test_apply_twitter_public_summary_uses_non_empty_fields(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_platform_posts_has_column", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext("cursor"))
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"media_urls": ["https://pbs.twimg.com/media/cover.jpg"]},
    )

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
    assert "media_urls = %s::jsonb" in sql
    assert "where tweet_id = %s and is_reply = false" in sql
    params = captured["params"]
    assert params[-1] == "1962923513301639212"
    assert any("https://x.com/BravoTV" == p for p in params)
    assert any(
        '"https://pbs.twimg.com/media/cover.jpg"' in str(p)
        and '"https://video.twimg.com/amplify_video/test.mp4"' in str(p)
        for p in params
    )


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


def test_mark_missing_comments_for_anchor_twitter_includes_quotes(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "twitter_tweets")
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext("cur"))

    def _fake_fetch_all_with_cursor(cur, sql: str, params: list[object]):
        del cur
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "row-1"}]

    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)

    marked = social_repo._mark_missing_comments_for_anchor(
        platform="twitter",
        anchor_id="root-1",
        observed_comment_ids={"reply-1", "quote-1"},
    )

    assert marked == 1
    normalized_sql = " ".join(str(captured.get("sql") or "").split()).lower()
    assert "(is_reply = true and reply_to_tweet_id = %s)" in normalized_sql
    assert "(is_quote = true and quoted_tweet_id = %s)" in normalized_sql
    assert "and tweet_id not in (%s,%s)" in normalized_sql
    assert captured["params"][:2] == ["root-1", "root-1"]


def test_platform_comment_media_needs_mirror_twitter_flags_html_hosted_media(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "_expected_cdn_host", lambda: "cdn.test")
    assert social_repo._twitter_comment_needs_media_mirror(
        {
            "media_urls": ["https://pbs.twimg.com/media/comment.jpg"],
            "hosted_media_urls": ["https://cdn.test/social/twitter/comment/media-01.html"],
            "media_mirror_status": "mirrored",
        }
    )


def test_reconcile_post_comment_count_twitter_does_not_decrease_root_counts(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def _fake_fetch_one_with_cursor(cur, sql: str, params: list[object]):
        del cur
        normalized = " ".join(sql.lower().split())
        if "from social.twitter_tweets where reply_to_tweet_id = %s and is_reply = true" in normalized:
            return {"active_count": 5}
        if "from social.twitter_tweets where quoted_tweet_id = %s and is_quote = true" in normalized:
            return {"active_count": 3}
        if "from social.twitter_tweets where tweet_id = %s and is_reply = false" in normalized:
            return {"replies_count": 20, "quotes": 11}
        if "update social.twitter_tweets set replies_count = %s, quotes = %s" in normalized:
            calls.append((sql, params))
            return {"id": "noop"}
        return None

    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext("cur"))
    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)

    total = social_repo._reconcile_post_comment_count(platform="twitter", post_db_id="tweet-root-1")
    assert total == 31
    assert calls == []


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


def test_refresh_youtube_post_detail_sync_derives_duration_from_iso_when_numeric_missing(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    captured: dict[str, Any] = {}

    class _FakeYouTubeScraper:
        def fetch_transcript(self, _source_id: str):
            return {
                "text": "",
                "segments": [],
                "language": None,
                "source": None,
                "error": "captions_unavailable",
            }

    monkeypatch.setattr("trr_backend.socials.youtube.YouTubeScraper", _FakeYouTubeScraper)
    monkeypatch.setattr(social_repo, "_youtube_fetch_single_video_metadata", lambda _video_id: {"duration": 0})
    monkeypatch.setattr(social_repo, "_youtube_transcript_ingest_enabled", lambda: False)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_upsert_youtube_video",
        lambda _context, **kwargs: (
            captured.setdefault("value", int(getattr(kwargs["video"], "duration_seconds", 0))),
            captured.setdefault("job_id", kwargs.get("job_id")),
            {"id": "db-yt-1"},
        )[2],
    )

    result = social_repo._refresh_youtube_post_detail_sync(
        context,
        source_id="vid123",
        account="bravo",
        row_json={
            "duration_seconds": 0,
            "duration": "PT1M32S",
            "published_at": datetime(2025, 1, 1, tzinfo=UTC),
            "is_short": False,
        },
        detail_job_id=None,
    )

    assert result["status"] == "success"
    assert captured["value"] == 92
    assert captured["job_id"] is None


def test_refresh_post_returns_detail_sync_media_and_comment_gap(monkeypatch) -> None:
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
        social_repo,
        "_refresh_load_platform_post_identity",
        lambda *_args, **_kwargs: {
            "id": "yt-db-1",
            "account": "bravo",
            "row_json": {"comments_count": 70},
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_refresh_post_detail_sync",
        lambda *_args, **_kwargs: {
            "status": "success",
            "post_id": "yt-db-1",
            "source": "yt_dlp_single_video",
        },
    )
    monkeypatch.setattr(
        social_repo,
        "refresh_post_comments",
        lambda *_args, **_kwargs: {
            "platform": "youtube",
            "source_id": "abc123",
            "comments_fetched": 60,
            "comments_upserted": 60,
            "total_comments_in_db": 60,
            "is_complete": False,
            "incomplete_reason": "count_gap",
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_refresh_post_media_sync",
        lambda *_args, **_kwargs: {
            "status": "success",
            "processed": 1,
            "mirrored_assets": 1,
            "error": None,
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_refresh_comment_gap_summary",
        lambda *_args, **_kwargs: {
            "reported": 70,
            "saved": 60,
            "is_complete": False,
            "reason": "count_gap",
        },
    )

    payload = social_repo.refresh_post(
        "season-1",
        platform="youtube",
        source_id="abc123",
        max_comments_per_post=500,
        fetch_replies=True,
    )

    assert payload["comments_upserted"] == 60
    assert payload["detail_sync"]["detail"]["status"] == "success"
    assert payload["detail_sync"]["comments"]["status"] == "incomplete"
    assert payload["detail_sync"]["comments"]["reason"] == "count_gap"
    assert payload["detail_sync"]["media"]["status"] == "success"
    assert payload["comment_gap"] == {
        "reported": 70,
        "saved": 60,
        "is_complete": False,
        "reason": "count_gap",
    }
    assert "warnings" not in payload


def test_refresh_post_collects_warning_when_detail_or_media_sync_fails(monkeypatch) -> None:
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
        social_repo,
        "_refresh_load_platform_post_identity",
        lambda *_args, **_kwargs: {
            "id": "yt-db-1",
            "account": "bravo",
            "row_json": {},
        },
    )

    def _raise_detail(*_args, **_kwargs):
        raise RuntimeError("detail_boom")

    def _raise_media(*_args, **_kwargs):
        raise RuntimeError("media_boom")

    monkeypatch.setattr(social_repo, "_refresh_post_detail_sync", _raise_detail)
    monkeypatch.setattr(
        social_repo,
        "refresh_post_comments",
        lambda *_args, **_kwargs: {
            "platform": "youtube",
            "source_id": "abc123",
            "comments_fetched": 70,
            "comments_upserted": 70,
            "total_comments_in_db": 70,
            "is_complete": True,
            "incomplete_reason": None,
        },
    )
    monkeypatch.setattr(social_repo, "_refresh_post_media_sync", _raise_media)
    monkeypatch.setattr(
        social_repo,
        "_refresh_comment_gap_summary",
        lambda *_args, **_kwargs: {
            "reported": 70,
            "saved": 70,
            "is_complete": True,
            "reason": None,
        },
    )

    payload = social_repo.refresh_post(
        "season-1",
        platform="youtube",
        source_id="abc123",
        max_comments_per_post=500,
        fetch_replies=True,
    )

    assert payload["detail_sync"]["detail"]["status"] == "failed"
    assert "detail_boom" in str(payload["detail_sync"]["detail"]["error"])
    assert payload["detail_sync"]["media"]["status"] == "failed"
    assert "media_boom" in str(payload["detail_sync"]["media"]["error"])
    assert payload["detail_sync"]["comments"]["status"] == "success"
    assert payload["comment_gap"]["is_complete"] is True
    assert payload["warnings"] == [
        "detail_sync_failed:RuntimeError",
        "media_sync_failed:RuntimeError",
    ]


def test_refresh_threads_post_detail_sync_upserts_without_job_fk(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_load_threads_cookies", lambda: {"csrftoken": "token"})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_count_stored_comments", lambda *_args, **_kwargs: {"thread-db-1": 6})

    class _FakeThreadsScraper:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def scrape_post(self, *_args, **_kwargs):
            post = SimpleNamespace(
                post_id="DNVtpvYM3yI",
                username="bravotv",
                text="Your prayers have been answered. #RHOSLC is back",
                media_urls=[],
                thumbnail_url=None,
                likes=0,
                replies=0,
                reposts=0,
                quotes=0,
                views=6829,
                posted_at=1723593600,
                raw_data={},
                to_dict=lambda: {},
            )
            return post, []

    monkeypatch.setattr("trr_backend.socials.threads.ThreadsScraper", _FakeThreadsScraper)

    def _fake_upsert(*_args, **kwargs):
        captured["job_id"] = kwargs.get("job_id")
        captured["account"] = kwargs.get("account")
        post = kwargs.get("post")
        captured["likes"] = getattr(post, "likes", None)
        captured["replies"] = getattr(post, "replies", None)
        captured["reposts"] = getattr(post, "reposts", None)
        captured["quotes"] = getattr(post, "quotes", None)
        captured["topic"] = (getattr(post, "raw_data", {}) or {}).get("topic")
        return {"id": "thread-db-1"}

    monkeypatch.setattr(social_repo, "_upsert_meta_threads_post", _fake_upsert)

    result = social_repo._refresh_threads_post_detail_sync(  # noqa: SLF001
        context,
        source_id="DNVtpvYM3yI",
        account="bravotv",
        row_json={
            "id": "thread-db-1",
            "url": "https://www.threads.com/@bravotv/post/DNVtpvYM3yI",
            "likes": 186,
            "replies_count": 6,
            "reposts": 11,
            "quotes": 2,
            "views": 100,
            "raw_data": {"topic": "bravotv > rhoslc"},
        },
        detail_job_id=None,
    )

    assert result["status"] == "success"
    assert captured["job_id"] is None
    assert captured["account"] == "bravotv"
    assert captured["likes"] == 186
    assert captured["replies"] == 6
    assert captured["reposts"] == 11
    assert captured["quotes"] == 2
    assert captured["topic"] == "bravotv > rhoslc"


def test_upsert_youtube_video_omits_job_id_when_none_to_preserve_existing_fk(monkeypatch: pytest.MonkeyPatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    captured_payload: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_platform_posts_has_column", lambda *_args, **_kwargs: False)

    def _fake_pg_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn=None):
        captured_payload["table"] = table
        captured_payload["payload"] = dict(payload)
        captured_payload["conflict_col"] = conflict_col
        return {"id": "yt-db-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_pg_upsert)

    video = SimpleNamespace(
        video_id="vid123",
        channel_id="chan-1",
        channel_title="Bravo",
        title="Test title",
        description="Test desc",
        duration="PT30S",
        duration_seconds=30,
        views=100,
        likes=10,
        comments=5,
        thumbnail_url="https://example.com/thumb.jpg",
        published_at=1735689600,
        tags=["RHOSLC"],
        to_dict=lambda: {"video_id": "vid123"},
    )

    result = social_repo._upsert_youtube_video(
        context,
        job_id=None,
        account="bravo",
        video=video,
        conn=None,
    )

    assert result == {"id": "yt-db-1"}
    assert captured_payload["table"] == "youtube_videos"
    assert captured_payload["conflict_col"] == "video_id"
    payload = captured_payload["payload"]
    assert isinstance(payload, dict)
    assert "job_id" not in payload


def test_refresh_post_uses_null_detail_job_id_and_trace_id_for_media(monkeypatch: pytest.MonkeyPatch) -> None:
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
        social_repo,
        "_refresh_load_platform_post_identity",
        lambda *_args, **_kwargs: {
            "id": "yt-db-1",
            "account": "bravo",
            "row_json": {"comments_count": 70},
        },
    )

    captured: dict[str, object] = {}

    def _fake_detail(*_args, **kwargs):
        captured["detail_job_id"] = kwargs.get("detail_job_id")
        return {"status": "success", "post_id": "yt-db-1", "source": "yt_dlp_single_video"}

    def _fake_media(*_args, **kwargs):
        captured["media_job_id"] = kwargs.get("refresh_job_id")
        return {"status": "success", "processed": 1, "mirrored_assets": 1, "error": None}

    monkeypatch.setattr(social_repo, "_refresh_post_detail_sync", _fake_detail)
    monkeypatch.setattr(social_repo, "_refresh_post_media_sync", _fake_media)
    monkeypatch.setattr(
        social_repo,
        "refresh_post_comments",
        lambda *_args, **_kwargs: {
            "platform": "youtube",
            "source_id": "abc123",
            "comments_fetched": 60,
            "comments_upserted": 60,
            "total_comments_in_db": 60,
            "is_complete": False,
            "incomplete_reason": "count_gap",
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_refresh_comment_gap_summary",
        lambda *_args, **_kwargs: {
            "reported": 70,
            "saved": 60,
            "is_complete": False,
            "reason": "count_gap",
        },
    )

    payload = social_repo.refresh_post(
        "season-1",
        platform="youtube",
        source_id="abc123",
        max_comments_per_post=500,
        fetch_replies=True,
    )

    assert payload["detail_sync"]["detail"]["status"] == "success"
    assert "warnings" not in payload
    assert captured["detail_job_id"] is None
    media_job_id = str(captured.get("media_job_id") or "")
    assert media_job_id


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


def test_refresh_post_comments_twitter_enqueues_comment_media_mirror_for_replies_and_quotes(monkeypatch) -> None:
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
        lambda _sql, _params: {"tweet_id": "123", "account": "bravotv", "replies_count": 1, "quotes": 1},
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(social_repo, "_count_stored_replies", lambda *_args, **_kwargs: {"123": 1})
    monkeypatch.setattr(social_repo, "_count_stored_quotes", lambda *_args, **_kwargs: {"123": 1})
    monkeypatch.setattr(social_repo, "_load_twitter_auth", lambda: ({}, "bearer"))
    monkeypatch.setattr(social_repo, "_load_twikit_credentials", lambda: {})
    monkeypatch.setattr(
        social_repo, "_fetch_and_apply_twitter_metric_summary", lambda **_kwargs: {"replies": 1, "quotes": 1}
    )
    monkeypatch.setattr(social_repo, "_mark_missing_comments_for_anchor", lambda **_kwargs: 0)
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **_kwargs: None)

    class _FakeTwitterScraper:
        comments_auth_failed = False
        last_reply_fetch_reason = ""
        last_quote_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def fetch_tweet_replies(self, *args, **kwargs):
            del args, kwargs
            return [SimpleNamespace(tweet_id="reply-1", reply_to_tweet_id="123", is_reply=True)]

        def fetch_tweet_quotes(self, *args, **kwargs):
            del args, kwargs
            return [SimpleNamespace(tweet_id="quote-1", quoted_tweet_id="123", is_quote=True)]

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", _FakeTwitterScraper)
    monkeypatch.setattr(
        social_repo,
        "_upsert_tweet",
        lambda *_args, tweet, **_kwargs: {
            "id": str(getattr(tweet, "tweet_id", "") or ""),
            "tweet_id": str(getattr(tweet, "tweet_id", "") or ""),
            "media_urls": ["https://pbs.twimg.com/media/item.jpg"],
            "hosted_media_urls": [],
            "media_mirror_status": "pending",
            "reply_to_tweet_id": str(getattr(tweet, "reply_to_tweet_id", "") or ""),
            "quoted_tweet_id": str(getattr(tweet, "quoted_tweet_id", "") or ""),
            "is_reply": bool(getattr(tweet, "is_reply", False)),
            "is_quote": bool(getattr(tweet, "is_quote", False)),
        },
    )
    enqueued_for: list[str] = []
    monkeypatch.setattr(
        social_repo,
        "_enqueue_twitter_comment_media_mirror_job",
        lambda *_args, comment_row, **_kwargs: (
            enqueued_for.append(str(comment_row.get("tweet_id") or "")) or f"job-{comment_row.get('tweet_id')}"
        ),
    )

    payload = social_repo.refresh_post_comments(
        "season-1",
        platform="twitter",
        source_id="123",
        max_comments_per_post=100,
        fetch_replies=True,
    )

    assert payload["comments_upserted"] == 1
    assert payload["quotes_upserted"] == 1
    assert payload["comment_media_mirror_jobs_enqueued"] == 2
    assert payload["comment_media_mirror_job_enqueue_errors"] == 0
    assert enqueued_for == ["reply-1", "quote-1"]


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
    assert "offset %s" in sql
    assert params == ["season-1", "running", "youtube", 100, 0]


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
    assert "offset %s" in sql
    assert params == ["season-1", "123e4567-e89b-12d3-a456-426614174000", "queued", "instagram", 50, 0]


def test_list_jobs_applies_offset(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    social_repo.list_jobs("season-1", limit=25, offset=50)

    sql = " ".join(str(captured["sql"]).lower().split())
    assert "offset %s" in sql
    assert captured["params"] == ["season-1", 25, 50]


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
    assert not social_repo._is_comment_fetch_complete(
        fetch_failed=False,
        fail_reason=None,
        quote_fail_reason="rate_limited",
        auth_failed=False,
        fetched_count=12,
        quotes_fetched=0,
        max_comments_per_post=1000,
        expected_count=42,
        expected_quotes=8,
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


def test_upsert_tiktok_comment_tree_without_run_id_preserves_last_seen_run(monkeypatch) -> None:
    captured_payload: dict[str, object] = {}

    class _Comment:
        comment_id = "comment-1"
        username = "viewer"
        user_id = "user-1"
        nickname = "Viewer"
        text = "hello"
        likes = 3
        is_reply = False
        reply_count = 0
        created_at = datetime(2026, 2, 10, tzinfo=UTC)
        replies: list[object] = []
        media_urls: list[str] = []

        def to_dict(self) -> dict[str, object]:
            return {"comment_id": self.comment_id}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        captured_payload.update(payload)
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "tiktok_comments")
    monkeypatch.setattr(social_repo, "_tiktok_comments_has_column", lambda _column: False)
    monkeypatch.setattr(social_repo, "_enqueue_tiktok_comment_media_mirror_job", lambda **_kwargs: None)

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )

    written = social_repo._upsert_tiktok_comment_tree(
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


def test_upsert_youtube_comment_tree_without_run_id_preserves_last_seen_run(monkeypatch) -> None:
    captured_payload: dict[str, object] = {}

    class _Comment:
        comment_id = "comment-1"
        author = "viewer"
        author_channel_id = "channel-1"
        text = "hello"
        likes = 3
        is_reply = False
        reply_count = 0
        published_at = datetime(2026, 2, 10, tzinfo=UTC)
        replies: list[object] = []

        def to_dict(self) -> dict[str, object]:
            return {"comment_id": self.comment_id}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        captured_payload.update(payload)
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "youtube_comments")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )

    written = social_repo._upsert_youtube_comment_tree(
        context,
        job_id="job-1",
        run_id=None,
        account="bravotv",
        video_db_id="video-1",
        comment=_Comment(),
    )

    assert written == 1
    assert "last_seen_run_id" not in captured_payload
    assert captured_payload["is_missing"] is False


def test_upsert_tweet_without_run_id_preserves_last_seen_run(monkeypatch) -> None:
    captured_payload: dict[str, object] = {}

    class _Tweet:
        tweet_id = "tweet-1"
        username = "bravotv"
        display_name = "Bravo"
        user_verified = False
        text = "tweet body"
        hashtags = ["rhoslc"]
        mentions = []
        media_urls = []
        likes = 3
        retweets = 1
        replies = 2
        quotes = 0
        views = 50
        is_reply = False
        is_retweet = False
        is_quote = False
        reply_to_tweet_id = None
        quoted_tweet_id = None
        created_at = datetime(2026, 2, 10, tzinfo=UTC)
        user_profile_url = ""
        user_avatar_url = ""

        def to_dict(self) -> dict[str, object]:
            return {"tweet_id": self.tweet_id}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        captured_payload.update(payload)
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "twitter_tweets")
    monkeypatch.setattr(social_repo, "_platform_posts_has_column", lambda _platform, _column: True)

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )

    row = social_repo._upsert_tweet(
        context,
        job_id="job-1",
        run_id=None,
        account="bravotv",
        tweet=_Tweet(),
    )

    assert row == {"id": "row-1"}
    assert "last_seen_run_id" not in captured_payload
    assert captured_payload["is_missing"] is False


def test_upsert_tweet_batch_uses_bulk_upsert_and_dedupes(monkeypatch) -> None:
    captured_chunks: list[list[str]] = []

    def _fake_bulk(
        table: str,
        payloads: list[dict[str, object]],
        *,
        conflict_col: str,
        conn: object | None = None,
    ) -> list[dict[str, object]]:
        assert table == "twitter_tweets"
        assert conflict_col == "tweet_id"
        assert conn is not None
        chunk_ids = [str(item.get("tweet_id") or "") for item in payloads]
        captured_chunks.append(chunk_ids)
        return [{"id": f"row-{tweet_id}", "tweet_id": tweet_id} for tweet_id in chunk_ids]

    monkeypatch.setattr(social_repo, "_pg_upsert_many", _fake_bulk)
    monkeypatch.setattr(social_repo, "_platform_posts_has_column", lambda _platform, _column: True)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda _table: True)

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    tweets = [
        SimpleNamespace(tweet_id="tweet-1", text="one"),
        SimpleNamespace(tweet_id="tweet-2", text="two"),
        SimpleNamespace(tweet_id="tweet-1", text="one-latest"),
    ]
    stats = social_repo._new_comment_persist_stats()

    rows = social_repo._upsert_tweet_batch(
        context,
        job_id="job-1",
        run_id="run-1",
        account="bravotv",
        tweets=tweets,
        persist_stats=stats,
        conn=object(),
        batch_size=10,
    )

    assert captured_chunks == [["tweet-1", "tweet-2"]]
    assert set(rows.keys()) == {"tweet-1", "tweet-2"}
    assert stats["comments_fetched"] == 3
    assert stats["comments_upserted"] == 2
    assert stats["comments_skipped_missing_id"] == 0


def test_upsert_tweet_batch_falls_back_to_single_upsert_when_conn_missing(monkeypatch) -> None:
    called_ids: list[str] = []

    def _fake_upsert(
        _context: SeasonContext,
        *,
        job_id: str | None,
        run_id: str | None,
        account: str,
        tweet: Any,
        persist_stats: dict[str, int] | None = None,
        conn: object | None = None,
    ) -> dict[str, object] | None:
        del job_id, run_id, account
        tweet_id = str(getattr(tweet, "tweet_id", "") or "")
        called_ids.append(tweet_id)
        if persist_stats is not None:
            persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1
            if tweet_id:
                persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1
            else:
                persist_stats["comments_skipped_missing_id"] = (
                    int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
                )
        if not tweet_id:
            return None
        return {"id": f"row-{tweet_id}", "tweet_id": tweet_id, "conn": conn}

    monkeypatch.setattr(social_repo, "_upsert_tweet", _fake_upsert)

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    stats = social_repo._new_comment_persist_stats()
    rows = social_repo._upsert_tweet_batch(
        context,
        job_id="job-1",
        run_id="run-1",
        account="bravotv",
        tweets=[SimpleNamespace(tweet_id="tweet-1"), SimpleNamespace(tweet_id=""), SimpleNamespace(tweet_id="tweet-2")],
        persist_stats=stats,
        conn=None,
    )

    assert called_ids == ["tweet-1", "", "tweet-2"]
    assert set(rows.keys()) == {"tweet-1", "tweet-2"}
    assert stats["comments_fetched"] == 3
    assert stats["comments_upserted"] == 2
    assert stats["comments_skipped_missing_id"] == 1


def test_upsert_facebook_comment_tree_without_run_id_preserves_last_seen_run(monkeypatch) -> None:
    captured_payload: dict[str, object] = {}

    class _Comment:
        comment_id = "comment-1"
        parent_source_comment_id = None
        username = "viewer"
        text = "hello"
        likes = 3
        is_reply = False
        reply_count = 0
        created_at = datetime(2026, 2, 10, tzinfo=UTC)
        replies: list[object] = []
        media_urls: list[str] = []

        def to_dict(self) -> dict[str, object]:
            return {"comment_id": self.comment_id}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        captured_payload.update(payload)
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "facebook_comments")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )

    written = social_repo._upsert_facebook_comment_tree(
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


def test_upsert_meta_threads_comment_tree_without_run_id_preserves_last_seen_run(monkeypatch) -> None:
    captured_payload: dict[str, object] = {}

    class _Comment:
        comment_id = "comment-1"
        parent_source_comment_id = None
        username = "viewer"
        text = "hello"
        likes = 3
        is_reply = False
        reply_count = 0
        created_at = datetime(2026, 2, 10, tzinfo=UTC)
        replies: list[object] = []
        media_urls: list[str] = []

        def to_dict(self) -> dict[str, object]:
            return {"comment_id": self.comment_id}

    def _fake_upsert(table: str, payload: dict[str, object], *, conflict_col: str, conn: object | None = None):
        captured_payload.update(payload)
        return {"id": "row-1"}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "meta_threads_comments")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )

    written = social_repo._upsert_meta_threads_comment_tree(
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
    run_id = "11111111-1111-4111-8111-111111111111"
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
        return run_id

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

    assert payload["run_id"] == run_id
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
    assert len(created_job_configs) == 2
    assert all(config["stage"] == "comments" for config in created_job_configs)
    assert all(config["max_posts_per_target"] == 0 for config in created_job_configs)

    captured_run_configs.clear()
    created_job_configs.clear()

    with pytest.raises(social_repo.SocialIngestValidationError) as exc_info:
        social_repo.ingest_season(
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
    assert exc_info.value.code == "NO_INGEST_TARGETS"
    assert created_job_configs == []


def test_ingest_season_creates_sharded_posts_for_adaptive_dual_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = "11111111-1111-4111-8111-111111111111"
    context = SeasonContext(
        season_id="season-sharded-run",
        show_id="show-sharded-run",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2026, 1, 1),
    )
    captured_run_configs: list[dict[str, object]] = []
    created_job_configs: list[dict[str, object]] = []

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(
        social_repo,
        "get_targets",
        lambda *_args, **_kwargs: {
            "targets": [
                {
                    "platform": "instagram",
                    "accounts": ["creatoracct"],
                    "hashtags": [],
                    "keywords": [],
                    "is_active": True,
                }
            ]
        },
    )

    def _fake_create_run(*_args, **kwargs):
        captured_run_configs.append(kwargs["config"])
        return run_id

    def _fake_create_job(*_args, **kwargs):
        created_job_configs.append(kwargs["config"])
        return f"job-{len(created_job_configs)}"

    monkeypatch.setattr(social_repo, "_create_run", _fake_create_run)
    monkeypatch.setattr(social_repo, "_create_job", _fake_create_job)
    monkeypatch.setattr(social_repo, "_update_run_summary", lambda _run_id: {"total_jobs": len(created_job_configs)})
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)

    payload = social_repo.ingest_season(
        context.season_id,
        platforms=["instagram"],
        source_scope="creator",
        sync_strategy="incremental",
        max_posts_per_target=100,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
        date_start=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        date_end=datetime(2026, 1, 1, 5, 59, tzinfo=UTC),
        runner_strategy="adaptive_dual_runner",
        runner_count=2,
        window_shard_hours=2,
        day_weight_profile="rhoslc_default",
        priority_mode="episode_peak_weighted",
        initiated_by="admin@test",
    )

    assert payload["run_id"] == run_id
    assert payload["queued_or_started_jobs"] == 3
    assert captured_run_configs[0]["runner_strategy"] == "adaptive_dual_runner"
    assert captured_run_configs[0]["runner_count"] == 2
    shard_indexes = sorted(int(config.get("shard_index")) for config in created_job_configs)
    assert shard_indexes == [0, 1, 2]
    assert all(config["stage"] == "posts" for config in created_job_configs)
    assert all(config.get("window_start") for config in created_job_configs)
    assert all(config.get("window_end") for config in created_job_configs)
    assert {"A", "B"} <= {str(config.get("runner_lane") or "") for config in created_job_configs}


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
    monkeypatch.setattr(social_repo, "_finalize_run_status", lambda run_id, **_kwargs: finalized.append(run_id) or {})

    recovered = social_repo.recover_stale_running_jobs(run_id=None, stage=None, stale_after_seconds=300)

    assert [row["id"] for row in recovered] == ["job-1", "job-2"]
    assert cleared == ["job-1", "job-2"]
    assert finalized == ["run-1", "run-2"]
    sql_text = str(captured_sql.get("sql") or "")
    params = captured_sql.get("params")
    assert isinstance(params, list)
    assert _count_unescaped_placeholders(sql_text) == len(params)
    assert "claimed_at = null" in sql_text
    assert "worker_id = null" in sql_text


def test_finish_job_retrying_releases_claim_ownership(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_calls: list[tuple[str, list[object]]] = []
    increments: list[dict[str, Any]] = []
    cleared: list[str] = []

    def _fake_fetch_one(sql: str, params: list[object]) -> dict[str, Any]:
        captured_calls.append((sql, list(params)))
        if "update social.scrape_jobs" in sql:
            return {
                "id": "job-1",
                "run_id": "run-1",
                "prior_status": "running",
                "prior_items_found": 5,
                "stage": "comments",
            }
        if "to_regclass" in sql:
            return {"exists": True}
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(
        social_repo,
        "_increment_run_counters_on_job_finish",
        lambda **kwargs: increments.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_clear_worker_heartbeat_for_job",
        lambda *, job_id, status="idle", metadata=None: cleared.append(job_id),
    )

    social_repo._finish_job(
        "job-1",
        status="retrying",
        items_found=11,
        error_message="temporary error",
        metadata={"stage": "comments"},
        last_error_code="network",
        last_error_class="NetworkError",
        next_available_at=datetime.now(UTC) + timedelta(seconds=30),
    )

    update_calls = [(sql, params) for sql, params in captured_calls if "update social.scrape_jobs" in sql]
    assert update_calls
    sql_text = str(update_calls[0][0])
    params = list(update_calls[0][1] or [])
    assert "worker_id = case when %s = 'retrying' then null else worker_id end" in sql_text
    assert "claimed_at = case when %s = 'retrying' then null else claimed_at end" in sql_text
    assert params[-2:] == ["retrying", "retrying"]
    assert increments
    assert increments[0]["new_status"] == "retrying"
    assert cleared == []


def test_claim_next_jobs_uses_batch_limit_and_run_fairness(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]):
        captured["sql"] = sql
        captured["params"] = list(params)
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_resolve_run_in_flight_cap", lambda: 4)

    rows = social_repo._claim_next_jobs(
        worker_id="worker-1",
        run_id=None,
        stage="comments",
        platform="twitter",
        limit=500,
    )

    assert rows == []
    sql_text = str(captured.get("sql") or "")
    params = list(captured.get("params") or [])
    assert "run_in_flight" in sql_text
    assert "row_number() over" in sql_text
    assert "coalesce(rif.in_flight, 0) < %s" in sql_text
    assert params[-3] == 4  # run in-flight cap
    assert params[-2] == 25  # capped batch limit
    assert params[-1] == "worker-1"


def test_claim_next_queued_jobs_treats_empty_platform_filter_as_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_claim_next_jobs(*, worker_id=None, run_id=None, stage=None, platform=None, limit=1):  # noqa: ANN001
        captured["worker_id"] = worker_id
        captured["run_id"] = run_id
        captured["stage"] = stage
        captured["platform"] = platform
        captured["limit"] = limit
        return []

    monkeypatch.setattr(social_repo, "_claim_next_jobs", _fake_claim_next_jobs)

    rows = social_repo.claim_next_queued_jobs(worker_id="worker-1", stage=None, platform=None, run_id="run-1")

    assert rows == []
    assert captured["worker_id"] == "worker-1"
    assert captured["run_id"] == "run-1"
    assert captured["stage"] is None
    assert captured["platform"] is None
    assert captured["limit"] == 5  # default batch size


def test_fetch_next_preclaimed_job_allows_worker_prefix_match(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_one(sql: str, params: list[object]):
        captured["sql"] = sql
        captured["params"] = list(params)
        return None

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    job = social_repo._fetch_next_preclaimed_job(
        worker_id="api-background:twitter",
        run_id="run-1",
        stage="comments",
        platform="twitter",
    )

    assert job is None
    sql_text = str(captured.get("sql") or "")
    params = list(captured.get("params") or [])
    assert "worker_id = %s" in sql_text
    assert "like worker_id || '%%'" in sql_text
    assert params[:4] == ["run-1", "api-background:twitter", "api-background:twitter", "api-background:twitter"]
    assert params[4:] == ["comments", "comments", "twitter", "twitter"]


def test_update_run_summary_prefers_incremental_counter_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(social_repo, "_run_counter_columns_ready", lambda: True)

    def _fake_fetch_one(sql: str, params: list[object]):  # noqa: ARG001
        normalized = " ".join(sql.lower().split())
        calls.append(normalized)
        if "from social.scrape_runs where id = %s" in normalized and "select total_jobs" in normalized:
            return {
                "total_jobs": 6,
                "completed_jobs": 3,
                "failed_jobs": 1,
                "active_jobs": 2,
                "items_found_total": 77,
                "stage_counts": {"posts": {"total": 3, "completed": 2, "failed": 0, "active": 1}},
            }
        if "update social.scrape_runs set summary = %s::jsonb" in normalized:
            return {"id": "run-1"}
        if "from social.scrape_jobs" in normalized:
            raise AssertionError("full scrape_jobs aggregation should not run in incremental mode")
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    summary = social_repo._update_run_summary("run-1")

    assert summary["total_jobs"] == 6
    assert summary["completed_jobs"] == 3
    assert summary["failed_jobs"] == 1
    assert summary["active_jobs"] == 2
    assert summary["items_found_total"] == 77
    assert summary["stage_counts"]["posts"]["active"] == 1
    assert any("select total_jobs" in call for call in calls)


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


def test_ingest_twitter_rhoslc_requires_explicit_hashtag(monkeypatch) -> None:
    upserted_ids: list[str] = []

    class _FakeTweet:
        def __init__(self, tweet_id: str, text: str, hashtags: list[str]) -> None:
            self.tweet_id = tweet_id
            self.text = text
            self.hashtags = hashtags
            self.username = "bravotv"
            self.display_name = "BravoTV"
            self.user_id = "u-1"
            self.user_profile_url = "https://x.com/bravotv"
            self.user_avatar_url = "https://images.test/bravotv.jpg"
            self.created_at = int(datetime(2025, 1, 2, tzinfo=UTC).timestamp())
            self.date_time = "2025-01-02 00:00:00"
            self.likes = 1
            self.retweets = 1
            self.replies = 0
            self.quotes = 0
            self.views = 0
            self.is_reply = False
            self.is_retweet = False
            self.is_quote = False
            self.reply_to_tweet_id = None
            self.quoted_tweet_id = None
            self.media_urls = []
            self.link_preview_media_count = 0

    class _FakeTwitterScraper:
        def __init__(self, *args, **kwargs) -> None:
            self.last_retrieval_meta = {"retrieval_mode": "graphql"}

        def scrape(self, _config, progress_cb=None):
            if progress_cb:
                progress_cb(
                    {"phase": "scrape_graphql_page", "pages_scanned": 1, "posts_checked": 2, "matched_posts": 2}
                )
            return [
                _FakeTweet("x-no-tag", "Salt Lake City cast dinner", []),
                _FakeTweet("x-with-tag", "Tonight on #RHOSLC", ["RHOSLC"]),
            ]

        def fetch_tweet_detail_summary(self, _tweet_id: str, delay: float = 0.0):
            del delay
            return None

        def fetch_public_tweet_summary(self, _tweet_id: str, delay: float = 0.0):
            del delay
            return None

    context = SeasonContext(
        season_id="season-rhoslc",
        show_id="show-rhoslc",
        show_name="The Real Housewives of Salt Lake City",
        show_slug="rhoslc",
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
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo, "_enqueue_platform_media_mirror_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_upsert_tweet",
        lambda *_args, **kwargs: (
            upserted_ids.append(str(getattr(kwargs.get("tweet"), "tweet_id", ""))),
            {"id": f"row-{getattr(kwargs.get('tweet'), 'tweet_id', 'unknown')}"},
        )[1],
    )

    _posts, _comments, meta = social_repo._ingest_twitter(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=["Salt Lake City"],
        opts=opts,
        job_id="job-1",
        include_reply_records=False,
        hydrate_audience_replies=False,
        stage="posts",
    )

    assert upserted_ids == ["x-with-tag"]
    assert (meta.get("persist_counters") or {}).get("posts_upserted") == 1
    assert meta.get("strict_rhoslc_hashtag_mode") is True


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


def test_ingest_instagram_posts_stage_passes_hashtags_and_matches_structured_tokens(monkeypatch) -> None:
    seen_scrape_hashtags: list[str] = []
    upserted_shortcodes: list[str] = []

    class _FakePost:
        shortcode = "abc123"
        caption = "Episode clip"
        hashtags = ["RHOSLC"]
        mentions = []
        collaborators = []
        profile_tags = []
        comments = 0
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
            seen_scrape_hashtags.extend(list(config.hashtags or []))
            return [_FakePost()]

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
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo, "_resolve_week_windows", lambda *_args, **_kwargs: ([], None))
    monkeypatch.setattr(social_repo, "_enrich_instagram_post_from_permalink", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_upsert_instagram_post",
        lambda _context, **kwargs: upserted_shortcodes.append(str(getattr(kwargs.get("post"), "shortcode", "")))
        or {"id": "post-db-1"},
    )

    posts, comments, meta = social_repo._ingest_instagram(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=[],
        opts=opts,
        job_id="job-1",
        stage="posts",
    )

    assert posts == 1
    assert comments == 0
    assert seen_scrape_hashtags == ["RHOSLC"]
    assert upserted_shortcodes == ["abc123"]
    assert (meta.get("persist_counters") or {}).get("posts_upserted") == 1


def test_ingest_instagram_details_refresh_prefers_gallery_views_when_detail_missing(monkeypatch) -> None:
    refreshed_calls: list[dict[str, object]] = []

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}

        def __init__(self, *args, **kwargs) -> None:
            pass

        def scrape_metrics_index(self, _config, progress_cb=None):
            return {
                "abc123": {
                    "likes": 90,
                    "comments": 11,
                    "views_observed": 2151,
                    "views_source": "node.overlay.play_count",
                    "views_raw_candidates": [{"source": "node.overlay.play_count", "raw": "2,151", "parsed": 2151}],
                }
            }

        def fetch_post_info(self, *_args, **_kwargs):
            return None

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
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="details_refresh",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda platform, *_args, **_kwargs: (
            [
                {
                    "id": "post-db-1",
                    "shortcode": "abc123",
                    "likes": 80,
                    "comments_count": 10,
                    "views": 500,
                    "posted_at": datetime(2026, 2, 15, tzinfo=UTC),
                }
            ]
            if platform == "instagram"
            else []
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_refresh_instagram_post_metrics_only",
        lambda **kwargs: refreshed_calls.append(kwargs),
    )

    posts, comments, meta = social_repo._ingest_instagram(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=[],
        opts=opts,
        job_id="job-1",
        stage="posts",
    )

    assert posts == 1
    assert comments == 0
    assert len(refreshed_calls) == 1
    call = refreshed_calls[0]
    assert call["views"] == 2151
    assert call["views_source"] == "node.overlay.play_count"
    assert meta.get("details_refresh_views_updated") == 1


def test_ingest_instagram_details_refresh_preserves_views_when_detail_has_no_view_candidate(monkeypatch) -> None:
    refreshed_calls: list[dict[str, object]] = []

    class _FakeInstagramScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}

        def __init__(self, *args, **kwargs) -> None:
            pass

        def scrape_metrics_index(self, _config, progress_cb=None):
            return {}

        def fetch_post_info(self, *_args, **_kwargs):
            return {"items": [{"id": "media-1"}]}

        def _parse_post_node(self, _node, _config):  # noqa: SLF001
            return SimpleNamespace(
                likes=91,
                comments=12,
                video_views=0,
                video_views_observed=None,
                video_views_source=None,
                video_views_raw_candidates=[],
            )

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
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="details_refresh",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    monkeypatch.setattr(social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda platform, *_args, **_kwargs: (
            [
                {
                    "id": "post-db-1",
                    "shortcode": "abc123",
                    "likes": 80,
                    "comments_count": 10,
                    "views": 500,
                    "posted_at": datetime(2026, 2, 15, tzinfo=UTC),
                }
            ]
            if platform == "instagram"
            else []
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_refresh_instagram_post_metrics_only",
        lambda **kwargs: refreshed_calls.append(kwargs),
    )

    posts, comments, meta = social_repo._ingest_instagram(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=[],
        opts=opts,
        job_id="job-1",
        stage="posts",
    )

    assert posts == 1
    assert comments == 0
    assert len(refreshed_calls) == 1
    call = refreshed_calls[0]
    assert call["views"] is None
    assert meta.get("details_refresh_views_preserved_missing") == 1


def test_ingest_tiktok_posts_stage_passes_hashtags_and_matches_structured_tokens(monkeypatch) -> None:
    seen_scrape_hashtags: list[str] = []
    upserted_video_ids: list[str] = []
    seen_sound_ids: list[str] = []

    class _FakePost:
        video_id = "vid-1"
        aweme_id = "vid-1"
        username = "bravotv"
        author_nickname = "Bravo"
        description = "Episode clip"
        hashtags = ["RHOSLC"]
        mentions = []
        media_urls: list[str] = []
        thumbnail_url = ""
        likes = 10
        comments = 0
        shares = 0
        views = 100
        duration = 12
        saves = 0
        create_time = int(datetime(2026, 2, 15, tzinfo=UTC).timestamp())

        def to_dict(self) -> dict[str, object]:
            return {"video_id": self.video_id, "music": {"id": "7540327234013301517"}}

    class _FakeTikTokScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}

        def __init__(self, *args, **kwargs) -> None:
            pass

        def scrape(self, config, progress_cb=None):
            seen_scrape_hashtags.extend(list(config.hashtags or []))
            return [_FakePost()]

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"tiktok"},
        source_scope="bravo",
        sync_strategy="full_refresh",
        max_posts_per_target=100,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
        sound_ids=["7540327234013301517"],
    )

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeTikTokScraper)
    monkeypatch.setattr(social_repo, "_load_tiktok_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(social_repo, "_load_show_cast_people", lambda _show_id: [])
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_upsert_tiktok_post",
        lambda _context, **kwargs: upserted_video_ids.append(str(getattr(kwargs.get("post"), "video_id", "")))
        or {"id": "tt-post-1"},
    )
    monkeypatch.setattr(
        social_repo,
        "_persist_tiktok_sound_snapshot",
        lambda *, sound_id, snapshot, conn=None: seen_sound_ids.append(str(sound_id)),
    )
    monkeypatch.setattr(
        social_repo,
        "_fetch_tiktok_sound_snapshot",
        lambda sound_id: {
            "title": None,
            "artist_name": None,
            "usage_count": 0,
            "posts": [],
            "source_url": str(sound_id),
        },
    )
    monkeypatch.setattr(social_repo, "_enqueue_platform_media_mirror_job", lambda **_kwargs: None)

    posts, comments, meta = social_repo._ingest_tiktok(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=[],
        opts=opts,
        job_id="job-1",
        stage="posts",
    )

    assert posts == 1
    assert comments == 0
    assert seen_scrape_hashtags == ["RHOSLC"]
    assert upserted_video_ids == ["vid-1"]
    assert "7540327234013301517" in seen_sound_ids
    assert (meta.get("sound_ids_requested") or []) == ["7540327234013301517"]
    assert "7540327234013301517" in (meta.get("sound_ids_discovered") or [])
    assert (meta.get("persist_counters") or {}).get("posts_upserted") == 1


def test_ingest_tiktok_comments_stage_does_not_count_failed_refresh_as_matched_post(monkeypatch) -> None:
    class _FakeTikTokScraper:
        comments_auth_failed = False
        last_retrieval_meta: dict[str, object] = {}
        last_comment_fetch_reason = ""

        def __init__(self, *args, **kwargs) -> None:
            pass

        def fetch_comments(self, *args, **kwargs):
            raise RuntimeError("fetch failed")

    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"tiktok"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=0,
        max_comments_per_post=25,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="comments_only",
        date_start=datetime(2026, 2, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 28, tzinfo=UTC),
    )

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeTikTokScraper)
    monkeypatch.setattr(social_repo, "_load_tiktok_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(social_repo, "_touch_job_heartbeat", lambda _job_id: None)
    monkeypatch.setattr(social_repo, "_update_job_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_load_existing_posts",
        lambda platform, *_args, **_kwargs: (
            [
                {
                    "id": "tt-post-db-1",
                    "video_id": "vid-1",
                    "posted_at": datetime(2026, 2, 15, tzinfo=UTC),
                    "comments_count": 4,
                }
            ]
            if platform == "tiktok"
            else []
        ),
    )
    monkeypatch.setattr(social_repo, "_load_comment_lifecycle_snapshots", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_decide_comment_refresh",
        lambda **_kwargs: social_repo.CommentRefreshDecision(should_refresh=True, reason="count_gap"),
    )

    posts, comments, meta = social_repo._ingest_tiktok(
        context,
        run_id="run-1",
        account="bravotv",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC"],
        opts=opts,
        job_id="job-1",
        stage="comments",
    )

    assert posts == 1
    assert comments == 0
    assert (meta.get("persist_counters") or {}).get("posts_upserted") == 0
    assert meta.get("incomplete_comment_fetches") == 1


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


def test_platform_post_needs_media_mirror_flags_non_cdn_thumbnail_host(monkeypatch) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "fb-db-1",
            "post_id": "post-123",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": ["https://video.test/main.mp4"],
            "hosted_thumbnail_url": "https://trr-backend.s3.amazonaws.com/social/facebook/x/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/social/facebook/x/media-01.mp4"],
            "media_mirror_status": "mirrored",
        }

        assert social_repo._platform_post_needs_media_mirror("facebook", post_row) is True
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_post_needs_media_mirror_flags_non_cdn_media_host(monkeypatch) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "yt-db-2",
            "video_id": "video-123",
            "thumbnail_url": "",
            "media_urls": [],
            "hosted_thumbnail_url": "https://cdn.test/social/youtube/x/thumb.jpg",
            "hosted_media_urls": ["https://trr-backend.s3.amazonaws.com/social/youtube/x/media-01.mp4"],
            "media_mirror_status": "mirrored",
        }

        assert social_repo._platform_post_needs_media_mirror("youtube", post_row) is True
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_post_needs_media_mirror_flags_html_hosted_media_urls(monkeypatch) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "tt-db-html-1",
            "video_id": "video-123",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": ["https://video.test/main.mp4"],
            "hosted_thumbnail_url": "https://cdn.test/social/tiktok/x/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/social/tiktok/x/media-01.html"],
            "media_mirror_status": "mirrored",
        }

        assert social_repo._platform_post_needs_media_mirror("tiktok", post_row) is True
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_post_needs_media_mirror_flags_page_like_hosted_media_urls(monkeypatch) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "tt-db-page-1",
            "video_id": "video-123",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": ["https://video.test/main.mp4"],
            "hosted_thumbnail_url": "https://cdn.test/social/tiktok/x/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/@bravotv/video/7540327205503601933"],
            "media_mirror_status": "mirrored",
        }

        assert social_repo._platform_post_needs_media_mirror("tiktok", post_row) is True
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_post_needs_media_mirror_flags_non_video_hosted_urls_for_youtube(monkeypatch) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "yt-db-nonvideo-1",
            "video_id": "video-123",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": ["https://video.test/main.mp4"],
            "hosted_thumbnail_url": "https://cdn.test/social/youtube/x/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/social/youtube/x/media-01.jpg"],
            "media_mirror_status": "mirrored",
        }

        assert social_repo._platform_post_needs_media_mirror("youtube", post_row) is True
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_post_needs_media_mirror_accepts_matching_cdn_hosts(monkeypatch) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "fb-db-2",
            "post_id": "post-456",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": ["https://video.test/main.mp4"],
            "hosted_thumbnail_url": "https://cdn.test/social/facebook/x/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/social/facebook/x/media-01.mp4"],
            "media_mirror_status": "mirrored",
        }

        assert social_repo._platform_post_needs_media_mirror("facebook", post_row) is False
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_post_needs_media_mirror_ignores_failed_status_when_hosted_urls_are_complete(monkeypatch) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "yt-db-complete-1",
            "video_id": "video-123",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": [],
            "hosted_thumbnail_url": "https://cdn.test/social/youtube/x/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/social/youtube/x/media-01.mp4"],
            "media_mirror_status": "failed",
        }

        assert social_repo._platform_post_needs_media_mirror("youtube", post_row) is False
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_post_needs_media_mirror_twitter_flags_video_hosted_thumbnail_with_image_candidate(
    monkeypatch,
) -> None:
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        post_row = {
            "id": "tw-db-video-thumb-1",
            "tweet_id": "tweet-123",
            "thumbnail_url": "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/1280x720/main.mp4?tag=12",
            "media_urls": [
                "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/1280x720/main.mp4?tag=12",
                "https://pbs.twimg.com/ext_tw_video_thumb/123/pu/img/cover.jpg",
            ],
            "hosted_thumbnail_url": "https://cdn.test/social/twitter/x/thumbnail.mp4",
            "hosted_media_urls": [
                "https://cdn.test/social/twitter/x/thumbnail.mp4",
                "https://cdn.test/social/twitter/x/media-02.jpg",
            ],
            "media_mirror_status": "mirrored",
        }

        assert social_repo._platform_post_needs_media_mirror("twitter", post_row) is True
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_platform_comment_media_needs_mirror_twitter_detects_missing_hosted_assets() -> None:
    assert (
        social_repo._platform_comment_media_needs_mirror(  # noqa: SLF001
            "twitter",
            {
                "media_urls": ["https://pbs.twimg.com/media/reply-1.jpg"],
                "hosted_media_urls": [],
                "media_mirror_status": "mirrored",
            },
        )
        is True
    )
    assert (
        social_repo._platform_comment_media_needs_mirror(  # noqa: SLF001
            "twitter",
            {
                "media_urls": ["https://pbs.twimg.com/media/reply-1.jpg"],
                "hosted_media_urls": ["https://cdn.test/social/twitter/reply-1.jpg"],
                "media_mirror_status": "mirrored",
            },
        )
        is False
    )


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
    assert mirrored_inputs and mirrored_inputs[0]["media_urls"] == [
        "https://www.tiktok.com/@bravotv/video/tt-video-123"
    ]
    assert metadata["mirror"]["selected_source"] == "yt_dlp_manifest"
    assert metadata["mirror"]["attempts"][0]["source"] == "yt_dlp_manifest"
    assert updates and updates[0]["media_mirror_status"] == "pending"


def test_run_platform_media_mirror_stage_tiktok_resolves_page_like_source_media(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000002"
    updates: list[dict[str, object]] = []
    mirrored_inputs: list[dict[str, object]] = []
    resolver_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "7540327205503601933",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": ["https://video.test/media-tokenized.bin"],
            "raw_data": {},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "https://cdn.test/social/tiktok/x/thumbnail.jpg",
            "hosted_media_urls": ["https://cdn.test/social/tiktok/x/media-01.html"],
            "media_mirror_status": "pending",
            "media_mirror_error": None,
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )

    def _fake_resolve_tiktok(**kwargs):  # noqa: ANN001
        resolver_calls.append(dict(kwargs))
        return {
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
        }

    monkeypatch.setattr(social_repo, "_resolve_tiktok_media_for_video_id", _fake_resolve_tiktok)

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
        job_id="job-tt-page-1",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert resolver_calls and resolver_calls[0]["allow_ytdlp"] is True
    assert mirrored_inputs and mirrored_inputs[0]["media_urls"] == ["https://video.test/main.mp4"]
    assert metadata["mirror"]["selected_source"] == "yt_dlp_manifest"
    assert updates and updates[0]["media_mirror_status"] == "pending"


def test_run_platform_media_mirror_stage_tiktok_fails_when_non_page_source_is_unresolved(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000003"
    updates: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "7540327205503601933",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": ["https://video.test/media-tokenized.bin"],
            "raw_data": {},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "https://cdn.test/social/tiktok/x/thumbnail.jpg",
            "hosted_media_urls": ["https://cdn.test/social/tiktok/x/media-01.html"],
            "media_mirror_status": "pending",
            "media_mirror_error": None,
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
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("resolve failed")),
    )
    monkeypatch.setattr(
        social_repo,
        "_mirror_platform_media_to_s3_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not mirror unresolved non-page URLs")),
    )

    posts, mirrored, metadata = social_repo._run_platform_media_mirror_stage(
        context=context,
        platform="tiktok",
        job_id="job-tt-page-fail-1",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 0
    assert metadata["mirror"]["status"] == "failed"
    assert metadata["mirror"]["error"] == "tiktok_media_unresolved"
    assert len(updates) >= 2
    assert updates[0]["media_mirror_status"] == "pending"
    assert updates[-1]["media_mirror_status"] == "failed"
    assert updates[-1]["media_mirror_error"] == "tiktok_media_unresolved"


def test_run_platform_media_mirror_stage_tiktok_uses_canonical_page_url_when_resolver_fails(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000004"
    mirrored_inputs: list[dict[str, object]] = []

    canonical_url = "https://www.tiktok.com/@bravotv/video/7540327205503601933"
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "7540327205503601933",
            "thumbnail_url": "https://img.test/thumb.jpg",
            "media_urls": [canonical_url],
            "raw_data": {"url": canonical_url},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "pending",
            "media_mirror_error": None,
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_mirror_fields",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        social_repo,
        "_resolve_tiktok_media_for_video_id",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("resolve failed")),
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
        job_id="job-tt-page-fallback-1",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert mirrored_inputs and mirrored_inputs[0]["media_urls"] == [canonical_url]
    assert metadata["mirror"]["status"] == "mirrored"


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


def test_run_platform_media_mirror_stage_twitter_prefers_non_video_thumbnail_from_existing_media(monkeypatch) -> None:
    context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_id = "00000000-0000-0000-0000-000000000002"
    mirrored_inputs: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": post_id,
            "source_id": "2011339494734078235",
            "thumbnail_url": "https://video.twimg.com/ext_tw_video/111/pu/vid/avc1/1280x720/main.mp4?tag=12",
            "media_urls": [
                "https://video.twimg.com/ext_tw_video/111/pu/vid/avc1/1280x720/main.mp4?tag=12",
                "https://pbs.twimg.com/ext_tw_video_thumb/111/pu/img/cover.jpg",
            ],
            "raw_data": {},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "mirrored",
            "media_mirror_error": "",
        },
    )
    monkeypatch.setattr(social_repo, "_update_platform_post_media_mirror_fields", lambda **_kwargs: None)

    def _fake_mirror_result(_context, *, platform: str, post, week_index: int | None, display_name: str | None = None):  # noqa: ANN001
        del display_name
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

    posts, mirrored, _metadata = social_repo._run_platform_media_mirror_stage(
        context=context,
        platform="twitter",
        job_id="job-twitter-2",
        config={"post_id": post_id, "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert mirrored_inputs
    assert mirrored_inputs[0]["thumbnail_url"] == "https://pbs.twimg.com/ext_tw_video_thumb/111/pu/img/cover.jpg"


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
    enforce_keyword_filter_flags: list[bool] = []

    class _FakeYouTubeScraper:
        last_retrieval_meta: dict[str, object] = {}

        def scrape(self, config, progress_cb=None):
            enforce_keyword_filter_flags.append(bool(getattr(config, "enforce_keyword_filter", True)))
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
    assert enforce_keyword_filter_flags == [False]

    filter_samples = meta.get("filter_samples") or []
    reasons = {str(sample.get("reason") or "") for sample in filter_samples if isinstance(sample, dict)}
    assert "show_terms_filtered" in reasons
    assert "up_to_date" in reasons


def test_ingest_youtube_progress_uses_matched_posts_for_scrape_counter(monkeypatch) -> None:
    class _FakeYouTubeScraper:
        last_retrieval_meta: dict[str, object] = {}

        def scrape(self, config, progress_cb=None):
            assert getattr(config, "enforce_keyword_filter", True) is False
            if progress_cb:
                progress_cb(
                    {
                        "phase": "scrape_posts",
                        "pages_scanned": 3,
                        "posts_checked": 50,
                        "matched_posts": 1,
                    }
                )
            return [
                SimpleNamespace(
                    video_id="vid-1",
                    title="RHOSLC preview",
                    description="RHOSLC",
                    comments=0,
                )
            ]

        def fetch_comments(self, *args, **kwargs):
            return []

    context = SeasonContext(
        season_id="season-youtube-progress",
        show_id="show-youtube-progress",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"youtube"},
        source_scope="bravo",
        sync_strategy="full",
        max_posts_per_target=100,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
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
        "_upsert_youtube_video",
        lambda *_args, **kwargs: {"id": f"db-{getattr(kwargs.get('video'), 'video_id', 'unknown')}"},
    )
    monkeypatch.setattr(social_repo, "_sync_youtube_video_comment_counts", lambda *_args, **_kwargs: 0)

    posts, comments, meta = social_repo._ingest_youtube(
        context,
        run_id="run-youtube-progress",
        account="bravo",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC", "Salt Lake City"],
        opts=opts,
        job_id="job-youtube-progress",
        stage="posts",
    )

    assert posts == 1
    assert comments == 0
    assert (meta.get("scrape_counters") or {}).get("posts") == 1


def test_ingest_youtube_post_limit_soft_cap_persists_video_and_short(monkeypatch) -> None:
    upserted_ids: list[str] = []

    class _FakeYouTubeScraper:
        last_retrieval_meta: dict[str, object] = {
            "videos_pages_scanned": 4,
            "shorts_pages_scanned": 3,
            "surface_cap_override_applied": False,
        }

        def scrape(self, _config, progress_cb=None):
            if progress_cb:
                progress_cb(
                    {
                        "phase": "scrape_posts",
                        "pages_scanned": 7,
                        "posts_checked": 20,
                        "matched_posts": 2,
                    }
                )
            return [
                SimpleNamespace(
                    video_id="vid-main",
                    title="RHOSLC full teaser",
                    description="RHOSLC",
                    comments=0,
                    source_surface="videos",
                    is_short=False,
                    url="https://www.youtube.com/watch?v=vid-main",
                    tags=[],
                ),
                SimpleNamespace(
                    video_id="short-main",
                    title="RHOSLC short teaser",
                    description="RHOSLC",
                    comments=0,
                    source_surface="shorts",
                    is_short=True,
                    url="https://www.youtube.com/shorts/short-main",
                    tags=[],
                ),
            ]

        def fetch_comments(self, *args, **kwargs):
            return []

    context = SeasonContext(
        season_id="season-youtube-cap",
        show_id="show-youtube-cap",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    opts = social_repo.IngestOptions(
        platforms={"youtube"},
        source_scope="bravo",
        sync_strategy="full",
        max_posts_per_target=1,
        max_comments_per_post=0,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_only",
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
    monkeypatch.setattr(social_repo, "_youtube_video_matches_owner_identity", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_youtube_transcript_ingest_enabled", lambda: False)
    monkeypatch.setattr(social_repo, "_enqueue_platform_media_mirror_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "_upsert_youtube_video",
        lambda *_args, **kwargs: (
            upserted_ids.append(str(getattr(kwargs.get("video"), "video_id", ""))),
            {"id": f"db-{getattr(kwargs.get('video'), 'video_id', 'unknown')}"},
        )[1],
    )
    monkeypatch.setattr(social_repo, "_sync_youtube_video_comment_counts", lambda *_args, **_kwargs: 0)

    posts, comments, meta = social_repo._ingest_youtube(
        context,
        run_id="run-youtube-cap",
        account="bravo",
        hashtags=["RHOSLC"],
        keywords=["RHOSLC"],
        opts=opts,
        job_id="job-youtube-cap",
        stage="posts",
    )

    assert posts == 2
    assert comments == 0
    assert upserted_ids == ["vid-main", "short-main"]
    assert meta["surface_cap_override_applied"] is True
    assert sorted(meta["surface_match_presence"]) == ["shorts", "videos"]


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
        caption="hello #RHOSLC @BravoTV",
        post_type="reel",
        media_urls=[],
        thumbnail_url=None,
        likes=0,
        comments=0,
        shares=0,
        views=0,
        user_avatar_url="https://images.test/facebook-avatar.jpg",
        posted_at=None,
        to_dict=lambda: {"posted_at": None},
    )

    monkeypatch.setattr(social_repo, "_now_utc", lambda: fixed_now)
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "facebook" and column in {"hashtags", "mentions", "user_avatar_url"},
    )

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
    assert payload["hashtags"] == ["RHOSLC"]
    assert payload["mentions"] == ["@BravoTV"]
    assert payload["user_avatar_url"] == "https://images.test/facebook-avatar.jpg"
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
        text="hello #RHOSLC @BravoTV",
        media_urls=[],
        thumbnail_url=None,
        likes=0,
        replies=0,
        reposts=0,
        quotes=0,
        views=0,
        user_avatar_url="https://images.test/threads-avatar.jpg",
        posted_at=None,
        to_dict=lambda: {"posted_at": None},
    )

    monkeypatch.setattr(social_repo, "_now_utc", lambda: fixed_now)
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "threads" and column in {"hashtags", "mentions", "user_avatar_url"},
    )

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
    assert payload["hashtags"] == ["RHOSLC"]
    assert payload["mentions"] == ["@BravoTV"]
    assert payload["user_avatar_url"] == "https://images.test/threads-avatar.jpg"
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


def test_load_twikit_credentials_derives_from_social_twitter_cookies_json_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {"auth_token": "twitter-token", "ct0": "twitter-ct0", "lang": "en"}
    monkeypatch.setenv("SOCIAL_TWITTER_COOKIES_JSON", json.dumps(payload))
    monkeypatch.delenv("TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("TWIKIT_COOKIES_JSON", raising=False)
    monkeypatch.delenv("TWIKIT_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWIKIT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWIKIT_CT0", raising=False)
    monkeypatch.delenv("TWIKIT_USERNAME", raising=False)
    monkeypatch.delenv("TWIKIT_PASSWORD", raising=False)
    monkeypatch.delenv("TWIKIT_EMAIL", raising=False)
    monkeypatch.setattr(social_repo, "_load_twitter_browser_cookies", lambda: {})

    creds = social_repo._load_twikit_credentials()

    assert creds == {"auth_token": "twitter-token", "ct0": "twitter-ct0"}


def test_load_twikit_credentials_prefers_twikit_cookie_env_over_preloaded_twitter_cookies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "TWIKIT_COOKIES_JSON",
        json.dumps({"auth_token": "twikit-token", "ct0": "twikit-ct0"}),
    )
    monkeypatch.delenv("TWIKIT_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWIKIT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWIKIT_CT0", raising=False)
    monkeypatch.delenv("TWIKIT_USERNAME", raising=False)
    monkeypatch.delenv("TWIKIT_PASSWORD", raising=False)
    monkeypatch.delenv("TWIKIT_EMAIL", raising=False)
    monkeypatch.setattr(social_repo, "_load_twitter_browser_cookies", lambda: {})

    creds = social_repo._load_twikit_credentials(
        {"auth_token": "twitter-token", "ct0": "twitter-ct0"},
    )

    assert creds == {"auth_token": "twikit-token", "ct0": "twikit-ct0"}


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
    monkeypatch.delenv("TWIKIT_COOKIES_JSON", raising=False)
    monkeypatch.delenv("TWIKIT_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWIKIT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWIKIT_CT0", raising=False)
    monkeypatch.delenv("TWIKIT_USERNAME", raising=False)
    monkeypatch.delenv("TWIKIT_PASSWORD", raising=False)
    monkeypatch.delenv("TWIKIT_EMAIL", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_HEADER", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_HEADER", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_FILE", raising=False)
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
            season_id="s1",
            show_id="sh1",
            show_name="The Real Housewives of Salt Lake City",
            season_number=6,
            anchor_date=date(2025, 9, 16),
            show_slug="RHOSLC",
        )
        assert social_repo._resolve_show_slug(ctx) == "RHOSLC"

    def test_slug_with_special_chars(self):
        ctx = SeasonContext(
            season_id="s1",
            show_id="sh1",
            show_name="Test",
            season_number=1,
            anchor_date=date(2025, 1, 1),
            show_slug="RHO-SLC!",
        )
        assert social_repo._resolve_show_slug(ctx) == "RHOSLC"

    def test_fallback_to_show_name(self):
        ctx = SeasonContext(
            season_id="s1",
            show_id="sh1",
            show_name="The Real Housewives of Salt Lake City",
            season_number=6,
            anchor_date=date(2025, 9, 16),
            show_slug=None,
        )
        assert social_repo._resolve_show_slug(ctx) == "TheRealHousewivesofSaltLakeCity"

    def test_fallback_no_name(self):
        ctx = SeasonContext(
            season_id="s1",
            show_id="sh1",
            show_name=None,
            season_number=1,
            anchor_date=date(2025, 1, 1),
            show_slug=None,
        )
        assert social_repo._resolve_show_slug(ctx) == "UnknownShow"

    def test_empty_slug_falls_back(self):
        ctx = SeasonContext(
            season_id="s1",
            show_id="sh1",
            show_name="Test Show",
            season_number=1,
            anchor_date=date(2025, 1, 1),
            show_slug="  ",
        )
        assert social_repo._resolve_show_slug(ctx) == "TestShow"


class TestBuildPostDisplayName:
    def test_basic(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOSLC",
            username="BravoBetsy",
            platform="tiktok",
            post_number=1,
        )
        assert result == "RHOSLCBravoBetsyTikTokPost1"

    def test_instagram(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOSLC",
            username="heatherrgay",
            platform="instagram",
            post_number=3,
        )
        assert result == "RHOSLCheatherrgayInstagramPost3"

    def test_youtube(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOBH",
            username="BravoTV",
            platform="youtube",
            post_number=2,
        )
        assert result == "RHOBHBravoTVYouTubePost2"

    def test_twitter(self):
        result = social_repo._build_post_display_name(
            show_slug="RHOSLC",
            username="user123",
            platform="twitter",
            post_number=1,
        )
        assert result == "RHOSLCuser123TwitterPost1"

    def test_unknown_platform(self):
        result = social_repo._build_post_display_name(
            show_slug="X",
            username="u",
            platform="newplatform",
            post_number=1,
        )
        assert result == "XuNewplatformPost1"


class TestBuildMediaObjectName:
    def test_thumbnail(self):
        assert (
            social_repo._build_media_object_name(
                "RHOSLCBravoBetsyTikTokPost1",
                is_thumbnail=True,
                slide_number=None,
            )
            == "RHOSLCBravoBetsyTikTokPost1_Thumbnail"
        )

    def test_single_media(self):
        assert (
            social_repo._build_media_object_name(
                "RHOSLCBravoBetsyTikTokPost1",
                is_thumbnail=False,
                slide_number=None,
            )
            == "RHOSLCBravoBetsyTikTokPost1"
        )

    def test_multi_slide(self):
        assert (
            social_repo._build_media_object_name(
                "RHOSLCheatherrgayInstagramPost2",
                is_thumbnail=False,
                slide_number=3,
            )
            == "RHOSLCheatherrgayInstagramPost2_S3"
        )


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


def test_get_queue_status_uses_cache_ttl_and_skips_recent_failures_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    query_calls: list[str] = []

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, params: list[object] | None = None):
        del params
        query_calls.append(" ".join(sql.split()).lower())
        return [
            {"platform": "instagram", "job_type": "posts", "status": "running", "total": 2},
            {"platform": "instagram", "job_type": "posts", "status": "queued", "total": 1},
        ]

    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS", "20")
    monkeypatch.setattr(social_repo, "_queue_status_cache", None)
    monkeypatch.setattr(social_repo, "_queue_status_last_good_cache", None)
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)

    payload = social_repo.get_queue_status(include_recent_failures=False, include_runs_summary=False)

    assert payload["queue"]["by_status"]["running"] == 2
    assert payload["queue"]["recent_failures"] == []
    assert len(query_calls) == 1
    assert social_repo.SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS_DEFAULT == 20


def test_get_queue_status_returns_stale_last_good_on_query_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _raise_query_failure(*_args, **_kwargs):
        raise RuntimeError("aggregate query failed")

    monkeypatch.setattr(social_repo, "_queue_status_cache", None)
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))

    baseline_payload = {
        "queue_enabled": True,
        "workers": {"healthy": True, "healthy_workers": 1},
        "queue": {
            "by_status": {"running": 3, "queued": 4, "pending": 0, "failed": 0},
            "by_platform": {"instagram": {"running": 3, "queued": 4}},
            "by_job_type": {"posts": {"running": 3, "queued": 4}},
            "recent_failures": [],
        },
    }
    monkeypatch.setattr(
        social_repo,
        "_queue_status_last_good_cache",
        (social_repo.time_module.monotonic(), baseline_payload),
    )

    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS", "0")
    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_STALE_FALLBACK_SECONDS", "120")
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _raise_query_failure)
    fallback = social_repo.get_queue_status(include_recent_failures=False, include_runs_summary=False)

    assert fallback == baseline_payload
    assert fallback["queue"]["by_status"]["running"] == 3
    assert fallback["queue"]["by_status"]["queued"] == 4


def test_get_queue_status_includes_stuck_jobs_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, params: list[object] | None = None):
        del sql, params
        return [
            {"platform": "twitter", "job_type": "comments", "status": "running", "total": 2},
            {"platform": "youtube", "job_type": "posts", "status": "queued", "total": 1},
        ]

    def _fake_fetch_all(sql: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        assert "from social.scrape_jobs j" in normalized
        return [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "run_id": "22222222-2222-2222-2222-222222222222",
                "platform": "twitter",
                "job_type": "comments",
                "status": "running",
                "worker_id": "social-worker:thomas",
                "created_at": datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
                "heartbeat_at": datetime(2026, 3, 1, 10, 5, tzinfo=UTC),
                "available_at": None,
                "error_message": None,
                "last_error_code": None,
                "stuck_reason": "running_stale_heartbeat",
                "stuck_for_seconds": 700,
            },
            {
                "id": "33333333-3333-3333-3333-333333333333",
                "run_id": "44444444-4444-4444-4444-444444444444",
                "platform": "youtube",
                "job_type": "posts",
                "status": "retrying",
                "worker_id": "social-worker:local",
                "created_at": datetime(2026, 3, 1, 9, 0, tzinfo=UTC),
                "heartbeat_at": datetime(2026, 3, 1, 9, 10, tzinfo=UTC),
                "available_at": datetime(2026, 3, 1, 9, 15, tzinfo=UTC),
                "error_message": "stale_heartbeat_timeout: no heartbeat for >= 300 seconds",
                "last_error_code": "stale_heartbeat_timeout",
                "stuck_reason": "retrying_stale_timeout",
                "stuck_for_seconds": 900,
            },
        ]

    def _fake_fetch_one(sql: str, _params: list[object] | None = None) -> dict[str, object]:
        normalized = " ".join(sql.split()).lower()
        if "count(*)::int as total" in normalized and "from social.scrape_jobs j" in normalized:
            return {"total": 2}
        return {}

    monkeypatch.setattr(social_repo, "_queue_status_cache", None)
    monkeypatch.setattr(social_repo, "_queue_status_last_good_cache", None)
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    payload = social_repo.get_queue_status(include_recent_failures=False)

    assert payload["queue"]["stuck_jobs_total"] == 2
    assert len(payload["queue"]["stuck_jobs"]) == 2
    assert payload["queue"]["stuck_jobs"][0]["stuck_reason"] == "running_stale_heartbeat"
    assert payload["queue"]["stuck_jobs"][1]["stuck_reason"] == "retrying_stale_timeout"


def test_get_queue_status_includes_runs_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, _params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "from social.scrape_jobs" in normalized:
            return [{"platform": "instagram", "job_type": "posts", "status": "queued", "total": 2}]
        if "from social.scrape_runs" in normalized:
            return [{"status": "running", "total": 1}, {"status": "failed", "total": 2}]
        return []

    monkeypatch.setattr(social_repo, "_queue_status_cache", None)
    monkeypatch.setattr(social_repo, "_queue_status_last_good_cache", None)
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", lambda limit=100: ([], 0))

    payload = social_repo.get_queue_status(include_recent_failures=False)

    runs_by_status = payload["queue"]["runs_by_status"]
    assert runs_by_status["running"] == 1
    assert runs_by_status["failed"] == 2
    assert payload["queue"]["runs_total"] == 3


def test_get_queue_status_recent_failures_include_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, _params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "group by 1, 2, 3" in normalized:
            return [{"platform": "twitter", "job_type": "comments", "status": "failed", "total": 1}]
        if "where status in ('failed', 'retrying')" in normalized:
            return [
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "run_id": "22222222-2222-2222-2222-222222222222",
                    "platform": "twitter",
                    "job_type": "comments",
                    "status": "failed",
                    "error_message": "boom",
                    "last_error_code": "x",
                    "last_error_class": "Error",
                    "created_at": datetime(2026, 3, 2, 10, 0, tzinfo=UTC),
                    "completed_at": datetime(2026, 3, 2, 10, 5, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo, "_queue_status_cache", None)
    monkeypatch.setattr(social_repo, "_queue_status_last_good_cache", None)
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)

    payload = social_repo.get_queue_status(include_stuck_jobs=False, include_runs_summary=False)

    assert payload["queue"]["recent_failures"][0]["run_id"] == "22222222-2222-2222-2222-222222222222"


def test_get_worker_detail_returns_joined_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_id = "social-worker:test"
    job_id = "11111111-1111-1111-1111-111111111111"
    run_id = "22222222-2222-2222-2222-222222222222"

    monkeypatch.setattr(social_repo, "_worker_heartbeat_schema_ready", lambda: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(
        social_repo,
        "_relation_exists",
        lambda name: name in {"social.scrape_jobs", "social.scrape_runs"},
    )

    def _fake_fetch_one(sql: str, params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "from social.scrape_workers" in normalized and "where worker_id = %s" in normalized:
            return {
                "worker_id": worker_id,
                "stage": "posts",
                "status": "working",
                "run_id": run_id,
                "current_job_id": job_id,
                "metadata": {"source": "worker"},
                "started_at": datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
                "last_seen_at": datetime(2026, 3, 2, 9, 5, tzinfo=UTC),
                "is_healthy": True,
            }
        if "from social.scrape_jobs j" in normalized:
            return {
                "id": job_id,
                "run_id": run_id,
                "platform": "twitter",
                "job_type": "comments",
                "status": "running",
                "stage": "comments",
                "config": {"account": "@bravotv"},
                "metadata": {
                    "activity": {"phase": "comments_scan"},
                    "persist_counters": {"posts_upserted": 2, "comments_upserted": 9},
                },
                "items_found": 11,
                "attempt_count": 1,
                "max_attempts": 3,
                "started_at": datetime(2026, 3, 2, 9, 1, tzinfo=UTC),
                "heartbeat_at": datetime(2026, 3, 2, 9, 5, tzinfo=UTC),
                "error_message": None,
                "last_error_code": None,
            }
        if "from social.scrape_runs" in normalized:
            return {
                "run_id": run_id,
                "status": "running",
                "source_scope": "bravo",
                "created_at": datetime(2026, 3, 2, 8, 59, tzinfo=UTC),
                "started_at": datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
                "completed_at": None,
                "summary": {"total_jobs": 18},
            }
        return None

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    payload = social_repo.get_worker_detail(worker_id)

    assert payload["worker"]["worker_id"] == worker_id
    assert payload["current_job"]["id"] == job_id
    assert payload["current_job"]["account_handle"] == "@bravotv"
    assert payload["run"]["run_id"] == run_id
    assert payload["currently_scraping"] == "comments_scan"
    assert payload["progress_made"]["items_found"] == 11
    assert payload["progress_made"]["comments_upserted"] == 9


def test_get_worker_detail_returns_worker_only_when_no_job(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_id = "social-worker:idle"
    monkeypatch.setattr(social_repo, "_worker_heartbeat_schema_ready", lambda: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda sql, params=None: {
            "worker_id": worker_id,
            "stage": "any",
            "status": "idle",
            "run_id": None,
            "current_job_id": None,
            "metadata": {},
            "started_at": datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
            "last_seen_at": datetime(2026, 3, 2, 9, 5, tzinfo=UTC),
            "is_healthy": True,
        }
        if "from social.scrape_workers" in " ".join(sql.split()).lower()
        else None,
    )

    payload = social_repo.get_worker_detail(worker_id)

    assert payload["current_job"] is None
    assert payload["run"] is None
    assert payload["currently_scraping"] == "any"


def test_validate_debug_patch_paths_rejects_traversal() -> None:
    with pytest.raises(ValueError, match="traversal"):
        social_repo._validate_debug_patch_paths(["../outside.py"])


def test_debug_ingest_job_uses_fallback_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("SOCIAL_DEBUG_OPENAI_MODEL", "gpt-5.3-codex")
    monkeypatch.setenv("SOCIAL_DEBUG_OPENAI_FALLBACK_MODEL", "gpt-5.2-codex")
    monkeypatch.setattr(
        social_repo,
        "_fetch_job_debug_context",
        lambda _job_id: {"job": {"id": "job-1", "run_id": "run-1"}, "worker": {}, "run": {}},
    )
    calls: list[str] = []

    def _fake_openai_completion(*, model: str, prompt: str, api_key: str, timeout_seconds: int):
        del prompt, api_key, timeout_seconds
        calls.append(model)
        if model == "gpt-5.3-codex":
            raise RuntimeError("rate limited")
        return {
            "root_cause": "fallback succeeded",
            "confidence": 0.7,
            "patch_unified_diff": "--- a/api/routers/socials.py\n+++ b/api/routers/socials.py\n@@\n-foo\n+bar\n",
            "files_touched": ["api/routers/socials.py"],
            "tests_to_run": ["pytest -q tests/api/routers/test_socials_season_analytics.py"],
        }

    monkeypatch.setattr(social_repo, "_run_social_debug_openai_completion", _fake_openai_completion)

    payload = social_repo.debug_ingest_job_with_openai("job-1", include_context=False)

    assert calls == ["gpt-5.3-codex", "gpt-5.2-codex"]
    assert payload["model_used"] == "gpt-5.2-codex"
    assert payload["fallback_used"] is True


def test_debug_ingest_job_apply_returns_check_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("SOCIAL_DEBUG_PATCH_APPLY_ENABLED", "true")
    monkeypatch.setattr(
        social_repo,
        "_fetch_job_debug_context",
        lambda _job_id: {"job": {"id": "job-1", "run_id": "run-1"}, "worker": {}, "run": {}},
    )
    monkeypatch.setattr(
        social_repo,
        "_run_social_debug_openai_completion",
        lambda **_kwargs: {
            "root_cause": "x",
            "confidence": 0.5,
            "patch_unified_diff": "--- a/api/routers/socials.py\n+++ b/api/routers/socials.py\n@@\n-foo\n+bar\n",
            "files_touched": ["api/routers/socials.py"],
            "tests_to_run": [],
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_run_git_apply",
        lambda **_kwargs: subprocess.CompletedProcess(
            args=["git", "apply"], returncode=1, stdout="", stderr="check failed"
        ),
    )

    payload = social_repo.debug_ingest_job_with_openai(
        "job-1",
        include_context=False,
        apply_patch=True,
        confirm_apply=True,
    )

    assert payload["apply"]["requested"] is True
    assert payload["apply"]["check_ok"] is False
    assert payload["apply"]["applied"] is False
    assert payload["apply"]["error"] == "check failed"


def test_cancel_stuck_jobs_targets_only_requested_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    finalize_calls: list[str] = []
    heartbeat_calls: list[str] = []

    def _fake_fetch_all(sql: str, params: list[object] | None = None) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params or []
        return [
            {"id": "11111111-1111-1111-1111-111111111111", "run_id": "55555555-5555-5555-5555-555555555555"},
            {"id": "22222222-2222-2222-2222-222222222222", "run_id": "55555555-5555-5555-5555-555555555555"},
        ]

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_count_stuck_jobs", lambda: 1)
    monkeypatch.setattr(
        social_repo,
        "_finalize_run_status",
        lambda run_id, force_recompute=False: finalize_calls.append(f"{run_id}:{force_recompute}") or {},
    )
    monkeypatch.setattr(
        social_repo,
        "_clear_worker_heartbeat_for_job",
        lambda job_id, status, metadata=None: heartbeat_calls.append(f"{job_id}:{status}"),
    )

    payload = social_repo.cancel_stuck_jobs(
        job_ids=[
            "11111111-1111-1111-1111-111111111111",
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
        ],
        cancelled_by="admin@example.com",
    )

    normalized_sql = " ".join(str(captured["sql"]).split()).lower()
    assert "status = 'running'" in normalized_sql
    assert "status = 'retrying'" in normalized_sql
    assert "stale_heartbeat_timeout" in normalized_sql
    assert payload["requested_job_ids_count"] == 2
    assert payload["cancelled_jobs"] == 2
    assert payload["affected_run_ids"] == ["55555555-5555-5555-5555-555555555555"]
    assert payload["stuck_jobs_remaining"] == 1
    assert finalize_calls == ["55555555-5555-5555-5555-555555555555:True"]
    assert len(heartbeat_calls) == 2


def test_cancel_stuck_jobs_clear_all_uses_null_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_params: list[object] = []

    def _fake_fetch_all(_sql: str, params: list[object] | None = None) -> list[dict[str, object]]:
        captured_params[:] = list(params or [])
        return []

    monkeypatch.setattr(social_repo, "_assert_social_queue_schema_ready", lambda: None)
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_count_stuck_jobs", lambda: 0)
    monkeypatch.setattr(social_repo, "_finalize_run_status", lambda run_id, force_recompute=False: {})
    monkeypatch.setattr(social_repo, "_clear_worker_heartbeat_for_job", lambda job_id, status, metadata=None: None)

    payload = social_repo.cancel_stuck_jobs(cancelled_by="admin@example.com")

    assert payload["requested_job_ids_count"] == 0
    assert payload["cancelled_jobs"] == 0
    assert captured_params[2] is None
    assert captured_params[3] is None


def test_purge_inactive_workers_deletes_non_active_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(social_repo, "_worker_heartbeat_schema_ready", lambda: True)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params=None: {"active_workers": 3, "total_workers": 28},
    )
    monkeypatch.setattr(
        social_repo.pg,
        "execute_returning",
        lambda _sql, _params=None: [{"worker_id": f"w-{idx}"} for idx in range(25)],
    )
    monkeypatch.setattr(social_repo, "_worker_health_cache", (0.0, None, {"healthy_workers": 0}))

    payload = social_repo.purge_inactive_workers(stale_after_seconds=180)

    assert payload["stale_after_seconds"] == 180
    assert payload["active_workers"] == 3
    assert payload["total_workers_before"] == 28
    assert payload["deleted_workers"] == 25
    assert payload["total_workers_after"] == 3
    assert payload["reason"] is None
    assert social_repo._worker_health_cache is None


def test_purge_inactive_workers_handles_missing_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(social_repo, "_worker_heartbeat_schema_ready", lambda: False)

    payload = social_repo.purge_inactive_workers(stale_after_seconds=120)

    assert payload["stale_after_seconds"] == 120
    assert payload["deleted_workers"] == 0
    assert payload["active_workers"] == 0
    assert payload["reason"] == "worker_heartbeat_schema_missing"


def test_get_tiktok_overview_includes_time_series_and_wow(monkeypatch: pytest.MonkeyPatch) -> None:
    season_id = "11111111-1111-1111-1111-111111111111"

    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "tiktok" and column == "saves",
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params=None: {
            "post_count": 4,
            "total_views": 400,
            "total_likes": 120,
            "total_comments": 40,
            "total_shares": 20,
            "total_saves": 8,
            "avg_engagement_rate": 0.47,
        },
    )

    def _fake_fetch_all(sql: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if "date_trunc('day'" in normalized:
            return [
                {
                    "period_start": date(2026, 2, 1),
                    "posts": 2,
                    "views": 200,
                    "likes": 60,
                    "comments": 20,
                    "shares": 10,
                    "saves": 4,
                },
                {
                    "period_start": date(2026, 2, 2),
                    "posts": 2,
                    "views": 200,
                    "likes": 60,
                    "comments": 20,
                    "shares": 10,
                    "saves": 4,
                },
            ]
        if "date_trunc('week'" in normalized:
            return [
                {
                    "period_start": date(2026, 1, 26),
                    "posts": 2,
                    "views": 100,
                    "likes": 30,
                    "comments": 10,
                    "shares": 5,
                    "saves": 2,
                },
                {
                    "period_start": date(2026, 2, 2),
                    "posts": 2,
                    "views": 300,
                    "likes": 90,
                    "comments": 30,
                    "shares": 15,
                    "saves": 6,
                },
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = social_repo.get_tiktok_overview(season_id)

    assert payload["season_id"] == season_id
    assert payload["kpis"]["post_count"] == 4
    assert payload["time_series"]["daily"][0]["period_start"] == "2026-02-01"
    assert payload["time_series"]["weekly"][1]["views"] == 300
    assert payload["wow_delta_pct"]["views"] == 200.0


def test_get_tiktok_sounds_returns_related_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    season_id = "11111111-1111-1111-1111-111111111111"

    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "tiktok" and column in {"sound_id", "saves"},
    )
    monkeypatch.setattr(
        social_repo,
        "_relation_exists",
        lambda qualified_name: qualified_name == "social.tiktok_sound_posts",
    )

    def _fake_fetch_all(sql: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if "from social.tiktok_sound_posts" in normalized:
            return [{"sound_id": "7540327234013301517", "related_post_count": 12}]
        if "from social.tiktok_posts p" in normalized:
            return [
                {
                    "sound_id": "7540327234013301517",
                    "title": "Lisa Flies Coach from RHOSLC",
                    "artist_name": "Bravo",
                    "usage_count": 2400,
                    "creator_post_count": 3,
                    "creator_views": 10000,
                    "creator_likes": 900,
                    "creator_comments": 120,
                    "creator_shares": 80,
                    "creator_saves": 40,
                    "last_creator_post_at": datetime(2026, 2, 10, tzinfo=UTC),
                    "last_seen_at": datetime(2026, 2, 11, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = social_repo.get_tiktok_sounds(season_id, search="lisa", limit=20)

    assert payload["season_id"] == season_id
    assert len(payload["sounds"]) == 1
    sound = payload["sounds"][0]
    assert sound["sound_id"] == "7540327234013301517"
    assert sound["related_post_count"] == 12
    assert sound["creator_post_count"] == 3


def test_get_tiktok_content_health_flags_underperforming_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    season_id = "11111111-1111-1111-1111-111111111111"

    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda platform, column: platform == "tiktok"
        and column in {"saves", "quality_score", "velocity_24h", "sound_id", "sound_title", "sound_author"},
    )

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda _sql, _params=None: [
            {
                "id": "a",
                "video_id": "vid-strong",
                "source_account": "creator",
                "posted_at": datetime(2026, 2, 1, tzinfo=UTC),
                "caption": "strong post",
                "thumbnail_url": "https://example.com/thumb.jpg",
                "url": "https://www.tiktok.com/@creator/video/vid-strong",
                "views": 1000,
                "likes": 200,
                "comments": 100,
                "shares": 50,
                "saves": 80,
                "quality_score": 0.9,
                "velocity_24h": 100,
                "sound_id": "s1",
                "sound_title": "sound one",
                "sound_author": "artist",
            },
            {
                "id": "b",
                "video_id": "vid-weak",
                "source_account": "creator",
                "posted_at": datetime(2026, 2, 2, tzinfo=UTC),
                "caption": "",
                "thumbnail_url": "",
                "url": "https://www.tiktok.com/@creator/video/vid-weak",
                "views": 1200,
                "likes": 90,
                "comments": 10,
                "shares": 10,
                "saves": 5,
                "quality_score": 0.2,
                "velocity_24h": 10,
                "sound_id": "s2",
                "sound_title": "sound two",
                "sound_author": "artist",
            },
        ],
    )

    payload = social_repo.get_tiktok_content_health(season_id, limit=10)

    assert payload["season_id"] == season_id
    assert payload["posts"]
    weak = next(item for item in payload["posts"] if item["post_id"] == "vid-weak")
    assert "low_saves" in weak["reason_flags"]
    assert "low_comments" in weak["reason_flags"]
    assert "missing_thumbnail" in weak["reason_flags"]
    assert "missing_caption" in weak["reason_flags"]
