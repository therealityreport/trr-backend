"""Unit tests for season social analytics helpers."""

from datetime import date

from trr_backend.repositories.social_season_analytics import (
    SeasonContext,
    _default_targets,
    _text_contains_any_term,
    _text_is_trailer_marker,
    _video_matches_season,
    sentiment_for_text,
)


def test_sentiment_for_text_deterministic() -> None:
    assert sentiment_for_text("I love this amazing episode") == ("positive", 2)
    assert sentiment_for_text("This was boring and awful") == ("negative", -2)
    assert sentiment_for_text("Just a comment without sentiment words") == ("neutral", 0)
    assert sentiment_for_text("") == ("neutral", 0)


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
