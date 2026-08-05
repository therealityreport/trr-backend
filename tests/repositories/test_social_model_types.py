from __future__ import annotations

import pickle
from dataclasses import asdict
from datetime import UTC, date, datetime

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.control_plane.models as control_plane_models
import trr_backend.socials.model_types as model_types
import trr_backend.socials.social_season_analytics_impl as social_impl


def test_social_model_types_preserve_public_identity_and_constants() -> None:
    for name in ("SeasonContext", "WeekWindow", "IngestOptions", "SentimentAnalyzerContext"):
        canonical = getattr(model_types, name)
        assert getattr(control_plane_models, name) is canonical
        assert getattr(control_plane, name) is canonical
        assert getattr(social_impl, name) is canonical
        assert getattr(legacy_repo, name) is canonical

    for name, expected in (
        ("COMMENT_MEDIA_MIRROR_STAGE", "comment_media_mirror"),
        ("DEFAULT_COMMENT_REFRESH_POLICY", "balanced"),
        ("DEFAULT_YOUTUBE_SOURCE_MODE", "hybrid"),
    ):
        canonical = getattr(model_types, name)
        assert canonical == expected
        assert getattr(control_plane_models, name) is canonical
        assert getattr(social_impl, name) is canonical
        assert getattr(legacy_repo, name) is canonical


def test_social_model_types_preserve_defaults_slots_and_asdict_shape() -> None:
    context = model_types.SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    assert model_types.SeasonContext.__slots__ == (
        "season_id",
        "show_id",
        "show_name",
        "season_number",
        "anchor_date",
        "show_slug",
    )
    assert not hasattr(context, "__dict__")
    assert asdict(context) == {
        "season_id": "season-1",
        "show_id": "show-1",
        "show_name": "Test Show",
        "season_number": 6,
        "anchor_date": date(2025, 1, 1),
        "show_slug": None,
    }

    window = model_types.WeekWindow(
        week_index=2,
        start_local=datetime(2025, 1, 8, tzinfo=UTC),
        end_local=datetime(2025, 1, 15, tzinfo=UTC),
    )
    assert model_types.WeekWindow.__slots__ == (
        "week_index",
        "start_local",
        "end_local",
        "week_type",
        "episode_number",
    )
    assert not hasattr(window, "__dict__")
    assert asdict(window) == {
        "week_index": 2,
        "start_local": datetime(2025, 1, 8, tzinfo=UTC),
        "end_local": datetime(2025, 1, 15, tzinfo=UTC),
        "week_type": "episode",
        "episode_number": None,
    }

    options = model_types.IngestOptions(
        platforms={"instagram"},
        source_scope="network",
        sync_strategy="incremental",
        max_posts_per_target=20,
        max_comments_per_post=50,
        max_replies_per_post=10,
        fetch_replies=True,
        ingest_mode="posts_and_comments",
        date_start=None,
        date_end=None,
    )
    assert not hasattr(options, "__dict__")
    assert options.comment_refresh_policy == "balanced"
    assert options.youtube_source_mode == "hybrid"
    assert options.comment_anchor_source_ids is None
    assert options.sound_ids is None
    assert options.youtube_force_reindex is False
    assert options.youtube_force_media_refresh is False
    assert options.youtube_force_comment_refresh is False
    assert options.comments_enable_media_followups is False
    assert options.details_refresh_skip_detail_fetch is False
    assert options.details_refresh_force_detail_fetch is False
    assert options.details_refresh_skip_media_followups is False

    sentiment = model_types.SentimentAnalyzerContext(
        cast_terms={"cast"},
        cast_phrases={"cast member"},
        episode_terms={"episode"},
        episode_summary="Summary",
    )
    assert model_types.SentimentAnalyzerContext.__slots__ == (
        "cast_terms",
        "cast_phrases",
        "episode_terms",
        "episode_summary",
    )
    assert not hasattr(sentiment, "__dict__")


def test_social_model_types_remain_pickle_compatible() -> None:
    context = model_types.SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name=None,
        season_number=1,
        anchor_date=date(2025, 1, 1),
    )
    restored = pickle.loads(pickle.dumps(context))  # noqa: S301
    assert restored == context
    assert type(restored) is model_types.SeasonContext

    original_module = model_types.SeasonContext.__module__
    try:
        model_types.SeasonContext.__module__ = social_impl.__name__
        legacy_payload = pickle.dumps(context)
    finally:
        model_types.SeasonContext.__module__ = original_module
    legacy_restored = pickle.loads(legacy_payload)  # noqa: S301
    assert legacy_restored == context
    assert type(legacy_restored) is model_types.SeasonContext

    legacy_global = pickle.loads(  # noqa: S301
        b"ctrr_backend.socials.social_season_analytics_impl\nSeasonContext\n."
    )
    assert legacy_global is model_types.SeasonContext
