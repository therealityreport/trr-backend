"""Analytics read models and derived reporting surfaces for the social control plane."""

from __future__ import annotations

from importlib import import_module

from trr_backend.socials.analytics.read_models import (
    _build_drivers,
    _build_ingest_shard_schedule,
    _normalize_week_totals_payload,
    _resolve_depth_defaults,
    _rows_for_platform,
    _rule_based_sentiment_for_text,
    _text_contains_any_term,
    _text_is_trailer_marker,
    _threads_post_matches_show_terms,
    _video_matches_season,
    _week_detail_instagram,
    _week_detail_tiktok,
    _week_summary_fast_threads,
    _week_summary_fast_tiktok,
    _week_summary_fast_youtube,
    _youtube_post_matches_show_terms,
    _youtube_title_is_cross_show_excluded,
    _youtube_video_matches_show_terms,
    build_csv,
    build_pdf,
    get_analytics,
    get_comments_coverage,
    get_mirror_coverage,
    get_post_comments,
    get_tiktok_cast_members,
    get_tiktok_content_health,
    get_tiktok_hashtags,
    get_tiktok_overview,
    get_tiktok_post_detail,
    get_tiktok_sentiment_trends,
    get_tiktok_sound_detail,
    get_tiktok_sound_posts,
    get_tiktok_sounds,
    get_week_detail,
    get_week_detail_summary,
    get_week_detail_summary_fast,
    get_week_live_health_snapshot,
    pdf_filename,
    sentiment_for_text,
)
from trr_backend.socials.provider_registry import register_legacy_patchable_aliases

__all__ = [
    "_build_drivers",
    "_build_ingest_shard_schedule",
    "_normalize_week_totals_payload",
    "_resolve_depth_defaults",
    "_rows_for_platform",
    "_rule_based_sentiment_for_text",
    "_text_contains_any_term",
    "_text_is_trailer_marker",
    "_threads_post_matches_show_terms",
    "_video_matches_season",
    "_week_detail_instagram",
    "_week_detail_tiktok",
    "_week_summary_fast_threads",
    "_week_summary_fast_tiktok",
    "_week_summary_fast_youtube",
    "_youtube_post_matches_show_terms",
    "_youtube_title_is_cross_show_excluded",
    "_youtube_video_matches_show_terms",
    "build_csv",
    "build_pdf",
    "get_analytics",
    "get_comments_coverage",
    "get_mirror_coverage",
    "get_post_comments",
    "get_tiktok_cast_members",
    "get_tiktok_content_health",
    "get_tiktok_hashtags",
    "get_tiktok_overview",
    "get_tiktok_post_detail",
    "get_tiktok_sentiment_trends",
    "get_tiktok_sound_detail",
    "get_tiktok_sound_posts",
    "get_tiktok_sounds",
    "get_week_detail",
    "get_week_detail_summary",
    "get_week_detail_summary_fast",
    "get_week_live_health_snapshot",
    "pdf_filename",
    "sentiment_for_text",
]


def _refresh_legacy_patchable_export(name: str):
    return getattr(import_module("trr_backend.socials.analytics.read_models"), name)


register_legacy_patchable_aliases(globals(), __all__, _refresh_legacy_patchable_export)
