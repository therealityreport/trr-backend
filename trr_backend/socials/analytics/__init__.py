"""Canonical analytics rooms for social scraper read models and exports."""

from __future__ import annotations

from importlib import import_module

from trr_backend.socials.analytics.read_models import (
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
)
from trr_backend.socials.provider_registry import register_legacy_patchable_aliases

__all__ = [
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
]


def _refresh_legacy_patchable_export(name: str):
    return getattr(import_module("trr_backend.socials.analytics.read_models"), name)


register_legacy_patchable_aliases(globals(), __all__, _refresh_legacy_patchable_export)
