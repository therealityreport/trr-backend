"""Shared-account catalog and profile flows for the social control plane."""

from __future__ import annotations

from trr_backend.socials.control_plane.shared_status_reads import (
    get_season_shared_status,
    list_shared_runs,
)
from trr_backend.socials.instagram.persistence import _batch_upsert_shared_catalog_instagram_posts
from trr_backend.socials.pipelines.account_catalog.progress import get_social_account_catalog_run_progress
from trr_backend.socials.read_models.account_profile.common import (
    get_social_account_profile_collaborators_tags,
    get_social_account_profile_comments,
    get_social_account_profile_hashtags,
    get_social_account_profile_posts,
    get_social_account_profile_summary,
)
from trr_backend.socials.social_season_analytics_impl import (
    _default_targets,
    _normalize_catalog_backfill_window,
    _shared_account_catalog_requires_modal_executor,
    cancel_shared_run,
    cancel_social_account_catalog_run,
    dismiss_social_account_catalog_run,
    get_season_context,
    get_shared_account_sources,
    get_social_account_catalog_freshness,
    get_social_account_catalog_gap_analysis_status,
    get_social_account_catalog_posts,
    get_social_account_catalog_review_queue,
    get_social_account_catalog_verification,
    get_social_account_profile_hashtag_timeline,
    get_targets,
    list_shared_review_queue,
    put_shared_account_sources,
    put_social_account_profile_hashtags,
    put_targets,
    resolve_shared_review_queue_item,
    resolve_social_account_catalog_review_queue_item,
)

batch_upsert_shared_catalog_instagram_posts = _batch_upsert_shared_catalog_instagram_posts
default_targets = _default_targets
normalize_catalog_backfill_window = _normalize_catalog_backfill_window
shared_account_catalog_requires_modal_executor = _shared_account_catalog_requires_modal_executor

__all__ = [
    "_batch_upsert_shared_catalog_instagram_posts",
    "_default_targets",
    "_normalize_catalog_backfill_window",
    "_shared_account_catalog_requires_modal_executor",
    "batch_upsert_shared_catalog_instagram_posts",
    "cancel_shared_run",
    "cancel_social_account_catalog_run",
    "default_targets",
    "dismiss_social_account_catalog_run",
    "get_season_context",
    "get_season_shared_status",
    "get_shared_account_sources",
    "get_social_account_catalog_freshness",
    "get_social_account_catalog_gap_analysis_status",
    "get_social_account_catalog_posts",
    "get_social_account_catalog_review_queue",
    "get_social_account_catalog_run_progress",
    "get_social_account_catalog_verification",
    "get_social_account_profile_collaborators_tags",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtag_timeline",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_posts",
    "get_social_account_profile_summary",
    "get_targets",
    "list_shared_review_queue",
    "list_shared_runs",
    "normalize_catalog_backfill_window",
    "put_shared_account_sources",
    "put_social_account_profile_hashtags",
    "put_targets",
    "resolve_shared_review_queue_item",
    "resolve_social_account_catalog_review_queue_item",
    "shared_account_catalog_requires_modal_executor",
]
