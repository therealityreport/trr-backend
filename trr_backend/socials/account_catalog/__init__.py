"""Compatibility package for account-catalog scraper orchestration.

Canonical owner: `trr_backend.socials.pipelines.account_catalog`.
"""

from __future__ import annotations

from trr_backend.socials.pipelines.account_catalog.launch import (
    begin_social_account_catalog_backfill_launch,
    finalize_social_account_catalog_backfill_launch,
    get_instagram_catalog_launch_capacity,
    launch_social_account_catalog_backfill,
    start_social_account_catalog_backfill,
)
from trr_backend.socials.pipelines.account_catalog.progress import get_social_account_catalog_run_progress
from trr_backend.socials.read_models.account_profile.common import (
    get_social_account_profile_collaborators_tags,
    get_social_account_profile_comments,
    get_social_account_profile_hashtags,
    get_social_account_profile_posts,
    get_social_account_profile_summary,
)

__all__ = [
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "get_social_account_catalog_run_progress",
    "get_instagram_catalog_launch_capacity",
    "get_social_account_profile_collaborators_tags",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_posts",
    "get_social_account_profile_summary",
    "launch_social_account_catalog_backfill",
    "start_social_account_catalog_backfill",
]
