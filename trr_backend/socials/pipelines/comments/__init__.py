"""Comment scrape pipeline orchestration.

Platform-specific modules in this package coordinate comment scrape launches,
dry-run previews, progress reads, cancellation, and incomplete-fill planning.
Persistence remains platform-owned.
"""

from __future__ import annotations

from importlib import import_module

from trr_backend.socials.pipelines.comments.instagram import (
    append_instagram_comments_catalog_stream_targets_to_active_run,
    cancel_social_account_comments_job,
    cancel_social_account_comments_run,
    execute_social_account_comments_run_auth_repair,
    get_active_social_account_comments_run,
    get_social_account_comments_coverage_diagnostics,
    get_social_account_comments_scrape_run_progress,
    preview_social_account_comments_scrape,
    rebalance_failed_instagram_comments_shard,
    rebalance_waiting_instagram_comments_shards,
    repair_instagram_comments_scrape_run_target_gaps,
    request_social_account_comments_run_auth_repair,
    start_social_account_comments_scrape,
)
from trr_backend.socials.provider_registry import register_legacy_patchable_aliases

__all__ = [
    "append_instagram_comments_catalog_stream_targets_to_active_run",
    "cancel_social_account_comments_job",
    "cancel_social_account_comments_run",
    "execute_social_account_comments_run_auth_repair",
    "get_active_social_account_comments_run",
    "get_social_account_comments_coverage_diagnostics",
    "get_social_account_comments_scrape_run_progress",
    "preview_social_account_comments_scrape",
    "rebalance_failed_instagram_comments_shard",
    "rebalance_waiting_instagram_comments_shards",
    "repair_instagram_comments_scrape_run_target_gaps",
    "request_social_account_comments_run_auth_repair",
    "start_social_account_comments_scrape",
]


def _refresh_legacy_patchable_export(name: str):
    return getattr(import_module("trr_backend.socials.pipelines.comments.instagram"), name)


register_legacy_patchable_aliases(globals(), __all__, _refresh_legacy_patchable_export)
