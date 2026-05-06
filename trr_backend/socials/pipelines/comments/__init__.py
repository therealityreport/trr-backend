"""Comment scrape pipeline orchestration.

Platform-specific modules in this package coordinate comment scrape launches,
dry-run previews, progress reads, cancellation, and incomplete-fill planning.
Persistence remains platform-owned.
"""

from __future__ import annotations

from trr_backend.socials.pipelines.comments.instagram import (
    cancel_social_account_comments_run,
    get_active_social_account_comments_run,
    get_social_account_comments_coverage_diagnostics,
    get_social_account_comments_scrape_run_progress,
    preview_social_account_comments_scrape,
    rebalance_failed_instagram_comments_shard,
    repair_instagram_comments_scrape_run_target_gaps,
    start_social_account_comments_scrape,
)

__all__ = [
    "cancel_social_account_comments_run",
    "get_active_social_account_comments_run",
    "get_social_account_comments_coverage_diagnostics",
    "get_social_account_comments_scrape_run_progress",
    "preview_social_account_comments_scrape",
    "rebalance_failed_instagram_comments_shard",
    "repair_instagram_comments_scrape_run_target_gaps",
    "start_social_account_comments_scrape",
]
