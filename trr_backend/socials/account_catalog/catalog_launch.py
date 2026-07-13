"""Compatibility bridge for account-catalog launch orchestration.

Canonical owner: `trr_backend.socials.pipelines.account_catalog.launch`.
Retained while callers that still import `trr_backend.socials.account_catalog`
are migrated to the universal `pipelines` package root.
"""

from __future__ import annotations

from trr_backend.socials.pipelines.account_catalog.launch import (
    begin_social_account_catalog_backfill_launch,
    finalize_social_account_catalog_backfill_launch,
    get_instagram_catalog_launch_capacity,
    launch_social_account_catalog_backfill,
    start_social_account_catalog_backfill,
)

_LOCAL_ROOM_NAMES = {
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "get_instagram_catalog_launch_capacity",
    "launch_social_account_catalog_backfill",
    "start_social_account_catalog_backfill",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}

__all__ = [
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "get_instagram_catalog_launch_capacity",
    "launch_social_account_catalog_backfill",
    "start_social_account_catalog_backfill",
]
