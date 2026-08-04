"""Compatibility package for account-catalog scraper orchestration.

Canonical owner: `trr_backend.socials.pipelines.account_catalog`.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from trr_backend.socials.read_models.account_profile.common import (
    get_social_account_profile_collaborators_tags,
    get_social_account_profile_comments,
    get_social_account_profile_hashtags,
    get_social_account_profile_posts,
    get_social_account_profile_summary,
)

_LAUNCH_EXPORTS = frozenset(
    {
        "begin_social_account_catalog_backfill_launch",
        "finalize_social_account_catalog_backfill_launch",
        "get_instagram_catalog_launch_capacity",
        "launch_social_account_catalog_backfill",
        "start_social_account_catalog_backfill",
    }
)
_PROGRESS_EXPORT = "get_social_account_catalog_run_progress"

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


def __getattr__(name: str) -> Any:
    if name == _PROGRESS_EXPORT:
        module = import_module("trr_backend.socials.pipelines.account_catalog.progress")
    elif name in _LAUNCH_EXPORTS:
        module = import_module("trr_backend.socials.pipelines.account_catalog.launch")
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
