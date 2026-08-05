"""Account catalog pipeline orchestration and progress helpers.

This package is the canonical home for catalog launch, progress, review queue,
and freshness workflows. Legacy `trr_backend.socials.account_catalog` modules
may temporarily bridge here while callers migrate.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "begin_social_account_catalog_backfill_launch": "launch",
    "finalize_social_account_catalog_backfill_launch": "launch",
    "get_instagram_catalog_launch_capacity": "launch",
    "get_social_account_catalog_freshness": "freshness",
    "get_social_account_catalog_gap_analysis_status": "freshness",
    "get_social_account_catalog_review_queue": "review_queue",
    "get_social_account_catalog_run_progress": "progress",
    "launch_social_account_catalog_backfill": "launch",
    "resolve_social_account_catalog_review_queue_item": "review_queue",
    "start_social_account_catalog_backfill": "launch",
}

__all__ = [
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "get_social_account_catalog_freshness",
    "get_instagram_catalog_launch_capacity",
    "get_social_account_catalog_gap_analysis_status",
    "get_social_account_catalog_review_queue",
    "get_social_account_catalog_run_progress",
    "launch_social_account_catalog_backfill",
    "resolve_social_account_catalog_review_queue_item",
    "start_social_account_catalog_backfill",
]


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
