"""Compatibility bridge for account-catalog progress reads.

Canonical owner: `trr_backend.socials.pipelines.account_catalog.progress`.
Retained while legacy account-catalog imports are migrated to `pipelines`.
"""

from __future__ import annotations

from trr_backend.socials.pipelines.account_catalog.progress import get_social_account_catalog_run_progress

_LOCAL_ROOM_NAMES = {"get_social_account_catalog_run_progress"}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}

__all__ = ["get_social_account_catalog_run_progress"]
