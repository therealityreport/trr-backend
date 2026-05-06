"""Canonical account-catalog review queue bridge.

The social-season core still owns the implementation for this slice. This
module is the package-routing owner so callers can move to
`pipelines.account_catalog` before the DB-heavy implementation is extracted.
"""

from __future__ import annotations

from trr_backend.socials.social_season_analytics_impl import (
    get_social_account_catalog_review_queue,
    resolve_social_account_catalog_review_queue_item,
)

__all__ = [
    "get_social_account_catalog_review_queue",
    "resolve_social_account_catalog_review_queue_item",
]
