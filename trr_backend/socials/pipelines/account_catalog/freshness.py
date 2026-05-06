"""Canonical account-catalog freshness bridge.

The social-season core still owns freshness and gap-analysis implementation.
This module gives the account-catalog pipeline a stable canonical import path
while that implementation is extracted in a later slice.
"""

from __future__ import annotations

from trr_backend.socials.social_season_analytics_impl import (
    get_social_account_catalog_freshness,
    get_social_account_catalog_gap_analysis_status,
)

__all__ = [
    "get_social_account_catalog_freshness",
    "get_social_account_catalog_gap_analysis_status",
]
