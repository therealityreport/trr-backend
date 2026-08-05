"""Social account profile read models.

This package owns profile dashboard, posts, comments, hashtags, collaborators,
and related bounded account-profile read paths.
"""

from __future__ import annotations

from importlib import import_module

from trr_backend.socials.provider_registry import register_legacy_patchable_aliases
from trr_backend.socials.read_models.account_profile.comment_breakdown import (
    build_instagram_comment_breakdown,
)
from trr_backend.socials.read_models.account_profile.common import (
    get_social_account_profile_collaborators_tags,
    get_social_account_profile_comments,
    get_social_account_profile_hashtags,
    get_social_account_profile_posts,
    get_social_account_profile_summary,
    get_social_hashtag_assignment_conflict_history,
)

__all__ = [
    "build_instagram_comment_breakdown",
    "get_social_account_profile_collaborators_tags",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_posts",
    "get_social_account_profile_summary",
    "get_social_hashtag_assignment_conflict_history",
]


def _refresh_legacy_patchable_export(name: str):
    return getattr(import_module("trr_backend.socials.read_models.account_profile.common"), name)


register_legacy_patchable_aliases(globals(), __all__, _refresh_legacy_patchable_export)
