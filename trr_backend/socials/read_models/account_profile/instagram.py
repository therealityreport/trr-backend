"""Instagram account-profile read-model bridge.

The bounded profile read implementations currently live in `common` because
the extracted functions still handle all supported profile platforms. This
module is the Instagram-specific canonical import path for the next extraction
slice.
"""

from __future__ import annotations

from trr_backend.socials.read_models.account_profile.common import (
    get_social_account_profile_collaborators_tags,
    get_social_account_profile_comments,
    get_social_account_profile_hashtags,
    get_social_account_profile_posts,
    get_social_account_profile_summary,
)

__all__ = [
    "get_social_account_profile_collaborators_tags",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_posts",
    "get_social_account_profile_summary",
]
