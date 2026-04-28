from __future__ import annotations

from trr_backend.socials.instagram.auth_resolver import resolve_instagram_auth_session
from trr_backend.socials.instagram.scrapling_session import InstagramScraplingSession, resolve_scrapling_session

InstagramCommentsScraplingSession = InstagramScraplingSession


def resolve_comments_scrapling_session(
    *,
    browser_account_id: str | None,
    caller_context: str,
) -> InstagramScraplingSession:
    return resolve_scrapling_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
        resolver=resolve_instagram_auth_session,
    )
