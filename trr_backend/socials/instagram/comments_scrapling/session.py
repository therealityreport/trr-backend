from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trr_backend.socials.instagram.auth_resolver import InstagramAuthSession, resolve_instagram_auth_session


@dataclass(slots=True)
class InstagramCommentsScraplingSession:
    auth_session: InstagramAuthSession
    browser_account_id: str | None
    cookies: list[dict[str, Any]]


def _cookies_to_scrapling(cookies: dict[str, str]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for name, value in (cookies or {}).items():
        cookie_name = str(name or "").strip()
        cookie_value = str(value or "").strip()
        if not (cookie_name and cookie_value):
            continue
        payload.append(
            {
                "name": cookie_name,
                "value": cookie_value,
                "domain": ".instagram.com",
                "path": "/",
            }
        )
    return payload


def resolve_comments_scrapling_session(
    *,
    browser_account_id: str | None,
    caller_context: str,
) -> InstagramCommentsScraplingSession:
    auth_session = resolve_instagram_auth_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
        allow_stale_browser_session=False,
    )
    return InstagramCommentsScraplingSession(
        auth_session=auth_session,
        browser_account_id=auth_session.browser_account_id or browser_account_id,
        cookies=_cookies_to_scrapling(auth_session.cookies),
    )
