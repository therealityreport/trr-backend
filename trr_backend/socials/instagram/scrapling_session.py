"""Shared session adapter for Instagram Scrapling lanes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from trr_backend.socials.instagram.auth_resolver import (
    InstagramAuthSession,
    resolve_instagram_auth_session,
)


@dataclass(slots=True)
class InstagramScraplingSession:
    auth_session: InstagramAuthSession
    browser_account_id: str | None
    cookies: list[dict[str, Any]]


def cookies_to_scrapling(cookies: dict[str, str]) -> list[dict[str, Any]]:
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


def scrapling_session_from_auth(
    auth_session: InstagramAuthSession,
    *,
    browser_account_id: str | None,
) -> InstagramScraplingSession:
    effective_browser_account_id = (
        str(auth_session.session_account_id or "").strip()
        or str(auth_session.browser_account_id or "").strip()
        or str(browser_account_id or "").strip()
        or None
    )
    return InstagramScraplingSession(
        auth_session=auth_session,
        browser_account_id=effective_browser_account_id,
        cookies=cookies_to_scrapling(auth_session.cookies),
    )


def resolve_scrapling_session(
    *,
    browser_account_id: str | None,
    caller_context: str,
    resolver: Callable[..., InstagramAuthSession] = resolve_instagram_auth_session,
) -> InstagramScraplingSession:
    auth_session = resolver(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
    )
    return scrapling_session_from_auth(auth_session, browser_account_id=browser_account_id)
