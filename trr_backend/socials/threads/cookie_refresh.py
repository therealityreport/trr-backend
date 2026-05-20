"""Threads cookie refresh helpers backed by Playwright."""

from __future__ import annotations

import logging
from dataclasses import replace

from trr_backend.socials.browser_cookie_refresh import SimpleLoginSpec, refresh_simple_login_cookies

logger = logging.getLogger(__name__)

_SPEC = SimpleLoginSpec(
    platform="threads",
    login_url="https://www.threads.com/login",
    validation_url="https://www.threads.com/@threads",
    cookie_domains=(".threads.com", "www.threads.com", ".instagram.com"),
    username_selectors=(
        'input[autocomplete="username"]',
        'input[placeholder="Username, phone or email"]',
        'input[name="email"]',
        'input[name="username"]',
    ),
    password_selectors=('input[autocomplete="current-password"]', 'input[type="password"]', 'input[name="pass"]'),
    submit_selectors=('button:has-text("Log in")', '[role="button"]:has-text("Log in")'),
    required_cookie_names_any=("sessionid",),
    required_cookie_names_all=("csrftoken",),
    invalid_url_markers=("/login",),
    invalid_body_patterns=(r"Log in with your Instagram account", r"Continue with Instagram", r"Page Not Found"),
    post_login_button_patterns=(r"^Not now$",),
    pre_login_button_patterns=(r"Continue with Instagram", r"Log in with Instagram"),
)
_DIRECT_LOGIN_SPEC = replace(_SPEC, pre_login_button_patterns=())


def refresh_threads_cookies(
    *,
    username: str,
    password: str,
    cookie_file: str,
    headless: bool = True,
    timeout_seconds: int = 120,
) -> dict[str, str]:
    try:
        return refresh_simple_login_cookies(
            spec=_SPEC,
            username=username,
            password=password,
            cookie_file=cookie_file,
            headless=headless,
            timeout_seconds=timeout_seconds,
        )
    except RuntimeError as exc:
        logger.info("Threads Instagram-entry login retrying with direct form fallback: %s", exc)
        return refresh_simple_login_cookies(
            spec=_DIRECT_LOGIN_SPEC,
            username=username,
            password=password,
            cookie_file=cookie_file,
            headless=headless,
            timeout_seconds=timeout_seconds,
        )
