"""X/Twitter cookie refresh helpers backed by Playwright."""

from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path
from typing import Any

from trr_backend.socials.browser_cookie_refresh import (
    cookie_payload,
    open_cookie_refresh_context,
    validate_browser_cookie_session,
    write_cookie_file,
)

logger = logging.getLogger(__name__)

LOGIN_URLS = (
    "https://x.com/i/flow/login",
    "https://x.com/login",
    "https://twitter.com/i/flow/login",
)
_TWITTER_COOKIE_DOMAINS = (".x.com", "x.com", ".twitter.com", "twitter.com")
_TWITTER_INVALID_URL_MARKERS = ("/i/flow/login", "/login", "/account/access", "/account/login_challenge")


def _remaining_timeout_ms(deadline: float, *, floor_ms: int = 1_000) -> int:
    remaining = int(max(0.0, deadline - time.monotonic()) * 1_000)
    return max(floor_ms, remaining)


def _body_text(page: Any) -> str:
    try:
        return page.locator("body").inner_text(timeout=2_000)
    except Exception:  # noqa: BLE001
        return ""


def _twitter_invalid_state_reason(page: Any, *, after_password: bool) -> str | None:
    current_url = str(page.url or "")
    current_url_lower = current_url.lower()
    for marker in _TWITTER_INVALID_URL_MARKERS:
        if marker in current_url_lower and (after_password or marker not in {"/i/flow/login", "/login"}):
            if "login_challenge" in marker or "account/access" in marker:
                return f"Twitter login requires additional verification ({current_url})"
            if after_password:
                return f"Twitter remained on the login flow after password submission ({current_url})"

    body_text = _body_text(page).lower()
    if "something went wrong" in body_text:
        return "Twitter login page returned an error shell"
    if after_password and "enter your password" in body_text and "log in" in body_text:
        return "Twitter rejected the configured credentials or requires a different account identifier"
    if "phone, email, or username" in body_text and "next" in body_text and after_password:
        return "Twitter restarted the login flow after password submission"
    return None


def _wait_for_visible(locator: Any, *, deadline: float, timeout_cap_ms: int = 10_000) -> bool:
    try:
        locator.wait_for(state="visible", timeout=min(timeout_cap_ms, _remaining_timeout_ms(deadline)))
        return True
    except Exception:  # noqa: BLE001
        return False


def _should_allow_headed_fallback() -> bool:
    return (os.getenv("SOCIAL_TWITTER_COOKIE_REFRESH_ALLOW_HEADED_FALLBACK") or "false").strip().lower() not in {
        "0",
        "false",
        "off",
        "no",
    }


def _refresh_twitter_cookies_once(
    *,
    username: str,
    password: str,
    cookie_file: str | Path,
    headless: bool,
    timeout_seconds: int,
) -> dict[str, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for Twitter cookie refresh") from exc

    deadline = time.monotonic() + max(30, int(timeout_seconds))
    with sync_playwright() as playwright:
        session = open_cookie_refresh_context(
            playwright,
            platform="twitter",
            headless=headless,
            viewport={"width": 1_440, "height": 1_600},
        )
        try:
            context = session.context
            page = context.new_page()
            page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=_remaining_timeout_ms(deadline))
            page.wait_for_timeout(2_000)
            existing_cookies = cookie_payload(context.cookies(), domains=_TWITTER_COOKIE_DOMAINS)
            if existing_cookies.get("auth_token") and existing_cookies.get("ct0"):
                write_cookie_file(cookie_file, existing_cookies)
                return existing_cookies

            for url in LOGIN_URLS:
                page.goto(url, wait_until="domcontentloaded", timeout=_remaining_timeout_ms(deadline, floor_ms=10_000))
                page.wait_for_timeout(4_000)
                error_reason = _twitter_invalid_state_reason(page, after_password=False)
                if error_reason != "Twitter login page returned an error shell":
                    break
            if (
                _twitter_invalid_state_reason(page, after_password=False)
                == "Twitter login page returned an error shell"
            ):
                raise RuntimeError("Twitter login page returned an error shell before credentials were entered")

            username_input = page.locator('input[autocomplete="username"], input[name="text"]').first
            if not _wait_for_visible(username_input, deadline=deadline):
                error_reason = _twitter_invalid_state_reason(page, after_password=False)
                raise RuntimeError(error_reason or "Twitter login page never presented the username field")
            username_input.fill(username, timeout=_remaining_timeout_ms(deadline))
            page.get_by_role("button", name=re.compile(r"^next$", re.IGNORECASE)).first.click(
                timeout=_remaining_timeout_ms(deadline),
            )
            page.wait_for_timeout(2_000)

            follow_up_input = page.locator('input[name="text"]').first
            try:
                if follow_up_input.is_visible(timeout=1_000):
                    follow_up_input.fill(username, timeout=_remaining_timeout_ms(deadline))
                    page.get_by_role("button", name=re.compile(r"^next$", re.IGNORECASE)).first.click(
                        timeout=_remaining_timeout_ms(deadline),
                    )
                    page.wait_for_timeout(2_000)
            except Exception:  # noqa: BLE001
                pass

            password_input = page.locator('input[name="password"]').first
            if not _wait_for_visible(password_input, deadline=deadline):
                error_reason = _twitter_invalid_state_reason(page, after_password=False)
                raise RuntimeError(error_reason or "Twitter login never reached the password step")
            password_input.fill(password, timeout=_remaining_timeout_ms(deadline))
            page.get_by_role("button", name=re.compile(r"^log in$", re.IGNORECASE)).first.click(
                timeout=_remaining_timeout_ms(deadline),
            )

            cookies: dict[str, str] = {}
            submitted_at = time.monotonic()
            while time.monotonic() < deadline:
                cookies = cookie_payload(context.cookies(), domains=_TWITTER_COOKIE_DOMAINS)
                if cookies.get("auth_token") and cookies.get("ct0"):
                    break
                if (time.monotonic() - submitted_at) >= 3:
                    error_reason = _twitter_invalid_state_reason(page, after_password=True)
                    if error_reason:
                        raise RuntimeError(error_reason)
                page.wait_for_timeout(750)
            if not (cookies.get("auth_token") and cookies.get("ct0")):
                error_reason = _twitter_invalid_state_reason(page, after_password=True)
                if error_reason:
                    raise RuntimeError(error_reason)
                raise RuntimeError("Twitter login completed without auth_token/ct0 cookies")

            is_valid, reason = validate_browser_cookie_session(
                cookies=cookies,
                validation_url="https://x.com/home",
                cookie_domains=_TWITTER_COOKIE_DOMAINS,
                required_cookie_names_any=("auth_token",),
                required_cookie_names_all=("ct0",),
                invalid_url_markers=_TWITTER_INVALID_URL_MARKERS,
                invalid_body_patterns=(r"Something went wrong", r"Log in to X"),
            )
            if not is_valid:
                raise RuntimeError(
                    f"Twitter cookie refresh produced an invalid authenticated session ({reason or 'unknown'})"
                )

            write_cookie_file(cookie_file, cookies)
            return cookies
        except PlaywrightTimeoutError as exc:
            raise RuntimeError("Timed out while refreshing Twitter cookies") from exc
        finally:
            session.close()


def refresh_twitter_cookies(
    *,
    username: str,
    password: str,
    cookie_file: str | Path,
    headless: bool = True,
    timeout_seconds: int = 120,
) -> dict[str, str]:
    try:
        return _refresh_twitter_cookies_once(
            username=username,
            password=password,
            cookie_file=cookie_file,
            headless=headless,
            timeout_seconds=timeout_seconds,
        )
    except RuntimeError as exc:
        if headless and _should_allow_headed_fallback() and "error shell" in str(exc).lower():
            logger.info("Retrying Twitter cookie refresh in headed mode after headless error shell")
            return _refresh_twitter_cookies_once(
                username=username,
                password=password,
                cookie_file=cookie_file,
                headless=False,
                timeout_seconds=timeout_seconds,
            )
        raise
