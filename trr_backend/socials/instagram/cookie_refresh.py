"""Instagram cookie refresh helpers backed by Playwright."""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

INSTAGRAM_LOGIN_URL = "https://www.instagram.com/accounts/login/"
LOGIN_BUTTON_NAME_RE = re.compile(r"^log\s*in$", re.IGNORECASE)
NOT_NOW_RE = re.compile(r"^not\s+now$", re.IGNORECASE)
SAVE_INFO_RE = re.compile(r"save your login info\?", re.IGNORECASE)
INVALID_CREDENTIAL_TEXT_RE = re.compile(
    r"(incorrect|wrong password|check your password|unable to log you in|try again later)",
    re.IGNORECASE,
)
CHALLENGE_URL_MARKERS = ("challenge", "two_factor", "checkpoint")
COOKIE_NAMES_PRIORITY = (
    "sessionid",
    "csrftoken",
    "ds_user_id",
    "mid",
    "rur",
    "ig_did",
    "datr",
)


def _locate_login_input(page: Any, *, name: str, fallback_label: str) -> Any:
    try:
        locator = page.locator(f'input[name="{name}"]').first
        locator.wait_for(state="visible", timeout=2_000)
        return locator
    except Exception:  # noqa: BLE001
        return page.get_by_label(fallback_label)


def _remaining_timeout_ms(deadline: float, *, floor_ms: int = 1_000) -> int:
    remaining = int(max(0.0, deadline - time.monotonic()) * 1_000)
    return max(floor_ms, remaining)


def _cookie_payload(cookies: list[dict[str, Any]]) -> dict[str, str]:
    payload: dict[str, str] = {}
    for cookie in cookies:
        domain = str(cookie.get("domain") or "")
        if "instagram.com" not in domain:
            continue
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "")
        if not name or not value:
            continue
        payload[name] = value

    prioritized = {name: payload[name] for name in COOKIE_NAMES_PRIORITY if name in payload}
    remaining = {name: value for name, value in payload.items() if name not in prioritized}
    prioritized.update(remaining)
    return prioritized


def _write_cookie_file(cookie_file: Path, cookies: dict[str, str]) -> None:
    cookie_file.parent.mkdir(parents=True, exist_ok=True)
    cookie_file.write_text(json.dumps(cookies, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _dismiss_optional_post_login_prompts(page: Any, *, deadline: float) -> None:
    for button_name in ("Not now", "Not Now"):
        try:
            locator = page.get_by_role("button", name=re.compile(f"^{re.escape(button_name)}$", re.IGNORECASE)).first
            if locator.is_visible(timeout=500):
                locator.click(timeout=min(2_000, _remaining_timeout_ms(deadline)))
                page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            continue


def _raise_if_login_failed(page: Any) -> None:
    current_url = str(page.url or "").lower()
    if any(marker in current_url for marker in CHALLENGE_URL_MARKERS):
        raise RuntimeError(f"Instagram login requires additional verification ({page.url})")

    body_text = ""
    try:
        body_text = page.locator("body").inner_text(timeout=2_000)
    except Exception:  # noqa: BLE001
        body_text = ""

    if SAVE_INFO_RE.search(body_text):
        return
    if INVALID_CREDENTIAL_TEXT_RE.search(body_text):
        raise RuntimeError("Instagram rejected the configured credentials")


def refresh_instagram_cookies(
    *,
    username: str,
    password: str,
    cookie_file: str | Path,
    headless: bool = True,
    timeout_seconds: int = 120,
    validation_username: str | None = None,
) -> dict[str, str]:
    """Log into Instagram and persist fresh cookies to disk."""

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for Instagram cookie refresh") from exc

    target_file = Path(cookie_file).expanduser()
    deadline = time.monotonic() + max(30, int(timeout_seconds))
    normalized_validation_username = str(validation_username or "").strip().lstrip("@")

    with sync_playwright() as playwright:
        launch_kwargs: dict[str, Any] = {
            "headless": bool(headless),
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        try:
            browser = playwright.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception:
            browser = playwright.chromium.launch(**launch_kwargs)

        try:
            context = browser.new_context(viewport={"width": 1_280, "height": 1_500})
            page = context.new_page()
            page.goto(
                INSTAGRAM_LOGIN_URL,
                wait_until="networkidle",
                timeout=_remaining_timeout_ms(deadline, floor_ms=10_000),
            )

            _locate_login_input(
                page,
                name="email",
                fallback_label="Mobile number, username or email",
            ).fill(
                username,
                timeout=_remaining_timeout_ms(deadline),
            )
            _locate_login_input(
                page,
                name="pass",
                fallback_label="Password",
            ).fill(
                password,
                timeout=_remaining_timeout_ms(deadline),
            )

            login_button = page.get_by_role("button", name=LOGIN_BUTTON_NAME_RE).first
            login_button.wait_for(state="visible", timeout=_remaining_timeout_ms(deadline))
            login_button.click(timeout=_remaining_timeout_ms(deadline))

            cookies: dict[str, str] = {}
            while time.monotonic() < deadline:
                cookies = _cookie_payload(context.cookies())
                if cookies.get("sessionid"):
                    break
                _raise_if_login_failed(page)
                page.wait_for_timeout(750)
            if not cookies.get("sessionid"):
                _raise_if_login_failed(page)
                raise RuntimeError("Instagram login completed without a session cookie")

            _dismiss_optional_post_login_prompts(page, deadline=deadline)

            if normalized_validation_username:
                page.goto(
                    f"https://www.instagram.com/{normalized_validation_username}/",
                    wait_until="domcontentloaded",
                    timeout=_remaining_timeout_ms(deadline),
                )
                page.wait_for_timeout(1_000)
                _raise_if_login_failed(page)

            cookies = _cookie_payload(context.cookies())
            if not cookies.get("sessionid"):
                raise RuntimeError("Instagram session cookie disappeared before refresh completed")

            _write_cookie_file(target_file, cookies)
            logger.info("Refreshed Instagram cookies into %s", target_file)
            return cookies
        except PlaywrightTimeoutError as exc:
            raise RuntimeError("Timed out while refreshing Instagram cookies") from exc
        finally:
            browser.close()
