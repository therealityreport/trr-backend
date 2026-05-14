"""Shared browser-backed cookie validation and refresh helpers."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SimpleLoginSpec:
    login_url: str
    validation_url: str
    cookie_domains: tuple[str, ...]
    username_selectors: tuple[str, ...]
    password_selectors: tuple[str, ...]
    submit_selectors: tuple[str, ...]
    required_cookie_names_any: tuple[str, ...]
    required_cookie_names_all: tuple[str, ...] = ()
    invalid_url_markers: tuple[str, ...] = ()
    invalid_body_patterns: tuple[str, ...] = ()
    post_login_button_patterns: tuple[str, ...] = ()
    pre_login_button_patterns: tuple[str, ...] = ()


def _remaining_timeout_ms(deadline: float, *, floor_ms: int = 1_000) -> int:
    remaining = int(max(0.0, deadline - time.monotonic()) * 1_000)
    return max(floor_ms, remaining)


def _body_text(page: Any) -> str:
    try:
        return page.locator("body").inner_text(timeout=2_000)
    except Exception:  # noqa: BLE001
        return ""


def launch_browser(playwright: Any, *, headless: bool) -> Any:
    launch_kwargs: dict[str, Any] = {
        "headless": bool(headless),
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    try:
        return playwright.chromium.launch(channel="chrome", **launch_kwargs)
    except Exception:
        return playwright.chromium.launch(**launch_kwargs)


def cookie_payload(cookies: list[dict[str, Any]], *, domains: tuple[str, ...]) -> dict[str, str]:
    payload: dict[str, str] = {}
    for cookie in cookies:
        domain = str(cookie.get("domain") or "").lower()
        if domains and not any(domain.endswith(candidate.lower()) for candidate in domains):
            continue
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "")
        if not name or not value:
            continue
        payload[name] = value
    return payload


def write_cookie_file(cookie_file: str | Path, cookies: dict[str, str]) -> None:
    target = Path(cookie_file).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(cookies, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _locate_first(page: Any, selectors: tuple[str, ...], *, deadline: float) -> Any:
    last_error: Exception | None = None
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=min(3_000, _remaining_timeout_ms(deadline)))
            return locator
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError(f"Unable to locate any selector from {selectors}") from last_error


def dismiss_post_login_buttons(page: Any, *, button_patterns: tuple[str, ...], deadline: float) -> None:
    for pattern in button_patterns:
        try:
            locator = page.get_by_role("button", name=re.compile(pattern, re.IGNORECASE)).first
            if locator.is_visible(timeout=500):
                locator.click(timeout=min(2_000, _remaining_timeout_ms(deadline)))
                page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            continue


def click_pre_login_buttons(page: Any, *, button_patterns: tuple[str, ...], deadline: float) -> None:
    for pattern in button_patterns:
        for role in ("button", "link"):
            try:
                locator = page.get_by_role(role, name=re.compile(pattern, re.IGNORECASE)).first
                if locator.is_visible(timeout=500):
                    locator.click(timeout=min(2_000, _remaining_timeout_ms(deadline)))
                    page.wait_for_timeout(1_000)
                    return
            except Exception:  # noqa: BLE001
                continue


def _has_required_authenticated_cookies(cookies: dict[str, str], *, spec: SimpleLoginSpec) -> bool:
    missing_all = [name for name in spec.required_cookie_names_all if not str(cookies.get(name) or "").strip()]
    if missing_all:
        return False
    if not spec.required_cookie_names_any:
        return True
    return any(str(cookies.get(name) or "").strip() for name in spec.required_cookie_names_any)


def _detect_invalid_login_state(page: Any, *, spec: SimpleLoginSpec) -> str | None:
    current_url = str(page.url or "")
    body_text = _body_text(page)
    for pattern in spec.invalid_body_patterns:
        if re.search(pattern, body_text, re.IGNORECASE):
            return f"Login page reported an authentication failure: {pattern}"
    current_url_lower = current_url.lower()
    for marker in spec.invalid_url_markers:
        if marker.lower() in current_url_lower:
            return f"Login redirected to invalid URL: {current_url}"
    return None


def validate_browser_cookie_session(
    *,
    cookies: dict[str, str],
    validation_url: str,
    cookie_domains: tuple[str, ...],
    required_cookie_names_any: tuple[str, ...] = (),
    required_cookie_names_all: tuple[str, ...] = (),
    invalid_url_markers: tuple[str, ...] = (),
    invalid_body_patterns: tuple[str, ...] = (),
    timeout_seconds: int = 45,
) -> tuple[bool, str | None]:
    missing_all = [name for name in required_cookie_names_all if not str(cookies.get(name) or "").strip()]
    if missing_all:
        return False, f"missing_required_cookie:{','.join(missing_all)}"
    has_any_required_cookie = any(str(cookies.get(name) or "").strip() for name in required_cookie_names_any)
    if required_cookie_names_any and not has_any_required_cookie:
        return False, f"missing_any_cookie:{','.join(required_cookie_names_any)}"

    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        return False, f"playwright_unavailable:{type(exc).__name__}"

    deadline = time.monotonic() + max(20, int(timeout_seconds))
    with sync_playwright() as playwright:
        browser = launch_browser(playwright, headless=True)
        try:
            context = browser.new_context(viewport={"width": 1_280, "height": 1_400})
            context.add_cookies(
                [
                    {
                        "name": name,
                        "value": value,
                        "domain": cookie_domains[0],
                        "path": "/",
                        "secure": True,
                    }
                    for name, value in cookies.items()
                    if value
                ]
            )
            page = context.new_page()
            try:
                response = page.goto(
                    validation_url,
                    wait_until="domcontentloaded",
                    timeout=_remaining_timeout_ms(deadline, floor_ms=5_000),
                )
                page.wait_for_timeout(3_000)
            except PlaywrightTimeoutError:
                return False, "validation_timeout"
            except PlaywrightError as exc:
                return False, f"validation_navigation_failed:{type(exc).__name__}"
            if response is not None and int(response.status or 0) >= 400:
                return False, f"validation_http_status:{response.status}"

            current_url = str(page.url or "").lower()
            if any(marker.lower() in current_url for marker in invalid_url_markers):
                return False, f"invalid_redirect:{page.url}"
            try:
                body_text = page.locator("body").inner_text(timeout=2_000)
            except Exception:  # noqa: BLE001
                body_text = ""
            if any(re.search(pattern, body_text, re.IGNORECASE) for pattern in invalid_body_patterns):
                return False, "login_prompt_detected"
            return True, None
        finally:
            browser.close()


def refresh_simple_login_cookies(
    *,
    spec: SimpleLoginSpec,
    username: str,
    password: str,
    cookie_file: str | Path,
    headless: bool = True,
    timeout_seconds: int = 120,
) -> dict[str, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for cookie refresh") from exc

    deadline = time.monotonic() + max(30, int(timeout_seconds))
    refreshed_cookies: dict[str, str] = {}
    with sync_playwright() as playwright:
        browser = launch_browser(playwright, headless=headless)
        try:
            context = browser.new_context(viewport={"width": 1_440, "height": 1_600})
            page = context.new_page()
            page.goto(
                spec.login_url,
                wait_until="domcontentloaded",
                timeout=_remaining_timeout_ms(deadline, floor_ms=10_000),
            )
            page.wait_for_timeout(2_000)
            click_pre_login_buttons(
                page,
                button_patterns=spec.pre_login_button_patterns,
                deadline=deadline,
            )

            _locate_first(page, spec.username_selectors, deadline=deadline).fill(
                username,
                timeout=_remaining_timeout_ms(deadline),
            )
            _locate_first(page, spec.password_selectors, deadline=deadline).fill(
                password,
                timeout=_remaining_timeout_ms(deadline),
            )
            _locate_first(page, spec.submit_selectors, deadline=deadline).click(
                timeout=_remaining_timeout_ms(deadline),
            )

            cookies: dict[str, str] = {}
            login_error_reason: str | None = None
            submitted_at = time.monotonic()
            while time.monotonic() < deadline:
                cookies = cookie_payload(context.cookies(), domains=spec.cookie_domains)
                if _has_required_authenticated_cookies(cookies, spec=spec):
                    break
                if (time.monotonic() - submitted_at) >= 3:
                    login_error_reason = _detect_invalid_login_state(page, spec=spec)
                    if login_error_reason:
                        raise RuntimeError(login_error_reason)
                page.wait_for_timeout(750)
            if not _has_required_authenticated_cookies(cookies, spec=spec):
                login_error_reason = login_error_reason or _detect_invalid_login_state(page, spec=spec)
                if login_error_reason:
                    raise RuntimeError(login_error_reason)
                raise RuntimeError("Cookie refresh completed without the required authenticated cookies")

            dismiss_post_login_buttons(
                page,
                button_patterns=spec.post_login_button_patterns,
                deadline=deadline,
            )
            refreshed_cookies = cookie_payload(context.cookies(), domains=spec.cookie_domains)
            if not _has_required_authenticated_cookies(refreshed_cookies, spec=spec):
                raise RuntimeError("Authenticated cookies disappeared before refresh completed")
        except PlaywrightTimeoutError as exc:
            raise RuntimeError("Timed out while refreshing cookies") from exc
        finally:
            browser.close()

    is_valid, reason = validate_browser_cookie_session(
        cookies=refreshed_cookies,
        validation_url=spec.validation_url,
        cookie_domains=spec.cookie_domains,
        required_cookie_names_any=spec.required_cookie_names_any,
        required_cookie_names_all=spec.required_cookie_names_all,
        invalid_url_markers=spec.invalid_url_markers,
        invalid_body_patterns=spec.invalid_body_patterns,
    )
    if not is_valid:
        raise RuntimeError(f"Cookie refresh produced an invalid authenticated session ({reason or 'unknown'})")
    write_cookie_file(cookie_file, refreshed_cookies)
    return refreshed_cookies
