"""SocialBlade cookie loading, validation, and refresh helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.socials.browser_cookie_refresh import (
    _body_text,
    cookie_payload,
    launch_browser,
    validate_browser_cookie_session,
    write_cookie_file,
)

SOCIALBLADE_COOKIE_DOMAINS = (".socialblade.com", "socialblade.com")
SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY = ("cf_clearance",)
SOCIALBLADE_ACCESS_DENIED_PATTERNS = (
    r"Access denied",
    r"Error reference number:\s*1020",
    r"SOCIAL BLADE ACCESS DENIED",
)
SOCIALBLADE_STEALTH_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
SOCIALBLADE_STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, "webdriver", { get: () => undefined });
Object.defineProperty(navigator, "platform", { get: () => "MacIntel" });
Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
window.chrome = window.chrome || { runtime: {} };
"""


def _default_socialblade_cookie_file_path() -> Path:
    return social_repo._default_platform_cookie_file_path("socialblade")  # noqa: SLF001


def socialblade_cookie_file_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(  # noqa: SLF001
        _default_socialblade_cookie_file_path(),
        "SOCIALBLADE_COOKIES_FILE",
    )


def _socialblade_validation_url() -> str:
    handle = str(os.getenv("SOCIALBLADE_VALIDATION_HANDLE") or "lisabarlow14").strip() or "lisabarlow14"
    return f"https://socialblade.com/instagram/user/{handle}"


def load_socialblade_cookies_from_sources() -> dict[str, str]:
    return social_repo._load_cookie_map_from_json_or_file(  # noqa: SLF001
        label="socialblade",
        raw_json_env_keys=("SOCIALBLADE_COOKIES_JSON",),
        file_env_keys=("SOCIALBLADE_COOKIES_FILE",),
        default_path=_default_socialblade_cookie_file_path(),
        required_cookie_names_any=SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY,
    )


def load_socialblade_cookies() -> dict[str, str]:
    return load_socialblade_cookies_from_sources()


def validate_socialblade_cookie_health(cookies: dict[str, str]) -> tuple[bool, str | None]:
    if not cookies:
        return False, "no_cookies_loaded"
    return validate_browser_cookie_session(
        cookies=cookies,
        validation_url=_socialblade_validation_url(),
        cookie_domains=SOCIALBLADE_COOKIE_DOMAINS,
        required_cookie_names_any=SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY,
        timeout_seconds=45,
    )


def _body_text_matches_access_denied(body_text: str) -> bool:
    normalized = body_text.lower().replace(" ", "")
    return any(pattern.lower().replace("\\s*", "") in normalized for pattern in SOCIALBLADE_ACCESS_DENIED_PATTERNS)


def normalize_socialblade_cookies(raw_payload: Any) -> list[dict[str, Any]]:
    if isinstance(raw_payload, list):
        normalized: list[dict[str, Any]] = []
        for cookie in raw_payload:
            if not isinstance(cookie, dict):
                continue
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "")
            if not name or not value:
                continue
            rendered = dict(cookie)
            rendered.setdefault("domain", ".socialblade.com")
            rendered.setdefault("path", "/")
            normalized.append(rendered)
        return normalized

    cookie_map = social_repo._coerce_cookie_map(raw_payload)  # noqa: SLF001
    normalized = []
    for name, value in cookie_map.items():
        normalized.append(
            {
                "name": name,
                "value": value,
                "domain": ".socialblade.com",
                "path": "/",
                "secure": True,
            }
        )
    return normalized


def refresh_socialblade_cookies(reason: str | None = None) -> dict[str, str]:
    del reason
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for SocialBlade cookie refresh") from exc

    target_url = _socialblade_validation_url()
    headless_raw = str(os.getenv("SOCIALBLADE_COOKIE_REFRESH_HEADLESS") or "true").strip().lower()
    headless = headless_raw not in {"0", "false", "off", "no"}

    with sync_playwright() as playwright:
        browser = launch_browser(playwright, headless=headless)
        try:
            context = browser.new_context(
                viewport={"width": 1440, "height": 1600},
                user_agent=SOCIALBLADE_STEALTH_USER_AGENT,
                locale="en-US",
                timezone_id="America/New_York",
            )
            context.add_init_script(SOCIALBLADE_STEALTH_INIT_SCRIPT)
            page = context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(4_000)
            body_text = _body_text(page)
            if _body_text_matches_access_denied(body_text):
                raise RuntimeError("SocialBlade cookie refresh was blocked by Cloudflare")
            cookies = cookie_payload(context.cookies(), domains=SOCIALBLADE_COOKIE_DOMAINS)
            if not cookies.get("cf_clearance"):
                raise RuntimeError("SocialBlade cookie refresh did not capture cf_clearance")
            write_cookie_file(socialblade_cookie_file_path(), cookies)
            return cookies
        finally:
            browser.close()
