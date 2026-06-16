"""Facebook cookie refresh helpers backed by Playwright."""

from __future__ import annotations

import requests

from trr_backend.socials.browser_cookie_refresh import SimpleLoginSpec, refresh_simple_login_cookies

_SPEC = SimpleLoginSpec(
    platform="facebook",
    login_url="https://www.facebook.com/login",
    validation_url="https://www.facebook.com/",
    cookie_domains=(".facebook.com", "www.facebook.com"),
    username_selectors=('input[name="email"]',),
    password_selectors=('input[name="pass"]',),
    submit_selectors=('[role="button"][aria-label="Log In"]', 'button[aria-label="Log In"]', 'input[type="submit"]'),
    required_cookie_names_any=("c_user",),
    required_cookie_names_all=("xs",),
    invalid_url_markers=("/login", "/two_step_verification", "/checkpoint/"),
    invalid_body_patterns=(
        r"Log into Facebook",
        r"Create new account",
        r"Enter the login code to continue",
        r"Check your notifications on another device",
    ),
)


def _validate_facebook_cookies_in_protocol(cookies: dict[str, str]) -> tuple[bool, str | None]:
    """Probe facebook.com/me with the cookies; dead sessions hard-redirect to login."""
    if not cookies.get("c_user") or not cookies.get("xs"):
        return False, "missing_required_cookies"
    try:
        response = requests.get(
            "https://www.facebook.com/me",
            cookies=cookies,
            timeout=(10, 30),
            headers={
                "user-agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
                "accept": "text/html,application/xhtml+xml",
            },
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        return False, f"probe_fetch_failed:{exc.__class__.__name__}"
    final_url = str(response.url or "").lower()
    for marker in _SPEC.invalid_url_markers:
        if marker in final_url:
            return False, f"login_redirect:{marker}"
    body = (response.text or "")[:20_000]
    if "Log into Facebook" in body or "Create new account" in body:
        return False, "login_prompt_detected"
    return True, None


def refresh_facebook_cookies(
    *,
    username: str,
    password: str,
    cookie_file: str,
    headless: bool = True,
    timeout_seconds: int = 120,
) -> dict[str, str]:
    return refresh_simple_login_cookies(
        spec=_SPEC,
        username=username,
        password=password,
        cookie_file=cookie_file,
        headless=headless,
        timeout_seconds=timeout_seconds,
        validator=_validate_facebook_cookies_in_protocol,
    )
