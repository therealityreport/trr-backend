"""Facebook cookie refresh helpers backed by Playwright."""

from __future__ import annotations

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
    )
