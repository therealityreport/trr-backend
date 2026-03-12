"""TikTok cookie refresh helpers backed by Playwright."""

from __future__ import annotations

from trr_backend.socials.browser_cookie_refresh import SimpleLoginSpec, refresh_simple_login_cookies

_SPEC = SimpleLoginSpec(
    login_url="https://www.tiktok.com/login/phone-or-email/email",
    validation_url="https://www.tiktok.com/messages?lang=en",
    cookie_domains=(".tiktok.com", "www.tiktok.com"),
    username_selectors=('input[name="username"]', 'input[placeholder="Email or username"]'),
    password_selectors=('input[type="password"]', 'input[placeholder="Password"]'),
    submit_selectors=('button[type="submit"]', 'button:has-text("Log in")'),
    required_cookie_names_any=("sessionid", "sessionid_ss", "sid_tt"),
    invalid_url_markers=("/login",),
    invalid_body_patterns=(r"\bLog in\b", r"\bSign up\b", r"Maximum number of attempts reached"),
)


def refresh_tiktok_cookies(
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
