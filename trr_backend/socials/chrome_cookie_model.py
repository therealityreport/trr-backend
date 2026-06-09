from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DEFAULT_CHROME_PLUGIN = "@chrome"
DEFAULT_CHROME_PROFILE = "codex@thereality.report"


@dataclass(frozen=True, slots=True)
class SocialCookiePlatformSpec:
    platform: str
    login_url: str
    cookie_domain: str
    required_cookie_names: tuple[str, ...]
    cookie_file_env: str
    cookie_json_env: str
    validation_command: tuple[str, ...]


SUPPORTED_SOCIAL_COOKIE_PLATFORMS: dict[str, SocialCookiePlatformSpec] = {
    "instagram": SocialCookiePlatformSpec(
        platform="instagram",
        login_url="https://www.instagram.com/accounts/login/",
        cookie_domain=".instagram.com",
        required_cookie_names=("sessionid", "csrftoken", "ds_user_id"),
        cookie_file_env="SOCIAL_INSTAGRAM_COOKIES_FILE",
        cookie_json_env="SOCIAL_INSTAGRAM_COOKIES_JSON",
        validation_command=(
            "scripts/socials/refresh_cookies.py",
            "--platform",
            "instagram",
            "--validation-mode",
            "comments_endpoint",
            "--validate-only",
        ),
    ),
    "tiktok": SocialCookiePlatformSpec(
        platform="tiktok",
        login_url="https://www.tiktok.com/login",
        cookie_domain=".tiktok.com",
        required_cookie_names=("sessionid", "sid_tt"),
        cookie_file_env="SOCIAL_TIKTOK_COOKIES_FILE",
        cookie_json_env="SOCIAL_TIKTOK_COOKIES_JSON",
        validation_command=("scripts/socials/refresh_cookies.py", "--platform", "tiktok", "--validate-only"),
    ),
    "twitter": SocialCookiePlatformSpec(
        platform="twitter",
        login_url="https://x.com/i/flow/login",
        cookie_domain=".x.com",
        required_cookie_names=("auth_token", "ct0"),
        cookie_file_env="SOCIAL_TWITTER_COOKIES_FILE",
        cookie_json_env="SOCIAL_TWITTER_COOKIES_JSON",
        validation_command=("scripts/socials/refresh_cookies.py", "--platform", "twitter", "--validate-only"),
    ),
    "x": SocialCookiePlatformSpec(
        platform="twitter",
        login_url="https://x.com/i/flow/login",
        cookie_domain=".x.com",
        required_cookie_names=("auth_token", "ct0"),
        cookie_file_env="SOCIAL_TWITTER_COOKIES_FILE",
        cookie_json_env="SOCIAL_TWITTER_COOKIES_JSON",
        validation_command=("scripts/socials/refresh_cookies.py", "--platform", "twitter", "--validate-only"),
    ),
    "facebook": SocialCookiePlatformSpec(
        platform="facebook",
        login_url="https://www.facebook.com/login",
        cookie_domain=".facebook.com",
        required_cookie_names=("c_user", "xs"),
        cookie_file_env="SOCIAL_FACEBOOK_COOKIES_FILE",
        cookie_json_env="SOCIAL_FACEBOOK_COOKIES_JSON",
        validation_command=("scripts/socials/refresh_cookies.py", "--platform", "facebook", "--validate-only"),
    ),
    "threads": SocialCookiePlatformSpec(
        platform="threads",
        login_url="https://www.threads.net/login",
        cookie_domain=".threads.net",
        required_cookie_names=("sessionid", "csrftoken"),
        cookie_file_env="SOCIAL_THREADS_COOKIES_FILE",
        cookie_json_env="SOCIAL_THREADS_COOKIES_JSON",
        validation_command=("scripts/socials/refresh_cookies.py", "--platform", "threads", "--validate-only"),
    ),
}


def normalize_social_cookie_platform(platform: str) -> str:
    normalized = str(platform or "").strip().lower()
    if normalized not in SUPPORTED_SOCIAL_COOKIE_PLATFORMS:
        supported = ", ".join(sorted(SUPPORTED_SOCIAL_COOKIE_PLATFORMS))
        raise ValueError(f"Unsupported social cookie platform: {platform!r}. Supported: {supported}")
    return normalized


def build_social_cookie_chrome_model(
    platform: str,
    *,
    account_handle: str | None = None,
    chrome_plugin: str = DEFAULT_CHROME_PLUGIN,
    chrome_profile: str = DEFAULT_CHROME_PROFILE,
) -> dict[str, Any]:
    normalized = normalize_social_cookie_platform(platform)
    spec = SUPPORTED_SOCIAL_COOKIE_PLATFORMS[normalized]
    account = str(account_handle or "").strip().lstrip("@") or None
    return {
        "platform": spec.platform,
        "account_handle": account,
        "chrome": {
            "plugin": chrome_plugin,
            "profile": chrome_profile,
            "route": f"{chrome_plugin}(profile: {chrome_profile})",
        },
        "login_url": spec.login_url,
        "cookie_domain": spec.cookie_domain,
        "required_cookie_names": list(spec.required_cookie_names),
        "output_contract": {
            "cookie_file_env": spec.cookie_file_env,
            "cookie_json_env": spec.cookie_json_env,
            "do_not_print_cookie_values": True,
        },
        "operator_steps": [
            "Open the login URL with the configured Chrome profile.",
            "Complete login, challenge, two-factor, or checkpoint prompts manually.",
            "Run the platform validation command and only sync cookies when validation passes.",
        ],
        "validation_command": list(spec.validation_command),
        "safety": {
            "browser_plugin_may_not_inspect_cookie_store": True,
            "requires_operator_confirmation_before_secret_sync": True,
            "secret_values_are_never_included_in_summary": True,
        },
    }
