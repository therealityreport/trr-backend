"""Public Instagram cookie-refresh facade and default GraphQL validation."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import trr_backend.socials.instagram.cookie_refresh_runtime as cookie_refresh_runtime
from trr_backend.socials.browser_cookie_refresh import ensure_private_file_mode

logger = logging.getLogger(__name__)

# Keep the established module-level constants, helpers, and monkeypatch seams
# available while the Playwright implementation lives in the leaf runtime.
DEFAULT_SOCIAL_AUTH_CHROME_PROFILE = cookie_refresh_runtime.DEFAULT_SOCIAL_AUTH_CHROME_PROFILE
INSTAGRAM_LOGIN_URL = cookie_refresh_runtime.INSTAGRAM_LOGIN_URL
LOGIN_BUTTON_NAME_RE = cookie_refresh_runtime.LOGIN_BUTTON_NAME_RE
NOT_NOW_RE = cookie_refresh_runtime.NOT_NOW_RE
SAVE_INFO_RE = cookie_refresh_runtime.SAVE_INFO_RE
INVALID_CREDENTIAL_TEXT_RE = cookie_refresh_runtime.INVALID_CREDENTIAL_TEXT_RE
CHALLENGE_URL_MARKERS = cookie_refresh_runtime.CHALLENGE_URL_MARKERS
COOKIE_NAMES_PRIORITY = cookie_refresh_runtime.COOKIE_NAMES_PRIORITY
_INSTAGRAM_BROWSER_SESSIONS = cookie_refresh_runtime._INSTAGRAM_BROWSER_SESSIONS
_INSTAGRAM_COOKIE_VALIDATION_MODES = cookie_refresh_runtime._INSTAGRAM_COOKIE_VALIDATION_MODES
_locate_login_input = cookie_refresh_runtime._locate_login_input
_remaining_timeout_ms = cookie_refresh_runtime._remaining_timeout_ms
_cookie_payload = cookie_refresh_runtime._cookie_payload
_write_cookie_file = cookie_refresh_runtime._write_cookie_file
_read_cookie_file = cookie_refresh_runtime._read_cookie_file
_validate_session_via_graphql = cookie_refresh_runtime._validate_session_via_graphql
_dismiss_optional_post_login_prompts = cookie_refresh_runtime._dismiss_optional_post_login_prompts
_raise_if_login_failed = cookie_refresh_runtime._raise_if_login_failed
_normalize_validation_mode = cookie_refresh_runtime._normalize_validation_mode
_wait_for_manual_instagram_auth = cookie_refresh_runtime._wait_for_manual_instagram_auth
_find_chrome_profile_dir = cookie_refresh_runtime._find_chrome_profile_dir
open_cookie_refresh_context = cookie_refresh_runtime.open_cookie_refresh_context
resolve_chrome_profile_selection = cookie_refresh_runtime.resolve_chrome_profile_selection
validate_browser_cookie_session = cookie_refresh_runtime.validate_browser_cookie_session
write_private_json_file = cookie_refresh_runtime.write_private_json_file


def read_instagram_cookie_file_metadata(cookie_file: str | Path) -> dict[str, Any]:
    target_file = Path(cookie_file).expanduser()
    if not target_file.is_file():
        return {}
    ensure_private_file_mode(target_file)
    try:
        payload = json.loads(target_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        logger.debug("Failed reading Instagram cookie metadata from %s", target_file, exc_info=True)
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): value for key, value in payload.items() if str(key).startswith("_")}


def _validate_saved_cookies_via_graphql(
    cookies: dict[str, str],
    *,
    validation_username: str,
    timeout_seconds: int,
) -> tuple[bool, str | None]:
    normalized = str(validation_username or "").strip().lstrip("@")
    if not normalized:
        return True, None
    try:
        from trr_backend.socials.instagram.scraper import InstagramScraper
    except Exception as exc:  # noqa: BLE001
        return False, f"graphql_validation_unavailable:{type(exc).__name__}"

    scraper = InstagramScraper(cookies=dict(cookies), browser_account_id=normalized)
    payload = scraper.fetch_posts_graphql(
        normalized,
        delay=0.0,
        request_timeout=(10, min(max(20, int(timeout_seconds)), 45)),
        allow_browser_fallback=False,
        allow_recovery=False,
    )
    payload_data = (payload or {}).get("data") if isinstance(payload, dict) else {}
    connection = (
        payload_data.get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
        if isinstance(payload_data, dict)
        else {}
    )
    if connection.get("edges"):
        return True, None

    retrieval_meta = dict(scraper.last_retrieval_meta or {})
    error_code = str(retrieval_meta.get("error_code") or "").strip().lower()
    error_message = str(retrieval_meta.get("error_message") or "").strip().lower()
    status_code = int(retrieval_meta.get("error_status_code") or 0)
    if error_code == "instagram_graphql_checkpoint_required" or error_message == "checkpoint_required":
        return False, "checkpoint_required"
    if error_code == "redirect_login":
        return False, "redirect_login"
    if error_code in {"instagram_graphql_cursor_unauthorized", "unauthorized"} or status_code == 401:
        return False, "unauthorized"
    if error_code in {"instagram_graphql_cursor_forbidden", "forbidden"} or status_code == 403:
        return False, "forbidden"
    if status_code == 429 and "wait" in error_message:
        return True, "rate_limited_soft_pass"
    return False, error_code or "graphql_validation_failed"


def _default_graphql_validator(
    *,
    validation_username: str | None,
    timeout_seconds: int,
) -> Callable[[dict[str, str]], tuple[bool, str | None]]:
    def _validator(cookies: dict[str, str]) -> tuple[bool, str | None]:
        return _validate_saved_cookies_via_graphql(
            cookies,
            validation_username=str(validation_username or ""),
            timeout_seconds=timeout_seconds,
        )

    return _validator


def refresh_instagram_cookies(
    *,
    username: str,
    password: str,
    cookie_file: str | Path,
    account_id: str | None = None,
    headless: bool = True,
    timeout_seconds: int = 120,
    validation_username: str | None = None,
    validator: Callable[[dict[str, str]], tuple[bool, str | None]] | None = None,
    validation_mode: str | None = "graphql_profile",
) -> dict[str, str]:
    """Log into Instagram and persist fresh cookies to disk."""

    normalized_validation_mode = _normalize_validation_mode(validation_mode)
    effective_validator = validator
    if normalized_validation_mode == "graphql_profile" and effective_validator is None:
        effective_validator = _default_graphql_validator(
            validation_username=validation_username,
            timeout_seconds=timeout_seconds,
        )
    return cookie_refresh_runtime.refresh_instagram_cookies(
        username=username,
        password=password,
        cookie_file=cookie_file,
        account_id=account_id,
        headless=headless,
        timeout_seconds=timeout_seconds,
        validation_username=validation_username,
        validator=effective_validator,
        validation_mode=normalized_validation_mode,
        browser_sessions=_INSTAGRAM_BROWSER_SESSIONS,
        cookie_writer=_write_cookie_file,
    )


def interactive_chrome_login(
    *,
    chrome_profile_name: str = DEFAULT_SOCIAL_AUTH_CHROME_PROFILE,
    cookie_file: str | Path = "data/instagram_cookies.json",
    timeout_seconds: int = 300,
    validation_username: str | None = None,
    account_id: str | None = None,
    headless: bool = True,
    validation_mode: str | None = "graphql_profile",
) -> dict[str, str]:
    """Open Chrome with the user's real profile for Instagram login.

    Args:
        headless: If True (default), runs in the background with the configured
                  Chrome profile. Pass False only for deliberate manual repair.
    """

    normalized_validation_mode = _normalize_validation_mode(validation_mode)
    validator = None
    if normalized_validation_mode == "graphql_profile":
        validator = _default_graphql_validator(
            validation_username=validation_username,
            timeout_seconds=timeout_seconds,
        )
    return cookie_refresh_runtime.interactive_chrome_login(
        chrome_profile_name=chrome_profile_name,
        cookie_file=cookie_file,
        timeout_seconds=timeout_seconds,
        validation_username=validation_username,
        account_id=account_id,
        headless=headless,
        validation_mode=normalized_validation_mode,
        validator=validator,
        browser_sessions=_INSTAGRAM_BROWSER_SESSIONS,
        cookie_reader=_read_cookie_file,
        cookie_writer=_write_cookie_file,
        browser_cookie_validator=validate_browser_cookie_session,
        chrome_profile_resolver=resolve_chrome_profile_selection,
    )
