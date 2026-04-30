"""Instagram cookie refresh helpers backed by Playwright."""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager
from trr_backend.socials.browser_cookie_refresh import validate_browser_cookie_session

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
_INSTAGRAM_BROWSER_SESSIONS = AccountBrowserSessionManager(
    platform="instagram",
    cookie_domains=(".instagram.com",),
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
    payload = {
        "_cookie_refreshed_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        **cookies,
    }
    cookie_file.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_instagram_cookie_file_metadata(cookie_file: str | Path) -> dict[str, Any]:
    target_file = Path(cookie_file).expanduser()
    if not target_file.is_file():
        return {}
    try:
        payload = json.loads(target_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        logger.debug("Failed reading Instagram cookie metadata from %s", target_file, exc_info=True)
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(key): value
        for key, value in payload.items()
        if str(key).startswith("_")
    }


def _read_cookie_file(cookie_file: str | Path) -> dict[str, str]:
    target_file = Path(cookie_file).expanduser()
    if not target_file.is_file():
        return {}
    try:
        payload = json.loads(target_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        logger.debug("Failed reading Instagram cookies from %s", target_file, exc_info=True)
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in payload.items()
        if not str(key).startswith("_") and str(key or "").strip() and str(value or "").strip()
    }


def _validate_session_via_graphql(page: Any, username: str, deadline: float) -> bool:
    """Check that the browser's cookies work for Instagram's GraphQL API.

    Makes a lightweight fetch() call from within the page context (so it
    uses the browser's cookies) to see if the API returns real data.
    """
    try:
        result = page.evaluate(
            """async (username) => {
                try {
                    const resp = await fetch(
                        `https://www.instagram.com/api/v1/users/web_profile_info/?username=${username}`,
                        { credentials: 'include', headers: { 'X-IG-App-ID': '936619743392459' } }
                    );
                    return { status: resp.status, ok: resp.ok };
                } catch (e) {
                    return { status: 0, ok: false, error: e.message };
                }
            }""",
            username,
        )
        status = int(result.get("status") or 0)
        if status == 200:
            logger.info("[instagram] GraphQL validation OK (status=%d)", status)
            return True
        logger.warning("[instagram] GraphQL validation failed (status=%d)", status)
        return False
    except Exception:  # noqa: BLE001
        logger.warning("[instagram] GraphQL validation probe threw an exception", exc_info=True)
        return False


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
    account_id: str | None = None,
    headless: bool = True,
    timeout_seconds: int = 120,
    validation_username: str | None = None,
    validator: Callable[[dict[str, str]], tuple[bool, str | None]] | None = None,
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

            if validator is not None:
                is_valid, validation_reason = validator(cookies)
                if not is_valid:
                    normalized_reason = str(validation_reason or "").strip() or "graphql_validation_failed"
                    raise RuntimeError(
                        f"Instagram login produced cookies that failed GraphQL validation ({normalized_reason})"
                    )

            _INSTAGRAM_BROWSER_SESSIONS.import_bootstrapped_session(
                account_id,
                context.storage_state(),
                fallback_account_id=normalized_validation_username or username,
            )
            _write_cookie_file(target_file, cookies)
            logger.info("Refreshed Instagram cookies into %s", target_file)
            return cookies
        except PlaywrightTimeoutError as exc:
            raise RuntimeError("Timed out while refreshing Instagram cookies") from exc
        finally:
            browser.close()


def interactive_chrome_login(
    *,
    chrome_profile_name: str = "entertainmentdatagroup@gmail.com",
    cookie_file: str | Path = "data/instagram_cookies.json",
    timeout_seconds: int = 300,
    validation_username: str | None = None,
    account_id: str | None = None,
    headless: bool = False,
) -> dict[str, str]:
    """Open Chrome with the user's real profile for Instagram login.

    Args:
        headless: If False (default), shows the browser so you can log in
                  manually and watch it work. If True, runs in background.
    """
    target_file = Path(cookie_file).expanduser()
    if not target_file.is_absolute():
        target_file = Path(__file__).resolve().parent.parent.parent.parent / cookie_file
    normalized_validation_username = str(validation_username or "").strip().lstrip("@")
    normalized_account_id = str(account_id or "").strip().lstrip("@")
    session_account_id = normalized_account_id or normalized_validation_username or chrome_profile_name
    session_paths = _INSTAGRAM_BROWSER_SESSIONS.session_paths(
        session_account_id,
        fallback_account_id=session_account_id,
    )
    saved_cookies = _read_cookie_file(session_paths.cookie_file_path)
    if saved_cookies.get("sessionid"):
        validation_url = (
            f"https://www.instagram.com/{normalized_validation_username}/"
            if normalized_validation_username
            else "https://www.instagram.com/"
        )
        valid, reason = validate_browser_cookie_session(
            cookies=saved_cookies,
            validation_url=validation_url,
            cookie_domains=(".instagram.com",),
            required_cookie_names_any=("sessionid",),
            invalid_url_markers=CHALLENGE_URL_MARKERS,
            invalid_body_patterns=(INVALID_CREDENTIAL_TEXT_RE.pattern,),
            timeout_seconds=min(max(20, int(timeout_seconds)), 45),
        )
        if valid:
            _write_cookie_file(target_file, saved_cookies)
            logger.info(
                "Reusing saved Instagram browser session for %s from %s",
                normalized_validation_username or chrome_profile_name,
                session_paths.cookie_file_path,
            )
            print("\n*** Reusing saved Instagram browser session. ***\n")
            return saved_cookies
        logger.info(
            "Saved Instagram browser session invalid for %s (%s); falling back to interactive Chrome login",
            normalized_validation_username or chrome_profile_name,
            reason,
        )

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for interactive Chrome login") from exc

    chrome_profile_dir = _find_chrome_profile_dir(chrome_profile_name)
    deadline = time.monotonic() + max(60, int(timeout_seconds))

    mode_label = "headless" if headless else "headed"
    logger.info(
        "[instagram] opening %s Chrome (profile=%s) for login — you have %ds",
        mode_label,
        chrome_profile_name,
        timeout_seconds,
    )

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(chrome_profile_dir),
            channel="chrome",
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
            viewport={"width": 1280, "height": 1400},
        )
        try:
            page = context.new_page()
            page.goto(
                "https://www.instagram.com/",
                wait_until="domcontentloaded",
                timeout=30_000,
            )

            # Check if existing session is stale — if so, clear Instagram
            # cookies so we don't immediately exit thinking we're logged in.
            initial_cookies = _cookie_payload(context.cookies())
            if initial_cookies.get("sessionid"):
                # Validate the existing session by checking the page content
                page.wait_for_timeout(2_000)
                body_text = ""
                try:
                    body_text = page.locator("body").inner_text(timeout=3_000)
                except Exception:  # noqa: BLE001
                    pass
                current_url = str(page.url or "").lower()
                session_looks_valid = (
                    "login" not in current_url
                    and "accounts/login" not in current_url
                    and not INVALID_CREDENTIAL_TEXT_RE.search(body_text)
                    and not any(marker in current_url for marker in CHALLENGE_URL_MARKERS)
                )
                if not session_looks_valid:
                    logger.info("[instagram] existing session cookie is stale — clearing for fresh login")
                    # Delete Instagram cookies so we wait for a fresh login
                    context.clear_cookies()
                    page.goto(
                        INSTAGRAM_LOGIN_URL,
                        wait_until="domcontentloaded",
                        timeout=_remaining_timeout_ms(deadline, floor_ms=10_000),
                    )

            # Poll for sessionid cookie — user handles auth manually
            cookies: dict[str, str] = {}
            if not headless:
                print("\n*** Instagram is open — please log in manually if needed. ***")
                print("*** The browser will close automatically once a valid session is detected. ***\n")
            else:
                print("\n*** Checking Instagram session in headless Chrome... ***\n")
            while time.monotonic() < deadline:
                cookies = _cookie_payload(context.cookies())
                if cookies.get("sessionid"):
                    # Validate session by navigating to a profile
                    if validation_username:
                        normalized = str(validation_username).strip().lstrip("@")
                        try:
                            page.goto(
                                f"https://www.instagram.com/{normalized}/",
                                wait_until="domcontentloaded",
                                timeout=15_000,
                            )
                            page.wait_for_timeout(3_000)
                            post_nav_url = str(page.url or "").lower()
                            if "accounts/login" in post_nav_url:
                                logger.info("[instagram] session cookie invalid — redirected to login, waiting for fresh auth")
                                print("\n*** Session expired — please log in again. ***\n")
                                context.clear_cookies()
                                page.goto(
                                    INSTAGRAM_LOGIN_URL,
                                    wait_until="domcontentloaded",
                                    timeout=_remaining_timeout_ms(deadline, floor_ms=10_000),
                                )
                                page.wait_for_timeout(2_000)
                                continue
                            # Validate via a lightweight GraphQL probe
                            graphql_ok = _validate_session_via_graphql(page, normalized, deadline)
                            if not graphql_ok:
                                logger.info("[instagram] session cookies failed GraphQL validation — clearing for re-login")
                                print("\n*** Session cookies don't work for GraphQL API — please log in again. ***\n")
                                context.clear_cookies()
                                page.goto(
                                    INSTAGRAM_LOGIN_URL,
                                    wait_until="domcontentloaded",
                                    timeout=_remaining_timeout_ms(deadline, floor_ms=10_000),
                                )
                                page.wait_for_timeout(2_000)
                                continue
                            cookies = _cookie_payload(context.cookies())
                        except Exception:  # noqa: BLE001
                            pass
                    break
                page.wait_for_timeout(2_000)

            if not cookies.get("sessionid"):
                raise RuntimeError("Timed out waiting for Instagram login — no session cookie detected")

            _INSTAGRAM_BROWSER_SESSIONS.import_bootstrapped_session(
                session_account_id,
                context.storage_state(),
                fallback_account_id=session_account_id,
            )
            _write_cookie_file(target_file, cookies)
            logger.info("Interactive login succeeded — cookies written to %s", target_file)
            print(f"\nSession captured (sessionid={cookies['sessionid'][:8]}…)")
            print(f"Cookies written to {target_file}")
            return cookies
        except PlaywrightTimeoutError as exc:
            raise RuntimeError("Timed out during interactive Instagram login") from exc
        finally:
            context.close()


def _find_chrome_profile_dir(profile_name: str) -> Path:
    """Locate a Chrome profile directory by display name or email."""
    chrome_base = (
        Path.home() / "Library" / "Application Support" / "Google" / "Chrome"
    )
    for entry in chrome_base.iterdir():
        prefs_file = entry / "Preferences"
        if not prefs_file.is_file():
            continue
        try:
            prefs = json.loads(prefs_file.read_text())
            name = prefs.get("profile", {}).get("name", "")
            account_info = prefs.get("account_info", [])
            emails = [a.get("email", "") for a in account_info if isinstance(a, dict)]
            if (
                name.lower() == profile_name.lower()
                or profile_name.lower() in [e.lower() for e in emails]
            ):
                return entry
        except (json.JSONDecodeError, OSError):
            continue
    raise FileNotFoundError(
        f"Chrome profile '{profile_name}' not found in {chrome_base}. "
        "Check profile name in chrome://settings or pass the profile email."
    )
