"""Shared browser-backed cookie validation and refresh helpers."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_SOCIAL_AUTH_CHROME_PROFILE = "codex@thereality.report"
DEFAULT_SOCIAL_AUTH_REFRESH_MIN_INTERVAL_SECONDS = 3_600
_PROFILE_FALSE_VALUES = {"0", "false", "off", "no"}


@dataclass(frozen=True)
class SimpleLoginSpec:
    platform: str
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


@dataclass
class CookieRefreshBrowserContext:
    context: Any
    browser: Any | None = None
    profile_path: Path | None = None
    user_data_dir: Path | None = None
    profile_directory: str | None = None
    preferences_path: Path | None = None

    def close(self) -> None:
        if self.browser is not None:
            self.browser.close()
            return
        self.context.close()


@dataclass(frozen=True)
class ChromeProfileSelection:
    user_data_dir: Path
    profile_directory: str
    preferences_path: Path
    display_name: str
    matched_profile: str
    matched_email: str | None = None

    @property
    def profile_path(self) -> Path:
        return self.user_data_dir / self.profile_directory


class ChromeProfileNotAvailableError(RuntimeError):
    """Raised when an auth refresh would otherwise launch a profile-less browser."""


class ChromeProfileLockedError(ChromeProfileNotAvailableError):
    """Raised when Chrome's profile lock would make auth refresh non-deterministic."""


class SocialAuthRefreshRateLimitError(RuntimeError):
    """Raised when a platform auth refresh attempt is blocked by the hard cooldown."""


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


def _chrome_profile_base_dir() -> Path:
    return Path.home() / "Library" / "Application Support" / "Google" / "Chrome"


def _backend_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_rate_limit_platform(platform: str) -> str:
    normalized = re.sub(r"[^a-z0-9_-]+", "-", str(platform or "").strip().lower()).strip("-")
    return normalized or "unknown"


def _social_auth_refresh_rate_limit_dir() -> Path:
    override = str(os.getenv("SOCIAL_AUTH_REFRESH_RATE_LIMIT_DIR") or "").strip()
    if override:
        return Path(override).expanduser()
    return _backend_root() / ".locks" / "social-auth-refresh"


def _social_auth_refresh_min_interval_seconds(platform: str) -> int:
    normalized_env_platform = _normalize_rate_limit_platform(platform).replace("-", "_").upper()
    candidates = [
        f"SOCIAL_{normalized_env_platform}_AUTH_REFRESH_MIN_INTERVAL_SECONDS",
        "SOCIAL_AUTH_REFRESH_MIN_INTERVAL_SECONDS",
    ]
    for env_key in candidates:
        raw = str(os.getenv(env_key) or "").strip()
        if not raw:
            continue
        try:
            return max(0, int(float(raw)))
        except ValueError:
            logger.warning("Ignoring invalid %s=%r", env_key, raw)
    return DEFAULT_SOCIAL_AUTH_REFRESH_MIN_INTERVAL_SECONDS


def _read_rate_limit_timestamp(lock_file: Path) -> float | None:
    try:
        payload = json.loads(lock_file.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:  # noqa: BLE001
        logger.warning("Ignoring unreadable social auth refresh rate-limit file %s", lock_file, exc_info=True)
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return float(payload.get("last_attempt_monotonic"))
    except (TypeError, ValueError):
        return None


def reserve_social_auth_refresh_attempt(platform: str) -> dict[str, object]:
    """Atomically reserve a social auth refresh attempt before opening Chrome."""

    normalized = _normalize_rate_limit_platform(platform)
    interval_seconds = _social_auth_refresh_min_interval_seconds(normalized)
    if interval_seconds <= 0 or _env_truthy(os.getenv("SOCIAL_AUTH_REFRESH_RATE_LIMIT_DISABLED")):
        return {"reserved": False, "platform": normalized, "rate_limit_disabled": True}

    rate_limit_dir = _social_auth_refresh_rate_limit_dir()
    rate_limit_dir.mkdir(parents=True, exist_ok=True)
    gate_dir = rate_limit_dir / f"{normalized}.lockdir"
    deadline = time.monotonic() + 5.0
    while True:
        try:
            gate_dir.mkdir(mode=0o700)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise SocialAuthRefreshRateLimitError(
                    f"{normalized} auth refresh is already reserved by another process"
                ) from None
            time.sleep(0.1)

    lock_file = rate_limit_dir / f"{normalized}.json"
    now = time.monotonic()
    try:
        last_attempt = _read_rate_limit_timestamp(lock_file)
        if last_attempt is not None:
            elapsed = max(0.0, now - last_attempt)
            remaining = interval_seconds - elapsed
            if remaining > 0:
                raise SocialAuthRefreshRateLimitError(
                    f"{normalized} auth refresh rate-limited; retry after {int(remaining) + 1}s"
                )
        payload = {
            "platform": normalized,
            "last_attempt_monotonic": now,
            "reserved_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            "min_interval_seconds": interval_seconds,
        }
        write_private_json_file(lock_file, payload)
        return {
            "reserved": True,
            "platform": normalized,
            "lock_file": str(lock_file),
            "min_interval_seconds": interval_seconds,
        }
    finally:
        shutil.rmtree(gate_dir, ignore_errors=True)


def resolve_social_auth_chrome_profile(platform: str | None = None, explicit_profile: str | None = None) -> str:
    if explicit_profile and str(explicit_profile).strip():
        return str(explicit_profile).strip()
    normalized_platform = str(platform or "").strip().upper()
    candidates = []
    if normalized_platform:
        candidates.append(f"SOCIAL_{normalized_platform}_CHROME_PROFILE")
    candidates.extend(
        [
            "SOCIAL_AUTH_CHROME_PROFILE",
            "SOCIAL_COOKIE_REFRESH_CHROME_PROFILE",
            "CODEX_SOCIAL_AUTH_CHROME_PROFILE",
        ]
    )
    for env_key in candidates:
        value = str(os.getenv(env_key) or "").strip()
        if value:
            return value
    return DEFAULT_SOCIAL_AUTH_CHROME_PROFILE


def social_auth_requires_chrome_profile(platform: str | None = None) -> bool:
    normalized_platform = str(platform or "").strip().upper()
    candidates = []
    if normalized_platform:
        candidates.append(f"SOCIAL_{normalized_platform}_REQUIRE_CHROME_PROFILE")
    candidates.append("SOCIAL_COOKIE_REFRESH_REQUIRE_CHROME_PROFILE")
    for env_key in candidates:
        value = str(os.getenv(env_key) or "").strip().lower()
        if value:
            return value not in _PROFILE_FALSE_VALUES
    return True


def find_chrome_profile_dir(profile_name: str) -> Path:
    return resolve_chrome_profile_selection(profile_name).profile_path


def _looks_like_inner_chrome_profile_dir(path: Path) -> bool:
    return (path / "Preferences").is_file()


def _chrome_lock_paths(selection: ChromeProfileSelection) -> tuple[Path, ...]:
    return (
        selection.user_data_dir / "SingletonLock",
        selection.user_data_dir / "SingletonCookie",
        selection.profile_path / "SingletonLock",
        selection.profile_path / "SingletonCookie",
    )


def _raise_if_chrome_profile_locked(selection: ChromeProfileSelection) -> None:
    existing_locks = [path for path in _chrome_lock_paths(selection) if path.exists() or path.is_symlink()]
    if not existing_locks:
        return
    lock_list = ", ".join(str(path) for path in existing_locks)
    raise ChromeProfileLockedError(
        "Chrome auth profile is locked by another Chrome process. "
        f"profile={selection.matched_profile!r} user_data_dir={selection.user_data_dir} "
        f"profile_directory={selection.profile_directory} locks={lock_list}. "
        "Close Chrome for that profile or use SOCIAL_AUTH_CHROME_PROFILE to choose an unlocked test profile."
    )


def resolve_chrome_profile_selection(profile_name: str) -> ChromeProfileSelection:
    chrome_base = _chrome_profile_base_dir()
    if not chrome_base.is_dir():
        raise ChromeProfileNotAvailableError(f"Chrome profile directory not found: {chrome_base}")
    if _looks_like_inner_chrome_profile_dir(chrome_base):
        raise ChromeProfileNotAvailableError(
            f"Chrome user-data root appears to be an inner profile directory: {chrome_base}. "
            "Use the Chrome root and --profile-directory instead."
        )

    normalized_profile = str(profile_name or "").strip().lower()
    if not normalized_profile:
        raise ChromeProfileNotAvailableError("Chrome profile name is empty")

    for entry in chrome_base.iterdir():
        prefs_file = entry / "Preferences"
        if not prefs_file.is_file():
            continue
        try:
            prefs = json.loads(prefs_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        raw_profile_display_name = str(prefs.get("profile", {}).get("name") or "").strip()
        profile_display_name = raw_profile_display_name.lower()
        account_info = prefs.get("account_info", [])
        emails = [
            str(account.get("email") or "").strip().lower() for account in account_info if isinstance(account, dict)
        ]
        if normalized_profile in {profile_display_name, *emails}:
            matched_email = normalized_profile if normalized_profile in emails else None
            return ChromeProfileSelection(
                user_data_dir=chrome_base,
                profile_directory=entry.name,
                preferences_path=prefs_file,
                display_name=raw_profile_display_name,
                matched_profile=profile_name,
                matched_email=matched_email,
            )

    raise ChromeProfileNotAvailableError(
        f"Chrome profile '{profile_name}' not found in {chrome_base}. "
        "Use SOCIAL_AUTH_CHROME_PROFILE or a platform-specific SOCIAL_<PLATFORM>_CHROME_PROFILE override."
    )


def _profile_directory_arg(profile_directory: str) -> str:
    normalized = str(profile_directory or "").strip()
    if not normalized:
        raise ChromeProfileNotAvailableError("Chrome profile directory is empty")
    return f"--profile-directory={normalized}"


def _with_profile_directory_arg(args: list[str], profile_directory: str) -> list[str]:
    without_existing = [arg for arg in args if not str(arg).startswith("--profile-directory")]
    return [*without_existing, _profile_directory_arg(profile_directory)]


def open_cookie_refresh_context(
    playwright: Any,
    *,
    platform: str,
    headless: bool,
    viewport: dict[str, int],
    profile_name: str | None = None,
    user_agent: str | None = None,
    locale: str | None = None,
    timezone_id: str | None = None,
    extra_args: list[str] | None = None,
    enforce_rate_limit: bool = True,
    require_profile: bool | None = None,
) -> CookieRefreshBrowserContext:
    if enforce_rate_limit:
        reserve_social_auth_refresh_attempt(platform)

    resolved_profile = resolve_social_auth_chrome_profile(platform, profile_name)
    effective_require_profile = (
        social_auth_requires_chrome_profile(platform) if require_profile is None else require_profile
    )
    profile_selection: ChromeProfileSelection | None = None
    if resolved_profile:
        try:
            profile_selection = resolve_chrome_profile_selection(resolved_profile)
        except ChromeProfileNotAvailableError:
            if effective_require_profile:
                raise
            logger.warning(
                "[%s] Chrome profile %r unavailable; falling back to profile-less cookie refresh because "
                "Chrome profile requirement is disabled",
                platform,
                resolved_profile,
            )

    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
        *(extra_args or []),
    ]
    context_kwargs: dict[str, Any] = {
        "viewport": viewport,
    }
    if user_agent:
        context_kwargs["user_agent"] = user_agent
    if locale:
        context_kwargs["locale"] = locale
    if timezone_id:
        context_kwargs["timezone_id"] = timezone_id

    if profile_selection is not None:
        _raise_if_chrome_profile_locked(profile_selection)
        launch_args = _with_profile_directory_arg(launch_args, profile_selection.profile_directory)
        logger.info(
            "[%s] launching Chrome auth context with profile=%s user_data_dir=%s profile_directory=%s",
            platform,
            resolved_profile,
            profile_selection.user_data_dir,
            profile_selection.profile_directory,
        )
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_selection.user_data_dir),
            channel="chrome",
            headless=bool(headless),
            args=launch_args,
            **context_kwargs,
        )
        return CookieRefreshBrowserContext(
            context=context,
            profile_path=profile_selection.profile_path,
            user_data_dir=profile_selection.user_data_dir,
            profile_directory=profile_selection.profile_directory,
            preferences_path=profile_selection.preferences_path,
        )

    if effective_require_profile:
        raise ChromeProfileNotAvailableError(
            f"{platform} cookie refresh requires Chrome profile {resolved_profile!r}; refusing profile-less launch"
        )

    browser = launch_browser(playwright, headless=headless)
    try:
        context = browser.new_context(**context_kwargs)
    except Exception:
        browser.close()
        raise
    return CookieRefreshBrowserContext(context=context, browser=browser)


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


def ensure_private_file_mode(path: str | Path) -> None:
    target = Path(path).expanduser()
    if not target.exists():
        return
    try:
        target.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod private cookie/session file %s", target, exc_info=True)


def reconcile_private_paths(*roots: str | Path, dir_mode: int = 0o700, file_mode: int = 0o600) -> int:
    """Best-effort chmod for private credential/session roots."""
    hardened = 0
    for root in roots:
        base = Path(root).expanduser()
        if not base.exists():
            continue
        try:
            for path in base.rglob("*"):
                try:
                    if path.is_dir():
                        path.chmod(dir_mode)
                    elif path.is_file():
                        path.chmod(file_mode)
                        hardened += 1
                except OSError:
                    logger.debug("Failed to chmod %s during private-path reconcile", path, exc_info=True)
            try:
                base.chmod(dir_mode)
            except OSError:
                logger.debug("Failed to chmod root %s during private-path reconcile", base, exc_info=True)
        except OSError:
            logger.debug("Failed to walk %s during private-path reconcile", base, exc_info=True)
    return hardened


def write_private_json_file(path: str | Path, payload: Any) -> None:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(target.parent)) as handle:
            temp_path = Path(handle.name)
            temp_path.chmod(0o600)
            handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        os.replace(temp_path, target)
        target.chmod(0o600)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except OSError:
                pass
        raise


def write_cookie_file(cookie_file: str | Path, cookies: dict[str, str]) -> None:
    write_private_json_file(cookie_file, cookies)


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
    validator: Callable[[dict[str, str]], tuple[bool, str | None]] | None = None,
) -> dict[str, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for cookie refresh") from exc

    deadline = time.monotonic() + max(30, int(timeout_seconds))
    refreshed_cookies: dict[str, str] = {}
    with sync_playwright() as playwright:
        session = open_cookie_refresh_context(
            playwright,
            platform=spec.platform,
            headless=headless,
            viewport={"width": 1_440, "height": 1_600},
        )
        try:
            context = session.context
            page = context.new_page()
            page.goto(
                spec.validation_url,
                wait_until="domcontentloaded",
                timeout=_remaining_timeout_ms(deadline, floor_ms=10_000),
            )
            page.wait_for_timeout(2_000)
            existing_cookies = cookie_payload(context.cookies(), domains=spec.cookie_domains)
            if _has_required_authenticated_cookies(existing_cookies, spec=spec) and not _detect_invalid_login_state(
                page,
                spec=spec,
            ):
                reuse_valid = True
                if validator is not None:
                    reuse_valid, reuse_reason = validator(existing_cookies)
                    if not reuse_valid:
                        logger.info(
                            "%s Chrome-profile cookies failed in-protocol validation (%s); "
                            "falling through to scripted login",
                            spec.platform,
                            reuse_reason or "unknown",
                        )
                if reuse_valid:
                    write_cookie_file(cookie_file, existing_cookies)
                    return existing_cookies

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
            session.close()

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
    if validator is not None:
        protocol_valid, protocol_reason = validator(refreshed_cookies)
        if not protocol_valid:
            raise RuntimeError(
                f"Cookie refresh produced cookies that failed in-protocol validation ({protocol_reason or 'unknown'})"
            )
    write_cookie_file(cookie_file, refreshed_cookies)
    return refreshed_cookies
