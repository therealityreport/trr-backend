"""SocialBlade cookie loading, validation, and refresh helpers."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from trr_backend.socials.browser_cookie_refresh import (
    _body_text,
    cookie_payload,
    open_cookie_refresh_context,
    resolve_chrome_profile_selection,
    resolve_social_auth_chrome_profile,
    write_cookie_file,
)
from trr_backend.socials.cookie_sources import (
    _default_platform_cookie_file_path,
    _platform_cookie_file_candidates,
    _platform_cookie_refresh_target_path,
    _select_preferred_cookie_candidate,
)
from trr_backend.socials.socialblade.cookies import normalize_socialblade_cookies as normalize_socialblade_cookies
from trr_backend.socials.socialblade.parser import (
    SOCIALBLADE_STEALTH_INIT_SCRIPT,
    SOCIALBLADE_STEALTH_USER_AGENT,
)
from trr_backend.socials.socialblade.runtime_fetch import run_socialblade_scrapling_fetch

SOCIALBLADE_COOKIE_DOMAINS = (".socialblade.com", "socialblade.com")
SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY = ("cf_clearance",)
SOCIALBLADE_REQUIRED_COOKIE_NAMES_ALL = ("session",)
SOCIALBLADE_ACCESS_DENIED_PATTERNS = (
    r"Access denied",
    r"Error reference number:\s*1020",
    r"SOCIAL BLADE ACCESS DENIED",
)
_DEFAULT_SOCIALBLADE_VALIDATION_HANDLE = "bravotv"
_DEFAULT_SHARED_CHROME_CDP_URL = "http://127.0.0.1:9422"
_DEFAULT_VISIBLE_CHROME_CDP_URL = "http://127.0.0.1:9222"
_FALLBACK_SHARED_CHROME_CDP_URLS = (
    "http://127.0.0.1:9422",
    "http://127.0.0.1:9222",
)
_RETIRED_CODEX_PROFILE_PREFIX = "codex" + "-agent"
_LEGACY_MANAGED_CHROME_PROFILE_NAMES = frozenset(
    {_RETIRED_CODEX_PROFILE_PREFIX, f"{_RETIRED_CODEX_PROFILE_PREFIX}-devtools"}
)
_MANAGED_CHROME_PROFILE_ENV_KEYS = (
    "CODEX_CHROME_SEED_PROFILE_DIR",
    "CODEX_CHROME_PROFILE_DIR",
    "CHROME_AGENT_PROFILE_DIR",
    "SOCIALBLADE_CHROME_PROFILE_DIR",
)


class VisibleManagedChromeProfileError(RuntimeError):
    """Raised when local SocialBlade Chrome profile routing is unsafe."""


class SocialBladeValidationBlockedError(RuntimeError):
    """Raised when live cookie validation is blocked by Cloudflare (1020-class).

    The extracted cookies are structurally complete (``cf_clearance`` + ``session``
    are present); only the validation *egress* was denied. Callers that opt in via
    ``allow_blocked_validation`` can still persist/push these cookies, because the
    block is an IP/fingerprint problem rather than a bad-cookie problem. The offending
    cookies ride along on the exception so the caller need not re-extract them.
    """

    def __init__(self, cookies: dict[str, str], reason: str | None) -> None:
        self.cookies = dict(cookies or {})
        self.reason = reason
        super().__init__(f"SocialBlade cookie validation blocked by Cloudflare ({reason or 'unknown'})")


def _default_socialblade_cookie_file_path() -> Path:
    return _default_platform_cookie_file_path("socialblade")


def socialblade_cookie_file_path() -> Path:
    return _platform_cookie_refresh_target_path(
        _default_socialblade_cookie_file_path(),
        "SOCIALBLADE_COOKIES_FILE",
    )


def _normalize_socialblade_validation_handle(handle: str | None = None) -> str:
    handle = (
        str(handle or os.getenv("SOCIALBLADE_VALIDATION_HANDLE") or _DEFAULT_SOCIALBLADE_VALIDATION_HANDLE).strip()
        or _DEFAULT_SOCIALBLADE_VALIDATION_HANDLE
    )
    return handle.lstrip("@").lower()


def _socialblade_validation_url(handle: str | None = None) -> str:
    return f"https://socialblade.com/instagram/user/{_normalize_socialblade_validation_handle(handle)}"


def _validate_socialblade_cookie_health_via_visible_browser(handle: str) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    from trr_backend.socials.socialblade.parser import _extract_profile_stats_from_body_text

    cdp_url = _socialblade_visible_chrome_cdp_url()
    if not _chrome_cdp_endpoint_reachable(cdp_url):
        raise RuntimeError(
            "Visible shared Chrome session is not running on port 9222; "
            "start the manual browser session before retrying SocialBlade validation"
        )

    preflight_socialblade_chrome_profile(require_visible_managed=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(cdp_url)
        try:
            if not browser.contexts:
                raise RuntimeError("Visible shared Chrome session is not available for SocialBlade validation")
            page = browser.contexts[0].new_page()
            try:
                page.goto(_socialblade_validation_url(handle), wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_timeout(4_000)
                body_text = _body_text(page)
                if _body_text_matches_access_denied(body_text):
                    raise RuntimeError("SocialBlade blocked by Cloudflare (1020 access denied)")
                stats, _rankings, _labels = _extract_profile_stats_from_body_text(body_text, "instagram")
                return {
                    "username": handle,
                    "profile_stats": stats,
                }
            finally:
                page.close()
        finally:
            browser.close()


def _coerce_socialblade_cookie_map(raw_payload: Any) -> dict[str, str]:
    """Coerce SocialBlade cookies without dropping browser-signaling underscore cookies."""
    cookies: dict[str, str] = {}
    if isinstance(raw_payload, dict):
        nested = raw_payload.get("cookies")
        if isinstance(nested, list):
            return _coerce_socialblade_cookie_map(nested)

        name = raw_payload.get("name")
        value = raw_payload.get("value")
        if name is not None and value is not None:
            name_str = str(name).strip()
            value_str = str(value)
            return {name_str: value_str} if name_str and value_str else {}

        for key, value in raw_payload.items():
            if value is None:
                continue
            key_str = str(key).strip()
            if not key_str:
                continue
            if isinstance(value, (str, int, float, bool)):
                value_str = str(value)
                if value_str:
                    cookies[key_str] = value_str
        return cookies

    if isinstance(raw_payload, list):
        for item in raw_payload:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            value = str(item.get("value") or "")
            if name and value:
                cookies[name] = value
    return cookies


def _parse_socialblade_cookie_header(raw: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    value = str(raw or "").strip()
    if not value:
        return cookies
    if value.lower().startswith("cookie:"):
        value = value.split(":", 1)[1].strip()
    for token in value.split(";"):
        pair = token.strip()
        if not pair or "=" not in pair:
            continue
        key, val = pair.split("=", 1)
        key = key.strip()
        if key:
            cookies[key] = val.strip()
    return cookies


def load_socialblade_cookies_from_sources() -> dict[str, str]:
    candidates: list[dict[str, str]] = []
    raw_json = str(os.getenv("SOCIALBLADE_COOKIES_JSON") or "").strip()
    if raw_json:
        try:
            cookies = _coerce_socialblade_cookie_map(json.loads(raw_json))
        except json.JSONDecodeError:
            cookies = _parse_socialblade_cookie_header(raw_json)
        if cookies:
            candidates.append(cookies)

    for path in _platform_cookie_file_candidates(
        _default_socialblade_cookie_file_path(),
        "SOCIALBLADE_COOKIES_FILE",
    ):
        if not path.is_file():
            continue
        try:
            cookies = _coerce_socialblade_cookie_map(json.loads(path.read_text(encoding="utf-8")))
        except Exception:  # noqa: BLE001
            cookies = {}
        if cookies:
            candidates.append(cookies)

    return _select_preferred_cookie_candidate(
        candidates,
        required_cookie_names_any=SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY,
        required_cookie_names_all=SOCIALBLADE_REQUIRED_COOKIE_NAMES_ALL,
    )


def load_socialblade_cookies() -> dict[str, str]:
    return load_socialblade_cookies_from_sources()


def validate_socialblade_cookie_health(
    cookies: dict[str, str],
    *,
    validation_handle: str | None = None,
    allow_visible_browser_retry: bool = False,
) -> tuple[bool, str | None]:
    if not cookies:
        return False, "no_cookies_loaded"
    reason = _missing_required_socialblade_cookie_reason(cookies)
    if reason:
        return False, reason

    handle = _normalize_socialblade_validation_handle(validation_handle)
    try:
        payload = run_socialblade_scrapling_fetch(
            handle,
            cookies,
            platform="instagram",
        )
    except Exception as exc:  # noqa: BLE001
        if not allow_visible_browser_retry:
            return False, f"validation_scrape_failed:{str(exc) or type(exc).__name__}"
        try:
            payload = _validate_socialblade_cookie_health_via_visible_browser(handle)
        except Exception as retry_exc:  # noqa: BLE001
            return False, f"validation_scrape_failed:{str(retry_exc) or type(retry_exc).__name__}"

    username = str(payload.get("username") or "").strip().lstrip("@").lower() if isinstance(payload, dict) else ""
    if username and username != handle:
        return False, f"validation_username_mismatch:{username}"
    profile_stats = payload.get("profile_stats") if isinstance(payload, dict) else None
    if not isinstance(profile_stats, dict):
        return False, "validation_missing_profile_stats"
    followers = profile_stats.get("followers")
    try:
        if int(followers or 0) <= 0:
            return False, "validation_missing_followers"
    except (TypeError, ValueError):
        return False, "validation_missing_followers"
    return True, None


def _utc_iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")


def _cookie_names_from_payload(raw_payload: Any) -> set[str]:
    if isinstance(raw_payload, dict):
        return {str(name).strip() for name, value in raw_payload.items() if str(name).strip() and value}
    if isinstance(raw_payload, list):
        names: set[str] = set()
        for item in raw_payload:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            value = item.get("value")
            if name and value:
                names.add(name)
        return names
    if isinstance(raw_payload, str):
        return {
            part.split("=", 1)[0].strip()
            for part in raw_payload.split(";")
            if "=" in part and part.split("=", 1)[0].strip()
        }
    return set()


def _cookie_file_metadata(path: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "path": str(path),
        "exists": path.is_file(),
        "modifiedAt": None,
    }
    if path.is_file():
        metadata["modifiedAt"] = _utc_iso_from_timestamp(path.stat().st_mtime)
    return metadata


def socialblade_cookie_health_report(
    *,
    validate: bool = True,
    validation_handle: str | None = None,
    allow_visible_browser_retry: bool = False,
) -> dict[str, Any]:
    """Return redacted SocialBlade cookie health for admin/operator preflight."""
    cookie_file = socialblade_cookie_file_path()
    loaded_cookies: dict[str, str] = {}
    load_error: str | None = None
    cookie_names: set[str] = set()

    try:
        loaded_cookies = load_socialblade_cookies_from_sources()
        cookie_names = set(loaded_cookies)
    except Exception as exc:  # noqa: BLE001
        load_error = str(exc) or type(exc).__name__
        if cookie_file.is_file():
            try:
                cookie_names = _cookie_names_from_payload(json.loads(cookie_file.read_text(encoding="utf-8")))
            except Exception:  # noqa: BLE001
                cookie_names = set()

    missing_all = [name for name in SOCIALBLADE_REQUIRED_COOKIE_NAMES_ALL if name not in cookie_names]
    has_any = any(name in cookie_names for name in SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY)
    missing_any = (
        [] if has_any or not SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY else list(SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY)
    )
    schema_reason = None
    if load_error:
        schema_reason = load_error
    elif missing_all:
        schema_reason = f"missing_required_cookie:{','.join(missing_all)}"
    elif missing_any:
        schema_reason = f"missing_any_cookie:{','.join(missing_any)}"

    validation_checked = False
    validation_ok: bool | None = None
    validation_reason: str | None = None
    if validate and not schema_reason:
        validation_checked = True
        validation_ok, validation_reason = validate_socialblade_cookie_health(
            loaded_cookies,
            validation_handle=validation_handle,
            allow_visible_browser_retry=allow_visible_browser_retry,
        )

    healthy = schema_reason is None and (validation_ok is not False)
    reason = schema_reason or validation_reason
    return {
        "platform": "socialblade",
        "healthy": healthy,
        "status": "healthy" if healthy else "unhealthy",
        "reason": reason,
        "retryable": not healthy,
        "cookieNames": sorted(cookie_names),
        "requiredCookies": {
            "any": list(SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY),
            "all": list(SOCIALBLADE_REQUIRED_COOKIE_NAMES_ALL),
            "missingAny": missing_any,
            "missingAll": missing_all,
        },
        "cookieFile": _cookie_file_metadata(cookie_file),
        "validation": {
            "checked": validation_checked,
            "healthy": validation_ok,
            "reason": validation_reason,
            "handle": _normalize_socialblade_validation_handle(validation_handle) if validation_checked else None,
            "url": _socialblade_validation_url(validation_handle) if validation_checked else None,
        },
        "checkedAt": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
    }


def require_socialblade_cookie_health(
    *,
    source: str = "SocialBlade",
    validation_handle: str | None = None,
    allow_visible_browser_retry: bool = False,
) -> dict[str, Any]:
    health = socialblade_cookie_health_report(
        validate=True,
        validation_handle=validation_handle,
        allow_visible_browser_retry=allow_visible_browser_retry,
    )
    if not bool(health.get("healthy")):
        reason = str(health.get("reason") or "unknown").strip()
        raise RuntimeError(f"{source} session preflight failed ({reason})")
    return health


def _missing_required_socialblade_cookie_reason(cookies: dict[str, str]) -> str | None:
    missing_all = [name for name in SOCIALBLADE_REQUIRED_COOKIE_NAMES_ALL if not str(cookies.get(name) or "").strip()]
    if missing_all:
        return f"missing_required_cookie:{','.join(missing_all)}"

    has_any_required_cookie = any(
        str(cookies.get(name) or "").strip() for name in SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY
    )
    if SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY and not has_any_required_cookie:
        return f"missing_any_cookie:{','.join(SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY)}"
    return None


def require_socialblade_authenticated_cookies(cookies: dict[str, str], *, source: str) -> None:
    reason = _missing_required_socialblade_cookie_reason(cookies)
    if reason:
        raise RuntimeError(f"{source} did not capture required SocialBlade authenticated cookies ({reason})")


def require_socialblade_usable_cookies(
    cookies: dict[str, str],
    *,
    source: str,
    validation_handle: str | None = None,
    allow_visible_browser_retry: bool = False,
) -> None:
    require_socialblade_authenticated_cookies(cookies, source=source)
    healthy, reason = validate_socialblade_cookie_health(
        cookies,
        validation_handle=validation_handle,
        allow_visible_browser_retry=allow_visible_browser_retry,
    )
    if not healthy:
        raise RuntimeError(f"{source} captured SocialBlade cookies failed validation ({reason or 'unknown'})")


def extract_socialblade_cookies_from_chrome_profile(
    *,
    chrome_profile: str | None = None,
    validation_handle: str | None = None,
    allow_visible_browser_retry: bool = False,
) -> dict[str, str]:
    """Extract and validate SocialBlade cookies from the real Chrome auth profile.

    Set ``allow_visible_browser_retry`` to let validation escalate to the visible
    shared Chrome session (the same real profile the cookies came from) when the
    headless fetch is challenged. When validation fails specifically with a
    Cloudflare 1020-class block but the required cookies are present, a
    ``SocialBladeValidationBlockedError`` (carrying the extracted cookies) is raised
    so opt-in callers can still persist/push a structurally-valid cookie set.
    """
    try:
        from pycookiecheat import chrome_cookies
    except ImportError as exc:  # pragma: no cover - dependency failure is environment-specific
        raise RuntimeError("SocialBlade Chrome cookie extraction requires pycookiecheat") from exc

    resolved_profile = resolve_social_auth_chrome_profile("socialblade", chrome_profile)
    selection = resolve_chrome_profile_selection(resolved_profile)
    cookie_file = selection.profile_path / "Cookies"
    if not cookie_file.is_file():
        raise FileNotFoundError(f"Cookie database not found at {cookie_file}")

    validation_url = _socialblade_validation_url(validation_handle)
    extracted = chrome_cookies(validation_url, cookie_file=str(cookie_file))
    cookies = {
        str(name): str(value) for name, value in dict(cast("dict[str, Any]", extracted or {})).items() if name and value
    }
    require_socialblade_authenticated_cookies(cookies, source=f"Chrome profile {selection.display_name}")
    healthy, reason = validate_socialblade_cookie_health(
        cookies,
        validation_handle=validation_handle,
        allow_visible_browser_retry=allow_visible_browser_retry,
    )
    if not healthy:
        if is_socialblade_cloudflare_block_reason(reason):
            # Structural cookies are present; only the validation egress was
            # Cloudflare-blocked (1020). Surface a typed error carrying the cookies so
            # opt-in callers can persist/push them instead of discarding a good set.
            raise SocialBladeValidationBlockedError(cookies, reason)
        raise RuntimeError(
            f"Chrome profile {selection.display_name} SocialBlade cookies failed validation ({reason or 'unknown'})"
        )
    write_cookie_file(socialblade_cookie_file_path(), cookies)
    return cookies


def _body_text_matches_access_denied(body_text: str) -> bool:
    normalized = body_text.lower().replace(" ", "")
    return any(pattern.lower().replace("\\s*", "") in normalized for pattern in SOCIALBLADE_ACCESS_DENIED_PATTERNS)


def is_socialblade_cloudflare_block_reason(reason: str | None) -> bool:
    """True when a validation failure reason is a Cloudflare 1020-class block.

    Distinguishes an *egress* denial (our IP/fingerprint was blocked, cookies are
    likely fine) from a genuine auth/structure failure (bad or incomplete cookies).
    The repair flow uses this to decide whether a freshly-extracted cookie set may
    still be persisted/pushed despite a blocked live validation. Matches both the
    ``validation_scrape_failed:...`` reason wrapper and the raw scraper/auth error
    messages (``... blocked by Cloudflare (1020 access denied)``).
    """
    text = str(reason or "").strip().lower()
    if not text:
        return False
    if "1020" in text or "blocked by cloudflare" in text:
        return True
    return _body_text_matches_access_denied(text)


def _chrome_cdp_endpoint_reachable(cdp_url: str, *, timeout_seconds: float = 1.0) -> bool:
    parsed = urlparse(cdp_url)
    hostname = parsed.hostname
    port = parsed.port
    if not hostname or not port:
        return False
    try:
        with socket.create_connection((hostname, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _socialblade_shared_chrome_cdp_url() -> str:
    explicit_url = str(os.getenv("SOCIALBLADE_SHARED_CHROME_CDP_URL") or "").strip()
    if explicit_url:
        return explicit_url

    for candidate in _FALLBACK_SHARED_CHROME_CDP_URLS:
        if _chrome_cdp_endpoint_reachable(candidate):
            return candidate
    return _DEFAULT_SHARED_CHROME_CDP_URL


def _socialblade_visible_chrome_cdp_url() -> str:
    explicit_url = str(os.getenv("SOCIALBLADE_VISIBLE_CHROME_CDP_URL") or "").strip()
    if explicit_url:
        return explicit_url
    return _DEFAULT_VISIBLE_CHROME_CDP_URL


def _visible_managed_chrome_workspace_script() -> Path:
    return Path(__file__).resolve().parents[4] / "scripts" / "ensure-managed-chrome.sh"


def _is_local_visible_managed_chrome_url(cdp_url: str) -> bool:
    parsed = urlparse(cdp_url)
    return parsed.hostname in {"127.0.0.1", "localhost"} and parsed.port == 9222


def _run_visible_managed_chrome_guard(cdp_url: str) -> bool:
    if not _is_local_visible_managed_chrome_url(cdp_url):
        return False

    launcher = _visible_managed_chrome_workspace_script()
    if not launcher.is_file():
        return False

    env = os.environ.copy()
    env["CODEX_CHROME_MODE"] = "shared"
    env["CODEX_CHROME_SHARED_PORT"] = "9222"
    env["CODEX_CHROME_PORT"] = "9222"
    env["CHROME_AGENT_DEBUG_PORT"] = "9222"
    env["CHROME_AGENT_HEADLESS"] = "0"

    try:
        subprocess.run(
            ["bash", str(launcher)],
            check=True,
            cwd=str(launcher.parent.parent),
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or str(exc)).strip()
        raise VisibleManagedChromeProfileError(
            "Visible shared Chrome is not using the expected openai-agent managed clone. "
            "If the user asks for the Codex profile, use the real codex@thereality.report Chrome profile, "
            f"not the managed clone. {detail}"
        ) from exc
    return True


def _legacy_managed_chrome_profile_name(raw_value: str) -> str | None:
    rendered = str(raw_value or "").strip()
    if not rendered:
        return None
    name = Path(rendered).expanduser().name
    if name in _LEGACY_MANAGED_CHROME_PROFILE_NAMES or name.startswith(f"{_RETIRED_CODEX_PROFILE_PREFIX}-"):
        return name
    return None


def preflight_socialblade_chrome_profile(*, require_visible_managed: bool = False) -> None:
    """Fail before a SocialBlade run can use the retired managed profile."""
    violations: list[str] = []
    for env_key in _MANAGED_CHROME_PROFILE_ENV_KEYS:
        env_value = str(os.getenv(env_key) or "").strip()
        legacy_name = _legacy_managed_chrome_profile_name(env_value)
        if legacy_name:
            violations.append(f"{env_key}={env_value!r} uses retired profile {legacy_name!r}")

    if violations:
        raise VisibleManagedChromeProfileError(
            "SocialBlade Chrome profile preflight failed: "
            + "; ".join(violations)
            + ". Use the openai-agent managed clone for automation. "
            "When the user says Codex profile, use the real codex@thereality.report Chrome profile."
        )

    if require_visible_managed:
        _ensure_visible_managed_chrome_available(_socialblade_visible_chrome_cdp_url())


def _ensure_visible_managed_chrome_available(cdp_url: str) -> bool:
    if _chrome_cdp_endpoint_reachable(cdp_url):
        _run_visible_managed_chrome_guard(cdp_url)
        return False
    if not _is_local_visible_managed_chrome_url(cdp_url):
        return False

    if not _run_visible_managed_chrome_guard(cdp_url):
        return False
    if not _chrome_cdp_endpoint_reachable(cdp_url, timeout_seconds=2.0):
        raise RuntimeError(
            "Visible shared Chrome was launched but the debugging endpoint on port 9222 is still unavailable"
        )
    return True


def _open_socialblade_repair_tab(cdp_url: str) -> bool:
    parsed = urlparse(cdp_url)
    if not parsed.scheme or not parsed.netloc:
        return False

    request = Request(
        f"{parsed.scheme}://{parsed.netloc}/json/new?{_socialblade_validation_url()}",
        method="PUT",
    )
    try:
        with urlopen(request, timeout=5):
            return True
    except Exception:
        return False


def _cdp_http_json(cdp_url: str, path: str, *, method: str = "GET") -> dict[str, Any]:
    parsed = urlparse(cdp_url)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError("Invalid Chrome CDP URL")

    request = Request(
        f"{parsed.scheme}://{parsed.netloc}{path}",
        method=method,
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


async def _cdp_send_command(websocket: Any, command_id: int, method: str, params: dict[str, Any] | None = None) -> Any:
    await websocket.send(json.dumps({"id": command_id, "method": method, "params": params or {}}))
    while True:
        message = json.loads(await websocket.recv())
        if message.get("id") != command_id:
            continue
        if message.get("error"):
            raise RuntimeError(str(message["error"].get("message") or message["error"]))
        return message.get("result")


async def _export_socialblade_cookies_via_cdp_protocol_async(cdp_url: str) -> dict[str, str]:
    import websockets

    validation_url = _socialblade_validation_url()
    target = _cdp_http_json(cdp_url, f"/json/new?{quote(validation_url, safe=':/?&=%')}", method="PUT")
    target_id = str(target.get("id") or "").strip()
    websocket_url = str(target.get("webSocketDebuggerUrl") or "").strip()
    if not websocket_url:
        raise RuntimeError("Managed Chrome did not expose a page websocket for SocialBlade cookie export")

    try:
        async with websockets.connect(websocket_url, open_timeout=10, close_timeout=2) as websocket:
            command_id = 1
            await _cdp_send_command(websocket, command_id, "Page.enable")
            command_id += 1
            await _cdp_send_command(websocket, command_id, "Network.enable")
            command_id += 1
            await asyncio.sleep(3)
            body_result = await _cdp_send_command(
                websocket,
                command_id,
                "Runtime.evaluate",
                {
                    "expression": "document.body ? document.body.innerText : ''",
                    "returnByValue": True,
                },
            )
            command_id += 1
            body_text = str(((body_result or {}).get("result") or {}).get("value") or "")
            if _body_text_matches_access_denied(body_text):
                raise RuntimeError("Managed Chrome SocialBlade session is blocked by Cloudflare")

            cookie_result = await _cdp_send_command(
                websocket,
                command_id,
                "Network.getCookies",
                {"urls": [validation_url]},
            )
            cookies = cookie_payload(cookie_result.get("cookies") or [], domains=SOCIALBLADE_COOKIE_DOMAINS)
            require_socialblade_authenticated_cookies(cookies, source="Managed Chrome")
            healthy, reason = await asyncio.to_thread(validate_socialblade_cookie_health, cookies)
            if not healthy:
                raise RuntimeError(
                    f"Managed Chrome captured SocialBlade cookies failed validation ({reason or 'unknown'})"
                )
            write_cookie_file(socialblade_cookie_file_path(), cookies)
            return cookies
    finally:
        if target_id:
            try:
                _cdp_http_json(cdp_url, f"/json/close/{target_id}")
            except Exception:
                pass


def _export_socialblade_cookies_via_cdp_protocol(cdp_url: str) -> dict[str, str]:
    return asyncio.run(_export_socialblade_cookies_via_cdp_protocol_async(cdp_url))


def _render_visible_cookie_refresh_error(exc: Exception, *, cdp_url: str, auto_launched: bool) -> str:
    detail = str(exc).strip() or type(exc).__name__
    if "usable SocialBlade Cloudflare clearance cookie" in detail or "blocked by Cloudflare" in detail:
        opened_repair_tab = _open_socialblade_repair_tab(cdp_url)
        if opened_repair_tab:
            return (
                "Opened SocialBlade in the visible shared Chrome window, but Cloudflare clearance is still missing. "
                "Complete the challenge there and retry."
            )
        return (
            "Visible shared Chrome reached SocialBlade, but Cloudflare clearance is still missing. "
            "Complete the challenge in the shared browser on port 9222 and retry."
        )
    if "did not expose any browser contexts" in detail:
        return "Visible shared Chrome started, but no browser context was available yet. Wait a moment and retry."
    if auto_launched:
        return f"Auto-launched visible shared Chrome, but SocialBlade cookie refresh still failed: {detail}"
    return f"Visible shared Chrome cookie refresh failed: {detail}"


def export_socialblade_cookies_from_shared_chrome(*, cdp_url: str | None = None) -> dict[str, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for SocialBlade cookie export") from exc

    resolved_cdp_url = cdp_url or _socialblade_shared_chrome_cdp_url()
    validation_url = _socialblade_validation_url()
    use_cdp_protocol_fallback = False

    with sync_playwright() as playwright:
        try:
            browser = playwright.chromium.connect_over_cdp(resolved_cdp_url)
        except Exception as exc:  # noqa: BLE001
            detail = str(exc)
            if "Browser.setDownloadBehavior" in detail or "context management is not supported" in detail:
                use_cdp_protocol_fallback = True
                browser = None
            else:
                raise
        if browser is not None:
            try:
                if not browser.contexts:
                    raise RuntimeError("Managed Chrome did not expose any browser contexts")
                context = browser.contexts[0]
                page = context.new_page()
                try:
                    page.goto(validation_url, wait_until="domcontentloaded", timeout=45_000)
                    page.wait_for_timeout(3_000)
                    body_text = _body_text(page)
                    if _body_text_matches_access_denied(body_text):
                        raise RuntimeError("Managed Chrome SocialBlade session is blocked by Cloudflare")
                    cookies = cookie_payload(context.cookies(), domains=SOCIALBLADE_COOKIE_DOMAINS)
                    require_socialblade_usable_cookies(cookies, source="Managed Chrome")
                    write_cookie_file(socialblade_cookie_file_path(), cookies)
                    return cookies
                finally:
                    page.close()
            finally:
                browser.close()

    if use_cdp_protocol_fallback:
        return _export_socialblade_cookies_via_cdp_protocol(resolved_cdp_url)

    raise RuntimeError("Managed Chrome cookie export failed")


def refresh_socialblade_cookies(
    reason: str | None = None,
    *,
    allow_headless_fallback: bool = True,
) -> dict[str, str]:
    del reason
    preflight_socialblade_chrome_profile()
    visible_cdp_url = _socialblade_visible_chrome_cdp_url()
    auto_launched_visible_chrome = False
    try:
        auto_launched_visible_chrome = _ensure_visible_managed_chrome_available(visible_cdp_url)
        return export_socialblade_cookies_from_shared_chrome(cdp_url=visible_cdp_url)
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, VisibleManagedChromeProfileError):
            raise
        if not allow_headless_fallback:
            raise RuntimeError(
                _render_visible_cookie_refresh_error(
                    exc,
                    cdp_url=visible_cdp_url,
                    auto_launched=auto_launched_visible_chrome,
                )
            ) from exc

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Playwright is required for SocialBlade cookie refresh") from exc

    target_url = _socialblade_validation_url()
    headless_raw = str(os.getenv("SOCIALBLADE_COOKIE_REFRESH_HEADLESS") or "true").strip().lower()
    headless = headless_raw not in {"0", "false", "off", "no"}

    with sync_playwright() as playwright:
        session = open_cookie_refresh_context(
            playwright,
            platform="socialblade",
            headless=headless,
            viewport={"width": 1440, "height": 1600},
            user_agent=SOCIALBLADE_STEALTH_USER_AGENT,
            locale="en-US",
            timezone_id="America/New_York",
        )
        try:
            context = session.context
            context.add_init_script(SOCIALBLADE_STEALTH_INIT_SCRIPT)
            page = context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(4_000)
            body_text = _body_text(page)
            if _body_text_matches_access_denied(body_text):
                raise RuntimeError("SocialBlade cookie refresh was blocked by Cloudflare")
            cookies = cookie_payload(context.cookies(), domains=SOCIALBLADE_COOKIE_DOMAINS)
            require_socialblade_authenticated_cookies(cookies, source="SocialBlade cookie refresh")
            write_cookie_file(socialblade_cookie_file_path(), cookies)
            return cookies
        finally:
            session.close()
