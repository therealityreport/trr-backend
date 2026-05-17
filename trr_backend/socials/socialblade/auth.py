"""SocialBlade cookie loading, validation, and refresh helpers."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

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
_DEFAULT_SOCIALBLADE_VALIDATION_HANDLE = "socialblade"
_DEFAULT_SHARED_CHROME_CDP_URL = "http://127.0.0.1:9422"
_DEFAULT_VISIBLE_CHROME_CDP_URL = "http://127.0.0.1:9222"
_FALLBACK_SHARED_CHROME_CDP_URLS = (
    "http://127.0.0.1:9422",
    "http://127.0.0.1:9222",
)


class VisibleManagedChromeProfileError(RuntimeError):
    """Raised when the local visible managed Chrome is not the codex profile."""


def _default_socialblade_cookie_file_path() -> Path:
    return social_repo._default_platform_cookie_file_path("socialblade")  # noqa: SLF001


def socialblade_cookie_file_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(  # noqa: SLF001
        _default_socialblade_cookie_file_path(),
        "SOCIALBLADE_COOKIES_FILE",
    )


def _socialblade_validation_url() -> str:
    handle = (
        str(os.getenv("SOCIALBLADE_VALIDATION_HANDLE") or _DEFAULT_SOCIALBLADE_VALIDATION_HANDLE).strip()
        or _DEFAULT_SOCIALBLADE_VALIDATION_HANDLE
    )
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
            "Visible shared Chrome is not using the expected codex@thereality.report profile. "
            f"{detail}"
        ) from exc
    return True


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
            if not cookies.get("cf_clearance"):
                raise RuntimeError("Managed Chrome does not have a usable SocialBlade Cloudflare clearance cookie")
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
                    if not cookies.get("cf_clearance"):
                        raise RuntimeError(
                            "Managed Chrome does not have a usable SocialBlade Cloudflare clearance cookie"
                        )
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
