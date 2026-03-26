from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import BoundedSemaphore
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from trr_backend.integrations import getty as getty_integration
from trr_backend.utils.env import load_env

logger = logging.getLogger(__name__)

_DEFAULT_CHROME_PROFILE_GLOBS = (
    "codex-agent",
    "codex-agent-*",
)
_GETTY_SIGN_IN_URL = "https://www.gettyimages.com/sign-in"
_CHROME_EXECUTABLE = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
_BROWSER_WAIT_MS = 1_500
_DEFAULT_BROWSER_SEARCH_PAGE_CONCURRENCY = 3
GettyPrefetchProgressCallback = Callable[[dict[str, Any]], None]


class GettyPrefetchSessionError(RuntimeError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class LocalGettyBridge:
    def __init__(
        self,
        *,
        session: requests.Session,
        auth_details: dict[str, Any],
        search_page_fetcher: getty_integration.GettySearchPageFetcher | None = None,
        browser_context: Any | None = None,
        cleanup_cb: Any | None = None,
        profile_dir: str | None = None,
    ) -> None:
        self.session = session
        self.auth_details = auth_details
        self.search_page_fetcher = search_page_fetcher
        self._browser_context = browser_context
        self._cleanup_cb = cleanup_cb
        self.profile_dir = profile_dir

    def close(self) -> None:
        if self._browser_context is not None:
            try:
                self._browser_context.close()
            except Exception:
                pass
            self._browser_context = None
        if self._cleanup_cb is not None:
            try:
                self._cleanup_cb()
            except Exception:
                pass
            self._cleanup_cb = None


def _iter_profile_dirs() -> list[Path]:
    explicit = str(os.getenv("TRR_GETTY_CHROME_PROFILE_DIR") or "").strip()
    if explicit:
        path = Path(explicit).expanduser()
        return [path] if path.exists() else []

    base = Path.home() / ".chrome-profiles"
    if not base.exists():
        return []

    discovered: list[Path] = []
    seen: set[str] = set()
    for pattern in _DEFAULT_CHROME_PROFILE_GLOBS:
        for candidate in sorted(base.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True):
            resolved = str(candidate.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            discovered.append(candidate)
    return discovered


def _iter_cookie_files() -> list[Path]:
    explicit = str(os.getenv("TRR_GETTY_COOKIE_FILE") or "").strip()
    candidates: list[Path] = []
    if explicit:
        cookie_path = Path(explicit).expanduser()
        if cookie_path.exists():
            candidates.append(cookie_path)
    for profile_dir in _iter_profile_dirs():
        cookie_path = profile_dir / "Default" / "Cookies"
        if cookie_path.exists():
            candidates.append(cookie_path)
    seen: set[str] = set()
    deduped: list[Path] = []
    for candidate in candidates:
        resolved = str(candidate.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(candidate)
    return deduped


def _load_cookie_jar(cookie_file: Path) -> tuple[requests.cookies.RequestsCookieJar | None, int]:
    try:
        import browser_cookie3  # type: ignore[import-not-found]
    except Exception:
        return None, 0

    try:
        jar = browser_cookie3.chrome(cookie_file=str(cookie_file), domain_name="gettyimages.com")
    except Exception:
        return None, 0

    cookies = requests.cookies.RequestsCookieJar()
    count = 0
    for cookie in jar:
        cookies.set(cookie.name, cookie.value, domain=cookie.domain, path=cookie.path)
        count += 1
    if count <= 0:
        return None, 0
    return cookies, count


def _playwright_ready() -> bool:
    if not Path(_CHROME_EXECUTABLE).exists():
        return False
    try:
        import playwright.sync_api  # noqa: F401
    except Exception:
        return False
    return True


def _resolve_browser_search_page_concurrency() -> int:
    raw_value = str(os.getenv("TRR_GETTY_BROWSER_SEARCH_PAGE_CONCURRENCY") or "").strip()
    if not raw_value:
        return _DEFAULT_BROWSER_SEARCH_PAGE_CONCURRENCY
    try:
        parsed = int(raw_value)
    except ValueError:
        return _DEFAULT_BROWSER_SEARCH_PAGE_CONCURRENCY
    return max(1, parsed)


def _context_cookies_to_session(browser_context: Any) -> tuple[requests.Session, int]:
    session = requests.Session()
    cookies = browser_context.cookies()
    count = 0
    for cookie in cookies:
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "")
        domain = str(cookie.get("domain") or "").strip() or None
        path = str(cookie.get("path") or "/").strip() or "/"
        if not name:
            continue
        session.cookies.set(name, value, domain=domain, path=path)
        if domain and "gettyimages.com" in domain:
            count += 1
    return session, count


def _extract_requested_search_page(url: str) -> int:
    parsed = urlparse(str(url or ""))
    raw_page = next(iter(parse_qs(parsed.query).get("page", [])), "").strip()
    try:
        page = int(raw_page)
    except (TypeError, ValueError):
        return 1
    return max(1, page)


def _canonicalize_search_url(url: str) -> str:
    parsed = urlparse(str(url or ""))
    query = parse_qs(parsed.query, keep_blank_values=False)
    query.pop("page", None)
    normalized_query = urlencode(
        sorted((key, values[-1]) for key, values in query.items() if values),
        doseq=True,
    )
    normalized_path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{normalized_path}?{normalized_query}"


def _browser_page_result(search_page: Any, response: Any | None) -> dict[str, Any]:
    html = search_page.content()
    current_page = getty_integration._extract_current_page_number(html)  # noqa: SLF001
    page_candidates = getty_integration._extract_search_asset_candidates(html)  # noqa: SLF001
    if not page_candidates:
        page_candidates = [
            {"detail_url": url}
            for url in getty_integration._extract_detail_urls_from_html(html)  # noqa: SLF001
        ]
    first_editorial_ids = getty_integration._collect_page_editorial_ids(page_candidates)  # noqa: SLF001
    page_signature = getty_integration._build_page_signature(page_candidates)  # noqa: SLF001
    status_code = None
    if response is not None:
        try:
            status_code = response.status
        except Exception:
            status_code = None
    return {
        "html": html,
        "response_url": str(search_page.url or "").strip() or None,
        "status_code": status_code,
        "current_page": current_page,
        "first_editorial_ids": first_editorial_ids,
        "page_signature": page_signature,
    }


def _profile_is_authenticated(page: Any) -> bool:
    page.goto(_GETTY_SIGN_IN_URL, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(_BROWSER_WAIT_MS)
    return "/sign-in" not in str(page.url or "")


def _submit_login(page: Any, *, email: str, password: str) -> bool:
    page.goto(_GETTY_SIGN_IN_URL, wait_until="domcontentloaded", timeout=60_000)
    page.locator("form").first.locator("#new_session_username").fill(email)
    page.locator("form").first.locator("#new_session_password").fill(password)
    page.locator("form").first.locator("button", has_text="SIGN IN").click()
    page.wait_for_load_state("domcontentloaded", timeout=60_000)
    page.wait_for_timeout(7_000)
    return "/sign-in" not in str(page.url or "")


def _build_browser_bridge(profile_dir: Path) -> LocalGettyBridge | None:
    if not _playwright_ready():
        logger.info("Getty browser bridge skipped: Playwright or Chrome unavailable.")
        return None

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None

    try:
        playwright = sync_playwright().start()
    except Exception as exc:
        logger.warning("Getty browser bridge failed to start Playwright for %s: %s", profile_dir, exc)
        return None

    browser_context = None
    try:
        browser_context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            executable_path=_CHROME_EXECUTABLE,
            headless=True,
        )
        search_page_slots = BoundedSemaphore(_resolve_browser_search_page_concurrency())
        query_pages: dict[str, dict[str, Any]] = {}
        page = browser_context.new_page()
        authenticated = _profile_is_authenticated(page)
        auth_mode = "chrome_profile_browser_session"
        auth_warning: str | None = None
        if not authenticated:
            email = str(os.getenv("TRR_GETTY_EMAIL") or "").strip()
            password = str(os.getenv("TRR_GETTY_PASSWORD") or "").strip()
            if email and password and _submit_login(page, email=email, password=password):
                authenticated = True
                auth_mode = "chrome_profile_browser_login_bootstrap"
            else:
                auth_warning = "Codex Getty Chrome profile is not authenticated; Getty scraping may be truncated."

        session, cookie_count = _context_cookies_to_session(browser_context)

        def _fetch_search_page(url: str) -> getty_integration.GettySearchPageFetchResult:
            with search_page_slots:
                requested_page = _extract_requested_search_page(url)
                query_key = _canonicalize_search_url(url)
                query_state = query_pages.get(query_key)
                search_page = None
                response = None
                try:
                    if query_state is None:
                        search_page = browser_context.new_page()
                    else:
                        search_page = query_state["page"]
                    response = search_page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    search_page.wait_for_timeout(_BROWSER_WAIT_MS)
                    result = _browser_page_result(search_page, response)
                    query_pages[query_key] = {
                        "page": search_page,
                        "current_page": result.get("current_page") or requested_page,
                    }
                    return result
                finally:
                    if search_page is not None and query_pages.get(query_key, {}).get("page") is not search_page:
                        try:
                            search_page.close()
                        except Exception:
                            pass

        return LocalGettyBridge(
            session=session,
            auth_details={
                "auth_mode": auth_mode,
                "auth_cookie_file": None,
                "auth_profile_dir": str(profile_dir),
                "auth_cookie_count": cookie_count,
                "auth_warning": auth_warning,
            },
            search_page_fetcher=_fetch_search_page,
            browser_context=browser_context,
            cleanup_cb=playwright.stop,
            profile_dir=str(profile_dir),
        )
    except Exception as exc:
        logger.warning("Getty browser bridge failed for %s: %s", profile_dir, exc)
        if browser_context is not None:
            try:
                browser_context.close()
            except Exception:
                pass
        try:
            playwright.stop()
        except Exception:
            pass
        return None


def _build_cookie_bridge() -> LocalGettyBridge:
    session = requests.Session()
    auth_details: dict[str, Any] = {
        "auth_mode": "anonymous",
        "auth_cookie_file": None,
        "auth_profile_dir": None,
        "auth_cookie_count": 0,
        "auth_warning": "No Getty Chrome session cookies were available; scraping anonymously.",
    }

    for cookie_file in _iter_cookie_files():
        jar, count = _load_cookie_jar(cookie_file)
        if jar is None or count <= 0:
            continue
        session.cookies.update(jar)
        auth_details.update(
            {
                "auth_mode": "chrome_profile_cookies",
                "auth_cookie_file": str(cookie_file),
                "auth_profile_dir": str(cookie_file.parent.parent),
                "auth_cookie_count": count,
                "auth_warning": None,
            }
        )
        break

    return LocalGettyBridge(session=session, auth_details=auth_details)


def _make_isolated_profile_dir(seed_profile_dir: Path) -> Path:
    root = Path(tempfile.mkdtemp(prefix="trr-getty-profile-"))
    default_dir = root / "Default"
    default_dir.mkdir(parents=True, exist_ok=True)
    local_state_src = seed_profile_dir / "Local State"
    if local_state_src.exists():
        try:
            shutil.copy2(local_state_src, root / "Local State")
        except Exception:
            pass
    return root


def _build_isolated_browser_bridge(seed_profile_dir: Path) -> LocalGettyBridge | None:
    if not _playwright_ready():
        logger.info("Getty isolated browser bridge skipped: Playwright or Chrome unavailable.")
        return None

    email = str(os.getenv("TRR_GETTY_EMAIL") or "").strip()
    password = str(os.getenv("TRR_GETTY_PASSWORD") or "").strip()
    if not email or not password:
        logger.info("Getty isolated browser bridge skipped: credentials unavailable in environment.")
        return None

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None

    isolated_profile_dir = _make_isolated_profile_dir(seed_profile_dir)
    try:
        playwright = sync_playwright().start()
    except Exception as exc:
        logger.warning("Getty isolated browser bridge failed to start Playwright for %s: %s", seed_profile_dir, exc)
        shutil.rmtree(isolated_profile_dir, ignore_errors=True)
        return None

    browser_context = None
    try:
        browser_context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(isolated_profile_dir),
            executable_path=_CHROME_EXECUTABLE,
            headless=True,
        )
        search_page_slots = BoundedSemaphore(_resolve_browser_search_page_concurrency())
        query_pages: dict[str, dict[str, Any]] = {}
        page = browser_context.new_page()
        if not _submit_login(page, email=email, password=password):
            logger.warning("Getty isolated browser bridge login bootstrap failed for %s.", seed_profile_dir)
            browser_context.close()
            playwright.stop()
            shutil.rmtree(isolated_profile_dir, ignore_errors=True)
            return None

        session, cookie_count = _context_cookies_to_session(browser_context)

        def _fetch_search_page(url: str) -> getty_integration.GettySearchPageFetchResult:
            with search_page_slots:
                requested_page = _extract_requested_search_page(url)
                query_key = _canonicalize_search_url(url)
                query_state = query_pages.get(query_key)
                search_page = None
                response = None
                try:
                    if query_state is None:
                        search_page = browser_context.new_page()
                    else:
                        search_page = query_state["page"]
                    response = search_page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    search_page.wait_for_timeout(_BROWSER_WAIT_MS)
                    result = _browser_page_result(search_page, response)
                    query_pages[query_key] = {
                        "page": search_page,
                        "current_page": result.get("current_page") or requested_page,
                    }
                    return result
                finally:
                    if search_page is not None and query_pages.get(query_key, {}).get("page") is not search_page:
                        try:
                            search_page.close()
                        except Exception:
                            pass

        def _cleanup() -> None:
            try:
                playwright.stop()
            finally:
                shutil.rmtree(isolated_profile_dir, ignore_errors=True)

        return LocalGettyBridge(
            session=session,
            auth_details={
                "auth_mode": "chrome_profile_browser_login_bootstrap_isolated",
                "auth_cookie_file": None,
                "auth_profile_dir": str(seed_profile_dir),
                "auth_cookie_count": cookie_count,
                "auth_warning": None,
            },
            search_page_fetcher=_fetch_search_page,
            browser_context=browser_context,
            cleanup_cb=_cleanup,
            profile_dir=str(isolated_profile_dir),
        )
    except Exception as exc:
        logger.warning("Getty isolated browser bridge failed for %s: %s", seed_profile_dir, exc)
        if browser_context is not None:
            try:
                browser_context.close()
            except Exception:
                pass
        try:
            playwright.stop()
        except Exception:
            pass
        shutil.rmtree(isolated_profile_dir, ignore_errors=True)
        return None


@contextmanager
def local_getty_bridge() -> Iterator[LocalGettyBridge]:
    bridge: LocalGettyBridge | None = None
    try:
        profile_dirs = _iter_profile_dirs()
        for profile_dir in profile_dirs:
            bridge = _build_browser_bridge(profile_dir)
            if bridge is not None:
                logger.info(
                    "Getty bridge selected live browser profile %s (auth_mode=%s).",
                    bridge.profile_dir or profile_dir,
                    bridge.auth_details.get("auth_mode"),
                )
                break
        if bridge is None:
            for profile_dir in profile_dirs:
                bridge = _build_isolated_browser_bridge(profile_dir)
                if bridge is not None:
                    logger.info(
                        "Getty bridge selected isolated browser profile %s seeded from %s (auth_mode=%s).",
                        bridge.profile_dir or "unknown",
                        profile_dir,
                        bridge.auth_details.get("auth_mode"),
                    )
                    break
        if bridge is None:
            bridge = _build_cookie_bridge()
            logger.info("Getty bridge fell back to %s.", bridge.auth_details.get("auth_mode"))
        yield bridge
    finally:
        if bridge is not None:
            bridge.close()


def build_local_getty_session() -> tuple[requests.Session, dict[str, Any]]:
    with local_getty_bridge() as bridge:
        return bridge.session, dict(bridge.auth_details)


def _bridge_supports_authenticated_browser_session(bridge: LocalGettyBridge) -> bool:
    auth_mode = str(bridge.auth_details.get("auth_mode") or "").strip()
    auth_warning = str(bridge.auth_details.get("auth_warning") or "").strip()
    return auth_mode.startswith("chrome_profile_browser") and not auth_warning


def _build_isolated_bridge_from_bridge(bridge: LocalGettyBridge) -> LocalGettyBridge | None:
    auth_profile_dir = str(bridge.auth_details.get("auth_profile_dir") or bridge.profile_dir or "").strip()
    if not auth_profile_dir:
        return None
    try:
        bridge.close()
    except Exception:
        logger.debug("Getty bridge close failed during isolated replacement.", exc_info=True)
    return _build_isolated_browser_bridge(Path(auth_profile_dir))


def _query_indicates_session_truncation(summary: dict[str, Any]) -> bool:
    termination_reason = str(summary.get("termination_reason") or "").strip().lower()
    if termination_reason in {"pagination_rewrite", "session_truncated"}:
        return True
    return bool(summary.get("pagination_rewrite_detected"))


def _raise_getty_session_error(message: str, *, code: str) -> None:
    raise GettyPrefetchSessionError(message, code=code)


def _build_query_specs(person_name: str, show_name: str | None = None) -> list[dict[str, Any]]:
    normalized_person_name = str(person_name or "").strip()
    queries = [
        {
            "label": "Bravo Search",
            "scope": "bravo",
            "phrase": f"{normalized_person_name} Bravo".strip(),
            "query_params": {"sort": "newest"},
        },
        {
            "label": "Broad Search",
            "scope": "broad",
            "phrase": normalized_person_name,
            "query_params": {"sort": "newest"},
        },
    ]
    return queries


def _emit_prefetch_progress(
    progress_cb: GettyPrefetchProgressCallback | None,
    heartbeat_cb: Callable[[], None] | None,
    payload: dict[str, Any],
) -> None:
    if heartbeat_cb is not None:
        try:
            heartbeat_cb()
        except Exception:
            logger.debug("Getty prefetch heartbeat callback failed.", exc_info=True)
    if progress_cb is None:
        return
    try:
        progress_cb(dict(payload))
    except Exception:
        logger.debug("Getty prefetch progress callback failed.", exc_info=True)


def _build_query_progress_message(
    label: str,
    payload: dict[str, Any],
) -> str:
    termination_reason = str(payload.get("termination_reason") or "").strip()
    requested_page = payload.get("requested_page")
    current_page = payload.get("current_page")
    display_page = current_page if isinstance(current_page, int) and current_page > 0 else requested_page
    fetched_total = int(payload.get("fetched_candidates_total") or 0)
    new_unique = int(payload.get("new_unique_count") or 0)
    if termination_reason == "challenge_page":
        return f"{label} hit a Getty challenge page."
    if termination_reason == "session_truncated":
        return f"{label} appears truncated after page 3."
    if termination_reason == "pagination_rewrite":
        if (
            isinstance(requested_page, int)
            and requested_page > 0
            and isinstance(current_page, int)
            and current_page > 0
        ):
            return f"{label} rewrote page {requested_page} to page {current_page}."
        return f"{label} rewrote to a prior page."
    if termination_reason == "duplicate_page":
        return f"{label} repeated a prior page after {fetched_total} candidates."
    if termination_reason == "natural_exhaustion":
        return f"{label} exhausted search results after {fetched_total} candidates."
    if isinstance(display_page, int) and display_page > 0:
        return f"{label} page {display_page}: {new_unique} new, {fetched_total} total candidates."
    return f"{label}: {new_unique} new, {fetched_total} total candidates."


def fetch_person_getty_prefetch_payload(
    person_name: str,
    *,
    show_name: str | None = None,
    mode: str = "full",
    progress_cb: GettyPrefetchProgressCallback | None = None,
    heartbeat_cb: Callable[[], None] | None = None,
) -> dict[str, Any]:
    load_env()
    normalized_person_name = str(person_name or "").strip()
    normalized_show_name = str(show_name or "").strip()
    normalized_mode = str(mode or "full").strip().lower()
    discovery_mode = normalized_mode == "discovery"
    credentials_available = bool(
        str(os.getenv("TRR_GETTY_EMAIL") or "").strip()
        and str(os.getenv("TRR_GETTY_PASSWORD") or "").strip()
    )

    t0 = time.perf_counter()
    with local_getty_bridge() as bridge:
        active_bridge = bridge
        replacement_bridge: LocalGettyBridge | None = None
        try:
            if not _bridge_supports_authenticated_browser_session(active_bridge):
                replacement_bridge = _build_isolated_bridge_from_bridge(active_bridge)
                if (
                    replacement_bridge is not None
                    and _bridge_supports_authenticated_browser_session(replacement_bridge)
                ):
                    active_bridge = replacement_bridge
                else:
                    if replacement_bridge is not None:
                        replacement_bridge.close()
                        replacement_bridge = None
                    if credentials_available:
                        _raise_getty_session_error(
                            "Getty login bootstrap failed for the codex Chrome profile.",
                            code="getty_login_bootstrap_failed",
                        )
                    _raise_getty_session_error(
                        "Getty profile is not authenticated in the codex Chrome profile.",
                        code="getty_profile_not_authenticated",
                    )

            session = active_bridge.session
            auth_details = active_bridge.auth_details
            search_page_fetcher = active_bridge.search_page_fetcher
            query_summaries: list[dict[str, Any]] = []
            merged_assets: list[dict[str, Any]] = []
            seen_editorial_ids: set[str] = set()

            query_specs = _build_query_specs(normalized_person_name, normalized_show_name or None)
            queries_total = len(query_specs)
            queries_completed = 0

            _emit_prefetch_progress(
                progress_cb,
                heartbeat_cb,
                {
                    "type": "phase",
                    "phase": "bridge_ready",
                    "status": "running",
                    "message": "Getty Chrome profile bridge ready.",
                    "person_name": normalized_person_name,
                    "show_name": normalized_show_name or None,
                    "prefetch_mode": "discovery" if discovery_mode else "full",
                    "queries_total": queries_total,
                    "queries_completed": queries_completed,
                    "auth_mode": auth_details.get("auth_mode"),
                    "auth_warning": auth_details.get("auth_warning"),
                    "session_validated": False,
                    "session_truncated": False,
                },
            )

            def _refresh_bridge_state() -> None:
                nonlocal session, auth_details, search_page_fetcher
                session = active_bridge.session
                auth_details = active_bridge.auth_details
                search_page_fetcher = active_bridge.search_page_fetcher

            def _attempt_isolated_replacement() -> bool:
                nonlocal active_bridge, replacement_bridge
                if active_bridge is not bridge:
                    return False
                candidate = _build_isolated_bridge_from_bridge(bridge)
                if candidate is None or not _bridge_supports_authenticated_browser_session(candidate):
                    if candidate is not None:
                        candidate.close()
                    return False
                replacement_bridge = candidate
                active_bridge = candidate
                _refresh_bridge_state()
                return True

            def _run_query(query_spec: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
                summary: dict[str, Any] = {}
                _emit_prefetch_progress(
                    progress_cb,
                    heartbeat_cb,
                    {
                        "type": "query_started",
                        "phase": "discovery",
                        "status": "running",
                        "message": f"Starting {query_spec['label']}...",
                        "label": query_spec["label"],
                        "scope": query_spec["scope"],
                        "phrase": query_spec["phrase"],
                        "query_url": getty_integration._build_search_url(  # noqa: SLF001
                            query_spec["phrase"],
                            query_params=dict(query_spec.get("query_params") or {}),
                        ),
                        "queries_total": queries_total,
                        "queries_completed": queries_completed,
                        "auth_mode": auth_details.get("auth_mode"),
                        "auth_warning": auth_details.get("auth_warning"),
                        "session_validated": bool(auth_details.get("session_validated")),
                        "session_truncated": bool(auth_details.get("session_truncated")),
                    },
                )

                def _candidate_progress(payload: dict[str, Any]) -> None:
                    _emit_prefetch_progress(
                        progress_cb,
                        heartbeat_cb,
                        {
                            "type": str(payload.get("type") or "page"),
                            "phase": "discovery",
                            "status": "running",
                            "message": _build_query_progress_message(str(query_spec["label"]), payload),
                            "label": query_spec["label"],
                            "scope": query_spec["scope"],
                            "phrase": query_spec["phrase"],
                            "query_url": payload.get("query_url"),
                            "requested_page": payload.get("requested_page"),
                            "expected_page": payload.get("expected_page"),
                            "current_page": payload.get("current_page"),
                            "response_url": payload.get("response_url"),
                            "page_classification": payload.get("page_classification"),
                            "page_candidate_count": payload.get("page_candidate_count"),
                            "new_unique_count": payload.get("new_unique_count"),
                            "fetched_candidates_total": payload.get("fetched_candidates_total"),
                            "termination_reason": payload.get("termination_reason"),
                            "page_signature": payload.get("page_signature"),
                            "first_editorial_ids": payload.get("first_editorial_ids"),
                            "site_image_total": payload.get("site_image_total"),
                            "site_event_total": payload.get("site_event_total"),
                            "site_video_total": payload.get("site_video_total"),
                            "queries_total": queries_total,
                            "queries_completed": queries_completed,
                            "auth_mode": auth_details.get("auth_mode"),
                            "auth_warning": auth_details.get("auth_warning"),
                            "session_validated": bool(auth_details.get("session_validated")),
                            "session_truncated": bool(auth_details.get("session_truncated")),
                        },
                    )

                raw_assets = getty_integration.search_editorial_assets(
                    query_spec["phrase"],
                    limit=0,
                    session=session,
                    query_params=dict(query_spec.get("query_params") or {}),
                    query_summary_out=summary,
                    search_page_fetcher=search_page_fetcher,
                    include_details=not discovery_mode,
                    candidate_progress_cb=_candidate_progress,
                )
                return summary, raw_assets

            for query_spec in query_specs:
                summary, raw_assets = _run_query(query_spec)
                if _query_indicates_session_truncation(summary):
                    auth_details["session_truncated"] = True
                    if _attempt_isolated_replacement():
                        summary, raw_assets = _run_query(query_spec)
                    if _query_indicates_session_truncation(summary):
                        requested_page = summary.get("expected_page") or 4
                        current_page = summary.get("current_page") or 1
                        _raise_getty_session_error(
                            (
                                "Getty session appears truncated after page 3. "
                                f"Getty rewrote page {requested_page} to page {current_page}."
                            ),
                            code="getty_session_truncated",
                        )
                    auth_details["session_truncated"] = False
                auth_details["session_validated"] = True

                usable_assets: list[dict[str, Any]] = []
                overlap_count = 0
                for asset in raw_assets:
                    editorial_id = str(asset.get("editorial_id") or "").strip()
                    if editorial_id and editorial_id in seen_editorial_ids:
                        overlap_count += 1
                        continue
                    if editorial_id:
                        seen_editorial_ids.add(editorial_id)
                    enriched_asset = dict(asset)
                    enriched_asset["source_query_scope"] = query_spec["scope"]
                    enriched_asset["source_query_label"] = query_spec["label"]
                    enriched_asset["source_query_phrase"] = query_spec["phrase"]
                    merged_assets.append(enriched_asset)
                    usable_assets.append(enriched_asset)

                summary["label"] = query_spec["label"]
                summary["scope"] = query_spec["scope"]
                summary["phrase"] = query_spec["phrase"]
                summary["query_params"] = dict(query_spec.get("query_params") or {})
                summary["fetched_asset_total"] = int(summary.get("fetched_candidates_total") or len(raw_assets) or 0)
                summary["usable_after_dedupe_total"] = len(usable_assets)
                summary["overlap_with_prior_queries"] = overlap_count
                summary["auth_mode"] = auth_details["auth_mode"]
                summary["session_validated"] = bool(auth_details.get("session_validated"))
                summary["session_truncated"] = bool(auth_details.get("session_truncated"))
                query_summaries.append(summary)
                queries_completed += 1
                _emit_prefetch_progress(
                    progress_cb,
                    heartbeat_cb,
                    {
                        "type": "query_completed",
                        "phase": "discovery",
                        "status": "running",
                        "message": (
                            f"{query_spec['label']} complete: {len(usable_assets)} usable assets "
                            f"({overlap_count} overlap)."
                        ),
                        "label": query_spec["label"],
                        "scope": query_spec["scope"],
                        "phrase": query_spec["phrase"],
                        "query_url": summary.get("query_url"),
                        "site_image_total": summary.get("site_image_total"),
                        "site_event_total": summary.get("site_event_total"),
                        "site_video_total": summary.get("site_video_total"),
                        "fetched_asset_total": summary.get("fetched_asset_total"),
                        "usable_after_dedupe_total": len(usable_assets),
                        "overlap_with_prior_queries": overlap_count,
                        "termination_reason": summary.get("termination_reason"),
                        "expected_page": summary.get("expected_page"),
                        "current_page": summary.get("current_page"),
                        "response_url": summary.get("response_url"),
                        "page_signature": summary.get("page_signature"),
                        "first_editorial_ids": summary.get("first_editorial_ids"),
                        "queries_total": queries_total,
                        "queries_completed": queries_completed,
                        "merged_total": len(merged_assets),
                        "auth_mode": auth_details.get("auth_mode"),
                        "auth_warning": auth_details.get("auth_warning"),
                        "session_validated": bool(auth_details.get("session_validated")),
                        "session_truncated": bool(auth_details.get("session_truncated")),
                    },
                )

            if discovery_mode:
                bravo_events: list[dict[str, Any]] = []
                broad_events: list[dict[str, Any]] = []
            else:
                _emit_prefetch_progress(
                    progress_cb,
                    heartbeat_cb,
                    {
                        "type": "phase",
                        "phase": "grouped_events",
                        "status": "running",
                        "message": "Fetching Getty grouped-event fallbacks...",
                        "queries_total": queries_total,
                        "queries_completed": queries_completed,
                        "merged_total": len(merged_assets),
                        "auth_mode": auth_details.get("auth_mode"),
                        "session_validated": bool(auth_details.get("session_validated")),
                        "session_truncated": bool(auth_details.get("session_truncated")),
                    },
                )
                bravo_phrase = f"{normalized_person_name} Bravo".strip()
                broad_phrase = normalized_person_name
                bravo_events = getty_integration.search_grouped_events(
                    bravo_phrase,
                    limit=0,
                    person_name=normalized_person_name,
                    source_query_scope="bravo",
                    session=session,
                    query_params={"sort": "newest"},
                    search_page_fetcher=search_page_fetcher,
                )
                broad_events = getty_integration.search_grouped_events(
                    broad_phrase,
                    limit=0,
                    person_name=normalized_person_name,
                    source_query_scope="broad",
                    session=session,
                    query_params={"sort": "newest"},
                    search_page_fetcher=search_page_fetcher,
                )

            merged_events: list[dict[str, Any]] = []
            seen_event_urls: set[str] = set()
            for event in bravo_events + broad_events:
                event_url = str(event.get("event_url") or "").strip()
                if event_url and event_url in seen_event_urls:
                    continue
                if event_url:
                    seen_event_urls.add(event_url)
                merged_events.append(event)

            elapsed_seconds = round(time.perf_counter() - t0, 1)
            _emit_prefetch_progress(
                progress_cb,
                heartbeat_cb,
                {
                    "type": "phase",
                    "phase": "completed",
                    "status": "completed",
                    "message": (
                        f"Getty {'discovery' if discovery_mode else 'prefetch'} complete: "
                        f"{len(merged_assets)} assets, {len(merged_events)} events."
                    ),
                    "queries_total": queries_total,
                    "queries_completed": queries_completed,
                    "merged_total": len(merged_assets),
                    "merged_events_total": len(merged_events),
                    "auth_mode": auth_details.get("auth_mode"),
                    "auth_warning": auth_details.get("auth_warning"),
                    "session_validated": bool(auth_details.get("session_validated")),
                    "session_truncated": bool(auth_details.get("session_truncated")),
                    "elapsed_seconds": elapsed_seconds,
                },
            )

            return {
                "person": normalized_person_name,
                "show_name": normalized_show_name or None,
                "prefetch_mode": "discovery" if discovery_mode else "full",
                "discovery_ready": True,
                "enrichment_status": "pending" if discovery_mode else "completed",
                "merged": merged_assets,
                "merged_total": len(merged_assets),
                "discovery_manifest": merged_assets,
                "candidate_manifest_total": len(merged_assets),
                "merged_events": merged_events,
                "merged_events_total": len(merged_events),
                "detail_enrichment_total": len(
                    {
                        str(asset.get("editorial_id") or "").strip()
                        for asset in merged_assets
                        if str(asset.get("editorial_id") or "").strip()
                    }
                ),
                "deferred_editorial_ids": sorted(
                    {
                        str(asset.get("editorial_id") or "").strip()
                        for asset in merged_assets
                        if str(asset.get("editorial_id") or "").strip()
                    }
                ),
                "image_overlap_count": sum(
                    int(item.get("overlap_with_prior_queries") or 0) for item in query_summaries
                ),
                "event_overlap_count": len(bravo_events) + len(broad_events) - len(merged_events),
                "query_summaries": query_summaries,
                "auth_mode": auth_details["auth_mode"],
                "auth_warning": auth_details.get("auth_warning"),
                "session_validated": bool(auth_details.get("session_validated")),
                "session_truncated": bool(auth_details.get("session_truncated")),
                "elapsed_seconds": elapsed_seconds,
            }
        finally:
            if replacement_bridge is not None:
                replacement_bridge.close()
