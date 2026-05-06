"""Canonical Instagram auth session resolution and bootstrap helpers."""

from __future__ import annotations

import asyncio
import contextvars
import fcntl
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any

from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager
from trr_backend.socials.instagram.cookie_refresh import (
    interactive_chrome_login,
    read_instagram_cookie_file_metadata,
    refresh_instagram_cookies,
)

logger = logging.getLogger(__name__)

INSTAGRAM_AUTH_RESOLVER_V2 = "INSTAGRAM_AUTH_RESOLVER_V2"
INSTAGRAM_AUTH_RESOLVER_VERSION = 2
INSTAGRAM_AUTH_RESOLVER_POSITIVE_TTL_SECONDS = 300
INSTAGRAM_AUTH_RESOLVER_NEGATIVE_TTL_SECONDS = 90
INSTAGRAM_AUTH_RESOLVER_SOFT_PASS_TTL_SECONDS = 90
INSTAGRAM_COMMENTS_AUTH_VALIDATION_ENV = "SOCIAL_INSTAGRAM_COMMENTS_AUTH_VALIDATION"
INSTAGRAM_COMMENTS_AUTH_VALIDATION_DEFAULT = "comments_endpoint"
INSTAGRAM_COMMENTS_AUTH_VALIDATION_MODES = {"comments_endpoint", "schema_only", "graphql_profile"}

_HANDLE_RE = re.compile(r"^[a-z0-9._]{1,30}$")
_SHORTCODE_RE = re.compile(r"^[A-Za-z0-9]{8,15}$")
_SESSION_LOCKS: dict[str, RLock] = {}
_SESSION_LOCKS_GUARD = RLock()
_VALIDATION_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CURRENT_AUTH_SESSION: contextvars.ContextVar[InstagramAuthSession | None] = contextvars.ContextVar(
    "instagram_current_auth_session",
    default=None,
)
_RUNTIME_OVERRIDE: dict[str, str] | None = None
_DEFAULT_BROWSER_SESSION_MANAGER = AccountBrowserSessionManager(
    platform="instagram",
    cookie_domains=(".instagram.com",),
)


@dataclass
class InstagramAuthSession:
    cookies: dict[str, str]
    source: str
    validated: bool
    validation_reason: str | None
    validation_category: str
    stale_ok: bool
    browser_account_id: str | None
    session_account_id: str | None
    caller_context: str | None
    cookie_file_path: Path | None
    storage_state_path: Path | None
    refreshed: bool
    refresh_method: str | None
    repaired_from_browser_session: bool
    resolver_version: int = INSTAGRAM_AUTH_RESOLVER_VERSION
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _CookieCandidate:
    cookies: dict[str, str]
    source: str
    path: Path | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_cookie_file_path() -> Path:
    return _project_root() / "scripts" / "socials" / "instagram" / "instagram_cookies.json"


def _default_session_account_id() -> str | None:
    explicit = (os.getenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID") or "").strip().lstrip("@")
    if explicit:
        return explicit.lower()
    validation_username = (os.getenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME") or "").strip().lstrip("@")
    if validation_username:
        return validation_username.lower()
    auth_username, _ = _auth_credentials()
    if auth_username:
        return auth_username.strip().lstrip("@").lower()
    return None


def _cookie_file_candidates() -> list[Path]:
    raw_candidates = [
        (os.getenv("SOCIAL_INSTAGRAM_COOKIES_FILE") or "").strip(),
        (os.getenv("INSTAGRAM_COOKIES_FILE") or "").strip(),
        str(_default_cookie_file_path()),
    ]
    return [Path(raw).expanduser() for raw in raw_candidates if raw]


def _canonical_cookie_file_path() -> Path:
    candidates = _cookie_file_candidates()
    return candidates[0] if candidates else _default_cookie_file_path()


def _env_truthy(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _is_local_environment() -> bool:
    return not (os.getenv("MODAL_TASK_ID") or os.getenv("MODAL_ENVIRONMENT"))


def _running_event_loop_active() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _auth_credentials() -> tuple[str | None, str | None]:
    username = (
        (os.getenv("SOCIAL_AUTH_INSTAGRAM_USERNAME") or "").strip()
        or (os.getenv("INSTAGRAM_USERNAME") or "").strip()
        or None
    )
    password = (
        (os.getenv("SOCIAL_AUTH_INSTAGRAM_PASSWORD") or "").strip()
        or (os.getenv("INSTAGRAM_PASSWORD") or "").strip()
        or None
    )
    return username, password


def _auto_refresh_enabled() -> bool:
    if _running_event_loop_active():
        return False
    raw = (os.getenv("SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH") or "").strip().lower()
    if raw:
        return raw not in {"0", "false", "off", "no"}
    username, password = _auth_credentials()
    return bool(username and password)


def _interactive_login_enabled() -> bool:
    raw = (os.getenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN") or "").strip().lower()
    if raw in {"0", "false", "off", "no"}:
        return False
    if _running_event_loop_active():
        return False
    return _is_local_environment()


def _resolver_v2_enabled() -> bool:
    return _env_truthy(INSTAGRAM_AUTH_RESOLVER_V2)


def resolve_instagram_comments_auth_validation_mode(value: str | None = None) -> str:
    raw_value = (
        str(value or "").strip().lower()
        or str(os.getenv(INSTAGRAM_COMMENTS_AUTH_VALIDATION_ENV) or "").strip().lower()
        or INSTAGRAM_COMMENTS_AUTH_VALIDATION_DEFAULT
    )
    if raw_value in INSTAGRAM_COMMENTS_AUTH_VALIDATION_MODES:
        return raw_value
    logger.warning(
        "Invalid %s=%r; falling back to %s",
        INSTAGRAM_COMMENTS_AUTH_VALIDATION_ENV,
        raw_value,
        INSTAGRAM_COMMENTS_AUTH_VALIDATION_DEFAULT,
    )
    return INSTAGRAM_COMMENTS_AUTH_VALIDATION_DEFAULT


def _looks_like_shortcode(value: str) -> bool:
    return bool(_SHORTCODE_RE.fullmatch(value) and any(ch.isupper() for ch in value))


def _looks_like_handle(value: str) -> bool:
    normalized = str(value or "").strip().lstrip("@")
    return bool(_HANDLE_RE.fullmatch(normalized.lower()) and not _looks_like_shortcode(normalized))


def _normalize_session_account_id(browser_account_id: str | None) -> tuple[str | None, str | None]:
    normalized = str(browser_account_id or "").strip().lstrip("@")
    default_account_id = _default_session_account_id()
    if not normalized:
        return default_account_id, None
    lower = normalized.lower()
    if default_account_id and default_account_id != lower:
        return default_account_id, normalized
    if _looks_like_handle(normalized):
        return lower, None
    if default_account_id:
        return default_account_id, normalized
    return None, normalized


def _cookie_fingerprint(cookies: dict[str, str]) -> str:
    payload = json.dumps(sorted((str(key), str(value)) for key, value in cookies.items()), separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _masked_fingerprint(cookies: dict[str, str]) -> str | None:
    if not cookies:
        return None
    return _cookie_fingerprint(cookies)[:12]


def _structural_validation(cookies: dict[str, str]) -> tuple[bool, str | None]:
    sessionid = bool(str(cookies.get("sessionid") or "").strip())
    csrftoken = bool(str(cookies.get("csrftoken") or "").strip())
    ds_user_id = bool(str(cookies.get("ds_user_id") or "").strip())
    if sessionid and csrftoken and ds_user_id:
        return True, None
    if not sessionid:
        return False, "no_sessionid"
    if not csrftoken and not ds_user_id:
        return False, "missing_csrftoken_and_ds_user_id"
    if not csrftoken:
        return False, "missing_csrftoken"
    return False, "missing_ds_user_id"


def _read_cookie_file(cookie_file: Path) -> dict[str, str]:
    if not cookie_file.is_file():
        return {}
    try:
        payload = json.loads(cookie_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        logger.debug("Failed reading Instagram cookie file %s", cookie_file, exc_info=True)
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in payload.items()
        if not str(key).startswith("_") and str(key or "").strip() and str(value or "").strip()
    }


def _safe_write_cookie_file(cookie_file: Path, cookies: dict[str, str]) -> None:
    cookie_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "_cookie_refreshed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **cookies,
    }
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(cookie_file.parent)) as handle:
        temp_path = Path(handle.name)
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp_path.chmod(0o600)
    os.replace(temp_path, cookie_file)


def _browser_session_candidate(
    session_account_id: str | None,
    *,
    browser_session_manager: AccountBrowserSessionManager,
) -> _CookieCandidate | None:
    if not session_account_id:
        return None
    session_paths = browser_session_manager.session_paths(session_account_id, fallback_account_id=session_account_id)
    cookies = _read_cookie_file(session_paths.cookie_file_path)
    if not cookies:
        return None
    return _CookieCandidate(
        cookies=cookies,
        source="browser_session",
        path=session_paths.cookie_file_path,
        metadata=read_instagram_cookie_file_metadata(session_paths.cookie_file_path),
    )


def _env_json_candidate() -> _CookieCandidate | None:
    raw_json = (os.getenv("SOCIAL_INSTAGRAM_COOKIES_JSON") or "").strip() or (
        os.getenv("INSTAGRAM_COOKIES_JSON") or ""
    ).strip()
    if not raw_json:
        return None
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        logger.warning("Invalid Instagram cookies JSON from env; ignoring env candidate")
        return None
    if not isinstance(payload, dict):
        logger.warning("Instagram cookies JSON env value is not an object; ignoring env candidate")
        return None
    cookies = {
        str(key): str(value)
        for key, value in payload.items()
        if not str(key).startswith("_") and value is not None and str(key or "").strip() and str(value or "").strip()
    }
    if not cookies:
        return None
    return _CookieCandidate(cookies=cookies, source="env_json")


def _file_candidates() -> list[_CookieCandidate]:
    candidates: list[_CookieCandidate] = []
    default_path = _default_cookie_file_path().expanduser().resolve()
    for path in _cookie_file_candidates():
        if not path.is_file():
            continue
        cookies = _read_cookie_file(path)
        if not cookies:
            continue
        source = "repo_default_cookie_file" if path.expanduser().resolve() == default_path else "configured_cookie_file"
        candidates.append(
            _CookieCandidate(
                cookies=cookies,
                source=source,
                path=path,
                metadata=read_instagram_cookie_file_metadata(path),
            )
        )
    return candidates


def _select_best_candidate(candidates: list[_CookieCandidate]) -> _CookieCandidate:
    source_priority = {
        "runtime_override": 5,
        "browser_session": 4,
        "env_json": 3,
        "configured_cookie_file": 2,
        "repo_default_cookie_file": 1,
    }

    def _score(candidate: _CookieCandidate) -> tuple[int, int, int]:
        cookies = candidate.cookies
        score = 0
        if str(cookies.get("sessionid") or "").strip():
            score += 4
        if str(cookies.get("csrftoken") or "").strip():
            score += 2
        if str(cookies.get("ds_user_id") or "").strip():
            score += 2
        return source_priority.get(candidate.source, 0), score, len(cookies)

    return max(candidates, key=_score)


@contextmanager
def _session_process_lock(session_account_id: str | None) -> Iterator[None]:
    if not session_account_id:
        yield
        return
    session_paths = _DEFAULT_BROWSER_SESSION_MANAGER.session_paths(
        session_account_id,
        fallback_account_id=session_account_id,
    )
    lock_path = session_paths.cookie_file_path.with_suffix(session_paths.cookie_file_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _session_thread_lock(session_account_id: str | None) -> Iterator[None]:
    if not session_account_id:
        yield
        return
    with _SESSION_LOCKS_GUARD:
        lock = _SESSION_LOCKS.get(session_account_id)
        if lock is None:
            lock = RLock()
            _SESSION_LOCKS[session_account_id] = lock
    with lock:
        yield


def _merge_missing_fields(base: dict[str, str], browser_cookies: dict[str, str]) -> tuple[dict[str, str], bool]:
    merged = dict(base)
    repaired = False
    for key in ("csrftoken", "ds_user_id", "mid"):
        if not str(merged.get(key) or "").strip() and str(browser_cookies.get(key) or "").strip():
            merged[key] = str(browser_cookies[key])
            repaired = True
    return merged, repaired


def _validation_ttl_seconds(category: str, validated: bool, stale_ok: bool) -> int:
    if validated and not stale_ok:
        return INSTAGRAM_AUTH_RESOLVER_POSITIVE_TTL_SECONDS
    if stale_ok:
        return INSTAGRAM_AUTH_RESOLVER_SOFT_PASS_TTL_SECONDS
    if category in {"checkpoint_required", "redirect_login", "unauthorized"}:
        return 0
    return INSTAGRAM_AUTH_RESOLVER_NEGATIVE_TTL_SECONDS


def _cache_get(fingerprint: str) -> dict[str, Any] | None:
    cached = _VALIDATION_CACHE.get(fingerprint)
    if not cached:
        return None
    timestamp, payload = cached
    ttl_seconds = int(payload.get("ttl_seconds") or 0)
    if ttl_seconds <= 0 or (time.monotonic() - timestamp) >= ttl_seconds:
        _VALIDATION_CACHE.pop(fingerprint, None)
        return None
    return dict(payload)


def _cache_set(fingerprint: str, payload: dict[str, Any]) -> None:
    ttl_seconds = int(payload.get("ttl_seconds") or 0)
    if ttl_seconds <= 0:
        return
    _VALIDATION_CACHE[fingerprint] = (time.monotonic(), dict(payload))


def _cache_evict(fingerprint: str | None) -> None:
    if not fingerprint:
        return
    _VALIDATION_CACHE.pop(fingerprint, None)


def _validation_username(session_account_id: str | None, *, caller_context: str | None = None) -> str:
    fallback = (os.getenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME") or "").strip().lstrip("@")
    if fallback:
        return fallback.lower()
    normalized_context = str(caller_context or "").strip().lstrip("@")
    if _looks_like_handle(normalized_context):
        return normalized_context.lower()
    return (session_account_id or "bravotv").lower()


def _validate_cookies_via_graphql(
    cookies: dict[str, str],
    *,
    session_account_id: str | None,
    caller_context: str | None = None,
    require_validation: bool,
) -> tuple[bool, str | None, str, bool]:
    structurally_valid, structural_reason = _structural_validation(cookies)
    if not structurally_valid:
        return False, structural_reason, "structural_invalid", False
    if not require_validation:
        # Skipping validation means the caller has opted out of the GraphQL
        # probe; treat that as a pass (first tuple element = True) so
        # downstream code doesn't trigger a forced refresh just because we
        # declined to validate. Structural checks above still guard us.
        return True, "validation_skipped", "validation_skipped", False

    fingerprint = _cookie_fingerprint(cookies)
    cached = _cache_get(fingerprint)
    if cached:
        return (
            bool(cached.get("validated")),
            str(cached.get("validation_reason") or "").strip() or None,
            str(cached.get("validation_category") or "graphql_validation_failed"),
            bool(cached.get("stale_ok")),
        )

    from trr_backend.socials.instagram.scraper import InstagramScraper

    validation_username = _validation_username(session_account_id, caller_context=caller_context)
    scraper = InstagramScraper(cookies=dict(cookies), browser_account_id=session_account_id or validation_username)
    payload = scraper.fetch_posts_graphql(
        validation_username,
        delay=0.0,
        request_timeout=(10, 20),
        allow_browser_fallback=False,
    )
    connection = (payload or {}).get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
    if connection.get("edges"):
        result = {
            "validated": True,
            "validation_reason": None,
            "validation_category": "validated",
            "stale_ok": False,
        }
        result["ttl_seconds"] = _validation_ttl_seconds("validated", True, False)
        _cache_set(fingerprint, result)
        return True, None, "validated", False

    retrieval_meta = dict(scraper.last_retrieval_meta or {})
    error_code = str(retrieval_meta.get("error_code") or "").strip().lower()
    error_message = str(retrieval_meta.get("error_message") or "").strip().lower()
    status_code = int(retrieval_meta.get("error_status_code") or 0)
    if error_code == "instagram_graphql_checkpoint_required" or error_message == "checkpoint_required":
        return False, "checkpoint_required", "checkpoint_required", False
    if error_code in {"redirect_login"}:
        return False, "redirect_login", "redirect_login", False
    if error_code in {"instagram_graphql_cursor_unauthorized", "unauthorized", "instagram_graphql_cursor_forbidden"}:
        return False, error_code or "unauthorized", "unauthorized", False
    if status_code in {401, 429} and "wait" in error_message:
        result = {
            "validated": True,
            "validation_reason": "rate_limited_soft_pass",
            "validation_category": "rate_limited_soft_pass",
            "stale_ok": True,
        }
        result["ttl_seconds"] = _validation_ttl_seconds("rate_limited_soft_pass", True, True)
        _cache_set(fingerprint, result)
        return True, "rate_limited_soft_pass", "rate_limited_soft_pass", True
    result = {
        "validated": False,
        "validation_reason": error_code or "graphql_validation_failed",
        "validation_category": "graphql_validation_failed",
        "stale_ok": False,
    }
    result["ttl_seconds"] = _validation_ttl_seconds("graphql_validation_failed", False, False)
    _cache_set(fingerprint, result)
    return False, result["validation_reason"], "graphql_validation_failed", False


def _promote_browser_session_to_canonical_file(
    cookies: dict[str, str],
    *,
    cookie_file_path: Path,
    browser_candidate: _CookieCandidate | None,
    observed_mtime: float | None,
    caller_context: str | None = None,
) -> bool:
    if browser_candidate is None:
        return False
    if cookie_file_path.exists():
        current_cookies = _read_cookie_file(cookie_file_path)
        if current_cookies == cookies:
            return False
    current_mtime = cookie_file_path.stat().st_mtime if cookie_file_path.exists() else None
    if observed_mtime is not None and current_mtime is not None and current_mtime != observed_mtime:
        current_cookies = _read_cookie_file(cookie_file_path)
        current_valid, _, _, _ = _validate_cookies_via_graphql(
            current_cookies,
            session_account_id=None,
            caller_context=caller_context,
            require_validation=True,
        )
        if current_valid:
            return False
    _safe_write_cookie_file(cookie_file_path, cookies)
    return True


def _refresh_with_credentials(
    *,
    session_account_id: str | None,
    caller_context: str | None = None,
    cookie_file_path: Path,
) -> tuple[dict[str, str], str | None]:
    if not _auto_refresh_enabled():
        return {}, None
    username, password = _auth_credentials()
    if not username or not password:
        return {}, None
    cookies = refresh_instagram_cookies(
        username=username,
        password=password,
        cookie_file=str(cookie_file_path),
        account_id=session_account_id or username,
        headless=(os.getenv("SOCIAL_INSTAGRAM_COOKIE_REFRESH_HEADLESS") or "true").strip().lower()
        not in {"0", "false", "off", "no"},
        timeout_seconds=max(30, int(os.getenv("SOCIAL_INSTAGRAM_COOKIE_REFRESH_TIMEOUT_SECONDS") or "120")),
        validation_username=_validation_username(session_account_id, caller_context=caller_context),
    )
    return cookies, "credential_refresh"


def _refresh_interactively(
    *,
    session_account_id: str | None,
    caller_context: str | None = None,
    cookie_file_path: Path,
) -> tuple[dict[str, str], str | None]:
    if not _interactive_login_enabled():
        return {}, None
    cookies = interactive_chrome_login(
        chrome_profile_name=(os.getenv("SOCIAL_INSTAGRAM_CHROME_PROFILE") or "").strip()
        or "entertainmentdatagroup@gmail.com",
        cookie_file=str(cookie_file_path),
        timeout_seconds=max(60, int(os.getenv("SOCIAL_INSTAGRAM_INTERACTIVE_TIMEOUT_SECONDS") or "300")),
        validation_username=_validation_username(session_account_id, caller_context=caller_context),
        account_id=session_account_id,
        headless=(os.getenv("SOCIAL_INSTAGRAM_BROWSER_MODE") or "").strip().lower() == "headless",
    )
    return cookies, "interactive_login"


def set_current_instagram_auth_session(auth_session: InstagramAuthSession | None) -> None:
    _CURRENT_AUTH_SESSION.set(auth_session)


def get_current_instagram_auth_session() -> InstagramAuthSession | None:
    return _CURRENT_AUTH_SESSION.get()


def set_instagram_runtime_override(cookies: dict[str, str] | None) -> None:
    global _RUNTIME_OVERRIDE
    _RUNTIME_OVERRIDE = dict(cookies) if cookies else None


def clear_instagram_auth_runtime_state() -> None:
    global _RUNTIME_OVERRIDE
    _RUNTIME_OVERRIDE = None
    _VALIDATION_CACHE.clear()
    set_current_instagram_auth_session(None)


def _build_auth_session(
    *,
    cookies: dict[str, str],
    source: str,
    validated: bool,
    validation_reason: str | None,
    validation_category: str,
    stale_ok: bool,
    browser_account_id: str | None,
    session_account_id: str | None,
    caller_context: str | None,
    cookie_file_path: Path | None,
    storage_state_path: Path | None,
    refreshed: bool,
    refresh_method: str | None,
    repaired_from_browser_session: bool,
    metadata: dict[str, Any] | None = None,
) -> InstagramAuthSession:
    return InstagramAuthSession(
        cookies=dict(cookies),
        source=source,
        validated=validated,
        validation_reason=validation_reason,
        validation_category=validation_category,
        stale_ok=stale_ok,
        browser_account_id=browser_account_id,
        session_account_id=session_account_id,
        caller_context=caller_context,
        cookie_file_path=cookie_file_path,
        storage_state_path=storage_state_path,
        refreshed=refreshed,
        refresh_method=refresh_method,
        repaired_from_browser_session=repaired_from_browser_session,
        metadata=dict(metadata or {}),
    )


def resolve_instagram_auth_session(
    *,
    browser_account_id: str | None,
    caller_context: str | None = None,
    require_validation: bool = True,
    browser_session_manager: AccountBrowserSessionManager | None = None,
) -> InstagramAuthSession:
    session_account_id, normalized_caller_context = _normalize_session_account_id(browser_account_id)
    caller_context = str(caller_context or normalized_caller_context or "").strip() or None
    validation_context = caller_context if _looks_like_handle(caller_context or "") else normalized_caller_context
    if not _looks_like_handle(validation_context or ""):
        validation_context = None
    if session_account_id is None and require_validation:
        raise RuntimeError("instagram_auth_session_account_unresolved")

    browser_session_manager = browser_session_manager or _DEFAULT_BROWSER_SESSION_MANAGER
    session_paths = (
        browser_session_manager.session_paths(session_account_id, fallback_account_id=session_account_id)
        if session_account_id
        else None
    )
    cookie_file_path = _canonical_cookie_file_path()
    observed_cookie_file_mtime = cookie_file_path.stat().st_mtime if cookie_file_path.exists() else None

    with _session_thread_lock(session_account_id), _session_process_lock(session_account_id):
        candidates: list[_CookieCandidate] = []
        if _RUNTIME_OVERRIDE:
            candidates.append(_CookieCandidate(cookies=dict(_RUNTIME_OVERRIDE), source="runtime_override"))
        browser_candidate = _browser_session_candidate(
            session_account_id, browser_session_manager=browser_session_manager
        )
        if browser_candidate is not None:
            candidates.append(browser_candidate)
        env_candidate = _env_json_candidate()
        if env_candidate is not None:
            candidates.append(env_candidate)
        candidates.extend(_file_candidates())
        if not candidates:
            auth_session = _build_auth_session(
                cookies={},
                source="none",
                validated=False,
                validation_reason="no_cookies_found",
                validation_category="structural_invalid",
                stale_ok=False,
                browser_account_id=browser_account_id,
                session_account_id=session_account_id,
                caller_context=caller_context,
                cookie_file_path=cookie_file_path,
                storage_state_path=session_paths.storage_state_path if session_paths else None,
                refreshed=False,
                refresh_method=None,
                repaired_from_browser_session=False,
            )
            set_current_instagram_auth_session(auth_session)
            return auth_session

        selected = _select_best_candidate(candidates)
        cookies = dict(selected.cookies)
        repaired_from_browser_session = False
        if browser_candidate is not None and selected.source != "runtime_override":
            cookies, repaired_from_browser_session = _merge_missing_fields(cookies, browser_candidate.cookies)
            if repaired_from_browser_session and selected.source != "browser_session":
                selected = _CookieCandidate(
                    cookies=cookies,
                    source=f"{selected.source}_repaired",
                    path=selected.path,
                    metadata=selected.metadata,
                )

        validated, validation_reason, validation_category, stale_ok = _validate_cookies_via_graphql(
            cookies,
            session_account_id=session_account_id,
            caller_context=validation_context,
            require_validation=require_validation,
        )
        refreshed = False
        refresh_method: str | None = None

        if selected.source == "repo_default_cookie_file" and not _is_local_environment():
            logger.warning("Instagram auth resolver fell through to repo default cookie file in non-local environment")

        if validation_category == "structural_invalid" and browser_candidate is not None:
            repaired_cookies, repaired = _merge_missing_fields(cookies, browser_candidate.cookies)
            if repaired:
                cookies = repaired_cookies
                repaired_from_browser_session = True
                validated, validation_reason, validation_category, stale_ok = _validate_cookies_via_graphql(
                    cookies,
                    session_account_id=session_account_id,
                    caller_context=validation_context,
                    require_validation=require_validation,
                )

        if (
            require_validation
            and not validated
            and validation_category not in {"checkpoint_required", "validation_skipped"}
        ):
            if browser_candidate is not None and selected.source != "browser_session":
                validated_browser, reason_browser, category_browser, stale_ok_browser = _validate_cookies_via_graphql(
                    browser_candidate.cookies,
                    session_account_id=session_account_id,
                    caller_context=validation_context,
                    require_validation=True,
                )
                if validated_browser:
                    cookies = dict(browser_candidate.cookies)
                    selected = browser_candidate
                    validated = validated_browser
                    validation_reason = reason_browser
                    validation_category = category_browser
                    stale_ok = stale_ok_browser

            if not validated and validation_category not in {"checkpoint_required", "unauthorized", "redirect_login"}:
                refreshed_cookies, refresh_method = _refresh_with_credentials(
                    session_account_id=session_account_id,
                    caller_context=validation_context,
                    cookie_file_path=cookie_file_path,
                )
                if refreshed_cookies:
                    old_fingerprint = _cookie_fingerprint(cookies) if cookies else None
                    _cache_evict(old_fingerprint)
                    cookies = dict(refreshed_cookies)
                    refreshed = True
                    validated, validation_reason, validation_category, stale_ok = _validate_cookies_via_graphql(
                        cookies,
                        session_account_id=session_account_id,
                        caller_context=validation_context,
                        require_validation=True,
                    )

            if not validated and validation_category in {"graphql_validation_failed", "unauthorized", "redirect_login"}:
                interactive_cookies, interactive_method = _refresh_interactively(
                    session_account_id=session_account_id,
                    caller_context=validation_context,
                    cookie_file_path=cookie_file_path,
                )
                if interactive_cookies:
                    old_fingerprint = _cookie_fingerprint(cookies) if cookies else None
                    _cache_evict(old_fingerprint)
                    cookies = dict(interactive_cookies)
                    refreshed = True
                    refresh_method = interactive_method
                    validated, validation_reason, validation_category, stale_ok = _validate_cookies_via_graphql(
                        cookies,
                        session_account_id=session_account_id,
                        caller_context=validation_context,
                        require_validation=True,
                    )

        promoted_from_browser = False
        if browser_candidate is not None and cookies and cookies == browser_candidate.cookies:
            promoted_from_browser = _promote_browser_session_to_canonical_file(
                cookies,
                cookie_file_path=cookie_file_path,
                browser_candidate=browser_candidate,
                observed_mtime=observed_cookie_file_mtime,
                caller_context=validation_context,
            )

        auth_session = _build_auth_session(
            cookies=cookies,
            source=selected.source if not promoted_from_browser else "browser_session_promoted",
            validated=validated,
            validation_reason=validation_reason,
            validation_category=validation_category,
            stale_ok=stale_ok,
            browser_account_id=browser_account_id,
            session_account_id=session_account_id,
            caller_context=caller_context,
            cookie_file_path=cookie_file_path,
            storage_state_path=session_paths.storage_state_path if session_paths else None,
            refreshed=refreshed,
            refresh_method=refresh_method,
            repaired_from_browser_session=repaired_from_browser_session,
            metadata={
                "fingerprint": _masked_fingerprint(cookies),
                "browser_session_used": browser_candidate is not None and cookies == browser_candidate.cookies,
                "browser_session_cookie_file": str(browser_candidate.path)
                if browser_candidate and browser_candidate.path
                else None,
            },
        )
        set_current_instagram_auth_session(auth_session)
        return auth_session


def build_authenticated_instagram_scraper(
    *,
    browser_account_id: str | None = None,
    caller_context: str | None = None,
    require_validation: bool = True,
) -> Any:
    from trr_backend.socials.instagram.scraper import InstagramScraper

    auth_session = resolve_instagram_auth_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
        require_validation=require_validation,
    )
    if not auth_session.cookies.get("sessionid"):
        return None
    scraper = InstagramScraper(cookies=auth_session.cookies, browser_account_id=auth_session.session_account_id)
    if hasattr(scraper, "attach_auth_session"):
        scraper.attach_auth_session(auth_session)
    return scraper


def resolve_instagram_comments_auth_session(
    *,
    browser_account_id: str | None,
    caller_context: str | None = None,
    validation_mode: str | None = None,
    browser_session_manager: AccountBrowserSessionManager | None = None,
) -> InstagramAuthSession:
    mode = resolve_instagram_comments_auth_validation_mode(validation_mode)
    auth_session = resolve_instagram_auth_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
        require_validation=mode == "graphql_profile",
        browser_session_manager=browser_session_manager,
    )
    auth_session.metadata.update(
        {
            "comments_auth_validation_mode": mode,
            "comments_profile_graphql_validation": mode == "graphql_profile",
        }
    )
    return auth_session


def auth_session_log_payload(auth_session: InstagramAuthSession) -> dict[str, Any]:
    return {
        "auth_cookie_source": auth_session.source,
        "auth_cookie_validated": auth_session.validated,
        "auth_cookie_validation_reason": auth_session.validation_reason,
        "auth_cookie_validation_category": auth_session.validation_category,
        "auth_cookie_refresh_attempted": auth_session.refreshed,
        "auth_cookie_refresh_method": auth_session.refresh_method,
        "auth_browser_session_used": bool(auth_session.metadata.get("browser_session_used")),
        "auth_cookie_repaired_from_browser_session": auth_session.repaired_from_browser_session,
        "auth_cookie_stale_ok": auth_session.stale_ok,
        "auth_session_account_id": auth_session.session_account_id,
        "auth_session_context": auth_session.caller_context,
        "auth_resolver_version": auth_session.resolver_version,
    }
