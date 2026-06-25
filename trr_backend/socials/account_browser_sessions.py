"""Shared account-scoped browser session helpers for social scraping."""

from __future__ import annotations

import logging
import os
import re
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trr_backend.socials.browser_cookie_refresh import (
    cookie_payload,
    ensure_private_file_mode,
    launch_browser,
    write_cookie_file,
    write_private_json_file,
)

logger = logging.getLogger(__name__)

_SESSION_LOCKS: dict[str, threading.RLock] = {}
_SESSION_LOCKS_GUARD = threading.Lock()
_DEFAULT_BROWSER_SESSION_DIR_NAME = "social-browser-sessions"


class BrowserSessionExecutionLockTimeout(TimeoutError):
    """Raised when an account-scoped browser session lock cannot be acquired."""

    def __init__(self, *, platform: str, account_id: str, timeout_seconds: float) -> None:
        self.platform = platform
        self.account_id = account_id
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"Timed out acquiring {platform} browser-session lock for {account_id} "
            f"after {timeout_seconds:.1f}s"
        )


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value or "").strip().lower())
    normalized = normalized.strip("._-")
    return normalized or "default"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _storage_root() -> Path:
    raw = str(os.getenv("SOCIAL_BROWSER_SESSION_DIR") or "").strip()
    if raw:
        root = Path(raw).expanduser()
    else:
        root = _project_root() / "data" / _DEFAULT_BROWSER_SESSION_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


@dataclass(frozen=True)
class BrowserAccountSessionPaths:
    platform: str
    account_id: str
    storage_state_path: Path
    cookie_file_path: Path


@dataclass
class BrowserAccountSessionHandle:
    platform: str
    account_id: str
    browser: Any
    context: Any
    paths: BrowserAccountSessionPaths
    cookie_domains: tuple[str, ...]
    network_policy_recorder: Any | None = None


class AccountBrowserSessionManager:
    """Manage browser state files and locks for one social platform."""

    def __init__(self, *, platform: str, cookie_domains: tuple[str, ...]) -> None:
        self.platform = _slugify(platform)
        self.cookie_domains = tuple(cookie_domains)

    def resolve_account_id(self, account_id: str | None = None, *, fallback_account_id: str | None = None) -> str:
        return _slugify(account_id or fallback_account_id or self.platform)

    def session_paths(
        self,
        account_id: str | None = None,
        *,
        fallback_account_id: str | None = None,
    ) -> BrowserAccountSessionPaths:
        resolved_account_id = self.resolve_account_id(account_id, fallback_account_id=fallback_account_id)
        platform_dir = _storage_root() / self.platform
        platform_dir.mkdir(parents=True, exist_ok=True)
        return BrowserAccountSessionPaths(
            platform=self.platform,
            account_id=resolved_account_id,
            storage_state_path=platform_dir / f"{resolved_account_id}.storage-state.json",
            cookie_file_path=platform_dir / f"{resolved_account_id}.cookies.json",
        )

    def _lock_key(self, account_id: str) -> str:
        return f"{self.platform}:{account_id}"

    def _lock_for(self, account_id: str) -> threading.RLock:
        lock_key = self._lock_key(account_id)
        with _SESSION_LOCKS_GUARD:
            lock = _SESSION_LOCKS.get(lock_key)
            if lock is None:
                lock = threading.RLock()
                _SESSION_LOCKS[lock_key] = lock
            return lock

    @contextmanager
    def execution_lock(
        self,
        account_id: str | None = None,
        *,
        fallback_account_id: str | None = None,
        timeout_seconds: float | None = None,
    ) -> Iterator[BrowserAccountSessionPaths]:
        paths = self.session_paths(account_id, fallback_account_id=fallback_account_id)
        lock = self._lock_for(paths.account_id)
        if timeout_seconds is None:
            with lock:
                yield paths
            return

        bounded_timeout = max(0.0, float(timeout_seconds))
        acquired = lock.acquire(timeout=bounded_timeout)
        if not acquired:
            raise BrowserSessionExecutionLockTimeout(
                platform=self.platform,
                account_id=paths.account_id,
                timeout_seconds=bounded_timeout,
            )
        try:
            yield paths
        finally:
            lock.release()

    def reset_account_context(
        self,
        account_id: str | None = None,
        *,
        fallback_account_id: str | None = None,
    ) -> BrowserAccountSessionPaths:
        paths = self.session_paths(account_id, fallback_account_id=fallback_account_id)
        with self._lock_for(paths.account_id):
            for target in (paths.storage_state_path, paths.cookie_file_path):
                try:
                    target.unlink()
                except FileNotFoundError:
                    continue
        return paths

    def import_bootstrapped_session(
        self,
        account_id: str | None,
        cookies_or_storage_state: Mapping[str, Any],
        *,
        fallback_account_id: str | None = None,
    ) -> BrowserAccountSessionPaths:
        paths = self.session_paths(account_id, fallback_account_id=fallback_account_id)
        payload = dict(cookies_or_storage_state or {})
        cookies_payload: dict[str, str]
        if isinstance(payload.get("cookies"), list):
            storage_state = {
                "cookies": list(payload.get("cookies") or []),
                "origins": list(payload.get("origins") or []),
            }
            cookies_payload = cookie_payload(storage_state["cookies"], domains=self.cookie_domains)
        else:
            cookies_payload = {
                str(name): str(value)
                for name, value in payload.items()
                if str(name or "").strip() and str(value or "").strip()
            }
            storage_state = {
                "cookies": [
                    {
                        "name": name,
                        "value": value,
                        "domain": self.cookie_domains[0] if self.cookie_domains else "",
                        "path": "/",
                        "secure": True,
                    }
                    for name, value in cookies_payload.items()
                ],
                "origins": [],
            }
        with self._lock_for(paths.account_id):
            write_private_json_file(paths.storage_state_path, storage_state)
            write_cookie_file(paths.cookie_file_path, cookies_payload)
        return paths

    def bootstrap_auth_with_notte(
        self,
        account_id: str | None,
        platform: str | None = None,
    ) -> BrowserAccountSessionPaths:
        del platform
        self.session_paths(account_id)
        try:
            __import__("notte")
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Notte integration is not installed or configured for this worker") from exc
        raise RuntimeError(
            "Notte auth bootstrap is not configured in TRR yet; "
            "import an external storage state via import_bootstrapped_session()."
        )

    @contextmanager
    def account_context(
        self,
        *,
        playwright: Any,
        account_id: str | None = None,
        fallback_account_id: str | None = None,
        headless: bool,
        viewport: Mapping[str, int] | None = None,
        user_agent: str | None = None,
        seed_cookies: Mapping[str, str] | None = None,
    ) -> Iterator[BrowserAccountSessionHandle]:
        paths = self.session_paths(account_id, fallback_account_id=fallback_account_id)
        context_kwargs: dict[str, Any] = {}
        if viewport is not None:
            context_kwargs["viewport"] = dict(viewport)
        if user_agent:
            context_kwargs["user_agent"] = str(user_agent)
        if paths.storage_state_path.exists():
            ensure_private_file_mode(paths.storage_state_path)
            context_kwargs["storage_state"] = str(paths.storage_state_path)

        with self._lock_for(paths.account_id):
            browser = launch_browser(playwright, headless=headless)
            context = browser.new_context(**context_kwargs)
            network_policy_recorder = None
            if self.platform == "instagram":
                try:
                    from trr_backend.socials.instagram.network_policy import install_sync_context_network_policy

                    network_policy_recorder = install_sync_context_network_policy(context)
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "Failed installing browser network policy for %s/%s",
                        self.platform,
                        paths.account_id,
                        exc_info=True,
                    )
            try:
                if seed_cookies:
                    cookies = [
                        {
                            "name": str(name),
                            "value": str(value),
                            "domain": self.cookie_domains[0] if self.cookie_domains else "",
                            "path": "/",
                            "secure": True,
                        }
                        for name, value in seed_cookies.items()
                        if str(name or "").strip() and str(value or "").strip()
                    ]
                    if cookies:
                        try:
                            context.add_cookies(cookies)
                        except Exception:  # noqa: BLE001
                            logger.debug(
                                "Failed seeding browser cookies for %s/%s",
                                self.platform,
                                paths.account_id,
                                exc_info=True,
                            )
                yield BrowserAccountSessionHandle(
                    platform=self.platform,
                    account_id=paths.account_id,
                    browser=browser,
                    context=context,
                    paths=paths,
                    cookie_domains=self.cookie_domains,
                    network_policy_recorder=network_policy_recorder,
                )
            finally:
                try:
                    try:
                        storage_state = context.storage_state()
                    except TypeError:
                        context.storage_state(path=str(paths.storage_state_path))
                        ensure_private_file_mode(paths.storage_state_path)
                    else:
                        write_private_json_file(paths.storage_state_path, storage_state)
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "Failed writing storage state for %s/%s",
                        self.platform,
                        paths.account_id,
                        exc_info=True,
                    )
                try:
                    cookies_payload = cookie_payload(context.cookies(), domains=self.cookie_domains)
                    if cookies_payload:
                        write_cookie_file(paths.cookie_file_path, cookies_payload)
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "Failed writing cookie payload for %s/%s",
                        self.platform,
                        paths.account_id,
                        exc_info=True,
                    )
                try:
                    context.close()
                finally:
                    browser.close()
