from __future__ import annotations

import contextlib
import fcntl
import os
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

_DEFAULT_CHROME_EXECUTABLE_CANDIDATES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
)
_DEFAULT_PLAYWRIGHT_ARGS = ("--disable-blink-features=AutomationControlled",)


def resolve_chrome_executable() -> str | None:
    for candidate in _DEFAULT_CHROME_EXECUTABLE_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    return None


def playwright_ready() -> bool:
    try:
        import playwright.sync_api  # noqa: F401
    except Exception:
        return False
    return True


def launch_browser(
    playwright: Any,
    *,
    headless: bool,
    proxy_server: str | None = None,
    extra_args: tuple[str, ...] = (),
) -> Any:
    launch_kwargs: dict[str, Any] = {
        "headless": bool(headless),
        "args": [*_DEFAULT_PLAYWRIGHT_ARGS, *extra_args],
    }
    proxy = str(proxy_server or "").strip()
    if proxy:
        launch_kwargs["proxy"] = {"server": proxy}
    executable_path = resolve_chrome_executable()
    if executable_path:
        try:
            return playwright.chromium.launch(executable_path=executable_path, **launch_kwargs)
        except Exception:
            pass
    try:
        return playwright.chromium.launch(channel="chrome", **launch_kwargs)
    except Exception:
        return playwright.chromium.launch(**launch_kwargs)


def launch_persistent_context(
    playwright: Any,
    *,
    user_data_dir: str | Path,
    headless: bool,
    proxy_server: str | None = None,
    extra_args: tuple[str, ...] = (),
) -> Any:
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(user_data_dir),
        "headless": bool(headless),
        "args": [*_DEFAULT_PLAYWRIGHT_ARGS, *extra_args],
    }
    proxy = str(proxy_server or "").strip()
    if proxy:
        launch_kwargs["proxy"] = {"server": proxy}
    executable_path = resolve_chrome_executable()
    if executable_path:
        try:
            return playwright.chromium.launch_persistent_context(executable_path=executable_path, **launch_kwargs)
        except Exception:
            pass
    try:
        return playwright.chromium.launch_persistent_context(channel="chrome", **launch_kwargs)
    except Exception:
        return playwright.chromium.launch_persistent_context(**launch_kwargs)


def create_seeded_profile_dir(seed_profile_dir: str | Path, *, prefix: str = "trr-playwright-profile-") -> Path:
    source = Path(seed_profile_dir).expanduser()
    root = Path(tempfile.mkdtemp(prefix=prefix))
    default_dir = root / "Default"
    default_dir.mkdir(parents=True, exist_ok=True)
    for relative in (
        Path("Local State"),
        Path("Default") / "Preferences",
        Path("Default") / "Secure Preferences",
    ):
        src = source / relative
        if not src.exists():
            continue
        dst = root / relative
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(src, dst)
        except Exception:
            continue
    return root


@contextlib.contextmanager
def exclusive_runtime_lock(
    name: str,
    *,
    root: str | Path | None = None,
) -> Iterator[Path]:
    lock_root = Path(root).expanduser() if root is not None else Path(tempfile.gettempdir()) / "trr-browser-locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    lock_path = lock_root / f"{name}.lock"
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"browser_runtime_locked:{name}") from exc
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()))
            handle.flush()
            yield lock_path
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
