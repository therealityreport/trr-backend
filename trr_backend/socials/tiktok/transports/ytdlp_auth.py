"""Cookie/proxy arguments shared by TikTok yt-dlp subprocess callers.

This module consumes supplied cookies and never loads or refreshes a session.
Only the returned temporary path belongs to the caller and may be removed.
"""

from __future__ import annotations

import logging
import os
import tempfile
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from trr_backend.socials.tiktok.constants import (
    ENV_TIKTOK_YTDLP_PROXY_URL,
    ENV_TIKTOK_YTDLP_USE_CANONICAL_COOKIES,
    TRUTHY_ENV_VALUES,
    YTDLP_COOKIE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)


def remove_temporary_cookie_file(path: str | None) -> None:
    if path:
        try:
            os.remove(path)
        except OSError:
            logger.debug("Failed to remove temporary yt-dlp cookie file")


def write_netscape_cookie_file(cookies: Mapping[str, Any] | None) -> str | None:
    rows: list[str] = []
    expires_at = int(time.time()) + YTDLP_COOKIE_TTL_SECONDS
    for raw_name, raw_value in (cookies or {}).items():
        name = str(raw_name or "").strip()
        value = str(raw_value or "").strip()
        if not name or not value or any(ch in name + value for ch in "\t\r\n"):
            continue
        rows.append("\t".join((".tiktok.com", "TRUE", "/", "TRUE", str(expires_at), name, value)))
    if not rows:
        return None
    fd, path = tempfile.mkstemp(prefix="tiktok_ytdlp_cookies_", suffix=".txt")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write("# Netscape HTTP Cookie File\n")
            handle.write("\n".join(rows) + "\n")
    except BaseException:
        remove_temporary_cookie_file(path)
        raise
    return path


def build_ytdlp_auth_args(
    cookies: Mapping[str, Any] | None,
    *,
    cookie_file: str | None,
) -> tuple[list[str], dict[str, Any], str | None]:
    """Return CLI arguments, safe metadata, and an owned temporary cookie path."""
    args: list[str] = []
    temp_path: str | None = None
    cookie_source = "netscape_file" if cookie_file else None
    if (
        not cookie_file
        and cookies
        and (str(os.getenv(ENV_TIKTOK_YTDLP_USE_CANONICAL_COOKIES) or "1").strip().lower() in TRUTHY_ENV_VALUES)
    ):
        temp_path = write_netscape_cookie_file(cookies)
        if temp_path:
            cookie_file = temp_path
            cookie_source = "canonical_cookies"
    if cookie_file:
        args.extend(["--cookies", cookie_file])
    proxy_url = str(os.getenv(ENV_TIKTOK_YTDLP_PROXY_URL) or "").strip() or None
    if proxy_url:
        args.extend(["--proxy", proxy_url])
    # Never fall back to the raw value: malformed proxies can contain secrets.
    try:
        proxy_label = (
            urlsplit(proxy_url if "://" in (proxy_url or "") else f"//{proxy_url}").hostname if proxy_url else None
        )
    except ValueError:
        proxy_label = "invalid-proxy"
    return (
        args,
        {
            "ytdlp_cookie_file_present": bool(cookie_file),
            "ytdlp_cookie_file_used": bool(cookie_file),
            "ytdlp_cookie_source": cookie_source,
            "ytdlp_proxy_enabled": bool(proxy_url),
            "ytdlp_proxy_label": proxy_label,
        },
        temp_path,
    )
