"""Shared Modal API canary helpers."""

from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_CANARY_ATTEMPTS = 3
DEFAULT_CANARY_TIMEOUT_SECONDS = 20


def health_url(api_web_url: str) -> str:
    base = str(api_web_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("Modal readiness did not return api_web_url for cold-start canary.")
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError(f"Invalid URL scheme for cold-start canary: {base}")
    return f"{base}/health"


def run_api_cold_start_canary(
    api_web_url: str,
    *,
    attempts: int = DEFAULT_CANARY_ATTEMPTS,
    timeout_seconds: int = DEFAULT_CANARY_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    url = health_url(api_web_url)
    last_error = ""
    for attempt in range(1, max(1, attempts) + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "trr-modal-deploy-canary/1"})
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                body = response.read(4096).decode("utf-8", errors="replace")
                status = int(getattr(response, "status", 0) or response.getcode())
            if 200 <= status < 300:
                return {"ok": True, "url": url, "status": status, "attempt": attempt, "body": body}
            last_error = f"HTTP {status}: {body[:200]}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = str(exc)
    raise RuntimeError(f"Modal API cold-start canary failed for {url}: {last_error}")


def skipped_api_canary(reason: str = "not_requested") -> dict[str, Any]:
    return {"ok": None, "ran": False, "reason": reason}
