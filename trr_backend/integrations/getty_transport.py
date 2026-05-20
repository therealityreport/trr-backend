from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse

import requests


@dataclass(slots=True)
class GettyProxyConfig:
    browser_proxy: str | dict[str, str] | None
    http_proxy_url: str | None
    proxy_fingerprint: str
    provider: str


_REMOTE_FAILURE_REASONS = {
    "challenge_page",
    "pagination_rewrite",
    "proxy_auth_failed",
    "proxy_tunnel_failed",
    "session_truncated",
    "zero_results_block_indicators",
}


def _explicit_proxy_urls_from_env() -> list[str]:
    raw = str(os.getenv("TRR_GETTY_PROXY_URLS") or "").strip()
    if not raw:
        return []
    return [value.strip() for value in raw.replace("\n", ",").split(",") if value.strip()]


def _decodo_env() -> tuple[str, str, str] | None:
    username = str(os.getenv("TRR_GETTY_PROXY_USERNAME") or os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("TRR_GETTY_PROXY_PASSWORD") or os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("TRR_GETTY_PROXY_GATEWAY") or os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return username, password, gateway


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _positive_int_env(name: str, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _decodo_session_username(username: str) -> tuple[str, str]:
    normalized = str(username or "").strip()
    if not normalized:
        return "", "unconfigured"
    if "-session-" in normalized:
        return normalized, "sticky_preconfigured"
    if not _env_truthy("TRR_GETTY_USE_STICKY_PROXY", True):
        return normalized, "rotating"
    ttl_minutes = max(
        1,
        min(
            1440,
            (_positive_int_env("TRR_GETTY_PROXY_SESSION_TTL_SECONDS", 600, minimum=60, maximum=86_400) + 59) // 60,
        ),
    )
    session_id = str(os.getenv("TRR_GETTY_PROXY_SESSION_ID") or "getty-remote").strip().lower() or "getty-remote"
    safe_session_id = "".join(ch for ch in session_id if ch.isalnum())[:32] or "gettyremote"
    return f"{normalized}-session-{safe_session_id}-sessionduration-{ttl_minutes}", "sticky"


def select_getty_proxy() -> GettyProxyConfig | None:
    explicit_urls = _explicit_proxy_urls_from_env()
    if explicit_urls:
        first_url = explicit_urls[0]
        parsed = urlparse(first_url)
        return GettyProxyConfig(
            browser_proxy=first_url,
            http_proxy_url=first_url,
            proxy_fingerprint=f"{parsed.hostname or 'unknown'}:{parsed.port or 0}:explicit",
            provider="explicit",
        )

    provider = str(os.getenv("TRR_GETTY_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"", "decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds is not None:
            username, password, gateway = creds
            session_username, session_mode = _decodo_session_username(username)
            return GettyProxyConfig(
                browser_proxy={
                    "server": f"http://{gateway}",
                    "username": session_username,
                    "password": password,
                },
                http_proxy_url=f"http://{quote(session_username, safe='')}:{quote(password, safe='')}@{gateway}",
                proxy_fingerprint=f"{gateway}:decodo:{session_mode}",
                provider="decodo",
            )
    return None


def build_remote_getty_session() -> tuple[requests.Session | None, dict[str, Any]]:
    proxy = select_getty_proxy()
    if proxy is None or not proxy.http_proxy_url:
        return None, {
            "getty_transport_mode": "decodo_remote",
            "getty_proxy_fingerprint": "none",
            "getty_runtime_probe_status": "disabled",
            "getty_runtime_probe_reason": "proxy_unconfigured",
            "getty_fallback_invoked": False,
            "getty_primary_failure_reason": "proxy_unconfigured",
            "auth_mode": "decodo_remote",
            "auth_warning": "Getty remote transport is unconfigured.",
            "session_validated": False,
            "session_truncated": False,
        }
    session = requests.Session()
    session.proxies.update({"http": proxy.http_proxy_url, "https": proxy.http_proxy_url})
    return session, {
        "getty_transport_mode": "decodo_remote",
        "getty_proxy_fingerprint": proxy.proxy_fingerprint,
        "getty_runtime_probe_status": "not_run",
        "getty_runtime_probe_reason": None,
        "getty_fallback_invoked": False,
        "getty_primary_failure_reason": None,
        "auth_mode": "decodo_remote",
        "auth_warning": None,
        "session_validated": False,
        "session_truncated": False,
    }


def classify_getty_transport_failure(
    query_summary: dict[str, Any] | None,
    *,
    query_assets: list[dict[str, Any]] | None = None,
) -> str | None:
    summary = query_summary or {}
    termination_reason = str(summary.get("termination_reason") or "").strip().lower()
    page_classification = str(summary.get("page_classification") or "").strip().lower()
    request_exception_class = str(summary.get("request_exception_class") or "").strip().lower()
    request_exception_message = str(summary.get("request_exception_message") or "").strip().lower()
    request_failure_text = " ".join(
        value for value in (termination_reason, request_exception_class, request_exception_message) if value
    )
    page_debug = list(summary.get("page_debug") or [])
    has_block_indicator = page_classification == "challenge_page" or any(
        str(item.get("page_classification") or "").strip().lower() == "challenge_page"
        for item in page_debug
        if isinstance(item, dict)
    )

    if (
        "407 proxy authentication required" in request_failure_text
        or "proxy authentication required" in request_failure_text
    ):
        return "proxy_auth_failed"
    if "proxyerror" in request_failure_text or "tunnel connection failed" in request_failure_text:
        return "proxy_tunnel_failed"
    if termination_reason == "challenge_page" or has_block_indicator:
        return "challenge_page"
    if termination_reason == "pagination_rewrite" or bool(summary.get("pagination_rewrite_detected")):
        return "pagination_rewrite"
    if termination_reason == "session_truncated":
        return "session_truncated"

    fetched_assets = len(query_assets or [])
    fetched_candidates_total = int(summary.get("fetched_candidates_total") or 0)
    site_image_total = summary.get("site_image_total")
    if (
        fetched_assets <= 0
        and fetched_candidates_total <= 0
        and (site_image_total in {None, 0})
        and termination_reason not in {"natural_exhaustion", "duplicate_page", "limit_reached"}
    ):
        return "zero_results_block_indicators"
    return None


def is_getty_remote_failure_fallback_reason(reason: str | None) -> bool:
    return str(reason or "").strip().lower() in _REMOTE_FAILURE_REASONS
