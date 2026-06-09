"""Decodo (ex-Smartproxy) proxy usage poll + daily budget threshold alert.

This module polls Decodo's documented traffic-statistics API for the calling
account/sub-user's usage over the current UTC day and, when usage exceeds a
configured daily budget, emits a WARNING-level structured log (shipped to Better
Stack via the root logger) plus an optional Sentry message.

Everything here is OFF by default and fully fail-open:

  * No creds (``DECODO_API_TOKEN`` or ``DECODO_API_USERNAME``/``DECODO_API_PASSWORD``)
    => the poll is a no-op and returns ``{"status": "skipped", ...}``.
  * No budget (``DECODO_DAILY_BUDGET_USD`` / ``DECODO_DAILY_BUDGET_GB``) => usage may
    still be fetched, but no alert can fire.
  * Any network/parse error is swallowed and reported as a non-raising result so the
    daily cron never crashes a worker over observability.

----------------------------------------------------------------------------------
Decodo usage-API assumptions (verified against public docs, 2026-06; FLAGGED):
  * Endpoint:  POST https://api.decodo.com/api/v2/statistics/traffic
  * Auth:      ``Authorization: <DECODO_API_TOKEN>`` header. The docs show the raw
               API key placed directly in the Authorization header. Decodo's Public
               API also accepts HTTP Basic (username:password); when only
               ``DECODO_API_USERNAME``/``DECODO_API_PASSWORD`` are set we send a
               Basic header instead. ``DECODO_API_AUTH_SCHEME`` can force a scheme
               prefix (e.g. "Bearer"/"Basic"/"Token") if the account requires it.
  * Body:      {"startDate": "<UTC start> 00:00:00", "endDate": "<UTC now>",
               "groupBy": "day"} (extra params like proxyType are optional).
  * Response:  {"metadata": {"total_rx_tx": <bytes>, "total_rx": ..., "total_tx": ...,
               "requests": ...}, "data": [...]}. We read combined bytes from
               ``metadata.total_rx_tx`` (falling back to summing the ``data`` rows'
               ``rx_tx_bytes`` / ``rx_bytes+tx_bytes``).
  * COST:      The documented traffic API does NOT return a USD/"spend" field — cost
               is only shown in the dashboard UI. We therefore DERIVE USD from GB used
               times a configurable price-per-GB (``DECODO_USD_PER_GB``, default 0.0
               => USD alerting disabled until you set your plan's rate). This is the
               one place where the exact dollar figure is an assumption rather than an
               API value. ===> TODO: if/when Decodo exposes a spend field on this
               endpoint (or a dedicated billing endpoint), read it directly instead of
               deriving from price-per-GB.
----------------------------------------------------------------------------------
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import UTC, datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

DECODO_TRAFFIC_API_URL = "https://api.decodo.com/api/v2/statistics/traffic"
_BYTES_PER_GB = 1_000_000_000  # Decodo bills on decimal GB (1 GB = 1e9 bytes).


def _env_str(name: str) -> str:
    return str(os.getenv(name) or "").strip()


def _env_float(name: str, default: float) -> float:
    raw = _env_str(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _resolve_auth_header() -> str | None:
    """Build the Authorization header value from env, or None when no creds exist.

    Precedence:
      1. ``DECODO_API_TOKEN`` (raw API key, sent as-is unless a scheme is forced).
      2. ``DECODO_API_USERNAME`` + ``DECODO_API_PASSWORD`` (HTTP Basic).
    ``DECODO_API_AUTH_SCHEME`` optionally prefixes the token form (e.g. "Bearer").
    """
    token = _env_str("DECODO_API_TOKEN")
    scheme = _env_str("DECODO_API_AUTH_SCHEME")
    if token:
        if scheme:
            return f"{scheme} {token}"
        return token
    username = _env_str("DECODO_API_USERNAME")
    password = _env_str("DECODO_API_PASSWORD")
    if username and password:
        encoded = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
        return f"Basic {encoded}"
    return None


def _today_range_utc(now: datetime | None = None) -> tuple[str, str]:
    """Return (startDate, endDate) strings for the current UTC day, Decodo format."""
    current = now or datetime.now(UTC)
    start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start.strftime(fmt), current.strftime(fmt)


def _combined_bytes_from_payload(payload: Any) -> int:
    """Extract combined download+upload bytes from a Decodo traffic response.

    Prefers ``metadata.total_rx_tx``; falls back to summing the per-period ``data``
    rows. Fail-open: returns 0 when the shape is unrecognized.
    """
    if not isinstance(payload, dict):
        return 0
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        for key in ("total_rx_tx", "total_bytes", "total"):
            value = metadata.get(key)
            try:
                if value is not None:
                    return max(0, int(float(value)))
            except (TypeError, ValueError):
                continue
        rx = metadata.get("total_rx")
        tx = metadata.get("total_tx")
        try:
            if rx is not None or tx is not None:
                return max(0, int(float(rx or 0)) + int(float(tx or 0)))
        except (TypeError, ValueError):
            pass
    total = 0
    data = payload.get("data")
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            value = row.get("rx_tx_bytes")
            if value is None:
                try:
                    value = int(float(row.get("rx_bytes") or 0)) + int(float(row.get("tx_bytes") or 0))
                except (TypeError, ValueError):
                    value = 0
            try:
                total += max(0, int(float(value)))
            except (TypeError, ValueError):
                continue
    return total


def _fetch_decodo_usage(
    *,
    auth_header: str,
    timeout_seconds: float = 15.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Call the Decodo traffic-statistics API for today's usage.

    Returns a dict ``{"ok": bool, "bytes": int, "requests": int, "raw": <payload|None>,
    "error": <str|None>}``. Fully fail-open: network/parse failures return
    ``ok=False`` with an ``error`` string rather than raising.

    NOTE: request shape is per Decodo's documented ``POST /api/v2/statistics/traffic``
    endpoint (see module docstring). If your account requires a sub-user-scoped
    variant, set ``DECODO_API_PROXY_TYPE`` and/or extend the body below.
    """
    start_date, end_date = _today_range_utc(now)
    body: dict[str, Any] = {
        "startDate": start_date,
        "endDate": end_date,
        "groupBy": "day",
    }
    proxy_type = _env_str("DECODO_API_PROXY_TYPE")
    if proxy_type:
        body["proxyType"] = proxy_type
    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        response = httpx.post(DECODO_TRAFFIC_API_URL, json=body, headers=headers, timeout=timeout_seconds)
    except Exception as exc:  # noqa: BLE001 - never raise from the poll
        return {"ok": False, "bytes": 0, "requests": 0, "raw": None, "error": f"{type(exc).__name__}: {exc}"}
    if response.status_code >= 400:
        return {
            "ok": False,
            "bytes": 0,
            "requests": 0,
            "raw": None,
            "error": f"http_{response.status_code}",
        }
    try:
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "bytes": 0, "requests": 0, "raw": None, "error": f"non_json_response: {exc}"}
    combined_bytes = _combined_bytes_from_payload(payload)
    requests_count = 0
    if isinstance(payload, dict) and isinstance(payload.get("metadata"), dict):
        try:
            requests_count = int(float(payload["metadata"].get("requests") or 0))
        except (TypeError, ValueError):
            requests_count = 0
    return {"ok": True, "bytes": combined_bytes, "requests": requests_count, "raw": payload, "error": None}


def _emit_sentry_message(message: str, *, extra: dict[str, Any]) -> bool:
    """Send a Sentry warning message if sentry_sdk is importable. Returns True if sent."""
    try:  # pragma: no cover - optional dependency in local/test envs
        import sentry_sdk
    except Exception:  # noqa: BLE001 - dependency may be absent
        return False
    try:
        with sentry_sdk.push_scope() as scope:  # type: ignore[attr-defined]
            for key, value in extra.items():
                scope.set_extra(key, value)
            sentry_sdk.capture_message(message, level="warning")
        return True
    except Exception:  # noqa: BLE001 - Sentry must never break the poll
        return False


def poll_decodo_usage(*, now: datetime | None = None) -> dict[str, Any]:
    """Poll Decodo usage and emit a threshold alert when the daily budget is exceeded.

    OFF by default: returns ``{"status": "skipped", "reason": "no_credentials"}`` when
    no ``DECODO_API_*`` creds are configured. Returns a structured result dict in all
    cases; never raises.
    """
    auth_header = _resolve_auth_header()
    if not auth_header:
        return {"status": "skipped", "reason": "no_credentials", "alert": False}

    budget_usd = _env_float("DECODO_DAILY_BUDGET_USD", 0.0)
    budget_gb = _env_float("DECODO_DAILY_BUDGET_GB", 0.0)
    usd_per_gb = _env_float("DECODO_USD_PER_GB", 0.0)

    usage = _fetch_decodo_usage(auth_header=auth_header, now=now)
    if not usage.get("ok"):
        # Fetch failed: log at WARNING so Better Stack captures the poll outage, but do
        # not treat it as a budget breach.
        logger.warning(
            "[decodo_usage] poll failed: error=%s url=%s",
            usage.get("error"),
            DECODO_TRAFFIC_API_URL,
        )
        return {"status": "error", "reason": usage.get("error"), "alert": False}

    used_bytes = int(usage.get("bytes") or 0)
    used_gb = used_bytes / _BYTES_PER_GB
    used_usd = used_gb * usd_per_gb if usd_per_gb > 0 else 0.0

    breaches: list[str] = []
    if budget_gb > 0 and used_gb >= budget_gb:
        breaches.append(f"GB {used_gb:.3f} >= budget {budget_gb:.3f}")
    if budget_usd > 0 and usd_per_gb > 0 and used_usd >= budget_usd:
        breaches.append(f"USD {used_usd:.2f} >= budget {budget_usd:.2f}")

    result: dict[str, Any] = {
        "status": "ok",
        "used_bytes": used_bytes,
        "used_gb": round(used_gb, 6),
        "used_usd": round(used_usd, 4),
        "requests": int(usage.get("requests") or 0),
        "budget_gb": budget_gb,
        "budget_usd": budget_usd,
        "usd_per_gb": usd_per_gb,
        "alert": bool(breaches),
    }

    if breaches:
        reason = "; ".join(breaches)
        logger.warning(
            "[decodo_usage] DAILY BUDGET EXCEEDED: %s | used_gb=%.3f used_usd=%.2f "
            "budget_gb=%.3f budget_usd=%.2f requests=%d",
            reason,
            used_gb,
            used_usd,
            budget_gb,
            budget_usd,
            result["requests"],
        )
        result["sentry_sent"] = _emit_sentry_message(
            f"Decodo daily proxy budget exceeded: {reason}",
            extra={
                "used_bytes": used_bytes,
                "used_gb": round(used_gb, 6),
                "used_usd": round(used_usd, 4),
                "budget_gb": budget_gb,
                "budget_usd": budget_usd,
                "requests": result["requests"],
            },
        )
    else:
        logger.info(
            "[decodo_usage] within budget: used_gb=%.3f used_usd=%.2f budget_gb=%.3f budget_usd=%.2f",
            used_gb,
            used_usd,
            budget_gb,
            budget_usd,
        )

    return result
