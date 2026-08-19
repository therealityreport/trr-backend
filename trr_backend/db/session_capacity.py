"""Small, dependency-neutral helpers for probing database session capacity."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

logger = logging.getLogger(__name__)


def probe_fresh_session_capacity(
    *,
    requested_sessions: int,
    max_probe_sessions: int,
    resolve_candidates: Callable[[], tuple[Mapping[str, Any], ...]],
    connect: Callable[[str, Mapping[str, Any]], Any],
    assert_read_only: Callable[[Any], None],
    sslmode_for_url: Callable[[str], str | None],
    error_message: Callable[[Exception], str],
    unavailable_reason: Callable[[str], str],
    default_connect_timeout_seconds: int,
) -> dict[str, Any]:
    """Reserve fresh Supavisor session slots without initializing a named pool.

    The callbacks keep this leaf independent from the pool module while allowing
    its caller to preserve the existing runtime policy and test seams.
    """

    safe_requested = max(0, min(int(requested_sessions), max(0, int(max_probe_sessions))))
    candidates = resolve_candidates()
    candidate = candidates[0] if candidates else {}
    target = {
        "source": candidate.get("source"),
        "host_class": candidate.get("host_class"),
        "connection_class": candidate.get("connection_class"),
        "port": candidate.get("port"),
    }
    if safe_requested == 0:
        return {
            "available": True,
            "blocked": False,
            "reason": "no_session_slots_requested",
            "requested_sessions": 0,
            "reserved_sessions": 0,
            "target": target,
            "error": None,
        }
    url = str(candidate.get("url") or "").strip()
    if not url:
        return {
            "available": False,
            "blocked": True,
            "reason": "database_configuration",
            "requested_sessions": safe_requested,
            "reserved_sessions": 0,
            "target": target,
            "error": "session_database_url_missing",
        }

    connections: list[Any] = []
    try:
        for _index in range(safe_requested):
            connect_kwargs: dict[str, Any] = {
                "dsn": url,
                "application_name": "trr-backend:session-capacity-probe",
                "connect_timeout": min(5, default_connect_timeout_seconds),
            }
            sslmode = sslmode_for_url(url)
            if sslmode:
                connect_kwargs["sslmode"] = sslmode
            conn = connect(url, connect_kwargs)
            connections.append(conn)
            conn.autocommit = True
            assert_read_only(conn)
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
        return {
            "available": True,
            "blocked": False,
            "reason": "fresh_session_reservation_succeeded",
            "requested_sessions": safe_requested,
            "reserved_sessions": len(connections),
            "target": target,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 - normalized without leaking the DSN
        reason = unavailable_reason(error_message(exc))
        return {
            "available": False,
            "blocked": True,
            "reason": reason,
            "requested_sessions": safe_requested,
            "reserved_sessions": len(connections),
            "target": target,
            "error": type(exc).__name__,
        }
    finally:
        for conn in connections:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                logger.debug("fresh session capacity probe close failed", exc_info=True)
