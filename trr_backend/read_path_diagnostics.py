from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def read_path_diagnostics_enabled() -> bool:
    raw = str(os.getenv("TRR_SUPABASE_READ_DIAGNOSTICS") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _payload_size_bytes(payload: Any) -> int | None:
    if payload is None:
        return None
    try:
        return len(json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"))
    except Exception:  # pragma: no cover - best effort only
        return None


def log_read_path(
    route: str,
    *,
    latency_ms: float,
    query_count: int | None = None,
    payload: Any = None,
    extra: dict[str, Any] | None = None,
) -> None:
    if not read_path_diagnostics_enabled():
        return

    fields = [f"route={route}", f"latency_ms={latency_ms:.1f}"]
    if query_count is not None:
        fields.append(f"query_count={int(query_count)}")

    payload_bytes = _payload_size_bytes(payload)
    if payload_bytes is not None:
        fields.append(f"payload_bytes={payload_bytes}")

    if extra:
        for key in sorted(extra):
            value = extra[key]
            if value is None:
                continue
            fields.append(f"{key}={value}")

    logger.info("[supabase-read] %s", " ".join(fields))
