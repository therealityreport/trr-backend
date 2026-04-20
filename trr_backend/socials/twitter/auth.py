from __future__ import annotations

from typing import Any


def has_cookie_auth(cookies: dict[str, Any] | None) -> bool:
    payload = cookies or {}
    return bool(str(payload.get("ct0") or "").strip())
