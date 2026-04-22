from __future__ import annotations

import hashlib
import math
import re
import uuid

_SESSION_PARAM_RE = re.compile(r"(?:^|-)session-[^-]+(?:-|$)")
_SESSION_DURATION_RE = re.compile(r"(?:^|-)sessionduration-\d+(?:-|$)")


def apply_decodo_session_affinity(
    username: str,
    *,
    use_sticky_proxy: bool,
    session_ttl_seconds: int,
    session_id: str | None = None,
) -> tuple[str, str]:
    """Return a Decodo username with optional sticky-session parameters.

    Decodo sticky sessions are encoded in the username itself. If the provided
    username is already session-scoped, preserve it as-is rather than stacking a
    second session suffix on top.
    """
    normalized = str(username or "").strip()
    if not normalized:
        return "", "rotating"

    has_session = bool(_SESSION_PARAM_RE.search(normalized))
    has_duration = bool(_SESSION_DURATION_RE.search(normalized))

    if has_session:
        return normalized, "sticky_preconfigured"
    if not use_sticky_proxy:
        return normalized, "rotating"

    normalized_session_id = str(session_id or "").strip().lower()
    if normalized_session_id:
        sticky_session_id = hashlib.sha256(normalized_session_id.encode("utf-8")).hexdigest()[:16]
    else:
        sticky_session_id = uuid.uuid4().hex[:12]
    ttl_minutes = max(1, min(1440, math.ceil(max(1, int(session_ttl_seconds)) / 60)))
    updated = f"{normalized}-session-{sticky_session_id}"
    if not has_duration:
        updated = f"{updated}-sessionduration-{ttl_minutes}"
    return updated, "sticky"
