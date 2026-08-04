"""Runtime context bridge for the current Instagram auth session."""

from __future__ import annotations

import contextvars
from typing import Any

_CURRENT_AUTH_SESSION: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "instagram_current_auth_session",
    default=None,
)


def set_current_instagram_auth_session(auth_session: Any | None) -> None:
    """Store the current auth session without importing the resolver facade."""

    _CURRENT_AUTH_SESSION.set(auth_session)


def get_current_instagram_auth_session() -> Any | None:
    """Return the auth session associated with the current context."""

    return _CURRENT_AUTH_SESSION.get()
