"""Database helpers for TRR backend scripts/services."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trr_backend.db.admin import (  # pragma: no cover - type hints only
        call_rpc_with_cache_reload_hint,
        create_supabase_admin_client,
        is_timeout_error,
    )

__all__ = [
    "call_rpc_with_cache_reload_hint",
    "create_supabase_admin_client",
    "is_timeout_error",
]


def __getattr__(name: str):  # noqa: ANN001
    if name in __all__:
        from trr_backend.db import admin as _admin

        return getattr(_admin, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
