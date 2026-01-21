"""
Repository layer for DB access patterns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trr_backend.repositories.shows import (  # pragma: no cover - type hints only
        ShowRepositoryError,
        find_show_by_imdb_id,
        find_show_by_tmdb_id,
        insert_show,
        update_show,
    )

__all__ = [
    "ShowRepositoryError",
    "find_show_by_imdb_id",
    "find_show_by_tmdb_id",
    "insert_show",
    "update_show",
]


def __getattr__(name: str):  # noqa: ANN001
    if name in __all__:
        from trr_backend.repositories import shows as _shows

        return getattr(_shows, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
