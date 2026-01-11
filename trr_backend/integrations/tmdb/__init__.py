"""
TMDb integration clients.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trr_backend.integrations.tmdb.client import (
        TmdbClientError,
        fetch_list_items,
        fetch_tv_external_ids,
        parse_tmdb_list_id,
    )

__all__ = [
    "TmdbClientError",
    "fetch_list_items",
    "fetch_tv_external_ids",
    "parse_tmdb_list_id",
]


def __getattr__(name: str):
    if name in __all__:
        from trr_backend.integrations.tmdb import client

        return getattr(client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
