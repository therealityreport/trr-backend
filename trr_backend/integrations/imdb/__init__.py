"""
IMDb integration clients.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trr_backend.integrations.imdb.episodic_client import (
        HttpImdbEpisodicClient,
        ImdbClientError,
        ImdbEpisodicClient,
        ImdbEpisodicCredits,
        ImdbNameId,
        ImdbTitleId,
    )

__all__ = [
    "HttpImdbEpisodicClient",
    "ImdbClientError",
    "ImdbEpisodicClient",
    "ImdbEpisodicCredits",
    "ImdbNameId",
    "ImdbTitleId",
]


def __getattr__(name: str):
    if name in __all__:
        from trr_backend.integrations.imdb import episodic_client

        return getattr(episodic_client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
