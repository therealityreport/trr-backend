"""Bounded admin read for an exact show slug."""

from __future__ import annotations

from typing import Any

from trr_backend.db import pg


def get_show_by_exact_slug(slug: str) -> tuple[dict[str, Any] | None, int]:
    """Return the single show whose stored slug matches after lowercasing."""
    normalized_slug = str(slug or "").strip().lower()
    row = pg.fetch_one(
        """
        SELECT
          core.shows.id::text AS id,
          core.shows.name,
          core.shows.slug
        FROM core.shows
        WHERE core.shows.slug IS NOT NULL
          AND btrim(core.shows.slug) <> ''
          AND lower(btrim(core.shows.slug)) = %s
        LIMIT 1
        """,
        [normalized_slug],
    )
    return (dict(row) if row is not None else None), 1
