"""Facebook posts-only shared catalog orchestration surface."""

from __future__ import annotations

from .catalog import FacebookPostsCatalogDependencies, scrape_shared_facebook_posts

__all__ = [
    "FacebookPostsCatalogDependencies",
    "scrape_shared_facebook_posts",
]
