"""YouTube posts-only shared catalog orchestration surface."""

from __future__ import annotations

from .catalog import YouTubePostsCatalogDependencies, scrape_shared_youtube_posts

__all__ = [
    "YouTubePostsCatalogDependencies",
    "scrape_shared_youtube_posts",
]
