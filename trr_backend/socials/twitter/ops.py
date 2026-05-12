"""Operator helpers for Twitter/X social scripts."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from trr_backend.socials.twitter.scraper import Tweet, TwitterScrapeConfig

PersistTwitterSearch = Callable[..., dict[str, Any]]


def persist_cli_search(
    tweets: Sequence[Tweet],
    *,
    raw_query: str,
    config: TwitterScrapeConfig,
    retrieval_meta: Mapping[str, Any],
    persist_search: PersistTwitterSearch,
    scrape_query_label: str | None = None,
    requested_via: str = "cli",
) -> dict[str, Any]:
    """Persist CLI search results using the platform-owned option contract."""
    label = str(scrape_query_label or raw_query).strip() or raw_query
    safe_meta = dict(retrieval_meta or {})
    return persist_search(
        list(tweets),
        raw_query=raw_query,
        normalized_search_query=config.build_search_query(),
        scrape_query_label=label,
        window_start_day=config.window_start_day(),
        window_end_day_exclusive=config.window_end_day_exclusive(),
        requested_via=requested_via,
        retrieval_meta=safe_meta,
        complete=bool(safe_meta.get("complete")),
    )
