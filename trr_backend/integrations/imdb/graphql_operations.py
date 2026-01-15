"""IMDb GraphQL persisted query operations registry."""

from __future__ import annotations

import os
from typing import Any

from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF
from trr_backend.integrations.imdb.graphql_persisted_client import (
    ImdbGraphQLPersistedClient,
)

# ============================================================================
# Persisted Query Registry
# ============================================================================
# CRITICAL: These hashes are discovered from IMDb's production client.
# If queries fail with "persisted query not found", hashes may have changed.
# Use scripts/discover_imdb_graphql_hashes.py to find updated values.
# ============================================================================

PERSISTED_QUERIES = {
    "TitleCreditPaginationV2": {
        "operation_name": "TitleCreditPaginationV2",
        # Real hash discovered from IMDb production client (2026-01-15)
        # Can be overridden via IMDB_GRAPHQL_HASH_TITLE_CREDIT_PAGINATION_V2 env var
        "sha256_hash": os.getenv(
            "IMDB_GRAPHQL_HASH_TITLE_CREDIT_PAGINATION_V2",
            "c2df29603060d12b6a76c48e2b47ac0ceee80e471f8cd8ee79abd672393e4bd8",
        ),
        "description": "Paginated title credits with filters (category, locale, etc.)",
    },
    # Additional operations can be added here as needed
}


# ============================================================================
# Operation Wrappers
# ============================================================================


def fetch_title_credits_paginated_v2(
    tconst: str,
    *,
    category_id: str = IMDB_JOB_CATEGORY_SELF,
    first: int | None = None,
    max_pages: int | None = None,
    locale: str | None = None,
    client: ImdbGraphQLPersistedClient | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch paginated title credits from IMDb GraphQL API.

    This returns ALL credits for a title (e.g., ~945 for tt1720601),
    which must be filtered by cast selection policy before inserting
    into core.show_cast.

    Args:
        tconst: IMDb title ID (e.g., "tt1720601")
        category_id: Filter by job category (default: IMDB_JOB_CATEGORY_SELF for reality shows)
        first: Page size (default: IMDB_GRAPHQL_PAGE_SIZE env var)
        max_pages: Hard cap on pagination (default: IMDB_GRAPHQL_MAX_PAGES env var)
        locale: Language locale (default: IMDB_GRAPHQL_LOCALE env var)
        client: Optional pre-configured client instance

    Returns:
        List of credit edge nodes (each contains credit details + episodeCount)

    Raises:
        ImdbGraphQLError: If GraphQL request fails

    Example:
        >>> from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF
        >>> credits = fetch_title_credits_paginated_v2("tt1720601", category_id=IMDB_JOB_CATEGORY_SELF)
        >>> len(credits)
        945  # All credits - needs filtering!

    Note:
        Use select_show_cast_from_graphql() to filter results for core.show_cast
    """
    if first is None:
        first = int(os.getenv("IMDB_GRAPHQL_PAGE_SIZE", "250"))

    if locale is None:
        locale = os.getenv("IMDB_GRAPHQL_LOCALE", "en-US")

    # Get operation metadata
    op_meta = PERSISTED_QUERIES["TitleCreditPaginationV2"]

    # Build variables
    # CRITICAL: Use IMDB_JOB_CATEGORY_SELF constant, NOT string literal
    variables = {
        "after": "",  # Cursor (empty string for first page, base64 token for subsequent)
        "category": category_id,  # Use constant from episodic_client
        "const": tconst,  # Required by API
        "first": first,
        "locale": locale,
        "originalTitleText": False,  # Required by API
        "tconst": tconst,  # Duplicate required by API
    }

    # Execute paginated query
    if client is None:
        client = ImdbGraphQLPersistedClient()

    edges = client.paginate_edges(
        operation_name=op_meta["operation_name"],
        sha256_hash=op_meta["sha256_hash"],
        variables=variables,
        edges_path="data.title.creditsV2.edges",
        page_info_path="data.title.creditsV2.pageInfo",
        max_pages=max_pages,
    )

    return edges


def select_show_cast_from_graphql(
    credits: list[dict[str, Any]],
    *,
    min_episodes: int | None = None,
    max_members: int | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """
    Select series cast from GraphQL credits for core.show_cast.

    Filters raw GraphQL credits (e.g., 945 total) to main cast members
    suitable for the show_cast table, using episode count heuristics.

    Strategy:
    1. Filter credits where episodeCount >= min_episodes
    2. Sort by episodeCount descending
    3. Take top max_members

    Args:
        credits: Raw GraphQL credit edges from fetch_title_credits_paginated_v2()
        min_episodes: Min episodes to qualify as series cast (default: IMDB_SHOW_CAST_MIN_EPISODES)
        max_members: Max cast members to prevent pollution (default: IMDB_SHOW_CAST_MAX_MEMBERS)

    Returns:
        Tuple of (filtered_credits, is_partial) where:
        - filtered_credits: Credits suitable for core.show_cast
        - is_partial: True if results were capped by max_members

    Example:
        >>> credits = fetch_title_credits_paginated_v2("tt1720601")
        >>> len(credits)
        945
        >>> main_cast, is_partial = select_show_cast_from_graphql(credits)
        >>> len(main_cast)
        75  # Filtered to main cast only
    """
    if min_episodes is None:
        min_episodes = int(os.getenv("IMDB_SHOW_CAST_MIN_EPISODES", "3"))

    if max_members is None:
        max_members = int(os.getenv("IMDB_SHOW_CAST_MAX_MEMBERS", "100"))

    # Filter by episode count threshold
    qualified = []
    for edge in credits:
        node = edge.get("node", {})
        episode_credits = node.get("episodeCredits", {})
        episode_count = episode_credits.get("total")

        # Skip if no episode count data
        if episode_count is None:
            continue

        # Apply threshold
        if episode_count >= min_episodes:
            qualified.append(edge)

    # Sort by episode count descending
    qualified.sort(
        key=lambda e: e.get("node", {}).get("episodeCredits", {}).get("total", 0),
        reverse=True,
    )

    # Apply max members cap
    is_partial = len(qualified) > max_members
    filtered = qualified[:max_members]

    return filtered, is_partial
