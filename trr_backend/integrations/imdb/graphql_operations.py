"""IMDb GraphQL persisted query operations registry."""

from __future__ import annotations

import os
from datetime import date, timedelta
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
    "HERO_SUB_NAV_EPISODE": {
        "operation_name": "HERO_SUB_NAV_EPISODE",
        "sha256_hash": os.getenv(
            "IMDB_GRAPHQL_HASH_HERO_SUB_NAV_EPISODE",
            "3f56a4c9c2cca81733ebabbf5e317e3da7f2a4a02069d406bec001ed611c80e4",
        ),
        "description": "Episode summary used by title hero (episode count + most recent).",
    },
    "EpisodesWidget_EpisodesCardContainer": {
        "operation_name": "EpisodesWidget_EpisodesCardContainer",
        "sha256_hash": os.getenv(
            "IMDB_GRAPHQL_HASH_EPISODES_WIDGET_CONTAINER",
            "b25f2b7759a5e94de7a20b90bbb7471eaa6a035eb4696bdc89a15957bd2df171",
        ),
        "description": "Episodes widget container (top rated + most recent).",
    },
    "EpisodesWidget_NextEpisode": {
        "operation_name": "EpisodesWidget_NextEpisode",
        "sha256_hash": os.getenv(
            "IMDB_GRAPHQL_HASH_EPISODES_WIDGET_NEXT_EPISODE",
            "f6f22a817cbb51b8eb0be17df7bfe0b6e32b898db525149547cfe184dbe4faf0",
        ),
        "description": "Episodes widget next episode + most recent.",
    },
    "HERO_WATCH_BOX": {
        "operation_name": "HERO_WATCH_BOX",
        "sha256_hash": os.getenv(
            "IMDB_GRAPHQL_HASH_HERO_WATCH_BOX",
            "45a4e574562d71de9f5c7efe76d5e47a08f06457c6a69749802218db5162f3d6",
        ),
        "description": "Hero watch providers + episode summary payload.",
    },
    "NameMainProjectsInDev": {
        "operation_name": "NameMainProjectsInDev",
        "sha256_hash": os.getenv(
            "IMDB_GRAPHQL_HASH_NAME_MAIN_PROJECTS_IN_DEV",
            "19507cdb3883a63d0e4e2a231ceb4c2835e1e76f68445a4a749a6787ba9f5aeb",
        ),
        "description": "Person projects in development for name pages.",
    },
    "Base_Title_Prompt": {
        "operation_name": "Base_Title_Prompt",
        "sha256_hash": os.getenv(
            "IMDB_GRAPHQL_HASH_BASE_TITLE_PROMPT",
            "a1db725f62c858a762c25e89d9aa6980834fba1054f271ea613f0e98e6762b5b",
        ),
        "description": "Base title prompt payload (plot, production status, watch options).",
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
        client = ImdbGraphQLPersistedClient ()

    edges = client.paginate_edges(
        operation_name=op_meta["operation_name"],
        sha256_hash=op_meta["sha256_hash"],
        variables=variables,
        edges_path="data.title.creditsV2.edges",
        page_info_path="data.title.creditsV2.pageInfo",
        max_pages=max_pages,
    )

    return edges


def _today() -> date:
    return date.today()


def _date_components(value: date) -> tuple[int, int, int]:
    return value.day, value.month, value.year


def fetch_hero_sub_nav_episode(
    tconst: str,
    *,
    locale: str | None = None,
    now_date: date | None = None,
    client: ImdbGraphQLPersistedClient | None = None,
) -> dict[str, Any]:
    if locale is None:
        locale = os.getenv("IMDB_GRAPHQL_LOCALE", "en-US")
    now_date = now_date or _today()
    yesterday = now_date - timedelta(days=1)

    op_meta = PERSISTED_QUERIES["HERO_SUB_NAV_EPISODE"]

    now_day, now_month, now_year = _date_components(now_date)
    y_day, y_month, y_year = _date_components(yesterday)
    variables = {
        "heroNowDateDay": now_day,
        "heroNowDateMonth": now_month,
        "heroNowDateYear": now_year,
        "heroYesterdayDateDay": y_day,
        "heroYesterdayDateMonth": y_month,
        "heroYesterdayDateYear": y_year,
        "locale": locale,
        "titleId": tconst,
    }

    if client is None:
        client = ImdbGraphQLPersistedClient ()

    return client.execute_query(op_meta["operation_name"], op_meta["sha256_hash"], variables)


def fetch_episodes_widget_container(
    tconst: str,
    *,
    locale: str | None = None,
    now_date: date | None = None,
    most_recent_after_days: int = 14,
    client: ImdbGraphQLPersistedClient | None = None,
) -> dict[str, Any]:
    if locale is None:
        locale = os.getenv("IMDB_GRAPHQL_LOCALE", "en-US")
    now_date = now_date or _today()
    most_recent_after = now_date - timedelta(days=max(0, most_recent_after_days))

    op_meta = PERSISTED_QUERIES["EpisodesWidget_EpisodesCardContainer"]

    now_day, now_month, now_year = _date_components(now_date)
    after_day, after_month, after_year = _date_components(most_recent_after)
    variables = {
        "const": tconst,
        "episodesNowDateDay": now_day,
        "episodesNowDateMonth": now_month,
        "episodesNowDateYear": now_year,
        "locale": locale,
        "mostRecentEpisodeAfterDateDay": after_day,
        "mostRecentEpisodeAfterDateMonth": after_month,
        "mostRecentEpisodeAfterDateYear": after_year,
    }

    if client is None:
        client = ImdbGraphQLPersistedClient ()

    return client.execute_query(op_meta["operation_name"], op_meta["sha256_hash"], variables)


def fetch_episodes_widget_next_episode(
    tconst: str,
    *,
    locale: str | None = None,
    now_date: date | None = None,
    most_recent_after_days: int = 14,
    client: ImdbGraphQLPersistedClient | None = None,
) -> dict[str, Any]:
    if locale is None:
        locale = os.getenv("IMDB_GRAPHQL_LOCALE", "en-US")
    now_date = now_date or _today()
    tomorrow = now_date + timedelta(days=1)
    most_recent_after = now_date - timedelta(days=max(0, most_recent_after_days))

    op_meta = PERSISTED_QUERIES["EpisodesWidget_NextEpisode"]

    now_day, now_month, now_year = _date_components(now_date)
    t_day, t_month, t_year = _date_components(tomorrow)
    after_day, after_month, after_year = _date_components(most_recent_after)
    variables = {
        "const": tconst,
        "episodesNowDateDay": now_day,
        "episodesNowDateMonth": now_month,
        "episodesNowDateYear": now_year,
        "episodesTomorrowDateDay": t_day,
        "episodesTomorrowDateMonth": t_month,
        "episodesTomorrowDateYear": t_year,
        "locale": locale,
        "mostRecentEpisodeAfterDateDay": after_day,
        "mostRecentEpisodeAfterDateMonth": after_month,
        "mostRecentEpisodeAfterDateYear": after_year,
    }

    if client is None:
        client = ImdbGraphQLPersistedClient ()

    return client.execute_query(op_meta["operation_name"], op_meta["sha256_hash"], variables)


def fetch_base_title_prompt(
    tconst: str,
    *,
    locale: str | None = None,
    include_user_preferred_services: bool = False,
    include_box_office_data: bool = False,
    is_pro_page: bool = False,
    postal_code: str | None = None,
    country: str = "US",
    client: ImdbGraphQLPersistedClient | None = None,
) -> dict[str, Any]:
    if locale is None:
        locale = os.getenv("IMDB_GRAPHQL_LOCALE", "en-US")
    op_meta = PERSISTED_QUERIES["Base_Title_Prompt"]
    location: dict[str, Any] | None = None
    if postal_code:
        location = {"postalCodeLocation": {"country": country, "postalCode": postal_code}}
    variables = {
        "id": tconst,
        "includeBoxOfficeData": bool(include_box_office_data),
        "includeUserPreferredServices": bool(include_user_preferred_services),
        "isProPage": bool(is_pro_page),
        "locale": locale,
    }
    if location is not None:
        variables["location"] = location

    if client is None:
        client = ImdbGraphQLPersistedClient ()

    return client.execute_query(op_meta["operation_name"], op_meta["sha256_hash"], variables)


def fetch_hero_watch_box(
    tconst: str,
    *,
    locale: str | None = None,
    now_date: date | None = None,
    country: str | None = None,
    postal_code: str | None = None,
    client: ImdbGraphQLPersistedClient | None = None,
) -> dict[str, Any]:
    if locale is None:
        locale = os.getenv("IMDB_GRAPHQL_LOCALE", "en-US")
    now_date = now_date or _today()
    yesterday = now_date - timedelta(days=1)
    country = (country or os.getenv("IMDB_GRAPHQL_COUNTRY", "US")).strip() or "US"
    postal_code = (postal_code or os.getenv("IMDB_GRAPHQL_POSTAL_CODE", "32099")).strip() or "32099"

    op_meta = PERSISTED_QUERIES["HERO_WATCH_BOX"]

    now_day, now_month, now_year = _date_components(now_date)
    y_day, y_month, y_year = _date_components(yesterday)
    variables = {
        "heroNowDateDay": now_day,
        "heroNowDateMonth": now_month,
        "heroNowDateYear": now_year,
        "heroYesterdayDateDay": y_day,
        "heroYesterdayDateMonth": y_month,
        "heroYesterdayDateYear": y_year,
        "id": tconst,
        "includeUserPreferredServices": False,
        "locale": locale,
        "location": {"postalCodeLocation": {"country": country, "postalCode": postal_code}},
    }

    if client is None:
        client = ImdbGraphQLPersistedClient ()

    return client.execute_query(op_meta["operation_name"], op_meta["sha256_hash"], variables)


def fetch_name_main_projects_in_dev(
    nconst: str,
    *,
    first: int = 5,
    locale: str | None = None,
    client: ImdbGraphQLPersistedClient | None = None,
) -> dict[str, Any]:
    if locale is None:
        locale = os.getenv("IMDB_GRAPHQL_LOCALE", "en-US")
    op_meta = PERSISTED_QUERIES["NameMainProjectsInDev"]
    variables = {"first": int(first), "locale": locale, "nconst": nconst}

    if client is None:
        client = ImdbGraphQLPersistedClient ()

    return client.execute_query(op_meta["operation_name"], op_meta["sha256_hash"], variables)


def select_show_cast_from_graphql(
    credits: list[dict[str, Any]],
    *,
    min_episodes_without_photo: int | None = None,
    max_members: int | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """
    Select series cast from GraphQL credits for core.show_cast.

    Filters raw GraphQL credits (e.g., 945 total) to cast members suitable
    for the show_cast table. Primary filtering happens via job category
    (IMDB_JOB_CATEGORY_SELF) which excludes non-cast like archival footage.

    Strategy:
    1. Always include: episodeCount > min_episodes_without_photo (regardless of photo)
    2. Include if: episodeCount <= min_episodes_without_photo AND has primaryImage
    3. Exclude if: episodeCount <= min_episodes_without_photo AND no primaryImage
    4. Sort by episodeCount descending
    5. Apply safety cap at max_members (default: 500)

    This ensures we only store low-episode-count cast if we can visually represent them.

    Args:
        credits: Raw GraphQL credit edges from fetch_title_credits_paginated_v2()
        min_episodes_without_photo: Min episodes required if no photo (default: 6 via env var)
            Cast with > this many episodes are always included.
            Cast with <= this many episodes need primaryImage to be included.
        max_members: Safety cap to prevent extreme cases (default: 500 via IMDB_SHOW_CAST_MAX_MEMBERS)

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
        180  # High-episode cast + low-episode cast with photos
    """
    if min_episodes_without_photo is None:
        min_episodes_without_photo = int(os.getenv("IMDB_SHOW_CAST_MIN_EPISODES_WITHOUT_PHOTO", "6"))

    if max_members is None:
        max_members = int(os.getenv("IMDB_SHOW_CAST_MAX_MEMBERS", "500"))

    # Filter by episode count + photo presence
    qualified = []
    for edge in credits:
        node = edge.get("node", {})
        episode_credits = node.get("episodeCredits", {})
        episode_count = episode_credits.get("total")

        # Skip if no episode count data
        if episode_count is None:
            continue

        # Check if has primary image
        name_dict = node.get("name", {})
        has_image = name_dict.get("primaryImage") is not None

        # Apply filtering logic
        if episode_count > min_episodes_without_photo:
            # Always include if above threshold (regardless of photo)
            qualified.append(edge)
        elif has_image:
            # Include if at/below threshold but has photo
            qualified.append(edge)
        # else: exclude (at/below threshold and no photo)

    # Sort by episode count descending
    qualified.sort(
        key=lambda e: e.get("node", {}).get("episodeCredits", {}).get("total", 0),
        reverse=True,
    )

    # Apply max members cap
    is_partial = len(qualified) > max_members
    filtered = qualified[:max_members]

    return filtered, is_partial
