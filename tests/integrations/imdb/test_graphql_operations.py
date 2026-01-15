"""Tests for IMDb GraphQL operations (cast selection, normalization, etc.)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from trr_backend.integrations.imdb.graphql_operations import (
    fetch_title_credits_paginated_v2,
    select_show_cast_from_graphql,
)


def test_select_show_cast_filters_by_episode_count() -> None:
    """Test cast selection filters by minimum episode count threshold."""
    credits = [
        {"node": {"name": {"id": "nm0001"}, "episodeCount": 10}},  # Included
        {"node": {"name": {"id": "nm0002"}, "episodeCount": 3}},   # Included (at threshold)
        {"node": {"name": {"id": "nm0003"}, "episodeCount": 2}},   # Excluded (below threshold)
        {"node": {"name": {"id": "nm0004"}, "episodeCount": 1}},   # Excluded
        {"node": {"name": {"id": "nm0005"}, "episodeCount": None}},  # Excluded (no data)
    ]

    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes=3, max_members=100)

    assert len(filtered) == 2
    assert filtered[0]["node"]["name"]["id"] == "nm0001"  # Highest episode count first
    assert filtered[1]["node"]["name"]["id"] == "nm0002"
    assert is_partial is False


def test_select_show_cast_sorts_by_episode_count_desc() -> None:
    """Test cast selection sorts by episode count descending."""
    credits = [
        {"node": {"name": {"id": "nm0001"}, "episodeCount": 5}},
        {"node": {"name": {"id": "nm0002"}, "episodeCount": 15}},  # Should be first
        {"node": {"name": {"id": "nm0003"}, "episodeCount": 10}},  # Should be second
    ]

    filtered, _ = select_show_cast_from_graphql(credits, min_episodes=3, max_members=100)

    assert filtered[0]["node"]["name"]["id"] == "nm0002"
    assert filtered[1]["node"]["name"]["id"] == "nm0003"
    assert filtered[2]["node"]["name"]["id"] == "nm0001"


def test_select_show_cast_caps_at_max_members() -> None:
    """Test cast selection caps results at max_members limit."""
    # Create 150 credits all qualifying (episodeCount >= 3)
    # episodeCount from 100 down to 1 (so 3-100 qualifies, that's 98 items)
    credits = [
        {"node": {"name": {"id": f"nm{i:04d}"}, "episodeCount": 100 - i}}
        for i in range(150)
    ]

    # min_episodes=3 will filter to episodeCount >= 3, which is 98 items (episodeCount 3-100)
    # Then capped to 100, but since we only have 98 qualifying, we get 98
    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes=3, max_members=100)

    # 150 credits with episodeCount from 100 down to (100-149) = -49
    # Only those >= 3 qualify: episodeCount 100, 99, 98, ..., 3 = 98 items
    assert len(filtered) == 98
    assert is_partial is False  # Not capped because 98 < 100

    # Test with lower max to actually trigger cap
    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes=3, max_members=50)
    assert len(filtered) == 50
    assert is_partial is True


def test_select_show_cast_not_partial_when_under_cap() -> None:
    """Test is_partial=False when result count is under max_members."""
    credits = [
        {"node": {"name": {"id": f"nm{i:04d}"}, "episodeCount": 10}}
        for i in range(50)
    ]

    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes=3, max_members=100)

    assert len(filtered) == 50
    assert is_partial is False


def test_fetch_title_credits_uses_env_defaults() -> None:
    """Test fetch_title_credits_paginated_v2 respects environment variable defaults."""
    mock_client = MagicMock()
    mock_client.paginate_edges.return_value = [
        {"node": {"name": {"id": "nm0001"}}},
    ]

    with patch.dict("os.environ", {
        "IMDB_GRAPHQL_PAGE_SIZE": "250",
        "IMDB_GRAPHQL_LOCALE": "en-US",
    }):
        with patch("trr_backend.integrations.imdb.graphql_operations.ImdbGraphQLPersistedClient") as mock_client_cls:
            mock_client_cls.return_value = mock_client

            _edges = fetch_title_credits_paginated_v2("tt1720601", client=mock_client)

    # Verify client.paginate_edges was called with correct variables
    call_args = mock_client.paginate_edges.call_args
    variables = call_args[1]["variables"]

    assert variables["const"] == "tt1720601"
    assert variables["tconst"] == "tt1720601"
    assert variables["first"] == 250
    assert variables["locale"] == "en-US"
    assert variables["after"] is None


def test_fetch_title_credits_uses_category_constant() -> None:
    """Test fetch_title_credits_paginated_v2 uses IMDB_JOB_CATEGORY_SELF constant."""
    from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF

    mock_client = MagicMock()
    mock_client.paginate_edges.return_value = []

    _edges = fetch_title_credits_paginated_v2("tt1720601", category_id=IMDB_JOB_CATEGORY_SELF, client=mock_client)

    call_args = mock_client.paginate_edges.call_args
    variables = call_args[1]["variables"]

    # Verify category uses constant, not hardcoded string
    assert variables["category"] == IMDB_JOB_CATEGORY_SELF
    assert isinstance(variables["category"], str)
