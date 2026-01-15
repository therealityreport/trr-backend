"""Tests for IMDb GraphQL operations (cast selection, normalization, etc.)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from trr_backend.integrations.imdb.graphql_operations import (
    fetch_title_credits_paginated_v2,
    select_show_cast_from_graphql,
)


def test_select_show_cast_filters_by_photo_and_episode_count() -> None:
    """Test cast selection filters by photo presence for low-episode cast."""
    credits = [
        # High episode count (> 6) - always included regardless of photo
        {"node": {"name": {"id": "nm0001", "primaryImage": {"url": "..."}}, "episodeCredits": {"total": 10}}},
        {"node": {"name": {"id": "nm0002"}, "episodeCredits": {"total": 8}}},  # No photo but high episodes
        # Low episode count (<= 6) - only included if has photo
        {"node": {"name": {"id": "nm0003", "primaryImage": {"url": "..."}}, "episodeCredits": {"total": 5}}},  # Has photo
        {"node": {"name": {"id": "nm0004"}, "episodeCredits": {"total": 4}}},  # No photo - EXCLUDED
        {"node": {"name": {"id": "nm0005", "primaryImage": {"url": "..."}}, "episodeCredits": {"total": 2}}},  # Has photo
        {"node": {"name": {"id": "nm0006"}, "episodeCredits": {"total": 1}}},  # No photo - EXCLUDED
        {"node": {"name": {"id": "nm0007"}, "episodeCredits": {"total": None}}},  # No data - EXCLUDED
    ]

    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes_without_photo=6, max_members=500)

    # Should include: nm0001 (10 eps), nm0002 (8 eps), nm0003 (5 eps + photo), nm0005 (2 eps + photo)
    # Should exclude: nm0004 (4 eps, no photo), nm0006 (1 ep, no photo), nm0007 (no data)
    assert len(filtered) == 4
    assert filtered[0]["node"]["name"]["id"] == "nm0001"  # 10 episodes (highest)
    assert filtered[1]["node"]["name"]["id"] == "nm0002"  # 8 episodes
    assert filtered[2]["node"]["name"]["id"] == "nm0003"  # 5 episodes + photo
    assert filtered[3]["node"]["name"]["id"] == "nm0005"  # 2 episodes + photo
    assert is_partial is False


def test_select_show_cast_high_episode_count_no_photo_included() -> None:
    """Test cast with high episode count are always included even without photo."""
    credits = [
        {"node": {"name": {"id": "nm0001"}, "episodeCredits": {"total": 20}}},  # No photo but high episodes
        {"node": {"name": {"id": "nm0002"}, "episodeCredits": {"total": 7}}},  # No photo, above threshold
        {"node": {"name": {"id": "nm0003"}, "episodeCredits": {"total": 5}}},  # No photo, below threshold - EXCLUDED
    ]

    filtered, _ = select_show_cast_from_graphql(credits, min_episodes_without_photo=6, max_members=500)

    assert len(filtered) == 2
    assert filtered[0]["node"]["name"]["id"] == "nm0001"
    assert filtered[1]["node"]["name"]["id"] == "nm0002"


def test_select_show_cast_low_episode_count_with_photo_included() -> None:
    """Test cast with low episode count are included if they have primaryImage."""
    credits = [
        {"node": {"name": {"id": "nm0001", "primaryImage": {"url": "..."}}, "episodeCredits": {"total": 3}}},  # Has photo
        {"node": {"name": {"id": "nm0002", "primaryImage": {"url": "..."}}, "episodeCredits": {"total": 1}}},  # Has photo
        {"node": {"name": {"id": "nm0003"}, "episodeCredits": {"total": 3}}},  # No photo - EXCLUDED
        {"node": {"name": {"id": "nm0004"}, "episodeCredits": {"total": 1}}},  # No photo - EXCLUDED
    ]

    filtered, _ = select_show_cast_from_graphql(credits, min_episodes_without_photo=6, max_members=500)

    assert len(filtered) == 2
    assert filtered[0]["node"]["name"]["id"] == "nm0001"  # 3 episodes + photo
    assert filtered[1]["node"]["name"]["id"] == "nm0002"  # 1 episode + photo


def test_select_show_cast_sorts_by_episode_count_desc() -> None:
    """Test cast selection sorts by episode count descending."""
    credits = [
        {"node": {"name": {"id": "nm0001", "primaryImage": {"url": "..."}}, "episodeCredits": {"total": 5}}},
        {"node": {"name": {"id": "nm0002"}, "episodeCredits": {"total": 15}}},  # Should be first (high episodes)
        {"node": {"name": {"id": "nm0003", "primaryImage": {"url": "..."}}, "episodeCredits": {"total": 10}}},  # Should be second
    ]

    filtered, _ = select_show_cast_from_graphql(credits, min_episodes_without_photo=6, max_members=500)

    assert filtered[0]["node"]["name"]["id"] == "nm0002"  # 15 episodes
    assert filtered[1]["node"]["name"]["id"] == "nm0003"  # 10 episodes
    assert filtered[2]["node"]["name"]["id"] == "nm0001"  # 5 episodes + photo


def test_select_show_cast_caps_at_max_members() -> None:
    """Test cast selection caps results at max_members limit."""
    # Create 150 credits all with high episode counts (all qualify)
    credits = [
        {"node": {"name": {"id": f"nm{i:04d}"}, "episodeCredits": {"total": 100 - i}}}
        for i in range(150)
    ]

    # All 150 have episode counts from 100 down to -49
    # Those with > 6 episodes qualify: 100 down to 7 = 94 items
    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes_without_photo=6, max_members=100)

    # 94 items qualify, capped at 100, so we get all 94
    assert len(filtered) == 94
    assert is_partial is False  # Not capped because 94 < 100

    # Test with lower max to actually trigger cap
    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes_without_photo=6, max_members=50)
    assert len(filtered) == 50
    assert is_partial is True


def test_select_show_cast_not_partial_when_under_cap() -> None:
    """Test is_partial=False when result count is under max_members."""
    credits = [
        {"node": {"name": {"id": f"nm{i:04d}"}, "episodeCredits": {"total": 10}}}
        for i in range(50)
    ]

    filtered, is_partial = select_show_cast_from_graphql(credits, min_episodes_without_photo=6, max_members=500)

    assert len(filtered) == 50
    assert is_partial is False


def test_fetch_title_credits_uses_env_defaults() -> None:
    """Test fetch_title_credits_paginated_v2 respects environment variable defaults."""
    mock_client = MagicMock()
    mock_client.paginate_edges.return_value = [
        {"node": {"name": {"id": "nm0001"}}},
    ]

    with patch.dict(
        "os.environ",
        {
            "IMDB_GRAPHQL_PAGE_SIZE": "250",
            "IMDB_GRAPHQL_LOCALE": "en-US",
        },
    ):
        with patch("trr_backend.integrations.imdb.graphql_operations.ImdbGraphQLPersistedClient") as mock_client_cls:
            mock_client_cls.return_value = mock_client

            _edges = fetch_title_credits_paginated_v2("tt1720601", client=mock_client)

    # Verify client.paginate_edges was called with correct variables
    call_args = mock_client.paginate_edges.call_args
    variables = call_args[1]["variables"]

    assert variables["after"] == ""  # Empty string for first page
    assert variables["category"] is not None  # Should have default category
    assert variables["const"] == "tt1720601"
    assert variables["first"] == 250
    assert variables["locale"] == "en-US"
    assert variables["originalTitleText"] is False
    assert variables["tconst"] == "tt1720601"


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
