"""Integration tests for GraphQL fallback in fetch_fullcredits_cast_with_fallback."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    ImdbFullCreditsError,
    fetch_fullcredits_cast_with_fallback,
    normalize_graphql_credits_to_cast_rows,
)


def test_fallback_uses_html_first_by_default() -> None:
    """Test that HTML is tried first when IMDB_CAST_PRIMARY_SOURCE=html (default)."""
    with patch.dict("os.environ", {"IMDB_CAST_PRIMARY_SOURCE": "html", "IMDB_GRAPHQL_ENABLED": "1"}):
        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_html_fetch") as mock_html:
            mock_html.return_value = ([MagicMock()], "fullcredits_html")

            rows, source_type = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=False)

    assert source_type == "fullcredits_html"
    mock_html.assert_called_once()


def test_fallback_uses_graphql_when_html_blocked() -> None:
    """Test that GraphQL is tried when HTML returns 403 blocked status."""
    with patch.dict("os.environ", {"IMDB_CAST_PRIMARY_SOURCE": "html", "IMDB_GRAPHQL_ENABLED": "1"}):
        # HTML fails with blocked status
        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_html_fetch") as mock_html:
            mock_html.side_effect = ImdbFullCreditsError("Blocked", status_code=403, is_blocked=True)

            # GraphQL succeeds
            with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_graphql_fetch") as mock_graphql:
                mock_graphql.return_value = ([MagicMock()], "credits_graphql_paginated")

                rows, source_type = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=False)

    assert source_type == "credits_graphql_paginated"
    mock_html.assert_called_once()
    mock_graphql.assert_called_once()


def test_fallback_uses_json_api_when_html_and_graphql_fail() -> None:
    """Test that JSON API is used as last resort when both HTML and GraphQL fail."""
    with patch.dict("os.environ", {"IMDB_CAST_PRIMARY_SOURCE": "html", "IMDB_GRAPHQL_ENABLED": "1"}):
        # HTML fails
        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_html_fetch") as mock_html:
            mock_html.side_effect = ImdbFullCreditsError("Blocked", status_code=403, is_blocked=True)

            # GraphQL fails
            with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_graphql_fetch") as mock_graphql:
                mock_graphql.side_effect = Exception("GraphQL error")

                # JSON API succeeds
                with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_json_api_fetch") as mock_json:
                    mock_json.return_value = ([MagicMock()], "credits_api_top_billed")

                    rows, source_type = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=False)

    assert source_type == "credits_api_top_billed"
    mock_html.assert_called_once()
    mock_graphql.assert_called_once()
    mock_json.assert_called_once()


def test_fallback_graphql_first_when_primary_source_graphql() -> None:
    """Test that GraphQL is tried first when IMDB_CAST_PRIMARY_SOURCE=graphql."""
    with patch.dict("os.environ", {"IMDB_CAST_PRIMARY_SOURCE": "graphql", "IMDB_GRAPHQL_ENABLED": "1"}):
        # GraphQL succeeds immediately
        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_graphql_fetch") as mock_graphql:
            mock_graphql.return_value = ([MagicMock()], "credits_graphql_paginated")

            # HTML should not be called
            with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_html_fetch") as mock_html:
                rows, source_type = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=False)

    assert source_type == "credits_graphql_paginated"
    mock_graphql.assert_called_once()
    mock_html.assert_not_called()


def test_fallback_skips_graphql_when_disabled() -> None:
    """Test that GraphQL tier is skipped when IMDB_GRAPHQL_ENABLED=0."""
    with patch.dict("os.environ", {"IMDB_CAST_PRIMARY_SOURCE": "html", "IMDB_GRAPHQL_ENABLED": "0"}):
        # HTML fails
        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_html_fetch") as mock_html:
            mock_html.side_effect = ImdbFullCreditsError("Blocked", status_code=403, is_blocked=True)

            # GraphQL should be skipped entirely
            with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_graphql_fetch") as mock_graphql:
                # JSON API succeeds
                with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_json_api_fetch") as mock_json:
                    mock_json.return_value = ([MagicMock()], "credits_api_top_billed")

                    rows, source_type = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=False)

    assert source_type == "credits_api_top_billed"
    mock_html.assert_called_once()
    mock_graphql.assert_not_called()
    mock_json.assert_called_once()


def test_fallback_raises_when_all_tiers_fail() -> None:
    """Test that error is raised when all fallback tiers are exhausted."""
    with patch.dict("os.environ", {"IMDB_CAST_PRIMARY_SOURCE": "html", "IMDB_GRAPHQL_ENABLED": "1"}):
        # All tiers fail
        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_html_fetch") as mock_html:
            mock_html.side_effect = ImdbFullCreditsError("HTML failed", status_code=403, is_blocked=True)

            with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_graphql_fetch") as mock_graphql:
                mock_graphql.side_effect = Exception("GraphQL failed")

                with patch("trr_backend.integrations.imdb.fullcredits_cast_parser._try_json_api_fetch") as mock_json:
                    mock_json.side_effect = Exception("JSON API failed")

                    with pytest.raises(ImdbFullCreditsError) as exc_info:
                        fetch_fullcredits_cast_with_fallback("tt1720601", verbose=False)

    assert "All fallback tiers failed" in str(exc_info.value)


def test_normalize_graphql_credits_extracts_cast_rows() -> None:
    """Test normalization of GraphQL edges to CastRow format."""
    edges = [
        {
            "node": {
                "name": {"id": "nm0000001", "nameText": {"text": "Jane Doe"}},
                "characters": [{"name": "Self"}],
                "category": {"id": "amzn1.imdb.concept.name_credit_group.self"},
            }
        },
        {
            "node": {
                "name": {"id": "nm0000002", "nameText": {"text": "John Smith"}},
                "characters": [{"name": "Limo Driver"}],
                "category": {"id": "amzn1.imdb.concept.name_credit_group.cast"},
            }
        },
    ]

    rows = normalize_graphql_credits_to_cast_rows(edges)

    assert len(rows) == 2
    assert rows[0].name_id == "nm0000001"
    assert rows[0].name == "Jane Doe"
    assert rows[0].raw_role_text == "Self"
    assert rows[0].job_category_id is not None  # Should have job_category_id for "Self" role

    assert rows[1].name_id == "nm0000002"
    assert rows[1].name == "John Smith"
    assert rows[1].raw_role_text == "Limo Driver"


def test_normalize_graphql_credits_handles_missing_fields() -> None:
    """Test normalization handles missing or malformed data gracefully."""
    edges = [
        {
            "node": {
                "name": {"id": "nm0000001", "nameText": {"text": "Valid Name"}},
                "characters": [{"name": "Role A"}],
            }
        },
        {
            "node": {
                # Missing name.nameText
                "name": {"id": "nm0000002"},
                "characters": [{"name": "Role B"}],
            }
        },
        {
            "node": {
                # Missing name.id
                "name": {"nameText": {"text": "No ID"}},
                "characters": [{"name": "Role C"}],
            }
        },
    ]

    rows = normalize_graphql_credits_to_cast_rows(edges)

    # Only first edge should be converted (has both id and name)
    assert len(rows) == 1
    assert rows[0].name_id == "nm0000001"
    assert rows[0].name == "Valid Name"


def test_graphql_fetch_returns_partial_source_type_when_capped() -> None:
    """Test _try_graphql_fetch returns partial source type when cast selection caps results."""
    # Mock GraphQL client to return many credits (all with episodeCredits.total >= 3)
    # Create 150 credits with episodeCredits.total from 150 down to 1
    # With min_episodes=3, credits 3-150 qualify (148 items)
    # With max_members=100, will be capped to 100
    # IMPORTANT: Need full GraphQL structure for normalization
    many_edges = [
        {
            "node": {
                "name": {"id": f"nm{i:04d}", "nameText": {"text": f"Actor {i}"}},
                "episodeCredits": {"total": 150 - i},
                "characters": [{"name": "Role"}],
            }
        }
        for i in range(150)
    ]

    with patch("trr_backend.integrations.imdb.graphql_operations.fetch_title_credits_paginated_v2") as mock_fetch:
        mock_fetch.return_value = many_edges

        with patch.dict(
            "os.environ",
            {
                "IMDB_SHOW_CAST_MIN_EPISODES": "3",
                "IMDB_SHOW_CAST_MAX_MEMBERS": "100",
            },
        ):
            from trr_backend.integrations.imdb.fullcredits_cast_parser import _try_graphql_fetch

            rows, source_type = _try_graphql_fetch("tt1720601", None, verbose=False)

    assert source_type == "credits_graphql_paginated_partial"
    assert len(rows) == 100  # Capped to max_members
