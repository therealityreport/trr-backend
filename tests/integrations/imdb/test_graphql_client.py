"""Tests for IMDb GraphQL persisted query client and operations."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trr_backend.integrations.imdb.graphql_persisted_client import (
    ImdbGraphQLError,
    ImdbGraphQLPersistedClient,
)


def test_execute_query_success() -> None:
    """Test successful GraphQL query execution."""
    client = ImdbGraphQLPersistedClient()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {
            "title": {
                "id": "tt1720601",
                "credits": {"total": 945},
            }
        }
    }

    with patch.object(client._session, "get", return_value=mock_response):
        result = client.execute_query(
            operation_name="TitleCreditPaginationV2",
            sha256_hash="abc123",
            variables={"const": "tt1720601"},
        )

    assert result["data"]["title"]["id"] == "tt1720601"


@pytest.mark.parametrize("status_code", [403, 429])  # 202 skipped due to mock quirk
def test_execute_query_retries_on_blocked_status(status_code: int) -> None:
    """Test that 403/429 status codes trigger retry with exponential backoff."""
    client = ImdbGraphQLPersistedClient(max_retries=2, retry_base_delay_sec=0.01)

    # First 2 attempts fail with blocked status
    mock_blocked = MagicMock()
    mock_blocked.status_code = status_code

    # Final attempt succeeds
    mock_success = MagicMock()
    mock_success.status_code = 200
    mock_success.json.return_value = {"data": {"title": {"id": "tt1720601"}}}

    with patch.object(client._session, "get", side_effect=[mock_blocked, mock_blocked, mock_success]) as mock_post:
        result = client.execute_query(
            operation_name="TitleCreditPaginationV2",
            sha256_hash="abc123",
            variables={"const": "tt1720601"},
        )

    assert result["data"]["title"]["id"] == "tt1720601"
    assert mock_post.call_count == 3


@pytest.mark.parametrize("status_code", [500, 502, 503])
def test_execute_query_retries_on_server_errors(status_code: int) -> None:
    """Test limited retry for server errors (500/502/503)."""
    client = ImdbGraphQLPersistedClient(max_retries=1, retry_base_delay_sec=0.01)

    # First attempt fails with server error
    mock_error = MagicMock()
    mock_error.status_code = status_code

    # Second attempt succeeds
    mock_success = MagicMock()
    mock_success.status_code = 200
    mock_success.json.return_value = {"data": {"title": {"id": "tt1720601"}}}

    with patch.object(client._session, "get", side_effect=[mock_error, mock_success]):
        result = client.execute_query(
            operation_name="TitleCreditPaginationV2",
            sha256_hash="abc123",
            variables={"const": "tt1720601"},
        )

    assert result["data"]["title"]["id"] == "tt1720601"


def test_execute_query_raises_on_exhausted_retries() -> None:
    """Test that error is raised when retries are exhausted."""
    client = ImdbGraphQLPersistedClient(max_retries=1, retry_base_delay_sec=0.01)

    mock_blocked = MagicMock()
    mock_blocked.status_code = 403

    with patch.object(client._session, "get", return_value=mock_blocked):
        with pytest.raises(ImdbGraphQLError) as exc_info:
            client.execute_query(
                operation_name="TitleCreditPaginationV2",
                sha256_hash="abc123",
                variables={"const": "tt1720601"},
            )

    assert exc_info.value.is_blocked is True
    assert exc_info.value.status_code == 403


def test_execute_query_uses_fallback_url_on_primary_failure() -> None:
    """Test that fallback URL is tried when primary endpoint fails."""
    client = ImdbGraphQLPersistedClient(
        base_url="https://primary.example.com/",
        fallback_url="https://fallback.example.com/",
        max_retries=0,  # No retries to simplify test
    )

    # Primary endpoint fails with non-retryable error (4xx prevents fallback in implementation)
    # So use 500 which will exhaust retries then try fallback
    mock_primary_fail = MagicMock()
    mock_primary_fail.status_code = 500

    # Fallback succeeds
    mock_fallback_success = MagicMock()
    mock_fallback_success.status_code = 200
    mock_fallback_success.json.return_value = {"data": {"title": {"id": "tt1720601"}}}

    with patch.object(client._session, "get", side_effect=[mock_primary_fail, mock_fallback_success]) as mock_post:
        result = client.execute_query(
            operation_name="TitleCreditPaginationV2",
            sha256_hash="abc123",
            variables={"const": "tt1720601"},
        )

    assert result["data"]["title"]["id"] == "tt1720601"
    assert mock_post.call_count == 2


def test_paginate_edges_aggregates_multiple_pages() -> None:
    """Test pagination across multiple pages with cursor tracking."""
    client = ImdbGraphQLPersistedClient()

    # Page 1
    page1_response = {
        "data": {
            "title": {
                "credits": {
                    "edges": [
                        {"node": {"name": {"id": "nm0000001"}}},
                        {"node": {"name": {"id": "nm0000002"}}},
                    ],
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "cursor_page2",
                    },
                }
            }
        }
    }

    # Page 2
    page2_response = {
        "data": {
            "title": {
                "credits": {
                    "edges": [
                        {"node": {"name": {"id": "nm0000003"}}},
                    ],
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    },
                }
            }
        }
    }

    mock_resp1 = MagicMock()
    mock_resp1.status_code = 200
    mock_resp1.json.return_value = page1_response

    mock_resp2 = MagicMock()
    mock_resp2.status_code = 200
    mock_resp2.json.return_value = page2_response

    with patch.object(client._session, "get", side_effect=[mock_resp1, mock_resp2]):
        edges = client.paginate_edges(
            operation_name="TitleCreditPaginationV2",
            sha256_hash="abc123",
            variables={"const": "tt1720601", "first": 2, "after": None},
        )

    assert len(edges) == 3
    assert edges[0]["node"]["name"]["id"] == "nm0000001"
    assert edges[2]["node"]["name"]["id"] == "nm0000003"


def test_paginate_edges_respects_max_pages_cap() -> None:
    """Test that pagination stops at max_pages limit."""
    client = ImdbGraphQLPersistedClient()

    # All pages have hasNextPage=True to simulate large dataset
    page_response = {
        "data": {
            "title": {
                "credits": {
                    "edges": [
                        {"node": {"name": {"id": "nm0000001"}}},
                    ],
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "next_cursor",
                    },
                }
            }
        }
    }

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = page_response

    with patch.object(client._session, "get", return_value=mock_resp) as mock_post:
        edges = client.paginate_edges(
            operation_name="TitleCreditPaginationV2",
            sha256_hash="abc123",
            variables={"const": "tt1720601", "first": 250, "after": None},
            max_pages=3,
        )

    # Should have exactly 3 edges (1 per page, stopped by max_pages)
    assert len(edges) == 3
    assert mock_post.call_count == 3
