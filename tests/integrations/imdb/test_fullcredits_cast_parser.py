from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    ImdbFullCreditsError,
    fetch_fullcredits_cast_with_fallback,
    filter_self_cast_rows,
    normalize_api_credits_to_cast_rows,
    parse_fullcredits_cast_html,
)


def test_parse_fullcredits_cast_html_extracts_cast_rows() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "fullcredits_cast_sample.html").read_text(encoding="utf-8")

    rows = parse_fullcredits_cast_html(html, series_id="tt1234567")
    assert len(rows) == 3

    first = rows[0]
    assert first.name_id == "nm0000001"
    assert first.name == "Jane Doe"
    assert first.billing_order == 1
    assert first.raw_role_text == "Self (as Jane)"
    assert first.job_category_id == "amzn1.imdb.concept.name_credit_group.cast123"

    second = rows[1]
    assert second.raw_role_text == "Limo Driver"


def test_filter_self_cast_rows_only_keeps_self_roles() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "fullcredits_cast_sample.html").read_text(encoding="utf-8")

    rows = parse_fullcredits_cast_html(html, series_id="tt1234567")
    self_rows = filter_self_cast_rows(rows)

    assert [row.name_id for row in self_rows] == ["nm0000001", "nm0000003"]
    assert self_rows[1].raw_role_text == "Self (archive footage)"


def test_normalize_api_credits_filters_crew_categories() -> None:
    """Test that crew categories (writer/producer/director) are filtered out."""
    from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits

    mock_credits = MagicMock(spec=ImdbTitleCredits)
    mock_credits.credits = [
        # Cast members (should be included)
        {
            "name": {"id": "nm0000001", "displayName": "Jane Doe"},
            "category": "actor",
            "characters": ["Dr. Smith"],
        },
        {
            "name": {"id": "nm0000002", "displayName": "John Doe"},
            "category": "actress",
            "characters": ["Nurse Lee"],
        },
        {
            "name": {"id": "nm0000003", "displayName": "Bob Self"},
            "category": "self",
            "characters": ["Self"],
        },
        # Crew members (should be filtered out)
        {
            "name": {"id": "nm0000004", "displayName": "Writer Name"},
            "category": "writer",
            "characters": None,
        },
        {
            "name": {"id": "nm0000005", "displayName": "Producer Name"},
            "category": "producer",
            "characters": None,
        },
        {
            "name": {"id": "nm0000006", "displayName": "Director Name"},
            "category": "director",
            "characters": None,
        },
    ]

    rows = normalize_api_credits_to_cast_rows(mock_credits)

    # Only 3 cast members should be included (actor, actress, self)
    assert len(rows) == 3
    assert [row.name_id for row in rows] == ["nm0000001", "nm0000002", "nm0000003"]
    assert rows[0].name == "Jane Doe"
    assert rows[1].name == "John Doe"
    assert rows[2].name == "Bob Self"


def test_normalize_api_credits_sets_job_category_for_self() -> None:
    """Test that job_category_id is set for 'self' roles."""
    from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits
    from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF

    mock_credits = MagicMock(spec=ImdbTitleCredits)
    mock_credits.credits = [
        {
            "name": {"id": "nm0000001", "displayName": "Jane Doe"},
            "category": "self",
            "characters": ["Self"],
        },
        {
            "name": {"id": "nm0000002", "displayName": "John Doe"},
            "category": "actor",
            "characters": ["Self - Guest"],  # "Self" in characters
        },
        {
            "name": {"id": "nm0000003", "displayName": "Alice Actor"},
            "category": "actress",
            "characters": ["Dr. Smith"],  # Not a self role
        },
    ]

    rows = normalize_api_credits_to_cast_rows(mock_credits)

    assert len(rows) == 3
    # First two should have job_category_id set (self category + Self in characters)
    assert rows[0].job_category_id == IMDB_JOB_CATEGORY_SELF
    assert rows[1].job_category_id == IMDB_JOB_CATEGORY_SELF
    # Third should not (regular actor)
    assert rows[2].job_category_id is None


def test_fetch_with_fallback_returns_html_source_on_success() -> None:
    """Test that successful HTML fetch returns 'fullcredits_html' as source_type."""
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "fullcredits_cast_sample.html").read_text(encoding="utf-8")

    with patch("trr_backend.integrations.imdb.fullcredits_cast_parser.HttpImdbFullCreditsClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client.fetch_fullcredits_page.return_value = html
        mock_client_class.return_value = mock_client

        rows, source_type = fetch_fullcredits_cast_with_fallback("tt1234567", verbose=False)

        assert source_type == "fullcredits_html"
        assert len(rows) == 3
        assert rows[0].name_id == "nm0000001"


@pytest.mark.parametrize("status_code", [202, 403, 429])
def test_fetch_with_fallback_triggers_on_blocked_status(status_code: int) -> None:
    """Test that 202/403/429 status codes trigger JSON API fallback."""
    with patch("trr_backend.integrations.imdb.fullcredits_cast_parser.HttpImdbFullCreditsClient") as mock_client_class:
        # Mock HTML fetch to raise blocked error
        mock_client = MagicMock()
        mock_client.fetch_fullcredits_page.side_effect = ImdbFullCreditsError(
            f"Blocked with HTTP {status_code}",
            status_code=status_code,
            is_blocked=True,
        )
        mock_client_class.return_value = mock_client

        # Mock JSON API fallback (patched at import location inside the function)
        with patch("trr_backend.integrations.imdb.credits_client.fetch_title_credits") as mock_api:
            from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits

            mock_credits = MagicMock(spec=ImdbTitleCredits)
            mock_credits.credits = [
                {
                    "name": {"id": "nm0000001", "displayName": "Jane Doe"},
                    "category": "actor",
                    "characters": ["Dr. Smith"],
                }
            ]
            mock_api.return_value = mock_credits

            rows, source_type = fetch_fullcredits_cast_with_fallback("tt1234567", verbose=False)

            # Should use JSON API fallback
            assert source_type == "credits_api_top_billed"
            assert len(rows) == 1
            assert rows[0].name_id == "nm0000001"
            assert rows[0].name == "Jane Doe"


def test_fetch_with_fallback_raises_when_both_fail() -> None:
    """Test that error is raised when all fallback tiers fail."""
    with patch.dict("os.environ", {"IMDB_GRAPHQL_ENABLED": "1"}):
        with patch(
            "trr_backend.integrations.imdb.fullcredits_cast_parser.HttpImdbFullCreditsClient"
        ) as mock_client_class:
            # Mock HTML fetch to raise blocked error
            mock_client = MagicMock()
            mock_client.fetch_fullcredits_page.side_effect = ImdbFullCreditsError(
                "Blocked with HTTP 403",
                status_code=403,
                is_blocked=True,
            )
            mock_client_class.return_value = mock_client

            # Mock GraphQL to also fail
            with patch("trr_backend.integrations.imdb.graphql_operations.fetch_title_credits_paginated_v2") as mock_gql:
                mock_gql.side_effect = Exception("GraphQL error")

                # Mock JSON API to also fail
                with patch("trr_backend.integrations.imdb.credits_client.fetch_title_credits") as mock_api:
                    mock_api.side_effect = Exception("JSON API error")

                    with pytest.raises(ImdbFullCreditsError) as exc_info:
                        fetch_fullcredits_cast_with_fallback("tt1234567", verbose=False)

                    assert "All fallback tiers failed" in str(exc_info.value)
                    assert exc_info.value.is_blocked is True
                    assert exc_info.value.status_code == 403
