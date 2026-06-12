from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    HttpImdbFullCreditsClient,
    ImdbFullCreditsError,
    fetch_fullcredits_cast_with_fallback,
    filter_self_cast_rows,
    normalize_api_credits_to_cast_rows,
    parse_fullcredits_cast_html,
    parse_fullcredits_crew_html,
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
    assert first.episode_count == 31
    assert first.episodes_label == "31 episodes"
    assert first.years_label == "2020–2026"

    second = rows[1]
    assert second.raw_role_text == "Limo Driver"
    assert second.episode_count is None


def test_filter_self_cast_rows_only_keeps_self_roles() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "fullcredits_cast_sample.html").read_text(encoding="utf-8")

    rows = parse_fullcredits_cast_html(html, series_id="tt1234567")
    self_rows = filter_self_cast_rows(rows)

    assert [row.name_id for row in self_rows] == ["nm0000001", "nm0000003"]
    assert self_rows[1].raw_role_text == "Self (archive footage)"


def test_parse_fullcredits_crew_html_extracts_allowlisted_sections() -> None:
    html = """
    <section class="ipc-page-section">
      <h3 class="ipc-title__text"><span id="producers">Producers</span></h3>
      <div data-testid="sub-section-producers">
        <ul>
          <li data-testid="name-credits-list-item">
            <a href="/name/nm0330404/">Lori Gordon</a>
            <div class="name-credits--crew-metadata">
              <a href="/name/nm0330404/">Lori Gordon</a>
              <div><span>executive producer</span></div>
              <div><button>110 episodes</button> • 2020–2026</div>
            </div>
          </li>
        </ul>
      </div>
    </section>
    <section class="ipc-page-section">
      <h3 class="ipc-title__text"><span id="visual-effects">Visual Effects</span></h3>
      <div data-testid="sub-section-visual-effects">
        <ul>
          <li data-testid="name-credits-list-item">
            <a href="/name/nm1234567/">Charlie Co</a>
            <div class="name-credits--crew-metadata">
              <a href="/name/nm1234567/">Charlie Co</a>
              <div><span>graphics &amp; main titles</span></div>
              <div><button>30 episodes</button> • 2024–2026</div>
            </div>
          </li>
        </ul>
      </div>
    </section>
    <section class="ipc-page-section">
      <h3 class="ipc-title__text"><span id="stunts">Stunts</span></h3>
      <div data-testid="sub-section-stunts">
        <ul>
          <li data-testid="name-credits-list-item">
            <a href="/name/nm7654321/">Ignore Me</a>
            <div class="name-credits--crew-metadata">
              <div><span>stunt performer</span></div>
            </div>
          </li>
        </ul>
      </div>
    </section>
    """

    rows = parse_fullcredits_crew_html(html)

    assert len(rows) == 2
    assert rows[0].credit_category == "Producers"
    assert rows[0].name_id == "nm0330404"
    assert rows[0].role == "executive producer"
    assert rows[0].episode_count == 110
    assert rows[0].episodes_label == "110 episodes"
    assert rows[0].years_label == "2020–2026"
    assert rows[1].credit_category == "Visual Effects"
    assert rows[1].name == "Charlie Co"


def test_save_debug_html_supports_symlinked_debug_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_dir = tmp_path / "artifacts" / "debug_html"
    debug_link = tmp_path / "debug_html"
    debug_link.symlink_to(target_dir, target_is_directory=True)
    monkeypatch.chdir(tmp_path)

    client = HttpImdbFullCreditsClient()
    response = MagicMock()
    response.status_code = 403
    response.text = "<html>blocked</html>"

    client._save_debug_html("tt11363282", response)

    assert target_dir.is_dir()
    saved_files = list(target_dir.glob("imdb_fullcredits_tt11363282_*_http403.html"))
    assert len(saved_files) == 1
    assert saved_files[0].read_text(encoding="utf-8") == "<html>blocked</html>"


def test_fetch_fullcredits_page_uses_browser_fallback_on_blocked_response() -> None:
    client = HttpImdbFullCreditsClient()
    response = MagicMock()
    response.status_code = 202
    response.text = "<html>blocked</html>"

    with patch.dict(
        "os.environ",
        {"IMDB_FULLCREDITS_MAX_RETRIES": "0", "IMDB_FULLCREDITS_SCRAPLING_FALLBACK_ENABLED": "0"},
    ):
        with patch.object(client._session, "get", return_value=response):
            with patch(
                "trr_backend.integrations.imdb.fullcredits_cast_parser._fetch_fullcredits_page_via_browser",
                return_value="<html><div class='full-credits-page-container'></div></html>",
            ) as mock_browser_fetch:
                html = client.fetch_fullcredits_page("tt11363282", verbose=False)

    assert "full-credits-page-container" in html
    mock_browser_fetch.assert_called_once()


def test_fetch_fullcredits_page_uses_scrapling_fallback_before_browser_on_blocked_response() -> None:
    client = HttpImdbFullCreditsClient()
    response = MagicMock()
    response.status_code = 202
    response.text = "<html>blocked</html>"

    with patch.dict("os.environ", {"IMDB_FULLCREDITS_MAX_RETRIES": "0"}):
        with patch.object(client._session, "get", return_value=response):
            with patch(
                "trr_backend.integrations.imdb.fullcredits_cast_parser._fetch_fullcredits_page_via_scrapling",
                return_value="<html><div class='full-credits-page-container'></div></html>",
            ) as mock_scrapling_fetch:
                with patch(
                    "trr_backend.integrations.imdb.fullcredits_cast_parser._fetch_fullcredits_page_via_browser",
                    return_value="<html><div>browser fallback</div></html>",
                ) as mock_browser_fetch:
                    html = client.fetch_fullcredits_page("tt11363282", verbose=False)

    assert "full-credits-page-container" in html
    mock_scrapling_fetch.assert_called_once()
    mock_browser_fetch.assert_not_called()


def test_scrapling_response_html_decodes_response_body() -> None:
    from trr_backend.integrations.imdb.fullcredits_cast_parser import _scrapling_response_html

    page = SimpleNamespace(body="<html>credits</html>".encode(), encoding="utf-8")

    assert _scrapling_response_html(page) == "<html>credits</html>"


def test_fetch_fullcredits_page_raises_when_browser_fallback_does_not_recover() -> None:
    client = HttpImdbFullCreditsClient()
    response = MagicMock()
    response.status_code = 403
    response.text = "<html>blocked</html>"

    with patch.dict(
        "os.environ",
        {"IMDB_FULLCREDITS_MAX_RETRIES": "0", "IMDB_FULLCREDITS_SCRAPLING_FALLBACK_ENABLED": "0"},
    ):
        with patch.object(client._session, "get", return_value=response):
            with patch(
                "trr_backend.integrations.imdb.fullcredits_cast_parser._fetch_fullcredits_page_via_browser",
                return_value=None,
            ) as mock_browser_fetch:
                with pytest.raises(ImdbFullCreditsError) as exc_info:
                    client.fetch_fullcredits_page("tt11363282", verbose=False)

    assert exc_info.value.is_blocked is True
    assert exc_info.value.status_code == 403
    mock_browser_fetch.assert_called_once()


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

        # Use primary_source="html" to explicitly test the HTML tier
        rows, source_type, person_images = fetch_fullcredits_cast_with_fallback(
            "tt1234567", verbose=False, primary_source="html"
        )

        assert source_type == "fullcredits_html"
        assert len(rows) == 3
        assert rows[0].name_id == "nm0000001"


@pytest.mark.parametrize("status_code", [202, 403, 429])
def test_fetch_with_fallback_triggers_on_blocked_status(status_code: int) -> None:
    """Test that 202/403/429 status codes trigger fallback chain."""
    with patch.dict("os.environ", {"IMDB_GRAPHQL_ENABLED": "1"}):
        with patch(
            "trr_backend.integrations.imdb.fullcredits_cast_parser.HttpImdbFullCreditsClient"
        ) as mock_client_class:
            # Mock HTML fetch to raise blocked error
            mock_client = MagicMock()
            mock_client.fetch_fullcredits_page.side_effect = ImdbFullCreditsError(
                f"Blocked with HTTP {status_code}",
                status_code=status_code,
                is_blocked=True,
            )
            mock_client_class.return_value = mock_client

            # Mock GraphQL to also fail
            with patch("trr_backend.integrations.imdb.graphql_operations.fetch_title_credits_paginated_v2") as mock_gql:
                mock_gql.side_effect = Exception("GraphQL error")

                # Mock JSON API fallback (last resort)
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

                    rows, source_type, person_images = fetch_fullcredits_cast_with_fallback("tt1234567", verbose=False)

                    # Should use JSON API fallback (last tier)
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
