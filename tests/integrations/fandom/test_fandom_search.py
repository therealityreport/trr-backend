from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from trr_backend.integrations import fandom


def test_load_fandom_community_allowlist_normalizes_and_dedupes(tmp_path: Path) -> None:
    allowlist_file = tmp_path / "allowlist.txt"
    allowlist_file.write_text(
        "\n".join(
            [
                "# comment",
                "real-housewives.fandom.com",
                "https://www.real-housewives.fandom.com",
                "teen-wolf.fandom.com",
                "not-fandom.example.com",
            ]
        ),
        encoding="utf-8",
    )
    fandom._load_fandom_community_allowlist_from_path.cache_clear()
    loaded = fandom.load_fandom_community_allowlist(str(allowlist_file))
    assert loaded == ("real-housewives.fandom.com", "teen-wolf.fandom.com")


def test_search_fandom_community_wiki_uses_rest_result_url() -> None:
    with patch(
        "trr_backend.integrations.fandom.fetch_html",
        return_value=(200, '{"items":[{"url":"https://real-housewives.fandom.com/wiki/Lisa_Barlow"}]}', None),
    ):
        result = fandom.search_fandom_community_wiki(
            "Lisa Barlow",
            community_domain="real-housewives.fandom.com",
        )
    assert result == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"


def test_search_fandom_community_wiki_candidates_paginates_api_search_results() -> None:
    requested_urls: list[str] = []

    def _fetch_html(url: str, timeout: float = 20.0, headers=None):
        requested_urls.append(url)
        if "rest.php" in url:
            return (500, "", None)
        if "api.php" in url and "sroffset=2" not in url:
            return (
                200,
                '{"query":{"search":[{"title":"Lisa Barlow/Gallery"}]},"continue":{"sroffset":2}}',
                None,
            )
        if "api.php" in url and "sroffset=2" in url:
            return (
                200,
                '{"query":{"search":[{"title":"Lisa Barlow"}]}}',
                None,
            )
        return (404, "", None)

    with patch("trr_backend.integrations.fandom.fetch_html", side_effect=_fetch_html):
        results = fandom.search_fandom_community_wiki_candidates(
            "Lisa Barlow",
            community_domain="real-housewives.fandom.com",
            max_results=5,
        )

    assert any("sroffset=2" in url for url in requested_urls)
    assert "https://real-housewives.fandom.com/wiki/Lisa_Barlow" in results


def test_search_allowlisted_fandom_wikis_filters_invalid_domains() -> None:
    with patch(
        "trr_backend.integrations.fandom.search_fandom_community_wiki",
        side_effect=lambda name, community_domain, timeout_seconds=20.0: (
            f"https://{community_domain}/wiki/Lisa_Barlow" if community_domain == "real-housewives.fandom.com" else None
        ),
    ):
        results = fandom.search_allowlisted_fandom_wikis(
            "Lisa Barlow",
            allowlist=(
                "https://www.real-housewives.fandom.com",
                "not-fandom.example.com",
                "teen-wolf.fandom.com",
            ),
            max_results=5,
        )

    assert results == ["https://real-housewives.fandom.com/wiki/Lisa_Barlow"]


def test_is_allowlisted_fandom_domain_checks_loaded_allowlist() -> None:
    allowlist = ("real-housewives.fandom.com",)
    assert fandom.is_allowlisted_fandom_domain(
        "https://real-housewives.fandom.com/wiki/Heather_Gay",
        allowlist=allowlist,
    )
    assert not fandom.is_allowlisted_fandom_domain(
        "https://teen-wolf.fandom.com/wiki/Heather_Gay",
        allowlist=allowlist,
    )


def test_load_fandom_allowlist_prefers_db_source() -> None:
    with patch(
        "trr_backend.integrations.fandom._load_fandom_community_allowlist_from_db",
        return_value=("real-housewives.fandom.com",),
    ):
        domains, source = fandom.load_fandom_community_allowlist_with_source()
    assert domains == ("real-housewives.fandom.com",)
    assert source == "database"


def test_load_fandom_allowlist_falls_back_to_file_source(tmp_path: Path) -> None:
    allowlist_file = tmp_path / "allowlist.txt"
    allowlist_file.write_text("real-housewives.fandom.com\n", encoding="utf-8")
    with patch(
        "trr_backend.integrations.fandom._load_fandom_community_allowlist_from_db",
        return_value=(),
    ):
        domains, source = fandom.load_fandom_community_allowlist_with_source(str(allowlist_file))
    assert domains == ("real-housewives.fandom.com",)
    assert source == "file"
