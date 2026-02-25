from __future__ import annotations

from unittest.mock import patch

from trr_backend.integrations.fandom_discovery import discover_fandom_candidate_pages, list_allpages_titles


def test_list_allpages_titles_uses_api_when_available() -> None:
    payload = '{"query":{"allpages":[{"title":"Lisa Barlow"},{"title":"Season 1"}]}}'
    with patch("trr_backend.integrations.fandom_discovery.fetch_html", return_value=(200, payload, None)):
        titles = list_allpages_titles("real-housewives.fandom.com", max_pages=1)
    assert titles == ["Lisa Barlow", "Season 1"]


def test_list_allpages_titles_uses_api_continuation() -> None:
    payload1 = '{"query":{"allpages":[{"title":"Lisa Barlow"}]},"continue":{"apcontinue":"Season_1"}}'
    payload2 = '{"query":{"allpages":[{"title":"Season 1"}]}}'
    with patch(
        "trr_backend.integrations.fandom_discovery.fetch_html",
        side_effect=[(200, payload1, None), (200, payload2, None)],
    ):
        titles = list_allpages_titles("real-housewives.fandom.com", max_pages=3)
    assert titles == ["Lisa Barlow", "Season 1"]


def test_list_allpages_titles_falls_back_to_html() -> None:
    html_page_1 = """
    <html>
      <body>
        <div class="mw-allpages-body">
          <ul><li><a href="/wiki/Lisa_Barlow">Lisa Barlow</a></li></ul>
        </div>
        <a href="/wiki/Special:AllPages?from=Season_1">Next page</a>
      </body>
    </html>
    """
    html_page_2 = """
    <html>
      <body>
        <div class="mw-allpages-body">
          <ul><li><a href="/wiki/Season_1">Season 1</a></li></ul>
        </div>
      </body>
    </html>
    """
    with patch(
        "trr_backend.integrations.fandom_discovery.fetch_html",
        side_effect=[(500, None, "boom"), (200, html_page_1, None), (200, html_page_2, None)],
    ):
        titles = list_allpages_titles("real-housewives.fandom.com", max_pages=2)
    assert titles == ["Lisa Barlow", "Season 1"]


def test_discover_candidates_includes_manual_and_search_sources() -> None:
    with patch(
        "trr_backend.integrations.fandom_discovery.search_allowlisted_fandom_wikis",
        return_value=["https://real-housewives.fandom.com/wiki/Lisa_Barlow"],
    ):
        candidates = discover_fandom_candidate_pages(
            query_name="Lisa Barlow",
            entity_kind="person",
            manual_page_urls=["https://real-housewives.fandom.com/wiki/Lisa_Barlow"],
            community_domains=("real-housewives.fandom.com",),
            include_allpages_scan=False,
            max_candidates=5,
        )
    assert candidates
    assert candidates[0].url == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"


def test_discover_candidates_filters_non_manual_low_score() -> None:
    manual_url = "https://real-housewives.fandom.com/wiki/Unknown_Page"
    low_score_search_url = "https://real-housewives.fandom.com/wiki/Totally_Unrelated_Topic"
    with patch(
        "trr_backend.integrations.fandom_discovery.search_allowlisted_fandom_wikis",
        return_value=[low_score_search_url],
    ):
        with patch(
            "trr_backend.integrations.fandom_discovery.build_fandom_wiki_url_from_name",
            return_value=None,
        ):
            candidates = discover_fandom_candidate_pages(
                query_name="Lisa Barlow",
                entity_kind="person",
                manual_page_urls=[manual_url],
                community_domains=("real-housewives.fandom.com",),
                include_allpages_scan=False,
                max_candidates=5,
            )

    assert candidates
    assert any(candidate.url == manual_url for candidate in candidates)
    assert not any(candidate.url == low_score_search_url for candidate in candidates)
