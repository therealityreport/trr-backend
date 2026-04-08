from __future__ import annotations

from unittest.mock import patch

from trr_backend.integrations.fandom_discovery import (
    discover_fandom_candidate_pages,
    list_allpages_titles,
    parse_allpages_html_page,
)


def test_list_allpages_titles_uses_api_when_available() -> None:
    payload = '{"query":{"allpages":[{"title":"Lisa Barlow"},{"title":"Season 1"}]}}'
    with patch("trr_backend.integrations.fandom_discovery.fetch_html", return_value=(200, payload, None)):
        titles = list_allpages_titles("real-housewives.fandom.com", max_pages=1)
    assert titles == ["Lisa Barlow", "Season 1"]


def test_list_allpages_titles_falls_back_to_html() -> None:
    html = """
    <html>
      <body>
        <div class="mw-allpages-body">
          <ul><li><a href="/wiki/Lisa_Barlow">Lisa Barlow</a></li></ul>
        </div>
      </body>
    </html>
    """
    with patch(
        "trr_backend.integrations.fandom_discovery.fetch_html",
        side_effect=[(500, None, "boom"), (200, html, None)],
    ):
        titles = list_allpages_titles("real-housewives.fandom.com", max_pages=1)
    assert titles == ["Lisa Barlow"]


def test_parse_allpages_html_page_scopes_titles_and_next_page_to_allpages_dom() -> None:
    html = """
    <html>
      <body>
        <a href="/wiki/Footer_Noise">Footer Noise</a>
        <div class="mw-allpages-body">
          <ul>
            <li><a href="/wiki/Lisa_Barlow/Gallery">Lisa Barlow/Gallery</a></li>
            <li><a href="/wiki/Lisa_Barlow">Lisa Barlow</a></li>
            <li><a href="/wiki/Special:AllPages">Special:AllPages</a></li>
          </ul>
        </div>
        <a href="/wiki/Special:AllPages?from=Wrong_Link">Wrong next page</a>
        <div class="mw-allpages-nav">
          <a href="/wiki/Special:AllPages?from=Meredith_Marks" title="Special:AllPages">
            Next page (Meredith Marks)
          </a>
        </div>
      </body>
    </html>
    """

    titles, next_page_url = parse_allpages_html_page(
        html,
        current_url="https://real-housewives.fandom.com/wiki/Special:AllPages?from=Lisa&to=&namespace=0",
    )

    assert titles == ["Lisa Barlow/Gallery", "Lisa Barlow"]
    assert next_page_url == "https://real-housewives.fandom.com/wiki/Special:AllPages?from=Meredith_Marks"


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
