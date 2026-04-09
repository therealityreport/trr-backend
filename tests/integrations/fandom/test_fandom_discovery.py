from __future__ import annotations

from unittest.mock import patch

from trr_backend.integrations.fandom_discovery import (
    crawl_allpages_directory_entries,
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


def test_crawl_allpages_directory_entries_follows_next_page_same_domain_only() -> None:
    page_one_html = """
    <html>
      <body>
        <div class="mw-allpages-body">
          <ul>
            <li><a href="/wiki/The_Real_Housewives_of_Salt_Lake_City">The Real Housewives of Salt Lake City</a></li>
          </ul>
        </div>
        <div class="mw-allpages-nav">
          <a
            href="/wiki/Special:AllPages?from=The_Real_Housewives_of_Salt_Lake_City_-_Season_1"
          >Next page</a>
        </div>
      </body>
    </html>
    """
    page_two_html = """
    <html>
      <body>
        <div class="mw-allpages-body">
          <ul>
            <li>
              <a href="/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_1">
                The Real Housewives of Salt Lake City - Season 1
              </a>
            </li>
            <li><a href="/wiki/Lisa_Barlow">Lisa Barlow</a></li>
          </ul>
        </div>
      </body>
    </html>
    """

    with patch(
        "trr_backend.integrations.fandom_discovery.fetch_html",
        side_effect=[
            (200, page_one_html, None),
            (200, page_two_html, None),
        ],
    ):
        entries = crawl_allpages_directory_entries(
            "https://real-housewives.fandom.com/wiki/Special:AllPages",
            max_pages=5,
        )

    assert [entry["page_title"] for entry in entries] == [
        "The Real Housewives of Salt Lake City",
        "The Real Housewives of Salt Lake City - Season 1",
        "Lisa Barlow",
    ]
    assert [entry["page_url"] for entry in entries] == [
        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_1",
        "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
    ]


def test_crawl_allpages_directory_entries_rejects_cross_domain_or_looping_next_pages() -> None:
    cross_domain_html = """
    <html>
      <body>
        <div class="mw-allpages-body">
          <ul>
            <li><a href="/wiki/Lisa_Barlow">Lisa Barlow</a></li>
          </ul>
        </div>
        <div class="mw-allpages-nav">
          <a href="https://realitytv-girl.fandom.com/wiki/Special:AllPages?from=Lisa_Barlow">Next page</a>
        </div>
      </body>
    </html>
    """
    looping_html = """
    <html>
      <body>
        <div class="mw-allpages-body">
          <ul>
            <li><a href="/wiki/Meredith_Marks">Meredith Marks</a></li>
          </ul>
        </div>
        <div class="mw-allpages-nav">
          <a href="/wiki/Special:AllPages">Next page</a>
        </div>
      </body>
    </html>
    """

    with patch(
        "trr_backend.integrations.fandom_discovery.fetch_html",
        side_effect=[
            (200, cross_domain_html, None),
            (200, looping_html, None),
        ],
    ):
        cross_domain_entries = crawl_allpages_directory_entries(
            "https://real-housewives.fandom.com/wiki/Special:AllPages",
            max_pages=5,
        )
        looping_entries = crawl_allpages_directory_entries(
            "https://real-housewives.fandom.com/wiki/Special:AllPages",
            max_pages=5,
        )

    assert cross_domain_entries == [
        {
            "page_title": "Lisa Barlow",
            "page_slug": "Lisa_Barlow",
            "page_url": "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
        }
    ]
    assert looping_entries == [
        {
            "page_title": "Meredith Marks",
            "page_slug": "Meredith_Marks",
            "page_url": "https://real-housewives.fandom.com/wiki/Meredith_Marks",
        }
    ]


def test_crawl_allpages_directory_entries_falls_back_to_api_when_html_is_blocked() -> None:
    challenge_html = """
    <html>
      <head><title>Just a moment...</title></head>
      <body>Enable JavaScript and cookies to continue</body>
    </html>
    """
    api_payload = """
    {
      "continue": {"apcontinue": "Lisa_Barlow", "continue": "-||"},
      "query": {
        "allpages": [
          {"title": "The Real Housewives of Salt Lake City"},
          {"title": "The Real Housewives of Salt Lake City - Season 1"}
        ]
      }
    }
    """
    api_payload_final = """
    {
      "query": {
        "allpages": [
          {"title": "Lisa Barlow"}
        ]
      }
    }
    """

    with patch(
        "trr_backend.integrations.fandom_discovery.fetch_html",
        side_effect=[
            (403, challenge_html, None),
            (200, api_payload, None),
            (200, api_payload_final, None),
        ],
    ):
        entries = crawl_allpages_directory_entries(
            "https://real-housewives.fandom.com/wiki/Special:AllPages",
            max_pages=5,
        )

    assert [entry["page_url"] for entry in entries] == [
        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_1",
        "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
    ]
