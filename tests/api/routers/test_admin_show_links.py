from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from api.routers.admin_show_links import (
    _cleanup_stale_pending_person_knowledge_links,
    _discover_people_links,
    _discover_season_links,
    _discover_show_links,
    _validated_person_knowledge_url,
)


def test_discover_show_links_uses_default_bravo_snapshot_variant() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.side_effect = [
            {
                "id": show_id,
                "name": "The Real Housewives of Salt Lake City",
                "networks": ["bravo"],
                "wikidata_id": None,
                "external_ids": {},
            },
            {
                "payload": {
                    "normalized": {
                        "news_show": [
                            {
                                "headline": "Cast announcement",
                                "article_url": "https://www.bravotv.com/the-daily-dish/cast-news",
                                "season_number": 6,
                            }
                        ]
                    }
                }
            },
        ]

        links = _discover_show_links(show_id)

    snapshot_call = fetch_one.call_args_list[1]
    assert snapshot_call.args[1] == [show_id, "default"]
    assert any(link.get("link_kind") == "cast_announcement" for link in links)


def test_discover_people_links_adds_bravo_profile_for_housewife_friend_on_bravo_show() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.return_value = {"networks": ["bravo"]}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [{"person_id": person_id}],
                [
                    {
                        "id": person_id,
                        "full_name": "Lisa Barlow",
                        "external_ids": {},
                        "fandom_url": "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                    }
                ],
            ]
            with patch(
                "api.routers.admin_show_links._validated_person_knowledge_url",
                side_effect=lambda url, kind, expected_name=None: url,
            ):
                links = _discover_people_links(show_id)

    assert any(link.get("link_kind") == "bravo_profile" for link in links)
    assert any(link.get("url") == "https://www.bravotv.com/people/lisa-barlow" for link in links)


def test_discover_people_links_skips_missing_wikipedia_and_fandom_pages() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.return_value = {"networks": ["bravo"]}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [{"person_id": person_id}],
                [
                    {
                        "id": person_id,
                        "full_name": "Georgia Gay",
                        "external_ids": {},
                        "fandom_url": "https://real-housewives.fandom.com/wiki/Georgia_Gay",
                    }
                ],
            ]
            with patch("api.routers.admin_show_links._validated_person_knowledge_url", return_value=None):
                links = _discover_people_links(show_id)

    assert not any(link.get("link_kind") == "wikipedia" for link in links)
    assert not any(link.get("link_kind") == "fandom" for link in links)
    assert any(link.get("link_kind") == "bravo_profile" for link in links)


def test_discover_season_links_prefers_wikidata_enwiki_sitelink() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
        fetch_all.return_value = [
            {
                "id": season_id,
                "season_number": 4,
                "external_wikidata_id": "Q122761552",
                "external_ids": {},
            }
        ]
        with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
            fetch_one.return_value = {"name": "The Real Housewives of Salt Lake City"}
            with patch(
                "api.routers.admin_show_links._resolve_wikidata_enwiki_url",
                return_value="https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City_season_4",
            ):
                links = _discover_season_links(show_id)

    season_wiki_links = [link for link in links if link.get("link_kind") == "wikipedia"]
    assert len(season_wiki_links) == 1
    assert season_wiki_links[0]["url"] == "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City_season_4"
    assert season_wiki_links[0]["source"] == "wikidata_sitelink"


def test_validated_person_knowledge_url_rejects_mismatched_wikipedia_page() -> None:
    _validated_person_knowledge_url.cache_clear()
    html = """
    <html>
      <head><title>The Real Housewives of Salt Lake City - Wikipedia</title></head>
      <body><h1 id="firstHeading">The Real Housewives of Salt Lake City</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links.try_fetch_html",
        return_value=(html, "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City", None),
    ):
        resolved = _validated_person_knowledge_url(
            "https://en.wikipedia.org/wiki/Georgia_Gay",
            kind="wikipedia",
            expected_name="Georgia Gay",
        )
    assert resolved is None


def test_validated_person_knowledge_url_rejects_mismatched_fandom_page() -> None:
    _validated_person_knowledge_url.cache_clear()
    html = """
    <html>
      <head><title>John Barlow | Real Housewives Wiki | Fandom</title></head>
      <body><h1 class="page-header__title">John Barlow</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links.try_fetch_html",
        return_value=(html, "https://real-housewives.fandom.com/wiki/John_Barlow", None),
    ):
        resolved = _validated_person_knowledge_url(
            "https://real-housewives.fandom.com/wiki/Henry_Barlow",
            kind="fandom",
            expected_name="Henry Barlow",
        )
    assert resolved is None


def test_validated_person_knowledge_url_accepts_matching_fandom_person_page() -> None:
    _validated_person_knowledge_url.cache_clear()
    html = """
    <html>
      <head><title>Lisa Barlow | Real Housewives Wiki | Fandom</title></head>
      <body><h1 class="page-header__title">Lisa Barlow</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links.try_fetch_html",
        return_value=(html, "https://real-housewives.fandom.com/wiki/Lisa_Barlow", None),
    ):
        resolved = _validated_person_knowledge_url(
            "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
            kind="fandom",
            expected_name="Lisa Barlow",
        )
    assert resolved == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"


def test_cleanup_stale_pending_person_knowledge_links_uses_identity_guard_when_keys_present() -> None:
    with patch("api.routers.admin_show_links.pg.execute_returning") as execute_returning:
        execute_returning.return_value = [{"id": "1"}]
        deleted = _cleanup_stale_pending_person_knowledge_links(
            str(uuid4()),
            {
                "person-1|wikipedia|https://en.wikipedia.org/wiki/lisa_barlow",
                "person-2|fandom|https://real-housewives.fandom.com/wiki/lisa_barlow",
            },
        )

    assert deleted == 1
    sql, params = execute_returning.call_args.args
    assert "NOT ((entity_id::text || '|' || link_kind || '|' || url_key) = ANY(%s::text[]))" in sql
    assert len(params) == 3
