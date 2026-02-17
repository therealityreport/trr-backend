from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from api.routers.admin_show_links import _discover_people_links, _discover_show_links


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

            links = _discover_people_links(show_id)

    assert any(link.get("link_kind") == "bravo_profile" for link in links)
    assert any(link.get("url") == "https://www.bravotv.com/people/lisa-barlow" for link in links)
