from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from api.routers.admin_show_links import (
    _cleanup_invalid_person_knowledge_links,
    _discover_people_links,
    _discover_season_links,
    _discover_show_links,
    _validate_person_knowledge_url,
    _validated_person_knowledge_url,
)


def test_discover_show_links_uses_default_bravo_snapshot_variant() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
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
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "fandom"
        and str(link.get("url") or "").startswith("https://real-housewives.fandom.com/wiki/")
        for link in links
    )


def test_discover_show_links_prefers_existing_show_level_fandom_links() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch(
            "api.routers.admin_show_links.pg.fetch_all",
            return_value=[
                {"url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"},
            ],
        ):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Real Housewives of Salt Lake City",
                    "networks": ["bravo"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"payload": {"normalized": {}}},
            ]
            links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 1
    assert fandom_links[0]["url"] == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
    assert fandom_links[0]["source"] == "core.entity_links"


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
                with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                    with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
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

            def _validate(url: str, kind: str, expected_name: str | None = None) -> str | None:
                if kind == "bravo_profile":
                    return url
                return None

            with patch("api.routers.admin_show_links._validated_person_knowledge_url", side_effect=_validate):
                with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                    with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                        links = _discover_people_links(show_id)

    assert not any(link.get("link_kind") == "wikipedia" for link in links)
    assert not any(link.get("link_kind") == "fandom" for link in links)
    assert any(link.get("link_kind") == "bravo_profile" for link in links)


def test_discover_people_links_generates_imdb_tmdb_links_from_person_ids() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"networks": ["bravo"]}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [],
                [
                    {
                        "id": person_id,
                        "full_name": "Heather Gay",
                        "external_ids": {"imdb": "nm1234567"},
                        "fandom_url": "",
                        "cast_tmdb_imdb_id": "nm1234567",
                        "cast_tmdb_tmdb_id": 98765,
                        "cast_tmdb_wikidata_id": "Q123",
                    }
                ],
            ]
            with patch(
                "api.routers.admin_show_links._validated_person_knowledge_url",
                side_effect=lambda url, kind, expected_name=None: url,
            ):
                with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                    with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                        links = _discover_people_links(show_id)

    imdb_links = [link for link in links if link.get("link_kind") == "imdb"]
    tmdb_links = [link for link in links if link.get("link_kind") == "tmdb"]
    assert len(imdb_links) == 1
    assert imdb_links[0]["url"] == "https://www.imdb.com/name/nm1234567/"
    assert imdb_links[0]["source"] == "core.people.external_ids"
    assert imdb_links[0]["status"] == "approved"
    assert imdb_links[0]["link_group"] == "knowledge"
    assert len(tmdb_links) == 1
    assert tmdb_links[0]["url"] == "https://www.themoviedb.org/person/98765"
    assert tmdb_links[0]["source"] == "core.cast_tmdb"
    assert tmdb_links[0]["status"] == "approved"
    assert tmdb_links[0]["link_group"] == "knowledge"


def test_discover_people_links_fandom_fallback_uses_allowlisted_domains_only() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"networks": ["bravo"]}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [],
                [
                    {
                        "id": person_id,
                        "full_name": "Lisa Barlow",
                        "external_ids": {},
                        "fandom_url": "",
                        "cast_tmdb_imdb_id": None,
                        "cast_tmdb_tmdb_id": None,
                        "cast_tmdb_wikidata_id": None,
                    }
                ],
            ]
            with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                with patch(
                    "api.routers.admin_show_links.search_allowlisted_fandom_wikis",
                    return_value=[
                        "https://teen-wolf.fandom.com/wiki/Lisa_Barlow",
                        "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_links._validated_person_knowledge_url",
                        side_effect=lambda url, kind, expected_name=None: (
                            url if kind == "fandom" and "real-housewives.fandom.com" in url else None
                        ),
                    ):
                        links = _discover_people_links(show_id)

    fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 1
    assert fandom_links[0]["url"] == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"


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
    with patch("api.routers.admin_show_links._fetch_wikipedia_page_summary", return_value=(None, True)):
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


def test_validate_person_knowledge_url_rejects_missing_wikipedia_article_from_api() -> None:
    with patch("api.routers.admin_show_links._fetch_wikipedia_page_summary", return_value=(None, False)):
        with patch("api.routers.admin_show_links.try_fetch_html") as try_fetch_html:
            resolved, outcome = _validate_person_knowledge_url(
                "https://en.wikipedia.org/wiki/Whitney_Comstock_Duncan",
                kind="wikipedia",
                expected_name="Whitney Comstock Duncan",
            )

    assert resolved is None
    assert outcome == "invalid"
    try_fetch_html.assert_not_called()


def test_validate_person_knowledge_url_accepts_wikipedia_article_from_api() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_wikipedia_page_summary",
        return_value=(
            {
                "title": "Lisa Barlow",
                "url": "https://en.wikipedia.org/wiki/Lisa_Barlow",
            },
            False,
        ),
    ):
        with patch("api.routers.admin_show_links.try_fetch_html") as try_fetch_html:
            resolved, outcome = _validate_person_knowledge_url(
                "https://en.wikipedia.org/wiki/Lisa_Barlow",
                kind="wikipedia",
                expected_name="Lisa Barlow",
            )

    assert resolved == "https://en.wikipedia.org/wiki/Lisa_Barlow"
    assert outcome == "valid"
    try_fetch_html.assert_not_called()


def test_validate_person_knowledge_url_rejects_wikipedia_owner_mismatch_from_api() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_wikipedia_page_summary",
        return_value=(
            {
                "title": "Heather Gay",
                "url": "https://en.wikipedia.org/wiki/Heather_Gay",
            },
            False,
        ),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "https://en.wikipedia.org/wiki/Heather_Gay",
            kind="wikipedia",
            expected_name="Ashley Gay",
        )

    assert resolved is None
    assert outcome == "invalid"


def test_validate_person_knowledge_url_rejects_imdb_owner_mismatch() -> None:
    html = """
    <html>
      <head><title>Heather Gay - IMDb</title></head>
      <body><h1>Heather Gay</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(200, html, "https://www.imdb.com/name/nm1234567/", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "nm1234567",
            kind="imdb",
            expected_name="Ashley Gay",
        )
    assert resolved is None
    assert outcome == "invalid"


def test_validate_person_knowledge_url_rejects_tmdb_owner_mismatch() -> None:
    html = """
    <html>
      <head><title>Heather Gay - The Movie Database</title></head>
      <body><h1>Heather Gay</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(200, html, "https://www.themoviedb.org/person/123-heather-gay", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "123",
            kind="tmdb",
            expected_name="Ashley Gay",
        )
    assert resolved is None
    assert outcome == "invalid"


def test_validate_person_knowledge_url_rejects_bravo_owner_mismatch() -> None:
    html = """
    <html>
      <head><title>Heather Gay | Bravo TV Official Site</title></head>
      <body><h1>Heather Gay</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(200, html, "https://www.bravotv.com/people/heather-gay", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "https://www.bravotv.com/people/heather-gay",
            kind="bravo_profile",
            expected_name="Ashley Gay",
        )
    assert resolved is None
    assert outcome == "invalid"


def test_validate_person_knowledge_url_rejects_nonexistent_imdb_page() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(404, "<html>Not found</html>", "https://www.imdb.com/name/nm0000000/", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "nm0000000",
            kind="imdb",
            expected_name="Heather Gay",
        )
    assert resolved is None
    assert outcome == "invalid"


def test_validate_person_knowledge_url_accepts_imdb_access_challenge_for_canonical_id() -> None:
    html = """
    <html>
      <head><title>IMDb Security Challenge</title></head>
      <body>JavaScript is disabled. Please enable JavaScript. Reference ID: abc123</body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(202, html, "https://www.imdb.com/name/nm1234567/", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "nm1234567",
            kind="imdb",
            expected_name="Ashley Gay",
        )
    assert resolved == "https://www.imdb.com/name/nm1234567/"
    assert outcome == "valid"


def test_validate_person_knowledge_url_returns_fetch_error_for_tmdb_fetch_failures() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(None, None, None, "network timeout"),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "123",
            kind="tmdb",
            expected_name="Heather Gay",
        )
    assert resolved is None
    assert outcome == "fetch_error"


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


def test_validate_person_knowledge_url_rejects_wikidata_mismatched_person() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_wikidata_summary",
        return_value=(
            {
                "item_id": "Q123",
                "label": "Heather Gay",
                "enwiki_title": "Heather Gay",
                "enwiki_url": "https://en.wikipedia.org/wiki/Heather_Gay",
            },
            False,
        ),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "https://www.wikidata.org/wiki/Q123",
            kind="wikidata",
            expected_name="Ashley Gay",
        )
    assert resolved is None
    assert outcome == "invalid"


def test_validate_person_knowledge_url_rejects_wikidata_without_enwiki() -> None:
    with patch("api.routers.admin_show_links._fetch_wikidata_summary", return_value=(None, False)):
        resolved, outcome = _validate_person_knowledge_url(
            "https://www.wikidata.org/wiki/Q122761552",
            kind="wikidata",
            expected_name="Lisa Barlow",
        )
    assert resolved is None
    assert outcome == "invalid"


def test_cleanup_invalid_person_knowledge_links_deletes_all_statuses_and_non_cast_rows() -> None:
    show_id = str(uuid4())
    cast_person_id = str(uuid4())
    non_cast_person_id = str(uuid4())
    invalid_wiki_id = str(uuid4())
    invalid_imdb_id = str(uuid4())
    invalid_tmdb_id = str(uuid4())
    non_cast_bravo_id = str(uuid4())

    with patch("api.routers.admin_show_links._load_show_cast_names_by_person_id") as cast_lookup:
        cast_lookup.return_value = {cast_person_id: "Lisa Barlow"}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": invalid_wiki_id,
                    "person_id": cast_person_id,
                    "link_kind": "wikipedia",
                    "status": "approved",
                    "url": "https://en.wikipedia.org/wiki/Heather_Gay",
                },
                {
                    "id": invalid_imdb_id,
                    "person_id": cast_person_id,
                    "link_kind": "imdb",
                    "status": "approved",
                    "url": "https://www.imdb.com/name/nm1234567/",
                },
                {
                    "id": invalid_tmdb_id,
                    "person_id": cast_person_id,
                    "link_kind": "tmdb",
                    "status": "pending",
                    "url": "https://www.themoviedb.org/person/12345",
                },
                {
                    "id": non_cast_bravo_id,
                    "person_id": non_cast_person_id,
                    "link_kind": "bravo_profile",
                    "status": "pending",
                    "url": "https://www.bravotv.com/people/robyn-dixon",
                },
            ]
            with patch("api.routers.admin_show_links._validate_person_knowledge_url") as validate_url:
                validate_url.return_value = (None, "invalid")
                with patch("api.routers.admin_show_links.pg.execute_returning") as execute_returning:
                    execute_returning.return_value = [
                        {"id": invalid_wiki_id},
                        {"id": invalid_imdb_id},
                        {"id": invalid_tmdb_id},
                        {"id": non_cast_bravo_id},
                    ]
                    result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 4
    assert result["invalid"] == 4
    assert result["promoted"] == 0
    assert result["deleted"] == 4
    assert result["validation_failures"] == 0
    sql, params = execute_returning.call_args.args
    assert "DELETE FROM core.entity_links" in sql
    assert params == [[invalid_wiki_id, invalid_imdb_id, invalid_tmdb_id, non_cast_bravo_id]]


def test_discover_people_links_skips_imdb_and_tmdb_when_validation_fails() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"networks": ["peacock"]}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": person_id,
                        "full_name": "Heather Gay",
                        "external_ids": {"imdb": "nm1234567", "tmdb_id": 12345},
                        "fandom_url": "",
                        "cast_tmdb_imdb_id": None,
                        "cast_tmdb_tmdb_id": None,
                        "cast_tmdb_wikidata_id": None,
                    }
                ],
            ]
            with patch(
                "api.routers.admin_show_links._validated_person_knowledge_url",
                return_value=None,
            ):
                links = _discover_people_links(show_id)

    assert not any(link.get("link_kind") == "imdb" for link in links)
    assert not any(link.get("link_kind") == "tmdb" for link in links)


def test_cleanup_invalid_person_knowledge_links_keeps_rows_on_validation_fetch_error() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    link_id = str(uuid4())

    with patch("api.routers.admin_show_links._load_show_cast_names_by_person_id") as cast_lookup:
        cast_lookup.return_value = {person_id: "Lisa Barlow"}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": link_id,
                    "person_id": person_id,
                    "link_kind": "wikipedia",
                    "status": "approved",
                    "url": "https://en.wikipedia.org/wiki/Lisa_Barlow",
                }
            ]
            with patch(
                "api.routers.admin_show_links._validate_person_knowledge_url",
                return_value=(None, "fetch_error"),
            ):
                with patch("api.routers.admin_show_links.pg.execute_returning") as execute_returning:
                    result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 0
    assert result["promoted"] == 0
    assert result["deleted"] == 0
    assert result["validation_failures"] == 1
    execute_returning.assert_not_called()


def test_cleanup_invalid_person_knowledge_links_promotes_pending_valid_rows() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    link_id = str(uuid4())

    with patch("api.routers.admin_show_links._load_show_cast_names_by_person_id") as cast_lookup:
        cast_lookup.return_value = {person_id: "Heather Gay"}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": link_id,
                    "person_id": person_id,
                    "link_kind": "imdb",
                    "status": "pending",
                    "url": "https://www.imdb.com/name/nm1234567/",
                }
            ]
            with patch(
                "api.routers.admin_show_links._validate_person_knowledge_url",
                return_value=("https://www.imdb.com/name/nm1234567/", "valid"),
            ):
                with patch("api.routers.admin_show_links.pg.execute_returning") as execute_returning:
                    execute_returning.return_value = [{"id": link_id}]
                    result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 0
    assert result["promoted"] == 1
    assert result["deleted"] == 0
    assert result["validation_failures"] == 0
    sql, params = execute_returning.call_args.args
    assert "UPDATE core.entity_links" in sql
    assert params == ["https://www.imdb.com/name/nm1234567/", "https://www.imdb.com/name/nm1234567/", link_id]


def test_cleanup_invalid_person_knowledge_links_deletes_pending_rows_on_fetch_error() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    link_id = str(uuid4())

    with patch("api.routers.admin_show_links._load_show_cast_names_by_person_id") as cast_lookup:
        cast_lookup.return_value = {person_id: "Heather Gay"}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": link_id,
                    "person_id": person_id,
                    "link_kind": "tmdb",
                    "status": "pending",
                    "url": "https://www.themoviedb.org/person/12345",
                }
            ]
            with patch(
                "api.routers.admin_show_links._validate_person_knowledge_url",
                return_value=(None, "fetch_error"),
            ):
                with patch("api.routers.admin_show_links.pg.execute_returning") as execute_returning:
                    execute_returning.return_value = [{"id": link_id}]
                    result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 1
    assert result["promoted"] == 0
    assert result["deleted"] == 1
    assert result["validation_failures"] == 1
    sql, params = execute_returning.call_args.args
    assert "DELETE FROM core.entity_links" in sql
    assert params == [[link_id]]
