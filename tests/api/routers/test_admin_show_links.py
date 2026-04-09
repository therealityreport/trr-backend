from __future__ import annotations

import asyncio
import json
import re
import time
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

import api.routers.admin_show_links as admin_show_links
import trr_backend.integrations.fandom as fandom_integration
from api.main import app
from api.routers.admin_show_links import (
    _canonicalize_url,
    _classify_submitted_link_input,
    _cleanup_invalid_person_knowledge_links,
    _cleanup_invalid_person_social_links,
    _cleanup_invalid_show_knowledge_links,
    _discover_people_links,
    _discover_season_links,
    _discover_show_links,
    _normalize_link_kind,
    _promote_pending_links_to_approved,
    _source_timeout_seconds,
    _sync_show_wikipedia_links,
    _validate_person_knowledge_url,
    _validate_person_social_url,
    _validated_person_knowledge_url,
)
from scripts import backfill_fandom_link_discovery


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture(autouse=True)
def _mock_fandom_page_directory():
    with patch(
        "trr_backend.repositories.fandom_page_directory.pg.fetch_one",
        return_value=None,
    ):
        yield


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


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
                {"url": ""},
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

    snapshot_call = fetch_one.call_args_list[2]
    assert snapshot_call.args[1] == [show_id, "default"]
    assert any(link.get("link_kind") == "cast_announcement" for link in links)


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
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._fetch_html_with_status",
                return_value=(
                    200,
                    "<html><body><h1>The Real Housewives of Salt Lake City</h1></body></html>",
                    "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                    None,
                ),
            ):
                links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 1
    assert fandom_links[0]["url"] == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
    assert fandom_links[0]["source"] == "core.entity_links"


def test_discover_show_links_assigns_real_housewives_wiki_without_bravo_network_metadata() -> None:
    show_id = str(uuid4())
    rhoslc_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"

    def _fetch_one(query: str, params=None):
        if "FROM core.shows" in query:
            return {
                "id": show_id,
                "name": "The Real Housewives of Salt Lake City",
                "networks": [],
                "wikidata_id": None,
                "external_ids": {},
            }
        if "FROM core.show_source_latest" in query:
            return {"payload": {"normalized": {}}}
        return {"url": ""}

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = _fetch_one
            with patch(
                "api.routers.admin_show_links._resolve_show_fandom_rule_context",
                return_value={
                    "preferred_community_domain": "real-housewives.fandom.com",
                    "community_domains": ["real-housewives.fandom.com"],
                    "candidate_urls": [rhoslc_url],
                    "include_allpages_scan": True,
                },
            ):
                with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=rhoslc_url):
                    with patch(
                        "api.routers.admin_show_links._fetch_html_with_status",
                        return_value=(
                            200,
                            "<html><body><h1>The Real Housewives of Salt Lake City</h1></body></html>",
                            rhoslc_url,
                            None,
                        ),
                    ):
                        links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 1
    assert fandom_links[0]["url"] == rhoslc_url
    assert fandom_links[0]["source"] == "bravo_default"


def test_collect_show_fandom_seed_urls_includes_real_housewives_wiki_for_rhoslc() -> None:
    show_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch(
                "api.routers.admin_show_links.search_real_housewives_wiki",
                return_value="https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
            ):
                seeds = admin_show_links._collect_show_fandom_seed_urls(
                    show_id,
                    show_name="The Real Housewives of Salt Lake City",
                    show_fandom_seed_urls=None,
                )

    assert seeds == ["https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"]


def test_collect_show_fandom_seed_urls_filters_internal_real_housewives_seed_pages() -> None:
    show_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch(
                "api.routers.admin_show_links.search_real_housewives_wiki",
                return_value="https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
            ):
                seeds = admin_show_links._collect_show_fandom_seed_urls(
                    show_id,
                    show_name="The Real Housewives of Salt Lake City",
                    show_fandom_seed_urls=[
                        "https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki",
                        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                    ],
                    franchise_rule_urls=[
                        "https://real-housewives.fandom.com/wiki/Special:AllPages",
                    ],
                )

    assert seeds == ["https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"]


def test_collect_show_fandom_seed_urls_skips_real_housewives_wiki_for_non_housewives_show() -> None:
    show_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch(
                "api.routers.admin_show_links.search_real_housewives_wiki",
                return_value="https://real-housewives.fandom.com/wiki/The_Traitors",
            ) as search_real_housewives_wiki:
                seeds = admin_show_links._collect_show_fandom_seed_urls(
                    show_id,
                    show_name="The Traitors",
                    show_fandom_seed_urls=None,
                )

    search_real_housewives_wiki.assert_not_called()
    assert seeds == []


def test_collect_seeded_fandom_candidate_urls_by_domain_can_skip_allpages_scan() -> None:
    requested_urls: list[str] = []

    def _fetch_html(url: str, *, timeout: float = 20.0):
        requested_urls.append(url)
        if url == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City":
            return (
                200,
                "<html><body><h1>The Real Housewives of Salt Lake City</h1></body></html>",
                url,
                None,
            )
        raise AssertionError(url)

    with patch(
        "api.routers.admin_show_links.load_fandom_community_allowlist",
        return_value=("real-housewives.fandom.com",),
    ):
        with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
            candidates_by_domain = admin_show_links._collect_seeded_fandom_candidate_urls_by_domain(
                ["https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"],
                fandom_allowlist=("real-housewives.fandom.com",),
                include_allpages_scan=False,
            )

    assert requested_urls == ["https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"]
    assert candidates_by_domain == {
        "real-housewives.fandom.com": ["https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"]
    }


def test_discover_show_links_prefers_core_entity_links_source_for_duplicate_fandom_url() -> None:
    show_id = str(uuid4())
    duplicate_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[{"url": duplicate_url}]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Real Housewives of Salt Lake City",
                    "networks": ["bravo"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=duplicate_url):
                with patch(
                    "api.routers.admin_show_links._fetch_html_with_status",
                    return_value=(
                        200,
                        "<html><body><h1>The Real Housewives of Salt Lake City</h1></body></html>",
                        duplicate_url,
                        None,
                    ),
                ):
                    links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 1
    assert fandom_links[0]["source"] == "core.entity_links"


def test_discover_show_links_ignores_existing_nonpreferred_real_housewives_fandom_link() -> None:
    show_id = str(uuid4())
    existing_url = "https://another-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
    rh_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"

    def _fetch_html(url: str, *, timeout: float = 20.0):
        if url in {existing_url, rh_url}:
            return (
                200,
                "<html><body><h1>The Real Housewives of Salt Lake City</h1></body></html>",
                url,
                None,
            )
        if url == "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City":
            return (404, "", url, None)
        return (404, "<html><body>Missing</body></html>", url, None)

    def _fetch_one(query: str, params=None):
        if "FROM core.shows" in query:
            return {
                "id": show_id,
                "name": "The Real Housewives of Salt Lake City",
                "networks": [],
                "wikidata_id": None,
                "external_ids": {},
            }
        if "FROM core.show_source_latest" in query:
            return {"payload": {"normalized": {}}}
        return {"url": ""}

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[{"url": existing_url}]):
            fetch_one.side_effect = _fetch_one
            with patch(
                "api.routers.admin_show_links._resolve_show_fandom_rule_context",
                return_value={
                    "effective_rule_key": "real_housewives",
                    "preferred_community_domain": "real-housewives.fandom.com",
                    "community_domains": ["real-housewives.fandom.com", "another-housewives.fandom.com"],
                    "candidate_urls": [rh_url],
                    "include_allpages_scan": True,
                },
            ):
                with patch(
                    "api.routers.admin_show_links.load_fandom_community_allowlist",
                    return_value=("real-housewives.fandom.com", "another-housewives.fandom.com"),
                ):
                    with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
                        links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert {str(link.get("url") or "") for link in fandom_links} == {rh_url}
    assert any(link.get("url") == rh_url and link.get("source") == "franchise_rule" for link in fandom_links)


def test_collect_show_fandom_seed_urls_filters_to_preferred_real_housewives_community() -> None:
    show_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com", "realitytv-girl.fandom.com"),
        ):
            seeds = admin_show_links._collect_show_fandom_seed_urls(
                show_id,
                show_name="The Real Housewives of Salt Lake City",
                show_fandom_seed_urls=[
                    "https://realitytv-girl.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                    "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                ],
                fandom_allowlist=("real-housewives.fandom.com", "realitytv-girl.fandom.com"),
                preferred_community_domain="real-housewives.fandom.com",
            )

    assert seeds == ["https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"]


def test_discover_show_links_expands_root_fandom_seed_to_show_page_candidates() -> None:
    show_id = str(uuid4())

    def _fetch_html(url: str, *, timeout: float = 20.0):
        if url == "https://thetraitors.fandom.com/":
            return (
                200,
                "<html><body><h1>The Traitors Wiki</h1></body></html>",
                "https://thetraitors.fandom.com/",
                None,
            )
        if url == "https://thetraitors.fandom.com/wiki/The_Traitors":
            return (
                404,
                "<html><body>There is currently no text in this page.</body></html>",
                url,
                None,
            )
        if url == "https://thetraitors.fandom.com/wiki/The_Traitors_(US)":
            return (
                200,
                """
                <html>
                  <head>
                    <meta property="og:site_name" content="The Traitors Wiki" />
                    <title>The Traitors (US) | The Traitors Wiki | Fandom</title>
                  </head>
                  <body><h1>The Traitors (US)</h1></body>
                </html>
                """,
                "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
                None,
            )
        return (404, "<html><body>Missing</body></html>", url, None)

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch(
            "api.routers.admin_show_links.pg.fetch_all",
            return_value=[{"url": "https://thetraitors.fandom.com/"}],
        ):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "networks": ["peacock"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=(None, None, "missing"),
            ):
                with patch(
                    "api.routers.admin_show_links._curated_show_fandom_base_urls",
                    return_value=(),
                ):
                    with patch(
                        "api.routers.admin_show_links.load_fandom_community_allowlist",
                        return_value=("thetraitors.fandom.com", "thetraitorsuk.fandom.com"),
                    ):
                        with patch(
                            "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                            return_value=[],
                        ):
                            with patch(
                                "api.routers.admin_show_links._search_fandom_allpages_html_candidates",
                                return_value=["https://thetraitors.fandom.com/wiki/The_Traitors_(US)"],
                            ):
                                with patch(
                                    "api.routers.admin_show_links._fetch_html_with_status",
                                    side_effect=_fetch_html,
                                ):
                                    links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    fandom_urls = {str(link.get("url") or "") for link in fandom_links}
    assert "https://thetraitors.fandom.com/" in fandom_urls
    assert "https://thetraitors.fandom.com/wiki/The_Traitors_(US)" in fandom_urls
    assert any(str(link.get("source") or "").endswith(":derived_show_page") for link in fandom_links)
    derived_page = next(
        link for link in fandom_links if link.get("url") == "https://thetraitors.fandom.com/wiki/The_Traitors_(US)"
    )
    assert derived_page["metadata"]["site_title"] == "The Traitors Wiki"


def test_discover_show_links_falls_back_to_ranked_candidate_discovery_for_show_pages() -> None:
    show_id = str(uuid4())

    def _fetch_html(url: str, *, timeout: float = 20.0):
        if url == "https://real-housewives.fandom.com/":
            return (
                200,
                "<html><body><h1>The Real Housewives Wiki</h1></body></html>",
                "https://real-housewives.fandom.com/",
                None,
            )
        if url == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City":
            return (
                200,
                """
                <html>
                  <head>
                    <meta property="og:site_name" content="The Real Housewives Wiki" />
                    <title>The Real Housewives of Salt Lake City | The Real Housewives Wiki | Fandom</title>
                  </head>
                  <body><h1>The Real Housewives of Salt Lake City</h1></body>
                </html>
                """,
                url,
                None,
            )
        return (404, "<html><body>Missing</body></html>", url, None)

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch(
            "api.routers.admin_show_links.pg.fetch_all",
            return_value=[{"url": "https://real-housewives.fandom.com/"}],
        ):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Real Housewives of Salt Lake City",
                    "networks": ["bravo"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=(None, None, "missing"),
            ):
                with patch(
                    "api.routers.admin_show_links._curated_show_fandom_base_urls",
                    return_value=(),
                ):
                    with patch(
                        "api.routers.admin_show_links.search_real_housewives_wiki",
                        return_value=None,
                    ):
                        with patch(
                            "api.routers.admin_show_links.load_fandom_community_allowlist",
                            return_value=("real-housewives.fandom.com",),
                        ):
                            with patch(
                                "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                                return_value=[],
                            ):
                                with patch(
                                    "api.routers.admin_show_links._search_fandom_allpages_html_candidates",
                                    return_value=[],
                                ):
                                    with patch(
                                        "api.routers.admin_show_links.discover_fandom_candidate_pages",
                                        return_value=[
                                            SimpleNamespace(
                                                url="https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                                                title="The Real Housewives of Salt Lake City",
                                                source="search",
                                            )
                                        ],
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links._fetch_html_with_status",
                                            side_effect=_fetch_html,
                                        ):
                                            links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    fandom_urls = {str(link.get("url") or "") for link in fandom_links}
    assert "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City" in fandom_urls
    assert "https://real-housewives.fandom.com/" not in fandom_urls
    derived_page = next(
        link
        for link in fandom_links
        if link.get("url") == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
    )
    assert derived_page["metadata"]["site_title"] == "The Real Housewives Wiki"
    assert str(derived_page.get("source") or "").endswith(":derived_show_page")


def test_discover_show_links_uses_real_housewives_rule_allpages_without_persisting_seed_urls() -> None:
    show_id = str(uuid4())
    requested_urls: list[str] = []

    def _fetch_one(query: str, params=None):
        if "FROM core.shows" in query:
            return {
                "id": show_id,
                "name": "The Real Housewives of Salt Lake City",
                "networks": ["bravo"],
                "wikidata_id": None,
                "external_ids": {},
            }
        if "FROM core.show_source_latest" in query:
            return {"payload": {"normalized": {}}}
        return {"url": ""}

    def _fetch_html(url: str, *, timeout: float = 20.0):
        requested_urls.append(url)
        if url == "https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki":
            return (
                200,
                "<html><body><h1>The Real Housewives Wiki</h1></body></html>",
                url,
                None,
            )
        if url == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City":
            return (
                200,
                """
                <html>
                  <head>
                    <meta property="og:site_name" content="The Real Housewives Wiki" />
                    <title>The Real Housewives of Salt Lake City | The Real Housewives Wiki | Fandom</title>
                  </head>
                  <body><h1>The Real Housewives of Salt Lake City</h1></body>
                </html>
                """,
                url,
                None,
            )
        return (404, "<html><body>Missing</body></html>", url, None)

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = _fetch_one
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=(None, None, "missing"),
            ):
                with patch(
                    "api.routers.admin_show_links._resolve_show_fandom_rule_context",
                    return_value={
                        "effective_rule_key": "real-housewives",
                        "community_domains": ["real-housewives.fandom.com"],
                        "include_allpages_scan": True,
                        "candidate_urls": [
                            "https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki",
                            "https://real-housewives.fandom.com/wiki/Special:AllPages",
                        ],
                    },
                ):
                    with patch(
                        "api.routers.admin_show_links.load_fandom_community_allowlist",
                        return_value=("real-housewives.fandom.com",),
                    ):
                        with patch(
                            "api.routers.admin_show_links.search_real_housewives_wiki",
                            return_value=None,
                        ):
                            with patch(
                                "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                                return_value=[],
                            ):
                                with patch(
                                    "api.routers.admin_show_links._search_fandom_allpages_html_candidates",
                                    return_value=[
                                        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                                    ],
                                ):
                                    with patch(
                                        "api.routers.admin_show_links.discover_fandom_candidate_pages",
                                        return_value=[],
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links._fetch_html_with_status",
                                            side_effect=_fetch_html,
                                        ):
                                            links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert [str(link.get("url") or "") for link in fandom_links] == [
        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
    ]
    assert "https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki" in requested_urls


def test_discover_show_links_uses_cached_directory_when_canonical_page_is_cloudflare_blocked() -> None:
    show_id = str(uuid4())
    canonical_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"

    def _fetch_html(url: str, *, timeout: float = 20.0):
        return (
            403,
            "<html><head><title>Just a moment...</title></head><body>Cloudflare</body></html>",
            url,
            None,
        )

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch(
            "api.routers.admin_show_links.pg.fetch_all",
            return_value=[{"url": "https://real-housewives.fandom.com/"}],
        ):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Real Housewives of Salt Lake City",
                    "networks": ["bravo"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=(None, None, "missing"),
            ):
                with patch(
                    "api.routers.admin_show_links.search_real_housewives_wiki",
                    return_value=None,
                ):
                    with patch(
                        "api.routers.admin_show_links.load_fandom_community_allowlist",
                        return_value=("real-housewives.fandom.com",),
                    ):
                        with patch(
                            "api.routers.admin_show_links.fandom_page_directory_repo.search_active_page_directory_entries",
                            return_value=[
                                {
                                    "community_domain": "real-housewives.fandom.com",
                                    "page_title": "The Real Housewives of Salt Lake City",
                                    "page_slug": "The_Real_Housewives_of_Salt_Lake_City",
                                    "page_url": canonical_url,
                                }
                            ],
                        ):
                            with patch(
                                "api.routers.admin_show_links.fandom_page_directory_repo.get_active_page_directory_entry_by_url",
                                return_value={
                                    "community_domain": "real-housewives.fandom.com",
                                    "page_title": "The Real Housewives of Salt Lake City",
                                    "page_slug": "The_Real_Housewives_of_Salt_Lake_City",
                                    "page_url": canonical_url,
                                },
                            ):
                                with patch(
                                    "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                                    return_value=[],
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._search_fandom_allpages_html_candidates",
                                        return_value=[],
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links.discover_fandom_candidate_pages",
                                            return_value=[],
                                        ):
                                            with patch(
                                                "api.routers.admin_show_links._fetch_html_with_status",
                                                side_effect=_fetch_html,
                                            ):
                                                links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert [str(link.get("url") or "") for link in fandom_links] == [canonical_url]


def test_discover_show_links_skips_fandom_season_pages_when_only_season_candidate_exists() -> None:
    show_id = str(uuid4())

    def _fetch_html(url: str, *, timeout: float = 20.0):
        if url == "https://real-housewives.fandom.com/":
            return (
                200,
                "<html><body><h1>The Real Housewives Wiki</h1></body></html>",
                url,
                None,
            )
        if url == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_4":
            return (
                200,
                """
                <html>
                  <head>
                    <meta property="og:site_name" content="The Real Housewives Wiki" />
                    <title>The Real Housewives of Salt Lake City - Season 4 | The Real Housewives Wiki | Fandom</title>
                  </head>
                  <body><h1>The Real Housewives of Salt Lake City - Season 4</h1></body>
                </html>
                """,
                url,
                None,
            )
        return (404, "<html><body>Missing</body></html>", url, None)

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch(
            "api.routers.admin_show_links.pg.fetch_all",
            return_value=[{"url": "https://real-housewives.fandom.com/"}],
        ):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Real Housewives of Salt Lake City",
                    "networks": ["bravo"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=(None, None, "missing"),
            ):
                with patch(
                    "api.routers.admin_show_links._curated_show_fandom_base_urls",
                    return_value=(),
                ):
                    with patch(
                        "api.routers.admin_show_links.load_fandom_community_allowlist",
                        return_value=("real-housewives.fandom.com",),
                    ):
                        with patch(
                            "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                            return_value=[
                                "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_4"
                            ],
                        ):
                            with patch(
                                "api.routers.admin_show_links._search_fandom_allpages_html_candidates",
                                return_value=[],
                            ):
                                with patch(
                                    "api.routers.admin_show_links.discover_fandom_candidate_pages",
                                    return_value=[],
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._fetch_html_with_status",
                                        side_effect=_fetch_html,
                                    ):
                                        links = _discover_show_links(show_id)

    fandom_urls = {
        str(link.get("url") or "")
        for link in links
        if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"
    }
    assert "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_4" not in fandom_urls


def test_score_fandom_show_candidate_url_rejects_wrong_same_franchise_show_page() -> None:
    score = admin_show_links._score_fandom_show_candidate_url(
        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_New_York_City",
        show_name="The Real Housewives of Salt Lake City",
    )

    assert score == 0


def test_discover_show_links_skips_missing_fandom_pages() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch(
            "api.routers.admin_show_links.pg.fetch_all",
            return_value=[
                {"url": "https://real-housewives.fandom.com/wiki/The_Traitors"},
            ],
        ):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "networks": ["peacock"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._fetch_html_with_status",
                return_value=(
                    200,
                    "There is currently no text in this page. You can search for this page title in other pages.",
                    "https://real-housewives.fandom.com/wiki/The_Traitors",
                    None,
                ),
            ):
                links = _discover_show_links(show_id)

    assert not any(link.get("link_kind") == "fandom" for link in links)


def test_discover_show_links_skips_bravotv_show_page_for_non_bravo_network() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "imdb_id": "tt15218000",
                    "tmdb_id": 204761,
                    "networks": ["peacock"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            links = _discover_show_links(show_id)

    assert not any(link.get("link_kind") == "official_page" for link in links)


def test_discover_show_links_includes_imdb_and_tmdb_show_pages() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "imdb_id": "tt15218000",
                    "tmdb_id": 204761,
                    "networks": [],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            links = _discover_show_links(show_id)

    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "imdb"
        and link.get("url") == "https://www.imdb.com/title/tt15218000/"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "tmdb"
        and link.get("url") == "https://www.themoviedb.org/tv/204761"
        for link in links
    )


def test_discover_show_links_derives_external_ids_from_wikidata_when_missing() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "imdb_id": None,
                    "tmdb_id": None,
                    "networks": ["peacock"],
                    "wikidata_id": "Q116449538",
                    "external_ids": {},
                },
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._fetch_wikidata_summary",
                return_value=(
                    {
                        "item_id": "Q116449538",
                        "label": "The Traitors",
                        "enwiki_title": "The Traitors (American TV series)",
                        "enwiki_url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                        "imdb_id": "tt15557874",
                        "tmdb_tv_id": "215943",
                        "tmdb_person_id": "",
                        "tvdb_id": "428163",
                        "tvmaze_show_id": "58177",
                        "tvmaze_season_id": "",
                        "ratinggraph_tv_show_id": "the-traitors-ratings-103483",
                        "trakt_id": "shows/the-traitors-us",
                        "x_topic_id": "1742326119434545480",
                    },
                    False,
                ),
            ):
                links = _discover_show_links(show_id)

    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "imdb"
        and link.get("url") == "https://www.imdb.com/title/tt15557874/"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "tmdb"
        and link.get("url") == "https://www.themoviedb.org/tv/215943"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "tvdb"
        and link.get("url") == "https://www.thetvdb.com/series/428163"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "tvmaze"
        and link.get("url") == "https://www.tvmaze.com/shows/58177"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "ratinggraph"
        and link.get("url") == "https://www.ratingraph.com/tv-shows/the-traitors-ratings-103483"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "trakt"
        and link.get("url") == "https://trakt.tv/shows/the-traitors-us"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "x_topic"
        and link.get("url") == "https://x.com/i/topics/1742326119434545480"
        for link in links
    )


def test_discover_show_links_includes_peacock_and_nbc_network_blog_links() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "imdb_id": "tt15218000",
                    "tmdb_id": 204761,
                    "networks": ["peacock", "nbc"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=("https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)", None, None),
            ):
                with patch(
                    "api.routers.admin_show_links.load_fandom_community_allowlist",
                    return_value=("thetraitors.fandom.com", "thetraitorsuk.fandom.com"),
                ):
                    with patch(
                        "api.routers.admin_show_links._fetch_html_with_status",
                        return_value=(
                            200,
                            "<html><body><h1>The Traitors (US)</h1></body></html>",
                            "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
                            None,
                        ),
                    ):
                        links = _discover_show_links(show_id)

    assert any(
        link.get("entity_type") == "show"
        and link.get("link_group") == "cast_announcements"
        and link.get("link_kind") == "network_blog"
        and link.get("url") == "https://www.peacocktv.com/blog/show/the-traitors"
        for link in links
    )
    assert any(
        link.get("entity_type") == "show"
        and link.get("link_group") == "cast_announcements"
        and link.get("link_kind") == "network_blog"
        and link.get("url") == "https://www.nbc.com/nbc-insider/franchise/the-traitors"
        for link in links
    )


def test_build_shared_social_source_links_reads_cloud_catalog_rows() -> None:
    show_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_all",
        return_value=[
            {"platform": "instagram", "account_handle": "bravotv", "metadata": {}},
            {"platform": "youtube", "account_handle": "bravo", "metadata": {}},
        ],
    ):
        links = admin_show_links._build_shared_social_source_links(show_id, source_scope="bravo")

    assert links == [
        {
            "entity_type": "show",
            "entity_id": show_id,
            "season_number": 0,
            "link_group": "social",
            "link_kind": "instagram",
            "label": "Instagram",
            "url": "https://www.instagram.com/bravotv",
            "source": "social.shared_account_sources",
            "metadata": {"shared_account_source_scope": "bravo"},
        },
        {
            "entity_type": "show",
            "entity_id": show_id,
            "season_number": 0,
            "link_group": "social",
            "link_kind": "youtube",
            "label": "YouTube",
            "url": "https://www.youtube.com/@bravo",
            "source": "social.shared_account_sources",
            "metadata": {"shared_account_source_scope": "bravo"},
        },
    ]


def test_discover_show_links_includes_curated_traitors_fandom_base_urls() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "imdb_id": None,
                    "tmdb_id": None,
                    "networks": ["peacock"],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch("api.routers.admin_show_links._resolve_wikipedia_url", return_value=(None, None, "missing")):
                with patch(
                    "api.routers.admin_show_links.load_fandom_community_allowlist",
                    return_value=("thetraitors.fandom.com", "thetraitorsuk.fandom.com"),
                ):
                    with patch("api.routers.admin_show_links._fetch_html_with_status") as fetch_html:
                        fetch_html.side_effect = [
                            (
                                200,
                                "<html><body><h1>The Traitors US</h1></body></html>",
                                "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US",
                                None,
                            ),
                            (
                                200,
                                "<html><body><h1>The Traitors (US)</h1></body></html>",
                                "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
                                None,
                            ),
                        ]
                        links = _discover_show_links(show_id)

    fandom_links = [link for link in links if link.get("entity_type") == "show" and link.get("link_kind") == "fandom"]
    assert any(
        link.get("url") == "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US"
        and link.get("source") == "curated_fandom_base"
        for link in fandom_links
    )
    assert any(
        link.get("url") == "https://thetraitors.fandom.com/wiki/The_Traitors_(US)"
        and link.get("source") == "curated_fandom_base"
        for link in fandom_links
    )


def test_discover_show_links_skips_missing_show_wikipedia_pages() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "A Show That Does Not Exist Anywhere",
                    "imdb_id": None,
                    "tmdb_id": None,
                    "networks": [],
                    "wikidata_id": None,
                    "external_ids": {},
                },
                {"url": ""},
                {"payload": {"normalized": {}}},
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=(None, None, "missing"),
            ):
                links = _discover_show_links(show_id)

    assert not any(link.get("entity_type") == "show" and link.get("link_kind") == "wikipedia" for link in links)


def test_discover_show_links_skips_show_wikipedia_page_with_mismatched_wikidata() -> None:
    show_id = str(uuid4())
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        with patch("api.routers.admin_show_links.pg.fetch_all", return_value=[]):
            fetch_one.side_effect = [
                {
                    "id": show_id,
                    "name": "The Traitors",
                    "imdb_id": "tt15218000",
                    "tmdb_id": 204761,
                    "networks": ["peacock"],
                    "wikidata_id": "Q147711660",
                    "external_ids": {},
                },
                {"payload": {"normalized": {}}},
            ]
            with patch("api.routers.admin_show_links._fetch_wikidata_summary", return_value=(None, False)):
                with patch(
                    "api.routers.admin_show_links._resolve_wikipedia_url",
                    return_value=("https://en.wikipedia.org/wiki/The_Traitors", "The Traitors", None),
                ):
                    with patch(
                        "api.routers.admin_show_links._resolve_wikipedia_wikidata_id",
                        return_value="Q111195888",
                    ):
                        links = _discover_show_links(show_id)

    assert not any(
        link.get("entity_type") == "show"
        and link.get("link_kind") == "wikipedia"
        and link.get("url") == "https://en.wikipedia.org/wiki/The_Traitors"
        for link in links
    )


def test_classify_submitted_link_input_routes_links_by_entity_type() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())
    person_id = str(uuid4())
    person = {
        "id": person_id,
        "name": "Alan Cumming",
        "name_norm": "alan cumming",
        "imdb_id": "nm0001086",
        "tmdb_id": "9346",
        "wikidata_id": None,
        "fandom_name_norm": "alan cumming",
    }
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "seasons_by_number": {
            2: {
                "id": season_id,
                "season_number": 2,
                "external_wikidata_id": None,
            }
        },
        "seasons_by_wikidata": {},
        "people_by_id": {person_id: person},
        "people_by_name": {"alan cumming": person},
        "people_by_slug": {"alan cumming": person},
        "people_by_imdb": {"nm0001086": person},
        "people_by_tmdb": {"9346": person},
        "people_by_wikidata": {},
    }

    season_rows, season_error = _classify_submitted_link_input(
        "https://www.themoviedb.org/tv/204761/season/2",
        context,
    )
    assert season_error is None
    assert season_rows[0]["entity_type"] == "season"
    assert season_rows[0]["entity_id"] == season_id
    assert season_rows[0]["season_number"] == 2
    assert season_rows[0]["link_kind"] == "tmdb"

    social_rows, social_error = _classify_submitted_link_input("instagram:@thetraitorsus", context)
    assert social_error is None
    assert social_rows[0]["entity_type"] == "show"
    assert social_rows[0]["link_group"] == "social"
    assert social_rows[0]["link_kind"] == "instagram"
    assert social_rows[0]["url"] == "https://www.instagram.com/thetraitorsus"

    person_rows, person_error = _classify_submitted_link_input(
        "https://thetraitors.fandom.com/wiki/Alan_Cumming",
        context,
    )
    assert person_error is None
    assert person_rows[0]["entity_type"] == "person"
    assert person_rows[0]["entity_id"] == person_id
    assert person_rows[0]["link_kind"] == "fandom"


def test_classify_submitted_link_input_recognizes_network_blog_urls() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock", "nbc"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    peacock_rows, peacock_error = _classify_submitted_link_input(
        "https://www.peacocktv.com/blog/show/the-traitors",
        context,
    )
    assert peacock_error is None
    assert peacock_rows[0]["entity_type"] == "show"
    assert peacock_rows[0]["link_group"] == "cast_announcements"
    assert peacock_rows[0]["link_kind"] == "network_blog"

    nbc_rows, nbc_error = _classify_submitted_link_input(
        "https://www.nbc.com/nbc-insider/franchise/the-traitors",
        context,
    )
    assert nbc_error is None
    assert nbc_rows[0]["entity_type"] == "show"
    assert nbc_rows[0]["link_group"] == "cast_announcements"
    assert nbc_rows[0]["link_kind"] == "network_blog"


def test_classify_submitted_link_input_recognizes_google_news_topic_urls() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Real Housewives of Salt Lake City",
        "show_name_norm": "the real housewives of salt lake city",
        "show_imdb_id": "tt11363282",
        "show_tmdb_id": "110381",
        "show_wikidata_id": None,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    rows, error = _classify_submitted_link_input(
        "https://news.google.com/topics/CAAqKAgKIiJDQkFTRXdvTkwyY3ZNVEZvYlhBeGVtUndNQklDWlc0b0FBUAE?ceid=US:en&oc=3",
        context,
    )

    assert error is None
    assert rows[0]["entity_type"] == "show"
    assert rows[0]["entity_id"] == show_id
    assert rows[0]["season_number"] == 0
    assert rows[0]["link_group"] == "official"
    assert rows[0]["link_kind"] == "google_news_url"
    assert rows[0]["label"] == "Google News"


def test_classify_submitted_link_input_accepts_traitors_fandom_base_urls() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(
            200,
            """
            <html>
              <head>
                <meta property="og:site_name" content="The Traitors Wiki" />
                <title>The Traitors (US) | The Traitors Wiki | Fandom</title>
              </head>
              <body><h1>The Traitors (US)</h1></body>
            </html>
            """,
            "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
            None,
        ),
    ):
        rows, error = _classify_submitted_link_input("https://thetraitors.fandom.com/wiki/The_Traitors_(US)", context)
    assert error is None
    assert rows[0]["entity_type"] == "show"
    assert rows[0]["link_kind"] == "fandom"
    assert rows[0]["metadata"]["site_title"] == "The Traitors Wiki"


def test_classify_submitted_link_input_accepts_traitors_fandom_root_domain_seed_without_scheme() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    rows, error = _classify_submitted_link_input("thetraitorsuk.fandom.com", context)
    assert error is None
    assert rows[0]["entity_type"] == "show"
    assert rows[0]["link_kind"] == "fandom"
    assert rows[0]["url"] == "https://thetraitorsuk.fandom.com/"
    assert rows[0]["metadata"]["fandom_seed_domain"] == "thetraitorsuk.fandom.com"


def test_search_fandom_allpages_html_candidates_uses_from_query_prefix_and_extracts_pages() -> None:
    requested_urls: list[str] = []

    def _fetch_html(url: str, *, timeout: float = 20.0):
        requested_urls.append(url)
        if "from=Alan" in url:
            return (
                200,
                """
                <html>
                  <body>
                    <a href="/wiki/Footer_Noise">Footer Noise</a>
                    <div class="mw-allpages-body">
                      <a href="/wiki/Alan_Cumming">Alan Cumming</a>
                      <a href="/wiki/Special:AllPages">Special</a>
                    </div>
                  </body>
                </html>
                """,
                "https://thetraitors.fandom.com/wiki/Special:AllPages?from=Alan&to=&namespace=0",
                None,
            )
        return (
            404,
            "<html><body>Missing</body></html>",
            url,
            None,
        )

    with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
        candidates = admin_show_links._search_fandom_allpages_html_candidates(
            community_domain="thetraitors.fandom.com",
            query="Alan Cumming",
            max_results=10,
        )

    assert any("from=Alan" in url for url in requested_urls)
    assert "https://thetraitors.fandom.com/wiki/Alan_Cumming" in candidates


def test_search_fandom_person_related_pages_filters_same_owner_results_and_paginates() -> None:
    api_calls: list[str] = []

    def _fetch_html(url: str, *, timeout: float = 20.0, headers=None):
        api_calls.append(url)
        if "api.php" in url and "sroffset=2" not in url:
            return (
                200,
                json.dumps(
                    {
                        "query": {
                            "search": [
                                {"title": "Angie Katsanevas"},
                                {"title": "Angie Katsanevas/Gallery"},
                                {"title": "Angie Harrington"},
                            ]
                        },
                        "continue": {"sroffset": 2},
                    }
                ),
                None,
            )
        if "api.php" in url and "sroffset=2" in url:
            return (
                200,
                json.dumps(
                    {
                        "query": {
                            "search": [
                                {"title": "Angie Katsanevas/Storylines"},
                                {"title": "Angie Katsanevas/Connections"},
                            ]
                        }
                    }
                ),
                None,
            )
        return (404, "", None)

    with patch("trr_backend.integrations.fandom.fetch_html", side_effect=_fetch_html):
        candidates = fandom_integration.search_fandom_person_related_pages(
            "angie katsanevas",
            community_domain="real-housewives.fandom.com",
            owner_page_url="https://real-housewives.fandom.com/wiki/Angie_Katsanevas",
            max_results=10,
        )

    assert any("sroffset=2" in url for url in api_calls)
    assert candidates == [
        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas",
        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Gallery",
        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Storylines",
        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Connections",
    ]


def test_search_fandom_allpages_html_candidates_follows_next_page() -> None:
    requested_urls: list[str] = []

    def _fetch_html(url: str, *, timeout: float = 20.0):
        requested_urls.append(url)
        if url.endswith("/wiki/Special:AllPages?from=Lisa&to=&namespace=0"):
            return (
                200,
                """
                <html>
                  <body>
                    <div class="mw-allpages-body">
                      <a href="/wiki/Lisa_Barlow/Gallery">Lisa Barlow/Gallery</a>
                    </div>
                    <div class="mw-allpages-nav">
                      <a href="/wiki/Special:AllPages?from=Erika_Jayne:_Bet_It_All_On_Blonde&to=&namespace=0">
                        Next page (Erika Jayne: Bet It All On Blonde)
                      </a>
                    </div>
                  </body>
                </html>
                """,
                "https://real-housewives.fandom.com/wiki/Special:AllPages?from=Lisa&to=&namespace=0",
                None,
            )
        if "Erika_Jayne:_Bet_It_All_On_Blonde" in url:
            return (
                200,
                """
                <html>
                  <body>
                    <div class="mw-allpages-body">
                      <a href="/wiki/Lisa_Barlow">Lisa Barlow</a>
                    </div>
                  </body>
                </html>
                """,
                "https://real-housewives.fandom.com/wiki/Special:AllPages?from=Erika_Jayne:_Bet_It_All_On_Blonde&to=&namespace=0",
                None,
            )
        return (404, "", url, None)

    with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
        candidates = admin_show_links._search_fandom_allpages_html_candidates(
            community_domain="real-housewives.fandom.com",
            query="Lisa Barlow",
            max_results=10,
        )

    assert any("Erika_Jayne:_Bet_It_All_On_Blonde" in url for url in requested_urls)
    assert "https://real-housewives.fandom.com/wiki/Lisa_Barlow" in candidates


def test_classify_submitted_link_input_rejects_traitors_fandom_url_on_wrong_domain() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors (American TV series)",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    rows, error = _classify_submitted_link_input("https://real-housewives.fandom.com/wiki/The_Traitors", context)
    assert rows == []
    assert error == "Fandom link is for a different community."


def test_add_show_links_keeps_only_canonical_person_fandom_url() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    person_record = {
        "id": person_id,
        "name": "Angie Katsanevas",
        "name_norm": "angie katsanevas",
        "imdb_id": None,
        "tmdb_id": None,
        "wikidata_id": None,
        "fandom_name_norm": "angie katsanevas",
    }
    context = {
        "show_id": show_id,
        "show_name": "The Real Housewives of Salt Lake City",
        "show_name_norm": "the real housewives of salt lake city",
        "show_imdb_id": None,
        "show_tmdb_id": None,
        "show_wikidata_id": None,
        "show_networks": ["bravo"],
        "is_bravo_show": True,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {person_id: person_record},
        "people_by_name": {"angie katsanevas": person_record},
        "people_by_slug": {"angie katsanevas": person_record},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    with patch("api.routers.admin_show_links._show_exists", return_value=True):
        with patch("api.routers.admin_show_links._load_show_link_classifier_context", return_value=context):
            with patch(
                "api.routers.admin_show_links._fetch_html_with_status",
                return_value=(
                    200,
                    """
                    <html>
                      <head><title>Angie Katsanevas | Real Housewives Wiki | Fandom</title></head>
                      <body><h1 class="page-header__title">Angie Katsanevas</h1></body>
                    </html>
                    """,
                    "https://real-housewives.fandom.com/wiki/Angie_Katsanevas",
                    None,
                ),
            ):
                with patch(
                    "api.routers.admin_show_links.search_fandom_person_related_pages",
                    return_value=[
                        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas",
                        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Gallery",
                        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Storylines",
                        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Connections",
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_links._validated_person_knowledge_url",
                        side_effect=lambda url, kind, expected_name=None, **kwargs: url if kind == "fandom" else None,
                    ):
                        with patch(
                            "api.routers.admin_show_links._upsert_link",
                            side_effect=lambda *args, **kwargs: {"id": str(uuid4())},
                        ):
                            with patch(
                                "api.routers.admin_show_links._build_shared_social_source_links",
                                return_value=[],
                            ):
                                with patch(
                                    "api.routers.admin_show_links._kickoff_show_link_discovery_for_manual_fandom_seed"
                                ):
                                    result = admin_show_links.add_show_links(
                                        UUID(show_id),
                                        admin_show_links.LinkBulkAddRequest(
                                            inputs=["https://real-housewives.fandom.com/wiki/Angie_Katsanevas"]
                                        ),
                                        MagicMock(),
                                        {"email": "admin@example.com"},
                                    )

    assert result["added"] == 1
    assert {assignment["url"] for assignment in result["assignments"]} == {
        "https://real-housewives.fandom.com/wiki/Angie_Katsanevas"
    }


def test_add_show_links_triggers_background_discovery_for_allowlisted_fandom_seed() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Real Housewives of Salt Lake City",
        "show_name_norm": "the real housewives of salt lake city",
        "show_imdb_id": None,
        "show_tmdb_id": None,
        "show_wikidata_id": None,
        "show_networks": ["bravo"],
        "is_bravo_show": True,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }
    kickoff_calls: list[dict[str, object]] = []
    backfill_calls: list[dict[str, object]] = []

    with patch("api.routers.admin_show_links._show_exists", return_value=True):
        with patch("api.routers.admin_show_links._load_show_link_classifier_context", return_value=context):
            with patch(
                "api.routers.admin_show_links._classify_submitted_link_input",
                return_value=(
                    [
                        {
                            "entity_type": "show",
                            "entity_id": show_id,
                            "season_number": 0,
                            "link_group": "knowledge",
                            "link_kind": "fandom",
                            "url": "https://real-housewives.fandom.com/",
                            "label": "Fandom",
                            "source": "manual_classifier",
                            "metadata": {},
                        }
                    ],
                    None,
                ),
            ):
                with patch(
                    "api.routers.admin_show_links._upsert_link",
                    side_effect=lambda *args, **kwargs: {"id": str(uuid4())},
                ):
                    with patch(
                        "api.routers.admin_show_links._build_shared_social_source_links",
                        return_value=[],
                    ):
                        with patch(
                            "api.routers.admin_show_links._kickoff_show_link_discovery_for_manual_fandom_seed",
                            side_effect=lambda **kwargs: kickoff_calls.append(kwargs),
                        ):
                            with patch(
                                "api.routers.admin_show_links.fandom_page_directory_repo.enqueue_fandom_page_directory_backfill",
                                side_effect=lambda **kwargs: backfill_calls.append(kwargs),
                            ):
                                result = admin_show_links.add_show_links(
                                    UUID(show_id),
                                    admin_show_links.LinkBulkAddRequest(inputs=["https://real-housewives.fandom.com/"]),
                                    MagicMock(),
                                    {"email": "admin@example.com"},
                                )

    assert result["added"] == 1
    assert len(kickoff_calls) == 1
    assert kickoff_calls[0]["show_id"] == show_id
    assert kickoff_calls[0]["actor"] == "admin@example.com"
    assert kickoff_calls[0]["inputs"] == ["https://real-housewives.fandom.com/"]
    assert len(backfill_calls) == 1
    assert backfill_calls[0]["community_domain"] == "real-housewives.fandom.com"


def test_add_show_links_restores_shared_social_source_links_for_bravo_shows() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Real Housewives of Salt Lake City",
        "show_name_norm": "the real housewives of salt lake city",
        "show_imdb_id": None,
        "show_tmdb_id": None,
        "show_wikidata_id": None,
        "show_networks": ["bravo"],
        "is_bravo_show": True,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    with patch("api.routers.admin_show_links._show_exists", return_value=True):
        with patch("api.routers.admin_show_links._load_show_link_classifier_context", return_value=context):
            with patch(
                "api.routers.admin_show_links._classify_submitted_link_input",
                return_value=(
                    [
                        {
                            "entity_type": "show",
                            "entity_id": show_id,
                            "season_number": 0,
                            "link_group": "knowledge",
                            "link_kind": "wikipedia",
                            "url": "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City",
                            "label": "Wikipedia",
                            "source": "manual_classifier",
                            "metadata": {},
                        }
                    ],
                    None,
                ),
            ):
                with patch(
                    "api.routers.admin_show_links._build_shared_social_source_links",
                    return_value=[
                        {
                            "entity_type": "show",
                            "entity_id": show_id,
                            "season_number": 0,
                            "link_group": "social",
                            "link_kind": "instagram",
                            "label": "Instagram",
                            "url": "https://www.instagram.com/bravotv",
                            "source": "social.shared_account_sources",
                            "metadata": {"shared_account_source_scope": "bravo"},
                        },
                        {
                            "entity_type": "show",
                            "entity_id": show_id,
                            "season_number": 0,
                            "link_group": "social",
                            "link_kind": "youtube",
                            "label": "YouTube",
                            "url": "https://www.youtube.com/@bravo",
                            "source": "social.shared_account_sources",
                            "metadata": {"shared_account_source_scope": "bravo"},
                        },
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_links._upsert_link",
                        side_effect=lambda *args, **kwargs: {"id": str(uuid4())},
                    ):
                        result = admin_show_links.add_show_links(
                            UUID(show_id),
                            admin_show_links.LinkBulkAddRequest(
                                inputs=["https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City"]
                            ),
                            MagicMock(),
                            {"email": "admin@example.com"},
                        )

    assert result["added"] == 1
    assert result["network_default_links_upserted"] == 2


def test_add_show_links_does_not_trigger_background_discovery_for_non_fandom_links() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Real Housewives of Salt Lake City",
        "show_name_norm": "the real housewives of salt lake city",
        "show_imdb_id": None,
        "show_tmdb_id": None,
        "show_wikidata_id": None,
        "show_networks": ["bravo"],
        "is_bravo_show": True,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    with patch("api.routers.admin_show_links._show_exists", return_value=True):
        with patch("api.routers.admin_show_links._load_show_link_classifier_context", return_value=context):
            with patch(
                "api.routers.admin_show_links._classify_submitted_link_input",
                return_value=(
                    [
                        {
                            "entity_type": "show",
                            "entity_id": show_id,
                            "season_number": 0,
                            "link_group": "knowledge",
                            "link_kind": "wikipedia",
                            "url": "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City",
                            "label": "Wikipedia",
                            "source": "manual_classifier",
                            "metadata": {},
                        }
                    ],
                    None,
                ),
            ):
                with patch(
                    "api.routers.admin_show_links._upsert_link",
                    side_effect=lambda *args, **kwargs: {"id": str(uuid4())},
                ):
                    with patch(
                        "api.routers.admin_show_links._build_shared_social_source_links",
                        return_value=[],
                    ):
                        with patch(
                            "api.routers.admin_show_links._kickoff_show_link_discovery_for_manual_fandom_seed"
                        ) as kickoff:
                            result = admin_show_links.add_show_links(
                                UUID(show_id),
                                admin_show_links.LinkBulkAddRequest(
                                    inputs=["https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City"]
                                ),
                                MagicMock(),
                                {"email": "admin@example.com"},
                            )

    assert result["added"] == 1
    kickoff.assert_not_called()


def test_backfill_fandom_link_discovery_filters_to_allowlisted_seed_shows() -> None:
    rows = [
        {
            "show_id": "show-1",
            "url": "https://real-housewives.fandom.com/",
        },
        {
            "show_id": "show-1",
            "url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
        },
        {
            "show_id": "show-2",
            "url": "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City",
        },
        {
            "show_id": "show-3",
            "url": "https://not-allowlisted.fandom.com/wiki/Show",
        },
    ]

    assert backfill_fandom_link_discovery.filter_allowlisted_fandom_seed_show_ids(rows) == ["show-1"]


def test_classify_submitted_link_input_recognizes_tvdb_urls() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    rows, error = _classify_submitted_link_input("https://www.thetvdb.com/series/428163", context)
    assert error is None
    assert rows[0]["entity_type"] == "show"
    assert rows[0]["link_group"] == "knowledge"
    assert rows[0]["link_kind"] == "tvdb"


def test_classify_submitted_link_input_recognizes_tvmaze_urls() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    rows, error = _classify_submitted_link_input("https://www.tvmaze.com/shows/58177/the-traitors-us", context)
    assert error is None
    assert rows[0]["entity_type"] == "show"
    assert rows[0]["link_group"] == "knowledge"
    assert rows[0]["link_kind"] == "tvmaze"


def test_classify_submitted_link_input_recognizes_ratinggraph_urls() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    rows, error = _classify_submitted_link_input(
        "https://www.ratingraph.com/tv-shows/the-traitors-ratings-103483",
        context,
    )
    assert error is None
    assert rows[0]["entity_type"] == "show"
    assert rows[0]["link_group"] == "knowledge"
    assert rows[0]["link_kind"] == "ratinggraph"


def test_classify_submitted_link_input_adds_connected_external_ids_from_wikidata_summary() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": None,
        "show_tmdb_id": None,
        "show_wikidata_id": "Q116449538",
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    with patch(
        "api.routers.admin_show_links._fetch_wikidata_summary",
        return_value=(
            {
                "item_id": "Q116449538",
                "label": "The Traitors",
                "enwiki_title": "The Traitors (American TV series)",
                "enwiki_url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                "imdb_id": "tt15557874",
                "tmdb_tv_id": "215943",
                "tmdb_person_id": "",
                "tvdb_id": "428163",
                "ratinggraph_tv_show_id": "the-traitors-ratings-103483",
                "trakt_id": "shows/the-traitors-us",
                "x_topic_id": "1742326119434545480",
            },
            False,
        ),
    ):
        rows, error = _classify_submitted_link_input("https://www.wikidata.org/wiki/Q116449538", context)

    assert error is None
    by_kind = {str(row.get("link_kind") or ""): str(row.get("url") or "") for row in rows}
    assert by_kind["wikidata"] == "https://www.wikidata.org/wiki/Q116449538"
    assert by_kind["wikipedia"] == "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)"
    assert by_kind["imdb"] == "https://www.imdb.com/title/tt15557874"
    assert by_kind["tmdb"] == "https://www.themoviedb.org/tv/215943"
    assert by_kind["tvdb"] == "https://www.thetvdb.com/series/428163"
    assert by_kind["ratinggraph"] == "https://www.ratingraph.com/tv-shows/the-traitors-ratings-103483"
    assert by_kind["trakt"] == "https://trakt.tv/shows/the-traitors-us"
    assert by_kind["x_topic"] == "https://x.com/i/topics/1742326119434545480"


def test_fetch_wikidata_summary_includes_presenter_in_cast_item_ids() -> None:
    qid = "Q99999999"
    payload = {
        "entities": {
            qid: {
                "labels": {"en": {"value": "Sample item"}},
                "sitelinks": {},
                "claims": {
                    "P161": [
                        {
                            "mainsnak": {
                                "snaktype": "value",
                                "datavalue": {"value": {"id": "Q123"}},
                            }
                        }
                    ],
                    "P371": [
                        {
                            "mainsnak": {
                                "snaktype": "value",
                                "datavalue": {"value": {"id": "Q456"}},
                            }
                        }
                    ],
                    "P179": [
                        {
                            "mainsnak": {
                                "snaktype": "value",
                                "datavalue": {"value": {"id": "Q789"}},
                            }
                        }
                    ],
                    "P12397": [
                        {
                            "mainsnak": {
                                "snaktype": "value",
                                "datavalue": {"value": "2033944"},
                            }
                        }
                    ],
                    "P646": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "/m/01qwz"}}},
                    ],
                    "P11194": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "alan-cumming"}}},
                    ],
                    "P2671": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "/g/11c5s8ty6j"}}},
                    ],
                    "P8013": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "shows/the-traitors-us"}}},
                    ],
                    "P8672": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "1742326119434545480"}}},
                    ],
                    "P2002": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "alan_cumming"}}},
                    ],
                    "P2003": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "alancummingreally"}}},
                    ],
                    "P2013": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "alancumming"}}},
                    ],
                    "P2397": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "UC12345"}}},
                    ],
                    "P7085": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "alancumming"}}},
                    ],
                    "P4265": [
                        {"mainsnak": {"snaktype": "value", "datavalue": {"value": "alan_cumming"}}},
                    ],
                },
            }
        }
    }

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(payload).encode("utf-8")

    with patch("api.routers.admin_show_links.urllib.request.urlopen", return_value=_FakeResponse()):
        admin_show_links._fetch_wikidata_summary.cache_clear()
        summary, fetch_error = admin_show_links._fetch_wikidata_summary(qid)
        admin_show_links._fetch_wikidata_summary.cache_clear()

    assert fetch_error is False
    assert summary is not None
    assert summary.get("cast_item_ids") == ["Q123", "Q456"]
    assert summary.get("part_of_series_item_ids") == ["Q789"]
    assert summary.get("tvdb_season_id") == "2033944"
    assert summary.get("freebase_id") == "/m/01qwz"
    assert summary.get("famous_birthdays_id") == "alan-cumming"
    assert summary.get("google_kg_id") == "/g/11c5s8ty6j"
    assert summary.get("trakt_id") == "shows/the-traitors-us"
    assert summary.get("x_topic_id") == "1742326119434545480"
    assert summary.get("twitter_usernames") == ["alan_cumming"]
    assert summary.get("instagram_usernames") == ["alancummingreally"]
    assert summary.get("facebook_usernames") == ["alancumming"]
    assert summary.get("youtube_channel_ids") == ["UC12345"]
    assert summary.get("tiktok_usernames") == ["alancumming"]
    assert summary.get("reddit_usernames") == ["alan_cumming"]


def test_curated_show_fandom_domains_match_traitors_us_name_variants() -> None:
    domains = admin_show_links._curated_show_fandom_domains("The Traitors (American TV series)")
    assert "thetraitors.fandom.com" in domains
    assert "thetraitorsuk.fandom.com" in domains


def test_classify_submitted_link_input_rejects_bravotv_links_for_non_bravo_show() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    rows, error = _classify_submitted_link_input("https://www.bravotv.com/the-traitors", context)
    assert rows == []
    assert error == "BravoTV links are only allowed for Bravo-network shows."


def test_classify_submitted_link_input_rejects_missing_wikipedia_article() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": "Q147711660",
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    with patch(
        "api.routers.admin_show_links._resolve_wikipedia_url",
        return_value=(None, None, "missing"),
    ):
        rows, error = _classify_submitted_link_input(
            "https://en.wikipedia.org/wiki/The_Traitors_(US_fake_page)",
            context,
        )

    assert rows == []
    assert error == "Wikipedia does not have an article with this exact name."


def test_classify_submitted_link_input_rejects_mismatched_show_wikipedia_variant() -> None:
    show_id = str(uuid4())
    context = {
        "show_id": show_id,
        "show_name": "The Traitors",
        "show_name_norm": "the traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": "Q147711660",
        "show_networks": ["peacock"],
        "is_bravo_show": False,
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_id": {},
        "people_by_name": {},
        "people_by_slug": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
    }

    with patch(
        "api.routers.admin_show_links._resolve_wikipedia_url",
        return_value=("https://en.wikipedia.org/wiki/The_Traitors", "The Traitors", None),
    ):
        with patch("api.routers.admin_show_links._resolve_wikipedia_wikidata_id", return_value="Q111195888"):
            rows, error = _classify_submitted_link_input("https://en.wikipedia.org/wiki/The_Traitors", context)

    assert rows == []
    assert error == "Wikipedia link points to a different show/version."


def test_sync_show_wikipedia_links_updates_auto_derived_season_links() -> None:
    show_id = str(uuid4())
    auto_season_link_id = str(uuid4())
    manual_season_link_id = str(uuid4())
    season_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
        fetch_all.side_effect = [
            [
                {
                    "id": auto_season_link_id,
                    "season_number": 2,
                    "season_id": season_id,
                    "source": "derived",
                    "discovered_by": "backend_discovery",
                },
                {
                    "id": manual_season_link_id,
                    "season_number": 2,
                    "season_id": season_id,
                    "source": "manual",
                    "discovered_by": "manual",
                },
            ],
            [
                {
                    "season_id": season_id,
                    "season_number": 2,
                    "external_wikidata_id": None,
                }
            ],
        ]
        with patch("api.routers.admin_show_links.pg.execute_returning") as execute_returning:
            with patch(
                "api.routers.admin_show_links._fetch_wikipedia_page_summary",
                return_value=(
                    {
                        "title": "The Traitors (American TV series)",
                        "url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                    },
                    False,
                ),
            ):
                with patch("api.routers.admin_show_links._resolve_wikidata_enwiki_url", return_value=None):
                    _sync_show_wikipedia_links(
                        show_id=show_id,
                        show_wikipedia_url="https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                        actor="admin@example.com",
                        exclude_link_id=None,
                    )

    assert execute_returning.call_count == 2
    show_update_params = execute_returning.call_args_list[0].args[1]
    assert show_update_params[0] == "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)"
    season_update_params = execute_returning.call_args_list[1].args[1]
    assert season_update_params[0] == "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)_season_2"
    assert season_update_params[3] == auto_season_link_id
    assert season_update_params[4] == show_id


def test_patch_show_link_cascades_when_show_wikipedia_is_updated() -> None:
    show_id = uuid4()
    link_id = uuid4()
    db = MagicMock()

    select_chain = MagicMock()
    select_chain.eq.return_value = select_chain
    select_chain.limit.return_value = select_chain
    select_chain.execute.return_value = MagicMock()

    update_chain = MagicMock()
    update_chain.eq.return_value = update_chain
    update_chain.execute.return_value = MagicMock()

    table_chain = MagicMock()
    table_chain.select.return_value = select_chain
    table_chain.update.return_value = update_chain
    db.schema.return_value.table.return_value = table_chain

    current_row = {
        "id": str(link_id),
        "show_id": str(show_id),
        "entity_type": "show",
        "link_kind": "wikipedia",
        "url": "https://en.wikipedia.org/wiki/The_Traitors",
    }
    updated_row = {
        **current_row,
        "url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
        "url_key": "https://en.wikipedia.org/wiki/the_traitors_(american_tv_series)",
    }

    with patch(
        "api.routers.admin_show_links.get_list_result",
        side_effect=[[current_row], [updated_row]],
    ):
        with patch(
            "api.routers.admin_show_links._resolve_wikipedia_url",
            return_value=(
                "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                "The Traitors (American TV series)",
                None,
            ),
        ):
            with patch("api.routers.admin_show_links._load_show_wikidata_id", return_value=None):
                with patch("api.routers.admin_show_links._sync_show_wikipedia_links") as sync_links:
                    result = admin_show_links.patch_show_link(
                        show_id=show_id,
                        link_id=link_id,
                        payload=admin_show_links.LinkPatchRequest(
                            url="https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)"
                        ),
                        db=db,
                        admin={"email": "admin@example.com"},
                    )

    assert result["url"] == "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)"
    sync_links.assert_called_once_with(
        show_id=str(show_id),
        show_wikipedia_url="https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
        actor="admin@example.com",
        exclude_link_id=str(link_id),
    )


def test_patch_show_link_rejects_missing_show_wikipedia_article() -> None:
    show_id = uuid4()
    link_id = uuid4()
    db = MagicMock()

    select_chain = MagicMock()
    select_chain.eq.return_value = select_chain
    select_chain.limit.return_value = select_chain
    select_chain.execute.return_value = MagicMock()
    db.schema.return_value.table.return_value.select.return_value = select_chain

    current_row = {
        "id": str(link_id),
        "show_id": str(show_id),
        "entity_type": "show",
        "link_kind": "wikipedia",
        "url": "https://en.wikipedia.org/wiki/The_Traitors",
    }

    with patch("api.routers.admin_show_links.get_list_result", return_value=[current_row]):
        with patch(
            "api.routers.admin_show_links._resolve_wikipedia_url",
            return_value=(None, None, "missing"),
        ):
            with pytest.raises(admin_show_links.HTTPException) as exc:
                admin_show_links.patch_show_link(
                    show_id=show_id,
                    link_id=link_id,
                    payload=admin_show_links.LinkPatchRequest(
                        url="https://en.wikipedia.org/wiki/The_Traitors_(US_fake_page)"
                    ),
                    db=db,
                    admin={"email": "admin@example.com"},
                )

    assert exc.value.status_code == 400
    assert exc.value.detail == "Wikipedia does not have an article with this exact name."


def test_canonicalize_url_normalizes_host_scheme_port_fragment_and_trailing_slash() -> None:
    assert _canonicalize_url("HTTPS://WWW.IMDB.COM:443/name/nm1234567/#bio") == "https://www.imdb.com/name/nm1234567"
    assert _canonicalize_url("http://example.com:80/path/") == "http://example.com/path"
    assert (
        _canonicalize_url("https://en.wikipedia.org/wiki/The_Traitors_%28American_TV_series%29")
        == "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)"
    )
    assert _canonicalize_url("https://www.tiktok.com/%40BravoTV/") == "https://www.tiktok.com/@BravoTV"


def test_normalize_link_kind_maps_wikia_to_fandom() -> None:
    assert _normalize_link_kind("wikia") == "fandom"
    assert _normalize_link_kind("FANDOM") == "fandom"


def test_source_timeout_seconds_uses_env_override_with_fallback() -> None:
    with patch.dict("os.environ", {"TRR_LINK_TIMEOUT_IMDB_SECONDS": "12.5"}, clear=False):
        assert _source_timeout_seconds("imdb", default=20.0) == 12.5
    with patch.dict("os.environ", {"TRR_LINK_TIMEOUT_IMDB_SECONDS": "invalid"}, clear=False):
        assert _source_timeout_seconds("imdb", default=20.0) == 20.0


def test_upsert_link_uses_show_scoped_conflict_key() -> None:
    db = MagicMock()
    execute_response = MagicMock()
    db.schema.return_value.table.return_value.upsert.return_value.execute.return_value = execute_response

    with patch("api.routers.admin_show_links.get_list_result", return_value=[{"id": "link-1"}]):
        row = admin_show_links._upsert_link(
            db,
            show_id=str(uuid4()),
            entity_type="person",
            entity_id=str(uuid4()),
            link_group="knowledge",
            link_kind="imdb",
            url="https://www.imdb.com/name/nm0169212/",
            label="IMDb",
            season_number=0,
            status="approved",
            confidence=0.99,
            source="test",
            discovered_by="test",
            metadata={},
            actor="test",
        )

    assert row["id"] == "link-1"
    upsert_call = db.schema.return_value.table.return_value.upsert.call_args
    assert upsert_call.kwargs["on_conflict"] == "show_id,entity_type,entity_id,link_kind,season_number,url_key"


def test_get_fandom_allowlist_requires_auth(client: TestClient) -> None:
    response = client.get("/api/v1/admin/fandom/allowlist")
    assert response.status_code == 401


def test_get_fandom_allowlist_returns_domains_with_source(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    with patch(
        "api.routers.admin_show_links.load_fandom_community_allowlist_with_source",
        return_value=(("real-housewives.fandom.com", "starwars.fandom.com"), "database"),
    ):
        response = client.get(
            "/api/v1/admin/fandom/allowlist",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["domains"] == ["real-housewives.fandom.com", "starwars.fandom.com"]
    assert payload["source"] == "database"
    assert payload["count"] == 2


def test_put_fandom_allowlist_rejects_invalid_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    response = client.put(
        "/api/v1/admin/fandom/allowlist",
        headers={"Authorization": f"Bearer {token}"},
        json={"domains": ["", "invalid host"]},
    )
    assert response.status_code == 400
    assert "At least one valid fandom domain is required" in str(response.json())


def test_put_fandom_allowlist_normalizes_dedupes_and_refreshes_cache(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    cursor = MagicMock()

    with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
        with patch("api.routers.admin_show_links.pg.db_cursor", return_value=nullcontext(cursor)):
            with patch("api.routers.admin_show_links.refresh_fandom_community_allowlist_cache") as refresh_cache:
                with patch(
                    "api.routers.admin_show_links.fandom_page_directory_repo.enqueue_fandom_page_directory_backfill"
                ) as enqueue_backfill:
                    response = client.put(
                        "/api/v1/admin/fandom/allowlist",
                        headers={"Authorization": f"Bearer {token}"},
                        json={
                            "domains": [
                                "https://real-housewives.fandom.com/wiki/Andy_Cohen",
                                "REAL-HOUSEWIVES.FANDOM.COM",
                                " starwars.fandom.com ",
                            ]
                        },
                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["domains"] == ["real-housewives.fandom.com", "starwars.fandom.com"]
    assert payload["count"] == 2
    refresh_cache.assert_called_once()
    assert enqueue_backfill.call_count == 2
    # One bulk deactivate + one upsert per normalized domain.
    assert cursor.execute.call_count == 3


def test_discover_people_links_adds_bravo_profile_for_housewife_friend_on_bravo_show() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    eligible_people_rows = [{"person_id": person_id}]
    person_rows = [
        {
            "id": person_id,
            "full_name": "Lisa Barlow",
            "external_ids": {},
            "fandom_url": "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
        }
    ]

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.return_value = {"networks": ["bravo"]}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:

            def _fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
                assert params == [show_id]
                if "FROM core.v_show_cast sc" in query:
                    return person_rows
                if "FROM core.entity_links" in query:
                    return []
                raise AssertionError(query)

            fetch_all.side_effect = _fetch_all
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=(eligible_people_rows, 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validated_or_carried_person_source_url",
                    side_effect=lambda person_id, candidate_url, kind, expected_name=None, **kwargs: candidate_url,
                ):
                    with patch(
                        "api.routers.admin_show_links._validated_person_knowledge_url",
                        side_effect=lambda url, kind, expected_name=None, **kwargs: url,
                    ):
                        with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                            with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                                links = _discover_people_links(show_id)

    assert any(link.get("link_kind") == "bravo_profile" for link in links)
    assert any(link.get("url") == "https://www.bravotv.com/people/lisa-barlow" for link in links)


def test_discover_people_links_adds_bravo_profile_for_any_cast_member_on_bravo_show() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    eligible_people_rows = [{"person_id": person_id}]
    person_rows = [
        {
            "id": person_id,
            "full_name": "Kyle Cooke",
            "external_ids": {},
            "fandom_url": "",
            "cast_tmdb_imdb_id": None,
            "cast_tmdb_tmdb_id": None,
            "cast_tmdb_wikidata_id": None,
            "cast_tmdb_facebook_id": None,
            "cast_tmdb_instagram_id": None,
            "cast_tmdb_tiktok_id": None,
            "cast_tmdb_twitter_id": None,
            "cast_tmdb_youtube_id": None,
            "cast_tmdb_freebase_id": None,
            "cast_tmdb_freebase_mid": None,
        }
    ]

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.return_value = {"name": "Summer House", "networks": ["bravo"], "wikidata_id": None}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:

            def _fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
                assert params == [show_id]
                if "FROM core.show_cast_role_assignments" in query:
                    return []
                if "FROM core.v_show_cast sc" in query:
                    return person_rows
                if "FROM core.entity_links" in query:
                    return []
                raise AssertionError(query)

            fetch_all.side_effect = _fetch_all
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=(eligible_people_rows, 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validated_person_knowledge_url",
                    side_effect=lambda url, kind, expected_name=None, **kwargs: url,
                ):
                    with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                        with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                            links = _discover_people_links(show_id)

    assert any(link.get("link_kind") == "bravo_profile" for link in links)
    assert any(link.get("url") == "https://www.bravotv.com/people/kyle-cooke" for link in links)


def test_discover_people_links_adds_featured_image_metadata_for_bravo_profile() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    eligible_people_rows = [{"person_id": person_id}]
    person_rows = [
        {
            "id": person_id,
            "full_name": "Kyle Cooke",
            "external_ids": {},
            "fandom_url": "",
            "cast_tmdb_imdb_id": None,
            "cast_tmdb_tmdb_id": None,
            "cast_tmdb_wikidata_id": None,
            "cast_tmdb_facebook_id": None,
            "cast_tmdb_instagram_id": None,
            "cast_tmdb_tiktok_id": None,
            "cast_tmdb_twitter_id": None,
            "cast_tmdb_youtube_id": None,
            "cast_tmdb_freebase_id": None,
            "cast_tmdb_freebase_mid": None,
        }
    ]

    with patch(
        "api.routers.admin_show_links.pg.fetch_one", return_value={"name": "Summer House", "networks": ["bravo"]}
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:

            def _fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
                assert params == [show_id]
                if "FROM core.v_show_cast sc" in query:
                    return person_rows
                if "FROM core.entity_links" in query:
                    return []
                raise AssertionError(query)

            fetch_all.side_effect = _fetch_all
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=(eligible_people_rows, 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validated_person_knowledge_url",
                    side_effect=lambda url, kind, expected_name=None, **kwargs: url,
                ):
                    with patch(
                        "api.routers.admin_show_links._build_person_page_link_metadata",
                        return_value={"featured_image_url": "https://cdn.example.com/kyle-cooke.jpg"},
                    ) as build_metadata:
                        with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                            with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                                links = _discover_people_links(show_id)

    bravo_link = next(link for link in links if link.get("link_kind") == "bravo_profile")
    assert bravo_link["metadata"] == {"featured_image_url": "https://cdn.example.com/kyle-cooke.jpg"}
    build_metadata.assert_any_call(
        "https://www.bravotv.com/people/kyle-cooke",
        kind="bravo_profile",
    )


def test_discover_people_links_adds_featured_image_metadata_for_fandom_pages() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    eligible_people_rows = [{"person_id": person_id}]
    fandom_url = "https://real-housewives.fandom.com/wiki/Lisa_Barlow"
    person_rows = [
        {
            "id": person_id,
            "full_name": "Lisa Barlow",
            "external_ids": {},
            "fandom_url": fandom_url,
            "cast_tmdb_imdb_id": None,
            "cast_tmdb_tmdb_id": None,
            "cast_tmdb_wikidata_id": None,
            "cast_tmdb_facebook_id": None,
            "cast_tmdb_instagram_id": None,
            "cast_tmdb_tiktok_id": None,
            "cast_tmdb_twitter_id": None,
            "cast_tmdb_youtube_id": None,
            "cast_tmdb_freebase_id": None,
            "cast_tmdb_freebase_mid": None,
        }
    ]

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"name": "RHOSLC", "networks": ["bravo"]}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:

            def _fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
                assert params == [show_id]
                if "FROM core.v_show_cast sc" in query:
                    return person_rows
                if "FROM core.entity_links" in query:
                    return []
                raise AssertionError(query)

            fetch_all.side_effect = _fetch_all
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=(eligible_people_rows, 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validated_person_knowledge_url",
                    side_effect=lambda url, kind, expected_name=None, **kwargs: url,
                ):
                    with patch(
                        "api.routers.admin_show_links._build_person_page_link_metadata",
                        side_effect=lambda url, *, kind: {
                            "featured_image_url": "https://cdn.example.com/lisa-barlow.jpg"
                        }
                        if kind == "fandom"
                        else {},
                    ) as build_metadata:
                        with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                            with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                                links = _discover_people_links(show_id)

    fandom_link = next(link for link in links if link.get("link_kind") == "fandom")
    assert fandom_link["metadata"]["featured_image_url"] == "https://cdn.example.com/lisa-barlow.jpg"
    build_metadata.assert_any_call(fandom_url, kind="fandom")


def test_discover_people_links_skips_missing_wikipedia_and_fandom_pages() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    eligible_people_rows = [{"person_id": person_id}]
    person_rows = [
        {
            "id": person_id,
            "full_name": "Georgia Gay",
            "external_ids": {},
            "fandom_url": "https://real-housewives.fandom.com/wiki/Georgia_Gay",
        }
    ]

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.return_value = {"networks": ["bravo"]}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:

            def _fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
                assert params == [show_id]
                if "FROM core.v_show_cast sc" in query:
                    return person_rows
                if "FROM core.entity_links" in query:
                    return []
                raise AssertionError(query)

            fetch_all.side_effect = _fetch_all

            def _validate(
                url: str,
                kind: str,
                expected_name: str | None = None,
                **kwargs,
            ) -> str | None:
                if kind == "bravo_profile":
                    return url
                return None

            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=(eligible_people_rows, 1),
            ):
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
                [],
            ]
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=([{"person_id": person_id}], 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validated_or_carried_person_source_url",
                    side_effect=lambda person_id, candidate_url, kind, expected_name=None, **kwargs: candidate_url,
                ):
                    with patch(
                        "api.routers.admin_show_links._validated_person_knowledge_url",
                        side_effect=lambda url, kind, expected_name=None, **kwargs: url,
                    ):
                        with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                            with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                                links = _discover_people_links(show_id)

    imdb_links = [link for link in links if link.get("link_kind") == "imdb"]
    tmdb_links = [link for link in links if link.get("link_kind") == "tmdb"]
    assert len(imdb_links) == 1
    assert imdb_links[0]["url"] == "https://www.imdb.com/name/nm1234567"
    assert imdb_links[0]["source"] == "core.people.external_ids"
    assert imdb_links[0]["status"] == "approved"
    assert imdb_links[0]["link_group"] == "knowledge"
    assert len(tmdb_links) == 1
    assert tmdb_links[0]["url"] == "https://www.themoviedb.org/person/98765"
    assert tmdb_links[0]["source"] == "core.cast_tmdb"
    assert tmdb_links[0]["status"] == "approved"
    assert tmdb_links[0]["link_group"] == "knowledge"


def test_discover_people_links_can_target_single_person() -> None:
    show_id = str(uuid4())
    selected_person_id = str(uuid4())
    other_person_id = str(uuid4())
    person_rows = [
        {
            "id": selected_person_id,
            "full_name": "Heather Gay",
            "external_ids": {"imdb": "nm1234567"},
            "fandom_url": "",
            "cast_tmdb_imdb_id": "nm1234567",
            "cast_tmdb_tmdb_id": 12345,
            "cast_tmdb_wikidata_id": None,
        },
        {
            "id": other_person_id,
            "full_name": "Whitney Rose",
            "external_ids": {"imdb": "nm7654321"},
            "fandom_url": "",
            "cast_tmdb_imdb_id": "nm7654321",
            "cast_tmdb_tmdb_id": 54321,
            "cast_tmdb_wikidata_id": None,
        },
    ]

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"networks": ["peacock"]}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = lambda query, params: person_rows if "FROM core.v_show_cast sc" in query else []
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=([{"person_id": selected_person_id}], 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validated_or_carried_person_source_url",
                    side_effect=lambda person_id, candidate_url, kind, expected_name=None, **kwargs: candidate_url,
                ):
                    with patch(
                        "api.routers.admin_show_links._validated_person_knowledge_url",
                        side_effect=lambda url, kind, expected_name=None, **kwargs: url,
                    ):
                        with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                            with patch("api.routers.admin_show_links.search_allowlisted_fandom_wikis", return_value=[]):
                                links = _discover_people_links(show_id, person_ids={selected_person_id})

    assert links
    assert {str(link.get("entity_id")) for link in links} == {selected_person_id}


def test_discover_people_links_emits_social_links_from_cast_tmdb_fields() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "networks": ["peacock"], "wikidata_id": None},
    ):
        with patch("api.routers.admin_show_links.load_fandom_community_allowlist", return_value=[]):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [
                        {
                            "id": person_id,
                            "full_name": "Alan Cumming",
                            "external_ids": {"tmdb_id": 5190},
                            "fandom_url": "",
                            "cast_tmdb_imdb_id": None,
                            "cast_tmdb_tmdb_id": 5190,
                            "cast_tmdb_wikidata_id": None,
                            "cast_tmdb_facebook_id": "alancumming",
                            "cast_tmdb_instagram_id": "alancummingreally",
                            "cast_tmdb_tiktok_id": "alancumming",
                            "cast_tmdb_twitter_id": "alan_cumming",
                            "cast_tmdb_youtube_id": "UC12345",
                            "cast_tmdb_freebase_id": "/m/01qwz",
                            "cast_tmdb_freebase_mid": None,
                        }
                    ],
                    [],
                ]
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], 1),
                ):
                    with patch(
                        "api.routers.admin_show_links._validated_or_carried_person_source_url",
                        side_effect=lambda person_id, candidate_url, kind, expected_name=None, **kwargs: candidate_url,
                    ):
                        with patch(
                            "api.routers.admin_show_links._validated_person_knowledge_url",
                            side_effect=lambda url, kind, expected_name=None, **kwargs: (
                                url if kind == "wikipedia" else None
                            ),
                        ):
                            with patch(
                                "api.routers.admin_show_links._validated_person_social_url",
                                side_effect=lambda url, kind: url,
                            ):
                                with patch(
                                    "api.routers.admin_show_links.search_real_housewives_wiki", return_value=None
                                ):
                                    with patch(
                                        "api.routers.admin_show_links.search_allowlisted_fandom_wikis",
                                        return_value=[],
                                    ):
                                        links = _discover_people_links(show_id)

    social_links = [link for link in links if link.get("link_group") == "social"]
    social_kinds = {str(link.get("link_kind") or "") for link in social_links}
    assert {"twitter", "instagram", "facebook", "youtube", "tiktok"}.issubset(social_kinds)
    assert all(str(link.get("source") or "") == "core.cast_tmdb_social_ids" for link in social_links)
    assert any(link.get("link_kind") == "freebase" for link in links)


def test_discover_people_links_fetches_tmdb_external_ids_when_missing_social_fields() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "networks": ["peacock"], "wikidata_id": None},
    ):
        with patch("api.routers.admin_show_links.load_fandom_community_allowlist", return_value=[]):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [
                        {
                            "id": person_id,
                            "full_name": "Alan Cumming",
                            "external_ids": {"tmdb_id": 5190},
                            "fandom_url": "",
                            "cast_tmdb_imdb_id": None,
                            "cast_tmdb_tmdb_id": 5190,
                            "cast_tmdb_wikidata_id": None,
                            "cast_tmdb_facebook_id": None,
                            "cast_tmdb_instagram_id": None,
                            "cast_tmdb_tiktok_id": None,
                            "cast_tmdb_twitter_id": None,
                            "cast_tmdb_youtube_id": None,
                            "cast_tmdb_freebase_id": None,
                            "cast_tmdb_freebase_mid": None,
                        }
                    ],
                    [],
                ]
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], 1),
                ):
                    with patch(
                        "api.routers.admin_show_links._fetch_tmdb_external_ids_payload",
                        return_value={
                            "imdb_id": "nm0001086",
                            "wikidata_id": "Q316629",
                            "freebase_id": "",
                            "freebase_mid": "",
                            "facebook_id": "",
                            "instagram_id": "alancummingreally",
                            "tiktok_id": "",
                            "twitter_id": "alan_cumming",
                            "youtube_id": "",
                        },
                    ):
                        with patch(
                            "api.routers.admin_show_links._persist_tmdb_external_ids_for_person"
                        ) as persist_tmdb:
                            with patch(
                                "api.routers.admin_show_links._validated_or_carried_person_source_url",
                                side_effect=(
                                    lambda person_id, candidate_url, kind, expected_name=None, **kwargs: candidate_url
                                ),
                            ):
                                with patch(
                                    "api.routers.admin_show_links._validated_person_knowledge_url",
                                    side_effect=lambda url, kind, expected_name=None, **kwargs: (
                                        url if kind == "wikipedia" else None
                                    ),
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._validated_person_social_url",
                                        side_effect=lambda url, kind: url,
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links._fetch_wikidata_summary",
                                            return_value=(None, True),
                                        ):
                                            with patch(
                                                "api.routers.admin_show_links.search_real_housewives_wiki",
                                                return_value=None,
                                            ):
                                                with patch(
                                                    "api.routers.admin_show_links.search_allowlisted_fandom_wikis",
                                                    return_value=[],
                                                ):
                                                    links = _discover_people_links(show_id)

    persist_tmdb.assert_called_once_with(
        person_id,
        "5190",
        {
            "imdb_id": "nm0001086",
            "wikidata_id": "Q316629",
            "freebase_id": "",
            "freebase_mid": "",
            "facebook_id": "",
            "instagram_id": "alancummingreally",
            "tiktok_id": "",
            "twitter_id": "alan_cumming",
            "youtube_id": "",
        },
    )
    assert any(
        link.get("link_kind") == "imdb"
        and link.get("source") == "tmdb_external_ids"
        and link.get("url") == "https://www.imdb.com/name/nm0001086"
        for link in links
    )
    assert any(
        link.get("link_kind") == "twitter"
        and link.get("source") == "tmdb_external_ids_social"
        and link.get("url") == "https://x.com/alan_cumming"
        for link in links
    )
    assert any(
        link.get("link_kind") == "instagram"
        and link.get("source") == "tmdb_external_ids_social"
        and link.get("url") == "https://www.instagram.com/alancummingreally"
        for link in links
    )


def test_discover_people_links_fandom_fallback_uses_allowlisted_domains_only() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Real Housewives of Salt Lake City", "networks": ["bravo"], "wikidata_id": None},
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
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
                [],
                [],
            ]
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=([{"person_id": person_id}], 1),
            ):
                with patch(
                    "api.routers.admin_show_links.load_fandom_community_allowlist",
                    return_value=("real-housewives.fandom.com",),
                ):
                    with patch(
                        "api.routers.admin_show_links._resolve_show_fandom_rule_context",
                        return_value={
                            "effective_rule_key": "real_housewives",
                            "community_domains": ["real-housewives.fandom.com"],
                            "candidate_urls": [],
                            "include_allpages_scan": True,
                        },
                    ):
                        with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                            with patch(
                                "api.routers.admin_show_links.search_allowlisted_fandom_wikis",
                                return_value=[
                                    "https://teen-wolf.fandom.com/wiki/Lisa_Barlow",
                                    "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                                ],
                            ):
                                with patch(
                                    "api.routers.admin_show_links._discover_related_person_fandom_urls",
                                    return_value=["https://real-housewives.fandom.com/wiki/Lisa_Barlow"],
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._validated_person_knowledge_url",
                                        side_effect=lambda url, kind, expected_name=None, **kwargs: (
                                            url if kind == "fandom" and "real-housewives.fandom.com" in url else None
                                        ),
                                    ):
                                        links = _discover_people_links(show_id)

    fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 1
    assert {str(link.get("url") or "") for link in fandom_links} == {
        "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
    }
    assert all("real-housewives.fandom.com" in str(link.get("url") or "") for link in fandom_links)
    assert any(link.get("metadata", {}).get("site_title") == "Real Housewives Wiki" for link in fandom_links)


def test_discover_people_links_fandom_fallback_includes_multiple_valid_distinct_pages() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Real Housewives of Salt Lake City", "networks": ["bravo"], "wikidata_id": None},
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
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
                [],
                [],
            ]
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=([{"person_id": person_id}], 1),
            ):
                with patch(
                    "api.routers.admin_show_links.load_fandom_community_allowlist",
                    return_value=("real-housewives.fandom.com",),
                ):
                    with patch(
                        "api.routers.admin_show_links._resolve_show_fandom_rule_context",
                        return_value={
                            "effective_rule_key": "real_housewives",
                            "community_domains": ["real-housewives.fandom.com"],
                            "candidate_urls": [],
                            "include_allpages_scan": True,
                        },
                    ):
                        with patch("api.routers.admin_show_links.search_real_housewives_wiki", return_value=None):
                            with patch(
                                "api.routers.admin_show_links.search_allowlisted_fandom_wikis",
                                return_value=[
                                    "https://real-housewives.fandom.com/wiki/Lisa",
                                    "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                                ],
                            ):
                                with patch(
                                    "api.routers.admin_show_links._discover_related_person_fandom_urls",
                                    side_effect=lambda **kwargs: [
                                        kwargs["validated_fandom_url"],
                                    ],
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._validated_person_knowledge_url",
                                        side_effect=lambda url, **kwargs: url,
                                    ):
                                        links = _discover_people_links(show_id)

    fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 2
    assert any(link.get("url") == "https://real-housewives.fandom.com/wiki/Lisa_Barlow" for link in fandom_links)
    assert any(link.get("url") == "https://real-housewives.fandom.com/wiki/Lisa" for link in fandom_links)


def test_discover_people_links_discovers_fandom_profiles_across_show_fandom_domains() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    show_fandom_urls = [
        "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US",
        "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
    ]

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "networks": ["peacock"], "wikidata_id": None},
    ):
        with patch("api.routers.admin_show_links.load_fandom_community_allowlist") as load_allowlist:
            load_allowlist.return_value = ("thetraitors.fandom.com", "thetraitorsuk.fandom.com")
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [
                        {
                            "id": person_id,
                            "full_name": "Alan Cumming",
                            "external_ids": {},
                            "fandom_url": "",
                            "cast_tmdb_imdb_id": None,
                            "cast_tmdb_tmdb_id": None,
                            "cast_tmdb_wikidata_id": None,
                        }
                    ],
                    [],
                ]

                def _search(
                    name: str,
                    *,
                    community_domain: str,
                    timeout_seconds: float = 20.0,
                    max_results: int = 5,
                ) -> list[str]:
                    if community_domain == "thetraitors.fandom.com":
                        return ["https://thetraitors.fandom.com/wiki/Alan_Cumming"]
                    if community_domain == "thetraitorsuk.fandom.com":
                        return ["https://thetraitorsuk.fandom.com/wiki/Alan_Cumming"]
                    return []

                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], 1),
                ):
                    with patch(
                        "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                        side_effect=_search,
                    ):
                        with patch(
                            "api.routers.admin_show_links._validated_person_knowledge_url",
                            side_effect=lambda url, kind, expected_name=None, **kwargs: url
                            if kind == "fandom"
                            else None,
                        ):
                            links = _discover_people_links(show_id, show_fandom_seed_urls=show_fandom_urls)

    fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 2
    assert any(link.get("url") == "https://thetraitors.fandom.com/wiki/Alan_Cumming" for link in fandom_links)
    assert any(link.get("url") == "https://thetraitorsuk.fandom.com/wiki/Alan_Cumming" for link in fandom_links)


def test_discover_people_links_uses_direct_fandom_domain_profile_urls_when_search_misses() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    show_fandom_urls = [
        "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US",
        "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
    ]

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "networks": ["peacock"], "wikidata_id": None},
    ):
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("thetraitors.fandom.com", "thetraitorsuk.fandom.com"),
        ):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [
                        {
                            "id": person_id,
                            "full_name": "Alan Cumming",
                            "external_ids": {},
                            "fandom_url": "",
                            "cast_tmdb_imdb_id": None,
                            "cast_tmdb_tmdb_id": None,
                            "cast_tmdb_wikidata_id": None,
                        }
                    ],
                    [],
                ]
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], 1),
                ):
                    with patch("api.routers.admin_show_links.search_fandom_community_wiki_candidates", return_value=[]):
                        with patch(
                            "api.routers.admin_show_links._validated_person_knowledge_url",
                            side_effect=lambda url, kind, expected_name=None, **kwargs: url
                            if kind == "fandom"
                            else None,
                        ):
                            links = _discover_people_links(show_id, show_fandom_seed_urls=show_fandom_urls)

    fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(fandom_links) == 2
    assert any(link.get("url") == "https://thetraitors.fandom.com/wiki/Alan_Cumming" for link in fandom_links)
    assert any(link.get("url") == "https://thetraitorsuk.fandom.com/wiki/Alan_Cumming" for link in fandom_links)


def test_discover_people_links_expands_matching_fandom_person_related_pages() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    show_fandom_urls = ["https://real-housewives.fandom.com/"]
    eligible_people_rows = [
        {
            "person_id": person_id,
            "full_name": "Angie Katsanevas",
        }
    ]

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Real Housewives of Salt Lake City", "networks": ["bravo"], "wikidata_id": None},
    ):
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:

                def _fetch_all(query: str, params=None):
                    if "FROM core.show_cast_role_assignments" in query:
                        return []
                    if "FROM core.v_show_cast sc" in query:
                        return [
                            {
                                "id": person_id,
                                "full_name": "Angie Katsanevas",
                                "external_ids": {},
                                "fandom_url": "",
                                "cast_tmdb_imdb_id": None,
                                "cast_tmdb_tmdb_id": None,
                                "cast_tmdb_wikidata_id": None,
                                "cast_tmdb_facebook_id": None,
                                "cast_tmdb_instagram_id": None,
                                "cast_tmdb_tiktok_id": None,
                                "cast_tmdb_twitter_id": None,
                                "cast_tmdb_youtube_id": None,
                                "cast_tmdb_freebase_id": None,
                                "cast_tmdb_freebase_mid": None,
                            }
                        ]
                    if "FROM core.entity_links" in query:
                        return []
                    raise AssertionError(query)

                fetch_all.side_effect = _fetch_all
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=(eligible_people_rows, 1),
                ):
                    with patch(
                        "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                        return_value=["https://real-housewives.fandom.com/wiki/Angie_Katsanevas"],
                    ):
                        with patch(
                            "api.routers.admin_show_links.search_fandom_person_related_pages",
                            return_value=[
                                "https://real-housewives.fandom.com/wiki/Angie_Katsanevas",
                                "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Gallery",
                                "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Storylines",
                                "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Connections",
                            ],
                        ):
                            with patch(
                                "api.routers.admin_show_links._validated_person_knowledge_url",
                                side_effect=lambda url, kind, expected_name=None, **kwargs: (
                                    url if kind == "fandom" else None
                                ),
                            ):
                                links = _discover_people_links(show_id, show_fandom_seed_urls=show_fandom_urls)

    fandom_urls = {
        str(link.get("url") or "")
        for link in links
        if link.get("entity_type") == "person" and link.get("link_kind") == "fandom"
    }
    assert fandom_urls == {"https://real-housewives.fandom.com/wiki/Angie_Katsanevas"}


def test_score_fandom_candidate_url_rejects_weak_token_overlap_person_match() -> None:
    score = admin_show_links._score_fandom_candidate_url(
        "https://thetraitors.fandom.com/wiki/Alan_Carr",
        expected_name="Alan Cumming",
    )
    assert score == 0


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


def test_discover_season_links_includes_tmdb_season_url_from_show_tmdb_id() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
        fetch_all.return_value = [
            {
                "id": season_id,
                "season_number": 2,
                "external_wikidata_id": "",
                "external_ids": {},
                "tmdb_season_id": "12345",
            }
        ]
        with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
            fetch_one.side_effect = [
                {"name": "The Traitors", "wikidata_id": None, "tmdb_id": "204761"},
                {"url": ""},
                {"url": ""},
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=(None, None, "missing"),
            ):
                links = _discover_season_links(show_id)

    tmdb_links = [link for link in links if link.get("link_kind") == "tmdb"]
    assert len(tmdb_links) == 1
    assert tmdb_links[0]["url"] == "https://www.themoviedb.org/tv/204761/season/2"
    assert tmdb_links[0]["source"] == "core.seasons.tmdb_season_id"


def test_discover_season_links_prefers_show_wikipedia_seed_variant() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())
    expected_season_url = "https://en.wikipedia.org/wiki/The_Traitors_%28American_TV_series%29_season_4"

    with patch(
        "api.routers.admin_show_links.pg.fetch_all",
        return_value=[
            {
                "id": season_id,
                "season_number": 4,
                "external_wikidata_id": "",
                "external_ids": {},
            }
        ],
    ):
        with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
            fetch_one.side_effect = [
                {"name": "The Traitors", "wikidata_id": None},
                {"url": ""},
                {"url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)"},
            ]

            def _resolve(url: str) -> tuple[str | None, str | None, str | None]:
                if url == "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)":
                    return url, "The Traitors (American TV series)", None
                if url == expected_season_url:
                    return url, "The Traitors (American TV series) season 4", None
                return None, None, "missing"

            with patch("api.routers.admin_show_links._resolve_wikipedia_url", side_effect=_resolve):
                links = _discover_season_links(show_id)

    season_wiki_links = [link for link in links if link.get("link_kind") == "wikipedia"]
    assert len(season_wiki_links) == 1
    assert season_wiki_links[0]["url"] == expected_season_url
    assert season_wiki_links[0]["source"] == "derived_show_wikipedia"


def test_discover_season_links_uses_show_wikidata_season_claims_when_missing_on_season() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())
    season_wikidata_id = "Q999004"
    season_wikipedia_url = "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)_season_4"

    with patch(
        "api.routers.admin_show_links.pg.fetch_all",
        return_value=[
            {
                "id": season_id,
                "season_number": 4,
                "external_wikidata_id": "",
                "external_ids": {},
            }
        ],
    ):
        with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
            fetch_one.side_effect = [
                {"name": "The Traitors", "wikidata_id": "Q12345"},
                {"url": ""},
            ]
            with patch(
                "api.routers.admin_show_links._fetch_wikidata_summary",
                side_effect=[
                    (
                        {
                            "cast_item_ids": [],
                            "season_item_ids": [season_wikidata_id],
                            "label": "The Traitors",
                            "enwiki_title": "The Traitors (American TV series)",
                            "enwiki_url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                        },
                        False,
                    ),
                    (
                        {
                            "cast_item_ids": [],
                            "season_item_ids": [],
                            "label": "The Traitors season 4",
                            "enwiki_title": "The Traitors (American TV series) season 4",
                            "enwiki_url": season_wikipedia_url,
                        },
                        False,
                    ),
                    (
                        {
                            "cast_item_ids": [],
                            "season_item_ids": [],
                            "label": "The Traitors season 4",
                            "enwiki_title": "The Traitors (American TV series) season 4",
                            "enwiki_url": season_wikipedia_url,
                        },
                        False,
                    ),
                ],
            ):
                with patch(
                    "api.routers.admin_show_links._resolve_wikidata_enwiki_url",
                    return_value=season_wikipedia_url,
                ):
                    with patch(
                        "api.routers.admin_show_links._resolve_wikipedia_url",
                        side_effect=lambda url: (
                            (season_wikipedia_url, "The Traitors (American TV series) season 4", None)
                            if url == season_wikipedia_url
                            else (None, None, "missing")
                        ),
                    ):
                        links = _discover_season_links(show_id)

    season_wikidata_links = [link for link in links if link.get("link_kind") == "wikidata"]
    season_wiki_links = [link for link in links if link.get("link_kind") == "wikipedia"]
    assert len(season_wikidata_links) == 1
    assert season_wikidata_links[0]["url"] == f"https://www.wikidata.org/wiki/{season_wikidata_id}"
    assert season_wikidata_links[0]["source"] == "show_wikidata_season_claims"
    assert len(season_wiki_links) == 1
    assert season_wiki_links[0]["url"] == season_wikipedia_url


def test_discover_season_links_discovers_fandom_pages_per_domain_and_skips_missing_pages() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())
    show_fandom_urls = [
        "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US",
        "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
    ]

    with patch("api.routers.admin_show_links.load_fandom_community_allowlist") as load_allowlist:
        load_allowlist.return_value = ("thetraitors.fandom.com", "thetraitorsuk.fandom.com")
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": season_id,
                        "season_number": 1,
                        "external_wikidata_id": "",
                        "external_ids": {},
                    }
                ],
                [],
            ]
            with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
                fetch_one.side_effect = [
                    {"name": "The Traitors", "wikidata_id": None},
                    {"url": ""},
                    {"url": ""},
                ]
                with patch(
                    "api.routers.admin_show_links._resolve_wikipedia_url",
                    return_value=(None, None, "missing"),
                ):

                    def _search(
                        query: str,
                        *,
                        community_domain: str,
                        timeout_seconds: float = 20.0,
                        max_results: int = 5,
                    ) -> list[str]:
                        if "season 1" not in query.lower():
                            return []
                        if community_domain == "thetraitors.fandom.com":
                            return ["https://thetraitors.fandom.com/wiki/The_Traitors_(US)_season_1"]
                        if community_domain == "thetraitorsuk.fandom.com":
                            return ["https://thetraitorsuk.fandom.com/wiki/The_Traitors_US_season_1"]
                        return []

                    with patch(
                        "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                        side_effect=_search,
                    ):

                        def _fetch_html(url: str, timeout: float = 20.0):
                            if "thetraitors.fandom.com" in url:
                                return (
                                    200,
                                    "<html><body><h1>The Traitors (US) Season 1</h1></body></html>",
                                    "https://thetraitors.fandom.com/wiki/The_Traitors_(US)_season_1",
                                    None,
                                )
                            return (
                                200,
                                (
                                    "There is currently no text in this page. "
                                    "You can search for this page title in other pages."
                                ),
                                "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US_season_1",
                                None,
                            )

                        with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
                            links = _discover_season_links(show_id, show_fandom_seed_urls=show_fandom_urls)

    season_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(season_fandom_links) == 1
    assert season_fandom_links[0]["url"] == "https://thetraitors.fandom.com/wiki/The_Traitors_(US)_season_1"
    assert season_fandom_links[0]["source"] == "fandom_domain_seed:thetraitors.fandom.com"


def test_discover_season_links_uses_seed_derived_fandom_urls_when_search_misses() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())
    show_fandom_urls = ["https://thetraitors.fandom.com/wiki/The_Traitors_(US)"]

    with patch(
        "api.routers.admin_show_links.load_fandom_community_allowlist",
        return_value=("thetraitors.fandom.com",),
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": season_id,
                        "season_number": 2,
                        "external_wikidata_id": "",
                        "external_ids": {},
                        "tmdb_season_id": None,
                    }
                ],
                [],
            ]
            with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
                fetch_one.side_effect = [
                    {"name": "The Traitors", "wikidata_id": None, "tmdb_id": "204761"},
                    {"url": ""},
                    {"url": ""},
                ]
                with patch(
                    "api.routers.admin_show_links._resolve_wikipedia_url",
                    return_value=(None, None, "missing"),
                ):
                    with patch("api.routers.admin_show_links.search_fandom_community_wiki_candidates", return_value=[]):

                        def _fetch_html(url: str, timeout: float = 20.0):
                            if "The_Traitors_(US)_season_2" in url:
                                return (
                                    200,
                                    "<html><body><h1>The Traitors (US) Season 2</h1></body></html>",
                                    "https://thetraitors.fandom.com/wiki/The_Traitors_(US)_season_2",
                                    None,
                                )
                            return (
                                200,
                                "There is currently no text in this page.",
                                url,
                                None,
                            )

                        with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
                            links = _discover_season_links(show_id, show_fandom_seed_urls=show_fandom_urls)

    season_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(season_fandom_links) == 1
    assert season_fandom_links[0]["url"] == "https://thetraitors.fandom.com/wiki/The_Traitors_(US)_season_2"
    assert season_fandom_links[0]["source"] == "fandom_domain_seed:thetraitors.fandom.com"


def test_discover_season_links_uses_later_valid_fandom_candidate_when_first_is_missing() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.load_fandom_community_allowlist",
        return_value=("real-housewives.fandom.com",),
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": season_id,
                        "season_number": 4,
                        "external_wikidata_id": "",
                        "external_ids": {},
                        "tmdb_season_id": None,
                    }
                ],
                [],
            ]
            with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
                fetch_one.side_effect = [
                    {"name": "The Real Housewives of Salt Lake City", "wikidata_id": None, "tmdb_id": None},
                    {"url": ""},
                    {"url": ""},
                ]
                with patch(
                    "api.routers.admin_show_links._resolve_wikipedia_url",
                    return_value=(None, None, "missing"),
                ):
                    with patch(
                        "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                        return_value=[
                            "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_season_4_draft",
                            "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_season_4",
                        ],
                    ):

                        def _fetch_html(url: str, timeout: float = 20.0):
                            if url.endswith("_draft"):
                                return (
                                    200,
                                    "There is currently no text in this page.",
                                    url,
                                    None,
                                )
                            return (
                                200,
                                "<html><body><h1>The Real Housewives of Salt Lake City Season 4</h1></body></html>",
                                "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_season_4",
                                None,
                            )

                        with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
                            links = _discover_season_links(
                                show_id,
                                show_fandom_seed_urls=[
                                    "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                                ],
                            )

    season_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(season_fandom_links) == 1
    assert (
        season_fandom_links[0]["url"]
        == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_season_4"
    )
    assert season_fandom_links[0]["metadata"]["site_title"] == "The Real Housewives Wiki"


def test_discover_season_links_rejects_cross_show_real_housewives_fandom_candidate() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.load_fandom_community_allowlist",
        return_value=("real-housewives.fandom.com",),
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": season_id,
                        "season_number": 1,
                        "external_wikidata_id": "",
                        "external_ids": {},
                        "tmdb_season_id": None,
                    }
                ],
                [],
            ]
            with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
                fetch_one.side_effect = [
                    {"name": "The Real Housewives of Salt Lake City", "wikidata_id": None, "tmdb_id": None},
                    {"url": ""},
                    {"url": ""},
                ]
                with patch(
                    "api.routers.admin_show_links._resolve_wikipedia_url",
                    return_value=(None, None, "missing"),
                ):
                    with patch(
                        "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                        return_value=[],
                    ):
                        with patch(
                            "api.routers.admin_show_links.discover_fandom_candidate_pages",
                            return_value=[
                                SimpleNamespace(
                                    url=(
                                        "https://real-housewives.fandom.com/wiki/"
                                        "Wife_Swap:_The_Real_Housewives_Edition_-_Season_1"
                                    ),
                                    title="Wife Swap: The Real Housewives Edition - Season 1",
                                    snippet="Wrong crossover page",
                                    source="search",
                                    score=0.91,
                                ),
                                SimpleNamespace(
                                    url=(
                                        "https://real-housewives.fandom.com/wiki/"
                                        "The_Real_Housewives_of_Salt_Lake_City_-_Season_1"
                                    ),
                                    title="The Real Housewives of Salt Lake City - Season 1",
                                    snippet="Correct show page",
                                    source="search",
                                    score=0.83,
                                ),
                            ],
                        ):

                            def _fetch_html(url: str, timeout: float = 20.0):
                                if "Wife_Swap" in url:
                                    return (
                                        200,
                                        (
                                            "<html><body><h1>"
                                            "Wife Swap: The Real Housewives Edition - Season 1"
                                            "</h1></body></html>"
                                        ),
                                        url,
                                        None,
                                    )
                                if "_-_Season_1" in url:
                                    return (
                                        200,
                                        (
                                            "<html><body><h1>"
                                            "The Real Housewives of Salt Lake City - Season 1"
                                            "</h1></body></html>"
                                        ),
                                        (
                                            "https://real-housewives.fandom.com/wiki/"
                                            "The_Real_Housewives_of_Salt_Lake_City_-_Season_1"
                                        ),
                                        None,
                                    )
                                return (
                                    200,
                                    "There is currently no text in this page.",
                                    url,
                                    None,
                                )

                            with patch("api.routers.admin_show_links._fetch_html_with_status", side_effect=_fetch_html):
                                links = _discover_season_links(
                                    show_id,
                                    show_fandom_seed_urls=[
                                        "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                                    ],
                                )

    season_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(season_fandom_links) == 1
    assert (
        season_fandom_links[0]["url"]
        == "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_1"
    )
    assert "Wife_Swap" not in season_fandom_links[0]["url"]


def test_discover_season_links_uses_cached_page_directory_before_live_fandom_search() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())
    cached_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_1"

    with patch(
        "api.routers.admin_show_links.load_fandom_community_allowlist",
        return_value=("real-housewives.fandom.com",),
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": season_id,
                        "season_number": 1,
                        "external_wikidata_id": "",
                        "external_ids": {},
                        "tmdb_season_id": None,
                    }
                ],
                [],
            ]
            with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
                fetch_one.side_effect = [
                    {"name": "The Real Housewives of Salt Lake City", "wikidata_id": None, "tmdb_id": None},
                    {"url": ""},
                    {"url": ""},
                ]
                with patch(
                    "api.routers.admin_show_links._resolve_wikipedia_url",
                    return_value=(None, None, "missing"),
                ):
                    with patch(
                        "api.routers.admin_show_links.fandom_page_directory_repo.search_active_page_directory_entries",
                        return_value=[
                            {
                                "community_domain": "real-housewives.fandom.com",
                                "page_title": "The Real Housewives of Salt Lake City - Season 1",
                                "page_slug": "The_Real_Housewives_of_Salt_Lake_City_-_Season_1",
                                "page_url": cached_url,
                            }
                        ],
                    ):
                        with patch(
                            "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                            side_effect=AssertionError("live fandom search should not run"),
                        ):
                            with patch(
                                "api.routers.admin_show_links.discover_fandom_candidate_pages",
                                side_effect=AssertionError("live fandom discovery should not run"),
                            ):
                                with patch(
                                    "api.routers.admin_show_links._fetch_html_with_status",
                                    return_value=(
                                        200,
                                        (
                                            "<html><body><h1>"
                                            "The Real Housewives of Salt Lake City - Season 1"
                                            "</h1></body></html>"
                                        ),
                                        cached_url,
                                        None,
                                    ),
                                ):
                                    links = _discover_season_links(
                                        show_id,
                                        show_fandom_seed_urls=[
                                            "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                                        ],
                                    )

    season_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(season_fandom_links) == 1
    assert season_fandom_links[0]["url"] == cached_url


def test_discover_season_links_uses_cached_page_directory_when_fandom_page_is_cloudflare_blocked() -> None:
    show_id = str(uuid4())
    season_id = str(uuid4())
    cached_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_1"

    with patch(
        "api.routers.admin_show_links.load_fandom_community_allowlist",
        return_value=("real-housewives.fandom.com",),
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": season_id,
                        "season_number": 1,
                        "external_wikidata_id": "",
                        "external_ids": {},
                        "tmdb_season_id": None,
                    }
                ],
                [],
            ]
            with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
                fetch_one.side_effect = [
                    {"name": "The Real Housewives of Salt Lake City", "wikidata_id": None, "tmdb_id": None},
                    {"url": ""},
                    {"url": ""},
                ]
                with patch(
                    "api.routers.admin_show_links._resolve_wikipedia_url",
                    return_value=(None, None, "missing"),
                ):
                    with patch(
                        "api.routers.admin_show_links.fandom_page_directory_repo.search_active_page_directory_entries",
                        return_value=[
                            {
                                "community_domain": "real-housewives.fandom.com",
                                "page_title": "The Real Housewives of Salt Lake City - Season 1",
                                "page_slug": "The_Real_Housewives_of_Salt_Lake_City_-_Season_1",
                                "page_url": cached_url,
                            }
                        ],
                    ):
                        with patch(
                            "api.routers.admin_show_links.fandom_page_directory_repo.get_active_page_directory_entry_by_url",
                            return_value={
                                "community_domain": "real-housewives.fandom.com",
                                "page_title": "The Real Housewives of Salt Lake City - Season 1",
                                "page_slug": "The_Real_Housewives_of_Salt_Lake_City_-_Season_1",
                                "page_url": cached_url,
                            },
                        ):
                            with patch(
                                "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                                side_effect=AssertionError("live fandom search should not run"),
                            ):
                                with patch(
                                    "api.routers.admin_show_links.discover_fandom_candidate_pages",
                                    side_effect=AssertionError("live fandom discovery should not run"),
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._fetch_html_with_status",
                                        return_value=(
                                            403,
                                            "<html><head><title>Just a moment...</title></head></html>",
                                            cached_url,
                                            None,
                                        ),
                                    ):
                                        links = _discover_season_links(
                                            show_id,
                                            show_fandom_seed_urls=[
                                                "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                                            ],
                                        )

    season_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(season_fandom_links) == 1
    assert season_fandom_links[0]["url"] == cached_url


def test_discover_people_links_filters_allowlisted_fandom_fallback_to_preferred_community() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    preferred_url = "https://real-housewives.fandom.com/wiki/Lisa_Barlow"

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.side_effect = [
            {"name": "The Real Housewives of Salt Lake City", "networks": ["bravo"], "wikidata_id": None},
            None,
        ]
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com", "realitytv-girl.fandom.com"),
        ):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.return_value = [
                    {
                        "id": person_id,
                        "full_name": "Lisa Barlow",
                        "external_ids": {},
                        "fandom_url": "",
                        "cast_tmdb_imdb_id": None,
                        "cast_tmdb_tmdb_id": None,
                        "cast_tmdb_wikidata_id": None,
                        "cast_tmdb_facebook_id": None,
                        "cast_tmdb_instagram_id": None,
                        "cast_tmdb_tiktok_id": None,
                        "cast_tmdb_twitter_id": None,
                        "cast_tmdb_youtube_id": None,
                        "cast_tmdb_freebase_id": None,
                        "cast_tmdb_freebase_mid": None,
                    }
                ]
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], {}),
                ):
                    with patch(
                        "api.routers.admin_show_links._resolve_show_fandom_rule_context",
                        return_value={
                            "effective_rule_key": "real_housewives",
                            "preferred_community_domain": "real-housewives.fandom.com",
                            "community_domains": ["real-housewives.fandom.com"],
                            "candidate_urls": [preferred_url],
                            "include_allpages_scan": True,
                        },
                    ):
                        with patch(
                            "api.routers.admin_show_links.search_allowlisted_fandom_wikis",
                            return_value=[
                                "https://realitytv-girl.fandom.com/wiki/Lisa_Barlow",
                                preferred_url,
                            ],
                        ):
                            with patch(
                                "api.routers.admin_show_links._validated_person_knowledge_url",
                                side_effect=lambda url, **_: url if "real-housewives.fandom.com" in url else None,
                            ):
                                with patch(
                                    "api.routers.admin_show_links._discover_related_person_fandom_urls",
                                    return_value=[preferred_url],
                                ):
                                    links = _discover_people_links(
                                        show_id,
                                        show_fandom_seed_urls=[
                                            "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                                        ],
                                    )

    person_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(person_fandom_links) == 1
    assert person_fandom_links[0]["url"] == preferred_url


def test_discover_people_links_uses_cached_page_directory_when_fandom_page_is_cloudflare_blocked() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    preferred_url = "https://real-housewives.fandom.com/wiki/Lisa_Barlow"
    eligible_row = {
        "person_id": person_id,
        "eligible_reason": "main_cast",
        "season_numbers": [1],
        "season_count": 1,
        "season_total": 1,
        "season_latest": 1,
        "season_first": 1,
        "episode_count": 10,
    }
    person_row = {
        "id": person_id,
        "full_name": "Lisa Barlow",
        "external_ids": {},
        "fandom_url": None,
        "cast_tmdb_imdb_id": None,
        "cast_tmdb_tmdb_id": None,
        "cast_tmdb_wikidata_id": None,
        "cast_tmdb_facebook_id": None,
        "cast_tmdb_instagram_id": None,
        "cast_tmdb_tiktok_id": None,
        "cast_tmdb_twitter_id": None,
        "cast_tmdb_youtube_id": None,
        "cast_tmdb_freebase_id": None,
        "cast_tmdb_freebase_mid": None,
    }

    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        fetch_one.side_effect = [
            {"name": "The Real Housewives of Salt Lake City", "networks": ["bravo"], "wikidata_id": None},
            None,
        ]
        with patch(
            "api.routers.admin_show_links.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [person_row],
                    [],
                    [],
                ]
                with patch(
                    "api.routers.admin_show_links._resolve_show_fandom_rule_context",
                    return_value={
                        "effective_rule_key": "real_housewives",
                        "preferred_community_domain": "real-housewives.fandom.com",
                        "community_domains": ["real-housewives.fandom.com"],
                        "candidate_urls": [
                            "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                        ],
                        "include_allpages_scan": True,
                    },
                ):
                    with patch(
                        "api.routers.admin_show_links.fandom_page_directory_repo.search_active_page_directory_entries",
                        return_value=[
                            {
                                "community_domain": "real-housewives.fandom.com",
                                "page_title": "Lisa Barlow",
                                "page_slug": "Lisa_Barlow",
                                "page_url": preferred_url,
                            }
                        ],
                    ):
                        with patch(
                            "api.routers.admin_show_links.fandom_page_directory_repo.get_active_page_directory_entry_by_url",
                            return_value={
                                "community_domain": "real-housewives.fandom.com",
                                "page_title": "Lisa Barlow",
                                "page_slug": "Lisa_Barlow",
                                "page_url": preferred_url,
                            },
                        ):
                            with patch(
                                "api.routers.admin_show_links._fetch_html_with_status",
                                return_value=(
                                    403,
                                    "<html><head><title>Just a moment...</title></head></html>",
                                    preferred_url,
                                    None,
                                ),
                            ):
                                with patch(
                                    "api.routers.admin_show_links.search_fandom_community_wiki_candidates",
                                    side_effect=AssertionError("live fandom search should not run"),
                                ):
                                    with patch(
                                        "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                                        return_value=([eligible_row], {}),
                                    ):
                                        links = _discover_people_links(
                                            show_id,
                                            show_fandom_seed_urls=[
                                                "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
                                            ],
                                        )

    person_fandom_links = [link for link in links if link.get("link_kind") == "fandom"]
    assert len(person_fandom_links) == 1
    assert person_fandom_links[0]["url"] == preferred_url


def test_discover_people_links_uses_show_wikidata_cast_claims_when_missing_on_person() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    cast_wikidata_id = "Q345678"

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"networks": ["peacock"], "wikidata_id": "Q12345"},
    ):
        with patch("api.routers.admin_show_links.load_fandom_community_allowlist", return_value=[]):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [
                        {
                            "id": person_id,
                            "full_name": "Arie Luyendyk Jr.",
                            "external_ids": {},
                            "fandom_url": "",
                            "cast_tmdb_imdb_id": None,
                            "cast_tmdb_tmdb_id": None,
                            "cast_tmdb_wikidata_id": None,
                        }
                    ],
                    [],
                ]
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], 1),
                ):
                    with patch(
                        "api.routers.admin_show_links._fetch_wikidata_summary",
                        side_effect=[
                            (
                                {
                                    "cast_item_ids": [cast_wikidata_id],
                                    "season_item_ids": [],
                                    "label": "The Traitors",
                                    "enwiki_title": "The Traitors (American TV series)",
                                    "enwiki_url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                                },
                                False,
                            ),
                            (
                                {
                                    "cast_item_ids": [],
                                    "season_item_ids": [],
                                    "label": "Arie Luyendyk Jr.",
                                    "enwiki_title": "Arie Luyendyk Jr.",
                                    "enwiki_url": "https://en.wikipedia.org/wiki/Arie_Luyendyk_Jr.",
                                    "imdb_id": "nm1741766",
                                    "tmdb_person_id": "2543898",
                                },
                                False,
                            ),
                            (
                                {
                                    "cast_item_ids": [],
                                    "season_item_ids": [],
                                    "label": "Arie Luyendyk Jr.",
                                    "enwiki_title": "Arie Luyendyk Jr.",
                                    "enwiki_url": "https://en.wikipedia.org/wiki/Arie_Luyendyk_Jr.",
                                    "imdb_id": "nm1741766",
                                    "tmdb_person_id": "2543898",
                                },
                                False,
                            ),
                        ],
                    ):
                        with patch(
                            "api.routers.admin_show_links._validated_person_knowledge_url",
                            side_effect=lambda url, kind, expected_name=None, **kwargs: (
                                url if kind in {"wikidata", "wikipedia"} else None
                            ),
                        ):
                            links = _discover_people_links(show_id)

    wikidata_links = [link for link in links if link.get("link_kind") == "wikidata"]
    assert len(wikidata_links) == 1
    assert wikidata_links[0]["url"] == f"https://www.wikidata.org/wiki/{cast_wikidata_id}"
    assert wikidata_links[0]["source"] == "show_wikidata_cast_claims"


def test_discover_people_links_uses_season_wikidata_cast_claims_when_show_claims_missing() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    season_wikidata_id = "Q990001"
    cast_wikidata_id = "Q990002"

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "networks": ["peacock"], "wikidata_id": "Q12345"},
    ):
        with patch("api.routers.admin_show_links.load_fandom_community_allowlist", return_value=[]):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [
                        {
                            "id": person_id,
                            "full_name": "Alan Cumming",
                            "external_ids": {},
                            "fandom_url": "",
                            "cast_tmdb_imdb_id": None,
                            "cast_tmdb_tmdb_id": None,
                            "cast_tmdb_wikidata_id": None,
                        }
                    ],
                    [],
                ]
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], 1),
                ):
                    with patch(
                        "api.routers.admin_show_links._fetch_wikidata_summary",
                        side_effect=[
                            (
                                {
                                    "cast_item_ids": [],
                                    "season_item_ids": [season_wikidata_id],
                                    "label": "The Traitors",
                                    "enwiki_title": "The Traitors (American TV series)",
                                    "enwiki_url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                                },
                                False,
                            ),
                            (
                                {
                                    "cast_item_ids": [cast_wikidata_id],
                                    "season_item_ids": [],
                                    "label": "The Traitors season 2",
                                    "enwiki_title": "The Traitors (American TV series) season 2",
                                    "enwiki_url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)_season_2",
                                },
                                False,
                            ),
                            (
                                {
                                    "cast_item_ids": [],
                                    "season_item_ids": [],
                                    "label": "Alan Cumming",
                                    "enwiki_title": "Alan Cumming",
                                    "enwiki_url": "https://en.wikipedia.org/wiki/Alan_Cumming",
                                    "imdb_id": "nm0001086",
                                    "tmdb_person_id": "9346",
                                },
                                False,
                            ),
                            (
                                {
                                    "cast_item_ids": [],
                                    "season_item_ids": [],
                                    "label": "Alan Cumming",
                                    "enwiki_title": "Alan Cumming",
                                    "enwiki_url": "https://en.wikipedia.org/wiki/Alan_Cumming",
                                    "imdb_id": "nm0001086",
                                    "tmdb_person_id": "9346",
                                },
                                False,
                            ),
                        ],
                    ):
                        with patch(
                            "api.routers.admin_show_links._validated_person_knowledge_url",
                            side_effect=lambda url, kind, expected_name=None, **kwargs: (
                                url if kind in {"wikidata", "wikipedia"} else None
                            ),
                        ):
                            links = _discover_people_links(show_id)

    wikidata_links = [link for link in links if link.get("link_kind") == "wikidata"]
    assert len(wikidata_links) == 1
    assert wikidata_links[0]["url"] == f"https://www.wikidata.org/wiki/{cast_wikidata_id}"
    assert wikidata_links[0]["source"] == "season_wikidata_cast_claims"


def test_resolve_wikipedia_url_marks_missing_when_summary_fetch_errors_but_html_is_missing_page() -> None:
    with patch("api.routers.admin_show_links._fetch_wikipedia_page_summary", return_value=(None, True)):
        with patch(
            "api.routers.admin_show_links._fetch_html_with_status",
            return_value=(
                200,
                (
                    "<html><body>"
                    "<p>Wikipedia does not have an article with this exact name.</p>"
                    "<p>There is currently no text in this page.</p>"
                    "</body></html>"
                ),
                "https://en.wikipedia.org/wiki/The_Traitors_season_4",
                None,
            ),
        ):
            resolved, _title, error = admin_show_links._resolve_wikipedia_url(
                "https://en.wikipedia.org/wiki/The_Traitors_season_4",
            )

    assert resolved is None
    assert error == "missing"


def test_classify_submitted_link_input_rejects_wikipedia_fetch_error() -> None:
    context = {
        "show_id": str(uuid4()),
        "show_slug": "the-traitors",
        "show_name": "The Traitors",
        "show_imdb_id": "tt15218000",
        "show_tmdb_id": "204761",
        "show_wikidata_id": None,
        "show_networks": ["peacock"],
        "seasons_by_number": {},
        "seasons_by_wikidata": {},
        "people_by_imdb": {},
        "people_by_tmdb": {},
        "people_by_wikidata": {},
        "people": [],
    }
    with patch("api.routers.admin_show_links._resolve_wikipedia_url", return_value=(None, None, "fetch_error")):
        rows, error = _classify_submitted_link_input(
            "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)_season_4",
            context,
        )

    assert rows == []
    assert error == "Could not verify the Wikipedia page right now. Try again."


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
            "api.routers.admin_show_links._fetch_html_with_status",
            return_value=(
                200,
                html,
                "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City",
                None,
            ),
        ):
            resolved = _validated_person_knowledge_url(
                "https://en.wikipedia.org/wiki/Georgia_Gay",
                kind="wikipedia",
                expected_name="Georgia Gay",
            )
    assert resolved is None


def test_validate_person_knowledge_url_rejects_missing_wikipedia_article_from_api() -> None:
    with patch("api.routers.admin_show_links._fetch_wikipedia_page_summary", return_value=(None, False)):
        with patch("api.routers.admin_show_links._fetch_html_with_status") as fetch_html:
            resolved, outcome = _validate_person_knowledge_url(
                "https://en.wikipedia.org/wiki/Whitney_Comstock_Duncan",
                kind="wikipedia",
                expected_name="Whitney Comstock Duncan",
            )

    assert resolved is None
    assert outcome == "invalid"
    fetch_html.assert_not_called()


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
        with patch("api.routers.admin_show_links._fetch_html_with_status") as fetch_html:
            resolved, outcome = _validate_person_knowledge_url(
                "https://en.wikipedia.org/wiki/Lisa_Barlow",
                kind="wikipedia",
                expected_name="Lisa Barlow",
            )

    assert resolved == "https://en.wikipedia.org/wiki/Lisa_Barlow"
    assert outcome == "valid"
    fetch_html.assert_not_called()


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


def test_validate_person_knowledge_url_classifies_imdb_challenge_without_owner_signal_as_fetch_error() -> None:
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
    assert resolved is None
    assert outcome == "fetch_error"


def test_validate_person_knowledge_url_accepts_imdb_challenge_when_owner_signal_present() -> None:
    html = """
    <html>
      <head><title>IMDb Security Challenge</title></head>
      <body>
        <h1>Heather Gay</h1>
        JavaScript is disabled. Please enable JavaScript. Reference ID: abc123
      </body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(202, html, "https://www.imdb.com/name/nm1234567/", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "nm1234567",
            kind="imdb",
            expected_name="Heather Gay",
        )
    assert resolved == "https://www.imdb.com/name/nm1234567/"
    assert outcome == "valid"


def test_validate_person_knowledge_url_classifies_tmdb_challenge_without_owner_signal_as_fetch_error() -> None:
    html = """
    <html>
      <head><title>The Movie Database (TMDB)</title></head>
      <body>Please verify you are human</body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(429, html, "https://www.themoviedb.org/person/12345", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "12345",
            kind="tmdb",
            expected_name="Heather Gay",
        )
    assert resolved is None
    assert outcome == "fetch_error"


def test_validate_person_knowledge_url_accepts_tmdb_challenge_when_owner_signal_present() -> None:
    html = """
    <html>
      <head><title>The Movie Database (TMDB)</title></head>
      <body>
        <h1>Heather Gay</h1>
        Please verify you are human
      </body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(403, html, "https://www.themoviedb.org/person/12345", None),
    ):
        resolved, outcome = _validate_person_knowledge_url(
            "12345",
            kind="tmdb",
            expected_name="Heather Gay",
        )
    assert resolved == "https://www.themoviedb.org/person/12345"
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


def test_validate_person_social_url_rejects_missing_instagram_profile() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(
            200,
            "<html><body>Sorry, this page isn't available.</body></html>",
            "https://www.instagram.com/alancummingsnaps/",
            None,
        ),
    ):
        resolved, outcome = _validate_person_social_url(
            "https://www.instagram.com/alancummingsnaps",
            kind="instagram",
        )

    assert resolved is None
    assert outcome == "invalid"


def test_validate_person_social_url_accepts_valid_instagram_profile() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(
            200,
            "<html><head><title>Alan Cumming (@alancummingreally) • Instagram photos and videos</title></head></html>",
            "https://www.instagram.com/alancummingreally/",
            None,
        ),
    ):
        resolved, outcome = _validate_person_social_url(
            "https://www.instagram.com/alancummingreally",
            kind="instagram",
        )

    assert resolved == "https://www.instagram.com/alancummingreally"
    assert outcome == "valid"


def test_validate_person_social_url_rejects_instagram_login_route() -> None:
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(
            200,
            "<html><head><title>Login • Instagram</title></head></html>",
            "https://www.instagram.com/accounts/login/",
            None,
        ),
    ):
        resolved, outcome = _validate_person_social_url(
            "https://www.instagram.com/accounts/login",
            kind="instagram",
        )

    assert resolved is None
    assert outcome == "invalid"


def test_validated_person_knowledge_url_rejects_mismatched_fandom_page() -> None:
    _validated_person_knowledge_url.cache_clear()
    html = """
    <html>
      <head><title>John Barlow | Real Housewives Wiki | Fandom</title></head>
      <body><h1 class="page-header__title">John Barlow</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(200, html, "https://real-housewives.fandom.com/wiki/John_Barlow", None),
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
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(200, html, "https://real-housewives.fandom.com/wiki/Lisa_Barlow", None),
    ):
        resolved = _validated_person_knowledge_url(
            "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
            kind="fandom",
            expected_name="Lisa Barlow",
        )
    assert resolved == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"


def test_validated_person_knowledge_url_rejects_matching_fandom_person_subpage() -> None:
    _validated_person_knowledge_url.cache_clear()
    html = """
    <html>
      <head><title>Angie Katsanevas/Gallery | Real Housewives Wiki | Fandom</title></head>
      <body><h1 class="page-header__title">Angie Katsanevas/Gallery</h1></body>
    </html>
    """
    with patch(
        "api.routers.admin_show_links._fetch_html_with_status",
        return_value=(
            200,
            html,
            "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Gallery",
            None,
        ),
    ):
        resolved = _validated_person_knowledge_url(
            "https://real-housewives.fandom.com/wiki/Angie_Katsanevas/Gallery",
            kind="fandom",
            expected_name="Angie Katsanevas",
        )
    assert resolved is None


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
                with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                    with patch("api.routers.admin_show_links._promote_pending_person_source_links", return_value=0):
                        with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=4):
                            result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 4
    assert result["invalid"] == 4
    assert result["promoted"] == 0
    assert result["deleted"] == 4
    assert result["validation_failures"] == 0


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
                [],
            ]
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=([{"person_id": person_id}], 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validate_person_knowledge_url",
                    return_value=(None, "invalid"),
                ):
                    with patch(
                        "api.routers.admin_show_links._load_preapproved_person_source_url",
                        return_value=None,
                    ):
                        with patch(
                            "api.routers.admin_show_links._validated_person_knowledge_url",
                            return_value=None,
                        ):
                            links = _discover_people_links(show_id)

    assert not any(link.get("link_kind") == "imdb" for link in links)
    assert not any(link.get("link_kind") == "tmdb" for link in links)


def test_discover_people_links_carries_forward_imdb_when_validation_fetch_errors() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"networks": ["peacock"]}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.side_effect = [
                [
                    {
                        "id": person_id,
                        "full_name": "Andy Cohen",
                        "external_ids": {"imdb": "nm0169212"},
                        "fandom_url": "",
                        "cast_tmdb_imdb_id": None,
                        "cast_tmdb_tmdb_id": None,
                        "cast_tmdb_wikidata_id": None,
                    }
                ],
                [],
            ]
            with patch(
                "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                return_value=([{"person_id": person_id}], 1),
            ):
                with patch(
                    "api.routers.admin_show_links._validate_person_knowledge_url",
                    return_value=(None, "fetch_error"),
                ):
                    with patch(
                        "api.routers.admin_show_links._load_preapproved_person_source_url",
                        return_value="https://www.imdb.com/name/nm0169212/",
                    ):
                        with patch(
                            "api.routers.admin_show_links._validated_person_knowledge_url",
                            return_value=None,
                        ):
                            links = _discover_people_links(show_id)

    imdb_links = [link for link in links if link.get("link_kind") == "imdb"]
    assert len(imdb_links) == 1
    assert imdb_links[0]["url"] == "https://www.imdb.com/name/nm0169212"


def test_load_preapproved_person_source_url_matches_by_url_key() -> None:
    person_id = str(uuid4())
    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"url": "https://www.imdb.com/name/nm0169212/"},
    ) as fetch_one:
        url = admin_show_links._load_preapproved_person_source_url(
            person_id=person_id,
            link_kind="imdb",
            candidate_url="https://www.imdb.com/name/nm0169212/",
        )
    assert url == "https://www.imdb.com/name/nm0169212"
    params = fetch_one.call_args.args[1]
    assert params[0] == person_id
    assert params[1] == "imdb"
    assert "https://www.imdb.com/name/nm0169212" in params[2]
    assert "https://www.imdb.com/name/nm0169212/" in params[2]


def test_load_preapproved_person_source_url_ignores_non_person_sources() -> None:
    with patch("api.routers.admin_show_links.pg.fetch_one") as fetch_one:
        url = admin_show_links._load_preapproved_person_source_url(
            person_id=str(uuid4()),
            link_kind="wikipedia",
            candidate_url="https://en.wikipedia.org/wiki/Andy_Cohen",
        )
    assert url is None
    fetch_one.assert_not_called()


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
                with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                    with patch("api.routers.admin_show_links._promote_pending_person_source_links", return_value=0):
                        with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=0):
                            result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 0
    assert result["promoted"] == 0
    assert result["deleted"] == 0
    assert result["validation_failures"] == 1


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
                with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                    with patch("api.routers.admin_show_links._promote_pending_person_source_links", return_value=1):
                        with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=0):
                            result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 0
    assert result["promoted"] == 1
    assert result["deleted"] == 0
    assert result["validation_failures"] == 0


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
                with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                    with patch("api.routers.admin_show_links._promote_pending_person_source_links", return_value=0):
                        with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1):
                            result = _cleanup_invalid_person_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 1
    assert result["promoted"] == 0
    assert result["deleted"] == 1
    assert result["validation_failures"] == 1


def test_cleanup_invalid_person_social_links_deletes_invalid_and_promotes_valid_pending_rows() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())
    invalid_link_id = str(uuid4())
    pending_link_id = str(uuid4())

    with patch("api.routers.admin_show_links._load_show_cast_names_by_person_id") as cast_lookup:
        cast_lookup.return_value = {person_id: "Alan Cumming"}
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": invalid_link_id,
                    "person_id": person_id,
                    "link_kind": "instagram",
                    "status": "approved",
                    "url": "https://www.instagram.com/alancummingsnaps",
                },
                {
                    "id": pending_link_id,
                    "person_id": person_id,
                    "link_kind": "twitter",
                    "status": "pending",
                    "url": "https://x.com/alan_cumming",
                },
            ]
            with patch("api.routers.admin_show_links._validate_person_social_url") as validate_url:
                validate_url.side_effect = [
                    (None, "invalid"),
                    ("https://x.com/alan_cumming", "valid"),
                ]
                with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                    with patch("api.routers.admin_show_links._promote_pending_person_source_links", return_value=1):
                        with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1):
                            result = _cleanup_invalid_person_social_links(show_id)

    assert result["scanned"] == 2
    assert result["invalid"] == 1
    assert result["promoted"] == 1
    assert result["deleted"] == 1
    assert result["validation_failures"] == 0


def test_cleanup_invalid_show_knowledge_links_deletes_non_manual_invalid_rows() -> None:
    show_id = str(uuid4())
    invalid_link_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"name": "The Traitors", "wikidata_id": None}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": invalid_link_id,
                    "entity_type": "show",
                    "link_kind": "fandom",
                    "status": "pending",
                    "url": "https://real-housewives.fandom.com/wiki/The_Traitors",
                    "source": "derived",
                    "discovered_by": "backend_discovery",
                }
            ]
            with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1):
                    result = _cleanup_invalid_show_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 1
    assert result["deleted"] == 1
    assert result["manual_skipped"] == 0
    assert result["validation_failures"] == 0


def test_cleanup_invalid_show_knowledge_links_deletes_show_level_fandom_seed_domains() -> None:
    show_id = str(uuid4())
    seed_link_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "wikidata_id": None},
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": seed_link_id,
                    "entity_type": "show",
                    "link_kind": "fandom",
                    "status": "approved",
                    "url": "https://thetraitors.fandom.com/",
                    "source": "manual_classifier",
                    "discovered_by": "manual_classifier",
                }
            ]
            with patch(
                "api.routers.admin_show_links.load_fandom_community_allowlist",
                return_value=("thetraitors.fandom.com", "thetraitorsuk.fandom.com"),
            ):
                with patch("api.routers.admin_show_links._fetch_html_with_status") as fetch_html:
                    with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                        with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1):
                            result = _cleanup_invalid_show_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 1
    assert result["deleted"] == 1
    assert result["validation_failures"] == 0
    fetch_html.assert_not_called()


def test_cleanup_invalid_show_knowledge_links_keeps_bravo_fandom_domains_without_global_allowlist() -> None:
    show_id = str(uuid4())
    seed_link_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={
            "name": "The Real Housewives of Salt Lake City",
            "wikidata_id": None,
            "networks": ["Bravo TV"],
        },
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": seed_link_id,
                    "entity_type": "show",
                    "link_kind": "fandom",
                    "status": "approved",
                    "url": "https://real-housewives.fandom.com/",
                    "source": "manual_classifier",
                    "discovered_by": "manual_classifier",
                }
            ]
            with patch("api.routers.admin_show_links.load_fandom_community_allowlist", return_value=()):
                with patch("api.routers.admin_show_links._fetch_html_with_status"):
                    with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                        with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=0):
                            result = _cleanup_invalid_show_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 0
    assert result["deleted"] == 0
    assert result["validation_failures"] == 0


def test_cleanup_invalid_show_knowledge_links_keeps_cached_canonical_fandom_pages_when_cloudflare_blocked() -> None:
    show_id = str(uuid4())
    show_link_id = str(uuid4())
    season_link_id = str(uuid4())
    show_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"
    season_url = "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_1"

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={
            "name": "The Real Housewives of Salt Lake City",
            "wikidata_id": None,
            "networks": ["Bravo TV"],
        },
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": show_link_id,
                    "entity_type": "show",
                    "link_kind": "fandom",
                    "status": "approved",
                    "url": show_url,
                    "source": "backend_discovery",
                    "discovered_by": "backend_discovery",
                },
                {
                    "id": season_link_id,
                    "entity_type": "season",
                    "link_kind": "fandom",
                    "status": "approved",
                    "url": season_url,
                    "source": "backend_discovery",
                    "discovered_by": "backend_discovery",
                },
            ]
            with patch(
                "api.routers.admin_show_links.load_fandom_community_allowlist",
                return_value=("real-housewives.fandom.com",),
            ):
                with patch(
                    "api.routers.admin_show_links.fandom_page_directory_repo.get_active_page_directory_entry_by_url",
                    side_effect=lambda community_domain, page_url: {
                        "community_domain": community_domain,
                        "page_title": (
                            "The Real Housewives of Salt Lake City"
                            if page_url == show_url
                            else "The Real Housewives of Salt Lake City - Season 1"
                        ),
                        "page_slug": (
                            "The_Real_Housewives_of_Salt_Lake_City"
                            if page_url == show_url
                            else "The_Real_Housewives_of_Salt_Lake_City_-_Season_1"
                        ),
                        "page_url": page_url,
                    },
                ):
                    with patch(
                        "api.routers.admin_show_links._fetch_html_with_status",
                        return_value=(
                            403,
                            "<html><head><title>Just a moment...</title></head></html>",
                            show_url,
                            None,
                        ),
                    ):
                        with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                            with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=0):
                                result = _cleanup_invalid_show_knowledge_links(show_id)

    assert result["scanned"] == 2
    assert result["invalid"] == 0
    assert result["deleted"] == 0


def test_cleanup_invalid_show_knowledge_links_deletes_show_level_fandom_season_pages() -> None:
    show_id = str(uuid4())
    invalid_link_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={
            "name": "The Real Housewives of Salt Lake City",
            "wikidata_id": None,
            "networks": ["Bravo TV"],
        },
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": invalid_link_id,
                    "entity_type": "show",
                    "link_kind": "fandom",
                    "status": "approved",
                    "url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_4",
                    "source": "backend_discovery",
                    "discovered_by": "backend_discovery",
                }
            ]
            with patch(
                "api.routers.admin_show_links._fetch_html_with_status",
                return_value=(
                    200,
                    """
                    <html>
                      <head>
                        <title>
                          The Real Housewives of Salt Lake City - Season 4 | The Real Housewives Wiki | Fandom
                        </title>
                      </head>
                      <body><h1>The Real Housewives of Salt Lake City - Season 4</h1></body>
                    </html>
                    """,
                    "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City_-_Season_4",
                    None,
                ),
            ):
                with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                    with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1):
                        result = _cleanup_invalid_show_knowledge_links(show_id)

    assert result["invalid"] == 1
    assert result["deleted"] == 1
    assert result["deleted_by_reason"]["fandom_not_show_level"] == 1


def test_cleanup_invalid_show_knowledge_links_deletes_manual_invalid_rows() -> None:
    show_id = str(uuid4())
    invalid_manual_link_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"name": "The Traitors", "wikidata_id": None}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": invalid_manual_link_id,
                    "entity_type": "show",
                    "link_kind": "fandom",
                    "status": "approved",
                    "url": "https://real-housewives.fandom.com/wiki/The_Traitors",
                    "source": "manual",
                    "discovered_by": "manual",
                }
            ]
            with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1) as delete_links:
                    result = _cleanup_invalid_show_knowledge_links(show_id)

    delete_links.assert_called_once()
    call_args = delete_links.call_args
    assert call_args.args[0] == [invalid_manual_link_id]
    assert "conn" in call_args.kwargs
    assert result["scanned"] == 1
    assert result["invalid"] == 1
    assert result["deleted"] == 1
    assert result["manual_skipped"] == 0
    assert result["deleted_by_reason"]["fandom_domain_mismatch"] == 1
    assert result["validation_failures"] == 0


def test_cleanup_invalid_show_knowledge_links_deletes_manual_pending_rows() -> None:
    show_id = str(uuid4())
    invalid_manual_link_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"name": "The Traitors", "wikidata_id": None}):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": invalid_manual_link_id,
                    "entity_type": "show",
                    "link_kind": "fandom",
                    "status": "pending",
                    "url": "https://real-housewives.fandom.com/wiki/The_Traitors",
                    "source": "manual",
                    "discovered_by": "manual",
                }
            ]
            with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1) as delete_links:
                    result = _cleanup_invalid_show_knowledge_links(show_id)

    delete_links.assert_called_once()
    call_args = delete_links.call_args
    assert call_args.args[0] == [invalid_manual_link_id]
    assert "conn" in call_args.kwargs
    assert result["scanned"] == 1
    assert result["invalid"] == 1
    assert result["deleted"] == 1
    assert result["manual_skipped"] == 0
    assert result["deleted_by_reason"]["fandom_domain_mismatch"] == 1
    assert result["validation_failures"] == 0


def test_normalize_existing_social_handle_urls_repairs_encoded_handles() -> None:
    show_id = str(uuid4())
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[{"id": str(uuid4())}], [{"id": str(uuid4())}]]
    with patch(
        "api.routers.admin_show_links.pg.fetch_all",
        return_value=[
            {
                "id": str(uuid4()),
                "url": "https://www.tiktok.com/%40BravoTV",
            },
            {
                "id": str(uuid4()),
                "url": "https://www.youtube.com/%40Bravo",
            },
        ],
    ):
        with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
            with patch("api.routers.admin_show_links.pg.db_cursor", return_value=nullcontext(cursor)):
                result = admin_show_links._normalize_existing_social_handle_urls(show_id, include_people=True)

    assert result == {"scanned": 2, "normalized": 2, "deleted_duplicates": 0}


def test_cleanup_invalid_show_knowledge_links_rejects_season_wikipedia_variant_mismatch() -> None:
    show_id = str(uuid4())
    invalid_link_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "wikidata_id": "Q116449538"},
    ):
        with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
            fetch_all.return_value = [
                {
                    "id": invalid_link_id,
                    "entity_type": "season",
                    "link_kind": "wikipedia",
                    "status": "approved",
                    "url": "https://en.wikipedia.org/wiki/The_Traitors_season_4",
                    "source": "derived",
                    "discovered_by": "backend_discovery",
                }
            ]
            with patch(
                "api.routers.admin_show_links._resolve_wikipedia_url",
                return_value=("https://en.wikipedia.org/wiki/The_Traitors_season_4", None, None),
            ):
                with patch("api.routers.admin_show_links._resolve_wikipedia_wikidata_id", return_value="Q122955840"):
                    with patch(
                        "api.routers.admin_show_links._fetch_wikidata_summary",
                        return_value=(
                            {
                                "item_id": "Q122955840",
                                "part_of_series_item_ids": ["Q111195888"],
                            },
                            False,
                        ),
                    ):
                        with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
                            with patch("api.routers.admin_show_links._delete_entity_links_by_id", return_value=1):
                                result = _cleanup_invalid_show_knowledge_links(show_id)

    assert result["scanned"] == 1
    assert result["invalid"] == 1
    assert result["deleted"] == 1
    assert result["manual_skipped"] == 0
    assert result["deleted_by_reason"]["wikipedia_variant_mismatch"] == 1
    assert result["validation_failures"] == 0


def test_promote_pending_links_to_approved_promotes_knowledge_and_network_blog() -> None:
    show_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.execute_returning",
        return_value=[{"id": str(uuid4())}, {"id": str(uuid4())}],
    ) as execute_returning:
        promoted = _promote_pending_links_to_approved(show_id, include_people=True)

    assert promoted == 2
    sql = execute_returning.call_args.args[0]
    params = execute_returning.call_args.args[1]
    assert "lower(link_group) = 'knowledge'" in sql
    assert "lower(link_kind) = 'network_blog'" in sql
    assert params[0] == show_id
    assert params[1] == ["show", "season", "person"]


def test_list_show_links_active_view_returns_approved_deduped_rows() -> None:
    show_id = UUID(str(uuid4()))
    duplicate_id_1 = str(uuid4())
    duplicate_id_2 = str(uuid4())
    with patch("api.routers.admin_show_links._show_exists", return_value=True):
        with patch(
            "api.routers.admin_show_links._normalize_legacy_knowledge_link_kinds",
            return_value=2,
        ) as normalize_legacy:
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.return_value = [
                    {
                        "id": duplicate_id_1,
                        "entity_type": "show",
                        "entity_id": str(show_id),
                        "season_number": 0,
                        "link_group": "knowledge",
                        "link_kind": "wikipedia",
                        "status": "approved",
                        "url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                    },
                    {
                        "id": duplicate_id_2,
                        "entity_type": "show",
                        "entity_id": str(show_id),
                        "season_number": 0,
                        "link_group": "knowledge",
                        "link_kind": "wikipedia",
                        "status": "approved",
                        "url": "https://en.wikipedia.org/wiki/The_Traitors_%28American_TV_series%29",
                    },
                ]
                rows = admin_show_links.list_show_links(
                    show_id=show_id,
                    _={"email": "admin@example.com"},
                    status="all",
                    entity_type="all",
                    view="active",
                )

    normalize_legacy.assert_called_once_with(str(show_id))
    sql = fetch_all.call_args.args[0]
    assert "lower(status) = 'approved'" in sql
    assert len(rows) == 1
    assert rows[0]["url"] == "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)"


def test_resolve_show_wikidata_id_uses_show_link_fallback() -> None:
    show_id = str(uuid4())
    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"url": "https://www.wikidata.org/wiki/Q116449538"},
    ):
        resolved = admin_show_links._resolve_show_wikidata_id(show_id, None)
    assert resolved == "Q116449538"


def test_build_connected_knowledge_rows_uses_primary_wikidata_url_when_context_is_missing() -> None:
    context = {
        "show_wikidata_id": None,
        "seasons_by_number": {},
        "people_by_id": {},
    }
    with patch(
        "api.routers.admin_show_links._fetch_wikidata_summary",
        return_value=(
            {
                "item_id": "Q116449538",
                "enwiki_url": "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)",
                "imdb_id": "tt15557874",
                "tmdb_tv_id": "215943",
                "tmdb_person_id": "",
                "tvdb_id": "428163",
                "tvmaze_show_id": "58177",
                "tvmaze_season_id": "",
                "ratinggraph_tv_show_id": "the-traitors-ratings-103483",
            },
            False,
        ),
    ):
        rows = admin_show_links._build_connected_knowledge_rows(
            context,
            entity_type="show",
            entity_id=str(uuid4()),
            season_number=0,
            primary_kind="wikidata",
            primary_url="https://www.wikidata.org/wiki/Q116449538",
        )
    kinds = {str(row.get("link_kind") or "") for row in rows}
    assert {"wikipedia", "imdb", "tmdb", "tvdb", "tvmaze", "ratinggraph"}.issubset(kinds)
    assert "wikidata" not in kinds


def test_normalize_legacy_knowledge_link_kinds_maps_rows_by_url_host() -> None:
    show_id = str(uuid4())
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[{"id": str(uuid4())}], [{"id": str(uuid4())}]]
    with patch(
        "api.routers.admin_show_links.pg.fetch_all",
        return_value=[
            {
                "id": str(uuid4()),
                "url": "https://en.wikipedia.org/wiki/Alan_Cumming",
                "link_kind": "knowledge_graph",
            },
            {
                "id": str(uuid4()),
                "url": "https://www.wikidata.org/wiki/Q316629",
                "link_kind": "kg",
            },
        ],
    ):
        with patch("api.routers.admin_show_links.pg.db_connection", return_value=nullcontext(object())):
            with patch("api.routers.admin_show_links.pg.db_cursor", return_value=nullcontext(cursor)):
                normalized = admin_show_links._normalize_legacy_knowledge_link_kinds(show_id)

    assert normalized == 2
    assert cursor.execute.call_count == 2
    first_params = cursor.execute.call_args_list[0].args[1]
    second_params = cursor.execute.call_args_list[1].args[1]
    assert first_params[0] == "wikipedia"
    assert second_params[0] == "wikidata"


def test_discover_people_links_derives_imdb_and_tmdb_from_wikidata_summary() -> None:
    show_id = str(uuid4())
    person_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links.pg.fetch_one",
        return_value={"name": "The Traitors", "networks": ["peacock"], "wikidata_id": None},
    ):
        with patch("api.routers.admin_show_links.load_fandom_community_allowlist", return_value=[]):
            with patch("api.routers.admin_show_links.pg.fetch_all") as fetch_all:
                fetch_all.side_effect = [
                    [
                        {
                            "id": person_id,
                            "full_name": "Alan Cumming",
                            "external_ids": {"wikidata": "Q316629"},
                            "fandom_url": "",
                            "cast_tmdb_imdb_id": None,
                            "cast_tmdb_tmdb_id": None,
                            "cast_tmdb_wikidata_id": None,
                        }
                    ],
                    [],
                ]
                with patch(
                    "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
                    return_value=([{"person_id": person_id}], 1),
                ):
                    with patch(
                        "api.routers.admin_show_links._fetch_wikidata_summary",
                        return_value=(
                            {
                                "item_id": "Q316629",
                                "imdb_id": "nm0001086",
                                "tmdb_person_id": "5190",
                                "label": "Alan Cumming",
                                "enwiki_title": "Alan Cumming",
                            },
                            False,
                        ),
                    ):
                        with patch(
                            "api.routers.admin_show_links._validated_or_carried_person_source_url",
                            side_effect=lambda person_id, candidate_url, kind, expected_name, fandom_allowlist=None: (
                                candidate_url
                            ),
                        ):
                            with patch(
                                "api.routers.admin_show_links._validated_person_knowledge_url",
                                side_effect=lambda url, kind, expected_name=None, **kwargs: (
                                    url if kind in {"wikidata", "wikipedia"} else None
                                ),
                            ):
                                with patch(
                                    "api.routers.admin_show_links.search_real_housewives_wiki", return_value=None
                                ):
                                    with patch(
                                        "api.routers.admin_show_links.search_allowlisted_fandom_wikis",
                                        return_value=[],
                                    ):
                                        links = _discover_people_links(show_id)

    imdb_links = [link for link in links if link.get("link_kind") == "imdb"]
    tmdb_links = [link for link in links if link.get("link_kind") == "tmdb"]
    assert len(imdb_links) == 1
    assert imdb_links[0]["source"] == "wikidata_person_external_ids"
    assert imdb_links[0]["url"] == "https://www.imdb.com/name/nm0001086"
    assert len(tmdb_links) == 1
    assert tmdb_links[0]["source"] == "wikidata_person_external_ids"
    assert tmdb_links[0]["url"] == "https://www.themoviedb.org/person/5190"


def test_discover_show_links_defaults_knowledge_rows_to_approved() -> None:
    show_id = UUID(str(uuid4()))
    db = MagicMock()

    discovered_rows = [
        {
            "entity_type": "show",
            "entity_id": str(show_id),
            "season_number": 0,
            "link_group": "knowledge",
            "link_kind": "imdb",
            "label": "IMDb",
            "url": "https://www.imdb.com/title/tt15557874/",
            "source": "core.shows.imdb_id",
        },
        {
            "entity_type": "person",
            "entity_id": str(uuid4()),
            "season_number": 0,
            "link_group": "knowledge",
            "link_kind": "freebase",
            "label": "Alan Cumming Freebase",
            "url": "https://g.co/kg/m/01qwz",
            "source": "connected_wikidata_identifiers",
            "status": "approved",
        },
        {
            "entity_type": "person",
            "entity_id": str(uuid4()),
            "season_number": 0,
            "link_group": "social",
            "link_kind": "twitter",
            "label": "Alan Cumming Twitter/X",
            "url": "https://x.com/alan_cumming",
            "source": "connected_wikidata_social",
            "status": "approved",
        },
        {
            "entity_type": "person",
            "entity_id": str(uuid4()),
            "season_number": 0,
            "link_group": "social",
            "link_kind": "instagram",
            "label": "Alan Cumming Instagram",
            "url": "https://www.instagram.com/alancummingreally",
            "source": "tmdb_external_ids_social",
            "status": "approved",
        },
    ]

    with patch("api.routers.admin_show_links._show_exists", return_value=True):
        with patch(
            "api.routers.admin_show_links._discover_show_links",
            side_effect=lambda show_id, stats=None: discovered_rows,
        ):
            with patch("api.routers.admin_show_links._discover_season_links", return_value=[]):
                with patch("api.routers.admin_show_links._discover_people_links", return_value=[]):
                    with patch("api.routers.admin_show_links._upsert_link") as upsert:
                        with patch(
                            "api.routers.admin_show_links._cleanup_invalid_show_knowledge_links",
                            return_value={
                                "scanned": 0,
                                "deleted": 0,
                                "manual_skipped": 0,
                                "deleted_by_reason": {},
                                "validation_failures": 0,
                            },
                        ):
                            with patch(
                                "api.routers.admin_show_links._count_discovery_scan_targets",
                                return_value={"show_scanned": 1, "season_scanned": 0, "people_scanned": 0},
                            ):
                                with patch(
                                    "api.routers.admin_show_links._normalize_legacy_knowledge_link_kinds",
                                    return_value=0,
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._promote_pending_links_to_approved",
                                        return_value=0,
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links._normalize_existing_social_handle_urls",
                                            return_value={"scanned": 0, "normalized": 0, "deleted_duplicates": 0},
                                        ):
                                            result = admin_show_links.discover_show_links(
                                                show_id=show_id,
                                                payload=admin_show_links.LinkDiscoverRequest(
                                                    include_seasons=False,
                                                    include_people=False,
                                                ),
                                                db=db,
                                                admin={"email": "admin@example.com"},
                                            )

    assert upsert.call_count == 4
    assert all(call.kwargs["status"] == "approved" for call in upsert.call_args_list)
    assert result["discovered"] == 4
    assert isinstance(result["run_id"], str) and len(result["run_id"]) > 0
    assert isinstance(result["started_at"], str) and result["started_at"]
    assert isinstance(result["finished_at"], str) and result["finished_at"]
    assert isinstance(result["duration_ms"], int)
    assert result["pending_links_promoted"] == 0
    assert result["status_counts"]["approved_added"] == 4
    assert result["status_counts"]["deleted_invalid"] == 0
    assert result["status_counts"]["skipped_fetch_error"] == 0
    assert result["validation_reasons"] == {}
    assert result["stage_counts"]["show_scanned"] == 1
    assert result["stage_counts"]["season_scanned"] == 0
    assert result["stage_counts"]["people_scanned"] == 0
    assert result["current_stage"] == "completed"
    assert result["message"] == "Links refresh complete."
    assert result["validated_live_counts_by_source"] == {
        "imdb": 1,
        "freebase": 1,
        "twitter": 1,
        "instagram": 1,
    }
    assert result["stage_progress"] == {
        "validated_links": 0,
        "promoted_links": 0,
        "deleted_links": 0,
        "normalized_social_urls": 0,
    }
    assert result["wikidata_identifier_links_added"] == 1
    assert result["wikidata_social_links_added"] == 1
    assert result["tmdb_social_links_added"] == 1
    assert result["fandom_candidates_tested"] == 0


def test_run_show_link_discovery_returns_structured_timeout_context() -> None:
    show_id = str(uuid4())
    db = MagicMock()

    def _slow_show_discovery(_show_id: str, *, stats: dict[str, object] | None = None) -> list[dict[str, object]]:
        time.sleep(0.01)
        return []

    with patch("api.routers.admin_show_links._link_discovery_run_timeout_seconds", return_value=0.001):
        with patch(
            "api.routers.admin_show_links._count_discovery_scan_targets",
            return_value={"show_scanned": 1, "season_scanned": 0, "people_scanned": 0},
        ):
            with patch("api.routers.admin_show_links._discover_show_links", side_effect=_slow_show_discovery):
                with patch("api.routers.admin_show_links._discover_season_links", return_value=[]):
                    with patch("api.routers.admin_show_links._discover_people_links", return_value=[]):
                        with patch(
                            "api.routers.admin_show_links._normalize_legacy_knowledge_link_kinds",
                            return_value=0,
                        ):
                            with patch(
                                "api.routers.admin_show_links._cleanup_invalid_person_knowledge_links",
                                return_value={
                                    "scanned": 0,
                                    "deleted": 0,
                                    "promoted": 0,
                                    "deleted_by_reason": {},
                                    "validation_failures": 0,
                                },
                            ):
                                with patch(
                                    "api.routers.admin_show_links._cleanup_invalid_person_social_links",
                                    return_value={
                                        "scanned": 0,
                                        "deleted": 0,
                                        "promoted": 0,
                                        "deleted_by_reason": {},
                                        "validation_failures": 0,
                                    },
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._cleanup_invalid_show_knowledge_links",
                                        return_value={
                                            "scanned": 0,
                                            "deleted": 0,
                                            "manual_skipped": 0,
                                            "deleted_by_reason": {},
                                            "validation_failures": 0,
                                        },
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links._promote_pending_links_to_approved",
                                            return_value=0,
                                        ):
                                            with patch(
                                                "api.routers.admin_show_links._normalize_existing_social_handle_urls",
                                                return_value={"scanned": 0, "normalized": 0, "deleted_duplicates": 0},
                                            ):
                                                result = admin_show_links._run_show_link_discovery(
                                                    show_id_str=show_id,
                                                    payload=admin_show_links.LinkDiscoverRequest(
                                                        include_seasons=True,
                                                        include_people=True,
                                                    ),
                                                    db=db,
                                                    actor="admin@example.com",
                                                )

    assert result["timed_out"] is True
    assert result["status"] == "timed_out"
    timeout = result.get("timeout")
    assert isinstance(timeout, dict)
    assert timeout.get("reason") == "server_processing_timeout"
    assert isinstance(timeout.get("stage"), str)
    assert isinstance(timeout.get("elapsed_ms"), int)
    assert isinstance(timeout.get("budget_ms"), int)


def test_count_discovery_scan_targets_uses_links_eligible_people_count() -> None:
    show_id = str(uuid4())

    with patch("api.routers.admin_show_links.pg.fetch_one", return_value={"season_count": 7}):
        with patch(
            "api.routers.admin_show_links.show_reads_repo.get_show_links_eligible_people",
            return_value=([{"person_id": "person-1"}, {"person_id": "person-2"}], 2),
        ):
            counts = admin_show_links._count_discovery_scan_targets(show_id)

    assert counts == {"show_scanned": 1, "season_scanned": 7, "people_scanned": 2}


def test_run_show_link_discovery_emits_stage_progress_snapshots() -> None:
    progress_events: list[tuple[str, dict[str, object]]] = []

    def _discover_people_links(_show_id: str, *, show_fandom_seed_urls=None, stats=None):
        assert stats is not None
        stats["people_total_targets"] = 9
        stats["people_processed"] = 4
        stats["people_links_discovered"] = 11
        stats["people_with_links"] = 3
        stats["current_person_name"] = "Heather Gay"
        return []

    with patch(
        "api.routers.admin_show_links._count_discovery_scan_targets",
        return_value={"show_scanned": 1, "season_scanned": 4, "people_scanned": 9},
    ):
        with patch(
            "api.routers.admin_show_links._discover_show_links",
            return_value=[
                {
                    "entity_type": "show",
                    "entity_id": "show-1",
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "fandom",
                    "url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                    "source": "derived",
                    "metadata": {
                        "page_title": "The Real Housewives of Salt Lake City",
                        "site_title": "The Real Housewives Wiki",
                    },
                }
            ],
        ):
            with patch("api.routers.admin_show_links._discover_season_links", return_value=[]):
                with patch("api.routers.admin_show_links._discover_people_links", side_effect=_discover_people_links):
                    with patch("api.routers.admin_show_links._upsert_link", return_value=None):
                        with patch(
                            "api.routers.admin_show_links._normalize_legacy_knowledge_link_kinds",
                            return_value=0,
                        ):
                            with patch(
                                "api.routers.admin_show_links._cleanup_invalid_person_knowledge_links",
                                return_value={
                                    "scanned": 0,
                                    "deleted": 0,
                                    "promoted": 0,
                                    "deleted_by_reason": {},
                                    "validation_failures": 0,
                                },
                            ):
                                with patch(
                                    "api.routers.admin_show_links._cleanup_invalid_person_social_links",
                                    return_value={
                                        "scanned": 0,
                                        "deleted": 0,
                                        "promoted": 0,
                                        "deleted_by_reason": {},
                                        "validation_failures": 0,
                                    },
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._cleanup_invalid_show_knowledge_links",
                                        return_value={
                                            "scanned": 0,
                                            "deleted": 0,
                                            "manual_skipped": 0,
                                            "deleted_by_reason": {},
                                            "validation_failures": 0,
                                        },
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links._promote_pending_links_to_approved",
                                            return_value=0,
                                        ):
                                            with patch(
                                                "api.routers.admin_show_links._normalize_existing_social_handle_urls",
                                                return_value={"scanned": 2, "normalized": 1, "deleted_duplicates": 0},
                                            ):
                                                admin_show_links._run_show_link_discovery(
                                                    show_id_str="show-1",
                                                    payload=admin_show_links.LinkDiscoverRequest(
                                                        include_seasons=True,
                                                        include_people=True,
                                                    ),
                                                    db=MagicMock(),
                                                    actor="admin@example.com",
                                                    stage_callback=lambda stage, payload: progress_events.append(
                                                        (stage, payload)
                                                    ),
                                                )

    show_start_payload = next(payload for stage, payload in progress_events if stage == "show_discovery_started")
    show_complete_payload = next(payload for stage, payload in progress_events if stage == "show_discovery_completed")
    people_complete_payload = next(
        payload for stage, payload in progress_events if stage == "people_discovery_completed"
    )

    assert show_start_payload["current_stage"] == "show_discovery_started"
    assert show_start_payload["discovered_rows"] == 0
    assert show_start_payload["scan_targets"] == {"show_scanned": 1, "season_scanned": 4, "people_scanned": 9}
    assert isinstance(show_start_payload["stage_budget"], dict)
    assert show_start_payload["stage_budget"]["max_fandom_candidates"] > 0
    assert show_start_payload["stage_progress"] == {
        "processed_targets": 0,
        "total_targets": 1,
        "links_discovered": 0,
        "targets_with_links": 0,
    }
    assert show_start_payload["target_progress"] == {
        "shows": {"completed": 0, "total": 1},
        "seasons": {"completed": 0, "total": 4},
        "cast_members": {"completed": 0, "total": 9},
    }

    assert show_complete_payload["current_stage"] == "show_discovery_completed"
    assert show_complete_payload["discovered_rows"] == 1
    assert show_complete_payload["rows"] == 1
    assert isinstance(show_complete_payload["stage_elapsed_ms"], int)
    assert show_complete_payload["validated_live_counts_by_source"] == {"fandom": 1}
    assert show_complete_payload["target_progress"] == {
        "shows": {"completed": 1, "total": 1},
        "seasons": {"completed": 0, "total": 4},
        "cast_members": {"completed": 0, "total": 9},
    }
    assert people_complete_payload["stage_progress"] == {
        "processed_targets": 4,
        "total_targets": 9,
        "current_target_label": "Heather Gay",
        "links_discovered": 11,
        "targets_with_links": 3,
    }
    assert people_complete_payload["target_progress"] == {
        "shows": {"completed": 1, "total": 1},
        "seasons": {"completed": 4, "total": 4},
        "cast_members": {"completed": 4, "total": 9},
    }
    social_repair_payload = next(
        payload for stage, payload in progress_events if stage == "social_url_repair_completed"
    )
    assert social_repair_payload["normalized"] == 1
    cleanup_payload = next(payload for stage, payload in progress_events if stage == "cleanup_completed")
    assert cleanup_payload["stage_progress"] == {
        "validated_links": 0,
        "promoted_links": 0,
        "deleted_links": 0,
        "normalized_social_urls": 1,
    }


def test_run_show_link_discovery_filters_fandom_seed_urls_to_canonical_show_pages() -> None:
    progress_events: list[tuple[str, dict[str, object]]] = []
    season_seed_inputs: list[list[str]] = []
    people_seed_inputs: list[list[str]] = []

    def _discover_season_links(_show_id: str, *, show_fandom_seed_urls=None, stats=None):
        season_seed_inputs.append(list(show_fandom_seed_urls or []))
        return []

    def _discover_people_links(_show_id: str, *, show_fandom_seed_urls=None, stats=None):
        people_seed_inputs.append(list(show_fandom_seed_urls or []))
        return []

    with patch(
        "api.routers.admin_show_links._count_discovery_scan_targets",
        return_value={"show_scanned": 1, "season_scanned": 4, "people_scanned": 9},
    ):
        with patch(
            "api.routers.admin_show_links._discover_show_links",
            return_value=[
                {
                    "entity_type": "show",
                    "entity_id": "show-1",
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "fandom",
                    "url": "https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki",
                    "source": "franchise_rule",
                    "metadata": {"page_title": "Real Housewives Wiki", "site_title": "The Real Housewives Wiki"},
                },
                {
                    "entity_type": "show",
                    "entity_id": "show-1",
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "fandom",
                    "url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                    "source": "franchise_rule:derived_show_page",
                    "metadata": {
                        "page_title": "The Real Housewives of Salt Lake City",
                        "site_title": "The Real Housewives Wiki",
                    },
                },
            ],
        ):
            with patch("api.routers.admin_show_links._discover_season_links", side_effect=_discover_season_links):
                with patch("api.routers.admin_show_links._discover_people_links", side_effect=_discover_people_links):
                    with patch("api.routers.admin_show_links._upsert_link", return_value=None):
                        with patch(
                            "api.routers.admin_show_links._normalize_legacy_knowledge_link_kinds",
                            return_value=0,
                        ):
                            with patch(
                                "api.routers.admin_show_links._cleanup_invalid_person_knowledge_links",
                                return_value={
                                    "scanned": 0,
                                    "deleted": 0,
                                    "promoted": 0,
                                    "deleted_by_reason": {},
                                    "validation_failures": 0,
                                },
                            ):
                                with patch(
                                    "api.routers.admin_show_links._cleanup_invalid_person_social_links",
                                    return_value={
                                        "scanned": 0,
                                        "deleted": 0,
                                        "promoted": 0,
                                        "deleted_by_reason": {},
                                        "validation_failures": 0,
                                    },
                                ):
                                    with patch(
                                        "api.routers.admin_show_links._cleanup_invalid_show_knowledge_links",
                                        return_value={
                                            "scanned": 0,
                                            "deleted": 0,
                                            "manual_skipped": 0,
                                            "deleted_by_reason": {},
                                            "validation_failures": 0,
                                        },
                                    ):
                                        with patch(
                                            "api.routers.admin_show_links._promote_pending_links_to_approved",
                                            return_value=0,
                                        ):
                                            with patch(
                                                "api.routers.admin_show_links._normalize_existing_social_handle_urls",
                                                return_value={"scanned": 0, "normalized": 0, "deleted_duplicates": 0},
                                            ):
                                                admin_show_links._run_show_link_discovery(
                                                    show_id_str="show-1",
                                                    payload=admin_show_links.LinkDiscoverRequest(
                                                        include_seasons=True,
                                                        include_people=True,
                                                    ),
                                                    db=MagicMock(),
                                                    actor="admin@example.com",
                                                    stage_callback=lambda stage, payload: progress_events.append(
                                                        (stage, payload)
                                                    ),
                                                )

    expected_seed = ["https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City"]
    assert season_seed_inputs == [expected_seed]
    assert people_seed_inputs == [expected_seed]
    season_started_payload = next(payload for stage, payload in progress_events if stage == "season_discovery_started")
    people_started_payload = next(payload for stage, payload in progress_events if stage == "people_discovery_started")
    assert season_started_payload["seed_urls"] == 1
    assert people_started_payload["seed_urls"] == 1


def test_discover_show_links_stream_emits_progress_events_before_complete() -> None:
    show_id = UUID(str(uuid4()))
    db = MagicMock()

    def _run_discovery(**kwargs):
        stage_callback = kwargs.get("stage_callback")
        assert callable(stage_callback)
        stage_callback("show_discovery_started", {"rows": 0})
        stage_callback("show_discovery_completed", {"rows": 2})
        return {
            "show_id": str(show_id),
            "run_id": "run-123",
            "duration_ms": 1234,
            "discovered": 2,
            "stage_counts": {"show_scanned": 1, "season_scanned": 0, "people_scanned": 0},
            "stage_timings_ms": {"show_stage_ms": 10, "season_stage_ms": 0, "people_stage_ms": 0, "validation_ms": 1},
            "status_counts": {"approved_added": 2, "deleted_invalid": 0, "skipped_fetch_error": 0},
            "validation_reasons": {},
            "status": "ok",
            "timed_out": False,
            "timeout": None,
        }

    with patch("api.routers.admin_show_links._show_exists", return_value=True):
        with patch("api.routers.admin_show_links._run_show_link_discovery", side_effect=_run_discovery):
            response = admin_show_links.discover_show_links_stream(
                show_id=show_id,
                payload=admin_show_links.LinkDiscoverRequest(include_seasons=True, include_people=True),
                db=db,
                admin={"email": "admin@example.com"},
                request=None,  # type: ignore[arg-type]
            )

            async def _read_chunks() -> list[str]:
                chunks: list[str] = []
                async for chunk in response.body_iterator:
                    if isinstance(chunk, bytes):
                        chunks.append(chunk.decode("utf-8", errors="ignore"))
                    else:
                        chunks.append(str(chunk))
                return chunks

            body_chunks = asyncio.run(_read_chunks())

    payload = "".join(body_chunks)
    start_index = payload.find('"stage": "starting"')
    show_started_index = payload.find('"stage": "show_discovery_started"')
    show_completed_index = payload.find('"stage": "show_discovery_completed"')
    complete_index = payload.find("event: complete")
    assert start_index != -1
    assert show_started_index != -1
    assert show_completed_index != -1
    assert complete_index != -1
    assert start_index < show_started_index < show_completed_index < complete_index
    assert '"stage_timings_ms"' in payload
    assert '"current_stage"' in payload


def test_discover_show_links_stream_includes_operation_contract_fields(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_links._show_exists", return_value=True):
            with patch(
                "api.routers.admin_show_links._run_show_link_discovery",
                return_value={
                    "show_id": show_id,
                    "run_id": "run-456",
                    "duration_ms": 25,
                    "discovered": 1,
                    "stage_counts": {"show_scanned": 1, "season_scanned": 0, "people_scanned": 0},
                    "stage_timings_ms": {
                        "show_stage_ms": 10,
                        "season_stage_ms": 0,
                        "people_stage_ms": 0,
                        "validation_ms": 1,
                    },
                    "status_counts": {"approved_added": 1, "deleted_invalid": 0, "skipped_fetch_error": 0},
                    "validation_reasons": {},
                    "status": "ok",
                    "timed_out": False,
                    "timeout": None,
                },
            ):
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/links/discover/stream",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"include_seasons": True, "include_people": True},
                )

    assert response.status_code == 200
    assert "event: complete" in response.text
    assert '"operation_id"' in response.text
    event_seq_matches = [int(match) for match in re.findall(r'"event_seq"\s*:\s*(\d+)', response.text)]
    assert event_seq_matches
    assert event_seq_matches == sorted(event_seq_matches)
    assert len(event_seq_matches) == len(set(event_seq_matches))


def test_validated_or_carried_person_source_url_falls_back_to_candidate_on_fetch_error() -> None:
    with patch(
        "api.routers.admin_show_links._validate_person_knowledge_url",
        return_value=(None, "fetch_error"),
    ):
        with patch(
            "api.routers.admin_show_links._load_preapproved_person_source_url",
            return_value=None,
        ):
            result = admin_show_links._validated_or_carried_person_source_url(
                person_id=str(uuid4()),
                candidate_url="https://www.imdb.com/name/nm0001086/",
                kind="imdb",
                expected_name="Alan Cumming",
                fandom_allowlist=(),
            )

    assert result == "https://www.imdb.com/name/nm0001086"
