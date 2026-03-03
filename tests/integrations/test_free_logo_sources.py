from __future__ import annotations

from unittest.mock import MagicMock, patch

from trr_backend.integrations import free_logo_sources as mod


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
        url: str,
        payload: dict | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.url = url
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


def test_search_wikimedia_commons_logo_candidates_builds_special_filepath_urls() -> None:
    session = MagicMock()
    session.get.side_effect = [
        _FakeResponse(
            url=(
                "https://commons.wikimedia.org/w/index.php?search=Bravo+logo"
                "&title=Special%3AMediaSearch&type=image&filemime=svg"
            ),
            text=(
                "<html><body>"
                '<a href="/wiki/File:Bravo_logo.svg">Bravo logo</a>'
                "</body></html>"
            ),
        ),
        _FakeResponse(
            url=(
                "https://commons.wikimedia.org/w/index.php?search=Bravo+icon"
                "&title=Special%3AMediaSearch&type=image&filemime=svg"
            ),
            text=(
                "<html><body>"
                '<a href="/wiki/File:Bravo_symbol.png">Bravo symbol</a>'
                "</body></html>"
            ),
        ),
        _FakeResponse(
            url=(
                "https://commons.wikimedia.org/w/index.php?search=Bravo"
                "&title=Special%3AMediaSearch&type=image&filemime=svg"
            ),
            text="<html><body></body></html>",
        ),
    ]

    rows = mod.search_wikimedia_commons_logo_candidates(["Bravo"], session=session)

    assert len(rows) == 2
    assert rows[0].source_provider == "wikimedia_commons"
    assert "Special:FilePath/Bravo_logo.svg" in rows[0].url
    assert rows[0].discovered_from == "https://commons.wikimedia.org/wiki/File:Bravo_logo.svg"


def test_search_1000logos_logo_candidates_parses_article_images() -> None:
    search_url = "https://1000logos.net/?s=Bravo"
    article_url = "https://1000logos.net/bravo-logo/"

    def _fake_get(url: str, **_: object) -> _FakeResponse:
        if "?s=Bravo" in url:
            return _FakeResponse(
                url=search_url,
                text='<html><body><a href="/bravo-logo/">Bravo logo</a></body></html>',
            )
        if "bravo-logo" in url:
            return _FakeResponse(
                url=article_url,
                text=(
                    "<html><body>"
                    '<img alt="Bravo logo" src="https://1000logos.net/wp-content/uploads/2021/10/Bravo-Logo.png" />'
                    "</body></html>"
                ),
            )
        return _FakeResponse(status_code=404, url=url)

    session = MagicMock()
    session.get.side_effect = _fake_get

    rows = mod.search_1000logos_logo_candidates(["Bravo"], session=session)

    assert rows
    assert rows[0].source_provider == "logos1000"
    assert rows[0].discovered_from == article_url
    assert "Bravo-Logo.png" in rows[0].url


def test_extract_official_logo_candidates_includes_brand_guidelines_page_assets() -> None:
    homepage_url = "https://example.com"
    guidelines_url = "https://example.com/brand-guidelines"

    def _fake_get(url: str, **_: object) -> _FakeResponse:
        if url.rstrip("/") == homepage_url:
            return _FakeResponse(
                url=homepage_url,
                text=(
                    "<html><head>"
                    '<meta property="og:image" content="/images/site-logo.png" />'
                    '<link rel="icon" href="/favicon.png" />'
                    "</head><body>"
                    '<a href="/brand-guidelines">Brand Guidelines</a>'
                    "</body></html>"
                ),
            )
        if "brand-guidelines" in url:
            return _FakeResponse(
                url=guidelines_url,
                text=(
                    "<html><body>"
                    '<img alt="Official wordmark" src="/assets/wordmark.svg" />'
                    "</body></html>"
                ),
            )
        return _FakeResponse(status_code=404, url=url)

    session = MagicMock()
    session.get.side_effect = _fake_get

    rows = mod.extract_official_logo_candidates([homepage_url], session=session)

    providers = {row.source_provider for row in rows}
    assert "official_site" in providers
    assert "favicon_appicons" in providers
    assert "brand_guidelines" in providers
    assert any("wordmark.svg" in row.url for row in rows)


def test_collect_free_logo_candidates_merges_logopedia_results() -> None:
    session = MagicMock()
    session.get.return_value = _FakeResponse(status_code=404, url="https://example.com")

    with patch(
        "trr_backend.integrations.free_logo_sources.fetch_logopedia_logo_candidates",
        return_value=["https://static.wikia.nocookie.net/logopedia/images/a/ab/Bravo_logo.svg"],
    ):
        rows = mod.collect_free_logo_candidates(
            target_label="Bravo",
            target_key="bravotv.com",
            discovered_from_urls=["https://www.bravotv.com"],
            session=session,
        )

    assert any(row.source_provider == "logos_fandom" for row in rows)
    assert any("Bravo_logo.svg" in row.url for row in rows)


def test_collect_free_logo_candidates_respects_source_provider_filter() -> None:
    with (
        patch(
            "trr_backend.integrations.free_logo_sources.search_wikimedia_commons_logo_candidates",
            return_value=[],
        ) as wikimedia_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.fetch_logopedia_logo_candidates",
            return_value=[],
        ) as fandom_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.search_1000logos_logo_candidates",
            return_value=[],
        ) as logos1000_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.search_worldvectorlogo_logo_candidates",
            return_value=[
                mod.FreeLogoCandidate(
                    url="https://worldvectorlogo.com/logo/bravo",
                    source_provider="worldvectorlogo",
                    discovered_from="https://worldvectorlogo.com/search/bravo",
                    context="search",
                )
            ],
        ) as worldvector_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.search_seeklogo_logo_candidates",
            return_value=[],
        ) as seeklogo_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.search_logowik_logo_candidates",
            return_value=[],
        ) as logowik_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.search_logo_wine_logo_candidates",
            return_value=[],
        ) as logo_wine_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.search_logosearch_logo_candidates",
            return_value=[],
        ) as logosearch_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.search_simple_icons_logo_candidates",
            return_value=[],
        ) as simpleicons_mock,
        patch(
            "trr_backend.integrations.free_logo_sources.extract_official_logo_candidates",
            return_value=[],
        ) as official_mock,
    ):
        rows = mod.collect_free_logo_candidates(
            target_label="Bravo",
            target_key="bravotv.com",
            source_provider="worldvectorlogo",
            discovered_from_urls=["https://www.bravotv.com"],
        )

    assert len(rows) == 1
    assert rows[0].source_provider == "worldvectorlogo"
    wikimedia_mock.assert_not_called()
    fandom_mock.assert_not_called()
    logos1000_mock.assert_not_called()
    worldvector_mock.assert_called_once()
    seeklogo_mock.assert_not_called()
    logowik_mock.assert_not_called()
    logo_wine_mock.assert_not_called()
    logosearch_mock.assert_not_called()
    simpleicons_mock.assert_not_called()
    official_mock.assert_not_called()


def test_collect_free_logo_candidates_filters_noisy_1000logos_assets() -> None:
    noisy = mod.FreeLogoCandidate(
        url="https://1000logos.net/assets/images/social/facebook.svg",
        source_provider="logos1000",
        discovered_from="https://1000logos.net/imdb-logo/",
        context="social",
    )
    useful = mod.FreeLogoCandidate(
        url="https://1000logos.net/wp-content/uploads/2022/10/IMDb-Logo.svg",
        source_provider="logos1000",
        discovered_from="https://1000logos.net/imdb-logo/",
        context="article",
    )
    with (
        patch("trr_backend.integrations.free_logo_sources.search_wikimedia_commons_logo_candidates", return_value=[]),
        patch("trr_backend.integrations.free_logo_sources.fetch_logopedia_logo_candidates", return_value=[]),
        patch(
            "trr_backend.integrations.free_logo_sources.search_1000logos_logo_candidates",
            return_value=[noisy, useful],
        ),
        patch("trr_backend.integrations.free_logo_sources.search_worldvectorlogo_logo_candidates", return_value=[]),
        patch("trr_backend.integrations.free_logo_sources.search_seeklogo_logo_candidates", return_value=[]),
        patch("trr_backend.integrations.free_logo_sources.search_logowik_logo_candidates", return_value=[]),
        patch("trr_backend.integrations.free_logo_sources.search_logo_wine_logo_candidates", return_value=[]),
        patch("trr_backend.integrations.free_logo_sources.search_logosearch_logo_candidates", return_value=[]),
        patch("trr_backend.integrations.free_logo_sources.search_simple_icons_logo_candidates", return_value=[]),
        patch("trr_backend.integrations.free_logo_sources.extract_official_logo_candidates", return_value=[]),
    ):
        rows = mod.collect_free_logo_candidates(
            target_label="IMDb",
            target_key="imdb.com",
            discovered_from_urls=["https://www.imdb.com/"],
        )

    assert any("IMDb-Logo.svg" in row.url for row in rows)
    assert all("assets/images/social" not in row.url for row in rows)


def test_collect_free_logo_candidates_scopes_official_sources_to_target_host() -> None:
    with patch(
        "trr_backend.integrations.free_logo_sources.extract_official_logo_candidates",
        return_value=[],
    ) as official_mock, patch(
        "trr_backend.integrations.free_logo_sources.search_wikimedia_commons_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.fetch_logopedia_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_1000logos_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_worldvectorlogo_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_seeklogo_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_logowik_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_logo_wine_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_logosearch_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_simple_icons_logo_candidates",
        return_value=[],
    ):
        mod.collect_free_logo_candidates(
            target_label="IMDb",
            target_key="imdb.com",
            discovered_from_urls=[
                "https://www.imdb.com/title/tt1234567/",
                "https://commons.wikimedia.org/wiki/File:IMDb_logo.svg",
            ],
            source_provider="official_site",
        )

    source_urls = official_mock.call_args.args[0]
    assert any(url.startswith("https://imdb.com") or url.startswith("https://www.imdb.com") for url in source_urls)
    assert all("commons.wikimedia.org" not in url for url in source_urls)


def test_collect_free_logo_candidates_uses_brand_name_query_term_only() -> None:
    with patch(
        "trr_backend.integrations.free_logo_sources.search_wikimedia_commons_logo_candidates",
        return_value=[],
    ) as wikimedia_mock, patch(
        "trr_backend.integrations.free_logo_sources.fetch_logopedia_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_1000logos_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_worldvectorlogo_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_seeklogo_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_logowik_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_logo_wine_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_logosearch_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.search_simple_icons_logo_candidates",
        return_value=[],
    ), patch(
        "trr_backend.integrations.free_logo_sources.extract_official_logo_candidates",
        return_value=[],
    ):
        mod.collect_free_logo_candidates(
            target_label="en.wikipedia.org",
            target_key="en.wikipedia.org",
            discovered_from_urls=["https://en.wikipedia.org/wiki/Main_Page"],
        )

    query_terms = wikimedia_mock.call_args.args[0]
    assert query_terms == ["wikipedia"]
