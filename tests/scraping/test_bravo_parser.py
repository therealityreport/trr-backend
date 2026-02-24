from __future__ import annotations

from dataclasses import dataclass

import pytest
import requests

from trr_backend.scraping import bravo_parser


@dataclass
class _FakeResponse:
    url: str
    text: str

    def raise_for_status(self) -> None:
        return None


def _mock_get_factory(pages: dict[str, str]):
    def _mock_get(url: str, **_: object) -> _FakeResponse:
        if url not in pages:
            raise requests.RequestException(f"unexpected url: {url}")
        return _FakeResponse(url=url, text=pages[url])

    return _mock_get


def test_parse_show_videos_extracts_runtime_and_clip_url(monkeypatch: pytest.MonkeyPatch) -> None:
    show_url = "https://www.bravotv.com/the-valley"
    watch_videos_url = "https://www.bravotv.com/the-valley/watch/videos"
    videos_url = "https://www.bravotv.com/the-valley/videos"
    clip_url = "https://www.bravotv.com/the-valley/video/the-valley-persian-style"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                watch_videos_url: """
                    <html><body></body></html>
                """,
                videos_url: """
                    <html><body>
                      <article>
                        <a href=\"/the-valley/video/the-valley-persian-style\">Watch</a>
                        <h3>The Valley Persian Style</h3>
                        <p>2:34 · Season 1</p>
                        <img src=\"/images/clip.jpg\" />
                      </article>
                    </body></html>
                """,
                clip_url: """
                    <html><head>
                      <meta property=\"article:published_time\" content=\"2026-02-09T13:54:05-05:00\" />
                    </head><body></body></html>
                """,
            }
        ),
    )

    videos = bravo_parser.parse_show_videos(show_url)

    assert len(videos) == 1
    assert videos[0]["title"] == "The Valley Persian Style"
    assert videos[0]["runtime"] == "2:34"
    assert videos[0]["clip_url"] == clip_url
    assert videos[0]["published_at"] == "2026-02-09T13:54:05-05:00"


def test_parse_show_news_extracts_headline_image_and_url(monkeypatch: pytest.MonkeyPatch) -> None:
    show_url = "https://www.bravotv.com/the-valley"
    news_url = "https://www.bravotv.com/the-valley/news"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                news_url: """
                    <html><body>
                      <article>
                        <a href=\"/the-valley/news/jax-and-brittany-update\">Read</a>
                        <h3>Jax and Brittany Share a New Update</h3>
                        <img src=\"/images/news.jpg\" />
                        <time datetime=\"2026-02-11T13:12:39-05:00\">February 11, 2026</time>
                      </article>
                    </body></html>
                """,
            }
        ),
    )

    news = bravo_parser.parse_show_news(show_url)

    assert len(news) == 1
    assert news[0]["headline"] == "Jax and Brittany Share a New Update"
    assert news[0]["article_url"] == "https://www.bravotv.com/the-valley/news/jax-and-brittany-update"
    assert news[0]["image_url"] == "https://www.bravotv.com/images/news.jpg"
    assert news[0]["published_at"] == "2026-02-11T13:12:39-05:00"


def test_parse_show_news_ignores_unrelated_latest_sidebar_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    show_url = "https://www.bravotv.com/summer-house"
    news_url = "https://www.bravotv.com/summer-house/news"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                news_url: """
                    <html><body>
                      <article>
                        <a href=\"/the-daily-dish/where-is-jules-daoud-summer-house-now\">Read</a>
                        <h3>Where is Jules Daoud from Summer House Now?</h3>
                        <img src=\"/images/relevant.jpg\" />
                      </article>
                      <article>
                        <a href=\"/the-daily-dish/rachel-zoe-sheds-light-on-divorce-ring\">Read</a>
                        <h3>Rachel Zoe Sheds New Light on Her Massive \"Divorce Ring\"</h3>
                        <img src=\"/images/unrelated.jpg\" />
                      </article>
                    </body></html>
                """,
            }
        ),
    )

    news = bravo_parser.parse_show_news(
        show_url,
        show_title="Summer House",
        person_urls=["https://www.bravotv.com/people/lindsay-hubbard"],
    )

    assert len(news) == 1
    assert news[0]["headline"] == "Where is Jules Daoud from Summer House Now?"
    assert "rachel-zoe" not in news[0]["article_url"]


def test_parse_person_page_excludes_global_footer_social_handles(monkeypatch: pytest.MonkeyPatch) -> None:
    person_url = "https://www.bravotv.com/people/janet-caperna"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                person_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"Janet Caperna\" />
                        <meta property=\"og:description\" content=\"Janet bio text\" />
                        <meta property=\"og:image\" content=\"/images/janet.jpg\" />
                      </head>
                      <body>
                        <a href=\"https://www.instagram.com/janetcaperna\">IG</a>
                        <a href=\"https://www.instagram.com/bravotv\">Bravo Footer</a>
                      </body>
                    </html>
                """,
            }
        ),
    )

    person = bravo_parser.parse_person_page(person_url)

    assert person["bio"] == "Janet bio text"
    assert person["hero_image_url"] == "https://www.bravotv.com/images/janet.jpg"
    assert person["social_links"].get("instagram") == "janetcaperna"
    assert "bravotv" not in person["social_links"].values()


def test_parse_person_page_profile_essentials_mode_skips_related_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person_url = "https://www.bravotv.com/people/andy-cohen"
    requested_urls: list[str] = []

    def _mock_get(url: str, **_: object) -> _FakeResponse:
        requested_urls.append(url)
        if url != person_url:
            raise requests.RequestException(f"unexpected url: {url}")
        return _FakeResponse(
            url=url,
            text="""
                <html>
                  <head>
                    <meta property="og:title" content="Andy Cohen" />
                    <meta property="og:description" content="Andy bio text" />
                    <meta property="og:image" content="/images/andy.jpg" />
                  </head>
                  <body>
                    <a href="https://www.instagram.com/bravoandy">IG</a>
                    <a href="/people/andy-cohen/videos/clip-1">Video</a>
                    <a href="/the-daily-dish/andy-story">Story</a>
                  </body>
                </html>
            """,
        )

    monkeypatch.setattr(bravo_parser.requests, "get", _mock_get)

    person = bravo_parser.parse_person_page(
        person_url,
        include_related_content=False,
        hydrate_related_dates=False,
    )

    assert requested_urls == [person_url]
    assert person["name"] == "Andy Cohen"
    assert person["bio"] == "Andy bio text"
    assert person["hero_image_url"] == "https://www.bravotv.com/images/andy.jpg"
    assert person["videos"] == []
    assert person["news"] == []


def test_parse_show_page_extracts_day_time_airs_text(monkeypatch: pytest.MonkeyPatch) -> None:
    show_url = "https://www.bravotv.com/summer-house"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                show_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"Summer House\" />
                        <meta property=\"og:description\" content=\"Description\" />
                      </head>
                      <body>
                        <div>Tuesdays at 8/7c</div>
                      </body>
                    </html>
                """,
            }
        ),
    )

    show = bravo_parser.parse_show_page(show_url)
    assert show["airs_text"] == "Tuesdays at 8/7c"


def test_parse_show_page_image_candidates_exclude_video_and_news_cards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    show_url = "https://www.bravotv.com/summer-house"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                show_url: """
                    <html>
                      <head>
                        <meta property=\"og:image\" content=\"/images/key-art.jpg\" />
                      </head>
                      <body>
                        <img src=\"/images/show-poster.jpg\" alt=\"Summer House key art\" />
                        <a href=\"/summer-house/videos/clip-one\">
                          <img src=\"/images/video-thumb.jpg\" alt=\"Video card\" />
                        </a>
                        <a href=\"/summer-house/news/story-one\">
                          <img src=\"/images/news-thumb.jpg\" alt=\"News card\" />
                        </a>
                      </body>
                    </html>
                """,
            }
        ),
    )

    show = bravo_parser.parse_show_page(show_url)
    image_urls = {item["url"] for item in show["image_candidates"]}

    assert "https://www.bravotv.com/images/key-art.jpg" in image_urls
    assert "https://www.bravotv.com/images/show-poster.jpg" in image_urls
    assert "https://www.bravotv.com/images/video-thumb.jpg" not in image_urls
    assert "https://www.bravotv.com/images/news-thumb.jpg" not in image_urls


def test_parse_bravo_show_bundle_candidate_people_only_uses_explicit_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    show_url = "https://www.bravotv.com/the-valley"
    explicit_person_url = "https://www.bravotv.com/people/andy-cohen"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                show_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"The Valley\" />
                        <meta property=\"og:description\" content=\"Description\" />
                      </head>
                      <body>
                        <a href=\"/people/discovered-only\">Discovered Person</a>
                      </body>
                    </html>
                """,
                explicit_person_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"Andy Cohen\" />
                        <meta property=\"og:description\" content=\"Bio text\" />
                        <meta property=\"og:image\" content=\"/images/andy.jpg\" />
                      </head>
                      <body>
                        <a href=\"https://www.instagram.com/bravoandy\">IG</a>
                      </body>
                    </html>
                """,
            }
        ),
    )

    bundle = bravo_parser.parse_bravo_show_bundle(
        show_url,
        include_videos=False,
        include_news=False,
        person_url_candidates=[explicit_person_url],
        candidate_people_only=True,
    )

    assert [result["url"] for result in bundle["person_candidate_results"]] == [explicit_person_url]
    assert [result["status"] for result in bundle["person_candidate_results"]] == ["ok"]
    assert [person["canonical_url"] for person in bundle["people"]] == [explicit_person_url]


def test_parse_bravo_show_bundle_default_merges_candidates_with_discovered_people(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    show_url = "https://www.bravotv.com/the-valley"
    explicit_person_url = "https://www.bravotv.com/people/andy-cohen"
    discovered_person_url = "https://www.bravotv.com/people/discovered-only"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                show_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"The Valley\" />
                        <meta property=\"og:description\" content=\"Description\" />
                      </head>
                      <body>
                        <a href=\"/people/discovered-only\">Discovered Person</a>
                      </body>
                    </html>
                """,
                explicit_person_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"Andy Cohen\" />
                        <meta property=\"og:description\" content=\"Bio text\" />
                      </head>
                      <body></body>
                    </html>
                """,
                discovered_person_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"Discovered Person\" />
                        <meta property=\"og:description\" content=\"Bio text\" />
                      </head>
                      <body></body>
                    </html>
                """,
            }
        ),
    )

    bundle = bravo_parser.parse_bravo_show_bundle(
        show_url,
        include_videos=False,
        include_news=False,
        person_url_candidates=[explicit_person_url],
    )

    result_urls = {result["url"] for result in bundle["person_candidate_results"]}
    assert result_urls == {explicit_person_url, discovered_person_url}


def test_parse_bravo_show_bundle_includes_candidate_people_and_skips_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    show_url = "https://www.bravotv.com/the-real-housewives-of-salt-lake-city"
    valid_person_url = "https://www.bravotv.com/people/andy-cohen"
    invalid_person_url = "https://www.bravotv.com/people/john-barlow"

    monkeypatch.setattr(
        bravo_parser.requests,
        "get",
        _mock_get_factory(
            {
                show_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"The Real Housewives of Salt Lake City\" />
                        <meta property=\"og:description\" content=\"Description\" />
                      </head>
                      <body></body>
                    </html>
                """,
                valid_person_url: """
                    <html>
                      <head>
                        <meta property=\"og:title\" content=\"Andy Cohen\" />
                        <meta property=\"og:description\" content=\"Bio\" />
                        <meta property=\"og:image\" content=\"/images/andy.jpg\" />
                      </head>
                      <body>
                        <h1>Andy Cohen</h1>
                      </body>
                    </html>
                """,
                invalid_person_url: """
                    <html>
                      <head>
                        <title>Page Not Found</title>
                      </head>
                      <body>
                        <h1>Page Not Found</h1>
                        <p>Sorry we couldn’t find what you were looking for.</p>
                      </body>
                    </html>
                """,
            }
        ),
    )

    bundle = bravo_parser.parse_bravo_show_bundle(
        show_url,
        include_videos=False,
        include_news=False,
        include_people=True,
        person_url_candidates=[valid_person_url, invalid_person_url],
    )

    assert [person["canonical_url"] for person in bundle["people"]] == [valid_person_url]
    assert bundle["discovered_person_urls"] == [valid_person_url]
    results = bundle["person_candidate_results"]
    assert len(results) == 2
    status_by_url = {row["url"]: row["status"] for row in results}
    assert status_by_url[valid_person_url] == "ok"
    assert status_by_url[invalid_person_url] == "missing"


def test_probe_bravo_person_url_candidates_yields_deterministic_order_and_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    andy_url = "https://www.bravotv.com/people/andy-cohen"
    missing_url = "https://www.bravotv.com/people/not-found"
    error_url = "https://www.bravotv.com/people/network-error"

    def _fake_parse(url: str) -> dict[str, object]:
        if url == missing_url:
            raise requests.RequestException(f"Bravo person page not found: {url}")
        if url == error_url:
            raise requests.RequestException("upstream timeout")
        return {
            "canonical_url": url,
            "name": "Resolved Person",
        }

    monkeypatch.setattr(bravo_parser, "parse_person_page", _fake_parse)

    probes = list(
        bravo_parser.probe_bravo_person_url_candidates(
            [andy_url, missing_url, error_url],
            max_people=10,
        )
    )

    assert [probe["candidate_url"] for probe in probes] == [andy_url, missing_url, error_url]
    assert [probe["status"] for probe in probes] == ["ok", "missing", "error"]
    assert probes[0]["url"] == andy_url
    assert probes[1]["url"] == missing_url
    assert probes[2]["url"] == error_url


def test_probe_bravo_person_url_candidates_respects_max_people_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed_urls: list[str] = []

    def _fake_parse(url: str) -> dict[str, object]:
        parsed_urls.append(url)
        return {
            "canonical_url": url,
            "name": "Resolved Person",
        }

    monkeypatch.setattr(bravo_parser, "parse_person_page", _fake_parse)

    probes = list(
        bravo_parser.probe_bravo_person_url_candidates(
            [
                "https://www.bravotv.com/people/a",
                "https://www.bravotv.com/people/b",
                "https://www.bravotv.com/people/c",
            ],
            max_people=2,
        )
    )

    assert len(probes) == 2
    assert parsed_urls == [
        "https://www.bravotv.com/people/a",
        "https://www.bravotv.com/people/b",
    ]


def test_probe_bravo_person_url_candidates_passes_lightweight_parse_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called_kwargs: list[dict[str, object]] = []

    def _fake_parse(url: str, **kwargs: object) -> dict[str, object]:
        called_kwargs.append(dict(kwargs))
        return {
            "canonical_url": url,
            "name": "Resolved Person",
        }

    monkeypatch.setattr(bravo_parser, "parse_person_page", _fake_parse)

    probes = list(
        bravo_parser.probe_bravo_person_url_candidates(
            ["https://www.bravotv.com/people/a"],
            max_people=1,
            include_related_content=False,
            hydrate_related_dates=False,
        )
    )

    assert len(probes) == 1
    assert probes[0]["status"] == "ok"
    assert called_kwargs == [
        {
            "include_related_content": False,
            "hydrate_related_dates": False,
        }
    ]
