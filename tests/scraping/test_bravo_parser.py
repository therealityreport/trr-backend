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
