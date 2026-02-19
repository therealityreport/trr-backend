from __future__ import annotations

from dataclasses import dataclass

import pytest
import requests

from trr_backend.scraping import google_news_parser


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


def test_topic_url_to_rss_candidates_builds_topics_rss_url() -> None:
    topic_url = (
        "https://news.google.com/topics/"
        "CAAqKAgKIiJDQkFTRXdvTkwyY3ZNVEZvYlhBeGVtUndNQklDWlc0b0FBUAE?ceid=US:en&oc=3"
    )

    candidates = google_news_parser.topic_url_to_rss_candidates(topic_url)

    assert len(candidates) >= 2
    assert candidates[0].startswith("https://news.google.com/topics/")
    assert "/rss/topics/" in candidates[1]
    assert "ceid=US%3Aen" in candidates[1] or "ceid=US:en" in candidates[1]


def test_parse_rss_items_extracts_title_link_source_and_pubdate() -> None:
    xml = """
    <rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
      <channel>
        <item>
          <title>RHOSLC Cast Update</title>
          <link>https://example.com/story-1</link>
          <pubDate>Wed, 12 Feb 2026 09:00:00 -0500</pubDate>
          <source url="https://www.usmagazine.com">Us Weekly</source>
          <description><![CDATA[<p>Season 6 update</p>]]></description>
          <media:content url="https://img.example.com/1.jpg" medium="image" />
        </item>
      </channel>
    </rss>
    """

    items = google_news_parser.parse_rss_items(xml)

    assert len(items) == 1
    item = items[0]
    assert item["headline"] == "RHOSLC Cast Update"
    assert item["article_url"] == "https://example.com/story-1"
    assert item["publisher_name"] == "Us Weekly"
    assert item["publisher_domain"] == "usmagazine.com"
    assert item["summary"] == "Season 6 update"
    assert item["image_url"] == "https://img.example.com/1.jpg"
    assert item["published_at"] == "2026-02-12T14:00:00Z"
    assert item["feed_rank"] == 0


def test_fetch_google_news_falls_back_to_search_rss_when_topic_has_no_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topic_url = "https://news.google.com/topics/CAAqKAgKIiJDQkFTRXdvTkwyY3ZNVEZvYlhBeGVtUndNQklDWlc0b0FBUAE?ceid=US:en&oc=3"
    candidates = google_news_parser.topic_url_to_rss_candidates(topic_url)
    fallback_url = google_news_parser.build_search_rss_url(
        "The Real Housewives of Salt Lake City",
        ["RHOSLC"],
    )

    monkeypatch.setattr(
        google_news_parser.requests,
        "get",
        _mock_get_factory(
            {
                candidates[0]: "<rss><channel></channel></rss>",
                candidates[1]: "<rss><channel></channel></rss>",
                fallback_url: """
                    <rss version="2.0">
                      <channel>
                        <item>
                          <title>RHOSLC Season 6 Rumors</title>
                          <link>https://example.com/story-2</link>
                          <pubDate>Thu, 13 Feb 2026 10:15:00 +0000</pubDate>
                          <source url="https://people.com">People</source>
                        </item>
                      </channel>
                    </rss>
                """,
            }
        ),
    )

    result = google_news_parser.fetch_google_news(
        topic_url=topic_url,
        show_name="The Real Housewives of Salt Lake City",
        show_aliases=["RHOSLC"],
    )

    assert result["fallback_used"] is True
    assert result["resolved_feed_url"] == fallback_url
    assert len(result["items"]) == 1
    assert result["items"][0]["headline"] == "RHOSLC Season 6 Rumors"

