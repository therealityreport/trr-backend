from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from trr_backend.socials.instagram.scraper import InstagramScraper
from trr_backend.socials.instagram.scraper import ScrapeConfig as InstagramScrapeConfig
from trr_backend.socials.tiktok.scraper import TikTokScrapeConfig, TikTokScraper
from trr_backend.socials.twitter.scraper import TwitterScrapeConfig, TwitterScraper
from trr_backend.socials.youtube.scraper import YouTubeScraper


@dataclass
class _FakeResponse:
    status_code: int
    payload: dict
    headers: dict[str, str] | None = None
    text: str = ""

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def json(self) -> dict:
        return self.payload


def test_tiktok_fetch_comments_adds_required_aid_param(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TikTokScraper()
    captured_params: dict[str, int] = {}

    def _fake_get(url: str, params: dict | None = None, **_: object) -> _FakeResponse:
        nonlocal captured_params
        captured_params = dict(params or {})
        return _FakeResponse(
            status_code=200,
            payload={
                "status_code": 0,
                "comments": [
                    {
                        "cid": "c1",
                        "text": "hello",
                        "user": {"unique_id": "tester", "uid": "u1"},
                        "create_time": 1735689600,
                        "digg_count": 5,
                        "reply_comment_total": 0,
                    }
                ],
                "has_more": 0,
                "cursor": 0,
            },
        )

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    comments = scraper.fetch_comments("123", username="acct", fetch_replies=False, delay=0)
    assert len(comments) == 1
    assert captured_params["aid"] == 1988


def test_tiktok_fetch_comments_sets_failure_reason_for_nonzero_status(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TikTokScraper()

    def _fake_get(url: str, params: dict | None = None, **_: object) -> _FakeResponse:
        return _FakeResponse(status_code=200, payload={"status_code": 5, "status_msg": "blocked"})

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    comments = scraper.fetch_comments("123", username="acct", fetch_replies=False, delay=0)
    assert comments == []
    assert scraper._last_api_fail_reason == "comment_status_5"


def test_youtube_parses_comment_view_model_schema() -> None:
    scraper = YouTubeScraper()
    entity_index = {
        "c1": {
            "properties": {
                "commentId": "c1",
                "content": {"content": "can confirm"},
                "publishedTime": "2 days ago",
            },
            "author": {"displayName": "@YouTube", "channelId": "chan-1"},
            "toolbar": {"likeCountNotliked": "1.2K", "replyCount": "34"},
        }
    }
    item = {
        "commentThreadRenderer": {
            "commentViewModel": {"commentViewModel": {"commentId": "c1"}},
        }
    }

    parsed = scraper._parse_comment_thread(  # noqa: SLF001
        item,
        "vid-1",
        "https://youtube.test/watch?v=vid-1",
        fetch_replies=False,
        delay=0,
        entity_index=entity_index,
    )
    assert parsed is not None
    assert parsed.comment_id == "c1"
    assert parsed.text == "can confirm"
    assert parsed.author == "@YouTube"
    assert parsed.likes == 1200
    assert parsed.reply_count == 34


def test_twitter_reply_fetch_retries_with_missing_feature_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    scraper._detail_hash = "abc123"  # bypass network hash discovery
    scraper._search_hash = "def456"

    requested_urls: list[str] = []

    first = _FakeResponse(
        status_code=400,
        payload={
            "errors": [
                {
                    "message": "The following features cannot be null: foo_flag, bar_flag",
                    "code": 336,
                }
            ]
        },
    )
    second = _FakeResponse(
        status_code=200,
        payload={
            "data": {
                "threaded_conversation_with_injections_v2": {
                    "instructions": []
                }
            }
        },
    )

    responses = [first, second]

    def _fake_get(url: str, **_: object) -> _FakeResponse:
        requested_urls.append(url)
        return responses.pop(0)

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    replies = scraper.fetch_tweet_replies("tweet-1", delay=0)
    assert replies == []
    assert len(requested_urls) == 2

    second_query = parse_qs(urlparse(requested_urls[1]).query)
    second_features = json.loads(second_query["features"][0])
    assert second_features["foo_flag"] is False
    assert second_features["bar_flag"] is False


def test_twitter_parse_tweet_result_reads_username_from_core_fallback() -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})

    parsed = scraper._parse_tweet_result(  # noqa: SLF001
        {
            "__typename": "Tweet",
            "legacy": {
                "id_str": "123",
                "full_text": "hello",
                "favorite_count": 1,
                "retweet_count": 2,
                "reply_count": 3,
                "quote_count": 4,
                "created_at": "Thu Feb 13 12:34:56 +0000 2026",
            },
            "core": {
                "user_results": {
                    "result": {
                        "core": {"screen_name": "tester", "name": "Test User"},
                        "is_blue_verified": True,
                    }
                }
            },
            "views": {"count": "10"},
        },
        TwitterScrapeConfig(query="x", date_start=datetime(2026, 2, 1), date_end=datetime(2026, 2, 14)),
    )
    assert parsed is not None
    assert parsed.username == "tester"
    assert parsed.display_name == "Test User"
    assert parsed.user_verified is True


def test_instagram_parse_post_node_populates_media_urls_and_thumbnail() -> None:
    scraper = InstagramScraper(cookies={})
    config = InstagramScrapeConfig(username="bravotv")
    node = {
        "id": "179999",
        "shortcode": "ABC123",
        "taken_at_timestamp": 1735689600,
        "edge_media_to_caption": {"edges": [{"node": {"text": "Caption"}}]},
        "edge_liked_by": {"count": 11},
        "edge_media_to_comment": {"count": 3},
        "display_url": "https://example.com/ig-primary.jpg",
    }

    parsed = scraper._parse_post_node(node, config)  # noqa: SLF001
    assert parsed.media_urls == ["https://example.com/ig-primary.jpg"]
    assert parsed.thumbnail_url == "https://example.com/ig-primary.jpg"


def test_tiktok_parse_post_item_populates_media_urls_and_thumbnail() -> None:
    scraper = TikTokScraper()
    config = TikTokScrapeConfig(username="bravotv")
    item = {
        "id": "12345",
        "createTime": 1735689600,
        "desc": "#RHOSLC sneak peek",
        "author": {"uniqueId": "bravotv", "nickname": "Bravo TV"},
        "stats": {"diggCount": 1, "commentCount": 2, "shareCount": 3, "playCount": 4},
        "music": {"title": "song", "authorName": "artist"},
        "video": {
            "playAddr": "https://example.com/play.mp4",
            "downloadAddr": "https://example.com/download.mp4",
            "cover": "https://example.com/cover.jpg",
            "dynamicCover": "https://example.com/dynamic.gif",
            "duration": 30,
        },
    }

    parsed = scraper._parse_post_item(item, config)  # noqa: SLF001
    assert parsed.media_urls[0] == "https://example.com/play.mp4"
    assert parsed.thumbnail_url == "https://example.com/play.mp4"


def test_tiktok_parse_ytdlp_metadata_populates_thumbnail_from_metadata() -> None:
    scraper = TikTokScraper()
    config = TikTokScrapeConfig(username="bravotv")
    metadata = {
        "id": "9999",
        "title": "Clip",
        "timestamp": 1735689600,
        "uploader": "bravotv",
        "thumbnail": "https://example.com/thumb-main.jpg",
        "thumbnails": [{"url": "https://example.com/thumb-alt.jpg"}],
    }

    parsed = scraper._parse_ytdlp_metadata(metadata, config)  # noqa: SLF001
    assert parsed.media_urls[0] == "https://example.com/thumb-main.jpg"
    assert parsed.thumbnail_url == "https://example.com/thumb-main.jpg"
