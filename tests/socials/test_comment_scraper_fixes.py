from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from trr_backend.socials.instagram.scraper import InstagramScraper
from trr_backend.socials.instagram.scraper import ScrapeConfig as InstagramScrapeConfig
from trr_backend.socials.tiktok.scraper import TikTokScrapeConfig, TikTokScraper
from trr_backend.socials.twitter.scraper import Tweet, TwitterScrapeConfig, TwitterScraper, mirror_tweet_media
from trr_backend.socials.youtube.scraper import YouTubeScrapeConfig, YouTubeScraper


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
    captured_timeout: object | None = None

    def _fake_get(url: str, params: dict | None = None, **_: object) -> _FakeResponse:
        nonlocal captured_params, captured_timeout
        captured_params = dict(params or {})
        captured_timeout = _.get("timeout")
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
    assert captured_timeout == scraper.REQUEST_TIMEOUT_SECONDS


def test_tiktok_fetch_comments_sets_failure_reason_for_nonzero_status(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TikTokScraper()

    def _fake_get(url: str, params: dict | None = None, **_: object) -> _FakeResponse:
        return _FakeResponse(status_code=200, payload={"status_code": 5, "status_msg": "blocked"})

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    comments = scraper.fetch_comments("123", username="acct", fetch_replies=False, delay=0)
    assert comments == []
    assert scraper._last_api_fail_reason == "comment_status_5"


def test_tiktok_fetch_user_detail_applies_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TikTokScraper()
    captured_timeout: object | None = None

    def _fake_get(url: str, params: dict | None = None, **kwargs: object) -> _FakeResponse:
        nonlocal captured_timeout
        captured_timeout = kwargs.get("timeout")
        return _FakeResponse(
            status_code=200,
            payload={"userInfo": {"user": {"secUid": "sec-1", "nickname": "Bravo"}}},
        )

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    payload = scraper.fetch_user_detail("bravotv", delay=0)
    assert payload is not None
    assert captured_timeout == scraper.REQUEST_TIMEOUT_SECONDS


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


def test_youtube_fetch_channel_videos_applies_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = YouTubeScraper()
    captured: dict[str, object] = {}

    def _fake_get(url: str, **kwargs: object) -> _FakeResponse:
        captured["url"] = url
        captured["timeout"] = kwargs.get("timeout")
        return _FakeResponse(
            status_code=200,
            payload={},
            text='var ytInitialData = {"contents":{}};',
        )

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    result = scraper.fetch_channel_videos("bravo", delay=0)
    assert result == {"contents": {}}
    assert captured["timeout"] == scraper.REQUEST_TIMEOUT_SECONDS


def test_youtube_parse_video_renderer_marks_shorts_and_canonical_url() -> None:
    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(channel_handle="bravo", keywords=[])
    renderer = {
        "videoId": "short12345",
        "title": {"runs": [{"text": "RHOSLC short"}]},
        "navigationEndpoint": {"commandMetadata": {"webCommandMetadata": {"url": "/shorts/short12345"}}},
        "ownerText": {
            "runs": [
                {
                    "text": "@Bravo",
                    "navigationEndpoint": {"browseEndpoint": {"canonicalBaseUrl": "/@Bravo"}},
                }
            ]
        },
    }

    parsed = scraper._parse_video_renderer(renderer, config, surface="shorts")  # noqa: SLF001
    assert parsed is not None
    assert parsed.is_short is True
    assert parsed.source_surface == "shorts"
    assert parsed.url == "https://www.youtube.com/shorts/short12345"


def test_youtube_process_video_data_filters_non_matching_owner() -> None:
    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(channel_handle="bravo", keywords=[])
    ownership_filtered = [0]
    renderers = [
        {
            "videoId": "vid-keep",
            "title": {"runs": [{"text": "RHOSLC recap"}]},
            "ownerText": {
                "runs": [
                    {
                        "text": "@Bravo",
                        "navigationEndpoint": {"browseEndpoint": {"canonicalBaseUrl": "/@Bravo"}},
                    }
                ]
            },
        },
        {
            "videoId": "vid-drop",
            "title": {"runs": [{"text": "RHOSLC recap"}]},
            "ownerText": {
                "runs": [
                    {
                        "text": "@OtherNetwork",
                        "navigationEndpoint": {"browseEndpoint": {"canonicalBaseUrl": "/@OtherNetwork"}},
                    }
                ]
            },
        },
    ]
    scraper._iter_video_renderers = lambda _data: iter(renderers)  # type: ignore[method-assign]

    videos = scraper._process_video_data(  # noqa: SLF001
        {"contents": {}},
        config,
        surface="videos",
        target_handle="bravo",
        ownership_filtered_counter=ownership_filtered,
    )
    assert [video.video_id for video in videos] == ["vid-keep"]
    assert ownership_filtered[0] == 1


def test_youtube_fetch_comments_applies_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = YouTubeScraper()
    captured_get: dict[str, object] = {}
    captured_post: dict[str, object] = {}

    def _fake_get(url: str, **kwargs: object) -> _FakeResponse:
        captured_get["url"] = url
        captured_get["timeout"] = kwargs.get("timeout")
        return _FakeResponse(status_code=200, payload={}, text="<html></html>")

    def _fake_post(url: str, **kwargs: object) -> _FakeResponse:
        captured_post["url"] = url
        captured_post["timeout"] = kwargs.get("timeout")
        return _FakeResponse(status_code=200, payload={})

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper, "_extract_ytinital_data", lambda text: {"contents": {}})
    monkeypatch.setattr(scraper, "_extract_comment_continuation", lambda data: "token-1")
    monkeypatch.setattr(scraper, "_build_comment_entity_index", lambda data: {})
    monkeypatch.setattr(scraper, "_parse_comment_response", lambda data: ([], None))
    monkeypatch.setattr(scraper.session, "get", _fake_get)
    monkeypatch.setattr(scraper.session, "post", _fake_post)

    comments = scraper.fetch_comments("vid-123", max_comments=10, fetch_replies=False, delay=0)
    assert comments == []
    assert captured_get["timeout"] == scraper.REQUEST_TIMEOUT_SECONDS
    assert captured_post["timeout"] == scraper.REQUEST_TIMEOUT_SECONDS


def test_youtube_estimate_publish_date_parses_absolute_premiere_date() -> None:
    scraper = YouTubeScraper()
    ts = scraper._estimate_publish_date("Premiered Oct 3, 2025")  # noqa: SLF001
    expected = int(datetime(2025, 10, 3, tzinfo=UTC).timestamp())
    assert ts == expected


def test_youtube_fetch_precise_publish_timestamp_parses_iso_upload_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = YouTubeScraper()
    html = (
        '<script>{"uploadDate":"2025-10-02T06:00:06-07:00"}</script>'
        '<meta itemprop="datePublished" content="2025-10-02T06:00:06-07:00">'
    )
    calls = 0

    def _fake_get(url: str, **kwargs: object) -> _FakeResponse:
        nonlocal calls
        del url, kwargs
        calls += 1
        return _FakeResponse(status_code=200, payload={}, text=html)

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    expected = int(datetime.fromisoformat("2025-10-02T06:00:06-07:00").timestamp())
    first = scraper._fetch_precise_publish_timestamp("vid-1", delay=0)  # noqa: SLF001
    second = scraper._fetch_precise_publish_timestamp("vid-1", delay=0)  # noqa: SLF001

    assert first == expected
    assert second == expected
    assert calls == 1


def test_youtube_process_video_data_refines_month_precision_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(
        channel_handle="bravo",
        keywords=["RHOSLC"],
        date_start=datetime(2025, 10, 1, tzinfo=UTC),
        date_end=datetime(2025, 10, 8, tzinfo=UTC),
    )
    out_of_range_estimate = int(datetime(2025, 11, 15, tzinfo=UTC).timestamp())
    precise_in_range = int(datetime(2025, 10, 3, tzinfo=UTC).timestamp())
    fetched: list[str] = []

    renderer = {
        "videoId": "vid-precise-1",
        "title": {"runs": [{"text": "RHOSLC preview"}]},
        "descriptionSnippet": {"runs": [{"text": "Bravo clip"}]},
        "publishedTimeText": {"simpleText": "4 months ago"},
        "thumbnail": {"thumbnails": [{"url": "https://example.com/thumb.jpg"}]},
    }

    monkeypatch.setattr(scraper, "_iter_video_renderers", lambda _data: iter([renderer]))
    monkeypatch.setattr(scraper, "_estimate_publish_date", lambda _text: out_of_range_estimate)

    def _fake_precise(video_id: str, delay: float = 2.0) -> int:
        del delay
        fetched.append(video_id)
        return precise_in_range

    monkeypatch.setattr(scraper, "_fetch_precise_publish_timestamp", _fake_precise)

    videos = scraper._process_video_data({}, config)  # noqa: SLF001

    assert fetched == ["vid-precise-1"]
    assert len(videos) == 1
    assert videos[0].published_at == precise_in_range


def test_youtube_process_video_data_refines_low_precision_even_when_initially_in_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(
        channel_handle="bravo",
        keywords=["RHOSLC"],
        date_start=datetime(2025, 8, 15, tzinfo=UTC),
        date_end=datetime(2026, 2, 20, tzinfo=UTC),
    )
    coarse_in_range = int(datetime(2025, 10, 22, tzinfo=UTC).timestamp())
    precise_expected = int(datetime(2025, 10, 2, tzinfo=UTC).timestamp())
    fetched: list[str] = []

    renderer = {
        "videoId": "vid-precise-2",
        "title": {"runs": [{"text": "RHOSLC clip"}]},
        "descriptionSnippet": {"runs": [{"text": "Bravo recap"}]},
        "publishedTimeText": {"simpleText": "4 months ago"},
        "thumbnail": {"thumbnails": [{"url": "https://example.com/thumb2.jpg"}]},
    }

    monkeypatch.setattr(scraper, "_iter_video_renderers", lambda _data: iter([renderer]))
    monkeypatch.setattr(scraper, "_estimate_publish_date", lambda _text: coarse_in_range)

    def _fake_precise(video_id: str, delay: float = 2.0) -> int:
        del delay
        fetched.append(video_id)
        return precise_expected

    monkeypatch.setattr(scraper, "_fetch_precise_publish_timestamp", _fake_precise)

    videos = scraper._process_video_data({}, config)  # noqa: SLF001

    assert fetched == ["vid-precise-2"]
    assert len(videos) == 1
    assert videos[0].published_at == precise_expected


def test_youtube_search_via_ytdlp_uses_date_aware_mode_for_windowed_scrapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(
        channel_handle="bravo",
        keywords=["RHOSLC"],
        date_start=datetime(2025, 10, 1, tzinfo=UTC),
        date_end=datetime(2025, 10, 8, tzinfo=UTC),
    )
    commands: list[list[str]] = []

    def _fake_run(cmd: list[str], capture_output: bool, text: bool, timeout: int) -> SimpleNamespace:
        del capture_output, text, timeout
        commands.append(cmd)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    videos = scraper._search_via_ytdlp(config)  # noqa: SLF001

    assert videos == []
    assert commands
    assert any(any(arg.startswith("ytsearchdate200:RHOSLC bravo") for arg in cmd) for cmd in commands)
    assert any(any(arg.startswith("ytsearchdate200:bravo") for arg in cmd) for cmd in commands)


def test_youtube_ytdlp_owner_match_requires_exact_channel_id() -> None:
    scraper = YouTubeScraper()
    matching = {"channel_id": "chan-1", "channel": "Bravo"}
    mismatched = {"channel_id": "chan-2", "channel": "Bravo"}

    assert (
        scraper._ytdlp_entry_matches_owner(  # noqa: SLF001
            matching,
            target_handle="bravo",
            target_channel_id="chan-1",
        )
        is True
    )
    assert (
        scraper._ytdlp_entry_matches_owner(  # noqa: SLF001
            mismatched,
            target_handle="bravo",
            target_channel_id="chan-1",
        )
        is False
    )


def test_twitter_extract_media_urls_from_link_preview_card() -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    media_urls, preview_count = scraper._extract_media_urls_from_tweet(  # noqa: SLF001
        tweet_payload={
            "entities": {
                "urls": [{"expanded_url": "https://t.co/x"}],
            }
        },
        result_payload={
            "card": {
                "binding_values": [
                    {"key": "photo_image_full_size_original", "value": {"string_value": "https://pbs.twimg.com/a.jpg"}}
                ]
            }
        },
    )

    assert media_urls == ["https://pbs.twimg.com/a.jpg"]
    assert preview_count == 1


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
        payload={"data": {"threaded_conversation_with_injections_v2": {"instructions": []}}},
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


def test_twitter_reply_fetch_rediscover_hashes_after_404(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    scraper._detail_hash = "oldhash"
    scraper._search_hash = "searchhash"

    requested_urls: list[str] = []
    requested_timeouts: list[object] = []
    discover_calls = 0

    responses = [
        _FakeResponse(status_code=404, payload={"errors": [{"message": "not found"}]}),
        _FakeResponse(
            status_code=200,
            payload={"data": {"threaded_conversation_with_injections_v2": {"instructions": []}}},
        ),
    ]

    def _fake_discover() -> None:
        nonlocal discover_calls
        discover_calls += 1
        if scraper._detail_hash is None:
            scraper._detail_hash = "newhash"

    def _fake_get(url: str, **kwargs: object) -> _FakeResponse:
        requested_urls.append(url)
        requested_timeouts.append(kwargs.get("timeout"))
        return responses.pop(0)

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper, "_discover_graphql_hashes", _fake_discover)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    replies = scraper.fetch_tweet_replies("tweet-1", delay=0)
    assert replies == []
    assert discover_calls >= 1
    assert len(requested_urls) == 2
    assert "/oldhash/TweetDetail" in requested_urls[0]
    assert "/newhash/TweetDetail" in requested_urls[1]
    assert all(timeout == scraper.REQUEST_TIMEOUT_SECONDS for timeout in requested_timeouts)


def test_twitter_reply_fetch_sets_reason_on_logical_api_error(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    scraper._detail_hash = "abc123"
    scraper._search_hash = "def456"

    def _fake_get(url: str, **_: object) -> _FakeResponse:
        del url
        return _FakeResponse(status_code=200, payload={"errors": [{"message": "auth required"}]})

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    replies = scraper.fetch_tweet_replies("tweet-1", delay=0)

    assert replies == []
    assert scraper.last_reply_fetch_reason == "api_errors"


def test_twitter_reply_fetch_falls_back_to_search_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    scraper._detail_hash = "abc123"
    scraper._search_hash = "def456"

    def _fake_get(url: str, **_: object) -> _FakeResponse:
        del url
        return _FakeResponse(status_code=404, payload={"errors": [{"message": "not found"}]})

    fallback_reply = SimpleNamespace(tweet_id="reply-1")
    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)
    monkeypatch.setattr(
        scraper,
        "_fetch_tweet_replies_via_search",
        lambda **_kwargs: [fallback_reply],
    )

    replies = scraper.fetch_tweet_replies("tweet-1", delay=0)

    assert replies == [fallback_reply]
    assert scraper.last_reply_fetch_reason is None


def test_twitter_fetch_public_tweet_summary_includes_reply_count_and_media(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper()

    payload = {
        "id_str": "123",
        "favorite_count": 77,
        "conversation_count": 89,
        "user": {
            "id_str": "u-1",
            "name": "Bravo",
            "screen_name": "BravoTV",
            "profile_image_url_https": "https://pbs.twimg.com/profile_images/bravo.jpg",
        },
        "video": {
            "variants": [
                {"type": "video/mp4", "src": "https://video.twimg.com/vid/480x270/a.mp4"},
                {"type": "video/mp4", "src": "https://video.twimg.com/vid/1280x720/b.mp4"},
            ]
        },
        "mediaDetails": [
            {
                "media_url_https": "https://pbs.twimg.com/media/thumb.jpg",
                "video_info": {
                    "variants": [
                        {"content_type": "video/mp4", "url": "https://video.twimg.com/vid/640x360/c.mp4"},
                    ]
                },
            }
        ],
    }

    def _fake_get(url: str, **_: object) -> _FakeResponse:
        assert "cdn.syndication.twimg.com/tweet-result" in url
        return _FakeResponse(status_code=200, payload=payload)

    monkeypatch.setattr(scraper.session, "get", _fake_get)

    summary = scraper.fetch_public_tweet_summary("123")

    assert summary is not None
    assert summary["tweet_id"] == "123"
    assert summary["username"] == "BravoTV"
    assert summary["display_name"] == "Bravo"
    assert summary["user_profile_url"] == "https://x.com/BravoTV"
    assert summary["user_avatar_url"] == "https://pbs.twimg.com/profile_images/bravo.jpg"
    assert summary["likes"] == 77
    assert summary["replies"] == 89
    assert "https://video.twimg.com/vid/1280x720/b.mp4" in summary["media_urls"]
    assert "https://pbs.twimg.com/media/thumb.jpg" in summary["media_urls"]


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
                        "rest_id": "999",
                        "legacy": {"profile_image_url_https": "https://pbs.twimg.com/profile_images/test.jpg"},
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
    assert parsed.user_id == "999"
    assert parsed.user_profile_url == "https://x.com/tester"
    assert parsed.user_avatar_url == "https://pbs.twimg.com/profile_images/test.jpg"


def test_twitter_parse_tweet_result_detects_legacy_quote_fields() -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})

    parsed = scraper._parse_tweet_result(  # noqa: SLF001
        {
            "__typename": "Tweet",
            "legacy": {
                "id_str": "789",
                "full_text": "legacy quote",
                "favorite_count": 1,
                "retweet_count": 2,
                "reply_count": 3,
                "quote_count": 4,
                "created_at": "Thu Feb 13 12:34:56 +0000 2026",
                "is_quote_status": True,
                "quoted_status_id_str": "root-999",
            },
            "core": {
                "user_results": {
                    "result": {
                        "core": {"screen_name": "tester", "name": "Test User"},
                    }
                }
            },
            "views": {"count": "10"},
        },
        TwitterScrapeConfig(query="x", date_start=datetime(2026, 2, 1), date_end=datetime(2026, 2, 14)),
    )
    assert parsed is not None
    assert parsed.is_quote is True
    assert parsed.quoted_tweet_id == "root-999"


def test_twitter_parse_tweet_result_prefers_mp4_variant_for_video_media() -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})

    parsed = scraper._parse_tweet_result(  # noqa: SLF001
        {
            "__typename": "Tweet",
            "legacy": {
                "id_str": "456",
                "full_text": "video tweet",
                "favorite_count": 1,
                "retweet_count": 2,
                "reply_count": 3,
                "quote_count": 4,
                "created_at": "Thu Feb 13 12:34:56 +0000 2026",
                "extended_entities": {
                    "media": [
                        {
                            "type": "video",
                            "media_url_https": "https://pbs.twimg.com/ext_tw_video_thumb/456/pu/img/thumb.jpg",
                            "video_info": {
                                "variants": [
                                    {
                                        "content_type": "application/x-mpegURL",
                                        "url": "https://video.twimg.com/ext.m3u8",
                                    },
                                    {
                                        "bitrate": 832000,
                                        "content_type": "video/mp4",
                                        "url": "https://video.twimg.com/456-832.mp4",
                                    },
                                    {
                                        "bitrate": 2176000,
                                        "content_type": "video/mp4",
                                        "url": "https://video.twimg.com/456-2176.mp4",
                                    },
                                ]
                            },
                        }
                    ]
                },
            },
            "core": {"user_results": {"result": {"core": {"screen_name": "tester", "name": "Test User"}}}},
            "views": {"count": "10"},
        },
        TwitterScrapeConfig(query="x", date_start=datetime(2026, 2, 1), date_end=datetime(2026, 2, 14)),
    )

    assert parsed is not None
    assert parsed.media_urls == ["https://video.twimg.com/456-2176.mp4"]


def test_twitter_fetch_tweet_quotes_keeps_legacy_quote_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})

    payload = {
        "data": {
            "search_by_raw_query": {
                "search_timeline": {
                    "timeline": {
                        "instructions": [
                            {
                                "type": "TimelineAddEntries",
                                "entries": [
                                    {
                                        "entryId": "tweet-quote-1",
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "Tweet",
                                                        "legacy": {
                                                            "id_str": "quote-1",
                                                            "full_text": "quoted reply",
                                                            "favorite_count": 5,
                                                            "retweet_count": 0,
                                                            "reply_count": 0,
                                                            "quote_count": 0,
                                                            "created_at": "Thu Feb 13 12:34:56 +0000 2026",
                                                            "is_quote_status": True,
                                                            "quoted_status_id_str": "root-123",
                                                        },
                                                        "core": {
                                                            "user_results": {
                                                                "result": {
                                                                    "core": {
                                                                        "screen_name": "viewer",
                                                                        "name": "Viewer",
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        "views": {"count": "3"},
                                                    }
                                                }
                                            }
                                        },
                                    }
                                ],
                            }
                        ]
                    }
                }
            }
        }
    }

    captured_queries: list[str] = []

    def _fake_fetch_search(query: str, cursor: str | None = None, delay: float = 2.0) -> dict:
        del cursor, delay
        captured_queries.append(query)
        return payload

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_quotes_via_tweet_detail", lambda *args, **kwargs: [])
    monkeypatch.setattr(scraper, "_fetch_search", _fake_fetch_search)

    quotes = scraper.fetch_tweet_quotes("root-123", delay=0, max_pages=1)

    assert len(quotes) == 1
    assert captured_queries[0] == "quoted_tweet_id:root-123"
    assert quotes[0].tweet_id == "quote-1"
    assert quotes[0].is_quote is True
    assert quotes[0].quoted_tweet_id == "root-123"


def test_twitter_fetch_tweet_quotes_sets_failure_reason_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    search_calls = 0

    def _fake_fetch_search(query: str, cursor: str | None = None, delay: float = 2.0) -> None:
        del query, cursor, delay
        nonlocal search_calls
        search_calls += 1
        scraper._last_graphql_status_code = 404  # noqa: SLF001
        return None

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_quotes_via_tweet_detail", lambda *args, **kwargs: [])
    monkeypatch.setattr(scraper, "_discover_graphql_hashes", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_search", _fake_fetch_search)

    quotes = scraper.fetch_tweet_quotes("root-123", delay=0, max_pages=1)

    assert quotes == []
    assert search_calls == 2
    assert scraper.last_quote_fetch_reason == "http_404"


def test_twitter_fetch_tweet_quotes_falls_back_to_twikit(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"}, twikit_credentials={"auth_token": "a", "ct0": "b"})
    search_calls = 0

    def _fake_fetch_search(query: str, cursor: str | None = None, delay: float = 2.0) -> None:
        del query, cursor, delay
        nonlocal search_calls
        search_calls += 1
        scraper._last_graphql_status_code = 404  # noqa: SLF001
        return None

    quote = Tweet(
        tweet_id="quote-77",
        date_time="",
        created_at=0,
        text="quote body",
        hashtags=[],
        mentions=[],
        likes=1,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url="https://x.com/viewer/status/quote-77",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=True,
        reply_to_tweet_id=None,
        quoted_tweet_id="root-123",
    )

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_quotes_via_tweet_detail", lambda *args, **kwargs: [])
    monkeypatch.setattr(scraper, "_discover_graphql_hashes", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_search", _fake_fetch_search)
    monkeypatch.setattr(
        scraper,
        "_fetch_tweet_quotes_via_twikit",
        lambda *, tweet_id, max_pages, delay: [quote],  # noqa: ARG005
    )

    quotes = scraper.fetch_tweet_quotes("root-123", delay=0, max_pages=1)

    assert search_calls == 2
    assert len(quotes) == 1
    assert quotes[0].tweet_id == "quote-77"
    assert scraper.last_quote_fetch_reason is None


def test_twitter_fetch_tweet_quotes_prefers_search_before_tweet_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    quote = Tweet(
        tweet_id="quote-search-1",
        date_time="",
        created_at=0,
        text="quote body",
        hashtags=[],
        mentions=[],
        likes=1,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url="https://x.com/viewer/status/quote-search-1",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=True,
        reply_to_tweet_id=None,
        quoted_tweet_id="root-123",
    )

    detail_called = {"value": False}
    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(
        scraper,
        "_fetch_quotes_via_tweet_detail",
        lambda *args, **kwargs: detail_called.__setitem__("value", True) or [],
    )
    monkeypatch.setattr(
        scraper,
        "_fetch_tweet_quotes_via_search",
        lambda **kwargs: [quote],
    )

    quotes = scraper.fetch_tweet_quotes("root-123", delay=0, max_pages=1)

    assert len(quotes) == 1
    assert quotes[0].tweet_id == "quote-search-1"
    assert detail_called["value"] is False


def test_twitter_fetch_tweet_quotes_populates_source_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    quote = Tweet(
        tweet_id="quote-detail-2",
        date_time="",
        created_at=0,
        text="quote body",
        hashtags=[],
        mentions=[],
        likes=1,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url="https://x.com/viewer/status/quote-detail-2",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=True,
        reply_to_tweet_id=None,
        quoted_tweet_id="root-123",
    )

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_tweet_quotes_via_search", lambda **kwargs: [])
    monkeypatch.setattr(scraper, "_fetch_quotes_via_tweet_detail", lambda *args, **kwargs: [quote])

    quotes = scraper.fetch_tweet_quotes("root-123", delay=0, max_pages=1)

    assert len(quotes) == 1
    assert scraper.last_quote_fetch_reason is None
    assert scraper.last_quote_fetch_meta["source_used"] == "tweet_detail"
    assert scraper.last_quote_fetch_meta["failure_reason"] is None
    # search_timeline is attempted first (primary), then tweet_detail (fallback)
    assert scraper.last_quote_fetch_meta["attempts"][0]["source"] == "search_timeline"
    assert scraper.last_quote_fetch_meta["attempts"][0]["count"] == 0
    assert scraper.last_quote_fetch_meta["attempts"][1]["source"] == "tweet_detail"
    assert scraper.last_quote_fetch_meta["attempts"][1]["count"] == 1


def test_twitter_fetch_tweet_quotes_caches_search_404_between_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"}, twikit_credentials={"auth_token": "a", "ct0": "b"})
    search_calls = 0

    quote = Tweet(
        tweet_id="quote-cache-1",
        date_time="",
        created_at=0,
        text="quote body",
        hashtags=[],
        mentions=[],
        likes=1,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url="https://x.com/viewer/status/quote-cache-1",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=True,
        reply_to_tweet_id=None,
        quoted_tweet_id="root-123",
    )

    def _fake_fetch_search(query: str, cursor: str | None = None, delay: float = 2.0) -> None:
        del query, cursor, delay
        nonlocal search_calls
        search_calls += 1
        scraper._last_graphql_status_code = 404  # noqa: SLF001
        return None

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_quotes_via_tweet_detail", lambda *args, **kwargs: [])
    monkeypatch.setattr(scraper, "_discover_graphql_hashes", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_search", _fake_fetch_search)
    monkeypatch.setattr(
        scraper,
        "_fetch_tweet_quotes_via_twikit",
        lambda *, tweet_id, max_pages, delay: [quote],  # noqa: ARG005
    )

    first = scraper.fetch_tweet_quotes("root-123", delay=0, max_pages=1)
    second = scraper.fetch_tweet_quotes("root-123", delay=0, max_pages=1)

    assert len(first) == 1
    assert len(second) == 1
    assert search_calls == 2
    search_attempts = [a for a in scraper.last_quote_fetch_meta["attempts"] if a["source"] == "search_timeline"]
    assert search_attempts
    assert search_attempts[0]["failure_reason"] == "http_404"


def test_twitter_fetch_quotes_via_tweet_detail_parses_tweet_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"})
    scraper._detail_hash = "detail-hash"
    scraper._search_hash = "search-hash"

    payload = {
        "data": {
            "threaded_conversation_with_injections_v2": {
                "instructions": [
                    {
                        "type": "TimelineAddEntries",
                        "entries": [
                            {
                                "entryId": "tweet-root-123",
                                "content": {
                                    "itemContent": {
                                        "tweet_results": {"result": {"legacy": {"id_str": "root-123"}}}
                                    }
                                },
                            },
                            {
                                "entryId": "tweet-quote-123",
                                "content": {
                                    "itemContent": {
                                        "tweet_results": {
                                            "result": {
                                                "__typename": "Tweet",
                                                "legacy": {
                                                    "id_str": "quote-123",
                                                    "full_text": "quoted text",
                                                    "favorite_count": 2,
                                                    "retweet_count": 0,
                                                    "reply_count": 0,
                                                    "quote_count": 0,
                                                    "created_at": "Thu Feb 13 12:34:56 +0000 2026",
                                                    "is_quote_status": True,
                                                    "quoted_status_id_str": "root-123",
                                                },
                                                "core": {
                                                    "user_results": {
                                                        "result": {"core": {"screen_name": "viewer", "name": "Viewer"}}
                                                    }
                                                },
                                                "views": {"count": "3"},
                                            }
                                        }
                                    }
                                },
                            },
                        ],
                    }
                ]
            }
        }
    }

    def _fake_get(url: str, **kwargs: object) -> _FakeResponse:
        del url, kwargs
        return _FakeResponse(status_code=200, payload=payload)

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)

    quotes = scraper._fetch_quotes_via_tweet_detail("root-123", delay=0)  # noqa: SLF001

    assert len(quotes) == 1
    assert quotes[0].tweet_id == "quote-123"
    assert quotes[0].quoted_tweet_id == "root-123"


def test_mirror_tweet_media_populates_hosted_media_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.media import s3_mirror

    tweet = Tweet(
        tweet_id="tweet-1",
        date_time="",
        created_at=0,
        text="body",
        hashtags=[],
        mentions=[],
        likes=0,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url="https://x.com/a/status/tweet-1",
        username="a",
        display_name="A",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=False,
        media_urls=["https://video.twimg.com/vid.mp4"],
    )

    monkeypatch.setattr(s3_mirror, "get_s3_client", lambda: object())
    monkeypatch.setattr(s3_mirror, "get_s3_bucket", lambda: "bucket")

    class _Result:
        def __init__(self, hosted_url: str, status: str):
            self.hosted_url = hosted_url
            self.status = status

    monkeypatch.setattr(
        s3_mirror,
        "mirror_urls_to_s3",
        lambda urls, **kwargs: [_Result("https://cdn.example.com/media/vid.mp4", "mirrored")],  # noqa: ARG005
    )

    mirrored = mirror_tweet_media([tweet])

    assert mirrored == {"tweet-1": ["https://cdn.example.com/media/vid.mp4"]}
    assert tweet.hosted_media_urls == ["https://cdn.example.com/media/vid.mp4"]


def test_twitter_fetch_tweet_replies_falls_back_to_twikit(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "csrf-token"}, twikit_credentials={"auth_token": "a", "ct0": "b"})

    def _raise_request(*_args, **_kwargs):
        response = requests.Response()
        response.status_code = 404
        raise requests.exceptions.HTTPError(response=response)

    reply = Tweet(
        tweet_id="reply-22",
        date_time="",
        created_at=0,
        text="reply body",
        hashtags=[],
        mentions=[],
        likes=0,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url="https://x.com/viewer/status/reply-22",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=True,
        is_retweet=False,
        is_quote=False,
        reply_to_tweet_id="root-123",
        quoted_tweet_id=None,
    )

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_discover_graphql_hashes", lambda: None)
    monkeypatch.setattr(scraper.session, "get", _raise_request)
    monkeypatch.setattr(scraper, "_fetch_tweet_replies_via_search", lambda *, tweet_id, delay, max_pages: [])  # noqa: ARG005
    monkeypatch.setattr(
        scraper,
        "_fetch_tweet_replies_via_twikit",
        lambda *, tweet_id, max_pages, delay: [reply],  # noqa: ARG005
    )

    replies = scraper.fetch_tweet_replies("root-123", delay=0)

    assert len(replies) == 1
    assert replies[0].tweet_id == "reply-22"
    assert scraper.last_reply_fetch_reason is None


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


def test_instagram_parse_post_node_handles_null_carousel_media() -> None:
    scraper = InstagramScraper(cookies={})
    config = InstagramScrapeConfig(username="bravotv")
    node = {
        "id": "180000",
        "shortcode": "DEF456",
        "taken_at_timestamp": 1735689600,
        "edge_media_to_caption": {"edges": [{"node": {"text": "Caption"}}]},
        "edge_liked_by": {"count": 3},
        "edge_media_to_comment": {"count": 1},
        "carousel_media": None,
        "edge_sidecar_to_children": None,
        "display_url": "https://example.com/ig-fallback.jpg",
    }

    parsed = scraper._parse_post_node(node, config)  # noqa: SLF001
    assert parsed.media_urls == ["https://example.com/ig-fallback.jpg"]
    assert parsed.thumbnail_url == "https://example.com/ig-fallback.jpg"


def test_instagram_parse_post_node_supports_actor_style_payload_and_normalizes_metadata() -> None:
    scraper = InstagramScraper(cookies={})
    config = InstagramScrapeConfig(username="fallback-user")
    node = {
        "id": "3706348645359165044",
        "shortCode": "DNvmHS0Yh50",
        "type": "Video",
        "productType": "clips",
        "timestamp": "2025-08-24T16:00:37.000Z",
        "caption": "Stream next day on @DisneyPlus and @hulu.",
        "hashtags": ["TheLastRhinosANewHope"],
        "mentions": ["DisneyPlus", "hulu."],
        "taggedUsers": [
            {"username": "natgeo"},
            {"username": "natgeoanimals"},
        ],
        "coauthorProducers": [
            {"username": "amivitale"},
            {"username": "natgeoanimals"},
        ],
        "likesCount": 44193,
        "commentsCount": 187,
        "videoViewCount": 528654,
        "ownerUsername": "natgeotv",
        "displayUrl": "https://example.com/cover.jpg",
        "videoUrl": "https://example.com/video.mp4",
    }

    parsed = scraper._parse_post_node(node, config)  # noqa: SLF001
    assert parsed.shortcode == "DNvmHS0Yh50"
    assert parsed.post_type == "reel"
    assert parsed.likes == 44193
    assert parsed.comments == 187
    assert parsed.video_views == 528654
    assert parsed.username == "natgeotv"
    assert parsed.media_urls == ["https://example.com/cover.jpg", "https://example.com/video.mp4"]
    assert parsed.thumbnail_url == "https://example.com/cover.jpg"
    assert parsed.hashtags == ["TheLastRhinosANewHope"]
    assert parsed.mentions == ["@DisneyPlus", "@hulu"]
    assert parsed.profile_tags == ["natgeo", "natgeoanimals"]
    assert parsed.collaborators == ["amivitale", "natgeoanimals"]


def test_instagram_parse_comment_supports_actor_style_payload_with_nested_replies() -> None:
    scraper = InstagramScraper(cookies={})
    comment_payload = {
        "id": "17949788698583607",
        "text": "Imagine scrolling to find the end of these comments! 😂",
        "ownerUsername": "gabriel2005120",
        "ownerProfilePicUrl": "https://scontent.example/profile.jpg",
        "timestamp": "2021-11-19T13:54:13.000Z",
        "likesCount": 12,
        "repliesCount": 1,
        "replies": [
            {
                "id": "17891234567890123",
                "text": "@gabriel2005120 right? It never ends 😅",
                "ownerUsername": "maria_adventures",
                "timestamp": "2021-11-19T14:02:45.000Z",
                "likesCount": 3,
                "repliesCount": 0,
                "replies": [],
            }
        ],
    }

    parsed = scraper._parse_comment(  # noqa: SLF001
        comment_payload,
        "DNvmHS0Yh50",
        "https://www.instagram.com/p/DNvmHS0Yh50/",
    )
    assert parsed.comment_id == "17949788698583607"
    assert parsed.username == "gabriel2005120"
    assert parsed.likes == 12
    assert parsed.reply_count == 1
    assert parsed.owner_profile_pic_url == "https://scontent.example/profile.jpg"
    assert len(parsed.replies) == 1
    reply = parsed.replies[0]
    assert reply.is_reply is True
    assert reply.parent_comment_id == parsed.comment_id
    assert reply.comment_id == "17891234567890123"
    assert reply.username == "maria_adventures"
    assert reply.likes == 3


def test_instagram_fetch_comments_paginates_with_headload_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "ok"})
    seen_params: list[dict[str, str]] = []

    responses = [
        _FakeResponse(
            status_code=200,
            payload={
                "comments": [{"pk": "c1", "text": "one", "user": {}, "child_comment_count": 0}],
                "has_more_comments": False,
                "has_more_headload_comments": True,
                "next_min_id": "cursor-1",
            },
            headers={"content-type": "application/json"},
        ),
        _FakeResponse(
            status_code=200,
            payload={
                "comments": [{"pk": "c2", "text": "two", "user": {}, "child_comment_count": 0}],
                "has_more_comments": False,
                "has_more_headload_comments": False,
            },
            headers={"content-type": "application/json"},
        ),
    ]

    def _fake_get(url: str, params: dict | None = None, **_: object) -> _FakeResponse:
        seen_params.append(dict(params or {}))
        return responses.pop(0)

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(scraper, "_get", _fake_get)
    monkeypatch.setattr(
        scraper,
        "_parse_comment",
        lambda data, *args, **kwargs: SimpleNamespace(
            comment_id=str(data.get("pk", "")),
            reply_count=0,
            replies=[],
        ),
    )

    comments = scraper.fetch_comments("ABC123", fetch_replies=False, delay=0)
    assert len(comments) == 2
    assert len(seen_params) == 2
    assert seen_params[1].get("min_id") == "cursor-1"


def test_instagram_fetch_comments_resets_sticky_auth_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "ok"})
    scraper.comments_auth_failed = True

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(
        scraper,
        "_get",
        lambda *_args, **_kwargs: _FakeResponse(
            status_code=200,
            payload={"status": "ok", "comments": [], "has_more_comments": False},
            headers={"content-type": "application/json"},
        ),
    )

    comments = scraper.fetch_comments("ABC123", fetch_replies=False, delay=0)
    assert comments == []
    assert scraper.comments_auth_failed is False


def test_instagram_fetch_comments_sets_invalid_shortcode_reason() -> None:
    scraper = InstagramScraper(cookies={"sessionid": "ok"})

    comments = scraper.fetch_comments("not-valid!!!", fetch_replies=False, delay=0)
    assert comments == []
    assert scraper.last_comment_fetch_reason == "invalid_shortcode"
    assert scraper.comments_auth_failed is False


def test_instagram_fetch_comments_sets_api_status_fail_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "ok"})

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(
        scraper,
        "_get",
        lambda *_args, **_kwargs: _FakeResponse(
            status_code=200,
            payload={"status": "fail", "message": "login required"},
            headers={"content-type": "application/json"},
        ),
    )

    comments = scraper.fetch_comments("ABC123", fetch_replies=False, delay=0)
    assert comments == []
    assert scraper.last_comment_fetch_reason == "api_status_fail"
    assert scraper.comments_auth_failed is True


def test_instagram_fetch_comment_replies_sets_api_status_fail_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "ok"})

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)
    monkeypatch.setattr(
        scraper,
        "_get",
        lambda *_args, **_kwargs: _FakeResponse(
            status_code=200,
            payload={"status": "fail", "message": "challenge required"},
            headers={"content-type": "application/json"},
        ),
    )
    replies = scraper._fetch_comment_replies("123", "comment-1", "ABC123", "https://www.instagram.com/p/ABC123/", 0)  # noqa: SLF001
    assert replies == []
    assert scraper.last_comment_fetch_reason == "api_status_fail"
    assert scraper.comments_auth_failed is True


def test_instagram_fetch_comments_handles_request_error_without_response_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "ok"})

    monkeypatch.setattr(scraper, "_rate_limit", lambda delay: None)

    def _raise_request_error(*_args, **_kwargs):
        raise requests.RequestException("timeout")

    monkeypatch.setattr(scraper, "_get", _raise_request_error)
    comments = scraper.fetch_comments("ABC123", fetch_replies=False, delay=0)
    assert comments == []
    assert scraper.last_comment_fetch_reason == "request_error"


def test_tiktok_parse_post_item_populates_media_urls_and_thumbnail() -> None:
    scraper = TikTokScraper()
    config = TikTokScrapeConfig(username="bravotv")
    item = {
        "id": "12345",
        "createTime": 1735689600,
        "desc": "#RHOSLC sneak peek",
        "author": {"uniqueId": "bravotv", "nickname": "Bravo TV"},
        "stats": {"diggCount": 1, "commentCount": 2, "shareCount": 3, "playCount": 4, "collectCount": 5},
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
    assert parsed.thumbnail_url == "https://example.com/cover.jpg"
    assert parsed.saves == 5


def test_tiktok_parse_post_item_supports_actor_style_payload() -> None:
    scraper = TikTokScraper()
    config = TikTokScrapeConfig(username="bravotv")
    item = {
        "id": "7553309587378539831",
        "text": "Unexpected prank#funny#prank#fyp @hulu.",
        "createTimeISO": "2025-09-23T15:39:30.000Z",
        "authorMeta": {"name": "chunniuc", "nickName": "chunniuc"},
        "musicMeta": {"musicName": "original sound", "musicAuthor": "chunniuc"},
        "webVideoUrl": "https://www.tiktok.com/@chunniuc/video/7553309587378539831",
        "mediaUrls": ["https://cdn.example.com/video.mp4"],
        "videoMeta": {
            "duration": 70,
            "coverUrl": "https://cdn.example.com/cover.jpg",
        },
        "diggCount": 1500000,
        "commentCount": 17300,
        "shareCount": 94900,
        "playCount": 147200000,
        "statsV2": {"collectCount": "471"},
        "hashtags": [{"name": "funny"}, {"name": "fyp"}],
        "mentions": ["@hulu.", {"name": "DisneyPlus"}],
        "detailedMentions": [{"uniqueId": "hulu."}],
    }

    parsed = scraper._parse_post_item(item, config)  # noqa: SLF001

    assert parsed.video_id == "7553309587378539831"
    assert parsed.create_time == int(datetime(2025, 9, 23, 15, 39, 30, tzinfo=UTC).timestamp())
    assert parsed.username == "chunniuc"
    assert parsed.author_nickname == "chunniuc"
    assert parsed.likes == 1500000
    assert parsed.comments == 17300
    assert parsed.shares == 94900
    assert parsed.saves == 471
    assert parsed.views == 147200000
    assert parsed.url == "https://www.tiktok.com/@chunniuc/video/7553309587378539831"
    assert parsed.media_urls == ["https://cdn.example.com/video.mp4"]
    assert parsed.thumbnail_url == "https://cdn.example.com/cover.jpg"
    assert parsed.hashtags == ["funny", "fyp", "prank"]
    assert parsed.mentions == ["hulu", "DisneyPlus"]


def test_tiktok_parse_comment_supports_actor_style_payload_with_nested_replies() -> None:
    scraper = TikTokScraper()
    data = {
        "text": (
            "Bella poarch -67.3M "
            "https://p16-comment-sign-useast8.tiktokcdn-us.com/tos-useast8-i-bwwwqb46tv-tx2/e891a80352d444a3bcc9d34957e5373c~tplv-jj85edgx6n-image-origin.jpeg"
        ),
        "diggCount": 246,
        "replyCommentTotal": 3,
        "createTimeISO": "2024-08-06T11:21:16.000Z",
        "uniqueId": "rizqirxq",
        "uid": "6904063862041396225",
        "cid": "7399984975553086214",
        "parentId": "7399984975553086000",
        "awemeId": "6862153058223197445",
        "commentLanguage": "es",
        "isAuthorLiked": False,
        "avatarThumbnail": "https://cdn.example.com/avatar.jpg",
        "videoWebUrl": "https://www.tiktok.com/@bellapoarch/video/6862153058223197445",
        "user": {
            "id": "7175984693090419717",
            "username": "21billie_eilish",
            "url": "https://www.tiktok.com/@21billie_eilish",
            "bio": "Billiestan for ever",
            "avatarUrl": "https://p16-sign-va.tiktokcdn.com/tos-maliva-avt-0068/example.webp",
            "region": "CO",
            "language": "es",
        },
        "replies": [
            {
                "id": "7399984975553086215",
                "text": "reply",
                "diggCount": 3,
                "createTimeISO": "2024-08-06T11:25:00.000Z",
                "uniqueId": "reply_user",
                "uid": "6904063862041396226",
                "replies": [],
            }
        ],
    }

    parsed = scraper._parse_comment(data, video_id="", post_url="")  # noqa: SLF001

    assert parsed.comment_id == "7399984975553086214"
    assert parsed.username == "rizqirxq"
    assert parsed.user_id == "6904063862041396225"
    assert parsed.likes == 246
    assert parsed.reply_count == 3
    assert parsed.video_id == "6862153058223197445"
    assert parsed.post_url == "https://www.tiktok.com/@bellapoarch/video/6862153058223197445"
    assert parsed.avatar_thumbnail_url == "https://cdn.example.com/avatar.jpg"
    assert parsed.comment_language == "es"
    assert parsed.is_author_liked is False
    assert parsed.aweme_id == "6862153058223197445"
    assert parsed.parent_source_comment_id == "7399984975553086000"
    assert parsed.user_url == "https://www.tiktok.com/@21billie_eilish"
    assert parsed.user_bio == "Billiestan for ever"
    assert parsed.user_avatar_url == "https://p16-sign-va.tiktokcdn.com/tos-maliva-avt-0068/example.webp"
    assert parsed.user_region == "CO"
    assert parsed.user_language == "es"
    assert parsed.media_urls == [
        "https://p16-comment-sign-useast8.tiktokcdn-us.com/tos-useast8-i-bwwwqb46tv-tx2/e891a80352d444a3bcc9d34957e5373c~tplv-jj85edgx6n-image-origin.jpeg"
    ]
    assert parsed.created_at == int(datetime(2024, 8, 6, 11, 21, 16, tzinfo=UTC).timestamp())
    assert len(parsed.replies) == 1
    assert parsed.replies[0].is_reply is True
    assert parsed.replies[0].parent_comment_id == "7399984975553086214"
    assert parsed.replies[0].comment_id == "7399984975553086215"


def test_tiktok_parse_ytdlp_metadata_populates_thumbnail_from_metadata() -> None:
    scraper = TikTokScraper()
    config = TikTokScrapeConfig(username="bravotv")
    metadata = {
        "id": "9999",
        "title": "Clip",
        "timestamp": 1735689600,
        "uploader": "bravotv",
        "url": "https://example.com/video.mp4",
        "collect_count": 12,
        "thumbnail": "https://example.com/thumb-main.jpg",
        "thumbnails": [{"url": "https://example.com/thumb-alt.jpg"}],
    }

    parsed = scraper._parse_ytdlp_metadata(metadata, config)  # noqa: SLF001
    assert parsed.media_urls[0] == "https://example.com/video.mp4"
    assert parsed.thumbnail_url == "https://example.com/thumb-main.jpg"
    assert parsed.saves == 12


def test_instagram_scrape_emits_progress_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={})
    config = InstagramScrapeConfig(username="bravotv")
    events: list[dict[str, int | str]] = []

    monkeypatch.setattr(
        scraper,
        "fetch_profile_info",
        lambda username, delay=0: {
            "data": {
                "user": {
                    "edge_owner_to_timeline_media": {
                        "edges": [{"node": {"shortcode": "abc", "taken_at_timestamp": 1735689600}}],
                        "page_info": {"has_next_page": False},
                    }
                }
            }
        },
    )
    monkeypatch.setattr(
        scraper,
        "_parse_post_node",
        lambda node, cfg: SimpleNamespace(shortcode="abc", date_time="2026-01-01"),
    )

    scraper.scrape(config, progress_cb=lambda payload: events.append(payload))

    assert events
    assert events[-1]["phase"] == "scrape_profile_page"
    assert int(events[-1]["pages_scanned"]) == 1


def test_instagram_graphql_stops_after_consecutive_no_match_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "cookie"})
    config = InstagramScrapeConfig(
        username="bravotv",
        hashtags=["rhoslc"],
        date_start=datetime(2024, 1, 1),
        date_end=datetime(2026, 1, 1),
        no_match_page_limit=2,
    )
    call_cursors: list[str | None] = []

    page_payloads = [
        {
            "data": {
                "xdt_api__v1__feed__user_timeline_graphql_connection": {
                    "edges": [
                        {
                            "node": {
                                "shortcode": "A1",
                                "taken_at_timestamp": 1735689600,
                                "edge_media_to_caption": {"edges": [{"node": {"text": "generic post"}}]},
                            }
                        }
                    ],
                    "page_info": {"has_next_page": True, "end_cursor": "cursor-1"},
                }
            }
        },
        {
            "data": {
                "xdt_api__v1__feed__user_timeline_graphql_connection": {
                    "edges": [
                        {
                            "node": {
                                "shortcode": "A2",
                                "taken_at_timestamp": 1735603200,
                                "edge_media_to_caption": {"edges": [{"node": {"text": "another generic post"}}]},
                            }
                        }
                    ],
                    "page_info": {"has_next_page": True, "end_cursor": "cursor-2"},
                }
            }
        },
        {
            "data": {
                "xdt_api__v1__feed__user_timeline_graphql_connection": {
                    "edges": [
                        {
                            "node": {
                                "shortcode": "A3",
                                "taken_at_timestamp": 1735516800,
                                "edge_media_to_caption": {"edges": [{"node": {"text": "would have matched later"}}]},
                            }
                        }
                    ],
                    "page_info": {"has_next_page": False, "end_cursor": None},
                }
            }
        },
    ]

    def _fake_fetch_posts_graphql(username: str, cursor: str | None = None, delay: float = 2.0) -> dict:
        del username, delay
        call_cursors.append(cursor)
        return page_payloads[len(call_cursors) - 1]

    monkeypatch.setattr(scraper, "fetch_posts_graphql", _fake_fetch_posts_graphql)

    posts = scraper._scrape_graphql(config)  # noqa: SLF001

    assert posts == []
    assert call_cursors == [None, "cursor-1"]
    assert scraper.last_retrieval_meta.get("stop_reason") == "no_match_page_limit_reached"
    assert scraper.last_retrieval_meta.get("no_match_pages") == 2
    assert scraper.last_retrieval_meta.get("pages_scanned") == 2


def test_tiktok_scrape_emits_progress_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TikTokScraper(cookies={})
    config = TikTokScrapeConfig(
        username="bravotv",
        date_start=datetime(2025, 1, 1),
        date_end=datetime(2027, 1, 1),
    )
    events: list[dict[str, int | str]] = []

    monkeypatch.setattr(
        scraper,
        "fetch_user_detail",
        lambda username, delay=0: {"userInfo": {"user": {"secUid": "sec-1", "nickname": "Bravo"}}},
    )
    monkeypatch.setattr(
        scraper,
        "fetch_posts",
        lambda username, sec_uid, cursor=0, delay=0: {
            "itemList": [{"id": "vid-1", "createTime": 1735689600, "desc": "clip"}],
            "hasMore": False,
            "cursor": 0,
        },
    )
    monkeypatch.setattr(
        scraper,
        "_parse_post_item",
        lambda item, cfg: SimpleNamespace(video_id="vid-1", date_time="2026-01-01", views=100),
    )
    monkeypatch.setattr(scraper, "_has_ytdlp", lambda: False)

    scraper.scrape(config, progress_cb=lambda payload: events.append(payload))

    assert any(event.get("phase") == "scrape_api_page" for event in events)
    assert any(int(event.get("pages_scanned") or 0) >= 1 for event in events)


def test_tiktok_scrape_skips_items_without_valid_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TikTokScraper(cookies={})
    config = TikTokScrapeConfig(
        username="bravotv",
        date_start=datetime(2025, 1, 1, tzinfo=UTC),
        date_end=datetime(2027, 1, 1, tzinfo=UTC),
    )
    parsed_ids: list[str] = []

    monkeypatch.setattr(
        scraper,
        "fetch_user_detail",
        lambda username, delay=0: {"userInfo": {"user": {"secUid": "sec-1", "nickname": "Bravo"}}},
    )
    monkeypatch.setattr(
        scraper,
        "fetch_posts",
        lambda username, sec_uid, cursor=0, delay=0: {
            "itemList": [
                {
                    "url": "https://www.tiktok.com/@shaiie_foeva/video/7533731959172861206",
                    "error": "Post not found or private.",
                },
                {
                    "id": "vid-2",
                    "createTime": 1735689600,
                    "desc": "clip",
                },
            ],
            "hasMore": False,
            "cursor": 0,
        },
    )

    def _fake_parse(item: dict, cfg: TikTokScrapeConfig) -> SimpleNamespace:
        del cfg
        parsed_ids.append(str(item.get("id") or ""))
        return SimpleNamespace(video_id=str(item.get("id") or ""), date_time="2026-01-01", views=100)

    monkeypatch.setattr(scraper, "_parse_post_item", _fake_parse)
    monkeypatch.setattr(scraper, "_has_ytdlp", lambda: False)

    posts = scraper.scrape(config)

    assert parsed_ids == ["vid-2"]
    assert [post.video_id for post in posts] == ["vid-2"]


def test_twitter_scrape_emits_progress_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TwitterScraper(cookies={"ct0": "token"})
    events: list[dict[str, int | str]] = []

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(
        scraper,
        "_fetch_search",
        lambda query, cursor=None, delay=0: {
            "data": {
                "search_by_raw_query": {
                    "search_timeline": {
                        "timeline": {
                            "instructions": [
                                {
                                    "type": "TimelineAddEntries",
                                    "entries": [
                                        {
                                            "entryId": "tweet-1",
                                            "content": {
                                                "itemContent": {
                                                    "tweet_results": {
                                                        "result": {
                                                            "legacy": {"id_str": "1", "text": "hello"},
                                                            "core": {
                                                                "user_results": {
                                                                    "result": {"legacy": {"screen_name": "bravo"}}
                                                                }
                                                            },
                                                        }
                                                    }
                                                }
                                            },
                                        }
                                    ],
                                }
                            ]
                        }
                    }
                }
            }
        },
    )
    monkeypatch.setattr(
        scraper,
        "_parse_tweet_result",
        lambda result, cfg: SimpleNamespace(
            tweet_id="1",
            username="bravo",
            date_time="2026-01-01",
            likes=1,
            retweets=0,
        ),
    )

    scraper.scrape(
        TwitterScrapeConfig(query="from:bravotv", date_start=datetime(2026, 1, 1), date_end=datetime(2026, 1, 2)),
        progress_cb=lambda payload: events.append(payload),
    )

    assert any(event.get("phase") == "scrape_graphql_page" for event in events)


def test_youtube_scrape_emits_progress_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = YouTubeScraper()
    events: list[dict[str, int | str]] = []

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda handle, delay=0, surface="videos": {"contents": {}})
    monkeypatch.setattr(scraper, "_process_video_data", lambda data, cfg, **kwargs: [])
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda data: None)
    monkeypatch.setattr(scraper, "_search_via_ytdlp", lambda cfg, **kwargs: [])

    scraper.scrape(
        YouTubeScrapeConfig(channel_handle="bravo", keywords=["rhoslc"]),
        progress_cb=lambda payload: events.append(payload),
    )

    assert any(event.get("phase") == "scrape_initial_page" for event in events)


def test_youtube_scrape_keeps_paging_through_too_recent_no_hit_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(
        channel_handle="bravo",
        date_start=datetime(2025, 10, 1, tzinfo=UTC),
        date_end=datetime(2025, 10, 7, tzinfo=UTC),
    )
    continuation_tokens = ["token-1", "token-2", "token-3", "token-4", "token-5", None]
    page_idx = 0

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda handle, delay=0, surface="videos": {"contents": {}} if surface == "videos" else None)
    monkeypatch.setattr(scraper, "_process_video_data", lambda data, cfg, **kwargs: [])
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda data: "token-0")
    monkeypatch.setattr(scraper, "_fetch_continuation", lambda token, delay=0: {"ok": True})

    def _fake_extract(_data: dict) -> tuple[list[dict], str | None]:
        nonlocal page_idx
        current = page_idx
        page_idx += 1
        return ([{"page_index": current}], continuation_tokens[current])

    def _fake_parse(renderer: dict, cfg: YouTubeScrapeConfig, **_kwargs) -> SimpleNamespace:
        del cfg
        idx = int(renderer.get("page_index") or 0)
        if idx < 5:
            ts = int(datetime(2025, 12, 1, tzinfo=UTC).timestamp())
            return SimpleNamespace(
                video_id=f"vid-too-recent-{idx}",
                title="Generic Bravo post",
                description="",
                published_at=ts,
                date_time="2025-12-01 00:00:00",
                published_text="",
            )
        ts = int(datetime(2025, 10, 2, tzinfo=UTC).timestamp())
        return SimpleNamespace(
            video_id="vid-in-range",
            title="Week 3 clip",
            description="",
            published_at=ts,
            date_time="2025-10-02 00:00:00",
            published_text="",
        )

    monkeypatch.setattr(scraper, "_extract_continuation_videos_and_token", _fake_extract)
    monkeypatch.setattr(scraper, "_parse_video_renderer", _fake_parse)

    videos = scraper.scrape(config)

    assert [video.video_id for video in videos] == ["vid-in-range"]
    assert int(scraper.last_retrieval_meta.get("continuation_pages") or 0) == 6
    assert int(scraper.last_retrieval_meta.get("pre_window_pages") or 0) == 5


def test_youtube_scrape_stops_when_pre_window_cap_is_reached(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = YouTubeScraper()
    scraper.PRE_WINDOW_PAGE_CAP = 3
    config = YouTubeScrapeConfig(
        channel_handle="bravo",
        date_start=datetime(2025, 10, 1, tzinfo=UTC),
        date_end=datetime(2025, 10, 7, tzinfo=UTC),
    )
    continuation_tokens = ["token-1", "token-2", "token-3", "token-4", None]
    page_idx = 0

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda handle, delay=0, surface="videos": {"contents": {}} if surface == "videos" else None)
    monkeypatch.setattr(scraper, "_process_video_data", lambda data, cfg, **kwargs: [])
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda data: "token-0")
    monkeypatch.setattr(scraper, "_fetch_continuation", lambda token, delay=0: {"ok": True})

    def _fake_extract(_data: dict) -> tuple[list[dict], str | None]:
        nonlocal page_idx
        current = page_idx
        page_idx += 1
        return ([{"page_index": current}], continuation_tokens[current])

    def _fake_parse(renderer: dict, cfg: YouTubeScrapeConfig, **_kwargs) -> SimpleNamespace:
        del cfg
        ts = int(datetime(2025, 12, 1, tzinfo=UTC).timestamp())
        return SimpleNamespace(
            video_id=f"vid-too-recent-{renderer.get('page_index')}",
            title="Generic Bravo post",
            description="",
            published_at=ts,
            date_time="2025-12-01 00:00:00",
            published_text="",
        )

    monkeypatch.setattr(scraper, "_extract_continuation_videos_and_token", _fake_extract)
    monkeypatch.setattr(scraper, "_parse_video_renderer", _fake_parse)

    videos = scraper.scrape(config)

    assert videos == []
    assert int(scraper.last_retrieval_meta.get("pre_window_pages") or 0) == 3
    assert scraper.last_retrieval_meta.get("scan_capped_reason") == "pre_window_cap"
