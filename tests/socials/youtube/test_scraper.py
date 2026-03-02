"""Unit tests for YouTube scraper traversal and comment parsing behavior."""

from datetime import UTC, datetime

from trr_backend.socials.youtube.scraper import YouTubeComment, YouTubeScrapeConfig, YouTubeScraper, YouTubeVideo


def _build_video(video_id: str, *, surface: str, published_at: int) -> YouTubeVideo:
    is_short = surface == "shorts"
    return YouTubeVideo(
        video_id=video_id,
        title=f"title-{video_id}",
        description="desc",
        date_time=datetime.fromtimestamp(published_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
        published_at=published_at,
        channel_id="channel-1",
        channel_title="Bravo",
        duration="PT1M0S",
        duration_seconds=60,
        views=1,
        likes=1,
        comments=1,
        url=(f"https://www.youtube.com/shorts/{video_id}" if is_short else f"https://www.youtube.com/watch?v={video_id}"),
        thumbnail_url="https://img.test/thumb.jpg",
        tags=[],
        keywords_matched=[],
        is_short=is_short,
        source_surface=surface,
    )


def test_apply_surface_guaranteed_limit_overrides_small_cap_when_both_surfaces_present() -> None:
    scraper = YouTubeScraper()
    videos = [
        _build_video("video-1", surface="videos", published_at=1_000),
        _build_video("short-1", surface="shorts", published_at=900),
        _build_video("video-2", surface="videos", published_at=800),
    ]

    limited, override_applied, effective_limit = scraper._apply_surface_guaranteed_limit(videos, max_results=1)

    assert override_applied is True
    assert effective_limit == 2
    assert len(limited) == 2
    assert {"videos", "shorts"} <= {scraper._video_surface(video) for video in limited}


def test_scrape_progress_reports_non_zero_shorts_initial_pages(monkeypatch) -> None:
    scraper = YouTubeScraper()
    progress_events: list[dict[str, int | str]] = []

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "chan"))
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: None)

    def _fake_process_video_data(*_args, **kwargs):
        if kwargs.get("return_stats"):
            return [], {
                "checked_renderers": 0,
                "before_window_items": 0,
                "after_window_items": 0,
                "window_candidate_items": 0,
                "timestamp_unknown": 0,
                "in_range_hits": 0,
            }
        return []

    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)

    config = YouTubeScrapeConfig(channel_handle="bravo", keywords=[], max_results=None)
    scraper.scrape(config, progress_cb=lambda payload: progress_events.append(dict(payload)))

    shorts_initial = next(event for event in progress_events if event.get("phase") == "scrape_initial_page_shorts")
    assert int(shorts_initial.get("pages_scanned") or 0) > 0


def test_fetch_comments_falls_back_to_shorts_bootstrap_when_watch_has_no_token(monkeypatch) -> None:
    scraper = YouTubeScraper()
    called_urls: list[str] = []

    class _FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    def _fake_get(url: str, **_kwargs):
        called_urls.append(url)
        return _FakeResponse(url)

    monkeypatch.setattr(scraper, "_rate_limit", lambda _delay: None)
    monkeypatch.setattr(scraper.session, "get", _fake_get)
    monkeypatch.setattr(
        scraper,
        "_extract_ytinital_data",
        lambda html: {"page": "watch"} if "watch?v=" in html else {"page": "shorts"},
    )
    monkeypatch.setattr(
        scraper,
        "_extract_comment_continuation",
        lambda data: None if data.get("page") == "watch" else "token-shorts",
    )
    monkeypatch.setattr(
        scraper,
        "_fetch_comment_continuation",
        lambda *_args, **_kwargs: {
            "onResponseReceivedActions": [
                {
                    "reloadContinuationItemsCommand": {
                        "continuationItems": [
                            {
                                "commentThreadRenderer": {
                                    "comment": {
                                        "commentRenderer": {
                                            "commentId": "c1",
                                            "contentText": {"runs": [{"text": "hello"}]},
                                            "authorText": {"simpleText": "Viewer"},
                                            "authorEndpoint": {"browseEndpoint": {"browseId": "UCX"}},
                                            "voteCount": {"simpleText": "1"},
                                            "replyCount": 0,
                                            "publishedTimeText": {"runs": [{"text": "1 day ago"}]},
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        },
    )

    comments = scraper.fetch_comments("abc123", fetch_replies=False, delay=0.0)
    assert len(comments) == 1
    assert isinstance(comments[0], YouTubeComment)
    assert comments[0].video_url == "https://www.youtube.com/shorts/abc123"
    assert called_urls == [
        "https://www.youtube.com/watch?v=abc123",
        "https://www.youtube.com/shorts/abc123",
    ]


def test_parse_comment_response_reads_on_response_received_actions() -> None:
    scraper = YouTubeScraper()
    items, next_continuation = scraper._parse_comment_response(
        {
            "onResponseReceivedActions": [
                {
                    "appendContinuationItemsAction": {
                        "continuationItems": [
                            {"commentThreadRenderer": {"comment": {}}},
                            {
                                "continuationItemRenderer": {
                                    "continuationEndpoint": {"continuationCommand": {"token": "next-token"}}
                                }
                            },
                        ]
                    }
                }
            ]
        }
    )

    assert len(items) == 1
    assert next_continuation == "next-token"


def test_fetch_comment_replies_parses_nested_comment_view_model(monkeypatch) -> None:
    scraper = YouTubeScraper()
    monkeypatch.setattr(scraper, "_rate_limit", lambda _delay: None)
    monkeypatch.setattr(
        scraper,
        "_fetch_comment_continuation",
        lambda *_args, **_kwargs: {
            "frameworkUpdates": {
                "entityBatchUpdate": {
                    "mutations": [
                        {
                            "payload": {
                                "commentEntityPayload": {
                                    "properties": {
                                        "commentId": "reply-1",
                                        "content": {"content": "reply text"},
                                        "publishedTime": "1 day ago",
                                    },
                                    "author": {"displayName": "Viewer", "channelId": "UCV"},
                                    "toolbar": {"likeCountNotliked": "5", "replyCount": "0"},
                                }
                            }
                        }
                    ]
                }
            },
            "onResponseReceivedActions": [
                {
                    "appendContinuationItemsAction": {
                        "continuationItems": [
                            {"commentViewModel": {"commentViewModel": {"commentId": "reply-1"}}}
                        ]
                    }
                }
            ],
        },
    )

    replies = scraper._fetch_comment_replies(
        "reply-token",
        "abc123",
        "https://www.youtube.com/watch?v=abc123",
        "parent-1",
        delay=0.0,
    )

    assert len(replies) == 1
    assert replies[0].comment_id == "reply-1"
    assert replies[0].is_reply is True
    assert replies[0].parent_comment_id == "parent-1"
