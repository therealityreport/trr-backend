"""Unit tests for YouTube scraper traversal and comment parsing behavior."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

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
        url=(
            f"https://www.youtube.com/shorts/{video_id}" if is_short else f"https://www.youtube.com/watch?v={video_id}"
        ),
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


def test_scrape_playlist_uses_ytdlp_entries_for_videos_and_shorts(monkeypatch) -> None:
    scraper = YouTubeScraper()
    playlist_id = "PLCsQHuMS6NoyF7gnzgOo7UxUrCcMtMH1v"
    progress_events: list[dict[str, int | str]] = []
    payload = {
        "id": playlist_id,
        "title": "The Traitors",
        "playlist_count": 2,
        "entries": [
            {
                "id": "video12345",
                "title": "The Traitors season 3 trailer",
                "description": "Peacock preview",
                "timestamp": int(datetime(2025, 1, 1, tzinfo=UTC).timestamp()),
                "duration": 180,
                "webpage_url": "https://www.youtube.com/watch?v=video12345",
                "view_count": 100,
                "like_count": 10,
                "comment_count": 5,
                "channel_id": "channel-peacock",
                "channel": "Peacock",
                "thumbnail": "https://img.test/video.jpg",
            },
            {
                "id": "short12345",
                "title": "Roundtable reveal",
                "description": "A short clip",
                "timestamp": int(datetime(2025, 1, 2, tzinfo=UTC).timestamp()),
                "duration": 42,
                "webpage_url": "https://www.youtube.com/shorts/short12345",
                "view_count": 200,
                "like_count": 20,
                "comment_count": 7,
                "channel_id": "channel-peacock",
                "channel": "Peacock",
                "thumbnail": "https://img.test/short.jpg",
            },
        ],
    }

    monkeypatch.setattr("trr_backend.socials.youtube.scraper.shutil.which", lambda _binary: "/usr/bin/yt-dlp")
    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "trr_backend.socials.youtube.scraper.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr=""),
    )

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            source_type="playlist",
            playlist_url=f"https://www.youtube.com/playlist?list={playlist_id}",
            enforce_keyword_filter=False,
        ),
        progress_cb=lambda event: progress_events.append(dict(event)),
    )

    assert [video.video_id for video in videos] == ["short12345", "video12345"]
    assert {video.source_surface for video in videos} == {"videos", "shorts"}
    assert scraper.last_retrieval_meta["retrieval_mode"] == "playlist_ytdlp"
    assert scraper.last_retrieval_meta["playlist_id"] == playlist_id
    assert scraper.last_retrieval_meta["posts_checked"] == 2
    assert progress_events[-1]["phase"] == "scrape_complete"


def test_extract_channel_header_avatar_from_data_prefers_yt3_and_upscales() -> None:
    scraper = YouTubeScraper()
    data = {
        "header": {
            "pageHeaderViewModel": {
                "heroImage": {
                    "sources": [
                        {"url": "https://images.test/not-yt3.jpg"},
                        {"url": "https://yt3.googleusercontent.com/abc=s160-c-k-c0x00ffffff-no-rj"},
                    ]
                }
            }
        }
    }

    avatar = scraper._extract_channel_header_avatar_from_data(data)  # noqa: SLF001
    assert avatar == "https://yt3.googleusercontent.com/abc=s1024-c-k-c0x00ffffff-no-rj"


def test_resolve_channel_about_snapshot_extracts_live_counts() -> None:
    scraper = YouTubeScraper()
    data = {
        "metadata": {
            "channelMetadataRenderer": {
                "title": "Bravo",
                "externalId": "channel-bravo",
                "vanityChannelUrl": "https://www.youtube.com/@bravo",
            }
        },
        "header": {
            "pageHeaderViewModel": {
                "heroImage": {
                    "sources": [
                        {"url": "https://yt3.googleusercontent.com/bravo=s160-c-k"},
                    ]
                }
            }
        },
        "onResponseReceivedEndpoints": [
            {
                "showEngagementPanelEndpoint": {
                    "engagementPanel": {
                        "engagementPanelSectionListRenderer": {
                            "content": {
                                "sectionListRenderer": {
                                    "contents": [
                                        {
                                            "itemSectionRenderer": {
                                                "contents": [
                                                    {
                                                        "aboutChannelRenderer": {
                                                            "metadata": {
                                                                "aboutChannelViewModel": {
                                                                    "title": "Bravo",
                                                                    "description": "Official Bravo channel",
                                                                    "subscriberCountText": "3.43M subscribers",
                                                                    "videoCountText": "12,562 videos",
                                                                    "viewCountText": "2,590,941,563 views",
                                                                    "canonicalChannelUrl": "https://www.youtube.com/@bravo",
                                                                }
                                                            }
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        ],
    }

    snapshot = scraper._extract_channel_about_snapshot_from_data(data, "bravo")  # noqa: SLF001

    assert snapshot == {
        "username": "bravo",
        "display_name": "Bravo",
        "bio": "Official Bravo channel",
        "avatar_url": "https://yt3.googleusercontent.com/bravo=s1024-c-k",
        "profile_url": "https://www.youtube.com/@bravo",
        "follower_count": 3430000,
        "total_posts": 12562,
        "channel_id": "channel-bravo",
    }


def test_parse_video_renderer_uses_header_avatar_fallback_when_renderer_avatar_missing() -> None:
    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(channel_handle="bravo", keywords=[])
    renderer = {
        "videoId": "abc1234",
        "title": {"runs": [{"text": "Bravo clip"}]},
        "descriptionSnippet": {"runs": [{"text": "episode"}]},
        "viewCountText": {"simpleText": "1,234 views"},
        "publishedTimeText": {"simpleText": "1 day ago"},
        "ownerText": {"runs": [{"text": "Bravo"}]},
        "thumbnail": {"thumbnails": [{"url": "https://img.test/thumb.jpg"}]},
    }

    parsed = scraper._parse_video_renderer(  # noqa: SLF001
        renderer,
        config,
        fallback_channel_avatar_url="https://yt3.googleusercontent.com/avatar=s160-c-k",
    )

    assert parsed is not None
    assert parsed.user_avatar_url == "https://yt3.googleusercontent.com/avatar=s1024-c-k"


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


def test_scrape_marks_continuation_fetch_failure_as_retryable(monkeypatch) -> None:
    scraper = YouTubeScraper()

    def _fake_fetch_channel_videos(handle: str, delay: float, *, surface: str, fast_mode: bool = False):
        del handle, delay, fast_mode
        return {"surface": surface}

    def _fake_process_video_data(*_args, **kwargs):
        surface = kwargs.get("surface")
        if surface == "videos":
            return [
                _build_video(
                    "video-1", surface="videos", published_at=int(datetime(2025, 8, 14, tzinfo=UTC).timestamp())
                )
            ], {
                "checked_renderers": 1,
                "before_window_items": 0,
                "after_window_items": 0,
                "window_candidate_items": 1,
                "timestamp_unknown": 0,
                "in_range_hits": 1,
            }
        return [], {
            "checked_renderers": 0,
            "before_window_items": 0,
            "after_window_items": 0,
            "window_candidate_items": 0,
            "timestamp_unknown": 0,
            "in_range_hits": 0,
        }

    monkeypatch.setattr(scraper, "fetch_channel_videos", _fake_fetch_channel_videos)
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "chan"))
    monkeypatch.setattr(scraper, "_extract_channel_title_from_data", lambda *_args, **_kwargs: "Bravo")
    monkeypatch.setattr(scraper, "_extract_channel_avatar_from_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scraper,
        "_extract_channel_continuation_token",
        lambda data: "continuation-token" if data.get("surface") == "videos" else None,
    )
    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)

    def _fake_fetch_continuation(*_args, **_kwargs):
        scraper._last_channel_continuation_error = "request_error"  # noqa: SLF001
        return None

    monkeypatch.setattr(scraper, "_fetch_continuation", _fake_fetch_continuation)

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=[],
            date_start=datetime(2025, 8, 1, tzinfo=UTC),
            date_end=datetime(2025, 8, 31, tzinfo=UTC),
            delay_seconds=0,
            max_results=None,
        )
    )

    assert len(videos) == 1
    assert scraper.last_retrieval_meta["error_code"] == "youtube_continuation_fetch_failed"
    assert scraper.last_retrieval_meta["retryable"] is True
    assert scraper.last_retrieval_meta["continuation_failure_reason"] == "request_error"
    assert scraper.last_retrieval_meta["continuation_failure_count"] == 1
    assert scraper.last_retrieval_meta["posts_checked"] == 1


def test_youtube_runtime_metadata_reports_hybrid_source_mode_and_counts() -> None:
    scraper = YouTubeScraper()
    scraper._request_count = 3  # noqa: SLF001
    scraper._last_transport = "channel_page_json"  # noqa: SLF001
    scraper._fallback_chain = ["channel_page_json", "yt_dlp_enrichment"]  # noqa: SLF001
    scraper._last_stop_reason = "complete"  # noqa: SLF001
    scraper._last_retryable = False  # noqa: SLF001
    scraper._last_complete = True  # noqa: SLF001
    scraper._last_source_mode = "hybrid"  # noqa: SLF001

    assert scraper.runtime_metadata["request_count"] == 3
    assert scraper.runtime_metadata["transport"] == "channel_page_json"
    assert scraper.runtime_metadata["fallback_chain"] == ["channel_page_json", "yt_dlp_enrichment"]
    assert scraper.runtime_metadata["source_mode"] == "hybrid"


def test_fetch_comments_falls_back_to_shorts_bootstrap_when_watch_has_no_token(monkeypatch) -> None:
    scraper = YouTubeScraper()
    called_urls: list[str] = []

    class _FakeResponse:
        def __init__(self, text: str):
            self.text = text
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

    def _fake_get(url: str, **_kwargs):
        called_urls.append(url)
        return _FakeResponse(url)

    monkeypatch.setattr(scraper, "_rate_limit", lambda _delay, **_kw: None)
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
    monkeypatch.setattr(scraper, "_rate_limit", lambda _delay, **_kw: None)
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
                        "continuationItems": [{"commentViewModel": {"commentViewModel": {"commentId": "reply-1"}}}]
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
