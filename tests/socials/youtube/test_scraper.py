"""Unit tests for YouTube scraper traversal and comment parsing behavior."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

from urllib3.util import Timeout

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


def test_fetch_continuation_uses_bounded_continuation_timeout(monkeypatch) -> None:
    scraper = YouTubeScraper()
    captured: dict[str, object] = {}

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"ok": True}

    def _post(*_args, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        return _Response()

    monkeypatch.setattr("trr_backend.socials.youtube.scraper.requests.post", _post)

    assert scraper._fetch_continuation("token", delay=0) == {"ok": True}
    assert isinstance(captured["timeout"], Timeout)
    assert captured["timeout"].total == scraper.CONTINUATION_REQUEST_TOTAL_TIMEOUT_SECONDS
    assert captured["timeout"].connect_timeout == scraper.CONTINUATION_REQUEST_TIMEOUT_SECONDS[0]
    assert captured["timeout"].read_timeout == scraper.CONTINUATION_REQUEST_TIMEOUT_SECONDS[1]


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
    monkeypatch.setattr(scraper, "_maybe_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: {"attempted": False})
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
    monkeypatch.setattr(scraper, "_maybe_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: {"attempted": False})

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

    config = YouTubeScrapeConfig(
        channel_handle="bravo",
        keywords=[],
        max_results=None,
        allow_ytdlp_search_supplement=False,
    )
    scraper.scrape(config, progress_cb=lambda payload: progress_events.append(dict(payload)))

    shorts_initial = next(event for event in progress_events if event.get("phase") == "scrape_initial_page_shorts")
    assert int(shorts_initial.get("pages_scanned") or 0) > 0


def test_scrape_can_disable_ytdlp_search_supplement(monkeypatch) -> None:
    scraper = YouTubeScraper()

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "chan"))
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trr_backend.socials.youtube.scraper.shutil.which", lambda _binary: "/usr/bin/yt-dlp")

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

    def _fail_search(*_args, **_kwargs):
        raise AssertionError("yt-dlp supplement should be disabled")

    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)
    monkeypatch.setattr(scraper, "_search_via_ytdlp", _fail_search)

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=["Bravo"],
            allow_ytdlp_search_supplement=False,
        )
    )

    assert videos == []
    assert scraper.last_retrieval_meta["yt_dlp_supplement_needed"] is True
    assert scraper.last_retrieval_meta["yt_dlp_supplement_enabled"] is False
    assert scraper.last_retrieval_meta["fallback_chain"] == ["channel_page_json"]


def test_scrape_uses_ytdlp_channel_fallback_when_channel_pages_are_empty(monkeypatch) -> None:
    scraper = YouTubeScraper()
    video_ts = int(datetime(2026, 5, 10, 12, 0, tzinfo=UTC).timestamp())
    short_ts = int(datetime(2026, 5, 11, 12, 0, tzinfo=UTC).timestamp())

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda *args, **kwargs: {"surface": kwargs.get("surface")})
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "UC123"))
    monkeypatch.setattr(scraper, "_extract_channel_title_from_data", lambda *_args, **_kwargs: "Bravo")
    monkeypatch.setattr(scraper, "_extract_channel_avatar_from_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trr_backend.socials.youtube.scraper.shutil.which", lambda _binary: "/usr/bin/yt-dlp")

    def _empty_page(*_args, **_kwargs):
        return [], {
            "checked_renderers": 0,
            "before_window_items": 0,
            "after_window_items": 0,
            "window_candidate_items": 0,
            "timestamp_unknown": 0,
            "in_range_hits": 0,
        }

    def _fake_run(cmd, **_kwargs):
        url = cmd[-1]
        if url.endswith("/videos"):
            payload = {
                "id": "video-yt-dlp",
                "title": "Bravo clip",
                "description": "clip",
                "timestamp": video_ts,
                "duration": 180,
                "webpage_url": "https://www.youtube.com/watch?v=video-yt-dlp",
                "channel_id": "UC123",
                "channel": "Bravo",
                "uploader_url": "https://www.youtube.com/@bravo",
            }
        else:
            payload = {
                "id": "short-yt-dlp",
                "title": "Bravo short",
                "description": "short",
                "timestamp": short_ts,
                "duration": 30,
                "webpage_url": "https://www.youtube.com/shorts/short-yt-dlp",
                "channel_id": "UC123",
                "channel": "Bravo",
                "uploader_url": "https://www.youtube.com/@bravo",
            }
        return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(scraper, "_process_video_data", _empty_page)
    monkeypatch.setattr("trr_backend.socials.youtube.scraper.subprocess.run", _fake_run)

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=[],
            date_start=datetime(2026, 5, 1, tzinfo=UTC),
            date_end=datetime(2026, 5, 18, tzinfo=UTC),
            delay_seconds=0,
            max_results=5,
        )
    )

    assert [video.video_id for video in videos] == ["short-yt-dlp", "video-yt-dlp"]
    assert {video.source_surface for video in videos} == {"videos", "shorts"}
    assert scraper.last_retrieval_meta["fallback_chain"] == ["channel_page_json", "yt_dlp_channel"]
    assert scraper.last_retrieval_meta["yt_dlp_channel_fallback_used"] is True
    assert scraper.last_retrieval_meta["yt_dlp_channel_fallback_posts_checked"] == 2
    assert scraper.last_retrieval_meta["matched_posts"] == 2


def test_scrape_skips_ytdlp_channel_fallback_for_bounded_window_no_hits(monkeypatch) -> None:
    scraper = YouTubeScraper()

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda *args, **kwargs: {"surface": kwargs.get("surface")})
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "UC123"))
    monkeypatch.setattr(scraper, "_extract_channel_title_from_data", lambda *_args, **_kwargs: "Bravo")
    monkeypatch.setattr(scraper, "_extract_channel_avatar_from_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_maybe_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: {"attempted": False})

    def _non_matching_page(*_args, **_kwargs):
        return [], {
            "checked_renderers": 24,
            "before_window_items": 0,
            "after_window_items": 24,
            "window_candidate_items": 0,
            "timestamp_unknown": 0,
            "in_range_hits": 0,
        }

    def _unexpected_ytdlp_fallback(*_args, **_kwargs):
        raise AssertionError("bounded window no-hit scans should not run full-channel yt-dlp fallback")

    monkeypatch.setattr(scraper, "_process_video_data", _non_matching_page)
    monkeypatch.setattr(scraper, "_scrape_channel_via_ytdlp", _unexpected_ytdlp_fallback)
    monkeypatch.setattr("trr_backend.socials.youtube.scraper.shutil.which", lambda _binary: "/usr/bin/yt-dlp")

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=[],
            date_start=datetime(2026, 5, 14, tzinfo=UTC),
            date_end=datetime(2026, 5, 15, tzinfo=UTC),
            delay_seconds=0,
        )
    )

    assert videos == []
    assert scraper.last_retrieval_meta["yt_dlp_channel_fallback_used"] is False
    assert (
        scraper.last_retrieval_meta["yt_dlp_channel_fallback_skip_reason"]
        == "bounded_window_no_hits_after_channel_scan"
    )
    assert scraper.last_retrieval_meta["posts_checked"] == 48


def test_scrape_applies_max_results_before_ytdlp_enrichment(monkeypatch) -> None:
    scraper = YouTubeScraper()
    enriched_batches: list[list[str]] = []

    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda *args, **kwargs: {"surface": kwargs.get("surface")})
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "chan"))
    monkeypatch.setattr(scraper, "_extract_channel_title_from_data", lambda *_args, **_kwargs: "Bravo")
    monkeypatch.setattr(scraper, "_extract_channel_avatar_from_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: None)

    def _fake_enrich(videos, *_args, **_kwargs):
        enriched_batches.append([video.video_id for video in videos])

    def _needs_enrichment(video_id: str, *, surface: str, published_at: int) -> YouTubeVideo:
        video = _build_video(video_id, surface=surface, published_at=published_at)
        video.likes = 0
        video.comments = 0
        video.duration_seconds = 0
        video.duration = ""
        return video

    def _fake_process_video_data(*_args, **kwargs):
        surface = kwargs.get("surface")
        if surface == "videos":
            videos = [
                _needs_enrichment("video-1", surface="videos", published_at=1_000),
                _needs_enrichment("video-2", surface="videos", published_at=900),
                _needs_enrichment("video-3", surface="videos", published_at=800),
            ]
        else:
            videos = [
                _needs_enrichment("short-1", surface="shorts", published_at=950),
                _needs_enrichment("short-2", surface="shorts", published_at=850),
                _needs_enrichment("short-3", surface="shorts", published_at=750),
            ]
        return videos, {
            "checked_renderers": len(videos),
            "before_window_items": 0,
            "after_window_items": 0,
            "window_candidate_items": len(videos),
            "timestamp_unknown": 0,
            "in_range_hits": len(videos),
        }

    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", _fake_enrich)
    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=[],
            delay_seconds=0,
            max_results=2,
        )
    )

    assert len(videos) == 2
    assert enriched_batches == [[video.video_id for video in videos]]
    assert {scraper._video_surface(video) for video in videos} == {"videos", "shorts"}
    assert scraper.last_retrieval_meta["requested_max_results"] == 2
    assert scraper.last_retrieval_meta["effective_max_results"] == 2


def test_scrape_skips_ytdlp_enrichment_for_large_channel_collection(monkeypatch) -> None:
    scraper = YouTubeScraper()
    monkeypatch.setattr(scraper, "YTDLP_ENRICH_MAX_VIDEOS", 2)
    monkeypatch.setattr(scraper, "fetch_channel_videos", lambda *args, **kwargs: {"surface": kwargs.get("surface")})
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "chan"))
    monkeypatch.setattr(scraper, "_extract_channel_title_from_data", lambda *_args, **_kwargs: "Bravo")
    monkeypatch.setattr(scraper, "_extract_channel_avatar_from_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trr_backend.socials.youtube.scraper.shutil.which", lambda _binary: "/usr/bin/yt-dlp")

    def _fail_enrich(*_args, **_kwargs):
        raise AssertionError("large catalog scrape should not enrich every video via yt-dlp")

    def _needs_enrichment(video_id: str, published_at: int) -> YouTubeVideo:
        video = _build_video(video_id, surface="videos", published_at=published_at)
        video.likes = 0
        video.comments = 0
        video.duration_seconds = 0
        video.duration = ""
        return video

    def _fake_process_video_data(*_args, **kwargs):
        if kwargs.get("surface") == "shorts":
            videos: list[YouTubeVideo] = []
        else:
            videos = [
                _needs_enrichment("video-1", 1_000),
                _needs_enrichment("video-2", 900),
                _needs_enrichment("video-3", 800),
            ]
        return videos, {
            "checked_renderers": len(videos),
            "before_window_items": 0,
            "after_window_items": 0,
            "window_candidate_items": len(videos),
            "timestamp_unknown": 0,
            "in_range_hits": len(videos),
        }

    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", _fail_enrich)
    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)

    videos = scraper.scrape(YouTubeScrapeConfig(channel_handle="bravo", keywords=[], delay_seconds=0))

    assert len(videos) == 3
    assert scraper.last_retrieval_meta["yt_dlp_enrichment"]["attempted"] is False
    assert scraper.last_retrieval_meta["yt_dlp_enrichment"]["skip_reason"] == "video_count_exceeds_limit"
    assert scraper.last_retrieval_meta["yt_dlp_enrichment"]["skipped_count"] == 3


def test_scrape_skips_continuations_after_max_results_target(monkeypatch) -> None:
    scraper = YouTubeScraper()
    fetched_surfaces: list[str] = []

    def _fake_fetch_channel_videos(_handle, _delay, *, surface: str, fast_mode: bool = False):
        del fast_mode
        fetched_surfaces.append(surface)
        return {"surface": surface}

    def _fake_process_video_data(*_args, **kwargs):
        surface = kwargs.get("surface")
        if surface == "videos":
            videos = [
                _build_video("video-1", surface="videos", published_at=1_000),
                _build_video("video-2", surface="videos", published_at=900),
                _build_video("video-3", surface="videos", published_at=800),
            ]
        else:
            videos = [_build_video("short-1", surface="shorts", published_at=950)]
        return videos, {
            "checked_renderers": len(videos),
            "before_window_items": 0,
            "after_window_items": 0,
            "window_candidate_items": len(videos),
            "timestamp_unknown": 0,
            "in_range_hits": len(videos),
        }

    monkeypatch.setattr(scraper, "fetch_channel_videos", _fake_fetch_channel_videos)
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "chan"))
    monkeypatch.setattr(scraper, "_extract_channel_title_from_data", lambda *_args, **_kwargs: "Bravo")
    monkeypatch.setattr(scraper, "_extract_channel_avatar_from_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: "continuation-token")
    monkeypatch.setattr(
        scraper,
        "_fetch_continuation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("continuation should be skipped")),
    )
    monkeypatch.setattr(scraper, "_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=[],
            delay_seconds=0,
            max_results=2,
            max_pages=10,
        )
    )

    assert fetched_surfaces == ["videos", "shorts"]
    assert len(videos) == 2
    assert {scraper._video_surface(video) for video in videos} == {"videos", "shorts"}
    assert scraper.last_retrieval_meta["continuation_pages"] == 0


def test_scrape_stops_bounded_window_after_in_range_no_hit_page(monkeypatch) -> None:
    scraper = YouTubeScraper()
    fetched_tokens: list[str] = []
    initial_ts = int(datetime(2026, 5, 8, tzinfo=UTC).timestamp())

    def _fake_fetch_channel_videos(_handle, _delay, *, surface: str, fast_mode: bool = False):
        del fast_mode
        return {"surface": surface}

    def _fake_process_video_data(*_args, **kwargs):
        surface = kwargs.get("surface")
        if surface == "videos":
            videos = [_build_video("video-in-window", surface="videos", published_at=initial_ts)]
            return videos, {
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

    def _fake_fetch_continuation(token: str, *_args, **_kwargs):
        fetched_tokens.append(token)
        return {"token": token}

    def _fake_extract_continuation(_data):
        return [{"videoRenderer": {"videoId": "no-hit"}}], "next-token"

    def _fake_process_renderer_batch(*_args, **_kwargs):
        return [], {
            "checked_renderers": 30,
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
        lambda data: "first-token" if data.get("surface") == "videos" else None,
    )
    monkeypatch.setattr(scraper, "_fetch_continuation", _fake_fetch_continuation)
    monkeypatch.setattr(scraper, "_extract_continuation_videos_and_token", _fake_extract_continuation)
    monkeypatch.setattr(scraper, "_process_renderer_batch", _fake_process_renderer_batch)
    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)
    monkeypatch.setattr(scraper, "_maybe_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: {"attempted": False})

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=[],
            date_start=datetime(2026, 5, 1, tzinfo=UTC),
            date_end=datetime(2026, 5, 18, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
            allow_ytdlp_video_enrichment=False,
        )
    )

    assert [video.video_id for video in videos] == ["video-in-window"]
    assert fetched_tokens == ["first-token"]
    assert scraper.last_retrieval_meta["continuation_pages_by_surface"]["videos"] == 1
    assert scraper.last_retrieval_meta["continuation_failure_count"] == 0
    assert scraper.runtime_metadata["complete"] is True


def test_scrape_applies_bounded_max_pages_across_surfaces(monkeypatch) -> None:
    scraper = YouTubeScraper()
    fetched_surfaces: list[str] = []

    def _fake_fetch_channel_videos(_handle, _delay, *, surface: str, fast_mode: bool = False):
        del fast_mode
        fetched_surfaces.append(surface)
        return {"surface": surface}

    def _fake_process_video_data(*_args, **_kwargs):
        return [], {
            "checked_renderers": 1,
            "before_window_items": 0,
            "after_window_items": 1,
            "window_candidate_items": 0,
            "timestamp_unknown": 0,
            "in_range_hits": 0,
        }

    def _fake_process_renderer_batch(*_args, **_kwargs):
        return [], {
            "checked_renderers": 30,
            "before_window_items": 0,
            "after_window_items": 30,
            "window_candidate_items": 0,
            "timestamp_unknown": 0,
            "in_range_hits": 0,
        }

    monkeypatch.setattr(scraper, "fetch_channel_videos", _fake_fetch_channel_videos)
    monkeypatch.setattr(scraper, "_extract_channel_identity_from_data", lambda *_args, **_kwargs: ("bravo", "chan"))
    monkeypatch.setattr(scraper, "_extract_channel_title_from_data", lambda *_args, **_kwargs: "Bravo")
    monkeypatch.setattr(scraper, "_extract_channel_avatar_from_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_extract_channel_continuation_token", lambda *_args, **_kwargs: "first-token")
    monkeypatch.setattr(scraper, "_fetch_continuation", lambda *_args, **_kwargs: {"items": []})
    monkeypatch.setattr(
        scraper,
        "_extract_continuation_videos_and_token",
        lambda *_args, **_kwargs: ([{"videoRenderer": {"videoId": "no-hit"}}], "next-token"),
    )
    monkeypatch.setattr(scraper, "_process_video_data", _fake_process_video_data)
    monkeypatch.setattr(scraper, "_process_renderer_batch", _fake_process_renderer_batch)
    monkeypatch.setattr(scraper, "_maybe_enrich_videos_via_ytdlp", lambda *_args, **_kwargs: {"attempted": False})
    monkeypatch.setattr("trr_backend.socials.youtube.scraper.shutil.which", lambda _binary: "/usr/bin/yt-dlp")

    videos = scraper.scrape(
        YouTubeScrapeConfig(
            channel_handle="bravo",
            keywords=[],
            date_start=datetime(2026, 5, 14, tzinfo=UTC),
            date_end=datetime(2026, 5, 15, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
            allow_ytdlp_video_enrichment=False,
        )
    )

    assert videos == []
    assert fetched_surfaces == ["videos"]
    assert scraper.last_retrieval_meta["continuation_pages"] == 1
    assert scraper.last_retrieval_meta["continuation_pages_by_surface"] == {"videos": 1, "shorts": 0}
    assert scraper.last_retrieval_meta["scan_capped_reason"] == "max_pages"


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
