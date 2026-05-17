from __future__ import annotations

import pytest

from trr_backend.socials.socialblade.scraper import (
    SocialBladeEndpointError,
    _build_profile_stats_from_user_payload,
    _build_total_followers_chart_from_daily_deltas,
    _build_total_followers_chart_from_total_rows,
    _extract_profile_stats_from_body_text,
    _followers_chart_from_table,
    _history_rows_to_metrics,
    _mark_payload_as_degraded_attempt,
    _merge_followers_charts,
    _modal_runtime_disallows_visible_socialblade_login,
    _normalize_table_data,
    _page_access_denied,
    _parse_metric_number,
    _scrape_socialblade_in_context,
    _socialblade_page_is_logged_in,
    _socialblade_profile_url,
    scrape_socialblade,
)

BODY_TEXT = """
Lisa Barlow
@lisabarlow14
View on Instagram

Followers

475,444

Following

7,090

Media Count

1,703

Engagement Rate

3.02%

Average Likes

13,894.63

Average Comments

456.75

Login to Favorite
Page Summary
Future Projections
Live Follower Count
B+
Grade
38,982nd

SB Rank

139,823rd

Followers Rank

45,085th

Engagement Rate Rank

LAST 14 DAYS
1.6K
Followers for the last 14 days
5
Media Count for the last 14 days
Daily Channel Metrics
Last 14 Days
Date    Followers   Following   Media Count
Thu2026-03-05   40  473,873 -2  7,072   1   1,699
Fri2026-03-06   145 474,018 -1  7,071   1   1,700
"""


def test_extract_profile_stats_from_body_text_prefers_primary_values() -> None:
    stats, rankings, labels = _extract_profile_stats_from_body_text(BODY_TEXT, "instagram")

    assert stats == {
        "followers": 475444,
        "following": 7090,
        "media_count": 1703,
        "engagement_rate": "3.02%",
        "average_likes": pytest.approx(13894.63),
        "average_comments": pytest.approx(456.75),
    }
    assert rankings == {
        "grade": "B+",
        "sb_rank": "38,982nd",
        "followers_rank": "139,823rd",
        "engagement_rate_rank": "45,085th",
    }
    assert labels["followers"] == "Followers"
    assert labels["chart_metric_label"] == "Followers"


def test_normalize_table_data_and_build_chart_from_followers_totals() -> None:
    metrics = _normalize_table_data(
        {
            "headers": [
                "Date",
                "Followers Delta",
                "Followers Total",
                "Following Delta",
                "Following Total",
                "Media Count Delta",
                "Media Count Total",
            ],
            "data": [
                {
                    "Date": "Thu2026-03-05",
                    "Followers Delta": "40",
                    "Followers Total": "473,873",
                    "Following Delta": "-2",
                    "Following Total": "7,072",
                    "Media Count Delta": "1",
                    "Media Count Total": "1,699",
                },
                {
                    "Date": "Fri2026-03-06",
                    "Followers Delta": "145",
                    "Followers Total": "474,018",
                    "Following Delta": "-1",
                    "Following Total": "7,071",
                    "Media Count Delta": "1",
                    "Media Count Total": "1,700",
                },
            ],
        },
        BODY_TEXT,
    )

    assert metrics["period"] == "Last 14 Days"
    assert metrics["row_count"] == 2
    assert metrics["headers"] == [
        "Date",
        "Followers Delta",
        "Followers Total",
        "Following Delta",
        "Following Total",
        "Media Count Delta",
        "Media Count Total",
    ]

    chart = _followers_chart_from_table(metrics, metric_label="Followers")
    assert chart == {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": 2,
        "date_range": {"from": "2026-03-05", "to": "2026-03-06"},
        "data": [
            {"date": "2026-03-05", "followers": 473873},
            {"date": "2026-03-06", "followers": 474018},
        ],
    }


def test_page_access_denied_detects_cloudflare_block() -> None:
    assert _page_access_denied("Access denied. Error reference number: 1020")


def test_socialblade_logged_in_detection_accepts_session_cookie() -> None:
    class DummyLocator:
        def count(self) -> int:
            return 0

    class DummyPage:
        def locator(self, _selector: str) -> DummyLocator:
            return DummyLocator()

        def text_content(self, _selector: str, **_kwargs):
            return "SOCIAL BLADE Personalized Homepage"

    class DummyContext:
        def cookies(self):
            return [{"name": "session", "value": "token", "domain": ".socialblade.com"}]

    assert _socialblade_page_is_logged_in(DummyPage(), DummyContext()) is True


def test_modal_runtime_disallows_visible_socialblade_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODAL_TASK_ID", "task-1")

    assert _modal_runtime_disallows_visible_socialblade_login() is True


def test_socialblade_profile_url_switches_route_by_platform() -> None:
    assert (
        _socialblade_profile_url("instagram", "lisabarlow14") == "https://socialblade.com/instagram/user/lisabarlow14"
    )
    assert _socialblade_profile_url("facebook", "bravotv") == "https://socialblade.com/facebook/user/bravotv"
    assert _socialblade_profile_url("tiktok", "bravotv") == "https://socialblade.com/tiktok/user/bravotv"
    assert _socialblade_profile_url("youtube", "facebookapp") == "https://socialblade.com/youtube/handle/facebookapp"
    assert _socialblade_profile_url("youtube", "UCabc123") == "https://socialblade.com/youtube/channel/UCabc123"


def test_socialblade_profile_url_normalizes_full_profile_urls() -> None:
    assert (
        _socialblade_profile_url("instagram", "https://socialblade.com/instagram/user/LisaBarlow14?foo=1")
        == "https://socialblade.com/instagram/user/lisabarlow14"
    )
    assert (
        _socialblade_profile_url("tiktok", "https://www.tiktok.com/@BravoTV?lang=en")
        == "https://socialblade.com/tiktok/user/bravotv"
    )
    assert (
        _socialblade_profile_url("youtube", "https://www.youtube.com/channel/UCabc123")
        == "https://socialblade.com/youtube/channel/UCabc123"
    )
    assert (
        _socialblade_profile_url("facebook", "https://www.facebook.com/profile.php?id=123456789")
        == "https://socialblade.com/facebook/user/123456789"
    )


def test_parse_metric_number_handles_labeled_suffix_values() -> None:
    assert _parse_metric_number("Likes 111.4M") == 111_400_000
    assert _parse_metric_number("111.4M Likes") == 111_400_000
    assert _parse_metric_number("Followers for the last 14 days 1.6K") == 1_600


def test_build_profile_stats_from_user_payload_formats_ranks() -> None:
    stats, rankings = _build_profile_stats_from_user_payload(
        {
            "followers": "475444",
            "following": 7090,
            "media_count": 1703,
            "engagement_rate": 3.019999980926514,
            "average_likes": 13894.625,
            "average_comments": 456.75,
            "grade": "B+",
            "ranks": {
                "sb": 38982,
                "followers": 139823,
                "engagement_rate": 45085,
            },
        }
    )

    assert stats == {
        "followers": 475444,
        "following": 7090,
        "media_count": 1703,
        "engagement_rate": "3.02%",
        "average_likes": pytest.approx(13894.625),
        "average_comments": pytest.approx(456.75),
    }
    assert rankings == {
        "grade": "B+",
        "sb_rank": "38,982nd",
        "followers_rank": "139,823rd",
        "engagement_rate_rank": "45,085th",
    }


def test_build_profile_stats_from_tiktok_payload_uses_likes_as_third_metric() -> None:
    stats, rankings = _build_profile_stats_from_user_payload(
        {
            "followers": "4.2M",
            "following": 432,
            "likes": "122.5M",
            "engagement_rate": 4.125,
            "average_likes": 12345.6,
            "average_comments": 789.1,
            "grade": "A-",
            "ranks": {
                "sb": 120,
                "followers": 650,
                "engagement_rate": 88,
            },
        },
        platform="tiktok",
    )

    assert stats == {
        "followers": 4200000,
        "following": 432,
        "media_count": 122500000,
        "engagement_rate": "4.12%",
        "average_likes": pytest.approx(12345.6),
        "average_comments": pytest.approx(789.1),
    }
    assert rankings == {
        "grade": "A-",
        "sb_rank": "120th",
        "followers_rank": "650th",
        "engagement_rate_rank": "88th",
    }


def test_history_rows_to_metrics_builds_totals_and_deltas() -> None:
    metrics = _history_rows_to_metrics(
        [
            {
                "date": "2026-03-05T00:00:00.000Z",
                "followers": 473873,
                "following": 7072,
                "media_count": 1699,
            },
            {
                "date": "2026-03-06T00:00:00.000Z",
                "followers": 474018,
                "following": 7071,
                "media_count": 1700,
            },
        ],
        limit=60,
    )

    assert metrics == {
        "period": "Last 2 Days",
        "row_count": 2,
        "headers": [
            "Date",
            "Followers Delta",
            "Followers Total",
            "Following Delta",
            "Following Total",
            "Media Count Delta",
            "Media Count Total",
        ],
        "data": [
            {
                "Date": "2026-03-05",
                "Followers Delta": "473873",
                "Followers Total": "473,873",
                "Following Delta": "7072",
                "Following Total": "7,072",
                "Media Count Delta": "1699",
                "Media Count Total": "1,699",
            },
            {
                "Date": "2026-03-06",
                "Followers Delta": "145",
                "Followers Total": "474,018",
                "Following Delta": "-1",
                "Following Total": "7,071",
                "Media Count Delta": "1",
                "Media Count Total": "1,700",
            },
        ],
    }


def test_history_rows_to_metrics_labels_tiktok_likes_history() -> None:
    metrics = _history_rows_to_metrics(
        [
            {
                "date": "2026-05-12T00:00:00.000Z",
                "followers": 1_000_000,
                "following": 400,
                "likes": 10_000_000,
            },
            {
                "date": "2026-05-13T00:00:00.000Z",
                "followers": 1_000_500,
                "following": 401,
                "likes": 10_002_000,
            },
        ],
        limit=60,
        platform="tiktok",
    )

    assert metrics["headers"] == [
        "Date",
        "Followers Delta",
        "Followers Total",
        "Following Delta",
        "Following Total",
        "Likes Delta",
        "Likes Total",
    ]
    assert metrics["data"][-1] == {
        "Date": "2026-05-13",
        "Followers Delta": "500",
        "Followers Total": "1,000,500",
        "Following Delta": "1",
        "Following Total": "401",
        "Likes Delta": "2000",
        "Likes Total": "10,002,000",
    }


def test_build_total_followers_chart_from_daily_deltas_reconstructs_totals() -> None:
    chart = _build_total_followers_chart_from_daily_deltas(
        150,
        [
            {"date": "2026-03-16T00:00:00.000Z", "followers": 10},
            {"date": "2026-03-17T00:00:00.000Z", "followers": 5},
        ],
    )

    assert chart == {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": 2,
        "date_range": {"from": "2026-03-16", "to": "2026-03-17"},
        "data": [
            {"date": "2026-03-16", "followers": 145},
            {"date": "2026-03-17", "followers": 150},
        ],
    }


def test_build_total_followers_chart_from_total_rows_uses_daily_total_points() -> None:
    chart = _build_total_followers_chart_from_total_rows(
        [
            {"date": "2026-03-16T00:00:00.000Z", "followers": "145"},
            {"date": "2026-03-17T00:00:00.000Z", "followers": 150},
            {"date": "2026-03-17T12:00:00.000Z", "followers": "151"},
        ],
    )

    assert chart == {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": 2,
        "date_range": {"from": "2026-03-16", "to": "2026-03-17"},
        "data": [
            {"date": "2026-03-16", "followers": 145},
            {"date": "2026-03-17", "followers": 151},
        ],
    }


def test_followers_chart_from_table_accepts_spaced_weekday_dates() -> None:
    chart = _followers_chart_from_table(
        {
            "headers": ["Date", "Followers Total"],
            "data": [
                {"Date": "Sat 2026-04-11", "Followers Total": "171,945"},
                {"Date": "Sun 2026-04-12", "Followers Total": "171,999"},
            ],
        },
        metric_label="Followers",
    )

    assert chart == {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": 2,
        "date_range": {"from": "2026-04-11", "to": "2026-04-12"},
        "data": [
            {"date": "2026-04-11", "followers": 171945},
            {"date": "2026-04-12", "followers": 171999},
        ],
    }


def test_merge_followers_charts_uses_sixty_day_table_to_extend_stale_chart() -> None:
    stale_chart = {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": 2,
        "date_range": {"from": "2026-01-11", "to": "2026-04-24"},
        "data": [
            {"date": "2026-01-11", "followers": 40237},
            {"date": "2026-04-24", "followers": 172238},
        ],
    }
    table_chart = _followers_chart_from_table(
        {
            "headers": ["Date", "Followers Total"],
            "data": [
                {"Date": "2026-04-13", "Followers Total": "172,029"},
                {"Date": "2026-04-24", "Followers Total": "172,238"},
                {"Date": "2026-05-13", "Followers Total": "172,666"},
            ],
        },
        metric_label="Followers",
    )

    merged = _merge_followers_charts(stale_chart, table_chart)

    assert merged == {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": 4,
        "date_range": {"from": "2026-01-11", "to": "2026-05-13"},
        "data": [
            {"date": "2026-01-11", "followers": 40237},
            {"date": "2026-04-13", "followers": 172029},
            {"date": "2026-04-24", "followers": 172238},
            {"date": "2026-05-13", "followers": 172666},
        ],
    }


def test_scrape_context_retries_412_in_visible_shared_browser(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyContext:
        def add_init_script(self, _script: str) -> None:
            return None

        def add_cookies(self, _cookies) -> None:
            return None

        def new_page(self):
            return DummyPage()

    class DummyPage:
        def goto(self, *_args, **_kwargs) -> None:
            return None

        def wait_for_timeout(self, *_args, **_kwargs) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "trr_backend.socials.socialblade.auth.normalize_socialblade_cookies",
        lambda cookies: cookies,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._extract_body_text",
        lambda _page: "Followers\n100\nFollowing\n10\nMedia Count\n5",
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._page_access_denied",
        lambda _body_text: False,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._scrape_authenticated_api",
        lambda _page, _handle: (_ for _ in ()).throw(
            SocialBladeEndpointError("/api/trpc/instagram.monthly?batch=1", 412)
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper.scrape_socialblade_with_shared_browser_session",
        lambda handle, platform="instagram", playwright=None: {
            "username": handle,
            "platform": platform,
            "history_source": "authenticated_api",
            "stats_refreshed": True,
        },
    )

    payload = _scrape_socialblade_in_context(
        DummyContext(),
        "heathergay",
        platform="instagram",
        playwright=None,
        cookies=[],
        allow_login_fallback=False,
        allow_visible_browser_retry=True,
    )

    assert payload == {
        "username": "heathergay",
        "platform": "instagram",
        "history_source": "authenticated_api",
        "stats_refreshed": True,
    }


def test_scrape_socialblade_uses_visible_browser_retry_when_scrapling_page_data_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_scrapling(*_args, **_kwargs):
        raise RuntimeError("SocialBlade scrape failed: incomplete profile stats or daily metrics data")

    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._run_scrapling_socialblade_fetch",
        fake_run_scrapling,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper.scrape_socialblade_with_shared_browser_session",
        lambda handle, platform="instagram", playwright=None: {
            "username": handle,
            "platform": platform,
            "history_source": "visible_browser",
            "stats_refreshed": True,
        },
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._refresh_socialblade_cookies_via_login",
        lambda: (_ for _ in ()).throw(AssertionError("login fallback should not run before visible retry")),
    )

    payload = scrape_socialblade(
        "thetraitorsus",
        {"cf_clearance": "token"},
        platform="instagram",
        allow_login_fallback=True,
        allow_visible_browser_retry=True,
    )

    assert payload == {
        "username": "thetraitorsus",
        "platform": "instagram",
        "history_source": "visible_browser",
        "stats_refreshed": True,
    }


@pytest.mark.parametrize(
    ("error", "platform"),
    [
        (RuntimeError("SocialBlade blocked by Cloudflare (1020 access denied)"), "instagram"),
        (RuntimeError("SocialBlade endpoint /api/trpc/tiktok.search returned HTTP 401"), "tiktok"),
        (RuntimeError("SocialBlade endpoint /api/trpc/facebook.user returned HTTP 403"), "facebook"),
    ],
)
def test_scrape_socialblade_uses_visible_browser_retry_for_socialblade_access_failures(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    platform: str,
) -> None:
    def fake_run_scrapling(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._run_scrapling_socialblade_fetch",
        fake_run_scrapling,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper.scrape_socialblade_with_shared_browser_session",
        lambda handle, platform="instagram", playwright=None: {
            "username": handle,
            "platform": platform,
            "history_source": "visible_browser",
            "stats_refreshed": True,
        },
    )

    payload = scrape_socialblade(
        "thetraitorsus",
        {"cf_clearance": "token"},
        platform=platform,
        allow_login_fallback=False,
        allow_visible_browser_retry=True,
    )

    assert payload == {
        "username": "thetraitorsus",
        "platform": platform,
        "history_source": "visible_browser",
        "stats_refreshed": True,
    }


def test_scrape_socialblade_logs_in_and_retries_when_history_is_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    def fake_run_scrapling(_handle: str, cookies: object, *, platform: str):
        calls.append(cookies)
        if len(calls) == 1:
            return {
                "username": "thetraitorsus",
                "platform": platform,
                "history_source": "page_trpc_capture",
                "stats_refreshed": True,
                "daily_channel_metrics_60day": {
                    "period": "Last 31 Days",
                    "row_count": 31,
                    "data": [],
                },
            }
        return {
            "username": "thetraitorsus",
            "platform": platform,
            "history_source": "authenticated_api",
            "stats_refreshed": True,
            "daily_channel_metrics_60day": {
                "period": "Last 60 Days",
                "row_count": 60,
                "data": [],
            },
        }

    monkeypatch.setenv("SOCIALBLADE_EMAIL", "operator@example.com")
    monkeypatch.setenv("SOCIALBLADE_PASSWORD", "password")
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._run_scrapling_socialblade_fetch",
        fake_run_scrapling,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._refresh_socialblade_cookies_via_login",
        lambda: {"cf_clearance": "fresh", "session": "logged-in"},
    )

    payload = scrape_socialblade(
        "thetraitorsus",
        {"cf_clearance": "stale"},
        platform="instagram",
        allow_login_fallback=True,
        allow_visible_browser_retry=False,
    )

    assert payload["history_source"] == "authenticated_api"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 60
    assert calls == [{"cf_clearance": "stale"}, {"cf_clearance": "fresh", "session": "logged-in"}]


def test_scrape_socialblade_keeps_seeded_modal_table_result_when_history_is_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_scrapling(_handle: str, _cookies: object, *, platform: str):
        return {
            "username": "bravotv",
            "platform": platform,
            "history_source": "table_fallback",
            "stats_refreshed": True,
            "runtime_metadata": {"seed_has_socialblade_session": True},
            "daily_channel_metrics_60day": {
                "period": "Last 14 Days",
                "row_count": 14,
                "data": [],
            },
        }

    monkeypatch.setenv("MODAL_TASK_ID", "task-1")
    monkeypatch.setenv("SOCIALBLADE_EMAIL", "operator@example.com")
    monkeypatch.setenv("SOCIALBLADE_PASSWORD", "password")
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._run_scrapling_socialblade_fetch",
        fake_run_scrapling,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._refresh_socialblade_cookies_via_login",
        lambda: (_ for _ in ()).throw(AssertionError("Modal should not try visible login for seeded sessions")),
    )

    payload = scrape_socialblade(
        "bravotv",
        {"session": "seeded"},
        platform="instagram",
        allow_login_fallback=True,
        allow_visible_browser_retry=False,
    )

    assert payload["stats_refreshed"] is True
    assert payload["history_source"] == "table_fallback"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 14
    assert payload["runtime_metadata"]["seed_has_socialblade_session"] is True


def test_scrape_socialblade_accepts_tiktok_daily_total_control_capture_without_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_scrapling(_handle: str, _cookies: object, *, platform: str):
        return {
            "username": "thetraitorsus",
            "platform": platform,
            "history_source": "page_trpc_capture_short",
            "stats_refreshed": True,
            "runtime_metadata": {
                "capture_control_updates": {
                    "last60Days": "selected",
                    "daily": "selected",
                    "total": "selected",
                },
            },
            "daily_channel_metrics_60day": {
                "period": "Last 60 Days",
                "row_count": 56,
                "data": [],
            },
            "daily_total_followers_chart": {
                "frequency": "daily",
                "metric": "total_followers",
                "total_data_points": 56,
                "date_range": {"from": "2026-03-19", "to": "2026-05-14"},
                "data": [{"date": "2026-03-19", "followers": 140900}],
            },
        }

    monkeypatch.setenv("MODAL_TASK_ID", "task-1")
    monkeypatch.setenv("SOCIALBLADE_EMAIL", "operator@example.com")
    monkeypatch.setenv("SOCIALBLADE_PASSWORD", "password")
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._run_scrapling_socialblade_fetch",
        fake_run_scrapling,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._refresh_socialblade_cookies_via_login",
        lambda: (_ for _ in ()).throw(AssertionError("verified TikTok chart controls should not require login")),
    )

    payload = scrape_socialblade(
        "thetraitorsus",
        {"cf_clearance": "seeded"},
        platform="tiktok",
        allow_login_fallback=True,
        allow_visible_browser_retry=False,
    )

    assert payload["stats_refreshed"] is True
    assert payload["history_source"] == "page_trpc_capture_short"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 56
    assert payload["daily_total_followers_chart"]["date_range"] == {"from": "2026-03-19", "to": "2026-05-14"}


def test_scrape_socialblade_persists_degraded_attempt_when_login_retry_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_scrapling(_handle: str, _cookies: object, *, platform: str):
        return {
            "username": "bravotv",
            "platform": platform,
            "history_source": "page_trpc_capture",
            "stats_refreshed": True,
            "daily_channel_metrics_60day": {
                "period": "Last 31 Days",
                "row_count": 31,
                "data": [],
            },
        }

    monkeypatch.setenv("SOCIALBLADE_EMAIL", "operator@example.com")
    monkeypatch.setenv("SOCIALBLADE_PASSWORD", "password")
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._run_scrapling_socialblade_fetch",
        fake_run_scrapling,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._refresh_socialblade_cookies_via_login",
        lambda: (_ for _ in ()).throw(RuntimeError("visible Chrome CDP connection refused")),
    )

    payload = scrape_socialblade(
        "https://www.instagram.com/bravotv/",
        {"cf_clearance": "stale"},
        platform="instagram",
        allow_login_fallback=True,
        allow_visible_browser_retry=False,
    )

    assert payload["username"] == "bravotv"
    assert payload["stats_refreshed"] is False
    assert payload["history_source"] == "page_trpc_capture_short"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 31
    assert "visible Chrome CDP connection refused" in payload["error"]


def test_complete_page_trpc_capture_is_not_downgraded_when_login_retry_fails() -> None:
    payload = {
        "username": "bravotv",
        "platform": "instagram",
        "history_source": "page_trpc_capture",
        "stats_refreshed": True,
        "daily_channel_metrics_60day": {
            "period": "Last 60 Days",
            "row_count": 60,
            "data": [],
        },
        "daily_total_followers_chart": {
            "frequency": "daily",
            "metric": "total_followers",
            "total_data_points": 60,
            "date_range": {"from": "2026-03-15", "to": "2026-05-13"},
            "data": [],
        },
    }

    rendered = _mark_payload_as_degraded_attempt(payload, RuntimeError("visible Chrome CDP connection refused"))

    assert rendered["stats_refreshed"] is True
    assert rendered["history_source"] == "page_trpc_capture"
    assert rendered["daily_channel_metrics_60day"]["row_count"] == 60
    assert rendered["runtime_metadata"]["login_retry_failed"] is True
    assert rendered["runtime_metadata"]["login_retry_error"] == "visible Chrome CDP connection refused"


def test_scrape_context_retries_search_challenge_in_visible_shared_browser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyContext:
        def add_init_script(self, _script: str) -> None:
            return None

        def add_cookies(self, _cookies) -> None:
            return None

        def new_page(self):
            return DummyPage()

    class DummyPage:
        def goto(self, *_args, **_kwargs) -> None:
            return None

        def wait_for_timeout(self, *_args, **_kwargs) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "trr_backend.socials.socialblade.auth.normalize_socialblade_cookies",
        lambda cookies: cookies,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._extract_body_text",
        lambda _page: "Followers\n100\nFollowing\n10\nMedia Count\n5",
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._page_access_denied",
        lambda _body_text: False,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._scrape_authenticated_api",
        lambda _page, _handle: (_ for _ in ()).throw(
            RuntimeError(
                "SocialBlade returned non-JSON data for "
                "/api/trpc/instagram.search?input=%7B%22json%22%3A%7B%22query%22%3A%22heathergay%22%7D%7D"
            )
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper.scrape_socialblade_with_shared_browser_session",
        lambda handle, platform="instagram", playwright=None: {
            "username": handle,
            "platform": platform,
            "history_source": "authenticated_api",
            "stats_refreshed": True,
        },
    )

    payload = _scrape_socialblade_in_context(
        DummyContext(),
        "heathergay",
        platform="instagram",
        playwright=None,
        cookies=[],
        allow_login_fallback=False,
        allow_visible_browser_retry=True,
    )

    assert payload == {
        "username": "heathergay",
        "platform": "instagram",
        "history_source": "authenticated_api",
        "stats_refreshed": True,
    }


def test_scrape_context_configures_page_controls_before_table_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class DummyContext:
        def add_init_script(self, _script: str) -> None:
            return None

        def add_cookies(self, _cookies) -> None:
            return None

        def new_page(self):
            return DummyPage()

    class DummyPage:
        def goto(self, *_args, **_kwargs) -> None:
            return None

        def wait_for_timeout(self, *_args, **_kwargs) -> None:
            return None

        def evaluate(self, _script: str):
            calls.append("evaluate_table")
            return {
                "headers": [
                    "Date",
                    "Followers Delta",
                    "Followers Total",
                    "Following Delta",
                    "Following Total",
                    "Media Count Delta",
                    "Media Count Total",
                ],
                "data": [
                    {
                        "Date": "Wed2026-03-25",
                        "Followers Delta": "10",
                        "Followers Total": "100",
                        "Following Delta": "1",
                        "Following Total": "20",
                        "Media Count Delta": "1",
                        "Media Count Total": "5",
                    }
                ],
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "trr_backend.socials.socialblade.auth.normalize_socialblade_cookies",
        lambda cookies: cookies,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._extract_body_text",
        lambda _page: "Followers\n100\nFollowing\n20\nMedia Count\n5\nDaily Channel Metrics\nLast 60 Days",
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._page_access_denied",
        lambda _body_text: False,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._scrape_authenticated_api",
        lambda _page, _handle: (_ for _ in ()).throw(RuntimeError("authenticated API unavailable")),
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._configure_socialblade_page_fallback_state",
        lambda _page: calls.append("configure_controls"),
    )

    payload = _scrape_socialblade_in_context(
        DummyContext(),
        "heathergay",
        platform="instagram",
        playwright=None,
        cookies=[],
        allow_login_fallback=False,
        allow_visible_browser_retry=False,
    )

    assert calls[:2] == ["configure_controls", "evaluate_table"]
    assert payload["history_source"] == "table_fallback"
    assert payload["daily_channel_metrics_60day"]["period"] == "Last 60 Days"


def test_scrape_socialblade_uses_visible_browser_retry_when_scrapling_path_is_challenged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_scrapling(*_args, **_kwargs):
        raise RuntimeError("scrapling failed") from SocialBladeEndpointError("/api/trpc/instagram.monthly?batch=1", 412)

    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper._run_scrapling_socialblade_fetch",
        fake_run_scrapling,
    )
    monkeypatch.setattr(
        "trr_backend.socials.socialblade.scraper.scrape_socialblade_with_shared_browser_session",
        lambda handle, platform="instagram", playwright=None: {
            "username": handle,
            "platform": platform,
            "history_source": "authenticated_api",
            "stats_refreshed": True,
        },
    )

    payload = scrape_socialblade(
        "heathergay",
        {"cf_clearance": "token"},
        platform="instagram",
        allow_login_fallback=False,
        allow_visible_browser_retry=True,
    )

    assert payload == {
        "username": "heathergay",
        "platform": "instagram",
        "history_source": "authenticated_api",
        "stats_refreshed": True,
    }
