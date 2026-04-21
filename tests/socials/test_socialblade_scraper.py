from __future__ import annotations

import pytest

from trr_backend.socials.socialblade.scraper import (
    SocialBladeEndpointError,
    _build_profile_stats_from_user_payload,
    _build_total_followers_chart_from_daily_deltas,
    _extract_profile_stats_from_body_text,
    _followers_chart_from_table,
    _history_rows_to_metrics,
    _normalize_table_data,
    _page_access_denied,
    _scrape_socialblade_in_context,
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


def test_socialblade_profile_url_switches_route_by_platform() -> None:
    assert (
        _socialblade_profile_url("instagram", "lisabarlow14") == "https://socialblade.com/instagram/user/lisabarlow14"
    )
    assert _socialblade_profile_url("facebook", "bravotv") == "https://socialblade.com/facebook/user/bravotv"
    assert _socialblade_profile_url("youtube", "facebookapp") == "https://socialblade.com/youtube/handle/facebookapp"
    assert _socialblade_profile_url("youtube", "UCabc123") == "https://socialblade.com/youtube/channel/UCabc123"


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
