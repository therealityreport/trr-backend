from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pytest

from trr_backend.socials.socialblade.fetcher import SocialBladeScraplingFetcher


@dataclass(slots=True)
class _DummyProxyConfig:
    browser_proxy: str | dict[str, str] | None = None
    proxy_rotator: object | None = None
    api_proxy_url: str | None = None
    fingerprint: str = "proxy.example:8080:explicit"
    session_mode: str = "explicit"


class _DummyResponse:
    def __init__(
        self,
        *,
        text: str,
        status_code: int = 200,
        cookies: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        url: str = "https://socialblade.com/instagram/user/thetraitorsus",
        captured_xhr: list[object] | None = None,
    ) -> None:
        self.text = text
        self.status_code = status_code
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.url = url
        self.captured_xhr = captured_xhr or []


def _trpc_payload(result: object) -> str:
    return json.dumps({"result": {"data": {"json": result}}})


def _trpc_batch_payload(*results: object) -> str:
    return json.dumps([{"result": {"data": {"json": item}}} for item in results])


SOCIALBLADE_HTML = """
<html>
  <body>
    <div>Followers</div>
    <div>475,444</div>
    <div>Following</div>
    <div>7,090</div>
    <div>Media Count</div>
    <div>1,703</div>
    <div>Engagement Rate</div>
    <div>3.02%</div>
    <div>Average Likes</div>
    <div>13,894.63</div>
    <div>Average Comments</div>
    <div>456.75</div>
    <div>B+</div>
    <div>Grade</div>
    <div>38,982nd</div>
    <div>SB Rank</div>
    <div>139,823rd</div>
    <div>Followers Rank</div>
    <div>45,085th</div>
    <div>Engagement Rate Rank</div>
    <div>Daily Channel Metrics</div>
    <div>Last 60 Days</div>
    <table>
      <tr>
        <th>Date</th>
        <th>Followers Delta</th>
        <th>Followers Total</th>
        <th>Following Delta</th>
        <th>Following Total</th>
        <th>Media Count Delta</th>
        <th>Media Count Total</th>
      </tr>
      <tr>
        <td>Thu2026-03-05</td>
        <td>40</td>
        <td>473,873</td>
        <td>-2</td>
        <td>7,072</td>
        <td>1</td>
        <td>1,699</td>
      </tr>
      <tr>
        <td>Fri2026-03-06</td>
        <td>145</td>
        <td>474,018</td>
        <td>-1</td>
        <td>7,071</td>
        <td>1</td>
        <td>1,700</td>
      </tr>
    </table>
  </body>
</html>
"""


def _build_fetcher(*, platform: str = "instagram") -> SocialBladeScraplingFetcher:
    return SocialBladeScraplingFetcher(
        cookies=[{"name": "cf_clearance", "value": "seed", "domain": ".socialblade.com"}],
        raw_cookies={"cf_clearance": "seed"},
        platform=platform,
    )


def test_scrapling_fetcher_builds_browser_fetcher_through_shared_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_fetcher = object()
    captured: dict[str, bool] = {}

    def fake_build_stealthy_fetcher() -> object:
        captured["called"] = True
        return fake_fetcher

    monkeypatch.setattr(
        "trr_backend.socials.socialblade.fetcher.build_stealthy_fetcher",
        fake_build_stealthy_fetcher,
    )

    fetcher = _build_fetcher(platform="instagram")

    assert captured["called"] is True
    assert fetcher._fetcher is fake_fetcher


def test_scrapling_fetcher_uses_direct_instagram_user_url_without_raw_init_script(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="instagram")
    captured: dict[str, Any] = {}

    async def fake_async_fetch(url: str, **kwargs: object):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _DummyResponse(text=SOCIALBLADE_HTML, cookies={"sbid": "warm-cookie"})

    monkeypatch.setattr(fetcher._fetcher, "async_fetch", fake_async_fetch)

    response = __import__("asyncio").run(fetcher._fetch_page("https://socialblade.com/instagram/user/thetraitorsus"))

    assert response.cookies["sbid"] == "warm-cookie"
    assert captured["url"] == "https://socialblade.com/instagram/user/thetraitorsus"
    assert "init_script" not in captured["kwargs"]
    assert callable(captured["kwargs"]["page_action"])
    assert captured["kwargs"]["capture_xhr"] == r"/api/trpc/"
    assert captured["kwargs"]["locale"] == "en-US"
    assert captured["kwargs"]["wait"] == 2_000


def test_scrapling_fetcher_passes_proxy_to_browser_and_http_client(monkeypatch: pytest.MonkeyPatch) -> None:
    browser_proxy = {"server": "http://proxy.example:8080", "username": "user", "password": "pass"}
    fetcher = SocialBladeScraplingFetcher(
        cookies=[{"name": "cf_clearance", "value": "seed", "domain": ".socialblade.com"}],
        raw_cookies={"cf_clearance": "seed"},
        platform="instagram",
        proxy_config=_DummyProxyConfig(browser_proxy=browser_proxy, api_proxy_url="http://proxy.example:8080"),
    )
    captured: dict[str, Any] = {}

    async def fake_async_fetch(url: str, **kwargs: object):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _DummyResponse(text=SOCIALBLADE_HTML, cookies={"sbid": "warm-cookie"})

    class _FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            captured["httpx_kwargs"] = kwargs

    monkeypatch.setattr(fetcher._fetcher, "async_fetch", fake_async_fetch)
    monkeypatch.setattr("trr_backend.socials.socialblade.fetcher.httpx.AsyncClient", _FakeAsyncClient)

    __import__("asyncio").run(fetcher._fetch_page("https://socialblade.com/instagram/user/thetraitorsus"))
    fetcher._rebuild_http_client()

    assert captured["kwargs"]["proxy"] == browser_proxy
    assert "proxy_rotator" not in captured["kwargs"]
    assert captured["httpx_kwargs"]["proxy"] == "http://proxy.example:8080"
    assert fetcher.runtime_metadata["selected_proxy_fingerprint"] == "proxy.example:8080:explicit"
    assert fetcher.runtime_metadata["proxy_session_mode"] == "explicit"


def test_scrapling_fetcher_uses_tiktok_user_url_with_trpc_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = _build_fetcher(platform="tiktok")
    captured: dict[str, Any] = {}

    async def fake_async_fetch(url: str, **kwargs: object):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _DummyResponse(text=SOCIALBLADE_HTML, cookies={"sbid": "warm-cookie"})

    monkeypatch.setattr(fetcher._fetcher, "async_fetch", fake_async_fetch)

    response = __import__("asyncio").run(fetcher._fetch_page("https://socialblade.com/tiktok/user/bravotv"))

    assert response.cookies["sbid"] == "warm-cookie"
    assert captured["url"] == "https://socialblade.com/tiktok/user/bravotv"
    assert captured["kwargs"]["load_dom"] is True
    assert callable(captured["kwargs"]["page_action"])


def test_scrapling_fetcher_extracts_browser_captured_sixty_day_history_and_daily_chart() -> None:
    history_rows = [
        {"date": "2026-02-24T00:00:00.000Z", "followers": 137168, "following": 27, "media_count": 309},
        {"date": "2026-02-25T00:00:00.000Z", "followers": 137943, "following": 27, "media_count": 312},
    ]
    daily_deltas = [
        {"date": "2026-02-24T00:00:00.000Z", "followers": 775},
        {"date": "2026-02-25T00:00:00.000Z", "followers": 98},
    ]
    daily_total_rows = [
        {"date": "2026-02-24T00:00:00.000Z", "followers": 137168},
        {"date": "2026-02-25T00:00:00.000Z", "followers": 137943},
    ]
    capture = {
        "user": {"id": "creator-1", "followers": "137943"},
        "responses": {
            "history60": {
                "status": 200,
                "text": _trpc_batch_payload(
                    {
                        "id": "creator-1",
                        "followers": "137943",
                        "following": 27,
                        "media_count": 312,
                        "engagement_rate": 2.62,
                        "average_likes": 1234,
                        "average_comments": 56,
                        "grade": "B+",
                        "ranks": {"sb": 39828, "followers": 318818, "engagement_rate": 46796},
                    },
                    history_rows,
                ),
            },
            "dailyDeltas": {
                "status": 200,
                "text": _trpc_batch_payload(daily_deltas),
            },
            "dailyTotalChart": {
                "status": 200,
                "text": _trpc_batch_payload(daily_total_rows),
            },
        },
    }
    html = f"""
    <html>
      <body>
        <script id="trr-socialblade-capture" type="application/json">{json.dumps(capture)}</script>
      </body>
    </html>
    """

    payload = SocialBladeScraplingFetcher._extract_captured_instagram_payload(html)

    assert payload["user"]["id"] == "creator-1"
    assert payload["history_rows"] == history_rows
    assert payload["daily_deltas"] == daily_deltas
    assert payload["daily_total_rows"] == daily_total_rows


def test_scrapling_fetcher_prefers_browser_captured_history_over_http_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="instagram")
    history_rows = [
        {
            "date": f"{date(2026, 1, 1) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 137168 + index,
            "following": 27,
            "media_count": 309 + index,
        }
        for index in range(60)
    ]
    daily_deltas = [
        {"date": "2026-02-24T00:00:00.000Z", "followers": 1},
        {"date": "2026-03-01T00:00:00.000Z", "followers": 1},
    ]
    daily_total_rows = [
        {
            "date": f"{date(2026, 1, 1) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 137_000 + index,
        }
        for index in range(75)
    ]
    user_payload = {
        "id": "creator-1",
        "followers": "137943",
        "following": 27,
        "media_count": 312,
        "engagement_rate": 2.62,
        "average_likes": 1234,
        "average_comments": 56,
        "grade": "B+",
        "ranks": {"sb": 39828, "followers": 318818, "engagement_rate": 46796},
    }
    capture = {
        "user": user_payload,
        "responses": {
            "history60": {"status": 200, "text": _trpc_batch_payload(user_payload, history_rows)},
            "dailyDeltas": {"status": 200, "text": _trpc_batch_payload(daily_deltas)},
            "dailyTotalChart": {"status": 200, "text": _trpc_batch_payload(daily_total_rows)},
        },
    }
    html = SOCIALBLADE_HTML.replace(
        "</body>",
        f'<script id="trr-socialblade-capture" type="application/json">{json.dumps(capture)}</script></body>',
    )

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=html, cookies={"sbid": "warm-cookie"})

    async def fake_fetch_http(*_args, **_kwargs):
        raise AssertionError("http search should not run when browser capture succeeded")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_fetch_http", fake_fetch_http)

    payload = __import__("asyncio").run(fetcher.scrape("thetraitorsus"))

    assert payload["history_source"] == "page_trpc_capture"
    assert payload["profile_stats"]["followers"] == 137943
    assert payload["daily_channel_metrics_60day"]["row_count"] == 60
    assert payload["daily_total_followers_chart"]["total_data_points"] == 75
    assert payload["daily_total_followers_chart"]["date_range"] == {"from": "2026-01-01", "to": "2026-03-16"}
    assert payload["runtime_metadata"]["fallback_chain"] == ["scrapling_warmup", "instagram_page_trpc_capture"]
    assert payload["runtime_metadata"]["capture_source"] == "html_script"
    assert payload["runtime_metadata"]["history_source_detail"] == "html_script"
    assert payload["runtime_metadata"]["profile_source"] == "html_script"


def test_scrapling_fetcher_prefers_captured_xhr_history_when_html_capture_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="instagram")
    history_rows = [
        {
            "date": f"{date(2026, 1, 1) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 137_168 + index,
            "following": 27,
            "media_count": 309 + index,
        }
        for index in range(60)
    ]
    daily_total_rows = [
        {
            "date": f"{date(2026, 1, 1) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 137_000 + index,
        }
        for index in range(75)
    ]
    user_payload = {
        "id": "creator-1",
        "followers": "137943",
        "following": 27,
        "media_count": 312,
        "engagement_rate": 2.62,
        "average_likes": 1234,
        "average_comments": 56,
        "grade": "B+",
        "ranks": {"sb": 39828, "followers": 318818, "engagement_rate": 46796},
    }
    xhr_responses: list[object] = [
        _DummyResponse(
            text=_trpc_batch_payload(user_payload, history_rows),
            url="https://socialblade.com/api/trpc/instagram.user,instagram.history?batch=1&input=%7B%7D",
        ),
        _DummyResponse(
            text=_trpc_batch_payload(daily_total_rows),
            url=(
                "https://socialblade.com/api/trpc/instagram.monthly?batch=1&input="
                "%7B%220%22%3A%7B%22json%22%3A%7B%22id%22%3A%22creator-1%22%2C"
                "%22limit%22%3A1096%2C%22period%22%3A%22daily%22%2C%22type%22%3A%22total%22%7D%7D%7D"
            ),
        ),
    ]

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=SOCIALBLADE_HTML, cookies={"sbid": "warm-cookie"}, captured_xhr=xhr_responses)

    async def fake_fetch_http(*_args, **_kwargs):
        raise AssertionError("http search should not run when captured XHR has full history")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_fetch_http", fake_fetch_http)

    payload = __import__("asyncio").run(fetcher.scrape("thetraitorsus"))

    assert payload["history_source"] == "page_trpc_capture"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 60
    assert payload["daily_total_followers_chart"]["total_data_points"] == 75
    assert payload["runtime_metadata"]["capture_source"] == "scrapling_xhr"
    assert payload["runtime_metadata"]["history_source_detail"] == "scrapling_xhr"
    assert payload["runtime_metadata"]["profile_source"] == "scrapling_xhr"
    assert payload["runtime_metadata"]["captured_xhr_count"] == 2
    assert payload["runtime_metadata"]["captured_xhr_paths"] == [
        "/api/trpc/instagram.user,instagram.history",
        "/api/trpc/instagram.monthly",
    ]


def test_scrapling_fetcher_prefers_tiktok_page_capture_and_labels_likes_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="tiktok")
    history_rows = [
        {
            "date": f"{date(2026, 3, 15) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 4_000_000 + index,
            "following": 432,
            "likes": 122_000_000 + (index * 10),
        }
        for index in range(60)
    ]
    user_payload = {
        "id": "tiktok-creator-1",
        "followers": "4.1M",
        "following": 432,
        "likes": "122.6M",
        "engagement_rate": 4.12,
        "average_likes": 12345,
        "average_comments": 678,
        "grade": "A-",
        "ranks": {"sb": 120, "followers": 650, "engagement_rate": 88},
    }
    capture = {
        "user": user_payload,
        "responses": {
            "history60": {"status": 200, "text": _trpc_batch_payload(user_payload, history_rows)},
            "dailyDeltas": {"status": 200, "text": _trpc_batch_payload([])},
        },
    }
    html = SOCIALBLADE_HTML.replace(
        "</body>",
        f'<script id="trr-socialblade-capture" type="application/json">{json.dumps(capture)}</script></body>',
    )

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=html, cookies={"sbid": "warm-cookie"})

    async def fake_fetch_http(*_args, **_kwargs):
        raise AssertionError("http search should not run when browser capture succeeded")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_fetch_http", fake_fetch_http)

    payload = __import__("asyncio").run(fetcher.scrape("bravotv"))

    assert payload["platform"] == "tiktok"
    assert payload["socialblade_url"] == "https://socialblade.com/tiktok/user/bravotv"
    assert payload["profile_stats"]["media_count"] == 122600000
    assert "Likes Total" in payload["daily_channel_metrics_60day"]["headers"]
    assert payload["runtime_metadata"]["fallback_chain"] == ["scrapling_warmup", "tiktok_page_trpc_capture"]


def test_scrapling_fetcher_tiktok_keeps_full_daily_total_chart_when_history_is_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="tiktok")
    history_rows = [
        {
            "date": f"{date(2026, 3, 19) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 140_900 + (index * 25),
            "following": 2,
            "likes": 8_700_000,
            "videos": 255 + index,
        }
        for index in range(56)
    ]
    daily_total_rows = [
        {
            "date": f"{date(2026, 1, 11) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 40_237 + (index * 100),
            "following": 2,
            "likes": 8_700_000,
            "videos": 255,
        }
        for index in range(124)
    ]
    user_payload = {
        "id": "tiktok-creator-1",
        "followers": "142.3K",
        "following": 2,
        "likes": "8.8M",
        "engagement_rate": 4.12,
        "average_likes": 12345,
        "average_comments": 678,
        "grade": "B",
        "ranks": {"sb": 11322, "followers": 8978, "engagement_rate": 88},
    }
    capture = {
        "user": user_payload,
        "control_updates": {
            "last60Days": "selected",
            "daily": "already_selected",
            "total": "selected",
        },
        "responses": {
            "history60": {"status": 200, "text": _trpc_batch_payload(user_payload, history_rows)},
            "dailyTotalChart": {"status": 200, "text": _trpc_batch_payload(daily_total_rows)},
        },
    }
    html = SOCIALBLADE_HTML.replace(
        "</body>",
        f'<script id="trr-socialblade-capture" type="application/json">{json.dumps(capture)}</script></body>',
    )

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=html, cookies={"sbid": "warm-cookie"})

    async def fake_scrape_authenticated_api(*_args, **_kwargs):
        raise AssertionError("long daily total chart capture should avoid shorter authenticated retry")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_scrape_authenticated_api", fake_scrape_authenticated_api)

    payload = __import__("asyncio").run(fetcher.scrape("thetraitorsus"))

    assert payload["history_source"] == "page_trpc_capture"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 56
    assert payload["daily_total_followers_chart"]["total_data_points"] == 124
    assert payload["daily_total_followers_chart"]["date_range"] == {"from": "2026-01-11", "to": "2026-05-14"}
    assert payload["daily_total_followers_chart"]["data"][0] == {"date": "2026-01-11", "followers": 40237}
    assert payload["runtime_metadata"]["fallback_chain"] == ["scrapling_warmup", "tiktok_page_trpc_capture"]
    assert payload["runtime_metadata"]["capture_control_updates"] == {
        "last60Days": "selected",
        "daily": "already_selected",
        "total": "selected",
    }


def test_scrapling_fetcher_tries_authenticated_api_when_page_capture_is_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="instagram")
    short_history_rows = [
        {
            "date": f"{date(2026, 4, 1) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 172_000 + index,
            "following": 27,
            "media_count": 427 + index,
        }
        for index in range(31)
    ]
    user_payload = {
        "id": "creator-1",
        "followers": "172031",
        "following": 27,
        "media_count": 458,
        "engagement_rate": 2.62,
        "average_likes": 1234,
        "average_comments": 56,
        "grade": "B+",
        "ranks": {"sb": 39828, "followers": 318818, "engagement_rate": 46796},
    }
    capture = {
        "user": user_payload,
        "responses": {
            "history60": {"status": 200, "text": _trpc_batch_payload(user_payload, short_history_rows)},
            "dailyDeltas": {"status": 200, "text": _trpc_batch_payload([])},
        },
    }
    html = SOCIALBLADE_HTML.replace(
        "</body>",
        f'<script id="trr-socialblade-capture" type="application/json">{json.dumps(capture)}</script></body>',
    )

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=html, cookies={"sbid": "warm-cookie"})

    async def fake_scrape_authenticated_api(_handle: str, _referer: str):
        metrics = {
            "period": "Last 60 Days",
            "row_count": 60,
            "headers": ["Date", "Followers Total"],
            "data": [{"Date": "2026-05-13", "Followers Total": "172,666"}],
        }
        chart = {
            "frequency": "daily",
            "metric": "total_followers",
            "total_data_points": 60,
            "date_range": {"from": "2026-03-15", "to": "2026-05-13"},
            "data": [{"date": "2026-05-13", "followers": 172666}],
        }
        return (
            {"followers": 172666, "following": 27, "media_count": 442},
            {"grade": "B+", "sb_rank": "41,058th", "followers_rank": "316,473rd"},
            metrics,
            chart,
        )

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_scrape_authenticated_api", fake_scrape_authenticated_api)

    payload = __import__("asyncio").run(fetcher.scrape("thetraitorsus"))

    assert payload["history_source"] == "authenticated_api"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 60
    assert payload["runtime_metadata"]["history_source_detail"] == "authenticated_api"
    assert payload["runtime_metadata"]["profile_source"] == "authenticated_api"
    assert payload["runtime_metadata"]["fallback_chain"] == [
        "scrapling_warmup",
        "instagram_page_trpc_capture_short",
        "instagram_trpc_http",
    ]


def test_scrapling_fetcher_labels_short_page_capture_as_degraded_when_auth_api_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="instagram")
    short_history_rows = [
        {
            "date": f"{date(2026, 4, 1) + timedelta(days=index)}T00:00:00.000Z",
            "followers": 172_000 + index,
            "following": 27,
            "media_count": 427 + index,
        }
        for index in range(31)
    ]
    user_payload = {
        "id": "creator-1",
        "followers": "172031",
        "following": 27,
        "media_count": 458,
        "engagement_rate": 2.62,
        "average_likes": 1234,
        "average_comments": 56,
        "grade": "B+",
        "ranks": {"sb": 39828, "followers": 318818, "engagement_rate": 46796},
    }
    capture = {
        "user": user_payload,
        "responses": {
            "history60": {"status": 200, "text": _trpc_batch_payload(user_payload, short_history_rows)},
            "dailyDeltas": {"status": 200, "text": _trpc_batch_payload([])},
        },
    }
    html = SOCIALBLADE_HTML.replace(
        "</body>",
        f'<script id="trr-socialblade-capture" type="application/json">{json.dumps(capture)}</script></body>',
    )

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=html, cookies={"sbid": "warm-cookie"})

    async def fake_scrape_authenticated_api(_handle: str, _referer: str):
        raise RuntimeError("authenticated API challenged")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_scrape_authenticated_api", fake_scrape_authenticated_api)

    payload = __import__("asyncio").run(fetcher.scrape("thetraitorsus"))

    assert payload["history_source"] == "page_trpc_capture_short"
    assert payload["daily_channel_metrics_60day"]["row_count"] == 31
    assert payload["runtime_metadata"]["fallback_chain"] == ["scrapling_warmup", "instagram_page_trpc_capture_short"]


def test_scrapling_fetcher_expands_socialblade_metric_table_columns() -> None:
    table = SocialBladeScraplingFetcher._extract_table_data(
        """
        <table>
          <tr><th>Date</th><th>followers</th><th>following</th><th>media count</th></tr>
          <tr><td>Sat 2026-04-11</td><td>78</td><td>171,945</td><td>--</td><td>27</td><td>1</td><td>427</td></tr>
        </table>
        """
    )

    assert table == {
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
                "Date": "Sat 2026-04-11",
                "Followers Delta": "78",
                "Followers Total": "171,945",
                "Following Delta": "--",
                "Following Total": "27",
                "Media Count Delta": "1",
                "Media Count Total": "427",
            }
        ],
    }


def test_scrapling_fetcher_ignores_non_date_tables_before_metrics_table() -> None:
    table = SocialBladeScraplingFetcher._extract_table_data(
        """
        <table>
          <tr><th>Name</th><th>Value</th></tr>
          <tr><td>Followers</td><td>172,666</td></tr>
        </table>
        <table>
          <tr>
            <th>Date</th>
            <th>Followers Delta</th>
            <th>Followers Total</th>
            <th>Following Delta</th>
            <th>Following Total</th>
          </tr>
          <tr>
            <td>Sun 2026-05-10</td>
            <td>10</td>
            <td>172,600</td>
            <td>0</td>
            <td>27</td>
          </tr>
        </table>
        """
    )

    assert table == {
        "headers": [
            "Date",
            "Followers Delta",
            "Followers Total",
            "Following Delta",
            "Following Total",
        ],
        "data": [
            {
                "Date": "Sun 2026-05-10",
                "Followers Delta": "10",
                "Followers Total": "172,600",
                "Following Delta": "0",
                "Following Total": "27",
            }
        ],
    }


def test_scrapling_fetcher_bridges_warmup_cookies_into_authenticated_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = _build_fetcher(platform="instagram")

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=SOCIALBLADE_HTML, cookies={"sbid": "warm-cookie"})

    async def fake_fetch_http(endpoint: str, *, referer: str):
        fetcher._request_count += 1
        assert referer == "https://socialblade.com/instagram/user/heathergay"
        assert fetcher._raw_cookies["sbid"] == "warm-cookie"
        if endpoint.startswith("/api/trpc/instagram.search"):
            return _DummyResponse(text=_trpc_payload({"platformResult": {"id": "creator-1"}}))
        if endpoint.startswith("/api/trpc/instagram.user?"):
            return _DummyResponse(
                text=_trpc_payload(
                    {
                        "followers": "475444",
                        "following": 7090,
                        "media_count": 1703,
                        "engagement_rate": 3.02,
                        "average_likes": 13894.63,
                        "average_comments": 456.75,
                        "grade": "B+",
                        "ranks": {"sb": 38982, "followers": 139823, "engagement_rate": 45085},
                    }
                )
            )
        if endpoint.startswith("/api/trpc/instagram.user,instagram.history"):
            return _DummyResponse(
                text=_trpc_batch_payload(
                    {"id": "creator-1"},
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
                )
            )
        if endpoint.startswith("/api/trpc/instagram.monthly"):
            assert "%22period%22%3A%22daily%22" in endpoint
            assert "%22type%22%3A%22total%22" in endpoint
            assert "%22limit%22%3A1096" in endpoint
            return _DummyResponse(
                text=_trpc_batch_payload(
                    [
                        {"date": "2026-03-05T00:00:00.000Z", "followers": 145},
                        {"date": "2026-03-06T00:00:00.000Z", "followers": 1426},
                    ]
                )
            )
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_fetch_http", fake_fetch_http)

    payload = __import__("asyncio").run(fetcher.scrape("heathergay"))

    assert payload["history_source"] == "authenticated_api"
    assert payload["profile_stats"]["followers"] == 475444
    assert payload["daily_channel_metrics_60day"]["row_count"] == 2
    assert payload["runtime_metadata"]["history_source_detail"] == "authenticated_api"
    assert payload["runtime_metadata"]["profile_source"] == "authenticated_api"
    assert payload["runtime_metadata"]["fallback_chain"] == ["scrapling_warmup", "instagram_trpc_http"]
    assert payload["runtime_metadata"]["request_count"] == 5


def test_scrapling_fetcher_falls_back_to_html_table_when_authenticated_api_challenged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher = _build_fetcher(platform="instagram")

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=SOCIALBLADE_HTML, cookies={"sbid": "warm-cookie"})

    async def fake_fetch_http(endpoint: str, *, referer: str):
        del referer
        fetcher._request_count += 1
        if endpoint.startswith("/api/trpc/instagram.search"):
            return _DummyResponse(text=_trpc_payload({"platformResult": {"id": "creator-1"}}))
        if endpoint.startswith("/api/trpc/instagram.user?"):
            return _DummyResponse(
                text=_trpc_payload(
                    {
                        "followers": "475444",
                        "following": 7090,
                        "media_count": 1703,
                        "engagement_rate": 3.02,
                        "average_likes": 13894.63,
                        "average_comments": 456.75,
                        "grade": "B+",
                        "ranks": {"sb": 38982, "followers": 139823, "engagement_rate": 45085},
                    }
                )
            )
        if endpoint.startswith("/api/trpc/instagram.user,instagram.history"):
            return _DummyResponse(
                text=_trpc_batch_payload(
                    {"id": "creator-1"},
                    [
                        {
                            "date": "2026-03-05T00:00:00.000Z",
                            "followers": 473873,
                            "following": 7072,
                            "media_count": 1699,
                        }
                    ],
                )
            )
        if endpoint.startswith("/api/trpc/instagram.monthly"):
            return _DummyResponse(text="{}", status_code=412)
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(fetcher, "_fetch_http", fake_fetch_http)

    payload = __import__("asyncio").run(fetcher.scrape("heathergay"))

    assert payload["history_source"] == "table_fallback"
    assert payload["daily_total_followers_chart"]["total_data_points"] == 2
    assert payload["runtime_metadata"]["history_source_detail"] == "html_table_fallback"
    assert payload["runtime_metadata"]["profile_source"] == "html_body_fallback"
    assert payload["runtime_metadata"]["fallback_chain"][-1] == "html_table_fallback"


@pytest.mark.parametrize(
    ("platform", "expected_followers", "expected_metric_label"),
    [
        ("facebook", 475444, "Likes"),
        ("youtube", 475444, "Subscribers"),
    ],
)
def test_scrapling_fetcher_supports_non_instagram_html_fallback_platforms(
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
    expected_followers: int,
    expected_metric_label: str,
) -> None:
    fetcher = _build_fetcher(platform=platform)

    html = SOCIALBLADE_HTML.replace("Followers", "Likes" if platform == "facebook" else "Subscribers", 1)
    if platform == "facebook":
        html = html.replace("Following", "Talking About", 1)
        html = html.replace("Media Count", "Posts", 1)
        html = html.replace("Average Likes", "Average Reactions", 1)
        html = html.replace("Followers Delta", "Likes Delta")
        html = html.replace("Followers Total", "Likes Total")
    if platform == "youtube":
        html = html.replace("Followers", "Subscribers")
        html = html.replace("Following", "Video Views", 1)
        html = html.replace("Media Count", "Uploads", 1)
        html = html.replace("Average Likes", "Average Views", 1)
        html = html.replace("Followers Delta", "Subscribers Delta")
        html = html.replace("Followers Total", "Subscribers Total")

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=html, cookies={"sbid": "warm-cookie"})

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)

    payload = __import__("asyncio").run(fetcher.scrape("bravotv"))

    assert payload["history_source"] == "table_fallback"
    assert payload["profile_stats"]["followers"] == expected_followers
    assert payload["chart_metric_label"] == expected_metric_label
    assert payload["runtime_metadata"]["history_source_detail"] == "html_table_fallback"
    assert payload["runtime_metadata"]["profile_source"] == "html_body_fallback"


def test_scrapling_fetcher_labels_non_chart_table_as_table_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = _build_fetcher(platform="tiktok")
    html = SOCIALBLADE_HTML.replace("Media Count", "Likes").replace("Followers Total", "Followers Count")

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text=html, cookies={"sbid": "warm-cookie"})

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)

    payload = __import__("asyncio").run(fetcher.scrape("bravotv"))

    assert payload["history_source"] == "table_fallback"
    assert payload["daily_total_followers_chart"] is None
    assert payload["daily_channel_metrics_60day"]["row_count"] == 2


def test_scrapling_fetcher_raises_on_access_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = _build_fetcher(platform="instagram")

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text="<html><body>Access denied. Error reference number: 1020</body></html>")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)

    with pytest.raises(RuntimeError, match="Cloudflare"):
        __import__("asyncio").run(fetcher.scrape("heathergay"))
