from __future__ import annotations

import json

import pytest

from trr_backend.socials.socialblade.fetcher import SocialBladeScraplingFetcher


class _DummyResponse:
    def __init__(
        self,
        *,
        text: str,
        status_code: int = 200,
        cookies: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.text = text
        self.status_code = status_code
        self.cookies = cookies or {}
        self.headers = headers or {}


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


def test_scrapling_fetcher_raises_on_access_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = _build_fetcher(platform="instagram")

    async def fake_fetch_page(_url: str):
        fetcher._request_count += 1
        return _DummyResponse(text="<html><body>Access denied. Error reference number: 1020</body></html>")

    monkeypatch.setattr(fetcher, "_fetch_page", fake_fetch_page)

    with pytest.raises(RuntimeError, match="Cloudflare"):
        __import__("asyncio").run(fetcher.scrape("heathergay"))
