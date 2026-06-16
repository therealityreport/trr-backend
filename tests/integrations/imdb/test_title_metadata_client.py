from __future__ import annotations

from dataclasses import dataclass

from trr_backend.integrations.imdb import title_metadata_client as mod


@dataclass
class _Response:
    status_code: int
    text: str = ""
    url: str = "https://www.imdb.com/title/tt6645582/episodes/?season=0"


class _BlockedSession:
    def get(self, url: str, **kwargs):  # noqa: ANN001
        season = (kwargs.get("params") or {}).get("season")
        suffix = f"?season={season}" if season is not None else ""
        return _Response(status_code=403, text="blocked", url=f"{url}{suffix}")


def test_fetch_episodes_page_uses_scrapling_on_blocked_imdb_response(monkeypatch) -> None:
    fallback_html = """
    <html>
      <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"contentData":{"section":{"episodes":{"items":[
          {"id":"tt9990001","season":0,"episode":1,"titleText":{"text":"Special"},"releaseDate":{"year":2024,"month":1,"day":1}}
        ]}}}}}}
      </script>
      <a href="/title/tt9990001/">Special</a>
    </html>
    """
    calls: list[str] = []

    def fake_scrapling(url: str, **_kwargs):  # noqa: ANN001
        calls.append(url)
        return fallback_html

    monkeypatch.setattr(mod, "_fetch_episodes_page_via_scrapling", fake_scrapling)

    client = mod.HttpImdbTitleMetadataClient(session=_BlockedSession())
    html = client.fetch_episodes_page("tt6645582", season=0)

    assert html == fallback_html
    assert calls == ["https://www.imdb.com/title/tt6645582/episodes/?season=0"]
    parsed = mod.parse_imdb_season_episodes_page(html, season=0)
    assert parsed[0].imdb_episode_id == "tt9990001"
    assert parsed[0].season == 0
