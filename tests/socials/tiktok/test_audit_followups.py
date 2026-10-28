"""Regression coverage for the post-extraction TikTok audit fixes."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper, media_resolver
from trr_backend.socials.tiktok.transports import ytdlp_auth


@pytest.fixture(autouse=True)
def _isolated_auth(monkeypatch):
    from trr_backend.socials import media_url_safety

    monkeypatch.delenv("SOCIAL_TIKTOK_YTDLP_PROXY_URL", raising=False)
    monkeypatch.delenv("SOCIAL_TIKTOK_YTDLP_USE_CANONICAL_COOKIES", raising=False)
    monkeypatch.setattr(media_resolver, "_find_ytdlp_cookie_file", lambda: None)
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: "/test/yt-dlp")
    monkeypatch.setattr(media_url_safety.socket, "getaddrinfo", lambda *_a, **_k: [(2, 1, 6, "", ("8.8.8.8", 443))])


@pytest.mark.parametrize("caller", ["resolver", "metadata", "profile", "bulk"])
@pytest.mark.parametrize("outcome", ["success", "timeout", "nonzero", "malformed"])
def test_ytdlp_callers_share_auth_and_cleanup(monkeypatch, caplog, caller, outcome):
    monkeypatch.setenv("SOCIAL_TIKTOK_YTDLP_PROXY_URL", "http://proxy-user:proxy-secret@proxy.example:7000")
    scraper = TikTokScraper(cookies={"sessionid": "cookie-secret"})
    monkeypatch.setattr(scraper, "_find_ytdlp_cookie_file", lambda: None)
    seen = {}

    def run(cmd, **_kwargs):
        path = Path(cmd[cmd.index("--cookies") + 1])
        seen["cookie_path"] = path
        assert path.stat().st_mode & 0o777 == 0o600
        assert "\tsessionid\tcookie-secret" in path.read_text()
        assert cmd[cmd.index("--proxy") + 1] == "http://proxy-user:proxy-secret@proxy.example:7000"
        if outcome == "timeout":
            raise subprocess.TimeoutExpired(cmd, 1, stderr="cookie-secret proxy-secret")
        stdout = json.dumps({"id": "12345", "url": "https://video.test/media.mp4"})
        if outcome == "malformed":
            stdout = "not-json cookie-secret proxy-secret"
        return SimpleNamespace(
            returncode=1 if outcome == "nonzero" else 0, stdout=stdout, stderr="cookie-secret proxy-secret"
        )

    monkeypatch.setattr(media_resolver.subprocess, "run", run)
    diagnostics = {}
    if caller == "resolver":
        _, _, diagnostics, _ = media_resolver._resolve_with_ytdlp(
            "https://www.tiktok.com/@bravotv/video/12345", cookies=scraper.cookies
        )
        assert diagnostics["success"] is (outcome == "success")
        assert diagnostics["ytdlp_cookie_source"] == "canonical_cookies"
        assert diagnostics["ytdlp_proxy_label"] == "proxy.example"
    elif caller == "metadata":
        result = scraper._ytdlp_get_video_metadata("https://www.tiktok.com/@bravotv/video/12345")
        assert bool(result) is (outcome == "success")
    elif caller == "profile":
        scraper._ytdlp_list_videos("bravotv", max_videos=1)
    else:
        scraper._scrape_via_ytdlp(TikTokScrapeConfig(username="bravotv", ytdlp_max_videos_hint=1))
        diagnostics = scraper.last_retrieval_meta
    assert not seen["cookie_path"].exists()
    for secret in ("cookie-secret", "proxy-secret", "proxy-user"):
        assert secret not in caplog.text
        assert secret not in str(diagnostics)


@pytest.mark.parametrize("canonical_enabled", ["0", "1"])
def test_resolver_preserves_operator_cookie_file(monkeypatch, tmp_path, canonical_enabled):
    path = tmp_path / "operator.txt"
    path.write_text("# Netscape HTTP Cookie File\n")
    monkeypatch.setenv("SOCIAL_TIKTOK_YTDLP_USE_CANONICAL_COOKIES", canonical_enabled)
    monkeypatch.setattr(media_resolver, "_find_ytdlp_cookie_file", lambda: str(path))

    def run(cmd, **_kwargs):
        assert cmd[cmd.index("--cookies") + 1] == str(path)
        assert "--proxy" not in cmd
        return SimpleNamespace(returncode=0, stdout='{"url":"https://video.test/media.mp4"}', stderr="")

    monkeypatch.setattr(media_resolver.subprocess, "run", run)
    _, _, attempt, _ = media_resolver._resolve_with_ytdlp(
        "https://www.tiktok.com/@bravotv/video/12345", cookies={"sessionid": "ignored"}
    )
    assert attempt["ytdlp_cookie_source"] == "netscape_file"
    assert path.exists()


@pytest.mark.parametrize("cookies,enabled", [({}, "1"), ({"sessionid": "secret"}, "0")])
def test_missing_or_disabled_canonical_cookies_keep_direct_route(monkeypatch, cookies, enabled):
    monkeypatch.setenv("SOCIAL_TIKTOK_YTDLP_USE_CANONICAL_COOKIES", enabled)
    args, meta, temp = ytdlp_auth.build_ytdlp_auth_args(cookies, cookie_file=None)
    assert args == []
    assert temp is None
    assert meta["ytdlp_cookie_source"] is None
    assert meta["ytdlp_proxy_enabled"] is False


def test_cookie_file_rejects_injected_rows():
    args, meta, path = ytdlp_auth.build_ytdlp_auth_args(
        {"sessionid": "safe", "bad\tname": "secret", "other": "secret\ninjected", "empty": ""}, cookie_file=None
    )
    try:
        text = Path(path).read_text()  # pyright: ignore[reportArgumentType]
        assert len(text.splitlines()) == 2
        assert "secret" not in text
        assert args == ["--cookies", path]
        assert meta["ytdlp_cookie_source"] == "canonical_cookies"
    finally:
        ytdlp_auth.remove_temporary_cookie_file(path)


def test_ytdlp_dates_are_utc_in_every_process_timezone():
    script = """
import time
time.tzset()
from trr_backend.socials.tiktok import TikTokScraper, TikTokScrapeConfig
post = TikTokScraper(cookies={})._parse_ytdlp_metadata(
    {'id': '1', 'timestamp': 1704069000}, TikTokScrapeConfig(username='bravotv')
)
assert post.create_time == 1704069000
print(post.date_time)
"""
    values = []
    for timezone in ("UTC", "America/New_York"):
        result = subprocess.run(
            [sys.executable, "-c", script], env={**os.environ, "TZ": timezone}, capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
        values.append(result.stdout.strip())
    assert values == ["2024-01-01 00:30:00"] * 2


def test_sound_fetch_uses_canonical_user_agent(monkeypatch):
    import trr_backend.repositories.social_season_analytics as repo
    from trr_backend.socials.tiktok.constants import TIKTOK_USER_AGENT
    from trr_backend.socials.tiktok.parsing import sounds

    captured = {}

    def get(url, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(ok=False)

    monkeypatch.setattr(sounds.requests, "get", get)
    repo._fetch_tiktok_sound_snapshot("123456789")  # pyright: ignore[reportAttributeAccessIssue]
    assert captured["headers"]["user-agent"] == TIKTOK_USER_AGENT


@pytest.mark.parametrize("runtime_override", [{}, {"sessionid": "runtime-cookie"}])
def test_backend_media_composition_uses_available_cookies_without_refresh(monkeypatch, runtime_override):
    import trr_backend.repositories.social_season_analytics as repo
    import trr_backend.socials.tiktok as tiktok

    captured = {}
    monkeypatch.setattr(repo, "_tiktok_cookie_runtime_override", runtime_override)
    monkeypatch.setattr(repo, "_load_tiktok_cookies_from_sources", lambda: {"sessionid": "source-cookie"})
    monkeypatch.setattr(repo, "_load_tiktok_cookies", lambda: pytest.fail("media resolution must not refresh cookies"))

    def resolve(video_id, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(source=None, media_urls=[], thumbnail_url=None, attempts=[])

    monkeypatch.setattr(tiktok, "resolve_tiktok_media", resolve)
    repo._resolve_tiktok_media_for_video_id(video_id="12345")  # pyright: ignore[reportAttributeAccessIssue]
    assert captured["cookies"] == (runtime_override or {"sessionid": "source-cookie"})
    repo._resolve_tiktok_media_for_video_id(video_id="12345", allow_ytdlp=False)  # pyright: ignore[reportAttributeAccessIssue]
    assert captured["cookies"] == {}
