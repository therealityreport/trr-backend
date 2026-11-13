"""Standalone Modal auth probes must compose providers before using pipelines."""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


@pytest.mark.parametrize("lane", ["posts", "comments"])
@pytest.mark.parametrize("blocked", [False, True])
def test_auth_probe_cold_start_with_fixture_session(lane, blocked):
    script = textwrap.dedent(
        """
        import importlib
        import json
        import socket
        import sys
        from types import SimpleNamespace

        def deny_external_access(*args, **kwargs):
            raise AssertionError("Cold auth fixture attempted external access")

        socket.create_connection = deny_external_access
        socket.socket.connect = deny_external_access
        socket.socket.connect_ex = deny_external_access
        from trr_backend import modal_jobs
        from trr_backend.db import pg
        for name in ("fetch_one", "fetch_all", "execute", "db_connection", "db_cursor"):
            if hasattr(pg, name):
                setattr(pg, name, deny_external_access)
        assert "trr_backend.socials.social_season_analytics_impl" not in sys.modules

        lane, blocked = sys.argv[1], sys.argv[2] == "True"
        package = "trr_backend.socials.instagram." + lane + "_scrapling."
        fetcher_module = importlib.import_module(package + "fetcher")
        proxy_module = importlib.import_module(package + "proxy")
        session_module = importlib.import_module(package + "session")
        cookies = {"sessionid": "fixture", "csrftoken": "fixture", "ds_user_id": "1"}
        original_cookies = dict(cookies)
        session = SimpleNamespace(
            cookies=[], browser_account_id="fixture",
            auth_session=SimpleNamespace(cookies=cookies, source="fixture", validated=True),
        )
        setattr(session_module, "resolve_" + lane + "_scrapling_session", lambda **kwargs: session)
        setattr(proxy_module, "select_" + lane + "_proxy", lambda **kwargs: None)
        calls = []

        class FixtureAuthBlocked(RuntimeError):
            error_code = "instagram_" + lane + "_warmup_auth_failed"

        class FakeFetcher:
            def __init__(self, **kwargs):
                self.runtime_metadata = {}
            async def warmup(self, *args):
                calls.append("warmup")
                if blocked:
                    raise FixtureAuthBlocked("fixture source restriction")
            async def validate_comments_endpoint(self, *args, **kwargs):
                calls.append("endpoint")
                return {"status": "valid", "result": "valid"}
            async def fetch_posts_page(self, *args, **kwargs):
                calls.append("endpoint")
                return SimpleNamespace(
                    auth_failed=False, fetch_failed=False, fetch_reason=None,
                    retryable=False, request_count=1, posts=[], has_next_page=False,
                )
            async def aclose(self):
                calls.append("close")

        setattr(fetcher_module, "Instagram" + lane.title() + "ScraplingFetcher", FakeFetcher)
        if lane == "comments":
            result = modal_jobs.probe_instagram_comments_auth.local("bravotv", "Det5B7akvkK", True)
        else:
            result = modal_jobs.probe_instagram_posts_auth.local("bravotv")
        assert result["ready"] is (not blocked), result
        assert result["status"] == ("auth_blocked" if blocked else "valid"), result
        assert result.get("exception_class") != "NameError", result
        assert cookies == original_cookies
        assert calls == (["warmup", "close"] if blocked else ["warmup", "endpoint", "close"]), calls
        print(json.dumps({"lane": lane, "status": result["status"], "source_requests": 0}))
        """
    )
    env = {
        **os.environ,
        "TRR_TEST_DISABLE_DOTENV": "1",
        "TRR_MODAL_ENABLED": "0",
        "APP_ENV": "test",
        "SOCIAL_INSTAGRAM_SCRAPE_MODE": "authenticated",
    }
    completed = subprocess.run(
        [sys.executable, "-c", script, lane, str(blocked)],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert '"source_requests": 0' in completed.stdout
