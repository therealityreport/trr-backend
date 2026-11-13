import base64
import subprocess
import sys
import time

import pytest

from trr_backend.socials.twitter import public_mirror, public_search, public_thread, public_transport

URL = "https://lurkbird.com/search/NASA/?product=latest"


@pytest.mark.parametrize("behavior", ["success", "failure", "timeout"])
def test_owned_worker_cleanup_and_clean_environment(monkeypatch, behavior):
    original = subprocess.Popen
    children = []
    sentinel = original([sys.executable, "-c", "import time; time.sleep(20)"])
    script = {
        "success": 'import json; print(json.dumps({"body":"' + base64.b64encode(b"public").decode() + '"}))',
        "failure": "raise SystemExit(1)",
        "timeout": "import time; time.sleep(20)",
    }[behavior]

    def spawn(command, **kwargs):
        assert command[1] == "-I"
        assert set(kwargs["env"]) == {"PATH"}
        child = original([sys.executable, "-I", "-c", script], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(public_transport.subprocess, "Popen", spawn)
    try:
        if behavior == "success":
            assert public_transport.fetch(URL, deadline=time.monotonic() + 2) == b"public"
        else:
            with pytest.raises(ValueError, match="deadline" if behavior == "timeout" else "worker failed"):
                public_transport.fetch(URL, deadline=time.monotonic() + 0.15)
        assert children[0].poll() is not None
        assert sentinel.poll() is None
    finally:
        sentinel.terminate()
        sentinel.wait(timeout=2)


def test_expired_deadline_does_not_spawn(monkeypatch):
    monkeypatch.setattr(public_transport.subprocess, "Popen", lambda *a, **k: pytest.fail("no worker"))
    with pytest.raises(ValueError, match="deadline"):
        public_transport.fetch(URL, deadline=time.monotonic() - 1)


def test_search_retains_first_page_when_next_read_expires(monkeypatch):
    from pathlib import Path

    body = (Path(__file__).parents[1] / "fixtures/twitter/lurkbird-search.html").read_bytes()
    deadlines = []

    def fetch(url, *, deadline):
        deadlines.append(deadline)
        if len(deadlines) == 2:
            raise ValueError("Public provider deadline")
        return body

    monkeypatch.setattr(public_search, "_fetch", fetch)
    result = public_search.search_public_posts("Artemis", provider="lurkbird", max_pages=2)
    assert len(result["posts"]) == 2 and result["stop_reason"] == "deadline"
    assert len(set(deadlines)) == 1


def test_thread_passes_one_deadline_to_all_branches(monkeypatch):
    from trr_backend.socials.twitter.scraper import Tweet

    deadlines = []

    def fetch(id, *, deadline, **kwargs):
        deadlines.append(deadline)
        if len(deadlines) > 1:
            return public_mirror.PublicMirrorConversation(error="Public provider deadline", stop_reason="deadline")
        post = Tweet(
            tweet_id="101",
            reply_to_tweet_id="100",
            replies=1,
            date_time="",
            created_at=0,
            text="",
            hashtags=[],
            mentions=[],
            likes=0,
            retweets=0,
            quotes=0,
            views=0,
            url="",
            username="",
            display_name="",
            user_verified=False,
            is_reply=True,
            is_retweet=False,
            is_quote=False,
        )
        return public_mirror.PublicMirrorConversation(tweets=[post], pages=[{}])

    monkeypatch.setattr(public_thread, "fetch_public_conversation", fetch)
    result = public_thread.expand_public_thread("100", provider="fxtwitter")
    assert len(set(deadlines)) == 1 and len(deadlines) == 2
    assert len(result["posts"]) == 1 and result["stop_reason"] == "deadline"
    assert result["complete"] is False
