# tests/scripts/test_twitter_scrape_persist.py
"""
Tests for the --persist flag on the Twitter scrape CLI.
Patches via monkeypatch.setattr on the module-level import so the
binding at call time is intercepted correctly.
"""

import sys

import pytest

from trr_backend.socials.twitter.scraper import Tweet


def _make_tweet(tweet_id: str = "t1") -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2026-01-05 20:00:00",
        created_at=1736114400,
        text="hi",
        hashtags=[],
        mentions=[],
        likes=0,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url="https://x.com/u/status/t1",
        username="u",
        display_name="U",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=False,
    )


def _run(argv: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["scrape"] + argv)
    import scripts.socials.twitter.scrape as scrape_mod

    scrape_mod.main()


def _stub_scrape_with_meta(tweets: list[Tweet], meta: dict | None = None):
    def _scrape(self, config):
        self.last_retrieval_meta = dict(
            meta or {"complete": True, "posts_checked": len(tweets), "stop_reason": "no_cursor"}
        )
        return list(tweets)

    return _scrape


def test_persist_calls_repository_with_default_scrape_query(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    persist_calls: list[dict] = []

    monkeypatch.setattr(
        TwitterScraper,
        "scrape",
        _stub_scrape_with_meta([_make_tweet()], {"complete": True, "posts_checked": 11}),
    )
    monkeypatch.setattr(
        scrape_mod,
        "persist_standalone_twitter_search",
        lambda tweets, **kwargs: persist_calls.append({"tweets": tweets, **kwargs})
        or {
            "requested": True,
            "succeeded": True,
            "scrape_query_label": kwargs["scrape_query_label"],
            "scrape_run_id": "run-cli-default",
            "tweets_upserted": len(tweets),
            "tweet_memberships_created": len(tweets),
            "tweet_memberships_total": len(tweets),
            "requested_via": kwargs["requested_via"],
            "error": None,
        },
    )

    _run(["--query", "#RHOSLC", "--start", "2026-01-01", "--end", "2026-01-11", "--persist"], monkeypatch)
    output = capsys.readouterr().out

    assert len(persist_calls) == 1
    assert persist_calls[0]["scrape_query_label"] == "#RHOSLC"
    assert "complete: True" in output
    assert "scrape_run_id: run-cli-default" in output


def test_persist_uses_explicit_scrape_query_when_provided(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    persist_calls: list[dict] = []

    monkeypatch.setattr(TwitterScraper, "scrape", _stub_scrape_with_meta([_make_tweet()]))
    monkeypatch.setattr(
        scrape_mod,
        "persist_standalone_twitter_search",
        lambda tweets, **kwargs: persist_calls.append(kwargs)
        or {
            "requested": True,
            "succeeded": True,
            "scrape_query_label": kwargs["scrape_query_label"],
            "scrape_run_id": "run-cli-explicit",
            "tweets_upserted": len(tweets),
            "tweet_memberships_created": len(tweets),
            "tweet_memberships_total": len(tweets),
            "requested_via": kwargs["requested_via"],
            "error": None,
        },
    )

    _run(
        [
            "--query",
            "@BravoTV",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-11",
            "--persist",
            "--scrape-query",
            "@BravoTV-jan2026",
        ],
        monkeypatch,
    )

    assert persist_calls[0]["scrape_query_label"] == "@BravoTV-jan2026"


def test_no_persist_does_not_call_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    persist_calls: list = []

    monkeypatch.setattr(TwitterScraper, "scrape", _stub_scrape_with_meta([_make_tweet()]))
    monkeypatch.setattr(
        scrape_mod,
        "persist_standalone_twitter_search",
        lambda *a, **kw: persist_calls.append(1) or {},
    )

    _run(["--query", "#RHOSLC", "--start", "2026-01-01", "--end", "2026-01-11"], monkeypatch)

    assert persist_calls == []


def test_persist_with_empty_results_still_records_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    persist_calls: list[dict] = []

    monkeypatch.setattr(
        TwitterScraper,
        "scrape",
        _stub_scrape_with_meta([], {"complete": False, "posts_checked": 500, "stop_reason": "max_pages_reached"}),
    )
    monkeypatch.setattr(
        scrape_mod,
        "persist_standalone_twitter_search",
        lambda tweets, **kwargs: persist_calls.append({"tweets": tweets, **kwargs})
        or {
            "requested": True,
            "succeeded": True,
            "scrape_query_label": kwargs["scrape_query_label"],
            "scrape_run_id": "run-cli-empty",
            "tweets_upserted": 0,
            "tweet_memberships_created": 0,
            "tweet_memberships_total": 0,
            "requested_via": kwargs["requested_via"],
            "error": None,
        },
    )

    _run(["--query", "#RHOSLC", "--start", "2026-01-01", "--end", "2026-01-11", "--persist"], monkeypatch)
    output = capsys.readouterr().out

    assert len(persist_calls) == 1
    assert persist_calls[0]["window_end_day_exclusive"] == "2026-01-12"
    assert "complete: False" in output
    assert "stop_reason: max_pages_reached" in output
    assert "scrape_run_id: run-cli-empty" in output


def test_persist_with_replies_mode_raises_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """--persist is not supported in --replies or --quotes mode; expect SystemExit(2)."""
    import sys

    # --replies + --persist
    with pytest.raises(SystemExit) as excinfo:
        monkeypatch.setattr(sys, "argv", ["scrape", "--replies", "--tweet", "123", "--persist"])
        import scripts.socials.twitter.scrape as scrape_mod

        scrape_mod.main()
    assert excinfo.value.code == 2

    # --quotes + --persist
    with pytest.raises(SystemExit) as excinfo:
        monkeypatch.setattr(sys, "argv", ["scrape", "--quotes", "--tweet", "123", "--persist"])
        scrape_mod.main()
    assert excinfo.value.code == 2
