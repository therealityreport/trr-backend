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
        tweet_id=tweet_id, date_time="2026-01-05 20:00:00", created_at=1736114400,
        text="hi", hashtags=[], mentions=[], likes=0, retweets=0, replies=0,
        quotes=0, views=0, url="https://x.com/u/status/t1",
        username="u", display_name="U", user_verified=False,
        is_reply=False, is_retweet=False, is_quote=False,
    )


def _run(argv: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["scrape"] + argv)
    import scripts.socials.twitter.scrape as scrape_mod
    scrape_mod.main()


def test_persist_calls_upsert_with_default_scrape_query(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    upsert_calls: list[dict] = []

    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [_make_tweet()])
    monkeypatch.setattr(
        scrape_mod,
        "upsert_standalone_tweets",
        lambda tweets, *, scrape_query: upsert_calls.append({"tweets": tweets, "scrape_query": scrape_query}) or [],
    )

    _run(["--query", "#RHOSLC", "--start", "2026-01-01", "--end", "2026-01-11", "--persist"], monkeypatch)

    assert len(upsert_calls) == 1
    assert upsert_calls[0]["scrape_query"] == "#RHOSLC"


def test_persist_uses_explicit_scrape_query_when_provided(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    upsert_calls: list[dict] = []

    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [_make_tweet()])
    monkeypatch.setattr(
        scrape_mod,
        "upsert_standalone_tweets",
        lambda tweets, *, scrape_query: upsert_calls.append({"scrape_query": scrape_query}) or [],
    )

    _run(
        ["--query", "@BravoTV", "--start", "2026-01-01", "--end", "2026-01-11",
         "--persist", "--scrape-query", "@BravoTV-jan2026"],
        monkeypatch,
    )

    assert upsert_calls[0]["scrape_query"] == "@BravoTV-jan2026"


def test_no_persist_does_not_call_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    upsert_calls: list = []

    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [_make_tweet()])
    monkeypatch.setattr(
        scrape_mod,
        "upsert_standalone_tweets",
        lambda *a, **kw: upsert_calls.append(1) or [],
    )

    _run(["--query", "#RHOSLC", "--start", "2026-01-01", "--end", "2026-01-11"], monkeypatch)

    assert upsert_calls == []


def test_persist_with_empty_results_does_not_call_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.socials.twitter.scrape as scrape_mod
    from trr_backend.socials.twitter.scraper import TwitterScraper

    upsert_calls: list = []

    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [])
    monkeypatch.setattr(
        scrape_mod,
        "upsert_standalone_tweets",
        lambda *a, **kw: upsert_calls.append(1) or [],
    )

    _run(["--query", "#RHOSLC", "--start", "2026-01-01", "--end", "2026-01-11", "--persist"], monkeypatch)

    # Guard at the call site: `if args.persist and tweets` skips upsert when empty
    assert upsert_calls == []


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
