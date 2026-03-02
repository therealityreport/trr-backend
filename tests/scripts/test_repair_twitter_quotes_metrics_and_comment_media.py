from __future__ import annotations

import json
from argparse import Namespace

import scripts.socials.repair_twitter_quotes_metrics_and_comment_media as mod


def test_main_dry_run_reports_counts_without_mutations(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda _argv=None: Namespace(
            season_id="season-1",
            source_account="",
            tweet_id="",
            limit=50,
            max_pages_cap=60,
            dry_run=True,
            apply=False,
        ),
    )
    monkeypatch.setattr(
        mod,
        "_load_root_rows",
        lambda **_kwargs: [
            {"tweet_id": "t1", "season_id": "season-1", "source_account": "bravotv"},
            {"tweet_id": "t2", "season_id": "season-1", "source_account": "bravotv"},
        ],
    )
    monkeypatch.setattr(mod.social_repo, "get_season_context", lambda _season_id: object())
    monkeypatch.setattr(mod, "_build_scraper", lambda: (_ for _ in ()).throw(AssertionError("unexpected scraper init")))
    monkeypatch.setattr(
        mod,
        "_enqueue_missing_comment_media_jobs_for_root",
        lambda **kwargs: 3 if kwargs.get("root_tweet_id") == "t1" else 1,
    )

    assert mod.main() == 0
    payload = json.loads(capsys.readouterr().out.strip())

    assert payload["dry_run"] is True
    assert payload["roots_scanned"] == 2
    assert payload["roots_refreshed"] == 0
    assert payload["media_jobs_enqueued"] == 4
    assert payload["unresolved_posts"] == []


def test_main_apply_updates_metrics_comments_and_media_jobs(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda _argv=None: Namespace(
            season_id="season-1",
            source_account="bravotv",
            tweet_id="1956000357282406729",
            limit=1,
            max_pages_cap=45,
            dry_run=False,
            apply=True,
        ),
    )
    monkeypatch.setattr(
        mod,
        "_load_root_rows",
        lambda **_kwargs: [
            {
                "tweet_id": "1956000357282406729",
                "season_id": "season-1",
                "source_account": "bravotv",
            }
        ],
    )

    scraper = object()
    monkeypatch.setattr(mod, "_build_scraper", lambda: scraper)
    monkeypatch.setattr(mod.social_repo, "get_season_context", lambda _season_id: object())

    calls: dict[str, int] = {"metrics": 0, "refresh": 0}

    def _fake_apply_metrics(**kwargs):
        assert kwargs["scraper"] is scraper
        assert kwargs["tweet_id"] == "1956000357282406729"
        calls["metrics"] += 1
        return {"likes": 7092, "replies": 338, "retweets": 1800, "quotes": 44, "views": 617900}

    monkeypatch.setattr(mod.social_repo, "_fetch_and_apply_twitter_metric_summary", _fake_apply_metrics)

    def _fake_refresh(*args, **kwargs):
        del args
        assert kwargs["platform"] == "twitter"
        assert kwargs["source_id"] == "1956000357282406729"
        calls["refresh"] += 1
        return {
            "comments_fetched": 338,
            "comments_upserted": 322,
            "quotes_fetched": 44,
            "quotes_upserted": 41,
            "comment_media_mirror_jobs_enqueued": 5,
        }

    monkeypatch.setattr(mod.social_repo, "refresh_post_comments", _fake_refresh)
    monkeypatch.setattr(mod, "_enqueue_missing_comment_media_jobs_for_root", lambda **_kwargs: 2)

    original_cap = mod.social_repo.TWITTER_COMMENT_MAX_PAGE_BUDGET
    try:
        assert mod.main() == 0
    finally:
        assert mod.social_repo.TWITTER_COMMENT_MAX_PAGE_BUDGET == original_cap

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is False
    assert payload["roots_scanned"] == 1
    assert payload["roots_refreshed"] == 1
    assert payload["replies_fetched"] == 338
    assert payload["replies_upserted"] == 322
    assert payload["quotes_fetched"] == 44
    assert payload["quotes_upserted"] == 41
    assert payload["media_jobs_enqueued"] == 7
    assert payload["unresolved_posts"] == []
    assert calls == {"metrics": 1, "refresh": 1}


def test_load_root_rows_applies_scope_filters(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(mod.pg, "fetch_all", _fake_fetch_all)

    mod._load_root_rows(
        season_id="season-xyz",
        source_account="BravoTV",
        tweet_id="1956000357282406729",
        limit=12,
    )
    sql = " ".join(str(captured["sql"]).lower().split())
    params = captured["params"]
    assert "t.is_reply = false" in sql
    assert "t.season_id = %s::uuid" in sql
    assert "lower(coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '')) = lower(%s)" in sql
    assert "t.tweet_id = %s" in sql
    assert "limit %s" in sql
    assert params == ["season-xyz", "BravoTV", "1956000357282406729", 12]
