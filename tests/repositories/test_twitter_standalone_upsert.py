"""Tests for standalone Twitter scrape persistence helpers."""

from contextlib import nullcontext
from typing import Any, cast
from unittest.mock import patch

from trr_backend.socials.twitter.scraper import Tweet


def _make_tweet(tweet_id: str, text: str = "hello") -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2026-01-05 20:00:00",
        created_at=1736114400,
        text=text,
        hashtags=["RHOSLC"],
        mentions=[],
        likes=10,
        retweets=2,
        replies=1,
        quotes=0,
        views=500,
        url=f"https://x.com/user/status/{tweet_id}",
        username="testuser",
        display_name="Test User",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=False,
    )


_PATCH = "trr_backend.repositories.twitter_standalone._pg_upsert_many"


def test_upsert_empty_list_is_noop():
    from trr_backend.repositories.twitter_standalone import upsert_standalone_tweets

    with patch(_PATCH) as mock_upsert:
        result = upsert_standalone_tweets([], scrape_query="#RHOSLC")
    mock_upsert.assert_not_called()
    assert result == []


def test_upsert_passes_correct_table_and_conflict_col():
    from trr_backend.repositories.twitter_standalone import upsert_standalone_tweets

    tweets = [_make_tweet("111"), _make_tweet("222")]
    with patch(_PATCH) as mock_upsert:
        mock_upsert.return_value = [{"tweet_id": "111"}, {"tweet_id": "222"}]
        upsert_standalone_tweets(tweets, scrape_query="#RHOSLC")

    args, kwargs = mock_upsert.call_args
    # _pg_upsert_many prepends "social." internally — always pass the bare table name
    assert args[0] == "twitter_tweets"
    assert kwargs.get("conflict_col") == "tweet_id"


def test_upsert_sets_scrape_query_on_each_row():
    from trr_backend.repositories.twitter_standalone import upsert_standalone_tweets

    tweets = [_make_tweet("333")]
    with patch(_PATCH) as mock_upsert:
        mock_upsert.return_value = [{"tweet_id": "333"}]
        upsert_standalone_tweets(tweets, scrape_query="@BravoTV")

    payloads = mock_upsert.call_args[0][1]
    assert payloads[0]["scrape_query"] == "@BravoTV"


def test_upsert_maps_core_tweet_fields():
    from trr_backend.repositories.twitter_standalone import upsert_standalone_tweets

    tweet = _make_tweet("444", text="Watch now #RHOSLC")
    with patch(_PATCH) as mock_upsert:
        mock_upsert.return_value = [{"tweet_id": "444"}]
        upsert_standalone_tweets([tweet], scrape_query="#RHOSLC")

    payload = mock_upsert.call_args[0][1][0]
    assert payload["tweet_id"] == "444"
    assert payload["text"] == "Watch now #RHOSLC"
    assert payload["username"] == "testuser"
    assert payload["likes"] == 10
    assert payload["hashtags"] == ["RHOSLC"]


def test_upsert_returns_upserted_rows():
    from trr_backend.repositories.twitter_standalone import upsert_standalone_tweets

    expected = [{"tweet_id": "555", "text": "hi"}]
    with patch(_PATCH) as mock_upsert:
        mock_upsert.return_value = expected
        result = upsert_standalone_tweets([_make_tweet("555")], scrape_query="#RHOSLC")
    assert result == expected


def test_tweet_to_payload_includes_raw_data():
    from trr_backend.repositories.twitter_standalone import _tweet_to_payload

    tweet = _make_tweet("600")
    payload = _tweet_to_payload(tweet, scrape_query="#TEST", scraped_at="2026-01-01T00:00:00+00:00")
    assert "raw_data" in payload
    # raw_data should be a dict (tweet.to_dict() output)
    assert isinstance(payload["raw_data"], dict)
    assert payload["raw_data"].get("tweet_id") == "600"


def test_tweet_to_payload_preserves_none_display_name():
    from trr_backend.repositories.twitter_standalone import _tweet_to_payload

    tweet = _make_tweet("700")
    cast(Any, tweet).display_name = None
    payload = _tweet_to_payload(tweet, scrape_query="#TEST", scraped_at="2026-01-01T00:00:00+00:00")
    assert payload["display_name"] is None


def test_tweet_to_payload_created_at_none_guard():
    from trr_backend.repositories.twitter_standalone import _tweet_to_payload

    tweet = _make_tweet("800")
    tweet.created_at = None  # type: ignore[assignment]
    payload = _tweet_to_payload(tweet, scrape_query="#TEST", scraped_at="2026-01-01T00:00:00+00:00")
    assert payload["created_at"] is None


def test_tweet_to_payload_writes_bookmarks_shares_and_thread_fields():
    from trr_backend.repositories.twitter_standalone import _tweet_to_payload

    tweet = _make_tweet("850")
    tweet.bookmarks = 8
    tweet.shares = 9
    tweet.thread_root_tweet_id = "root-1"
    tweet.thread_position = 2
    tweet.is_thread_part = True
    tweet.twitter_context_role = "audience_reply"

    payload = _tweet_to_payload(tweet, scrape_query="#TEST", scraped_at="2026-01-01T00:00:00+00:00")

    assert payload["bookmarks"] == 8
    assert payload["shares"] == 9
    assert payload["thread_root_tweet_id"] == "root-1"
    assert payload["thread_position"] == 2
    assert payload["is_thread_part"] is True
    assert payload["twitter_context_role"] == "audience_reply"


def test_tweet_to_payload_defaults_shares_to_retweets_when_missing():
    from trr_backend.repositories.twitter_standalone import _tweet_to_payload

    tweet = _make_tweet("851")
    tweet.retweets = 7
    tweet.shares = 0

    payload = _tweet_to_payload(tweet, scrape_query="#TEST", scraped_at="2026-01-01T00:00:00+00:00")

    assert payload["shares"] == 7


def test_persist_search_records_run_and_memberships():
    from trr_backend.repositories.twitter_standalone import persist_standalone_twitter_search

    tweets = [_make_tweet("900"), _make_tweet("901")]
    fake_conn = object()
    with (
        patch(
            "trr_backend.repositories.twitter_standalone.pg.db_connection",
            return_value=nullcontext(fake_conn),
        ),
        patch(
            "trr_backend.repositories.twitter_standalone.upsert_standalone_tweets",
            return_value=[{"tweet_id": "900"}, {"tweet_id": "901"}],
        ) as mock_upsert,
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_run",
            return_value={"id": "run-1"},
        ) as mock_insert_run,
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_memberships",
            return_value=[{"tweet_id": "900"}, {"tweet_id": "901"}],
        ) as mock_insert_memberships,
    ):
        summary = persist_standalone_twitter_search(
            tweets,
            raw_query="#RHOSLC",
            normalized_search_query="#RHOSLC since:2026-01-01 until:2026-01-12",
            scrape_query_label="#RHOSLC",
            window_start_day="2026-01-01",
            window_end_day_exclusive="2026-01-12",
            requested_via="api",
            retrieval_meta={"posts_checked": 99},
            complete=True,
        )

    mock_upsert.assert_called_once()
    mock_insert_run.assert_called_once()
    mock_insert_memberships.assert_called_once_with("run-1", ["900", "901"], conn=mock_upsert.call_args.kwargs["conn"])
    assert summary["succeeded"] is True
    assert summary["scrape_run_id"] == "run-1"
    assert summary["tweets_upserted"] == 2
    assert summary["tweet_memberships_created"] == 2


def test_persist_search_creates_run_for_empty_results():
    from trr_backend.repositories.twitter_standalone import persist_standalone_twitter_search

    fake_conn = object()
    with (
        patch(
            "trr_backend.repositories.twitter_standalone.pg.db_connection",
            return_value=nullcontext(fake_conn),
        ),
        patch(
            "trr_backend.repositories.twitter_standalone.upsert_standalone_tweets",
            return_value=[],
        ) as mock_upsert,
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_run",
            return_value={"id": "run-empty"},
        ) as mock_insert_run,
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_memberships",
            return_value=[],
        ) as mock_insert_memberships,
    ):
        summary = persist_standalone_twitter_search(
            [],
            raw_query="@BravoTV",
            normalized_search_query="@BravoTV since:2026-01-01 until:2026-01-12",
            scrape_query_label="@BravoTV",
            window_start_day="2026-01-01",
            window_end_day_exclusive="2026-01-12",
            requested_via="cli",
            retrieval_meta={"posts_checked": 0},
            complete=True,
        )

    mock_upsert.assert_called_once_with([], scrape_query="@BravoTV", conn=mock_insert_run.call_args.kwargs["conn"])
    mock_insert_run.assert_called_once()
    mock_insert_memberships.assert_called_once()
    assert summary["scrape_run_id"] == "run-empty"
    assert summary["tweet_memberships_created"] == 0
    assert summary["tweet_memberships_total"] == 0


def test_persist_search_dedupes_duplicate_tweet_ids():
    from trr_backend.repositories.twitter_standalone import persist_standalone_twitter_search

    duplicate = _make_tweet("dup-1")
    fake_conn = object()
    with (
        patch(
            "trr_backend.repositories.twitter_standalone.pg.db_connection",
            return_value=nullcontext(fake_conn),
        ),
        patch(
            "trr_backend.repositories.twitter_standalone.upsert_standalone_tweets",
            return_value=[{"tweet_id": "dup-1"}],
        ) as mock_upsert,
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_run",
            return_value={"id": "run-deduped"},
        ),
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_memberships",
            return_value=[{"tweet_id": "dup-1"}],
        ) as mock_insert_memberships,
    ):
        summary = persist_standalone_twitter_search(
            [duplicate, duplicate],
            raw_query="#DUPES",
            normalized_search_query="#DUPES since:2026-01-01 until:2026-01-12",
            scrape_query_label="#DUPES",
            window_start_day="2026-01-01",
            window_end_day_exclusive="2026-01-12",
            requested_via="api",
            retrieval_meta={"posts_checked": 2},
            complete=False,
        )

    assert len(mock_upsert.call_args.args[0]) == 1
    mock_insert_memberships.assert_called_once()
    assert summary["tweet_memberships_total"] == 1


def test_persist_search_preserves_history_across_overlapping_queries():
    from trr_backend.repositories.twitter_standalone import persist_standalone_twitter_search

    tweet = _make_tweet("shared-1")
    run_rows = [{"id": "run-a"}, {"id": "run-b"}]
    with (
        patch(
            "trr_backend.repositories.twitter_standalone.pg.db_connection",
            return_value=nullcontext(object()),
        ),
        patch(
            "trr_backend.repositories.twitter_standalone.upsert_standalone_tweets",
            return_value=[{"tweet_id": "shared-1"}],
        ),
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_run",
            side_effect=run_rows,
        ),
        patch(
            "trr_backend.repositories.twitter_standalone._insert_scrape_query_memberships",
            side_effect=[[{"tweet_id": "shared-1"}], [{"tweet_id": "shared-1"}]],
        ) as mock_insert_memberships,
    ):
        first = persist_standalone_twitter_search(
            [tweet],
            raw_query="#BRAVO",
            normalized_search_query="#BRAVO since:2026-01-01 until:2026-01-12",
            scrape_query_label="#BRAVO",
            window_start_day="2026-01-01",
            window_end_day_exclusive="2026-01-12",
            requested_via="api",
            retrieval_meta={"posts_checked": 1},
            complete=True,
        )
        second = persist_standalone_twitter_search(
            [tweet],
            raw_query="#RHOSLC",
            normalized_search_query="#RHOSLC since:2026-01-01 until:2026-01-12",
            scrape_query_label="#RHOSLC",
            window_start_day="2026-01-01",
            window_end_day_exclusive="2026-01-12",
            requested_via="api",
            retrieval_meta={"posts_checked": 1},
            complete=True,
        )

    assert mock_insert_memberships.call_count == 2
    assert first["scrape_run_id"] == "run-a"
    assert second["scrape_run_id"] == "run-b"
