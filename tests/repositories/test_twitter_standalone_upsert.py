"""Tests for upsert_standalone_tweets(). Does not hit a real database."""
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
