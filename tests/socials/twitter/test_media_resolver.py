from __future__ import annotations

from trr_backend.socials.twitter.media_resolver import (
    canonical_tweet_url,
    normalize_tweet_id,
    resolve_twitter_media,
)
from trr_backend.socials.twitter.scraper import Tweet, TwitterScraper


def _tweet_fixture(*, tweet_id: str = "1956000357282406729", username: str = "BravoTV") -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2025-08-14 12:00:00",
        created_at=1755172800,
        text="fixture",
        hashtags=[],
        mentions=[],
        likes=0,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url=f"https://x.com/{username}/status/{tweet_id}",
        username=username,
        display_name="Bravo",
        user_verified=True,
        is_reply=False,
        is_retweet=False,
        is_quote=False,
        media_urls=[
            "https://video.twimg.com/ext_tw_video/abc/pu/vid/avc1/1280x720/video.mp4",
            "https://video.twimg.com/ext_tw_video/abc/pu/vid/avc1/1280x720/video.mp4",
            "https://pbs.twimg.com/media/image.jpg",
        ],
    )


def test_normalize_tweet_id_supports_status_url_shapes() -> None:
    assert normalize_tweet_id("1956000357282406729") == "1956000357282406729"
    assert normalize_tweet_id("https://x.com/BravoTV/status/1956000357282406729?s=20") == "1956000357282406729"
    assert normalize_tweet_id("https://x.com/i/status/1956000357282406729") == "1956000357282406729"


def test_canonical_tweet_url_formats_with_username() -> None:
    assert canonical_tweet_url("1956000357282406729", "@BravoTV") == "https://x.com/BravoTV/status/1956000357282406729"
    assert canonical_tweet_url("1956000357282406729") == "https://x.com/i/status/1956000357282406729"


def test_resolve_twitter_media_returns_missing_id_reason() -> None:
    payload = resolve_twitter_media(tweet_id_or_url="not-a-tweet")

    assert payload["tweet_id"] == ""
    assert payload["media_urls"] == []
    assert payload["attempts"][0]["reason_code"] == "missing_tweet_id"


def test_resolve_twitter_media_uses_tweet_detail_and_dedupes_media(monkeypatch) -> None:
    monkeypatch.setattr(TwitterScraper, "fetch_tweet_detail", lambda self, tweet_id, delay=0.0: _tweet_fixture())

    payload = resolve_twitter_media(tweet_id_or_url="https://x.com/i/status/1956000357282406729")

    assert payload["tweet_id"] == "1956000357282406729"
    assert payload["canonical_url"] == "https://x.com/BravoTV/status/1956000357282406729"
    assert payload["media_urls"] == [
        "https://video.twimg.com/ext_tw_video/abc/pu/vid/avc1/1280x720/video.mp4",
        "https://pbs.twimg.com/media/image.jpg",
    ]
    assert payload["attempts"][0]["success"] is True


def test_resolve_twitter_media_returns_not_found_reason_when_no_media(monkeypatch) -> None:
    monkeypatch.setattr(TwitterScraper, "fetch_tweet_detail", lambda self, tweet_id, delay=0.0: None)

    payload = resolve_twitter_media(tweet_id_or_url="1956000357282406729")

    assert payload["tweet_id"] == "1956000357282406729"
    assert payload["media_urls"] == []
    assert payload["attempts"][0]["reason_code"] == "twitter_media_not_found"
