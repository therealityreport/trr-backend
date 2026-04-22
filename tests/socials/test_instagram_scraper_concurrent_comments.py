from __future__ import annotations

from trr_backend.socials.instagram.scraper import ConcurrentCommentFetchResult, InstagramScraper


class _BoomScraper(InstagramScraper):
    def fetch_comments(self, shortcode, **kwargs):  # type: ignore[override]
        del kwargs
        if shortcode == "bad":
            raise RuntimeError("rate_limited")
        return []


def test_concurrent_fetch_returns_comments_and_errors() -> None:
    scraper = _BoomScraper.__new__(_BoomScraper)
    scraper._concurrent_rate_limit_lock = None

    result = scraper.fetch_comments_concurrent(
        ["ok", "bad"],
        max_comments=10,
        fetch_replies=False,
        delay=0,
        fast_mode=False,
        max_workers=2,
    )

    assert isinstance(result, ConcurrentCommentFetchResult)
    assert result.comments["ok"] == []
    assert result.errors["bad"] == "RuntimeError: rate_limited"
    assert result.had_failures is True
