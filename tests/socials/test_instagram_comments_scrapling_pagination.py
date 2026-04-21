from __future__ import annotations

from unittest.mock import MagicMock

from trr_backend.socials.instagram.scraper import InstagramScraper


def _mock_json_response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.headers = {"content-type": "application/json"}
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_fetch_comments_stops_on_repeated_cursor(monkeypatch) -> None:
    scraper = InstagramScraper.__new__(InstagramScraper)
    scraper.last_comment_fetch_reason = None
    scraper.comments_auth_failed = False
    scraper._request_cookies = lambda: {}
    scraper._get_headers = lambda *_args, **_kwargs: {}
    scraper._shortcode_to_media_id = lambda shortcode: f"{shortcode}_media"
    scraper._rate_limit_with_lock = lambda *_args, **_kwargs: None
    scraper._parse_comment = lambda *_args, **_kwargs: MagicMock(reply_count=0, replies=[])

    responses = iter(
        [
            _mock_json_response(
                {
                    "status": "ok",
                    "comments": [{"id": "c1", "text": "hi"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                }
            ),
            _mock_json_response(
                {
                    "status": "ok",
                    "comments": [{"id": "c2", "text": "hi"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                }
            ),
        ]
    )
    scraper._get = lambda *_args, **_kwargs: next(responses)

    comments = scraper.fetch_comments("ABC123", fetch_replies=False, delay=0)

    assert len(comments) == 2
    assert scraper.last_comment_fetch_reason == "pagination_repeated_cursor"


def test_fetch_comment_replies_stops_on_page_cap(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_PAGES", "1")
    scraper = InstagramScraper.__new__(InstagramScraper)
    scraper.last_comment_fetch_reason = None
    scraper.comments_auth_failed = False
    scraper._request_cookies = lambda: {}
    scraper._get_headers = lambda *_args, **_kwargs: {}
    scraper._rate_limit_with_lock = lambda *_args, **_kwargs: None
    scraper._parse_comment = lambda *_args, **_kwargs: MagicMock()

    responses = iter(
        [
            _mock_json_response(
                {
                    "status": "ok",
                    "child_comments": [{"id": "r1"}],
                    "has_more_tail_child_comments": True,
                    "next_min_child_cursor": "reply-2",
                }
            ),
            _mock_json_response(
                {
                    "status": "ok",
                    "child_comments": [{"id": "r2"}],
                    "has_more_tail_child_comments": True,
                    "next_min_child_cursor": "reply-3",
                }
            ),
        ]
    )
    scraper._get = lambda *_args, **_kwargs: next(responses)

    replies = scraper._fetch_comment_replies(
        "media-1",
        "comment-1",
        "ABC123",
        "https://www.instagram.com/p/ABC123/",
        delay=0,
    )

    assert len(replies) == 1
    assert scraper.last_comment_fetch_reason == "pagination_page_cap_reached"
