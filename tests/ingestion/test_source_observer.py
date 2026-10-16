import httpx
import pytest

from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher


def test_raw_comment_page_is_observed_before_returning_it_to_parser():
    fetcher = InstagramCommentsScraplingFetcher.__new__(InstagramCommentsScraplingFetcher)
    observed = []
    fetcher._source_payload_observer = lambda kind, payload: observed.append((kind, payload))
    response = httpx.Response(
        200,
        json={"status": "ok", "comments": [{"pk": "comment-1", "text": "Hello"}]},
        headers={"set-cookie": "sessionid=not-evidence"},
    )
    result = fetcher._decode_json_response_result(response, attempt=1)
    assert observed == [("comments_api", result["payload"])]
    assert "set-cookie" not in str(observed)
    assert not result["failed"]


def test_failed_evidence_sink_does_not_return_successful_payload():
    fetcher = InstagramCommentsScraplingFetcher.__new__(InstagramCommentsScraplingFetcher)

    def failed_sink(kind, payload):
        raise RuntimeError("evidence_store_unavailable")

    fetcher._source_payload_observer = failed_sink
    with pytest.raises(RuntimeError, match="evidence_store_unavailable"):
        fetcher._decode_json_response_result(httpx.Response(200, json={"status": "ok", "comments": []}), attempt=1)


def test_existing_fetcher_caller_without_sink_retains_response_behavior():
    fetcher = InstagramCommentsScraplingFetcher.__new__(InstagramCommentsScraplingFetcher)
    result = fetcher._decode_json_response_result(httpx.Response(200, json={"status": "ok", "comments": []}), attempt=1)
    assert result["payload"]["comments"] == [] and not result["failed"]


def test_rest_child_evidence_includes_parent_without_changing_legacy_payload():
    fetcher = InstagramCommentsScraplingFetcher.__new__(InstagramCommentsScraplingFetcher)
    observed = []
    fetcher._source_payload_observer = lambda kind, payload: observed.append(payload)
    result = fetcher._decode_json_response_result(
        httpx.Response(200, json={"status": "ok", "child_comments": []}),
        attempt=1,
        request_url="https://www.instagram.com/api/v1/media/42/comments/parent-1/child_comments/",
    )
    assert observed[0]["parent_comment_id"] == "parent-1"
    assert "parent_comment_id" not in result["payload"]
