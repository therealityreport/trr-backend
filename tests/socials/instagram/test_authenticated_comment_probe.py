import json

import httpx
import pytest

from trr_backend.ingestion.credential_snapshot import CredentialSnapshot
from trr_backend.socials.instagram.authenticated_comment_probe import SUBJECTS, probe_authenticated_comment_ids


@pytest.fixture
def snapshot():
    return CredentialSnapshot.from_environ(
        {
            "TRR_INSTAGRAM_INGESTION_AUTH_USERNAME": "livestnation",
            "TRR_INSTAGRAM_INGESTION_AUTH_USER_ID": "12345",
            "SOCIAL_INSTAGRAM_COOKIES_JSON": json.dumps(
                {
                    "sessionid": "SECRET_SESSION",
                    "csrftoken": "SECRET_CSRF",
                    "ds_user_id": "12345",
                }
            ),
        }
    )


def identity():
    return {"status": "ok", "user": {"pk": "12345", "username": "livestnation"}}


def row(key, parent=None):
    return {
        "pk": key,
        "text": "PRIVATE_TEXT",
        "parent_comment_id": parent,
        "user": {"is_private": True},
        "is_hidden": False,
    }


def page(rows, child=False, **extra):
    return {
        "status": "ok",
        "child_comments" if child else "comments": rows,
        "has_more_tail_child_comments" if child else "has_more_comments": False,
        **extra,
    }


def successful_pages():
    return [
        identity(),
        page([row(next(iter(SUBJECTS[0][1])))], comment_count=48),
        page([row(key) for key, parent in SUBJECTS[1][1].items() if parent is None], comment_count=96),
        page([row(key, parent) for key, parent in SUBJECTS[1][1].items() if parent], child=True),
    ]


def run(snapshot, responses, **kwargs):
    requests = []

    def handler(request):
        requests.append(request)
        assert request.headers["x-csrftoken"] == "SECRET_CSRF"
        assert "SECRET_SESSION" in request.headers["cookie"]
        value = responses[len(requests) - 1]
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, httpx.Response) else httpx.Response(200, json=value)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = probe_authenticated_comment_ids(snapshot, client=client, sleep=lambda _: None, **kwargs)
    assert result["requests"] == len(requests)
    for secret in ("SECRET_SESSION", "SECRET_CSRF", "PRIVATE_TEXT", "https://secret"):
        assert secret not in json.dumps(result)
    return result, requests


def test_verified_identity_and_six_ids(snapshot):
    result, requests = run(snapshot, successful_pages())
    assert result["verdict"] == "passed"
    assert result["auth_state"] == "authenticated"
    assert len(requests) == 4
    assert requests[0].url.path == "/api/v1/accounts/current_user/"
    assert all(not subject["missing_targets"] for subject in result["subjects"])
    assert result["subjects"][1]["reply_count"] == 2
    assert result["subjects"][0]["target_metadata"]["18281777497294705"]["is_private"] is True


@pytest.mark.parametrize(
    "user", [{"pk": "99", "username": "livestnation"}, {"pk": "12345", "username": "thereality.report"}, None]
)
def test_identity_mismatch_stops_before_comments(snapshot, user):
    result, requests = run(snapshot, [{"status": "ok", "user": user}])
    assert result["stop_reason"] == "identity_mismatch"
    assert result["auth_state"] == "identity_mismatch"
    assert len(requests) == 1


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (httpx.Response(429), "http_rate_limited"),
        (httpx.Response(403), "source_auth_failed"),
        (httpx.Response(302, headers={"location": "https://secret"}), "endpoint_redirect"),
        (httpx.Response(200, headers={"retry-after": "30"}), "server_retry_after"),
        (httpx.Response(200, text="not json"), "invalid_json"),
        ({"status": "fail", "challenge": {"url": "https://secret"}}, "source_auth_failed"),
        (httpx.ConnectError("https://secret SECRET_SESSION"), "network_error"),
    ],
)
def test_stop_without_retry_or_leak(snapshot, response, reason):
    result, requests = run(snapshot, [identity(), response])
    assert result["stop_reason"] == reason
    assert result["verdict"] == "inconclusive"
    assert len(requests) == 2


def test_cap_shared_across_subjects_and_identity(snapshot):
    responses = successful_pages()[:2] + [page([], has_more_comments=True, next_max_id=str(i)) for i in range(20)]
    result, requests = run(snapshot, responses)
    assert result["stop_reason"] == "request_limit"
    assert len(requests) == 12
    assert result["subjects"][0]["missing_targets"] == []
    assert result["subjects"][1]["missing_targets"]


def test_repeated_cursor_stops(snapshot):
    response = page([], has_more_comments=True, next_max_id="cursor")
    result, requests = run(snapshot, [identity(), response, response])
    assert result["stop_reason"] == "repeated_cursor"
    assert len(requests) == 3


@pytest.mark.parametrize(
    "response", [page([], has_more_comments=True), page([], has_more_comments="true"), {"status": "ok"}]
)
def test_malformed_pagination_or_connection(snapshot, response):
    result, _ = run(snapshot, [identity(), response])
    assert result["stop_reason"] in {"malformed_pagination", "missing_comment_connection"}


def test_parent_linkage_mismatch(snapshot):
    responses = successful_pages()
    responses[-1]["child_comments"][0]["parent_comment_id"] = "999"
    result, _ = run(snapshot, responses)
    assert result["stop_reason"] == "reply_parent_mismatch"
    assert result["verdict"] == "inconclusive"


def test_native_exhaustion_is_inconclusive_and_flags_nullable(snapshot):
    responses = [identity(), page([]), page([]), page([], child=True)]
    result, _ = run(snapshot, responses)
    assert result["stop_reason"] == "targets_not_observed"
    assert result["verdict"] == "inconclusive"
    responses = successful_pages()
    responses[1]["comments"][0].pop("user")
    result, _ = run(snapshot, responses)
    assert result["subjects"][0]["target_metadata"]["18281777497294705"]["is_private"] is None


def test_cap_cannot_be_raised(snapshot):
    with pytest.raises(ValueError):
        probe_authenticated_comment_ids(snapshot, max_requests=13)
