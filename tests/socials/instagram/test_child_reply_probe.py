from __future__ import annotations

import json
from urllib.parse import parse_qs

import httpx
import pytest

from trr_backend.ingestion.credential_snapshot import CredentialSnapshot
from trr_backend.socials.instagram.child_reply_probe import (
    CONNECTION,
    media_id_from_shortcode,
    probe_child_reply_access,
    project_reply_page,
)

PARENT = "17966467277918174"
BOOTSTRAP = '"LSD",[],{"token":"private-lsd"}'


def _page(*identities, parent=PARENT):
    return {
        "data": {
            CONNECTION: {
                "edges": [
                    {"node": {"pk": identity, "parent_comment_id": parent, "text": "private body"}}
                    for identity in identities
                ],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }


def _run(responses, **kwargs):
    sent, debits = [], []

    def handler(request):
        sent.append(request)
        response = responses[len(sent) - 1]
        return response

    with httpx.Client(transport=httpx.MockTransport(handler), trust_env=False) as client:
        report = probe_child_reply_access(
            "DY1vXi-kRBb", PARENT, client=client, before_request=debits.append, sleep=lambda _: None, **kwargs
        )
    return report, sent, debits


def test_child_rejection_tries_alternate_once_and_records_only_safe_proof():
    report, sent, debits = _run(
        [
            httpx.Response(200, text=BOOTSTRAP),
            httpx.Response(200, json={"errors": [{"code": 1675004, "message": "Rate limit exceeded"}]}),
            httpx.Response(200, json=_page("123456789")),
        ]
    )
    assert report["requests"] == len(debits) == len(sent) == 3
    assert report["positive_reply_proof"] is True
    assert report["stop_reason"] is None
    assert report["events"][1]["native_codes"] == ["1675004"]
    assert report["events"][2]["reply_ids"] == ["123456789"]
    assert "private-lsd" not in json.dumps(report)
    assert "private body" not in json.dumps(report)
    forms = [parse_qs(request.content.decode()) for request in sent[1:]]
    assert [form["doc_id"][0] for form in forms] == ["34884685271179117", "36239935742272683"]
    assert all(
        json.loads(form["variables"][0])["__relay_internal__pv__PolarisIsLoggedInrelayprovider"] is False
        for form in forms
    )


@pytest.mark.parametrize(
    "response,reason",
    [
        (httpx.Response(429, json={}), "http_rate_limited"),
        (httpx.Response(200, headers={"retry-after": "120"}, json=_page()), "server_retry_after"),
        (httpx.Response(200, json={"status": "fail", "message": "challenge_required"}), "source_auth_failed"),
        (httpx.Response(302, headers={"location": "https://example.com/"}), "endpoint_redirect"),
    ],
)
def test_server_rejections_stop_without_retry_or_next_surface(response, reason):
    report, sent, debits = _run([httpx.Response(200, text=BOOTSTRAP), response], include_rest=True)
    assert report["stop_reason"] == reason
    assert report["requests"] == len(sent) == len(debits) == 2
    assert report["positive_reply_proof"] is False


def test_empty_alternate_and_unrelated_connection_are_not_positive():
    report, _, _ = _run(
        [
            httpx.Response(200, text=BOOTSTRAP),
            httpx.Response(200, json={"data": {"comments": [{"id": "123456789"}]}}),
            httpx.Response(200, json=_page()),
        ]
    )
    assert report["positive_reply_proof"] is False
    assert report["events"][1]["connection_found"] is False
    assert report["events"][2]["connection_found"] is True


def test_mismatched_parent_does_not_prove_reply_access():
    report, _, _ = _run(
        [
            httpx.Response(200, text=BOOTSTRAP),
            httpx.Response(200, json=_page("123456789", parent="999999999")),
        ]
    )
    assert report["stop_reason"] == "reply_parent_mismatch"
    assert report["positive_reply_proof"] is False


def test_authenticated_probe_requires_explicit_pinned_snapshot_and_only_one_graphql_method():
    snapshot = CredentialSnapshot.from_environ(
        {
            "TRR_INSTAGRAM_INGESTION_AUTH_USERNAME": "livestnation",
            "TRR_INSTAGRAM_INGESTION_AUTH_USER_ID": "123",
            "SOCIAL_INSTAGRAM_COOKIES_JSON": json.dumps(
                {"sessionid": "secret", "csrftoken": "csrf", "ds_user_id": "123"}
            ),
        }
    )
    report, sent, _ = _run(
        [
            httpx.Response(200, text=BOOTSTRAP),
            httpx.Response(200, json=_page("123456789")),
            httpx.Response(200, json={"status": "ok", "child_comments": [{"pk": "987654321"}]}),
        ],
        credential_snapshot=snapshot,
        include_rest=True,
    )
    assert report["authenticated"] is True
    assert len(sent) == 3
    assert all("sessionid=secret" in request.headers["cookie"] for request in sent)
    form = parse_qs(sent[1].content.decode())
    assert json.loads(form["variables"][0])["__relay_internal__pv__PolarisIsLoggedInrelayprovider"] is True
    assert "secret" not in json.dumps(report)
    assert report["events"][2]["reply_count"] == 1


def test_anonymous_client_refuses_existing_login_cookies_without_send():
    with httpx.Client(cookies={"sessionid": "secret"}) as client:
        report = probe_child_reply_access("DY1vXi-kRBb", PARENT, client=client, sleep=lambda _: None)
    assert report["requests"] == 0
    assert report["stop_reason"] == "unexpected_authenticated_cookie"


def test_input_validation_and_shortcode_identity():
    assert media_id_from_shortcode("CfhVcINpHiY") == "2873672329179986072"
    with pytest.raises(ValueError):
        probe_child_reply_access("../../bad", PARENT)
    with pytest.raises(ValueError):
        probe_child_reply_access("DY1vXi-kRBb", "bad/parent")
    assert project_reply_page({"comments": [{"pk": "123"}]}, PARENT, rest=True)["positive_reply_proof"] is False
