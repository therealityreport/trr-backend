"""Real observed-session preparation with synthetic HTTP and no ambient access."""

import json
from urllib.parse import parse_qs

import pytest

from tests.ingestion.test_ordered_listing import ACCOUNT, PROFILE, card, envelope
from tests.ingestion.test_public_transport import no_ambient_clients as no_ambient_clients
from tests.ingestion.test_public_transport import source_case
from trr_backend.ingestion.ordered_listing import DOC_ID, ENDPOINT, OPERATION, PROVIDER, parse_page
from trr_backend.ingestion.ordered_listing_transport import OrderedListingSource
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.ingestion.providers import InvalidSourceError


def document(*, token=True, profile=None):
    raw = envelope([card(201)])
    if profile:
        raw["data"]["xig_user_by_username"].update(profile)
    payload = [{"data": {"xig_user_by_username": PROFILE}}, raw]
    body = '<script type="application/json" data-sjs>' + json.dumps(payload) + "</script>"
    if token:
        body += '"LSD",[],{"token":"fixture-public-token"}'
    return {"body": body, "headers": {"content-type": "text/html"}, "cookies": {"csrftoken": "fixture-public-csrf"}}


def call(case, cursor=None):
    return case.source(
        cursor=cursor, pre_source_request=lambda: case.events.append("count"), source_observer=case.retained.append
    )


@pytest.mark.parametrize("account", ["@bravotv", "BravoTV", "../other", None])
def test_account_rejected_before_session_creation(account):
    with pytest.raises(InvalidSourceError):
        OrderedListingSource(account=account, session_factory=lambda: pytest.fail("no session permitted"))


def test_root_uses_one_public_document_and_retains_no_runtime_tokens():
    case = source_case(OrderedListingSource, [document()], account=ACCOUNT)
    result = call(case)
    assert parse_page(result, account=ACCOUNT, cursor=None).cards[0].native_id == "201"
    assert len(case.session.sent) == case.gate.request_count == case.events.count("count") == 1
    assert case.session.closed and case.session.sent[0][0].url == f"https://www.instagram.com/{ACCOUNT}/"
    assert "fixture-public-token" not in json.dumps(case.retained)


def test_continuation_exact_endpoint_operation_variables_viewer_and_native_root():
    native = envelope([card(202)], cursor="prior-native-cursor", outgoing="advanced-native-cursor")
    case = source_case(
        OrderedListingSource,
        [document(), {"payload": {"data": native["data"]}}],
        account=ACCOUNT,
        profile={"id": PROFILE["id"], "pk": PROFILE["pk"]},
    )
    result = call(case, "prior-native-cursor")
    assert parse_page(result, account=ACCOUNT, cursor="prior-native-cursor").cards[0].native_id == "202"
    assert len(case.session.sent) == case.gate.request_count == case.events.count("count") == 2
    request, options = case.session.sent[1]
    assert request.method == "POST" and request.url == ENDPOINT
    form = parse_qs(request.body)
    assert form["doc_id"] == [DOC_ID] and form["fb_api_req_friendly_name"] == [OPERATION]
    assert form["av"] == form["__user"] == ["0"]
    assert json.loads(form["variables"][0]) == {
        "after": "prior-native-cursor",
        "first": 12,
        "id": PROFILE["id"],
        PROVIDER: False,
    }
    assert options["allow_redirects"] is False and options["proxies"] == {}
    assert "sessionid" not in request.headers.get("Cookie", "") and "Authorization" not in request.headers
    assert case.session.closed and len(case.retained) == 2


@pytest.mark.parametrize("reason", ["missing_token", "different_profile", "missing_profile"])
def test_missing_fresh_bootstrap_or_prior_identity_prevents_post(reason):
    case = source_case(
        OrderedListingSource,
        [document(token=reason != "missing_token")],
        account=ACCOUNT,
        profile=None
        if reason == "missing_profile"
        else {"id": "another" if reason == "different_profile" else PROFILE["id"]},
    )
    with pytest.raises(InvalidSourceError):
        call(case, "prior-native-cursor")
    assert len(case.session.sent) == case.events.count("count") == 1
    assert case.session.closed


@pytest.mark.parametrize("status,headers", [(401, {}), (429, {"retry-after": "37"})])
def test_source_rejection_never_rotates_endpoint_or_retries(status, headers):
    case = source_case(
        OrderedListingSource,
        [
            document(),
            {
                "status": status,
                "payload": {"message": "unauthorized"},
                "headers": {"content-type": "application/json", **headers},
            },
        ],
        account=ACCOUNT,
        profile={"id": PROFILE["id"]},
    )
    with pytest.raises(SourceFailure):
        call(case, "prior-native-cursor")
    assert len(case.session.sent) == 2 and case.session.sent[-1][0].url == ENDPOINT
    assert case.session.closed
    if status == 429:
        assert any(isinstance(event, tuple) and event[0] == "failure" and event[-1] == 37 for event in case.events)
