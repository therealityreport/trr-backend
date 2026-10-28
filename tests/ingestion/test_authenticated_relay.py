import json

import httpx
import pytest

from trr_backend.ingestion.authenticated_relay import (
    AUTH_RELAY_FAMILY,
    CHILD_DOCUMENT,
    PARENT_DOCUMENT,
    PARENT_ROOT_DOCUMENT,
    WWW_RELAY_URL,
    AuthenticatedRelayPageClient,
    WwwRelayContext,
)
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch


def document(*, actor="222", user="0", logged=True, threads=False, token="document-secret", extra=None):
    definitions = [
        ["CurrentUserInitialData", [], {"USER_ID": user, "NON_FACEBOOK_USER_ID": actor}, 1],
        ["RelayAPIConfigDefaults", [], {"actorID": actor}, 1],
        ["DTSGInitialData", [], {"token": token}, 1],
        ["LSD", [], {"token": "lsd-secret"}, 1],
        [
            "SiteData",
            [],
            {
                "haste_session": "fixture-session",
                "pr": 1,
                "client_revision": 101,
                "hsi": "123",
                "comet_env": 7,
                "__spin_r": 101,
                "__spin_b": "trunk",
                "__spin_t": 123,
            },
            1,
        ],
        ["SprinkleConfig", [], {"param_name": "jazoest", "should_randomize": False, "version": 2}, 1],
    ]
    box = {"define": definitions}
    if extra is not None:
        box["require"] = extra
    root = {
        "require": [["Root", "call", None, [{"__bbox": box}]]],
        "gkx": {"4150": {"result": logged}, "23962": {"result": threads}},
    }
    return '<script type="application/json" data-sjs>' + json.dumps(root, separators=(",", ":")) + "</script>"


def test_www_actor_and_provider_fields_come_from_document():
    context = WwwRelayContext.from_html(document(threads=True))
    form = context.request(media_id="333", cursor="native-next")
    variables = json.loads(form["variables"])
    assert form["av"] == "222"
    assert form["__user"] == "0"
    assert form["jazoest"] == "2" + str(sum(map(ord, "document-secret")))
    assert (form["doc_id"], form["fb_api_req_friendly_name"]) == PARENT_DOCUMENT
    assert variables["after"] == "native-next"
    assert variables["first"] == 10
    assert variables["__relay_internal__pv__PolarisThreadsStyleCommentsEnabledrelayprovider"] is True
    assert "secret" not in repr(context)
    child = context.request(media_id="333", cursor=None, parent_id="444")
    assert (child["doc_id"], child["fb_api_req_friendly_name"]) == CHILD_DOCUMENT
    assert json.loads(child["variables"])["first"] == 5
    assert json.loads(child["variables"])["is_chronological"] is None


@pytest.mark.parametrize(
    "html",
    [
        document(logged=False),
        document(token=""),
        document().replace('"actorID":"222"', '"actorID":"999"'),
        document().replace('"23962"', '"unrelated"'),
        document().replace('"USER_ID":"0"', '"USER_ID":null'),
    ],
)
def test_missing_or_inconsistent_document_context_is_rejected(html):
    with pytest.raises(ValueError, match="native_relay"):
        WwwRelayContext.from_html(html)


def test_nested_actor_definition_does_not_override_document():
    nested = [
        ["Nested", "call", None, [{"__bbox": {"define": [["RelayAPIConfigDefaults", [], {"actorID": "999"}, 1]]}}]]
    ]
    assert WwwRelayContext.from_html(document(extra=nested)).form["av"] == "222"


def make_client(handler, *, before=None, observer=None, failed=None):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return AuthenticatedRelayPageClient(
        cookies={"sessionid": "session-secret", "ds_user_id": "111", "csrftoken": "csrf-secret"},
        pre_source_request=before or (lambda: None),
        source_observer=observer or (lambda *args: None),
        source_failure_observer=failed,
        client=client,
    ), client


def test_native_page_uses_www_endpoint_and_retains_only_cursor_provenance():
    requests, observed, counts = [], [], []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, text=document()) if request.method == "GET" else httpx.Response(200, json=page())

    source, client = make_client(handler, before=lambda: counts.append(1), observer=lambda *args: observed.append(args))
    with client:
        source.bootstrap("Abc123")
        payload = source.page(media_id="333", cursor="native-next")
    assert len(requests) == len(counts) == 2
    assert requests[0].headers["accept"] == "text/html,application/xhtml+xml"
    assert "x-requested-with" not in requests[0].headers
    assert str(requests[-1].url) == WWW_RELAY_URL
    assert requests[-1].headers["x-fb-friendly-name"] == PARENT_DOCUMENT[1]
    assert "x-root-field-name" not in requests[-1].headers
    assert "x-bloks-version-id" not in requests[-1].headers
    assert payload["cursor_payload"]["adapter_family"] == AUTH_RELAY_FAMILY
    assert payload["cursor_payload"]["request_cursor"] == "native-next"
    assert "secret" not in json.dumps(observed)


@pytest.mark.parametrize(
    "status,code", [(401, "source_auth_failed"), (429, "rate_limited"), (302, "endpoint_redirect")]
)
def test_http_failure_stops_without_redirect_retry_or_secondary_transport(status, code):
    requests, failures = [], []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, headers={"location": "https://example.invalid/", "retry-after": "10"})

    source, client = make_client(handler, failed=lambda *args: failures.append(args))
    with client, pytest.raises(SourceFailure) as error:
        source.bootstrap("Abc123")
    assert error.value.failure.code == code
    assert len(requests) == 1
    assert failures == [(code, 10)]


def test_native_200_error_is_observed_and_not_treated_as_exhaustion():
    requests, observed = [], []

    def handler(request):
        requests.append(request)
        return (
            httpx.Response(200, text=document())
            if request.method == "GET"
            else httpx.Response(200, json={"error": 1357054})
        )

    source, client = make_client(handler, observer=lambda *args: observed.append(args))
    with client:
        source.bootstrap("Abc123")
        with pytest.raises(SourceFailure) as error:
            source.page(media_id="333", cursor="native-next")
    assert error.value.failure.code == "source_incomplete"
    assert observed[0][1]["error"] == 1357054
    assert len(requests) == 2


def test_budget_callback_abort_sends_nothing():
    def before():
        raise RuntimeError("budget_stop")

    source, client = make_client(lambda request: pytest.fail("Unexpected send"), before=before)
    with client, pytest.raises(RuntimeError, match="budget_stop"):
        source.bootstrap("Abc123")


def test_changed_cookie_identity_stops_before_post():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, text=document(), headers={"set-cookie": "ds_user_id=999; Path=/"})

    source, client = make_client(handler)
    with client, pytest.raises(CredentialIdentityMismatch):
        source.bootstrap("Abc123")
    assert len(requests) == 1


def page():
    return {
        "data": {
            "xdt_api__v1__media__media_id__comments__connection": {
                "edges": [],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }


@pytest.mark.parametrize(
    "payload,code",
    [
        ({"error": 1357001}, "source_auth_failed"),
        ({"error": 1357054, "rate_limited": True}, "rate_limited"),
        ({"success": False, **page()}, "source_incomplete"),
        ({"data": {}}, "source_incomplete"),
        (
            {
                "data": {
                    "xdt_api__v1__media__media_id__comments__connection": {
                        "edges": [],
                        "page_info": {"has_next_page": True, "end_cursor": " "},
                    }
                }
            },
            "source_incomplete",
        ),
    ],
)
def test_native_failure_or_missing_connection_is_retained_and_rejected(payload, code):
    observed, failures = [], []
    source, client = make_client(
        lambda r: httpx.Response(200, text=document()) if r.method == "GET" else httpx.Response(200, json=payload),
        observer=lambda *args: observed.append(args),
        failed=lambda *args: failures.append(args),
    )
    with client:
        source.bootstrap("Abc123")
        with pytest.raises(SourceFailure) as error:
            source.page(media_id="333", cursor="native-next")
    assert error.value.failure.code == code
    assert len(observed) == len(failures) == 1
    assert failures[0][0] == code


def test_timeout_records_one_failure_without_retry():
    sends, failures = [], []

    def handler(request):
        sends.append(request)
        raise httpx.ReadTimeout("private transport error")

    source, client = make_client(handler, failed=lambda *args: failures.append(args))
    with client, pytest.raises(SourceFailure) as error:
        source.bootstrap("Abc123")
    assert error.value.failure.code == "request_failed"
    assert len(sends) == 1
    assert failures == [("request_failed", None)]


def test_deep_json_records_parse_failure():
    failures = []
    source, client = make_client(
        lambda r: httpx.Response(200, text=document())
        if r.method == "GET"
        else httpx.Response(200, text="[" * 1200 + "0" + "]" * 1200),
        failed=lambda *args: failures.append(args),
    )
    with client:
        source.bootstrap("Abc123")
        with pytest.raises(SourceFailure) as error:
            source.page(media_id="333", cursor="native-next")
    assert error.value.failure.code == "parse_error"
    assert failures == [("parse_error", None)]


def test_compressed_bootstrap_and_page_are_decoded_only_once():
    import gzip

    def handler(request):
        body = document() if request.method == "GET" else json.dumps(page())
        return httpx.Response(200, content=gzip.compress(body.encode()), headers={"content-encoding": "gzip"})

    source, client = make_client(handler)
    with client:
        source.bootstrap("Abc123")
        payload = source.page(media_id="333", cursor="native-next")
    assert payload["data"] == page()["data"]


def test_native_bootstrap_root_precedes_continuation_without_second_root_request():
    native = page()
    connection = native["data"]["xdt_api__v1__media__media_id__comments__connection"]
    connection["edges"] = [{"node": {"pk": str(i)}} for i in range(100, 115)]
    connection["page_info"] = {"has_next_page": True, "end_cursor": "native-after-15"}
    stream = {
        "require": [
            [
                "ScheduledServerJS",
                "handle",
                None,
                [
                    {
                        "__bbox": {
                            "require": [
                                [
                                    "RelayPrefetchedStreamCache",
                                    "next",
                                    [],
                                    [
                                        "adp_PolarisPostCommentsContainerQueryRelayPreloader_native",
                                        {"__bbox": {"complete": True, "sequence_number": 0, "result": native}},
                                    ],
                                ]
                            ]
                        }
                    }
                ],
            ]
        ]
    }
    html = document() + '<script type="application/json" data-sjs>' + json.dumps(stream) + "</script>"
    requests, observed = [], []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, text=html) if request.method == "GET" else httpx.Response(200, json=page())

    source, client = make_client(handler, observer=lambda *args: observed.append(args))
    with client:
        source.bootstrap("Abc123")
        root = source.root_page()
        assert len(requests) == len(observed) == 1
        assert root["cursor_payload"]["relay_doc_id"] == "29631174456466007"
        assert root["cursor_payload"]["page_size"] is None
        source.page(
            media_id="333",
            cursor=root["data"]["xdt_api__v1__media__media_id__comments__connection"]["page_info"]["end_cursor"],
        )
    assert len(requests) == 2
    assert observed[1][1]["cursor_payload"]["request_cursor"] == "native-after-15"


@pytest.mark.parametrize(
    "marker,code",
    [
        ({"success": False}, "source_incomplete"),
        ({"error": 1357001}, "source_auth_failed"),
        ({"rate_limited": True}, "rate_limited"),
    ],
)
def test_root_failure_is_not_removed_by_projection(marker, code):
    result = {**page(), **marker}
    frame = {"__bbox": {"complete": True, "sequence_number": 0, "result": result}}
    inner = [
        "RelayPrefetchedStreamCache",
        "next",
        [],
        ["adp_PolarisPostCommentsContainerQueryRelayPreloader_native", frame],
    ]
    stream = {"require": [["ScheduledServerJS", "handle", None, [{"__bbox": {"require": [inner]}}]]]}
    html = document() + '<script type="application/json" data-sjs>' + json.dumps(stream) + "</script>"
    failures, sends = [], []

    def handler(request):
        sends.append(request)
        return httpx.Response(200, text=html)

    source, client = make_client(handler, failed=lambda *args: failures.append(args))
    with client:
        source.bootstrap("Abc123")
        with pytest.raises(SourceFailure) as error:
            source.root_page()
    assert error.value.failure.code == code
    assert failures == [(code, None)]
    assert len(sends) == 1


def test_parent_root_cannot_be_requested_as_a_pagination_query():
    sends = []

    def handler(request):
        sends.append(request)
        return httpx.Response(200, text=document())

    source, client = make_client(handler)
    with client:
        source.bootstrap("Abc123")
        with pytest.raises(ValueError, match="native_parent_root_requires_bootstrap_page"):
            source.page(media_id="333")
    assert len(sends) == 1


def test_bootstrap_diagnostic_exposes_only_safe_structural_reason():
    html = document() + '<link rel="canonical" href="https://www.instagram.com/p/Wrong/?token=private-token">'
    source, client = make_client(lambda request: httpx.Response(200, text=html))
    with client:
        source.bootstrap("Abc123")
        with pytest.raises(SourceFailure):
            source.root_page()
    assert source.last_diagnostic == {"phase": "comments_bootstrap", "reason": "document_url_mismatch"}
    assert "private-token" not in json.dumps(source.last_diagnostic)


def test_context_diagnostic_exposes_only_static_reason():
    source, client = make_client(lambda request: httpx.Response(200, text="<html>private-token</html>"))
    with client, pytest.raises(SourceFailure):
        source.bootstrap("Abc123")
    assert source.last_diagnostic == {
        "phase": "relay_context",
        "reason": "native_relay_context_missing_or_conflicting",
    }


def test_client_rendered_root_uses_container_operation_without_pagination_variables():
    from urllib.parse import parse_qs

    sends, observed = [], []

    def handler(request):
        sends.append(request)
        return httpx.Response(200, text=document()) if request.method == "GET" else httpx.Response(200, json=page())

    source, client = make_client(handler, observer=lambda *args: observed.append(args))
    with client:
        source.bootstrap("Abc123")
        payload = source.fetch_root_page(media_id="333")
    form = parse_qs(sends[-1].content.decode())
    assert len(sends) == 2
    assert (form["doc_id"][0], form["fb_api_req_friendly_name"][0]) == PARENT_ROOT_DOCUMENT
    assert json.loads(form["variables"][0]) == {
        "media_id": "333",
        "__relay_internal__pv__PolarisIsLoggedInrelayprovider": True,
        "__relay_internal__pv__PolarisThreadsStyleCommentsEnabledrelayprovider": False,
    }
    assert payload["cursor_payload"]["page_size"] is None
    assert payload["cursor_payload"]["request_cursor"] is None
    assert payload["cursor_payload"]["relay_doc_id"] == PARENT_ROOT_DOCUMENT[0]
    assert "secret" not in json.dumps(observed)
