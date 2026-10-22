"""Synthetic native ServerJS wrappers; no HTTP or credential fixtures."""

import json
from copy import deepcopy

import pytest

from trr_backend.ingestion.authenticated_bootstrap import (
    CONTAINER_PRELOADER_PREFIX,
    PARENT_CONNECTION,
    AuthenticatedBootstrapError,
    extract_parent_bootstrap,
)

CODE = "Dd37XeDly4U"
URL = f"https://www.instagram.com/p/{CODE}/"


def frame():
    return {
        "complete": True,
        "sequence_number": 0,
        "result": {
            "data": {
                PARENT_CONNECTION: {
                    "edges": [{"node": {"pk": str(18000000000000000 + n), "text": "native"}} for n in range(15)],
                    "page_info": {"has_next_page": True, "end_cursor": "native-container-end"},
                }
            },
            "extensions": {"csrf_token": "do-not-retain"},
        },
    }


def entry(value=None, key=CONTAINER_PRELOADER_PREFIX + "6ad95f2fd135d4146214030"):
    return ["RelayPrefetchedStreamCache", "next", [], [key, {"__bbox": frame() if value is None else value}]]


def document(*entries, metadata=True, outer_name="ScheduledServerJS"):
    box = {"__bbox": {"require": list(entries)}}
    root = {"require": [[outer_name, "handle", None, [box]]]}
    tags = (
        f'<link rel="canonical" href="{URL}"><meta property="og:url" content="https://www.instagram.com/bravotv/p/{CODE}/">'
        if metadata
        else ""
    )
    return tags + '<script type="application/json" data-sjs>' + json.dumps(root) + "</script>"


def extract(html, **kwargs):
    return extract_parent_bootstrap(html, shortcode=CODE, document_url=kwargs.get("document_url", URL))


def test_complete_native_initial_connection_keeps_fifteen_edges_and_actual_cursor():
    value = frame()
    value["result"]["data"][PARENT_CONNECTION]["sessionid"] = "do-not-retain"
    native = deepcopy(value)
    result = extract(document(entry(value)))
    connection = result["data"][PARENT_CONNECTION]
    assert len(connection["edges"]) == 15
    assert connection["page_info"] == {"has_next_page": True, "end_cursor": "native-container-end"}
    assert "do-not-retain" not in json.dumps(result)
    assert set(result) == {"data"} and "cursor_payload" not in result
    assert value == native


def test_explicit_known_get_url_binds_document_without_optional_metadata():
    assert len(extract(document(entry(), metadata=False))["data"][PARENT_CONNECTION]["edges"]) == 15


@pytest.mark.parametrize("changed", ["canonical", "og:url"])
def test_conflicting_document_metadata_is_rejected(changed):
    html = document(entry())
    needle = URL if changed == "canonical" else f"https://www.instagram.com/bravotv/p/{CODE}/"
    html = html.replace(needle, "https://www.instagram.com/p/OtherCode/", 1)
    with pytest.raises(AuthenticatedBootstrapError):
        extract(html)


@pytest.mark.parametrize(
    "url",
    [
        "",
        "https://www.instagram.com/p/OtherCode/",
        "http://www.instagram.com/p/" + CODE,
        "https://example.com/p/" + CODE,
        URL + "?sessionid=do-not-retain",
    ],
)
def test_wrong_or_unbound_get_url_is_rejected(url):
    with pytest.raises(AuthenticatedBootstrapError):
        extract(document(entry()), document_url=url)


@pytest.mark.parametrize("entries", [(entry(), entry()), (entry(key="adp_UnrelatedQuery_123"),), ()])
def test_multiple_missing_or_wrong_preloader_is_rejected(entries):
    with pytest.raises(AuthenticatedBootstrapError):
        extract(document(*entries))


@pytest.mark.parametrize(
    "change",
    [
        "incomplete",
        "patch",
        "boolean_sequence",
        "missing_sequence",
        "errors",
        "auth_failure",
        "rate_limited",
        "incremental",
        "missing_connection",
        "missing_cursor",
        "bad_flag",
        "malformed_edge",
    ],
)
def test_incomplete_or_malformed_native_frame_is_rejected(change):
    value = frame()
    connection = value["result"]["data"][PARENT_CONNECTION]
    if change == "incomplete":
        value["complete"] = False
    elif change == "patch":
        value["sequence_number"] = 1
    elif change == "boolean_sequence":
        value["sequence_number"] = False
    elif change == "missing_sequence":
        value.pop("sequence_number")
    elif change == "errors":
        value["result"]["errors"] = [{"message": "do-not-retain"}]
    elif change == "auth_failure":
        value["result"]["auth_failed"] = True
    elif change == "rate_limited":
        value["result"]["rate_limited"] = True
    elif change == "incremental":
        value["result"]["incremental"] = []
    elif change == "missing_connection":
        value["result"]["data"] = {}
    elif change == "missing_cursor":
        connection["page_info"]["end_cursor"] = None
    elif change == "bad_flag":
        connection["page_info"]["has_next_page"] = 1
    else:
        connection["edges"] = [{"node": None}]
    with pytest.raises(AuthenticatedBootstrapError, match="^native_comments_bootstrap_invalid$"):
        extract(document(entry(value)))


def test_nested_preloader_lookalike_is_not_selected():
    nested = ["Unrelated", "call", None, [{"__bbox": {"require": [entry()]}}]]
    with pytest.raises(AuthenticatedBootstrapError):
        extract(document(nested))


def test_wrong_serverjs_wrapper_is_not_selected():
    with pytest.raises(AuthenticatedBootstrapError):
        extract(document(entry(), outer_name="Unrelated"))


@pytest.mark.parametrize(
    "inner", [["RelayPrefetchedStreamCache", "next", []], ["RelayPrefetchedStreamCache", "next", [], []]]
)
def test_malformed_stream_wrapper_is_rejected(inner):
    with pytest.raises(AuthenticatedBootstrapError):
        extract(document(inner))


def test_document_size_is_bounded(monkeypatch):
    from trr_backend.ingestion import authenticated_bootstrap

    monkeypatch.setattr(authenticated_bootstrap, "_MAX_DOCUMENT_BYTES", 20)
    with pytest.raises(AuthenticatedBootstrapError):
        extract(document(entry()))


def test_empty_exhausted_native_connection_is_valid():
    value = frame()
    value["result"]["data"][PARENT_CONNECTION] = {
        "edges": [],
        "page_info": {"has_next_page": False, "end_cursor": None},
    }
    assert extract(document(entry(value)))["data"][PARENT_CONNECTION]["edges"] == []


@pytest.mark.parametrize(
    ("markers", "failure_code", "retry_after_seconds"),
    [
        ({"success": False}, "source_incomplete", None),
        ({"failed": True}, "source_incomplete", None),
        ({"error": 1357001}, "source_auth_failed", None),
        ({"auth_failed": True}, "source_auth_failed", None),
        ({"rate_limited": True, "retry_after": 17}, "rate_limited", 17),
        ({"error": "do-not-retain", "rate_limited": True, "retry_after": 23}, "rate_limited", 23),
        ({"error": "do-not-retain", "auth_failed": True}, "source_auth_failed", None),
        ({"rate_limited": True, "retry_after": -1}, "rate_limited", None),
    ],
)
def test_native_failure_is_rejected_before_projection_with_safe_classification(
    markers, failure_code, retry_after_seconds
):
    value = frame()
    value["result"].update(markers)
    with pytest.raises(AuthenticatedBootstrapError, match="^native_comments_bootstrap_invalid$") as raised:
        extract(document(entry(value)))
    assert raised.value.failure_code == failure_code
    assert raised.value.retry_after_seconds == retry_after_seconds
    assert "do-not-retain" not in str(raised.value)


def test_structural_bootstrap_failure_defaults_to_incomplete_without_retry():
    with pytest.raises(AuthenticatedBootstrapError) as raised:
        extract(document())
    assert raised.value.failure_code == "source_incomplete"
    assert raised.value.retry_after_seconds is None
