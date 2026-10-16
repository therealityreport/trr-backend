"""Pure response-policy tests; no source, credentials, storage or database."""

from copy import deepcopy

import pytest

from trr_backend.socials.instagram.graphql_response import (
    GRAPHQL_RESPONSE_POLICY_VERSION,
    MAX_ERROR_ITEMS,
    PROVENANCE_KEY,
    ROOT_FIELD,
    classify_graphql_response,
    project_graphql_provenance,
)


def native_page():
    return {
        "status": "ok",
        "data": {
            ROOT_FIELD: {
                "edges": [
                    {
                        "node": {
                            "id": str(index + 1),
                            "shortcode": "post" + str(index),
                            "owner": {"username": "fixture_account"},
                            "taken_at": 1792152000,
                            "caption": {"text": "login checkpoint permission rate limit are public caption words"},
                            "location": {"id": "place", "profile_pic_url": None},
                        }
                    }
                    for index in range(12)
                ],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        },
    }


def ancillary_error(index=5):
    return {
        "message": "A server field error occurred",
        "severity": "ERROR",
        "path": [ROOT_FIELD, "edges", index, "node", "location", "profile_pic_url"],
    }


def partial_page():
    return {**native_page(), "errors": [ancillary_error()]}


def test_only_known_ancillary_field_is_accepted_with_complete_connection_and_policy_provenance():
    payload = partial_page()
    payload["errors"].append(ancillary_error(8))
    original = deepcopy(payload)
    decision = classify_graphql_response(payload)
    assert decision.accepted and decision.reason is None and not decision.auth_failed
    proof = project_graphql_provenance(payload)
    assert proof["policy_version"] == GRAPHQL_RESPONSE_POLICY_VERSION
    assert proof["error_count"] == 2 and len(proof["errors"]) == 2
    assert proof["errors"][0]["path"] == ancillary_error()["path"]
    assert proof["errors"][0]["index"] == 5
    assert proof["errors"][0]["severity"] == "ERROR"
    assert "message" not in str(proof) and payload == original


@pytest.mark.parametrize(
    "errors",
    [
        None,
        {},
        "source error",
        [None],
        [{}],
        [{"severity": "ERROR"}],
        [ancillary_error(), {}],
        [ancillary_error()] * (MAX_ERROR_ITEMS + 1),
    ],
)
def test_unknown_pathless_malformed_mixed_and_oversized_errors_reject(errors):
    payload = {**native_page(), "errors": errors}
    assert not classify_graphql_response(payload).accepted
    assert project_graphql_provenance(payload)["classification"] == "rejected"


@pytest.mark.parametrize("index", [True, False, -1, 12, "5", None, 2**40])
def test_error_edge_index_must_be_integer_not_bool_and_reference_observed_node(index):
    payload = {**native_page(), "errors": [ancillary_error(index)]}
    assert not classify_graphql_response(payload).accepted


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p["errors"][0].update(path=[ROOT_FIELD, "edges", 5, "node", "caption", "text"]),
        lambda p: p["errors"][0].update(severity="WARNING"),
        lambda p: p["errors"][0].update(severity="error"),
        lambda p: p["data"][ROOT_FIELD]["edges"][5].update(node=None),
        lambda p: p["data"][ROOT_FIELD]["edges"][3].update(node={}),
        lambda p: p["data"][ROOT_FIELD]["page_info"].update(has_next_page="false"),
        lambda p: p["data"][ROOT_FIELD]["page_info"].update(has_next_page=True),
        lambda p: p["data"][ROOT_FIELD].pop("page_info"),
    ],
)
def test_ancillary_error_never_waives_other_field_or_connection_validation(change):
    payload = partial_page()
    change(payload)
    assert not classify_graphql_response(payload).accepted


@pytest.mark.parametrize(
    "marker,auth",
    [
        ({"error": 1357001}, True),
        ({"error": 42}, False),
        ({"status": "fail"}, False),
        ({"message": "  LOGIN_REQUIRED  "}, False),
        ({"message": "User session has expired"}, False),
        ({"message": "Permission denied"}, False),
        ({"error_type": "RATE_LIMITED"}, False),
        ({"message": "Please   wait a few minutes"}, False),
        ({"message": "Too many requests"}, False),
        ({"auth_failed": True}, True),
        ({"permission_denied": True}, True),
        ({"rate_limited": True}, False),
        ({"http_status": 401}, True),
        ({"success": False}, False),
    ],
)
def test_top_failure_auth_permission_and_rate_markers_override_ancillary_path(marker, auth):
    payload = {**partial_page(), **marker}
    decision = classify_graphql_response(payload)
    assert not decision.accepted and decision.auth_failed is auth
    assert project_graphql_provenance(payload)["classification"] == "rejected"


@pytest.mark.parametrize(
    "marker,auth",
    [
        ({"message": "login_required"}, True),
        ({"message": "Your SESSION has EXPIRED"}, False),
        ({"message": "Forbidden: access denied"}, False),
        ({"message": "permission missing"}, False),
        ({"message": "Please wait before trying again"}, False),
        ({"message": "Too many requests"}, False),
        ({"extensions": {"code": "PERMISSION_DENIED"}}, True),
        ({"extensions": {"error_type": "OAuthException"}}, True),
        ({"extensions": {"http_status": 429}}, False),
        ({"extensions": {"code": 1357001}}, True),
    ],
)
def test_error_context_auth_rate_and_permission_markers_are_never_ancillary(marker, auth):
    payload = partial_page()
    payload["errors"][0].update(marker)
    decision = classify_graphql_response(payload)
    assert not decision.accepted and decision.auth_failed is auth
    assert project_graphql_provenance(payload)["classification"] == "rejected"


def test_error_provenance_does_not_retain_arbitrary_paths_messages_traces_or_auth_values():
    payload = {
        **native_page(),
        "errors": [
            {
                "path": ["private-token", "sessionid"],
                "message": "private-token",
                "trace": {"Cookie": "private-token"},
                "severity": "private-token",
                "extensions": {"authorization": "private-token"},
            }
        ],
    }
    proof = project_graphql_provenance(payload)
    assert "private-token" not in str(proof) and "sessionid" not in str(proof)
    assert proof["errors"] == [{"path": [], "index": None, "severity": "OTHER", "classification": "rejected"}]


def test_external_acceptance_flags_do_not_authorize_unknown_errors_or_invalid_indices():
    good = project_graphql_provenance(partial_page())
    payload = {**native_page(), PROVENANCE_KEY: good, "errors": [{"accepted": True}]}
    assert not classify_graphql_response(payload).accepted
    projected = {**native_page(), PROVENANCE_KEY: deepcopy(good)}
    projected[PROVENANCE_KEY]["errors"][0]["path"][2] = 99
    projected[PROVENANCE_KEY]["errors"][0]["index"] = 99
    assert not classify_graphql_response(projected).accepted
    projected[PROVENANCE_KEY] = {"accepted": True}
    assert not classify_graphql_response(projected).accepted


def test_error_provenance_is_bounded_and_connection_direction_is_revalidated():
    payload = partial_page()
    payload["data"][ROOT_FIELD]["page_info"] = {"has_previous_page": False, "start_cursor": None}
    proof = project_graphql_provenance(payload)
    assert classify_graphql_response(payload, reverse=True).accepted
    assert classify_graphql_response({**payload, PROVENANCE_KEY: proof}, reverse=True).accepted
    assert not classify_graphql_response(payload).accepted
    payload["errors"] *= 100
    proof = project_graphql_provenance(payload)
    assert proof["error_count"] == 100 and len(proof["errors"]) == MAX_ERROR_ITEMS
    assert proof["classification"] == "rejected"


def test_no_error_success_preserves_incidental_caption_words_and_missing_root_reason():
    assert classify_graphql_response(native_page()).accepted
    assert classify_graphql_response({**native_page(), "errors": []}).accepted
    assert project_graphql_provenance(native_page()) is None
    assert classify_graphql_response({"status": "ok", "data": {}}).reason == "graphql_empty_connection"
