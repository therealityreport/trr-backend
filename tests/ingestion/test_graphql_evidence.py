"""Pure evidence/listing/replay policy parity; no database or source clients."""

from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from tests.socials.instagram.test_graphql_response import ancillary_error, native_page, partial_page
from trr_backend.ingestion.domain import Stage, fingerprint, stable_json
from trr_backend.ingestion.evidence import Evidence, sanitize_payload
from trr_backend.ingestion.listing import parse_listing_page
from trr_backend.ingestion.providers import PARSER_VERSION, InvalidSourceError
from trr_backend.ingestion.replay import _listing, prepare_replay
from trr_backend.socials.instagram.graphql_response import PROVENANCE_KEY, classify_graphql_response

OBSERVED = datetime(2026, 10, 16, 12, tzinfo=UTC)


def evidence(payload):
    return Evidence.capture(
        source_identity="instagram:listing:fixture_account:page:root",
        captured_at=OBSERVED,
        parser_version=PARSER_VERSION,
        payload=payload,
    )


def test_safe_partial_response_round_trips_original_observation_and_all_post_identities():
    raw = partial_page()
    raw["errors"][0].update(message="A server field error occurred: do-not-retain", trace="do-not-retain")
    captured = evidence(raw)
    retained = captured.read(pinned=True)
    assert captured.captured_at == OBSERVED
    assert "do-not-retain" not in captured.payload_json
    assert "errors" not in retained and retained[PROVENANCE_KEY]["error_count"] == 1
    assert sanitize_payload(retained) == retained
    assert classify_graphql_response(retained).accepted
    page = parse_listing_page(retained)
    assert len(page.posts) == 12 and page.posts[5]["shortcode"] == "post5"
    assert page.posts[5]["location"]["profile_pic_url"] is None
    parsed = _listing(
        captured, account="fixture_account", target_key="page:root", parser_version=PARSER_VERSION, pinned=True
    )
    assert parsed.observed_at == OBSERVED and len(parsed.posts) == 12
    assert [post.target_key for post in parsed.posts] == ["post" + str(index) for index in range(12)]
    assert all(post.observed_at == OBSERVED for post in parsed.posts)
    preview, replayed = prepare_replay(
        str(uuid4()),
        "fixture_account",
        {
            "evidence": captured,
            "stage": Stage.LISTING,
            "target_key": "page:root",
            "run_id": str(uuid4()),
            "pinned": True,
        },
        parser_version=PARSER_VERSION,
    )
    assert preview["writes"] == preview["source_requests"] == 0
    assert len(replayed.posts) == 12 and replayed.observed_at == OBSERVED  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p.update(errors=[{"message": "unknown do-not-retain", "path": ["do-not-retain"]}]),
        lambda p: p["errors"].append({"message": "unknown"}),
        lambda p: p["errors"][0].update(path=["other", "do-not-retain"]),
        lambda p: p["errors"][0].update(message="LOGIN_REQUIRED do-not-retain"),
        lambda p: p["errors"][0].update(message="Please wait do-not-retain"),
        lambda p: p["errors"][0].update(extensions={"code": "PERMISSION_DENIED", "access_token": "do-not-retain"}),
        lambda p: p["errors"][0].update(path=ancillary_error(True)["path"]),
        lambda p: p["errors"][0].update(path=ancillary_error(12)["path"]),
        lambda p: p.update(error=1357001),
        lambda p: p.update(message="User session has expired do-not-retain"),
        lambda p: p.update(http_status=401),
        lambda p: p.update(errors=None),
    ],
)
def test_rejected_raw_errors_stay_rejected_after_sanitization_and_offline_replay(change):
    payload = partial_page()
    change(payload)
    assert not classify_graphql_response(payload).accepted
    captured = evidence(payload)
    retained = captured.read(pinned=True)
    assert "do-not-retain" not in captured.payload_json
    assert retained[PROVENANCE_KEY]["classification"] == "rejected"
    assert sanitize_payload(retained) == retained
    assert not classify_graphql_response(retained).accepted
    with pytest.raises(InvalidSourceError):
        parse_listing_page(retained)
    with pytest.raises(InvalidSourceError):
        _listing(
            captured, account="fixture_account", target_key="page:root", parser_version=PARSER_VERSION, pinned=True
        )


def test_legacy_evidence_stays_readable_without_schema_or_hash_change():
    payload = native_page()
    legacy = Evidence(
        "instagram:listing:fixture_account:page:root",
        OBSERVED,
        PARSER_VERSION,
        stable_json(payload),
        fingerprint(payload),
    )
    assert legacy.schema_version == 1 and legacy.read(pinned=True) == payload
    assert PROVENANCE_KEY not in legacy.read(pinned=True)
    old_post = evidence({"shortcode": "single", "pk": "1", "owner": {"username": "fixture_account"}})
    parsed = _listing(
        old_post, account="fixture_account", target_key="single", parser_version=PARSER_VERSION, pinned=True
    )
    assert len(parsed.posts) == 1 and parsed.posts[0].target_key == "single"


@pytest.mark.parametrize("marker", [{"error": 1357001}, {"status": "fail"}])
def test_projection_cannot_turn_bad_legacy_single_post_envelope_into_replay_success(marker):
    captured = evidence({"shortcode": "single", "pk": "1", "owner": {"username": "fixture_account"}, **marker})
    with pytest.raises(InvalidSourceError):
        _listing(captured, account="fixture_account", target_key="single", parser_version=PARSER_VERSION, pinned=True)


@pytest.mark.parametrize(
    "marker",
    [{"http_status": 401}, {"error": 1357001}, {"message": "User session has expired"}, {"rate_limited": True}],
)
def test_dropped_top_rejection_without_errors_is_preserved_in_evidence(marker):
    payload = {**native_page(), **marker}
    assert not classify_graphql_response(payload).accepted
    safe = evidence(payload).read(pinned=True)
    assert not classify_graphql_response(safe).accepted
    assert safe[PROVENANCE_KEY]["classification"] == "rejected"


def test_forged_acceptance_flag_and_tampered_error_count_cannot_authorize_replay():
    payload = evidence(partial_page()).read(pinned=True)
    payload[PROVENANCE_KEY]["accepted"] = True
    assert not classify_graphql_response(payload).accepted
    payload = evidence(partial_page()).read(pinned=True)
    payload[PROVENANCE_KEY]["error_count"] = 0
    assert not classify_graphql_response(payload).accepted
    payload = evidence(partial_page()).read(pinned=True)
    payload[PROVENANCE_KEY]["errors"][0]["index"] = True
    assert not classify_graphql_response(payload).accepted
    original = deepcopy(payload)
    assert sanitize_payload(sanitize_payload(payload)) == sanitize_payload(payload)
    assert payload == original


@pytest.mark.parametrize("extensions", [False, True], ids=["direct-error", "extensions"])
@pytest.mark.parametrize(
    "marker,reason,auth",
    [
        ({"auth_failed": True}, "source_auth_failed", True),
        ({"login_required": True}, "source_auth_failed", True),
        ({"checkpoint_required": True}, "source_auth_failed", True),
        ({"challenge_required": True}, "source_auth_failed", True),
        ({"permission_denied": True}, "source_auth_failed", True),
        ({"permission_denied": "false"}, "graphql_error", False),
        ({"rate_limited": True}, "rate_limited", False),
        ({"error": 1357001}, "login_required", True),
        ({"error": 42}, "graphql_error", False),
        ({"failed": True}, "graphql_error", False),
        ({"success": False}, "graphql_error", False),
        ({"status": "fail"}, "fail", False),
        ({"status": "error"}, "error", False),
        ({"http_status": 401}, "source_auth_failed", True),
        ({"http_status": 403}, "source_auth_failed", True),
        ({"status_code": 429}, "rate_limited", False),
    ],
)
def test_error_context_machine_failure_cannot_become_accepted_evidence_or_replay(extensions, marker, reason, auth):
    payload = partial_page()
    payload["errors"][0].update({"extensions": marker} if extensions else marker)
    original = deepcopy(payload)
    decision = classify_graphql_response(payload)
    assert not decision.accepted and decision.reason == reason and decision.auth_failed is auth
    with pytest.raises(InvalidSourceError):
        parse_listing_page(payload)
    projected = sanitize_payload(payload)
    assert projected[PROVENANCE_KEY]["classification"] == "rejected"
    assert projected[PROVENANCE_KEY]["errors"][0]["classification"] == "rejected"
    assert projected[PROVENANCE_KEY]["errors"][0]["path"] == ancillary_error()["path"]
    assert sanitize_payload(projected) == projected
    captured = evidence(payload)
    retained = captured.read(pinned=True)
    assert retained == projected and not classify_graphql_response(retained).accepted
    with pytest.raises(InvalidSourceError):
        parse_listing_page(retained)
    with pytest.raises(InvalidSourceError):
        _listing(
            captured, account="fixture_account", target_key="page:root", parser_version=PARSER_VERSION, pinned=True
        )
    assert payload == original


def test_false_machine_markers_preserve_the_known_ancillary_response():
    payload = partial_page()
    payload["errors"][0]["extensions"] = dict.fromkeys(
        (
            "auth_failed",
            "login_required",
            "checkpoint_required",
            "challenge_required",
            "permission_denied",
            "rate_limited",
        ),
        False,
    )
    assert classify_graphql_response(payload).accepted
    assert classify_graphql_response(evidence(payload).read(pinned=True)).accepted
