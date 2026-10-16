"""Pure bounded native rejection provenance; no source or database calls."""

import asyncio
import json
from datetime import UTC, datetime

import pytest

from trr_backend.ingestion.comment_transport import PublicRelayCommentSource
from trr_backend.ingestion.comments import CommentSourceError, comment_page_failure
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.public_diagnostics import (
    current_public_comment_failure_basis,
    current_public_diagnostic,
    project_public_comment_failure_basis,
    record_public_comment_failure_basis,
    record_public_diagnostic,
    reset_public_diagnostic,
)


@pytest.mark.parametrize(
    "payload,basis",
    [
        ({"rate_limited": True}, "explicit_rate_true"),
        ({"feedback_required": True}, "explicit_feedback_true"),
        ({"rate_limited": "false"}, "malformed_truthy_rate"),
        ({"feedback_required": {"private": "secret"}}, "malformed_truthy_feedback"),
        ({"data": {"http_status": 429}}, "embedded_http_429"),
        ({"status_code": 429}, "embedded_http_429"),
        ({"errors": [{"message": "Please wait while we process this operation"}]}, "please_wait_message_marker"),
        ({"errors": [{"message": "Too many requests"}]}, "rate_message_marker"),
        ({"errors": [{"extensions": {"code": "feedback_required"}}]}, "feedback_message_marker"),
        ({"errors": [{"extensions": {"error_code": 1675004}}]}, "legacy_rate_code"),
    ],
)
def test_fixed_basis_explains_true_malformed_broad_and_legacy_signals(payload, basis):
    projected = project_public_comment_failure_basis(payload)
    assert projected["bases"] == [basis]
    assert projected["scan_truncated"] is False
    # A diagnostic signal never substitutes for the maintained classifier.
    if basis != "legacy_rate_code":
        assert comment_page_failure(payload)[0] == "rate_limited"
    else:
        assert comment_page_failure(payload)[0] == "source_incomplete"


def test_multierror_projection_keeps_only_fixed_tokens_and_flags():
    secret = "cookie-session-private-sentinel"
    payload = {
        "cookies": {"sessionid": secret},
        "headers": {"authorization": secret},
        "errors": [
            {"message": f"Please wait: {secret}", "severity": "CRITICAL", "extensions": {"error_code": 1675004}},
            {"message": "rate_limit", "extensions": {"feedback_required": "false", "http_status": 429}},
        ],
        "data": {"xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection": {}},
    }
    proof = project_public_comment_failure_basis(payload)
    assert proof["bases"] == [
        "malformed_truthy_feedback",
        "embedded_http_429",
        "rate_message_marker",
        "please_wait_message_marker",
        "legacy_rate_code",
    ]
    assert proof["error_items"] == "multiple" and proof["critical_error"] is True
    assert proof["child_connection_present"] is True
    serialized = json.dumps(proof)
    assert secret not in serialized and "1675004" not in serialized
    assert "cookies" not in serialized and "authorization" not in serialized
    assert all(type(value) in (str, bool, list) for value in proof.values())


@pytest.mark.parametrize("flag", [False, None, "", 0, [], {}])
def test_false_or_empty_flags_are_not_positive_native_signals(flag):
    assert project_public_comment_failure_basis({"rate_limited": flag, "feedback_required": flag})["bases"] == []


def test_identified_comments_and_unrelated_content_do_not_enter_error_projection():
    payload = {
        "data": {"comments": [{"pk": "fixture", "text": "Please wait rate_limit feedback_required"}]},
        "headers": {"message": "rate_limit"},
        "user": {"message": "Please wait"},
    }
    assert project_public_comment_failure_basis(payload)["bases"] == []


def test_projection_reports_bounded_scan_without_storing_long_text_or_native_counts():
    proof = project_public_comment_failure_basis({"errors": [{"message": "private" * 1000}] * 100})
    assert proof["scan_truncated"] is True and proof["error_items"] == "truncated"
    assert proof["bases"] == [] and "private" not in json.dumps(proof)
    assert (
        project_public_comment_failure_basis({"errors": [{"message": "safe", "extensions": {"errors": []}}]})[
            "scan_truncated"
        ]
        is False
    )


def test_first_public_basis_is_copied_reset_and_http_contract_is_unchanged():
    reset_public_diagnostic()
    try:
        record_public_comment_failure_basis({"rate_limited": True}, public_only=False)
        assert current_public_comment_failure_basis() is None
        record_public_comment_failure_basis({"rate_limited": True}, public_only=True)
        record_public_comment_failure_basis({"feedback_required": True}, public_only=True)
        output = current_public_comment_failure_basis()
        output["bases"].append("private-mutation-sentinel")
        assert current_public_comment_failure_basis()["bases"] == ["explicit_rate_true"]
        http = {
            "event": "instagram_public_request_failed",
            "error_code": "rate_limited",
            "status": 200,
            "method": "POST",
            "endpoint_category": "graphql",
        }
        record_public_diagnostic(http)
        assert current_public_diagnostic() == http  # No new HTTP diagnostic keys.
    finally:
        reset_public_diagnostic()
    assert current_public_comment_failure_basis() is None


def test_public_adapter_records_raw_basis_before_sanitization_with_unchanged_failure_callback():
    reset_public_diagnostic()
    raw = {"errors": [{"message": "Please wait private-cookie-sentinel", "severity": "CRITICAL"}]}
    seen, callbacks, sends = [], [], []

    class OfflineFetcher:
        def __init__(self, **kwargs):
            assert kwargs["public_only"] is True and kwargs["cookies"] == [] and kwargs["raw_cookies"] == {}
            self.observer, self.before = kwargs["source_payload_observer"], kwargs["before_source_request"]

        async def fetch_comments_for_shortcode(self, *args, **kwargs):
            self.before("graphql_replies")
            self.observer("graphql_replies", raw)
            pytest.fail("failed native page continued after the existing rejection")

        async def aclose(self):
            pass

    def observe(kind, payload):
        proof = current_public_comment_failure_basis()
        seen.append(proof)
        evidence = Evidence.capture(
            source_identity="fixture", captured_at=datetime.now(UTC), parser_version="fixture", payload=payload
        )
        assert "private-cookie-sentinel" not in evidence.payload_json
        assert "please wait" not in evidence.payload_json.lower()
        assert proof["bases"] == ["please_wait_message_marker"]
        raise CommentSourceError("fixture native page rejected")

    with pytest.raises(CommentSourceError, match="fixture native page rejected"):
        PublicRelayCommentSource(fetcher_factory=OfflineFetcher)(
            shortcode="fixture",
            checkpoint={},
            source_observer=observe,
            pre_source_request=lambda: sends.append("admitted"),
            source_failure_observer=lambda code, delay: callbacks.append((code, delay)),
        )
    assert len(seen) == 1 and callbacks == [("rate_limited", None)] and sends == ["admitted"]
    # The diagnostic is intentionally read inside the async callback.
    assert current_public_comment_failure_basis() is None


def test_async_context_does_not_leak_basis_to_other_attempt():
    reset_public_diagnostic()

    async def one():
        record_public_comment_failure_basis({"rate_limited": True}, public_only=True)
        return current_public_comment_failure_basis()

    assert asyncio.run(one())["bases"] == ["explicit_rate_true"]
    assert current_public_comment_failure_basis() is None
