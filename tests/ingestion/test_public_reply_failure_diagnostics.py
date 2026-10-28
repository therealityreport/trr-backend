"""Normal public child failures retain bounded facts without changing page proof."""

import hashlib
import json

import httpx
import pytest

from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.evidence import Evidence

KEY = "public_reply_failure_diagnostics"


def test_real_public_reply_callback_retains_safe_numeric_facts_and_original_body_hash(monkeypatch):
    retained, calls = [], []
    failed = {
        "status": "fail",
        "errors": [
            {
                "code": 12345,
                "error_code": "67890",
                "error_subcode": 12,
                "severity": "CRITICAL",
                "message": "secret source message",
                "extensions": {"code": 54321, "error_subcode": 42, "token": "secret extension token"},
            }
        ],
        "headers": {"authorization": "secret header"},
    }
    raw = b"for (;;);" + json.dumps(failed, separators=(",", ":")).encode()

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        fields, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append((parent, fields["doc_id"]))
        if parent is None:
            return httpx.Response(
                200, json=public.page([public.actual_parent_shape("bad"), public.actual_parent_shape("good")])
            )
        if parent == "bad":
            return httpx.Response(200, content=raw, headers={"content-type": "application/json"})
        return httpx.Response(200, json=public.page([], child=True))

    def observe(kind, payload):
        retained.append(
            (
                kind,
                Evidence.capture(
                    source_identity="fixture",
                    captured_at=public.OBSERVED,
                    parser_version="fixture",
                    payload=dict(payload),
                ),
            )
        )
        if payload.get("parent_comment_id") == "bad":
            from trr_backend.ingestion.reply_failures import RetainedPublicReplyFailureError

            raise RetainedPublicReplyFailureError("bad")

    fetch, instances = public.source(monkeypatch, handler, expected=2)
    summary = fetch(shortcode="fixture", checkpoint={}, source_observer=observe, pre_source_request=lambda: None)
    failures = [evidence for kind, evidence in retained if kind == "graphql_replies" and "bad" in evidence.payload_json]
    assert failures
    for evidence in failures:
        payload = evidence.read(pinned=True)
        facts = payload[KEY]
        assert facts["failure_class"] == "source_incomplete"
        assert facts["http_status"] == 200 and facts["body_complete"] is True
        assert facts["body_sha256"] == hashlib.sha256(raw).hexdigest()
        assert facts["body_hash_basis"] == "buffered_decoded_response_body"
        assert facts["errors"] == [
            {
                "code": 12345,
                "error_code": 67890,
                "error_subcode": 12,
                "severity": "CRITICAL",
                "extensions": {"code": 54321, "error_subcode": 42},
            }
        ]
        assert "secret" not in evidence.payload_json
    assert any(parent == "good" for parent, _ in calls)
    assert summary.public_reply_failures == ("bad",)
    assert instances[0]._reply_scheduler_telemetry["committed_pages"] == 1
    assert all(
        KEY not in evidence.read(pinned=True)
        for kind, evidence in retained
        if kind != "graphql_replies" or '"parent_comment_id":"good"' in evidence.payload_json
    )


def _failure_payload():
    return public.maintained._durable_relay_payload(
        {"status": "fail", "errors": [{"severity": "CRITICAL", "code": 123}]},
        cursor=None,
        doc_id="36239935742272683",
        friendly_name="Children",
        sort_order="popular",
        page_size=12,
        parent="bad",
    )


def _facts(**changes):
    return {
        "version": 1,
        "http_status": 200,
        "failure_class": "source_incomplete",
        "body_complete": False,
        "body_sha256": None,
        "body_hash_basis": None,
        "envelope": {},
        "errors": [],
        "errors_truncated": False,
        **changes,
    }


def test_bounded_projection_drops_secrets_invalid_numbers_and_nested_extension_data():
    from trr_backend.ingestion.public_reply_failure_diagnostics import MAX_CODE, project_diagnostics

    value = _facts(
        token="secret",
        headers={"cookie": "secret"},
        body_sha256="secret incomplete digest",
        envelope={
            "code": MAX_CODE,
            "error_code": True,
            "error_subcode": -1,
            "severity": {"token": "secret"},
            "extensions": {
                "code": "123",
                "error_code": MAX_CODE + 1,
                "error_subcode": "9" * 1000,
                "message": "secret",
                "extensions": {"code": 1},
            },
        },
        errors=[
            {
                "code": "24",
                "error_code": 3.0,
                "error_subcode": [123],
                "severity": "secret",
                "message": "secret",
                "extensions": {"error_code": 0, "authorization": "secret"},
            }
        ]
        * 33,
    )
    result = project_diagnostics(value)
    assert result is not None
    assert result["envelope"] == {"code": MAX_CODE, "severity": "OTHER", "extensions": {"code": 123}}
    assert result["body_sha256"] is None and result["body_complete"] is False
    assert result["errors_truncated"] is True and len(result["errors"]) == 32
    assert result["errors"] == [{"code": 24, "severity": "OTHER", "extensions": {"error_code": 0}}] * 32
    assert "secret" not in json.dumps(result)
    assert project_diagnostics(result) == result


def test_invalid_diagnostic_schema_cannot_enter_evidence():
    from trr_backend.ingestion.public_reply_failure_diagnostics import project_diagnostics

    for changes in [
        {"version": True},
        {"version": 2},
        {"http_status": True},
        {"http_status": 600},
        {"failure_class": "source unavailable secret"},
        {"body_complete": 1},
        {"errors": "secret"},
        {"body_complete": True, "body_sha256": None},
        {"body_complete": True, "body_sha256": "X" * 64},
    ]:
        assert project_diagnostics(_facts(**changes)) is None
        captured = Evidence.capture(
            source_identity="fixture",
            captured_at=public.OBSERVED,
            parser_version="fixture",
            payload={**_failure_payload(), KEY: _facts(**changes)},
        )
        assert KEY not in captured.read(pinned=True)


def test_complete_body_hash_uses_spool_chunks_only_and_incomplete_body_is_not_read():
    from types import SimpleNamespace

    from trr_backend.ingestion.public_reply_failure_diagnostics import capture_diagnostics

    class Spool:
        calls = 0

        def iter_chunks(self):
            self.calls += 1
            for _ in range(128):
                yield b"x" * 65536

    spool = Spool()
    response = SimpleNamespace(status_code=200, body_complete=True, spool=spool)
    result = capture_diagnostics({"errors": []}, response, failure_class="source_incomplete")
    digest = hashlib.sha256()
    for _ in range(128):
        digest.update(b"x" * 65536)
    assert result["body_sha256"] == digest.hexdigest() and spool.calls == 1
    response.body_complete = False
    result = capture_diagnostics({}, response, failure_class="source_incomplete")
    assert result["body_complete"] is False and result["body_sha256"] is None and spool.calls == 1
    response = SimpleNamespace(status_code=200)
    assert capture_diagnostics({}, response, failure_class="source_incomplete")["body_sha256"] is None


def test_failure_roundtrip_is_idempotent_and_success_bytes_keep_legacy_identity():
    from trr_backend.ingestion.domain import fingerprint, stable_json
    from trr_backend.ingestion.evidence import sanitize_payload

    failed = Evidence.capture(
        source_identity="fixture",
        captured_at=public.OBSERVED,
        parser_version="fixture",
        payload={**_failure_payload(), KEY: _facts()},
    )
    retained = failed.read(pinned=True)
    assert retained[KEY] == _facts() and sanitize_payload(retained) == retained
    assert failed.content_hash == fingerprint(retained)
    success = public.maintained._durable_relay_payload(
        public.page([], child=True),
        cursor=None,
        doc_id="36239935742272683",
        friendly_name="Children",
        sort_order="popular",
        page_size=12,
        parent="good",
    )
    evidence = Evidence.capture(
        source_identity="fixture",
        captured_at=public.OBSERVED,
        parser_version="fixture",
        payload={**success, KEY: _facts()},
    )
    assert evidence.payload_json == stable_json(success) and evidence.content_hash == fingerprint(success)
    assert KEY not in evidence.read(pinned=True)
    for changed in [{"parent_comment_id": None}, {"cursor_payload": {"adapter_family": "authenticated"}}]:
        captured = Evidence.capture(
            source_identity="fixture",
            captured_at=public.OBSERVED,
            parser_version="fixture",
            payload={**_failure_payload(), KEY: _facts(), **changed},
        )
        assert KEY not in captured.read(pinned=True)


def test_diagnostic_variation_preserves_exact_legacy_failure_fingerprint_and_hold_continuity():
    from datetime import timedelta

    from trr_backend.ingestion.domain import fingerprint
    from trr_backend.ingestion.reply_failures import record_failure

    payload = _failure_payload()
    old = Evidence.capture(
        source_identity="fixture", captured_at=public.OBSERVED, parser_version="fixture", payload=payload
    )
    previous = record_failure(
        None, payload=payload, evidence=old, evidence_id="old", owner_generation=1, now=public.OBSERVED
    )
    original_hash = previous["request_fingerprint"]
    assert original_hash == fingerprint(
        {"query": payload["cursor_payload"], "parent": "bad", "content_hash": old.content_hash}
    )
    hashes = {old.content_hash}
    for index, code in enumerate((111, 222, 333), 2):
        changed = {**payload, KEY: _facts(errors=[{"code": code, "severity": "CRITICAL"}])}
        captured = Evidence.capture(
            source_identity="fixture", captured_at=public.OBSERVED, parser_version="fixture", payload=changed
        )
        before = captured.payload_json
        hashes.add(captured.content_hash)
        previous = record_failure(
            previous,
            payload=changed,
            evidence=captured,
            evidence_id=f"new-{index}",
            owner_generation=2,
            now=public.OBSERVED,
        )
        assert previous["request_fingerprint"] == original_hash
        assert previous["failure_count"] == index and previous["technical_hold"] == (index >= 3)
        assert previous["content_hash"] == captured.content_hash and captured.payload_json == before
        assert previous["diagnostic_version"] == 1  # pyright: ignore[reportTypedDictNotRequiredAccess]
        assert previous["next_attempt_at"] == (public.OBSERVED + timedelta(seconds=60 * 2 ** (index - 1))).isoformat()
    assert len(hashes) == 4
    # Only this validated new metadata is excluded. Native source changes retain
    # their existing effect on the failure identity.
    changed = {**payload, "status": "error", KEY: _facts()}
    captured = Evidence.capture(
        source_identity="fixture", captured_at=public.OBSERVED, parser_version="fixture", payload=changed
    )
    native_changed = record_failure(
        None, payload=changed, evidence=captured, evidence_id="native-changed", owner_generation=2, now=public.OBSERVED
    )
    assert native_changed["request_fingerprint"] != original_hash


@pytest.mark.parametrize("state", ["old_eligible", "new_eligible", "held", "future"])
def test_normal_resume_prioritizes_only_eligible_missing_diagnostic_version(monkeypatch, state):
    from datetime import timedelta

    from trr_backend.ingestion.comments import CommentResumeNode
    from trr_backend.ingestion.reply_failures import record_failure

    calls = []
    payload = _failure_payload()
    evidence = Evidence.capture(
        source_identity="fixture", captured_at=public.OBSERVED, parser_version="fixture", payload=payload
    )
    technical = record_failure(
        None, payload=payload, evidence=evidence, evidence_id="old-native", owner_generation=1, now=public.OBSERVED
    )
    if state == "new_eligible":
        technical["diagnostic_version"] = 1
    elif state == "held":
        technical["technical_hold"] = True
    elif state == "future":
        technical["next_attempt_at"] = (public.OBSERVED + timedelta(hours=1)).isoformat()
    checkpoint = {
        "comment_pages": {
            "adapter_family": "instagram_public_relay",
            "adapter_version": 1,
            "observed": {name: {"expected_replies": None} for name in ("bad", "good", "bad-two")},
            "lanes": {
                "__top__": {
                    "exhausted": True,
                    "started_from_root": True,
                    "relay_doc_id": "top-doc",
                    "relay_friendly_name": "Top",
                    "sort_order": "recent",
                    "page_size": 12,
                }
            },
        },
        "public_reply_failures": {"bad": technical, "bad-two": {**technical, "parent_comment_id": "bad-two"}},
    }
    inventory = tuple(
        CommentResumeNode(public.actual_parent_shape(name), public.OBSERVED) for name in ("bad", "good", "bad-two")
    )

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        assert parent is not None  # top lane is already proved exhausted
        calls.append(parent)
        return httpx.Response(200, json=public.page([public.node(parent + "-reply", 0)], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=None)

    def before():
        pass

    before.observation_clock = lambda: public.OBSERVED + timedelta(minutes=5)  # pyright: ignore[reportFunctionMemberAccess]
    fetch(
        shortcode="fixture",
        checkpoint=checkpoint,
        resume_inventory=inventory,
        source_observer=lambda *args: None,
        pre_source_request=before,
    )
    if state == "old_eligible":
        assert calls == ["bad", "bad-two", "good"]
    elif state == "new_eligible":
        assert calls == ["good", "bad", "bad-two"]
    else:
        assert calls == ["good"]
    assert checkpoint["public_reply_failures"]["bad"] == technical


def test_qualified_retention_retires_priority_without_resetting_hold_or_waits():
    from datetime import timedelta

    from trr_backend.ingestion.reply_failures import record_failure, resume_failure

    payload = _failure_payload()
    old = Evidence.capture(
        source_identity="fixture", captured_at=public.OBSERVED, parser_version="fixture", payload=payload
    )
    initial = record_failure(
        None, payload=payload, evidence=old, evidence_id="old", owner_generation=1, now=public.OBSERVED
    )
    now = public.OBSERVED + timedelta(minutes=5)
    assert resume_failure(initial, now=now)["needs_diagnostics"] is True
    qualified_payload = {**payload, KEY: _facts()}
    qualified = Evidence.capture(
        source_identity="fixture", captured_at=now, parser_version="fixture", payload=qualified_payload
    )
    updated = record_failure(
        initial, payload=qualified_payload, evidence=qualified, evidence_id="new", owner_generation=2, now=now
    )
    assert updated["diagnostic_version"] == 1 and updated["failure_count"] == 2  # pyright: ignore[reportTypedDictNotRequiredAccess]
    assert updated["request_fingerprint"] == initial["request_fingerprint"]
    assert "needs_diagnostics" not in resume_failure(updated, now=now + timedelta(days=1))
    for changed in [{"technical_hold": True}, {"next_attempt_at": (now + timedelta(hours=1)).isoformat()}]:
        resumed = resume_failure({**initial, **changed}, now=now)
        assert resumed["deferred"] is True and resumed["pending"] is False
        assert "needs_diagnostics" not in resumed
    # No qualified evidence was retained: the usual future retry remains
    # eligible for priority after its unchanged wait; this grants no new send.
    assert resume_failure(initial, now=now)["pending"] is True
