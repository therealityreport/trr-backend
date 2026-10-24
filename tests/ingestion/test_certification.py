"""Offline strict acceptance, preserved auditor regressions, no external calls."""

import hashlib
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts.ingestion.certify_january import storage_binding
from scripts.ingestion.january_audit import common, hosted, offline_comments, posts
from scripts.ingestion.january_audit.offline_hosted import Offline as TestHostedBytes  # noqa: F401
from scripts.ingestion.january_audit.offline_media import MediaAuditOffline as TestMediaRecords  # noqa: F401
from trr_backend.ingestion.certification import certify, inventory_fingerprint

# Preserve the existing producer/consumer certificate regression classes too.
for _name, _value in vars(offline_comments).items():
    if isinstance(_value, type) and _value.__module__ == offline_comments.__name__:
        globals()["TestRetained" + _name] = _value

del _name, _value


@pytest.fixture(autouse=True)
def offline_identity():
    value = common.AuditContext(
        "11111111-1111-4111-8111-111111111111",
        "a" * 64,
        "bravotv",
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
    )
    with (
        common.bound(value),
        patch("socket.socket.connect", side_effect=AssertionError("offline_guard_external_call")),
        patch("psycopg2.connect", side_effect=AssertionError("offline_guard_external_call")),
    ):
        yield


def evidence():
    identity = {"run_id": common.RUN, "intent_hash": common.INTENT, "full_history_gate_closed": True}
    listing = {
        **identity,
        "reported_january_keys": ["ABC"],
        "independently_verified_january_count": 1,
        "native_membership": {"ABC": {"native_id": "123"}},
        "mismatches": [],
        "listing_proof": {"independent_native_membership_verified": True, "boundary_verified": True},
        "gates": {"lifecycle": {"status": "passed"}},
        "run_state": "completed",
    }
    reports = {"listing": listing}
    for name in ("posts", "comments", "media"):
        reports[name] = {
            **identity,
            "status": "passed",
            "failures": [],
            "pending": [],
            "audited_january_count": 1,
            "verified": [{"shortcode": "ABC"}],
        }
    reports["comments"].update(
        count_gaps=[],
        global_validation={"status": "passed"},
        target_results=[
            {
                "shortcode": "ABC",
                "status": "passed",
                "diagnostics": [],
                "settlement": {"count_gap": 0},
                "source_page_evidence_complete": True,
            }
        ],
    )
    reports["media"].update(comment_proof_mode="strict_aggregate")
    reports["media"]["verified"][0]["inventory_fingerprint"] = {
        "version": "instagram-media-inventory-v1",
        "sha256": "b" * 64,
    }
    plan = [{"storage_key": "a.png", "roles": [{"shortcode": "ABC", "role": "cover_original"}]}]
    before = {"reports": reports, "public_only": True, "query_fingerprint": "c" * 64, "physical_plan": plan}
    byte_report = {
        "status": "passed",
        "failures": [],
        "objects": [
            {
                "object_signature": posts.digest(plan[0]),
                "storage_key": "a.png",
                "roles": plan[0]["roles"],
                "status": "verified",
                "bytes": 10,
                "observed_sha256": "d" * 64,
                "storage_cdn_full_byte_equality": True,
                "decode": {"all_frames_decoded": True},
            }
        ],
    }
    return before, deepcopy(before), byte_report


def acceptance(before, after, byte_report, **changes):
    options = {
        "storage_binding_sha256": "1" * 64,
        "storage_binding_after_sha256": "1" * 64,
        "source_sha256": "2" * 64,
        "source_after_sha256": "2" * 64,
    }
    return certify(before, after, byte_report, **{**options, **changes})


def test_complete_unchanged_evidence_can_pass():
    result = acceptance(*evidence())
    assert result["final_acceptance"] is True
    assert all(gate["status"] == "passed" for gate in result["gates"].values())


@pytest.mark.parametrize(
    "mutation",
    [
        lambda b: b["reports"]["listing"].update(reported_january_keys=[]),
        lambda b: b["reports"]["listing"].update(mismatches=[{"code": "missing"}]),
        lambda b: b["reports"]["listing"].update(run_state="running"),
        lambda b: b["reports"]["posts"]["verified"].clear(),
        lambda b: b["reports"]["comments"]["count_gaps"].append({"count_gap": 1}),
        lambda b: b["reports"]["comments"]["target_results"][0]["settlement"].update(count_gap=1),
        lambda b: b["reports"]["comments"]["target_results"][0].update(source_page_evidence_complete=False),
        lambda b: b["reports"]["media"].update(comment_proof_mode="verified_target_subset"),
        lambda b: b["reports"]["media"]["verified"][0].pop("inventory_fingerprint"),
        lambda b: b.update(public_only=False),
    ],
)
def test_failed_gate_never_certifies(mutation):
    before, after, byte_report = evidence()
    mutation(before)
    mutation(after)
    assert acceptance(before, after, byte_report)["final_acceptance"] is False


@pytest.mark.parametrize("change", ["query", "source", "binding", "membership", "media"])
def test_changed_evidence_invalidates_acceptance(change):
    before, after, byte_report = evidence()
    options = {}
    if change == "query":
        after["query_fingerprint"] = "e" * 64
    if change == "source":
        options["source_after_sha256"] = "e" * 64
    if change == "binding":
        options["storage_binding_after_sha256"] = "e" * 64
    if change == "membership":
        after["reports"]["listing"]["native_membership"]["ABC"]["native_id"] = "999"
    if change == "media":
        after["reports"]["media"]["verified"][0]["inventory_fingerprint"]["sha256"] = "e" * 64
    assert acceptance(before, after, byte_report, **options)["gates"]["unchanged_evidence"]["status"] == "blocked"


@pytest.mark.parametrize("change", ["missing", "duplicate", "partial", "decode", "signature", "parent_role"])
def test_incomplete_or_unbound_bytes_never_certify(change):
    before, after, byte_report = evidence()
    if change == "missing":
        byte_report["objects"].clear()
    if change == "duplicate":
        byte_report["objects"].append(deepcopy(byte_report["objects"][0]))
    if change == "partial":
        byte_report["objects"][0]["storage_cdn_full_byte_equality"] = False
    if change == "decode":
        byte_report["objects"][0]["decode"] = {}
    if change == "signature":
        byte_report["objects"][0]["object_signature"] = "e" * 64
    if change == "parent_role":
        byte_report["objects"][0]["roles"] = [{"shortcode": "OTHER", "role": "cover_original"}]
    assert acceptance(before, after, byte_report)["final_acceptance"] is False


def test_independent_inventory_matches_contract_and_tracks_text_only_parent():
    from trr_backend.ingestion.media import inventory_fingerprint as maintained

    post = {
        "id": "p",
        "shortcode": "ABC",
        "media_urls": ["https://cdn.example/a.jpg?token=private"],
        "scraped_at": datetime(2026, 1, 1, tzinfo=UTC),
        "raw_data": {"owner": {"name": "é"}},
    }
    comments = [{"id": "row", "comment_id": "123", "parent_comment_id": None, "media_urls": []}]
    expected = maintained(post, comments)
    assert inventory_fingerprint(post, comments) == expected
    post["media_urls"] = ["https://cdn.example/a.jpg?token=rotated"]
    assert inventory_fingerprint(post, comments) == expected
    comments[0]["parent_comment_id"] = "parent"
    assert inventory_fingerprint(post, comments) != expected


def test_multiple_verified_public_aliases_and_unknown_url():
    aliases = ["https://media.example", "https://pub-approved.r2.dev/prefix"]
    assert (
        hosted.object_key("https://pub-approved.r2.dev/prefix/social/a.png", "social/a.png", aliases) == "social/a.png"
    )
    with pytest.raises(common.GuardError):
        hosted.object_key("https://pub-other.r2.dev/prefix/social/a.png", "social/a.png", aliases)


def test_storage_binding_requires_account_bucket_endpoint_and_verified_alias(tmp_path):
    account = "a" * 32
    binding = {
        "version": 1,
        "provider": "r2",
        "account_id": account,
        "bucket": "trr-media-prod",
        "endpoint_url": f"https://{account}.r2.cloudflarestorage.com",
        "verified_at": "2026-01-01T00:00:00+00:00",
        "public_bases": [{"url": "https://pub-approved.r2.dev", "verified": True}],
        "verification": {
            "method": "s3_object_and_public_byte_hash",
            "objects": [{"key": "canary", "sha256": "b" * 64, "size": 10}],
        },
    }
    config = SimpleNamespace(
        provider="r2",
        bucket="trr-media-prod",
        endpoint_url=binding["endpoint_url"],
        cdn_base_url="https://pub-approved.r2.dev",
    )
    path = tmp_path / "binding.json"

    def write():
        import json

        path.write_text(json.dumps(binding))
        return hashlib.sha256(path.read_bytes()).hexdigest()

    assert storage_binding(path, write(), config)[1] == [config.cdn_base_url]
    config.bucket = "other"
    with pytest.raises(common.GuardError):
        storage_binding(path, write(), config)
    config.bucket = "trr-media-prod"
    binding["public_bases"][0]["verified"] = False
    with pytest.raises(common.GuardError):
        storage_binding(path, write(), config)


def test_source_gap_is_distinct_from_traversal_and_bytes():
    before, after, byte_report = evidence()
    for snapshot in (before, after):
        snapshot["reports"]["comments"]["count_gaps"] = [{"shortcode": "ABC", "count_gap": 1}]
        snapshot["reports"]["comments"]["target_results"][0]["settlement"]["count_gap"] = 1
    result = acceptance(before, after, byte_report)
    assert result["final_acceptance"] is False
    assert result["gates"]["source_count_parity"]["status"] == "blocked"
    assert result["gates"]["comments_traversal_and_parentage"]["status"] == "passed"
    assert result["gates"]["hosted_bytes"]["status"] == "passed"


def test_rate_limit_pauses_queued_reads_and_preserves_retry_after():
    from threading import Event

    from scripts.ingestion.january_audit.offline_hosted import BASE, Client, Response, Session, png, report

    item = hosted.physical_plan(report(), BASE)[0]
    client = Client(png())
    stop = Event()
    Session.response = Response(png(), status=429)
    Session.response.headers["Retry-After"] = "120"
    with patch("requests.Session", Session):
        result = hosted.inspect_object(item, client, SimpleNamespace(bucket="trr-media-prod"), stop=stop)
        paused = hosted.inspect_object(item, client, SimpleNamespace(bucket="trr-media-prod"), stop=stop)
    assert result["error_code"] == "cdn_rate_limited" and result["retry_after_seconds"] == 120
    assert stop.is_set() and client.calls == 1
    assert paused["error_code"] == "hosted_reads_paused_after_rate_limit"


def test_cli_blocked_output_is_private_versioned_and_never_overwritten(tmp_path):
    import json

    from scripts.ingestion import certify_january as command

    output = tmp_path / "certificate.json"
    args = [
        "--run-id",
        common.RUN,
        "--intent-hash",
        common.INTENT,
        "--date-start",
        "2026-01-01T00:00:00Z",
        "--date-end",
        "2026-02-01T00:00:00Z",
        "--binding",
        "unused",
        "--storage-binding",
        "unused",
        "--expected-storage-binding-sha256",
        "a" * 64,
        "--output",
        str(output),
    ]
    with patch.object(command, "run", side_effect=common.GuardError("fixture_guard")):
        assert command.main(args) == 2
        result = json.loads(output.read_text())
        assert result["final_acceptance"] is False and result["gates"]["preconditions"]["status"] == "blocked"
        assert output.stat().st_mode & 0o777 == 0o600
        with pytest.raises(FileExistsError):
            command.main(args)
    assert json.loads(output.read_text()) == result


def test_known_gap_preflight_lists_all_targets_without_native_membership_claim():
    from scripts.ingestion.certify_january import preflight_report

    targets = [{"stage": "listing", "target_key": "page-1", "outcome": "committed", "listing_keys": ["ABC", "DEF"]}]
    for stage in ("post_details", "comments", "media"):
        for key in ("ABC", "DEF"):
            targets.append(
                {
                    "stage": stage,
                    "target_key": key,
                    "outcome": "committed",
                    "receipt_settlement": {"count_gap": 201 if key == "ABC" and stage == "comments" else 0},
                }
            )
    result = preflight_report({"state": "completed"}, targets, common.context())
    assert result["final_acceptance"] is False
    assert result["known_count_gap_total"] == 201
    assert result["target_inventories"]["comments"] == ["ABC", "DEF"]
    assert result["inventory_mismatches"] == []
    assert result["independent_native_membership_verified"] is False
    assert result["retained_page_scans"] == result["storage_requests"] == 0


def test_applied_attachment_uses_only_maintained_package_imports():
    import builtins

    from scripts.ingestion.january_audit import comments
    from scripts.ingestion.january_audit.offline_media import fixture

    values, source, _ = fixture(attachment=True)
    saved = values["comments"][0]
    row = {**values["row"], "outcome": "committed"}
    codes = []
    original_import = builtins.__import__

    def maintained_import(name, *args, **kwargs):
        if name.startswith("verify_january_"):
            raise AssertionError("private_standalone_auditor_import_forbidden")
        return original_import(name, *args, **kwargs)

    with (
        patch.object(comments.common, "rows", return_value=[row]),
        patch("builtins.__import__", side_effect=maintained_import),
    ):
        assert (
            comments.applied_attachment(
                None,
                source.shortcode,
                source.post_id,
                saved,
                saved["raw_data"],
                lambda ok, code: codes.append(code) if not ok else None,
                pinned=True,
            )
            is True
        )
    assert codes == []
