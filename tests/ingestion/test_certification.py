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
                "settlement": {"count_gap": 0, "expected_total": 1, "observed_total": 1, "basis": "count_parity"},
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


def observed_gap_fixture():
    from scripts.ingestion.january_audit import comments, media
    from scripts.ingestion.january_audit.offline_media import LISTING_HASH, OBSERVED, RELEASE

    values, source, result = offline_comments.certificate_fixture()
    values["allow_verified_comment_subset"] = False
    values["audited_keys"] = ["Fixture"]
    row = values["row"]
    settlement = {"basis": "public_native_exhaustion", "count_gap": 2, "expected_total": 3, "observed_total": 1}
    row["comments_identity"]["settlement"] = settlement
    old = values["comment_proof"]
    verified = deepcopy(old["verified"][0])
    verified.update(settlement=settlement, media_binding=media.comment_binding(row, values["comments"]))
    target = comments.target_certificate(
        "Fixture",
        "committed",
        verified,
        [],
        None,
        settlement,
        old["target_results"][0]["source_page_evidence_manifest"],
    )
    proof = {
        **old,
        **comments.certificate_envelope(
            ["Fixture"],
            RELEASE,
            LISTING_HASH,
            manifest={"run_id": common.RUN, "stage": "comments", "discovery_complete": True, "sealed_at": OBSERVED},
        ),
        "target_results": [target],
        "verified": [verified],
        "verified_targets": 1,
        "audited_january_count": 1,
        "status": "pending",
        "failures": [],
        "pending": [],
        "count_gaps": [{"shortcode": "Fixture", **settlement}],
    }
    values["comment_proof"] = proof
    post_proof = {
        "status": "passed",
        "operator_scope": "post_records_only_not_final_certifier",
        "run_id": common.RUN,
        "intent_hash": common.INTENT,
        "release": RELEASE,
        "full_history_gate_closed": True,
        "failures": [],
        "pending": [],
        "audited_january_count": 1,
        "verified_post_count": 1,
        "verified": [
            {
                "shortcode": "Fixture",
                "post_row_sha256": posts.digest(row["post"]),
                "sidecar_sha256": posts.digest(row["sidecar"]),
                "canonical_sha256": posts.digest(row["canonical"]),
            }
        ],
    }
    return values, source, result, post_proof


def inspect_observed_fixture(values, post_proof):
    from scripts.ingestion.january_audit import media
    from scripts.ingestion.january_audit.offline_media import AT, LISTING_HASH, RELEASE

    class Cursor:
        connection = object()

        def execute(self, sql, args=()):
            assert sql.startswith(("show ", "select "))

        def fetchone(self):
            return {"transaction_read_only": "on", "transaction_isolation": "repeatable read", "at": AT}

    def rows(cur, sql, args=()):
        cur.execute(sql, args)
        if "from social.instagram_ingestion_targets t" in sql:
            return [values["row"]]
        if "join social.instagram_comments c" in sql:
            return values["comments"]
        if "from social.social_post_media_assets" in sql:
            return values["saved_assets"]
        if "from social.avatar_registry" in sql:
            return values["registry"]
        raise AssertionError("unexpected_observed_fixture_query")

    listing = {
        "run_id": common.RUN,
        "intent_hash": common.INTENT,
        "listing_proof": {"independent_native_membership_verified": True, "boundary_verified": True},
        "reported_january_keys": ["Fixture"],
        "independently_verified_january_count": 1,
    }
    progress = {"intent": {}, "intent_hash": common.INTENT, "generation": 1, "state": "completed"}
    with (
        patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
        patch.object(common, "rows", side_effect=rows),
    ):
        return media.inspect(
            Cursor(),
            {"successor_intent": {}},
            RELEASE,
            listing,
            values["comment_proof"],
            LISTING_HASH,
            complete_observed_inventory=True,
            post_proof=post_proof,
        )


@pytest.mark.parametrize("state,stop", [("running", True), ("queued", True), ("completed", False), ("blocked", False)])
def test_observed_inventory_terminal_gap_proceeds_past_preflight(state, stop):
    from scripts.ingestion.certify_january import active_gap_preflight_stop, preflight_report

    targets = [
        {"stage": "comments", "target_key": "ABC", "outcome": "unresolved", "receipt_settlement": {"count_gap": 2}}
    ]
    report = preflight_report({"state": state}, targets, common.context())
    assert active_gap_preflight_stop(report) is stop
    assert report["final_acceptance"] is False


@pytest.mark.parametrize("failure", ["unknown", "parent", "reply", "source", "extra", "post", "saved"])
def test_observed_inventory_refuses_unknown_or_incomplete_native_proof(failure):
    from scripts.ingestion.january_audit import media

    values, _, _, post_proof = observed_gap_fixture()
    proof = values["comment_proof"]
    if failure == "unknown":
        proof["target_results"][0]["settlement"]["expected_total"] = None
    if failure == "parent":
        proof["target_results"][0]["diagnostics"].append("native_parent_uuid_mismatch")
    if failure == "reply":
        proof["target_results"][0]["diagnostics"].append("native_lanes_or_settlement_incomplete")
    if failure == "source":
        proof["target_results"][0]["source_page_evidence_complete"] = False
    if failure == "extra":
        proof["verified"].append({**deepcopy(proof["verified"][0]), "shortcode": "Foreign"})
    if failure == "post":
        post_proof["status"] = "blocked"
    if failure == "saved":
        values["comments"][0]["parent_comment_external_id"] = "missing-parent"
    report = inspect_observed_fixture(values, post_proof)
    assert report["status"] != "passed" and not report["verified"]
    assert report["byte_read_plan"]["objects"] == []
    assert report["comment_proof_mode"] == media.OBSERVED_INVENTORY_SCOPE


def test_observed_inventory_gap_all_media_and_whole_bytes_pass_without_acceptance(monkeypatch):
    import io
    from urllib.parse import urlsplit

    from PIL import Image

    from scripts.ingestion.january_audit import offline_media
    from scripts.ingestion.january_audit.offline_hosted import Body, Response

    def encoded(fmt):
        value = io.BytesIO()
        Image.new("RGB", (12, 8), "red").save(value, format="JPEG" if fmt.lower() in {"jpg", "jpeg"} else fmt.upper())
        return value.getvalue()

    jpeg_hash = hashlib.sha256(encoded("jpeg")).hexdigest()
    monkeypatch.setattr(offline_media, "SHA", jpeg_hash)
    original_host = offline_media.PureStorage.host

    def measured_host(self, *args, **kwargs):
        original, display = original_host(self, *args, **kwargs)
        for entry in [original, *display.values()]:
            body = encoded(entry["format"])
            entry["bytes"] = len(body)
            if "sha256" in entry:
                entry["sha256"] = hashlib.sha256(body).hexdigest()
        return original, display

    monkeypatch.setattr(offline_media.PureStorage, "host", measured_host)
    values, _, _, post_proof = observed_gap_fixture()
    before_comments = deepcopy(values["comment_proof"])
    report = inspect_observed_fixture(values, post_proof)
    assert report["status"] == "passed" and report["verified_targets"] == 1, report["failures"]
    assert values["comment_proof"] == before_comments
    plan = hosted.physical_plan(report, "https://media.example")
    by_key = {item["storage_key"]: item for item in plan}
    calls = []

    class Client:
        def get_object(self, *, Bucket, Key):  # noqa: N803 - exact boto3 keyword contract.
            assert Bucket == "fixture-bucket"
            calls.append(("storage", Key))
            item = by_key[Key]
            fmt = item["format"] or "jpeg"
            body = encoded(fmt)
            return {
                "Body": Body(body),
                "ContentLength": len(body),
                "ContentType": "image/jpeg" if fmt in {"jpg", "jpeg"} else "image/" + fmt,
                "ResponseMetadata": {"HTTPStatusCode": 200},
            }

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            assert kwargs["stream"] and not kwargs["allow_redirects"]
            key = urlsplit(url).path.lstrip("/")
            calls.append(("public", key))
            fmt = by_key[key]["format"] or "jpeg"
            return Response(encoded(fmt), "image/jpeg" if fmt in {"jpg", "jpeg"} else "image/" + fmt)

    with patch("requests.Session", Session):
        objects = [hosted.inspect_object(item, Client(), SimpleNamespace(bucket="fixture-bucket")) for item in plan]
    assert all(item["status"] == "verified" for item in objects), objects
    assert len(calls) == 2 * len(plan)
    snapshot, _, _ = evidence()
    listing = snapshot["reports"]["listing"]
    listing.update(reported_january_keys=["Fixture"], native_membership={"Fixture": {"native_id": "123"}})
    snapshot["reports"].update(posts=post_proof, comments=values["comment_proof"], media=report)
    snapshot["physical_plan"] = plan
    certificate = acceptance(snapshot, deepcopy(snapshot), {"status": "passed", "failures": [], "objects": objects})
    for name in ("posts", "comments_traversal_and_parentage", "media", "hosted_bytes", "unchanged_evidence"):
        assert certificate["gates"][name]["status"] == "passed", certificate
    assert certificate["gates"]["source_count_parity"]["status"] == "blocked"
    assert certificate["gates"]["comments"]["status"] == "blocked"
    assert certificate["final_acceptance"] is False


def test_observed_inventory_independent_post_row_proof_must_match_current_rows():
    values, _, _, post_proof = observed_gap_fixture()
    post_proof["verified"][0]["post_row_sha256"] = "f" * 64
    report = inspect_observed_fixture(values, post_proof)
    assert report["status"] == "blocked"
    assert "observed_inventory_independent_post_row_sha256_changed" in report["failures"][0]["codes"]
    assert report["byte_read_plan"]["objects"] == []


def test_exact_parity_rejects_observed_count_above_expected_without_blocking_media_inspection():
    before, after, byte_report = evidence()
    for snapshot in (before, after):
        snapshot["reports"]["comments"]["target_results"][0]["settlement"].update(expected_total=0, observed_total=1)
    result = acceptance(before, after, byte_report)
    assert result["gates"]["source_count_parity"]["status"] == "blocked"
    assert result["gates"]["comments"]["status"] == "blocked"
    assert result["gates"]["media"]["status"] == result["gates"]["hosted_bytes"]["status"] == "passed"
    assert result["gates"]["comments_traversal_and_parentage"]["status"] == "passed"
    assert result["final_acceptance"] is False
