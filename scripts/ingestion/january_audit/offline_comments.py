#!/usr/bin/env python3
"""Focused pure certificate fixtures; no live source, DB, native or storage."""

import hashlib
import json
import unittest
from contextlib import ExitStack
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from . import comments, media
from .offline_media import AT, LISTING_HASH, OBSERVED, RELEASE, fixture


def certificate_fixture(other_status="blocked"):
    values, source, result = fixture(comment=True)
    row = values["row"]
    # One independently read source page can be both root and receipt.
    root = row["comments_checkpoint"]["comment_pages"]["scan_root_evidence_id"]
    row["comments_evidence_id"] = root
    legacy = values["comment_proof"]["verified"][0]
    legacy["media_binding"] = media.comment_binding(row, values["comments"])
    legacy.update(
        settlement={"basis": "count_parity", "count_gap": 0, "expected_total": 1, "observed_total": 1},
        source_pages=1,
        native_comments=1,
        native_replies=0,
        saved_rows_sha256=media.digest(values["comments"]),
        original_settlement_at=row["comments_observed_at"].isoformat(),
    )
    page = {
        "page_id": str(uuid4()),
        "evidence_id": root,
        "run_id": media.common.RUN,
        "stage": "comments",
        "target_key": "Fixture",
        "source_identity": "instagram:comments:Fixture:comments_api",
        "content_hash": "d" * 64,
        "parser_version": "instagram-comments-v1",
        "schema_version": 1,
        "captured_at": row["comments_observed_at"].isoformat(),
    }
    cert = comments.target_certificate("Fixture", "committed", deepcopy(legacy), [], None, None, [page])
    other = {
        "shortcode": "Other",
        "status": other_status,
        "diagnostics": ["active_saved_inventory_mismatch"]
        if other_status == "blocked"
        else ["target_deep_audit_unresolved"],
        "settlement": {},
        "source_page_evidence_manifest": [],
        "source_page_evidence_sha256": media.digest([]),
        "source_page_evidence_complete": False,
    }
    envelope = comments.certificate_envelope(
        ["Fixture", "Other"],
        RELEASE,
        LISTING_HASH,
        manifest={"run_id": media.common.RUN, "stage": "comments", "discovery_complete": True, "sealed_at": OBSERVED},
    )
    proof = {
        **values["comment_proof"],
        **envelope,
        "target_results": [cert, other],
        "audited_january_count": 2,
        "observed_at": AT.isoformat(),
        "status": other_status,
        "failures": [{"shortcode": "Other", "codes": ["active_saved_inventory_mismatch"]}]
        if other_status == "blocked"
        else [],
        "pending": [{"shortcode": "Other", "outcome": "unresolved"}] if other_status == "pending" else [],
        "count_gaps": [],
        "final_acceptance": False,
    }
    artifact_hash = hashlib.sha256(json.dumps(proof, sort_keys=True, default=str).encode()).hexdigest()
    values.update(
        comment_proof=proof,
        allow_verified_comment_subset=True,
        audited_keys=["Fixture", "Other"],
        comment_proof_sha256=artifact_hash,
        expected_comment_proof_sha256=artifact_hash,
    )
    return values, source, result


def repin(values):
    """Recompute both artifact hash values, so structural checks do the work."""
    value = hashlib.sha256(json.dumps(values["comment_proof"], sort_keys=True, default=str).encode()).hexdigest()
    values["comment_proof_sha256"] = value
    values["expected_comment_proof_sha256"] = value


def producer_fixture(*, media_states=None, include_unattempted=False, selected_media=None):
    """Real retained-page Evidence/parser/advance/settlement, mocked SELECTs."""
    from trr_backend.ingestion.comments import (
        COMMENT_PARSER_VERSION,
        CommentFetchSummary,
        _advance,
        _settlement,
        parse_comment_evidence,
    )
    from trr_backend.ingestion.evidence import Evidence

    keys = ["Zero", "Gap", "Extra", "Unresolved"] + (["Unattempted"] if include_unattempted else [])
    targets, pages, saved, media_targets = [], [], [], []
    for key in keys:
        post_id, root, token = str(uuid4()), str(uuid4()), str(uuid4())
        expected = 1 if key == "Gap" else 0
        payload = {
            "comments": [],
            "comment_count": expected,
            "has_more_comments": False,
            "has_more_headload_comments": False,
            "cursor_payload": {
                "adapter_family": "instagram_public_relay",
                "adapter_version": 1,
                "source": "root",
                "relay_doc_id": "fixture-doc",
                "relay_friendly_name": "fixture-name",
                "sort_order": "recent",
                "page_size": 20,
            },
        }
        evidence = Evidence.capture(
            source_identity=f"instagram:comments:{key}:comments_api",
            captured_at=OBSERVED,
            parser_version=COMMENT_PARSER_VERSION,
            payload=payload,
        )
        page = parse_comment_evidence(evidence, pinned=True)
        state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
        _advance(state, page, scan_started_at=OBSERVED)
        state["scan_root_evidence_id"] = root
        settlement = _settlement(state, CommentFetchSummary(expected_count=expected, full_observation=True))
        state["settlement"] = settlement
        target = {
            "target_key": key,
            "target_outcome": "unresolved" if key == "Unresolved" else "committed",
            "checkpoint": {"comment_pages": state},
            "target_owner_generation": 1,
            "target_owner_token": token,
            "owner_generation": 1,
            "receipt_token": token,
            "receipt_outcome": "committed",
            "dispatch_identity_matches": True,
            "canonical_identity": {"post_id": post_id, "observed_comment_ids": [], "settlement": settlement},
            "evidence_id": root,
            "observed_at": OBSERVED,
            "post_id": post_id,
            "post_observed_at": OBSERVED - timedelta(minutes=2),
            "comments_count": expected,
            "source_account": "bravotv",
            "username": "bravotv",
            "detail_identity": {"cache_source": {"fixture_expected": expected}},
            "detail_observed_at": OBSERVED - timedelta(minutes=2),
            "detail_committed_at": OBSERVED - timedelta(minutes=1),
            "detail_evidence_id": str(uuid4()),
        }
        targets.append(target)
        media_token, media_generation = (media_states or {}).get(key, (str(uuid4()), 2))
        media_target = {
            "target_key": key,
            "owner_token": media_token,
            "owner_generation": media_generation,
            "asset_manifest": None if key == "Unresolved" else {},
        }
        if key == "Zero" and selected_media is not None:
            selected = selected_media["row"]
            post_id = str(selected["post"]["id"])
            target.update(
                post_id=post_id, post_observed_at=OBSERVED, detail_observed_at=OBSERVED, detail_committed_at=OBSERVED
            )
            target["canonical_identity"]["post_id"] = post_id
            media_target.update(
                owner_token=selected["target_owner_token"],
                owner_generation=selected["target_owner_generation"],
                asset_manifest=selected["post"]["asset_manifest"],
            )
        media_targets.append(media_target)
        if key != "Unresolved":
            pages.append(
                {
                    "page_id": str(uuid4()),
                    "run_id": media.common.RUN,
                    "stage": "comments",
                    "target_key": key,
                    "owner_generation": 1,
                    "evidence_id": root,
                    "observed_at": OBSERVED,
                    "captured_at": OBSERVED,
                    "content_hash": evidence.content_hash,
                    "evidence_hash": evidence.content_hash,
                    "parent_comment_id": None,
                    "next_cursor": None,
                    "exhausted": True,
                    "canonical_identity": {"post_id": post_id, "comment_ids": {}},
                    "checkpoint": {"comment_pages": deepcopy(state)},
                    "source_identity": evidence.source_identity,
                    "parser_version": evidence.parser_version,
                    "schema_version": evidence.schema_version,
                    "payload": evidence.payload_json,
                    "expires_at": evidence.expires_at,
                    "source_run_state": "running",
                }
            )
        if key == "Extra":
            saved.append(
                {
                    "shortcode": key,
                    "id": str(uuid4()),
                    "post_id": post_id,
                    "comment_id": "preserved-unknown",
                    "text": "historical",
                    "uuid_parent_native_id": None,
                    "uuid_parent_post_id": None,
                }
            )
    manifest = {"run_id": media.common.RUN, "stage": "comments", "discovery_complete": True, "sealed_at": OBSERVED}
    listing = {
        "run_id": media.common.RUN,
        "intent_hash": media.common.INTENT,
        "reported_january_keys": keys,
        "independently_verified_january_count": len(keys),
        "listing_proof": {"independent_native_membership_verified": True, "boundary_verified": True},
    }

    class Cursor:
        connection = object()

        def execute(self, sql, args=()):
            if not (sql.startswith("show ") or sql.startswith("select ")):
                raise AssertionError("mutation_attempt")

        def fetchone(self):
            return {"transaction_read_only": "on", "transaction_isolation": "repeatable read", "at": AT}

    def rows(cur, sql, args=()):
        cur.execute(sql, args)
        if "from social.instagram_ingestion_manifests" in sql:
            return [manifest]
        if "from social.instagram_ingestion_targets t" in sql:
            return media_targets if "p.asset_manifest" in sql else targets
        if "from social.instagram_ingestion_comment_pages p" in sql:
            return [page for page in pages if page["target_key"] == args[1]]
        if "join social.instagram_comments c" in sql:
            return saved
        raise AssertionError("unexpected_query")

    def read_reference(cur, reference, *, source_identity):
        captured = (
            OBSERVED
            if selected_media is not None and source_identity == "instagram:post:Zero"
            else OBSERVED - timedelta(minutes=2)
        )
        evidence = Evidence.capture(
            source_identity=source_identity,
            captured_at=captured,
            parser_version="fixture-post-v1",
            payload={"comment_count": reference["fixture_expected"]},
        )
        return evidence, {"source_run_state": "running"}

    def parse_post(evidence, **kwargs):
        return SimpleNamespace(
            post={"comments_count": evidence.read(pinned=True)["comment_count"]},
            source_payload={},
            observed_at=evidence.captured_at,
        )

    progress = {
        "intent": {},
        "intent_hash": media.common.INTENT,
        "generation": 1,
        "account": "bravotv",
        "state": "running",
    }
    return Cursor(), listing, manifest, progress, rows, read_reference, parse_post, targets


def producer_unowned_consumer_fixture(generation):
    """Real comment producer output plus an independently reserved media row."""
    from dataclasses import replace

    from trr_backend.ingestion.evidence import Evidence
    from trr_backend.ingestion.media import mirror_snapshot
    from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, serialize_media_result

    from .offline_media import PureStorage

    values, source, _ = fixture()
    source = replace(source, shortcode="Zero")
    result = mirror_snapshot(source, PureStorage(), check=lambda: None, request=lambda: None)
    evidence = Evidence.capture(
        source_identity="instagram:media:Zero",
        captured_at=source.observed_at,
        parser_version=MEDIA_RESULT_REPLAY_VERSION,
        payload=serialize_media_result(result),
    )
    row = values["row"]
    row.update(
        target_key="Zero",
        source_identity=evidence.source_identity,
        captured_at=evidence.captured_at,
        expires_at=evidence.expires_at,
        content_hash=evidence.content_hash,
        payload=evidence.payload_json,
    )
    row["post"].update(shortcode="Zero", **deepcopy(result.post_fields))
    row["sidecar"]["asset_manifest"] = deepcopy(result.post_fields["asset_manifest"])
    row["canonical"]["source_id"] = "Zero"
    values["saved_assets"][0].update(
        hosted_url=result.post_fields["hosted_media_urls"][0],
        hosted_thumbnail_url=result.post_fields["hosted_thumbnail_url"],
    )
    from .offline_media import repin_inventory

    values, source, result = repin_inventory(values, source, result)
    states = dict.fromkeys(("Gap", "Extra", "Unattempted"), (None, generation))
    cur, listing, manifest, progress, rows, read_reference, parse_post, targets = producer_fixture(
        media_states=states, include_unattempted=True, selected_media=values
    )
    with (
        patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
        patch.object(comments.common, "rows", side_effect=rows),
        patch.object(comments.common, "read_reference", side_effect=read_reference),
        patch("trr_backend.ingestion.providers.parse_evidence", side_effect=parse_post),
        patch("trr_backend.ingestion.application.validate_detail"),
        patch("trr_backend.ingestion.canonical._post_payload", side_effect=lambda post, *args: post),
    ):
        proof = comments.inspect(cur, {"successor_intent": {}}, RELEASE, listing, LISTING_HASH)
    selected = next(target for target in targets if target["target_key"] == "Zero")
    row.update(
        comments_identity=selected["canonical_identity"],
        comments_observed_at=selected["observed_at"],
        comments_checkpoint=selected["checkpoint"],
        comments_evidence_id=selected["evidence_id"],
        comments_receipt_token=selected["receipt_token"],
        comments_owner_generation=selected["owner_generation"],
        comments_target_owner_token=selected["target_owner_token"],
        comments_target_owner_generation=selected["target_owner_generation"],
    )
    values.update(
        comment_proof=proof, allow_verified_comment_subset=True, audited_keys=listing["reported_january_keys"]
    )
    repin(values)
    return values


def set_artifact_media_reservation(values, key, reservation):
    """Keep both supported diagnostic projections aligned before re-pinning."""
    for field in ("target_results", "verified"):
        for entry in values["comment_proof"][field]:
            if entry["shortcode"] == key:
                entry["media_binding"]["media_reservation"] = deepcopy(reservation)
    repin(values)


class SubsetCertificatesOffline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.guards = ExitStack()

        def forbidden(*args, **kwargs):
            raise AssertionError("offline_guard_external_call")

        for name in (
            "socket.socket.connect",
            "socket.create_connection",
            "requests.sessions.Session.request",
            "psycopg2.connect",
            "botocore.client.BaseClient._make_api_call",
            "trr_backend.db.pg.db_connection",
        ):
            cls.guards.enter_context(patch(name, side_effect=forbidden))

    @classmethod
    def tearDownClass(cls):
        cls.guards.close()

    def envelope_codes(self, values):
        return media.subset_envelope_errors(
            values["comment_proof"],
            values["release"],
            values["listing_hash"],
            values["audited_keys"],
            values["comment_proof_sha256"],
            values["expected_comment_proof_sha256"],
        )

    def audit_codes(self, values):
        return media.audit_target(**values).get("failure", {}).get("codes", [])

    def test_good_target_in_globally_blocked_or_pending_aggregate_yields_plan(self):
        for other_status in ("blocked", "pending"):
            with self.subTest(other_status=other_status):
                values, _, _ = certificate_fixture(other_status)
                before = deepcopy(values)
                result = media.audit_target(**values)
                self.assertIn("verified", result, result)
                self.assertTrue(result["plan"])
                self.assertEqual(values, before)
                self.assertEqual(values["comment_proof"]["status"], other_status)

    def test_strict_default_remains_aggregate_gate(self):
        values, _, _ = certificate_fixture()
        values["allow_verified_comment_subset"] = False
        self.assertIn("deep_comment_proof_not_passing_or_bound", self.audit_codes(values))
        strict, _, _ = fixture(comment=True)
        self.assertIn("verified", media.audit_target(**strict))

    def test_old_report_never_infers_target_certificate(self):
        values, _, _ = fixture(comment=True)
        values.update(
            allow_verified_comment_subset=True,
            audited_keys=["Fixture"],
            comment_proof_sha256="a" * 64,
            expected_comment_proof_sha256="a" * 64,
        )
        self.assertIn("supported_comment_target_certificate_required", self.audit_codes(values))

    def test_global_common_bindings_are_required(self):
        changes = {
            "account": "other",
            "generation": 2,
            "run_id": "foreign",
            "intent_hash": "foreign",
            "release": "/other",
            "listing_proof_sha256": "0" * 64,
            "full_history_gate_closed": False,
            "final_acceptance": True,
            "target_certificate_version": True,
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                values, _, _ = certificate_fixture()
                values["comment_proof"][field] = value
                self.assertTrue(self.envelope_codes(values))
                self.assertNotIn("verified", media.audit_target(**values))

    def test_global_validation_and_hash_pin_hold_every_target(self):
        for case in ("global", "pin_missing", "pin_wrong", "unclassified"):
            values, _, _ = certificate_fixture()
            if case == "global":
                values["comment_proof"]["global_validation"]["status"] = "blocked"
            elif case == "pin_missing":
                values["expected_comment_proof_sha256"] = None
            elif case == "pin_wrong":
                values["expected_comment_proof_sha256"] = "0" * 64
            else:
                values["comment_proof"]["failures"].append({"code": "unknown_failure"})
            self.assertTrue(self.envelope_codes(values), case)
            self.assertNotIn("verified", media.audit_target(**values), case)

    def test_exact_keyset_unique_complete_coverage(self):
        for case in ("duplicate", "missing", "foreign", "digest", "keys"):
            values, _, _ = certificate_fixture()
            proof = values["comment_proof"]
            if case == "duplicate":
                proof["target_results"].append(deepcopy(proof["target_results"][0]))
            elif case == "missing":
                proof["target_results"].pop()
            elif case == "foreign":
                proof["target_results"][1]["shortcode"] = "Foreign"
            elif case == "digest":
                proof["audited_keyset_sha256"] = "0" * 64
            else:
                proof["audited_keys"] = ["Fixture", "Foreign"]
            self.assertTrue(self.envelope_codes(values), case)
            self.assertNotIn("verified", media.audit_target(**values), case)

    def test_gap_legacy_extra_and_unresolved_targets_are_ineligible(self):
        values, _, _ = certificate_fixture()
        good = values["comment_proof"]["target_results"][0]
        pages = good["source_page_evidence_manifest"]
        for case in ("gap", "legacy_extra", "unresolved"):
            deep = deepcopy(good)
            diagnostics = []
            pending = None
            gap = None
            outcome = "committed"
            if case == "gap":
                deep["settlement"]["count_gap"] = 1
                gap = {"shortcode": "Fixture", **deep["settlement"]}
            elif case == "legacy_extra":
                diagnostics = ["active_saved_inventory_mismatch"]
            else:
                outcome = "unresolved"
                pending = {"outcome": "unresolved"}
            certificate = comments.target_certificate("Fixture", outcome, deep, diagnostics, pending, gap, pages)
            self.assertNotEqual(certificate["status"], "passed", case)
            values["comment_proof"]["target_results"][0] = certificate
            self.assertNotIn("verified", media.audit_target(**values), case)

    def test_passed_certificate_cannot_contradict_legacy_diagnostics(self):
        for field, entry in (
            ("failures", {"shortcode": "Fixture", "codes": ["active_saved_inventory_mismatch"]}),
            ("pending", {"shortcode": "Fixture", "outcome": "unresolved"}),
            (
                "count_gaps",
                {
                    "shortcode": "Fixture",
                    "basis": "public_native_exhaustion",
                    "expected_total": 2,
                    "observed_total": 1,
                    "count_gap": 1,
                },
            ),
        ):
            values, _, _ = certificate_fixture()
            values["comment_proof"][field].append(entry)
            repin(values)
            self.assertIn("comment_certificate_target_status_inconsistent", self.audit_codes(values))
        values, _, _ = certificate_fixture()
        values["comment_proof"]["verified"][0]["checkpoint_sha256"] = "0" * 64
        repin(values)
        self.assertIn("comment_certificate_legacy_verified_inconsistent", self.audit_codes(values))

    def test_exact_current_binding_drift_is_rejected(self):
        for field in (
            "saved_rows_sha256",
            "post_id",
            "scan_root_evidence_id",
            "receipt",
            "media_reservation",
            "media_manifest_sha256",
        ):
            values, _, _ = certificate_fixture()
            cert = values["comment_proof"]["target_results"][0]
            cert["media_binding"][field] = "stale"
            self.assertNotIn("verified", media.audit_target(**values), field)
        values, _, _ = certificate_fixture()
        values["comments"][0]["text"] = "changed"
        self.assertIn("deep_comment_saved_rows_changed", self.audit_codes(values))

    def test_source_page_manifest_is_exact_and_cannot_fabricate_clock(self):
        for field, value in (
            ("content_hash", "bad"),
            ("parser_version", None),
            ("schema_version", 99),
            ("captured_at", "bad"),
            ("run_id", "foreign"),
            ("target_key", "foreign"),
            ("source_identity", "foreign"),
        ):
            values, _, _ = certificate_fixture()
            cert = values["comment_proof"]["target_results"][0]
            cert["source_page_evidence_manifest"][0][field] = value
            cert["source_page_evidence_sha256"] = media.digest(cert["source_page_evidence_manifest"])
            self.assertNotIn("verified", media.audit_target(**values), field)
        values, _, _ = certificate_fixture()
        values["comment_proof"]["target_results"][0]["source_page_evidence_manifest"][0]["captured_at"] = (
            AT + timedelta(days=1)
        ).isoformat()
        self.assertNotIn("verified", media.audit_target(**values))

    def test_unknown_exception_invalidates_all_comment_certificates(self):
        listing = {"reported_january_keys": ["Fixture", "Other"]}
        with patch.object(comments, "_inspect", side_effect=RuntimeError("private source URL")):
            report = comments.inspect(None, {}, RELEASE, listing, LISTING_HASH)
        self.assertEqual(report["global_validation"]["status"], "blocked")
        self.assertEqual(len(report["target_results"]), 2)
        self.assertTrue(all(entry["status"] == "blocked" for entry in report["target_results"]))
        self.assertEqual(report["verified"], [])
        self.assertNotIn("private source URL", json.dumps(report))

    def test_null_empty_text_equivalence_is_preserved(self):
        self.assertTrue(comments.equal_native_text(None, ""))
        self.assertTrue(comments.equal_native_text("", ""))
        self.assertFalse(comments.equal_native_text(None, " "))
        self.assertFalse(comments.equal_native_text(None, "native"))

    def test_envelope_inspection_rejects_bad_snapshot_before_classification(self):
        class Cursor:
            connection = object()

            def execute(self, *args):
                pass

            def fetchone(self):
                return {"transaction_read_only": "off"}

        report = comments.inspect(Cursor(), {}, RELEASE, {"reported_january_keys": ["Fixture"]}, LISTING_HASH)
        self.assertEqual(report["global_validation"]["status"], "blocked")
        self.assertTrue(all(entry["status"] == "blocked" for entry in report["target_results"]))

    def test_media_subset_aggregate_never_passes_and_preserves_exclusions(self):
        values, _, _ = certificate_fixture()
        other = deepcopy(values["row"])
        other.update(target_key="Other", target_outcome="pending")

        class Cursor:
            connection = object()

            def execute(self, sql, args=()):
                if not (sql.startswith("show ") or sql.startswith("select ")):
                    raise AssertionError("mutation")

            def fetchone(self):
                return {"transaction_read_only": "on", "transaction_isolation": "repeatable read", "at": AT}

        def rows(cur, sql, args=()):
            cur.execute(sql, args)
            if "from social.instagram_ingestion_targets t" in sql:
                return [values["row"], other]
            if "join social.instagram_comments c" in sql:
                return values["comments"]
            if "from social.social_post_media_assets" in sql:
                return values["saved_assets"]
            if "from social.avatar_registry" in sql:
                return values["registry"]
            raise AssertionError("unexpected query")

        listing = {
            "run_id": media.common.RUN,
            "intent_hash": media.common.INTENT,
            "reported_january_keys": ["Fixture", "Other"],
            "independently_verified_january_count": 2,
            "listing_proof": {"independent_native_membership_verified": True, "boundary_verified": True},
        }
        progress = {"intent": {}, "intent_hash": media.common.INTENT, "generation": 1, "state": "running"}
        with (
            patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
            patch.object(media.common, "rows", side_effect=rows),
        ):
            report = media.inspect(
                Cursor(),
                {"successor_intent": {}},
                RELEASE,
                listing,
                values["comment_proof"],
                LISTING_HASH,
                allow_verified_comment_subset=True,
                comment_proof_sha256=values["comment_proof_sha256"],
                expected_comment_proof_sha256=values["expected_comment_proof_sha256"],
            )
        self.assertEqual(report["status"], "pending")
        self.assertEqual(report["comment_proof_mode"], "verified_target_subset")
        self.assertEqual(report["verified_targets"], 1)
        self.assertEqual(report["pending"][0]["shortcode"], "Other")
        self.assertFalse(report["final_acceptance"])
        self.assertTrue(report["byte_read_plan"]["objects"])

    def test_real_retained_page_producer_classifies_zero_gap_extra_and_unresolved(self):
        cur, listing, manifest, progress, rows, read_reference, parse_post, targets = producer_fixture()
        with (
            patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
            patch.object(comments.common, "rows", side_effect=rows),
            patch.object(comments.common, "read_reference", side_effect=read_reference),
            patch("trr_backend.ingestion.providers.parse_evidence", side_effect=parse_post),
            patch("trr_backend.ingestion.application.validate_detail"),
            patch("trr_backend.ingestion.canonical._post_payload", side_effect=lambda post, *args: post),
        ):
            report = comments.inspect(cur, {"successor_intent": {}}, RELEASE, listing, LISTING_HASH)
        self.assertEqual(report["global_validation"]["status"], "passed", report)
        self.assertEqual(report["status"], "blocked")
        classified = {entry["shortcode"]: entry for entry in report["target_results"]}
        self.assertEqual(set(classified), set(listing["reported_january_keys"]))
        self.assertEqual(classified["Zero"]["status"], "passed")
        self.assertEqual(classified["Gap"]["status"], "pending")
        self.assertEqual(classified["Gap"]["settlement"]["count_gap"], 1)
        self.assertIn("Gap", {entry["shortcode"] for entry in report["verified"]})
        self.assertEqual(classified["Extra"]["status"], "blocked")
        self.assertEqual(classified["Extra"]["settlement"]["count_gap"], 0)
        self.assertIsInstance(classified["Extra"]["media_binding"], dict)
        self.assertEqual(classified["Extra"]["media_binding"]["observed_comment_ids"], ["preserved-unknown"])
        self.assertIn("active_saved_inventory_mismatch", classified["Extra"]["diagnostics"])
        self.assertEqual(classified["Unresolved"]["status"], "pending")
        zero = classified["Zero"]
        self.assertEqual(zero["source_page_evidence_sha256"], comments.digest(zero["source_page_evidence_manifest"]))
        self.assertEqual(zero["source_page_evidence_manifest"][0]["captured_at"], OBSERVED.isoformat())
        self.assertFalse(report["final_acceptance"])

    def test_producer_global_manifest_failure_holds_all_targets(self):
        cur, listing, manifest, progress, rows, read_reference, parse_post, targets = producer_fixture()
        manifest["discovery_complete"] = False
        with (
            patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
            patch.object(comments.common, "rows", side_effect=rows),
        ):
            report = comments.inspect(cur, {"successor_intent": {}}, RELEASE, listing, LISTING_HASH)
        self.assertEqual(report["global_validation"]["status"], "blocked")
        self.assertEqual(len(report["target_results"]), 4)
        self.assertTrue(all(entry["status"] == "blocked" for entry in report["target_results"]))

    def test_malformed_unselected_certificate_holds_repinning_subset(self):
        mutations = (
            lambda item: item.update(diagnostics=[{"code": "active_saved_inventory_mismatch"}]),
            lambda item: item.update(diagnostics=["future_unclassified_diagnostic"]),
            lambda item: item.update(status="passed", diagnostics=[]),
            lambda item: item.update(settlement="unsupported"),
            lambda item: item.update(source_page_evidence_manifest={}),
            lambda item: item.update(media_binding={"post_id": "missing-rest"}),
            lambda item: item.update(checkpoint_sha256={"invalid": "type"}),
            lambda item: item.update(source_page_evidence_complete="false"),
            lambda item: item.update(native_comments="unknown"),
        )
        for mutation in mutations:
            values, _, _ = certificate_fixture()
            mutation(values["comment_proof"]["target_results"][1])
            repin(values)
            self.assertIn("comment_certificate_target_schema_invalid", self.envelope_codes(values))
            self.assertNotIn("verified", media.audit_target(**values))

    def test_malformed_unselected_legacy_diagnostics_hold_repinning_subset(self):
        bad_records = (
            ("failures", {"shortcode": "Other"}),
            ("failures", {"shortcode": "Other", "codes": [{}]}),
            ("failures", {"shortcode": "Other", "codes": ["future_unclassified_diagnostic"]}),
            ("pending", {"shortcode": "Other", "outcome": {}}),
            ("count_gaps", {"shortcode": "Other", "count_gap": "unknown"}),
            ("verified", {"shortcode": "Other"}),
        )
        for field, entry in bad_records:
            values, _, _ = certificate_fixture()
            values["comment_proof"][field] = [entry]
            repin(values)
            self.assertIn("comment_certificate_unclassified_or_foreign_diagnostics", self.envelope_codes(values))
            self.assertNotIn("verified", media.audit_target(**values))

    def test_valid_ineligible_extra_targets_allow_globally_bound_subset(self):
        for status in ("pending", "blocked"):
            values, _, _ = certificate_fixture(status)
            other = values["comment_proof"]["target_results"][1]
            other.update(media_binding=None, checkpoint_sha256="2" * 64, saved_rows_sha256="3" * 64)
            repin(values)
            self.assertEqual(self.envelope_codes(values), [])
            self.assertIn("verified", media.audit_target(**values))
        # Full producer output includes the gap's legacy verified diagnostic,
        # a blocked preserved-extra target, and an unresolved empty target.
        cur, listing, manifest, progress, rows, read_reference, parse_post, targets = producer_fixture()
        with (
            patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
            patch.object(comments.common, "rows", side_effect=rows),
            patch.object(comments.common, "read_reference", side_effect=read_reference),
            patch("trr_backend.ingestion.providers.parse_evidence", side_effect=parse_post),
            patch("trr_backend.ingestion.application.validate_detail"),
            patch("trr_backend.ingestion.canonical._post_payload", side_effect=lambda post, *args: post),
        ):
            proof = comments.inspect(cur, {"successor_intent": {}}, RELEASE, listing, LISTING_HASH)
        value = hashlib.sha256(json.dumps(proof, sort_keys=True, default=str).encode()).hexdigest()
        self.assertEqual(
            media.subset_envelope_errors(proof, RELEASE, LISTING_HASH, listing["reported_january_keys"], value, value),
            [],
        )

    def test_final_page_clock_is_exact_after_manifest_and_artifact_repinning(self):
        values, _, _ = certificate_fixture()
        positive = media.audit_target(**values)
        self.assertIn("verified", positive)
        self.assertEqual(len(positive["plan"]), 14)
        cert = values["comment_proof"]["target_results"][0]
        original_receipt = deepcopy(cert["media_binding"]["receipt"])
        cert["source_page_evidence_manifest"][-1]["captured_at"] = (
            media.stamp(original_receipt["observed_at"]) - timedelta(days=1)
        ).isoformat()
        cert["source_page_evidence_sha256"] = media.digest(cert["source_page_evidence_manifest"])
        repin(values)
        self.assertIn("comment_target_source_page_receipt_clock_mismatch", self.audit_codes(values))
        self.assertEqual(cert["media_binding"]["receipt"], original_receipt)
        self.assertEqual(
            media.stamp(values["row"]["comments_observed_at"]), media.stamp(original_receipt["observed_at"])
        )
        cert["source_page_evidence_manifest"][-1]["captured_at"] = original_receipt["observed_at"]
        cert["source_page_evidence_sha256"] = media.digest(cert["source_page_evidence_manifest"])
        repin(values)
        self.assertIn("verified", media.audit_target(**values))

    def test_supported_diagnostics_cover_current_maintained_projection(self):
        from trr_backend.ingestion.canonical import _COMMENT_FIELDS, _COMMENT_METADATA

        expected = (set(_COMMENT_FIELDS) | set(_COMMENT_METADATA)) - {"media_urls"}
        self.assertTrue({f"source_attested_{field}_mismatch" for field in expected} <= media._SUPPORTED_COMMENT_CODES)
        self.assertTrue(
            media._certificate_codes(["attachment_applied_proof_changed", "page_replay_scan_root_evidence_id_mismatch"])
        )
        self.assertFalse(media._certificate_codes(["future_unclassified_diagnostic"]))

    def test_producer_unowned_reservations_allow_independent_selected_media(self):
        for generation in (0, 3):
            with self.subTest(generation=generation):
                values = producer_unowned_consumer_fixture(generation)
                proof = values["comment_proof"]
                self.assertEqual(proof["global_validation"]["status"], "passed")
                self.assertEqual(self.envelope_codes(values), [])
                classified = {entry["shortcode"]: entry for entry in proof["target_results"]}
                self.assertEqual(classified["Zero"]["status"], "passed")
                self.assertEqual(classified["Unattempted"]["status"], "passed")
                self.assertEqual(classified["Gap"]["status"], "pending")
                self.assertEqual(classified["Extra"]["status"], "blocked")
                for key in ("Gap", "Extra", "Unattempted"):
                    self.assertEqual(
                        classified[key]["media_binding"]["media_reservation"],
                        {"owner_token": "None", "owner_generation": generation},
                    )
                for entry in proof["verified"]:
                    if entry["shortcode"] in {"Gap", "Unattempted"}:
                        self.assertEqual(
                            entry["media_binding"]["media_reservation"],
                            {"owner_token": "None", "owner_generation": generation},
                        )
                result = media.audit_target(**values)
                self.assertIn("verified", result, result)
                self.assertTrue(result["plan"])
                self.assertEqual(result["verified"]["shortcode"], "Zero")
                self.assertEqual(proof["status"], "blocked")
                self.assertFalse(proof["final_acceptance"])

    def test_selected_unowned_reservation_never_yields_media_plan(self):
        for generation in (0, 3):
            values = producer_unowned_consumer_fixture(generation)
            set_artifact_media_reservation(values, "Zero", {"owner_token": "None", "owner_generation": generation})
            self.assertEqual(self.envelope_codes(values), [])
            # Match the current projection too: the explicit selected-media
            # eligibility check must stand independently of a binding mismatch.
            values["row"].update(
                target_owner_token=None,
                receipt_token=None,
                target_owner_generation=generation,
                owner_generation=generation,
            )
            codes, pending = [], []
            media.verify_comment_proof(
                values["row"],
                values["comments"],
                values["comment_proof"],
                RELEASE,
                LISTING_HASH,
                lambda ok, code, codes=codes: codes.append(code) if not ok else None,
                pending,
                allow_verified_comment_subset=True,
                audited_keys=values["audited_keys"],
                comment_proof_sha256=values["comment_proof_sha256"],
                expected_comment_proof_sha256=values["expected_comment_proof_sha256"],
            )
            self.assertIn("comment_target_media_reservation_required", codes)
            result = media.audit_target(**values)
            self.assertNotIn("verified", result)
            self.assertNotIn("plan", result)

    def test_repinned_malformed_reservations_and_unowned_receipts_hold_all(self):
        invalid = (
            ("invalid-token", 0),
            ("none", 0),
            (None, 0),
            ("None", -1),
            ("None", True),
            ("None", "3"),
            (str(uuid4()), 0),
            (str(uuid4()), True),
        )
        for token, generation in invalid:
            values = producer_unowned_consumer_fixture(0)
            set_artifact_media_reservation(
                values, "Unattempted", {"owner_token": token, "owner_generation": generation}
            )
            self.assertTrue(self.envelope_codes(values), (token, generation))
            self.assertNotIn("verified", media.audit_target(**values))
        for generation in (0, 3):
            values = producer_unowned_consumer_fixture(generation)
            for field in ("target_results", "verified"):
                for entry in values["comment_proof"][field]:
                    if entry["shortcode"] == "Gap":
                        entry["media_binding"]["receipt"].update(owner_token="None", owner_generation=generation)
            repin(values)
            self.assertTrue(self.envelope_codes(values))
            self.assertNotIn("verified", media.audit_target(**values))

    def test_unowned_projection_preserves_strict_and_current_binding_guards(self):
        values = producer_unowned_consumer_fixture(0)
        values["allow_verified_comment_subset"] = False
        self.assertIn("deep_comment_proof_not_passing_or_bound", self.audit_codes(values))
        values = producer_unowned_consumer_fixture(3)
        changed = str(uuid4())
        values["row"].update(target_owner_token=changed, receipt_token=changed)
        self.assertIn("deep_comment_binding_media_reservation_mismatch", self.audit_codes(values))

    def test_no_live_calls_guard(self):
        import socket

        import psycopg2
        import requests

        for call in (
            lambda: requests.get("https://example.com"),
            lambda: psycopg2.connect(""),
            lambda: socket.create_connection(("example.com", 443)),
        ):
            with self.assertRaises(AssertionError):
                call()


if __name__ == "__main__":
    unittest.main()
