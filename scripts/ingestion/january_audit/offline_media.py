#!/usr/bin/env python3
"""Pure offline regression fixtures. Run with maintained backend on PYTHONPATH."""

from __future__ import annotations

import json
import unittest
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch
from uuid import uuid4

from trr_backend.ingestion.comment_attachment_refresh import AttachmentRefresh, attachment_baseline
from trr_backend.ingestion.domain import utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import MEDIA_VERSION, AvatarSource, CommentMedia, MediaSnapshot, mirror_snapshot
from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, serialize_media_result

from . import media as audit

AT = datetime(2026, 10, 23, 12, tzinfo=UTC)
OBSERVED = AT - timedelta(days=1)
SOURCE = "https://scontent.cdninstagram.com/image.jpg"
GIF = "https://cdn.fbsbx.com/fixture.gif"
SHA = "b" * 64
LISTING_HASH = "a" * 64
RELEASE = "/fixture/maintained-release"


class PureStorage:
    prefix = ""

    def host(self, url, *, root, name, variants=False, **kwargs):
        suffix = Path(url.split("?", 1)[0]).suffix
        key = f"{root}/{name}{suffix}"
        entry = {
            "url": "https://media.example/" + key,
            "storage_key": key,
            "bytes": 120,
            "sha256": SHA,
            "format": suffix[1:],
            "content_type": "image/" + ("jpeg" if suffix == ".jpg" else suffix[1:]),
            "width": 12,
            "height": 8,
        }
        display = {}
        if variants:
            for size in ("thumb", "card", "detail", "poster_card", "poster_detail"):
                for fmt in ("webp", "jpeg"):
                    role = f"{size}_{fmt}"
                    variant_key = f"{root}/{role}.{fmt}"
                    display[role] = {
                        "url": "https://media.example/" + variant_key,
                        "storage_key": variant_key,
                        "bytes": 50,
                        "format": fmt,
                        "content_type": "image/" + fmt,
                        "width": 12,
                        "height": 8,
                        "source_role": "cover",
                        "variant_key": role,
                    }
        return entry, display


def _fixture(*, attachment=False, comment=False, all_roles=False):
    post_id, canonical_id, comment_id, row_id = str(uuid4()), str(uuid4()), "123", str(uuid4())
    owner_url = "https://scontent.cdninstagram.com/owner.jpg"
    owner_key = "social/instagram/profile-pics/bravotv/" + SHA + ".jpg"
    avatars = [
        AvatarSource(
            "bravotv",
            owner_url,
            "owner",
            observed_at=OBSERVED,
            cached_hosted_url="https://media.example/" + owner_key,
            cached_sha256=SHA,
        )
    ]
    saved = []
    comment_sources = []
    proof = None
    if attachment or comment or all_roles:
        saved_row = {
            "id": row_id,
            "post_id": post_id,
            "comment_id": comment_id,
            "username": "author",
            "user_id": "9",
            "text": "",
            "source_account": "bravotv",
            "is_reply": False,
            "parent_comment_id": None,
            "parent_comment_external_id": None,
            "is_missing": False,
            "deleted_at": None,
            "scraped_at": OBSERVED,
            "last_seen_at": OBSERVED,
            "source_snapshot_type": "full_comments_scrape",
            "media_urls": [SOURCE],
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": None,
            "raw_data": {},
            "shortcode": "Fixture",
            "uuid_parent_native_id": None,
            "uuid_parent_post_id": None,
        }
        if attachment:
            info = {"id": "456", "images": {"fixed_height": {"url": GIF}}}
            proof = AttachmentRefresh(
                row_id,
                comment_id,
                post_id,
                "Fixture",
                None,
                "456",
                OBSERVED + timedelta(hours=1),
                attachment_baseline(saved_row),
                GIF,
                info,
                "instagram:comment-attachment:Fixture:123",
                "",
            )
            proof = replace(proof, evidence_sha256=proof._evidence().content_hash)
            proof.validate()
            saved_row["raw_data"] = {"ingestion_attachment": proof.to_payload(), "giphy_media_info": info}
            saved_row["media_urls"] = [GIF]
        comment_sources = [
            CommentMedia(
                comment_id, row_id, tuple(saved_row["media_urls"]), "unknown-" + "1" * 24, OBSERVED, proof, None
            )
        ]
        saved.append(saved_row)
    if all_roles:
        for handle, role, native_id in (
            ("tagged", "tagged", None),
            ("collab", "collaborator", None),
            ("mentioned", "mention", None),
            ("author", "comment", comment_id),
        ):
            avatars.append(
                AvatarSource(
                    handle,
                    f"https://scontent.cdninstagram.com/{handle}.jpg",
                    role,
                    native_id,
                    OBSERVED,
                    f"https://media.example/social/instagram/profile-pics/{handle}/{SHA}.jpg",
                    SHA,
                )
            )
    source = MediaSnapshot(
        OBSERVED,
        post_id,
        canonical_id,
        "Fixture",
        str(uuid4()),
        0,
        1,
        (SOURCE,),
        SOURCE,
        1,
        tuple(comment_sources),
        tuple(avatars),
        True,
        True,
        include_comment_avatars=all_roles,
    )
    result = mirror_snapshot(source, cast(Any, PureStorage()), check=lambda: None, request=lambda: None)
    payload = serialize_media_result(result)
    evidence = Evidence.capture(
        source_identity="instagram:media:Fixture",
        captured_at=OBSERVED,
        parser_version=MEDIA_RESULT_REPLAY_VERSION,
        payload=payload,
    )
    post = {
        "id": post_id,
        "shortcode": "Fixture",
        "username": "bravotv",
        "source_account": "bravotv",
        "owner_username": "bravotv",
        "owner_profile_pic_url": owner_url,
        "scraped_at": OBSERVED.isoformat(),
        "media_urls": [SOURCE],
        "thumbnail_url": SOURCE,
        "raw_data": {"detail_snapshot": {"media_count": 1}},
        **deepcopy(result.post_fields),
    }
    if all_roles:
        post.update(
            profile_tags=["tagged"],
            collaborators=["collab"],
            mentions=["mentioned"],
            tagged_users_detail=[{"username": "tagged", "profile_pic_url": avatars[1].source_url}],
            collaborators_detail=[{"username": "collab", "profile_pic_url": avatars[2].source_url}],
        )
        # Mention source is registry fallback under maintained loader semantics.
        saved[0]["author_profile_pic_url"] = avatars[-1].source_url
    canonical = {
        "id": canonical_id,
        "source_id": "Fixture",
        "last_seen_at": OBSERVED.isoformat(),
        "last_scraped_at": OBSERVED.isoformat(),
    }
    asset = {
        "id": str(uuid4()),
        "post_id": canonical_id,
        "position": 0,
        "source_url": SOURCE,
        "thumbnail_url": SOURCE,
        "hosted_url": result.post_fields["hosted_media_urls"][0],
        "hosted_thumbnail_url": result.post_fields["hosted_thumbnail_url"],
        "mirror_status": "mirrored",
        "mirror_error": None,
        "media_payload": {
            "ingestion_media": deepcopy(result.post_fields["asset_manifest"]["ingestion_media"]),
            "raw_child_posts": [],
        },
    }
    for row in saved:
        row.update(result.comment_fields[row["comment_id"]])
        row["raw_data"]["ingestion_media"] = {"version": MEDIA_VERSION, "observed_at": utc_timestamp(OBSERVED)}
    token, comment_token = str(uuid4()), str(uuid4())
    row = {
        "target_key": "Fixture",
        "target_outcome": "committed",
        "target_owner_token": token,
        "target_owner_generation": 3,
        "owner_generation": 3,
        "receipt_token": token,
        "receipt_outcome": "committed",
        "dispatch_identity_matches": True,
        "canonical_identity": {"post_id": post_id, "canonical_post_id": canonical_id},
        "observed_at": OBSERVED,
        "evidence_id": str(uuid4()),
        "source_identity": evidence.source_identity,
        "captured_at": evidence.captured_at,
        "expires_at": evidence.expires_at,
        "parser_version": evidence.parser_version,
        "schema_version": evidence.schema_version,
        "content_hash": evidence.content_hash,
        "payload": evidence.payload_json,
        "post": post,
        "sidecar": {"raw_data": {}, "asset_manifest": deepcopy(result.post_fields["asset_manifest"])},
        "canonical": canonical,
        "comments_identity": {
            "post_id": post_id,
            "observed_comment_ids": [c["comment_id"] for c in saved],
            "settlement": {"sealed": True},
        },
        "comments_observed_at": OBSERVED + timedelta(hours=2),
        "comments_outcome": "committed",
        "comments_receipt_outcome": "committed",
        "comments_checkpoint": {"comment_pages": {"version": 1, "scan_root_evidence_id": str(uuid4())}},
        "comments_evidence_id": str(uuid4()),
        "comments_target_owner_token": comment_token,
        "comments_target_owner_generation": 4,
        "comments_receipt_token": comment_token,
        "comments_owner_generation": 4,
        "comments_dispatch_identity_matches": True,
    }
    registry = [
        {
            "account_handle": avatar.username,
            "source_url": avatar.source_url,
            "hosted_url": avatar.cached_hosted_url,
            "content_hash": SHA,
            "last_checked_at": OBSERVED,
            "status": "mirrored",
            "id": str(uuid4()),
        }
        for avatar in avatars
    ]
    comment_proof = {
        "status": "passed",
        "operator_scope": "comments_and_replies_not_final_certifier",
        "run_id": audit.common.RUN,
        "intent_hash": audit.common.INTENT,
        "release": RELEASE,
        "full_history_gate_closed": True,
        "listing_proof_sha256": LISTING_HASH,
        "failures": [],
        "pending": [],
        "count_gaps": [],
        "verified": [
            {
                "shortcode": "Fixture",
                "checkpoint_sha256": audit.digest(row["comments_checkpoint"]),
                "saved_rows_sha256": "diagnostic-not-current",
                "media_binding": audit.comment_binding(row, saved),
            }
        ],
    }
    return (
        {
            "row": row,
            "comments": saved,
            "saved_assets": [asset],
            "registry": registry,
            "at": AT,
            "pinned": True,
            "comment_proof": comment_proof,
            "release": RELEASE,
            "listing_hash": LISTING_HASH,
        },
        source,
        result,
    )


def fixture(**options):
    values, source, result = _fixture(**options)
    return repin_inventory(values, source, result)


def repin_inventory(values, source, result):
    from trr_backend.ingestion.certification import inventory_fingerprint

    row = values["row"]
    fingerprint = inventory_fingerprint({**row["post"], "payload_sidecar": row["sidecar"]}, values["comments"])
    source = replace(source, inventory_fingerprint=fingerprint)
    result = replace(result, snapshot=source)
    evidence = Evidence.capture(
        source_identity="instagram:media:" + source.shortcode,
        captured_at=OBSERVED,
        parser_version=MEDIA_RESULT_REPLAY_VERSION,
        payload=serialize_media_result(result),
    )
    row.update(payload=evidence.payload_json, content_hash=evidence.content_hash)
    row["canonical_identity"]["inventory_fingerprint"] = fingerprint
    return values, source, result


class MediaAuditOffline(unittest.TestCase):
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

    def run_fixture(self, **options):
        values, source, result = fixture(**options)
        return values, source, result, audit.audit_target(**values)

    def codes(self, values):
        return audit.audit_target(**values).get("failure", {}).get("codes", [])

    def test_positive_complete_records_and_rich_plan_no_mutation(self):
        values, source, result = fixture(all_roles=True)
        before = deepcopy(values)
        report = audit.audit_target(**values)
        self.assertIn("verified", report, report)
        self.assertEqual(values, before)
        self.assertEqual(
            {entry["avatar_identity"]["role"] for entry in report["plan"] if "avatar_identity" in entry},
            {"owner", "tagged", "collaborator", "mention", "comment"},
        )
        for entry in report["plan"]:
            for field in (
                "source_evidence_sha256",
                "source_evidence_parser",
                "source_evidence_schema",
                "legacy_post_id",
                "canonical_post_id",
                "storage_key",
                "source_reference",
                "required_decode_mode",
                "historical_hash_limit",
                "original_observed_at",
            ):
                self.assertIn(field, entry)
        variants = [entry for entry in report["plan"] if entry["role"] == "card_webp"]
        self.assertEqual(variants[0]["historical_hash_limit"], "original_bytes_hash_not_retained")

    def test_exact_reservation_and_comment_fences(self):
        for field, value in (
            ("receipt_token", "stale"),
            ("owner_generation", 2),
            ("comments_receipt_token", "stale"),
            ("comments_owner_generation", 2),
        ):
            with self.subTest(field=field):
                values, _, _ = fixture()
                values["row"][field] = value
                self.assertTrue(self.codes(values))

    def test_proof_absence_and_missing_binding_are_pending(self):
        values, _, _ = fixture()
        values["comment_proof"] = None
        self.assertEqual(audit.audit_target(**values)["pending"]["codes"], ["separate_deep_comment_proof_required"])
        values, _, _ = fixture()
        del values["comment_proof"]["verified"][0]["media_binding"]
        self.assertIn("explicit_comment_media_binding_required", audit.audit_target(**values)["pending"]["codes"])
        self.assertEqual(audit.exit_status({"status": "pending"}), 3)

    def test_exact_comment_contract_drift_and_unique_inventory(self):
        for field in (
            "scan_root_evidence_id",
            "post_id",
            "receipt",
            "media_reservation",
            "media_manifest_sha256",
            "saved_rows_sha256",
        ):
            values, _, _ = fixture(comment=True)
            values["comment_proof"]["verified"][0]["media_binding"][field] = "wrong"
            self.assertTrue(self.codes(values), field)
        values, _, _ = fixture(comment=True)
        values["comment_proof"]["verified"][0]["media_binding"]["observed_comment_ids"] *= 2
        self.assertIn("deep_comment_unique_inventory_mismatch", self.codes(values))

    def test_join_only_comment_fields_do_not_change_canonical_hash(self):
        values, _, _ = fixture(comment=True)
        a = audit.digest(audit.canonical_comment_rows(values["comments"]))
        values["comments"][0]["uuid_parent_native_id"] = "diagnostic"
        self.assertEqual(a, audit.digest(audit.canonical_comment_rows(values["comments"])))
        values["comments"][0]["text"] = "new"
        self.assertNotEqual(a, audit.digest(audit.canonical_comment_rows(values["comments"])))

    def test_original_source_clocks_count_thumbnail_and_markers(self):
        for field in ("scraped_at", "last_seen_at", "last_scraped_at"):
            values, _, _ = fixture()
            values["row"]["post" if field == "scraped_at" else "canonical"][field] = AT.isoformat()
            self.assertIn("source_provenance_" + field + "_mismatch", self.codes(values))
        values, _, _ = fixture()
        values["row"]["post"]["raw_data"]["detail_snapshot"]["media_count"] = 2
        self.assertIn("current_native_media_count_mismatch", self.codes(values))
        values, _, _ = fixture()
        values["row"]["post"]["thumbnail_url"] = "https://scontent.cdninstagram.com/wrong.jpg"
        self.assertIn("current_thumbnail_source_mismatch", self.codes(values))
        values, _, _ = fixture(comment=True)
        values["comments"][0]["raw_data"]["ingestion_media"]["observed_at"] = AT.isoformat()
        self.assertIn("comment_ingestion_media_marker_mismatch", self.codes(values))

    def test_ordered_children_and_asset_ordering(self):
        values, source, result = fixture()
        children = [{"slide_index": 0, "display_url": SOURCE}]
        values["row"]["sidecar"]["child_posts_data"] = children
        values["saved_assets"][0]["media_payload"]["raw_child_posts"] = deepcopy(children)
        repin_inventory(values, source, result)
        self.assertIn("verified", audit.audit_target(**values))
        children[0]["display_url"] = "https://scontent.cdninstagram.com/wrong.jpg"
        self.assertIn("ordered_child_source_mismatch", self.codes(values))
        values, _, _ = fixture()
        values["saved_assets"][0]["position"] = 1
        self.assertIn("canonical_asset_position_set_mismatch", self.codes(values))

    def test_retained_exact_avatar_version_survives_current_different_image(self):
        values, _, _ = fixture()
        newer = {
            **values["registry"][0],
            "source_url": "https://scontent.cdninstagram.com/new-owner.jpg",
            "last_checked_at": AT,
        }
        values["registry"].insert(0, newer)
        report = audit.audit_target(**values)
        self.assertIn("verified", report, report)
        self.assertEqual(report["verified"]["avatar_slots"][0]["current_different_versions"], 1)
        values["registry"][1]["content_hash"] = "c" * 64
        self.assertIn("avatar_registry_version_proof_mismatch", self.codes(values))

    def test_omitted_avatar_slot_and_registry_time_or_status_fail(self):
        for field, value in (
            ("last_checked_at", AT),
            ("status", "failed"),
            ("hosted_url", "https://media.example/other.jpg"),
        ):
            values, _, _ = fixture()
            values["registry"][0][field] = value
            self.assertTrue(self.codes(values))
        values, _, _ = fixture()
        values["row"]["post"]["mentions"] = ["newperson"]
        self.assertTrue(self.codes(values))

    def test_already_applied_attachment_is_independent_and_baseline_is_preapply(self):
        values, source, result = fixture(attachment=True)
        before = deepcopy(values["comments"][0])
        assert source.comments[0].attachment_refresh is not None
        self.assertNotEqual(attachment_baseline(before), source.comments[0].attachment_refresh.baseline_sha256)
        checks = []
        native = audit.verify_attachment(
            source.comments[0], before, source, lambda ok, code: checks.append(code) if not ok else None
        )
        self.assertEqual(checks, [])
        assert native is not None
        self.assertEqual(native["kind"], "attachment_refresh")
        self.assertEqual(values["comments"][0], before)
        report = audit.audit_target(**values)
        self.assertIn("verified", report, report)
        gif = next(entry for entry in report["plan"] if entry["role"].startswith("comment:"))
        self.assertEqual(gif["required_decode_mode"], "gif_all_frames_native_identity")
        self.assertTrue(gif["native_gif_proof"]["proof"]["native_gif_id"])

    def test_attachment_version_only_and_corruption_fail_closed(self):
        values, source, _ = fixture(attachment=True)
        assert source.comments[0].attachment_refresh is not None
        applied = replace(
            source.comments[0],
            attachment_refresh=None,
            attachment_version=source.comments[0].attachment_refresh.evidence_sha256,
        )
        checks = []
        audit.verify_attachment(
            applied, values["comments"][0], source, lambda ok, code: checks.append(code) if not ok else None
        )
        self.assertEqual(checks, [])
        for field in (
            "post_id",
            "comment_id",
            "parent_comment_id",
            "native_gif_id",
            "baseline_sha256",
            "evidence_sha256",
        ):
            corrupted = deepcopy(values["comments"][0])
            corrupted["raw_data"]["ingestion_attachment"][field] = "corrupt"
            with self.assertRaises(ValueError, msg=field):
                audit.verify_attachment(source.comments[0], corrupted, source, lambda ok, code: None)
        del values["comments"][0]["raw_data"]["ingestion_attachment"]
        self.assertIn("attachment_applied_marker_missing", self.codes(values))

    def test_older_attachment_marker_is_history_after_newer_native_gif_capture(self):
        values, source, _ = fixture(attachment=True)
        saved = values["comments"][0]
        current = OBSERVED + timedelta(hours=2)
        saved["scraped_at"] = current
        new_gif = "https://cdn.fbsbx.com/new-version.gif"
        saved["media_urls"] = [new_gif]
        saved["raw_data"]["giphy_media_info"] = {"id": "789", "images": {"fixed_height": {"url": new_gif}}}
        old = source.comments[0]
        assert old.attachment_refresh is not None
        item = replace(
            old,
            observed_at=current,
            media_urls=(new_gif,),
            attachment_refresh=None,
            attachment_version=old.attachment_refresh.evidence_sha256,
        )
        checks = []
        proof = audit.verify_attachment(item, saved, source, lambda ok, code: checks.append(code) if not ok else None)
        self.assertEqual(checks, [])
        assert proof is not None
        self.assertEqual(proof["native_gif_id"], "789")
        self.assertTrue(proof["retained_history"]["current_selection_requires_separate_deep_comment_proof"])
        self.assertEqual(proof["retained_history"]["proof"]["native_gif_id"], "456")
        saved["media_urls"] = [GIF]
        audit.verify_attachment(item, saved, source, lambda ok, code: checks.append(code) if not ok else None)
        self.assertIn("native_gif_selection_mismatch", checks)

    def test_current_native_giphy_has_independent_identity_and_selection(self):
        values, source, _ = fixture(comment=True)
        item = replace(source.comments[0], media_urls=(GIF,))
        saved = values["comments"][0]
        saved["media_urls"] = [GIF]
        saved["raw_data"]["giphy_media_info"] = {"id": "456", "images": {"fixed_height": {"url": GIF}}}
        checks = []
        proof = audit.verify_attachment(item, saved, source, lambda ok, code: checks.append(code) if not ok else None)
        self.assertEqual(checks, [])
        assert proof is not None
        self.assertEqual(proof["native_gif_id"], "456")
        del saved["raw_data"]["giphy_media_info"]
        audit.verify_attachment(item, saved, source, lambda ok, code: checks.append(code) if not ok else None)
        self.assertIn("native_gif_identity_proof_missing", checks)

    def test_nested_missing_comment_receipt_contract_is_pending(self):
        values, _, _ = fixture()
        del values["comment_proof"]["verified"][0]["media_binding"]["receipt"]["evidence_id"]
        self.assertIn("explicit_comment_media_binding_required", audit.audit_target(**values)["pending"]["codes"])

    def test_inspect_queries_are_read_only_and_keep_unaffected_targets(self):
        values, _, _ = fixture()
        pending = deepcopy(values["row"])
        pending["target_key"], pending["target_outcome"] = "Pending", "pending"
        queries = []

        class Cursor:
            connection = object()

            def execute(self, sql, args=()):
                queries.append(sql)
                self.last = sql
                if not (sql.startswith("show ") or sql.startswith("select ")):
                    raise AssertionError("mutation_attempt")

            def fetchone(self):
                return {"transaction_read_only": "on", "transaction_isolation": "repeatable read", "at": AT}

        def rows(cur, sql, args=()):
            cur.execute(sql, args)
            if "from social.instagram_ingestion_targets t" in sql:
                self.assertIn("r.owner_token=t.owner_token and r.owner_generation=t.owner_generation", sql)
                self.assertIn("cr.owner_token=ct.owner_token and cr.owner_generation=ct.owner_generation", sql)
                return [values["row"], pending]
            if "join social.instagram_comments c" in sql:
                return values["comments"]
            if "from social.social_post_media_assets" in sql:
                return values["saved_assets"]
            if "from social.avatar_registry" in sql:
                self.assertIn("account_handle=any(%s)", sql)
                self.assertIn("bravotv", args[0])
                return values["registry"]
            raise AssertionError("unexpected_query")

        listing = {
            "run_id": audit.common.RUN,
            "intent_hash": audit.common.INTENT,
            "listing_proof": {"independent_native_membership_verified": True, "boundary_verified": True},
            "reported_january_keys": ["Fixture", "Pending"],
            "independently_verified_january_count": 2,
        }
        progress = {"intent": {}, "intent_hash": audit.common.INTENT, "generation": 1, "state": "running"}
        with (
            patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
            patch.object(audit.common, "rows", side_effect=rows),
        ):
            report = audit.inspect(
                Cursor(), {"successor_intent": {}}, RELEASE, listing, values["comment_proof"], LISTING_HASH
            )
        self.assertEqual(report["status"], "pending")
        self.assertEqual(report["verified_targets"], 1)
        self.assertFalse(report["final_acceptance"])
        self.assertEqual(report["database_mutations"], 0)
        self.assertTrue(report["byte_read_plan"]["objects"])

    def test_expected_target_failures_are_batched_and_do_not_leak_urls(self):
        values, _, _ = fixture()
        values["row"]["payload"] = '{"secret":"https://example.com/?signed=private"}'
        bad = audit.audit_target(**values)
        values, _, _ = fixture()
        good = audit.audit_target(**values)
        results = audit.reconcile_plans([bad, good])
        self.assertIn("failure", results[0])
        self.assertIn("verified", results[1])
        self.assertNotIn("private", json.dumps(results[0]))
        values["row"]["schema_version"] = 999
        self.assertIn("target_evidence_or_saved_state_invalid", self.codes(values))

    def test_key_and_url_conflicts_only_block_affected_targets(self):
        def record(key, url, storage_key, sha=SHA):
            return {
                "verified": {"shortcode": key},
                "plan": [{"shortcode": key, "url": url, "storage_key": storage_key, "historical_sha256": sha}],
            }

        for conflict in (
            record("B", "https://cdn.example/a", "other"),
            record("B", "https://cdn.example/b", "key"),
            record("B", "https://cdn.example/a", "key", "d" * 64),
        ):
            results = audit.reconcile_plans(
                [record("A", "https://cdn.example/a", "key"), conflict, record("C", "https://cdn.example/c", "safe")]
            )
            self.assertIn("failure", results[0])
            self.assertIn("failure", results[1])
            self.assertIn("verified", results[2])

    def test_storage_key_url_mismatch_fails_without_signed_url_output(self):
        values, source, result = fixture()
        check_codes = []
        result.post_fields["asset_manifest"]["originals"]["cover"]["storage_key"] = "wrong/key.jpg"
        audit.object_plan(
            values["row"], source, result, {}, [], lambda ok, code: check_codes.append(code) if not ok else None
        )
        self.assertIn("hosted_storage_key_url_mismatch", check_codes)

    def test_network_database_and_storage_guards_are_active(self):
        import socket

        import psycopg2
        import requests

        for callback in (
            lambda: requests.get("https://example.com"),
            lambda: psycopg2.connect(""),
            lambda: socket.create_connection(("example.com", 443)),
        ):
            with self.assertRaises(AssertionError):
                callback()


if __name__ == "__main__":
    unittest.main()
