#!/usr/bin/env python3
"""Read-only media records audit and byte plan; never final January acceptance.

No source, hosted-object or storage calls. The sole controller may execute the
SELECT-only snapshot. An independently passing deep comment proof is mandatory.
Exit codes: 0 passed records, 2 blocked, 3 pending. Bytes remain a separate gate.
"""

# Retained SQL and diagnostic vocabularies are kept intact for audit comparison.
# ruff: noqa: E501
from __future__ import annotations

import json
import re
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import UUID

from . import common
from .posts import digest, equal

COMMENT_BINDING_FIELDS = {
    "checkpoint_sha256": "digest of the exact current comments target checkpoint",
    "saved_rows_sha256": "retained deep-comment diagnostic hash (not used for current media binding)",
    "media_binding": {
        "saved_rows_sha256": "digest of canonical full comment rows with only three join fields removed",
        "post_id": "current receipt and saved legacy post UUID",
        "scan_root_evidence_id": "current checkpoint comment_progress scan root",
        "observed_comment_ids": "unique exact current native comment ID inventory",
        "receipt": ["owner_token", "owner_generation", "evidence_id", "observed_at", "canonical_identity_sha256"],
        "media_reservation": ["owner_token", "owner_generation"],
        "media_manifest_sha256": "digest of exact saved post asset_manifest including ingestion_media seal",
    },
}
EXPECTED_TARGET_ERRORS = (ValueError, TypeError, KeyError, IndexError, AttributeError, OverflowError, common.GuardError)


def exit_status(report):
    return {"passed": 0, "blocked": 2, "pending": 3}[report["status"]]


def refs(urls):
    from trr_backend.ingestion.media import source_reference

    return [source_reference(url) for url in urls or []]


def stamp(value):
    value = common.stamp(value)
    if value.utcoffset() is None:
        raise ValueError("timezone_required")
    return value


def comment_saved_rows(rows):
    """Keep the exact projection/order used by the separate deep comment audit."""
    return sorted(rows, key=lambda row: (str(row["comment_id"]), str(row["id"])))


def canonical_comment_rows(rows):
    joined = {"shortcode", "uuid_parent_native_id", "uuid_parent_post_id"}
    return [{key: value for key, value in row.items() if key not in joined} for row in comment_saved_rows(rows)]


def comment_binding(row, comments):
    """Expected artifact contract, independently computed from current state."""
    from trr_backend.ingestion.comments import _progress

    state = _progress(row["comments_checkpoint"])
    return {
        "saved_rows_sha256": digest(canonical_comment_rows(comments)),
        "post_id": str(row["post"]["id"]),
        "scan_root_evidence_id": state.get("scan_root_evidence_id"),
        "observed_comment_ids": sorted(str(c["comment_id"]) for c in comments),
        "receipt": {
            "owner_token": str(row["comments_receipt_token"]),
            "owner_generation": row["comments_owner_generation"],
            "evidence_id": str(row["comments_evidence_id"]),
            "observed_at": stamp(row["comments_observed_at"]).isoformat(),
            "canonical_identity_sha256": digest(row["comments_identity"]),
        },
        "media_reservation": {
            "owner_token": str(row["target_owner_token"]),
            "owner_generation": row["target_owner_generation"],
        },
        "media_manifest_sha256": digest(row["post"].get("asset_manifest") or {}),
    }


def _certificate_hash(value):
    return isinstance(value, str) and bool(re.fullmatch(r"[a-f0-9]{64}", value))


def _certificate_uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def _certificate_clock(value):
    try:
        return isinstance(value, str) and stamp(value).utcoffset() is not None
    except (ValueError, TypeError, AttributeError):
        return False


# Version 1 recognizes the producer's static diagnostics and maintained field
# projections. Unknown codes hold the complete artifact instead of disappearing
# inside an otherwise valid unselected target.
_SUPPORTED_COMMENT_CODES = set(
    """
active_saved_inventory_mismatch attachment_applied_identity_mismatch attachment_applied_marker_missing
attachment_applied_proof_changed attachment_applied_selection_mismatch attachment_current_comment_missing
attachment_current_native_gif_mismatch attachment_current_post_mismatch attachment_media_receipt_fence_mismatch
attachment_media_result_missing attachment_ordinary_media_proof_required attachment_proof_predates_native_comment
attachment_snapshot_original_clock_mismatch attachment_snapshot_saved_identity_mismatch attachment_snapshot_saved_source_mismatch
attachment_version_changed child_restart_not_root comment_original_capture_clock_mismatch
current_native_inventory_mismatch current_scan_root_missing_or_duplicate fresh_full_comment_observation_missing
global_audit_precondition_or_unclassified_failure independent_post_comment_count_mismatch native_created_at_mismatch
native_expected_total_mismatch native_gif_identity_proof_missing native_gif_selection_mismatch
native_lanes_or_settlement_incomplete native_parent_external_mismatch native_parent_uuid_mismatch
native_saved_uuid_mismatch native_uuid_changed_during_scan newer_attachment_retained_media_proof_pending
page_canonical_native_id_set_mismatch page_canonical_post_id_mismatch page_evidence_hash_or_clock_mismatch
page_generation_invalid page_source_mismatch parsed_page_receipt_mismatch
post_count_capture_binding_or_order_mismatch raw_native_parent_mismatch saved_account_mismatch
scan_cutoff_after_capture selected_comment_attachment_mismatch settlement_last_page_clock_mismatch
settlement_native_inventory_not_unique settlement_post_mapping_mismatch settlement_receipt_checkpoint_mismatch
settlement_receipt_fence_mismatch source_attested_comment_raw_fields_mismatch source_page_evidence_manifest_incomplete
target_deep_audit_unresolved target_retained_evidence_or_deep_validation_failed top_level_has_uuid_parent
zero_gap_settlement_required
""".split()
)
_SUPPORTED_COMMENT_CODES.update(
    f"page_replay_{field}_mismatch"
    for field in "version observed lanes expected adapter_family adapter_version scan_started_at scan_root_evidence_id".split()
)
_SUPPORTED_COMMENT_CODES.update(
    f"final_page_replay_{field}_mismatch"
    for field in "observed lanes expected adapter_family adapter_version scan_started_at scan_root_evidence_id".split()
)
_SUPPORTED_COMMENT_CODES.update(
    f"native_content_{field}_mismatch" for field in "text username user_id likes is_reply reply_depth".split()
)
_SUPPORTED_SOURCE_ATTESTED_FIELDS = set(
    """
post_id comment_id username user_id text likes reply_count is_reply parent_comment_external_id reply_depth created_at
scraped_at last_seen_at last_seen_run_id source_account source_snapshot_type author_full_name author_profile_pic_url
 author_profile_pic_url_hd author_is_verified author_fbid_v2 author_is_mentionable author_is_private author_latest_reel_media
 author_profile_pic_id hosted_media_urls media_mirror_status media_mirror_error media_mirror_attempt_count
media_mirror_last_attempt_at media_mirror_last_job_id hosted_author_profile_pic_url comment_url is_covered is_ranked
comment_index phase did_report_as_spam status is_edited is_pinned meta_ai_comment_type child_comment_count
liked_by_media_coauthors cursor_min_id cursor_param cursor_payload comment_filter_param
""".split()
)
_SUPPORTED_COMMENT_CODES.update(f"source_attested_{field}_mismatch" for field in _SUPPORTED_SOURCE_ATTESTED_FIELDS)


def _certificate_codes(value, *, empty=False):
    return (
        isinstance(value, list)
        and (empty or bool(value))
        and all(isinstance(code, str) and code in _SUPPORTED_COMMENT_CODES for code in value)
        and len(value) == len(set(value))
    )


def _certificate_settlement(value, *, empty=False):
    if not isinstance(value, dict):
        return False
    if not value:
        return empty
    return (
        set(value) == {"basis", "count_gap", "expected_total", "observed_total"}
        and all(
            type(value[field]) is int and value[field] >= 0
            for field in ("count_gap", "expected_total", "observed_total")
        )
        and (value["count_gap"] == max(value["expected_total"] - value["observed_total"], 0))
        and value["basis"] == ("public_native_exhaustion" if value["count_gap"] else "count_parity")
    )


def _owned_media_reservation(value):
    return (
        isinstance(value, dict)
        and set(value) == {"owner_token", "owner_generation"}
        and (
            _certificate_uuid(value["owner_token"])
            and type(value["owner_generation"]) is int
            and value["owner_generation"] >= 1
        )
    )


def _certificate_binding(value):
    fields = {
        "saved_rows_sha256",
        "post_id",
        "scan_root_evidence_id",
        "observed_comment_ids",
        "receipt",
        "media_reservation",
        "media_manifest_sha256",
    }
    if not isinstance(value, dict) or set(value) != fields:
        return False
    ids = value["observed_comment_ids"]
    receipt, reservation = value["receipt"], value["media_reservation"]
    if (
        not isinstance(ids, list)
        or not all(isinstance(item, str) and item for item in ids)
        or len(ids) != len(set(ids))
    ):
        return False
    if not isinstance(receipt, dict) or set(receipt) != {
        "owner_token",
        "owner_generation",
        "evidence_id",
        "observed_at",
        "canonical_identity_sha256",
    }:
        return False
    if not isinstance(reservation, dict) or set(reservation) != {"owner_token", "owner_generation"}:
        return False
    return all(_certificate_hash(value[field]) for field in ("saved_rows_sha256", "media_manifest_sha256")) and (
        _certificate_uuid(value["post_id"])
        and _certificate_uuid(value["scan_root_evidence_id"])
        and _certificate_uuid(receipt["evidence_id"])
        and _certificate_clock(receipt["observed_at"])
        and _certificate_hash(receipt["canonical_identity_sha256"])
        and _certificate_uuid(receipt["owner_token"])
        and type(receipt["owner_generation"]) is int
        and receipt["owner_generation"] >= 1
        # Comment completeness may be proved before media has an owner, or
        # after checkpoint_failure cleared it. This exact producer projection
        # is structural data; selected media eligibility is checked separately.
        and (
            _owned_media_reservation(reservation)
            or reservation["owner_token"] == "None"
            and type(reservation["owner_generation"]) is int
            and reservation["owner_generation"] >= 0
        )
    )


_DEEP_COMMENT_FIELDS = {
    "shortcode",
    "native_comments",
    "native_replies",
    "source_pages",
    "settlement",
    "original_settlement_at",
    "checkpoint_sha256",
    "saved_rows_sha256",
    "media_binding",
}
_CERTIFICATE_FIELDS = {
    "shortcode",
    "status",
    "diagnostics",
    "settlement",
    "source_page_evidence_manifest",
    "source_page_evidence_sha256",
    "source_page_evidence_complete",
}


def _deep_comment_shape(entry, *, required):
    if required and not _DEEP_COMMENT_FIELDS <= set(entry):
        return False
    for field in ("native_comments", "native_replies", "source_pages"):
        if field in entry and (type(entry[field]) is not int or entry[field] < (1 if field == "source_pages" else 0)):
            return False
    if "native_comments" in entry and "native_replies" in entry and entry["native_replies"] > entry["native_comments"]:
        return False
    if any(
        field in entry and not _certificate_hash(entry[field]) for field in ("checkpoint_sha256", "saved_rows_sha256")
    ):
        return False
    if "original_settlement_at" in entry and not _certificate_clock(entry["original_settlement_at"]):
        return False
    if (
        "media_binding" in entry
        and entry["media_binding"] is not None
        and not _certificate_binding(entry["media_binding"])
    ):
        return False
    return not required or _certificate_binding(entry["media_binding"])


def _source_page_shape(page, key, observed_at):
    fields = {
        "page_id",
        "evidence_id",
        "run_id",
        "stage",
        "target_key",
        "source_identity",
        "content_hash",
        "parser_version",
        "schema_version",
        "captured_at",
    }
    return (
        isinstance(page, dict)
        and set(page) == fields
        and all(_certificate_uuid(page[field]) for field in ("page_id", "evidence_id"))
        and (
            page["run_id"] == common.RUN
            and page["stage"] == "comments"
            and page["target_key"] == key
            and isinstance(page["source_identity"], str)
            and page["source_identity"]
            in {f"instagram:comments:{key}:{kind}" for kind in ("comments_api", "graphql_comments", "graphql_replies")}
            and _certificate_hash(page["content_hash"])
            and isinstance(page["parser_version"], str)
            and bool(page["parser_version"])
            and type(page["schema_version"]) is int
            and page["schema_version"] == 1
            and _certificate_clock(page["captured_at"])
            and _certificate_clock(observed_at)
            and stamp(page["captured_at"]) <= stamp(observed_at)
        )
    )


def _target_certificate_shape(entry, observed_at):
    if (
        not isinstance(entry, dict)
        or not _CERTIFICATE_FIELDS <= set(entry)
        or not set(entry) <= _CERTIFICATE_FIELDS | _DEEP_COMMENT_FIELDS
    ):
        return False
    key, status = entry["shortcode"], entry["status"]
    if (
        not isinstance(key, str)
        or not key
        or not isinstance(status, str)
        or status not in {"passed", "pending", "blocked"}
    ):
        return False
    passed = status == "passed"
    if not _certificate_codes(entry["diagnostics"], empty=passed) or passed and entry["diagnostics"]:
        return False
    if not _certificate_settlement(entry["settlement"], empty=not passed) or not _deep_comment_shape(
        entry, required=passed
    ):
        return False
    pages = entry["source_page_evidence_manifest"]
    complete = entry["source_page_evidence_complete"]
    if (
        not isinstance(pages, list)
        or type(complete) is not bool
        or not _certificate_hash(entry["source_page_evidence_sha256"])
    ):
        return False
    if entry["source_page_evidence_sha256"] != digest(pages) or not all(
        _source_page_shape(page, key, observed_at) for page in pages
    ):
        return False
    if len({page["page_id"] for page in pages}) != len(pages) or len({page["evidence_id"] for page in pages}) != len(
        pages
    ):
        return False
    if complete and (
        not pages or len(pages) != entry.get("source_pages") or not _certificate_binding(entry.get("media_binding"))
    ):
        return False
    return not passed or complete and entry["settlement"]["count_gap"] == 0


def _legacy_comment_shape(field, entry, keys):
    if not isinstance(entry, dict) or not isinstance(entry.get("shortcode"), str) or entry["shortcode"] not in keys:
        return False
    if field == "failures":
        return set(entry) == {"shortcode", "codes"} and _certificate_codes(entry["codes"])
    if field == "pending":
        return (
            set(entry) == {"shortcode", "outcome"}
            and isinstance(entry["outcome"], str)
            and entry["outcome"] in {"unresolved", "source_unavailable", "cached_satisfied"}
        )
    if field == "count_gaps":
        return (
            set(entry) == {"shortcode", "basis", "count_gap", "expected_total", "observed_total"}
            and _certificate_settlement({key: value for key, value in entry.items() if key != "shortcode"})
            and entry["count_gap"] > 0
        )
    return (
        set(entry) == _DEEP_COMMENT_FIELDS
        and _deep_comment_shape(entry, required=True)
        and _certificate_settlement(entry["settlement"])
    )


def subset_envelope_errors(proof, release, listing_hash, audited_keys, actual_hash, expected_hash):
    """All certificate consumers must validate the whole envelope first."""
    codes = []

    def check(ok, code):
        if not ok:
            codes.append(code)

    if (
        not isinstance(proof, dict)
        or type(proof.get("target_certificate_version")) is not int
        or proof.get("target_certificate_version") != 1
    ):
        return ["supported_comment_target_certificate_required"]
    check(
        isinstance(actual_hash, str)
        and bool(re.fullmatch(r"[a-f0-9]{64}", actual_hash))
        and expected_hash == actual_hash,
        "comment_artifact_expected_sha256_mismatch_or_missing",
    )
    check(
        proof.get("operator_scope") == "comments_and_replies_not_final_certifier"
        and proof.get("account") == common.ACCOUNT
        and type(proof.get("generation")) is int
        and proof.get("generation") == common.GENERATION
        and proof.get("run_id") == common.RUN
        and proof.get("intent_hash") == common.INTENT
        and proof.get("release") == str(release)
        and proof.get("listing_proof_sha256") == listing_hash
        and proof.get("full_history_gate_closed") is True
        and proof.get("final_acceptance") is False,
        "comment_certificate_common_binding_mismatch",
    )
    global_result = proof.get("global_validation") or {}
    check(
        isinstance(global_result, dict)
        and global_result.get("status") == "passed"
        and global_result.get("codes") == []
        and isinstance(global_result.get("comments_manifest_sha256"), str)
        and bool(re.fullmatch(r"[a-f0-9]{64}", global_result["comments_manifest_sha256"])),
        "comment_certificate_global_validation_not_passed",
    )
    valid_keys = (
        isinstance(audited_keys, list)
        and bool(audited_keys)
        and all(isinstance(key, str) and key for key in audited_keys)
    )
    if not valid_keys or len(audited_keys) != len(set(audited_keys)):
        return [*codes, "current_audited_keyset_invalid"]
    keys = sorted(audited_keys)
    check(
        proof.get("audited_keys") == keys
        and proof.get("audited_keyset_sha256") == digest(keys)
        and proof.get("audited_january_count") == len(keys),
        "comment_certificate_audited_keyset_mismatch",
    )
    results = proof.get("target_results")
    valid_results = isinstance(results, list) and all(
        _target_certificate_shape(entry, proof.get("observed_at")) for entry in results
    )
    check(valid_results, "comment_certificate_target_schema_invalid")
    if valid_results:
        for entry in results:
            if entry["source_page_evidence_complete"]:
                pages = entry["source_page_evidence_manifest"]
                binding = entry["media_binding"]
                check(
                    pages[0]["evidence_id"] == binding["scan_root_evidence_id"]
                    and pages[-1]["evidence_id"] == binding["receipt"]["evidence_id"],
                    "comment_target_source_page_root_or_receipt_mismatch",
                )
                check(
                    stamp(pages[-1]["captured_at"]) == stamp(binding["receipt"]["observed_at"]),
                    "comment_target_source_page_receipt_clock_mismatch",
                )
    result_keys = [entry["shortcode"] for entry in results] if valid_results else []
    check(
        valid_results and len(result_keys) == len(set(result_keys)) == len(keys) and set(result_keys) == set(keys),
        "comment_certificate_target_coverage_not_exact",
    )
    check(proof.get("status") in {"passed", "pending", "blocked"}, "comment_certificate_aggregate_status_invalid")
    for field in ("failures", "pending", "count_gaps", "verified"):
        entries = proof.get(field)
        valid_entries = isinstance(entries, list) and all(
            _legacy_comment_shape(field, entry, keys) for entry in entries
        )
        if valid_entries:
            valid_entries = len({entry["shortcode"] for entry in entries}) == len(entries)
        check(valid_entries, "comment_certificate_unclassified_or_foreign_diagnostics")
        if valid_results and valid_entries:
            by_key = {entry["shortcode"]: entry for entry in results}
            for entry in entries:
                if entry["shortcode"] not in by_key:
                    check(False, "comment_certificate_target_coverage_not_exact")
                    continue
                status = by_key[entry["shortcode"]]["status"]
                check(field != "failures" or status == "blocked", "comment_certificate_target_status_inconsistent")
                check(field != "pending" or status == "pending", "comment_certificate_target_status_inconsistent")
                check(field != "count_gaps" or status != "passed", "comment_certificate_target_status_inconsistent")
                if field == "verified":
                    certificate = by_key[entry["shortcode"]]
                    check(
                        status in {"passed", "pending"}
                        and all(certificate.get(name) == value for name, value in entry.items()),
                        "comment_certificate_legacy_verified_inconsistent",
                    )
    return sorted(set(codes))


def subset_target_entry(proof, key, check, pending):
    matches = [entry for entry in proof["target_results"] if entry["shortcode"] == key]
    if len(matches) != 1:
        check(False, "comment_target_certificate_not_unique")
        return None
    entry = matches[0]
    if entry["status"] != "passed":
        if entry["status"] == "blocked":
            check(False, "comment_target_certificate_blocked")
        else:
            pending.append("comment_target_certificate_pending")
        return None
    settlement = entry.get("settlement") or {}
    check(
        entry.get("diagnostics") == []
        and isinstance(settlement, dict)
        and type(settlement.get("count_gap")) is int
        and settlement.get("count_gap") == 0,
        "comment_target_certificate_not_zero_gap",
    )
    for field in ("failures", "pending", "count_gaps"):
        check(
            not any(item["shortcode"] == key for item in proof[field]),
            "comment_target_certificate_contradicts_legacy_diagnostics",
        )
    legacy = [item for item in proof["verified"] if item["shortcode"] == key]
    check(
        len(legacy) <= 1
        and all(
            item.get("settlement") == settlement
            and item.get("media_binding") == entry.get("media_binding")
            and item.get("checkpoint_sha256") == entry.get("checkpoint_sha256")
            for item in legacy
        ),
        "comment_target_certificate_contradicts_legacy_verified",
    )
    pages = entry.get("source_page_evidence_manifest")
    valid_pages = isinstance(pages, list) and bool(pages) and all(isinstance(page, dict) for page in pages)
    check(
        valid_pages
        and entry.get("source_page_evidence_complete") is True
        and len(pages) == entry.get("source_pages")
        and entry.get("source_page_evidence_sha256") == digest(pages),
        "comment_target_source_page_manifest_invalid",
    )
    if valid_pages:
        ids = [page.get("evidence_id") for page in pages]
        page_ids = [page.get("page_id") for page in pages]
        check(
            all(isinstance(value, str) and value for value in [*ids, *page_ids])
            and len(ids) == len(set(ids))
            and len(page_ids) == len(set(page_ids)),
            "comment_target_source_page_identity_not_unique",
        )
        binding = entry.get("media_binding") or {}
        check(
            ids[0] == binding.get("scan_root_evidence_id")
            and ids[-1] == (binding.get("receipt") or {}).get("evidence_id"),
            "comment_target_source_page_root_or_receipt_mismatch",
        )
        check(
            stamp(pages[-1]["captured_at"]) == stamp(binding["receipt"]["observed_at"]),
            "comment_target_source_page_receipt_clock_mismatch",
        )
        for page in pages:
            valid = (
                page.get("run_id") == common.RUN and page.get("stage") == "comments" and page.get("target_key") == key
            )
            valid = valid and page.get("source_identity") in {
                f"instagram:comments:{key}:{kind}" for kind in ("comments_api", "graphql_comments", "graphql_replies")
            }
            valid = (
                valid
                and isinstance(page.get("content_hash"), str)
                and bool(re.fullmatch(r"[a-f0-9]{64}", page["content_hash"]))
            )
            valid = valid and isinstance(page.get("parser_version"), str) and bool(page["parser_version"])
            valid = valid and type(page.get("schema_version")) is int and page["schema_version"] == 1
            try:
                captured = stamp(page.get("captured_at"))
                valid = valid and captured <= stamp(proof["observed_at"])
            except (ValueError, TypeError, AttributeError, KeyError):
                valid = False
            check(valid, "comment_target_source_page_binding_invalid")
    return entry


OBSERVED_INVENTORY_SCOPE = "complete_observed_inventory_v1"


@dataclass(frozen=True)
class ObservedInventoryProof:
    comments: dict
    posts: dict
    errors: tuple[str, ...]
    comment_sha256: str
    post_sha256: str


def stable_proof_hash(proof):
    return digest({key: value for key, value in proof.items() if key != "observed_at"})


def observed_inventory_proof(proof, post_proof, release, listing_hash, keys):
    """Validate every observed target; source parity remains a separate gate."""
    errors = []
    comments_by_key, posts_by_key = {}, {}
    comment_hash = post_hash = ""
    try:
        comment_hash, post_hash = stable_proof_hash(proof), stable_proof_hash(post_proof)
        # Reuse the full envelope/schema validation, without admitting a subset.
        errors.extend(subset_envelope_errors(proof, release, listing_hash, keys, comment_hash, comment_hash))
        if errors:
            return ObservedInventoryProof({}, {}, tuple(sorted(set(errors))), comment_hash, post_hash)
        valid_post = (
            post_proof.get("status") == "passed"
            and post_proof.get("operator_scope") == "post_records_only_not_final_certifier"
            and post_proof.get("run_id") == common.RUN
            and post_proof.get("intent_hash") == common.INTENT
            and post_proof.get("release") == str(release)
            and post_proof.get("full_history_gate_closed") is True
            and not post_proof.get("failures")
            and not post_proof.get("pending")
            and post_proof.get("audited_january_count") == post_proof.get("verified_post_count") == len(keys)
        )
        post_rows = post_proof.get("verified", [])
        valid_post = valid_post and len(post_rows) == len(keys) and {row["shortcode"] for row in post_rows} == set(keys)
        if not valid_post:
            errors.append("observed_inventory_independent_post_proof_required")
        else:
            posts_by_key = {row["shortcode"]: row for row in post_rows}
            if any(
                not all(
                    _certificate_hash(row.get(field))
                    for field in ("post_row_sha256", "sidecar_sha256", "canonical_sha256")
                )
                for row in post_rows
            ):
                errors.append("observed_inventory_post_fingerprints_missing")
        entries = proof["target_results"]
        comments_by_key = {row["shortcode"]: row for row in proof["verified"]}
        if (
            proof.get("failures")
            or proof.get("pending")
            or len(comments_by_key) != len(keys)
            or len(proof["verified"]) != len(keys)
            or set(comments_by_key) != set(keys)
        ):
            errors.append("observed_inventory_all_native_targets_required")
        gaps = {
            row["shortcode"]: {key: value for key, value in row.items() if key != "shortcode"}
            for row in proof["count_gaps"]
        }
        expected_gaps = {}
        for entry in entries:
            settlement = entry["settlement"]
            if not _certificate_settlement(settlement) or not _deep_comment_shape(entry, required=True):
                errors.append("observed_inventory_known_native_settlement_required")
                continue
            gap = settlement["count_gap"]
            if gap:
                expected_gaps[entry["shortcode"]] = settlement
            allowed = ["zero_gap_settlement_required"] if gap else []
            if (
                entry["diagnostics"] != allowed
                or entry["status"] != ("pending" if gap else "passed")
                or entry["source_page_evidence_complete"] is not True
                or entry["native_comments"] != settlement["observed_total"]
                or entry["native_comments"] != len(entry["media_binding"]["observed_comment_ids"])
            ):
                errors.append("observed_inventory_native_traversal_or_parentage_unresolved")
        if gaps != expected_gaps or proof.get("status") != ("pending" if expected_gaps else "passed"):
            errors.append("observed_inventory_count_gap_manifest_mismatch")
    except (KeyError, TypeError, ValueError, AttributeError):
        errors.append("observed_inventory_full_proof_shape_invalid")
    return ObservedInventoryProof(comments_by_key, posts_by_key, tuple(sorted(set(errors))), comment_hash, post_hash)


def verify_comment_proof(
    row,
    comments,
    proof,
    release,
    listing_hash,
    check,
    pending,
    *,
    allow_verified_comment_subset=False,
    audited_keys=None,
    comment_proof_sha256=None,
    expected_comment_proof_sha256=None,
    observed_inventory=None,
):
    if proof is None:
        pending.append("separate_deep_comment_proof_required")
        return
    if observed_inventory is not None:
        if not isinstance(observed_inventory, ObservedInventoryProof):
            check(False, "observed_inventory_versioned_proof_required")
            return
        if observed_inventory.errors:
            for code in observed_inventory.errors:
                check(False, code)
            return
        entry = observed_inventory.comments.get(row["target_key"])
        if entry is None:
            check(False, "observed_inventory_current_target_missing")
            return
    elif allow_verified_comment_subset:
        errors = subset_envelope_errors(
            proof, release, listing_hash, audited_keys, comment_proof_sha256, expected_comment_proof_sha256
        )
        if errors:
            for code in errors:
                check(False, code)
            return
        entry = subset_target_entry(proof, row["target_key"], check, pending)
        if entry is None:
            return
    else:
        check(
            proof.get("status") == "passed"
            and proof.get("operator_scope") == "comments_and_replies_not_final_certifier"
            and proof.get("run_id") == common.RUN
            and proof.get("intent_hash") == common.INTENT
            and proof.get("release") == str(release)
            and proof.get("full_history_gate_closed") is True
            and proof.get("listing_proof_sha256") == listing_hash
            and not proof.get("failures")
            and not proof.get("pending")
            and not proof.get("count_gaps"),
            "deep_comment_proof_not_passing_or_bound",
        )
        matches = [entry for entry in proof.get("verified", []) if entry.get("shortcode") == row["target_key"]]
        if len(matches) != 1:
            pending.append("current_target_deep_comment_proof_required")
            return
        entry = matches[0]
    if not all(field in entry for field in COMMENT_BINDING_FIELDS):
        pending.append("explicit_comment_media_binding_required")
        return
    expected = comment_binding(row, comments)
    check(bool(expected["scan_root_evidence_id"]), "current_comment_scan_root_missing")
    actual = entry["media_binding"]
    if allow_verified_comment_subset:
        check(
            _owned_media_reservation(actual.get("media_reservation") if isinstance(actual, dict) else None),
            "comment_target_media_reservation_required",
        )
    if not isinstance(actual, dict) or not all(field in actual for field in expected):
        pending.append("explicit_comment_media_binding_required")
        return
    if not isinstance(actual.get("receipt"), dict) or not isinstance(actual.get("media_reservation"), dict):
        check(False, "deep_comment_receipt_or_reservation_invalid")
        return
    if any(field not in actual[section] for section in ("receipt", "media_reservation") for field in expected[section]):
        pending.append("explicit_comment_media_binding_required")
        return
    ids = actual["observed_comment_ids"]
    check(
        isinstance(ids, list) and len(ids) == len(set(ids)) and sorted(ids) == expected["observed_comment_ids"],
        "deep_comment_unique_inventory_mismatch",
    )
    for field in expected:
        if field != "observed_comment_ids":
            check(actual.get(field) == expected[field], "deep_comment_binding_" + field + "_mismatch")
    check(entry["checkpoint_sha256"] == digest(row["comments_checkpoint"]), "deep_comment_checkpoint_changed")
    check(actual["saved_rows_sha256"] == digest(canonical_comment_rows(comments)), "deep_comment_saved_rows_changed")


def source_inventory(post, sidecar, canonical, source, saved_assets, check):
    """Reconstruct current native loader inputs independently of result output."""
    from trr_backend.ingestion.media_snapshot import _mapping, _urls

    raw = {**_mapping(post.get("raw_data")), **_mapping(sidecar.get("raw_data"))}
    detail = _mapping(raw.get("detail_snapshot"))
    children = sidecar.get("child_posts_data") or post.get("child_posts_data") or []
    expected = detail.get("media_count", raw.get("carousel_media_count"))
    if expected is None:
        expected = len(children) if children else 1
    check(type(expected) is int and expected == source.expected_media_count, "current_native_media_count_mismatch")
    check(refs(_urls(post.get("media_urls"))) == refs(source.media_urls), "media_source_inventory_mismatch")
    if children:
        check(len(children) == expected, "current_ordered_children_count_mismatch")
        # Each canonical asset retains the full ordered native child inventory.
        for asset in saved_assets:
            check(
                (asset.get("media_payload") or {}).get("raw_child_posts") == children,
                "canonical_ordered_children_mismatch",
            )
        child_urls = []
        for child in children:
            candidates = child.get("media_urls")
            if candidates:
                child_urls.extend(_urls(candidates))
            else:
                url = (
                    child.get("video_url")
                    or child.get("display_url")
                    or child.get("image_url")
                    or child.get("thumbnail_url")
                )
                if url:
                    child_urls.append(url)
        check(
            len(child_urls) == expected and refs(child_urls) == refs(source.media_urls), "ordered_child_source_mismatch"
        )
        check(
            all(child.get("slide_index", index) == index for index, child in enumerate(children)),
            "ordered_child_index_mismatch",
        )
    else:
        check(expected == 1, "current_ordered_children_missing")
    thumbnail = post.get("thumbnail_url") or next(
        (
            url
            for url in _urls(post.get("media_urls"))
            if not urlsplit(url).path.lower().endswith((".mp4", ".mov", ".m4v", ".webm"))
        ),
        "",
    )
    check(refs([thumbnail]) == refs([source.thumbnail_url]), "current_thumbnail_source_mismatch")
    for table, fields in ((post, ("scraped_at",)), (canonical, ("last_seen_at", "last_scraped_at"))):
        for field in fields:
            check(equal(table.get(field), source.observed_at), "source_provenance_" + field + "_mismatch")
    return raw


def verify_attachment(item, saved, source, check):
    """Pure already-applied attachment validation, independent of comment gate.

    ``check(bool, static_code)`` collects failed checks; malformed retained proof
    raises ValueError. A preapply baseline is retained history and is never
    recomputed from the already-applied row. Older markers do not describe a
    newer full native capture; the separate deep audit must prove that capture.
    """
    from trr_backend.ingestion.comment_attachment_refresh import (
        AttachmentRefresh,
        _retained_giphy,
        _row_parent,
        _selected_url,
    )
    from trr_backend.ingestion.media import source_reference

    check(
        (item.row_id, item.comment_id, source.post_id)
        == (str(saved["id"]), str(saved["comment_id"]), str(saved["post_id"])),
        "attachment_snapshot_saved_identity_mismatch",
    )
    check(refs(item.media_urls) == refs(saved.get("media_urls")), "attachment_snapshot_saved_source_mismatch")
    check(equal(saved.get("scraped_at"), item.observed_at), "attachment_snapshot_original_clock_mismatch")
    raw = saved.get("raw_data") or {}
    marker = raw.get("ingestion_attachment")
    proof = item.attachment_refresh
    history = None
    if marker is not None:
        retained = AttachmentRefresh.from_payload(marker)
        check(
            (retained.row_id, retained.comment_id, retained.post_id, retained.shortcode, retained.parent_comment_id)
            == (
                str(saved["id"]),
                str(saved["comment_id"]),
                str(saved["post_id"]),
                source.shortcode,
                _row_parent(saved),
            ),
            "attachment_applied_identity_mismatch",
        )
        current_capture = stamp(saved["scraped_at"])
        if proof is None and retained.captured_at < current_capture:
            check(item.attachment_version == retained.evidence_sha256, "attachment_version_changed")
            history = {
                "proof": retained.to_payload(),
                "status": "older_than_current_full_native_capture",
                "current_capture_at": current_capture.isoformat(),
                "current_selection_requires_separate_deep_comment_proof": True,
            }
        else:
            check(retained.captured_at >= current_capture, "attachment_proof_predates_native_comment")
            check(
                raw.get("giphy_media_info") == retained.giphy_media_info
                and refs(saved.get("media_urls")) == [source_reference(retained.source_url)],
                "attachment_applied_selection_mismatch",
            )
            if proof is not None:
                proof.validate()
                check(marker == proof.to_payload(), "attachment_applied_proof_changed")
                # The exact same-proof exception permits an earlier attachment
                # version in the preapply snapshot; it never updates clocks.
            else:
                check(item.attachment_version == retained.evidence_sha256, "attachment_version_changed")
            return {
                "kind": "attachment_refresh",
                "native_gif": True,
                "proof": retained.to_payload(),
                "preapply_baseline_sha256": retained.baseline_sha256,
                "baseline_semantics": "retained_preapply_fingerprint_not_recomputed_from_applied_row",
            }
    else:
        check(proof is None and item.attachment_version is None, "attachment_applied_marker_missing")
    info = raw.get("giphy_media_info") or raw.get("giphyMediaInfo")
    if info is not None:
        normalized = _retained_giphy(info)
        check(
            bool(normalized.get("id"))
            and refs(item.media_urls) == [_selected_url(normalized)]
            and refs(saved.get("media_urls")) == refs(item.media_urls),
            "native_gif_selection_mismatch",
        )
        return {
            "kind": "native_giphy",
            "native_gif": True,
            "native_gif_id": normalized.get("id"),
            "giphy_media_info": normalized,
            "comment_id": item.comment_id,
            "row_id": item.row_id,
            "post_id": source.post_id,
            "parent_comment_id": _row_parent(saved),
            "selection": "fixed_height_first_party_preferred",
            "retained_history": history,
        }
    check(
        not any(urlsplit(url).path.lower().endswith(".gif") for url in item.media_urls),
        "native_gif_identity_proof_missing",
    )
    return (
        {"kind": "historical_attachment_refresh", "native_gif": False, "retained_history": history} if history else None
    )


class RegistryRows:
    """Pure cursor adapter: maintained loader reconstructs slots, with no DB."""

    def __init__(self, rows):
        self.rows = rows

    def execute(self, query, args):
        if "from social.avatar_registry" not in query:
            raise AssertionError("unexpected_loader_query")
        self.selected = [row for row in self.rows if row["account_handle"] in args[0]]

    def fetchall(self):
        return self.selected


def verify_cached_aliases(target, source, registry, at, pinned, check):
    """Independent auxiliary-envelope/source/registry binding, with no I/O."""
    from trr_backend.ingestion.cached_avatar_alias import (
        registry_cohort,
        slot,
        validate_retained,
        verify_current_source,
        verify_parent,
        verify_promoted,
        verify_sealed_source,
    )

    verified, required_slots, authorized_slots = [], set(), set()
    records = {}
    for row in target.get("_cached_avatar_aliases", []):
        try:
            proof = validate_retained(row, run_id=common.RUN, target_key=target["target_key"], now=at, pinned=pinned)
            records[str(row["evidence_id"])] = row, proof
        except (ValueError, TypeError, KeyError):
            check(False, "cached_avatar_alias_auxiliary_proof_invalid")
    for row in target.get("_cached_avatar_aliases", []):
        try:
            proof = validate_retained(row, run_id=common.RUN, target_key=target["target_key"], now=at, pinned=pinned)
            verify_sealed_source(proof, row["_source_evidence"], now=at, pinned=pinned)
            verify_current_source(proof, row["_current_source_evidence"], now=at, pinned=pinned)
            verify_parent(proof, records)
            check(proof["verified_at"] <= at.isoformat(), "cached_avatar_alias_future_verification")
            check(proof["account"] == common.ACCOUNT, "cached_avatar_alias_account_mismatch")
            check(proof["post_id"] == source.post_id and proof["canonical_post_id"] == source.canonical_post_id,
                  "cached_avatar_alias_post_identity_mismatch")
            check(proof["owner_generation"] <= target["owner_generation"], "cached_avatar_alias_owner_lineage_mismatch")
            # Removed historical roles do not constrain current work. Every
            # current role sharing the historical asset still needs its own
            # independently sealed authorization; do not infer a role change.
            related = [a for a in source.avatars if all(slot(a)[k] == proof["slot"][k]
                                                       for k in ("username", "source_reference"))]
            required_slots.update(digest(slot(a)) for a in related)
            avatars = [a for a in related if slot(a) == proof["current_slot"]]
            if not avatars:
                continue
            check(len(avatars) == 1, "cached_avatar_alias_slot_mismatch")
            identity = digest(proof["current_slot"])
            if len(avatars) == 1:
                a = avatars[0]
                # An interrupted attempt deliberately leaves its valid unused
                # proof. Only the exact final inventory/result authorization
                # can justify the promoted cache used by this receipt.
                if not (proof["current_inventory"] == source.inventory_fingerprint
                        and a.cached_sha256 == proof["sha256"] and a.cached_hosted_url == proof["hosted_url"]
                        and equal(proof["byte_clock"], a.observed_at)):
                    continue
            cohort = registry_cohort(registry, proof["slot"]["username"], proof["slot"]["source_reference"])
            if proof["parent_evidence_id"] is not None:
                from trr_backend.ingestion.domain import fingerprint

                # Its envelope, both seals, root and identity/time bounds were
                # validated above. A valid older cohort is retained history,
                # not authority for the current cohort. Require a fresh match
                # through the ordinary per-slot authorization coverage check.
                if proof["current_cohort_sha256"] != fingerprint(cohort):
                    continue
            verify_promoted(proof, cohort)
            authorized_slots.add(identity)
            verified.append({"evidence_id": str(row["evidence_id"]), "sha256": row["content_hash"],
                             "source_clock": proof["source_clock"], "byte_clock": proof["byte_clock"],
                             "verified_at": proof["verified_at"]})
        except (ValueError, TypeError, KeyError):
            check(False, "cached_avatar_alias_auxiliary_proof_invalid")
    check(required_slots <= authorized_slots, "cached_avatar_alias_current_inventory_or_result_mismatch")
    return verified


def load_cached_aliases(cur, targets):
    """All reads flow through common.rows and enter both snapshot fingerprints."""
    from trr_backend.ingestion.cached_avatar_alias import read_auxiliary, regional_reference

    class RecordedRows:
        def execute(self, sql, args):
            self.selected = common.rows(cur, sql, args)

        def fetchall(self):
            return self.selected

    recorded = RecordedRows()
    sources = {}
    for target in targets:
        if target["target_outcome"] != "committed" or not target.get("payload"):
            continue
        payload = json.loads(target["payload"])
        references = sorted({ref for a in payload.get("avatars", []) if (ref := regional_reference(a.get("profile_pic_url")))})
        if not references:
            continue
        aliases = read_auxiliary(recorded, common.RUN, target["target_key"], references)
        for row in aliases:
            # Validate the closed auxiliary envelope before using any embedded ID.
            from trr_backend.ingestion.cached_avatar_alias import validate_retained

            proof = validate_retained(row, run_id=common.RUN, target_key=target["target_key"])
            for field, target_field in (("source_evidence_id", "_source_evidence"),
                                        ("current_source_evidence_id", "_current_source_evidence")):
                evidence_id = proof[field]
                if evidence_id not in sources:
                    found = common.rows(cur, "select * from social.instagram_ingestion_evidence where evidence_id=%s", (evidence_id,))
                    common.require(len(found) == 1, "cached_avatar_alias_source_evidence_missing")
                    sources[evidence_id] = found[0]
                row[target_field] = sources[evidence_id]
        target["_cached_avatar_aliases"] = aliases


def load_saved_owner_assets(cur, targets, at, pinned):
    from trr_backend.ingestion import cached_avatar_alias as cached
    from trr_backend.ingestion import saved_owner_asset as saved

    class RecordedRows:
        def execute(self, sql, args):
            self.selected = common.rows(cur, sql, args)

        def fetchall(self):
            return self.selected

        def fetchone(self):
            return self.selected[0] if self.selected else None

    reader = RecordedRows()
    for target in targets:
        if target["target_outcome"] != "committed" or not target.get("payload"):
            continue
        payload = json.loads(target["payload"])
        references = sorted({ref for a in payload.get("avatars", []) if (ref := cached.regional_reference(a.get("profile_pic_url")))})
        if not references:
            continue
        records = saved.read_records(reader, common.RUN, target["target_key"], references, at=at, pinned=pinned)
        target["_saved_owner_assets"] = records
        cohorts, current_cohorts = {}, {}
        for item in records.values():
            p = item["input"]
            key = p["historical_handle"], p["slot"]["source_reference"]
            if key not in cohorts:
                cohorts[key] = saved.read_cohort(reader, *key)
            current_key = p["slot"]["username"], p["slot"]["source_reference"]
            if current_key not in current_cohorts:
                current_cohorts[current_key] = saved.read_cohort(reader, *current_key)
        target["_saved_owner_historical_cohorts"] = cohorts
        target["_saved_owner_current_cohorts"] = current_cohorts


def verify_saved_owner_assets(row, source, result, post, sidecar, registry, check):
    from trr_backend.ingestion import saved_owner_asset as saved

    records = row.get("_saved_owner_assets", {})
    identities = []
    if not records:
        return identities
    try:
        saved.target_records(records, source, common.ACCOUNT)
        for index, avatar in enumerate(source.avatars):
            related = [r for r in records.values() if saved._related(avatar, r["origin"])]
            if not related:
                continue
            eligible = []
            for item in related:
                p, auth = item["input"], item["auth"]
                historical = row["_saved_owner_historical_cohorts"][(p["historical_handle"], p["slot"]["source_reference"])]
                current = row["_saved_owner_current_cohorts"][(p["slot"]["username"], p["slot"]["source_reference"])]
                if not (saved.applicable(item, source, avatar, historical, current, postcommit=True)
                        and auth["owner_token"] == str(row["target_owner_token"])
                        and auth["owner_generation"] == row["target_owner_generation"]
                        and avatar.cached_sha256 == p["sha256"] and avatar.cached_hosted_url == p["destination_url"]
                        and saved.stamp(avatar.observed_at) == p["byte_clock"]):
                    continue
                # Both bindings are historical observations. Only the exact
                # current input/result projection can authorize this receipt.
                actual = saved.binding(post, sidecar, source, avatar)
                expected = dict(p["binding"])
                expected["hosted_owner_url"] = result.post_fields.get("hosted_owner_profile_pic_url", expected["hosted_owner_url"])
                if actual != expected:
                    continue
                eligible.append(item)
            check(bool(eligible), "saved_owner_asset_current_authorization_missing")
            if not eligible:
                continue
            item = max(eligible, key=lambda r: (r["auth"]["verified_at"], str(r["row"]["evidence_id"])))
            p, auth = item["input"], item["auth"]
            identities.append({"avatar_index": index, "proof_evidence_id": str(item["row"]["evidence_id"]),
                "proof_sha256": item["row"]["content_hash"], "input_id": auth["input_id"],
                "input_sha256": auth["input_sha256"], "origin_input_id": auth["origin_input_id"],
                "origin_input_sha256": auth["origin_input_sha256"], "historical_handle": p["historical_handle"],
                "current_handle": p["slot"]["username"], "source_reference": p["slot"]["source_reference"],
                "source_url": p["source_url"], "source_key": p["source_key"], "sha256": p["sha256"],
                "byte_clock": p["byte_clock"], "source_clock": item["origin"]["source_clock"],
                "verification": auth["old_bytes"]})
    except (ValueError, TypeError, KeyError):
        check(False, "saved_owner_asset_proof_invalid")
    return identities


def saved_owner_byte_plans(local_plan, identities):
    plans = []
    for identity in identities:
        original = next(p for p in local_plan if p["role"] == "avatar:" + str(identity["avatar_index"]))
        entry = dict(original)
        meta = identity["verification"]
        entry.update(role="saved_owner_asset_source:" + str(identity["avatar_index"]),
            url=identity["source_url"], storage_key=identity["source_key"], hosted_path=hosted_key(identity["source_url"]),
            expected_bytes=meta["bytes"], historical_sha256=identity["sha256"], historical_hash_limit=None,
            format=meta["format"].lower(), content_type="image/" + {"JPEG": "jpeg"}.get(meta["format"], meta["format"].lower()),
            width=meta["width"], height=meta["height"], original_observed_at=identity["byte_clock"],
            storage_key_derivation="sealed_saved_owner_asset_historical_key", saved_owner_asset_source=identity)
        entry.pop("avatar_identity", None)
        plans.append(entry)
    return plans


def verify_avatars(post, raw, comments, source, result, registry, at, check):
    from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader

    loader = MediaSnapshotLoader(include_comment_avatars=source.include_comment_avatars, clock=lambda: at)
    expected, unavailable = loader._avatars(RegistryRows(registry), post, raw, comments, source.observed_at)

    def slot(avatar):
        return avatar.username, avatar.role, avatar.comment_id

    check(len({slot(a) for a in source.avatars}) == len(source.avatars), "avatar_duplicate_slot")
    retained = {slot(a): a for a in source.avatars}
    check(
        set(retained) == {slot(a) for a in expected} and tuple(source.unavailable_avatars) == unavailable,
        "current_native_avatar_slot_inventory_mismatch",
    )
    check(len(result.avatars) == len(source.avatars), "result_avatar_count_mismatch")
    identities = []
    for current in expected:
        old = retained.get(slot(current))
        if old is None:
            continue
        check(refs([old.source_url]) == refs([current.source_url]), "avatar_current_source_version_changed")
        check(
            current.cached_hosted_url is not None and current.cached_sha256 is not None,
            "avatar_exact_version_registry_unproved",
        )
        check(equal(current.observed_at, old.observed_at), "avatar_original_registry_clock_mismatch")
        index = source.avatars.index(old)
        observed = result.avatars[index]
        check(
            observed.get("username") == current.username
            and refs([observed["source_url"]]) == refs([current.source_url])
            and observed.get("hosted_url") == current.cached_hosted_url
            and observed.get("sha256") == current.cached_sha256
            and equal(observed.get("observed_at"), current.observed_at),
            "avatar_registry_version_proof_mismatch",
        )
        # Cached inputs are optional for newly mirrored versions; current saved
        # registry must nevertheless prove the exact result and original time.
        if old.cached_hosted_url or old.cached_sha256:
            check(
                old.cached_hosted_url == current.cached_hosted_url and old.cached_sha256 == current.cached_sha256,
                "avatar_retained_cache_changed",
            )
        identities.append(
            {
                "username": old.username,
                "role": old.role,
                "comment_id": old.comment_id,
                "source_reference": refs([old.source_url])[0],
                "registry_observed_at": stamp(current.observed_at).isoformat(),
                "registry_status": "mirrored",
                "current_different_versions": sum(
                    1
                    for row in registry
                    if row.get("account_handle") == old.username
                    and row.get("source_url")
                    and refs([row["source_url"]]) != refs([old.source_url])
                ),
            }
        )
    return identities


def hosted_key(url):
    from trr_backend.ingestion.media import source_reference

    if source_reference(url) != url:
        raise ValueError("hosted_reference_required")
    path = unquote(urlsplit(url).path).lstrip("/")
    if ".." in path.split("/") or "\\" in path:
        raise ValueError("invalid_storage_path")
    # Bucket/CDN prefix is not invented; exact maintained storage suffix is
    # retained below, together with its derivation and the URL path.
    return path


def object_plan(row, source, result, attachments, avatar_slots, check):
    manifest = result.post_fields["asset_manifest"]
    base = {
        "shortcode": source.shortcode,
        "legacy_post_id": source.post_id,
        "canonical_post_id": source.canonical_post_id,
        "source_evidence_id": str(row["evidence_id"]),
        "source_evidence_sha256": row["content_hash"],
        "source_evidence_parser": row["parser_version"],
        "source_evidence_schema": row["schema_version"],
        "original_post_observed_at": source.observed_at.isoformat(),
        "media_reservation": {
            "owner_token": str(row["target_owner_token"]),
            "owner_generation": row["target_owner_generation"],
        },
    }
    plan = []

    def add(role, url, key, source_url, observed, metadata=None, **extra):
        path = hosted_key(url)
        check(bool(key) and (path == key or path.endswith("/" + key)), "hosted_storage_key_url_mismatch")
        metadata = metadata or {}
        historical_hash = metadata.get("sha256")
        entry = {
            **base,
            "role": role,
            "url": url,
            "storage_key": key,
            "hosted_path": path,
            "source_reference": refs([source_url])[0],
            "expected_bytes": metadata.get("bytes"),
            "historical_sha256": historical_hash,
            "historical_hash_limit": None if historical_hash else "original_bytes_hash_not_retained",
            "format": metadata.get("format"),
            "content_type": metadata.get("content_type"),
            "width": metadata.get("width"),
            "height": metadata.get("height"),
            "original_observed_at": stamp(observed).isoformat(),
            **extra,
        }
        fmt = str(entry.get("format") or Path(urlsplit(url).path).suffix.lstrip(".")).lower()
        entry["native_gif"] = bool((extra.get("native_gif_proof") or {}).get("native_gif"))
        entry["required_animation"] = (
            "all_frames_native_identity_no_unavailable_placeholder" if entry["native_gif"] else None
        )
        entry["required_decode_mode"] = (
            "gif_all_frames_native_identity"
            if entry["native_gif"]
            else ("video_full_stream" if fmt in {"mp4", "mov", "m4v", "webm"} else "image_full_decode")
        )
        plan.append(entry)

    for role, item in [
        ("cover_original", manifest["originals"]["cover"]),
        *[(f"media_original_{i:02d}", item) for i, item in enumerate(manifest["originals"]["media"], 1)],
        *list(manifest["display_variants"].items()),
    ]:
        source_url = (
            source.media_urls[int(role.rsplit("_", 1)[1]) - 1]
            if role.startswith("media_original_")
            else source.thumbnail_url
        )
        add(
            role,
            item["url"],
            item["storage_key"],
            source_url,
            source.observed_at,
            item,
            storage_key_derivation="exact_retained_owned_manifest",
            manifest_sha256=digest(manifest),
        )
    root = f"social/instagram/{source.show_id}/{source.season_number}/week-{source.week_index if source.week_index is not None else 'unknown'}"
    for item in source.comments:
        for index, url in enumerate(result.comment_fields[item.comment_id]["hosted_media_urls"], 1):
            path = hosted_key(url)
            suffix = Path(path).suffix
            key = f"{root}/{item.storage_source_key}/media-{index:02d}{suffix}"
            # A maintained storage prefix is derivable from the exact owned
            # manifest root, rather than from a URL or an environment default.
            cover_key = manifest["originals"]["cover"]["storage_key"]
            marker = root + "/" + source.shortcode + "/"
            check(marker in cover_key, "comment_storage_prefix_unproved")
            prefix = cover_key.split(marker, 1)[0] if marker in cover_key else ""
            key = prefix + key
            add(
                f"comment:{item.comment_id}:{index}",
                url,
                key,
                item.media_urls[index - 1],
                item.observed_at,
                comment_id=item.comment_id,
                comment_row_id=item.row_id,
                storage_source_key=item.storage_source_key,
                storage_key_derivation="retained_context_and_comment_storage_source_key_plus_hosted_extension",
                native_gif_proof=attachments[item.comment_id],
            )
    from trr_backend.media.s3_mirror import build_instagram_profile_pic_s3_key

    for index, avatar in enumerate(result.avatars):
        old = source.avatars[index]
        url = avatar["hosted_url"]
        key = build_instagram_profile_pic_s3_key(old.username, avatar["sha256"], Path(urlsplit(url).path).suffix)
        slot = next(
            (
                slot
                for slot in avatar_slots
                if (slot["username"], slot["role"], slot["comment_id"]) == (old.username, old.role, old.comment_id)
            ),
            {},
        )
        add(
            f"avatar:{index}",
            url,
            key,
            old.source_url,
            avatar["observed_at"],
            {"sha256": avatar["sha256"]},
            avatar_identity=slot,
            comment_id=old.comment_id,
            storage_key_derivation="maintained_profile_pic_content_hash_key",
        )
    return plan


def audit_target(
    row,
    comments,
    saved_assets,
    registry,
    at,
    pinned,
    comment_proof,
    release,
    listing_hash,
    *,
    allow_verified_comment_subset=False,
    audited_keys=None,
    comment_proof_sha256=None,
    expected_comment_proof_sha256=None,
    observed_inventory=None,
):
    from trr_backend.ingestion.domain import ReplayUnavailable
    from trr_backend.ingestion.evidence import Evidence
    from trr_backend.ingestion.media import MEDIA_VERSION
    from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, _media

    problems, pending = [], []

    def check(condition, code):
        if not condition:
            problems.append(code)

    key = row["target_key"]
    if row["target_outcome"] != "committed":
        return {"pending": {"shortcode": key, "outcome": row["target_outcome"]}}
    try:
        check(row["receipt_outcome"] == "committed" and common.receipt_fence(row), "media_receipt_fence_mismatch")
        check(row["parser_version"] == MEDIA_RESULT_REPLAY_VERSION, "ordinary_result_v1_evidence_required")
        check(row["payload"] is not None, "ordinary_result_payload_missing")
        if problems:
            return {"failure": {"shortcode": key, "codes": sorted(set(problems))}}
        evidence = Evidence(
            source_identity=row["source_identity"],
            captured_at=row["captured_at"],
            parser_version=row["parser_version"],
            payload_json=row["payload"],
            content_hash=row["content_hash"],
            schema_version=row["schema_version"],
        )
        evidence.read(now=at, pinned=pinned)
        # _media performs an additional pure read; the first read established
        # expiry using this audit's immutable snapshot clock.
        parsed = _media(evidence, target_key=key, parser_version=MEDIA_RESULT_REPLAY_VERSION, pinned=pinned)
        if parsed.source_unavailable is not None or parsed.result is None:
            return {"failure": {"shortcode": key, "codes": ["decoded_ordinary_result_required"]}}
        result, source = parsed.result, parsed.result.snapshot
        post, sidecar, canonical = row["post"] or {}, row["sidecar"] or {}, row["canonical"] or {}
        check(
            source.post_id == str(post.get("id")) and source.canonical_post_id == str(canonical.get("id")),
            "media_canonical_identity_mismatch",
        )
        identity = row["canonical_identity"] or {}
        check(
            identity.get("post_id") == source.post_id and identity.get("canonical_post_id") == source.canonical_post_id,
            "receipt_identity_mismatch",
        )
        check(
            (post.get("source_account") or post.get("username") or "").lower() == common.ACCOUNT,
            "saved_media_account_mismatch",
        )
        check(
            row["observed_at"] == evidence.captured_at == source.observed_at and source.observed_at <= at,
            "media_original_clock_mismatch",
        )
        from trr_backend.ingestion.certification import inventory_fingerprint

        current_inventory = inventory_fingerprint({**post, "payload_sidecar": sidecar}, comments)
        retained_inventory = source.result_inventory_fingerprint or source.inventory_fingerprint
        check(retained_inventory == current_inventory, "media_inventory_fingerprint_changed_or_missing")
        check(
            identity.get("inventory_fingerprint") == current_inventory,
            "receipt_inventory_fingerprint_changed_or_missing",
        )
        if observed_inventory is not None and not observed_inventory.errors:
            independent = observed_inventory.posts.get(key) or {}
            for field, value in (
                ("post_row_sha256", post),
                ("sidecar_sha256", sidecar),
                ("canonical_sha256", canonical),
            ):
                check(independent.get(field) == digest(value), "observed_inventory_independent_" + field + "_changed")
        raw = source_inventory(post, sidecar, canonical, source, saved_assets, check)
        manifest = result.post_fields["asset_manifest"]
        for field, value in result.post_fields.items():
            if field in {"asset_manifest", "hosted_tagged_profile_pics"}:
                continue
            check(post.get(field) == value, "post_media_" + field + "_mismatch")
        for handle, url in result.post_fields.get("hosted_tagged_profile_pics", {}).items():
            check((post.get("hosted_tagged_profile_pics") or {}).get(handle) == url, "tagged_profile_hosted_mismatch")
        for saved_manifest in (post.get("asset_manifest") or {}, sidecar.get("asset_manifest") or {}):
            for field, value in manifest.items():
                check(saved_manifest.get(field) == value, "owned_manifest_" + field + "_mismatch")
        check(
            [asset["position"] for asset in saved_assets] == list(range(source.expected_media_count)),
            "canonical_asset_position_set_mismatch",
        )
        for index, asset in enumerate(saved_assets):
            if index >= source.expected_media_count:
                continue
            check(refs([asset["source_url"]]) == refs([source.media_urls[index]]), "canonical_asset_source_mismatch")
            check(
                asset.get("hosted_url") == result.post_fields["hosted_media_urls"][index]
                and asset.get("mirror_status") == "mirrored"
                and asset.get("mirror_error") is None,
                "canonical_asset_hosted_mismatch",
            )
            check(
                (asset.get("media_payload") or {}).get("ingestion_media") == manifest["ingestion_media"],
                "canonical_asset_capture_marker_mismatch",
            )
            for field in ("scraped_at", "last_seen_at", "last_scraped_at", "metadata_scraped_at"):
                if asset.get(field) is not None:
                    check(equal(asset[field], source.observed_at), "canonical_asset_source_provenance_mismatch")
            if index == 0:
                check(
                    refs([asset["thumbnail_url"]]) == refs([source.thumbnail_url])
                    and asset.get("hosted_thumbnail_url") == result.post_fields["hosted_thumbnail_url"],
                    "canonical_asset_cover_mismatch",
                )
        by_id = {str(comment["comment_id"]): comment for comment in comments}
        check(len(by_id) == len(comments), "saved_comment_duplicate_native_id")
        comment_identity = row["comments_identity"] or {}
        ids = comment_identity.get("observed_comment_ids", [])
        check(
            isinstance(ids, list)
            and len(ids) == len(set(ids))
            and set(ids) == set(by_id)
            and str(comment_identity.get("post_id")) == source.post_id,
            "sealed_comments_inventory_mismatch",
        )
        comment_fence = {
            name: row["comments_" + name]
            for name in (
                "owner_generation",
                "target_owner_generation",
                "receipt_token",
                "target_owner_token",
                "dispatch_identity_matches",
            )
        }
        check(
            row["comments_outcome"] == row["comments_receipt_outcome"] == "committed"
            and common.receipt_fence(comment_fence),
            "comments_receipt_fence_mismatch",
        )
        check(
            row["comments_observed_at"] is not None and row["comments_observed_at"] >= source.observed_at,
            "comment_receipt_predates_post",
        )
        verify_comment_proof(
            row,
            comments,
            comment_proof,
            release,
            listing_hash,
            check,
            pending,
            allow_verified_comment_subset=allow_verified_comment_subset,
            audited_keys=audited_keys,
            comment_proof_sha256=comment_proof_sha256,
            expected_comment_proof_sha256=expected_comment_proof_sha256,
            observed_inventory=observed_inventory,
        )
        selected = {
            str(comment["comment_id"])
            for comment in comments
            if comment.get("media_urls") or source.include_comment_avatars
        }
        check(selected == {item.comment_id for item in source.comments}, "selected_comment_media_inventory_mismatch")
        attachments = {}
        for item in source.comments:
            saved = by_id.get(item.comment_id)
            if saved is None:
                check(False, "source_comment_missing")
                continue
            check(
                str(saved["id"]) == item.row_id and refs(saved.get("media_urls")) == refs(item.media_urls),
                "comment_media_identity_or_source_mismatch",
            )
            check(
                equal(saved.get("scraped_at"), item.observed_at)
                and saved.get("source_snapshot_type") == "full_comments_scrape",
                "comment_media_source_clock_mismatch",
            )
            check(
                (saved.get("raw_data") or {}).get("ingestion_media")
                == {"version": MEDIA_VERSION, "observed_at": item.observed_at.isoformat()},
                "comment_ingestion_media_marker_mismatch",
            )
            for field, value in result.comment_fields[item.comment_id].items():
                check(saved.get(field) == value, "saved_comment_media_" + field + "_mismatch")
            attachments[item.comment_id] = verify_attachment(item, saved, source, check)
        verify_cached_aliases(row, source, registry, at, pinned, check)
        avatar_slots = verify_avatars(post, raw, comments, source, result, registry, at, check)
        saved_owner_sources = verify_saved_owner_assets(row, source, result, post, sidecar, registry, check)
        local_plan = object_plan(row, source, result, attachments, avatar_slots, check)
        local_plan.extend(saved_owner_byte_plans(local_plan, saved_owner_sources))
        for entry in local_plan:
            if entry["role"].startswith("media_original_"):
                position = int(entry["role"].rsplit("_", 1)[1]) - 1
                entry["canonical_asset_id"] = (
                    str(saved_assets[position]["id"]) if position < len(saved_assets) else None
                )
            elif entry["role"] == "cover_original" or entry["role"] in manifest["display_variants"]:
                entry["canonical_asset_id"] = str(saved_assets[0]["id"]) if saved_assets else None
        if problems:
            return {"failure": {"shortcode": key, "codes": sorted(set(problems))}}
        if pending:
            return {"pending": {"shortcode": key, "codes": sorted(set(pending))}}
        return {
            "verified": {
                "shortcode": key,
                "source_evidence_id": str(row["evidence_id"]),
                "source_hash": evidence.content_hash,
                "record_identity": {
                    "legacy_post_id": source.post_id,
                    "canonical_post_id": source.canonical_post_id,
                    "media_reservation": {
                        "owner_token": str(row["target_owner_token"]),
                        "owner_generation": row["target_owner_generation"],
                    },
                },
                "object_role_sha256": {entry["role"]: digest(entry) for entry in local_plan},
                "original_observed_at": source.observed_at.isoformat(),
                "canonical_assets": len(saved_assets),
                "selected_comment_attachments": len(source.comments),
                "object_roles": len(local_plan),
                "inventory_fingerprint": current_inventory,
                "post_sha256": digest(post),
                "sidecar_sha256": digest(sidecar),
                "assets_sha256": digest(saved_assets),
                "comments_sha256": digest(comments),
                "comment_binding": comment_binding(row, comments),
                "avatar_slots": avatar_slots,
                **({"saved_owner_asset_sources": saved_owner_sources} if saved_owner_sources else {}),
            },
            "plan": local_plan,
        }
    except (*EXPECTED_TARGET_ERRORS, ReplayUnavailable):
        # Library exceptions may include signed source URLs; output only this
        # static category and already collected static validation codes.
        return {"failure": {"shortcode": key, "codes": sorted({*problems, "target_evidence_or_saved_state_invalid"})}}


def reconcile_plans(results):
    """Block every affected target on key OR URL conflict; keep others usable."""
    groups = defaultdict(list)
    fields = ("expected_bytes", "historical_sha256", "format", "content_type", "width", "height", "storage_key", "url")
    for result in results:
        for entry in result.get("plan", []):
            groups[("url", entry["url"])].append(entry)
            groups[("key", entry["storage_key"])].append(entry)
    affected = set()
    for entries in groups.values():
        for field in fields:
            values = {json.dumps(entry[field], sort_keys=True) for entry in entries if entry.get(field) is not None}
            if len(values) > 1:
                affected.update(entry["shortcode"] for entry in entries)
    for result in results:
        if result.get("verified", {}).get("shortcode") in affected:
            key = result["verified"]["shortcode"]
            result.clear()
            result["failure"] = {"shortcode": key, "codes": ["shared_object_metadata_conflict"]}
    return results


def inspect(
    cur,
    binding,
    release,
    listing,
    comment_proof=None,
    listing_hash=None,
    *,
    allow_verified_comment_subset=False,
    comment_proof_sha256=None,
    expected_comment_proof_sha256=None,
    complete_observed_inventory=False,
    post_proof=None,
):
    from trr_backend.ingestion.queries import Queries

    for setting, expected in (("transaction_read_only", "on"), ("transaction_isolation", "repeatable read")):
        cur.execute("show " + setting)
        common.require(cur.fetchone()[setting] == expected, "snapshot_" + setting)
    cur.execute("select clock_timestamp() as at")
    at = cur.fetchone()["at"]
    progress = Queries(read_connection=lambda: nullcontext(cur.connection)).progress(common.RUN, common.ACCOUNT)
    common.require(
        progress["intent"] == binding["successor_intent"]
        and progress["intent_hash"] == common.INTENT
        and progress["generation"] == common.GENERATION,
        "run_binding_mismatch",
    )
    common.require(
        listing["run_id"] == common.RUN
        and listing["intent_hash"] == common.INTENT
        and listing["listing_proof"]["independent_native_membership_verified"] is True
        and listing["listing_proof"]["boundary_verified"] is True,
        "native_boundary_audit_required",
    )
    keys = listing["reported_january_keys"]
    common.require(
        len(keys) == len(set(keys)) == listing["independently_verified_january_count"] and bool(keys),
        "audited_keys_mismatch",
    )
    common.require(
        not (complete_observed_inventory and allow_verified_comment_subset), "observed_inventory_subset_forbidden"
    )
    observed_inventory = (
        observed_inventory_proof(comment_proof, post_proof, release, listing_hash, keys)
        if complete_observed_inventory
        else None
    )
    targets = common.rows(
        cur,
        """select t.target_key,t.outcome as target_outcome,t.owner_token as target_owner_token,
        t.owner_generation as target_owner_generation,r.owner_generation,r.owner_token as receipt_token,
        r.outcome as receipt_outcome,r.canonical_identity,r.observed_at,r.evidence_id,
        exists(select 1 from social.instagram_ingestion_dispatches d where d.owner_token=r.owner_token
          and d.run_id=t.run_id and d.stage=t.stage and d.target_key=t.target_key and d.generation=1) as dispatch_identity_matches,
        e.source_identity,e.captured_at,e.expires_at,e.parser_version,e.schema_version,e.content_hash,e.payload,
        to_jsonb(p) as post,to_jsonb(s) as sidecar,to_jsonb(q) as canonical,
        cr.canonical_identity as comments_identity,cr.observed_at as comments_observed_at,
        ct.outcome as comments_outcome,ct.checkpoint as comments_checkpoint,
        ct.owner_token as comments_target_owner_token,ct.owner_generation as comments_target_owner_generation,
        cr.owner_token as comments_receipt_token,cr.owner_generation as comments_owner_generation,
        cr.outcome as comments_receipt_outcome,cr.evidence_id as comments_evidence_id,
        exists(select 1 from social.instagram_ingestion_dispatches cd where cd.owner_token=cr.owner_token
          and cd.run_id=ct.run_id and cd.stage=ct.stage and cd.target_key=ct.target_key and cd.generation=1) as comments_dispatch_identity_matches
      from social.instagram_ingestion_targets t
      left join social.instagram_ingestion_receipts r on r.run_id=t.run_id and r.stage=t.stage and r.target_key=t.target_key
        and r.owner_token=t.owner_token and r.owner_generation=t.owner_generation
      left join social.instagram_ingestion_evidence e on e.evidence_id=r.evidence_id and e.run_id=r.run_id and e.stage=r.stage and e.target_key=r.target_key
      left join social.instagram_posts p on p.shortcode=t.target_key
      left join social.instagram_post_payloads s on s.post_id=p.id
      left join social.social_posts q on q.platform='instagram' and q.source_id=t.target_key
      left join social.instagram_ingestion_targets ct on ct.run_id=t.run_id and ct.stage='comments' and ct.target_key=t.target_key
      left join social.instagram_ingestion_receipts cr on cr.run_id=ct.run_id and cr.stage=ct.stage and cr.target_key=ct.target_key
        and cr.owner_token=ct.owner_token and cr.owner_generation=ct.owner_generation
      where t.run_id=%s and t.stage='media' order by t.target_key""",
        (common.RUN,),
    )
    common.require(
        len(targets) == len(keys) and {row["target_key"] for row in targets} == set(keys), "media_target_set_mismatch"
    )
    ids = [row["post"]["id"] for row in targets if row["post"]]
    canonical_ids = [row["canonical"]["id"] for row in targets if row["canonical"]]
    # Exact projection of the independent comment auditor, including parent IDs.
    comments = common.rows(
        cur,
        """select p.shortcode,c.*,par.comment_id as uuid_parent_native_id,par.post_id as uuid_parent_post_id
      from social.instagram_posts p join social.instagram_comments c on c.post_id=p.id
      left join social.instagram_comments par on par.id=c.parent_comment_id
      where c.post_id=any(%s::uuid[]) and not c.is_missing and c.deleted_at is null order by p.shortcode,c.comment_id,c.id""",
        (ids,),
    )
    assets = common.rows(
        cur,
        "select * from social.social_post_media_assets where platform='instagram' and post_id=any(%s::uuid[]) order by post_id,position",
        (canonical_ids,),
    )
    from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader, _mapping

    handles = set()

    class RequestedHandles(RegistryRows):
        def execute(self, query, args):
            handles.update(args[0])
            super().execute(query, args)

    for target in targets:
        if target["target_outcome"] != "committed" or not target["post"]:
            continue
        post = target["post"]
        raw = {**_mapping(post.get("raw_data")), **_mapping((target["sidecar"] or {}).get("raw_data"))}
        # Both policies only collect handles for the read query; the retained
        # explicit policy is validated later, independently for every target.
        for include_authors in (False, True):
            try:
                MediaSnapshotLoader(include_comment_avatars=include_authors, clock=lambda: at)._avatars(
                    RequestedHandles([]),
                    post,
                    raw,
                    [row for row in comments if str(row["post_id"]) == str(post["id"])],
                    stamp(post["scraped_at"]),
                )
            except EXPECTED_TARGET_ERRORS:
                pass  # Missing source URLs are expected before registry read.
    registry = common.rows(
        cur,
        """select * from social.avatar_registry where platform='instagram'
      and account_handle=any(%s) and last_checked_at is not null
      order by account_handle,last_checked_at desc,coalesce(status='mirrored' and nullif(hosted_url,'') is not null
      and content_hash ~ '^[a-f0-9]{64}$',false) desc,id desc""",
        (sorted(handles),),
    )
    load_cached_aliases(cur, targets)
    load_saved_owner_assets(cur, targets, at, progress["state"] != "completed")
    by_comment, by_asset = defaultdict(list), defaultdict(list)
    for row in comments:
        by_comment[str(row["post_id"])].append(row)
    for row in assets:
        by_asset[str(row["post_id"])].append(row)
    results = []
    for row in targets:
        post_id, canonical_id = str((row["post"] or {}).get("id")), str((row["canonical"] or {}).get("id"))
        results.append(
            audit_target(
                row,
                by_comment[post_id],
                by_asset[canonical_id],
                registry,
                at,
                progress["state"] != "completed",
                comment_proof,
                release,
                listing_hash,
                allow_verified_comment_subset=allow_verified_comment_subset,
                audited_keys=keys,
                comment_proof_sha256=comment_proof_sha256,
                expected_comment_proof_sha256=expected_comment_proof_sha256,
                observed_inventory=observed_inventory,
            )
        )
    reconcile_plans(results)
    failures = [result["failure"] for result in results if "failure" in result]
    pending = [result["pending"] for result in results if "pending" in result]
    verified = [result["verified"] for result in results if "verified" in result]
    plan = [entry for result in results for entry in result.get("plan", [])]
    return {
        "schema_version": 2,
        "operator_scope": "media_records_and_byte_plan_not_final_certifier",
        "final_acceptance": False,
        "status": "blocked" if failures else "pending" if pending or allow_verified_comment_subset else "passed",
        "run_id": common.RUN,
        "intent_hash": common.INTENT,
        "comment_proof_mode": OBSERVED_INVENTORY_SCOPE
        if complete_observed_inventory
        else ("verified_target_subset" if allow_verified_comment_subset else "strict_aggregate"),
        "inventory_scope": OBSERVED_INVENTORY_SCOPE if complete_observed_inventory else None,
        "comment_observed_inventory_sha256": observed_inventory.comment_sha256 if observed_inventory else None,
        "independent_post_proof_sha256": observed_inventory.post_sha256 if observed_inventory else None,
        "observed_at": at.isoformat(),
        "release": str(release),
        "full_history_gate_closed": True,
        "audited_january_count": len(keys),
        "verified_targets": len(verified),
        "verified": verified,
        "pending": pending,
        "failures": failures,
        "required_comment_proof_fields": COMMENT_BINDING_FIELDS,
        "byte_read_plan": {
            "status": "ready_for_verified_subset" if plan else "pending",
            "objects": plan,
            "physical_urls": len({entry["url"] for entry in plan}),
        },
        "native_requests": 0,
        "storage_requests": 0,
        "database_mutations": 0,
    }
