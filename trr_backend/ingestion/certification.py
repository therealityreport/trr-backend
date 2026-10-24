"""Pure, fail-closed final acceptance for complete January audit inventories.

This module performs no database, source, or storage I/O. Operator tooling supplies
fresh complete audits and whole-object results from the reviewed storage binding.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

CERTIFICATION_VERSION = "instagram-january-certification-v1"
GATE_VERSION = "instagram-january-gates-v1"
MEDIA_INVENTORY_VERSION = "instagram-media-inventory-v1"
OBSERVED_INVENTORY_SCOPE = "complete_observed_inventory_v1"


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, default=str, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def inventory_fingerprint(post: Mapping, comments: list[dict]) -> dict[str, str]:
    """Independent implementation of the settled v1 source inventory contract."""

    def clean(value):
        if isinstance(value, datetime):
            return value.replace(tzinfo=UTC).isoformat() if value.tzinfo is None else value.astimezone(UTC).isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, str) and value.startswith("https://"):
            parts = urlsplit(value)
            if not parts.hostname or parts.username or parts.password:
                raise ValueError("invalid_media_reference")
            return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        if isinstance(value, Mapping):
            return {key: clean(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [clean(item) for item in value]
        return value

    raw = {**(post.get("raw_data") or {}), **((post.get("payload_sidecar") or {}).get("raw_data") or {})}
    post_columns = (
        "id shortcode scraped_at media_urls thumbnail_url source_account username owner_username "
        "owner_profile_pic_url owner_profile_pic_url_hd profile_tags collaborators mentions "
        "tagged_users_detail collaborators_detail child_posts_data"
    ).split()
    raw_columns = (
        "owner_detail owner user profile_tags collaborators mentions tagged_users_detail "
        "collaborators_detail carousel_media_count detail_snapshot"
    ).split()
    comment_columns = (
        "id comment_id parent_comment_id scraped_at source_account source_snapshot_type media_urls "
        "username author_profile_pic_url author_profile_pic_url_hd"
    ).split()
    material = clean(
        {
            "version": MEDIA_INVENTORY_VERSION,
            "post": {key: post.get(key) for key in post_columns},
            "source": {key: raw.get(key) for key in raw_columns},
            "children": (post.get("payload_sidecar") or {}).get("child_posts_data")
            or post.get("child_posts_data")
            or [],
            "comments": [
                {key: row.get(key) for key in comment_columns}
                for row in sorted(comments, key=lambda row: (str(row["comment_id"]), str(row["id"])))
            ],
        }
    )
    return {"version": MEDIA_INVENTORY_VERSION, "sha256": digest(material)}


def snapshot_fingerprint(snapshot):
    """Verification clocks may advance; source clocks, query rows and identities may not."""
    reports = {
        name: {key: value for key, value in report.items() if key != "observed_at"}
        for name, report in snapshot["reports"].items()
    }
    return digest({"reports": reports, "query_fingerprint": snapshot["query_fingerprint"]})


def _hash(value):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _exact_count_parity(settlement):
    return (
        all(
            type(settlement.get(field)) is int and settlement[field] >= 0
            for field in ("expected_total", "observed_total", "count_gap")
        )
        and settlement["count_gap"] == 0
        and settlement["expected_total"] == settlement["observed_total"]
    )


def _known_observed_settlements(proof):
    """Media-only scope still requires known totals and complete native topology."""
    if proof["global_validation"]["status"] != "passed":
        return False
    for entry in proof["target_results"]:
        settlement = entry["settlement"]
        if not all(
            type(settlement.get(field)) is int and settlement[field] >= 0
            for field in ("expected_total", "observed_total", "count_gap")
        ):
            return False
        gap = settlement["count_gap"]
        if (
            gap != max(settlement["expected_total"] - settlement["observed_total"], 0)
            or settlement.get("basis") != ("public_native_exhaustion" if gap else "count_parity")
            or entry["diagnostics"] != (["zero_gap_settlement_required"] if gap else [])
            or entry["status"] != ("pending" if gap else "passed")
            or entry["source_page_evidence_complete"] is not True
            or entry["native_comments"] != settlement["observed_total"]
            or entry["native_comments"] != len(entry["media_binding"]["observed_comment_ids"])
        ):
            return False
    return True


def _stable_audit_hash(report):
    material = {key: value for key, value in report.items() if key != "observed_at"}
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def certify(
    before,
    after,
    byte_report,
    *,
    storage_binding_sha256,
    storage_binding_after_sha256,
    source_sha256,
    source_after_sha256,
):
    """Only an unchanged, exhaustive, passing audit can produce final_acceptance."""
    gates = {}

    def gate(name, valid, code):
        gates[name] = {
            "version": GATE_VERSION,
            "status": "passed" if valid else "blocked",
            "diagnostics": [] if valid else [code],
        }

    result = {
        "certification_version": CERTIFICATION_VERSION,
        "schema_version": 1,
        "final_acceptance": False,
        "status": "blocked",
        "gates": gates,
        "native_requests": 0,
        "database_mutations": 0,
        "storage_writes": 0,
    }
    try:
        listing = before["reports"]["listing"]
        keys = listing["reported_january_keys"]
        wanted = set(keys)
        result.update(
            run_id=listing["run_id"],
            intent_hash=listing["intent_hash"],
            audited_keys=sorted(wanted),
            audited_january_count=len(keys),
        )
        gate(
            "january_listing",
            bool(keys)
            and len(keys) == len(wanted) == listing["independently_verified_january_count"]
            and set(listing["native_membership"]) == wanted
            and listing["listing_proof"]["independent_native_membership_verified"] is True
            and listing["listing_proof"]["boundary_verified"] is True
            and not listing["mismatches"],
            "exact_native_membership_required",
        )
        gate(
            "lifecycle",
            listing["gates"]["lifecycle"]["status"] == "passed" and listing["run_state"] == "completed",
            "run_not_completed",
        )
        all_binding = all(
            report.get("run_id") == listing["run_id"]
            and report.get("intent_hash") == listing["intent_hash"]
            and report.get("full_history_gate_closed") is True
            for snapshot in (before, after)
            for report in snapshot["reports"].values()
        )
        gate(
            "public_scope_and_full_history",
            all_binding and before["public_only"] is True and after["public_only"] is True,
            "public_scope_or_full_history_binding_invalid",
        )
        for name in ("posts", "comments", "media"):
            report = before["reports"][name]
            verified = report["verified"]
            valid = report["status"] == "passed" and not report["failures"] and not report["pending"]
            valid = valid and len(verified) == len(keys) and {entry["shortcode"] for entry in verified} == wanted
            valid = valid and report["audited_january_count"] == len(keys)
            if name == "comments":
                targets = report["target_results"]
                exact_targets = len(targets) == len(keys) and {entry["shortcode"] for entry in targets} == wanted
                parity = (
                    exact_targets
                    and not report["count_gaps"]
                    and all(_exact_count_parity(entry["settlement"]) for entry in targets)
                )
                traversal = (
                    exact_targets
                    and not report["failures"]
                    and not report["pending"]
                    and all(entry["source_page_evidence_complete"] is True for entry in targets)
                )
                gate("source_count_parity", parity, "source_comment_count_gap_or_unknown")
                gate("comments_traversal_and_parentage", traversal, "native_pages_ids_or_parents_unresolved")
                valid = valid and not report["count_gaps"] and report["global_validation"]["status"] == "passed"
                valid = valid and len(targets) == len(keys) and {entry["shortcode"] for entry in targets} == wanted
                valid = valid and all(
                    entry["status"] == "passed"
                    and not entry["diagnostics"]
                    and _exact_count_parity(entry["settlement"])
                    and entry["source_page_evidence_complete"] is True
                    for entry in targets
                )
            if name == "media":
                if report["comment_proof_mode"] == OBSERVED_INVENTORY_SCOPE:
                    valid = valid and report.get("inventory_scope") == OBSERVED_INVENTORY_SCOPE
                    valid = valid and _known_observed_settlements(before["reports"]["comments"])
                    valid = (
                        valid
                        and gates["posts"]["status"] == "passed"
                        and gates["comments_traversal_and_parentage"]["status"] == "passed"
                    )
                    valid = valid and report.get("comment_observed_inventory_sha256") == _stable_audit_hash(
                        before["reports"]["comments"]
                    )
                    valid = valid and report.get("independent_post_proof_sha256") == _stable_audit_hash(
                        before["reports"]["posts"]
                    )
                else:
                    valid = valid and report["comment_proof_mode"] == "strict_aggregate"
                valid = valid and all(
                    entry["inventory_fingerprint"]["version"] == MEDIA_INVENTORY_VERSION
                    and _hash(entry["inventory_fingerprint"]["sha256"])
                    for entry in verified
                )
            gate(name, valid, "complete_strict_inventory_required")
        expected = before["physical_plan"]
        objects = byte_report["objects"]
        signatures = {
            hashlib.sha256(json.dumps(item, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()
            for item in expected
        }
        expected_roles = sorted((role["shortcode"], role["role"]) for item in expected for role in item["roles"])
        actual_roles = sorted((role["shortcode"], role["role"]) for item in objects for role in item["roles"])
        bytes_ok = bool(expected) and len(objects) == len(expected) == len(signatures)
        bytes_ok = (
            bytes_ok and {item["object_signature"] for item in objects} == signatures and actual_roles == expected_roles
        )
        bytes_ok = bytes_ok and byte_report["status"] == "passed" and not byte_report["failures"]
        for item in objects:
            decoded = item.get("decode") or {}
            complete = (
                decoded.get("all_frames_decoded") is True or decoded.get("all_video_audio_streams_decoded") is True
            )
            bytes_ok = bytes_ok and item["status"] == "verified" and item["storage_cdn_full_byte_equality"] is True
            bytes_ok = (
                bytes_ok
                and type(item["bytes"]) is int
                and item["bytes"] > 0
                and _hash(item["observed_sha256"])
                and complete
            )
        gate("hosted_bytes", bytes_ok, "all_full_bodies_and_decode_required")
        stable = snapshot_fingerprint(before) == snapshot_fingerprint(after)
        stable = stable and _hash(source_sha256) and source_sha256 == source_after_sha256
        stable = stable and _hash(storage_binding_sha256) and storage_binding_sha256 == storage_binding_after_sha256
        gate("unchanged_evidence", stable, "source_records_or_binding_changed_during_verification")
        result.update(
            evidence_fingerprint=snapshot_fingerprint(before),
            evidence_after_fingerprint=snapshot_fingerprint(after),
            storage_binding_sha256=storage_binding_sha256,
            source_sha256=source_sha256,
            physical_objects=len(expected),
            required_roles=len(expected_roles),
            byte_report=byte_report,
        )
        passed = all(item["status"] == "passed" for item in gates.values())
        result.update(status="passed" if passed else "blocked", final_acceptance=passed)
    except (KeyError, TypeError, ValueError, AttributeError):
        gate("certificate_shape", False, "missing_or_invalid_required_evidence")
    return result
