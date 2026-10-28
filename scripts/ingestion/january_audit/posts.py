#!/usr/bin/env python3
"""Read-only source-to-canonical post audit; never certifies comments or media."""

# Auditor closures are consumed synchronously within each target/page.
# ruff: noqa: B023

# Retained SQL and diagnostic vocabularies are kept intact for audit comparison.
# ruff: noqa: E501
from __future__ import annotations

import hashlib
import json
from collections import Counter
from contextlib import nullcontext
from datetime import datetime

from . import common


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def equal(actual, expected):
    if isinstance(expected, datetime):
        try:
            return common.stamp(actual) == expected if isinstance(actual, str) else actual == expected
        except (ValueError, TypeError):
            return False
    return actual == expected


def attested_equal(actual, expected):
    """Compare source-attested nonempty values under maintained merge rules."""
    from trr_backend.ingestion.canonical import _present

    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            attested_equal(actual.get(k), v) for k, v in expected.items() if _present(v)
        )
    return equal(actual, expected)


def inspect(cur, binding, release, listing_report):
    from trr_backend.ingestion.application import validate_detail
    from trr_backend.ingestion.canonical import _post_payload
    from trr_backend.ingestion.domain import RunIntent, Stage
    from trr_backend.ingestion.freshness import detail_eligibility
    from trr_backend.ingestion.providers import parse_evidence
    from trr_backend.ingestion.queries import Queries

    for setting, expected in (("transaction_read_only", "on"), ("transaction_isolation", "repeatable read")):
        cur.execute("show " + setting)
        common.require(cur.fetchone()[setting] == expected, "snapshot_" + setting)
    cur.execute("select clock_timestamp() as at")
    at = cur.fetchone()["at"]
    progress = Queries(read_connection=lambda: nullcontext(cur.connection)).progress(common.RUN, common.ACCOUNT)
    intent = RunIntent.from_payload(progress["intent"])
    common.require(intent.digest == progress["intent_hash"] == common.INTENT, "live_intent_mismatch")
    common.require(
        progress["intent"] == binding["successor_intent"] and progress["generation"] == common.GENERATION,
        "binding_mismatch",
    )
    common.require(
        listing_report["run_id"] == common.RUN and listing_report["intent_hash"] == common.INTENT,
        "audit_report_binding_mismatch",
    )
    common.require(
        listing_report["listing_proof"]["boundary_verified"] is True
        and listing_report["listing_proof"]["independent_native_membership_verified"] is True,
        "audited_listing_required",
    )
    keys = listing_report["reported_january_keys"]
    common.require(
        len(keys) == listing_report["independently_verified_january_count"] and bool(keys), "audited_keys_mismatch"
    )
    records = common.rows(
        cur,
        """select t.target_key,t.outcome as target_outcome,t.checkpoint,t.committed_at,
        t.owner_generation as target_owner_generation,t.owner_token as target_owner_token,
        r.outcome as receipt_outcome,r.owner_generation,r.owner_token as receipt_token,
        r.canonical_identity,r.observed_at,r.evidence_id,
        exists(select 1 from social.instagram_ingestion_dispatches d where d.owner_token=r.owner_token
          and d.run_id=t.run_id and d.stage=t.stage and d.target_key=t.target_key and d.generation=1) as dispatch_identity_matches,
        to_jsonb(p) as post,to_jsonb(s) as sidecar,to_jsonb(c) as catalog,
        to_jsonb(cs) as catalog_sidecar,to_jsonb(q) as canonical
      from social.instagram_ingestion_targets t
      left join social.instagram_ingestion_receipts r on r.run_id=t.run_id and r.stage=t.stage and r.target_key=t.target_key
        and r.owner_token=t.owner_token and r.owner_generation=t.owner_generation
      left join social.instagram_posts p on p.shortcode=t.target_key
      left join social.instagram_post_payloads s on s.post_id=p.id
      left join social.instagram_account_catalog_posts c on c.source_id=t.target_key
      left join social.instagram_account_catalog_post_payloads cs on cs.catalog_post_id=c.id
      left join social.social_posts q on q.platform='instagram' and q.source_id=t.target_key
      where t.run_id=%s and t.stage='post_details' order by t.target_key""",
        (common.RUN,),
    )
    common.require(
        {r["target_key"] for r in records} == set(keys) and len(records) == len(keys), "detail_target_set_mismatch"
    )
    listing_rows = common.rows(
        cur,
        """select r.canonical_identity from social.instagram_ingestion_receipts r
        join social.instagram_ingestion_targets t using(run_id,stage,target_key)
        where r.run_id=%s and r.stage='listing' and r.outcome='committed'
          and r.owner_token=t.owner_token and r.owner_generation=t.owner_generation""",
        (common.RUN,),
    )
    listing_identities = {}
    for page in listing_rows:
        for identity in page["canonical_identity"].get("posts", []):
            if identity.get("shortcode") in keys:
                previous = listing_identities.setdefault(identity["shortcode"], identity)
                common.require(previous == identity, "listing_canonical_identity_conflict")
    failures, pending, verified = [], [], []
    outcomes = Counter()
    for record in records:
        key = record["target_key"]
        problems = []

        def check(condition, code):
            if not condition:
                problems.append(code)

        outcomes[record["target_outcome"] or "pending"] += 1
        if record["target_outcome"] not in ("committed", "cached_satisfied"):
            pending.append({"shortcode": key, "outcome": record["target_outcome"]})
            continue
        check(record["target_outcome"] == record["receipt_outcome"], "receipt_outcome_mismatch")
        check(common.receipt_fence(record), "receipt_fence_mismatch")
        identity = record["canonical_identity"] or {}
        prior = listing_identities.get(key, {})
        for field, table in (("post_id", "post"), ("catalog_post_id", "catalog"), ("canonical_post_id", "canonical")):
            check(str(identity.get(field)) == str((record[table] or {}).get("id")), field + "_mapping_mismatch")
            check(str(prior.get(field)) == str(identity.get(field)), field + "_listing_identity_mismatch")
        reference = identity.get("cache_source")
        if record["target_outcome"] == "cached_satisfied":
            check(record["evidence_id"] is None, "cache_receipt_has_new_capture")
            if not isinstance(reference, dict):
                problems.append("cache_original_reference_missing")
                failures.append({"shortcode": key, "codes": problems})
                continue
        else:
            retained = common.rows(
                cur, "select * from social.instagram_ingestion_evidence where evidence_id=%s", (record["evidence_id"],)
            )
            if len(retained) != 1:
                problems.append("detail_capture_absent")
                failures.append({"shortcode": key, "codes": problems})
                continue
            reference = {
                field: retained[0][field]
                for field in (
                    "evidence_id",
                    "run_id",
                    "stage",
                    "target_key",
                    "content_hash",
                    "parser_version",
                    "schema_version",
                    "captured_at",
                )
            }
        # PostgreSQL returns UUID/datetime objects for fresh captures. JSON
        # evidence references use their canonical strings for the same facts.
        reference = {
            **reference,
            "evidence_id": str(reference["evidence_id"]),
            "run_id": str(reference["run_id"]),
            "captured_at": common.stamp(reference["captured_at"]).isoformat(),
        }
        evidence, source_row = common.read_reference(cur, reference, source_identity="instagram:post:" + key)
        check(source_row["captured_at"] == record["observed_at"], "original_receipt_clock_mismatch")
        parsed = parse_evidence(
            evidence,
            account=common.ACCOUNT,
            stage=Stage.DETAILS,
            target_key=key,
            parser_version=evidence.parser_version,
            pinned=source_row["source_run_state"] != "completed",
        )
        source = dict(parsed.source_payload)
        try:
            proof = validate_detail(parsed)
        except Exception as exc:
            problems.append("detail_coverage_" + type(exc).__name__)
            failures.append({"shortcode": key, "codes": problems})
            continue
        source["detail_snapshot"] = proof
        assert parsed.post is not None
        expected = _post_payload(parsed.post, source, common.ACCOUNT, parsed.observed_at)
        post, sidecar, catalog, catalog_sidecar, canonical = [
            record[name] or {} for name in ("post", "sidecar", "catalog", "catalog_sidecar", "canonical")
        ]
        check(
            post.get("shortcode") == catalog.get("source_id") == canonical.get("source_id") == key,
            "canonical_shortcode_mismatch",
        )
        assert parsed.post is not None
        check(str(post.get("media_id")) == str(parsed.post.source_id), "native_id_mismatch")
        check(common.START <= common.stamp(post["posted_at"]) < common.END, "canonical_date_outside_january")
        # Only fields actually attested by this capture are compared. Enrichment
        # absent from this capture is neither erased nor represented as proved.
        for field, value in expected.items():
            if field == "raw_data":
                continue
            if field == "views":
                check(type(post.get(field)) is int and post[field] >= value, "post_preserved_views_below_capture")
            else:
                check(attested_equal(post.get(field), value), "post_" + field + "_mismatch")
        catalog_fields = (
            "source_account",
            "posted_at",
            "permalink",
            "caption",
            "media_type",
            "media_urls",
            "thumbnail_url",
            "hashtags",
            "mentions",
            "collaborators",
            "profile_tags",
            "likes",
            "comments_count",
            "music_info",
            "audio_url",
            "child_posts_data",
            "owner_username",
            "video_play_count",
            "video_duration",
        )
        for field in catalog_fields:
            if field in expected:
                check(attested_equal(catalog.get(field), expected[field]), "catalog_" + field + "_mismatch")
        for field, source_field in (("shares", "media_repost_count"), ("paid_partnership", "is_paid_partnership")):
            if source_field in expected:
                check(catalog.get(field) == expected[source_field], "catalog_" + field + "_mismatch")
        if "views" in expected:
            check(
                type(catalog.get("views")) is int and catalog["views"] >= expected["views"],
                "catalog_preserved_views_below_capture",
            )
        for field, from_field in (("body", "caption"), ("media_type", "media_type"), ("posted_at", "posted_at")):
            if from_field in expected:
                check(equal(canonical.get(field), expected[from_field]), "social_post_" + field + "_mismatch")
        for field, source_field in (
            ("like_count", "likes"),
            ("comment_count", "comments_count"),
            ("view_count", "views"),
            ("share_count", "media_repost_count"),
        ):
            if source_field in expected:
                check(
                    type(canonical.get(field)) is int and canonical[field] >= expected[source_field],
                    "social_preserved_" + field + "_below_capture",
                )
        check(
            canonical.get("owner_handle") == post.get("username")
            and canonical.get("owner_handle_norm") == str(post.get("username") or common.ACCOUNT).lower().lstrip("@"),
            "social_owner_binding_mismatch",
        )
        if post.get("owner_user_id") is not None:
            check(equal(canonical.get("owner_id"), post["owner_user_id"]), "social_owner_id_mismatch")
        check(
            canonical.get("canonical_url") == (post.get("permalink") or f"https://www.instagram.com/p/{key}/"),
            "social_canonical_url_mismatch",
        )
        refs = common.rows(
            cur,
            """select legacy_table,legacy_pk,legacy_source_id,post_id from social.social_post_legacy_refs
            where platform='instagram' and legacy_schema='social' and post_id=%s
              and legacy_table in ('instagram_posts','instagram_account_catalog_posts')""",
            (identity.get("canonical_post_id"),),
        )
        for table, field in (("instagram_posts", "post_id"), ("instagram_account_catalog_posts", "catalog_post_id")):
            check(
                any(
                    r["legacy_table"] == table and r["legacy_pk"] == identity[field] and r["legacy_source_id"] == key
                    for r in refs
                ),
                "legacy_ref_" + table + "_mismatch",
            )
        for table, field in (
            (post, "scraped_at"),
            (catalog, "last_seen_at"),
            (catalog, "updated_at"),
            (canonical, "last_seen_at"),
            (canonical, "last_scraped_at"),
        ):
            check(equal(table.get(field), parsed.observed_at), "source_capture_clock_" + field + "_mismatch")
        for payload in (sidecar, catalog_sidecar):
            raw = payload.get("raw_data") or {}
            saved_proof = raw.get("detail_snapshot") or {}
            check(all(saved_proof.get(k) == v for k, v in proof.items()), "sidecar_detail_coverage_or_clock_mismatch")
            if "evidence_reference" in saved_proof:
                saved_reference = saved_proof["evidence_reference"]
                check(saved_reference == reference, "sidecar_original_evidence_reference_mismatch")
                common.read_reference(cur, saved_reference, source_identity="instagram:post:" + key)
            check(
                equal(payload.get("payload_updated_at"), parsed.observed_at), "sidecar_original_payload_clock_mismatch"
            )
            check(
                attested_equal(payload.get("child_posts_data"), expected.get("child_posts_data", [])),
                "sidecar_child_column_inventory_mismatch",
            )
            check(attested_equal(raw, expected["raw_data"]), "sidecar_source_attested_fields_mismatch")
            check(
                raw.get("child_posts_data", []) == expected["raw_data"].get("child_posts_data", []),
                "sidecar_child_inventory_mismatch",
            )
            for field in ("shortcode", "pk", "caption", "likes", "comments", "media_urls"):
                if field in expected["raw_data"]:
                    check(raw.get(field) == expected["raw_data"][field], "sidecar_" + field + "_mismatch")
        check(parsed.observed_at <= at, "source_capture_in_future")
        unknown_fields = []
        native = parsed.post.raw_data

        def hidden(*names):
            return any(str(native.get(name) or "").lower() in {"true", "1", "yes", "on", "y", "t"} for name in names)

        if hidden("like_and_view_counts_disabled", "hide_like_and_view_counts", "like_count_hidden", "likes_hidden"):
            unknown_fields.append("likes")
        if hidden(
            "like_and_view_counts_disabled",
            "hide_like_and_view_counts",
            "like_count_hidden",
            "likes_hidden",
            "view_count_hidden",
            "views_hidden",
        ):
            unknown_fields.append("views")
        if hidden("comment_count_hidden", "comments_count_hidden"):
            unknown_fields.append("comments_count")
        if record["target_outcome"] == "cached_satisfied":
            # Eligibility at receipt commit time must be proved separately from
            # its original observed_at, which intentionally remains unchanged.
            commit = record["committed_at"]
            at_commit = detail_eligibility(
                post=post, sidecar=sidecar, canonical_identity=identity, intent=intent, now=commit
            )
            check(at_commit.eligible and at_commit.observed_at == parsed.observed_at, "cache_not_eligible_at_commit")
        if problems:
            failures.append({"shortcode": key, "codes": sorted(set(problems))})
        else:
            verified.append(
                {
                    "shortcode": key,
                    "native_id": str(parsed.post.source_id),
                    "source_evidence_id": str(reference["evidence_id"]),
                    "source_hash": evidence.content_hash,
                    "original_captured_at": parsed.observed_at.isoformat(),
                    "canonical_identity": {
                        field: identity[field] for field in ("post_id", "catalog_post_id", "canonical_post_id")
                    },
                    "post_row_sha256": digest(post),
                    "sidecar_sha256": digest(sidecar),
                    "catalog_sha256": digest(catalog),
                    "canonical_sha256": digest(canonical),
                    "outcome": record["target_outcome"],
                    "source_unknown_fields": unknown_fields,
                }
            )
    return {
        "schema_version": 1,
        "operator_scope": "post_records_only_not_final_certifier",
        "final_acceptance": False,
        "status": "blocked" if failures else "pending" if pending else "passed",
        "observed_at": at.isoformat(),
        "run_id": common.RUN,
        "intent_hash": common.INTENT,
        "release": str(release),
        "full_history_gate_closed": True,
        "audited_january_count": len(keys),
        "verified_post_count": len(verified),
        "outcomes": dict(outcomes),
        "pending": pending,
        "failures": failures,
        "verified": verified,
        "native_requests": 0,
        "database_mutations": 0,
    }
