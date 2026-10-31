#!/usr/bin/env python3
"""Read-only native comment-page, saved content and reply-topology audit."""

# Auditor closures are consumed synchronously within each target/page.
# ruff: noqa: B023

# Retained SQL and diagnostic vocabularies are kept intact for audit comparison.
# ruff: noqa: E501
from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager, nullcontext

from . import common
from .posts import attested_equal, digest, equal


def equal_native_text(actual, expected):
    # The maintained writer omits an empty native string on a new row, whose
    # nullable SQL text is NULL. Nonempty text and whitespace still compare exactly.
    return ("" if actual is None else actual) == expected


def media_binding(target, state, saved, media_target, asset_manifest):
    """Bind the deep native proof to exact current media inputs, without writes."""
    joined = {"shortcode", "uuid_parent_native_id", "uuid_parent_post_id"}
    canonical = [
        {k: v for k, v in row.items() if k not in joined}
        for row in sorted(saved, key=lambda row: (str(row["comment_id"]), str(row["id"])))
    ]
    return {
        "saved_rows_sha256": digest(canonical),
        "post_id": str(target["post_id"]),
        "scan_root_evidence_id": state.get("scan_root_evidence_id"),
        "observed_comment_ids": sorted(str(row["comment_id"]) for row in saved),
        "receipt": {
            "owner_token": str(target["receipt_token"]),
            "owner_generation": target["owner_generation"],
            "evidence_id": str(target["evidence_id"]),
            "observed_at": common.stamp(target["observed_at"]).isoformat(),
            "canonical_identity_sha256": digest(target["canonical_identity"]),
        },
        "media_reservation": {
            "owner_token": str(media_target["owner_token"]),
            "owner_generation": media_target["owner_generation"],
        },
        "media_manifest_sha256": digest(asset_manifest or {}),
    }


def applied_attachment(cur, key, post_id, saved, native_raw, check, *, pinned):
    """Validate a newer already-applied refresh from retained ordinary evidence."""
    from trr_backend.ingestion.evidence import Evidence
    from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, _media

    from .media import verify_attachment

    found = common.rows(
        cur,
        """select t.owner_token as target_owner_token,t.owner_generation as target_owner_generation,
        r.owner_token as receipt_token,r.owner_generation,r.outcome,r.canonical_identity,
        exists(select 1 from social.instagram_ingestion_dispatches d where d.run_id=t.run_id and d.stage=t.stage
           and d.target_key=t.target_key and d.owner_token=r.owner_token and d.generation=1) as dispatch_identity_matches,
        e.source_identity,e.captured_at,e.parser_version,e.schema_version,e.payload,e.content_hash
      from social.instagram_ingestion_targets t join social.instagram_ingestion_receipts r
        on r.run_id=t.run_id and r.stage=t.stage and r.target_key=t.target_key
        and r.owner_token=t.owner_token and r.owner_generation=t.owner_generation
      join social.instagram_ingestion_evidence e on e.evidence_id=r.evidence_id and e.run_id=r.run_id
        and e.stage=r.stage and e.target_key=r.target_key
      where t.run_id=%s and t.stage='media' and t.target_key=%s and t.outcome='committed'""",
        (common.RUN, key),
    )
    if len(found) != 1:
        check(False, "newer_attachment_retained_media_proof_pending")
        return False
    row = found[0]
    check(row["outcome"] == "committed" and common.receipt_fence(row), "attachment_media_receipt_fence_mismatch")
    check(row["parser_version"] == MEDIA_RESULT_REPLAY_VERSION, "attachment_ordinary_media_proof_required")
    e = Evidence(
        source_identity=row["source_identity"],
        captured_at=row["captured_at"],
        parser_version=row["parser_version"],
        schema_version=row["schema_version"],
        payload_json=row["payload"],
        content_hash=row["content_hash"],
    )
    e.read(pinned=pinned)
    parsed = _media(e, target_key=key, parser_version=row["parser_version"], pinned=pinned)
    if parsed.result is None:
        check(False, "attachment_media_result_missing")
        return False
    snapshot = parsed.result.snapshot
    check(snapshot.shortcode == key and snapshot.post_id == str(post_id), "attachment_current_post_mismatch")
    selected = [item for item in snapshot.comments if item.comment_id == str(saved["comment_id"])]
    if len(selected) != 1:
        check(False, "attachment_current_comment_missing")
        return False
    proof = verify_attachment(selected[0], saved, snapshot, check)
    info = native_raw.get("giphy_media_info") or native_raw.get("giphyMediaInfo") or {}
    check(
        bool(info.get("id"))
        and isinstance(proof, dict)
        and str(info["id"]) == str((proof.get("proof") or {}).get("native_gif_id")),
        "attachment_current_native_gif_mismatch",
    )
    return True


TARGET_CERTIFICATE_VERSION = 1


def source_page_manifest(row):
    """Describe one exact retained page only after evidence read and parse."""
    return {
        "page_id": str(row["page_id"]),
        "evidence_id": str(row["evidence_id"]),
        "run_id": str(row["run_id"]),
        "stage": row["stage"],
        "target_key": row["target_key"],
        "source_identity": row["source_identity"],
        "content_hash": row["evidence_hash"],
        "parser_version": row["parser_version"],
        "schema_version": row["schema_version"],
        "captured_at": common.stamp(row["captured_at"]).isoformat(),
    }


def target_certificate(key, outcome, deep_entry, diagnostics, pending_entry, gap_entry, pages, context=None):
    """Internal classification; external legacy verified[] is never a certificate."""
    codes = sorted(set(diagnostics))
    context = context or {}
    settlement = (deep_entry or {}).get("settlement") or context.get("settlement") or (gap_entry or {})
    if gap_entry or type(settlement.get("count_gap")) is not int or settlement.get("count_gap") != 0:
        codes.append("zero_gap_settlement_required")
    if outcome != "committed" or pending_entry:
        codes.append("target_deep_audit_unresolved")
    complete = bool(deep_entry) and bool(pages) and len(pages) == deep_entry.get("source_pages")
    if deep_entry and not complete:
        codes.append("source_page_evidence_manifest_incomplete")
    eligible = outcome == "committed" and complete and not codes and not pending_entry
    return {
        **context,
        **(deep_entry or {}),
        "shortcode": key,
        "status": "passed" if eligible else "blocked" if diagnostics else "pending",
        "diagnostics": sorted(set(codes)),
        "settlement": settlement,
        "source_page_evidence_manifest": pages,
        "source_page_evidence_sha256": digest(pages),
        "source_page_evidence_complete": complete,
    }


def certificate_envelope(keys, release, listing_hash, *, global_codes=(), manifest=None):
    keys = sorted(keys)
    return {
        "target_certificate_version": TARGET_CERTIFICATE_VERSION,
        "account": common.ACCOUNT,
        "generation": common.GENERATION,
        "run_id": common.RUN,
        "intent_hash": common.INTENT,
        "release": str(release),
        "listing_proof_sha256": listing_hash,
        "full_history_gate_closed": True,
        "audited_keys": keys,
        "audited_keyset_sha256": digest(keys),
        "global_validation": {
            "status": "blocked" if global_codes else "passed",
            "codes": list(global_codes),
            "comments_manifest_sha256": digest(manifest) if manifest is not None else None,
        },
    }


def inspect(cur, binding, release, listing, listing_hash=None):
    """An unknown exception invalidates every certificate in this artifact."""
    try:
        return _inspect(cur, binding, release, listing, listing_hash)
    except Exception:
        keys = listing.get("reported_january_keys", []) if isinstance(listing, dict) else []
        keys = sorted({key for key in keys if isinstance(key, str)}) if isinstance(keys, list) else []
        code = "global_audit_precondition_or_unclassified_failure"
        envelope = certificate_envelope(keys, release, listing_hash, global_codes=[code])
        return {
            **envelope,
            "schema_version": 1,
            "operator_scope": "comments_and_replies_not_final_certifier",
            "status": "blocked",
            "final_acceptance": False,
            "verified": [],
            "verified_targets": 0,
            "audited_january_count": len(keys),
            "failures": [{"code": code}],
            "pending": [],
            "count_gaps": [],
            "target_results": [
                {
                    "shortcode": key,
                    "status": "blocked",
                    "diagnostics": [code],
                    "source_page_evidence_manifest": [],
                    "source_page_evidence_sha256": digest([]),
                    "source_page_evidence_complete": False,
                    "settlement": {},
                }
                for key in keys
            ],
            "native_requests": 0,
            "database_mutations": 0,
        }


PAGE_PAYLOAD_SQL = """select p.*,e.source_identity,e.captured_at,e.expires_at,e.parser_version,e.schema_version,
        e.payload,e.content_hash as evidence_hash,r.state as source_run_state
      from social.instagram_ingestion_comment_pages p
      join social.instagram_ingestion_evidence e using(evidence_id,run_id,stage,target_key)
      join social.instagram_ingestion_runs r using(run_id)
      where p.run_id=%s and p.target_key=%s order by p.target_key,p.committed_at,p.page_id"""
PAGE_METADATA_FIELDS = ("page_id", "evidence_id", "target_key", "committed_at", "captured_at")


def read_settled_pages(cur, targets, settled):
    """Retain only ordered page identities, never growing checkpoint histories."""
    pages = []
    for target in targets:
        key = target["target_key"]
        if key not in settled:
            continue
        found = common.rows(
            cur,
            """select p.page_id,p.evidence_id,p.target_key,p.committed_at,e.captured_at
            from social.instagram_ingestion_comment_pages p
            join social.instagram_ingestion_evidence e using(evidence_id,run_id,stage,target_key)
            where p.run_id=%s and p.target_key=%s order by p.target_key,p.committed_at,p.page_id""",
            (common.RUN, key),
        )
        metadata = [{field: row[field] for field in PAGE_METADATA_FIELDS} for row in found]
        positions = [(row["committed_at"], str(row["page_id"])) for row in metadata]
        common.require(len({str(row["page_id"]) for row in metadata}) == len(metadata), "page_inventory_duplicate")
        common.require(
            all(left < right for left, right in zip(positions, positions[1:], strict=False)), "page_inventory_order"
        )
        common.require(all(row["target_key"] == key for row in metadata), "page_inventory_target")
        pages.extend(metadata)
    return pages


@contextmanager
def page_payloads(cur, metadata, *, target_key, start=0):
    """Fingerprint every historical full row; replay only the current suffix."""
    with common.stream_rows(cur, PAGE_PAYLOAD_SQL, (common.RUN, target_key)) as stream:

        def values():
            count = 0
            for index, row in enumerate(stream):
                common.require(index < len(metadata), "page_stream_extra_row")
                common.require(
                    all(row[field] == metadata[index][field] for field in PAGE_METADATA_FIELDS),
                    "page_stream_inventory_changed",
                )
                count += 1
                if index >= start:
                    yield row
            common.require(count == len(metadata), "page_stream_missing_row")

        iterator = values()
        try:
            yield iterator
        finally:
            iterator.close()


def _inspect(cur, binding, release, listing, listing_hash):
    from trr_backend.ingestion.application import validate_detail
    from trr_backend.ingestion.canonical import _comment_media_urls, _comment_payload, _post_payload, _time
    from trr_backend.ingestion.comment_checkpoints import page_progress_matches, page_progress_metadata
    from trr_backend.ingestion.comments import (
        CommentFetchSummary,
        _advance,
        _progress,
        _settlement,
        parse_comment_evidence,
    )
    from trr_backend.ingestion.domain import ReplayUnavailable, Stage
    from trr_backend.ingestion.evidence import Evidence
    from trr_backend.ingestion.providers import parse_evidence
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
        and type(progress["generation"]) is int
        and progress["generation"] == common.GENERATION
        and progress["account"] == common.ACCOUNT,
        "run_binding_mismatch",
    )
    common.require(
        isinstance(listing_hash, str)
        and len(listing_hash) == 64
        and all(c in "0123456789abcdef" for c in listing_hash),
        "listing_artifact_hash_required",
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
        isinstance(keys, list)
        and all(isinstance(key, str) and bool(key) for key in keys)
        and len(keys) == len(set(keys)) == listing["independently_verified_january_count"]
        and bool(keys),
        "audited_keys_mismatch",
    )
    manifests = common.rows(
        cur, "select * from social.instagram_ingestion_manifests where run_id=%s and stage='comments'", (common.RUN,)
    )
    common.require(
        len(manifests) == 1
        and manifests[0]["discovery_complete"] is True
        and manifests[0]["sealed_at"] is not None
        and str(manifests[0]["run_id"]) == common.RUN
        and manifests[0]["stage"] == "comments"
        and common.stamp(manifests[0]["sealed_at"]) <= at,
        "comments_discovery_not_sealed",
    )
    targets = common.rows(
        cur,
        """select t.target_key,t.outcome as target_outcome,t.checkpoint,t.owner_generation as target_owner_generation,
        t.owner_token as target_owner_token,r.owner_generation,r.owner_token as receipt_token,
        r.outcome as receipt_outcome,r.canonical_identity,r.evidence_id,r.observed_at,
        exists(select 1 from social.instagram_ingestion_dispatches d where d.owner_token=r.owner_token
          and d.run_id=t.run_id and d.stage=t.stage and d.target_key=t.target_key and d.generation=1) as dispatch_identity_matches,
        p.id as post_id,p.scraped_at as post_observed_at,p.comments_count,p.source_account,p.username,
        dr.canonical_identity as detail_identity,dr.evidence_id as detail_evidence_id,dr.observed_at as detail_observed_at,
        dt.committed_at as detail_committed_at
      from social.instagram_ingestion_targets t
      left join social.instagram_ingestion_receipts r on r.run_id=t.run_id and r.stage=t.stage and r.target_key=t.target_key
        and r.owner_token=t.owner_token and r.owner_generation=t.owner_generation
      left join social.instagram_posts p on p.shortcode=t.target_key
      left join social.instagram_ingestion_targets dt on dt.run_id=t.run_id and dt.stage='post_details' and dt.target_key=t.target_key
      left join social.instagram_ingestion_receipts dr on dr.run_id=dt.run_id and dr.stage=dt.stage and dr.target_key=dt.target_key
        and dr.owner_token=dt.owner_token and dr.owner_generation=dt.owner_generation
      where t.run_id=%s and t.stage='comments' order by t.target_key""",
        (common.RUN,),
    )
    common.require(
        len(targets) == len(keys) and {r["target_key"] for r in targets} == set(keys), "comment_target_set_mismatch"
    )
    settled = {r["target_key"] for r in targets if r["target_outcome"] == "committed"}
    pages = read_settled_pages(cur, targets, settled)
    saved = common.rows(
        cur,
        """select p.shortcode,c.*,par.comment_id as uuid_parent_native_id,par.post_id as uuid_parent_post_id
      from social.instagram_posts p join social.instagram_comments c on c.post_id=p.id
      left join social.instagram_comments par on par.id=c.parent_comment_id
      where p.shortcode=any(%s) and not c.is_missing and c.deleted_at is null
      order by p.shortcode,c.comment_id,c.id""",
        (sorted(settled),),
    )
    by_page, by_saved = defaultdict(list), defaultdict(list)
    for row in pages:
        by_page[row["target_key"]].append(row)
    for row in saved:
        by_saved[row["shortcode"]].append(row)
    media_rows = common.rows(
        cur,
        """select t.target_key,t.owner_token,t.owner_generation,p.asset_manifest
      from social.instagram_ingestion_targets t left join social.instagram_posts p on p.shortcode=t.target_key
      where t.run_id=%s and t.stage='media'""",
        (common.RUN,),
    )
    common.require(
        len({row["target_key"] for row in media_rows}) == len(media_rows)
        and {row["target_key"] for row in media_rows} == set(keys),
        "media_binding_target_set_mismatch",
    )
    media_by_key = {row["target_key"]: row for row in media_rows}
    pending, failures, gaps, verified = [], [], [], []
    page_manifests = defaultdict(list)
    for target in targets:
        key = target["target_key"]
        if key not in settled:
            pending.append({"shortcode": key, "outcome": target["target_outcome"]})
            continue
        try:
            problems = []

            def check(condition, code):
                if not condition:
                    problems.append(code)

            check(
                target["receipt_outcome"] == "committed" and common.receipt_fence(target),
                "settlement_receipt_fence_mismatch",
            )
            check(
                (target["source_account"] or target["username"] or "").lower().lstrip("@") == common.ACCOUNT,
                "saved_account_mismatch",
            )
            identity = target["canonical_identity"] or {}
            check(str(identity.get("post_id")) == str(target["post_id"]), "settlement_post_mapping_mismatch")
            observed_ids = identity.get("observed_comment_ids")
            check(
                isinstance(observed_ids, list)
                and all(isinstance(i, str) and bool(i) for i in observed_ids)
                and len(observed_ids) == len(set(observed_ids)),
                "settlement_native_inventory_not_unique",
            )
            state = _progress(target["checkpoint"])
            root = state.get("scan_root_evidence_id")
            target_pages = by_page[key]
            scan_pages = target_pages
            roots = [i for i, p in enumerate(scan_pages) if str(p["evidence_id"]) == root]
            if len(roots) != 1:
                with page_payloads(cur, target_pages, target_key=key, start=len(target_pages)) as unused:
                    for _ in unused:
                        pass
                failures.append({"shortcode": key, "codes": ["current_scan_root_missing_or_duplicate"]})
                continue
            scan_pages = scan_pages[roots[0] :]
            reconstructed = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
            native = {}
            page_uuid = {}
            restart_roots = {
                h["root_evidence_id"]: h
                for h in target["checkpoint"].get("public_lane_restart_history", [])
                if h.get("root_evidence_id")
            }
            with page_payloads(cur, target_pages, target_key=key, start=roots[0]) as streamed_pages:
                for row in streamed_pages:
                    check(
                        row["content_hash"] == row["evidence_hash"] and row["observed_at"] == row["captured_at"],
                        "page_evidence_hash_or_clock_mismatch",
                    )
                    check(
                        row["source_identity"]
                        in {
                            f"instagram:comments:{key}:{kind}"
                            for kind in ("comments_api", "graphql_comments", "graphql_replies")
                        },
                        "page_source_mismatch",
                    )
                    check(
                        isinstance(row["owner_generation"], int) and row["owner_generation"] >= 1,
                        "page_generation_invalid",
                    )
                    evidence = Evidence(
                        source_identity=row["source_identity"],
                        captured_at=row["captured_at"],
                        parser_version=row["parser_version"],
                        payload_json=row["payload"],
                        content_hash=row["evidence_hash"],
                        schema_version=row["schema_version"],
                    )
                    pinned = row["source_run_state"] != "completed"
                    evidence.read(now=at, pinned=pinned)
                    page = parse_comment_evidence(evidence, parser_version=evidence.parser_version, pinned=pinned)
                    page_manifests[key].append(source_page_manifest(row))
                    check(
                        page.parent_comment_id == row["parent_comment_id"]
                        and page.next_cursor == row["next_cursor"]
                        and page.exhausted == row["exhausted"]
                        and page.observed_at == row["observed_at"],
                        "parsed_page_receipt_mismatch",
                    )
                    if page.started_from_root and not page.parent_comment_id:
                        native.clear()
                        page_uuid.clear()
                    if str(row["evidence_id"]) in restart_roots and page.parent_comment_id:
                        check(page.started_from_root, "child_restart_not_root")
                        removed = {
                            i for i, v in reconstructed["observed"].items() if v["parent"] == page.parent_comment_id
                        }
                        reconstructed["observed"] = {
                            i: v for i, v in reconstructed["observed"].items() if i not in removed
                        }
                        for i in removed:
                            native.pop(i, None)
                            page_uuid.pop(i, None)
                    page_state = page_progress_metadata(row["checkpoint"])
                    started = common.stamp(page_state["scan_started_at"])
                    check(started <= page.observed_at, "scan_cutoff_after_capture")
                    _advance(reconstructed, page, scan_started_at=started)
                    if not page.parent_comment_id and page.started_from_root:
                        reconstructed["scan_root_evidence_id"] = str(row["evidence_id"])
                    # Reply-access administration is independent from native-page facts.
                    for field in (
                        "version",
                        "observed",
                        "lanes",
                        "expected",
                        "adapter_family",
                        "adapter_version",
                        "scan_started_at",
                        "scan_root_evidence_id",
                    ):
                        check(
                            page_progress_matches(row["checkpoint"], reconstructed, fields=(field,)),
                            "page_replay_" + field + "_mismatch",
                        )
                    current_ids = {}

                    def visit(comment, raw, depth=0):
                        parent = comment.parent_comment_id
                        depth = max(depth, int(raw.get("reply_depth") or (1 if parent else 0)))
                        native[comment.comment_id] = (comment, raw, page.observed_at, parent, depth)
                        current_ids[comment.comment_id] = True
                        for child, child_raw in zip(comment.replies, raw.get("replies", []), strict=True):
                            visit(child, child_raw, depth + 1)

                    for comment, raw in zip(page.comments, page.source_payloads, strict=True):
                        visit(comment, raw)
                    identifiers = row["canonical_identity"].get("comment_ids", {})
                    check(
                        str(row["canonical_identity"].get("post_id")) == str(target["post_id"]),
                        "page_canonical_post_id_mismatch",
                    )
                    check(set(identifiers) == set(current_ids), "page_canonical_native_id_set_mismatch")
                    for i, value in identifiers.items():
                        check(i not in page_uuid or page_uuid[i] == value, "native_uuid_changed_during_scan")
                        page_uuid[i] = value
            check(
                str(target["evidence_id"]) == str(scan_pages[-1]["evidence_id"])
                and target["observed_at"] == scan_pages[-1]["captured_at"],
                "settlement_last_page_clock_mismatch",
            )
            check(
                set(native) == set(state["observed"]) == set(identity.get("observed_comment_ids", [])),
                "current_native_inventory_mismatch",
            )
            for field in (
                "observed",
                "lanes",
                "expected",
                "adapter_family",
                "adapter_version",
                "scan_started_at",
                "scan_root_evidence_id",
            ):
                check(reconstructed.get(field) == state.get(field), "final_page_replay_" + field + "_mismatch")
            settlement = identity.get("settlement") or {}
            check(settlement == state.get("settlement"), "settlement_receipt_checkpoint_mismatch")
            # PublicRelayCommentSource receives the canonical post count. Its strict
            # fetch path returns that same expected count unchanged. Bind it to the
            # retained post capture, never to the settlement being verified.
            detail_identity = target["detail_identity"] or {}
            reference = detail_identity.get("cache_source")
            if not isinstance(reference, dict):
                retained = common.rows(
                    cur,
                    "select * from social.instagram_ingestion_evidence where evidence_id=%s",
                    (target["detail_evidence_id"],),
                )
                common.require(len(retained) == 1, "independent_post_count_capture_missing")
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
            detail, source_row = common.read_reference(cur, reference, source_identity="instagram:post:" + key)
            parsed_post = parse_evidence(
                detail,
                account=common.ACCOUNT,
                stage=Stage.DETAILS,
                target_key=key,
                parser_version=detail.parser_version,
                pinned=source_row["source_run_state"] != "completed",
            )
            validate_detail(parsed_post)
            assert parsed_post.post is not None
            native_post = _post_payload(
                parsed_post.post, parsed_post.source_payload, common.ACCOUNT, parsed_post.observed_at
            )
            expected = native_post.get("comments_count")
            check(
                type(expected) is int and expected >= 0 and target["comments_count"] == expected,
                "independent_post_comment_count_mismatch",
            )
            check(
                equal(target["post_observed_at"], parsed_post.observed_at)
                and target["detail_observed_at"] == parsed_post.observed_at
                and target["detail_committed_at"] <= common.stamp(state["scan_started_at"]),
                "post_count_capture_binding_or_order_mismatch",
            )
            if state.get("expected") is not None:
                check(
                    expected is not None and settlement.get("expected_total") == expected,
                    "native_expected_total_mismatch",
                )
            computed = _settlement(state, CommentFetchSummary(expected_count=expected, full_observation=True))
            check(computed == settlement and computed is not None, "native_lanes_or_settlement_incomplete")
            if settlement.get("count_gap", 0):
                gaps.append({"shortcode": key, **settlement})
            rows_by_id = {r["comment_id"]: r for r in by_saved[key]}
            check(
                len(rows_by_id) == len(by_saved[key]) and set(rows_by_id) == set(native),
                "active_saved_inventory_mismatch",
            )
            for i, (comment, raw, observed, parent, depth) in native.items():
                row = rows_by_id.get(i)
                if row is None:
                    continue
                check(str(row["id"]) == page_uuid.get(i), "native_saved_uuid_mismatch")
                for field, expected in (
                    ("text", comment.text),
                    ("username", comment.author.username or ""),
                    ("user_id", comment.author.user_id),
                    ("likes", max(0, comment.likes_count)),
                    ("is_reply", bool(parent)),
                    ("reply_depth", depth),
                ):
                    if expected is not None:
                        check(
                            equal_native_text(row.get(field), expected)
                            if field == "text"
                            else equal(row.get(field), expected),
                            "native_content_" + field + "_mismatch",
                        )
                check(
                    equal(row.get("scraped_at"), observed) and equal(row.get("last_seen_at"), observed),
                    "comment_original_capture_clock_mismatch",
                )
                check(
                    row.get("source_snapshot_type") == "full_comments_scrape"
                    and str(row.get("last_seen_run_id")) == common.RUN,
                    "fresh_full_comment_observation_missing",
                )
                if comment.created_at is not None:
                    check(equal(row.get("created_at"), _time(comment.created_at)), "native_created_at_mismatch")
                expected_payload = _comment_payload(
                    comment,
                    raw,
                    post_id=str(target["post_id"]),
                    account=common.ACCOUNT,
                    observed_at=observed,
                    run_id=common.RUN,
                    parent_id=parent,
                    depth=depth,
                    metadata_writes_enabled=True,
                )
                preserved_ranked = row.get("is_ranked") and not raw.get("is_ranked", raw.get("isRanked"))
                for field, value in expected_payload.items():
                    if field in {"raw_data", "first_seen_at", "media_urls"}:
                        continue
                    if preserved_ranked and field in {"is_ranked", "comment_index"}:
                        continue
                    if field == "cursor_payload" and isinstance(value, dict):
                        check(attested_equal(row.get(field), value), "source_attested_" + field + "_mismatch")
                    else:
                        check(equal(row.get(field), value), "source_attested_" + field + "_mismatch")
                marker = (row.get("raw_data") or {}).get("ingestion_attachment")
                refreshed = bool(marker and common.stamp(marker["captured_at"]) >= observed)
                if refreshed:
                    refreshed = applied_attachment(
                        cur, key, target["post_id"], row, raw, check, pinned=progress["state"] != "completed"
                    )
                expected_raw = expected_payload.get("raw_data") or {}
                if refreshed:
                    expected_raw = {
                        k: v for k, v in expected_raw.items() if k not in {"giphy_media_info", "giphyMediaInfo"}
                    }
                check(
                    attested_equal(row.get("raw_data") or {}, expected_raw),
                    "source_attested_comment_raw_fields_mismatch",
                )
                check((row.get("raw_data") or {}).get("parent_comment_id") == parent, "raw_native_parent_mismatch")
                check(row.get("parent_comment_external_id") == parent, "native_parent_external_mismatch")
                if parent:
                    check(
                        parent in rows_by_id
                        and str(row.get("parent_comment_id")) == str(rows_by_id[parent]["id"])
                        and str(row.get("uuid_parent_post_id")) == str(target["post_id"]),
                        "native_parent_uuid_mismatch",
                    )
                else:
                    check(row.get("parent_comment_id") is None, "top_level_has_uuid_parent")
                if not refreshed:
                    selected = _comment_media_urls(raw)
                    check((row.get("media_urls") or []) == selected, "selected_comment_attachment_mismatch")
            if problems:
                failures.append({"shortcode": key, "codes": sorted(set(problems))})
            else:
                verified.append(
                    {
                        "shortcode": key,
                        "native_comments": len(native),
                        "native_replies": sum(bool(v[3]) for v in native.values()),
                        "source_pages": len(scan_pages),
                        "settlement": settlement,
                        "original_settlement_at": target["observed_at"].isoformat(),
                        "checkpoint_sha256": digest(target["checkpoint"]),
                        "saved_rows_sha256": digest(by_saved[key]),
                        "media_binding": media_binding(
                            target, state, by_saved[key], media_by_key[key], media_by_key[key]["asset_manifest"]
                        ),
                    }
                )
        except (common.GuardError, ValueError, ReplayUnavailable):
            failures.append({"shortcode": key, "codes": ["target_retained_evidence_or_deep_validation_failed"]})
    verified_by_key = {entry["shortcode"]: entry for entry in verified}
    failures_by_key = {entry["shortcode"]: entry for entry in failures}
    pending_by_key = {entry["shortcode"]: entry for entry in pending}
    gaps_by_key = {entry["shortcode"]: entry for entry in gaps}
    target_results = []
    for target in targets:
        key = target["target_key"]
        checkpoint = target.get("checkpoint") or {}
        current_state = checkpoint.get("comment_pages") if isinstance(checkpoint, dict) else None
        context = {
            "checkpoint_sha256": digest(target.get("checkpoint")),
            "saved_rows_sha256": digest(by_saved[key]),
            "settlement": (target.get("canonical_identity") or {}).get("settlement") or {},
            "media_binding": None,
        }
        # Ineligible targets retain available current diagnostics, without
        # inventing absent receipt identities or observation clocks.
        if isinstance(current_state, dict) and all(
            target.get(field) is not None
            for field in ("post_id", "receipt_token", "owner_generation", "evidence_id", "observed_at")
        ):
            context["media_binding"] = media_binding(
                target, current_state, by_saved[key], media_by_key[key], media_by_key[key]["asset_manifest"]
            )
        target_results.append(
            target_certificate(
                key,
                target["target_outcome"],
                verified_by_key.get(key),
                failures_by_key.get(key, {}).get("codes", []),
                pending_by_key.get(key),
                gaps_by_key.get(key),
                page_manifests[key],
                context,
            )
        )
    envelope = certificate_envelope(keys, release, listing_hash, manifest=manifests[0])
    return {
        **envelope,
        "target_results": target_results,
        "schema_version": 1,
        "operator_scope": "comments_and_replies_not_final_certifier",
        "final_acceptance": False,
        "status": "blocked" if failures else "pending" if pending or gaps else "passed",
        "run_id": common.RUN,
        "intent_hash": common.INTENT,
        "observed_at": at.isoformat(),
        "release": str(release),
        "full_history_gate_closed": True,
        "audited_january_count": len(keys),
        "verified_targets": len(verified),
        "verified": verified,
        "failures": failures,
        "count_gaps": gaps,
        "pending": pending,
        "native_requests": 0,
        "database_mutations": 0,
    }
