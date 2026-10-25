#!/usr/bin/env python3
"""Read-only January acceptance preflight, for execution by the sole runtime owner.

This operator cannot certify final acceptance. It verifies available native listing
proof and reports saved-record preconditions. Detailed cache clocks, comment source
content/topology, canonical media assets and actual hosted bytes remain explicit
manual gates. No transport, replay/apply, scheduling or business writer is called.
Default mode avoids re-reading previously verified listing history; --audit-listing
performs an independent complete native membership/boundary audit when needed.
"""

# Retained SQL and diagnostic vocabularies are kept intact for audit comparison.
# ruff: noqa: E501
from __future__ import annotations

import hashlib
import json
import re
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime

DEEP_GATES = {
    "post_details": "Canonical field coverage, sidecar clocks and cache provenance require source audit",
    "comments_replies": "Retained pages, current scan root, native content/parents and lane exhaustion require source audit",
    "media_records": "Ordinary result-v1 must match persisted manifest, canonical assets and all comment attachments",
    "hosted_bytes": "Full object reads, original hashes, decoding and GIF animation remain separate verification",
}
QUERY_FINGERPRINT_VERSION = "january-query-row-multiset-v2"


class GuardError(RuntimeError):
    """Messages are static identifiers, never raw exception text or payloads."""


def require(condition, code):
    if not condition:
        raise GuardError(code)


def stamp(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def receipt_fence(row):
    """Target reservation generations can advance independently of the run generation."""
    return (
        isinstance(row["owner_generation"], int)
        and row["owner_generation"] >= 1
        and row["owner_generation"] == row["target_owner_generation"]
        and str(row["receipt_token"]) == str(row["target_owner_token"])
        and row["dispatch_identity_matches"] is True
    )


def query_sql(sql):
    return re.sub(r"\bgeneration\s*=\s*1\b", "generation=" + str(context().generation), sql)


def row_fingerprint(row):
    return hashlib.sha256(
        json.dumps(
            row, sort_keys=True, default=str, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def record_query(sql, args, row_hashes):
    """Retain exact full-row coverage as an order-independent multiset digest.

    Duplicate multiplicity is preserved. Raw payload/checkpoint rows never stay
    in the invocation ledger; only fixed-size hashes are sorted transiently.
    """
    ordered = sorted(row_hashes)
    hashed = hashlib.sha256()
    for value in ordered:
        hashed.update(value.encode("ascii"))
    context().query_material.append(
        {"sql": sql, "args": args, "row_count": len(ordered), "rows_sha256": hashed.hexdigest()}
    )


def rows(cur, sql, args=()):
    sql = query_sql(sql)
    cur.execute(sql, args)
    result = [dict(row) for row in cur.fetchall()]
    record_query(sql, args, (row_fingerprint(row) for row in result))
    return result


@contextmanager
def stream_rows(cur, sql, args=()):
    """One full row at a time, on the caller's existing read-only snapshot."""
    from uuid import uuid4

    from psycopg2.extras import RealDictCursor

    sql = query_sql(sql)
    hashes = []
    completed = False
    with cur.connection.cursor(name="january_audit_" + uuid4().hex, cursor_factory=RealDictCursor) as stream:
        stream.itersize = 1
        stream.execute(sql, args)

        def values():
            nonlocal completed
            for value in stream:
                row = dict(value)
                hashes.append(row_fingerprint(row))
                yield row
            completed = True

        iterator = values()
        try:
            yield iterator
            require(completed, "page_stream_not_fully_consumed")
            record_query(sql, args, hashes)
        finally:
            iterator.close()


def configure_read_snapshot(connection, cur):
    require(connection.autocommit is False, "non_autocommit_snapshot_required")
    cur.execute("set transaction isolation level repeatable read read only")
    cur.execute("set local statement_timeout='30s'")
    cur.execute("set local lock_timeout='2s'")


def read_reference(cur, reference, *, source_identity, expected_target=None, expected_stage=None):
    from trr_backend.ingestion.evidence import Evidence

    require(isinstance(reference, dict), "reference_missing")
    found = rows(
        cur,
        """select e.*,r.account,r.state as source_run_state
        from social.instagram_ingestion_evidence e join social.instagram_ingestion_runs r using(run_id)
        where e.evidence_id=%s""",
        (reference["evidence_id"],),
    )
    require(len(found) == 1, "reference_absent")
    row = found[0]
    require(row["account"] == context().account, "reference_account_mismatch")
    for key in ("run_id", "stage", "target_key", "content_hash", "parser_version", "schema_version"):
        require(str(row[key]) == str(reference[key]), "reference_" + key + "_mismatch")
    require(row["captured_at"] == stamp(reference["captured_at"]), "reference_capture_mismatch")
    require(row["source_identity"] == source_identity, "reference_source_mismatch")
    if expected_target is not None:
        require(row["target_key"] == expected_target, "reference_target_mismatch")
    if expected_stage is not None:
        require(row["stage"] == expected_stage, "reference_stage_mismatch")
    require(row["payload"] is not None, "reference_payload_expired")
    evidence = Evidence(
        source_identity=row["source_identity"],
        captured_at=row["captured_at"],
        parser_version=row["parser_version"],
        payload_json=row["payload"],
        content_hash=row["content_hash"],
        schema_version=row["schema_version"],
    )
    # Pinned reads are legitimate only while that actual source run is unfinished.
    evidence.read(pinned=row["source_run_state"] != "completed")
    return evidence, row


def listing_audit(cur, intent, pages, final_state):
    from trr_backend.ingestion.ordered_listing import date_proof, parse_page, window_coverage

    selected, previous, native_ids = {}, {}, set()
    cursor, observed_dates = None, []
    for row in pages:
        checkpoint, identity = row["checkpoint"], row["canonical_identity"]
        require(checkpoint.get("cursor") == cursor, "listing_cursor_chain_mismatch")
        reference = checkpoint["ordered_v2"]["grid_ref"]
        require(reference["run_id"] == context().run_id, "grid_run_mismatch")
        evidence, source = read_reference(
            cur,
            reference,
            source_identity=f"instagram:listing:{context().account}:{row['target_key']}",
            expected_target=row["target_key"],
            expected_stage="listing",
        )
        page = parse_page(evidence.read(pinned=True), account=context().account, cursor=cursor)
        proofs, page_selected = [], {}
        refs = identity.get("date_evidence")
        require(isinstance(refs, dict), "listing_date_references_missing")
        require(set(refs) == {card.shortcode for card in page.cards}, "listing_date_reference_set_mismatch")
        for card in page.cards:
            ref = refs[card.shortcode]
            detail, _ = read_reference(cur, ref, source_identity=f"instagram:post:{card.shortcode}")
            proof = date_proof(card, detail, ref, account=context().account)
            proofs.append(proof)
            observed_dates.append(proof.posted_at)
            if context().date_start <= proof.posted_at < context().date_end:
                native = {"native_id": card.native_id, "posted_at": proof.posted_at.isoformat()}
                require(
                    card.shortcode not in selected or selected[card.shortcode] == native, "native_identity_conflict"
                )
                selected[card.shortcode] = page_selected[card.shortcode] = native
        require(set(identity.get("post_keys", [])) == set(page_selected), "listing_selected_keys_mismatch")
        require(
            {item.get("shortcode") for item in identity.get("posts", [])} == set(page_selected),
            "listing_post_identity_set_mismatch",
        )
        coverage = window_coverage(intent, page, tuple(proofs), previous)
        current_ids = {card.native_id for card in page.cards}
        coverage.update(
            native_ids=sorted(native_ids | current_ids),
            new_native_id_count=len(current_ids - native_ids),
            overlap_native_id_count=len(current_ids & native_ids),
        )
        require(coverage == identity.get("ordered_v2"), "listing_receipt_coverage_mismatch")
        require(receipt_fence(row), "listing_receipt_owner_fence_mismatch")
        require(row["request_hash"] == source["content_hash"], "discovery_grid_hash_mismatch")
        native_ids.update(current_ids)
        previous, cursor = coverage, page.end_cursor
    require(previous == final_state.get("ordered_v2", {}), "listing_final_coverage_mismatch")
    require(cursor == final_state.get("cursor"), "listing_final_cursor_mismatch")
    boundary = (
        bool(selected)
        and previous.get("discovery_complete") is True
        and previous.get("coverage_basis") == "ordered_window_crossing_heuristic_v1"
    )
    return selected, {
        "independent_native_membership_verified": True,
        "native_ids": len(native_ids),
        "oldest_native_date": min(observed_dates).isoformat() if observed_dates else None,
        "boundary_verified": bool(boundary),
        "verified_pages": len(pages),
    }


def summarize_gates(selected_count, boundary, lifecycle):
    gates = {
        "january_listing": {
            "status": "passed" if selected_count and boundary else "pending",
            "nonempty_native_january_required": True,
        },
        "lifecycle": {"status": "passed" if selected_count and lifecycle else "pending"},
    }
    for name, reason in DEEP_GATES.items():
        gates[name] = {"status": "pending", "reason": reason}
    return gates


def inspect(cur, binding, release, *, audit_listing=False):
    from trr_backend.ingestion.domain import RunIntent
    from trr_backend.ingestion.queries import Queries

    cur.execute("show transaction_read_only")
    require(cur.fetchone()["transaction_read_only"] == "on", "read_only_transaction_required")
    cur.execute("show transaction_isolation")
    require(cur.fetchone()["transaction_isolation"] == "repeatable read", "repeatable_read_snapshot_required")
    cur.execute("select clock_timestamp() as at")
    at = cur.fetchone()["at"]
    progress = Queries(read_connection=lambda: nullcontext(cur.connection)).progress(
        context().run_id, context().account
    )
    intent = RunIntent.from_payload(progress["intent"])
    require(progress["intent_hash"] == intent.digest == context().intent_hash, "live_intent_mismatch")
    require(progress["intent"] == binding["successor_intent"], "live_binding_intent_mismatch")
    require(
        progress["generation"] == context().generation and progress["execution_placement"] == "local",
        "live_execution_mismatch",
    )
    require(intent.date_start == context().date_start and intent.date_end == context().date_end, "live_window_mismatch")
    profile = progress["intent"]["source_profile"]
    require(
        profile["access_mode"] == "public"
        and profile["listing_family"] == "instagram_profile_ordered_public"
        and profile["source_contract_version"] == 2
        and profile["adapter_version"] == 1,
        "live_source_profile_mismatch",
    )
    pages = rows(
        cur,
        """select t.target_key,t.checkpoint,r.owner_generation,
        t.owner_generation as target_owner_generation,r.owner_token as receipt_token,
        t.owner_token as target_owner_token,r.canonical_identity,d.request_hash,
        (r.owner_token=t.owner_token and t.owner_generation=r.owner_generation and exists(
            select 1 from social.instagram_ingestion_dispatches x where x.owner_token=r.owner_token
            and x.run_id=r.run_id and x.generation=1 and x.stage=r.stage
            and x.target_key=r.target_key)) as dispatch_identity_matches
        from social.instagram_ingestion_targets t
        join social.instagram_ingestion_receipts r using(run_id,stage,target_key)
        join social.instagram_ingestion_discovery_pages d on d.run_id=t.run_id and d.stage=t.stage and d.page_id=t.target_key
        where t.run_id=%s and t.stage='listing' and t.outcome='committed' and r.outcome='committed'
        order by t.committed_at,t.target_key""",
        (context().run_id,),
    )
    state = rows(
        cur,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='listing'",
        (context().run_id,),
    )[0]["cursor_state"]
    reported = sorted({key for row in pages for key in row["canonical_identity"].get("post_keys", [])})
    selected, proof = (
        {},
        {
            "independent_native_membership_verified": False,
            "boundary_verified": False,
            "reason": "Native history audit deferred; receipt keys alone never prove January membership",
        },
    )
    if audit_listing:
        selected, proof = listing_audit(cur, intent, pages, state)
    targets = rows(
        cur,
        "select stage,target_key,outcome from social.instagram_ingestion_targets where run_id=%s and stage<>'listing'",
        (context().run_id,),
    )
    mismatch = []
    wanted = set(selected) if audit_listing else set(reported)
    for stage in ("post_details", "comments", "media"):
        actual = {row["target_key"] for row in targets if row["stage"] == stage}
        if actual != wanted:
            mismatch.append(
                {
                    "code": "downstream_key_set_mismatch",
                    "stage": stage,
                    "missing": sorted(wanted - actual),
                    "extra": sorted(actual - wanted),
                }
            )
    if audit_listing:
        require(set(reported) == set(selected), "independent_listing_key_set_mismatch")
    gates = summarize_gates(
        len(selected), proof["boundary_verified"], progress["complete"] and progress["state"] == "completed"
    )
    # This is a preparatory checker, never a substitute for deep source/byte acceptance.
    return {
        "schema_version": 1,
        "status": "pending",
        "final_acceptance": False,
        "operator_scope": "read_only_preflight_not_final_certifier",
        "observed_at": at.isoformat(),
        "run_id": context().run_id,
        "generation": context().generation,
        "intent_hash": context().intent_hash,
        "release": str(release),
        "full_history_gate_closed": True,
        "run_state": progress["state"],
        "stages": progress["stages"],
        "committed_listing_pages": len(pages),
        "reported_january_keys": reported,
        "independently_verified_january_count": len(selected) if audit_listing else None,
        "listing_proof": proof,
        "native_membership": selected,
        "gates": gates,
        "mismatches": mismatch,
        "byte_read_plan": {
            "status": "pending",
            "objects": [],
            "reason": "Construct only from audited ordinary result-v1 after canonical media inventory comparison",
        },
        "database_mutations": 0,
        "native_requests": 0,
        "storage_requests": 0,
    }


# All invocation identity comes from the maintained command, never a private release.
@dataclass
class AuditContext:
    run_id: str
    intent_hash: str
    account: str
    date_start: datetime
    date_end: datetime
    generation: int = 1
    query_material: list = field(default_factory=list)
    query_fingerprint_version: str = field(default=QUERY_FINGERPRINT_VERSION, init=False)


_CONTEXT = ContextVar("january_audit_context")


def context():
    return _CONTEXT.get()


@contextmanager
def bound(value):
    token = _CONTEXT.set(value)
    try:
        yield value
    finally:
        _CONTEXT.reset(token)


def __getattr__(name):
    fields = {
        "RUN": "run_id",
        "INTENT": "intent_hash",
        "ACCOUNT": "account",
        "START": "date_start",
        "END": "date_end",
        "GENERATION": "generation",
    }
    if name in fields:
        return getattr(context(), fields[name])
    raise AttributeError(name)
