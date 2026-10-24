"""Fenced offline recovery of comments omitted by an earlier retained-page parse.

No provider or source transport is called. Each page stays partial: existing
ReplayCommands writes canonical rows and journals apply receipts; this operation
never changes the comments checkpoint, settlement, or missing/deleted inventory.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import nullcontext
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from psycopg2 import Error as DatabaseError
from psycopg2.extras import RealDictCursor, execute_values

from api.schemas.v2.instagram_ingestion import InstagramReplayV2
from trr_backend.ingestion.canonical import CanonicalWriteError, _comment_media_urls
from trr_backend.ingestion.comments import COMMENT_PARSER_VERSION, CommentSourceError, parse_comment_evidence
from trr_backend.ingestion.domain import Conflict, ReplayUnavailable, RunIntent, fingerprint, stable_json
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.ingestion.replay_commands import ReplayCommands
from trr_backend.ingestion.validation_gate import require_allowed_intent

VERSION = "instagram-retained-comment-recovery-v1"
STREAM_BATCH_PAGES = 128
_CANONICAL = """select c.id,c.comment_id,c.parent_comment_external_id,c.parent_comment_id,
    p.comment_id as uuid_parent_native_id,c.media_urls,c.is_missing,c.missing_at,c.deleted_at,
    c.scraped_at,c.last_seen_at
    from social.instagram_comments c left join social.instagram_comments p on p.id=c.parent_comment_id
    where c.post_id=%s"""
_STREAM = """select p.page_id,p.committed_at,p.observed_at,p.content_hash as page_hash,
    e.evidence_id,e.run_id,e.stage,e.target_key,e.source_identity,e.captured_at,
    e.content_hash,e.parser_version,e.schema_version,e.payload
    from social.instagram_ingestion_comment_pages p
    join social.instagram_ingestion_evidence e using(evidence_id,run_id,stage,target_key)
    where p.run_id=%s and p.target_key=%s
    order by p.committed_at,p.page_id"""


def _references(urls):
    result = []
    for value in urls or []:
        parts = urlsplit(value)
        if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
            raise ValueError("invalid_retained_attachment_reference")
        result.append(urlunsplit((parts.scheme, parts.netloc, parts.path, "", "")))
    return result


def _flatten(page):
    expected = {}

    def visit(comment, raw, parent=None):
        parent = (
            parent or comment.parent_comment_id or raw.get("parent_comment_external_id") or raw.get("parent_comment_id")
        )
        parent = str(parent) if parent else None
        identity = str(comment.comment_id)
        if identity in expected:
            raise CommentSourceError("Duplicate retained native identity")
        expected[identity] = {"parent": parent, "media_urls": _references(_comment_media_urls(raw))}
        children = raw.get("replies") or raw.get("child_comments") or []
        for child, child_raw in zip(comment.replies, children, strict=True):
            visit(child, child_raw, identity)

    for comment, raw in zip(page.comments, page.source_payloads, strict=True):
        visit(comment, raw)
    return expected


def _differences(expected, canonical):
    missing, parents, media, inactive, would_drop = [], [], [], [], []
    for identity, native in expected.items():
        saved = canonical.get(identity)
        if saved is None:
            missing.append(identity)
            continue
        if saved.get("is_missing") or saved.get("deleted_at") is not None:
            inactive.append(identity)
        parent = native["parent"]
        if saved.get("parent_comment_external_id") != parent or saved.get("uuid_parent_native_id") != parent:
            parents.append(
                {
                    "comment_id": identity,
                    "expected_parent": parent,
                    "saved_external_parent": saved.get("parent_comment_external_id"),
                    "saved_uuid_parent": saved.get("uuid_parent_native_id"),
                }
            )
        actual = _references(saved.get("media_urls"))
        absent = sorted(set(native["media_urls"]) - set(actual))
        if absent:
            media.append({"comment_id": identity, "missing_urls": absent})
        # A nonempty incoming list replaces canonical URLs. Empty incoming lists
        # are omitted by the existing writer and preserve existing attachments.
        extras = sorted(set(actual) - set(native["media_urls"])) if native["media_urls"] else []
        if extras:
            would_drop.append({"comment_id": identity, "preserved_urls": extras})
    return {
        "missing_native_ids": sorted(missing),
        "parent_mismatches": parents,
        "missing_attachments": media,
        "inactive_native_ids": sorted(inactive),
        "attachment_preservation_conflicts": would_drop,
    }


def _tombstone_conflicts(expected, canonical, observed_at):
    conflicts = []
    for identity in expected:
        row = canonical.get(identity) or {}
        missing = row.get("is_missing") and (row.get("missing_at") is None or row["missing_at"] >= observed_at)
        # Explicit deletions require their owning restoration workflow. Even a
        # later retained observation cannot silently clear an explicit deletion.
        if missing or row.get("deleted_at") is not None:
            conflicts.append(
                {
                    "comment_id": identity,
                    "missing_at": row["missing_at"].isoformat() if row.get("missing_at") else None,
                    "deleted_at": row["deleted_at"].isoformat() if row.get("deleted_at") else None,
                }
            )
    return conflicts


def _has_discrepancy(difference):
    return any(
        difference[field]
        for field in ("missing_native_ids", "parent_mismatches", "missing_attachments", "inactive_native_ids")
    )


def _idle(target, now):
    return not (
        target.get("owner_token")
        and (
            target.get("owner_expires_at") is not None
            and target["owner_expires_at"] > now
            or target.get("owner_expires_at") is None
            and target.get("outcome") == "unresolved"
        )
    )


def _parser_identity():
    from trr_backend.ingestion import canonical, comments
    from trr_backend.socials.instagram import post_normalizer

    return fingerprint(
        {
            module.__name__: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
            for module in (canonical, comments, post_normalizer)
        }
    )


def parser_implementation_fingerprint():
    """Version the parser dependencies and audit semantics together."""
    return fingerprint({"parser": _parser_identity(), "audit": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})


def retained_audit_summary_key(*, run_id, account, generation, target_key, checkpoint_hash, parser_hash):
    return "retained-comment-summary-v1:" + fingerprint(
        {
            "run_id": str(run_id),
            "account": account,
            "generation": generation,
            "target_key": target_key,
            "checkpoint_hash": checkpoint_hash,
            "parser_implementation_sha256": parser_hash,
        }
    )


def _write_summary(cur, report, binding):
    fields = (
        "version",
        "run_id",
        "account",
        "generation",
        "target_key",
        "checkpoint_hash",
        "status",
        "scan_root_evidence_id",
        "parser_implementation_sha256",
        "stream_complete",
        "pages_audited",
        "historical_pages_audited",
        "current_scan_pages_audited",
        "parsed_native_occurrences",
        "unique_native_ids",
        "current_scan_unique_native_ids",
        "replayed_pages",
        "recovered_native_ids",
        "unresolved_discrepancies",
        "current_checkpoint_coverage_changed",
        "source_requests",
        "final_acceptance",
    )
    summary = {key: report[key] for key in fields}
    key = retained_audit_summary_key(
        run_id=report["run_id"],
        account=report["account"],
        generation=report["generation"],
        target_key=report["target_key"],
        checkpoint_hash=report["checkpoint_hash"],
        parser_hash=report["parser_implementation_sha256"],
    )
    cur.execute(
        """insert into social.instagram_ingestion_commands
        (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)
        on conflict(account,idempotency_key) do nothing""",
        (
            report["account"],
            key,
            fingerprint({"command": "retained_comment_summary_v1", **binding}),
            report["run_id"],
            stable_json(summary),
        ),
    )
    report["summary_key"] = key


def _journal(cur, run_id, account, binding, receipts):
    if not receipts:
        return
    values = []
    for receipt in receipts:
        request = {
            "command": VERSION,
            **binding,
            "evidence_id": receipt["evidence_id"],
            "evidence_hash": receipt["evidence_hash"],
            "result_hash": fingerprint(receipt),
        }
        hashed = fingerprint(request)
        values.append((account, "retained-comment-audit-v1:" + hashed, hashed, run_id, stable_json(receipt)))
    execute_values(
        cur,
        """insert into social.instagram_ingestion_commands
        (account,idempotency_key,command_hash,run_id,result) values %s
        on conflict(account,idempotency_key) do nothing""",
        values,
        page_size=100,
    )


def audit_and_replay_retained(store, *, run_id, account, generation, target_key, expected_checkpoint_hash):
    """Audit every retained scan without changing current traversal coverage.

    Invalid ownership/checkpoint requests raise Conflict. Unusable retained
    source, guarded canonical conflicts, and unavailable streaming return
    needs_fresh_source with explicit discrepancies; they never force a write.
    """
    UUID(str(run_id))
    if type(generation) is not int or generation < 1 or not re.fullmatch(r"[a-f0-9]{64}", expected_checkpoint_hash):
        raise ValueError("Exact retained-recovery generation and checkpoint hash required")
    if not isinstance(target_key, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", target_key):
        raise ValueError("Bounded retained-recovery target required")
    report = {
        "version": VERSION,
        "run_id": str(run_id),
        "account": account,
        "generation": generation,
        "target_key": target_key,
        "checkpoint_hash": expected_checkpoint_hash,
        "status": "needs_fresh_source",
        "final_acceptance": False,
        "source_requests": 0,
        "stream_complete": False,
        "pages_audited": 0,
        "replayed_pages": 0,
        "parsed_native_occurrences": 0,
        "unique_native_ids": 0,
        "recovered_native_ids": [],
        "pages": [],
        "unresolved_discrepancies": [],
        "preserved_existing_rows": True,
    }
    try:
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("set local lock_timeout='2s'")
            run = store._execution_run(cur, str(run_id), account, shared_fence=True)
            intent = RunIntent.from_payload(run["intent"])
            require_allowed_intent(intent)
            if (
                run["generation"] != generation
                or run["state"] not in {"queued", "running", "blocked", "completed"}
                or intent.account != account
                or intent.digest != run["intent_hash"]
                or intent.effective_source_profile.access_mode != "public"
                or run.get("execution_hold_reason")
            ):
                raise Conflict("Retained recovery requires the exact current unheld public run")
            target = _row(
                cur,
                """select checkpoint,owner_token,owner_expires_at,outcome,clock_timestamp() as database_now
                from social.instagram_ingestion_targets where run_id=%s and stage='comments' and target_key=%s
                for update""",
                (str(run_id), target_key),
            )
            if not target or not _idle(target, target["database_now"]):
                raise Conflict("Retained recovery requires an idle comment target")
            checkpoint = target["checkpoint"]
            if fingerprint(checkpoint) != expected_checkpoint_hash:
                raise Conflict("Retained recovery checkpoint changed since review")
            state = checkpoint.get("comment_pages") or {}
            root = state.get("scan_root_evidence_id")
            if state.get("adapter_family") != "instagram_public_relay" or state.get("adapter_version") != 1 or not root:
                raise Conflict("Retained recovery requires a current public scan root")
            root_row = _row(
                cur,
                """select page_id,committed_at from social.instagram_ingestion_comment_pages
                where run_id=%s and target_key=%s and evidence_id=%s""",
                (str(run_id), target_key, root),
            )
            if root_row is None:
                report.update(status="offline_audit_blocked", reason="current_scan_root_page_missing")
                return report
            post = _row(cur, "select id from social.instagram_posts where shortcode=%s", (target_key,))
            if post is None:
                report.update(status="offline_audit_blocked", reason="canonical_parent_post_missing")
                return report
            # ReplayCommands invalidates this media target before taking the
            # canonical comment lock. Preserve the existing lock order and owner.
            media = _row(
                cur,
                """select owner_token,owner_expires_at,outcome,clock_timestamp() as database_now
                from social.instagram_ingestion_targets where run_id=%s and stage='media' and target_key=%s
                for update""",
                (str(run_id), target_key),
            )
            if media is not None and not _idle(media, media["database_now"]):
                raise Conflict("Retained recovery cannot invalidate an active media owner")
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:comments:" + str(post["id"]),)
            )
            cur.execute(_CANONICAL + " order by c.comment_id", (str(post["id"]),))
            canonical = {row["comment_id"]: dict(row) for row in cur.fetchall()}
            parser_hash = parser_implementation_fingerprint()
            binding = {
                "run_id": str(run_id),
                "generation": generation,
                "target_key": target_key,
                "checkpoint_hash": expected_checkpoint_hash,
                "scan_root_evidence_id": str(root),
                "parser_version": COMMENT_PARSER_VERSION,
                "parser_implementation_sha256": parser_hash,
            }
            report.update(scan_root_evidence_id=str(root), parser_implementation_sha256=parser_hash)
            replay = ReplayCommands(
                Store(transaction=lambda: nullcontext(conn), execution_placement=store.execution_placement)
            )
            all_native, current_native, recovered, receipts = set(), set(), set(), []
            with conn.cursor(name="retained_comment_recovery_" + uuid4().hex, cursor_factory=RealDictCursor) as stream:
                stream.itersize = STREAM_BATCH_PAGES
                stream.execute(_STREAM, (str(run_id), target_key))
                for row in stream:
                    historical = (row["committed_at"], row["page_id"]) < (root_row["committed_at"], root_row["page_id"])
                    receipt = {
                        "version": VERSION,
                        "evidence_id": str(row["evidence_id"]),
                        "evidence_hash": row["content_hash"],
                        "page_id": str(row["page_id"]),
                        "status": "needs_fresh_source",
                        "parsed_native_count": None,
                        "observation_scope": "historical" if historical else "current_scan",
                        "captured_at": row["captured_at"].isoformat(),
                    }
                    try:
                        if (
                            str(row["run_id"]) != str(run_id)
                            or row["stage"] != "comments"
                            or row["target_key"] != target_key
                            or row["source_identity"]
                            not in {
                                f"instagram:comments:{target_key}:{kind}"
                                for kind in ("comments_api", "graphql_comments", "graphql_replies")
                            }
                            or row["content_hash"] != row["page_hash"]
                            or row["captured_at"] != row["observed_at"]
                        ):
                            raise ReplayUnavailable("Retained page evidence identity mismatch")
                        evidence = Evidence(
                            source_identity=row["source_identity"],
                            captured_at=row["captured_at"],
                            parser_version=row["parser_version"],
                            schema_version=row["schema_version"],
                            payload_json=row["payload"],
                            content_hash=row["content_hash"],
                        )
                        expected = _flatten(parse_comment_evidence(evidence, pinned=run["state"] != "completed"))
                        difference = _differences(expected, canonical)
                        all_native.update(expected)
                        if not historical:
                            current_native.update(expected)
                        tombstones = _tombstone_conflicts(expected, canonical, row["captured_at"])
                        report["parsed_native_occurrences"] += len(expected)
                        receipt.update(
                            parsed_native_count=len(expected),
                            native_inventory_sha256=fingerprint(expected),
                            **difference,
                            tombstone_conflicts=tombstones,
                        )
                        if tombstones:
                            receipt["reason"] = "newer_or_unknown_canonical_tombstone_preserved"
                        elif not _has_discrepancy(difference):
                            receipt["status"] = "matched"
                        elif difference["attachment_preservation_conflicts"]:
                            receipt["reason"] = "canonical_attachment_preservation_required"
                        else:
                            cur.execute("savepoint retained_comment_page")
                            try:
                                key = "retained-comment-replay-v1:" + fingerprint(
                                    {
                                        **binding,
                                        "evidence_id": str(row["evidence_id"]),
                                        "evidence_hash": row["content_hash"],
                                    }
                                )
                                result = replay(
                                    account,
                                    UUID(str(run_id)),
                                    InstagramReplayV2(
                                        evidence_id=row["evidence_id"],
                                        parser_version=COMMENT_PARSER_VERSION,
                                        apply=True,
                                    ),
                                    idempotency_key=key,
                                    initiated_by=VERSION,
                                )
                                cur.execute(
                                    _CANONICAL + " and c.comment_id=any(%s::text[])",
                                    (str(post["id"]), sorted(expected)),
                                )
                                updated = {saved["comment_id"]: dict(saved) for saved in cur.fetchall()}
                                remaining = _differences(expected, updated)
                                if _has_discrepancy(remaining):
                                    cur.execute("rollback to savepoint retained_comment_page")
                                    receipt.update(
                                        reason="canonical_replay_did_not_close_discrepancy", remaining=remaining
                                    )
                                else:
                                    canonical.update(updated)
                                    recovered.update(difference["missing_native_ids"])
                                    report["replayed_pages"] += 1
                                    receipt.update(
                                        status="recovered",
                                        replay_receipt_id=str(result.receipt_id),
                                        replay_writes=result.writes,
                                    )
                            except (Conflict, ReplayUnavailable, CanonicalWriteError, CommentSourceError):
                                cur.execute("rollback to savepoint retained_comment_page")
                                receipt["reason"] = "canonical_replay_guard_rejected_retained_page"
                            finally:
                                cur.execute("release savepoint retained_comment_page")
                    except (ReplayUnavailable, CommentSourceError, ValueError, TypeError, KeyError):
                        receipt["reason"] = "retained_payload_unavailable_or_invalid"
                    receipts.append(receipt)
            _journal(cur, str(run_id), account, binding, receipts)
            report.update(
                stream_complete=True,
                pages_audited=len(receipts),
                unique_native_ids=len(all_native),
                current_scan_unique_native_ids=len(current_native),
                historical_pages_audited=sum(item["observation_scope"] == "historical" for item in receipts),
                current_scan_pages_audited=sum(item["observation_scope"] == "current_scan" for item in receipts),
                current_checkpoint_coverage_changed=False,
                recovered_native_ids=sorted(recovered),
                pages=receipts,
                unresolved_discrepancies=[item for item in receipts if item["status"] == "needs_fresh_source"],
            )
            report["status"] = (
                "needs_fresh_source"
                if report["unresolved_discrepancies"]
                else "recovered"
                if recovered or report["replayed_pages"]
                else "matched"
            )
            report["audit_journal_entries"] = len(receipts)
            _write_summary(cur, report, binding)
            return report
    except DatabaseError as error:
        # The outer Store transaction rolls back every page and receipt. No
        # partial recoveries or successful stream proof survive that rollback.
        report.update(
            status="offline_audit_blocked",
            reason="retained_stream_or_transaction_failed",
            database_error_type=type(error).__name__,
            stream_complete=False,
            replayed_pages=0,
            recovered_native_ids=[],
            pages=[],
            audit_journal_entries=0,
            unresolved_discrepancies=[],
        )
        return report
