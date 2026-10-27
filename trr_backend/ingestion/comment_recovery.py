"""Audited public comment recovery without cancelling unrelated target owners.

Recovery preserves old checkpoints in the command journal and all immutable
receipts. It authorizes explicit root observations; it never counts an old row
as fresh coverage or deletes a canonical comment.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent, fingerprint, stable_json
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.ingestion.validation_gate import require_allowed_intent


class RecoveryAdmissionDeferred(Conflict):
    """A new root was not admitted; no command or target mutation committed."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


_PRIORITY_SHORTLIST = 8


def _replayed_recovery(cur, account, idempotency_key, command_hash):
    previous = _row(
        cur,
        """select command_hash,result from social.instagram_ingestion_commands
        where account=%s and idempotency_key=%s""",
        (account, idempotency_key),
    )
    if previous:
        if previous["command_hash"] != command_hash:
            raise Conflict("Idempotency key belongs to another recovery")
        return previous["result"]
    return None


def _claim_root_admission(cur, run_id, account):
    # Wrappers can already hold target/post locks. Never wait for this gate:
    # a gate holder might itself be waiting for one of those rows.
    isolation = _row(cur, "select current_setting('transaction_isolation') as isolation")["isolation"]
    if isolation != "read committed":
        raise Conflict("Root admission requires a current read-committed snapshot")
    claimed = _row(
        cur,
        "select pg_try_advisory_xact_lock(hashtextextended(%s,0)) as claimed",
        (f"instagram-root-admission:{account}:{run_id}",),
    )
    if not claimed["claimed"]:
        raise RecoveryAdmissionDeferred("recovery_admission_busy")


def _check_root_capacity(cur, run_id, target_key):
    # Restarts disappear after the first committed page. The recovery command
    # remains throughout that unresolved pass, including cooldown/blocked work.
    outstanding = _row(
        cur,
        """select count(*) as count,coalesce(bool_or(target_key=%s),false) as includes_target
        from social.instagram_ingestion_targets where run_id=%s and stage='comments'
        and outcome='unresolved' and (
            coalesce(checkpoint->'public_lane_restarts','{}'::jsonb)<>'{}'::jsonb
            or checkpoint#>>'{public_recovery,command_key}' is not null)""",
        (target_key, run_id),
    )
    if outstanding["count"] > 4 or (outstanding["count"] == 4 and not outstanding["includes_target"]):
        raise RecoveryAdmissionDeferred("recovery_root_capacity_full")


def _known_max(*values):
    return max((value for value in values if type(value) is int and value >= 0), default=None)


def _strength(state):
    settlement = state.get("settlement") or {}
    return {
        "observed_ids": sorted((state.get("observed") or {}).keys()),
        "expected_total": _known_max(settlement.get("expected_total"), state.get("expected")),
        "count_gap": settlement.get("count_gap"),
        "unsupported": sorted(
            key for key, value in (state.get("observed") or {}).items() if value.get("unsupported_reply_topology")
        ),
    }


def _progress_frontier(recovery):
    """Return scheduling evidence, never a substitute for a fresh scan."""
    baseline = recovery.get("baseline") or {}
    saved = recovery.get("progress")
    if saved is None:
        observed = set(baseline.get("observed_ids") or [])
        unsupported = set(baseline.get("unsupported") or [])
        return {
            "version": 1,
            "observed_ids": sorted(observed),
            "best_observed_total": len(observed),
            "supported_ids": sorted(observed - unsupported),
        }
    if (
        not isinstance(saved, dict)
        or saved.get("version") != 1
        or not isinstance(saved.get("observed_ids"), list)
        or not isinstance(saved.get("supported_ids"), list)
        or any(not isinstance(value, str) for value in saved["observed_ids"] + saved["supported_ids"])
        or type(saved.get("best_observed_total")) is not int
        or saved["best_observed_total"] < 0
        or not set(saved["supported_ids"]).issubset(saved["observed_ids"])
    ):
        raise Conflict("Invalid public recovery progress frontier")
    return saved


def _advance_progress(recovery, strength):
    progress = _progress_frontier(recovery)
    observed = set(strength["observed_ids"])
    supported = observed - set(strength["unsupported"])
    return {
        "version": 1,
        "observed_ids": sorted(set(progress["observed_ids"]) | observed),
        "best_observed_total": max(progress["best_observed_total"], len(observed)),
        "supported_ids": sorted(set(progress["supported_ids"]) | supported),
    }


def unchanged_passes(checkpoint, outcome):
    recovery = checkpoint.get("public_recovery") or {}
    count = recovery.get("no_improvement_passes", 0)
    if not recovery or outcome not in {"committed", "cached_satisfied"}:
        return count
    strength = _strength(checkpoint.get("comment_pages") or {})
    progress = _progress_frontier(recovery)
    observed = set(strength["observed_ids"])
    supported = observed - set(strength["unsupported"])
    improved = bool(observed - set(progress["observed_ids"]))
    improved |= len(observed) > progress["best_observed_total"]
    improved |= bool(supported - set(progress["supported_ids"]))
    return 0 if improved else count + 1


def _can_recover_gap(checkpoint, outcome):
    gap = ((checkpoint.get("comment_pages") or {}).get("settlement") or {}).get("count_gap")
    return type(gap) is int and gap > 0 and unchanged_passes(checkpoint, outcome) < 2


def _canonical_shortfall_priority(row, saved_counts):
    state = row["checkpoint"].get("comment_pages") or {}
    expected = _known_max(
        (state.get("settlement") or {}).get("expected_total"),
        state.get("expected"),
        ((row["checkpoint"].get("public_recovery") or {}).get("baseline") or {}).get("expected_total"),
    )
    saved = saved_counts.get(row["target_key"])
    if expected is None or saved is None:
        return 1
    return 0 if saved < expected else 2


def _active_saved_counts(cur, shortcodes):
    """Rank recovery candidates using current canonical rows, not scan proof."""
    cur.execute(
        """select p.shortcode, count(distinct c.comment_id) filter
        (where not c.is_missing and c.deleted_at is null) as active_saved_ids
        from social.instagram_posts p left join social.instagram_comments c on c.post_id=p.id
        where p.shortcode=any(%s::text[]) group by p.shortcode""",
        (list(shortcodes),),
    )
    return {row["shortcode"]: row["active_saved_ids"] for row in cur.fetchall()}


def _prioritize_recovery_rows(cur, rows, sizes):
    rows = sorted(
        rows,
        key=lambda row: (
            unchanged_passes(row["checkpoint"], row["outcome"]),
            sizes.get(row["target_key"], 0),
            -row["checkpoint"]["comment_pages"]["settlement"]["count_gap"],
            row["target_key"],
        ),
    )
    shortlist = rows[:_PRIORITY_SHORTLIST]
    if shortlist:
        saved_counts = _active_saved_counts(cur, [row["target_key"] for row in shortlist])
        rows[:_PRIORITY_SHORTLIST] = sorted(shortlist, key=lambda row: _canonical_shortfall_priority(row, saved_counts))
    return rows


def enqueue_next_recovery(store, *, run_id, account, generation, intent_hash, source_revision, blocked_stage=False):
    """One bounded control step at a drained January worker completion boundary.

    The worker remains the only execution owner. No timer, monitor, provider
    request or independent process is introduced by this function.
    """
    from trr_backend.ingestion.productive_comment_budget import extend_productive_budgets

    extended = extend_productive_budgets(
        store,
        run_id=run_id,
        account=account,
        generation=generation,
        intent_hash=intent_hash,
        source_revision=source_revision,
    )
    if extended:
        return {"status": "productive_budget_extended", "targets": extended}
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select intent,intent_hash,generation from social.instagram_ingestion_runs where run_id=%s and account=%s",
            (run_id, account),
        )
        run = cur.fetchone()
        if not run:
            raise Conflict("Bound recovery run not found")
        intent = RunIntent.from_payload(run["intent"])
        if (
            run["intent_hash"] != intent_hash
            or intent.digest != intent_hash
            or run["generation"] != generation
            or intent.date_start != datetime(2026, 1, 1, tzinfo=UTC)
            or intent.date_end != datetime(2026, 2, 1, tzinfo=UTC)
            or intent.effective_source_profile.access_mode != "public"
        ):
            raise Conflict("Automatic recovery requires the exact frozen public January binding")
        cur.execute(
            """select target_key from social.instagram_ingestion_targets
            where run_id=%s and outcome='unresolved' and (%s=false or
                (stage='comments' and blocked_reason is null and attempts<max_attempts
                 and (next_attempt_at is null or next_attempt_at<=clock_timestamp())
                 and (owner_expires_at is null or owner_expires_at<=clock_timestamp()))) limit 1""",
            (run_id, blocked_stage),
        )
        if cur.fetchone():
            # A completion decision may be replayed after its recovery command
            # committed but before the next ordinal. Never complete over it.
            return {"status": "work_pending"}
        window = run.get("_bounded_window")
        # Validate the durable local window before loading the settled cohort.
        if store.execution_placement == "local":
            from trr_backend.ingestion.bounded_recovery import require_window

            window = require_window(cur, dict(run, run_id=run_id, account=account))
        if window:
            outstanding = _row(
                cur,
                """select count(*) as count from social.instagram_ingestion_targets
                where run_id=%s and stage='comments' and outcome='unresolved' and (
                    coalesce(checkpoint->'public_lane_restarts','{}'::jsonb)<>'{}'::jsonb
                    or checkpoint#>>'{public_recovery,command_key}' is not null)""",
                (run_id,),
            )
            if outstanding["count"] >= 4:
                # A blocked/cooling pass still occupies its root slot. Avoid
                # cohort decoding and audits when no new root can be admitted;
                # recover_public_target retains the authoritative locked check.
                return {"status": "recovery_admission_wait", "reason": "recovery_root_capacity_full"}
        # Retain offline audits for parity and exhausted targets, but do not let
        # that cohort hold every recoverable gap behind a full-run audit.
        cur.execute(
            """select target_key,outcome,checkpoint from social.instagram_ingestion_targets
            where run_id=%s and stage='comments' and outcome in ('committed','cached_satisfied')
            order by coalesce((checkpoint->'public_recovery'->>'pass')::int,0),target_key""",
            (run_id,),
        )
        rows = cur.fetchall()
        rows.sort(key=lambda row: not _can_recover_gap(row["checkpoint"], row["outcome"]))
        if window:
            rows = [row for row in rows if _can_recover_gap(row["checkpoint"], row["outcome"])]
            cur.execute(
                """select target_key,count(*) as pages from social.instagram_ingestion_comment_pages
                where run_id=%s group by target_key""",
                (run_id,),
            )
            sizes = {row["target_key"]: row["pages"] for row in cur.fetchall()}
            # Keep the existing fair order across the cohort. Counting every
            # post's inventory here can delay each completion decision; rank
            # only a bounded prefix and leave the remaining candidates eligible.
            rows = _prioritize_recovery_rows(cur, rows, sizes)
            cur.execute(
                """select result from social.instagram_ingestion_commands
                where account=%s and run_id=%s and result->>'window_id'=%s
                and result->>'command'='bounded_audit_deferred'""",
                (account, run_id, window["window_id"]),
            )
            deferred = {row["result"].get("summary_key") for row in cur.fetchall()}
            cur.execute(
                """select idempotency_key from social.instagram_ingestion_commands
                where account=%s and run_id=%s and idempotency_key=any(%s::text[])
                and result->'stream_complete'='true'::jsonb""",
                (account, run_id, list(deferred)),
            )
            # A committed summary wins over an uncertain child acknowledgment.
            deferred -= {row["idempotency_key"] for row in cur.fetchall()}
        from trr_backend.ingestion.retained_comment_recovery import (
            audit_and_replay_retained,
            canonical_inventory_fingerprints,
            parser_implementation_fingerprint,
            retained_audit_summary_key,
        )

        parser_hash = parser_implementation_fingerprint()
        canonical_hashes = {} if window else canonical_inventory_fingerprints(cur, [row["target_key"] for row in rows])
        chosen = []
        for row in rows:
            if window:
                canonical_hashes.update(canonical_inventory_fingerprints(cur, [row["target_key"]]))
            row["checkpoint_hash"] = fingerprint(row["checkpoint"])
            row["summary_key"] = retained_audit_summary_key(
                run_id=run_id,
                account=account,
                generation=generation,
                target_key=row["target_key"],
                checkpoint_hash=row["checkpoint_hash"],
                parser_hash=parser_hash,
                canonical_hash=canonical_hashes.get(row["target_key"]),
            )
            if not window or row["summary_key"] not in deferred:
                chosen.append(row)
                if window:
                    break
        rows = chosen
        # One combined lookup, rather than one roundtrip per settled target.
        cur.execute(
            """select idempotency_key,result from social.instagram_ingestion_commands
            where account=%s and run_id=%s and idempotency_key=any(%s::text[])""",
            (account, run_id, [row["summary_key"] for row in rows]),
        )
        summaries = {row["idempotency_key"]: row["result"] for row in cur.fetchall()}
    for row in rows:
        summary = summaries.get(row["summary_key"])
        if summary is None or summary.get("stream_complete") is not True:
            budget = {}
            if window:
                remaining = (datetime.fromisoformat(window["deadline_at"]) - datetime.now(UTC)).total_seconds()
                budget["deadline_monotonic"] = time.monotonic() + max(0, min(60, remaining))
            if window:
                from trr_backend.ingestion.retained_comment_recovery import bounded_retained_audit

                audit_operation = bounded_retained_audit
            else:
                audit_operation = audit_and_replay_retained
            audit = audit_operation(
                store,
                run_id=run_id,
                account=account,
                generation=generation,
                target_key=row["target_key"],
                expected_checkpoint_hash=row["checkpoint_hash"],
                **budget,
            )
            if audit.get("stream_complete") is not True:
                if window:
                    result = {
                        "command": "bounded_audit_deferred",
                        "status": "bounded_audit_deferred",
                        "window_id": window["window_id"],
                        "target_key": row["target_key"],
                        "checkpoint_hash": row["checkpoint_hash"],
                        "summary_key": row["summary_key"],
                        "reason": audit.get("reason", "incomplete_audit"),
                    }
                    with store.transaction() as conn, conn.cursor() as cur:
                        cur.execute(
                            """insert into social.instagram_ingestion_commands
                            (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)
                            on conflict do nothing""",
                            (
                                account,
                                "bounded-audit:" + fingerprint(result),
                                fingerprint(result),
                                run_id,
                                stable_json(result),
                            ),
                        )
                    return result
                # A database/local failure never authorizes a source retry.
                raise Conflict("Retained offline audit prerequisite is incomplete")
            return {
                "status": "retained_audit_completed",
                "target_key": row["target_key"],
                "checkpoint_sha256": row["checkpoint_hash"],
                "retained_audit_status": audit["status"],
                "retained_pages_audited": audit["pages_audited"],
                "retained_native_ids_recovered": len(audit["recovered_native_ids"]),
            }
        # Require this target's current durable audit before its next root.
        # Other targets' outstanding audits cannot prevent this recovery.
        if not _can_recover_gap(row["checkpoint"], row["outcome"]):
            continue
        request_hash = row["checkpoint_hash"]
        try:
            recover_public_target(
                store,
                run_id=run_id,
                account=account,
                generation=generation,
                target_key=row["target_key"],
                expected_checkpoint_hash=request_hash,
                reason="Public native traversal ended below retained expected total",
                source_revision=source_revision,
                initiated_by="january-worker-completion",
                idempotency_key=f"january-recovery:{run_id}:{request_hash}",
            )
        except RecoveryAdmissionDeferred as exc:
            return {"status": "recovery_admission_wait", "reason": exc.code}
        return {
            "status": "public_recovery_queued",
            "target_key": row["target_key"],
            "checkpoint_sha256": request_hash,
            "retained_audit_status": summary["status"],
            "retained_pages_audited": summary["pages_audited"],
            "retained_native_ids_recovered": len(summary["recovered_native_ids"]),
        }
    return None


def recover_public_target(
    store: Store,
    *,
    run_id: str,
    account: str,
    generation: int,
    target_key: str,
    expected_checkpoint_hash: str,
    lanes: tuple[str, ...] = ("__top__",),
    reason: str,
    source_revision: str,
    initiated_by: str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Reopen one idle target, preserving attempts and source evidence.

    A completed no-improvement recovery is counted only at its next explicit
    recovery request. Two such complete passes prohibit further root rescans.
    Ordinary timed attempts remain continuations, not recovery passes.
    """
    if not lanes or len(set(lanes)) != len(lanes) or ("__top__" in lanes and len(lanes) != 1):
        raise ValueError("Select either the top root or distinct child roots")
    if not all(
        isinstance(value, str) and 0 < len(value.strip()) <= 200
        for value in (target_key, reason, source_revision, initiated_by, idempotency_key, *lanes)
    ):
        raise ValueError("Recovery requires bounded identities, reason and operator")
    request = {
        "command": "recover_public_comments_v1",
        "run_id": run_id,
        "generation": generation,
        "target_key": target_key,
        "expected_checkpoint_hash": expected_checkpoint_hash,
        "lanes": list(lanes),
        "reason": reason,
        "source_revision": source_revision,
        "initiated_by": initiated_by,
    }
    command_hash = fingerprint(request)
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        previous = _replayed_recovery(cur, account, idempotency_key, command_hash)
        if previous is not None:
            return previous
        # A wrapper may hold the target needed by the current command owner.
        # The command gate must not wait either, even for the identical request.
        claimed = _row(
            cur,
            "select pg_try_advisory_xact_lock(hashtextextended(%s,0)) as claimed",
            (f"instagram-command:{account}:{idempotency_key}",),
        )
        if not claimed["claimed"]:
            raise RecoveryAdmissionDeferred("recovery_admission_busy")
        # Another owner could have committed between lookup and gate claim.
        previous = _replayed_recovery(cur, account, idempotency_key, command_hash)
        if previous is not None:
            return previous
        _claim_root_admission(cur, run_id, account)
        run = store._execution_run(cur, run_id, account, shared_fence=True)
        intent = RunIntent.from_payload(run["intent"])
        require_allowed_intent(intent)
        if (
            run["generation"] != generation
            or run["state"] not in {"running", "queued"}
            or intent.effective_source_profile.access_mode != "public"
            or run.get("execution_hold_reason")
        ):
            raise Conflict("Recovery requires the exact current unheld public run")
        target = _row(
            cur,
            """select *,clock_timestamp() as database_now from social.instagram_ingestion_targets
            where run_id=%s and stage='comments' and target_key=%s for update""",
            (run_id, target_key),
        )
        if not target:
            raise LookupError("Recovery comment target not found")
        now = target.pop("database_now")
        if target["owner_token"] and (
            target["owner_expires_at"] is None
            and target["outcome"] == "unresolved"
            or target["owner_expires_at"] is not None
            and target["owner_expires_at"] > now
        ):
            raise Conflict("Recovery target has an active or uncertain owner")
        if target.get("next_attempt_at") and target["next_attempt_at"] > now:
            raise Conflict("Recovery cannot bypass source backoff")
        if target.get("blocked_reason") not in (None, "cursor_stalled"):
            raise Conflict("Recovery cannot clear another source or identity block")
        checkpoint = target["checkpoint"]
        if fingerprint(checkpoint) != expected_checkpoint_hash:
            raise Conflict("Recovery evidence changed since review")
        state = checkpoint.get("comment_pages") or {}
        if state.get("adapter_family") != "instagram_public_relay" or state.get("adapter_version") != 1:
            raise Conflict("Recovery requires retained public Relay provenance")
        if checkpoint.get("public_lane_restarts"):
            raise Conflict("A root recovery is already pending")
        source_lanes = state.get("lanes") or {}
        if any(lane not in source_lanes for lane in lanes):
            raise Conflict("Recovery lane has no retained source proof")
        recovery = checkpoint.get("public_recovery") or {}
        settlement = state.get("settlement") or {}
        observed_ids = sorted((state.get("observed") or {}).keys())
        strength = {
            "observed_ids": observed_ids,
            "expected_total": _known_max(
                settlement.get("expected_total"),
                state.get("expected"),
                (recovery.get("baseline") or {}).get("expected_total"),
            ),
            "count_gap": settlement.get("count_gap"),
            "unsupported": sorted(
                key for key, value in (state.get("observed") or {}).items() if value.get("unsupported_reply_topology")
            ),
        }
        no_improvement = unchanged_passes(checkpoint, target["outcome"])
        if no_improvement >= 2:
            raise Conflict("Two complete recovery passes added no IDs or stronger completeness proof")
        # Seed a first recovery from its retained observations even when the
        # target was unresolved. Later incomplete passes do not advance it.
        progress = (
            _advance_progress(recovery, strength)
            if not recovery or target["outcome"] in {"committed", "cached_satisfied"}
            else _progress_frontier(recovery)
        )
        _check_root_capacity(cur, run_id, target_key)
        restarts = {
            lane: {
                "lane": lane,
                "previous_lane_hash": fingerprint(source_lanes[lane]),
                "previous_lane": source_lanes[lane],
                "reason": reason,
                "source_revision": source_revision,
                "initiated_by": initiated_by,
                "requested_at": now.isoformat(),
                "generation": generation,
            }
            for lane in lanes
        }
        resumable = dict(state)
        resumable.pop("settlement", None)  # Terminal proof stays in the journal/receipt.
        updated = {
            **checkpoint,
            "comment_pages": resumable,
            "public_lane_restarts": restarts,
            "public_recovery": {
                "version": 1,
                "command_key": idempotency_key,
                "pass": recovery.get("pass", 0) + 1,
                "baseline": strength,
                "progress": progress,
                "no_improvement_passes": no_improvement,
                "preserve_saved_rows": True,
            },
        }
        # Keep the prior terminal checkpoint and receipt references in the
        # journal, never inside every subsequent page checkpoint.
        cur.execute(
            """select receipt_id from social.instagram_ingestion_receipts
            where run_id=%s and stage='comments' and target_key=%s order by committed_at""",
            (run_id, target_key),
        )
        result = {
            **request,
            "at": now.isoformat(),
            "previous_checkpoint": checkpoint,
            "previous_outcome": target["outcome"],
            "previous_receipts": [str(row["receipt_id"]) for row in cur.fetchall()],
            "preserved_attempts": target["attempts"],
            "pass": updated["public_recovery"]["pass"],
        }
        cur.execute(
            """update social.instagram_ingestion_targets set checkpoint=%s::jsonb,outcome='unresolved',
            owner_token=null,owner_started_at=null,owner_expires_at=null,blocked_reason=null,
            max_attempts=greatest(max_attempts,attempts+12)
            where run_id=%s and stage='comments' and target_key=%s""",
            (stable_json(updated), run_id, target_key),
        )
        # Media must re-prove the inventory after the new comment traversal.
        # Preserve all receipts/checkpoints; no object bytes are removed.
        from trr_backend.ingestion.media import invalidate_media_inventory

        invalidate_media_inventory(cur, run_id, target_key)
        cur.execute(
            """insert into social.instagram_ingestion_commands(account,idempotency_key,command_hash,run_id,result)
            values(%s,%s,%s,%s,%s::jsonb)""",
            (account, idempotency_key, command_hash, run_id, stable_json(result)),
        )
        return result


def target_status(checkpoint: dict, outcome: str) -> dict:
    """Compact recovery ledger classification, not a completeness certificate."""
    state = checkpoint.get("comment_pages") or {}
    observed = state.get("observed") or {}
    settlement = state.get("settlement") or {}
    reasons = []
    if outcome == "unresolved" and checkpoint.get("last_error_code"):
        reasons.append("source_failure")
    if any(value.get("unsupported_reply_topology") for value in observed.values()):
        reasons.append("unsupported_reply_topology")
    if any(value.get("repeated") for value in (state.get("lanes") or {}).values()):
        reasons.append("cursor_stalled")
    if settlement.get("count_gap", 0):
        reasons.append("count_gap")
    expected = settlement.get("expected_total", state.get("expected"))
    if type(expected) is int and expected >= 0 and len(observed) > expected:
        reasons.append("count_overshoot")
    if outcome == "unresolved":
        reasons.append("pending_traversal")
    return {
        "outcome": outcome,
        "reasons": reasons,
        "observed": len(observed),
        "settlement": settlement,
        "candidate_complete": not reasons and outcome in {"committed", "cached_satisfied"},
        "final_acceptance": False,
    }
