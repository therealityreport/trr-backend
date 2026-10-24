"""Audited public comment recovery without cancelling unrelated target owners.

Recovery preserves old checkpoints in the command journal and all immutable
receipts. It authorizes explicit root observations; it never counts an old row
as fresh coverage or deletes a canonical comment.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent, fingerprint, stable_json
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.ingestion.validation_gate import require_allowed_intent


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


def unchanged_passes(checkpoint, outcome):
    recovery = checkpoint.get("public_recovery") or {}
    count = recovery.get("no_improvement_passes", 0)
    if not recovery or outcome not in {"committed", "cached_satisfied"}:
        return count
    strength, baseline = _strength(checkpoint.get("comment_pages") or {}), recovery["baseline"]
    improved = bool(set(strength["observed_ids"]) - set(baseline["observed_ids"]))
    improved |= (
        type(strength["count_gap"]) is int
        and type(baseline["count_gap"]) is int
        and strength["count_gap"] < baseline["count_gap"]
    )
    improved |= bool(set(baseline["unsupported"]) - set(strength["unsupported"]))
    return 0 if improved else count + 1


def enqueue_next_recovery(store, *, run_id, account, generation, intent_hash, source_revision, blocked_stage=False):
    """One bounded control step at a drained January worker completion boundary.

    The worker remains the only execution owner. No timer, monitor, provider
    request or independent process is introduced by this function.
    """
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
        cur.execute(
            """select target_key,outcome,checkpoint from social.instagram_ingestion_targets
            where run_id=%s and stage='comments' and outcome in ('committed','cached_satisfied')
            and (checkpoint->'comment_pages'->'settlement'->>'count_gap')::int>0
            order by coalesce((checkpoint->'public_recovery'->>'pass')::int,0),target_key""",
            (run_id,),
        )
        rows = cur.fetchall()
    for row in rows:
        if unchanged_passes(row["checkpoint"], row["outcome"]) >= 2:
            continue
        request_hash = fingerprint(row["checkpoint"])
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
        return {"target_key": row["target_key"], "checkpoint_sha256": request_hash}
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
        cur.execute(
            "select pg_advisory_xact_lock(hashtextextended(%s,0))", (f"instagram-command:{account}:{idempotency_key}",)
        )
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
