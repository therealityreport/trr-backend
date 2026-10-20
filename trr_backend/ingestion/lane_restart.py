"""Explicit operator authorization to restart one stalled public comment lane."""

from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent, fingerprint, stable_json
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.ingestion.validation_gate import require_allowed_intent


def pending_restarts(checkpoint: dict[str, Any]) -> dict[str, Any]:
    pending = checkpoint.get("public_lane_restarts") or {}
    lanes = (checkpoint.get("comment_pages") or {}).get("lanes") or {}
    if not isinstance(pending, dict):
        raise Conflict("Invalid public lane restart authorization")
    for lane, record in pending.items():
        if (
            not isinstance(record, dict)
            or not record.get("reason")
            or not record.get("initiated_by")
            or not record.get("source_revision")
            or record.get("previous_lane_hash") != fingerprint(lanes.get(lane))
        ):
            raise Conflict("Public lane restart no longer matches its retained cursor")
    return pending


def restart_lane(
    store: Store,
    *,
    run_id: str,
    account: str,
    generation: int,
    target_key: str,
    lane: str,
    reason: str,
    source_revision: str,
    initiated_by: str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Authorize a root request without deleting evidence or starting execution.

    Requires a cancelled run with no active owners. Ordinary resume cannot clear
    a stalled lane. This command preserves attempts and grants one more attempt.
    """
    if not all(
        isinstance(x, str) and 0 < len(x.strip()) <= 200
        for x in (target_key, lane, reason, source_revision, initiated_by, idempotency_key)
    ):
        raise ValueError("Restart requires bounded explicit identity, reason, source revision and operator")
    command_hash = fingerprint(
        {
            "command": "restart_public_comment_lane",
            "run_id": run_id,
            "generation": generation,
            "target_key": target_key,
            "lane": lane,
            "reason": reason,
            "source_revision": source_revision,
            "initiated_by": initiated_by,
        }
    )
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"instagram-command:{account}:{idempotency_key}",),
        )
        previous = _row(
            cur,
            """select command_hash,result from social.instagram_ingestion_commands
            where account=%s and idempotency_key=%s""",
            (account, idempotency_key),
        )
        if previous:
            if previous["command_hash"] != command_hash:
                raise Conflict("Idempotency key belongs to another command")
            return previous["result"]
        run = store._run(cur, run_id, account)
        intent = RunIntent.from_payload(run["intent"])
        require_allowed_intent(intent)
        store.check_placement(run)
        if (
            run["state"] != "cancelled"
            or run["generation"] != generation
            or intent.source_profile is None
            or intent.source_profile.access_mode != "public"
            or run.get("execution_hold_reason")
        ):
            raise Conflict("Restart requires the exact cancelled public run generation without an execution hold")
        active = _row(
            cur,
            """select target_key from social.instagram_ingestion_targets where run_id=%s
            and (owner_expires_at>clock_timestamp() or
            (owner_token is not null and owner_expires_at is null)) limit 1""",
            (run_id,),
        )
        if active:
            raise Conflict("Run still has an active or uncertain owner")
        target = _row(
            cur,
            """select * from social.instagram_ingestion_targets
            where run_id=%s and stage='comments' and target_key=%s for update""",
            (run_id, target_key),
        )
        if not target or target["outcome"] != "unresolved":
            raise Conflict("Restart requires an unresolved comment target")
        checkpoint = dict(target["checkpoint"])
        state = checkpoint.get("comment_pages") or {}
        previous_lane = (state.get("lanes") or {}).get(lane)
        if state.get("adapter_family") != "instagram_public_relay" or not previous_lane:
            raise Conflict("Restart requires a retained public Relay lane")
        if not previous_lane.get("repeated"):
            raise Conflict("Only a classified stalled lane may be restarted")
        if target.get("blocked_reason") not in (None, "cursor_stalled"):
            raise Conflict("Restart cannot clear another block")
        pending = dict(pending_restarts(checkpoint))
        if lane in pending:
            raise Conflict("This lane already has a pending root restart")
        record = {
            "lane": lane,
            "previous_lane_hash": fingerprint(previous_lane),
            "previous_lane": previous_lane,
            "reason": reason,
            "source_revision": source_revision,
            "initiated_by": initiated_by,
            "requested_at": datetime.now(UTC).isoformat(),
            "generation": generation,
        }
        pending[lane] = record
        checkpoint["public_lane_restarts"] = pending
        cur.execute(
            """update social.instagram_ingestion_targets set checkpoint=%s::jsonb,
            blocked_reason=null,max_attempts=greatest(max_attempts,attempts+1)
            where run_id=%s and stage='comments' and target_key=%s""",
            (stable_json(checkpoint), run_id, target_key),
        )
        result = {
            "run_id": run_id,
            "generation": generation,
            "state": "cancelled",
            "target_key": target_key,
            "restart": record,
        }
        cur.execute(
            """insert into social.instagram_ingestion_commands
            (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
            (account, idempotency_key, command_hash, run_id, stable_json(result)),
        )
        return result
