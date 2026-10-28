#!/usr/bin/env python3
"""Review January public comment recovery; apply only an explicit idle target.

Credentials come from the maintained runtime environment. No authentication,
proxy, source requests, worker starts, cancellation or global progress resets.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.comment_recovery import recover_public_target, target_status
from trr_backend.ingestion.domain import Conflict, RunIntent, fingerprint
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.validation_gate import require_allowed_intent


def ledger(store, run_id, account, generation, intent_hash):
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.instagram_ingestion_runs where run_id=%s and account=%s", (run_id, account))
        run = cur.fetchone()
        if not run:
            raise LookupError("Run not found")
        store.check_placement(run)
        intent = RunIntent.from_payload(run["intent"])
        require_allowed_intent(intent)
        if (
            run["generation"] != generation
            or run["intent_hash"] != intent_hash
            or intent.digest != intent_hash
            or intent.effective_source_profile.access_mode != "public"
            or intent.date_start != datetime(2026, 1, 1, tzinfo=UTC)
            or intent.date_end != datetime(2026, 2, 1, tzinfo=UTC)
        ):
            raise Conflict("Exact frozen public January intent required")
        cur.execute(
            """select target_key,outcome,checkpoint,attempts,max_attempts,blocked_reason,
            owner_token,owner_started_at,owner_expires_at,next_attempt_at,clock_timestamp() as now
            from social.instagram_ingestion_targets where run_id=%s and stage='comments' order by target_key""",
            (run_id,),
        )
        targets = []
        for row in cur.fetchall():
            status = target_status(row["checkpoint"], row["outcome"])
            active = bool(row["owner_token"]) and (
                row["owner_expires_at"] is not None
                and row["owner_expires_at"] > row["now"]
                or row["owner_expires_at"] is None
                and row["outcome"] == "unresolved"
            )
            targets.append(
                {
                    **status,
                    "target_key": row["target_key"],
                    "checkpoint_sha256": fingerprint(row["checkpoint"]),
                    "attempts": row["attempts"],
                    "max_attempts": row["max_attempts"],
                    "active_or_uncertain_owner": active,
                    "blocked_reason": row["blocked_reason"],
                    "next_attempt_at": row["next_attempt_at"],
                    "recovery": row["checkpoint"].get("public_recovery"),
                }
            )
        return {
            "version": 1,
            "run_id": run_id,
            "intent_hash": intent_hash,
            "generation": generation,
            "observed_at": datetime.now(UTC).isoformat(),
            "targets": targets,
            "final_acceptance": False,
        }


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id", required=True, type=UUID)
    p.add_argument("--account", default="bravotv")
    p.add_argument("--generation", type=int, required=True)
    p.add_argument("--intent-hash", required=True)
    p.add_argument("--placement", required=True, choices=["local", "modal"])
    p.add_argument("--target-key")
    p.add_argument("--checkpoint-sha256")
    p.add_argument("--lane", action="append", default=[])
    p.add_argument("--reason")
    p.add_argument("--source-revision")
    p.add_argument("--initiated-by", default="january-public-recovery")
    p.add_argument("--idempotency-key")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--output", type=Path)
    args = p.parse_args(argv)
    store = Store(execution_placement=args.placement)
    result = ledger(store, str(args.run_id), args.account, args.generation, args.intent_hash)
    if args.target_key:
        selected = [t for t in result["targets"] if t["target_key"] == args.target_key]
        if not selected:
            raise LookupError("Target absent from frozen inventory")
        result = {**result, "targets": selected}
    if args.apply:
        if not all([args.target_key, args.checkpoint_sha256, args.reason, args.source_revision, args.idempotency_key]):
            p.error("--apply requires target, checkpoint SHA, reason, source revision and idempotency key")
        result = recover_public_target(
            store,
            run_id=str(args.run_id),
            account=args.account,
            generation=args.generation,
            target_key=args.target_key,
            expected_checkpoint_hash=args.checkpoint_sha256,
            lanes=tuple(args.lane or ["__top__"]),
            reason=args.reason,
            source_revision=args.source_revision,
            initiated_by=args.initiated_by,
            idempotency_key=args.idempotency_key,
        )
        # Full previous checkpoint is retained privately in the command journal.
        result = {k: v for k, v in result.items() if k != "previous_checkpoint"}
    text = json.dumps(result, default=str, indent=2)
    if args.output:
        with args.output.open("x") as f:
            f.write(text + "\n")
        args.output.chmod(0o600)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
