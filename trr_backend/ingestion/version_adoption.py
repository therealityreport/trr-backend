"""Explicit adoption of unsealed public work; never resume or rewrite intent."""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent, fingerprint, stable_json
from trr_backend.ingestion.persistence import Store, _row

PREVIOUS_WORKFLOW_VERSION = "instagram-ingestion-v1"
ADOPTED_WORKFLOW_VERSION = "instagram-ingestion-v2"


def _command_hash(*, run_id, account, expected_generation, expected_intent_hash):
    return fingerprint(
        {
            "command": "adopt_profile_version",
            "run_id": str(UUID(str(run_id))),
            "account": account,
            "expected_generation": expected_generation,
            "expected_intent_hash": expected_intent_hash,
            "workflow_version": ADOPTED_WORKFLOW_VERSION,
        }
    )


def adopt_profile_version(
    store: Store,
    *,
    run_id: str,
    account: str,
    expected_generation: int,
    expected_intent_hash: str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Change only the run version and journal the guarded operator command.

    The operator owns the shared coordinator lease. Drained old dispatches may
    expire in this same transaction, using the existing placement proof. No
    manifests, observations, generations or source identities are converted.
    """
    run_id = str(UUID(str(run_id)))
    if account != RunIntent(account=account).account:
        raise ValueError("Exact canonical account required")
    if type(expected_generation) is not int or expected_generation < 1:
        raise ValueError("Exact positive generation required")
    if not isinstance(expected_intent_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_intent_hash):
        raise ValueError("Exact frozen intent hash required")
    if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 200:
        raise ValueError("Bounded idempotency key required")
    command_hash = _command_hash(
        run_id=run_id,
        account=account,
        expected_generation=expected_generation,
        expected_intent_hash=expected_intent_hash,
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
                raise Conflict("Idempotency key already belongs to a different command")
            return previous["result"]
        run = store._run(cur, run_id, account)
        intent = RunIntent.from_payload(run["intent"])
        if (
            run["state"] != "cancelled"
            or run["generation"] != expected_generation
            or run["intent_hash"] != expected_intent_hash
            or intent.digest != expected_intent_hash
            or intent.account != account
            or intent.effective_source_profile.access_mode != "public"
            or run.get("execution_placement", "modal") != "local"
            or run.get("execution_hold_reason")
            or run["engine"] != "dbos"
            or run["workflow_version"] != PREVIOUS_WORKFLOW_VERSION
        ):
            raise Conflict("Adoption requires the exact unheld cancelled public local v1 run")
        media = _row(
            cur,
            """select discovery_complete,sealed_at from social.instagram_ingestion_manifests
            where run_id=%s and stage='media' for update""",
            (run_id,),
        )
        if not media or media["discovery_complete"] or media["sealed_at"] is not None:
            raise Conflict("Adoption requires an existing unsealed media manifest")
        store._expire_drained_dispatches(cur, run, expected_generation)
        cur.execute(
            "update social.instagram_ingestion_runs set workflow_version=%s where run_id=%s",
            (ADOPTED_WORKFLOW_VERSION, run_id),
        )
        result = {
            "run_id": run_id,
            "account": account,
            "state": "cancelled",
            "generation": expected_generation,
            "intent_hash": expected_intent_hash,
            "execution_placement": "local",
            "previous_workflow_version": PREVIOUS_WORKFLOW_VERSION,
            "workflow_version": ADOPTED_WORKFLOW_VERSION,
        }
        cur.execute(
            """insert into social.instagram_ingestion_commands
            (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
            (account, idempotency_key, command_hash, run_id, stable_json(result)),
        )
        return result
