"""Immutable phase journal for explicitly authorized useful-progress windows.

The operator requests comment draining. The worker must finish its dispatch
drain before requesting media; the database checks refuse remaining ownership
or ambiguous sends. This module never changes targets, runs, or dispatches.
"""

from __future__ import annotations

import re

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, LostOwnership, fingerprint, stable_json
from trr_backend.ingestion.persistence import _required_row, _row

_SUFFIX = {"draining_comments": ":drain-comments", "media": ":media"}
_PREVIOUS = {"draining_comments": "comments", "media": "draining_comments"}


def _binding(window):
    if not isinstance(window, dict) or (window.get("stop_policy") or {}).get("mode") != "useful_progress":
        raise Conflict("Phase transitions require a useful-progress recovery window")
    try:
        return {
            "run_id": str(window["run_id"]),
            "account": window["account"],
            "generation": window["generation"],
            "intent_hash": window["expected_intent_hash"],
            "window_id": window["window_id"],
            "source_revision": window["source_revision"],
        }
    except KeyError:
        raise Conflict("Recovery window phase binding is incomplete") from None


def _request(binding, to_phase, reason):
    return {"command": "transition_bounded_recovery_phase", **binding, "to_phase": to_phase, "reason": reason}


def phase(cur, window):
    """Read the exact window's phase without invoking window validation again."""
    # Historical fixed-duration windows retain their original scheduling policy.
    if (window.get("stop_policy") or {}).get("mode") != "useful_progress":
        return "comments"
    binding = _binding(window)
    keys = {binding["window_id"] + suffix: value for value, suffix in _SUFFIX.items()}
    cur.execute(
        """select idempotency_key,run_id,command_hash,result from social.instagram_ingestion_commands
        where account=%s and idempotency_key in (%s,%s)""",
        (binding["account"], *keys),
    )
    found = set()
    for raw in cur.fetchall():
        row = dict(raw)
        result = row["result"]
        target_phase = keys[row["idempotency_key"]]
        if (
            not isinstance(result, dict)
            or str(row["run_id"]) != binding["run_id"]
            or any(result.get(key) != value for key, value in binding.items())
            or result.get("command") != "transition_bounded_recovery_phase"
            or result.get("to_phase") != target_phase
            or result.get("phase") != target_phase
            or result.get("previous_phase") != _PREVIOUS[target_phase]
            or row["command_hash"] != fingerprint(_request(binding, target_phase, result.get("reason")))
        ):
            raise Conflict("Recovery phase journal does not match this window")
        found.add(target_phase)
    if "media" in found and "draining_comments" not in found:
        raise Conflict("Media phase has no comment drain receipt")
    return "media" if "media" in found else "draining_comments" if found else "comments"


def transition(store, *, run_id, account, generation, intent_hash, window_id, source_revision, to_phase, reason):
    """Journal a legal phase change, replaying only in the bound current window.

    Replays precede mutable phase/drain checks, while expiry, placement, source,
    intent and generation checks still apply. The run lock serializes this
    command against reservations. An existing receipt never reopens a phase.
    """
    if to_phase not in _SUFFIX:
        raise ValueError("Unsupported recovery phase transition")
    if not isinstance(reason, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", reason):
        raise ValueError("Recovery phase reason must be a bounded identifier")
    binding = {
        "run_id": str(run_id),
        "account": account,
        "generation": generation,
        "intent_hash": intent_hash,
        "window_id": window_id,
        "source_revision": source_revision,
    }
    command = _request(binding, to_phase, reason)
    command_hash = fingerprint(command)
    key = window_id + _SUFFIX[to_phase]
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"instagram-command:{account}:{window_id}:phase",),
        )
        previous = _row(
            cur,
            """select command_hash,result from social.instagram_ingestion_commands
            where account=%s and idempotency_key=%s""",
            (account, key),
        )
        if previous and previous["command_hash"] != command_hash:
            raise Conflict("Recovery phase command belongs to another request")
        run = store._execution_run(cur, run_id, account)
        window = run.get("_bounded_window")
        if (
            run["generation"] != generation
            or run["state"] not in {"queued", "running"}
            or run["intent_hash"] != intent_hash
            or _binding(window) != binding
            or window.get("expected_workflow_version") != run["workflow_version"]
        ):
            raise LostOwnership("Recovery phase execution binding changed")
        if previous:
            return previous["result"]
        current = phase(cur, window)
        if current != _PREVIOUS[to_phase]:
            raise Conflict("Recovery phase transition is out of order")
        if to_phase == "media":
            active = _row(
                cur,
                """select target_key from social.instagram_ingestion_targets
                where run_id=%s and outcome='unresolved'
                and (owner_expires_at>clock_timestamp()
                    or (owner_token is not null and owner_expires_at is null)) limit 1""",
                (run_id,),
            )
            uncertain = _row(
                cur,
                """select d.owner_token from social.instagram_ingestion_dispatches d
                join social.instagram_ingestion_targets t using(run_id,stage,target_key)
                where d.run_id=%s and d.generation=%s and t.outcome='unresolved'
                and d.state in ('sending','accepted')
                and not exists(select 1 from social.instagram_ingestion_receipts r
                    where r.run_id=d.run_id and r.stage=d.stage and r.target_key=d.target_key
                        and r.owner_token=d.owner_token) limit 1""",
                (run_id, generation),
            )
            if active or uncertain:
                raise Conflict("Comment dispatch drain still has a live or uncertain execution owner")
        result = {
            **command,
            "phase": to_phase,
            "previous_phase": current,
            "transitioned_at": _required_row(cur, "select clock_timestamp() as now")["now"].isoformat(),
        }
        cur.execute(
            """insert into social.instagram_ingestion_commands
            (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
            (account, key, command_hash, run_id, stable_json(result)),
        )
        return result
