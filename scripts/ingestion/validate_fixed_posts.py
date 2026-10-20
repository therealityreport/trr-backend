#!/usr/bin/env python3
"""Exact-manifest public validation. Mutations/source execution require --apply.

Use one shared private --budget-path for all diagnostic probes and acceptance.
No credentials, source contents, or connection strings are written to stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from trr_backend.ingestion.domain import Conflict
from trr_backend.ingestion.fixed_validation import AccountBudget, FixedManifest, inspect_run, run_tranche, start_run


def _save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(json.dumps(value, default=str, sort_keys=True))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _control(store, manifest, state, action, path):
    pending = state.get("pending_control")
    if pending is None:
        pending = {
            "action": action,
            "generation": state["generation"],
            "key": f"fixed-{action}:{state['run_id']}:{state['generation']}",
        }
        state["pending_control"] = pending
        _save(path, state)
    if pending["action"] != action:
        raise Conflict("Resolve the persisted control command before another action")
    result = store.control(
        state["run_id"],
        manifest.account,
        action=action,
        expected_generation=pending["generation"],
        expected_placement="local",
        idempotency_key=pending["key"],
    )
    state["generation"] = result["generation"]
    state.pop("pending_control")
    _save(path, state)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "inspect", "cancel", "resume", "restart-lane"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--budget-path", type=Path, required=True)
    parser.add_argument("--interrupt-after", choices=("parent", "child"))
    parser.add_argument("--target-key")
    parser.add_argument("--lane")
    parser.add_argument("--reason")
    parser.add_argument("--initiated-by")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--apply", action="store_true")
    options = parser.parse_args(argv)
    try:
        manifest = FixedManifest.from_payload(json.loads(options.manifest.read_text()))
        if options.action == "restart-lane" and not all(
            (options.target_key, options.lane, options.reason, options.initiated_by, options.idempotency_key)
        ):
            raise ValueError("Restart-lane requires target-key, lane, reason, initiated-by and idempotency-key")
        if not options.apply and options.action != "inspect":
            print(
                json.dumps(
                    {
                        "status": "plan",
                        "action": options.action,
                        "manifest_hash": manifest.digest,
                        "intent_hash": manifest.intent().digest,
                        "posts": [p.shortcode for p in manifest.posts],
                        "source_profile": "public",
                        "scope": manifest.intent().scope,
                    }
                )
            )
            return 0

        # Import only after explicit execution/read request; construction alone
        # never selects credentials, makes connections, or dispatches source work.
        from scripts.ingestion.local_driver import _project_dsn
        from trr_backend.db.connection import resolve_database_url_candidate_details
        from trr_backend.ingestion import composition
        from trr_backend.ingestion.validation_gate import ValidationWorkflowStore

        candidates = resolve_database_url_candidate_details()
        if not candidates or any(not _project_dsn(candidate["url"]) for candidate in candidates):
            raise Conflict("All business database candidates must target the approved TRR project")
        store = ValidationWorkflowStore(execution_placement="local")
        budget = AccountBudget(options.budget_path, manifest.account)
        state = json.loads(options.state.read_text()) if options.state.exists() else None
        if state and (state["manifest_hash"] != manifest.digest or state["intent_hash"] != manifest.intent().digest):
            raise Conflict("Private state differs from frozen manifest/intent")
        if options.action == "start":
            if state is None:
                state = {
                    "manifest_hash": manifest.digest,
                    "intent_hash": manifest.intent().digest,
                    "manifest": manifest.payload(),
                    "start_key": "fixed:" + uuid4().hex,
                }
                _save(options.state, state)  # Preserve the command key before an ambiguous response.
            result = start_run(
                store, manifest, workflow_version=composition.WORKFLOW_VERSION, idempotency_key=state["start_key"]
            )
            state["run_id"] = result["run_id"]
            state["generation"] = result["generation"]
            _save(options.state, state)
        elif state is None or not state.get("run_id"):
            raise Conflict("A persisted run identity is required")
        status = inspect_run(store, manifest, state["run_id"])
        if options.action == "restart-lane":
            from trr_backend.ingestion.lane_restart import restart_lane

            if options.target_key not in {post.shortcode for post in manifest.posts}:
                raise Conflict("Lane restart target is outside the frozen selection")
            restart_lane(
                store,
                run_id=state["run_id"],
                account=manifest.account,
                generation=status["run"]["generation"],
                target_key=options.target_key,
                lane=options.lane,
                reason=options.reason,
                source_revision=manifest.source_revision,
                initiated_by=options.initiated_by,
                idempotency_key=options.idempotency_key,
            )
        if options.action in {"cancel", "resume"}:
            current = status["run"]
            if not state.get("pending_control") and state["generation"] != current["generation"]:
                raise Conflict("Run generation changed; inspect before another control command")
            if not (
                options.action == "resume"
                and current["state"] in {"queued", "running"}
                and not state.get("pending_control")
            ):
                _control(store, manifest, state, options.action, options.state)
        if options.action in {"start", "resume"}:

            def worker_factory(workflow_store):
                worker = composition.build_worker(execution_placement="local")
                worker.store = workflow_store
                return worker

            from trr_backend.ingestion.admission import Admission

            execution = run_tranche(
                store,
                manifest,
                state["run_id"],
                budget=budget,
                worker_factory=worker_factory,
                interrupt_after=options.interrupt_after,
                admit=Admission().admit,
            )
            state["last_tranche"] = {"reason": execution["reason"], "outcomes": execution["outcomes"]}
        status = inspect_run(store, manifest, state["run_id"])
        state["generation"] = status["run"]["generation"]
        state["validation"] = status["validation"]
        _save(options.state, state)
        # Full checkpoints and source evidence stay in PostgreSQL/private state.
        print(
            json.dumps(
                {
                    "status": status["validation"]["status"],
                    "workflow_state": status["run"]["state"],
                    "validation": status["validation"],
                    "run_id": state["run_id"],
                    "generation": state["generation"],
                    "manifest_hash": manifest.digest,
                    "intent_hash": manifest.intent().digest,
                    "last_tranche": state.get("last_tranche"),
                    "targets": [
                        {
                            key: row[key]
                            for key in (
                                "stage",
                                "target_key",
                                "outcome",
                                "attempts",
                                "requests",
                                "next_attempt_at",
                                "blocked_reason",
                            )
                        }
                        for row in status["targets"]
                    ],
                    "budget": budget.inspect(),
                },
                default=str,
            )
        )
        return 0 if status["validation"]["passed"] else 2
    except Exception as error:
        print(json.dumps({"status": "failed", "failure_class": type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
