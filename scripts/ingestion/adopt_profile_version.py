#!/usr/bin/env python3
"""Plan or explicitly adopt v2 for a cancelled local public BravoTV 2026 run.

No resume, worker launch, source request or credential repair occurs here.
Connection configuration comes from the same explicit environment as local_driver.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ingestion.local_driver import CoordinatorLease, LocalRequest, validate_configuration, validate_run
from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.version_adoption import (
    ADOPTED_WORKFLOW_VERSION,
    PREVIOUS_WORKFLOW_VERSION,
    _command_hash,
    adopt_profile_version,
)
from trr_backend.ingestion.workflows import RuntimeConfig


class _SafeParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse normally echoes rejected values. Arguments are nonsecret,
        # but an accidental credential must not become diagnostic output.
        raise ValueError("Invalid adoption arguments")


def run_adoption(request, *, idempotency_key, store, lease, abort):
    @contextmanager
    def guarded_transaction():
        with store.transaction() as conn:
            if abort.is_set():
                raise LostOwnership("Coordinator lease is unavailable")
            with conn.cursor() as cur:
                cur.execute("set local lock_timeout='5s'")
                cur.execute("set local statement_timeout='5s'")
            yield conn
            if abort.is_set():
                raise LostOwnership("Coordinator lease was lost before adoption commit")

    guarded = Store(guarded_transaction, execution_placement="local")
    with lease:
        if abort.is_set():
            raise LostOwnership("Coordinator lease is unavailable")
        # An exact journal replay is valid after the first call changed v1->v2.
        # Resolve it under the lease before applying mutable v1 prerequisites.
        previous = guarded.command_result(
            "bravotv",
            idempotency_key,
            _command_hash(
                run_id=request.run_id,
                account="bravotv",
                expected_generation=request.generation,
                expected_intent_hash=request.intent_hash,
            ),
        )
        if previous is not None:
            return previous
        validate_run(guarded, request, expected_workflow_version=PREVIOUS_WORKFLOW_VERSION)
        return adopt_profile_version(
            guarded,
            run_id=request.run_id,
            account="bravotv",
            expected_generation=request.generation,
            expected_intent_hash=request.intent_hash,
            idempotency_key=idempotency_key,
        )


def main(argv=None):
    parser = _SafeParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--intent-hash", required=True)
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--apply", action="store_true")
    apply = False
    handlers = {}
    try:
        options = parser.parse_args(argv)
        apply = options.apply
        request = LocalRequest(options.run_id, options.intent_hash, options.generation, "local")
        if not 1 <= len(options.idempotency_key) <= 200:
            raise ValueError("Bounded idempotency key required")
        if not apply:
            print(
                json.dumps(
                    {
                        "status": "plan",
                        "run_id": request.run_id,
                        "account": "bravotv",
                        "generation": request.generation,
                        "intent_hash": request.intent_hash,
                        "execution_placement": "local",
                        "previous_workflow_version": PREVIOUS_WORKFLOW_VERSION,
                        "workflow_version": ADOPTED_WORKFLOW_VERSION,
                        "resume": False,
                    }
                )
            )
            return 0
        import psycopg

        dsn = os.environ["TRR_INSTAGRAM_DBOS_DATABASE_URL"].strip()
        role = os.environ["TRR_INSTAGRAM_DBOS_RUNTIME_ROLE"].strip()
        validate_configuration(dsn)
        abort = threading.Event()
        config = RuntimeConfig(dsn, ADOPTED_WORKFLOW_VERSION, runtime_role=role)
        lease = CoordinatorLease(lambda: psycopg.connect(dsn, autocommit=True, connect_timeout=5), config, abort)
        for sig in (signal.SIGTERM, signal.SIGINT):
            handlers[sig] = signal.signal(sig, lambda *_: abort.set())
        result = run_adoption(
            request,
            idempotency_key=options.idempotency_key,
            store=Store(execution_placement="local"),
            lease=lease,
            abort=abort,
        )
        print(json.dumps({"status": "adopted", **result, "resume": False}))
        return 0
    except Exception as error:
        print(json.dumps({"status": "failed", "failure_class": type(error).__name__}))
        return 1
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        if apply:
            from trr_backend.db import pg

            pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
