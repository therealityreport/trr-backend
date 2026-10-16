"""Isolated fake Modal boundary. No production imports or network calls.

Fencing/idempotency are application SQL guarantees, not engine guarantees.
The checkpoint commits before blocking so the coordinator can kill a worker
at a reproducible point without pretending a Python exception is process loss.
"""

import os
import time

import psycopg


def connection():
    return psycopg.connect(os.environ["EVAL_DATABASE_URL"])


def initialize():
    with connection() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS eval_runs (
          run_id text PRIMARY KEY, owner_epoch int NOT NULL DEFAULT 1,
          cancelled boolean NOT NULL DEFAULT false, version text NOT NULL,
          cooldown_until timestamptz NOT NULL DEFAULT now());
        CREATE TABLE IF NOT EXISTS eval_targets (
          run_id text REFERENCES eval_runs, target int, attempts int NOT NULL DEFAULT 0,
          PRIMARY KEY(run_id,target));
        CREATE TABLE IF NOT EXISTS eval_dispatches (
          run_id text, target int, owner_epoch int, accepted_at timestamptz DEFAULT now(),
          PRIMARY KEY(run_id,target));
        CREATE TABLE IF NOT EXISTS eval_receipts (
          run_id text, target int, owner_epoch int, committed_at timestamptz DEFAULT now(),
          PRIMARY KEY(run_id,target));
        CREATE TABLE IF NOT EXISTS eval_events (
          run_id text, stage text, at timestamptz DEFAULT now(), PRIMARY KEY(run_id,stage));
        """)


def create_run(run_id, count, version="v1", cooldown=0):
    with connection() as conn:
        conn.execute(
            "INSERT INTO eval_runs(run_id,version,cooldown_until) VALUES (%s,%s,now()+%s*interval '1 second')",
            (run_id, version, cooldown),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO eval_targets(run_id,target) VALUES (%s,%s)", [(run_id, i) for i in range(count)]
            )


def event(run_id, stage):
    with connection() as conn:
        conn.execute("INSERT INTO eval_events(run_id,stage) VALUES (%s,%s) ON CONFLICT DO NOTHING", (run_id, stage))


def checkpoint(run_id, stage, requested):
    if requested == stage:
        event(run_id, stage)
        # Only the faulting first worker receives EVAL_INJECT=1.
        if os.environ.get("EVAL_INJECT") == "1":
            while True:
                time.sleep(0.1)


def state(run_id):
    with connection() as conn:
        return conn.execute(
            "SELECT owner_epoch,cancelled,version,GREATEST(0,extract(epoch FROM cooldown_until-now())) "
            "FROM eval_runs WHERE run_id=%s",
            (run_id,),
        ).fetchone()


def perform(run_id, target, epoch, fault):
    with connection() as conn:
        conn.execute("UPDATE eval_targets SET attempts=attempts+1 WHERE run_id=%s AND target=%s", (run_id, target))
    checkpoint(run_id, "before_dispatch", fault if target == 0 else "")
    with connection() as conn:
        owner, cancelled = conn.execute(
            "SELECT owner_epoch,cancelled FROM eval_runs WHERE run_id=%s FOR UPDATE", (run_id,)
        ).fetchone()
        if owner != epoch or cancelled:
            return "fenced"
        conn.execute(
            "INSERT INTO eval_dispatches(run_id,target,owner_epoch) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (run_id, target, epoch),
        )
    checkpoint(run_id, "after_dispatch", fault if target == 0 else "")
    with connection() as conn:
        owner, cancelled = conn.execute(
            "SELECT owner_epoch,cancelled FROM eval_runs WHERE run_id=%s FOR UPDATE", (run_id,)
        ).fetchone()
        if owner != epoch or cancelled:
            return "fenced"
        conn.execute(
            "INSERT INTO eval_receipts(run_id,target,owner_epoch) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (run_id, target, epoch),
        )
    checkpoint(run_id, "after_commit", fault if target == 0 else "")
    return "committed"


def snapshot(run_id):
    with connection() as conn:
        return {
            "targets": conn.execute("SELECT count(*) FROM eval_targets WHERE run_id=%s", (run_id,)).fetchone()[0],
            "dispatches": conn.execute("SELECT count(*) FROM eval_dispatches WHERE run_id=%s", (run_id,)).fetchone()[0],
            "receipts": conn.execute("SELECT count(*) FROM eval_receipts WHERE run_id=%s", (run_id,)).fetchone()[0],
            "attempts": conn.execute(
                "SELECT coalesce(sum(attempts),0) FROM eval_targets WHERE run_id=%s", (run_id,)
            ).fetchone()[0],
        }
