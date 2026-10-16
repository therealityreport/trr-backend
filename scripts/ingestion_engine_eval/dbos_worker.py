"""Real DBOS workflow executor, launched and killed by run.py."""

import argparse
import json
import os
import time

import store
from dbos import DBOS, SetWorkflowID


@DBOS.step()
def dispatch(run_id, target, epoch, fault):
    return store.perform(run_id, target, epoch, fault)


@DBOS.step()
def read_state(run_id):
    # Persisted read fixes the initial cooldown for replay. Live cancellation
    # and ownership are also enforced transactionally by the adapter.
    epoch, cancelled, version, cooldown = store.state(run_id)
    return epoch, cancelled, version, float(cooldown)


@DBOS.workflow()
def ingest(run_id, count, fault):
    epoch, cancelled, version, cooldown = read_state(run_id)
    store.event(run_id, "timer_started")
    DBOS.sleep(cooldown)
    for target in range(count):
        dispatch(run_id, target, epoch, fault)
    return version


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["start", "recover", "cancel", "resume", "duplicate", "hold", "assert-cancelled"]
    )
    parser.add_argument("run_id")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--fault", default="")
    parser.add_argument("--version", default="v1")
    args = parser.parse_args()
    DBOS(
        config={
            "name": "trr-engine-evaluation",
            "application_version": args.version,
            "system_database_url": os.environ["EVAL_DBOS_URL"],
            "executor_id": "eval-local",
            "admin_server": False,
            "enable_otlp": False,
        }
    )
    DBOS.launch()
    try:
        if args.action == "assert-cancelled":
            status = DBOS.get_workflow_status(args.run_id)
            assert status is not None and status.status == "CANCELLED", status
            print(json.dumps({"terminal_engine_status": status.status}), flush=True)
            return
        if args.action in ("start", "duplicate"):
            with SetWorkflowID(args.run_id):
                handle = DBOS.start_workflow(ingest, args.run_id, args.count, args.fault)
        elif args.action == "cancel":
            DBOS.cancel_workflow(args.run_id)
            print(json.dumps({"cancelled": args.run_id}), flush=True)
            return
        elif args.action == "resume":
            handle = DBOS.resume_workflow(args.run_id)
        elif args.action == "hold":
            time.sleep(3)
            print(json.dumps({"held": args.version}), flush=True)
            return
        else:
            handle = DBOS.retrieve_workflow(args.run_id)
        result = handle.get_result()
        print(json.dumps({"result": result, "history_steps": len(DBOS.list_workflow_steps(args.run_id))}), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    main()
