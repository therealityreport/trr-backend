"""Real Temporal workflow and activity executor, same store as DBOS."""

import argparse
import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    import store


@activity.defn
def dispatch(args: list) -> str:
    return store.perform(*args)


@activity.defn
def read_state(run_id: str) -> list:
    epoch, cancelled, version, cooldown = store.state(run_id)
    return [epoch, cancelled, version, float(cooldown)]


@workflow.defn
class Ingest:
    @workflow.run
    async def run(self, args: list) -> str:
        run_id, count, fault = args
        epoch, cancelled, version, cooldown = await workflow.execute_activity(
            read_state, run_id, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.execute_activity(mark_timer, run_id, start_to_close_timeout=timedelta(seconds=10))
        await asyncio.sleep(cooldown)
        for target in range(count):
            await workflow.execute_activity(
                dispatch,
                [run_id, target, epoch, fault],
                start_to_close_timeout=timedelta(seconds=4),
                retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100), maximum_attempts=8),
            )
        return version


@activity.defn
def mark_timer(run_id: str) -> None:
    store.event(run_id, "timer_started")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["start", "recover", "cancel", "resume", "duplicate", "hold", "assert-cancelled"]
    )
    parser.add_argument("run_id")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--fault", default="")
    parser.add_argument("--version", default="v1")
    args = parser.parse_args()
    client = await Client.connect(os.environ["EVAL_TEMPORAL_ADDRESS"])
    if args.action == "assert-cancelled":
        status = (await client.get_workflow_handle(args.run_id).describe()).status
        assert status == WorkflowExecutionStatus.CANCELED, status
        print(json.dumps({"terminal_engine_status": status.name}), flush=True)
        return
    if args.action == "cancel":
        await client.get_workflow_handle(args.run_id).cancel()
        print(json.dumps({"cancel_requested": args.run_id}), flush=True)
        return
    if args.action == "resume":
        old = client.get_workflow_handle(args.run_id)
        history = await old.fetch_history()
        boundary = next(
            e.event_id for e in history.events if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
        )
        await client.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace="default",
                workflow_execution=WorkflowExecution(workflow_id=args.run_id),
                reason="isolated cancellation/resume evaluation",
                workflow_task_finish_event_id=boundary,
                request_id=args.run_id + "-reset",
            )
        )
    # Explicit version-specific queues are the baseline routing guard. This does
    # not claim Temporal deployment-version pinning has been evaluated.
    async with Worker(
        client,
        task_queue="trr-eval-" + args.version,
        workflows=[Ingest],
        activities=[dispatch, read_state, mark_timer],
        activity_executor=ThreadPoolExecutor(max_workers=2),
        graceful_shutdown_timeout=timedelta(seconds=1),
    ):
        if args.action == "hold":
            await asyncio.sleep(3)
            return
        if args.action in ("start", "duplicate"):
            try:
                handle = await client.start_workflow(
                    Ingest.run,
                    [args.run_id, args.count, args.fault],
                    id=args.run_id,
                    task_queue="trr-eval-" + args.version,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                )
            except WorkflowAlreadyStartedError:
                handle = client.get_workflow_handle(args.run_id)
        else:
            handle = client.get_workflow_handle(args.run_id)
        result = await handle.result()
        history = await handle.fetch_history()
        print(
            json.dumps(
                {
                    "result": result,
                    "history_events": len(history.events),
                    "history_protobuf_bytes": sum(e.ByteSize() for e in history.events),
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
