"""Native Temporal deployment pinning probe, separate from queue-version guard.

Stops v1 while a workflow waits for a signal, promotes v2 on the SAME queue,
then signals the old workflow. It must wait for v1 and finish on v1's activity.
"""

import argparse
import asyncio
import json
from datetime import timedelta
from pathlib import Path

from temporalio import activity, workflow
from temporalio.api.workflowservice.v1 import SetWorkerDeploymentCurrentVersionRequest
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, WorkerDeploymentConfig

ACTIVE_VERSION = ""


@activity.defn
async def build() -> str:
    return ACTIVE_VERSION


@workflow.defn
class Pinned:
    def __init__(self):
        self.released = False

    @workflow.signal
    def release(self):
        self.released = True

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.released)
        return await workflow.execute_activity(build, start_to_close_timeout=timedelta(seconds=5))


def worker(client, version):
    return Worker(
        client,
        task_queue="native-pinning",
        workflows=[Pinned],
        activities=[build],
        deployment_config=WorkerDeploymentConfig(
            version=WorkerDeploymentVersion("trr-eval-pinning", version),
            use_worker_versioning=True,
            default_versioning_behavior=VersioningBehavior.PINNED,
        ),
    )


async def promote(client, version):
    await asyncio.sleep(0.5)
    return await client.workflow_service.set_worker_deployment_current_version(
        SetWorkerDeploymentCurrentVersionRequest(
            namespace="default", deployment_name="trr-eval-pinning", build_id=version, identity="isolated-eval"
        )
    )


async def main(root):
    global ACTIVE_VERSION
    root.mkdir(parents=True, exist_ok=False)
    (root / "downloads").mkdir()
    env = await WorkflowEnvironment.start_local(
        download_dest_dir=str(root / "downloads"), dev_server_database_filename=str(root / "temporal.db"), ui=False
    )
    try:
        client = env.client
        ACTIVE_VERSION = "v1"
        async with worker(client, "v1"):
            await promote(client, "v1")
            handle = await client.start_workflow(Pinned.run, id="native-pinned-run", task_queue="native-pinning")
            # Wait for the initial task to complete; that pins the execution.
            deadline = asyncio.get_running_loop().time() + 10
            while True:
                history = await handle.fetch_history()
                if any(e.HasField("workflow_task_completed_event_attributes") for e in history.events):
                    break
                assert asyncio.get_running_loop().time() < deadline, "v1 workflow did not start"
                await asyncio.sleep(0.1)
        ACTIVE_VERSION = "v2"
        async with worker(client, "v2"):
            await promote(client, "v2")
            await handle.signal(Pinned.release)
            await asyncio.sleep(2)
            description = await handle.describe()
            assert description.status.name == "RUNNING", description.status
            held = str(description.raw_description.workflow_execution_info.versioning_info)
        ACTIVE_VERSION = "v1"
        async with worker(client, "v1"):
            result = await asyncio.wait_for(handle.result(), 20)
            assert result == "v1", result
        evidence = {
            "status": "PASS",
            "same_task_queue": True,
            "promoted_current_version": "v2",
            "old_execution_held_without_v1": True,
            "recovered_activity_version": result,
            "versioning_info": held,
        }
        (root / "result.json").write_text(json.dumps(evidence, indent=2))
        print(json.dumps(evidence), flush=True)
    finally:
        await env.shutdown()
        (root / "cleanup.json").write_text(json.dumps({"temporal_shutdown_called": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", required=True, type=Path)
    asyncio.run(main(parser.parse_args().runtime_dir))
