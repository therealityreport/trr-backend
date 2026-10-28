"""Temporary release gate while fixed-post public retrieval is unproven.

Routing ownership stays with v2. This gate does not disable capabilities or
return control to a legacy dispatcher. Removing it requires a reviewed release.
"""

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.workflow_store import WorkflowStore


def require_allowed_intent(intent: RunIntent) -> None:
    if intent.account == "bravotv" and (intent.scope == "full_history" or intent.action == "resume_tail"):
        raise Conflict("Bravo full history is held pending fixed-post retrieval acceptance")


class _ValidationGate(Store):
    def start(self, intent, **kwargs):
        require_allowed_intent(intent)
        return super().start(intent, **kwargs)

    def _execution_run(self, cur, run_id, account, *, shared_fence=False):
        run = super()._execution_run(cur, run_id, account, shared_fence=shared_fence)
        require_allowed_intent(RunIntent.from_payload(run["intent"]))
        return run

    def control(self, run_id, account, *, action, **kwargs):
        if action in {"resume", "retry_targets"}:
            with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                run = self._run(cur, run_id, account)
                require_allowed_intent(RunIntent.from_payload(run["intent"]))
        return super().control(run_id, account, action=action, **kwargs)


class ValidationStore(_ValidationGate, Store):
    pass


class ValidationWorkflowStore(_ValidationGate, WorkflowStore):
    pass
