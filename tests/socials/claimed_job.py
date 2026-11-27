from contextlib import nullcontext
from typing import Any


class ClaimedJob:
    """A claimed job row and its function-local persistence calls for offline runner tests."""

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.job_id = "job-1"
        self.claim_token = "fixture-claim"
        self.finished: dict[str, Any] = {}

    def row(self, config: dict[str, Any], **fields: Any) -> dict[str, Any]:
        return {
            "id": self.job_id,
            "run_id": "run-1",
            "status": "running",
            "worker_id": self.worker_id,
            "config": {**config, "claim_token": self.claim_token},
            **fields,
        }

    def write_scope(self, job_id: str, *, worker_id: str, claim_token: str) -> nullcontext[bool]:
        assert (job_id, worker_id, claim_token) == (self.job_id, self.worker_id, self.claim_token)
        return nullcontext(True)

    def finish(self, job_id: str, *, expected_worker_id: str, expected_claim_token: str, **fields: Any) -> bool:
        assert (job_id, expected_worker_id, expected_claim_token) == (self.job_id, self.worker_id, self.claim_token)
        self.finished.update(id=job_id, **fields)
        return True

    def finish_followups(self, job_id: str, *, worker_id: str, claim_token: str, status: str, **_fields: Any) -> None:
        assert (job_id, worker_id, claim_token) == (self.job_id, self.worker_id, self.claim_token)
        assert self.finished["status"] == status
