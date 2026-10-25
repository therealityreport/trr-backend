"""Owner-bound context for a received public reply, never shared mutable state."""

import time
from contextvars import ContextVar

from trr_backend.ingestion.identity_policy import RequestPermit

request_permit: ContextVar[RequestPermit | None] = ContextVar("instagram_reply_request_permit", default=None)


# Set only by the private worker entrypoint. Monotonic clocks share one kernel
# across its parent and child; response observation still uses its original UTC.
execution_deadline: ContextVar[float | None] = ContextVar("instagram_activity_execution_deadline", default=None)
EXECUTION_CLEANUP_RESERVE_SECONDS = 20.0


def source_deadline(owner_deadline: float | None) -> float | None:
    hard = execution_deadline.get()
    if hard is None:
        return owner_deadline
    soft = hard - EXECUTION_CLEANUP_RESERVE_SECONDS
    return soft if owner_deadline is None else min(owner_deadline, soft)


def check_execution_budget() -> None:
    soft = source_deadline(None)
    if soft is not None and time.monotonic() >= soft:
        from trr_backend.ingestion.domain import Failure
        from trr_backend.ingestion.post_transport import SourceFailure

        raise SourceFailure(Failure("target_deadline"))
