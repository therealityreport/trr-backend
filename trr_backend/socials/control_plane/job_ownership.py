"""Import-neutral ownership of acquired social job claims and write scopes."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from typing import Any

from trr_backend.db import pg

# Never reconstruct an acquired identity from the current database row.
_job_claim_context: ContextVar[tuple[str, str | None, str | None] | None] = ContextVar("social_job_claim", default=None)


def _job_claim_identity(
    job_id: str, *, claim_token: str | None = None, worker_id: str | None = None
) -> tuple[str | None, str | None]:
    bound = _job_claim_context.get()
    if bound is not None and bound[0] == job_id:
        if claim_token is None:
            claim_token = bound[1]
        if worker_id is None:
            worker_id = bound[2]
    return claim_token, worker_id


@contextmanager
def _job_claim_write_scope(
    job_id: str,
    *,
    claim_token: str | None = None,
    worker_id: str | None = None,
    claim_identity: Callable[..., tuple[str | None, str | None]] = _job_claim_identity,
):
    """Lock an acquired standard claim for coupled writes; stale claims yield False.

    Exceptions must escape this scope so every coupled write rolls back. A
    worker must not catch a failed completion inside this transaction.
    """
    claim_token, worker_id = claim_identity(job_id, claim_token=claim_token, worker_id=worker_id)
    with pg.transaction() as conn:
        with pg.db_cursor(conn=conn, label="guard_social_job_claim") as cur:
            row = pg.fetch_one_with_cursor(
                cur,
                """select id from social.scrape_jobs
                where id = %s and status = 'running' and claimed_at is not null
                  and worker_id = %s::text and config->>'claim_token' = %s::text
                  and coalesce(config->>'detail_contract_version', '') <> '1'
                for update""",
                [job_id, worker_id, claim_token],
            )
        yield bool(row)


@contextmanager
def _job_followup_write_scope(
    job_id: str,
    *,
    expected_status: str,
    claim_token: str | None,
    worker_id: str | None,
    administrative: bool = False,
    conn: Any | None = None,
):
    """Hold the retained generation while applying short DB-only followups."""
    with pg.transaction() if conn is None else nullcontext(conn) as conn:
        with pg.db_cursor(conn=conn, label="guard_social_job_followup") as cur:
            row = pg.fetch_one_with_cursor(
                cur,
                """select id from social.scrape_jobs
                where id = %s and status = %s
                  and (config->>'claim_token') is not distinct from %s::text
                  and worker_id is not distinct from %s::text
                  and (%s::boolean or %s::text is not null)
                for update""",
                [
                    job_id,
                    expected_status,
                    claim_token,
                    None if expected_status == "retrying" else worker_id,
                    administrative,
                    claim_token,
                ],
            )
        yield bool(row)
