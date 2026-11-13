"""Claim-bound checkpoint adapter for existing TRR social jobs.

This module creates no queue, schedule or worker. It cannot run before persistence
acceptance. Only an acquired running Twitter job may retain collection state.
"""

from __future__ import annotations

import json

from trr_backend.db import pg
from trr_backend.socials.control_plane.job_ownership import _job_claim_write_scope
from trr_backend.socials.twitter.public_collect import collect_public


class PublicJobOwnershipLostError(RuntimeError):
    pass


def run_claimed_public_collection(
    job_id: str,
    *,
    worker_id: str,
    claim_token: str,
    acceptance: dict,
    mode: str = "resume",
    cancelled=None,
    fetcher=None,
) -> dict:
    required = {"rhoslc_search", "bravotv_profile", "nullable_schema", "known_value_merge", "consumer_nulls"}
    if not all(acceptance.get(key) is True for key in required):
        raise ValueError("Public persistence acceptance gates have not passed")
    if not worker_id or not claim_token:
        raise ValueError("An acquired job claim is required")

    def owned_scope():
        return _job_claim_write_scope(job_id, worker_id=worker_id, claim_token=claim_token)

    with owned_scope() as owned:
        if not owned:
            raise PublicJobOwnershipLostError("Public job claim was lost")
        row = pg.fetch_one("SELECT platform, config FROM social.scrape_jobs WHERE id = %s", [job_id])
        config = (row or {}).get("config") or {}
        spec = config.get("public_x") or {}
        if (row or {}).get("platform") != "twitter" or spec.get("provider") != "fxtwitter":
            raise ValueError("Existing job must explicitly select public FxTwitter")
        state = config.get("public_x_checkpoint")
        if mode == "fresh" and state is not None:
            raise ValueError("Use resume or refresh for an existing checkpoint")

    # Network calls occur outside short claim-write transactions.
    def save(checkpoint):
        with owned_scope() as owned:
            if not owned:
                raise PublicJobOwnershipLostError("Public job claim was lost")
            pg.execute(
                """UPDATE social.scrape_jobs
                SET config = jsonb_set(config, '{public_x_checkpoint}', %s::jsonb), heartbeat_at = now()
                WHERE id = %s""",
                [json.dumps(checkpoint), job_id],
            )

    def cancelled_or_lost():
        if cancelled and cancelled():
            return True
        with owned_scope() as owned:
            if not owned:
                raise PublicJobOwnershipLostError("Public job claim was lost")
        return False

    allowed = {"query", "capability", "sort", "max_pages", "max_items", "date_start", "date_end", "deadline_seconds"}
    if set(spec) - allowed - {"provider"}:
        raise ValueError("Unsupported public job option")
    return collect_public(
        **{k: v for k, v in spec.items() if k in allowed},
        mode=mode if state is not None or mode == "refresh" else "fresh",
        state=state,
        cancelled=cancelled_or_lost,
        on_checkpoint=save,
        fetcher=fetcher,
    )
