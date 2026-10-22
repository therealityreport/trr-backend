"""Interrupted durable waits recheck current targets without changing the journal."""

import threading
from datetime import UTC, datetime

import pytest

from tests.ingestion import test_local_worker as existing
from tests.ingestion.test_local_execution import fixture_run, resume_local, sql
from trr_backend.ingestion.composition import WORKFLOW_VERSION

base_database = existing.base_database
database = existing.database
clean = existing.clean


@pytest.mark.parametrize("target_still_waiting", [False, True])
def test_replayed_long_wait_rechecks_live_deadline_without_rewriting_decision(database, target_still_waiting):
    _, _, request = fixture_run(database)
    run = resume_local(database, request)
    local = database[1]
    item = local.seed(
        {
            "run_id": request.run_id,
            "account": "bravotv",
            "generation": run["generation"],
            "engine": "dbos",
            "workflow_version": WORKFLOW_VERSION,
        }
    )
    sql(database, "update social.instagram_ingestion_targets set next_attempt_at=clock_timestamp()+interval '7 hour'")
    old = local.decision(item, 0, datetime.now(UTC))
    assert old["kind"] == "wait" and old["seconds"] > 25000
    if not target_still_waiting:
        sql(database, "update social.instagram_ingestion_targets set next_attempt_at=null")

    class Abort(threading.Event):
        def __init__(self):
            super().__init__()
            self.waits = []

        def wait(self, timeout=None):
            self.waits.append(timeout)
            if target_still_waiting:
                self.set()
                return True
            return False

    abort, stop, calls = Abort(), threading.Event(), []
    existing._worker(
        database,
        existing._commit(local, calls, abort),
        abort=abort,
        stop=stop,
        on_cycle=existing._stop_when_idle(abort, stop),
    )
    assert abort.waits and max(abort.waits) <= 5
    assert calls == ([] if target_still_waiting else ["post1"])
    assert sql(
        database,
        "select decision from social.instagram_ingestion_decisions where execution_id=%s and ordinal=0",
        (item["execution_id"],),
    ) == [(old,)]
    target = sql(database, "select requests,next_attempt_at from social.instagram_ingestion_targets")[0]
    assert target[0] == (0 if target_still_waiting else 1)
    if target_still_waiting:
        assert target[1] > datetime.now(UTC)
