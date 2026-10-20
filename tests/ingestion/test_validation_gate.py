from datetime import UTC, datetime
from uuid import uuid4

import pytest

from tests.ingestion.test_persistence import database, launch  # noqa: F401
from trr_backend.ingestion.domain import Conflict, RunIntent, Stage
from trr_backend.ingestion.validation_gate import ValidationStore, ValidationWorkflowStore


def test_gate_rejects_new_full_history_without_opening_database():
    def no_database():
        pytest.fail("held launch must not open the database")

    store = ValidationStore(no_database)
    with pytest.raises(Conflict, match="fixed-post retrieval"):
        store.start(RunIntent(account="bravotv"))


def test_existing_full_history_cannot_resume_or_dispatch_but_can_cancel(database):  # noqa: F811
    old_store, transaction = database
    run = launch(old_store)
    guarded = ValidationStore(transaction)
    cancelled = guarded.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    assert cancelled["state"] == "cancelled"
    with pytest.raises(Conflict, match="fixed-post retrieval"):
        guarded.control(run["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
    with pytest.raises(Conflict, match="fixed-post retrieval"):
        ValidationWorkflowStore(transaction).seed(
            {
                "run_id": run["run_id"],
                "account": "bravotv",
                "generation": run["generation"],
                "engine": "dbos",
                "workflow_version": "test-v1",
            }
        )


def test_gate_allows_bounded_validation_and_preserves_v2_store(database):  # noqa: F811
    _, transaction = database
    store = ValidationStore(transaction)
    result = store.start(
        RunIntent(
            account="bravotv",
            scope="recent_window",
            date_start=datetime(2026, 9, 1, tzinfo=UTC),
            date_end=datetime(2026, 10, 1, tzinfo=UTC),
            selected_tasks=(Stage.DETAILS, Stage.COMMENTS),
        ),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="test-v1",
        initiated_by="fixture",
    )
    assert result["state"] == "queued"
