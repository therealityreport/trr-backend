from contextlib import contextmanager

import pytest
from pydantic import ValidationError
from test_persistence import database as database_fixture
from test_persistence import launch, target

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2, InstagramIngestionProgressV2
from trr_backend.ingestion.queries import Queries

database = database_fixture


def test_progress_runs_inside_read_only_transaction_and_retains_truth(database):
    store, transaction = database
    run = launch(store)
    target(store, run)

    @contextmanager
    def read_only():
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY")
            yield conn

    queries = Queries(read_only)
    snapshot = queries.progress(run["run_id"], "bravotv")
    typed = InstagramIngestionProgressV2.model_validate(snapshot)
    assert not typed.complete
    assert typed.allowed_actions == ["cancel"]
    assert next(s for s in typed.stages if s.stage == "post_details").unresolved == 1
    assert not next(s for s in typed.stages if s.stage == "listing").discovery_complete
    assert queries.progress(run["run_id"], "bravotv") == snapshot
    with pytest.raises(LookupError):
        queries.progress(run["run_id"], "unrelated")


def test_launch_schema_rejects_unknown_fields_and_empty_tasks():
    with pytest.raises(ValidationError):
        InstagramIngestionLaunchV2.model_validate({"unknown": True})
    with pytest.raises(ValidationError):
        InstagramIngestionLaunchV2.model_validate({"selected_tasks": []})
    result = InstagramIngestionLaunchV2.model_validate({"selected_tasks": ["media", "comments", "media"]})
    assert result.selected_tasks == ["comments", "media"]
    assert InstagramIngestionLaunchV2().selected_tasks == ["post_details", "comments", "media"]


def test_progress_schema_rejects_false_completion(database):
    store, transaction = database
    run = launch(store)
    snapshot = Queries(transaction).progress(run["run_id"], "bravotv")
    snapshot["complete"] = True
    with pytest.raises(ValidationError, match="Run completion"):
        InstagramIngestionProgressV2.model_validate(snapshot)


def test_progress_explains_execution_hold(database):
    store, transaction = database
    run = launch(store)
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("alter table social.instagram_ingestion_runs add column if not exists execution_hold_reason text")
        cur.execute(
            """update social.instagram_ingestion_runs set state='cancelled',execution_hold_reason='operator_review'
            where run_id=%s""",
            (run["run_id"],),
        )
    progress = InstagramIngestionProgressV2.model_validate(Queries(transaction).progress(run["run_id"], "bravotv"))
    assert progress.execution_hold_reason == "operator_review"
    assert progress.allowed_actions == []
