from datetime import UTC, datetime, timedelta

import pytest

from tests.ingestion.test_persistence import database as database_fixture
from tests.ingestion.test_persistence import launch, target
from trr_backend.ingestion.application import PostActivity
from trr_backend.ingestion.domain import Conflict
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.providers import InvalidSourceError

database = database_fixture


def test_fetch_retained_before_validation_and_receipt_atomic(database):
    store, tx = database
    args = target(store, launch(store))
    token = str(store.reserve(**args)["owner_token"])

    def fetch(**callbacks):
        callbacks["check_cancelled"]()
        callbacks["on_request"]()
        # Separate transaction proves the remote call has no business row lock.
        with tx() as conn, conn.cursor() as cur:
            cur.execute("set local lock_timeout='100ms'")
            cur.execute(
                "select run_id from social.instagram_ingestion_runs where run_id=%s for update", (args["run_id"],)
            )
        return {
            "items": [
                {
                    "pk": "1",
                    "shortcode": "post1",
                    "owner": {"username": "bravotv"},
                    "caption": {"text": "Example"},
                    "like_count": 0,
                    "comment_count": 0,
                    "display_url": "https://cdn.example/image.jpg",
                }
            ]
        }

    def writer(conn, **payload):
        source = payload["source_payload"]
        assert source["detail_snapshot"]["likes_present"]
        assert source["detail_snapshot"]["media_count"] == 1
        assert source["detail_snapshot"]["observed_at"] == payload["observed_at"].isoformat()
        with conn.cursor() as cur:
            cur.execute("select count(*) from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))
            assert cur.fetchone()[0] == 1
            cur.execute("insert into social.test_canonical values(%s,1)", (args["run_id"],))
        return {"shortcode": "post1"}

    receipt = PostActivity(store, writer=writer).execute(**args, owner_token=token, fetch=fetch)
    assert receipt["outcome"] == "committed"
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,requests,outcome from social.instagram_ingestion_targets where run_id=%s",
            (args["run_id"],),
        )
        assert cur.fetchone() == (1, 1, "committed")


def test_partial_source_remains_replayable_without_false_completion(database):
    store, tx = database
    args = target(store, launch(store))
    token = str(store.reserve(**args)["owner_token"])
    with pytest.raises(InvalidSourceError, match="coverage"):
        PostActivity(store, writer=lambda *a, **k: pytest.fail("Invalid source reached writer")).execute(
            **args,
            owner_token=token,
            fetch=lambda **callbacks: {"pk": "1", "shortcode": "post1", "owner": {"username": "bravotv"}},
        )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))
        assert cur.fetchone()[0] == 1
        cur.execute("select outcome from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],))
        assert cur.fetchone()[0] == "unresolved"


def test_receipt_cannot_relabel_old_evidence_as_new(database):
    store, _ = database
    args = target(store, launch(store))
    token = str(store.reserve(**args)["owner_token"])
    store.acquire(**args, owner_token=token)
    evidence = Evidence.capture(
        source_identity="instagram:post1",
        captured_at=datetime.now(UTC) - timedelta(days=1),
        parser_version="v1",
        payload={"shortcode": "post1"},
    )
    evidence_id = store.retain_evidence(**args, owner_token=token, evidence=evidence)
    with pytest.raises(Conflict, match="Observation time"):
        store.commit_target(
            **args,
            owner_token=token,
            observed_at=datetime.now(UTC),
            evidence_id=evidence_id,
            write_canonical=lambda conn: pytest.fail("Relabelled evidence reached writer"),
        )
