from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.protobuf.timestamp_pb2 import Timestamp as ProtobufTimestamp

from scripts.db import reconcile_legacy_survey_storage as reconcile

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)


def snapshot(data, path="users/owner"):
    return SimpleNamespace(
        to_dict=lambda: data, id=path.split("/")[-1], reference=SimpleNamespace(path=path), update_time=NOW
    )


def survey_x():
    return snapshot(
        {
            "uid": "owner",
            "surveyX": {
                "completed": True,
                "completedAt": NOW,
                "responses": {
                    "view_live_tv_household": "yes",
                    "view_platforms_subscriptions": ["service"],
                    "primaryPlatform": "service",
                    "watchFrequency": "weekly",
                    "watchMode": "live",
                    "view_reality_cowatch": "alone",
                    "view_live_chats_social": "no",
                    "view_devices_reality": ["tv"],
                },
            },
        }
    )


class Connection:
    def __init__(self, *, collision=None):
        self.rows = {}
        self.collision = collision
        self.inserts = []
        self.rollbacks = 0
        self.result = None

    def __enter__(self):
        self.before = deepcopy(self.rows)
        return self

    def __exit__(self, kind, error, traceback):
        if kind:
            self.rows = self.before
            self.rollbacks += 1

    def cursor(self, **kwargs):
        return self.Cursor(self)

    class Cursor:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, statement, values=None):
            if "INSERT INTO" not in str(statement):
                return
            assert "DO NOTHING" in str(statement)
            assert "DO UPDATE" not in str(statement)
            table = next(table for table in reconcile.service.TABLES.values() if table in str(statement))
            self.connection.inserts.append(table)
            if table == self.connection.collision:
                self.connection.result = None
                return
            self.connection.rows[table] = values
            self.connection.result = {"id": "inserted"}

        def fetchone(self):
            return self.connection.result


def prepared_x():
    candidate = reconcile.candidate_from_snapshot(survey_x())
    return reconcile.prepare_rows(candidate, {"uid": "owner", "email": "owner@example.test"})


def test_prepare_reuses_validation_and_preserves_source_answers_and_timestamps():
    prepared = prepared_x()
    assert [key for key, _ in prepared] == ["survey_x", "global_profile"]
    for _key, row in prepared:
        assert row["created_at"] == NOW and row["updated_at"] == NOW
        assert row["app_user_id"] == "owner"
        provenance = row["extra"].adapted
        assert provenance["legacy_answers"] == survey_x().to_dict()["surveyX"]["responses"]
        assert provenance["legacy_import"]["project"] == reconcile.FIREBASE_PROJECT
        assert len(provenance["legacy_import"]["payload_sha256"]) == 64


def test_invalid_source_answers_cannot_reach_writes():
    doc = survey_x()
    doc.to_dict()["surveyX"]["responses"]["view_devices_reality"] = "invalid"
    with pytest.raises(reconcile.ReconciliationError, match="source_validation_failed"):
        reconcile.prepare_rows(reconcile.candidate_from_snapshot(doc), {"uid": "owner"})


@pytest.mark.parametrize(
    "states,expected",
    [
        (["matching", "matching"], "existing_matching_preserved"),
        (["matching", "absent"], "existing_partial_pair_preserved"),
        (["absent", "matching"], "existing_partial_pair_preserved"),
        (["conflict", "absent"], "existing_conflict_preserved"),
    ],
)
def test_existing_rows_never_overwritten_or_half_imported(monkeypatch, states, expected):
    answers = prepared_x()
    statuses = iter(states)
    monkeypatch.setattr(reconcile, "existing_state", lambda *args: next(statuses))
    connection = Connection()
    assert reconcile.reconcile_candidate(connection, answers, execute=True) == expected
    assert not connection.inserts and not connection.rows


def test_dry_run_is_read_only(monkeypatch):
    monkeypatch.setattr(reconcile, "existing_state", lambda *args: "absent")
    connection = Connection()
    assert reconcile.reconcile_candidate(connection, prepared_x(), execute=False) == "eligible"
    assert not connection.inserts


def test_x_pair_inserts_atomically(monkeypatch):
    monkeypatch.setattr(reconcile, "existing_state", lambda *args: "absent")
    connection = Connection()
    assert reconcile.reconcile_candidate(connection, prepared_x(), execute=True) == "inserted"
    assert len(connection.rows) == 2


def test_profile_insert_race_rolls_back_x(monkeypatch):
    monkeypatch.setattr(reconcile, "existing_state", lambda *args: "absent")
    connection = Connection(collision="survey_global_profile_responses")
    assert reconcile.reconcile_candidate(connection, prepared_x(), execute=True) == "concurrent_existing_row_preserved"
    assert connection.rollbacks == 1 and connection.rows == {}


def test_existing_explicit_rating_conflict_preserved():
    candidate = reconcile.candidate_from_snapshot(
        snapshot(
            {
                "uid": "owner",
                "completed": True,
                "completionPct": 100,
                "createdAt": NOW,
                "updatedAt": NOW,
                "ranking": [{"id": "a", "label": "A", "img": "image"}],
                "seasonRating": 8,
            },
            "surveyResults/tt12623782/seasons/S06/episodes/E01/responses/owner",
        )
    )
    row = reconcile.prepare_rows(candidate, {"uid": "owner"})[0][1]
    cursor = SimpleNamespace(
        execute=lambda *args: None,
        fetchone=lambda: {
            "completed": True,
            "ranking": [{"id": "a"}],
            "season_rating": 9,
        },
    )
    assert reconcile.existing_state(cursor, candidate.kind, row) == "conflict"


def test_ambiguous_identity_and_missing_timestamps_block():
    data = survey_x().to_dict()
    data["uid"] = "other"
    with pytest.raises(reconcile.ReconciliationError, match="source_identity_mismatch"):
        reconcile.candidate_from_snapshot(snapshot(data))
    data["uid"] = "owner"
    del data["surveyX"]["completedAt"]
    with pytest.raises(reconcile.ReconciliationError, match="missing_source_timestamp"):
        reconcile.candidate_from_snapshot(snapshot(data))


def test_target_guards(monkeypatch):
    monkeypatch.delenv("FIRESTORE_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("FIREBASE_AUTH_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("NEXT_PUBLIC_USE_FIREBASE_EMULATORS", raising=False)
    dsn = "postgresql://postgres.vwxfvzutyufrkhfgoeaa:unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres"
    reconcile.verify_targets(
        reconcile.FIREBASE_PROJECT, reconcile.POSTGRES_PROJECT, {"project_id": reconcile.FIREBASE_PROJECT}, dsn
    )
    with pytest.raises(reconcile.ReconciliationError, match="postgres_credential_target_mismatch"):
        reconcile.verify_targets(
            reconcile.FIREBASE_PROJECT,
            reconcile.POSTGRES_PROJECT,
            {"project_id": reconcile.FIREBASE_PROJECT},
            "postgresql://other:unused@localhost/postgres",
        )
    with pytest.raises(reconcile.ReconciliationError, match="firebase_credential_target_mismatch"):
        reconcile.verify_targets(reconcile.FIREBASE_PROJECT, reconcile.POSTGRES_PROJECT, {"project_id": "other"}, dsn)


def test_collection_group_protobuf_revision_preserves_answer_timestamps_and_nanoseconds():
    doc = snapshot(
        {
            "uid": "owner",
            "completed": True,
            "completionPct": 100,
            "createdAt": NOW,
            "updatedAt": NOW,
            "ranking": [{"id": "a", "label": "A", "img": "image"}],
            "seasonRating": 8,
        },
        "surveyResults/tt12623782/seasons/S06/episodes/E01/responses/owner",
    )
    revision = ProtobufTimestamp(seconds=int(NOW.timestamp()), nanos=987654321)
    doc.update_time = revision
    candidate = reconcile.candidate_from_snapshot(doc)
    assert candidate.created_at == NOW and candidate.updated_at == NOW
    assert isinstance(candidate.source_version, DatetimeWithNanoseconds)
    assert candidate.source_version.nanosecond == 987654321
    prepared = reconcile.prepare_rows(candidate, {"uid": "owner"})
    assert prepared[0][1]["created_at"] == NOW
    assert prepared[0][1]["updated_at"] == NOW
    assert prepared[0][1]["extra"].adapted["legacy_import"]["source_updated_at"].endswith(".987654321Z")
    document_revision = DatetimeWithNanoseconds.from_timestamp_pb(revision)
    assert reconcile.source_revision(revision) == reconcile.source_revision(document_revision)
    changed = ProtobufTimestamp(seconds=revision.seconds, nanos=987654322)
    assert reconcile.source_revision(changed) != reconcile.source_revision(candidate.source_version)
