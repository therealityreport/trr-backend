"""Operator launch preserves frozen scope and ambiguous-command recovery."""

import json

import pytest

from scripts.ingestion.launch_authenticated_local import main
from tests.ingestion import test_local_worker as local
from trr_backend.ingestion import composition, credential_snapshot
from trr_backend.ingestion.domain import SourceProfile

base_database = local.base_database
database = local.database
clean = local.clean


def test_default_operator_plan_has_no_credentials_database_or_state(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        credential_snapshot.CredentialSnapshot, "from_environ", lambda: pytest.fail("plan read credentials")
    )
    state = tmp_path / "state.json"
    assert main(["--state", str(state), "--source-revision", "fixture"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "plan" and report["source_requests"] == report["runs_created"] == 0
    assert report["date_start"] == "2026-01-01T00:00:00+00:00"
    assert report["date_end_exclusive"] == "2026-02-01T00:00:00+00:00"
    assert not state.exists()


def test_operator_recovers_created_run_when_final_state_save_was_interrupted(database, tmp_path, monkeypatch):
    from scripts.ingestion import launch_authenticated_local as operator
    from scripts.ingestion import local_driver
    from trr_backend.db import connection

    service = local._service(database)
    service.source_profile = SourceProfile.for_authenticated_relay()
    monkeypatch.setattr(composition, "build_api", lambda: service)
    monkeypatch.setattr(credential_snapshot.CredentialSnapshot, "from_environ", lambda: object())
    monkeypatch.setattr(connection, "resolve_database_url_candidate_details", lambda: [{"url": "offline-fixture"}])
    monkeypatch.setattr(local_driver, "_project_dsn", lambda url: url == "offline-fixture")
    real_save = operator._save
    failures = [True]

    def interrupted_save(path, state):
        if state.get("run_id") and failures[0]:
            failures[0] = False
            raise RuntimeError("interrupted after command create")
        return real_save(path, state)

    monkeypatch.setattr(operator, "_save", interrupted_save)
    state = tmp_path / "state.json"
    args = ["--state", str(state), "--source-revision", "fixture", "--apply"]
    assert main(args) == 2
    assert "run_id" not in json.loads(state.read_text())
    assert main(args) == 0
    saved = json.loads(state.read_text())
    assert saved["run_id"]
    assert local.sql(database, "select count(*) from social.instagram_ingestion_runs") == [(1,)]
    assert local.sql(database, "select execution_placement from social.instagram_ingestion_runs") == [("local",)]
    assert state.stat().st_mode & 0o777 == 0o600
