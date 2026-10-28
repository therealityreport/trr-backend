"""Offline HTTP contract tests; the service is explicit and never dispatches."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2.instagram_ingestion import router
from trr_backend.ingestion.domain import Conflict, ReplayUnavailable, RunIntent

BASE = "/api/v2/admin/social/profiles/instagram/@BravoTV/ingestion"
NOW = datetime(2026, 10, 16, tzinfo=UTC)
RUN_ID = uuid4()
EVIDENCE_ID = uuid4()


class FakeService:
    def __init__(self):
        self.calls = []
        self.mutations = []
        self.enabled = True
        self.replay_apply_enabled = True
        self.error = None
        self.diagnostic_extra = False
        self.accepted_launches = {}

    def capabilities(self, handle):
        self.calls.append(("capabilities", handle))
        return {
            "enabled": self.enabled,
            "account": handle,
            "engine": "dbos",
            "workflow_version": "v1",
            "replay_apply_enabled": self.replay_apply_enabled,
        }

    def _scope(self, handle, run_id):
        if handle != "bravotv" or run_id != RUN_ID:
            raise LookupError("must not leak foreign account data")
        if self.error:
            raise self.error

    def command(self):
        return {"run_id": RUN_ID, "state": "queued", "engine": "dbos", "workflow_version": "v1", "generation": 1}

    def launch(self, handle, payload, **kwargs):
        key = kwargs["idempotency_key"]
        if key in self.accepted_launches:
            return self.accepted_launches[key]
        if not self.enabled:
            raise HTTPException(503, "New-run routing is disabled")
        self.mutations.append(("launch", handle, payload, kwargs))
        if self.error:
            raise self.error
        result = self.command()
        self.accepted_launches[key] = result
        return result

    def progress(self, handle, run_id):
        self._scope(handle, run_id)
        self.calls.append(("progress", handle, run_id))
        stages = [
            {
                "stage": stage,
                "discovery_complete": False,
                "sealed_at": None,
                "total": 0,
                "committed": 0,
                "cached_satisfied": 0,
                "source_unavailable": 0,
                "unresolved": 0,
                "attempts": 0,
                "requests": 0,
                "retry_at": None,
                "blocked_reasons": [],
                "last_committed_at": None,
                "complete": False,
            }
            for stage in ("listing", "post_details", "comments", "media")
        ]
        return {
            **self.command(),
            "account": handle,
            "intent": RunIntent(account=handle).payload(),
            "intent_hash": "hash",
            "last_committed_at": None,
            "created_at": NOW,
            "updated_at": NOW,
            "stages": stages,
            "pinned_evidence_count": 0,
            "complete": False,
            "allowed_actions": ["cancel"],
        }

    def recent(self, handle, **kwargs):
        self.calls.append(("recent", handle, kwargs))
        return {"runs": []}

    def control(self, handle, run_id, **kwargs):
        self._scope(handle, run_id)
        self.mutations.append(("control", handle, run_id, kwargs))
        return self.command()

    def retry(self, handle, run_id, payload, **kwargs):
        self._scope(handle, run_id)
        self.mutations.append(("retry", handle, run_id, payload, kwargs))
        return self.command()

    def diagnostics(self, handle, run_id, **kwargs):
        self._scope(handle, run_id)
        self.calls.append(("diagnostics", handle, run_id, kwargs))
        result = {
            "run_id": run_id,
            "account": handle,
            "targets": [],
            "evidence": [],
            "targets_truncated": False,
            "evidence_truncated": False,
        }
        if self.diagnostic_extra:
            result["owner_token"] = "must-never-be-serialized"
        return result

    def replay(self, handle, run_id, payload, **kwargs):
        self._scope(handle, run_id)
        self.mutations.append(("replay", handle, run_id, payload, kwargs))
        return {
            "mode": "apply" if payload.apply else "dry_run",
            "evidence_id": payload.evidence_id,
            "run_id": run_id,
            "stage": "post_details",
            "target_key": "post1",
            "captured_at": NOW,
            "content_hash": "hash",
            "capture_parser_version": "v1",
            "parser_version": payload.parser_version,
            "schema_version": 1,
            "normalized": {"caption": "public"},
            "comparison": {"previous": None, "current": {"caption": "public"}, "changed": True},
            "writes": 1 if payload.apply else 0,
            "source_requests": 0,
            "receipt_id": uuid4() if payload.apply else None,
        }


@pytest.fixture
def api():
    app = FastAPI()
    app.include_router(router, prefix="/api/v2")
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service-identity",
        "admin_uid": "verified-admin",
        "token": "never-pass-this-token",
    }
    service = FakeService()
    app.state.instagram_ingestion = service
    with TestClient(app) as client:
        yield app, client, service


def assert_problem(response, status):
    assert response.status_code == status, response.text
    problem = response.json()["detail"]
    assert problem["status"] == status
    assert problem["code"]
    assert problem["trace_id"]
    assert problem["request_id"]
    assert "never-pass-this-token" not in response.text


def test_all_gets_are_read_only_and_normalize_scope(api):
    _, client, service = api
    paths = [
        "/capabilities",
        "/runs/recent?source_scope=bravo&limit=7",
        f"/runs/{RUN_ID}",
        f"/runs/{RUN_ID}/diagnostics?limit=5",
    ]
    for path in paths:
        assert client.get(BASE + path).status_code == 200
    assert not service.mutations
    assert ("recent", "bravotv", {"source_scope": "network", "limit": 7}) in service.calls
    assert ("progress", "bravotv", RUN_ID) in service.calls
    assert ("diagnostics", "bravotv", RUN_ID, {"limit": 5}) in service.calls


def test_absent_service_is_explicit_and_never_dispatches(api):
    app, client, service = api
    del app.state.instagram_ingestion
    result = client.get(BASE + "/capabilities")
    assert result.status_code == 200
    assert result.json()["enabled"] is False
    assert_problem(client.get(BASE + "/runs/recent"), 503)
    assert_problem(client.post(BASE + "/runs", json={}, headers={"Idempotency-Key": "key"}), 503)
    assert not service.mutations


def test_disabled_new_run_routing_preserves_existing_run_reads_and_controls(api):
    _, client, service = api
    service.enabled = False
    headers = {"Idempotency-Key": "key"}
    assert client.get(BASE + "/capabilities").json()["enabled"] is False
    assert_problem(client.post(BASE + "/runs", json={}, headers=headers), 503)
    for suffix in ("/runs/recent", f"/runs/{RUN_ID}", f"/runs/{RUN_ID}/diagnostics"):
        assert client.get(BASE + suffix).status_code == 200
    for action in ("cancel", "resume"):
        assert client.post(BASE + f"/runs/{RUN_ID}/{action}", headers=headers).status_code == 202
    assert (
        client.post(
            BASE + f"/runs/{RUN_ID}/retry-targets",
            headers=headers,
            json={"stage": "comments", "target_keys": ["post1"]},
        ).status_code
        == 202
    )
    assert (
        client.post(
            BASE + f"/runs/{RUN_ID}/replay",
            headers=headers,
            json={"evidence_id": str(EVIDENCE_ID), "parser_version": "v1"},
        ).status_code
        == 200
    )
    assert [command[0] for command in service.mutations] == ["control", "control", "retry", "replay"]


def test_replay_apply_is_gated_independently_of_new_run_routing(api):
    _, client, service = api
    service.enabled = False
    service.replay_apply_enabled = False
    headers = {"Idempotency-Key": "key"}
    payload = {"evidence_id": str(EVIDENCE_ID), "parser_version": "v1", "apply": True}
    assert_problem(client.post(BASE + f"/runs/{RUN_ID}/replay", headers=headers, json=payload), 503)
    assert not service.mutations
    service.replay_apply_enabled = True
    assert client.post(BASE + f"/runs/{RUN_ID}/replay", headers=headers, json=payload).status_code == 200
    assert len(service.mutations) == 1


@pytest.mark.parametrize("status", [401, 403])
def test_shared_admin_dependency_failures_are_typed(api, status):
    app, client, service = api

    def denied():
        raise HTTPException(status, "Admin required", headers={"WWW-Authenticate": "Bearer"})

    app.dependency_overrides[require_internal_admin] = denied
    response = client.get(BASE + "/capabilities")
    assert_problem(response, status)
    assert response.headers["www-authenticate"] == "Bearer"
    assert not service.calls


@pytest.mark.parametrize("key", [None, "", "   ", "x" * 201])
@pytest.mark.parametrize(
    "suffix,body",
    [
        ("/runs", {}),
        (f"/runs/{RUN_ID}/cancel", None),
        (f"/runs/{RUN_ID}/resume", None),
        (f"/runs/{RUN_ID}/retry-targets", {"stage": "comments", "target_keys": ["post1"]}),
        (f"/runs/{RUN_ID}/replay", {"evidence_id": str(EVIDENCE_ID), "parser_version": "v1"}),
    ],
)
def test_mutations_require_bounded_nonblank_idempotency_key(api, key, suffix, body):
    _, client, service = api
    headers = {} if key is None else {"Idempotency-Key": key}
    assert_problem(client.post(BASE + suffix, json=body, headers=headers), 422)
    assert not service.mutations


def test_launch_preserves_all_knobs_and_verified_actor_without_token(api):
    _, client, service = api
    response = client.post(
        BASE + "/runs",
        headers={"Idempotency-Key": " launch-key "},
        json={
            "action": "sync_recent",
            "source_scope": "bravo",
            "lookback_days": 7,
            "selected_tasks": ["media", "comments", "media"],
            "detail_worker_count": 12,
            "comments_worker_count": 24,
            "comments_enable_media_followups": False,
            "enable_cap4_canary": True,
            "force_catalog_rediscovery": True,
            "operator_confirmation": "approved",
            "apply_run_id": str(RUN_ID),
        },
    )
    assert response.status_code == 202, response.text
    _, handle, payload, kwargs = service.mutations[0]
    assert handle == "bravotv"
    assert payload.source_scope == "network"
    assert payload.lookback_days == 7
    assert payload.selected_tasks == ["comments", "media"]
    assert payload.detail_worker_count == 12 and payload.comments_worker_count == 24
    assert payload.comments_enable_media_followups is False
    assert payload.enable_cap4_canary and payload.force_catalog_rediscovery
    assert payload.operator_confirmation == "approved" and payload.apply_run_id == RUN_ID
    assert kwargs == {"idempotency_key": "launch-key", "initiated_by": "verified-admin"}


@pytest.mark.parametrize(
    "body",
    [
        {"unknown": True},
        {"lookback_days": 0},
        {"lookback_days": 31},
        {"backfill_scope": "bounded_window"},
        {"backfill_scope": "bounded_window", "date_start": "2026-10-15T00:00:00Z"},
        {"backfill_scope": "bounded_window", "date_start": "2026-10-15T00:00:00Z", "date_end": "2026-10-14T00:00:00Z"},
        {"selected_tasks": []},
        {"comments_worker_count": 25},
    ],
)
def test_launch_rejects_invalid_contract_without_command(api, body):
    _, client, service = api
    assert_problem(client.post(BASE + "/runs", json=body, headers={"Idempotency-Key": "key"}), 422)
    assert not service.mutations


@pytest.mark.parametrize("suffix", ["/cancel", "/resume"])
def test_empty_control_body_rejects_unknown_fields(api, suffix):
    _, client, service = api
    assert_problem(
        client.post(BASE + f"/runs/{RUN_ID}" + suffix, json={"recover": True}, headers={"Idempotency-Key": "key"}), 422
    )
    assert not service.mutations
    assert client.post(BASE + f"/runs/{RUN_ID}" + suffix, headers={"Idempotency-Key": "key"}).status_code == 202
    assert len(service.mutations) == 1


@pytest.mark.parametrize(
    "suffix",
    [
        "/runs/recent?limit=0",
        "/runs/recent?limit=101",
        "/runs/recent?limit=x",
        "/runs/recent?source_scope=unrecognized",
        f"/runs/{RUN_ID}/diagnostics?limit=101",
        "/runs/not-a-uuid",
    ],
)
def test_query_and_uuid_boundaries(api, suffix):
    _, client, service = api
    assert_problem(client.get(BASE + suffix), 422)
    assert not service.mutations


@pytest.mark.parametrize("handle,run_id", [("unrelated", RUN_ID), ("bravotv", uuid4())])
def test_account_and_run_scoping_passes_both_identities(api, handle, run_id):
    _, client, service = api
    base = BASE.replace("@BravoTV", handle)
    for suffix in (f"/runs/{run_id}", f"/runs/{run_id}/diagnostics"):
        assert_problem(client.get(base + suffix), 404)
    assert_problem(client.post(base + f"/runs/{run_id}/cancel", headers={"Idempotency-Key": "key"}), 404)
    assert not service.mutations


def test_retry_and_replay_forward_scoped_typed_requests(api):
    _, client, service = api
    headers = {"Idempotency-Key": "key"}
    response = client.post(
        BASE + f"/runs/{RUN_ID}/retry-targets", headers=headers, json={"stage": "comments", "target_keys": ["post1"]}
    )
    assert response.status_code == 202
    assert service.mutations[-1][2] == RUN_ID
    for apply in (False, True):
        response = client.post(
            BASE + f"/runs/{RUN_ID}/replay",
            headers=headers,
            json={"evidence_id": str(EVIDENCE_ID), "parser_version": "v2", "apply": apply},
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["captured_at"] == "2026-10-16T00:00:00Z"
        assert result["source_requests"] == 0
        assert result["writes"] == int(apply)
        assert bool(result["receipt_id"]) is apply
        assert service.mutations[-1][2] == RUN_ID


@pytest.mark.parametrize(
    "error,status",
    [(Conflict("conflict"), 409), (ReplayUnavailable("expired"), 410), (RuntimeError("private source contents"), 500)],
)
def test_service_failures_are_typed_and_do_not_leak_exception_text(api, error, status):
    _, client, service = api
    service.error = error
    response = client.get(BASE + f"/runs/{RUN_ID}")
    assert_problem(response, status)
    assert "private source contents" not in response.text


def test_diagnostics_forbid_persistence_or_secret_fields(api):
    _, client, service = api
    service.diagnostic_extra = True
    response = client.get(BASE + f"/runs/{RUN_ID}/diagnostics")
    assert_problem(response, 500)
    assert "must-never-be-serialized" not in response.text


def test_openapi_has_strict_errors_unique_operations_and_required_mutation_keys(api):
    app, _, _ = api
    schema = app.openapi()
    operations = []
    for path in schema["paths"].values():
        for method, operation in path.items():
            operations.append(operation["operationId"])
            for status in ("401", "403", "422", "503"):
                assert operation["responses"][status]["content"]["application/json"]["schema"]["$ref"].endswith(
                    "/InstagramIngestionProblemResponseV2"
                )
            if method == "post":
                header = next(p for p in operation["parameters"] if p["name"] == "Idempotency-Key")
                assert header["required"] and header["schema"]["maxLength"] == 200
    assert len(operations) == 9 and len(set(operations)) == 9
    assert schema["components"]["schemas"]["InstagramIngestionLaunchV2"]["additionalProperties"] is False


@pytest.mark.parametrize("suffix", ["/runs/recent?unknown=1", f"/runs/{RUN_ID}/diagnostics?unknown=1"])
def test_unknown_query_fields_are_rejected(api, suffix):
    _, client, service = api
    assert_problem(client.get(BASE + suffix), 422)
    assert not service.mutations


def test_invalid_response_logs_do_not_include_rejected_fields(api, caplog):
    _, client, service = api
    service.diagnostic_extra = True
    assert_problem(client.get(BASE + f"/runs/{RUN_ID}/diagnostics"), 500)
    assert "must-never-be-serialized" not in caplog.text


def test_http_launch_replays_accepted_key_after_new_run_routing_is_disabled(api):
    _, client, service = api
    headers = {"Idempotency-Key": "accepted-key"}
    original = client.post(BASE + "/runs", json={}, headers=headers)
    assert original.status_code == 202
    service.enabled = False
    replayed = client.post(BASE + "/runs", json={}, headers=headers)
    assert replayed.status_code == 202 and replayed.json() == original.json()
    assert len(service.mutations) == 1
    assert_problem(client.post(BASE + "/runs", json={}, headers={"Idempotency-Key": "new-key"}), 503)
    assert len(service.mutations) == 1
