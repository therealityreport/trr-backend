from __future__ import annotations

import copy
import time
from contextlib import contextmanager

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.v2.survey_submissions import router
from trr_backend.services import survey_submissions as service

SECRET = "test-only-survey-assertion-secret"


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", SECRET)
    app = FastAPI()
    app.include_router(router, prefix="/api/v2")
    return TestClient(app)


def headers(**overrides):
    now = int(time.time())
    claims = {
        "sub": "firebase-owner",
        "email": "owner@example.test",
        "provider": "firebase",
        "scope": "survey:own",
        "iss": "trr-app-internal",
        "aud": "trr-backend-survey-submissions",
        "iat": now,
        "nbf": now,
        "exp": now + 60,
    }
    claims.update(overrides)
    return {"authorization": "Bearer " + jwt.encode(claims, SECRET, algorithm="HS256")}


@pytest.mark.parametrize(
    "claims",
    [
        {"exp": 1},
        {"scope": "internal_admin"},
        {"aud": "trr-backend-internal-admin"},
        {"provider": "supabase"},
        {"sub": ""},
        {"exp": int(time.time()) + 500},
    ],
)
def test_rejects_invalid_assertions(client, claims):
    assert client.get("/api/v2/surveys/x", headers=headers(**claims)).status_code == 401


def test_rejects_unsigned_and_forged_tokens(client):
    assert client.get("/api/v2/surveys/x").status_code == 401
    forged = headers()["authorization"][:-8] + "abcdefgh"
    assert client.get("/api/v2/surveys/x", headers={"authorization": forged}).status_code == 401


def test_server_derived_owner(client, monkeypatch):
    seen = []
    monkeypatch.setattr(service, "read_response", lambda user, *args: seen.append(user) or None)
    response = client.get("/api/v2/surveys/x?uid=attacker", headers=headers())
    assert response.json() == {"state": None}
    assert seen == [{"uid": "firebase-owner", "email": "owner@example.test"}]


def test_submitted_read_excludes_drafts(monkeypatch):
    seen = []

    @contextmanager
    def transaction():
        yield

    monkeypatch.setattr(service.pg, "transaction", transaction)
    monkeypatch.setattr(service.pg, "execute", lambda *args: None)
    monkeypatch.setattr(service.pg, "fetch_one", lambda sql, params: seen.append((sql, params)))
    service.read_response({"uid": "owner"}, "rhoslc_s6_v1", "S06", "E01")
    assert "completed IS TRUE" in seen[0][0]
    assert seen[0][1] == ["owner", "S06", "E01"]


def test_show_identity_mismatch_and_draft_rejected(client):
    body = {"surveyKey": "rhop_s10", "appUserId": "someone-else", "responses": {"completed": True}}
    assert client.post("/api/v2/surveys/show-responses", headers=headers(), json=body).status_code == 403
    body["appUserId"] = "firebase-owner"
    body["responses"]["completed"] = False
    assert client.post("/api/v2/surveys/show-responses", headers=headers(), json=body).status_code == 400


def test_x_and_profile_share_transaction_and_rollback(monkeypatch):
    rows = {}
    fail_profile = False
    outcomes = []

    @contextmanager
    def connection(**kwargs):
        before = copy.deepcopy(rows)
        try:
            yield object()
            outcomes.append("commit")
        except Exception:
            rows.clear()
            rows.update(before)
            outcomes.append("rollback")
            raise

    def execute(sql, params):
        assert service.pg.has_active_transaction()
        assert params == ["owner"]

    def returning(sql, values):
        assert service.pg.has_active_transaction()
        table = sql.split("INSERT INTO public.")[1].split(" ")[0]
        assert "ON CONFLICT (app_user_id)" in sql
        if table == "survey_global_profile_responses" and fail_profile:
            raise RuntimeError("profile write failed")
        rows[table] = [value.adapted if hasattr(value, "adapted") else value for value in values]
        return [{"updated_at": "2026-10-31T00:00:00Z"}]

    monkeypatch.setattr(service.pg, "db_connection", connection)
    monkeypatch.setattr(service.pg, "execute", execute)
    monkeypatch.setattr(service.pg, "execute_returning", returning)
    payload = {"responses": {"watchMode": "live"}}
    service.submit_x({"uid": "owner"}, payload)
    assert len(rows) == 2 and outcomes == ["commit"]
    original = copy.deepcopy(rows)
    fail_profile = True
    with pytest.raises(RuntimeError, match="profile write failed"):
        service.submit_x({"uid": "owner"}, {"responses": {"watchMode": "binge"}})
    assert rows == original and outcomes == ["commit", "rollback"]
