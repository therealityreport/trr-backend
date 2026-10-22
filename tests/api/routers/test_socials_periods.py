from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.socials import analytics


def test_periods_route_is_admin_only_and_uses_calendar_boundary(monkeypatch) -> None:
    secret = "test-secret-32-bytes-minimum-abcdef"
    monkeypatch.setenv("SUPABASE_JWT_SECRET", secret)
    now = datetime.now(UTC)
    token = jwt.encode(
        {"sub": "admin", "role": "service_role", "iat": now, "exp": now + timedelta(minutes=5)},
        secret,
        algorithm="HS256",
    )
    season_id = str(uuid4())
    calls = []

    def calendar(sid, **kwargs):
        calls.append((sid, kwargs))
        return {"season_id": sid, "weekly": [{"week_index": 0, "start": "2026-09-10T00:00:00Z"}]}

    monkeypatch.setattr(analytics, "get_periods", calendar)
    monkeypatch.setattr(analytics, "get_analytics", lambda *_args, **_kwargs: pytest.fail("full analytics called"))
    client = TestClient(app)
    path = f"/api/v1/admin/socials/seasons/{season_id}/analytics/periods"
    assert client.get(path).status_code in {401, 403}
    response = client.get(
        path,
        params={"source_scope": "bravo", "timezone": "America/New_York"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json()["weekly"][0]["week_index"] == 0
    assert calls == [(season_id, {"source_scope": "network", "timezone": "America/New_York"})]
