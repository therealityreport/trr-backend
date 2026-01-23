"""Regression tests for show cast endpoint admin client usage."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api import deps
from api.main import app


def _create_mock_response(data=None):
    mock_resp = MagicMock()
    mock_resp.data = data if data is not None else []
    mock_resp.error = None
    return mock_resp


def _build_admin_client() -> MagicMock:
    mock_client = MagicMock()
    response = _create_mock_response([])
    (
        mock_client.schema.return_value.table.return_value.select.return_value.eq.return_value.order.return_value.range.return_value.execute
    ).return_value = response
    return mock_client


@pytest.fixture
def admin_client():
    return _build_admin_client()


@pytest.fixture
def anon_client():
    return MagicMock()


@pytest.fixture
def client(admin_client, anon_client):
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: admin_client
    app.dependency_overrides[deps.get_supabase_client] = lambda: anon_client
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_list_show_cast_uses_admin_client_legacy_table(client, admin_client, anon_client, monkeypatch):
    monkeypatch.delenv("ENABLE_CREDITS_V2_READ", raising=False)

    show_id = "00000000-0000-0000-0000-000000000000"
    response = client.get(f"/api/v1/shows/{show_id}/cast")

    assert response.status_code == 200
    admin_client.schema.assert_called_with("core")
    admin_client.schema.return_value.table.assert_called_with("show_cast")
    admin_client.schema.return_value.table.return_value.select.return_value.eq.assert_called_with(
        "show_id",
        show_id,
    )
    anon_client.schema.assert_not_called()


def test_list_show_cast_uses_admin_client_v2_view(client, admin_client, anon_client, monkeypatch):
    monkeypatch.setenv("ENABLE_CREDITS_V2_READ", "1")

    show_id = "00000000-0000-0000-0000-000000000000"
    response = client.get(f"/api/v1/shows/{show_id}/cast")

    assert response.status_code == 200
    admin_client.schema.assert_called_with("core")
    admin_client.schema.return_value.table.assert_called_with("v_show_cast_from_credits")
    admin_client.schema.return_value.table.return_value.select.return_value.eq.assert_called_with(
        "show_id",
        show_id,
    )
    anon_client.schema.assert_not_called()
