"""New operator endpoints preserve admin access and canonical schedule scope."""

import asyncio
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.main import app
from api.routers.socials import reddit
from trr_backend.repositories import reddit_runtime, reddit_workflows


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("get", "/runs/{id}/targets", None),
        ("post", "/runs/{id}/control", {"action": "cancel"}),
        ("get", "/runs/{id}/compare?baseline_id={id}", None),
        ("get", "/communities/{id}/coverage?season_id={id}", None),
        ("patch", "/communities/{id}/settings?season_id={id}", {"schedule": {"enabled": True}}),
        ("post", "/communities/{id}/analysis", {"season_id": "{id}"}),
        ("post", "/communities/{id}/lifecycle", {"season_id": "{id}"}),
        ("post", "/communities/{id}/reclassify", {"season_id": "{id}"}),
        ("get", "/communities/{id}/legacy-inventory?season_id={id}", None),
        ("get", "/jobs/{id}", None),
    ],
)
def test_operator_endpoints_reject_unauthenticated(method, path, body):
    identity = str(uuid4())
    args = (
        {
            "json": {
                key: (value.replace("{id}", identity) if isinstance(value, str) else value)
                for key, value in body.items()
            }
        }
        if body
        else {}
    )
    response = getattr(TestClient(app), method)("/api/v1/admin/socials/reddit" + path.replace("{id}", identity), **args)
    assert response.status_code in {401, 403}


def test_schedule_settings_resolve_missing_template_only_on_enable(monkeypatch):
    async def invoke(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(reddit, "_run_admin_repo_call", invoke)
    monkeypatch.setattr(reddit_workflows, "get_settings", lambda **kw: {"capture_payload": {}})
    template = Mock(return_value={"subreddit": "bravo", "period_key": "scheduled-season"})
    monkeypatch.setattr(reddit_runtime, "schedule_capture_template", template)
    update = Mock(return_value={"schedule_enabled": True})
    monkeypatch.setattr(reddit_workflows, "update_settings", update)
    community, season = uuid4(), uuid4()
    asyncio.run(reddit.reddit_update_settings(community, season, {"schedule": {"enabled": True}}))
    template.assert_called_once_with(str(community), str(season))
    assert update.call_args.kwargs["patch"]["capture_payload"]["subreddit"] == "bravo"
    template.reset_mock()
    asyncio.run(reddit.reddit_update_settings(community, season, {"analysis": {"enabled": False}}))
    template.assert_not_called()


@pytest.mark.parametrize("error,status", [(LookupError("missing"), 404), (ValueError("wrong scope"), 400)])
def test_compare_bad_scope_has_deliberate_error_status(monkeypatch, error, status):
    monkeypatch.setattr(reddit, "_run_admin_repo_call", AsyncMock(side_effect=error))
    with pytest.raises(HTTPException) as raised:
        asyncio.run(reddit.reddit_run_compare(uuid4(), uuid4()))
    assert raised.value.status_code == status


def test_episode_admission_is_explicitly_unavailable_before_activation(monkeypatch):
    from trr_backend.repositories import reddit_capture

    monkeypatch.setattr(reddit_capture, "enabled", lambda: False)
    route = reddit.reddit_episode_discovery
    with pytest.raises(HTTPException) as raised:
        asyncio.run(route(uuid4(), {"subreddit": "test"}))
    assert raised.value.status_code == 503
