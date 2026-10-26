"""Legacy launches cannot bypass the v2 Instagram admission owner."""

import asyncio
import inspect
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from api.routers.socials import profile_comments


@pytest.mark.parametrize(
    "name",
    [
        "post_social_account_comments_scrape_route",
        "post_social_account_comments_audit_cursor_retries_route",
        "post_social_account_comments_run_rebalance_route",
        "post_social_account_comments_run_resume_route",
        "post_social_account_comments_run_repair_auth_route",
        "post_social_account_comments_run_public_recovery_route",
        "post_social_account_comments_run_authenticated_followup_route",
        "post_social_account_comments_run_guarded_restart_route",
    ],
)
def test_legacy_instagram_mutation_is_rejected_before_dispatch(monkeypatch, name):
    monkeypatch.setattr("trr_backend.ingestion.composition.v2_owns_instagram_launches", lambda: True)
    function = getattr(profile_comments, name)
    values = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "run_id": uuid4(),
        "user": {},
        "_": {},
        "payload": SimpleNamespace(dry_run=False),
        "background_tasks": None,
        "dry_run": False,
        "use_proof_defaults": False,
    }
    kwargs = {key: values[key] for key in inspect.signature(function).parameters if key in values}
    with pytest.raises(HTTPException) as caught:
        result = function(**kwargs)
        if inspect.isawaitable(result):
            asyncio.run(result)
    assert caught.value.status_code == 409
    assert caught.value.detail["code"] == "instagram_v2_owns_launches"
