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
            asyncio.run(result)  # pyright: ignore[reportArgumentType]
    assert caught.value.status_code == 409
    assert caught.value.detail["code"] == "instagram_v2_owns_launches"  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize(
    "module_name,function_name,kwargs",
    [
        (
            "trr_backend.socials.pipelines.comments.instagram",
            "start_social_account_comments_scrape",
            {"mode": "profile"},
        ),
        ("trr_backend.socials.pipelines.account_catalog.launch", "begin_social_account_catalog_backfill_launch", {}),
        ("trr_backend.socials.pipelines.account_catalog.launch", "start_social_account_catalog_backfill", {}),
    ],
)
def test_internal_new_run_helpers_reject_v2_ownership_before_probes(monkeypatch, module_name, function_name, kwargs):
    import importlib

    module = importlib.import_module(module_name)
    monkeypatch.setattr("trr_backend.ingestion.composition.v2_owns_instagram_launches", lambda: True)
    monkeypatch.setattr(module, "_sync_core_overrides", lambda: None)

    class LegacyConflictError(ValueError):
        def __init__(self, code, message):
            self.code = code
            super().__init__(message)

    monkeypatch.setattr(module, "SocialIngestConflictError", LegacyConflictError, raising=False)
    monkeypatch.setattr(
        module,
        "_normalize_social_account_profile_platform",
        lambda *args: pytest.fail("Legacy admission/probes were reached"),
        raising=False,
    )
    with pytest.raises(LegacyConflictError) as caught:
        getattr(module, function_name)(" Instagram ", "bravotv", **kwargs)
    assert caught.value.code == "INSTAGRAM_V2_OWNS_LAUNCHES"


@pytest.mark.parametrize("enabled,platform", [(False, "instagram"), (True, "tiktok")])
@pytest.mark.parametrize(
    "module_name,function_name,kwargs",
    [
        (
            "trr_backend.socials.pipelines.comments.instagram",
            "start_social_account_comments_scrape",
            {"mode": "profile"},
        ),
        ("trr_backend.socials.pipelines.account_catalog.launch", "begin_social_account_catalog_backfill_launch", {}),
        ("trr_backend.socials.pipelines.account_catalog.launch", "start_social_account_catalog_backfill", {}),
    ],
)
def test_helper_guard_preserves_other_configuration(monkeypatch, module_name, function_name, kwargs, enabled, platform):
    import importlib

    module = importlib.import_module(module_name)
    monkeypatch.setattr("trr_backend.ingestion.composition.v2_owns_instagram_launches", lambda: enabled)
    monkeypatch.setattr(module, "_sync_core_overrides", lambda: None)

    class ExistingValidationReachedError(Exception):
        pass

    def existing_validation(*args):
        raise ExistingValidationReachedError

    monkeypatch.setattr(module, "_normalize_social_account_profile_platform", existing_validation, raising=False)
    with pytest.raises(ExistingValidationReachedError):
        getattr(module, function_name)(platform, "bravotv", **kwargs)
