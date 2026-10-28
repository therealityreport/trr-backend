from __future__ import annotations

import pytest

from trr_backend.ingestion import composition
from trr_backend.ingestion.command_service import ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict


class _Cursor:
    def __init__(self, active=0, recent=0, legacy_active=0):
        self.row = {"active": active, "recent": recent, "legacy_active": legacy_active}
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def fetchone(self):
        return self.row


def _policy(accounts=("bravotv",), maximum=3):
    return composition.AccountRunPolicy(maximum, account_source=lambda: frozenset(accounts))


@pytest.mark.parametrize(
    ("action", "scope"),
    [
        ("backfill", "full_history"),
        ("backfill", "bounded_window"),
        ("sync_newer", "head_gap"),
        ("sync_recent", "recent_window"),
        ("resume_tail", "full_history"),
        # Capacity sees the raw payload; its default backfill_scope is ignored for sync actions.
        ("sync_newer", "full_history"),
        ("sync_recent", "full_history"),
    ],
)
def test_account_policy_accepts_supported_action_scopes(action, scope):
    _policy().validate_scope("bravotv", action=action, scope=scope, date_start=None, date_end=None)


@pytest.mark.parametrize(
    ("action", "scope"),
    [("backfill", "head_gap"), ("sync_newer", "recent_window"), ("delete_everything", "full_history")],
)
def test_account_policy_rejects_mismatched_or_unknown_actions(action, scope):
    with pytest.raises(Conflict):
        _policy().validate_scope("bravotv", action=action, scope=scope, date_start=None, date_end=None)


def test_account_policy_rejects_accounts_outside_active_shared_sources():
    with pytest.raises(Conflict, match="active shared Instagram source"):
        _policy().validate_scope(
            "someone_else", action="backfill", scope="full_history", date_start=None, date_end=None
        )


def test_unreadable_account_source_admits_nobody():
    def broken():
        raise RuntimeError("database unavailable")

    policy = composition.AccountRunPolicy(3, account_source=broken)
    assert policy.allows_account("bravotv") is False


def test_counts_scope_to_the_account_and_ignore_cancelled_runs():
    cursor = _Cursor(active=0, recent=1)
    _policy().counts(cursor, including_current=False, account="bravotv")
    sql, params = cursor.calls[0]
    assert "account = %s" in sql and "social.scrape_jobs" in sql
    assert params == (["queued", "running", "blocked"], ["completed", "cancelled", "failed"], "bravotv", "bravotv")


def test_an_active_legacy_job_blocks_a_v2_launch():
    with pytest.raises(Conflict, match="older-system"):
        _policy().counts(_Cursor(legacy_active=1), including_current=False, account="bravotv")


@pytest.mark.parametrize(
    ("enabled", "mode", "expected"),
    [("true", "accounts", True), ("true", "canary", False), ("", "accounts", False), ("false", "accounts", False)],
)
def test_v2_owns_instagram_launches_only_in_enabled_account_mode(monkeypatch, enabled, mode, expected):
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_ENABLED", enabled)
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_POLICY", mode)
    assert composition.v2_owns_instagram_launches() is expected


def test_one_active_run_per_account():
    with pytest.raises(Conflict, match="already has an active"):
        _policy().counts(_Cursor(active=1), including_current=False, account="bravotv")
    # The run being created inside the guarded transaction is allowed.
    _policy().counts(_Cursor(active=1), including_current=True, account="bravotv")
    with pytest.raises(Conflict, match="already has an active"):
        _policy().counts(_Cursor(active=2), including_current=True, account="bravotv")


def test_daily_run_limit_per_account():
    _policy(maximum=3).counts(_Cursor(recent=2), including_current=False, account="bravotv")
    with pytest.raises(Conflict, match="Daily"):
        _policy(maximum=3).counts(_Cursor(recent=3), including_current=False, account="bravotv")
    _policy(maximum=3).counts(_Cursor(recent=3), including_current=True, account="bravotv")
    with pytest.raises(Conflict, match="Daily"):
        _policy(maximum=3).counts(_Cursor(recent=4), including_current=True, account="bravotv")


def test_counts_require_an_account():
    with pytest.raises(Conflict):
        _policy().counts(_Cursor(), including_current=False)


def test_policy_mode_defaults_to_existing_canary(monkeypatch):
    monkeypatch.delenv("TRR_INSTAGRAM_INGESTION_POLICY", raising=False)
    sentinel = object()
    monkeypatch.setattr(composition.CanaryPolicy, "configured", classmethod(lambda cls: sentinel))
    assert composition.configured_run_policy() is sentinel


def test_policy_mode_accounts_and_limits(monkeypatch):
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_POLICY", "accounts")
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_MAX_RUNS_PER_DAY", "6")
    policy = composition.configured_run_policy()
    assert isinstance(policy, composition.AccountRunPolicy)
    assert policy.max_runs_per_day == 6
    for bad in ("0", "49", "many"):
        monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_MAX_RUNS_PER_DAY", bad)
        with pytest.raises(ServiceUnavailableError):
            composition.configured_run_policy()


def test_unknown_policy_mode_fails_closed(monkeypatch):
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_POLICY", "everything")
    with pytest.raises(ServiceUnavailableError):
        composition.configured_run_policy()


def test_legacy_catalog_routes_reject_instagram_only_while_v2_owns_launches(monkeypatch):
    from fastapi import HTTPException

    from api.routers.socials.catalog_backfill import _reject_legacy_instagram_launch

    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_ENABLED", "true")
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_POLICY", "accounts")
    with pytest.raises(HTTPException) as caught:
        _reject_legacy_instagram_launch("Instagram")
    assert caught.value.status_code == 409
    assert caught.value.detail["code"] == "instagram_v2_owns_launches"  # pyright: ignore[reportArgumentType]
    _reject_legacy_instagram_launch("tiktok")
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_POLICY", "canary")
    _reject_legacy_instagram_launch("instagram")


class _Capabilities:
    def __init__(self, account, enabled=True, actions=("backfill", "sync_recent", "sync_newer", "resume_tail")):
        self.account, self.enabled, self.supported_actions = account, enabled, list(actions)

    def model_copy(self, *, update):
        copy = _Capabilities(self.account, self.enabled, self.supported_actions)
        for key, value in update.items():
            setattr(copy, key, value)
        return copy


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (_policy(), ["backfill", "sync_recent", "sync_newer", "resume_tail"]),
        (composition.CanaryPolicy(frozenset({"bravotv"}), 1), ["backfill"]),
    ],
)
def test_capabilities_follow_the_policy(monkeypatch, policy, expected):
    service = object.__new__(composition.CanaryCommandService)
    service.store = type("Store", (), {"policy": policy})()  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setattr(composition.CommandService, "capabilities", lambda self, handle: _Capabilities(handle))
    assert service.capabilities("bravotv").supported_actions == expected
    blocked = service.capabilities("someone_else")
    assert blocked.enabled is False and blocked.supported_actions == []
