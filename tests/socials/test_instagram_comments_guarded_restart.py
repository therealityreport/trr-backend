"""Unit tests for the Instagram comments guarded-restart pipeline (REVISED §5).

The guarded restart must, under the account advisory lock: cancel the original
run with ``cancelled_by="comments_guarded_restart"``, relaunch a public-relay
run that preserves the original date window / target filter while forcing the
public-only shape (worker cap start 12, batch size 10, no auth probe), stamp
guarded-restart audit fields on the cancelled run, and return the public-only
proof. No real database is used: the DB-touching helpers and the advisory-lock
``pg`` primitives are faked.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

import trr_backend.socials.pipelines.comments.instagram as pipeline


class _FakePg:
    """Minimal pg stand-in: advisory-lock primitives + audit-stamp capture."""

    def __init__(self) -> None:
        self.audit_updates: list[tuple[str, list[Any]]] = []

    @contextmanager
    def db_connection(self, *, label: str = "", pool_name: str = "default"):  # noqa: ARG002
        yield SimpleNamespace(name="lock-conn")

    @contextmanager
    def db_cursor(self, *, conn: Any = None, label: str = ""):  # noqa: ARG002
        yield SimpleNamespace(name="cursor")

    def fetch_one_with_cursor(self, cur: Any, sql: str, params: list[Any]):  # noqa: ARG002
        # pg_try_advisory_lock always succeeds in tests.
        if "pg_try_advisory_lock" in sql:
            return {"locked": True}
        if "pg_advisory_unlock" in sql:
            return {"unlocked": True}
        return {}

    def fetch_one(self, sql: str, params: list[Any], *, conn: Any = None):  # noqa: ARG002
        normalized = " ".join(sql.lower().split())
        if "update social.scrape_runs" in normalized and "guarded_restart_to_run_id" in normalized:
            self.audit_updates.append((normalized, list(params)))
            return {"id": str(params[-1])}
        return {}


def _install_guarded_restart_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    run_config: dict[str, Any],
    run_status: str = "running",
) -> dict[str, Any]:
    fake_pg = _FakePg()
    monkeypatch.setattr(pipeline, "pg", fake_pg)

    captured: dict[str, Any] = {"cancel": None, "start": None, "pg": fake_pg}

    def _fake_load_run_row(*, platform: str, account_handle: str, run_id: str, conn: Any = None):  # noqa: ARG001
        return {
            "id": run_id,
            "run_id": run_id,
            "status": run_status,
            "source_scope": "network",
            "config": dict(run_config),
            "summary": {},
        }

    def _fake_cancel(*, platform: str, account_handle: str, run_id: str, cancelled_by=None, conn=None):  # noqa: ARG001
        captured["cancel"] = {
            "platform": platform,
            "account_handle": account_handle,
            "run_id": run_id,
            "cancelled_by": cancelled_by,
        }
        return {"run_id": run_id, "status": "cancelled", "accepted": True, "cancelled_jobs": 3}

    def _fake_start(platform: str, account_handle: str, **kwargs: Any):
        captured["start"] = {"platform": platform, "account_handle": account_handle, **kwargs}
        return {
            "run_id": "new-run-77777777-7777-7777-7777-777777777777",
            "status": "queued",
            "comments_load_strategy": "public_relay",
            "instagram_access_proof": {"no_cookies": True, "mode": "public_relay"},
        }

    monkeypatch.setattr(pipeline, "_load_social_account_comments_run_row", _fake_load_run_row)
    monkeypatch.setattr(pipeline, "cancel_social_account_comments_run", _fake_cancel)
    monkeypatch.setattr(pipeline, "start_social_account_comments_scrape", _fake_start)
    return captured


def test_guarded_restart_cancels_old_and_starts_public_relay_with_original_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_config = {
        "platform": "instagram",
        "account": "bravotv",
        "mode": "profile",
        "refresh_policy": "stale_or_missing",
        "target_filter": "incomplete",
        "date_start": "2025-03-01T00:00:00+00:00",
        "date_end": "2025-09-01T00:00:00+00:00",
        "comments_load_strategy": "comments_endpoint",
        "comments_worker_count": 4,
        "comments_target_batch_size": 25,
    }
    captured = _install_guarded_restart_fakes(monkeypatch, run_config=run_config)

    result = pipeline.guarded_restart_social_account_comments_run(
        platform="Instagram",
        account_handle="@BravoTV",
        run_id="old-run-11111111-1111-1111-1111-111111111111",
        initiated_by="admin@example.com",
    )

    # Old run cancelled with the guarded-restart marker.
    assert captured["cancel"]["run_id"] == "old-run-11111111-1111-1111-1111-111111111111"
    assert captured["cancel"]["cancelled_by"] == "comments_guarded_restart"
    assert captured["cancel"]["platform"] == "instagram"
    assert captured["cancel"]["account_handle"] == "bravotv"

    # New run is public-relay only with the original window preserved and the
    # public-only worker cap / batch size forced.
    start = captured["start"]
    assert start["comments_load_strategy"] == "public_relay"
    assert start["skip_launch_auth_probe"] is True
    assert start["date_start"] == "2025-03-01T00:00:00+00:00"
    assert start["date_end"] == "2025-09-01T00:00:00+00:00"
    assert start["target_filter"] == "incomplete"
    assert start["comments_worker_count"] == 4
    assert start["comments_target_batch_size"] == 10
    assert start["cancel_active_before_relaunch"] is False

    # Audit fields stamped on the old run summary.
    assert captured["pg"].audit_updates, "guarded restart should stamp audit fields on old run"
    audit_sql, audit_params = captured["pg"].audit_updates[0]
    assert "guarded_restart_to_run_id" in audit_sql
    assert "public_comments_guarded_restart" in audit_params
    assert "new-run-77777777-7777-7777-7777-777777777777" in audit_params

    # Return contract.
    assert result["accepted"] is True
    assert result["old_run_id"] == "old-run-11111111-1111-1111-1111-111111111111"
    assert result["new_run_id"] == "new-run-77777777-7777-7777-7777-777777777777"
    assert result["public_only_proof"]["no_cookies"] is True
    assert result["public_only_proof"]["no_proxy"] is True
    assert result["public_only_proof"]["comments_load_strategy"] == "public_relay"
    assert result["comments_worker_cap_start"] == 4
    assert result["comments_target_batch_size"] == 10
    assert result["date_window"]["date_start"] == "2025-03-01T00:00:00+00:00"
    assert result["date_window"]["date_end"] == "2025-09-01T00:00:00+00:00"
    assert result["date_window"]["used_proof_defaults"] is False
    assert result["cancellation_summary"]["cancelled_jobs"] == 3


def test_guarded_restart_applies_proof_defaults_only_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No date window in original config.
    run_config = {
        "platform": "instagram",
        "account": "bravotv",
        "mode": "profile",
        "target_filter": None,
    }
    captured = _install_guarded_restart_fakes(monkeypatch, run_config=run_config)

    result = pipeline.guarded_restart_social_account_comments_run(
        platform="instagram",
        account_handle="bravotv",
        run_id="old-run-22222222-2222-2222-2222-222222222222",
        use_proof_defaults=True,
    )

    start = captured["start"]
    assert start["date_start"] == "2025-01-01T00:00:00+00:00"
    assert start["date_end"] == "2027-01-01T00:00:00+00:00"
    # Target filter defaults to incomplete when the original run had none.
    assert start["target_filter"] == "incomplete"
    assert result["date_window"]["used_proof_defaults"] is True


def test_guarded_restart_without_proof_request_keeps_window_unbounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_config = {"platform": "instagram", "account": "bravotv", "mode": "profile"}
    captured = _install_guarded_restart_fakes(monkeypatch, run_config=run_config)

    result = pipeline.guarded_restart_social_account_comments_run(
        platform="instagram",
        account_handle="bravotv",
        run_id="old-run-33333333-3333-3333-3333-333333333333",
        use_proof_defaults=False,
    )

    start = captured["start"]
    assert start["date_start"] is None
    assert start["date_end"] is None
    assert result["date_window"]["used_proof_defaults"] is False


def test_guarded_restart_rejects_non_instagram_platform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_guarded_restart_fakes(monkeypatch, run_config={"mode": "profile"})
    with pytest.raises(pipeline.SocialIngestValidationError) as excinfo:
        pipeline.guarded_restart_social_account_comments_run(
            platform="tiktok",
            account_handle="bravotv",
            run_id="run-1",
        )
    assert excinfo.value.code == "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM"


def test_guarded_restart_conflict_when_account_lock_held(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _install_guarded_restart_fakes(monkeypatch, run_config={"mode": "profile"})

    # Force the advisory lock acquisition to fail.
    def _locked_fetch_one_with_cursor(cur: Any, sql: str, params: list[Any]):  # noqa: ARG001
        if "pg_try_advisory_lock" in sql:
            return {"locked": False}
        return {}

    monkeypatch.setattr(captured["pg"], "fetch_one_with_cursor", _locked_fetch_one_with_cursor)

    with pytest.raises(pipeline.SocialIngestConflictError) as excinfo:
        pipeline.guarded_restart_social_account_comments_run(
            platform="instagram",
            account_handle="bravotv",
            run_id="run-1",
        )
    assert excinfo.value.code == "SOCIAL_ACCOUNT_COMMENTS_LAUNCH_IN_PROGRESS"
    # Neither cancel nor start should run when the lock is unavailable.
    assert captured["cancel"] is None
    assert captured["start"] is None
