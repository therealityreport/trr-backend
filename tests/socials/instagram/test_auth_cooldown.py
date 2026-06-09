"""Unit tests for the cross-process account auth cooldown (A4).

These exercise record/read/clear against an in-memory fake that mimics the
``social.account_auth_cooldown`` UPSERT/UPDATE/SELECT semantics, so no live
database is required. The fake is wired in by monkeypatching the ``pg`` functions
that ``auth_cooldown`` imports lazily.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from trr_backend.db import pg
from trr_backend.socials.instagram import auth_cooldown


class _FakeCooldownStore:
    """In-memory stand-in for social.account_auth_cooldown.

    Implements just enough of the two-statement UPSERT in record_auth_block, the
    active-only SELECT in get_active_cooldown, and the guarded UPDATE in
    clear_cooldown to validate the module's behaviour.
    """

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.now = datetime(2026, 6, 7, 12, 0, 0, tzinfo=UTC)

    # -- pg shims -----------------------------------------------------------
    @contextmanager
    def db_connection(self, *, label: str = "write", pool_name: str = "default"):
        assert pool_name == "social_control"
        yield object()

    def execute_returning(
        self,
        query: str,
        params: list[Any] | None = None,
        *,
        conn: Any = None,
    ) -> list[dict[str, Any]]:
        params = list(params or [])
        normalized = " ".join(query.split()).lower()
        if normalized.startswith("insert into"):
            platform, account, error_code, blocker_kind = params[0], params[1], params[2], params[3]
            key = (platform, account)
            existing = self.rows.get(key)
            if existing is None:
                row = {
                    "platform": platform,
                    "account_handle": account,
                    "cooldown_until": None,
                    "consecutive_auth_failures": 1,
                    "last_error_code": error_code,
                    "blocker_kind": blocker_kind,
                }
                self.rows[key] = row
            else:
                existing["consecutive_auth_failures"] += 1
                existing["last_error_code"] = error_code
                # checkpoint is sticky: never downgrade an existing checkpoint.
                if existing["blocker_kind"] != "checkpoint":
                    existing["blocker_kind"] = blocker_kind
                row = existing
            return [{"consecutive_auth_failures": row["consecutive_auth_failures"]}]
        if normalized.startswith("update") and "cooldown_until = now() + make_interval" in normalized:
            delay_seconds, platform, account = float(params[0]), params[1], params[2]
            row = self.rows[(platform, account)]
            row["cooldown_until"] = self.now + timedelta(seconds=delay_seconds)
            return [dict(row)]
        if normalized.startswith("update") and "consecutive_auth_failures = 0" in normalized:
            blocker_kind, platform, account = params[0], params[1], params[2]
            forced = len(params) < 4
            row = self.rows.get((platform, account))
            if row is None:
                return []
            if row["cooldown_until"] is None and row["consecutive_auth_failures"] == 0:
                return []
            if not forced and row["blocker_kind"] == "checkpoint":
                return []
            row["consecutive_auth_failures"] = 0
            row["cooldown_until"] = None
            row["last_error_code"] = None
            row["blocker_kind"] = blocker_kind
            return [{"platform": platform}]
        raise AssertionError(f"unexpected execute_returning query: {normalized}")

    def fetch_one(
        self,
        query: str,
        params: list[Any] | None = None,
        *,
        conn: Any = None,
        pool_name: str = "default",
    ) -> dict[str, Any] | None:
        assert pool_name == "social_control"
        params = list(params or [])
        platform, account = params[0], params[1]
        row = self.rows.get((platform, account))
        if row is None or row["cooldown_until"] is None:
            return None
        if row["cooldown_until"] <= self.now:
            return None
        return dict(row)


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> _FakeCooldownStore:
    fake = _FakeCooldownStore()
    monkeypatch.setattr(pg, "db_connection", fake.db_connection)
    monkeypatch.setattr(pg, "execute_returning", fake.execute_returning)
    monkeypatch.setattr(pg, "fetch_one", fake.fetch_one)
    return fake


def test_record_auth_block_sets_escalating_cooldown(store: _FakeCooldownStore) -> None:
    first = auth_cooldown.record_auth_block("instagram", "acct", "http_401")
    assert first is not None
    assert first.consecutive_auth_failures == 1
    assert first.blocker_kind == "auth"
    assert first.cooldown_until > store.now
    first_delay = (first.cooldown_until - store.now).total_seconds()

    second = auth_cooldown.record_auth_block("instagram", "acct", "http_403")
    assert second is not None
    assert second.consecutive_auth_failures == 2
    second_delay = (second.cooldown_until - store.now).total_seconds()
    # Escalation: the second deadline must be meaningfully further out even after
    # jitter (curve doubles base_delay; jitter is +/-25%).
    assert second_delay > first_delay


def test_get_active_cooldown_returns_only_future_deadline(store: _FakeCooldownStore) -> None:
    assert auth_cooldown.get_active_cooldown("instagram", "acct") is None

    auth_cooldown.record_auth_block("instagram", "acct", "http_401")
    active = auth_cooldown.get_active_cooldown("instagram", "acct")
    assert active is not None
    assert active.account_handle == "acct"

    # Advance time past the deadline → no longer active.
    store.now = active.cooldown_until + timedelta(seconds=1)
    assert auth_cooldown.get_active_cooldown("instagram", "acct") is None


def test_clear_cooldown_resets_auth_block(store: _FakeCooldownStore) -> None:
    auth_cooldown.record_auth_block("instagram", "acct", "http_401")
    assert auth_cooldown.get_active_cooldown("instagram", "acct") is not None

    cleared = auth_cooldown.clear_cooldown("instagram", "acct")
    assert cleared is True
    assert auth_cooldown.get_active_cooldown("instagram", "acct") is None
    # Counter reset → a subsequent block starts again at 1.
    again = auth_cooldown.record_auth_block("instagram", "acct", "http_401")
    assert again is not None
    assert again.consecutive_auth_failures == 1


def test_clear_cooldown_is_noop_on_active_checkpoint(store: _FakeCooldownStore) -> None:
    recorded = auth_cooldown.record_auth_block("instagram", "acct", "redirect_to_checkpoint")
    assert recorded is not None
    assert recorded.blocker_kind == "checkpoint"

    # A4.6: checkpoint is a non-clearing blocker. An incidental success must not
    # clear it without an explicit force (operator intervention).
    assert auth_cooldown.clear_cooldown("instagram", "acct") is False
    assert auth_cooldown.get_active_cooldown("instagram", "acct") is not None

    assert auth_cooldown.clear_cooldown("instagram", "acct", force=True) is True
    assert auth_cooldown.get_active_cooldown("instagram", "acct") is None


def test_checkpoint_is_sticky_and_not_downgraded(store: _FakeCooldownStore) -> None:
    auth_cooldown.record_auth_block("instagram", "acct", "redirect_to_checkpoint")
    # A later plain 401 must not downgrade the blocker kind back to "auth".
    after = auth_cooldown.record_auth_block("instagram", "acct", "http_401")
    assert after is not None
    assert after.blocker_kind == "checkpoint"


def test_force_clear_resets_checkpoint_blocker_before_next_auth_failure(store: _FakeCooldownStore) -> None:
    auth_cooldown.record_auth_block("instagram", "acct", "redirect_to_checkpoint")

    assert auth_cooldown.clear_cooldown("instagram", "acct", force=True) is True

    after = auth_cooldown.record_auth_block("instagram", "acct", "http_401")
    assert after is not None
    assert after.blocker_kind == "auth"
    assert after.last_error_code == "http_401"


def test_classify_blocker_kind() -> None:
    assert auth_cooldown.classify_blocker_kind("http_401") == "auth"
    assert auth_cooldown.classify_blocker_kind("http_403") == "auth"
    assert auth_cooldown.classify_blocker_kind("redirect_to_checkpoint") == "checkpoint"
    assert auth_cooldown.classify_blocker_kind("checkpoint_required") == "checkpoint"
    assert auth_cooldown.classify_blocker_kind("redirect_to_login") == "checkpoint"
    assert auth_cooldown.is_checkpoint_error_code("challenge_required") is True
    assert auth_cooldown.is_checkpoint_error_code("http_401") is False


def test_record_auth_block_fails_open_on_db_error(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def _boom(*, label: str = "write", pool_name: str = "default"):
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(pg, "db_connection", _boom)
    # Must swallow and return None rather than raising into the lane.
    assert auth_cooldown.record_auth_block("instagram", "acct", "http_401") is None


def test_get_active_cooldown_fails_open_on_db_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("db down")

    monkeypatch.setattr(pg, "fetch_one", _boom)
    assert auth_cooldown.get_active_cooldown("instagram", "acct") is None


def test_cooldown_backoff_seconds_respects_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_AUTH_COOLDOWN_BASE_SECONDS", "10")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_AUTH_COOLDOWN_MAX_SECONDS", "100")
    # attempt 1 ~ 10s (+/-25%), capped well under max.
    delay1 = auth_cooldown.cooldown_backoff_seconds(1)
    assert 7.0 <= delay1 <= 13.0
    # attempt 10 would be 10 * 2**9 = 5120s but is capped at 100s (+/-25%).
    delay10 = auth_cooldown.cooldown_backoff_seconds(10)
    assert delay10 <= 125.0
