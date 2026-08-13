"""Tests for psycopg2 pool timeout configuration."""

from __future__ import annotations

import pytest

from trr_backend.db import pg as pg_module
from trr_backend.db.pg import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_IDLE_IN_TX_TIMEOUT_MS,
    DEFAULT_STATEMENT_TIMEOUT_MS,
    _build_pool_for_url,
    _is_statement_timeout_error,
    _is_transient_transport_error,
)


@pytest.fixture(autouse=True)
def _pool_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin pool sizing to avoid side-effects from pool-sizing logic."""
    monkeypatch.setenv("TRR_DB_POOL_MINCONN", "1")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "1")
    # Clear any env overrides for the settings under test.
    monkeypatch.delenv("TRR_DB_CONNECT_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("TRR_DB_STATEMENT_TIMEOUT_MS", raising=False)
    monkeypatch.delenv("TRR_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS", raising=False)
    monkeypatch.delenv("TRR_PREVIEW_READ_ONLY", raising=False)


@pytest.fixture()
def captured_kwargs(monkeypatch: pytest.MonkeyPatch, _isolate_non_live_tests: None) -> dict:
    """Replace the guarded pool constructor with a local kwargs capture fake."""
    captured: dict = {}

    def capturing_pool(minconn: int, maxconn: int, **kwargs: object) -> None:
        captured.update(kwargs)
        raise ConnectionError("intentional -- just capturing kwargs")

    monkeypatch.setattr(pg_module, "ThreadedConnectionPool", capturing_pool)
    return captured


TEST_DSN = "postgresql://user:pass@localhost:5432/testdb"


class TestConnectTimeout:
    def test_connect_timeout_as_kwarg(self, captured_kwargs: dict) -> None:
        """connect_timeout must be a top-level DSN kwarg, not in options."""
        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(TEST_DSN)

        assert captured_kwargs["connect_timeout"] == DEFAULT_CONNECT_TIMEOUT_SECONDS
        # Must NOT appear in the options string.
        options = captured_kwargs.get("options", "")
        assert "connect_timeout" not in options

    def test_connect_timeout_env_override(self, monkeypatch: pytest.MonkeyPatch, captured_kwargs: dict) -> None:
        """TRR_DB_CONNECT_TIMEOUT_SECONDS overrides default."""
        monkeypatch.setenv("TRR_DB_CONNECT_TIMEOUT_SECONDS", "5")

        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(TEST_DSN)

        assert captured_kwargs["connect_timeout"] == 5


class TestStatementTimeout:
    def test_statement_timeout_in_options(self, captured_kwargs: dict) -> None:
        """statement_timeout appears in options string."""
        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(TEST_DSN)

        assert f"statement_timeout={DEFAULT_STATEMENT_TIMEOUT_MS}" in captured_kwargs["options"]

    def test_idle_in_transaction_still_in_options(self, captured_kwargs: dict) -> None:
        """idle_in_transaction_session_timeout still present."""
        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(TEST_DSN)

        assert f"idle_in_transaction_session_timeout={DEFAULT_IDLE_IN_TX_TIMEOUT_MS}" in captured_kwargs["options"]

    def test_multi_option_format(self, captured_kwargs: dict) -> None:
        """Options use multi -c format."""
        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(TEST_DSN)

        expected = (
            f"-c idle_in_transaction_session_timeout={DEFAULT_IDLE_IN_TX_TIMEOUT_MS}"
            f" -c statement_timeout={DEFAULT_STATEMENT_TIMEOUT_MS}"
        )
        assert captured_kwargs["options"] == expected

    def test_statement_timeout_env_override(self, monkeypatch: pytest.MonkeyPatch, captured_kwargs: dict) -> None:
        """TRR_DB_STATEMENT_TIMEOUT_MS overrides default."""
        monkeypatch.setenv("TRR_DB_STATEMENT_TIMEOUT_MS", "5000")

        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(TEST_DSN)

        assert "statement_timeout=5000" in captured_kwargs["options"]

    def test_uri_options_are_merged_with_timeout_settings(self, captured_kwargs: dict) -> None:
        """Existing libpq URI options survive the backend timeout defaults."""
        dsn = f"{TEST_DSN}?options=-c%20lock_timeout%3D2500"

        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(dsn)

        assert captured_kwargs["options"] == (
            "-c lock_timeout=2500 "
            f"-c idle_in_transaction_session_timeout={DEFAULT_IDLE_IN_TX_TIMEOUT_MS} "
            f"-c statement_timeout={DEFAULT_STATEMENT_TIMEOUT_MS}"
        )

    @pytest.mark.parametrize(
        "pool_name",
        ("default", "social_profile", "social_control", "social_progress", "health", "session_control"),
    )
    def test_preview_read_only_is_applied_to_every_pool_after_uri_options(
        self,
        monkeypatch: pytest.MonkeyPatch,
        captured_kwargs: dict,
        pool_name: str,
    ) -> None:
        """Preview mode forces read-only after preserving earlier URI options."""
        monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
        dsn = f"{TEST_DSN}?options=-c%20default_transaction_read_only%3Doff"

        with pytest.raises(ConnectionError, match="intentional"):
            _build_pool_for_url(dsn, pool_name=pool_name)

        assert captured_kwargs["options"] == (
            "-c default_transaction_read_only=off "
            f"-c idle_in_transaction_session_timeout={DEFAULT_IDLE_IN_TX_TIMEOUT_MS} "
            f"-c statement_timeout={DEFAULT_STATEMENT_TIMEOUT_MS} "
            "-c default_transaction_read_only=on"
        )


class TestStatementTimeoutDetection:
    def test_statement_timeout_detected(self) -> None:
        """A psycopg2-style statement timeout error is correctly identified."""
        err = Exception("canceling statement due to statement timeout")
        assert _is_statement_timeout_error(err) is True

    def test_transient_error_not_detected_as_timeout(self) -> None:
        """A connection error is not misclassified as a statement timeout."""
        err = Exception("connection reset by peer")
        assert _is_statement_timeout_error(err) is False

    def test_statement_timeout_excluded_from_transient(self) -> None:
        """A statement timeout error must NOT be classified as a transient transport error."""
        err = Exception("canceling statement due to statement timeout")
        assert _is_transient_transport_error(err) is False

    def test_psycopg_connect_timeout_is_transient(self) -> None:
        """A direct-endpoint timeout can fall through to the next configured candidate."""
        err = Exception("connection to server at db.example.test, port 5432 failed: timeout expired")
        assert _is_transient_transport_error(err) is True
