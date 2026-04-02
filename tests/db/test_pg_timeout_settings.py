"""Tests for psycopg2 pool timeout configuration."""

from __future__ import annotations

import psycopg2.pool
import pytest

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


@pytest.fixture()
def captured_kwargs(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Patch ThreadedConnectionPool.__init__ to capture kwargs without connecting."""
    captured: dict = {}

    def capturing_init(self, minconn, maxconn, **kwargs):
        captured.update(kwargs)
        raise ConnectionError("intentional -- just capturing kwargs")

    monkeypatch.setattr(psycopg2.pool.ThreadedConnectionPool, "__init__", capturing_init)
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
