"""Tests for startup validation fail-fast lane enforcement in api/main.py."""

from __future__ import annotations

import pytest

from trr_backend.db import connection


@pytest.fixture(autouse=True)
def _clear_caches():
    """Clear lru_cache on resolution functions before each test."""
    connection.resolve_database_url_candidate_details.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()
    yield
    connection.resolve_database_url_candidate_details.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()


@pytest.fixture(autouse=True)
def _set_auth_envs(monkeypatch):
    """Set required auth envs so tests don't fail on auth validation."""
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "test-secret")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-jwt-secret")


@pytest.fixture()
def _force_deployed_runtime(monkeypatch):
    """Delete all env vars that _is_local_or_dev_runtime() checks so tests run as deployed."""
    for var in (
        "CI",
        "GITHUB_ACTIONS",
        "APP_ENV",
        "ENVIRONMENT",
        "TRR_ENV",
        "TRR_ENVIRONMENT",
        "PYTHON_ENV",
        "TRR_LOCAL_DEV",
    ):
        monkeypatch.delenv(var, raising=False)


class TestStartupLaneValidation:
    """Validate that _validate_startup_config enforces connection lane rules."""

    def test_session_lane_allowed(self, monkeypatch):
        """Session-mode pooler on :5432 is the expected runtime lane."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://postgres.abcref:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
        )
        # Should NOT raise
        _validate_startup_config()

    @pytest.mark.usefixtures("_force_deployed_runtime")
    def test_transaction_lane_allowed_for_explicit_flight_test(self, monkeypatch):
        """Transaction-mode pooler requires an explicit transaction URL and flight-test flag."""
        from api.main import _validate_startup_config

        monkeypatch.setenv("TRR_DB_RUNTIME_LANE", "transaction")
        monkeypatch.setenv("TRR_DB_TRANSACTION_FLIGHT_TEST", "1")
        monkeypatch.setenv(
            "TRR_DB_TRANSACTION_URL",
            "postgresql://postgres.abcref:pw@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
        )
        monkeypatch.setenv(
            "TRR_DB_SESSION_URL",
            "postgresql://postgres.abcref:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
        )

        _validate_startup_config()

    def test_local_lane_allowed(self, monkeypatch):
        """Local Postgres is always allowed."""
        from api.main import _validate_startup_config

        monkeypatch.setenv("TRR_DB_URL", "postgresql://postgres:postgres@localhost:54322/postgres")
        # Should NOT raise
        _validate_startup_config()

    @pytest.mark.usefixtures("_force_deployed_runtime")
    def test_transaction_lane_rejected_in_deployed(self, monkeypatch):
        """Transaction-mode pooler (:6543) must fail-fast in deployed runtime."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://postgres.abcref:pw@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
        )

        with pytest.raises(RuntimeError, match="transaction"):
            _validate_startup_config()

    @pytest.mark.usefixtures("_force_deployed_runtime")
    def test_direct_lane_rejected_in_deployed(self, monkeypatch):
        """Direct-host DSN must fail-fast in deployed runtime."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://postgres:pw@db.abcref.supabase.co:5432/postgres",
        )

        with pytest.raises(RuntimeError, match="direct"):
            _validate_startup_config()

    @pytest.mark.usefixtures("_force_deployed_runtime")
    def test_direct_source_rejected_in_deployed_even_with_session_url(self, monkeypatch):
        """TRR_DB_DIRECT_URL is local-only, regardless of URL shape."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_DIRECT_URL",
            "postgresql://postgres.abcref:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
        )

        with pytest.raises(RuntimeError, match="TRR_DB_DIRECT_URL"):
            _validate_startup_config()

    def test_direct_lane_allowed_in_local_dev_with_explicit_direct_source(self, monkeypatch):
        """Direct-host DSN is allowed locally only through TRR_DB_DIRECT_URL."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_DIRECT_URL",
            "postgresql://postgres:pw@db.abcref.supabase.co:5432/postgres",
        )
        monkeypatch.setenv("TRR_LOCAL_DEV", "1")

        _validate_startup_config()

    def test_direct_lane_rejected_in_local_dev_from_compatibility_source(self, monkeypatch):
        """Direct-host DSN must not be smuggled through TRR_DB_URL."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://postgres:pw@db.abcref.supabase.co:5432/postgres",
        )
        monkeypatch.setenv("TRR_LOCAL_DEV", "1")

        with pytest.raises(RuntimeError, match="direct"):
            _validate_startup_config()

    @pytest.mark.usefixtures("_force_deployed_runtime")
    def test_other_lane_rejected_in_deployed(self, monkeypatch):
        """Other-classified DSN must fail-fast in deployed runtime."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://user:pw@some-random-host.example.com:5432/db",
        )

        with pytest.raises(RuntimeError, match="other"):
            _validate_startup_config()

    @pytest.mark.usefixtures("_force_deployed_runtime")
    def test_pooler_ambiguous_port_rejected_in_deployed(self, monkeypatch):
        """Pooler on non-standard port must fail-fast in deployed runtime."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://postgres:pw@aws-0-us-east-1.pooler.supabase.com:9999/postgres",
        )

        with pytest.raises(RuntimeError, match="pooler"):
            _validate_startup_config()

    def test_other_lane_rejected_in_local_dev(self, monkeypatch):
        """Other-classified DSN must fail-fast even in local/dev — grace period closed."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://user:pw@some-random-host.example.com:5432/db",
        )
        # Ensure IS local/dev
        monkeypatch.setenv("TRR_LOCAL_DEV", "1")

        with pytest.raises(RuntimeError, match="other"):
            _validate_startup_config()

    def test_no_candidates_rejected(self, monkeypatch):
        """No DB candidates at all must fail-fast."""
        from api.main import _validate_startup_config

        monkeypatch.delenv("TRR_DB_URL", raising=False)
        monkeypatch.delenv("TRR_DB_FALLBACK_URL", raising=False)

        with pytest.raises(RuntimeError, match="No database URL candidates"):
            _validate_startup_config()

    def test_transaction_lane_rejected_even_in_local_dev(self, monkeypatch):
        """Transaction-mode pooler (:6543) must fail-fast even in local/dev — no grace period."""
        from api.main import _validate_startup_config

        monkeypatch.setenv(
            "TRR_DB_URL",
            "postgresql://postgres.abcref:pw@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
        )
        # Ensure IS local/dev
        monkeypatch.setenv("TRR_LOCAL_DEV", "1")

        with pytest.raises(RuntimeError, match="transaction"):
            _validate_startup_config()
