"""Tests for Supabase timeout configuration and detection."""

from __future__ import annotations

from unittest.mock import patch

import httpx

from trr_backend.db.admin import (
    get_supabase_http2_enabled,
    get_supabase_timeout_config,
    is_timeout_error,
)


def clear_supabase_caches() -> None:
    """Clear all lru_cache caches for supabase config functions."""
    get_supabase_timeout_config.cache_clear()
    get_supabase_http2_enabled.cache_clear()


class TestTimeoutConfig:
    """Tests for timeout configuration parsing."""

    def test_get_timeout_config_defaults(self) -> None:
        """Test timeout config returns defaults when env vars not set."""
        clear_supabase_caches()
        with patch.dict("os.environ", {}, clear=True):
            postgrest, storage, pool = get_supabase_timeout_config()

            assert postgrest == 15.0
            assert storage == 30.0
            assert pool == 5.0

    def test_get_timeout_config_custom_values(self) -> None:
        """Test timeout config parses custom values from env vars."""
        clear_supabase_caches()
        with patch.dict(
            "os.environ",
            {
                "SUPABASE_POSTGREST_TIMEOUT_SEC": "20",
                "SUPABASE_STORAGE_TIMEOUT_SEC": "60",
                "SUPABASE_HTTP_POOL_TIMEOUT_SEC": "10",
            },
        ):
            postgrest, storage, pool = get_supabase_timeout_config()

            assert postgrest == 20.0
            assert storage == 60.0
            assert pool == 10.0

    def test_get_timeout_config_invalid_values(self) -> None:
        """Test timeout config falls back to defaults for invalid values."""
        clear_supabase_caches()
        with patch.dict(
            "os.environ",
            {
                "SUPABASE_POSTGREST_TIMEOUT_SEC": "invalid",
                "SUPABASE_STORAGE_TIMEOUT_SEC": "-5",
                "SUPABASE_HTTP_POOL_TIMEOUT_SEC": "0",
            },
        ):
            postgrest, storage, pool = get_supabase_timeout_config()

            # Should all fall back to defaults
            assert postgrest == 15.0
            assert storage == 30.0
            assert pool == 5.0


class TestHttp2Config:
    """Tests for HTTP/2 configuration."""

    def test_http2_disabled_by_default(self) -> None:
        """Test HTTP/2 is disabled by default."""
        clear_supabase_caches()
        with patch.dict("os.environ", {}, clear=True):
            assert get_supabase_http2_enabled() is False

    def test_http2_enabled_with_env_var(self) -> None:
        """Test HTTP/2 can be enabled via env var."""
        for value in ("1", "true", "True", "TRUE", "yes", "YES", "on", "ON"):
            clear_supabase_caches()
            with patch.dict("os.environ", {"SUPABASE_HTTP2_ENABLED": value}):
                assert get_supabase_http2_enabled() is True, f"Failed for value: {value}"

    def test_http2_disabled_with_other_values(self) -> None:
        """Test HTTP/2 is disabled for non-truthy values."""
        for value in ("0", "false", "False", "no", "off", "random"):
            clear_supabase_caches()
            with patch.dict("os.environ", {"SUPABASE_HTTP2_ENABLED": value}):
                assert get_supabase_http2_enabled() is False, f"Failed for value: {value}"


class TestTimeoutDetection:
    """Tests for timeout error detection."""

    def test_detects_timeout_exception(self) -> None:
        """Test detection of httpx.TimeoutException."""
        exc = httpx.TimeoutException("Timeout")
        assert is_timeout_error(exc) is True

    def test_detects_connect_timeout(self) -> None:
        """Test detection of httpx.ConnectTimeout."""
        exc = httpx.ConnectTimeout("Connect timeout")
        assert is_timeout_error(exc) is True

    def test_detects_read_timeout(self) -> None:
        """Test detection of httpx.ReadTimeout."""
        exc = httpx.ReadTimeout("Read timeout")
        assert is_timeout_error(exc) is True

    def test_detects_write_timeout(self) -> None:
        """Test detection of httpx.WriteTimeout."""
        exc = httpx.WriteTimeout("Write timeout")
        assert is_timeout_error(exc) is True

    def test_detects_pool_timeout(self) -> None:
        """Test detection of httpx.PoolTimeout."""
        exc = httpx.PoolTimeout("Pool timeout")
        assert is_timeout_error(exc) is True

    def test_detects_wrapped_timeout_via_cause(self) -> None:
        """Test detection of timeout wrapped in another exception via __cause__."""
        timeout_exc = httpx.ReadTimeout("Read timeout")
        wrapper_exc = RuntimeError("Wrapper error")
        wrapper_exc.__cause__ = timeout_exc

        assert is_timeout_error(wrapper_exc) is True

    def test_detects_wrapped_timeout_via_context(self) -> None:
        """Test detection of timeout wrapped in another exception via __context__."""
        timeout_exc = httpx.WriteTimeout("Write timeout")
        wrapper_exc = ValueError("Wrapper error")
        wrapper_exc.__context__ = timeout_exc

        assert is_timeout_error(wrapper_exc) is True

    def test_detects_deeply_nested_timeout(self) -> None:
        """Test detection of timeout nested multiple levels deep."""
        timeout_exc = httpx.ConnectTimeout("Connect timeout")
        middle_exc = RuntimeError("Middle error")
        middle_exc.__cause__ = timeout_exc
        outer_exc = ValueError("Outer error")
        outer_exc.__context__ = middle_exc

        assert is_timeout_error(outer_exc) is True

    def test_non_timeout_error(self) -> None:
        """Test non-timeout errors are not detected as timeouts."""
        exc = ValueError("Not a timeout")
        assert is_timeout_error(exc) is False

    def test_nested_non_timeout_error(self) -> None:
        """Test nested non-timeout errors are not detected as timeouts."""
        inner_exc = KeyError("Inner error")
        outer_exc = RuntimeError("Outer error")
        outer_exc.__cause__ = inner_exc

        assert is_timeout_error(outer_exc) is False
