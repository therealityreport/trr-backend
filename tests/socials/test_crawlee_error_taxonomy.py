from __future__ import annotations

from trr_backend.socials.crawlee_runtime.error_taxonomy import classify_exception


def test_classify_exception_rate_limited_retryable() -> None:
    error_code, error_class, retryable = classify_exception(RuntimeError("HTTP 429 Too Many Requests"))
    assert error_code == "rate_limited"
    assert error_class == "RuntimeError"
    assert retryable is True


def test_classify_exception_auth_not_retryable() -> None:
    error_code, _, retryable = classify_exception(ValueError("login required: unauthorized"))
    assert error_code == "auth"
    assert retryable is False


def test_classify_exception_network_retryable() -> None:
    error_code, _, retryable = classify_exception(ConnectionError("connection timeout to upstream"))
    assert error_code == "network"
    assert retryable is True


def test_classify_exception_parse_not_retryable() -> None:
    error_code, _, retryable = classify_exception(RuntimeError("json decode error"))
    assert error_code == "parse"
    assert retryable is False


def test_classify_exception_unknown_defaults() -> None:
    error_code, _, retryable = classify_exception(RuntimeError("unexpected fatal condition"))
    assert error_code == "unknown"
    assert retryable is False
