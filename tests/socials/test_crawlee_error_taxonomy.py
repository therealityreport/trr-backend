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


def test_classify_exception_empty_timeout_is_retryable_network() -> None:
    error_code, error_class, retryable = classify_exception(TimeoutError())
    assert error_code == "network"
    assert error_class == "TimeoutError"
    assert retryable is True


def test_classify_exception_preserves_structured_scraper_code_and_retryable() -> None:
    class _WarmupError(RuntimeError):
        error_code = "instagram_posts_warmup_no_cookies"
        retryable = True

    error_code, error_class, retryable = classify_exception(_WarmupError("warmup bridge failed"))
    assert error_code == "instagram_posts_warmup_no_cookies"
    assert error_class == "_WarmupError"
    assert retryable is True


def test_classify_exception_checkpoint_marker_is_auth_terminal() -> None:
    error_code, _, retryable = classify_exception(RuntimeError("instagram_graphql_checkpoint_required"))
    assert error_code == "instagram_graphql_checkpoint_required"
    assert retryable is False


def test_classify_exception_transient_upstream_body_decode_is_retryable() -> None:
    error_code, _, retryable = classify_exception(RuntimeError("upstream response body json decode failed"))
    assert error_code == "parse"
    assert retryable is True


def test_classify_exception_unknown_defaults() -> None:
    error_code, _, retryable = classify_exception(RuntimeError("unexpected fatal condition"))
    assert error_code == "unknown"
    assert retryable is False
