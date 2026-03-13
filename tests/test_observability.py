from __future__ import annotations

import logging

from trr_backend import observability


def test_build_better_stack_event_includes_trace_and_service(monkeypatch) -> None:
    monkeypatch.setenv("TRR_ENV", "staging")
    token = observability.bind_trace_id("trace-123")
    try:
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname=__file__,
            lineno=42,
            msg="hello %s",
            args=("world",),
            exc_info=None,
        )

        event = observability._build_better_stack_event(record, service_name="trr-backend-api")
    finally:
        observability.reset_trace_id(token)

    assert event["message"] == "hello world"
    assert event["service"] == "trr-backend-api"
    assert event["trace_id"] == "trace-123"
    assert event["environment"] == "staging"
    assert event["level"] == "INFO"


def test_configure_runtime_observability_adds_better_stack_handler(monkeypatch) -> None:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    monkeypatch.setenv("BETTER_STACK_SOURCE_TOKEN", "source-token")
    monkeypatch.setenv("BETTER_STACK_INGESTING_HOST", "logs.example.com")
    monkeypatch.setenv("TRR_LOG_LEVEL", "DEBUG")

    try:
        observability.configure_runtime_observability(service_name="trr-backend-modal-jobs")

        handler_names = {getattr(handler, "name", "") for handler in root_logger.handlers}
        assert "trr-stream" in handler_names
        assert "trr-better-stack" in handler_names
        assert root_logger.level == logging.DEBUG
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
