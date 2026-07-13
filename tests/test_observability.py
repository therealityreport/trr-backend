from __future__ import annotations

import logging
import time
from queue import Queue
from threading import Event, Timer

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


def test_better_stack_queue_preserves_request_trace() -> None:
    record = logging.LogRecord("test.logger", logging.INFO, __file__, 1, "queued", (), None)
    handler = observability.NonBlockingQueueHandler(Queue())
    token = observability.bind_trace_id("trace-queued")
    try:
        prepared = handler.prepare(record)
    finally:
        observability.reset_trace_id(token)

    event = observability._build_better_stack_event(prepared, service_name="trr-backend-api")

    assert event["trace_id"] == "trace-queued"


def test_better_stack_queue_filters_urllib3_before_enqueue() -> None:
    log_queue: Queue[logging.LogRecord] = Queue(maxsize=1)
    handler = observability.NonBlockingQueueHandler(log_queue)

    handler.handle(logging.LogRecord("urllib3", logging.INFO, __file__, 1, "noise", (), None))
    handler.handle(logging.LogRecord("test.logger", logging.INFO, __file__, 2, "signal", (), None))

    queued = log_queue.get_nowait()
    assert queued.name == "test.logger"
    assert queued.getMessage() == "signal"
    assert log_queue.empty()


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
        listener = getattr(observability, "_better_stack_listener", None)
        if listener is not None:
            listener.stop()
            observability._better_stack_listener = None
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)


def test_better_stack_shipping_does_not_block_the_caller(monkeypatch) -> None:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    entered = Event()
    release = Event()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    def blocked_emit(_self, _record) -> None:
        entered.set()
        release.wait(timeout=1)

    monkeypatch.setenv("BETTER_STACK_SOURCE_TOKEN", "source-token")
    monkeypatch.setattr(observability.BetterStackHTTPHandler, "emit", blocked_emit)

    try:
        observability.configure_runtime_observability(service_name="trr-backend-api")
        timer = Timer(0.25, release.set)
        timer.start()
        started_at = time.perf_counter()
        root_logger.info("database checkout")
        elapsed = time.perf_counter() - started_at
        timer.join()

        assert entered.wait(timeout=1)
        assert elapsed < 0.1
    finally:
        release.set()
        listener = getattr(observability, "_better_stack_listener", None)
        if listener is not None:
            listener.stop()
            observability._better_stack_listener = None
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
