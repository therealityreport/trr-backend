"""Bounded transport concurrency with generator transitions owned by one caller.

Branch generators own parsing, canonical commits and pagination decisions. Tasks
only pace, wait or transport immutable requests. A transport slot is not released
until its response has been sent to the generator and its synchronous commit has
finished. Pacing is shielded and drained: a cancellation cannot abandon a thread
that the pacing callback is awaiting. Callbacks must honor their bounded deadline.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import sys
import tempfile
import time
from collections import deque
from collections.abc import Awaitable, Callable, Generator, Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass, field
from functools import partial
from typing import Any


class ReplyDeadlineExceeded(TimeoutError):  # noqa: N818 - public scheduler interface
    """The owner must retain unresolved progress when no further send is allowed."""


@dataclass(frozen=True)
class ReplyRequest:
    parent_id: str
    doc_id: str
    sort_order: str
    page_size: int
    cursor: str | None = None
    owner_generation: int = 0
    deadline: float | None = None
    public_cookie_snapshot: tuple[tuple[str, str, str, str], ...] = field(default=(), repr=False)
    friendly_name: str = ""
    request_data: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    request_headers: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    request_permit: Any = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.parent_id or not self.doc_id or type(self.page_size) is not int or not 1 <= self.page_size <= 50:
            raise ValueError("Reply request identity and page size are required")
        if type(self.owner_generation) is not int or self.owner_generation < 0:
            raise ValueError("Invalid reply owner generation")
        _validate_deadline(self.deadline)
        for name, width in (("public_cookie_snapshot", 4), ("request_data", 2), ("request_headers", 2)):
            values = tuple(tuple(item) for item in getattr(self, name))
            if any(len(item) != width or any(not isinstance(value, str) for value in item) for item in values):
                raise ValueError("Request snapshots must contain immutable string tuples")
            object.__setattr__(self, name, values)


@dataclass(frozen=True)
class Delay:
    seconds: float
    deadline: float | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.seconds) or self.seconds < 0:
            raise ValueError("Invalid reply delay")
        _validate_deadline(self.deadline)


def _validate_deadline(deadline: float | None) -> None:
    if deadline is not None and not math.isfinite(deadline):
        raise ValueError("Reply deadline must be finite")


@dataclass
class SchedulerTelemetry:
    requests: int = 0
    network_seconds: float = 0.0
    pacing_seconds: float = 0.0
    queue_seconds: float = 0.0
    owner_seconds: float = 0.0
    max_outstanding: int = 0
    completed_queue_seconds: float = 0.0


class DiskResponseSpool:
    """Disk-only waiting body, with no retained in-memory response buffer.

    Transport should write streamed chunks; caller-created input buffers are outside
    this helper. ``json`` is for the serialized owner after completion, never HTTP
    tasks. It materializes parsed content, not an additional waiting response buffer.
    No body is truncated. ``close`` also closes/removes the anonymous temporary file.
    """

    MAX_MEMORY_BYTES = 8 * 1024 * 1024

    class ProtocolLimitError(ValueError):
        """A retained response exceeded a fixed parsing/transport bound."""

        def __init__(self, reason: str) -> None:
            self.reason = reason
            super().__init__(reason)

    def __init__(self) -> None:
        self._file = tempfile.TemporaryFile(mode="w+b", buffering=0)
        self.size = 0

    @property
    def closed(self) -> bool:
        return self._file.closed

    def write(self, chunk: bytes) -> None:
        remaining = memoryview(chunk)
        while remaining:
            written = self._file.write(remaining)
            if not written:
                raise OSError("Response spool made no write progress")
            self.size += written
            remaining = remaining[written:]

    class _JSONReader:
        """Bounded raw UTF-8 reader; returned strings are semantic output."""

        def __init__(self, file, limit_error):
            import codecs

            self.file, self.limit_error = file, limit_error
            self.decoder = codecs.getincrementaldecoder("utf-8")()
            self.buffer, self.position, self.eof = "", 0, False
            self.peak_raw_buffer_bytes = 0

        def peek(self):
            import sys

            while self.position == len(self.buffer) and not self.eof:
                raw = self.file.read(64 * 1024)
                self.eof = not raw
                self.buffer = self.decoder.decode(raw, final=self.eof)
                self.position = 0
                self.peak_raw_buffer_bytes = max(
                    self.peak_raw_buffer_bytes, sys.getsizeof(raw) + sys.getsizeof(self.buffer)
                )
            return self.buffer[self.position : self.position + 1]

        def take(self):
            value = self.peek()
            self.position += bool(value)
            return value

        def whitespace(self):
            while self.peek() and self.peek() in " \t\r\n":
                # Consume a whole bounded whitespace run without growing it.
                tail = self.buffer[self.position :]
                self.position += len(tail) - len(tail.lstrip(" \t\r\n"))

        def exact(self, text):
            if any(self.take() != expected for expected in text):
                raise ValueError("Invalid reply JSON response")

        def string(self):
            import io
            import re

            self.exact('"')
            output = io.StringIO()
            special = re.compile(r'["\\\x00-\x1f]')
            escapes = {'"': '"', "\\": "\\", "/": "/", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}
            try:
                while self.peek():
                    match = special.search(self.buffer, self.position)
                    end = match.start() if match else len(self.buffer)
                    output.write(self.buffer[self.position : end])
                    self.position = end
                    if match is None:
                        continue
                    char = self.take()
                    if char == '"':
                        return output.getvalue()
                    if char != "\\":
                        raise ValueError("Unescaped JSON control character")
                    escape = self.take()
                    if escape in escapes:
                        output.write(escapes[escape])
                    elif escape == "u":
                        digits = "".join(self.take() for _ in range(4))
                        if len(digits) != 4 or any(c not in "0123456789abcdefABCDEF" for c in digits):
                            raise ValueError("Invalid JSON Unicode escape")
                        code = int(digits, 16)
                        if 0xD800 <= code <= 0xDBFF:
                            # Consume a following escape only when it is a low
                            # surrogate. Otherwise preserve the lone surrogate,
                            # matching Python's standard JSON decoder.
                            output.write(chr(code))
                        elif 0xDC00 <= code <= 0xDFFF:
                            current = output.tell()
                            if current:
                                output.seek(current - 1)
                                previous = output.read(1)
                                if previous and 0xD800 <= ord(previous) <= 0xDBFF:
                                    output.seek(current - 1)
                                    output.write(chr(0x10000 + ((ord(previous) - 0xD800) << 10) + code - 0xDC00))
                                else:
                                    output.seek(current)
                                    output.write(chr(code))
                            else:
                                output.write(chr(code))
                        else:
                            output.write(chr(code))
                    else:
                        raise ValueError("Invalid JSON escape")
                raise ValueError("Unterminated reply JSON string")
            finally:
                output.close()

        def value(self, depth=0):
            import json

            self.whitespace()
            first = self.peek()
            if first == '"':
                return self.string()
            if first in ("{", "["):
                if depth >= 128:
                    raise self.limit_error("json_nesting_limit")
                self.take()
                is_object = first == "{"
                result = {} if is_object else []
                closing = "}" if is_object else "]"
                self.whitespace()
                if self.peek() == closing:
                    self.take()
                    return result
                while True:
                    self.whitespace()
                    if is_object:
                        key = self.string()
                        self.whitespace()
                        self.exact(":")
                        result[key] = self.value(depth + 1)
                    else:
                        result.append(self.value(depth + 1))
                    self.whitespace()
                    delimiter = self.take()
                    if delimiter == closing:
                        return result
                    if delimiter != ",":
                        raise ValueError("Invalid reply JSON delimiter")
            for literal, value in (("true", True), ("false", False), ("null", None)):
                if first == literal[0]:
                    self.exact(literal)
                    return value
            # JSON numeric conversion is bounded separately from arbitrarily
            # long strings. The standard Python decoder also rejects integers
            # above its configured digit limit. No raw string token is retained.
            number = ""
            while self.peek() and self.peek() in "-+0123456789.eE":
                number += self.take()
                if len(number) > 4096:
                    raise self.limit_error("json_number_limit")
            if not number:
                raise ValueError("Invalid reply JSON value")
            return json.loads(number)

    def json(self) -> Any:
        self._file.seek(0)
        offset = 0
        while chunk := self._file.read(64 * 1024):
            stripped = chunk.lstrip(b" \t\r\n")
            offset += len(chunk) - len(stripped)
            if stripped:
                break
        self._file.seek(offset)
        if self._file.read(9) == b"for (;;);":
            offset += 9
        self._file.seek(offset)
        reader = self._JSONReader(self._file, self.ProtocolLimitError)
        try:
            value = reader.value()
            reader.whitespace()
            if reader.peek():
                raise ValueError("Trailing reply JSON content")
            return value
        finally:
            self.raw_buffer_peak_bytes = reader.peak_raw_buffer_bytes

    def iter_chunks(self, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        if not 1 <= chunk_size <= self.MAX_MEMORY_BYTES:
            raise ValueError("Invalid spool read buffer size")
        self._file.seek(0)
        while chunk := self._file.read(chunk_size):
            yield chunk

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> DiskResponseSpool:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    @classmethod
    def from_chunks(cls, chunks: Iterable[bytes]) -> DiskResponseSpool:
        spool = cls()
        try:
            for chunk in chunks:
                spool.write(chunk)
            return spool
        except BaseException:
            spool.close()
            raise


async def _drain_pacing(callback: Awaitable[Any]) -> Any:
    """Shield the whole callback, including any to_thread work it awaits."""
    task = asyncio.ensure_future(callback)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        # Retrieve any callback error without replacing the original cancellation.
        if not task.cancelled():
            task.exception()
        raise


async def run_reply_lanes(
    lanes: Mapping[str, Generator[ReplyRequest | Delay, Any, Any]],
    *,
    request: Callable[[ReplyRequest], Awaitable[Any]],
    pace: Callable[[ReplyRequest], Awaitable[Any]],
    before_dispatch: Callable[[ReplyRequest], ReplyRequest | None] | None = None,
    concurrency: int = 1,
    stop: Callable[[], bool] | None = None,
    telemetry: SchedulerTelemetry | None = None,
    close_response: Callable[[Any], Any] | None = None,
    drain_received_on_stop: bool = False,
    drain_received_after_error: Callable[[BaseException], bool] | None = None,
    inspect_received: Callable[[list[Any]], None] | None = None,
    clock: Callable[[], float] = time.monotonic,
    offload_owner: bool = False,
) -> dict[str, Any]:
    """Run synchronous branch generators; return only finished lane values.

    Requests are sent/raised into their generator in transport completion order.
        ``Delay`` sends True after its wait. Expired sends/waits throw
    ``ReplyDeadlineExceeded`` into the owner, allowing its normal failure checkpoint.
    The before-dispatch callback runs on the event loop after pacing, between
    owner transitions. It may refresh an
    immutable descriptor, and may return None to retain the original descriptor.
    On stop/error/cancellation every task is cancelled and awaited, every generator
        closed, and every successful unconsumed response closed. When explicitly
        requested, a boolean stop first commits already completed successful responses
        through their owner. A typed source-stop classifier may also permit this drain;
        ownership and persistence failures must never qualify. Transport callbacks must
    close partially constructed responses when they themselves fail or are cancelled.
    """
    if type(concurrency) is not int or concurrency not in (1, 2):
        raise ValueError("Reply concurrency must be 1 or 2")
    metrics = telemetry if telemetry is not None else SchedulerTelemetry()
    results: dict[str, Any] = {}
    ready: deque[tuple[str, ReplyRequest | Delay, float]] = deque()
    pending: dict[asyncio.Task, tuple[str, str, ReplyRequest | Delay]] = {}
    completed: asyncio.PriorityQueue[tuple[int, int, asyncio.Task]] = asyncio.PriorityQueue()
    completion_order: dict[asyncio.Task, tuple[int, float]] = {}
    sequence = 0
    unstarted = iter(lanes)
    active: set[str] = set()
    parent_lanes: dict[str, str] = {}
    halted = False
    owner_executor = (
        ThreadPoolExecutor(max_workers=1, thread_name_prefix="instagram-reply-owner") if offload_owner else None
    )
    owner_context = copy_context() if offload_owner else None

    async def owner_call(callback, *args, **kwargs):
        if owner_executor is None:
            return callback(*args, **kwargs)
        # One persistent context also keeps generator-held ContextVar tokens
        # valid across advances and close. Cancellation joins a committing
        # transition before any response, generator or executor can be closed.
        future = asyncio.get_running_loop().run_in_executor(
            owner_executor, owner_context.run, partial(callback, *args, **kwargs)
        )
        return await _drain_pacing(future)

    def should_stop() -> bool:
        return bool(stop and stop())

    def transition(key: str, value: Any, error: BaseException | None, initial: bool):
        branch = lanes[key]
        try:
            item = next(branch) if initial else branch.throw(error) if error is not None else branch.send(value)
            return False, item
        except StopIteration as finished:
            # StopIteration must not cross an asyncio Future boundary.
            return True, finished.value

    async def advance(
        key: str, value: Any = None, error: BaseException | None = None, *, initial: bool = False
    ) -> None:
        nonlocal halted
        started = clock()
        try:
            finished, item = await owner_call(transition, key, value, error, initial)
            # Scheduler state stays on the event loop, between owner transitions.
            if finished:
                results[key] = item
                active.discard(key)
            else:
                if not isinstance(item, (ReplyRequest, Delay)):
                    raise TypeError("Reply generators must yield ReplyRequest or Delay")
                if isinstance(item, ReplyRequest):
                    previous = parent_lanes.setdefault(item.parent_id, key)
                    if previous != key:
                        raise ValueError("A parent may belong to only one reply lane")
                ready.append((key, item, clock()))
        finally:
            metrics.owner_seconds += max(0.0, clock() - started)
        halted = should_stop()

    def launch(key: str, kind: str, item: ReplyRequest | Delay, awaitable: Awaitable[Any]) -> None:
        task = asyncio.create_task(awaitable)
        pending[task] = key, kind, item

        def finished(completed_task: asyncio.Task) -> None:
            nonlocal sequence
            sequence += 1
            completion_order[completed_task] = sequence, clock()
            completed.put_nowait((0 if kind == "request" else 1, sequence, completed_task))

        task.add_done_callback(finished)
        metrics.max_outstanding = max(
            metrics.max_outstanding, sum(phase != "delay" for _, phase, _ in pending.values())
        )

    async def paced(item: ReplyRequest) -> Any:
        started = clock()
        try:
            return await _drain_pacing(pace(item))
        finally:
            metrics.pacing_seconds += max(0.0, clock() - started)

    async def transported(item: ReplyRequest) -> Any:
        started = clock()
        try:
            remaining = None if item.deadline is None else max(0.0, item.deadline - clock())
            try:
                async with asyncio.timeout(remaining):
                    return await request(item)
            except TimeoutError:
                raise ReplyDeadlineExceeded("Reply response deadline expired") from None
        except Exception as error:
            error._instagram_request_permit = item.request_permit
            raise
        finally:
            metrics.network_seconds += max(0.0, clock() - started)

    async def delayed(item: Delay) -> bool:
        remaining = item.seconds
        if item.deadline is not None:
            remaining = min(remaining, max(0.0, item.deadline - clock()))
        await asyncio.sleep(remaining)
        if item.deadline is not None and clock() >= item.deadline:
            raise ReplyDeadlineExceeded("Reply wait deadline expired")
        return True

    async def close(value: Any) -> None:
        callback = close_response or getattr(value, "aclose", None) or getattr(value, "close", None)
        if callback is not None:
            result = callback(value) if close_response else callback()
            if inspect.isawaitable(result):
                await result

    async def drain_received(*, suppress_source_errors: bool = False) -> None:
        received = sorted(
            (
                task
                for task, (_, kind, _) in pending.items()
                if kind == "request" and task.done() and not task.cancelled() and task.exception() is None
            ),
            key=lambda task: completion_order.get(task, (sequence + 1, 0))[0],
        )
        for task in received:
            key, _, _ = pending[task]
            value = task.result()
            try:
                try:
                    await advance(key, value)
                except BaseException as error:
                    if not (
                        suppress_source_errors and drain_received_after_error and drain_received_after_error(error)
                    ):
                        raise
            finally:
                try:
                    await close(value)
                finally:
                    pending.pop(task)

    try:
        exhausted = False
        while not halted:
            while not exhausted and len(active) < concurrency and not should_stop():
                try:
                    key = next(unstarted)
                except StopIteration:
                    exhausted = True
                    break
                active.add(key)
                await advance(key, initial=True)
                if halted:
                    break
            if halted or should_stop():
                halted = True
                break
            if not ready and not pending:
                break
            slots = sum(kind != "delay" for _, kind, _ in pending.values())
            # A ready descriptor is dispatched only when its own pacing slot is
            # available. Delays release slots so other parents can make progress.
            for _ in range(len(ready)):
                if halted or should_stop():
                    halted = True
                    break
                key, item, queued = ready.popleft()
                if item.deadline is not None and clock() >= item.deadline:
                    await advance(key, error=ReplyDeadlineExceeded("Reply dispatch deadline expired"))
                elif isinstance(item, Delay):
                    launch(key, "delay", item, delayed(item))
                elif slots < concurrency:
                    metrics.queue_seconds += max(0.0, clock() - queued)
                    launch(key, "pace", item, paced(item))
                    slots += 1
                else:
                    ready.append((key, item, queued))
            if halted or should_stop():
                break
            if not pending:
                continue
            _, _, task = await completed.get()
            # A transport can finish in the same event-loop turn as pacing,
            # before its done callback has enqueued the response. Give that
            # callback priority before admitting another HTTP request.
            if pending[task][1] == "pace" and any(
                candidate.done() and phase == "request" for candidate, (_, phase, _) in pending.items()
            ):
                completed.put_nowait((1, completion_order[task][0], task))
                await asyncio.sleep(0)
                continue
            if inspect_received:
                inspect_received(
                    [
                        candidate.result()
                        for candidate, (_, phase, _) in pending.items()
                        if phase == "request"
                        and candidate.done()
                        and not candidate.cancelled()
                        and candidate.exception() is None
                    ]
                )
            key, kind, item = pending[task]
            _, finished_at = completion_order.pop(task)
            if kind == "request":
                metrics.completed_queue_seconds += max(0.0, clock() - finished_at)
            if kind == "pace":
                try:
                    task.result()
                    if should_stop():
                        halted = True
                        continue
                    refreshed = before_dispatch(item) if before_dispatch else None
                    descriptor = item if refreshed is None else refreshed
                    if not isinstance(descriptor, ReplyRequest):
                        raise TypeError("before_dispatch must return a ReplyRequest or None")
                    if should_stop():
                        halted = True
                        continue
                    if descriptor.deadline is not None and clock() >= descriptor.deadline:
                        raise ReplyDeadlineExceeded("Reply dispatch deadline expired")
                except Exception as error:
                    await advance(key, error=error)
                else:
                    metrics.requests += 1
                    # The completed pacing slot becomes the request slot rather
                    # than temporarily counting as an additional outstanding send.
                    pending.pop(task)
                    launch(key, "request", descriptor, transported(descriptor))
                finally:
                    pending.pop(task, None)
            else:
                # Keep this task/slot registered until owner advancement and
                # response cleanup finish. No task can mutate generator state.
                value = None
                received = False
                try:
                    try:
                        value = task.result()
                        received = kind == "request"
                    except Exception as error:
                        await advance(key, error=error)
                    else:
                        await advance(key, value)
                finally:
                    try:
                        if received:
                            await close(value)
                    finally:
                        pending.pop(task)
        if halted and drain_received_on_stop:
            # No yields from this drain can dispatch. Owner exceptions propagate
            # immediately: after a persistence/ownership failure no further branch
            # is allowed to commit. In-flight responses are cancelled below.
            await drain_received()
        return results
    except BaseException as error:
        if drain_received_after_error and drain_received_after_error(error):
            await drain_received(suppress_source_errors=not isinstance(error, ReplyDeadlineExceeded))
        raise
    finally:
        exception_active = sys.exc_info()[0] is not None

        async def cleanup():
            try:
                tasks = list(pending)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                if tasks:
                    drain = asyncio.gather(*tasks, return_exceptions=True)
                    while not drain.done():
                        try:
                            await asyncio.shield(drain)
                        except asyncio.CancelledError:
                            continue
                    for task in tasks:
                        if pending[task][1] == "request" and not task.cancelled() and task.exception() is None:
                            try:
                                await close(task.result())
                            except Exception:
                                pass
            finally:
                close_errors = []
                try:
                    for branch in lanes.values():
                        try:
                            await owner_call(branch.close)
                        except Exception as error:
                            close_errors.append(error)
                finally:
                    if owner_executor is not None:
                        # Every submitted transition has been joined above.
                        owner_executor.shutdown(wait=True)
                if close_errors and not exception_active:
                    raise ExceptionGroup("Reply generator cleanup failed", close_errors)

        # Repeated cancellation cannot interrupt generator/response cleanup or
        # shut the executor down while it still owns a committing transition.
        if owner_executor is not None:
            await _drain_pacing(cleanup())
        else:
            # Keep default-mode generator ContextVar tokens in the caller's
            # original task context, including during close.
            await cleanup()
