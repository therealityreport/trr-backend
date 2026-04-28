"""Bounded background task queue snapshots for local social control-plane work."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from queue import Full, Queue
from threading import Lock, Thread
from time import monotonic
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _QueuedTask:
    key: str
    target: Callable[..., Any]
    kwargs: dict[str, Any]
    queued_at: float


@dataclass
class _GroupState:
    queue: Queue[_QueuedTask]
    active_keys: set[str] = field(default_factory=set)
    queued_keys: set[str] = field(default_factory=set)
    queued_at_by_key: dict[str, float] = field(default_factory=dict)
    worker_started: bool = False
    completed_count: int = 0
    exception_count: int = 0


_LOCK = Lock()
_GROUPS: dict[str, _GroupState] = {}

_GROUP_LIMIT_ENV: dict[str, tuple[str, int]] = {
    "catalog-finalize": ("TRR_CATALOG_FINALIZER_MAX_ACTIVE", 1),
    "social-dispatch": ("TRR_SOCIAL_DISPATCH_BACKGROUND_MAX_ACTIVE", 1),
}
_GROUP_QUEUE_ENV: dict[str, tuple[str, int]] = {
    "catalog-finalize": ("TRR_CATALOG_FINALIZER_QUEUE_MAXSIZE", 25),
    "social-dispatch": ("TRR_SOCIAL_DISPATCH_BACKGROUND_QUEUE_MAXSIZE", 25),
}


def _env_int(name: str, *, default: int, minimum: int = 1) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        logger.warning("[background-task-queue] invalid integer env %s=%r; using %s", name, raw, default)
        return default


def _env_name_for_group(group: str, *, suffix: str) -> str:
    return f"TRR_{group.upper().replace('-', '_')}_{suffix}"


def _max_active_for_group(group: str) -> int:
    env_name, default = _GROUP_LIMIT_ENV.get(group, (_env_name_for_group(group, suffix="MAX_ACTIVE"), 1))
    return _env_int(env_name, default=default)


def _queue_maxsize_for_group(group: str) -> int:
    env_name, default = _GROUP_QUEUE_ENV.get(group, (_env_name_for_group(group, suffix="QUEUE_MAXSIZE"), 25))
    return _env_int(env_name, default=default)


def _normalize_group(group: str) -> str:
    return str(group or "").strip() or "default"


def _normalize_key(key: str) -> str:
    return str(key or "").strip()


def _group_state(group: str) -> _GroupState:
    state = _GROUPS.get(group)
    if state is None:
        state = _GroupState(queue=Queue(maxsize=_queue_maxsize_for_group(group)))
        _GROUPS[group] = state
    return state


def _ensure_worker_started(group: str, state: _GroupState) -> None:
    if state.worker_started:
        return
    state.worker_started = True
    Thread(
        target=_worker_loop,
        name=f"{group}:queue-worker",
        daemon=True,
        args=(group,),
    ).start()


def _worker_loop(group: str) -> None:
    while True:
        with _LOCK:
            state = _group_state(group)
        queued_task = state.queue.get()
        try:
            with _LOCK:
                state.queued_keys.discard(queued_task.key)
                state.queued_at_by_key.pop(queued_task.key, None)
                state.active_keys.add(queued_task.key)
            try:
                queued_task.target(**queued_task.kwargs)
                with _LOCK:
                    state.completed_count += 1
            except Exception:
                with _LOCK:
                    state.exception_count += 1
                logger.exception("[background-task-queue] task failed group=%s key=%s", group, queued_task.key)
        finally:
            with _LOCK:
                state.active_keys.discard(queued_task.key)
            state.queue.task_done()


def submit_named_background_task(
    *,
    group: str,
    key: str,
    thread_name: str,
    target: Callable[..., Any],
    kwargs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_group = _normalize_group(group)
    normalized_key = _normalize_key(key)
    if not normalized_key:
        return {
            "submitted": False,
            "state": "missing_key",
            "group": normalized_group,
            "key": normalized_key,
        }

    queued_at = monotonic()
    with _LOCK:
        state = _group_state(normalized_group)
        _ensure_worker_started(normalized_group, state)
        if normalized_key in state.active_keys or normalized_key in state.queued_keys:
            return {
                "submitted": False,
                "state": "duplicate",
                "group": normalized_group,
                "key": normalized_key,
                "active_count": len(state.active_keys),
                "queued_count": len(state.queued_keys),
                "queue_size": state.queue.qsize(),
                "max_active": _max_active_for_group(normalized_group),
                "queue_maxsize": state.queue.maxsize,
            }

        task = _QueuedTask(
            key=normalized_key,
            target=target,
            kwargs=dict(kwargs or {}),
            queued_at=queued_at,
        )
        try:
            state.queue.put_nowait(task)
        except Full:
            return {
                "submitted": False,
                "state": "queue_full",
                "group": normalized_group,
                "key": normalized_key,
                "active_count": len(state.active_keys),
                "queued_count": len(state.queued_keys),
                "queue_size": state.queue.qsize(),
                "max_active": _max_active_for_group(normalized_group),
                "queue_maxsize": state.queue.maxsize,
            }

        state.queued_keys.add(normalized_key)
        state.queued_at_by_key[normalized_key] = queued_at
        logger.info(
            "[background-task-queue] submitted group=%s key=%s state=queued queue_size=%s",
            normalized_group,
            normalized_key,
            state.queue.qsize(),
        )
        return {
            "submitted": True,
            "state": "queued",
            "group": normalized_group,
            "key": normalized_key,
            "thread_name": thread_name,
            "active_count": len(state.active_keys),
            "queued_count": len(state.queued_keys),
            "queue_size": state.queue.qsize(),
            "max_active": _max_active_for_group(normalized_group),
            "queue_maxsize": state.queue.maxsize,
        }


def background_task_snapshot() -> dict[str, Any]:
    now = monotonic()
    with _LOCK:
        groups = {
            group: {
                "active_count": len(state.active_keys),
                "active_keys": sorted(state.active_keys),
                "queued_count": len(state.queued_keys),
                "queued_keys": sorted(state.queued_keys),
                "queue_size": state.queue.qsize(),
                "queue_maxsize": state.queue.maxsize,
                "max_active": _max_active_for_group(group),
                "oldest_queued_age_seconds": (
                    round(now - min(state.queued_at_by_key.values()), 3) if state.queued_at_by_key else None
                ),
                "completed_count": state.completed_count,
                "exception_count": state.exception_count,
            }
            for group, state in sorted(_GROUPS.items())
        }
    return {"groups": groups}
