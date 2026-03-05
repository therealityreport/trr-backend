"""Execution manager and SSE helpers for resumable admin operations."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import socket
import time
from collections.abc import AsyncGenerator, Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from typing import Any

from fastapi import Request
from fastapi.responses import StreamingResponse

from trr_backend.job_plane import canonical_execution_mode, execution_owner_label, is_remote_job_plane_enabled
from trr_backend.repositories import admin_operations

logger = logging.getLogger(__name__)

_OPERATION_WORKERS = max(1, int(os.getenv("TRR_ADMIN_OPERATION_WORKERS", "8")))
_EVENT_POLL_INTERVAL_SECONDS = float(os.getenv("TRR_ADMIN_OPERATION_STREAM_POLL_SECONDS", "0.5"))
_RETENTION_PURGE_INTERVAL_SECONDS = max(60, int(os.getenv("TRR_ADMIN_OPERATION_PURGE_INTERVAL_SECONDS", "3600")))
_RETENTION_HOURS = max(1, int(os.getenv("TRR_ADMIN_OPERATION_RETENTION_HOURS", "336")))
_OPERATION_CLAIM_LEASE_SECONDS = max(30, int(os.getenv("TRR_ADMIN_OPERATION_CLAIM_LEASE_SECONDS", "300")))
_OPERATION_RETRY_BASE_SECONDS = max(5, int(os.getenv("TRR_ADMIN_OPERATION_RETRY_BASE_SECONDS", "20")))
_OPERATION_RETRY_MAX_ATTEMPTS = max(1, int(os.getenv("TRR_ADMIN_OPERATION_RETRY_MAX_ATTEMPTS", "3")))

_EXECUTOR = ThreadPoolExecutor(max_workers=_OPERATION_WORKERS, thread_name_prefix="admin-op")
_FUTURES: dict[str, Future[Any]] = {}
_FUTURES_LOCK = Lock()
_LAST_PURGE_MONOTONIC = 0.0


def _to_json_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        parsed = raw.strip()
        if not parsed:
            return {}
        try:
            decoded = json.loads(parsed)
            if isinstance(decoded, dict):
                return decoded
            return {"value": decoded}
        except Exception:  # noqa: BLE001
            return {"message": parsed}
    if raw is None:
        return {}
    return {"value": raw}


def _extract_sse_blocks(buffer: str) -> tuple[list[str], str]:
    normalized = buffer.replace("\r\n", "\n")
    blocks = normalized.split("\n\n")
    if len(blocks) == 1:
        return [], normalized
    return [block for block in blocks[:-1] if block.strip()], blocks[-1]


def _parse_sse_block(block: str) -> tuple[str, dict[str, Any]]:
    event_type = "message"
    data_lines: list[str] = []
    for line in block.split("\n"):
        if not line or line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_type = line.split(":", 1)[1].strip() or "message"
            continue
        if line.startswith("data:"):
            data_lines.append(line.split(":", 1)[1].lstrip())
    payload_raw = "\n".join(data_lines).strip()
    payload = _to_json_payload(payload_raw)
    return event_type, payload


def _sse_chunk(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=True, default=str)}\n\n"


def _ensure_operation_payload(
    operation_id: str,
    payload: dict[str, Any],
    *,
    request_id: str | None,
) -> dict[str, Any]:
    merged = dict(payload)
    # Always enforce canonical operation identity from persisted operation row.
    merged["operation_id"] = operation_id
    # Event sequence is assigned by persisted event rows, never producer payload.
    merged.pop("event_seq", None)
    if request_id and "request_id" not in merged:
        merged["request_id"] = request_id
    return merged


def _update_operation_from_event(
    operation_id: str,
    *,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    lowered = event_type.strip().lower()
    if lowered == "complete":
        admin_operations.update_operation_status(
            operation_id,
            status="completed",
            result_payload=payload,
            progress_payload=payload,
        )
        return
    if lowered == "error":
        if admin_operations.is_cancel_requested(operation_id):
            admin_operations.update_operation_status(
                operation_id,
                status="cancelled",
                error_payload=payload,
                progress_payload=payload,
            )
            return
        admin_operations.update_operation_status(
            operation_id,
            status="failed",
            error_payload=payload,
            progress_payload=payload,
        )
        return
    admin_operations.update_operation_progress(operation_id, progress_payload=payload)


def _append_event_and_update(
    operation_id: str,
    *,
    event_type: str,
    payload: dict[str, Any],
    request_id: str | None,
) -> None:
    enriched = _ensure_operation_payload(operation_id, payload, request_id=request_id)
    event_row = admin_operations.append_operation_event(
        operation_id,
        event_type=event_type,
        event_payload=enriched,
    )
    seq = int(event_row.get("event_seq") or 0)
    if seq > 0:
        enriched["event_seq"] = seq
    _update_operation_from_event(operation_id, event_type=event_type, payload=enriched)


def _consume_sync_chunks(
    operation_id: str,
    chunks: Iterable[str | bytes],
    *,
    request_id: str | None,
) -> None:
    if isinstance(chunks, (str, bytes)):
        chunks = [chunks]
    buffer = ""
    for raw_chunk in chunks:
        if raw_chunk is None:
            continue
        chunk = raw_chunk.decode("utf-8", errors="replace") if isinstance(raw_chunk, bytes) else str(raw_chunk)
        buffer += chunk
        blocks, buffer = _extract_sse_blocks(buffer)
        for block in blocks:
            event_type, payload = _parse_sse_block(block)
            _append_event_and_update(operation_id, event_type=event_type, payload=payload, request_id=request_id)

    if buffer.strip():
        event_type, payload = _parse_sse_block(buffer)
        _append_event_and_update(operation_id, event_type=event_type, payload=payload, request_id=request_id)


async def _consume_async_chunks(
    operation_id: str,
    chunks: AsyncGenerator[str | bytes, None],
    *,
    request_id: str | None,
) -> None:
    buffer = ""
    async for raw_chunk in chunks:
        if raw_chunk is None:
            continue
        chunk = raw_chunk.decode("utf-8", errors="replace") if isinstance(raw_chunk, bytes) else str(raw_chunk)
        buffer += chunk
        blocks, buffer = _extract_sse_blocks(buffer)
        for block in blocks:
            event_type, payload = _parse_sse_block(block)
            _append_event_and_update(operation_id, event_type=event_type, payload=payload, request_id=request_id)

    if buffer.strip():
        event_type, payload = _parse_sse_block(buffer)
        _append_event_and_update(operation_id, event_type=event_type, payload=payload, request_id=request_id)


def _maybe_purge_retained_operations() -> None:
    global _LAST_PURGE_MONOTONIC
    now = time.monotonic()
    if _LAST_PURGE_MONOTONIC and (now - _LAST_PURGE_MONOTONIC) < _RETENTION_PURGE_INTERVAL_SECONDS:
        return
    _LAST_PURGE_MONOTONIC = now
    try:
        deleted = admin_operations.purge_old_operations(retention_hours=_RETENTION_HOURS)
        if deleted:
            logger.info("Purged old admin operations: deleted=%s retention_hours=%s", deleted, _RETENTION_HOURS)
    except Exception:  # noqa: BLE001
        logger.debug("Failed to purge old admin operations", exc_info=True)


def _run_operation_worker(
    operation_id: str,
    producer: Callable[[], Any],
    *,
    request_id: str | None,
) -> None:
    _maybe_purge_retained_operations()
    admin_operations.touch_operation_started(operation_id)

    try:
        produced = producer()
        if produced is None:
            pass
        elif inspect.isasyncgen(produced):
            asyncio.run(_consume_async_chunks(operation_id, produced, request_id=request_id))
        elif inspect.isawaitable(produced):
            awaited = asyncio.run(produced)
            if inspect.isasyncgen(awaited):
                asyncio.run(_consume_async_chunks(operation_id, awaited, request_id=request_id))
            elif isinstance(awaited, (list, tuple, set)):
                _consume_sync_chunks(operation_id, awaited, request_id=request_id)
            elif awaited is not None:
                _consume_sync_chunks(operation_id, [str(awaited)], request_id=request_id)
        else:
            _consume_sync_chunks(operation_id, produced, request_id=request_id)

        op = admin_operations.get_operation(operation_id)
        if op and not admin_operations.operation_is_terminal(str(op.get("status") or "")):
            if admin_operations.is_cancel_requested(operation_id):
                payload = {"stage": "cancelled", "message": "Operation cancelled", "operation_id": operation_id}
                _append_event_and_update(operation_id, event_type="error", payload=payload, request_id=request_id)
                admin_operations.update_operation_status(
                    operation_id,
                    status="cancelled",
                    error_payload=payload,
                    progress_payload=payload,
                )
            else:
                admin_operations.update_operation_status(operation_id, status="completed")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Admin operation worker failed: operation_id=%s", operation_id)
        payload = {
            "stage": "operation",
            "error": "Operation failed",
            "detail": str(exc),
            "operation_id": operation_id,
        }
        try:
            _append_event_and_update(operation_id, event_type="error", payload=payload, request_id=request_id)
        finally:
            admin_operations.mark_operation_failed(operation_id, error_payload=payload)
    finally:
        with _FUTURES_LOCK:
            _FUTURES.pop(operation_id, None)


def ensure_operation_execution(
    operation_id: str,
    *,
    producer: Callable[[], Any],
    request_id: str | None = None,
) -> bool:
    with _FUTURES_LOCK:
        existing = _FUTURES.get(operation_id)
        if existing and not existing.done():
            return False
        future = _EXECUTOR.submit(_run_operation_worker, operation_id, producer, request_id=request_id)
        _FUTURES[operation_id] = future
    return True


def _stream_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }


async def operation_stream_generator(
    operation_id: str,
    *,
    after_seq: int = 0,
    request: Request | None = None,
) -> AsyncGenerator[str, None]:
    next_seq = max(0, int(after_seq))

    while True:
        replay_after = next_seq
        events = admin_operations.normalize_operation_events(
            admin_operations.stream_events_after_seq(operation_id, after_seq=next_seq, limit=500)
        )
        if events:
            logger.info(
                "Admin operation replay: operation_id=%s after_seq=%s events_replayed=%s",
                operation_id,
                replay_after,
                len(events),
            )
        for event in events:
            seq = int(event.get("event_seq") or 0)
            event_payload = _to_json_payload(event.get("event_payload"))
            payload = _ensure_operation_payload(
                operation_id,
                event_payload,
                request_id=str(event_payload.get("request_id") or "") or None,
            )
            if seq > 0:
                payload["event_seq"] = seq
                next_seq = seq
            event_type = str(event.get("event_type") or "message")
            yield _sse_chunk(event_type, payload)

        operation = admin_operations.get_operation(operation_id)
        if not operation:
            error_payload = {
                "operation_id": operation_id,
                "stage": "operation",
                "error": "Operation not found",
            }
            yield _sse_chunk("error", error_payload)
            return

        if admin_operations.operation_is_terminal(str(operation.get("status") or "")):
            return

        if request is not None:
            try:
                if await request.is_disconnected():
                    return
            except Exception:  # noqa: BLE001
                pass

        await asyncio.sleep(_EVENT_POLL_INTERVAL_SECONDS)


def operation_stream_response(
    operation_id: str,
    *,
    after_seq: int = 0,
    request: Request | None = None,
) -> StreamingResponse:
    return StreamingResponse(
        operation_stream_generator(operation_id, after_seq=after_seq, request=request),
        media_type="text/event-stream",
        headers=_stream_headers(),
    )


def _resolve_remote_operation_producer(
    *,
    operation_type: str,
    request_payload: dict[str, Any],
) -> Callable[[], Any] | None:
    """Build a producer callable from persisted operation metadata."""
    normalized_type = str(operation_type or "").strip().lower()
    if normalized_type == "admin_asset_batch_jobs":
        from api.routers.admin_asset_batch_jobs import build_batch_jobs_operation_producer

        return build_batch_jobs_operation_producer(request_payload=request_payload)
    if normalized_type == "admin_scrape_import_images":
        from api.routers.admin_scrape import build_scrape_import_operation_producer

        return build_scrape_import_operation_producer(request_payload=request_payload)
    if normalized_type == "admin_show_links_discover":
        from api.routers.admin_show_links import build_show_links_discovery_operation_producer

        return build_show_links_discovery_operation_producer(request_payload=request_payload)
    if normalized_type == "admin_show_bravo_preview":
        from api.routers.admin_show_bravo import build_bravo_preview_operation_producer

        return build_bravo_preview_operation_producer(request_payload=request_payload)
    if normalized_type == "admin_show_refresh":
        from api.routers.admin_show_sync import build_show_refresh_operation_producer

        return build_show_refresh_operation_producer(request_payload=request_payload)
    if normalized_type == "admin_show_refresh_photos":
        from api.routers.admin_show_sync import build_show_refresh_photos_operation_producer

        return build_show_refresh_photos_operation_producer(request_payload=request_payload)
    if normalized_type == "admin_person_refresh_images":
        from api.routers.admin_person_images import build_person_refresh_images_operation_producer

        return build_person_refresh_images_operation_producer(request_payload=request_payload)
    if normalized_type == "admin_person_reprocess_images":
        from api.routers.admin_person_images import build_person_reprocess_images_operation_producer

        return build_person_reprocess_images_operation_producer(request_payload=request_payload)
    return None


def _run_remote_claimed_operation(operation: dict[str, Any]) -> None:
    operation_id = str(operation.get("id") or "").strip()
    if not operation_id:
        return

    operation_type = str(operation.get("operation_type") or "").strip()
    request_payload = operation.get("request_payload") if isinstance(operation.get("request_payload"), dict) else {}
    request_id = str(operation.get("request_id") or "").strip() or None
    claim_token = str(operation.get("claim_token") or "").strip() or None

    producer = _resolve_remote_operation_producer(operation_type=operation_type, request_payload=request_payload)
    if producer is None:
        logger.error(
            "Unsupported remote admin operation type: operation_id=%s operation_type=%s request_id=%s",
            operation_id,
            operation_type,
            request_id,
        )
        payload = {
            "stage": "operation",
            "operation_id": operation_id,
            "error": "Unsupported remote admin operation type",
            "operation_type": operation_type,
        }
        _append_event_and_update(operation_id, event_type="error", payload=payload, request_id=request_id)
        admin_operations.update_operation_status(
            operation_id,
            status="failed",
            error_payload=payload,
            progress_payload=payload,
        )
        admin_operations.release_operation_claim(operation_id, claim_token=claim_token)
        return

    try:
        _run_operation_worker(operation_id, producer, request_id=request_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Remote admin operation execution crashed: operation_id=%s", operation_id)
        payload = {
            "stage": "operation",
            "operation_id": operation_id,
            "error": "Remote worker crash",
            "detail": str(exc),
        }
        attempts = int(operation.get("attempt_count") or 1)
        delay = _OPERATION_RETRY_BASE_SECONDS * max(1, attempts)
        admin_operations.mark_operation_retry(
            operation_id,
            claim_token=claim_token,
            error_payload=payload,
            retry_delay_seconds=delay,
            max_attempts=_OPERATION_RETRY_MAX_ATTEMPTS,
        )
    finally:
        admin_operations.release_operation_claim(operation_id, claim_token=claim_token)


def claim_and_execute_next_operation(
    *,
    worker_id: str,
    operation_types: Iterable[str] | None = None,
) -> bool:
    claimed = admin_operations.claim_next_operation(
        worker_id,
        lease_seconds=_OPERATION_CLAIM_LEASE_SECONDS,
        operation_types=operation_types,
    )
    if not claimed:
        return False

    logger.info(
        "Admin operation claimed: worker_id=%s operation_id=%s operation_type=%s attempt=%s",
        worker_id,
        str(claimed.get("id") or ""),
        str(claimed.get("operation_type") or ""),
        int(claimed.get("attempt_count") or 0),
    )
    _run_remote_claimed_operation(claimed)
    return True


def run_remote_operation_worker_loop(
    *,
    worker_id: str | None = None,
    operation_types: Iterable[str] | None = None,
    poll_seconds: float = 2.0,
    once: bool = False,
) -> int:
    normalized_worker_id = (worker_id or "").strip() or f"admin-ops:{socket.gethostname()}:{os.getpid()}"
    safe_poll = max(0.2, float(poll_seconds))

    while True:
        claimed = claim_and_execute_next_operation(worker_id=normalized_worker_id, operation_types=operation_types)
        if once:
            return 0 if claimed else 1
        if not claimed:
            time.sleep(safe_poll)


def start_operation_for_stream(
    *,
    operation_type: str,
    producer: Callable[[], Any] | None,
    request_payload: dict[str, Any] | None,
    initiated_by: str | None,
    request: Request | None,
    allow_attach: bool = True,
) -> dict[str, Any]:
    request_id = None
    client_session_id = None
    client_workflow_id = None
    if request is not None:
        request_id = (request.headers.get("x-trr-request-id") or "").strip() or None
        client_session_id = (request.headers.get("x-trr-tab-session-id") or "").strip() or None
        client_workflow_id = (request.headers.get("x-trr-flow-key") or "").strip() or None

    operation, attached = admin_operations.create_or_attach_operation(
        operation_type=operation_type,
        request_payload=request_payload,
        initiated_by=initiated_by,
        request_id=request_id,
        client_session_id=client_session_id,
        client_workflow_id=client_workflow_id,
        allow_attach=allow_attach,
    )

    operation_id = str(operation.get("id") or "")
    if not operation_id:
        raise RuntimeError("Failed to create admin operation")

    remote_mode = is_remote_job_plane_enabled()
    logger.info(
        (
            "Admin operation create_or_attach: operation_type=%s "
            "operation_id=%s attached=%s execution_owner=%s execution_mode=%s "
            "client_session_id=%s client_workflow_id=%s request_id=%s"
        ),
        operation_type,
        operation_id,
        attached,
        execution_owner_label(),
        canonical_execution_mode(),
        client_session_id,
        client_workflow_id,
        request_id,
    )

    if not attached:
        if remote_mode:
            logger.info("Queued admin operation for remote worker ownership: operation_id=%s", operation_id)
        else:
            if producer is None:
                raise RuntimeError("Local admin operation execution requires producer callable")
            ensure_operation_execution(operation_id, producer=producer, request_id=request_id)

    refreshed = admin_operations.get_operation(operation_id)
    if not refreshed:
        raise RuntimeError("Operation created but could not be loaded")

    # Emit immediate envelope so clients can persist operation_id/event_seq before progress arrives.
    admin_operations.append_operation_event(
        operation_id,
        event_type="operation",
        event_payload={
            "operation_id": operation_id,
            "status": str(refreshed.get("status") or "pending"),
            "attached": bool(attached),
            "request_id": request_id,
            "execution_owner": execution_owner_label(),
            "execution_mode_canonical": canonical_execution_mode(),
        },
    )

    refreshed["attached"] = attached
    refreshed["execution_owner"] = execution_owner_label()
    refreshed["execution_mode_canonical"] = canonical_execution_mode()
    return refreshed
