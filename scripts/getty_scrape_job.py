#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from trr_backend.utils.env import load_env  # noqa: E402


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class GettyScrapeStateWriter:
    def __init__(self, state_path: Path) -> None:
        self._state_path = state_path
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {}

    def init_state(self, *, person_name: str, show_name: str | None, mode: str) -> None:
        self._state = {
            "status": "queued",
            "stage": "queued",
            "progress_message": "Getty discovery queued.",
            "prefetch_mode": mode,
            "person_name": person_name,
            "show_name": show_name,
            "discovery_ready": False,
            "enrichment_status": "pending" if mode == "discovery" else "running",
            "created_at": _utcnow_iso(),
            "started_at": None,
            "completed_at": None,
            "heartbeat_at": _utcnow_iso(),
            "updated_at": _utcnow_iso(),
            "last_error": None,
            "last_error_code": None,
            "queries_total": 0,
            "queries_completed": 0,
            "query_summaries_live": [],
            "active_query": None,
        }
        self._flush()

    def update(self, **patch: Any) -> None:
        with self._lock:
            self._state.update({key: value for key, value in patch.items() if value is not None})
            self._state["updated_at"] = _utcnow_iso()
            self._flush()

    def touch_heartbeat(self) -> None:
        with self._lock:
            now = _utcnow_iso()
            self._state["heartbeat_at"] = now
            self._state["updated_at"] = now
            self._flush()

    def _flush(self) -> None:
        temp_path = self._state_path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(self._state, default=str), encoding="utf-8")
        temp_path.replace(self._state_path)


def _normalize_error_code(message: str) -> str:
    normalized = message.lower()
    if "timed out" in normalized or "timeout" in normalized:
        return "TIMEOUT"
    if "sign-in" in normalized or "authenticated" in normalized:
        return "AUTH_REQUIRED"
    if "challenge" in normalized:
        return "CHALLENGE_PAGE"
    return "SCRAPE_FAILED"


def main() -> int:
    load_env()
    parser = argparse.ArgumentParser(description="Run Getty scrape job and persist progress state.")
    parser.add_argument("person_name", help="Getty person phrase")
    parser.add_argument("--state-file", required=True, help="Path to the mutable JSON state file")
    parser.add_argument("--show-name", dest="show_name", default=None, help="Optional show name")
    parser.add_argument(
        "--mode",
        dest="mode",
        default="discovery",
        choices=("discovery", "full"),
        help="Getty prefetch mode",
    )
    parser.add_argument(
        "--heartbeat-interval-seconds",
        dest="heartbeat_interval_seconds",
        type=float,
        default=5.0,
        help="Interval between heartbeat file touches",
    )
    args = parser.parse_args()

    from trr_backend.integrations.getty_local_prefetch import fetch_person_getty_prefetch_payload

    state_writer = GettyScrapeStateWriter(Path(args.state_file))
    state_writer.init_state(person_name=args.person_name, show_name=args.show_name, mode=args.mode)

    stop_heartbeat = threading.Event()

    def _heartbeat_loop() -> None:
        while not stop_heartbeat.wait(max(1.0, float(args.heartbeat_interval_seconds))):
            state_writer.touch_heartbeat()

    heartbeat_thread = threading.Thread(target=_heartbeat_loop, name="getty-scrape-heartbeat", daemon=True)
    heartbeat_thread.start()

    live_query_summaries: dict[str, dict[str, Any]] = {}

    def _on_progress(payload: dict[str, Any]) -> None:
        label = str(payload.get("label") or "").strip() or None
        scope = str(payload.get("scope") or "").strip() or None
        active_query = None
        if label or scope or payload.get("phrase") or payload.get("query_url"):
            active_query = {
                "label": label,
                "scope": scope,
                "phrase": payload.get("phrase"),
                "query_url": payload.get("query_url"),
            }
        if payload.get("type") == "query_completed":
            summary = {
                "label": label,
                "scope": scope,
                "phrase": payload.get("phrase"),
                "query_url": payload.get("query_url"),
                "site_image_total": payload.get("site_image_total"),
                "site_event_total": payload.get("site_event_total"),
                "site_video_total": payload.get("site_video_total"),
                "fetched_asset_total": payload.get("fetched_asset_total"),
                "usable_after_dedupe_total": payload.get("usable_after_dedupe_total"),
                "overlap_with_prior_queries": payload.get("overlap_with_prior_queries"),
                "termination_reason": payload.get("termination_reason"),
                "expected_page": payload.get("expected_page"),
                "current_page": payload.get("current_page"),
                "response_url": payload.get("response_url"),
                "page_signature": payload.get("page_signature"),
                "first_editorial_ids": payload.get("first_editorial_ids"),
                "session_validated": payload.get("session_validated"),
                "session_truncated": payload.get("session_truncated"),
            }
            key = scope or label or f"query-{len(live_query_summaries) + 1}"
            live_query_summaries[key] = summary
        state_writer.update(
            status="running",
            stage=str(payload.get("phase") or "discovery"),
            progress_message=str(payload.get("message") or "Getty scrape running."),
            active_query=active_query,
            requested_page=payload.get("requested_page"),
            expected_page=payload.get("expected_page"),
            current_page=payload.get("current_page"),
            response_url=payload.get("response_url"),
            fetched_candidates_total=payload.get("fetched_candidates_total"),
            page_candidate_count=payload.get("page_candidate_count"),
            new_unique_count=payload.get("new_unique_count"),
            termination_reason=payload.get("termination_reason"),
            page_classification=payload.get("page_classification"),
            page_signature=payload.get("page_signature"),
            first_editorial_ids=payload.get("first_editorial_ids"),
            site_image_total=payload.get("site_image_total"),
            site_event_total=payload.get("site_event_total"),
            site_video_total=payload.get("site_video_total"),
            queries_total=payload.get("queries_total"),
            queries_completed=payload.get("queries_completed"),
            merged_total=payload.get("merged_total"),
            merged_events_total=payload.get("merged_events_total"),
            auth_mode=payload.get("auth_mode"),
            auth_warning=payload.get("auth_warning"),
            session_validated=payload.get("session_validated"),
            session_truncated=payload.get("session_truncated"),
            elapsed_seconds=payload.get("elapsed_seconds"),
            query_summaries_live=list(live_query_summaries.values()),
        )

    def _heartbeat() -> None:
        state_writer.touch_heartbeat()

    try:
        state_writer.update(
            status="running",
            stage="starting",
            started_at=_utcnow_iso(),
            progress_message=(
                f'Fetching Getty search candidates for "{args.person_name}" '
                "via the codex Chrome profile..."
            ),
        )
        result = fetch_person_getty_prefetch_payload(
            args.person_name,
            show_name=args.show_name,
            mode=args.mode,
            progress_cb=_on_progress,
            heartbeat_cb=_heartbeat,
        )
        state_writer.update(
            **result,
            status="completed",
            stage="completed",
            progress_message=(
                f"Getty {args.mode} complete: {int(result.get('merged_total') or 0)} assets"
                f" in {float(result.get('elapsed_seconds') or 0):.1f}s."
            ),
            completed_at=_utcnow_iso(),
            query_summaries_live=list(live_query_summaries.values()),
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        error_code = getattr(exc, "code", None)
        state_writer.update(
            status="failed",
            stage="failed",
            progress_message="Getty scrape failed.",
            last_error=message,
            last_error_code=str(error_code or _normalize_error_code(message)),
            completed_at=_utcnow_iso(),
        )
        return 1
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=1.0)
        state_writer.touch_heartbeat()


if __name__ == "__main__":
    raise SystemExit(main())
