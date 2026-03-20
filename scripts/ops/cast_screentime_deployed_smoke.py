#!/usr/bin/env python3
"""Exercise the deployed cast-screentime control plane against a real backend."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import time
from pathlib import Path
from typing import Any

import requests


def _normalize_api_base_url(raw: str) -> str:
    clean = raw.strip().rstrip("/")
    if not clean:
        raise ValueError("TRR_API_URL is required")
    return clean if clean.endswith("/api/v1") else f"{clean}/api/v1"


def _service_headers() -> dict[str, str]:
    service_role_key = (os.getenv("TRR_CORE_SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not service_role_key:
        raise ValueError("TRR_CORE_SUPABASE_SERVICE_ROLE_KEY is required")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }
    internal_secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    if internal_secret:
        headers["X-TRR-Internal-Admin-Secret"] = internal_secret
    return headers


def _admin_request(
    session: requests.Session,
    *,
    api_base: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    response = session.request(
        method=method,
        url=f"{api_base}{path}",
        headers=_service_headers(),
        json=payload,
        timeout=(10, 60),
    )
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict):
        raise RuntimeError(f"Unexpected response body for {path}")
    return body


def _guess_content_type(video_path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(video_path))
    return guessed or "video/mp4"


def _upload_via_presigned_put(put_url: str, *, video_path: Path, content_type: str) -> dict[str, Any]:
    with video_path.open("rb") as handle:
        response = requests.put(
            put_url,
            data=handle,
            headers={"Content-Type": content_type},
            timeout=(30, 300),
        )
    response.raise_for_status()
    return {"status_code": response.status_code, "content_length": video_path.stat().st_size}


def _wait_for_run(
    session: requests.Session,
    *,
    api_base: str,
    run_id: str,
    poll_seconds: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_payload: dict[str, Any] | None = None
    while time.time() < deadline:
        payload = _admin_request(
            session,
            api_base=api_base,
            method="GET",
            path=f"/admin/cast-screentime/runs/{run_id}",
            payload=None,
        )
        last_payload = payload
        status = str(payload.get("status") or "")
        if status in {"success", "failed", "cancelled"}:
            return payload
        time.sleep(poll_seconds)
    if last_payload is None:
        raise TimeoutError(f"Run {run_id} never returned a payload")
    raise TimeoutError(f"Timed out waiting for run {run_id}: last_status={last_payload.get('status')}")


def _transition_review_status(
    session: requests.Session,
    *,
    api_base: str,
    run_id: str,
    review_status: str,
) -> dict[str, Any]:
    return _admin_request(
        session,
        api_base=api_base,
        method="POST",
        path=f"/admin/cast-screentime/runs/{run_id}/review-status",
        payload={"review_status": review_status},
    )


def _publish_run(session: requests.Session, *, api_base: str, run_id: str) -> dict[str, Any]:
    return _admin_request(
        session,
        api_base=api_base,
        method="POST",
        path=f"/admin/cast-screentime/runs/{run_id}/publish",
        payload={},
    )


def _build_owner_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "owner_scope": args.owner_scope,
        "owner_id": args.owner_id,
        "show_id": args.show_id,
        "season_id": args.season_id,
        "episode_id": args.episode_id,
    }


def _run_upload_flow(session: requests.Session, *, api_base: str, args: argparse.Namespace) -> dict[str, Any]:
    video_path = Path(args.video_file).expanduser().resolve()
    if not video_path.exists():
        raise FileNotFoundError(f"Video file does not exist: {video_path}")
    content_type = args.content_type or _guess_content_type(video_path)
    upload_payload = {
        **_build_owner_payload(args),
        "filename": args.filename or video_path.name,
        "content_type": content_type,
        "expected_size_bytes": video_path.stat().st_size,
        "video_class": args.video_class,
        "promo_subtype": args.promo_subtype,
    }
    upload_session = _admin_request(
        session,
        api_base=api_base,
        method="POST",
        path="/admin/cast-screentime/upload-sessions",
        payload=upload_payload,
    )
    put_url = str(upload_session.get("put_url") or "")
    if not put_url:
        raise RuntimeError("Upload session did not return a put_url")
    upload_result = _upload_via_presigned_put(put_url, video_path=video_path, content_type=content_type)
    complete_payload = _admin_request(
        session,
        api_base=api_base,
        method="POST",
        path=f"/admin/cast-screentime/upload-sessions/{upload_session['upload_session_id']}/complete",
        payload={"upload_session_id": upload_session["upload_session_id"]},
    )
    return {
        "mode": "upload",
        "upload_session": upload_session,
        "upload_result": upload_result,
        "video_asset": complete_payload.get("video_asset"),
    }


def _run_import_flow(session: requests.Session, *, api_base: str, args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        **_build_owner_payload(args),
        "source_mode": args.source_mode,
        "video_class": args.video_class,
        "promo_subtype": args.promo_subtype,
    }
    if args.source_mode == "social_youtube_row":
        payload["social_youtube_video_id"] = args.social_youtube_video_id
    else:
        payload["source_url"] = args.source_url
    response = _admin_request(
        session,
        api_base=api_base,
        method="POST",
        path="/admin/cast-screentime/video-assets/import",
        payload=payload,
    )
    return {
        "mode": "import",
        "import_response": response,
        "video_asset": response.get("video_asset"),
    }


def _create_run(session: requests.Session, *, api_base: str, video_asset_id: str) -> dict[str, Any]:
    response = _admin_request(
        session,
        api_base=api_base,
        method="POST",
        path=f"/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        payload={},
    )
    run = response.get("run")
    if not isinstance(run, dict) or not run.get("id"):
        raise RuntimeError("Run creation did not return a run payload")
    return run


def _advance_to_approved(
    session: requests.Session,
    *,
    api_base: str,
    run_id: str,
    current_review_status: str | None = None,
) -> list[dict[str, Any]]:
    current = str(current_review_status or "").strip() or "draft"
    transitions = []
    ordered = ("draft", "ready_for_review", "in_review", "approved")
    try:
        start_index = ordered.index(current)
    except ValueError:
        start_index = 0
    for status in ordered[start_index + 1 :]:
        transitions.append(_transition_review_status(session, api_base=api_base, run_id=run_id, review_status=status))
    return transitions


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("TRR_API_URL", ""),
        help="TRR API base URL. Defaults to TRR_API_URL.",
    )
    parser.add_argument("--owner-scope", choices=("show", "season", "episode"), required=True)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--show-id")
    parser.add_argument("--season-id")
    parser.add_argument("--episode-id")
    parser.add_argument("--video-class", choices=("episode", "promo"), default="episode")
    parser.add_argument("--promo-subtype", choices=("trailer", "episode_teaser"))
    parser.add_argument("--wait", action="store_true", help="Poll run status until it reaches a terminal state.")
    parser.add_argument("--poll-seconds", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--approve", action="store_true", help="Advance the run to approved after it succeeds.")
    parser.add_argument("--publish", action="store_true", help="Publish the run after approval. Episode assets only.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    upload_parser = subparsers.add_parser("upload-run", help="Upload a local video, promote it, and create a run.")
    upload_parser.add_argument("--video-file", required=True)
    upload_parser.add_argument("--filename")
    upload_parser.add_argument("--content-type")

    import_parser = subparsers.add_parser(
        "import-run",
        help="Mirror a remote source into a video asset, then create a run.",
    )
    import_parser.add_argument(
        "--source-mode",
        choices=("youtube_url", "external_url", "social_youtube_row"),
        required=True,
    )
    import_parser.add_argument("--source-url")
    import_parser.add_argument("--social-youtube-video-id")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    api_base = _normalize_api_base_url(args.api_base_url)
    session = requests.Session()
    summary: dict[str, Any]
    try:
        if args.command == "upload-run":
            summary = _run_upload_flow(session, api_base=api_base, args=args)
        else:
            if args.source_mode == "social_youtube_row" and not args.social_youtube_video_id:
                raise ValueError("--social-youtube-video-id is required for source_mode=social_youtube_row")
            if args.source_mode != "social_youtube_row" and not args.source_url:
                raise ValueError("--source-url is required for URL import modes")
            summary = _run_import_flow(session, api_base=api_base, args=args)

        video_asset = summary.get("video_asset")
        if not isinstance(video_asset, dict) or not video_asset.get("id"):
            raise RuntimeError("Smoke flow did not produce a video asset")
        run = _create_run(session, api_base=api_base, video_asset_id=str(video_asset["id"]))
        summary["run"] = run

        if args.wait:
            summary["terminal_run"] = _wait_for_run(
                session,
                api_base=api_base,
                run_id=str(run["id"]),
                poll_seconds=args.poll_seconds,
                timeout_seconds=args.timeout_seconds,
            )

        if args.approve:
            current_review_status = None
            terminal_run = summary.get("terminal_run")
            if isinstance(terminal_run, dict):
                current_review_status = str(terminal_run.get("review_status") or "").strip() or None
            summary["review_transitions"] = _advance_to_approved(
                session,
                api_base=api_base,
                run_id=str(run["id"]),
                current_review_status=current_review_status,
            )

        if args.publish:
            if args.video_class != "episode":
                raise ValueError("--publish is only valid for episode assets")
            if not args.approve:
                raise ValueError("--publish requires --approve")
            summary["publish_result"] = _publish_run(session, api_base=api_base, run_id=str(run["id"]))
    finally:
        session.close()

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
